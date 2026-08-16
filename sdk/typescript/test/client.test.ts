import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { AlreadyClosedError, NanocachedClient, WrongNodeError } from "../src/index.js";
import { HashRing } from "../src/hashRing.js";
import { startMockDiscovery, startMockNode, type MockNode } from "./mockServers.js";

describe("NanocachedClient against a single node", () => {
  it("round-trips set/get/delete", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await client.set("greeting", "hello");
        assert.deepEqual(await client.get("greeting"), Buffer.from("hello"));

        assert.equal(await client.delete("greeting"), true);
        assert.equal(await client.get("greeting"), null);
        assert.equal(await client.delete("greeting"), false);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("handles binary keys/values and empty values", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        const key = Uint8Array.from([1, 2, 3]);
        const value = Uint8Array.from([0, 255, 10]);
        await client.set(key, value);
        assert.deepEqual(await client.get(key), Buffer.from(value));

        await client.set("empty", "");
        assert.deepEqual(await client.get("empty"), Buffer.alloc(0));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("stores with a TTL and rejects invalid TTLs before writing", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await client.set("k", "v", { ttlSeconds: 60 });
        assert.deepEqual(await client.get("k"), Buffer.from("v"));

        await assert.rejects(client.set("k", "v", { ttlSeconds: -1 }), RangeError);
        // The rejected set must not have poisoned the shared connection.
        assert.deepEqual(await client.get("k"), Buffer.from("v"));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("pipelines concurrent requests on one connection", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await Promise.all(Array.from({ length: 20 }, (_, i) => client.set(`key-${i}`, `value-${i}`)));
        const values = await Promise.all(Array.from({ length: 20 }, (_, i) => client.get(`key-${i}`)));
        values.forEach((value, i) => assert.deepEqual(value, Buffer.from(`value-${i}`)));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("authenticates with a shared secret", async () => {
    const node = await startMockNode({ requiredSecret: "s3cret" });
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port, authSecret: "s3cret" });
      try {
        await client.set("k", "v");
        assert.deepEqual(await client.get("k"), Buffer.from("v"));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("reports a missing secret differently from a wrong one", async () => {
    const node = await startMockNode({ requiredSecret: "s3cret" });
    try {
      await assert.rejects(
        NanocachedClient.connect({ host: "127.0.0.1", port: node.port }),
        /requires authentication/,
      );
      await assert.rejects(
        NanocachedClient.connect({ host: "127.0.0.1", port: node.port, authSecret: "wrong" }),
        /authentication failed/,
      );
    } finally {
      await node.close();
    }
  });

  it("propagates WrongNodeError in single mode (no discovery to refresh from)", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        node.answerWrongNodeOnce();
        await assert.rejects(client.get("k"), WrongNodeError);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("rejects use after close, while close itself stays idempotent", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      client.close();
      assert.equal(client.isClosed(), true);
      await assert.rejects(client.get("k"), AlreadyClosedError);
      await assert.rejects(client.set("k", "v"), AlreadyClosedError);
      await assert.rejects(client.delete("k"), AlreadyClosedError);
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient against a discovery-fronted cluster", () => {
  async function startCluster(): Promise<{
    nodes: Array<{ name: string; mock: MockNode }>;
    discovery: Awaited<ReturnType<typeof startMockDiscovery>>;
    close(): Promise<void>;
  }> {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    // UUID-shaped names, matching what real nodes register with (see
    // doc/adr/0009-*.md) — FNV-1a spreads these across the ring, where
    // near-identical short names like "node-a"/"node-b" would collapse
    // into one narrow band and route every key to a single node.
    const nodes = [
      { name: "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", mock: nodeA },
      { name: "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47", mock: nodeB },
    ];
    const discovery = await startMockDiscovery(nodes.map(({ name, mock }) => ({ name, address: mock.address })));

    return {
      nodes,
      discovery,
      close: async () => {
        await Promise.all([discovery.close(), nodeA.close(), nodeB.close()]);
      },
    };
  }

  it("routes keys across the cluster and reads its own writes", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
      try {
        assert.equal(client.nodeUrls.length, 2);

        const keys = Array.from({ length: 50 }, (_, i) => `key-${i}`);
        await Promise.all(keys.map((key) => client.set(key, `value of ${key}`)));
        for (const key of keys) {
          assert.deepEqual(await client.get(key), Buffer.from(`value of ${key}`));
        }

        // With 50 keys over 2 nodes, both stores must have received some —
        // and between them, all of them.
        const [a, b] = cluster.nodes.map(({ mock }) => mock.store.size);
        assert.equal(a + b, keys.length);
        assert.ok(a > 0 && b > 0, `keys were not spread across nodes (${a}/${b})`);
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("agrees with the shared hash ring about which node owns a key", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
      try {
        const ring = new HashRing(cluster.nodes.map(({ name }) => name));

        for (let i = 0; i < 20; i++) {
          const key = `key-${i}`;
          await client.set(key, "v");
          const owner = cluster.nodes.find(({ name }) => name === ring.route(Buffer.from(key)));
          assert.ok(owner?.mock.store.has(key), `${key} did not land on ${ring.route(Buffer.from(key))}`);
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("retries once through a node-list refresh when a node answers W", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
      try {
        const key = "some-key";
        const ring = new HashRing(cluster.nodes.map(({ name }) => name));
        const owner = cluster.nodes.find(({ name }) => name === ring.route(Buffer.from(key)))!;

        await client.set(key, "v");

        owner.mock.answerWrongNodeOnce();
        assert.deepEqual(await client.get(key), Buffer.from("v"));
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("propagates WrongNodeError when a node still answers W after a refresh", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
      try {
        const key = "some-key";
        const ring = new HashRing(cluster.nodes.map(({ name }) => name));
        const owner = cluster.nodes.find(({ name }) => name === ring.route(Buffer.from(key)))!;

        owner.mock.answerWrongNodeOnce();
        owner.mock.answerWrongNodeOnce();
        await assert.rejects(client.get(key), WrongNodeError);
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("refuses to start against a cluster with no live nodes", async () => {
    const discovery = await startMockDiscovery([]);
    try {
      await assert.rejects(
        NanocachedClient.connect({ host: "127.0.0.1", port: discovery.port }),
        /no live nodes/,
      );
    } finally {
      await discovery.close();
    }
  });
});
