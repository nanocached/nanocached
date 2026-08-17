import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { AlreadyClosedError, DiscoveryBusyError, NanocachedClient, WrongNodeError } from "../src/index.js";
import { HashRing } from "../src/hashRing.js";
import { startMockDiscovery, startMockNode, unusedPort, type MockNode } from "./mockServers.js";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Polls until `condition` holds — used to wait for the client to notice a
 * server-side FIN without hardcoding a sleep long enough to be flaky. */
async function waitFor(condition: () => boolean, what: string): Promise<void> {
  for (let i = 0; i < 200; i++) {
    if (condition()) return;
    await delay(5);
  }
  throw new Error(`timed out waiting for ${what}`);
}

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

// Reaches into the client's internals to observe when it has processed a
// server-side FIN — there is deliberately no public API for this, and the
// tests must not proceed before the close event has fired.
function singleConnectionClosed(client: NanocachedClient): boolean {
  return (client as any).target.connection.isClosed();
}
function memberConnectionClosed(client: NanocachedClient, name: string): boolean {
  return (client as any).target.members.get(name).connection.isClosed();
}

describe("NanocachedClient discovery seeds", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  it("rejects when neither host/port nor seeds are given", async () => {
    await assert.rejects(NanocachedClient.connect({}), /needs either host\/port or a non-empty seeds list/);
  });

  it("connects through the second seed when the first is unreachable", async () => {
    const node = await startMockNode();
    const discovery = await startMockDiscovery([{ name: names[0], address: node.address }]);
    const deadPort = await unusedPort();
    try {
      const client = await NanocachedClient.connect({
        seeds: [
          { host: "127.0.0.1", port: deadPort },
          { host: "127.0.0.1", port: discovery.port },
        ],
      });
      try {
        assert.equal(client.url, `127.0.0.1:${discovery.port}`);
        await client.set("k", "v");
        assert.deepEqual(await client.get("k"), Buffer.from("v"));
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), node.close()]);
    }
  });

  it("skips a warming-up discovery server and uses the next seed", async () => {
    const node = await startMockNode();
    const [warming, healthy] = await Promise.all([
      startMockDiscovery([{ name: names[0], address: node.address }]),
      startMockDiscovery([{ name: names[0], address: node.address }]),
    ]);
    warming.setWarmingUp(true);
    try {
      const client = await NanocachedClient.connect({
        seeds: [
          { host: "127.0.0.1", port: warming.port },
          { host: "127.0.0.1", port: healthy.port },
        ],
      });
      try {
        assert.equal(client.url, `127.0.0.1:${healthy.port}`);
        await client.set("k", "v");
        assert.deepEqual(await client.get("k"), Buffer.from("v"));
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([warming.close(), healthy.close(), node.close()]);
    }
  });

  it("rejects with DiscoveryBusyError when every seed is warming up", async () => {
    const [first, second] = await Promise.all([startMockDiscovery([]), startMockDiscovery([])]);
    first.setWarmingUp(true);
    second.setWarmingUp(true);
    try {
      await assert.rejects(
        NanocachedClient.connect({
          seeds: [
            { host: "127.0.0.1", port: first.port },
            { host: "127.0.0.1", port: second.port },
          ],
        }),
        DiscoveryBusyError,
      );
    } finally {
      await Promise.all([first.close(), second.close()]);
    }
  });

  it("warns when multiple seeds resolve to a single pinned node", async () => {
    const node = await startMockNode();
    const deadPort = await unusedPort();
    const warn = mock.method(console, "warn", () => {});
    try {
      const client = await NanocachedClient.connect({
        seeds: [
          { host: "127.0.0.1", port: node.port },
          { host: "127.0.0.1", port: deadPort },
        ],
      });
      client.close();

      const messages = warn.mock.calls.map((call) => String(call.arguments[0]));
      assert.ok(
        messages.some((message) => message.includes("pinned to that single server")),
        `expected a pinned-node warning, got: ${JSON.stringify(messages)}`,
      );
    } finally {
      warn.mock.restore();
      await node.close();
    }
  });

  it("does not warn when a single host/port intentionally targets a node", async () => {
    const node = await startMockNode();
    const warn = mock.method(console, "warn", () => {});
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      client.close();
      assert.equal(warn.mock.calls.length, 0, JSON.stringify(warn.mock.calls.map((c) => c.arguments)));
    } finally {
      warn.mock.restore();
      await node.close();
    }
  });

  it("refreshes the node list through the next seed when the first stops answering", async () => {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const nodes = [
      { name: names[0], address: nodeA.address },
      { name: names[1], address: nodeB.address },
    ];
    const [primary, standby] = await Promise.all([startMockDiscovery(nodes), startMockDiscovery(nodes)]);
    try {
      const client = await NanocachedClient.connect({
        seeds: [
          { host: "127.0.0.1", port: primary.port },
          { host: "127.0.0.1", port: standby.port },
        ],
      });
      try {
        const key = "some-key";
        await client.set(key, "v");

        // The primary discovery restarts into its grace period; the next
        // forced refresh (triggered by a W answer) must fall through to
        // the standby and still let the retry succeed.
        primary.setWarmingUp(true);
        const ring = new HashRing(names);
        const owner = ring.route(Buffer.from(key)) === names[0] ? nodeA : nodeB;
        owner.answerWrongNodeOnce();

        assert.deepEqual(await client.get(key), Buffer.from("v"));
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([primary.close(), standby.close(), nodeA.close(), nodeB.close()]);
    }
  });
});

describe("NanocachedClient reconnect-on-use", () => {
  it("transparently reconnects after the server closes an idle connection", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await client.set("k", "v");

        // Simulate the server's 30s idle timeout: a clean server-side FIN.
        node.dropConnections();
        await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");

        assert.deepEqual(await client.get("k"), Buffer.from("v"));
        assert.equal(node.connectionCount(), 2);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("shares one reconnect between concurrent requests", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await client.set("k", "v");
        node.dropConnections();
        await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");

        const values = await Promise.all(Array.from({ length: 10 }, () => client.get("k")));
        for (const value of values) assert.deepEqual(value, Buffer.from("v"));
        assert.equal(node.connectionCount(), 2, "concurrent requests dialed more than one reconnect");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a mismatched response kind poisons the connection and the next request redials", async () => {
    // A well-formed response of the wrong kind (`S` answering a G) means
    // the request/response streams are off by one; reusing the connection
    // would answer every later request with the previous one's response.
    // The connection is poisoned; the next request transparently redials.
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await client.set("k", "v");
        node.answerStoredToGetOnce();
        await assert.rejects(client.get("k"), /does not match the request/);

        assert.deepEqual(await client.get("k"), Buffer.from("v"));
        assert.equal(node.connectionCount(), 2);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("connecting to a silent server fails within the deadline", async () => {
    // A server that accepts the TCP connection but never answers the
    // handshake (a blackholed address behaves the same way) must fail
    // the connect within the deadline instead of hanging.
    const { createServer } = await import("node:net");
    const accepted = new Set<import("node:net").Socket>();
    const silent = createServer((socket) => {
      // Keep the socket paused (never read, never answer) — but track it,
      // since an unread FIN never surfaces and server.close() would wait
      // on it forever.
      accepted.add(socket);
      socket.on("error", () => {});
    });
    const port = await new Promise<number>((resolve) => {
      silent.listen(0, "127.0.0.1", () => {
        resolve((silent.address() as { port: number }).port);
      });
    });
    try {
      const { connectAndIdentify } = await import("../src/identify.js");
      await assert.rejects(
        connectAndIdentify({ host: "127.0.0.1", port, connectDeadlineMs: 100 }),
        /no response from server within/,
      );
    } finally {
      for (const socket of accepted) socket.destroy();
      await new Promise<void>((resolve) => silent.close(() => resolve()));
    }
  });

  it("a malformed value length poisons the connection and the next request redials", async () => {
    // Regression for issue #8: a garbage `V <len>` header desyncs the
    // stream; the connection must be poisoned (never reused mid-frame)
    // so the next request transparently redials.
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        await client.set("k", "v");
        node.answerMalformedValueOnce();
        await assert.rejects(client.get("k"), /invalid value length/);

        // The poisoned connection is replaced lazily; no stray bytes leak
        // into this response.
        assert.deepEqual(await client.get("k"), Buffer.from("v"));
        assert.equal(node.connectionCount(), 2);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a refresh finishing after close() installs no new connections", async () => {
    // Regression for issue #10: close() must win against an in-flight
    // node-list refresh — a freshly dialed socket installed afterwards
    // would leak with nothing left to close it.
    const node = await startMockNode();
    const discovery = await startMockDiscovery([
      { name: "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", address: node.address },
    ]);
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: discovery.port });
      const before = node.connectionCount();
      client.close();
      await (client as any).refreshNodeList();
      assert.equal(node.connectionCount(), before, "refresh after close dialed a node");
    } finally {
      await Promise.all([discovery.close(), node.close()]);
    }
  });

  it("propagates the dial error when the node is gone for good", async () => {
    const node = await startMockNode();
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
    try {
      await node.close();
      await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");
      await assert.rejects(client.get("k"), /ECONNREFUSED/);
    } finally {
      client.close();
    }
  });

  it("reconnects to the routed cluster member only", async () => {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];
    const discovery = await startMockDiscovery([
      { name: names[0], address: nodeA.address },
      { name: names[1], address: nodeB.address },
    ]);
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: discovery.port });
      try {
        const key = "some-key";
        await client.set(key, "v");

        const ring = new HashRing(names);
        const ownerName = ring.route(Buffer.from(key));
        const owner = ownerName === names[0] ? nodeA : nodeB;
        const other = owner === nodeA ? nodeB : nodeA;

        owner.dropConnections();
        await waitFor(() => memberConnectionClosed(client, ownerName), "the client to see the FIN");

        assert.deepEqual(await client.get(key), Buffer.from("v"));
        assert.equal(owner.connectionCount(), 2);
        assert.equal(other.connectionCount(), 1, "reconnected a member whose connection never died");
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), nodeA.close(), nodeB.close()]);
    }
  });
});

describe("NanocachedClient keep-alive", () => {
  it("rejects a non-positive or non-integer interval synchronously", async () => {
    for (const keepAliveIntervalMs of [0, -5, 3.5, NaN, Infinity]) {
      await assert.rejects(
        NanocachedClient.connect({ host: "127.0.0.1", port: 1, keepAliveIntervalMs }),
        RangeError,
      );
    }
  });

  it("pings an idle connection often enough to reset the server's idle timer", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        host: "127.0.0.1",
        port: node.port,
        keepAliveIntervalMs: 40,
      });
      try {
        await waitFor(() => node.getCount() >= 2, "keep-alive pings to arrive");
        // The pings rode the original connection — no reconnects happened.
        assert.equal(node.connectionCount(), 1);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("stops pinging once the client is closed", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        host: "127.0.0.1",
        port: node.port,
        keepAliveIntervalMs: 20,
      });
      await waitFor(() => node.getCount() >= 1, "a keep-alive ping to arrive");
      client.close();

      const pingsAtClose = node.getCount();
      await delay(100);
      assert.equal(node.getCount(), pingsAtClose);
    } finally {
      await node.close();
    }
  });

  it("does not ping a connection that real traffic keeps busy", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        host: "127.0.0.1",
        port: node.port,
        keepAliveIntervalMs: 60,
      });
      try {
        for (let i = 0; i < 10; i++) {
          await client.set("k", "v");
          await delay(15);
        }
        // Every request above reset the idle clock well inside the 60ms
        // interval, so no ping should ever have fired: no `G` at all.
        assert.equal(node.getCount(), 0);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient replication (ADR-0011, R=2)", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  async function startReplicatedCluster() {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const nodes = [
      { name: names[0], mock: nodeA },
      { name: names[1], mock: nodeB },
    ];
    const discovery = await startMockDiscovery(
      nodes.map(({ name, mock }) => ({ name, address: mock.address })),
      { replication: 2 },
    );

    return {
      nodes,
      discovery,
      ownerOf(key: string) {
        const ring = new HashRing(names);
        const [primary, replica] = ring.owners(Buffer.from(key), 2);
        return {
          primary: nodes.find(({ name }) => name === primary)!,
          replica: nodes.find(({ name }) => name === replica)!,
        };
      },
      close: async () => {
        await Promise.all([discovery.close(), nodeA.close(), nodeB.close()]);
      },
    };
  }

  it("learns R from discovery and fans writes out to every owner", async () => {
    const cluster = await startReplicatedCluster();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
      try {
        assert.equal(client.replication, 2);

        const keys = Array.from({ length: 20 }, (_, i) => `key-${i}`);
        await Promise.all(keys.map((key) => client.set(key, `value of ${key}`)));

        // With R=2 over 2 nodes, every key must be on BOTH nodes.
        for (const key of keys) {
          for (const { name, mock } of cluster.nodes) {
            assert.ok(mock.store.has(key), `${key} is missing from ${name}`);
          }
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("serves reads from the replica when the primary node dies", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
    try {
      const key = "survives-a-node-death";
      await client.set(key, "still here");

      const { primary } = cluster.ownerOf(key);
      // Kill the primary outright — server gone, not just the connection.
      await primary.mock.close();
      await waitFor(() => memberConnectionClosed(client, primary.name), "the client to see the FIN");

      assert.deepEqual(await client.get(key), Buffer.from("still here"));
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("does not fail a write when a replica is down", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
    try {
      const key = "written-despite-dead-replica";
      const { primary, replica } = cluster.ownerOf(key);

      await replica.mock.close();
      await waitFor(() => memberConnectionClosed(client, replica.name), "the client to see the FIN");

      await client.set(key, "v");
      assert.ok(primary.mock.store.has(key));
      assert.deepEqual(await client.get(key), Buffer.from("v"));
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("routes writes around a dead primary once discovery drops it", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
    try {
      const key = "written-after-primary-death";
      const { primary, replica } = cluster.ownerOf(key);

      // The primary dies AND discovery has already noticed: the first
      // write attempt fails on the dead primary, forcing a refresh that
      // re-ranks onto the survivor, and the retry succeeds.
      await primary.mock.close();
      cluster.discovery.setNodes([{ name: replica.name, address: replica.mock.address }]);
      await waitFor(() => memberConnectionClosed(client, primary.name), "the client to see the FIN");

      await client.set(key, "v");
      assert.deepEqual(await client.get(key), Buffer.from("v"));
      assert.ok(replica.mock.store.has(key));
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("fans deletes out to every owner", async () => {
    const cluster = await startReplicatedCluster();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: cluster.discovery.port });
      try {
        const key = "deleted-everywhere";
        await client.set(key, "v");
        for (const { mock } of cluster.nodes) assert.ok(mock.store.has(key));

        assert.equal(await client.delete(key), true);
        for (const { name, mock } of cluster.nodes) {
          assert.ok(!mock.store.has(key), `${key} still present on ${name}`);
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("reports replication 1 against a single node", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ host: "127.0.0.1", port: node.port });
      try {
        assert.equal(client.replication, 1);
      } finally {
        client.close();
      }
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
