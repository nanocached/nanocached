import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { test } from "node:test";
import { NanocachedClient } from "../src/client.js";
import { NanocachedClusterClient } from "../src/clusterClient.js";
import { fetchNodes } from "../src/discovery.js";
import { HashRing } from "../src/hashRing.js";
import { discoveryBinary, nodeBinary, startTestDiscovery, startTestServer } from "./testServer.js";

const binariesExist = existsSync(nodeBinary) && existsSync(discoveryBinary);

// Like client.test.ts, these exercise the real nanocached-node and
// nanocached-discovery binaries rather than mocks. Skip instead of failing
// hard when they aren't built.
const describeOrSkip = binariesExist ? test : test.skip;

/** Polls the discovery server until exactly `count` nodes are registered,
 * so tests don't race the first heartbeat after starting a node. */
async function waitForNodeCount(
  discovery: { host: string; port: number },
  count: number,
  authSecret?: string,
  timeoutMs = 2000,
): Promise<string[]> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const nodes = await fetchNodes({ ...discovery, authSecret });
    if (nodes.length === count) return nodes;
    if (Date.now() > deadline) {
      throw new Error(`expected ${count} registered nodes, saw ${nodes.length} after ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

describeOrSkip("routes get/set/delete across nodes discovered from nanocached-discovery", async () => {
  const discovery = await startTestDiscovery();
  const discoveryAddr = { host: "127.0.0.1", port: discovery.port };
  const servers = await Promise.all([
    startTestServer({ discovery: discoveryAddr }),
    startTestServer({ discovery: discoveryAddr }),
    startTestServer({ discovery: discoveryAddr }),
  ]);

  try {

    // Each node registers asynchronously via its own heartbeat; wait until
    // discovery has caught up to all of them before connecting.
    await waitForNodeCount(discoveryAddr, servers.length);

    const cluster = await NanocachedClusterClient.connect({
      discoveryHost: discoveryAddr.host,
      discoveryPort: discoveryAddr.port,
    });

    try {
      const keys = Array.from({ length: 30 }, (_, i) => `key-${i}`);

      for (const key of keys) {
        await cluster.set(key, `value-${key}`);
      }
      for (const key of keys) {
        assert.deepEqual(await cluster.get(key), Buffer.from(`value-${key}`));
      }

      // Confirm requests are actually split across the real, dynamically
      // ported nodes — not just always landing on the same one — by
      // checking, for a sample of keys, that the value is present on
      // exactly the node the same HashRing algorithm computes for it.
      const nodeAddresses = await fetchNodes(discoveryAddr);
      const ring = new HashRing(nodeAddresses);
      const usedPorts = new Set<number>();

      for (const key of keys) {
        const expectedNode = ring.route(Buffer.from(key));
        const port = Number(expectedNode.split(":")[1]);
        usedPorts.add(port);

        const direct = await NanocachedClient.connect({ host: "127.0.0.1", port });
        try {
          assert.deepEqual(await direct.get(key), Buffer.from(`value-${key}`));
        } finally {
          direct.close();
        }
      }

      assert.ok(usedPorts.size > 1, "expected keys to spread across more than one node");

      assert.equal(await cluster.delete(keys[0]), true);
      assert.equal(await cluster.get(keys[0]), null);
    } finally {
      cluster.close();
    }
  } finally {
    await Promise.all(servers.map((server) => server.stop()));
    await discovery.stop();
  }
});

describeOrSkip("fails when no nodes are registered with the discovery server", async () => {
  const discovery = await startTestDiscovery();
  try {
    await assert.rejects(
      NanocachedClusterClient.connect({ discoveryHost: "127.0.0.1", discoveryPort: discovery.port }),
      /no live nodes/,
    );
  } finally {
    await discovery.stop();
  }
});

describeOrSkip("authenticates to both the discovery server and every node", async () => {
  const discovery = await startTestDiscovery({ authSecret: "s3cret" });
  const server = await startTestServer({
    authSecret: "s3cret",
    discovery: { host: "127.0.0.1", port: discovery.port },
  });

  try {
    const discoveryAddr = { host: "127.0.0.1", port: discovery.port };
    await waitForNodeCount(discoveryAddr, 1, "s3cret");

    const cluster = await NanocachedClusterClient.connect({
      discoveryHost: discoveryAddr.host,
      discoveryPort: discoveryAddr.port,
      authSecret: "s3cret",
    });
    try {
      await cluster.set("name", "Alice");
      assert.deepEqual(await cluster.get("name"), Buffer.from("Alice"));
    } finally {
      cluster.close();
    }
  } finally {
    await server.stop();
    await discovery.stop();
  }
});
