import { afterEach, describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import {
  AlreadyClosedError,
  AuthenticationError,
  ConnectionLostError,
  contentDigest,
  DecompressionError,
  DiscoveryBusyError,
  NanocachedClient,
  NanocachedError,
  NotNumericError,
  PartialWrongNodeError,
  RetryableError,
  WrongNodeError,
} from "../src/index.js";
import { HashRing } from "../src/hashRing.js";
import { FIRE_AND_FORGET_TUNING, KEEPALIVE_TUNING, MAX_BATCH_KEYS } from "../src/client.js";
import { REQUEST_TIMEOUT_TUNING } from "../src/connection.js";
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("greeting", "hello");
        assert.equal(await client.get("greeting"), "hello");

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

  it("handles binary keys/values and empty values via getBytes", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const key = Uint8Array.from([1, 2, 3]);
        const value = Uint8Array.from([0, 255, 10]);
        await client.set(key, value);
        assert.deepEqual(await client.getBytes(key), Buffer.from(value));

        await client.set("empty", "");
        assert.deepEqual(await client.getBytes("empty"), Buffer.alloc(0));
        assert.equal(await client.get("empty"), "");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("get() strictly decodes UTF-8, rejecting a value that isn't valid UTF-8", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        // A lone continuation byte is never valid UTF-8 on its own.
        const invalid = Uint8Array.from([0xff]);
        await client.set("garbage", invalid);

        assert.deepEqual(await client.getBytes("garbage"), Buffer.from(invalid));
        await assert.rejects(client.get("garbage"), TypeError);
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v", 60);
        assert.equal(await client.get("k"), "v");

        await assert.rejects(client.set("k", "v", -1), RangeError);
        // The rejected set must not have poisoned the shared connection.
        assert.equal(await client.get("k"), "v");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("rejects an empty key before writing, without poisoning the shared connection", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await assert.rejects(client.get(""), RangeError);
        await assert.rejects(client.set("", "v"), RangeError);
        await assert.rejects(client.delete(""), RangeError);

        // None of the rejected calls above wrote anything to the shared,
        // pipelined connection — a concurrent valid call on it must still
        // succeed normally, and no reconnect should have happened.
        await client.set("k", "v");
        const results = await Promise.all([
          client.get("k"),
          client.set("", "poison-attempt").catch((error) => error),
          client.get("k"),
        ]);
        assert.equal(results[0], "v");
        assert.ok(results[1] instanceof RangeError);
        assert.equal(results[2], "v");
        assert.equal(node.connectionCount(), 1, "an empty-key rejection triggered a reconnect");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("ttlSeconds 0 (the default) means no expiry", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("no-ttl-explicit", "v", 0);
        await client.set("no-ttl-default", "v");
        assert.equal(await client.get("no-ttl-explicit"), "v");
        assert.equal(await client.get("no-ttl-default"), "v");
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await Promise.all(Array.from({ length: 20 }, (_, i) => client.set(`key-${i}`, `value-${i}`)));
        const values = await Promise.all(Array.from({ length: 20 }, (_, i) => client.get(`key-${i}`)));
        values.forEach((value, i) => assert.equal(value, `value-${i}`));
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
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        authSecret: "s3cret",
      });
      try {
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
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
      // Both shapes are matchable as AuthenticationError (issue #47
      // item 5), not just by message.
      await assert.rejects(
        NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] }),
        (error: unknown) =>
          error instanceof AuthenticationError && /requires authentication/.test(error.message),
      );
      await assert.rejects(
        NanocachedClient.connect({
          addresses: [{ host: "127.0.0.1", port: node.port }],
          authSecret: "wrong",
        }),
        (error: unknown) =>
          error instanceof AuthenticationError && /authentication failed/.test(error.message),
      );
    } finally {
      await node.close();
    }
  });

  it("propagates WrongNodeError in single mode (no discovery to refresh from)", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      client.close();
      assert.equal(client.isClosed(), true);
      await assert.rejects(client.get("k"), AlreadyClosedError);
      await assert.rejects(client.set("k", "v"), AlreadyClosedError);
      await assert.rejects(client.delete("k"), AlreadyClosedError);
    } finally {
      await node.close();
    }
  });

  it("warns exactly once on a second close(), while staying idempotent", async () => {
    const node = await startMockNode();
    const warn = mock.method(console, "warn", () => {});
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      client.close();
      client.close();
      client.close();

      const messages = warn.mock.calls.map((call) => String(call.arguments[0]));
      const closeWarnings = messages.filter((message) =>
        message.includes("close() called again on an already-closed client"),
      );
      assert.equal(closeWarnings.length, 2, JSON.stringify(messages));
      assert.equal(client.isClosed(), true);
    } finally {
      warn.mock.restore();
      await node.close();
    }
  });

  it("warns when connect() is called again for an address with an open connection", async () => {
    const node = await startMockNode();
    const warn = mock.method(console, "warn", () => {});
    try {
      const first = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const second = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
        try {
          const messages = warn.mock.calls.map((call) => String(call.arguments[0]));
          assert.ok(
            messages.some((message) => message.includes("was close() forgotten?")),
            `expected a forgotten-close warning, got: ${JSON.stringify(messages)}`,
          );
        } finally {
          second.close();
        }
      } finally {
        first.close();
      }
    } finally {
      warn.mock.restore();
      await node.close();
    }
  });
});

describe("NanocachedClient value compression (value compression)", () => {
  it("does not touch the wire format when compress is off (the default)", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const value = "x".repeat(1000);
        await client.set("k", value);
        assert.deepEqual(node.store.get("k"), Buffer.from(value, "utf8"));
        assert.equal(await client.get("k"), value);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("compresses a value at or above compressionThreshold and decompresses it back", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
        compressionThreshold: 64,
      });
      try {
        const value = "x".repeat(1000);
        await client.set("k", value);

        const stored = node.store.get("k")!;
        assert.equal(stored[0], 0x01, "expected the DEFLATE marker byte on the wire");
        assert.ok(stored.length < value.length, "a highly repetitive value must actually shrink on the wire");

        assert.equal(await client.get("k"), value);
        assert.deepEqual(await client.getBytes("k"), Buffer.from(value, "utf8"));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("getWithToken's digest is computed from the raw (marker-prefixed) wire bytes, not the decompressed value (issue #141)", async () => {
    // The critical CAS/compression correctness point: the server never
    // decompresses, so the digest it would compute for a `k`/`x`
    // `<cond>` is over the marker-prefixed wire bytes — computing it from
    // the decompressed value instead would silently produce a digest that
    // never matches the server's, breaking every CAS call under
    // compression.
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
        compressionThreshold: 64,
      });
      try {
        const value = "x".repeat(1000);
        await client.set("k", value);

        const stored = node.store.get("k")!;
        assert.equal(stored[0], 0x01, "expected the DEFLATE marker byte on the wire");

        const result = await client.getWithToken("k");
        assert.ok(result !== null);
        // The returned value is the ordinary decompressed one...
        assert.deepEqual(result!.value, Buffer.from(value, "utf8"));
        // ...but the token must hash the raw wire bytes (marker included),
        // exactly what the mock server's own `k`/`x` digest evaluation
        // hashes — never the decompressed value.
        assert.equal(result!.token, contentDigest(stored));
        assert.notEqual(result!.token, contentDigest(Buffer.from(value, "utf8")), "must not hash the decompressed value");

        // And a replace() using that token must actually succeed against
        // the (compression-aware) mock server, proving the two agree.
        assert.equal(await client.replace("k", result!.token, "replacement"), true);
        assert.equal(await client.get("k"), "replacement");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a CAS write goes through the same compression pipeline as set (issue #141)", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
        compressionThreshold: 64,
      });
      try {
        const value = "y".repeat(1000);
        assert.equal(await client.putIfAbsent("k", value), true);

        const stored = node.store.get("k")!;
        assert.equal(stored[0], 0x01, "a new CAS-written value must be compressed exactly like set's own");
        assert.ok(stored.length < Buffer.from(value, "utf8").length);

        // A plain get (no CAS involved) must be able to decompress it —
        // proving the wire bytes are a well-formed compressed value, not
        // raw uncompressed bytes with a stray marker.
        assert.equal(await client.get("k"), value);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("rejects an oversized value before compression, even if it would compress under the cap", async () => {
    // Regression (issue #47 audit item 3): the request-size cap must be
    // checked against the *original* value, matching Python's set() —
    // not the compressed frame, which a highly repetitive value can
    // shrink well under MAX_REQUEST_BYTES even though the uncompressed
    // value the caller asked to store never could have fit the server's
    // own request cap.
    const { MAX_REQUEST_BYTES } = await import("../src/protocol.js");
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
        compressionThreshold: 16,
      });
      try {
        const oversized = Buffer.alloc(MAX_REQUEST_BYTES + 1000, "a"); // DEFLATE-friendly
        await assert.rejects(client.set("k", oversized), RangeError);
        // Rejected before any I/O — the key was never written.
        assert.equal(node.store.has("k"), false);

        // Still usable afterward — none of the above touched the wire.
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("leaves a value below compressionThreshold unmarked-but-prefixed on the wire", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
        compressionThreshold: 256,
      });
      try {
        await client.set("k", "short");
        const stored = node.store.get("k")!;
        assert.deepEqual(stored, Buffer.concat([Buffer.from([0x00]), Buffer.from("short", "utf8")]));
        assert.equal(await client.get("k"), "short");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("passes incompressible data through unmarked-but-prefixed rather than bloating it", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
        compressionThreshold: 16,
      });
      try {
        const value = randomBytes(512);
        await client.set("k", value);
        const stored = node.store.get("k")!;
        assert.deepEqual(stored, Buffer.concat([Buffer.from([0x00]), value]));
        assert.deepEqual(await client.getBytes("k"), value);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("rejects a value written by a compress-disabled client with a clear error, not silent corruption", async () => {
    const node = await startMockNode();
    try {
      const writer = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        // A legacy/uncompressed writer's value whose first byte happens to
        // collide with the DEFLATE marker (0x01) — value compression's
        // documented hazard of enabling compress against a keyspace other
        // clients still touch without it.
        await writer.set("k", Uint8Array.from([0x01, 2, 3, 4]));
      } finally {
        writer.close();
      }

      const reader = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
        compress: true,
      });
      try {
        await assert.rejects(() => reader.getBytes("k"), /decompress|marker/);
      } finally {
        reader.close();
      }
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

describe("NanocachedClient discovery addresses", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  it("rejects when addresses is empty", async () => {
    await assert.rejects(NanocachedClient.connect({ addresses: [] }), /needs a non-empty addresses list/);
  });

  it("connects through the second address when the first is unreachable", async () => {
    const node = await startMockNode();
    const discovery = await startMockDiscovery([{ name: names[0], address: node.address }]);
    const deadPort = await unusedPort();
    try {
      const client = await NanocachedClient.connect({
        addresses: [
          { host: "127.0.0.1", port: deadPort },
          { host: "127.0.0.1", port: discovery.port },
        ],
      });
      try {
        assert.equal(client.url, `127.0.0.1:${discovery.port}`);
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), node.close()]);
    }
  });

  it("skips a warming-up discovery server and uses the next address", async () => {
    const node = await startMockNode();
    const [warming, healthy] = await Promise.all([
      startMockDiscovery([{ name: names[0], address: node.address }]),
      startMockDiscovery([{ name: names[0], address: node.address }]),
    ]);
    warming.setWarmingUp(true);
    try {
      const client = await NanocachedClient.connect({
        addresses: [
          { host: "127.0.0.1", port: warming.port },
          { host: "127.0.0.1", port: healthy.port },
        ],
      });
      try {
        assert.equal(client.url, `127.0.0.1:${healthy.port}`);
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([warming.close(), healthy.close(), node.close()]);
    }
  });

  it("rejects with DiscoveryBusyError when every address is warming up", async () => {
    const [first, second] = await Promise.all([startMockDiscovery([]), startMockDiscovery([])]);
    first.setWarmingUp(true);
    second.setWarmingUp(true);
    try {
      await assert.rejects(
        NanocachedClient.connect({
          addresses: [
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

  it("warns when multiple addresses resolve to a single pinned node", async () => {
    const node = await startMockNode();
    const deadPort = await unusedPort();
    const warn = mock.method(console, "warn", () => {});
    try {
      const client = await NanocachedClient.connect({
        addresses: [
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

  it("does not warn when a single address intentionally targets a node", async () => {
    const node = await startMockNode();
    const warn = mock.method(console, "warn", () => {});
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      client.close();
      assert.equal(warn.mock.calls.length, 0, JSON.stringify(warn.mock.calls.map((c) => c.arguments)));
    } finally {
      warn.mock.restore();
      await node.close();
    }
  });

  it("refreshes the node list through the next address when the first stops answering", async () => {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const nodes = [
      { name: names[0], address: nodeA.address },
      { name: names[1], address: nodeB.address },
    ];
    const [primary, standby] = await Promise.all([startMockDiscovery(nodes), startMockDiscovery(nodes)]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [
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

        assert.equal(await client.get(key), "v");
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");

        // Simulate the server's 30s idle timeout: a clean server-side FIN.
        node.dropConnections();
        await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");

        assert.equal(await client.get("k"), "v");
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        node.dropConnections();
        await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");

        const values = await Promise.all(Array.from({ length: 10 }, () => client.get("k")));
        for (const value of values) assert.equal(value, "v");
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        node.answerStoredToGetOnce();
        await assert.rejects(client.get("k"), /does not match the request/);

        assert.equal(await client.get("k"), "v");
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

  it("shares one deadline budget across the dial and the handshake read, instead of a fresh one for each", async () => {
    // Regression (issue #47 audit item 3): the dial and the ack read used
    // to each get their own independent deadlineMs-long timer, so a dial
    // that ate most of the budget still left the read phase a full fresh
    // budget — up to ~2x the configured deadline overall. Simulate a slow
    // dial by stubbing Date.now to report a large jump right after
    // connectSocket resolves (the dial itself is still near-instant on
    // loopback; only the clock the deadline math reads from is fooled),
    // then assert the ack-read phase only gets what's left of the budget.
    const { createServer } = await import("node:net");
    const accepted = new Set<import("node:net").Socket>();
    const silent = createServer((socket) => {
      accepted.add(socket);
      socket.on("error", () => {});
    });
    const port = await new Promise<number>((resolve) => {
      silent.listen(0, "127.0.0.1", () => {
        resolve((silent.address() as { port: number }).port);
      });
    });
    const realNow = Date.now;
    let calls = 0;
    const nowMock = mock.method(Date, "now", () => {
      calls++;
      // Call #1 is identifyOnce's `startedAt`, taken before the dial;
      // every call after that (inside remainingDeadline, once the dial
      // has resolved) reports as if 450 of the 500ms budget were already
      // spent on it, leaving only ~50ms for the read phase.
      return calls === 1 ? realNow() : realNow() + 450;
    });
    try {
      const { connectAndIdentify } = await import("../src/identify.js");
      const started = process.hrtime.bigint();
      await assert.rejects(
        connectAndIdentify({ host: "127.0.0.1", port, connectDeadlineMs: 500 }),
        /no response from server within/,
      );
      const elapsedMs = Number(process.hrtime.bigint() - started) / 1_000_000;
      // A fresh, undoubled budget would wait close to another 500ms here;
      // a shared budget only has the ~50ms simulated remainder left.
      assert.ok(elapsedMs < 250, `expected the read phase to use only the remaining budget, took ${elapsedMs}ms`);
    } finally {
      nowMock.mock.restore();
      for (const socket of accepted) socket.destroy();
      await new Promise<void>((resolve) => silent.close(() => resolve()));
    }
  });

  it("remainingDeadline subtracts elapsed time from the shared budget, clamped at zero", async () => {
    const { remainingDeadline } = await import("../src/identify.js");
    const now = Date.now();
    assert.equal(remainingDeadline(500, now), 500);
    assert.ok(remainingDeadline(500, now - 450) <= 50);
    assert.ok(remainingDeadline(500, now - 450) > 0);
    assert.equal(remainingDeadline(500, now - 999_999), 0);
  });

  it("an `N` header with no newline fails promptly instead of buffering forever", async () => {
    // Regression for the unbounded-buffer-growth issue (issue #12
    // follow-up) on the discovery path: a malicious/corrupted discovery
    // server can send `N` and then withhold the header's terminating LF
    // forever. connectAndIdentify must detect this and fail long before
    // it has buffered anywhere near the server's willingness to stream.
    const discovery = await startMockDiscovery([]);
    try {
      discovery.answerUnterminatedListOnce();
      const { connectAndIdentify } = await import("../src/identify.js");
      await assert.rejects(
        connectAndIdentify({ host: "127.0.0.1", port: discovery.port }),
        /invalid node-list header|missing header terminator/,
      );
      assert.ok(
        discovery.unterminatedListBytesSent() < 64 * 1024,
        `client kept the connection open through ${discovery.unterminatedListBytesSent()} bytes without a newline`,
      );
    } finally {
      await discovery.close();
    }
  });

  it("an oversized node-list response fails instead of buffering unbounded memory", async () => {
    // Regression: bounds a malicious discovery server's memory pressure
    // with an aggregate cap on the whole `N ...` response (16 MiB),
    // independent of the per-field caps — this same constant is being
    // added to all six SDKs.
    const { createServer } = await import("node:net");
    const server = createServer((socket) => {
      socket.on("error", () => {});
      socket.on("data", (chunk: Buffer) => {
        const text = chunk.toString("ascii");
        if (text.startsWith("A ")) {
          socket.write("Od\n");
          return;
        }
        if (text.startsWith("L")) {
          // A well-formed, oversized `N` response: many entries each near
          // the per-field cap, adding up to comfortably past the 16 MiB
          // aggregate cap. Declared count is a lie relative to what's
          // actually sent — the client must bail out from the size alone,
          // well before needing all of it.
          const fieldLength = 64 * 1024; // MAX_NODE_FIELD_LENGTH
          const entryCount = 300; // ~300 * ~128 KiB > 16 MiB
          socket.write(`N ${entryCount} 1\n`);
          const name = Buffer.alloc(fieldLength, 0x61 /* 'a' */);
          const addr = Buffer.alloc(fieldLength, 0x62 /* 'b' */);
          const entry = Buffer.concat([Buffer.from(`${fieldLength} ${fieldLength}\n`), name, addr, Buffer.from("\n")]);
          for (let i = 0; i < entryCount; i++) {
            if (socket.destroyed) break;
            socket.write(entry);
          }
        }
      });
    });
    const port = await new Promise<number>((resolve) => {
      server.listen(0, "127.0.0.1", () => resolve((server.address() as { port: number }).port));
    });
    try {
      const { connectAndIdentify } = await import("../src/identify.js");
      await assert.rejects(connectAndIdentify({ host: "127.0.0.1", port }), /exceeds maximum size/);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("a malformed value length poisons the connection and the next request redials", async () => {
    // Regression for issue #8: a garbage `V <len>` header desyncs the
    // stream; the connection must be poisoned (never reused mid-frame)
    // so the next request transparently redials.
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        node.answerMalformedValueOnce();
        await assert.rejects(client.get("k"), /invalid value length/);

        // The poisoned connection is replaced lazily; no stray bytes leak
        // into this response.
        assert.equal(await client.get("k"), "v");
        assert.equal(node.connectionCount(), 2);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a `V` header with no newline poisons the connection promptly instead of buffering forever", async () => {
    // Regression for the unbounded-buffer-growth issue (issue #12
    // follow-up): a malicious/corrupted server can send `V` and then
    // withhold the header's terminating LF forever. The client must
    // detect this and poison the connection long before it has buffered
    // anywhere near the server's willingness to keep streaming.
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        node.answerUnterminatedValueOnce();
        await assert.rejects(client.get("k"), /invalid value length|missing header terminator/);

        // Detected quickly: nowhere near the mock's 512 KiB safety cap.
        assert.ok(
          node.unterminatedValueBytesSent() < 64 * 1024,
          `client kept the connection open through ${node.unterminatedValueBytesSent()} bytes without a newline`,
        );

        // The poisoned connection is replaced lazily; the next request
        // transparently redials.
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }] });
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
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
    try {
      await node.close();
      await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");
      await assert.rejects(client.get("k"), /ECONNREFUSED/);
    } finally {
      client.close();
    }
  });

  it("cools down a failed reconnect address instead of redialing it on every call", async () => {
    const node = await startMockNode();
    const port = node.port;
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port }],
      // Timing: a wide cooldown window and fast-rejection bound keep this from flaking on loaded CI runners.
      reconnectCooldownMs: 1000,
    });
    try {
      await client.set("k", "v");
      await node.close();
      await waitFor(() => singleConnectionClosed(client), "the client to see the FIN");

      // Nothing listens on `port` anymore, so this redial fails fast with
      // ECONNREFUSED and starts the cooldown window for that address.
      await assert.rejects(client.get("k"), /ECONNREFUSED/);

      // A listener now sits on the same port and answers immediately with
      // bytes the identify handshake rejects outright — deliberately not
      // the ECONNRESET/close-before-reply shape that triggers connectAndIdentify's
      // legacy-server fallback redial (identify.ts), so each dial against
      // it fails after exactly one connection, letting `connections` below
      // tell "cooldown skipped the dial" apart from "cooldown let it
      // through" unambiguously.
      const { createServer } = await import("node:net");
      let connections = 0;
      const garbageSockets = new Set<import("node:net").Socket>();
      const garbage = createServer((socket) => {
        connections++;
        garbageSockets.add(socket);
        socket.on("error", () => {});
        socket.on("close", () => garbageSockets.delete(socket));
        socket.write("XXX");
      });
      await new Promise<void>((resolve, reject) => {
        garbage.once("error", reject);
        garbage.listen(port, "127.0.0.1", () => resolve());
      });
      try {
        // Still within the cooldown window: rejects with the cached
        // failure near-instantly, without dialing the listener at all.
        const start = Date.now();
        await assert.rejects(client.get("k"), /ECONNREFUSED/);
        const elapsed = Date.now() - start;
        assert.ok(elapsed < 500, `expected a cooldown-fast rejection, took ${elapsed}ms`);
        assert.equal(connections, 0, "the cooldown did not prevent a redial");

        // Once the cooldown window has passed, the address is dialed
        // again, this time reaching the listener.
        await delay(1200);
        await assert.rejects(client.get("k"), /unexpected response to A/);
        assert.equal(connections, 1, "the address was never redialed after the cooldown elapsed");
      } finally {
        // The client's own socket.destroy() (identify.ts) closes its end
        // on a parse error, but doesn't guarantee the server side has
        // finished tearing down by the time this runs — close every
        // accepted socket explicitly so garbage.close()'s callback isn't
        // left waiting on one that's merely mid-teardown.
        for (const socket of garbageSockets) socket.destroy();
        await new Promise<void>((resolve) => garbage.close(() => resolve()));
      }
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }] });
      try {
        const key = "some-key";
        await client.set(key, "v");

        const ring = new HashRing(names);
        const ownerName = ring.route(Buffer.from(key));
        const owner = ownerName === names[0] ? nodeA : nodeB;
        const other = owner === nodeA ? nodeB : nodeA;

        owner.dropConnections();
        await waitFor(() => memberConnectionClosed(client, ownerName), "the client to see the FIN");

        assert.equal(await client.get(key), "v");
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
  // Keep-alive is always on with an internal interval (issue #27);
  // KEEPALIVE_TUNING exists only so these tests can shorten it.
  const defaultIntervalMs = KEEPALIVE_TUNING.intervalMs;
  afterEach(() => {
    KEEPALIVE_TUNING.intervalMs = defaultIntervalMs;
  });

  it("pings an idle connection often enough to reset the server's idle timer", async () => {
    const node = await startMockNode();
    try {
      KEEPALIVE_TUNING.intervalMs = 40;
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
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
      KEEPALIVE_TUNING.intervalMs = 20;
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
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
      KEEPALIVE_TUNING.intervalMs = 60;
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
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

describe("NanocachedError base class (issue #44)", () => {
  it("parents every SDK error class onto one catchable family", () => {
    // Callers can catch "an expected nanocached failure" with a single
    // `instanceof NanocachedError`, like the other five SDKs' base
    // type/enum/sentinels allow.
    assert.ok(new AlreadyClosedError() instanceof NanocachedError);
    assert.ok(new WrongNodeError() instanceof NanocachedError);
    assert.ok(new ConnectionLostError("x") instanceof NanocachedError);
    assert.ok(new DecompressionError("x") instanceof NanocachedError);
    assert.ok(new DiscoveryBusyError() instanceof NanocachedError);
    // Still Errors too — existing instanceof checks keep passing.
    assert.ok(new WrongNodeError() instanceof Error);
  });

  it("classifies an auth failure as a NanocachedError", async () => {
    // Auth failure used to be a plain Error — outside any catchable
    // family.
    const node = await startMockNode({ requiredSecret: "s3cret" });
    try {
      await assert.rejects(
        NanocachedClient.connect({
          addresses: [{ host: "127.0.0.1", port: node.port }],
          authSecret: "wrong",
        }),
        (error: unknown) =>
          error instanceof NanocachedError && /authentication failed/.test((error as Error).message),
      );
    } finally {
      await node.close();
    }
  });
});

describe("unsolicited busy response (issue #45)", () => {
  it("poisons the connection the moment the frame arrives", async () => {
    // Regression: an unsolicited `B` (connection-limit busy) only
    // recorded lastError and kept parsing — the connection died only
    // when the server's follow-up FIN landed, and until then the client
    // could keep writing requests into it. It must be poisoned
    // immediately, like the other five SDKs.
    const { createServer, connect } = await import("node:net");
    const { Connection } = await import("../src/connection.js");

    const serverSockets: import("node:net").Socket[] = [];
    const server = createServer((socket) => {
      serverSockets.push(socket);
      // Unsolicited busy with nothing pending — and deliberately NO
      // server-side close afterwards: the client must not need the FIN.
      socket.write("B\n");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as import("node:net").AddressInfo).port;

    try {
      const socket = connect(port, "127.0.0.1");
      await new Promise<void>((resolve) => socket.once("connect", resolve));
      const connection = new Connection(socket);

      await waitFor(() => connection.isClosed(), "the busy frame to poison the connection");
      assert.ok(socket.destroyed, "the client must destroy the socket itself");
      await assert.rejects(connection.get("k"), /connection limit reached/);
    } finally {
      for (const socket of serverSockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

describe("a failed write poisons the connection (audit finding)", () => {
  it("marks the connection closed and rejects other in-flight requests promptly, not after the 30s timeout", async () => {
    // Regression: Connection.send()'s write callback only spliced the
    // failing waiter out of `pending` and rejected it — unlike
    // mismatch(), the request-timer handler, and onData's tag-mismatch
    // path (and unlike the Python SDK's _connection.py, which _poison()s
    // on any write OSError), it never marked the connection closed or
    // destroyed the socket. Every other in-flight request was left
    // pending, with nothing left to notice the socket was dead until the
    // 30s request timeout eventually fired.
    const { createServer, connect } = await import("node:net");
    const { Connection } = await import("../src/connection.js");

    const serverSockets: import("node:net").Socket[] = [];
    const server = createServer((socket) => {
      serverSockets.push(socket);
      // Never reply — the point is to prove a *write* failure alone
      // poisons the connection, independent of anything the server does.
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as import("node:net").AddressInfo).port;

    try {
      const socket = connect(port, "127.0.0.1");
      await new Promise<void>((resolve) => socket.once("connect", resolve));
      const connection = new Connection(socket);

      // Sent while the socket is still healthy, so its write succeeds
      // and it's left genuinely pending — proving it gets swept up by
      // the poison triggered below, not just failing on its own write.
      const secondRequest = connection.get("second");

      // Stub the next write to fail, the way a dead peer (EPIPE,
      // ECONNRESET) would, without actually breaking the socket —
      // deterministic, and doesn't depend on OS-level timing.
      const writeMock = mock.method(
        socket,
        "write",
        ((_data: unknown, cb?: (error?: Error) => void) => {
          if (typeof cb === "function") cb(Object.assign(new Error("write EPIPE"), { code: "EPIPE" }));
          return true;
        }) as typeof socket.write,
      );

      try {
        const started = Date.now();
        await assert.rejects(connection.get("first"), /connection failed/);
        assert.equal(connection.isClosed(), true);
        assert.ok(socket.destroyed, "the client must destroy the socket itself");
        await assert.rejects(secondRequest, /connection failed/);
        assert.ok(
          Date.now() - started < 2_000,
          "other in-flight requests must not wait for the 30s request timeout",
        );
      } finally {
        writeMock.mock.restore();
      }
    } finally {
      for (const socket of serverSockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

describe("NanocachedClient request timeout (issue #42)", () => {
  // REQUEST_TIMEOUT_TUNING exists only so these tests can shorten it.
  const defaultTimeoutMs = REQUEST_TIMEOUT_TUNING.timeoutMs;
  afterEach(() => {
    REQUEST_TIMEOUT_TUNING.timeoutMs = defaultTimeoutMs;
  });

  it("fails a request to a half-open server within the timeout instead of hanging", async () => {
    // Regression: a server that completes the A handshake but then never
    // answers a G/S/D used to hang get/set/delete forever — there was no
    // in-flight request timeout at all.
    const node = await startMockNode();
    try {
      REQUEST_TIMEOUT_TUNING.timeoutMs = 150;
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
      });
      try {
        await client.set("k", "v");
        node.goSilentAfterHandshake();

        const started = Date.now();
        // The client's retry layer redials once after the first timeout;
        // the redialed connection times out too, so this settles after
        // roughly two windows — still bounded.
        await assert.rejects(
          client.get("k"),
          (error: unknown) =>
            error instanceof Error && /request timed out/.test(error.message),
        );
        assert.ok(Date.now() - started < 2_000, "get() should fail well under 2s");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("steady new requests do not postpone half-open detection", async () => {
    // The deadline is progress-based: new sends must not extend it while
    // an older request is still waiting (mirrors the Go SDK's regression
    // test of the same name).
    const node = await startMockNode();
    try {
      REQUEST_TIMEOUT_TUNING.timeoutMs = 200;
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: node.port }],
      });
      try {
        await client.set("k", "v");
        node.goSilentAfterHandshake();

        // New requests keep arriving well inside every deadline window
        // (once the connection is poisoned they just fail fast).
        const ticker = setInterval(() => {
          client.get("more").catch(() => {});
        }, 50);
        try {
          const started = Date.now();
          await assert.rejects(
            client.get("k"),
            (error: unknown) =>
              error instanceof Error && /request timed out/.test(error.message),
          );
          assert.ok(Date.now() - started < 2_000, "get() should fail well under 2s");
        } finally {
          clearInterval(ticker);
        }
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient incr/decr against a single node (issue #129)", () => {
  it("returns null on a missing key", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        assert.equal(await client.incr("missing"), null);
        assert.equal(await client.decr("missing"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("throws NotNumericError when the stored value isn't INCR's counter grammar", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("greeting", "hello");
        await assert.rejects(client.incr("greeting"), NotNumericError);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("increments an existing counter and returns the new value", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("counter", "10");
        assert.equal(await client.incr("counter"), 11);
        assert.equal(await client.incr("counter", 5), 16);
        assert.equal(await client.get("counter"), "16");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("decr with a positive amount is the same result as incr with the negated delta", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("a", "100");
        await client.set("b", "100");
        assert.equal(await client.decr("a", 7), await client.incr("b", -7));
        assert.equal(await client.get("a"), await client.get("b"));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("negative deltas can drive the counter negative", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("counter", "3");
        assert.equal(await client.incr("counter", -10), -7);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("works scoped to a namespace, same as get/set/delete", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const ns = client.namespace("counters");
        await ns.set("hits", "1");
        assert.equal(await ns.incr("hits"), 2);
        assert.equal(await client.incr("hits"), null); // default namespace is untouched
        assert.equal(node.namespacedStore("counters").get("hits")?.toString("ascii"), "2");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient compare-and-set against a single node (issue #141)", () => {
  it("putIfAbsent stores and returns true when the key is missing", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        assert.equal(await client.putIfAbsent("k", "v1"), true);
        assert.equal(await client.get("k"), "v1");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("putIfAbsent returns false and leaves the value untouched when the key already exists", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "original");
        assert.equal(await client.putIfAbsent("k", "v2"), false);
        assert.equal(await client.get("k"), "original");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("replaceIfPresent replaces and returns true when the key exists, regardless of its value", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "original");
        assert.equal(await client.replaceIfPresent("k", "updated"), true);
        assert.equal(await client.get("k"), "updated");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("replaceIfPresent returns false and stores nothing when the key is missing", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        assert.equal(await client.replaceIfPresent("missing", "v"), false);
        assert.equal(await client.get("missing"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("replace succeeds when the token matches the current stored bytes, and fails when it doesn't", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v1");
        const token = contentDigest(Buffer.from("v1", "utf8"));

        // Stale token (value changed underneath since the digest was taken).
        await client.set("k", "v-changed");
        assert.equal(await client.replace("k", token, "v2"), false);
        assert.equal(await client.get("k"), "v-changed");

        // Fresh token from an actual read.
        const fresh = await client.getWithToken("k");
        assert.ok(fresh !== null);
        assert.equal(await client.replace("k", fresh!.token, "v2"), true);
        assert.equal(await client.get("k"), "v2");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("replace fails against a missing key", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const token = contentDigest(Buffer.from("anything", "utf8"));
        assert.equal(await client.replace("missing", token, "v"), false);
        assert.equal(await client.get("missing"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("deleteIfMatches deletes and returns true when the token matches", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v1");
        const { token } = (await client.getWithToken("k"))!;
        assert.equal(await client.deleteIfMatches("k", token), true);
        assert.equal(await client.get("k"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("deleteIfMatches returns false on a stale token or a missing key, without deleting", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v1");
        const staleToken = contentDigest(Buffer.from("not the stored value", "utf8"));
        assert.equal(await client.deleteIfMatches("k", staleToken), false);
        assert.equal(await client.get("k"), "v1");

        const missingToken = contentDigest(Buffer.from("anything", "utf8"));
        assert.equal(await client.deleteIfMatches("missing", missingToken), false);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("getWithToken returns null on a miss, matching get's own convention", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        assert.equal(await client.getWithToken("missing"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("getWithToken's token equals contentDigest of the raw stored bytes", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "hello world");
        const result = await client.getWithToken("k");
        assert.ok(result !== null);
        assert.deepEqual(result!.value, Buffer.from("hello world", "utf8"));
        assert.equal(result!.token, contentDigest(Buffer.from("hello world", "utf8")));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("works scoped to a namespace, same as get/set/delete/incr", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const ns = client.namespace("things");
        assert.equal(await ns.putIfAbsent("k", "v1"), true);
        assert.equal(await client.get("k"), null); // default namespace untouched
        assert.equal(node.namespacedStore("things").get("k")?.toString("utf8"), "v1");

        const { token } = (await ns.getWithToken("k"))!;
        assert.equal(await ns.replace("k", token, "v2"), true);
        assert.equal(await ns.deleteIfMatches("k", contentDigest(Buffer.from("v2", "utf8"))), true);
        assert.equal(await ns.getBytes("k"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("applies the TTL given to putIfAbsent/replaceIfPresent/replace", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.putIfAbsent("k", "v1", 120);
        assert.equal(node.lastSetTtl(), 120);

        await client.replaceIfPresent("k", "v2", 60);
        assert.equal(node.lastSetTtl(), 60);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient replication (client-side replication, R=2)", () => {
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
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
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "survives-a-node-death";
      await client.set(key, "still here");

      const { primary } = cluster.ownerOf(key);
      // Kill the primary outright — server gone, not just the connection.
      await primary.mock.close();
      await waitFor(() => memberConnectionClosed(client, primary.name), "the client to see the FIN");

      assert.equal(await client.get(key), "still here");
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("does not fail a write when a replica is down", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "written-despite-dead-replica";
      const { primary, replica } = cluster.ownerOf(key);

      await replica.mock.close();
      await waitFor(() => memberConnectionClosed(client, replica.name), "the client to see the FIN");

      await client.set(key, "v");
      assert.ok(primary.mock.store.has(key));
      assert.equal(await client.get(key), "v");
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("routes writes around a dead primary once discovery drops it", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
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
      assert.equal(await client.get(key), "v");
      assert.ok(replica.mock.store.has(key));
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("fans deletes out to every owner", async () => {
    const cluster = await startReplicatedCluster();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
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

describe("NanocachedClient incr/decr cluster replication (issue #129) — primary computes, replicas get the result via set", () => {
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

  it("sends `i` only to the primary; the replica gets a `set` of the literal result and never sees `i`", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "incr-replicates-as-set";
      const { primary, replica } = cluster.ownerOf(key);

      await client.set(key, "10");
      assert.equal(await client.incr(key, 5), 15);

      // The critical assertion (not just "same final value" — a buggy
      // implementation that replayed `i` on the replica from the same
      // seed would produce the same final byte value and this would pass
      // despite being wrong): the replica must receive the primary's
      // literal result as an ordinary `set`/`s`, and must NEVER itself
      // receive an `i` frame.
      assert.equal(primary.mock.incrCount(), 1, "primary must receive exactly one `i` frame");
      assert.equal(replica.mock.incrCount(), 0, "replica must never receive an `i` frame");
      assert.equal(replica.mock.lastCommand(), "S", "replica must receive the result as a set");
      assert.equal(primary.mock.store.get(key)?.toString("ascii"), "15");
      assert.equal(replica.mock.store.get(key)?.toString("ascii"), "15");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("forwards the primary's remaining TTL to the replica's set", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "incr-replicates-ttl";
      const { primary, replica } = cluster.ownerOf(key);

      await client.set(key, "10", 120);
      assert.equal(await client.incr(key), 11);

      assert.equal(primary.mock.incrCount(), 1);
      assert.equal(replica.mock.incrCount(), 0);
      assert.equal(replica.mock.lastCommand(), "S");
      assert.equal(replica.mock.lastSetTtl(), 120, "the replica's set must carry the primary's remaining TTL");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("never touches the replica on a miss or a non-numeric value — nothing was written", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      // Deltas, not absolute counts: with only 2 nodes, the two keys below
      // may well share the same primary/replica, so incrCount() must be
      // compared against its own before-value for each key rather than
      // asserted as an absolute total.
      const missKey = "incr-miss";
      const missOwners = cluster.ownerOf(missKey);
      const missPrimaryBefore = missOwners.primary.mock.incrCount();
      const missReplicaBefore = missOwners.replica.mock.incrCount();
      assert.equal(await client.incr(missKey), null);
      assert.equal(missOwners.primary.mock.incrCount(), missPrimaryBefore + 1);
      assert.equal(missOwners.replica.mock.incrCount(), missReplicaBefore);

      const nonNumericKey = "incr-non-numeric";
      await client.set(nonNumericKey, "not-a-number");
      const owners = cluster.ownerOf(nonNumericKey);
      const primaryBefore = owners.primary.mock.incrCount();
      const replicaBefore = owners.replica.mock.incrCount();
      await assert.rejects(client.incr(nonNumericKey), NotNumericError);
      assert.equal(owners.primary.mock.incrCount(), primaryBefore + 1);
      assert.equal(owners.replica.mock.incrCount(), replicaBefore);
      // The replica's copy must be untouched — no set was fanned out for
      // a request nothing was actually written by.
      assert.equal(owners.replica.mock.store.get(nonNumericKey)?.toString("ascii"), "not-a-number");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("decr replicates the same way: one `i` on the primary, a `set` on the replica", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "decr-replicates-as-set";
      const { primary, replica } = cluster.ownerOf(key);

      await client.set(key, "10");
      assert.equal(await client.decr(key, 3), 7);

      assert.equal(primary.mock.incrCount(), 1);
      assert.equal(replica.mock.incrCount(), 0);
      assert.equal(replica.mock.lastCommand(), "S");
      assert.equal(replica.mock.store.get(key)?.toString("ascii"), "7");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("counts a swallowed replica-write failure when the replica is dead, same counter as set/delete", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "incr-dead-replica";
      const { primary, replica } = cluster.ownerOf(key);
      await client.set(key, "10");

      await replica.mock.close();
      await waitFor(() => memberConnectionClosed(client, replica.name), "the client to see the FIN");

      assert.equal(client.stats().replicaWriteFailures, 0);
      // A dead replica must not fail the increment — the primary already
      // applied it, so its result is what's returned regardless.
      assert.equal(await client.incr(key), 11);
      assert.equal(client.stats().replicaWriteFailures, 1);
      assert.equal(primary.mock.store.get(key)?.toString("ascii"), "11");
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });
});

describe("NanocachedClient getMany/getManyBytes/setMany/setManyBytes against a single node (issues #128/#150/#151)", () => {
  it("returns hits and misses in one call, missing keys simply absent", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("a", "1");
        await client.set("c", "3");

        const values = await client.getMany(["a", "b", "c"]);
        assert.equal(values.size, 2);
        assert.equal(values.get("a"), "1");
        assert.equal(values.get("c"), "3");
        assert.equal(values.has("b"), false);
        assert.equal(node.multiGetCount(), 1);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("setMany then getMany round trips a shared TTL across the whole batch", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.setMany({ a: "1", b: "2" }, 60);
        assert.equal(node.lastSetTtl(), 60);
        assert.equal(node.multiSetCount(), 1);

        const values = await client.getMany(["a", "b"]);
        assert.equal(values.get("a"), "1");
        assert.equal(values.get("b"), "2");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("getManyBytes/setManyBytes round-trip raw bytes", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.setManyBytes({ a: Buffer.from([1, 2, 3]) });
        const values = await client.getManyBytes(["a"]);
        assert.deepEqual(values.get("a"), Buffer.from([1, 2, 3]));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("rejects an empty keys/values input synchronously", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await assert.rejects(client.getMany([]), RangeError);
        await assert.rejects(client.getManyBytes([]), RangeError);
        await assert.rejects(client.setMany({}), RangeError);
        await assert.rejects(client.setManyBytes({}), RangeError);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("splits a batch larger than MAX_BATCH_KEYS into more than one m/o sub-frame, transparently", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const total = MAX_BATCH_KEYS + 50;
        const values: Record<string, string> = {};
        const keys: string[] = [];
        for (let i = 0; i < total; i++) {
          const key = `key-${i}`;
          keys.push(key);
          values[key] = `value-${i}`;
        }

        await client.setMany(values);
        assert.equal(node.multiSetCount(), 2);

        const got = await client.getMany(keys);
        assert.equal(node.multiGetCount(), 2);
        assert.equal(got.size, total);
        for (const [key, want] of Object.entries(values)) {
          assert.equal(got.get(key), want);
        }
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("works scoped to a namespace, same as get/set/delete", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const users = client.namespace("users");
        await users.setMany({ "1": "alice", "2": "bob" });
        const values = await users.getMany(["1", "2"]);
        assert.equal(values.get("1"), "alice");
        assert.equal(values.get("2"), "bob");
        // The default namespace is untouched.
        assert.equal((await client.getMany(["1", "2"])).size, 0);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a per-key W propagates immediately in single mode — no ring to refresh against", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("a", "1");
        await client.set("b", "2");
        node.answerMultiWrongNodeTimes("a", 1);

        try {
          await client.getMany(["a", "b"]);
          assert.fail("expected getMany to reject");
        } catch (error) {
          assert.ok(error instanceof PartialWrongNodeError);
          assert.ok(error instanceof WrongNodeError);
          assert.equal((error as PartialWrongNodeError<Map<string, string>>).partialValues.get("b"), "2");
        }
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient getMany/getManyBytes/setMany/setManyBytes cluster replication (issues #128/#150/#151)", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  async function startReplicatedCluster(replication = 2) {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const nodes = [
      { name: names[0], mock: nodeA },
      { name: names[1], mock: nodeB },
    ];
    const discovery = await startMockDiscovery(
      nodes.map(({ name, mock }) => ({ name, address: mock.address })),
      { replication },
    );

    return {
      nodes,
      discovery,
      ownerOf(key: string) {
        const ring = new HashRing(names);
        const [primary] = ring.owners(Buffer.from(key), replication);
        return nodes.find(({ name }) => name === primary)!;
      },
      close: async () => {
        await Promise.all([discovery.close(), nodeA.close(), nodeB.close()]);
      },
    };
  }

  function keyWithPrimary(name: string, replication = 2): string {
    const ring = new HashRing(names);
    for (let i = 0; i < 1000; i++) {
      const key = `key-${i}`;
      if (ring.owners(Buffer.from(key), replication)[0] === name) return key;
    }
    throw new Error(`no key routes to ${name}`);
  }

  it("splits a batch across owners by primary and reassembles it in caller order", async () => {
    const cluster = await startReplicatedCluster(1);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const values: Record<string, string> = {};
      const keys: string[] = [];
      for (let i = 0; i < 20; i++) {
        const key = `key-${i}`;
        keys.push(key);
        values[key] = `value-${i}`;
      }
      await client.setMany(values);

      const got = await client.getMany(keys);
      for (const [key, want] of Object.entries(values)) {
        assert.equal(got.get(key), want);
      }

      // With 20 keys spread over 2 owners by HRW, both nodes should have
      // answered at least one `m` — proving the batch really was split by
      // owner rather than all sent to a single node.
      for (const { mock } of cluster.nodes) {
        assert.ok(mock.multiGetCount() > 0, "expected every node to receive at least one m frame");
      }
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("stores on every owner with replication 2", async () => {
    const cluster = await startReplicatedCluster(2);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const values: Record<string, string> = {};
      const keys: string[] = [];
      for (let i = 0; i < 10; i++) {
        const key = `key-${i}`;
        keys.push(key);
        values[key] = `value-${i}`;
      }
      await client.setMany(values);

      for (const key of keys) {
        for (const { mock } of cluster.nodes) {
          assert.ok(mock.store.has(key), `expected ${key} on every owner`);
        }
      }
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("sends exactly one o sub-frame to a node that is primary for one key and replica for another", async () => {
    const cluster = await startReplicatedCluster(2);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const keyA = keyWithPrimary(names[0]);
      const keyB = keyWithPrimary(names[1]);

      await client.setMany({ [keyA]: "va", [keyB]: "vb" });

      for (const { mock } of cluster.nodes) {
        assert.equal(mock.multiSetCount(), 1, "expected one sub-frame covering both its primary and replica key");
        assert.ok(mock.store.has(keyA) && mock.store.has(keyB));
      }
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("recovers a per-key W after one forced refresh", async () => {
    const cluster = await startReplicatedCluster(1);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const wrongKey = "wrong-key";
      const okKey = "ok-key";
      await client.set(wrongKey, "w");
      await client.set(okKey, "ok");

      const owner = cluster.ownerOf(wrongKey);
      owner.mock.answerMultiWrongNodeTimes(wrongKey, 1);

      const values = await client.getMany([wrongKey, okKey]);
      assert.equal(values.get(wrongKey), "w");
      assert.equal(values.get(okKey), "ok");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("degrades to a partial map wrapped in PartialWrongNodeError when a per-key W persists", async () => {
    const cluster = await startReplicatedCluster(1);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const wrongKey = "wrong-key";
      const okKey = "ok-key";
      await client.set(wrongKey, "w");
      await client.set(okKey, "ok");

      const owner = cluster.ownerOf(wrongKey);
      owner.mock.answerMultiWrongNodeTimes(wrongKey, 2); // survives the initial pass AND the one retry

      try {
        await client.getMany([wrongKey, okKey]);
        assert.fail("expected getMany to reject");
      } catch (error) {
        assert.ok(error instanceof PartialWrongNodeError);
        assert.ok(error instanceof WrongNodeError);
        const partial = (error as PartialWrongNodeError<Map<string, string>>).partialValues;
        assert.equal(partial.get(okKey), "ok");
        assert.equal(partial.has(wrongKey), false);
      }
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("setMany recovers a per-key W after one forced refresh", async () => {
    const cluster = await startReplicatedCluster(1);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const wrongKey = "wrong-key";
      const okKey = "ok-key";
      const owner = cluster.ownerOf(wrongKey);
      owner.mock.answerMultiWrongNodeTimes(wrongKey, 1);

      await client.setMany({ [wrongKey]: "w", [okKey]: "ok" });

      assert.ok(owner.mock.store.has(wrongKey));
      assert.ok(owner.mock.store.has(okKey));
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("setMany throws plain WrongNodeError (no partial payload) when a per-key W persists", async () => {
    const cluster = await startReplicatedCluster(1);
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const wrongKey = "wrong-key";
      const owner = cluster.ownerOf(wrongKey);
      owner.mock.answerMultiWrongNodeTimes(wrongKey, 2);

      await assert.rejects(client.setMany({ [wrongKey]: "w", "ok-key": "ok" }), (error: unknown) => {
        assert.ok(error instanceof WrongNodeError);
        assert.ok(!(error instanceof PartialWrongNodeError));
        return true;
      });
    } finally {
      client.close();
      await cluster.close();
    }
  });
});

describe("NanocachedClient compare-and-set cluster replication (issue #141) — primary evaluates, replicas get the result via set/delete", () => {
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

  it("putIfAbsent: sends `k` only to the primary; the replica gets a `set` of the literal result and never sees `k`", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "cas-putifabsent-replicates-as-set";
      const { primary, replica } = cluster.ownerOf(key);

      assert.equal(await client.putIfAbsent(key, "v1"), true);

      // The critical assertion — not just "same final value" (a buggy
      // implementation replaying `k` on the replica could coincidentally
      // agree): the replica must receive the primary's literal result as
      // an ordinary `set`/`s`, and must NEVER itself receive a `k` frame.
      assert.equal(primary.mock.casCount(), 1, "primary must receive exactly one `k` frame");
      assert.equal(replica.mock.casCount(), 0, "replica must never receive a `k` frame");
      assert.equal(replica.mock.lastCommand(), "S", "replica must receive the result as a set");
      assert.equal(primary.mock.store.get(key)?.toString("utf8"), "v1");
      assert.equal(replica.mock.store.get(key)?.toString("utf8"), "v1");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("replace: sends `k` only to the primary; the replica gets a `set` of the literal new value and never sees `k`", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "cas-replace-replicates-as-set";
      const { primary, replica } = cluster.ownerOf(key);

      await client.set(key, "v1", 120);
      const { token } = (await client.getWithToken(key))!;

      assert.equal(await client.replace(key, token, "v2", 120), true);

      assert.equal(primary.mock.casCount(), 1, "primary must receive exactly one `k` frame");
      assert.equal(replica.mock.casCount(), 0, "replica must never receive a `k` frame");
      assert.equal(replica.mock.lastCommand(), "S", "replica must receive the result as a set");
      assert.equal(replica.mock.lastSetTtl(), 120, "the replica's set must carry the same TTL");
      assert.equal(primary.mock.store.get(key)?.toString("utf8"), "v2");
      assert.equal(replica.mock.store.get(key)?.toString("utf8"), "v2");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("deleteIfMatches: sends `x` only to the primary; the replica gets a plain `delete` and never sees `x`", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "cas-delete-replicates-as-delete";
      const { primary, replica } = cluster.ownerOf(key);

      await client.set(key, "v1");
      const { token } = (await client.getWithToken(key))!;

      assert.equal(await client.deleteIfMatches(key, token), true);

      assert.equal(primary.mock.casDeleteCount(), 1, "primary must receive exactly one `x` frame");
      assert.equal(replica.mock.casDeleteCount(), 0, "replica must never receive an `x` frame");
      assert.equal(replica.mock.lastCommand(), "D", "replica must receive the result as a plain delete");
      assert.ok(!primary.mock.store.has(key));
      assert.ok(!replica.mock.store.has(key));
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("never touches the replica on a condition mismatch — nothing was written", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      // Deltas, not absolute counts: with only 2 nodes, keys below may
      // well share the same primary/replica, so cas(Delete)Count() must
      // be compared against its own before-value per key.
      const key = "cas-mismatch";
      await client.set(key, "original");
      const owners = cluster.ownerOf(key);
      const primaryBefore = owners.primary.mock.casCount();
      const replicaBefore = owners.replica.mock.casCount();

      const staleToken = contentDigest(Buffer.from("not the stored value", "utf8"));
      assert.equal(await client.replace(key, staleToken, "v2"), false);

      assert.equal(owners.primary.mock.casCount(), primaryBefore + 1);
      assert.equal(owners.replica.mock.casCount(), replicaBefore);
      // The replica's copy must be untouched — no set was fanned out for
      // a request nothing was actually written by.
      assert.equal(owners.replica.mock.store.get(key)?.toString("utf8"), "original");
      assert.equal(owners.primary.mock.store.get(key)?.toString("utf8"), "original");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("counts a swallowed replica-write failure when the replica is dead, same counter as set/delete/incr", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "cas-dead-replica";
      const { primary, replica } = cluster.ownerOf(key);

      await replica.mock.close();
      await waitFor(() => memberConnectionClosed(client, replica.name), "the client to see the FIN");

      assert.equal(client.stats().replicaWriteFailures, 0);
      // A dead replica must not fail the CAS — the primary already
      // applied it, so its result is what's returned regardless.
      assert.equal(await client.putIfAbsent(key, "v1"), true);
      assert.equal(client.stats().replicaWriteFailures, 1);
      assert.equal(primary.mock.store.get(key)?.toString("utf8"), "v1");
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });
});

describe("NanocachedClient fire-and-forget replica writes (fire-and-forget replica writes)", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  // A "did it wait for the mock's delay" assertion can't compare the
  // measured elapsed time against the delay exactly: setTimeout's firing
  // clock and Date.now() aren't the same clock, so an 80ms delay can be
  // observed as 79ms. Slack the lower bound by this much rather than
  // asserting on the boundary; still miles away from the ~0ms an
  // immediate return would show.
  const TIMING_TOLERANCE_MS = 20;

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

  afterEach(() => {
    FIRE_AND_FORGET_TUNING.maxInFlight = 32;
  });

  it("by default, a write still waits for the replica leg", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const { replica } = cluster.ownerOf("k");
      replica.mock.delaySets(80);

      const start = Date.now();
      await client.set("k", "v");
      assert.ok(
        Date.now() - start >= 80 - TIMING_TOLERANCE_MS,
        "set() should have waited for the replica",
      );
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("returns as soon as the primary acks when enabled", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const { replica } = cluster.ownerOf("k");
      replica.mock.delaySets(200);

      const start = Date.now();
      await client.set("k", "v");
      assert.ok(Date.now() - start < 200, "set() should not have waited for the replica");

      await waitFor(() => replica.mock.store.has("k"), "the background write to land on the replica");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("falls back to synchronous past the in-flight cap", async () => {
    FIRE_AND_FORGET_TUNING.maxInFlight = 2;

    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const { replica } = cluster.ownerOf("k");
      replica.mock.delaySets(150);

      const elapsed = await Promise.all(
        Array.from({ length: 3 }, async () => {
          const start = Date.now();
          await client.set("k", "v");
          return Date.now() - start;
        }),
      );

      assert.ok(
        elapsed.some((ms) => ms >= 150 - TIMING_TOLERANCE_MS),
        `expected at least one call to fall back to synchronous, got ${elapsed}`,
      );
      assert.ok(
        elapsed.some((ms) => ms < 150),
        `expected at least one call to return fast, got ${elapsed}`,
      );
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("close() drains in-flight background replica writes", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const { replica } = cluster.ownerOf("k");
      replica.mock.delaySets(80);

      await client.set("k", "v");
      // The drain contract (fire-and-forget replica writes as amended by issue #47 item 3):
      // close() resolves only after the in-flight replica write finished.
      await client.close();
      assert.ok(replica.mock.store.has("k"), "close() resolved before the background replica write finished");
    } finally {
      await cluster.close();
    }
  });

  it("close() loops its drain instead of only awaiting one snapshot (issue #47 item 3)", async () => {
    // A single `if (size > 0) await Promise.allSettled([...snapshot])`
    // misses a leg that registers itself while that one await is already
    // in flight — e.g. a set() mid-maybeRefreshNodeList() when close()
    // takes its snapshot. Reproduced directly here (rather than racing
    // real I/O timing, which can't guarantee landing in that window):
    // register a second leg from inside the first leg's own completion,
    // exactly modeling "one more leg shows up mid-drain".
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const backgroundWrites: Set<Promise<void>> = (client as any).backgroundReplicaWrites;

      let secondLegStarted = false;
      let secondLegDone = false;
      const firstLeg = new Promise<void>((resolve) => setTimeout(resolve, 20)).then(() => {
        secondLegStarted = true;
        const secondLeg = new Promise<void>((resolve) => setTimeout(resolve, 20)).then(() => {
          secondLegDone = true;
        });
        backgroundWrites.add(secondLeg);
        secondLeg.finally(() => backgroundWrites.delete(secondLeg));
      });
      backgroundWrites.add(firstLeg);
      firstLeg.finally(() => backgroundWrites.delete(firstLeg));

      await client.close();

      assert.ok(secondLegStarted, "test setup: the second leg never registered");
      assert.ok(secondLegDone, "close() resolved before a leg registered mid-drain finished");
    } finally {
      await cluster.close();
    }
  });

  it("close() waits for a replica write racing its own drain instead of abandoning it (issue #47 item 3)", async () => {
    // End-to-end companion to the deterministic loop test above: a burst
    // of real writes overlapping a concurrent close() — some inevitably
    // land between acking on the primary and registering their replica
    // leg right as close() takes its snapshot. Not guaranteed to hit that
    // exact window on every run, but the assertion holds regardless:
    // nothing may land on the replica after close() has already resolved
    // — if it does, a leg was abandoned mid-drain instead of waited for.
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const { replica } = cluster.ownerOf("k");
      replica.mock.delaySets(15);

      const total = 40;
      const writes: Promise<void>[] = [];
      let closePromise: Promise<void> | null = null;
      for (let i = 0; i < total; i++) {
        writes.push(client.set(`k${i}`, "v").catch(() => {}));
        if (i === Math.floor(total / 2) && closePromise === null) {
          closePromise = client.close();
        }
      }
      await Promise.all(writes);
      await closePromise;

      const sizeAtClose = replica.mock.store.size;
      await new Promise((resolve) => setTimeout(resolve, 100));
      assert.equal(
        replica.mock.store.size,
        sizeAtClose,
        "a replica write landed after close() had already resolved — it was abandoned mid-drain",
      );
    } finally {
      await cluster.close();
    }
  });
});

describe("fire-and-forget replica leg errors surface when the primary also fails (issue #188)", () => {
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

  it("set(): a genuine bug on the fire-and-forget replica leg surfaces once the primary fails too", async () => {
    // Regression: writeToOwners' fire-and-forget branch pushed a resolved
    // placeholder into synchronousReplicaWrites instead of the real leg
    // promise, so a non-network error (a programming bug, not a dead
    // replica) from a background replica leg vanished completely even
    // when the primary write failed and the call had to reject with
    // *something* anyway.
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const key = "boom-key";
      const { primary, replica } = cluster.ownerOf(key);

      // Establish real connections to both owners first...
      await client.set(key, "v");

      // ...then stub the primary's connection to fail the way a dead
      // connection ordinarily would, and the replica's to simulate a bug
      // in this SDK's own code (a TypeError, not a network-class error).
      const primaryConnection = (client as any).target.members.get(primary.name).connection;
      const replicaConnection = (client as any).target.members.get(replica.name).connection;
      mock.method(primaryConnection, "set", () => Promise.reject(new ConnectionLostError("primary connection lost")));
      mock.method(replicaConnection, "set", () => {
        throw new TypeError("injected programming bug");
      });

      await assert.rejects(client.set(key, "v2"), (error: unknown) => {
        assert.ok(error instanceof TypeError, `expected the replica leg's TypeError to surface, got ${error}`);
        return true;
      });
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("set(): an ordinary connection failure on the fire-and-forget replica leg stays silent, even when the primary fails too", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const key = "boom-key-2";
      const { primary, replica } = cluster.ownerOf(key);

      await client.set(key, "v");

      // NanocachedError rather than ConnectionLostError here: the latter
      // is retryable (withWrongNodeRetry forces a refresh and retries
      // the whole set() once — isConnectionError), which would run this
      // whole scenario twice and muddy the assertions below. A plain
      // NanocachedError is swallowable (see isSwallowable) but not
      // connection-shaped, so it propagates on the first attempt only —
      // still a fully representative "ordinary, expected failure".
      const primaryConnection = (client as any).target.members.get(primary.name).connection;
      const replicaConnection = (client as any).target.members.get(replica.name).connection;
      mock.method(primaryConnection, "set", () => Promise.reject(new NanocachedError("primary connection lost")));
      mock.method(replicaConnection, "set", () => Promise.reject(new NanocachedError("replica connection lost")));

      await assert.rejects(client.set(key, "v2"), (error: unknown) => {
        assert.ok(
          error instanceof NanocachedError && /primary connection lost/.test(error.message),
          `expected the primary's own error, got ${error}`,
        );
        return true;
      });
      assert.equal(
        client.stats().replicaWriteFailures,
        1,
        "the replica's ordinary connection failure must still be counted as a swallow",
      );
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("setMany(): a genuine bug on a fire-and-forget pure-replica leg surfaces once a primary-holding leg fails too", async () => {
    // Same regression as above, for multiSetPass's fire-and-forget
    // pure-replica legs: a single key at replication 2 makes the
    // replica-owning node's whole leg pure-replica (it holds no primary
    // key at all), so it's fire-and-forget-eligible on its own.
    //
    // Unlike writeToOwners, an ordinary (swallowable) failure on a
    // primary-holding leg never throws in multiSetPass — it's folded
    // into the returned retry list instead (see runLeg's own catch), so
    // it can't stand in for "the primary leg fails" here. The primary's
    // own leg is given a distinct programming bug (RangeError) instead,
    // just to force Promise.all(legs) to reject — the assertion below is
    // that the pure-replica leg's own bug (TypeError) is what actually
    // surfaces, not the primary leg's.
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      fireAndForgetReplicas: true,
    });
    try {
      const key = "boom-key";
      const { primary, replica } = cluster.ownerOf(key);

      await client.setMany({ [key]: "v" });

      const primaryConnection = (client as any).target.members.get(primary.name).connection;
      const replicaConnection = (client as any).target.members.get(replica.name).connection;
      mock.method(primaryConnection, "multiSet", () => {
        throw new RangeError("injected primary-leg programming bug");
      });
      mock.method(replicaConnection, "multiSet", () => {
        throw new TypeError("injected pure-replica-leg programming bug");
      });

      await assert.rejects(client.setMany({ [key]: "v2" }), (error: unknown) => {
        assert.ok(error instanceof TypeError, `expected the pure-replica leg's TypeError to surface, got ${error}`);
        return true;
      });
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });
});

describe("NanocachedClient read repair (read repair)", () => {
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

  it("by default, a clean miss on the primary is not repaired", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const { primary, replica } = cluster.ownerOf("k");
      replica.mock.store.set("k", Buffer.from("from-replica"));

      assert.equal(await client.get("k"), null);
      assert.ok(!primary.mock.store.has("k"), "primary was repaired despite readRepair being off");
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("finds a value on a replica and repairs the primary", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readRepair: true,
    });
    try {
      const { primary, replica } = cluster.ownerOf("k");
      replica.mock.store.set("k", Buffer.from("from-replica"));

      assert.equal(await client.get("k"), "from-replica");
      await waitFor(() => primary.mock.store.has("k"), "the primary to be repaired");
      // The original TTL can't be recovered from a GET; a repair must
      // not use TTL 0 (no expiry), which would permanently resurrect
      // already-expired data — see READ_REPAIR_TTL_SECONDS in client.ts.
      assert.equal(primary.mock.lastSetTtl(), 60);
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("stays a clean miss when no owner has the value", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readRepair: true,
    });
    try {
      assert.equal(await client.get("nowhere"), null);
    } finally {
      client.close();
      await cluster.close();
    }
  });
});

describe("NanocachedClient hedged reads (issue #64)", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  // Generous versus the other suites' TIMING_TOLERANCE_MS (20) so this
  // stays green on CI (ubuntu), which the task calling for this suite
  // asked to be treated as noisier than a dev machine.
  const TIMING_TOLERANCE_MS = 30;

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

  async function timed<T>(promise: Promise<T>): Promise<[T, number]> {
    const start = Date.now();
    const value = await promise;
    return [value, Date.now() - start];
  }

  it("rejects a non-positive readHedgeAfterMs", async () => {
    for (const bad of [0, -1]) {
      await assert.rejects(
        () => NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: 1 }], readHedgeAfterMs: bad }),
        NanocachedError,
      );
    }
  });

  it("a hit from the replica wins over a slow primary", async () => {
    const cluster = await startReplicatedCluster();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
        readHedgeAfterMs: 50,
      });
      await client.set("k", "v");
      const { primary, replica } = cluster.ownerOf("k");
      primary.mock.delayGets(400);

      const [value, elapsed] = await timed(client.get("k"));

      assert.equal(value, "v");
      assert.ok(elapsed < 400 - TIMING_TOLERANCE_MS, `elapsed was ${elapsed}`);
      assert.ok(elapsed >= 50 - TIMING_TOLERANCE_MS, `elapsed was ${elapsed}`);
      assert.equal(replica.mock.getCount(), 1, "the replica should have been hedged to");

      // The slow primary's leg was left to finish, not cancelled, and
      // close() drained it.
      await client.close();
      assert.equal((client as any).hedgedReads.size, 0);
      assert.equal(primary.mock.getCount(), 1);
    } finally {
      await cluster.close();
    }
  });

  it("a hedge leg racing close() is refused, not registered (#91)", async () => {
    const cluster = await startReplicatedCluster();
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
        readHedgeAfterMs: 50,
      });
      await client.set("k", "v");

      // Simulate close() having begun: the flag is set and the drain has
      // already run. A read that only now reaches leg registration must not
      // slip a leg in — start() must reject instead of dialing against a
      // connection teardown is closing (issue #91). Setting `closed`
      // directly reproduces exactly the state start() sees at that point.
      (client as any).closed = true;
      const op = async () => {
        throw new Error("the leg must never be dialed after close() began");
      };
      await assert.rejects(
        (client as any).readHedged("k", op, ["a", "b"]),
        AlreadyClosedError,
      );
      assert.equal((client as any).hedgedReads.size, 0, "no hedge leg may be registered after close() began");

      // Restore so close() runs its real teardown rather than the
      // already-closed warning-and-return path.
      (client as any).closed = false;
      await client.close();
    } finally {
      await cluster.close();
    }
  });

  it("a fast primary is never hedged", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readHedgeAfterMs: 50,
    });
    try {
      await client.set("k", "v");
      const { replica } = cluster.ownerOf("k");
      for (let i = 0; i < 5; i++) {
        assert.equal(await client.get("k"), "v");
      }
      assert.equal(replica.mock.getCount(), 0);
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("a replica miss waits for the primary", async () => {
    // Hedging must never turn a hit into a miss: the replica lacks the
    // copy and answers first, but the primary's answer is what counts.
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readHedgeAfterMs: 50,
    });
    try {
      await client.set("k", "v");
      const { primary, replica } = cluster.ownerOf("k");
      replica.mock.store.delete("k");
      primary.mock.delayGets(200);

      const [value, elapsed] = await timed(client.get("k"));

      assert.equal(value, "v");
      assert.ok(elapsed >= 200 - TIMING_TOLERANCE_MS, `elapsed was ${elapsed}`);
      assert.equal(replica.mock.getCount(), 1);

      // A key nobody has: the miss is accepted once the primary has
      // answered it too.
      assert.equal(await client.get("absent"), null);
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("off by default: a slow primary bounds the read", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
    });
    try {
      await client.set("k", "v");
      const { primary, replica } = cluster.ownerOf("k");
      primary.mock.delayGets(200);

      const [value, elapsed] = await timed(client.get("k"));

      assert.equal(value, "v");
      assert.ok(elapsed >= 200 - TIMING_TOLERANCE_MS, `elapsed was ${elapsed}`);
      assert.equal(replica.mock.getCount(), 0);
    } finally {
      client.close();
      await cluster.close();
    }
  });

  it("a dead primary fails over immediately", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readHedgeAfterMs: 500,
    });
    try {
      await client.set("k", "v");
      const { primary } = cluster.ownerOf("k");
      await primary.mock.close();
      await waitFor(() => memberConnectionClosed(client, primary.name), "the client to see the FIN");

      const [value, elapsed] = await timed(client.get("k"));

      assert.equal(value, "v");
      assert.ok(elapsed < 500 - TIMING_TOLERANCE_MS, `elapsed was ${elapsed}`);
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });
});

describe("NanocachedClient.stats() (observability for by-design swallows)", () => {
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

  it("starts at zero", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        assert.deepEqual(client.stats(), { replicaWriteFailures: 0, readRepairFailures: 0, refreshFailures: 0, transientRetries: 0 });
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("counts a swallowed replica-write failure when a replica is dead (client-side replication / fire-and-forget replica writes)", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "written-despite-dead-replica";
      const { primary, replica } = cluster.ownerOf(key);

      await replica.mock.close();
      await waitFor(() => memberConnectionClosed(client, replica.name), "the client to see the FIN");

      assert.equal(client.stats().replicaWriteFailures, 0);
      await client.set(key, "v");
      assert.ok(primary.mock.store.has(key));
      assert.equal(client.stats().replicaWriteFailures, 1);
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("swallows a failed owner probe without counting it (issue #43)", async () => {
    // readRepairFailures counts failed repair *write-backs* only,
    // matching the other five SDKs — a failed owner probe during the
    // repair scan is swallowed silently.
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readRepair: true,
    });
    try {
      const key = "read-repair-swallow";
      const { replica } = cluster.ownerOf(key);

      await replica.mock.close();
      await waitFor(() => memberConnectionClosed(client, replica.name), "the client to see the FIN");

      // The primary reports a clean miss, then read repair probes the
      // (dead) replica and swallows the resulting connection failure —
      // without counting it.
      assert.equal(await client.get(key), null);
      assert.equal(client.stats().readRepairFailures, 0);
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("counts a failed repair write-back (read repair, issue #43)", async () => {
    // The write-back leg is what the counter measures — the replica's
    // value is still returned to the caller, but the background repair
    // write to the primary fails and is counted.
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({
      addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
      readRepair: true,
    });
    try {
      const key = "read-repair-write-back";
      const { primary, replica } = cluster.ownerOf(key);
      replica.mock.store.set(key, Buffer.from("from-replica"));
      // GETs against the primary keep missing normally; only the
      // repair's background S is answered with W and swallowed.
      primary.mock.answerWrongNodeOnSetOnce();

      assert.equal(client.stats().readRepairFailures, 0);
      assert.equal(await client.get(key), "from-replica");
      await waitFor(() => client.stats().readRepairFailures >= 1, "the failed repair write-back to be counted");
    } finally {
      client.close();
      await cluster.close().catch(() => {});
    }
  });

  it("counts a swallowed refresh failure for an unreachable discovery seed", async () => {
    const node = await startMockNode();
    const discovery = await startMockDiscovery([{ name: names[0], address: node.address }]);
    const deadPort = await unusedPort();
    try {
      const client = await NanocachedClient.connect({
        addresses: [
          { host: "127.0.0.1", port: deadPort },
          { host: "127.0.0.1", port: discovery.port },
        ],
      });
      try {
        assert.equal(client.stats().refreshFailures, 0);
        // Forces a fresh fetchNodeList walk: the dead port fails first,
        // counted as a refresh failure, before discovery answers.
        await (client as any).refreshNodeList();
        assert.equal(client.stats().refreshFailures, 1);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), node.close()]);
    }
  });

  it("counts a swallowed refresh failure when a discovery server reports no live nodes", async () => {
    // Regression (issue #47 audit item 7): a discovery server that's up
    // but knows no live nodes is just as unusable for a refresh as one
    // that's unreachable, and must be counted the same way.
    const node = await startMockNode();
    const discovery = await startMockDiscovery([{ name: names[0], address: node.address }]);
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }] });
      try {
        assert.equal(client.stats().refreshFailures, 0);
        discovery.setNodes([]);
        // Forces a fresh fetchNodeList walk against the now-empty
        // discovery response.
        await (client as any).refreshNodeList();
        assert.equal(client.stats().refreshFailures, 1);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), node.close()]);
    }
  });

  it("does not let a programming error from a replica leg clobber an already-successful primary write", async () => {
    const cluster = await startReplicatedCluster();
    const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
    try {
      const key = "boom-key";
      const { replica } = cluster.ownerOf(key);

      // Establish real connections to both owners first...
      await client.set(key, "v");
      // ...then stub the replica's own connection to simulate a bug in
      // this SDK's own code, e.g. a TypeError from a bad internal call —
      // this must NOT be swallowed the same way a dead replica is (it
      // isn't counted in replicaWriteFailures below), but it also must
      // not be allowed to discard an already-successful primary write:
      // the write completed, so set() must resolve, not reject.
      const replicaConnection = (client as any).target.members.get(replica.name).connection;
      mock.method(replicaConnection, "set", () => {
        throw new TypeError("injected programming bug");
      });

      await client.set(key, "v2");
      assert.equal(client.stats().replicaWriteFailures, 0, "a programming error must not be counted as a swallow");
      assert.equal(await client.get(key), "v2", "the primary's successful write must still have taken effect");
    } finally {
      client.close();
      await cluster.close().catch(() => {});
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
    // Node identity decoupled from address) — FNV-1a spreads these across the ring, where
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        assert.equal(client.nodeUrls.length, 2);

        const keys = Array.from({ length: 50 }, (_, i) => `key-${i}`);
        await Promise.all(keys.map((key) => client.set(key, `value of ${key}`)));
        for (const key of keys) {
          assert.equal(await client.get(key), `value of ${key}`);
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        const key = "some-key";
        const ring = new HashRing(cluster.nodes.map(({ name }) => name));
        const owner = cluster.nodes.find(({ name }) => name === ring.route(Buffer.from(key)))!;

        await client.set(key, "v");

        owner.mock.answerWrongNodeOnce();
        assert.equal(await client.get(key), "v");
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
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
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
        NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }] }),
        /no live nodes/,
      );
    } finally {
      await discovery.close();
    }
  });
});

describe("NanocachedClient response tags (echoed response tags)", () => {
  it("negotiates tags and round-trips pipelined requests", async () => {
    const node = await startMockNode({ supportTags: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await Promise.all(Array.from({ length: 20 }, (_, i) => client.set(`key-${i}`, `value-${i}`, i)));
        const values = await Promise.all(Array.from({ length: 20 }, (_, i) => client.get(`key-${i}`)));
        values.forEach((value, i) => assert.equal(value, `value-${i}`));

        assert.equal(await client.delete("key-0"), true);
        assert.equal(await client.delete("key-0"), false);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a desynced stream is caught by the tag check before any caller sees wrong data", async () => {
    // The exact misdelivery request pipelining left open: the server (as a stand-in
    // for any off-by-one stream corruption) never answers the first GET,
    // so the second GET's response arrives at the first GET's pending
    // slot. Without tags the first caller would receive the second's
    // value as a plausible, exception-free wrong answer; the tag check
    // must poison the connection before either caller sees anything.
    const node = await startMockNode({ supportTags: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");

        node.swallowGetOnce();
        const first = client.get("a");
        const second = client.get("k");
        await assert.rejects(first, /desynced/);
        await assert.rejects(second, /desynced/);

        // The poisoned connection redials transparently on next use.
        assert.equal(await client.get("k"), "v");
        assert.equal(node.connectionCount(), 2);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a response echoing the wrong tag poisons the connection", async () => {
    const node = await startMockNode({ supportTags: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        node.answerWrongTagOnce();
        await assert.rejects(client.get("k"), /desynced/);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("falls back to the untagged protocol against a pre-0019 server", async () => {
    // An old server treats any extended `A` (with `T`, or `T R` —
    // retryable-error status, issue #125) as a parse error and closes
    // without replying; the client must redial down to the plain form
    // and run untagged — transparently, with the same results.
    const node = await startMockNode({ closeOnExtendedAuth: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
        // Three dials: the `A ... T R` probe and the `A ... T` fallback
        // the server slammed shut in turn, then the plain fallback that
        // stuck.
        assert.equal(node.connectionCount(), 3);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient retryable-error status (issue #125)", () => {
  it("probes with A <len> T R, recorded by the mock", async () => {
    const node = await startMockNode({ supportTags: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        assert.equal(node.lastAuthHeader(), "A 1 T R");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("falls back to A <len> T against a server that understands tags but predates R", async () => {
    // The middle stage of the three-stage probe (A <len> T R -> A <len> T
    // -> A <len>): a server that predates R treats the fuller extended
    // form as a parse error and closes without replying.
    const node = await startMockNode({ supportTags: true, closeOnRetryableAuth: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
        // Two dials: the `A ... T R` probe the server slammed shut, then
        // the `A ... T` fallback that stuck.
        assert.equal(node.connectionCount(), 2);
        assert.equal(node.lastAuthHeader(), "A 1 T");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("R once then success: transparently retries, no new connection, transientRetries == 1", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        node.answerRetryableTimes(1);

        const start = Date.now();
        assert.equal(await client.get("k"), "v");
        // Bounded retry sleeps 50ms before the first retry.
        assert.ok(Date.now() - start >= 45, "expected the 50ms pre-retry delay to have elapsed");

        // Two G requests reached the mock (the R'd attempt and the retry
        // that succeeded), but only one connection was ever dialed.
        assert.equal(node.getCount(), 2);
        assert.equal(node.connectionCount(), 1);
        assert.equal(client.stats().transientRetries, 1);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("R three times: raises RetryableError without poisoning the connection; transientRetries == 3", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        node.answerRetryableTimes(3);

        await assert.rejects(client.get("k"), RetryableError);

        // The connection stays open and usable for a following op —
        // no teardown, no reconnect/refresh path.
        assert.equal(node.connectionCount(), 1);
        assert.equal(await client.get("k"), "v");
        assert.equal(client.stats().transientRetries, 3);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("tagged mode: a retried R pairs with the right in-flight request among pipelined ops", async () => {
    const node = await startMockNode({ supportTags: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("a", "value-a");
        await client.set("b", "value-b");

        // Only the first of the two pipelined GETs the mock receives is
        // answered R; the client must retry exactly that one (with a
        // fresh tag) without disturbing the other in-flight request.
        node.answerRetryableTimes(1);
        const [a, b] = await Promise.all([client.get("a"), client.get("b")]);
        assert.equal(a, "value-a");
        assert.equal(b, "value-b");

        assert.equal(node.connectionCount(), 1);
        assert.equal(client.stats().transientRetries, 1);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("works via SDK proxy mode (viaProxy) too", async () => {
    const proxy = await startMockNode();
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([{ name: "proxy-1", address: proxy.address }]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: discovery.port }],
        viaProxy: true,
      });
      try {
        await client.set("k", "v");
        proxy.answerRetryableTimes(1);
        assert.equal(await client.get("k"), "v");
        assert.equal(proxy.connectionCount(), 1);
        assert.equal(client.stats().transientRetries, 1);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), proxy.close()]);
    }
  });
});

describe("NanocachedClient tolerant bootstrap (issue #67)", () => {
  // Issue #67: connect() must tolerate a node that discovery still lists
  // but that can't be reached (dead, not yet evicted), the way steady-state
  // requests already do — and fail only when no node is reachable.
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];

  function ownerNames(key: string): string[] {
    return new HashRing(names).owners(Buffer.from(key), 2);
  }

  function keyWithPrimary(name: string): string {
    for (let i = 0; i < 1000; i++) {
      const key = `key-${i}`;
      if (ownerNames(key)[0] === name) return key;
    }
    throw new Error(`no key routes to ${name}`);
  }

  async function startCluster(dead: Set<string>) {
    const nodes = new Map<string, MockNode>();
    const entries: Array<{ name: string; address: string }> = [];
    for (const name of names) {
      if (dead.has(name)) {
        const port = await unusedPort();
        entries.push({ name, address: `127.0.0.1:${port}` });
      } else {
        const node = await startMockNode();
        nodes.set(name, node);
        entries.push({ name, address: node.address });
      }
    }
    const discovery = await startMockDiscovery(entries, { replication: 2 });
    return {
      nodes,
      discovery,
      close: async () => {
        await Promise.all([discovery.close(), ...[...nodes.values()].map((node) => node.close())]);
      },
    };
  }

  it("connect() succeeds with one unreachable node", async () => {
    const [dead, live] = names;
    const cluster = await startCluster(new Set([dead]));
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
        reconnectCooldownMs: 50,
      });
      try {
        assert.equal(client.replication, 2);
        assert.equal((client as any).target.members.get(dead).connection, null);
        assert.ok((client as any).target.members.get(live).connection !== null);

        // A key whose primary is alive: the write lands, the dead replica
        // leg is swallowed and counted, the read hits.
        const key = keyWithPrimary(live);
        await client.set(key, "v");
        assert.equal(await client.get(key), "v");
        assert.equal(client.stats().replicaWriteFailures, 1);

        // A key whose primary is the dead node: the read fails over to the
        // live replica right away — well under the 5s dial timeout,
        // whether or not the reconnect cooldown for the dead address is
        // still armed (an ECONNREFUSED dial fails fast either way).
        const other = keyWithPrimary(dead);
        cluster.nodes.get(live)!.store.set(other, Buffer.from("replica copy"));
        const start = Date.now();
        assert.equal(await client.get(other), "replica copy");
        assert.ok(Date.now() - start < 500, "fallback to the live replica was not fast");
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("connect() fails only when every listed node is unreachable", async () => {
    const cluster = await startCluster(new Set(names));
    try {
      await assert.rejects(
        NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] }),
      );
    } finally {
      await cluster.close();
    }
  });

  it("redials an unreachable node once the cooldown has passed", async () => {
    const [dead, live] = names;
    const cluster = await startCluster(new Set([dead]));
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
        reconnectCooldownMs: 50,
      });
      try {
        // Bring the "dead" node up on the address discovery listed.
        const deadAddress: string = (client as any).target.members.get(dead).address;
        const port = Number(deadAddress.split(":")[1]);
        const revived = await startMockNode({ port });
        cluster.nodes.set(dead, revived);
        await delay(100);

        const key = keyWithPrimary(dead);
        await client.set(key, "v");
        assert.ok(revived.store.has(key));
        assert.ok((client as any).target.members.get(dead).connection !== null);
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("refresh purges cooldowns for departed addresses (#96)", async () => {
    // A node that leaves the cluster must not leave its per-address
    // reconnect-cooldown entry behind — in a churny deployment (a fresh
    // IP:port per restart) those would accumulate unboundedly.
    const [dead, live] = names;
    const cluster = await startCluster(new Set([dead]));
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }],
        reconnectCooldownMs: 60_000,
      });
      try {
        const deadAddress: string = (client as any).target.members.get(dead).address;
        const cooldowns: Map<string, unknown> = (client as any).reconnectCooldowns;
        // The unreachable node armed its cooldown at bootstrap.
        assert.ok(cooldowns.has(deadAddress), "no cooldown armed for the unreachable node");

        // Discovery drops the dead node; the refresh must purge its cooldown
        // alongside its member entry.
        cluster.discovery.setNodes([{ name: live, address: cluster.nodes.get(live)!.address }]);
        await (client as any).refreshNodeList();

        assert.ok(!(client as any).target.members.has(dead), "departed node still in members");
        assert.ok(!cooldowns.has(deadAddress), "cooldown for departed address was not purged");
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });
});

describe("NanocachedClient namespaces (first-class namespaces, issue #105)", () => {
  it("round-trips get/getBytes/set/delete through a namespace handle", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const users = client.namespace("users");
        await users.set("alice", "hello", 60);
        assert.equal(await users.get("alice"), "hello");
        assert.deepEqual(await users.getBytes("alice"), Buffer.from("hello"));
        assert.equal(node.lastSetTtl(), 60);

        assert.equal(await users.delete("alice"), true);
        assert.equal(await users.get("alice"), null);
        assert.equal(await users.delete("alice"), false);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("isolates the same key name across the default namespace and two named ones", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const users = client.namespace("users");
        const orders = client.namespace("orders");

        await client.set("k", "default value");
        await users.set("k", "users value");
        await orders.set("k", "orders value");

        assert.equal(await client.get("k"), "default value");
        assert.equal(await users.get("k"), "users value");
        assert.equal(await orders.get("k"), "orders value");

        // Three genuinely independent entries on the wire, not one key
        // getting overwritten three times.
        assert.equal(node.store.get("k")?.toString("utf8"), "default value");
        assert.equal(node.namespacedStore("users").get("k")?.toString("utf8"), "users value");
        assert.equal(node.namespacedStore("orders").get("k")?.toString("utf8"), "orders value");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("namespace(\"\") is not rejected and behaves exactly like the root client, using legacy frames", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const root = client.namespace("");
        assert.deepEqual(root.namespace, Buffer.alloc(0));

        await root.set("k", "v");
        assert.equal(node.lastCommand(), "S", "the empty namespace must send the legacy S frame, not s");
        assert.equal(await client.get("k"), "v", "namespace(\"\") shares storage with the root client");

        assert.equal(await root.get("k"), "v");
        assert.equal(node.lastCommand(), "G", "the empty namespace must send the legacy G frame, not g");

        assert.equal(await root.delete("k"), true);
        assert.equal(node.lastCommand(), "D", "the empty namespace must send the legacy D frame, not d");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a non-empty namespace sends the lowercase g/s/d frames", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const users = client.namespace("users");

        await users.set("k", "v");
        assert.equal(node.lastCommand(), "s");

        await users.get("k");
        assert.equal(node.lastCommand(), "g");

        await users.delete("k");
        assert.equal(node.lastCommand(), "d");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("accepts a raw-bytes namespace, not just a string, and encodes a string namespace as UTF-8", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const byString = client.namespace("日本");
        await byString.set("k", "v");

        const byBytes = client.namespace(Buffer.from("日本", "utf8"));
        assert.equal(await byBytes.get("k"), "v", "a string namespace and its UTF-8 encoding must address the same namespace");
        assert.deepEqual(byBytes.namespace, Buffer.from("日本", "utf8"));
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("a namespace handle is invalid once the client is closed, exactly like the client itself", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      const users = client.namespace("users");
      client.close();

      await assert.rejects(users.get("k"), AlreadyClosedError);
      await assert.rejects(users.getBytes("k"), AlreadyClosedError);
      await assert.rejects(users.set("k", "v"), AlreadyClosedError);
      await assert.rejects(users.delete("k"), AlreadyClosedError);
    } finally {
      await node.close();
    }
  });

  it("routes a namespaced key by (namespace, key), agreeing with the shared hash ring", async () => {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];
    const nodes = [
      { name: names[0], mock: nodeA },
      { name: names[1], mock: nodeB },
    ];
    const discovery = await startMockDiscovery(nodes.map(({ name, mock }) => ({ name, address: mock.address })));
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }] });
      try {
        const users = client.namespace("users");
        const ring = new HashRing(names);
        const key = "alpha";

        await users.set(key, "v");

        // Discovery's default replication is 1 (mockServers.ts), so
        // exactly one of the two nodes should hold the write — whichever
        // one the ring picks as the (namespace, key) primary.
        const namespacedOwner = ring.route(Buffer.from(key), Buffer.from("users"));
        const other = nodes.find(({ name }) => name !== namespacedOwner)!;
        const owner = nodes.find(({ name }) => name === namespacedOwner)!;
        assert.ok(owner.mock.namespacedStore("users").has(key), `${key} did not land on ${namespacedOwner} in namespace "users"`);
        assert.ok(!other.mock.namespacedStore("users").has(key), `${key} unexpectedly landed on the non-owner ${other.name}`);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), nodeA.close(), nodeB.close()]);
    }
  });

  it("W refresh-and-retry on a namespaced key routes by (namespace, key)", async () => {
    const [nodeA, nodeB] = await Promise.all([startMockNode(), startMockNode()]);
    const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47"];
    const nodes = [
      { name: names[0], mock: nodeA },
      { name: names[1], mock: nodeB },
    ];
    const discovery = await startMockDiscovery(nodes.map(({ name, mock }) => ({ name, address: mock.address })));
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }] });
      try {
        const users = client.namespace("users");
        const key = "alpha";
        const ring = new HashRing(names);
        const owner = nodes.find(({ name }) => name === ring.route(Buffer.from(key), Buffer.from("users")))!;

        await users.set(key, "v");

        owner.mock.answerWrongNodeOnce();
        assert.equal(await users.get(key), "v");
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), nodeA.close(), nodeB.close()]);
    }
  });

  it("clear() empties one namespace, leaving the default namespace and other namespaces untouched", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const users = client.namespace("users");
        const orders = client.namespace("orders");

        await client.set("k", "default value");
        await users.set("k", "users value");
        await orders.set("k", "orders value");

        await users.clear();
        assert.equal(node.lastCommand(), "c", "clear() must send the lowercase c frame");

        assert.equal(await users.get("k"), null, "the cleared namespace must be empty");
        assert.equal(await client.get("k"), "default value", "the default namespace must survive");
        assert.equal(await orders.get("k"), "orders value", "the other namespace must survive");
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("namespace(\"\").clear() clears the default namespace (c 0), same as clearAll's default-namespace half", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        await client.namespace("").clear();
        assert.equal(await client.get("k"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("clearAll() empties every namespace, including the default one", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        const users = client.namespace("users");
        await client.set("k", "default value");
        await users.set("k", "users value");

        await client.clearAll();

        assert.equal(await client.get("k"), null);
        assert.equal(await users.get("k"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("clear()/clearAll() raise AlreadyClosedError after close()", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      const users = client.namespace("users");
      client.close();

      await assert.rejects(users.clear(), AlreadyClosedError);
      await assert.rejects(client.clearAll(), AlreadyClosedError);
    } finally {
      await node.close();
    }
  });

  it("sends the clear to the single node in standalone mode", async () => {
    const node = await startMockNode();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        await client.clearAll();
        assert.equal(node.clearCount(), 1);
        assert.equal(await client.get("k"), null);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});

describe("NanocachedClient clear/clearAll fan-out (issue #106)", () => {
  const names = ["5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6", "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47", "b2e6a1c4-3f9d-4a7b-8e5c-1d0f6a2b9c3e"];

  async function startCluster() {
    const mocks = await Promise.all([startMockNode(), startMockNode(), startMockNode()]);
    const nodes = names.map((name, i) => ({ name, mock: mocks[i] }));
    const discovery = await startMockDiscovery(nodes.map(({ name, mock }) => ({ name, address: mock.address })));
    return {
      nodes,
      discovery,
      close: async () => {
        await Promise.all([discovery.close(), ...mocks.map((m) => m.close())]);
      },
    };
  }

  it("fans a namespaced clear out to every node in the cluster, not just the key's owners", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        await client.namespace("users").clear();
        for (const { name, mock } of cluster.nodes) {
          assert.equal(mock.clearCount(), 1, `${name} did not receive the clear`);
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("fans clearAll() out to every node in the cluster", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        await client.clearAll();
        for (const { name, mock } of cluster.nodes) {
          assert.equal(mock.clearCount(), 1, `${name} did not receive the flush`);
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("one node failing the first pass still succeeds once the refresh-and-retry reaches it", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        const flaky = cluster.nodes[0].mock;
        flaky.failClearOnce();

        await client.clearAll();

        // The first pass failed on `flaky` (connection destroyed instead
        // of acked); the retry re-sends to *every* node of the refreshed
        // list (per issue #106's spec), not just the one that failed, so
        // every node — including the two that already succeeded — ends
        // up with 2 clear attempts, and the whole call still succeeds.
        for (const { name, mock } of cluster.nodes) {
          assert.equal(mock.clearCount(), 2, `${name} did not see both the first pass and the retry`);
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("a node that keeps failing raises an error naming it, after the one retry", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        const stubborn = cluster.nodes[0].mock;
        // Fails both the first pass and the retry after the forced refresh.
        stubborn.failClearOnce();
        stubborn.failClearOnce();

        await assert.rejects(client.clearAll(), (error: unknown) => {
          assert.ok(error instanceof NanocachedError);
          assert.match((error as Error).message, /clear failed on node\(s\)/);
          assert.match((error as Error).message, new RegExp(names[0]));
          return true;
        });
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });

  it("never silently succeeds on a partial clear — the other nodes were cleared but the call still throws", async () => {
    const cluster = await startCluster();
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: cluster.discovery.port }] });
      try {
        await Promise.all(cluster.nodes.map(({ mock }) => mock.namespacedStore("users").set("k", Buffer.from("v"))));

        const stubborn = cluster.nodes[0].mock;
        stubborn.failClearOnce();
        stubborn.failClearOnce();

        await assert.rejects(client.namespace("users").clear());

        // The other two nodes' namespace was still genuinely cleared —
        // the failure is reported, not swallowed, even though most of
        // the cluster did clear.
        const healthy = cluster.nodes.slice(1);
        for (const { mock } of healthy) {
          assert.equal(mock.namespacedStore("users").has("k"), false);
        }
      } finally {
        client.close();
      }
    } finally {
      await cluster.close();
    }
  });
});

describe("NanocachedClient SDK proxy mode (issue #122, viaProxy)", () => {
  // A proxy is just a MockNode from the wire's point of view (see
  // via-proxy-spec.md: "A proxy looks exactly like a single node that
  // owns every key") — registered with the mock discovery's separate
  // `setProxies` roster instead of `setNodes`.
  function proxyAddressOf(client: NanocachedClient): string {
    return (client as any).target.address;
  }

  it("routes every op to the proxy discovery hands back, and never touches the node list", async () => {
    const [proxy, node] = await Promise.all([startMockNode(), startMockNode()]);
    const discovery = await startMockDiscovery([{ name: "some-node", address: node.address }]);
    discovery.setProxies([{ name: "proxy-1", address: proxy.address }]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: discovery.port }],
        viaProxy: true,
      });
      try {
        assert.equal(proxyAddressOf(client), proxy.address);

        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
        assert.equal(await client.delete("k"), true);
        await client.namespace("ns").set("nk", "nv");
        assert.equal(await client.namespace("ns").get("nk"), "nv");
        await client.clearAll();

        // Never dialed the node list at all — proxy mode fetches `Q`,
        // never `L`, and never opens a connection to a "node" address.
        assert.equal(discovery.listCount(), 0);
        assert.ok(discovery.listProxiesCount() >= 1);
        assert.equal(node.connectionCount(), 0);
        assert.equal(proxy.connectionCount(), 1);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), proxy.close(), node.close()]);
    }
  });

  it("spreads a fleet of fresh clients across every registered proxy", async () => {
    const [proxyA, proxyB] = await Promise.all([startMockNode(), startMockNode()]);
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([
      { name: "proxy-a", address: proxyA.address },
      { name: "proxy-b", address: proxyB.address },
    ]);
    try {
      const clients = await Promise.all(
        Array.from({ length: 20 }, () =>
          NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }], viaProxy: true }),
        ),
      );
      try {
        assert.ok(proxyA.connectionCount() > 0, "proxy A was never picked");
        assert.ok(proxyB.connectionCount() > 0, "proxy B was never picked");
        assert.equal(proxyA.connectionCount() + proxyB.connectionCount(), 20);
      } finally {
        for (const client of clients) client.close();
      }
    } finally {
      await Promise.all([discovery.close(), proxyA.close(), proxyB.close()]);
    }
  });

  it("fails over to the reachable proxy when the other one is down", async () => {
    const live = await startMockNode();
    const deadPort = await unusedPort();
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([
      { name: "proxy-dead", address: `127.0.0.1:${deadPort}` },
      { name: "proxy-live", address: live.address },
    ]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: discovery.port }],
        viaProxy: true,
      });
      try {
        assert.equal(proxyAddressOf(client), live.address);
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), live.close()]);
    }
  });

  it("falls through a discovery seed still in its startup grace to the next seed's Q", async () => {
    const proxy = await startMockNode();
    const [warming, healthy] = await Promise.all([startMockDiscovery([]), startMockDiscovery([])]);
    warming.setWarmingUp(true);
    healthy.setProxies([{ name: "proxy-1", address: proxy.address }]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [
          { host: "127.0.0.1", port: warming.port },
          { host: "127.0.0.1", port: healthy.port },
        ],
        viaProxy: true,
      });
      try {
        assert.equal(proxyAddressOf(client), proxy.address);
        assert.equal(await client.get("missing"), null);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([warming.close(), healthy.close(), proxy.close()]);
    }
  });

  it("rejects connect with the dial error when every registered proxy is unreachable", async () => {
    const [deadPortA, deadPortB] = await Promise.all([unusedPort(), unusedPort()]);
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([
      { name: "proxy-a", address: `127.0.0.1:${deadPortA}` },
      { name: "proxy-b", address: `127.0.0.1:${deadPortB}` },
    ]);
    try {
      await assert.rejects(
        NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }], viaProxy: true }),
        (error: unknown) => error instanceof Error && typeof (error as NodeJS.ErrnoException).code === "string",
      );
    } finally {
      await discovery.close();
    }
  });

  it("rejects connect with a clear error when no proxies are registered", async () => {
    const discovery = await startMockDiscovery([]);
    try {
      await assert.rejects(
        NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: discovery.port }], viaProxy: true }),
        /no proxies registered/,
      );
    } finally {
      await discovery.close();
    }
  });

  it("rejects connect with a clear error when viaProxy is pointed at a cache node", async () => {
    const node = await startMockNode();
    try {
      await assert.rejects(
        NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }], viaProxy: true }),
        /not a discovery server/,
      );
    } finally {
      await node.close();
    }
  });

  it("reconnects by re-fetching Q and landing on the surviving proxy", async () => {
    const [proxyA, proxyB] = await Promise.all([startMockNode(), startMockNode()]);
    const byAddress = new Map([
      [proxyA.address, proxyA],
      [proxyB.address, proxyB],
    ]);
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([
      { name: "proxy-a", address: proxyA.address },
      { name: "proxy-b", address: proxyB.address },
    ]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: discovery.port }],
        viaProxy: true,
      });
      try {
        await client.set("k", "v");

        const landedAddress = proxyAddressOf(client);
        const dead = byAddress.get(landedAddress)!;
        const survivorAddress = landedAddress === proxyA.address ? proxyB.address : proxyA.address;
        const survivor = byAddress.get(survivorAddress)!;

        // Fully closing the dead proxy's server (not just dropping its
        // connections) makes both the same-proxy redial and any stray
        // dial during the Q-refresh fail fast, instead of hanging on a
        // half-open TCP handshake.
        await dead.close();
        await waitFor(() => singleConnectionClosed(client), "the client to notice the dead proxy");

        assert.equal(await client.get("k"), null);
        assert.equal(proxyAddressOf(client), survivorAddress);
        assert.ok(survivor.connectionCount() > 0);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), proxyA.close().catch(() => {}), proxyB.close().catch(() => {})]);
    }
  });

  it("ignores readHedgeAfterMs — a proxy connection has no replicas to hedge to", async () => {
    const proxy = await startMockNode();
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([{ name: "proxy-1", address: proxy.address }]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: discovery.port }],
        viaProxy: true,
        readHedgeAfterMs: 10,
      });
      try {
        await client.set("k", "v");
        proxy.delayGets(50);
        assert.equal(await client.get("k"), "v");
        // A hedge would have sent a second G shortly after the first;
        // exactly one reached the wire.
        assert.equal(proxy.getCount(), 1);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), proxy.close()]);
    }
  });

  it("getMany/setMany ride the single proxy connection, no owner grouping (issues #128/#150/#151)", async () => {
    const proxy = await startMockNode();
    const discovery = await startMockDiscovery([]);
    discovery.setProxies([{ name: "proxy-1", address: proxy.address }]);
    try {
      const client = await NanocachedClient.connect({
        addresses: [{ host: "127.0.0.1", port: discovery.port }],
        viaProxy: true,
      });
      try {
        await client.setMany({ a: "1", b: "2" });
        assert.equal(proxy.multiSetCount(), 1);

        const values = await client.getMany(["a", "b", "missing"]);
        assert.equal(values.get("a"), "1");
        assert.equal(values.get("b"), "2");
        assert.equal(values.has("missing"), false);
        assert.equal(proxy.multiGetCount(), 1);
      } finally {
        client.close();
      }
    } finally {
      await Promise.all([discovery.close(), proxy.close()]);
    }
  });
});
