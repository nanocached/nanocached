import { afterEach, describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import {
  AlreadyClosedError,
  AuthenticationError,
  ConnectionLostError,
  DecompressionError,
  DiscoveryBusyError,
  NanocachedClient,
  NanocachedError,
  WrongNodeError,
} from "../src/index.js";
import { HashRing } from "../src/hashRing.js";
import { FIRE_AND_FORGET_TUNING, KEEPALIVE_TUNING } from "../src/client.js";
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

describe("NanocachedClient value compression (doc/adr/0013-*.md)", () => {
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
        // collide with the DEFLATE marker (0x01) — doc/adr/0013-*.md's
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
      reconnectCooldownMs: 200,
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
        assert.ok(elapsed < 100, `expected a cooldown-fast rejection, took ${elapsed}ms`);
        assert.equal(connections, 0, "the cooldown did not prevent a redial");

        // Once the cooldown window has passed, the address is dialed
        // again, this time reaching the listener.
        await delay(250);
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

describe("NanocachedClient fire-and-forget replica writes (doc/adr/0014-*.md)", () => {
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
      // The drain contract (ADR-0014 as amended by issue #47 item 3):
      // close() resolves only after the in-flight replica write finished.
      await client.close();
      assert.ok(replica.mock.store.has("k"), "close() resolved before the background replica write finished");
    } finally {
      await cluster.close();
    }
  });
});

describe("NanocachedClient read repair (doc/adr/0015-*.md)", () => {
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
        assert.deepEqual(client.stats(), { replicaWriteFailures: 0, readRepairFailures: 0, refreshFailures: 0 });
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });

  it("counts a swallowed replica-write failure when a replica is dead (ADR-0011/0014)", async () => {
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

  it("counts a failed repair write-back (ADR-0015, issue #43)", async () => {
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

describe("NanocachedClient response tags (doc/adr/0019-*.md)", () => {
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
    // The exact misdelivery ADR-0016 left open: the server (as a stand-in
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
    // An old server treats `A ... T` as a parse error and closes without
    // replying; the client must redial once with the plain form and run
    // untagged — transparently, with the same results.
    const node = await startMockNode({ closeOnExtendedAuth: true });
    try {
      const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await client.set("k", "v");
        assert.equal(await client.get("k"), "v");
        // Two dials: the extended attempt the server slammed shut, then
        // the plain fallback that stuck.
        assert.equal(node.connectionCount(), 2);
      } finally {
        client.close();
      }
    } finally {
      await node.close();
    }
  });
});
