import { describe, it } from "node:test";
import assert from "node:assert/strict";
import Keyv from "keyv";
import { createCache } from "cache-manager";
import { AlreadyClosedError } from "nanocached";
import { nanocachedKeyvStore, NanocachedKeyvStore } from "../src/index.js";
import { startMockNode, type MockNode } from "./mockNode.js";

// The framework-level entry point most tests below drive the adapter
// through is `new Keyv({ store })` — Keyv's own consumption API, never
// `NanocachedKeyvStore` methods directly — per the shared adapter spec's
// lesson from #107: the deliverable is "the framework's idioms work", not
// just "the interface is implemented".
async function connectStore(node: MockNode, config: Partial<Parameters<typeof nanocachedKeyvStore>[0]> = {}) {
  return nanocachedKeyvStore({
    addresses: [{ host: "127.0.0.1", port: node.port }],
    ...config,
  });
}

describe("nanocached-keyv, through Keyv's own API", () => {
  it("round-trips get/set/delete for objects, null, and arrays", async () => {
    const node = await startMockNode();
    try {
      const store = await connectStore(node);
      const keyv = new Keyv({ store, useKeyPrefix: false });
      try {
        await keyv.set("obj", { a: 1, b: [1, 2, 3] });
        assert.deepEqual(await keyv.get("obj"), { a: 1, b: [1, 2, 3] });

        await keyv.set("nullish", null);
        assert.equal(await keyv.get("nullish"), null); // round-trips, distinct from a miss

        assert.equal(await keyv.get("never-set"), undefined);

        assert.equal(await keyv.delete("obj"), true);
        assert.equal(await keyv.get("obj"), undefined);
      } finally {
        await store.disconnect();
      }
    } finally {
      await node.close();
    }
  });

  describe("ttl: milliseconds in, whole seconds (rounded up) on the wire", () => {
    it("a ttl under a second rounds up to 1", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("k", "v", 250); // 0.25s
          assert.equal(node.lastSetTtl(), 1);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    it("a ttl above a second rounds up to the next whole second", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("k", "v", 2_500); // 2.5s -> 3s
          assert.equal(node.lastSetTtl(), 3);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    it("no ttl at all sends 0 (no expiry) on the wire", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("k", "v");
          assert.equal(node.lastSetTtl(), 0);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });
  });

  describe("has()/getMany()/deleteMany() — omitted here, correct via Keyv's own fallbacks", () => {
    it("has() reflects presence for a live key", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          assert.equal(await keyv.has("k"), false);
          await keyv.set("k", "v");
          assert.equal(await keyv.has("k"), true);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    // The regression test that matters most in this module: a naive
    // has() built directly on this wire (just checking raw storage
    // presence) would misreport this key as present, because the wire's
    // whole-second TTL hasn't swept it yet even though Keyv's own
    // precise-millisecond `expires` deadline (embedded in the value's
    // JSON envelope) has already passed. Proven live against keyv 5.6.0
    // during design — see the module README's "Honest subset" section.
    // Omitting has() lets Keyv's own get()-based fallback (which decodes
    // that envelope and checks `expires`) answer correctly instead.
    it("has() correctly returns false once a key's precise expiry has passed, even though the wire's coarser TTL has not yet swept it", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("k", "v", 1); // 1ms Keyv-level deadline; rounds up to a 1s wire TTL
          await new Promise((resolve) => setTimeout(resolve, 50));

          assert.equal(await keyv.has("k"), false);
          assert.equal(await keyv.get("k"), undefined);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    it("getMany()/deleteMany() round-trip and preserve key order, with undefined holes for misses", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("a", 1);
          await keyv.set("b", 2);
          await keyv.set("c", 3);

          assert.deepEqual(await keyv.getMany(["b", "missing", "a", "c"]), [2, undefined, 1, 3]);

          await keyv.deleteMany(["a", "c"]);
          assert.deepEqual(await keyv.getMany(["a", "b", "c"]), [undefined, 2, undefined]);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });
  });

  describe("clear()", () => {
    it("sends exactly one clear frame on the wire", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("a", 1);
          await keyv.set("b", 2);
          assert.equal(node.clearCount(), 0);

          await keyv.clear();
          assert.equal(node.clearCount(), 1);
          assert.equal(await keyv.get("a"), undefined);
          assert.equal(await keyv.get("b"), undefined);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    it("isolates two stores on different nanocached namespaces against the same node", async () => {
      const node = await startMockNode();
      try {
        const storeA = await connectStore(node, { namespace: "ns-a" });
        const storeB = await connectStore(node, { namespace: "ns-b" });
        const keyvA = new Keyv({ store: storeA, useKeyPrefix: false });
        const keyvB = new Keyv({ store: storeB, useKeyPrefix: false });
        try {
          await keyvA.set("shared-key", "from-a");
          await keyvB.set("shared-key", "from-b");
          assert.equal(await keyvA.get("shared-key"), "from-a");
          assert.equal(await keyvB.get("shared-key"), "from-b");

          await keyvA.clear();
          assert.equal(await keyvA.get("shared-key"), undefined);
          assert.equal(await keyvB.get("shared-key"), "from-b"); // untouched
        } finally {
          await storeA.disconnect();
          await storeB.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    it('defaults to the "keyv" namespace when none is configured', async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        const keyv = new Keyv({ store, useKeyPrefix: false });
        try {
          await keyv.set("k", "v");
          assert.equal(node.store("keyv").has("k"), true);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });
  });

  describe("namespace vs. Keyv's own prefixing — two independent concepts", () => {
    it("useKeyPrefix: false sends the raw key onto the wire, unprefixed", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node, { namespace: "sessions" });
        const keyv = new Keyv({ store, namespace: "some-keyv-namespace", useKeyPrefix: false });
        try {
          await keyv.set("user:42", "Ada");
          assert.equal(node.store("sessions").has("user:42"), true);
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });

    it("leaving Keyv's own prefixing on still works correctly — just bakes an extra string into the wire key", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node, { namespace: "sessions" });
        // useKeyPrefix defaults to true; Keyv's own default namespace is "keyv".
        const keyv = new Keyv({ store });
        try {
          await keyv.set("user:42", "Ada");
          assert.equal(node.store("sessions").has("keyv:user:42"), true);
          assert.equal(await keyv.get("user:42"), "Ada"); // still round-trips correctly
        } finally {
          await store.disconnect();
        }
      } finally {
        await node.close();
      }
    });
  });

  describe("disconnect()", () => {
    // Asserted directly against the store, not through `keyv.get`/`keyv.set`:
    // Keyv's own `throwOnErrors` defaults to false, so by default it
    // swallows whatever a store's methods throw/reject (emitting an
    // `error` event instead) rather than propagating it — a Keyv-level
    // policy, not something this adapter controls. This test is about
    // this store's own lifecycle contract, so it goes straight to the
    // source.
    it("closes cleanly, and later operations reject with AlreadyClosedError", async () => {
      const node = await startMockNode();
      try {
        const store = await connectStore(node);
        await store.disconnect();

        await assert.rejects(() => store.get("k"), AlreadyClosedError);
        await assert.rejects(() => store.set("k", "v"), AlreadyClosedError);
      } finally {
        await node.close();
      }
    });
  });

  it("exposes the underlying SDK client on the store", async () => {
    const node = await startMockNode();
    try {
      const store = await connectStore(node);
      try {
        assert.ok(store instanceof NanocachedKeyvStore);
        assert.equal(typeof store.client.close, "function");
        assert.equal(store.client.isClosed(), false);
      } finally {
        await store.disconnect();
      }
    } finally {
      await node.close();
    }
  });
});

describe("nanocached-keyv, through cache-manager v6+'s createCache()", () => {
  it("get/set/del round trip through the Cache consumption API", async () => {
    const node = await startMockNode();
    try {
      const store = await connectStore(node);
      const cache = createCache({ stores: [new Keyv({ store, useKeyPrefix: false })] });
      try {
        await cache.set("user:1", { name: "Ada" });
        assert.deepEqual(await cache.get("user:1"), { name: "Ada" });

        assert.equal(await cache.del("user:1"), true);
        assert.equal(await cache.get("user:1"), undefined);
      } finally {
        await store.disconnect();
      }
    } finally {
      await node.close();
    }
  });
});
