import { describe, it } from "node:test";
import assert from "node:assert/strict";
import Keyv from "keyv";
import KeyvV5 from "keyv5";
import { ConnectionLostError } from "nanocached";
import { nanocachedKeyvStore, type NanocachedKeyvStore } from "../src/index.js";
import { startMockNode, type MockNode } from "./mockNode.js";

// Direct `new Keyv(store)` tests against keyv 6 (the `keyv` dev dependency).
// test/store.test.ts and test/nestjs.test.ts drive the same adapter through
// keyv 5 — imported there as `keyv5`, an npm alias of keyv@^5 — which is also
// the version cache-manager 7 depends on.
//
// keyv 6 does not call this adapter directly. The adapter does not advertise
// `capabilities.expires`, so keyv 6 wraps it in its own `KeyvBridgeAdapter`,
// which turns keyv 6's absolute expiry back into the relative ttl the
// adapter's `set()` takes. The tests below pin what that combination does on
// the wire.

async function withStore(
  fn: (ctx: { node: MockNode; store: NanocachedKeyvStore }) => Promise<void>,
  config: Partial<Parameters<typeof nanocachedKeyvStore>[0]> = {},
): Promise<void> {
  const node = await startMockNode();
  try {
    const store = await nanocachedKeyvStore({ addresses: [{ host: "127.0.0.1", port: node.port }], ...config });
    try {
      await fn({ node, store });
    } finally {
      await store.disconnect();
    }
  } finally {
    await node.close();
  }
}

describe("nanocached-keyv under keyv 6", () => {
  it("is used through keyv 6's bridge, not directly", async () => {
    await withStore(async ({ store }) => {
      assert.equal((store as { capabilities?: unknown }).capabilities, undefined);
      const keyv = new Keyv(store);
      assert.equal(keyv.store.constructor.name, "KeyvBridgeAdapter");
    });
  });

  it("round-trips get/set/delete for objects, null and arrays; a miss is undefined", async () => {
    await withStore(async ({ store }) => {
      const keyv = new Keyv(store);
      assert.equal(await keyv.set("obj", { a: 1, b: [1, 2, 3] }), true);
      assert.deepEqual(await keyv.get("obj"), { a: 1, b: [1, 2, 3] });

      await keyv.set("nullish", null);
      assert.equal(await keyv.get("nullish"), null); // round-trips, distinct from a miss

      assert.equal(await keyv.get("never-set"), undefined);

      assert.equal(await keyv.delete("obj"), true);
      assert.equal(await keyv.get("obj"), undefined);
    });
  });

  it("sends the raw key onto the wire when Keyv has no namespace of its own", async () => {
    await withStore(
      async ({ node, store }) => {
        const keyv = new Keyv(store);
        await keyv.set("user:42", "Ada");
        assert.equal(node.store("sessions").has("user:42"), true);
        assert.equal(await keyv.get("user:42"), "Ada");
      },
      { namespace: "sessions" },
    );
  });

  describe("ttl: absolute expiry in, whole seconds (rounded up) on the wire", () => {
    it("a ttl under a second rounds up to 1", async () => {
      await withStore(async ({ node, store }) => {
        await new Keyv(store).set("k", "v", 250);
        assert.equal(node.lastSetTtl(), 1);
      });
    });

    it("a ttl above a second rounds up to the next whole second", async () => {
      await withStore(async ({ node, store }) => {
        await new Keyv(store).set("k", "v", 2_500);
        assert.equal(node.lastSetTtl(), 3);
      });
    });

    it("a whole-second ttl stays that many seconds (the bridge's few elapsed milliseconds do not tip it down)", async () => {
      await withStore(async ({ node, store }) => {
        const keyv = new Keyv(store);
        for (const seconds of [1, 2, 60]) {
          await keyv.set("k", "v", seconds * 1000);
          assert.equal(node.lastSetTtl(), seconds, `ttl ${seconds}s`);
        }
      });
    });

    it("the Keyv-level default ttl applies too", async () => {
      await withStore(async ({ node, store }) => {
        await new Keyv(store, { ttl: 4_200 }).set("k", "v");
        assert.equal(node.lastSetTtl(), 5);
      });
    });

    it("no ttl at all sends 0 (no expiry) on the wire", async () => {
      await withStore(async ({ node, store }) => {
        await new Keyv(store).set("k", "v");
        assert.equal(node.lastSetTtl(), 0);
      });
    });

    it("an entry is hidden once its precise expiry passes, even before the wire's whole-second ttl sweeps it", async () => {
      await withStore(async ({ store }) => {
        const keyv = new Keyv(store);
        await keyv.set("k", "v", 20); // 20 ms deadline; rounds up to a 1 s wire ttl
        assert.equal(await keyv.get("k"), "v");
        await new Promise((resolve) => setTimeout(resolve, 80));
        assert.equal(await keyv.get("k"), undefined);
        assert.equal(await keyv.has("k"), false);
      });
    });

    it("a ttl that is already over never reaches the wire as a write, and deletes what was there", async () => {
      await withStore(async ({ node, store }) => {
        const keyv = new Keyv(store);
        await keyv.set("k", "stale", 60_000);
        assert.equal(node.store("keyv").has("k"), true);

        // Below Keyv's own set(): a negative ttl is dropped by Keyv, so
        // use setRaw with an expiry in the past.
        await keyv.setRaw("k", { value: "late", expires: Date.now() - 1000 });
        assert.equal(node.store("keyv").has("k"), false);
        assert.equal(await keyv.get("k"), undefined);
      });
    });
  });

  describe("has / getMany / setMany / deleteMany — Keyv's own fallbacks over get/set/delete", () => {
    it("has() reflects presence", async () => {
      await withStore(async ({ store }) => {
        const keyv = new Keyv(store);
        assert.equal(await keyv.has("k"), false);
        await keyv.set("k", "v");
        assert.equal(await keyv.has("k"), true);
      });
    });

    it("getMany()/deleteMany() preserve key order, with undefined holes for misses", async () => {
      await withStore(async ({ store }) => {
        const keyv = new Keyv(store);
        await keyv.set("a", 1);
        await keyv.set("b", 2);
        await keyv.set("c", 3);

        assert.deepEqual(await keyv.getMany(["b", "missing", "a", "c"]), [2, undefined, 1, 3]);

        assert.deepEqual(await keyv.deleteMany(["a", "c"]), [true, true]);
        assert.deepEqual(await keyv.getMany(["a", "b", "c"]), [undefined, 2, undefined]);
      });
    });

    it("setMany() writes every entry, each with its own wire ttl", async () => {
      await withStore(async ({ node, store }) => {
        const keyv = new Keyv(store);
        assert.deepEqual(
          await keyv.setMany([
            { key: "a", value: 1, ttl: 1_500 },
            { key: "b", value: 2 },
          ]),
          [true, true],
        );
        assert.deepEqual(await keyv.getMany(["a", "b"]), [1, 2]);
        assert.equal(node.store("keyv").size, 2);
      });
    });
  });

  describe("clear()", () => {
    it("is the nanocached namespace's CLEAR: exactly one frame, and nothing else is touched", async () => {
      const node = await startMockNode();
      const address = [{ host: "127.0.0.1", port: node.port }];
      const storeA = await nanocachedKeyvStore({ addresses: address, namespace: "ns-a" });
      const storeB = await nanocachedKeyvStore({ addresses: address, namespace: "ns-b" });
      try {
        const keyvA = new Keyv(storeA);
        const keyvB = new Keyv(storeB);
        await keyvA.set("shared-key", "from-a");
        await keyvA.set("other", "x");
        await keyvB.set("shared-key", "from-b");
        assert.equal(node.clearCount(), 0);

        await keyvA.clear();
        assert.equal(node.clearCount(), 1);
        assert.equal(await keyvA.get("shared-key"), undefined);
        assert.equal(await keyvA.get("other"), undefined);
        assert.equal(await keyvB.get("shared-key"), "from-b"); // untouched
      } finally {
        await storeA.disconnect();
        await storeB.disconnect();
        await node.close();
      }
    });

    // keyv 6 prefixes keys itself (inside its bridge, `<namespace>::<key>`)
    // when the Keyv has a namespace, and clears such a namespace by
    // enumerating the store's keys — which this wire cannot do. keyv 6 then
    // refuses rather than wiping every namespace; that refusal is what is
    // pinned here. (keyv 5 on the same adapter wipes the whole nanocached
    // namespace instead; see test/store.test.ts.)
    it("with a Keyv namespace set, clear() reports an error and deletes nothing", async () => {
      await withStore(async ({ node, store }) => {
        const keyv = new Keyv(store, { namespace: "users" });
        const seen: unknown[] = [];
        keyv.on("error", (error: unknown) => seen.push(error));

        await keyv.set("foo", "bar");
        assert.equal(node.store("keyv").has("users::foo"), true); // keyv 6's own prefix, separator "::"
        assert.equal(await keyv.get("foo"), "bar");

        await keyv.clear();
        assert.equal(node.clearCount(), 0);
        assert.equal(seen.length, 1);
        assert.match(String((seen[0] as Error).message), /Can't clear namespace "users"/);
        assert.equal(await keyv.get("foo"), "bar"); // still there

        // With nothing listening, the same failure rejects instead.
        const unlistened = new Keyv(store, { namespace: "users" });
        await assert.rejects(() => unlistened.clear(), /Can't clear namespace "users"/);
        assert.equal(node.clearCount(), 0);
      });
    });

    it("two Keyv namespaces on one store do not see each other's keys", async () => {
      await withStore(async ({ store }) => {
        const users = new Keyv(store, { namespace: "users" });
        const cache = new Keyv(store, { namespace: "cache" });
        await users.set("foo", "users");
        await cache.set("foo", "cache");
        assert.equal(await users.get("foo"), "users");
        assert.equal(await cache.get("foo"), "cache");
      });
    });
  });

  it("a keyv 5 Keyv and a keyv 6 Keyv can share one store: each one's set() is read by the other", async () => {
    await withStore(async ({ node, store }) => {
      const v5 = new KeyvV5({ store, useKeyPrefix: false });
      const v6 = new Keyv(store);

      await v5.set("from-5", { n: 5 }, 60_000);
      await v6.set("from-6", { n: 6 }, 60_000);
      assert.equal(node.lastSetTtl(), 60);

      assert.deepEqual(await v6.get("from-5"), { n: 5 });
      assert.deepEqual(await v5.get("from-6"), { n: 6 });
    });
  });

  describe("errors", () => {
    it("an oversize value is a RangeError: set() resolves false with an error listener, rejects without one", async () => {
      await withStore(async ({ node, store }) => {
        const tooBig = "x".repeat(1024 * 1024); // over the request limit (1 MiB minus a 256-byte header)

        const listened = new Keyv(store);
        const seen: unknown[] = [];
        listened.on("error", (error: unknown) => seen.push(error));
        assert.equal(await listened.set("big", tooBig), false);
        assert.equal(seen.length, 1);
        assert.ok(seen[0] instanceof RangeError, `expected a RangeError, got ${String(seen[0])}`);
        assert.equal(node.store("keyv").has("big"), false);

        const unlistened = new Keyv(store);
        await assert.rejects(() => unlistened.set("big", tooBig), RangeError);
      });
    });

    const isOutage = (error: unknown): boolean =>
      error instanceof ConnectionLostError || (error as { code?: string }).code === "ECONNREFUSED";

    it("an unreachable node: with an error listener get() resolves undefined and set() false, each emitting the error; without one both reject", async () => {
      const node = await startMockNode();
      const store = await nanocachedKeyvStore({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      try {
        await node.close(); // the node goes away after the store connected

        const listened = new Keyv(store);
        const seen: unknown[] = [];
        listened.on("error", (error: unknown) => seen.push(error));
        assert.equal(await listened.get("k"), undefined);
        assert.equal(await listened.set("k", "v"), false);
        assert.equal(seen.length, 2); // keyv 6 emits get() failures too (keyv 5.6 did not)
        assert.ok(seen.every(isOutage), `expected connection failures, got ${seen.map(String).join(", ")}`);

        const unlistened = new Keyv(store);
        await assert.rejects(() => unlistened.get("k"), isOutage);
        await assert.rejects(() => unlistened.set("k", "v"), isOutage);
      } finally {
        await store.disconnect();
      }
    });
  });

  it("disconnect() through Keyv closes the underlying client", async () => {
    const node = await startMockNode();
    try {
      const store = await nanocachedKeyvStore({ addresses: [{ host: "127.0.0.1", port: node.port }] });
      const keyv = new Keyv(store);
      await keyv.set("k", "v");
      assert.equal(store.client.isClosed(), false);
      await keyv.disconnect();
      assert.equal(store.client.isClosed(), true);
    } finally {
      await node.close();
    }
  });
});
