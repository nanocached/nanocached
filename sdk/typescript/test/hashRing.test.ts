import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { fmix64, fnv1a, HashRing } from "../src/hashRing.js";

describe("fnv1a", () => {
  // Published FNV-1a 64-bit test vectors — these pin the hash to the exact
  // function the Rust side (and every other client) uses, not just to
  // whatever this file's implementation happens to compute.
  it("matches the published 64-bit FNV-1a test vectors", () => {
    assert.equal(fnv1a(Buffer.alloc(0)), 0xcbf29ce484222325n);
    assert.equal(fnv1a(Buffer.from("a")), 0xaf63dc4c8601ec8cn);
    assert.equal(fnv1a(Buffer.from("foobar")), 0x85944171f73967e8n);
  });

  it("wraps multiplication to 64 bits", () => {
    const hash = fnv1a(Buffer.from("some longer input that overflows many times"));
    assert.ok(hash >= 0n && hash < 1n << 64n);
  });
});

describe("cross-language score vectors", () => {
  // Pinned outputs of the full ADR-0011 score pipeline
  // (fmix64(fnv1a(name) ^ fnv1a(key))) — src/hash_ring.rs asserts these
  // exact values, so the two implementations agree byte-for-byte or one
  // side's test fails.
  it("matches the Rust implementation exactly", () => {
    assert.equal(fmix64(0n), 0n);
    assert.equal(fmix64(1n), 0xb456bcfc34c2cb2cn);
    assert.equal(fmix64(0xcbf29ce484222325n), 0xefd01f60ba992926n);

    const ring = new HashRing(["node-a", "node-b", "node-c"]);
    assert.deepEqual(ring.owners(Buffer.from("alpha"), 3), ["node-c", "node-b", "node-a"]);
    assert.deepEqual(ring.owners(Buffer.from("beta"), 3), ["node-a", "node-c", "node-b"]);
    assert.deepEqual(ring.owners(Buffer.alloc(0), 3), ["node-a", "node-b", "node-c"]);
  });
});

describe("HashRing", () => {
  const nodes = ["node-a", "node-b", "node-c"];

  it("routes every key to a member of the ring", () => {
    const ring = new HashRing(nodes);
    for (let i = 0; i < 200; i++) {
      assert.ok(nodes.includes(ring.route(Buffer.from(`key-${i}`))));
    }
  });

  it("throws instead of silently returning undefined when routing on an empty ring", () => {
    // Regression (issue #47 audit item 6): owners(key, 1)[0] used to
    // return undefined uncaught.
    const ring = new HashRing([]);
    assert.throws(() => ring.route(Buffer.from("k")), RangeError);
  });

  it("routes deterministically", () => {
    const ring = new HashRing(nodes);
    for (let i = 0; i < 50; i++) {
      const key = Buffer.from(`key-${i}`);
      assert.equal(ring.route(key), ring.route(key));
    }
  });

  it("ranks independently of the constructor's node order", () => {
    const ring = new HashRing(nodes);
    const shuffled = new HashRing([nodes[2], nodes[0], nodes[1]]);
    for (let i = 0; i < 200; i++) {
      const key = Buffer.from(`key-${i}`);
      assert.deepEqual(ring.owners(key, 3), shuffled.owners(key, 3));
    }
  });

  it("returns distinct owners, capped by the cluster size", () => {
    const ring = new HashRing(nodes);
    const owners = ring.owners(Buffer.from("some-key"), 2);
    assert.equal(owners.length, 2);
    assert.notEqual(owners[0], owners[1]);
    assert.equal(ring.owners(Buffer.from("some-key"), 10).length, 3);
  });

  it("routes everything to the only node of a one-node ring", () => {
    const ring = new HashRing(["only"]);
    for (let i = 0; i < 20; i++) {
      assert.equal(ring.route(Buffer.from(`key-${i}`)), "only");
    }
  });

  it("never reorders existing nodes when a node is added", () => {
    // The HRW property ADR-0011's handoff and replica-cleanup rules rest
    // on: an added node only inserts into a key's ranking.
    const before = new HashRing(nodes);
    const after = new HashRing([...nodes, "node-d"]);

    for (let i = 0; i < 500; i++) {
      const key = Buffer.from(`key-${i}`);
      const oldOrder = before.owners(key, 3);
      const newOrder = after.owners(key, 4).filter((node) => node !== "node-d");
      assert.deepEqual(newOrder, oldOrder);
    }
  });

  it("only remaps keys owned by a removed node", () => {
    const before = new HashRing(nodes);
    const after = new HashRing([nodes[0], nodes[1]]);

    for (let i = 0; i < 500; i++) {
      const key = Buffer.from(`key-${i}`);
      const owner = before.route(key);
      if (owner !== nodes[2]) {
        assert.equal(after.route(key), owner);
      }
    }
  });

  it("spreads keys evenly across all nodes", () => {
    const ring = new HashRing(nodes);
    const counts = new Map<string, number>(nodes.map((node) => [node, 0]));

    const total = 3000;
    for (let i = 0; i < total; i++) {
      const owner = ring.route(Buffer.from(`key-${i}`));
      counts.set(owner, (counts.get(owner) ?? 0) + 1);
    }

    // HRW measures within ~2% of fair; 15% is a loose regression bound
    // that the old banded ring (up to 95% off) would fail immediately.
    const fair = total / nodes.length;
    for (const [node, count] of counts) {
      assert.ok(Math.abs(count - fair) / fair < 0.15, `${node} received ${count} of ${total} keys`);
    }
  });

  it("spreads each replica rank evenly too", () => {
    const ring = new HashRing(nodes);
    const secondCounts = new Map<string, number>(nodes.map((node) => [node, 0]));

    const total = 3000;
    for (let i = 0; i < total; i++) {
      const [, second] = ring.owners(Buffer.from(`key-${i}`), 2);
      secondCounts.set(second, (secondCounts.get(second) ?? 0) + 1);
    }

    const fair = total / nodes.length;
    for (const [node, count] of secondCounts) {
      assert.ok(Math.abs(count - fair) / fair < 0.15, `${node} is 2nd replica for ${count} of ${total}`);
    }
  });
});
