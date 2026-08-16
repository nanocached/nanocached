import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { fnv1a, HashRing } from "../src/hashRing.js";

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

describe("HashRing", () => {
  // Real node names are per-process random UUIDs (doc/adr/0009-*.md), and the
  // ring's spread depends on that: FNV-1a places near-identical short strings
  // ("node-a#0", "node-b#0", …) in one narrow band of the hash space, which
  // collapses the distribution. UUID-shaped names here match production.
  const nodes = [
    "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
    "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
    "c3a1e8f2-9d4b-4a67-b520-7f1e6d3c8a94",
  ];

  it("routes every key to a member of the ring", () => {
    const ring = new HashRing(nodes);
    for (let i = 0; i < 200; i++) {
      assert.ok(nodes.includes(ring.route(Buffer.from(`key-${i}`))));
    }
  });

  it("routes deterministically", () => {
    const ring = new HashRing(nodes);
    for (let i = 0; i < 50; i++) {
      const key = Buffer.from(`key-${i}`);
      assert.equal(ring.route(key), ring.route(key));
    }
  });

  it("routes independently of the constructor's node order", () => {
    const ring = new HashRing(nodes);
    const shuffled = new HashRing([nodes[2], nodes[0], nodes[1]]);
    for (let i = 0; i < 200; i++) {
      const key = Buffer.from(`key-${i}`);
      assert.equal(ring.route(key), shuffled.route(key));
    }
  });

  it("routes everything to the only node of a one-node ring", () => {
    const ring = new HashRing(["only"]);
    for (let i = 0; i < 20; i++) {
      assert.equal(ring.route(Buffer.from(`key-${i}`)), "only");
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

  it("spreads keys across all nodes", () => {
    const ring = new HashRing(nodes);
    const counts = new Map<string, number>(nodes.map((node) => [node, 0]));

    const total = 3000;
    for (let i = 0; i < total; i++) {
      const owner = ring.route(Buffer.from(`key-${i}`));
      counts.set(owner, (counts.get(owner) ?? 0) + 1);
    }

    // 128 virtual nodes per node keeps the split near even; a node falling
    // below ~1/3 of its fair share would mean the ring construction broke.
    for (const [node, count] of counts) {
      assert.ok(count > total / nodes.length / 3, `${node} only received ${count} of ${total} keys`);
    }
  });
});
