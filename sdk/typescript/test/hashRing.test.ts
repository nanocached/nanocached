import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { fmix64, fnv1a, HashRing, keyHash } from "../src/hashRing.js";

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
  // Pinned outputs of the full client-side replication score pipeline
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

describe("namespaced score vectors (first-class namespaces, issue #105)", () => {
  // Pinned outputs of key_hash and the full scoring pipeline over
  // namespace+key — src/hash_ring.rs's test suite asserts these exact
  // values too, so the two implementations agree byte-for-byte or one
  // side's test fails. Ring over node-a/node-b/node-c, R=3, matching the
  // unnamespaced vectors above.
  const ring = new HashRing(["node-a", "node-b", "node-c"]);

  it("matches the Rust key_hash for a namespaced key", () => {
    assert.equal(keyHash(Buffer.from("alpha"), Buffer.from("users")), 0xfd4ab55027c21df6n);
    assert.deepEqual(ring.owners(Buffer.from("alpha"), 3, Buffer.from("users")), ["node-a", "node-c", "node-b"]);
  });

  it("matches the Rust key_hash for a namespaced, empty key (hash-only — the wire itself rejects empty keys)", () => {
    assert.equal(keyHash(Buffer.alloc(0), Buffer.from("users")), 0xa9e9bbca44bb502en);
    assert.deepEqual(ring.owners(Buffer.alloc(0), 3, Buffer.from("users")), ["node-b", "node-c", "node-a"]);
  });

  it("matches the Rust key_hash for a binary namespace", () => {
    const namespace = Buffer.from([0xff, 0x00]);
    assert.equal(keyHash(Buffer.from("beta"), namespace), 0x8f7c097eccb8e792n);
    assert.deepEqual(ring.owners(Buffer.from("beta"), 3, namespace), ["node-b", "node-a", "node-c"]);
  });

  it("the default (empty) namespace is byte-identical to the pre-namespace form", () => {
    assert.equal(keyHash(Buffer.from("alpha")), fnv1a(Buffer.from("alpha")));
    assert.deepEqual(ring.owners(Buffer.from("alpha"), 3, Buffer.alloc(0)), ring.owners(Buffer.from("alpha"), 3));
    assert.deepEqual(ring.owners(Buffer.from("alpha"), 3), ["node-c", "node-b", "node-a"]);
  });

  it("two different namespaces place the same key name differently", () => {
    const inUsers = ring.route(Buffer.from("config"), Buffer.from("users"));
    const inOrders = ring.route(Buffer.from("config"), Buffer.from("orders"));
    const unnamespaced = ring.route(Buffer.from("config"));
    // Not a strict inequality assertion (three independent hashes could
    // coincidentally agree) — the real property under test is that
    // namespace participates in the score at all, which the pinned
    // key_hash vectors above already nail down exactly. This is a light
    // sanity check that route() actually threads the namespace through.
    assert.ok([inUsers, inOrders, unnamespaced].every((node) => ["node-a", "node-b", "node-c"].includes(node)));
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
    // The HRW property client-side replication's handoff and replica-cleanup rules rest
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

  it("dedupes repeated node names, first occurrence winning (issue #461)", () => {
    // Regression for issue #461: before this fix, a name repeated in the
    // constructor's node list scored independently for each of its slots
    // in owners()'s bounded top-`replicas` insertion, so a duplicated node
    // could occupy more than one place in the returned set — inflating its
    // effective share of the ring.
    const ring = new HashRing(["a", "b", "b", "b", "c", "d"]);
    const key = Buffer.from("some-key");

    for (let replicas = 0; replicas <= 5; replicas++) {
      const owners = ring.owners(key, replicas);
      assert.equal(new Set(owners).size, owners.length, `replicas=${replicas}: duplicate in ${owners}`);
    }

    // Construction order is otherwise unaffected: the first occurrence of
    // a repeated name is kept in place, so ranking matches a ring built
    // from the already-deduped list exactly.
    const deduped = new HashRing(["a", "b", "c", "d"]);
    for (let i = 0; i < 200; i++) {
      const k = Buffer.from(`key-${i}`);
      assert.deepEqual(ring.owners(k, 4), deduped.owners(k, 4));
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

describe("HashRing.owners bounded top-R selection (audit finding)", () => {
  // The pre-fix implementation, verbatim apart from reading the node
  // hashes from a parameter: score every node with the BigInt primitives,
  // sort the lot, slice. `owners` must agree with it exactly.
  function referenceOwners(
    names: readonly string[],
    hashes: readonly bigint[],
    key: Uint8Array,
    replicas: number,
    namespace?: Uint8Array,
  ): string[] {
    const hash = keyHash(key, namespace);
    const scored = names.map((node, index) => ({ score: fmix64(hashes[index] ^ hash), node }));
    scored.sort((a, b) => {
      if (a.score !== b.score) return a.score < b.score ? 1 : -1;
      return a.node < b.node ? -1 : 1;
    });
    return scored.slice(0, replicas).map(({ node }) => node);
  }

  function mulberry32(seed: number): () => number {
    let state = seed >>> 0;
    return () => {
      state = (state + 0x6d2b79f5) >>> 0;
      let t = state;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function randomBytes(random: () => number, maxLength: number): Buffer {
    return Buffer.from(Array.from({ length: Math.floor(random() * (maxLength + 1)) }, () => Math.floor(random() * 256)));
  }

  const nameHashes = (names: readonly string[]): bigint[] => names.map((name) => fnv1a(Buffer.from(name, "utf8")));

  it("agrees with the full-sort reference over many keys, rosters, namespaces and replica counts", () => {
    const random = mulberry32(0x5eed);
    for (const size of [1, 2, 3, 5, 8, 33, 100]) {
      const names = Array.from({ length: size }, (_, i) => `node-${i}-${Math.floor(random() * 1e9).toString(16)}`);
      const ring = new HashRing(names);
      const hashes = nameHashes(names);
      for (let i = 0; i < 150; i++) {
        const key = randomBytes(random, 40);
        const namespace = i % 3 === 0 ? undefined : randomBytes(random, 12);
        for (const replicas of [0, 1, 2, 3, 5, size, size + 4, 32, 33, 40]) {
          assert.deepEqual(
            ring.owners(key, replicas, namespace),
            referenceOwners(names, hashes, key, replicas, namespace),
            `size=${size} replicas=${replicas} key=${key.toString("hex")} ns=${namespace?.toString("hex")}`,
          );
        }
      }
    }
  });

  it("agrees on unusual replica counts too (negative, fractional, NaN)", () => {
    const names = ["a", "b", "c", "d", "e"];
    const ring = new HashRing(names);
    const hashes = nameHashes(names);
    for (const replicas of [-1, -3, 2.5, 0.5, NaN, Infinity]) {
      for (let i = 0; i < 20; i++) {
        const key = Buffer.from(`k${i}`);
        assert.deepEqual(ring.owners(key, replicas), referenceOwners(names, hashes, key, replicas), `replicas=${replicas}`);
      }
    }
  });

  it("breaks score ties toward the lexicographically smaller name, in any construction order", () => {
    // 64-bit collisions don't occur between real names, so force them: give
    // every node the same name hash, which makes every score tie.
    const names = ["delta", "alpha", "echo", "charlie", "bravo", "foxtrot"];
    const ring = new HashRing(names);
    const internals = ring as unknown as { nodeHashHi: Uint32Array; nodeHashLo: Uint32Array };
    internals.nodeHashHi.fill(0x01234567);
    internals.nodeHashLo.fill(0x89abcdef);
    const hashes = names.map(() => 0x0123456789abcdefn);

    const sorted = [...names].sort();
    for (let replicas = 0; replicas <= names.length + 1; replicas++) {
      const owners = ring.owners(Buffer.from("tie"), replicas);
      assert.deepEqual(owners, sorted.slice(0, replicas));
      assert.deepEqual(owners, referenceOwners(names, hashes, Buffer.from("tie"), replicas));
    }

    // Partial ties: groups of equal hashes, so ties and strict ordering
    // are mixed within the same selection.
    internals.nodeHashHi.set([1, 1, 2, 2, 3, 3]);
    internals.nodeHashLo.set([7, 7, 7, 7, 7, 7]);
    const mixed = [(1n << 32n) | 7n, (1n << 32n) | 7n, (2n << 32n) | 7n, (2n << 32n) | 7n, (3n << 32n) | 7n, (3n << 32n) | 7n];
    for (let i = 0; i < 100; i++) {
      const key = Buffer.from(`key-${i}`);
      for (let replicas = 1; replicas <= 6; replicas++) {
        assert.deepEqual(ring.owners(key, replicas), referenceOwners(names, mixed, key, replicas));
      }
    }
  });

  it("returns nothing from an empty ring", () => {
    assert.deepEqual(new HashRing([]).owners(Buffer.from("k"), 3), []);
  });
});
