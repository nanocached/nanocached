import assert from "node:assert/strict";
import { test } from "node:test";
import { HashRing, fnv1a } from "../src/hashRing.js";

// Expected values below were captured by running the exact same algorithm
// (copied verbatim) as a standalone Rust program, to empirically confirm
// this port matches src/bin/bench.rs bit-for-bit rather than merely being
// "a" plausible translation of it. See the description on HashRing for why
// that specific match (not just "some" consistent hash) matters: nodes are
// independent, so routing disagreement between clients means they silently
// stop seeing each other's data for a given key.

test("fnv1a matches Rust's implementation for known inputs", () => {
  assert.equal(fnv1a(Buffer.from("")), 14695981039346656037n);
  assert.equal(fnv1a(Buffer.from("hello")), 11831194018420276491n);
  assert.equal(fnv1a(Buffer.from("127.0.0.1:8356#0")), 15664520825264499745n);
  assert.equal(fnv1a(Buffer.from("127.0.0.1:8356#1")), 15664519725752871534n);
  assert.equal(fnv1a(Buffer.from("127.0.0.1:8356#127")), 10551945309340677689n);
  assert.equal(fnv1a(Buffer.from("127.0.0.1:8357#0")), 16229343247058481278n);
  assert.equal(fnv1a(Buffer.from("nanocached:12345")), 5257553620434289232n);
});

test("HashRing.route matches Rust's HashRing for the same node list and keys", () => {
  const ring = new HashRing(["127.0.0.1:8356", "127.0.0.1:8357", "127.0.0.1:8358"]);

  assert.equal(ring.route(Buffer.from("name")), "127.0.0.1:8356");
  assert.equal(ring.route(Buffer.from("session:123")), "127.0.0.1:8358");
  assert.equal(ring.route(Buffer.from("user:42")), "127.0.0.1:8357");
  assert.equal(ring.route(Buffer.from("a")), "127.0.0.1:8356");
  assert.equal(ring.route(Buffer.from("nanocached:0")), "127.0.0.1:8358");
  assert.equal(ring.route(Buffer.from("nanocached:1")), "127.0.0.1:8358");
  assert.equal(ring.route(Buffer.from("nanocached:9999")), "127.0.0.1:8356");
});

test("HashRing.route is deterministic for a fixed ring", () => {
  const ring = new HashRing(["127.0.0.1:8356", "127.0.0.1:8357"]);
  const first = ring.route(Buffer.from("some-key"));
  for (let i = 0; i < 20; i++) {
    assert.equal(ring.route(Buffer.from("some-key")), first);
  }
});

test("HashRing.route only ever returns a node from the given list", () => {
  const nodes = ["127.0.0.1:8356", "127.0.0.1:8357", "127.0.0.1:8358", "127.0.0.1:8359"];
  const ring = new HashRing(nodes);

  for (let i = 0; i < 200; i++) {
    const node = ring.route(Buffer.from(`key-${i}`));
    assert.ok(nodes.includes(node), `${node} is not one of ${nodes.join(", ")}`);
  }
});
