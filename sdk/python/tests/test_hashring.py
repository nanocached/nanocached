import unittest

from nanocached._hashring import HashRing, fmix64, fnv1a


class Fnv1aTests(unittest.TestCase):
    def test_matches_published_vectors(self):
        # Published FNV-1a 64-bit test vectors.
        self.assertEqual(fnv1a(b""), 0xCBF29CE484222325)
        self.assertEqual(fnv1a(b"a"), 0xAF63DC4C8601EC8C)
        self.assertEqual(fnv1a(b"foobar"), 0x85944171F73967E8)


class CrossLanguageVectorTests(unittest.TestCase):
    def test_matches_rust_and_typescript_exactly(self):
        # Pinned outputs of the full client-side replication score pipeline — the Rust and
        # TypeScript implementations assert these exact values too.
        self.assertEqual(fmix64(0), 0)
        self.assertEqual(fmix64(1), 0xB456BCFC34C2CB2C)
        self.assertEqual(fmix64(0xCBF29CE484222325), 0xEFD01F60BA992926)

        ring = HashRing(["node-a", "node-b", "node-c"])
        self.assertEqual(ring.owners(b"alpha", 3), ["node-c", "node-b", "node-a"])
        self.assertEqual(ring.owners(b"beta", 3), ["node-a", "node-c", "node-b"])
        self.assertEqual(ring.owners(b"", 3), ["node-a", "node-b", "node-c"])


class HashRingTests(unittest.TestCase):
    NODES = ["node-a", "node-b", "node-c"]

    def test_owners_are_distinct_and_capped(self):
        ring = HashRing(self.NODES)
        owners = ring.owners(b"some-key", 2)
        self.assertEqual(len(owners), 2)
        self.assertNotEqual(owners[0], owners[1])
        self.assertEqual(len(ring.owners(b"some-key", 10)), 3)

    def test_ranking_is_order_independent(self):
        ring = HashRing(self.NODES)
        shuffled = HashRing([self.NODES[2], self.NODES[0], self.NODES[1]])
        for i in range(200):
            key = f"key-{i}".encode()
            self.assertEqual(ring.owners(key, 3), shuffled.owners(key, 3))

    def test_adding_a_node_never_reorders_existing_nodes(self):
        before = HashRing(self.NODES)
        after = HashRing(self.NODES + ["node-d"])
        for i in range(500):
            key = f"key-{i}".encode()
            new_order = [n for n in after.owners(key, 4) if n != "node-d"]
            self.assertEqual(new_order, before.owners(key, 3))

    def test_spreads_keys_evenly(self):
        ring = HashRing(self.NODES)
        counts = {node: 0 for node in self.NODES}
        total = 3000
        for i in range(total):
            counts[ring.route(f"key-{i}".encode())] += 1
        fair = total / len(self.NODES)
        for node, count in counts.items():
            self.assertLess(abs(count - fair) / fair, 0.15, f"{node} got {count}/{total}")

    def test_routing_on_an_empty_ring_raises_value_error(self):
        # Regression (issue #47 audit item 6): owners(key, 1) on an empty
        # ring returns [], and [0] on that used to raise a bare
        # IndexError — not one of this SDK's own error types.
        ring = HashRing([])
        with self.assertRaises(ValueError):
            ring.route(b"k")


class NamespaceCrossLanguageVectorTests(unittest.TestCase):
    # Namespaces (issue #105): pinned outputs of key_hash(namespace, key)
    # and the resulting top-3 owners over node-a/node-b/node-c — the Rust
    # server (src/hash_ring.rs) and every other SDK assert these exact
    # same vectors, next to the pre-namespace alpha/beta/"" vectors above.
    NODES = ["node-a", "node-b", "node-c"]

    def test_matches_the_cross_language_namespace_vectors(self):
        from nanocached._hashring import key_hash

        self.assertEqual(key_hash(b"users", b"alpha"), 0xFD4AB55027C21DF6)
        # Hash-only vector: the wire itself rejects an empty key, but the
        # ring's own hash pipeline has no such restriction.
        self.assertEqual(key_hash(b"users", b""), 0xA9E9BBCA44BB502E)
        self.assertEqual(key_hash(b"\xff\x00", b"beta"), 0x8F7C097ECCB8E792)

        ring = HashRing(self.NODES)
        self.assertEqual(
            ring.owners(b"alpha", 3, namespace=b"users"), ["node-a", "node-c", "node-b"]
        )
        self.assertEqual(
            ring.owners(b"", 3, namespace=b"users"), ["node-b", "node-c", "node-a"]
        )
        self.assertEqual(
            ring.owners(b"beta", 3, namespace=b"\xff\x00"), ["node-b", "node-a", "node-c"]
        )

    def test_default_namespace_matches_the_legacy_unnamespaced_form(self):
        # Rolling-upgrade invariant (issue #105 spec): an un-namespaced
        # key's placement must not move when namespaces are introduced —
        # empty ns == the legacy form, byte-for-byte.
        ring = HashRing(self.NODES)
        self.assertEqual(ring.owners(b"alpha", 3, namespace=b""), ring.owners(b"alpha", 3))
        self.assertEqual(ring.owners(b"alpha", 3), ["node-c", "node-b", "node-a"])

    def test_namespace_and_key_boundaries_are_unambiguous(self):
        # A delimiter-free split: the length prefix keeps ("ab", "c") and
        # ("a", "bc") apart, and a namespaced key never collides with the
        # un-namespaced concatenation.
        from nanocached._hashring import key_hash

        self.assertNotEqual(key_hash(b"ab", b"c"), key_hash(b"a", b"bc"))
        self.assertNotEqual(key_hash(b"ab", b"c"), key_hash(b"", b"abc"))


if __name__ == "__main__":
    unittest.main()
