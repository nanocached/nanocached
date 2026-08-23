"""Namespace isolation (issue #105/#106): two Django cache aliases backed
by the same node but different OPTIONS.NAMESPACE never see each other's
keys, and clearing one never touches the other — clear() is that
namespace's CLEAR, not a whole-store flush."""

from __future__ import annotations

import unittest

from support import ISOLATION_NODE
from django.core.cache import caches


class NamespaceIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache_a = caches["isolation_a"]  # NAMESPACE="alias-a"
        self.cache_b = caches["isolation_b"]  # NAMESPACE="alias-b", same node
        self.cache_a.clear()
        self.cache_b.clear()

    def test_same_key_in_two_namespaces_does_not_collide(self) -> None:
        self.cache_a.set("shared-key", "value-from-a")
        self.cache_b.set("shared-key", "value-from-b")
        self.assertEqual(self.cache_a.get("shared-key"), "value-from-a")
        self.assertEqual(self.cache_b.get("shared-key"), "value-from-b")

    def test_clear_on_one_alias_leaves_the_other_intact(self) -> None:
        self.cache_a.set("only-in-a", "value")
        self.cache_b.set("only-in-b", "value")

        clear_count_before = ISOLATION_NODE.clear_count
        self.cache_a.clear()

        # Exactly one `c` frame reached the node for this clear() call.
        self.assertEqual(ISOLATION_NODE.clear_count, clear_count_before + 1)
        self.assertIsNone(self.cache_a.get("only-in-a"))
        self.assertEqual(self.cache_b.get("only-in-b"), "value")

    def test_wire_entries_are_keyed_by_namespace(self) -> None:
        self.cache_a.set("k", "v")
        wire_key = self.cache_a.make_key("k", version=self.cache_a.version).encode()
        self.assertIn((b"alias-a", wire_key), ISOLATION_NODE.ns_store)
        self.assertNotIn((b"alias-b", wire_key), ISOLATION_NODE.ns_store)


if __name__ == "__main__":
    unittest.main()
