"""Framework-level coverage driven through django.core.cache.caches, per
the issue #108 spec's #1 lesson from #107: the deliverable is "the
framework's idioms work", not just this backend's own methods called
directly. Every test here goes through the standard Django cache API."""

from __future__ import annotations

import unittest

from support import ROUNDTRIP_NODE
from django.core.cache import caches


class _Point:
    """Module-level (not a local class) because pickle needs to find it
    by its qualified name — a per-test local class isn't picklable, and
    values here go through the real pickle.dumps/loads round trip."""

    def __init__(self, x: int, y: int) -> None:
        self.x, self.y = x, y

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Point) and (self.x, self.y) == (other.x, other.y)


class RoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = caches["default"]
        self.cache.clear()

    def test_round_trip_string(self) -> None:
        self.cache.set("greeting", "hello")
        self.assertEqual(self.cache.get("greeting"), "hello")

    def test_round_trip_non_string_value(self) -> None:
        # Pickled, Django convention — a dict (and, separately, a
        # model-less plain object) must round-trip unchanged.
        payload = {"user": "ada", "roles": ["admin", "staff"], "count": 3}
        self.cache.set("payload", payload)
        self.assertEqual(self.cache.get("payload"), payload)

        self.cache.set("point", _Point(1, 2))
        self.assertEqual(self.cache.get("point"), _Point(1, 2))

    def test_missing_key_returns_default(self) -> None:
        self.assertIsNone(self.cache.get("does-not-exist"))
        self.assertEqual(self.cache.get("does-not-exist", "fallback"), "fallback")

    def test_none_value_is_not_confused_with_a_miss(self) -> None:
        # pickle.dumps(None) is valid, non-empty bytes — a cached None
        # must come back as None, not as the "missing" default.
        self.cache.set("nullable", None)
        self.assertIsNone(self.cache.get("nullable", "fallback-should-not-appear"))
        self.assertTrue(self.cache.has_key("nullable"))

    def test_add_semantics(self) -> None:
        self.assertTrue(self.cache.add("fresh", "first"))
        self.assertEqual(self.cache.get("fresh"), "first")
        # Already present: add() is a no-op and reports False.
        self.assertFalse(self.cache.add("fresh", "second"))
        self.assertEqual(self.cache.get("fresh"), "first")

    def test_delete_and_has_key(self) -> None:
        self.cache.set("temp", "value")
        self.assertTrue(self.cache.has_key("temp"))
        self.assertTrue(self.cache.delete("temp"))
        self.assertFalse(self.cache.has_key("temp"))
        # Deleting an already-absent key returns False, not an error.
        self.assertFalse(self.cache.delete("temp"))

    def test_touch(self) -> None:
        self.cache.set("ttl-key", "value", timeout=300)
        self.assertTrue(self.cache.touch("ttl-key", timeout=60))
        self.assertEqual(self.cache.get("ttl-key"), "value")
        self.assertFalse(self.cache.touch("missing-key", timeout=60))

    def test_get_or_set(self) -> None:
        self.assertEqual(self.cache.get_or_set("computed", "default-value"), "default-value")
        self.assertEqual(self.cache.get("computed"), "default-value")
        # Already set: get_or_set() must not overwrite it.
        self.assertEqual(self.cache.get_or_set("computed", "different-value"), "default-value")

    def test_get_or_set_with_callable_default(self) -> None:
        calls = []

        def compute() -> str:
            calls.append(1)
            return "computed-once"

        self.assertEqual(self.cache.get_or_set("lazy", compute), "computed-once")
        self.assertEqual(self.cache.get_or_set("lazy", compute), "computed-once")
        self.assertEqual(len(calls), 1)

    def test_get_many_set_many_delete_many(self) -> None:
        failed = self.cache.set_many({"a": 1, "b": 2, "c": 3})
        self.assertEqual(failed, [])
        self.assertEqual(self.cache.get_many(["a", "b", "c", "missing"]), {"a": 1, "b": 2, "c": 3})

        self.cache.delete_many(["a", "b"])
        self.assertEqual(self.cache.get_many(["a", "b", "c"]), {"c": 3})

    def test_incr_and_decr_are_refused(self) -> None:
        self.cache.set("counter", 1)
        with self.assertRaises(NotImplementedError):
            self.cache.incr("counter")
        with self.assertRaises(NotImplementedError):
            self.cache.decr("counter")

    def test_wire_key_matches_pickled_value(self) -> None:
        # OPTIONS.NAMESPACE ("django", the default here) is what actually
        # isolates this alias's keys from any other namespace sharing the
        # node — assert the mock's ns_store directly, keyed the way the
        # namespaced `s`/`g` frames address it.
        import pickle

        self.cache.set("wired", "on-the-wire")
        wire_key = self.cache.make_key("wired", version=self.cache.version)
        stored = ROUNDTRIP_NODE.ns_store.get((b"django", wire_key.encode()))
        self.assertIsNotNone(stored)
        self.assertEqual(pickle.loads(stored), "on-the-wire")


if __name__ == "__main__":
    unittest.main()
