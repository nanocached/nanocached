"""Framework-level coverage driven through django.core.cache.caches, per
the issue #108 spec's #1 lesson from #107: the deliverable is "the
framework's idioms work", not just this backend's own methods called
directly. Every test here goes through the standard Django cache API."""

from __future__ import annotations

import asyncio
import enum
import time
import unittest

from support import ROUNDTRIP_NODE
from django.core.cache import caches


class _Status(enum.IntEnum):
    """Module-level for pickle, like _Point below — stands in for
    Django's own models.IntegerChoices pattern (issue #392)."""

    ACTIVE = 1
    DISABLED = 2


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

    def test_int_subclasses_keep_their_type(self) -> None:
        # issue #392: IntEnum/IntegerChoices used to match the counter
        # fast path (isinstance(value, int)), round-tripping as a bare
        # int and silently losing enum identity. Subclasses must take
        # the pickle path; exact ints keep the counter encoding.
        self.cache.set("status", _Status.ACTIVE)
        got = self.cache.get("status")
        self.assertIsInstance(got, _Status)
        self.assertIs(got, _Status.ACTIVE)

        self.cache.set("plain", 1)
        self.assertIs(type(self.cache.get("plain")), int)

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

    def test_delete_many_fans_out_concurrently(self) -> None:
        # Regression for issue #233: delete_many used to call delete()
        # once per key, each one blocking the calling thread on its own
        # separate round trip before the next key's delete even started.
        # It's now fanned out concurrently (asyncio.gather, in one _run
        # round trip) instead — patch the namespace handle's delete()
        # with an artificial per-call delay and check the wall time is
        # close to ONE delay, not N of them stacked sequentially.
        keys = [f"dm-{i}" for i in range(5)]
        for key in keys:
            self.cache.set(key, "v")

        # set() above already forced a connect, so _namespace_handle is
        # populated by now.
        handle = self.cache._namespace_handle
        original_delete = handle.delete
        delay_seconds = 0.1

        async def delayed_delete(key):
            await asyncio.sleep(delay_seconds)
            return await original_delete(key)

        handle.delete = delayed_delete
        try:
            start = time.monotonic()
            self.cache.delete_many(keys)
            elapsed = time.monotonic() - start
        finally:
            handle.delete = original_delete

        self.assertLess(
            elapsed,
            delay_seconds * len(keys),
            "delete_many should fan its deletes out concurrently, not run them one at a time",
        )
        self.assertEqual(self.cache.get_many(keys), {})

    def test_get_many_skips_a_corrupt_entry_instead_of_failing_the_batch(self) -> None:
        # Regression for issue #332: _decode_value's pickle.loads raising on
        # one corrupt/incompatible entry used to abort get_many's whole dict
        # comprehension, losing every other key in the batch along with it.
        # Write the "bad" key's bytes directly into the mock node's store
        # (bypassing set()/_encode_value, which would never produce this) to
        # simulate corruption/incompatibility on the wire, the same way
        # test_wire_key_matches_pickled_value reaches into ns_store directly.
        self.cache.set("good", "fine")
        wire_key = self.cache.make_key("bad", version=self.cache.version)
        ROUNDTRIP_NODE.ns_store[(b"django", wire_key.encode())] = b"\xffnot a valid pickle stream"

        result = self.cache.get_many(["good", "bad", "missing"])

        self.assertEqual(result, {"good": "fine"})

        # Only get_many degrades per-key — a direct get() on the same
        # corrupt key still raises/propagates exactly as it did before.
        with self.assertRaises(Exception):
            self.cache.get("bad")

    def test_delete_many_waits_for_every_leg_then_raises_on_a_failure(self) -> None:
        # Regression for issue #332: _delete_all used plain asyncio.gather,
        # which re-raises the first exception as soon as it's seen instead
        # of waiting for every concurrently-dispatched delete() to finish —
        # leaving sibling legs' outcomes unobserved. Patch one key's
        # delete() to fail and assert (a) it still raises, surfacing the
        # failure to the caller, and (b) every other key was still deleted
        # (proving the fan-out ran all legs to completion, not abandoned
        # mid-flight).
        keys = [f"dm-fail-{i}" for i in range(5)]
        for key in keys:
            self.cache.set(key, "v")

        handle = self.cache._namespace_handle
        original_delete = handle.delete
        failing_key = self.cache.make_key(keys[2], version=self.cache.version)

        async def flaky_delete(cache_key):
            if cache_key == failing_key:
                raise RuntimeError("simulated delete failure")
            return await original_delete(cache_key)

        handle.delete = flaky_delete
        try:
            with self.assertRaises(RuntimeError):
                self.cache.delete_many(keys)
        finally:
            handle.delete = original_delete

        # Every other leg still completed despite the one failure.
        surviving = [key for key in keys if key != keys[2]]
        self.assertEqual(self.cache.get_many(surviving), {})

    def test_incr_and_decr(self) -> None:
        self.cache.set("counter", 10)
        self.assertEqual(self.cache.incr("counter"), 11)
        self.assertEqual(self.cache.incr("counter", 4), 15)
        self.assertEqual(self.cache.decr("counter"), 14)
        self.assertEqual(self.cache.decr("counter", 4), 10)
        self.assertEqual(self.cache.get("counter"), 10)

    def test_incr_on_a_missing_key_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            self.cache.incr("does-not-exist")
        with self.assertRaises(ValueError):
            self.cache.decr("does-not-exist")

    def test_incr_on_a_non_numeric_value_raises_not_numeric_error(self) -> None:
        from nanocached import NotNumericError

        self.cache.set("greeting", "hello")
        with self.assertRaises(NotNumericError):
            self.cache.incr("greeting")

    def test_int_values_round_trip_without_pickling(self) -> None:
        # Issue #129: a plain int is stored as INCR's own decimal-ASCII
        # grammar, not pickled — bool is excluded (it's not a counter).
        self.cache.set("count", 42)
        self.assertEqual(self.cache.get("count"), 42)
        self.assertIsInstance(self.cache.get("count"), int)

        self.cache.set("flag", True)
        self.assertIs(self.cache.get("flag"), True)

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
