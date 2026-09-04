"""Regression coverage for issue #439's policy on get_many/set_many:
retry a mid-batch partial failure's remainder exactly once, merging what
already resolved; propagate unchanged if that retry also fails. Mirrors
the parity fixes in adapters/cache-manager (mgetResolved/msetResolved,
PR #452) and adapters/jcache (getManyBytesResolvingWrongNode/
setManyBytesResolvingWrongNode, PR #451).

Every test drives the standard Django cache API (cache.get_many(...),
cache.set_many(...)), per the issue #108 spec's "test through the
framework's idioms" rule (see test_round_trip.py's own module doc) — the
partial failure itself is injected by monkeypatching the namespace
handle's get_many_bytes/set_many, the same technique
test_delete_many_fans_out_concurrently and
test_delete_many_waits_for_every_leg_then_raises_on_a_failure already use
for delete_many."""

from __future__ import annotations

import unittest

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from django.core.cache import caches

from nanocached import (
    PartialConnectionLostError,
    PartialSetConnectionLostError,
    PartialWrongNodeError,
    WrongNodeError,
)


class GetManyPartialFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = caches["default"]
        self.cache.clear()
        # A round trip through set_many forces a connect, so
        # _namespace_handle is populated by the time a test patches it.
        self.cache.set_many({"a": 1, "b": 2, "c": 3})
        self.handle = self.cache._namespace_handle
        self.original_get_many_bytes = self.handle.get_many_bytes
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        self.handle.get_many_bytes = self.original_get_many_bytes

    def test_wrong_node_partial_failure_is_retried_once_and_merged(self) -> None:
        # First call: genuinely fetch, then report the last key as still
        # wrong-node (a stale-routing-table hiccup) — exactly the shape
        # PartialWrongNodeError carries. Every call after that behaves
        # normally, so the retry _get_many_bytes_resolved makes for the
        # remainder succeeds and the batch's result is complete.
        original = self.original_get_many_bytes
        calls = []

        async def flaky(keys):
            calls.append(list(keys))
            if len(calls) == 1:
                real = await original(keys)
                missing = list(keys)[-1]
                partial = {k: v for k, v in real.items() if k != missing}
                raise PartialWrongNodeError(partial)
            return await original(keys)

        self.handle.get_many_bytes = flaky

        result = self.cache.get_many(["a", "b", "c"])

        self.assertEqual(result, {"a": 1, "b": 2, "c": 3})
        # One initial call (raises) plus exactly one retry — not looping.
        self.assertEqual(len(calls), 2)

    def test_wrong_node_partial_failure_propagates_on_a_second_failure(self) -> None:
        async def always_fails(keys):
            raise PartialWrongNodeError({})

        self.handle.get_many_bytes = always_fails

        with self.assertRaises(WrongNodeError):
            self.cache.get_many(["a", "b", "c"])

    def test_connection_lost_partial_failure_is_retried_once_and_merged(self) -> None:
        original = self.original_get_many_bytes
        calls = []

        async def flaky(keys):
            calls.append(list(keys))
            if len(calls) == 1:
                real = await original(keys)
                missing = list(keys)[-1]
                partial = {k: v for k, v in real.items() if k != missing}
                raise PartialConnectionLostError(partial, "simulated connection loss")
            return await original(keys)

        self.handle.get_many_bytes = flaky

        result = self.cache.get_many(["a", "b", "c"])

        self.assertEqual(result, {"a": 1, "b": 2, "c": 3})
        self.assertEqual(len(calls), 2)

    def test_connection_lost_partial_failure_propagates_on_a_second_failure(self) -> None:
        async def always_fails(keys):
            raise PartialConnectionLostError({}, "simulated connection loss")

        self.handle.get_many_bytes = always_fails

        with self.assertRaises(PartialConnectionLostError):
            self.cache.get_many(["a", "b", "c"])


class SetManyPartialFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = caches["default"]
        self.cache.clear()
        # Force a connect so _namespace_handle is populated before a test
        # patches it.
        self.cache.set("warm-up", "v")
        self.handle = self.cache._namespace_handle
        self.original_set_many = self.handle.set_many
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        self.handle.set_many = self.original_set_many

    def test_wrong_node_failure_resends_the_whole_batch_once(self) -> None:
        # set_many's WrongNodeError carries no partial payload (see the
        # SDK's own docstring), so the fix resends the WHOLE batch once —
        # safe, since every per-key write is idempotent.
        original = self.original_set_many
        calls = []

        async def flaky(payload, *, ttl_seconds=0):
            calls.append(dict(payload))
            if len(calls) == 1:
                raise WrongNodeError()
            return await original(payload, ttl_seconds=ttl_seconds)

        self.handle.set_many = flaky

        failed = self.cache.set_many({"x": 1, "y": 2, "z": 3})

        self.assertEqual(failed, [])
        self.assertEqual(len(calls), 2)
        # The retry resent every key, not just a remainder.
        self.assertEqual(set(calls[1]), set(calls[0]))
        self.assertEqual(self.cache.get_many(["x", "y", "z"]), {"x": 1, "y": 2, "z": 3})

    def test_wrong_node_failure_propagates_on_a_second_failure(self) -> None:
        async def always_fails(payload, *, ttl_seconds=0):
            raise WrongNodeError()

        self.handle.set_many = always_fails

        with self.assertRaises(WrongNodeError):
            self.cache.set_many({"x": 1, "y": 2})

    def test_connection_lost_partial_failure_retries_only_the_remainder(self) -> None:
        original = self.original_set_many
        calls = []

        async def flaky(payload, *, ttl_seconds=0):
            calls.append(dict(payload))
            if len(calls) == 1:
                keys = list(payload)
                stored_key = keys[0]
                # Genuinely store the "already confirmed" key so the
                # partial_keys this raises actually did land, matching
                # what the SDK's real PartialSetConnectionLostError means.
                await original({stored_key: payload[stored_key]}, ttl_seconds=ttl_seconds)
                raise PartialSetConnectionLostError({stored_key}, "simulated connection loss")
            return await original(payload, ttl_seconds=ttl_seconds)

        self.handle.set_many = flaky

        failed = self.cache.set_many({"x": 1, "y": 2, "z": 3})

        self.assertEqual(failed, [])
        self.assertEqual(len(calls), 2)
        # The retry only resent the remainder, not the already-stored key.
        self.assertEqual(len(calls[1]), 2)
        self.assertEqual(self.cache.get_many(["x", "y", "z"]), {"x": 1, "y": 2, "z": 3})

    def test_connection_lost_partial_failure_propagates_on_a_second_failure(self) -> None:
        async def always_fails(payload, *, ttl_seconds=0):
            raise PartialSetConnectionLostError(set(), "simulated connection loss")

        self.handle.set_many = always_fails

        with self.assertRaises(PartialSetConnectionLostError):
            self.cache.set_many({"x": 1, "y": 2})


if __name__ == "__main__":
    unittest.main()
