"""The lifecycle contract: Django calls close() on every already-touched
cache alias after every request (request_finished signal), so close() is a
no-op by default — the loop thread and connection survive the request
cycle (the point of persistent connections). CLOSE_ON_REQUEST opts into
real per-request teardown; shutdown() always tears down and must leave no
thread behind, with the next operation lazily reconnecting."""

from __future__ import annotations

import threading
import unittest

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from mock_node import MockNode
from nanocached_django import NanocachedCache


class CloseLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.node = MockNode().start()
        self.addCleanup(self.node.close)

    def _new_backend(self, **extra_options) -> NanocachedCache:
        return NanocachedCache(
            self.node.address,
            {"OPTIONS": {"NAMESPACE": "closetest", **extra_options}, "TIMEOUT": 300},
        )

    def test_close_is_a_no_op_by_default(self) -> None:
        # The per-request close must not cost a reconnect — same stance
        # as django-redis.
        backend = self._new_backend()
        backend.set("k", "v")
        thread = backend._loop_thread

        backend.close()

        self.assertIs(backend._loop_thread, thread)
        self.assertTrue(thread.is_alive())
        self.assertEqual(backend.get("k"), "v")
        backend.shutdown()

    def test_close_on_request_option_makes_close_tear_down(self) -> None:
        backend = self._new_backend(CLOSE_ON_REQUEST=True)
        backend.set("k", "v")
        thread = backend._loop_thread
        self.assertTrue(thread.is_alive())

        backend.close()

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(backend._loop)

    def test_shutdown_stops_the_loop_thread(self) -> None:
        backend = self._new_backend()
        backend.set("k", "v")  # forces the lazy connect
        thread = backend._loop_thread
        self.assertIsNotNone(thread)
        self.assertTrue(thread.is_alive())

        backend.shutdown()

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(backend._loop)
        self.assertIsNone(backend._loop_thread)

    def test_shutdown_before_any_use_or_twice_is_a_no_op(self) -> None:
        backend = self._new_backend()
        backend.shutdown()  # never connected — must not raise
        backend.set("k", "v")
        backend.shutdown()
        backend.shutdown()  # second shutdown — must not raise

    def test_backend_reconnects_after_shutdown(self) -> None:
        backend = self._new_backend()
        backend.set("k", "v")
        backend.shutdown()

        backend.set("k", "v2")
        self.assertEqual(backend.get("k"), "v2")
        self.assertTrue(backend._loop_thread.is_alive())

        backend.shutdown()

    def test_run_survives_a_shutdown_race_via_reconnect(self) -> None:
        """issue #185: _run() used to read self._loop and
        self._namespace_handle as two separate unguarded attribute
        accesses. A shutdown() landing between them cleared both to None
        mid-flight, surfacing as a raw AttributeError instead of either a
        clean reconnect or the backend's own documented error.

        This deterministically forces that exact interleaving: a get()
        is parked right after its _ensure_started() call returns (the
        real backend is already connected at that point, same as
        production), a concurrent shutdown() is then allowed to run to
        completion, and only then is the get() allowed to proceed into
        the snapshot-under-lock read _run() now does. The fix must
        reconnect and complete the call rather than raising
        AttributeError."""
        backend = self._new_backend()
        backend.set("k", "v")  # forces the lazy connect up front

        original_ensure_started = backend._ensure_started
        ensure_started_returned = threading.Event()
        shutdown_finished = threading.Event()

        def patched_ensure_started() -> None:
            original_ensure_started()
            # Signal the shutdown thread, then hold here until shutdown()
            # has fully torn the loop/handle down — this is the exact
            # race window between _ensure_started() returning and _run()
            # snapshotting (loop, handle) under the lock.
            ensure_started_returned.set()
            self.assertTrue(shutdown_finished.wait(timeout=5))

        # Instance attribute shadows the class method for self._run()'s
        # own self._ensure_started() call.
        backend._ensure_started = patched_ensure_started

        results: dict[str, object] = {}

        def do_get() -> None:
            try:
                results["value"] = backend.get("k")
            except Exception as exc:  # pragma: no cover - failure path
                results["error"] = exc

        def do_shutdown() -> None:
            self.assertTrue(ensure_started_returned.wait(timeout=5))
            backend.shutdown()
            shutdown_finished.set()

        getter = threading.Thread(target=do_get)
        shutter = threading.Thread(target=do_shutdown)
        getter.start()
        shutter.start()
        getter.join(timeout=10)
        shutter.join(timeout=10)

        self.assertNotIn("error", results, f"_run raised: {results.get('error')!r}")
        self.assertEqual(results.get("value"), "v")
        backend.shutdown()

    def test_shutdown_still_stops_the_loop_thread_when_close_raises(self) -> None:
        """issue #185: shutdown() used to call client.close(), then
        loop.stop()/thread.join()/loop.close() unconditionally after —
        if close() raised, that teardown was skipped entirely and the
        loop thread (plus whatever socket it still held) leaked."""
        backend = self._new_backend()
        backend.set("k", "v")  # forces the lazy connect
        thread = backend._loop_thread
        self.assertTrue(thread.is_alive())

        async def raising_close() -> None:
            raise RuntimeError("simulated close() failure")

        backend._client.close = raising_close

        with self.assertRaises(RuntimeError):
            backend.shutdown()

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(backend._loop)
        self.assertIsNone(backend._loop_thread)
        self.assertIsNone(backend._client)
        self.assertIsNone(backend._namespace_handle)


if __name__ == "__main__":
    unittest.main()
