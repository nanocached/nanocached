"""The lifecycle contract: Django calls close() on every already-touched
cache alias after every request (request_finished signal), so close() is a
no-op by default — the loop thread and connection survive the request
cycle (the point of persistent connections). CLOSE_ON_REQUEST opts into
real per-request teardown; shutdown() always tears down and must leave no
thread behind, with the next operation lazily reconnecting."""

from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
