"""A cache operation racing a concurrent shutdown() must return — with a
result or an error — instead of blocking its thread forever.

_run() used to read (loop, handle) under the bridge lock and submit its
coroutine to that loop only after releasing it. A shutdown() in that gap
could stop the loop before the submission landed: the submitted callback
then sat in the stopped loop's ready queue, loop.close() discarded it, and
the caller's .result() (no timeout) never returned."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import unittest
from collections.abc import Coroutine
from typing import Any
from unittest import mock

import support  # noqa: F401 - configures settings.CACHES / django.setup()

from nanocached import ConnectionLostError

from mock_node import MockNode
from nanocached_django import NanocachedCache

# Every wait below is bounded: the failure being guarded against is a
# thread blocked forever, which must fail the test, not hang the suite.
_WAIT = 5.0


class ShutdownSubmitRaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.node = MockNode().start()
        self.addCleanup(self.node.close)

    def _new_backend(self) -> NanocachedCache:
        backend = NanocachedCache(
            self.node.address,
            {"OPTIONS": {"NAMESPACE": "submitrace"}, "TIMEOUT": 300},
        )
        self.addCleanup(backend.shutdown)
        return backend

    def _start_getter(
        self, backend: NanocachedCache, idents: list[int] | None = None
    ) -> tuple[threading.Thread, dict[str, object]]:
        outcome: dict[str, object] = {}

        def do_get() -> None:
            if idents is not None:
                idents.append(threading.get_ident())
            try:
                outcome["value"] = backend.get("k")
            except BaseException as exc:
                outcome["error"] = exc

        getter = threading.Thread(target=do_get, daemon=True)
        getter.start()
        return getter, outcome

    def _join(self, thread: threading.Thread, what: str) -> None:
        thread.join(timeout=_WAIT)
        self.assertFalse(thread.is_alive(), f"{what} never returned")

    def test_a_submission_racing_shutdown_never_lands_on_a_stopped_loop(self) -> None:
        """The getter is held right before it submits its coroutine, and
        released by shutdown() once the loop has stopped and its thread
        exited, just before loop.close(). Before the fix that submission
        was made outside the bridge lock, so it landed on the stopped loop
        and get() never returned. Now the submission is made under the
        lock, so shutdown() cannot clear the bridge, let alone reach
        loop.close(), until the getter has submitted: the getter's bounded
        wait runs out and it submits to the live loop."""
        backend = self._new_backend()
        backend.set("k", "v")  # forces the lazy connect
        loop = backend._loop
        assert loop is not None

        getter_idents: list[int] = []
        about_to_submit = threading.Event()
        release_submit = threading.Event()
        submitted = threading.Event()
        original_submit = asyncio.run_coroutine_threadsafe

        def gated_submit(
            coro: Coroutine[Any, Any, Any], target_loop: asyncio.AbstractEventLoop
        ) -> concurrent.futures.Future[Any]:
            if threading.get_ident() not in getter_idents:
                return original_submit(coro, target_loop)
            about_to_submit.set()
            release_submit.wait(timeout=1.0)
            try:
                return original_submit(coro, target_loop)
            finally:
                submitted.set()

        original_close = loop.close

        def close_after_late_submit() -> None:
            release_submit.set()
            submitted.wait(timeout=_WAIT)
            original_close()

        with (
            mock.patch.object(loop, "close", close_after_late_submit),
            mock.patch.object(asyncio, "run_coroutine_threadsafe", gated_submit),
        ):
            getter, outcome = self._start_getter(backend, getter_idents)
            self.assertTrue(about_to_submit.wait(timeout=_WAIT))
            shutter = threading.Thread(target=backend.shutdown, daemon=True)
            shutter.start()
            self._join(getter, "get()")
            self._join(shutter, "shutdown()")
        # Submitted to the live loop, the get either completes or is
        # failed by shutdown()'s client.close(), whichever comes first.
        if "error" in outcome:
            self.assertIsInstance(outcome["error"], ConnectionLostError)
        else:
            self.assertEqual(outcome, {"value": "v"})

    def test_an_operation_waiting_on_a_reply_when_shutdown_runs_fails_instead_of_hanging(
        self,
    ) -> None:
        """A get() already waiting on the node's reply when shutdown() runs:
        client.close() fails the pending request before shutdown() stops
        the loop, so get() raises ConnectionLostError rather than being
        left pending on a stopped loop."""
        backend = self._new_backend()
        backend.set("k", "v")  # forces the lazy connect

        request_received = threading.Event()
        release_reply = threading.Event()

        def hold_reply() -> None:
            request_received.set()
            release_reply.wait(timeout=_WAIT)

        self.node.before_access_applies = hold_reply
        try:
            getter, outcome = self._start_getter(backend)
            self.assertTrue(request_received.wait(timeout=_WAIT))
            shutter = threading.Thread(target=backend.shutdown, daemon=True)
            shutter.start()
            self._join(shutter, "shutdown()")
            self._join(getter, "get()")
        finally:
            release_reply.set()
        self.assertIsInstance(outcome.get("error"), ConnectionLostError)


if __name__ == "__main__":
    unittest.main()
