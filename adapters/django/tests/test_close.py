"""The lifecycle contract: Django calls close() on every already-touched
cache alias after every request (request_finished signal), so close() is a
no-op by default — the loop thread and connection survive the request
cycle (the point of persistent connections). CLOSE_ON_REQUEST opts into
real per-request teardown; shutdown() always tears down and must leave no
thread behind, with the next operation lazily reconnecting."""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
import unittest
import warnings
from unittest import mock

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from mock_node import MockNode
from nanocached_django import NanocachedCache


def _run_in_fork_child(test: unittest.TestCase, body, timeout: float = 5.0) -> None:
    """Runs ``body()`` in a real ``os.fork()`` child and fails ``test`` if
    the child raised or did not finish within ``timeout`` seconds (it is then
    SIGKILLed, so a hang fails the test instead of blocking the suite). The
    child leaves through ``os._exit`` so it never runs the test runner's
    teardown or atexit hooks."""
    with warnings.catch_warnings():
        # 3.12+ warns that fork() in a multi-threaded process may deadlock;
        # that is the very situation under test.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        code = 1
        try:
            body()
            code = 0
        finally:
            os._exit(code)
    deadline = time.monotonic() + timeout
    status = 0
    while time.monotonic() < deadline:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            break
        time.sleep(0.01)
    else:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        test.fail(f"the forked child did not finish within {timeout} s")
    test.assertTrue(
        os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0,
        f"the forked child failed (wait status {status})",
    )


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

    def test_a_forked_child_gets_a_fresh_loop_instead_of_a_dead_bridge(self) -> None:
        # issue #393: under a preforking WSGI server with preload
        # (Gunicorn preload_app, uWSGI without lazy-apps), a warm-up
        # cache touch in the master starts the loop thread before
        # fork(); only the forking thread survives in each worker, so
        # the inherited loop has no driving thread there and every
        # run_coroutine_threadsafe(...).result() blocks forever. The
        # backend records the loop-starting PID and must rebuild the
        # bridge when it changes. Simulated via a patched os.getpid —
        # a real fork() with live threads is not something a unittest
        # should attempt.
        backend = self._new_backend()
        backend.set("k", "v")
        parent_loop = backend._loop
        parent_thread = backend._loop_thread
        parent_client = backend._client

        with mock.patch("os.getpid", return_value=backend._loop_pid + 1):
            self.assertEqual(backend.get("k"), "v")
            self.assertIsNot(backend._loop, parent_loop)
            self.assertIsNot(backend._loop_thread, parent_thread)
            self.assertTrue(backend._loop_thread.is_alive())
            backend.shutdown()

        # The backend intentionally left the simulated parent's bridge
        # alone (a real child must not touch the parent's sockets); this
        # test is the "parent" too, though, so drain it here to leak
        # neither the thread nor the client's background tasks.
        asyncio.run_coroutine_threadsafe(parent_client.close(), parent_loop).result()
        parent_loop.call_soon_threadsafe(parent_loop.stop)
        parent_thread.join()
        parent_loop.close()

    def test_fork_hook_resets_a_lifecycle_lock_still_held_at_fork_time(self) -> None:
        # issue #414: if another thread holds _lifecycle_lock at the
        # moment of fork() (e.g. a threaded warm-up cache touch racing
        # gunicorn preload_app), the forked child inherits it already
        # locked with no thread that could ever release it — CPython
        # does not reinitialize a plain threading.Lock after fork the
        # way it does its own import lock, so every subsequent
        # `with self._lifecycle_lock` in the child (starting with the
        # very next _ensure_started() call) would block forever. The fix
        # registers an os.register_at_fork(after_in_child=...) hook that
        # swaps in a fresh, unlocked Lock() for every live instance.
        #
        # This calls that registered hook directly rather than
        # performing a real os.fork() — fork() alongside live threads
        # and an asyncio loop, from inside a unittest process, is
        # exactly the combination Python's own documentation warns is
        # unsafe — but it still exercises the real mechanism: the exact
        # callable object handed to os.register_at_fork().
        from nanocached_django.backend import _reset_lifecycle_locks_in_child

        backend = self._new_backend()
        backend.set("k", "v")  # forces the lazy connect, registers the hook
        backend._lifecycle_lock.acquire()  # simulate another thread holding it at fork time
        old_lock = backend._lifecycle_lock

        _reset_lifecycle_locks_in_child()

        self.assertIsNot(backend._lifecycle_lock, old_lock)
        # The new lock is a fresh, unlocked Lock() — acquiring it must
        # not block.
        self.assertTrue(backend._lifecycle_lock.acquire(timeout=1))
        backend._lifecycle_lock.release()
        backend.shutdown()

    @unittest.skipUnless(hasattr(os, "fork"), "needs fork()")
    def test_shutdown_in_a_forked_child_returns_and_leaves_the_parent_alone(self) -> None:
        # issue #1482: shutdown() in a fork() child that inherited a started
        # bridge used to wait on client.close() against a loop whose thread
        # did not survive the fork, forever.
        backend = self._new_backend()
        self.addCleanup(backend.shutdown)
        backend.set("k", "v")
        parent_thread = backend._loop_thread

        def child() -> None:
            backend.shutdown()
            assert backend._loop is None and backend._client is None
            # The next operation in the child starts a loop of its own.
            assert backend.get("k") == "v"
            assert backend._loop_pid == os.getpid()
            backend.shutdown()

        _run_in_fork_child(self, child)

        self.assertTrue(parent_thread.is_alive())
        self.assertIs(backend._loop_thread, parent_thread)
        self.assertEqual(backend.get("k"), "v")

    @unittest.skipUnless(hasattr(os, "fork"), "needs fork()")
    def test_close_on_request_close_in_a_forked_child_returns(self) -> None:
        # issue #1482: with CLOSE_ON_REQUEST (a private bridge) and a
        # preloaded warm-up touch, the first request in a worker that does
        # not touch the cache reaches close() -> shutdown().
        backend = self._new_backend(CLOSE_ON_REQUEST=True)
        self.addCleanup(backend.shutdown)
        backend.set("k", "v")
        parent_thread = backend._loop_thread

        def child() -> None:
            backend.close()
            assert backend._loop is None and backend._client is None
            assert backend.get("k") == "v"
            backend.close()

        _run_in_fork_child(self, child)

        self.assertTrue(parent_thread.is_alive())
        self.assertEqual(backend.get("k"), "v")

    @unittest.skipUnless(hasattr(os, "fork"), "needs fork()")
    def test_fork_resets_the_lock_of_a_close_on_request_bridge(self) -> None:
        # issue #1482: a CLOSE_ON_REQUEST instance's private bridge is not in
        # the shared registry, but the at-fork lock reset must cover it too:
        # a lock held by another thread at fork time stays locked in the
        # child, which holds no thread that could release it.
        backend = self._new_backend(CLOSE_ON_REQUEST=True)
        self.addCleanup(backend.shutdown)
        backend.set("k", "v")

        def child() -> None:
            backend.close()

        self.assertTrue(backend._lifecycle_lock.acquire(timeout=1))
        try:
            _run_in_fork_child(self, child)
        finally:
            backend._lifecycle_lock.release()

    def test_fork_hook_resets_the_lock_of_a_close_on_request_bridge(self) -> None:
        from nanocached_django.backend import _reset_lifecycle_locks_in_child

        backend = self._new_backend(CLOSE_ON_REQUEST=True)
        backend.set("k", "v")
        backend._lifecycle_lock.acquire()  # held by "another thread" at fork time
        old_lock = backend._lifecycle_lock

        _reset_lifecycle_locks_in_child()

        self.assertIsNot(backend._lifecycle_lock, old_lock)
        self.assertTrue(backend._lifecycle_lock.acquire(timeout=1))
        backend._lifecycle_lock.release()
        backend.shutdown()

    def test_fork_hook_is_registered_on_posix(self) -> None:
        import os

        from nanocached_django import backend as backend_module

        backend = self._new_backend()
        self.addCleanup(backend.shutdown)
        if hasattr(os, "register_at_fork"):
            self.assertTrue(backend_module._fork_hook_registered)

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
