"""Backend instances built from the same connection options share one
loop thread and one client.

Django's ``CacheHandler`` keeps instances in an asgiref ``Local``, so every
short-lived thread (or ASGI request context) constructs a fresh
``NanocachedCache``. When each instance owned its own loop thread, client
and sockets, those leaked for the life of the process: ``close()`` is a
no-op by default and the keepalive pings stop the server's idle timeout
from reclaiming the connections, until the node connection limit was hit.
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from mock_node import MockNode
from nanocached_django import NanocachedCache
from nanocached_django.backend import NanocachedClient


def _loop_threads() -> set[threading.Thread]:
    return {t for t in threading.enumerate() if t.name == "nanocached-django-loop"}


class SharedBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.node = MockNode().start()
        self.addCleanup(self.node.close)
        self.threads_before = _loop_threads()

    def _new_backend(self, **extra_options) -> NanocachedCache:
        return NanocachedCache(
            self.node.address,
            {"OPTIONS": {"NAMESPACE": "shared", **extra_options}, "TIMEOUT": 300},
        )

    def _new_loop_threads(self) -> set[threading.Thread]:
        return _loop_threads() - self.threads_before

    def test_instances_from_many_short_lived_threads_share_one_loop_and_client(self) -> None:
        connect_calls = []
        real_connect = NanocachedClient.connect

        async def counting_connect(*args, **kwargs):
            connect_calls.append(args)
            return await real_connect(*args, **kwargs)

        errors = []

        def worker(n: int) -> None:
            try:
                # A fresh instance per thread, as CacheHandler's Local
                # produces — never closed, the thread just ends.
                backend = self._new_backend()
                backend.set(f"k{n}", n)
                self.assertEqual(backend.get(f"k{n}"), n)
            except BaseException as error:  # noqa: BLE001 - reported below
                errors.append(error)

        with mock.patch.object(NanocachedClient, "connect", counting_connect):
            threads = [threading.Thread(target=worker, args=(n,)) for n in range(12)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.addCleanup(self._new_backend().shutdown)
        self.assertEqual(errors, [])
        self.assertEqual(len(connect_calls), 1)
        self.assertEqual(len(self._new_loop_threads()), 1)
        self.assertEqual(self.node.connection_count, 1)

    def test_instances_in_fresh_async_contexts_share_one_loop_and_client(self) -> None:
        # CacheHandler's Local is per asgiref context, so each ASGI request
        # builds its own instance; Django's own a-methods run the sync
        # methods through sync_to_async.
        from asgiref.sync import async_to_sync

        self.addCleanup(self._new_backend().shutdown)

        async def request(n: int) -> None:
            backend = self._new_backend()
            await backend.aset(f"a{n}", n)
            self.assertEqual(await backend.aget(f"a{n}"), n)

        def one_context(n: int) -> None:
            async_to_sync(request)(n)

        threads = [threading.Thread(target=one_context, args=(n,)) for n in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(self._new_loop_threads()), 1)
        self.assertEqual(self.node.connection_count, 1)

    def test_namespaces_share_the_client_but_stay_isolated(self) -> None:
        a = self._new_backend(NAMESPACE="ns-a")
        b = self._new_backend(NAMESPACE="ns-b")
        self.addCleanup(a.shutdown)

        a.set("k", "from-a")
        b.set("k", "from-b")

        self.assertIs(a._client, b._client)
        self.assertEqual(a.get("k"), "from-a")
        self.assertEqual(b.get("k"), "from-b")

    def test_different_connection_options_do_not_share(self) -> None:
        plain = self._new_backend()
        other = self._new_backend(COMPRESS=True)
        self.addCleanup(plain.shutdown)
        self.addCleanup(other.shutdown)

        plain.set("k", "v")
        other.set("k", "v")

        self.assertIsNot(plain._client, other._client)
        self.assertEqual(len(self._new_loop_threads()), 2)

    def test_shutdown_reconnects_every_sharing_instance_lazily(self) -> None:
        first = self._new_backend()
        second = self._new_backend()
        self.addCleanup(first.shutdown)
        first.set("k", "v")

        first.shutdown()

        self.assertIsNone(second._client)
        self.assertEqual(second.get("k"), "v")  # reconnects
        self.assertEqual(first.get("k"), "v")  # and so does the other one

    def test_close_on_request_keeps_a_private_bridge_and_leaves_shared_ones_alone(self) -> None:
        shared = self._new_backend()
        self.addCleanup(shared.shutdown)
        per_request = self._new_backend(CLOSE_ON_REQUEST=True)
        shared.set("k", "v")
        per_request.set("k", "v2")
        shared_thread = shared._loop_thread
        private_thread = per_request._loop_thread
        self.assertIsNot(shared_thread, private_thread)

        per_request.close()
        shared.close()

        private_thread.join(timeout=5)
        self.assertFalse(private_thread.is_alive())
        self.assertTrue(shared_thread.is_alive())
        self.assertEqual(shared.get("k"), "v2")


if __name__ == "__main__":
    unittest.main()
