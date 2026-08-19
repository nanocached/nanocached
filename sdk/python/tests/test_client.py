import asyncio
import contextlib
import io
import os
import ssl
import unittest

from nanocached import (
    AlreadyClosedError,
    DecompressionError,
    DiscoveryBusyError,
    HashRing,
    NanocachedClient,
    WrongNodeError,
)

from mock_servers import MockDiscovery, MockNode, unused_port

NAMES = [
    "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
    "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
]


async def wait_for(condition, what: str, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.005)


class SingleNodeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_round_trips_set_get_delete(self):
        client = await self.connect()
        try:
            await client.set("greeting", "hello")
            self.assertEqual(await client.get("greeting"), "hello")
            self.assertTrue(await client.delete("greeting"))
            self.assertIsNone(await client.get("greeting"))
            self.assertFalse(await client.delete("greeting"))
        finally:
            client.close()

    async def test_get_returns_a_decoded_string(self):
        client = await self.connect()
        try:
            await client.set("greeting", "hello")
            value = await client.get("greeting")
            self.assertIsInstance(value, str)
            self.assertEqual(value, "hello")
        finally:
            client.close()

    async def test_get_raises_on_invalid_utf8(self):
        client = await self.connect()
        try:
            await client.set(b"bad-utf8", b"\xff\xfe")
            with self.assertRaises(UnicodeDecodeError):
                await client.get(b"bad-utf8")
            # get_bytes must still hand back the raw, undecoded value.
            self.assertEqual(await client.get_bytes(b"bad-utf8"), b"\xff\xfe")
        finally:
            client.close()

    async def test_get_bytes_round_trips_byte_values(self):
        client = await self.connect()
        try:
            await client.set(b"\x01\x02", b"\x00\xff")
            self.assertEqual(await client.get_bytes(b"\x01\x02"), b"\x00\xff")
            self.assertIsNone(await client.get_bytes("missing"))
        finally:
            client.close()

    async def test_handles_binary_and_empty_values(self):
        client = await self.connect()
        try:
            await client.set(b"\x01\x02", b"\x00\xff")
            self.assertEqual(await client.get_bytes(b"\x01\x02"), b"\x00\xff")
            await client.set("empty", "")
            self.assertEqual(await client.get("empty"), "")
        finally:
            client.close()

    async def test_ttl_zero_means_no_expiry(self):
        client = await self.connect()
        try:
            await client.set("k", "v")  # ttl_seconds defaults to 0
            self.assertEqual(await client.get("k"), "v")
            await client.set("k", "v", ttl_seconds=0)
            self.assertEqual(await client.get("k"), "v")
        finally:
            client.close()

    async def test_ttl_validation_is_synchronous(self):
        client = await self.connect()
        try:
            await client.set("k", "v", ttl_seconds=60)
            self.assertEqual(await client.get("k"), "v")
            with self.assertRaises(ValueError):
                await client.set("k", "v", ttl_seconds=-1)
            # The rejected set must not have poisoned the connection.
            self.assertEqual(await client.get("k"), "v")
        finally:
            client.close()

    async def test_authentication(self):
        secure = await MockNode(required_secret=b"s3cret").start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", secure.port)], auth_secret="s3cret"
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
            finally:
                client.close()

            with self.assertRaisesRegex(Exception, "requires authentication"):
                await NanocachedClient.connect([("127.0.0.1", secure.port)])
            with self.assertRaisesRegex(Exception, "authentication failed"):
                await NanocachedClient.connect([("127.0.0.1", secure.port)], auth_secret="wrong")
        finally:
            await secure.close()

    async def test_async_context_manager_closes_the_client(self):
        async with await self.connect() as client:
            await client.set("k", "v")
            self.assertEqual(await client.get("k"), "v")
            self.assertFalse(client.closed)
        self.assertTrue(client.closed)

    async def test_wrong_node_propagates_in_single_mode(self):
        client = await self.connect()
        try:
            self.node.answer_wrong_node_once()
            with self.assertRaises(WrongNodeError):
                await client.get("k")
        finally:
            client.close()

    async def test_rejects_use_after_close(self):
        client = await self.connect()
        client.close()
        client.close()  # idempotent
        self.assertTrue(client.closed)
        with self.assertRaises(AlreadyClosedError):
            await client.get("k")

    async def test_replication_is_one(self):
        client = await self.connect()
        try:
            self.assertEqual(client.replication, 1)
        finally:
            client.close()


class CompressionTests(unittest.IsolatedAsyncioTestCase):
    """doc/adr/0013-*.md."""

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_wire_format_is_untouched_when_compress_is_off(self):
        client = await self.connect()
        try:
            value = "x" * 1000
            await client.set("k", value)
            self.assertEqual(self.node.store[b"k"], value.encode("utf-8"))
            self.assertEqual(await client.get("k"), value)
        finally:
            client.close()

    async def test_compresses_at_or_above_the_threshold_and_decompresses_back(self):
        client = await self.connect(compress=True, compression_threshold=64)
        try:
            value = "x" * 1000
            await client.set("k", value)

            stored = self.node.store[b"k"]
            self.assertEqual(stored[0], 0x01)
            self.assertLess(len(stored), len(value))

            self.assertEqual(await client.get("k"), value)
            self.assertEqual(await client.get_bytes("k"), value.encode("utf-8"))
        finally:
            client.close()

    async def test_below_threshold_value_is_prefixed_but_not_compressed(self):
        client = await self.connect(compress=True, compression_threshold=256)
        try:
            await client.set("k", "short")
            self.assertEqual(self.node.store[b"k"], bytes([0x00]) + b"short")
            self.assertEqual(await client.get("k"), "short")
        finally:
            client.close()

    async def test_incompressible_data_passes_through_unbloated(self):
        client = await self.connect(compress=True, compression_threshold=16)
        try:
            value = os.urandom(512)
            await client.set("k", value)
            self.assertEqual(self.node.store[b"k"], bytes([0x00]) + value)
            self.assertEqual(await client.get_bytes("k"), value)
        finally:
            client.close()

    async def test_reading_a_legacy_value_with_compress_enabled_raises_clearly(self):
        writer = await self.connect()
        try:
            # A legacy/uncompressed writer's value whose first byte happens
            # to collide with the DEFLATE marker (0x01) — doc/adr/0013-*.md's
            # documented hazard of enabling compress against a keyspace
            # other clients still touch without it.
            await writer.set("k", bytes([0x01, 2, 3, 4]))
        finally:
            writer.close()

        reader = await self.connect(compress=True)
        try:
            with self.assertRaises(DecompressionError):
                await reader.get_bytes("k")
        finally:
            reader.close()


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_transparently_reconnects_after_a_server_fin(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.drop_connections()
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the client to see the FIN",
                )
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 2)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_concurrent_requests_share_one_redial(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.drop_connections()
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the client to see the FIN",
                )
                values = await asyncio.gather(*[client.get("k") for _ in range(10)])
                self.assertTrue(all(value == "v" for value in values))
                self.assertEqual(node.connection_count, 2, "redial was not shared")
            finally:
                client.close()
        finally:
            await node.close()


class MalformedResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_malformed_value_length_poisons_the_connection(self):
        # Regression for issue #8: a garbage `V <len>` header desyncs the
        # stream; the connection must be poisoned and the error must be
        # connection-classified, so the next request redials cleanly.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.answer_malformed_value_once()
                with self.assertRaises(ConnectionError):
                    await client.get("k")

                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 2)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_a_mismatched_response_kind_poisons_the_connection(self):
        # A well-formed response of the wrong kind (`S` answering a G)
        # means the request/response streams are off by one; reusing the
        # connection would answer every later request with the previous
        # one's response. It must poison and redial like malformed input.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.answer_stored_to_get_once()
                with self.assertRaises(ConnectionError):
                    await client.get("k")

                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 2)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_a_cancelled_request_poisons_the_connection(self):
        # A caller abandoning an in-flight request (asyncio.wait_for)
        # leaves its response unread on the wire; reusing the connection
        # would desync the stream, silently answering later requests with
        # earlier responses. The cancel must poison the connection.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.delay_next_get(30.0)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.get("k"), timeout=0.05)

                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 2)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_connect_times_out_against_a_silent_server(self):
        # A server that accepts the TCP connection but never answers the
        # handshake (a blackholed address behaves the same way) must fail
        # the connect within the deadline instead of hanging.
        from nanocached import _identify

        # Track accepted writers: nothing on the server side ever closes
        # them (that's the point of a silent server), and on 3.12.1+
        # Server.wait_closed() waits for every connection to finish.
        accepted: list[asyncio.StreamWriter] = []
        silent = await asyncio.start_server(
            lambda reader, writer: accepted.append(writer), "127.0.0.1", 0
        )
        port = silent.sockets[0].getsockname()[1]
        original = _identify.CONNECT_DEADLINE
        _identify.CONNECT_DEADLINE = 0.1
        try:
            with self.assertRaises(ConnectionError):
                await NanocachedClient.connect([("127.0.0.1", port)])
        finally:
            _identify.CONNECT_DEADLINE = original
            for writer in accepted:
                writer.close()
            silent.close()
            await silent.wait_closed()

    async def test_a_refresh_finishing_after_close_installs_no_connections(self):
        # Regression for issue #10.
        node = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], node.address)]).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            before = node.connection_count
            client.close()
            await client._refresh_node_list()
            self.assertEqual(node.connection_count, before)
        finally:
            await discovery.close()
            await node.close()


class KeepAliveTests(unittest.IsolatedAsyncioTestCase):
    # Keep-alive is always on with an internal interval (issue #27); the
    # module-level constant exists only so these tests can shorten it.

    def setUp(self):
        from nanocached import client as client_module

        self._client_module = client_module
        self._default_interval = client_module._KEEPALIVE_INTERVAL

    def tearDown(self):
        self._client_module._KEEPALIVE_INTERVAL = self._default_interval

    async def test_pings_an_idle_connection(self):
        node = await MockNode().start()
        try:
            self._client_module._KEEPALIVE_INTERVAL = 0.04
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await wait_for(lambda: node.get_count >= 2, "keep-alive pings")
                self.assertEqual(node.connection_count, 1)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_stops_after_close(self):
        node = await MockNode().start()
        try:
            self._client_module._KEEPALIVE_INTERVAL = 0.02
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            await wait_for(lambda: node.get_count >= 1, "a keep-alive ping")
            client.close()
            pings = node.get_count
            await asyncio.sleep(0.1)
            self.assertEqual(node.get_count, pings)
        finally:
            await node.close()


class AddressesTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_empty_addresses(self):
        with self.assertRaisesRegex(ValueError, "needs a non-empty addresses list"):
            await NanocachedClient.connect([])

    async def test_fails_over_to_the_second_address(self):
        node = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], node.address)]).start()
        dead = await unused_port()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", dead), ("127.0.0.1", discovery.port)]
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
            finally:
                client.close()
        finally:
            await discovery.close()
            await node.close()

    async def test_skips_a_warming_up_address(self):
        node = await MockNode().start()
        warming = await MockDiscovery([(NAMES[0], node.address)]).start()
        healthy = await MockDiscovery([(NAMES[0], node.address)]).start()
        warming.warming_up = True
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", warming.port), ("127.0.0.1", healthy.port)]
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
            finally:
                client.close()
        finally:
            await warming.close()
            await healthy.close()
            await node.close()

    async def test_raises_busy_when_every_address_is_warming(self):
        first = await MockDiscovery([]).start()
        second = await MockDiscovery([]).start()
        first.warming_up = True
        second.warming_up = True
        try:
            with self.assertRaises(DiscoveryBusyError):
                await NanocachedClient.connect(
                    [("127.0.0.1", first.port), ("127.0.0.1", second.port)]
                )
        finally:
            await first.close()
            await second.close()


class CloseWarningTests(unittest.IsolatedAsyncioTestCase):
    async def test_double_close_warns_once(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            client.close()  # the real close — must not warn
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                client.close()  # the forgotten second close — warns once
            self.assertTrue(client.closed)
            warnings = captured.getvalue().count(
                "close() called again on an already-closed client"
            )
            self.assertEqual(warnings, 1)
        finally:
            await node.close()

    async def test_forgotten_close_warns_on_reconnect_to_the_same_single_address(self):
        node = await MockNode().start()
        try:
            first = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                captured = io.StringIO()
                with contextlib.redirect_stderr(captured):
                    second = await NanocachedClient.connect([("127.0.0.1", node.port)])
                try:
                    self.assertIn(
                        "while a previous connection to it is still open — was close() forgotten?",
                        captured.getvalue(),
                    )
                finally:
                    second.close()
            finally:
                first.close()
        finally:
            await node.close()

    async def test_no_forgotten_close_warning_for_multi_address_configs(self):
        # Legitimate concurrent clients pointed at the same set of
        # addresses must not false-positive (issue #12).
        node = await MockNode().start()
        other = await unused_port()
        try:
            first = await NanocachedClient.connect(
                [("127.0.0.1", node.port), ("127.0.0.1", other)]
            )
            try:
                captured = io.StringIO()
                with contextlib.redirect_stderr(captured):
                    second = await NanocachedClient.connect(
                        [("127.0.0.1", node.port), ("127.0.0.1", other)]
                    )
                try:
                    self.assertNotIn("forgotten", captured.getvalue())
                finally:
                    second.close()
            finally:
                first.close()
        finally:
            await node.close()


class TlsOptionTests(unittest.TestCase):
    def test_tls_false_silently_ignores_ca(self):
        from nanocached.client import _build_ssl_context

        self.assertIsNone(_build_ssl_context(False, "/no/such/ca.pem"))

    def test_tls_true_without_ca_uses_the_default_trust_store(self):
        from nanocached.client import _build_ssl_context

        context = _build_ssl_context(True, None)
        self.assertIsInstance(context, ssl.SSLContext)

    def test_tls_true_with_an_unreadable_ca_file_is_a_connect_time_error(self):
        from nanocached.client import _build_ssl_context

        with self.assertRaises(OSError):
            _build_ssl_context(True, "/no/such/ca.pem")


class TtlEncodingTests(unittest.TestCase):
    def test_ttl_zero_omits_the_ttl_field(self):
        from nanocached._connection import _encode_set

        self.assertEqual(_encode_set(b"k", b"v", 0), b"S 1 1\nkv")

    def test_nonzero_ttl_includes_the_ttl_field(self):
        from nanocached._connection import _encode_set

        self.assertEqual(_encode_set(b"k", b"v", 60), b"S 1 1 60\nkv")


class ClusterTests(unittest.IsolatedAsyncioTestCase):
    async def start_cluster(self, replication: int = 1):
        node_a = await MockNode().start()
        node_b = await MockNode().start()
        nodes = [(NAMES[0], node_a), (NAMES[1], node_b)]
        discovery = await MockDiscovery(
            [(name, node.address) for name, node in nodes], replication=replication
        ).start()
        return nodes, discovery

    async def test_routes_and_reads_its_own_writes(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                keys = [f"key-{i}" for i in range(50)]
                for key in keys:
                    await client.set(key, f"value of {key}")
                for key in keys:
                    self.assertEqual(await client.get(key), f"value of {key}")

                stores = [len(node.store) for _, node in nodes]
                self.assertEqual(sum(stores), len(keys))
                self.assertTrue(all(count > 0 for count in stores), stores)
            finally:
                client.close()
        finally:
            await discovery.close()
            for _, node in nodes:
                await node.close()

    async def test_agrees_with_the_shared_ring(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                ring = HashRing([name for name, _ in nodes])
                for i in range(20):
                    key = f"key-{i}"
                    await client.set(key, "v")
                    owner = dict(nodes)[ring.route(key.encode())]
                    self.assertIn(key.encode(), owner.store, key)
            finally:
                client.close()
        finally:
            await discovery.close()
            for _, node in nodes:
                await node.close()

    async def test_wrong_node_triggers_refresh_and_one_retry(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key = "some-key"
                await client.set(key, "v")
                owner = dict(nodes)[HashRing([n for n, _ in nodes]).route(key.encode())]
                owner.answer_wrong_node_once()
                self.assertEqual(await client.get(key), "v")

                owner.answer_wrong_node_once()
                owner.answer_wrong_node_once()
                with self.assertRaises(WrongNodeError):
                    await client.get(key)
            finally:
                client.close()
        finally:
            await discovery.close()
            for _, node in nodes:
                await node.close()


class ReplicationTests(unittest.IsolatedAsyncioTestCase):
    async def start_cluster(self):
        node_a = await MockNode().start()
        node_b = await MockNode().start()
        nodes = {NAMES[0]: node_a, NAMES[1]: node_b}
        discovery = await MockDiscovery(
            [(name, node.address) for name, node in nodes.items()], replication=2
        ).start()
        return nodes, discovery

    def owners_of(self, key: str):
        return HashRing(NAMES).owners(key.encode(), 2)

    async def test_fans_writes_out_to_every_owner(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                self.assertEqual(client.replication, 2)
                keys = [f"key-{i}" for i in range(20)]
                for key in keys:
                    await client.set(key, "v")
                for key in keys:
                    for name, node in nodes.items():
                        self.assertIn(key.encode(), node.store, f"{key} missing from {name}")
            finally:
                client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_reads_fail_over_when_the_primary_dies(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "survives"
            await client.set(key, "still here")
            primary = self.owners_of(key)[0]
            await nodes[primary].close()
            await wait_for(
                lambda: client._members[primary].connection.closed,
                "the client to see the FIN",
            )
            self.assertEqual(await client.get(key), "still here")
        finally:
            client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_a_dead_replica_does_not_fail_writes(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "written-anyway"
            primary, replica = self.owners_of(key)
            await nodes[replica].close()
            await wait_for(
                lambda: client._members[replica].connection.closed,
                "the client to see the FIN",
            )
            await client.set(key, "v")
            self.assertIn(key.encode(), nodes[primary].store)
            self.assertEqual(await client.get(key), "v")
        finally:
            client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_writes_route_around_a_dead_primary_once_discovery_drops_it(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "written-after-primary-death"
            primary, replica = self.owners_of(key)

            # The primary dies AND discovery has already noticed: the first
            # write attempt fails on the dead primary, forcing a refresh
            # that re-ranks onto the survivor, and the retry succeeds.
            await nodes[primary].close()
            discovery.nodes = [(replica, nodes[replica].address)]
            await wait_for(
                lambda: client._members[primary].connection.closed,
                "the client to see the FIN",
            )

            await client.set(key, "v")
            self.assertEqual(await client.get(key), "v")
        finally:
            client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_fans_deletes_out_to_every_owner(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key = "gone-everywhere"
                await client.set(key, "v")
                for node in nodes.values():
                    self.assertIn(key.encode(), node.store)
                self.assertTrue(await client.delete(key))
                for node in nodes.values():
                    self.assertNotIn(key.encode(), node.store)
            finally:
                client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


class FireAndForgetReplicaWritesTests(unittest.IsolatedAsyncioTestCase):
    # doc/adr/0014-*.md

    def setUp(self):
        from nanocached import client as client_module

        self._client_module = client_module
        self._default_cap = client_module._MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES

    def tearDown(self):
        self._client_module._MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES = self._default_cap

    async def start_cluster(self):
        node_a = await MockNode().start()
        node_b = await MockNode().start()
        nodes = {NAMES[0]: node_a, NAMES[1]: node_b}
        discovery = await MockDiscovery(
            [(name, node.address) for name, node in nodes.items()], replication=2
        ).start()
        return nodes, discovery

    def owners_of(self, key: str):
        return HashRing(NAMES).owners(key.encode(), 2)

    async def test_by_default_a_write_still_waits_for_the_replica_leg(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                _, replica = self.owners_of("k")
                nodes[replica].delay_sets(0.08)

                start = asyncio.get_running_loop().time()
                await client.set("k", "v")
                elapsed = asyncio.get_running_loop().time() - start
                self.assertGreaterEqual(elapsed, 0.08)
            finally:
                client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_returns_as_soon_as_the_primary_acks_when_enabled(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], fire_and_forget_replicas=True
            )
            try:
                _, replica = self.owners_of("k")
                nodes[replica].delay_sets(0.2)

                start = asyncio.get_running_loop().time()
                await client.set("k", "v")
                elapsed = asyncio.get_running_loop().time() - start
                self.assertLess(elapsed, 0.2)

                await wait_for(
                    lambda: b"k" in nodes[replica].store,
                    "the background write to land on the replica",
                )
            finally:
                client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_falls_back_to_synchronous_past_the_cap(self):
        self._client_module._MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES = 2

        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], fire_and_forget_replicas=True
            )
            try:
                _, replica = self.owners_of("k")
                nodes[replica].delay_sets(0.15)

                async def timed_set() -> float:
                    start = asyncio.get_running_loop().time()
                    await client.set("k", "v")
                    return asyncio.get_running_loop().time() - start

                elapsed = await asyncio.gather(*(timed_set() for _ in range(3)))

                self.assertTrue(any(e >= 0.15 for e in elapsed), elapsed)
                self.assertTrue(any(e < 0.15 for e in elapsed), elapsed)
            finally:
                client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_close_drains_in_flight_background_replica_writes(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], fire_and_forget_replicas=True
            )
            _, replica = self.owners_of("k")
            nodes[replica].delay_sets(0.08)

            await client.set("k", "v")
            client.close()  # should not abandon the still-in-flight replica write

            await wait_for(
                lambda: b"k" in nodes[replica].store,
                "close() to drain the background replica write",
            )
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


if __name__ == "__main__":
    unittest.main()
