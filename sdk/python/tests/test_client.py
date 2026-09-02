import asyncio
import contextlib
import io
import os
import random
import ssl
import traceback
import unittest
from unittest import mock

from nanocached import (
    AlreadyClosedError,
    AuthenticationError,
    ClientStats,
    CompressionIncompatibleError,
    ConnectionLostError,
    DecompressionError,
    DiscoveryBusyError,
    HashRing,
    NanocachedClient,
    NanocachedError,
    NotNumericError,
    PartialConnectionLostError,
    PartialSetConnectionLostError,
    PartialWrongNodeError,
    RetryableError,
    WrongNodeError,
    content_digest,
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
            await client.close()

    async def test_get_returns_a_decoded_string(self):
        client = await self.connect()
        try:
            await client.set("greeting", "hello")
            value = await client.get("greeting")
            self.assertIsInstance(value, str)
            self.assertEqual(value, "hello")
        finally:
            await client.close()

    async def test_get_raises_on_invalid_utf8(self):
        client = await self.connect()
        try:
            await client.set(b"bad-utf8", b"\xff\xfe")
            with self.assertRaises(UnicodeDecodeError):
                await client.get(b"bad-utf8")
            # get_bytes must still hand back the raw, undecoded value.
            self.assertEqual(await client.get_bytes(b"bad-utf8"), b"\xff\xfe")
        finally:
            await client.close()

    async def test_get_bytes_round_trips_byte_values(self):
        client = await self.connect()
        try:
            await client.set(b"\x01\x02", b"\x00\xff")
            self.assertEqual(await client.get_bytes(b"\x01\x02"), b"\x00\xff")
            self.assertIsNone(await client.get_bytes("missing"))
        finally:
            await client.close()

    async def test_handles_binary_and_empty_values(self):
        client = await self.connect()
        try:
            await client.set(b"\x01\x02", b"\x00\xff")
            self.assertEqual(await client.get_bytes(b"\x01\x02"), b"\x00\xff")
            await client.set("empty", "")
            self.assertEqual(await client.get("empty"), "")
        finally:
            await client.close()

    async def test_ttl_zero_means_no_expiry(self):
        client = await self.connect()
        try:
            await client.set("k", "v")  # ttl_seconds defaults to 0
            self.assertEqual(await client.get("k"), "v")
            await client.set("k", "v", ttl_seconds=0)
            self.assertEqual(await client.get("k"), "v")
        finally:
            await client.close()

    async def test_pipelines_concurrent_requests_on_one_connection(self):
        # Same shape as the TypeScript SDK's own pipelining test: N
        # concurrent requests on a single connection, each independently
        # verified to round-trip its own value (request pipelining) — a
        # bug in matching responses to the right caller in send order
        # would show up as swapped or wrong values here.
        client = await self.connect()
        try:
            await asyncio.gather(*(client.set(f"key-{i}", f"value-{i}") for i in range(20)))
            values = await asyncio.gather(*(client.get(f"key-{i}") for i in range(20)))
            for i, value in enumerate(values):
                self.assertEqual(value, f"value-{i}")
        finally:
            await client.close()

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
            await client.close()

    async def test_empty_key_is_rejected_before_touching_the_connection(self):
        # An empty key would serialize to a frame the server rejects with
        # no reply (ParseError::EmptyKey, src/command.rs), closing the
        # shared, pipelined connection and taking every other in-flight
        # request on it down with it — same consequence as an invalid
        # ttl_seconds, so it must be rejected the same way: synchronously,
        # before anything is written.
        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.get("")
            with self.assertRaises(ValueError):
                await client.set("", "v")
            with self.assertRaises(ValueError):
                await client.delete("")

            # None of the rejections above wrote anything to the shared
            # connection — a concurrent valid call on it must still
            # succeed normally, and no reconnect should have happened.
            await client.set("k", "v")
            results = await asyncio.gather(
                client.get("k"),
                client.set("", "poison-attempt"),
                client.get("k"),
                return_exceptions=True,
            )
            self.assertEqual(results[0], "v")
            self.assertIsInstance(results[1], ValueError)
            self.assertEqual(results[2], "v")
            self.assertEqual(self.node.connection_count, 1, "an empty-key rejection triggered a reconnect")
        finally:
            await client.close()

    async def test_oversized_key_and_value_are_rejected_before_touching_the_connection(self):
        from nanocached.client import _MAX_REQUEST_BYTES

        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.get(b"k" * (_MAX_REQUEST_BYTES + 1))
            with self.assertRaises(ValueError):
                await client.set("k", b"v" * (_MAX_REQUEST_BYTES + 1))
            with self.assertRaises(ValueError):
                await client.delete(b"k" * (_MAX_REQUEST_BYTES + 1))

            # Still usable afterward — none of the above touched the wire.
            await client.set("k", "v")
            self.assertEqual(await client.get("k"), "v")
        finally:
            await client.close()

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
                await client.close()

            # Both shapes are matchable as AuthenticationError (issue #47
            # item 5), not just by message.
            with self.assertRaisesRegex(AuthenticationError, "requires authentication"):
                await NanocachedClient.connect([("127.0.0.1", secure.port)])
            with self.assertRaisesRegex(AuthenticationError, "authentication failed"):
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
            await client.close()

    async def test_rejects_use_after_close(self):
        client = await self.connect()
        await client.close()
        await client.close()  # idempotent
        self.assertTrue(client.closed)
        with self.assertRaises(AlreadyClosedError):
            await client.get("k")

    async def test_replication_is_one(self):
        client = await self.connect()
        try:
            self.assertEqual(client.replication, 1)
        finally:
            await client.close()


class CompressionTests(unittest.IsolatedAsyncioTestCase):
    """value compression."""

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
            await client.close()

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
            await client.close()

    async def test_below_threshold_value_is_prefixed_but_not_compressed(self):
        client = await self.connect(compress=True, compression_threshold=256)
        try:
            await client.set("k", "short")
            self.assertEqual(self.node.store[b"k"], bytes([0x00]) + b"short")
            self.assertEqual(await client.get("k"), "short")
        finally:
            await client.close()

    async def test_incompressible_data_passes_through_unbloated(self):
        client = await self.connect(compress=True, compression_threshold=16)
        try:
            value = os.urandom(512)
            await client.set("k", value)
            self.assertEqual(self.node.store[b"k"], bytes([0x00]) + value)
            self.assertEqual(await client.get_bytes("k"), value)
        finally:
            await client.close()

    async def test_an_oversized_value_is_rejected_before_compression_even_if_it_would_compress_under_the_cap(self):
        # Regression (issue #47 audit item 3): the request-size cap must be
        # checked against the *original* value, matching Rust's and Go's
        # Set — not the compressed frame, which a highly repetitive value
        # can shrink well under _MAX_REQUEST_BYTES even though the
        # uncompressed value the caller asked to store never could have
        # fit the server's own request cap.
        from nanocached.client import _MAX_REQUEST_BYTES

        client = await self.connect(compress=True, compression_threshold=16)
        try:
            oversized = b"a" * (_MAX_REQUEST_BYTES + 1000)  # DEFLATE-friendly
            with self.assertRaises(ValueError):
                await client.set("k", oversized)
            # Rejected before any I/O — the key was never written.
            self.assertNotIn(b"k", self.node.store)

            # Still usable afterward — none of the above touched the wire.
            await client.set("k", "v")
            self.assertEqual(await client.get("k"), "v")
        finally:
            await client.close()

    async def test_reading_a_legacy_value_with_compress_enabled_raises_clearly(self):
        writer = await self.connect()
        try:
            # A legacy/uncompressed writer's value whose first byte happens
            # to collide with the DEFLATE marker (0x01) — value compression's
            # documented hazard of enabling compress against a keyspace
            # other clients still touch without it.
            await writer.set("k", bytes([0x01, 2, 3, 4]))
        finally:
            await writer.close()

        reader = await self.connect(compress=True)
        try:
            with self.assertRaises(DecompressionError):
                await reader.get_bytes("k")
        finally:
            await reader.close()


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
                await client.close()
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
                await client.close()
        finally:
            await node.close()

    async def test_a_cancelled_awaiter_does_not_cause_a_redundant_second_dial(self):
        # The shielded dial task keeps running even if the caller that
        # started it is cancelled (e.g. a timed-out asyncio.wait_for); the
        # in-flight entry must only be cleared once that dial task itself
        # finishes, or a later caller would start a redundant second dial
        # to the same address instead of sharing the one already running.
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

                real_open = client._open_node_connection
                calls = 0

                async def slow_open(address):
                    nonlocal calls
                    calls += 1
                    await asyncio.sleep(0.05)
                    return await real_open(address)

                client._open_node_connection = slow_open

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.get("k"), timeout=0.01)

                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(calls, 1, "the cancelled awaiter caused a redundant second dial")
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_reconnect_cooldown_skips_a_known_dead_address(self):
        node = await MockNode().start()
        port = node.port
        client = await NanocachedClient.connect(
            [("127.0.0.1", port)],
            # Timing: a wide cooldown window and fast-rejection bound keep this from flaking on loaded CI runners.
            reconnect_cooldown=1.0,
        )
        try:
            await client.set("k", "v")
            await node.close()
            await wait_for(
                lambda: client._single is not None and client._single.closed,
                "the client to see the FIN",
            )

            # Nothing listens on `port` anymore, so this redial fails fast
            # and starts the cooldown window for that address.
            with self.assertRaises(ConnectionError):
                await client.get("k")

            # A listener now sits on the same port and answers immediately
            # with bytes the identify handshake rejects outright —
            # deliberately not the IncompleteReadError/ConnectionResetError/
            # BrokenPipeError shape that triggers connect_and_identify's
            # legacy-server fallback redial (_identify.py), so each dial
            # against it fails after exactly one connection, letting
            # `connections` below tell "cooldown skipped the dial" apart
            # from "cooldown let it through" unambiguously.
            connections = 0

            async def handle(reader, writer):
                nonlocal connections
                connections += 1
                writer.write(b"XXX")
                try:
                    await writer.drain()
                except (ConnectionError, OSError):
                    pass
                writer.close()

            garbage = await asyncio.start_server(handle, "127.0.0.1", port)
            try:
                # Still within the cooldown window: rejects with the
                # cached failure near-instantly, without dialing the
                # listener at all.
                start = asyncio.get_running_loop().time()
                with self.assertRaises(ConnectionError):
                    await client.get("k")
                elapsed = asyncio.get_running_loop().time() - start
                self.assertLess(elapsed, 0.5, f"expected a cooldown-fast rejection, took {elapsed}s")
                self.assertEqual(connections, 0, "the cooldown did not prevent a redial")

                # Once the cooldown window has passed, the address is
                # dialed again, this time reaching the listener.
                await asyncio.sleep(1.2)
                with self.assertRaisesRegex(NanocachedError, "unexpected response to A"):
                    await client.get("k")
                self.assertEqual(
                    connections, 1, "the address was never redialed after the cooldown elapsed"
                )
            finally:
                garbage.close()
                await garbage.wait_closed()
        finally:
            await client.close()

    async def test_close_drains_an_in_flight_redial(self):
        # A redial kept alive via asyncio.shield after its original caller
        # was cancelled/timed out (see
        # test_a_cancelled_awaiter_does_not_cause_a_redundant_second_dial)
        # used to keep running unobserved past close(): the connection it
        # eventually adopted could leak past close() (breaking the
        # README's "close() returns after all connections are finished"),
        # and a later failure in it had nothing left to retrieve its
        # exception, surfacing as an "exception was never retrieved"
        # warning. close() must drain it the same way it already drains
        # background replica writes.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            await client.set("k", "v")
            node.drop_connections()
            await wait_for(
                lambda: client._single is not None and client._single.closed,
                "the client to see the FIN",
            )

            real_open = client._open_node_connection
            calls = 0

            async def slow_open(address):
                nonlocal calls
                calls += 1
                await asyncio.sleep(0.08)
                return await real_open(address)

            client._open_node_connection = slow_open

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(client.get("k"), timeout=0.01)

            # The redial is still running in the background, shielded
            # from the caller above, which already gave up on it.
            self.assertTrue(client._redials, "expected a redial still in flight")

            start = asyncio.get_running_loop().time()
            await client.close()
            elapsed = asyncio.get_running_loop().time() - start
            self.assertGreaterEqual(
                elapsed, 0.06, "close() returned before the in-flight redial finished"
            )
            self.assertFalse(client._redials, "close() left a redial task untracked")
            self.assertEqual(calls, 1)
            # The connection the drained redial adopted must itself have
            # been torn down by close(), not leaked.
            self.assertTrue(client._single is not None and client._single.closed)
        finally:
            await node.close()

    async def test_close_awaits_each_connections_read_task_leaving_nothing_pending(self):
        # Issue #412(b): close() reaped every other background task
        # (redials, refresh, keepalive, background replica writes,
        # hedged reads — see test_close_awaits_the_keepalive_task_
        # leaving_nothing_pending's own issue #189 fix above) but never
        # awaited a Connection's own `_read_task` — _teardown() only
        # called Connection.close(), which closes the writer and
        # returns immediately without waiting for the read loop to
        # actually notice and finish. A short-lived program torn down
        # right after close() could log "Task was destroyed but it is
        # pending!" for it. Assert the read task itself is both
        # finished and gone from asyncio.all_tasks() by the time
        # close() returns.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            await client.set("k", "v")
            read_task = client._single._read_task
            self.assertIsNotNone(read_task)
            await client.close()
            self.assertTrue(read_task.done())
            self.assertNotIn(read_task, asyncio.all_tasks())
        finally:
            await node.close()


class MalformedResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_malformed_value_length_poisons_the_connection(self):
        # Regression for issue #8: a garbage `V <len>` header desyncs the
        # stream; the connection must be poisoned so the next request
        # redials cleanly. Raises NanocachedError, not ConnectionError —
        # this is this SDK's own wire-frame-parse violation (issue #47
        # audit item 5), matching the TypeScript SDK's protocol.ts, which
        # raises its own plain NanocachedError uniformly for every raw
        # parse violation; _read_loop's except clause still catches and
        # poisons on either type, so this is a type-consistency fix only.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.answer_malformed_value_once()
                with self.assertRaises(NanocachedError):
                    await client.get("k")

                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 2)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_an_unterminated_value_header_poisons_the_connection_promptly(self):
        # Regression for issue #8 follow-up: readuntil()'s
        # LimitOverrunError (a ValueError subclass — neither
        # ConnectionError nor NanocachedError/OSError) previously escaped
        # _read_loop's except clauses uncaught, killing the read task
        # silently: _poison() never ran, the writer never closed, and
        # every pending/future request hung forever. A malicious or
        # corrupted server sending `V` and withholding the header's `\n`
        # must instead fail promptly and close the connection.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                node.answer_unterminated_value_once()
                # Before the fix, this hung forever (LimitOverrunError
                # killed the read task silently, and no timeout in this
                # SDK ever intervenes) — the outer wait_for turns that
                # into a prompt, unambiguous test failure instead of a
                # stuck test run. The 512 KiB mock write loop finishing
                # before the reader task gets scheduled is a harmless
                # artifact of both ends sharing one event loop in this
                # test; readuntil()'s own 64 KiB limit is what actually
                # bounds a real, separate-process client.
                with self.assertRaises(ConnectionError):
                    await asyncio.wait_for(client.get("k"), timeout=5.0)

                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the connection to be closed",
                )
            finally:
                await client.close()
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
                await client.close()
        finally:
            await node.close()

    async def test_an_unverified_untagged_trailing_byte_poisons_the_connection(self):
        # Audit finding: the untagged fast path read the trailing byte
        # after S/D/N/W with readexactly(1) but never checked it was
        # `\n`. The TypeScript SDK's protocol.ts already validates this
        # (tryParseResponse) and raises a desync error; mirror that here
        # instead of silently accepting a tagged-shaped reply (`S1\n`) on
        # an untagged connection. Raises NanocachedError, not
        # ConnectionError (issue #47 audit item 5) — see the malformed-
        # value-length regression test above for why.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                node.answer_malformed_status_once()
                with self.assertRaisesRegex(NanocachedError, "desynced"):
                    await client.get("k")

                # The poisoned connection redials transparently on next use.
                self.assertIsNone(await client.get("k"))
                self.assertEqual(node.connection_count, 2)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_tagged_response_missing_its_tag_poisons_the_connection(self):
        # A tagged connection's fixed-form replies (S/D/N/W) must always
        # carry a trailing ` <tag>` field; a reply that omits it (as this
        # one does, `N\n` instead of `N <tag>\n`) desyncs the stream just
        # like an untagged connection's unverified trailing byte above.
        # Raises NanocachedError, not ConnectionError (issue #47 audit
        # item 5) — see the malformed-value-length regression test for why.
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                node.answer_missing_tag_once()
                with self.assertRaisesRegex(NanocachedError, "missing its tag"):
                    await client.get("k")

                # The poisoned connection redials transparently on next use.
                self.assertIsNone(await client.get("k"))
                self.assertEqual(node.connection_count, 2)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_tagged_response_with_an_invalid_tag_value_poisons_the_connection(self):
        # _parse_tag's own invalid-value path (issue #47 audit item 5):
        # a non-numeric tag field (`V 1 abc\n1` instead of `V 1 <tag>\n1`)
        # is protocol garbage distinct from the missing-tag desync above.
        # Raises NanocachedError, not ConnectionError — see the
        # malformed-value-length regression test for why.
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                node.answer_invalid_tag_value_once()
                with self.assertRaisesRegex(NanocachedError, "invalid response tag"):
                    await client.get("k")

                # The poisoned connection redials transparently on next use.
                self.assertIsNone(await client.get("k"))
                self.assertEqual(node.connection_count, 2)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_cancelled_request_does_not_poison_the_connection(self):
        # Request pipelining: pipelining leaves an abandoned request
        # (asyncio.wait_for) in the pending queue rather than removing
        # it — its still-coming response is discarded by the read loop
        # once the future is seen to be cancelled, and every request
        # queued behind it (including the next one this test makes) is
        # matched to its own response normally. No reconnect needed.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                # The mock serves one connection's requests strictly in
                # order, so the second get() below can't get its answer
                # until this delayed one is served — keep this short.
                node.delay_next_get(0.15)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.get("k"), timeout=0.02)

                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 1)
            finally:
                await client.close()
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

    async def test_open_cluster_rejects_a_nested_discovery_address_without_leaking(self):
        # Regression (pass-7 audit): if a roster entry points at another
        # discovery server, dialing it returns a ClusterTarget, not a
        # NodeTarget. _open_cluster's non-node-address cleanup must (1)
        # raise NanocachedError, not the AttributeError that closing a
        # writer-less ClusterTarget used to throw, and (2) still close the
        # sockets it already opened to the genuine nodes this round.
        from nanocached import _identify
        from nanocached.client import ClusterTarget, DiscoveredNode, NodeTarget

        class SpyWriter:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        node_writer = SpyWriter()
        node_target = NodeTarget(reader=object(), writer=node_writer, tagged=True)
        nested = ClusterTarget(nodes=[], replication=1)

        async def fake_identify(host, port, secret, ssl_context):
            # First address is a genuine node, second is a nested discovery.
            return node_target if port == 9001 else nested

        client = NanocachedClient.__new__(NanocachedClient)
        client._auth_secret = None
        client._ssl_context = None
        identified = ClusterTarget(
            nodes=[
                DiscoveredNode(name="real", address="127.0.0.1:9001"),
                DiscoveredNode(name="nested", address="127.0.0.1:9002"),
            ],
            replication=1,
        )

        with mock.patch.object(_identify, "connect_and_identify", fake_identify), mock.patch(
            "nanocached.client.connect_and_identify", fake_identify
        ):
            with self.assertRaises(NanocachedError):
                await client._open_cluster(identified)

        self.assertTrue(
            node_writer.closed,
            "the genuine node's socket must be closed during non-node-address cleanup",
        )

    async def test_a_refresh_finishing_after_close_installs_no_connections(self):
        # Regression for issue #10.
        node = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], node.address)]).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            before = node.connection_count
            await client.close()
            await client._refresh_node_list()
            self.assertEqual(node.connection_count, before)
        finally:
            await discovery.close()
            await node.close()

    async def test_refresh_dials_newly_discovered_nodes_concurrently(self):
        # Issue #190: _refresh_node_list used to dial every newly
        # discovered node in a sequential `for` loop, unlike
        # _open_cluster's own concurrent bootstrap dial — several nodes
        # joining at once, more than one of them slow to accept, stalled
        # every operation waiting on _before_operation()'s refresh for
        # roughly the SUM of their dial times instead of the max.
        existing = await MockNode().start()
        new_a = await MockNode().start()
        new_b = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], existing.address)]).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                delay = 0.12
                new_a.delay_next_auth(delay)
                new_b.delay_next_auth(delay)
                discovery.nodes = [
                    (NAMES[0], existing.address),
                    ("new-a", new_a.address),
                    ("new-b", new_b.address),
                ]

                start = asyncio.get_running_loop().time()
                await client._maybe_refresh(force=True)
                elapsed = asyncio.get_running_loop().time() - start

                # Concurrent dialing takes roughly `delay` (the max of the
                # two); a sequential loop would take roughly `2 * delay`
                # (the sum) — the threshold sits well between the two.
                self.assertLess(elapsed, delay * 1.5)
                self.assertIn("new-a", client._members)
                self.assertIn("new-b", client._members)
                self.assertIsNotNone(client._members["new-a"].connection)
                self.assertIsNotNone(client._members["new-b"].connection)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await existing.close()
            await new_a.close()
            await new_b.close()


class IdentifyMalformedResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_an_unterminated_node_list_header_fails_promptly(self):
        # Regression: mirrors the `V`-path LimitOverrunError fix on the
        # discovery path. readuntil() can raise LimitOverrunError here
        # too (neither NanocachedError nor OSError); left unwrapped it
        # would break client.py's `except (NanocachedError, OSError)`
        # contracts ("try next address silently", "refresh swallows
        # failures") and escape raw to callers instead.
        from nanocached import _identify

        discovery = await MockDiscovery([]).start()
        try:
            discovery.answer_unterminated_list_once()
            # Left unwrapped, this would either escape as a raw
            # LimitOverrunError or (once CONNECT_DEADLINE elapses) as a
            # plain ConnectionError — neither satisfies NanocachedError,
            # so this assertion also catches a regression back to the
            # unwrapped exception, not just a hang.
            with self.assertRaises(NanocachedError):
                await asyncio.wait_for(
                    _identify.connect_and_identify("127.0.0.1", discovery.port, None, None),
                    timeout=5.0,
                )
        finally:
            await discovery.close()

    async def test_an_oversized_node_list_response_fails_instead_of_buffering_unbounded_memory(self):
        # Regression: bounds a malicious discovery server's memory
        # pressure with an aggregate cap on the whole `N ...` response
        # (16 MiB), independent of the per-field caps — this same
        # constant is being added to all six SDKs.
        from nanocached import _identify

        field_length = 64 * 1024  # _MAX_NODE_FIELD_LENGTH
        entry_count = 300  # ~300 * ~128 KiB > 16 MiB

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readuntil(b"\n")  # the `A ...` frame
                writer.write(b"Od\n")
                await writer.drain()
                await reader.readuntil(b"\n")  # the `L\n` frame
                writer.write(b"N %d 1\n" % entry_count)
                name = b"a" * field_length
                addr = b"b" * field_length
                entry = b"%d %d\n%b%b\n" % (field_length, field_length, name, addr)
                for _ in range(entry_count):
                    writer.write(entry)
                    await writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                writer.close()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            with self.assertRaises(NanocachedError) as ctx:
                await asyncio.wait_for(
                    _identify.connect_and_identify("127.0.0.1", port, None, None),
                    timeout=10.0,
                )
            self.assertIn("exceeds maximum size", str(ctx.exception))
        finally:
            server.close()
            await server.wait_closed()


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
                await client.close()
        finally:
            await node.close()

    async def test_stops_after_close(self):
        node = await MockNode().start()
        try:
            self._client_module._KEEPALIVE_INTERVAL = 0.02
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            await wait_for(lambda: node.get_count >= 1, "a keep-alive ping")
            await client.close()
            pings = node.get_count
            await asyncio.sleep(0.1)
            self.assertEqual(node.get_count, pings)
        finally:
            await node.close()

    async def test_only_swallows_network_class_errors(self):
        # issue #192: the ping loop's `except Exception: pass` used to
        # hide any bug in connection.get() itself, not just a dead or
        # flaky connection. Narrowed to _SWALLOWABLE_ERRORS (the same
        # network-class tuple used everywhere else in this module), a
        # non-network error raised by a ping must still surface as the
        # keepalive task's own exception instead of vanishing.
        node = await MockNode().start()
        try:
            self._client_module._KEEPALIVE_INTERVAL = 0.02
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                boom = RuntimeError("not a network error")

                async def raise_boom(key):
                    raise boom

                client._single.get = raise_boom

                await wait_for(
                    lambda: client._keepalive_task.done(),
                    "the keepalive task to crash on a non-network error",
                )
                self.assertIs(client._keepalive_task.exception(), boom)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_close_awaits_the_keepalive_task_leaving_nothing_pending(self):
        # Issue #189: close() cancelled the keepalive task but never
        # gathered it (unlike _redials/_refresh_task/
        # _background_replica_writes/_hedged_reads) — a loop torn down
        # right after close() could log "Task was destroyed but it is
        # pending!" for it. Assert the task itself is both finished and
        # gone from asyncio.all_tasks() by the time close() returns.
        node = await MockNode().start()
        try:
            self._client_module._KEEPALIVE_INTERVAL = 0.02
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            await wait_for(lambda: node.get_count >= 1, "a keep-alive ping")
            keepalive_task = client._keepalive_task
            self.assertIsNotNone(keepalive_task)
            await client.close()
            self.assertTrue(keepalive_task.done())
            self.assertNotIn(keepalive_task, asyncio.all_tasks())
        finally:
            await node.close()


class RequestTimeoutTests(unittest.IsolatedAsyncioTestCase):
    # The progress-based request timeout (issue #42); the module-level
    # constant exists only so these tests can shorten it.

    def setUp(self):
        from nanocached import _connection as connection_module

        self._connection_module = connection_module
        self._default_timeout = connection_module._REQUEST_TIMEOUT

    def tearDown(self):
        self._connection_module._REQUEST_TIMEOUT = self._default_timeout

    async def test_a_request_to_a_half_open_server_fails_within_the_timeout(self):
        # Regression: a server that completes the A handshake but then
        # never answers a G/S/D used to hang get/set/delete forever —
        # there was no in-flight request timeout at all.
        node = await MockNode().start()
        try:
            self._connection_module._REQUEST_TIMEOUT = 0.15
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.go_silent_after_handshake()

                started = asyncio.get_running_loop().time()
                # The client's retry layer redials once after the first
                # timeout; the redialed connection times out too, so this
                # settles after roughly two windows — still bounded.
                with self.assertRaisesRegex(ConnectionError, "request timed out"):
                    await client.get("k")
                self.assertLess(asyncio.get_running_loop().time() - started, 2.0)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_steady_new_requests_do_not_postpone_half_open_detection(self):
        # The deadline is progress-based: new sends must not extend it
        # while an older request is still waiting (mirrors the Go SDK's
        # regression test of the same name).
        node = await MockNode().start()
        try:
            self._connection_module._REQUEST_TIMEOUT = 0.2
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                node.go_silent_after_handshake()

                async def steady_traffic():
                    # New requests keep arriving well inside every
                    # deadline window (once the connection is poisoned
                    # they just fail fast).
                    while True:
                        await asyncio.sleep(0.05)
                        with contextlib.suppress(Exception):
                            await client.get("more")

                ticker = asyncio.ensure_future(steady_traffic())
                try:
                    started = asyncio.get_running_loop().time()
                    with self.assertRaisesRegex(ConnectionError, "request timed out"):
                        await client.get("k")
                    self.assertLess(asyncio.get_running_loop().time() - started, 2.0)
                finally:
                    ticker.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ticker
            finally:
                await client.close()
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
                await client.close()
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
                await client.close()
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
            await client.close()  # the real close — must not warn
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                await client.close()  # the forgotten second close — warns once
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
                    await second.close()
            finally:
                await first.close()
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
                    await second.close()
            finally:
                await first.close()
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


class NamespaceEncodingTests(unittest.TestCase):
    # Namespaces (issue #105): docs/protocol.html "g / s / d" is the
    # authoritative wire spec these pin.

    def test_default_namespace_still_encodes_the_legacy_uppercase_frames(self):
        # SDK rule: the default (empty) namespace must keep sending
        # legacy G/S/D byte-for-byte, so an unchanged client talking to
        # an old, pre-namespace server keeps working.
        from nanocached._connection import _encode_delete, _encode_get, _encode_set

        self.assertEqual(_encode_get(b"k"), b"G 1\nk")
        self.assertEqual(_encode_set(b"k", b"v", 0), b"S 1 1\nkv")
        self.assertEqual(_encode_delete(b"k"), b"D 1\nk")
        # Passing namespace=b"" explicitly is identical to omitting it.
        self.assertEqual(_encode_get(b"k", namespace=b""), b"G 1\nk")

    def test_non_empty_namespace_encodes_the_lowercase_frames(self):
        from nanocached._connection import _encode_delete, _encode_get, _encode_set

        self.assertEqual(_encode_get(b"k", namespace=b"users"), b"g 5 1\nusersk")
        self.assertEqual(_encode_delete(b"k", namespace=b"users"), b"d 5 1\nusersk")
        self.assertEqual(_encode_set(b"k", b"v", 0, namespace=b"users"), b"s 5 1 1\nuserskv")
        self.assertEqual(_encode_set(b"k", b"v", 60, namespace=b"users"), b"s 5 1 1 60\nuserskv")

    def test_tagged_namespaced_frames_keep_the_tag_as_the_last_header_field(self):
        from nanocached._connection import _encode_delete, _encode_get, _encode_set

        self.assertEqual(_encode_get(b"k", tag=7, namespace=b"users"), b"g 5 1 7\nusersk")
        self.assertEqual(_encode_delete(b"k", tag=7, namespace=b"users"), b"d 5 1 7\nusersk")
        # ttl+tag form: ttl still precedes tag, both after val-len.
        self.assertEqual(
            _encode_set(b"k", b"v", 0, tag=7, namespace=b"users"), b"s 5 1 1 7\nuserskv"
        )
        self.assertEqual(
            _encode_set(b"k", b"v", 60, tag=7, namespace=b"users"), b"s 5 1 1 60 7\nuserskv"
        )

    def test_namespace_may_contain_arbitrary_bytes(self):
        # No delimiter, no escaping, no hierarchy — sliced by its declared
        # length like every other body field.
        from nanocached._connection import _encode_get

        self.assertEqual(_encode_get(b"beta", namespace=b"\xff\x00"), b"g 2 4\n\xff\x00beta")


class ClearEncodingTests(unittest.TestCase):
    # Clear / flush (issue #106): docs/protocol.html "c / F" is the
    # authoritative wire spec these pin. Unlike g/s/d, c/F have no legacy
    # uppercase form — the default namespace is just namespace-length 0.

    def test_encodes_untagged_clear_and_clear_all(self):
        from nanocached._connection import _encode_clear, _encode_clear_all

        self.assertEqual(_encode_clear(b"users"), b"c 5\nusers")
        self.assertEqual(_encode_clear_all(), b"F\n")

    def test_empty_namespace_clears_the_default_namespace(self):
        from nanocached._connection import _encode_clear

        self.assertEqual(_encode_clear(b""), b"c 0\n")
        self.assertEqual(_encode_clear(), b"c 0\n")

    def test_tagged_clear_and_clear_all_keep_the_tag_as_the_last_header_field(self):
        from nanocached._connection import _encode_clear, _encode_clear_all

        self.assertEqual(_encode_clear(b"users", tag=7), b"c 5 7\nusers")
        self.assertEqual(_encode_clear_all(tag=7), b"F 7\n")

    def test_namespace_may_contain_arbitrary_bytes(self):
        from nanocached._connection import _encode_clear

        self.assertEqual(_encode_clear(b"\xff\x00"), b"c 2\n\xff\x00")


class IncrEncodingTests(unittest.TestCase):
    # Counters (issue #129): docs/protocol.html "i" is the authoritative
    # wire spec these pin — INCR has no legacy uppercase form, so even
    # the default namespace still carries <namespace-length> 0.

    def test_encodes_untagged_incr(self):
        from nanocached._connection import _encode_incr

        self.assertEqual(_encode_incr(b"k", 5), b"i 0 1 5\nk")

    def test_encodes_negative_delta(self):
        from nanocached._connection import _encode_incr

        self.assertEqual(_encode_incr(b"k", -5), b"i 0 1 -5\nk")

    def test_encodes_namespaced_incr(self):
        from nanocached._connection import _encode_incr

        self.assertEqual(_encode_incr(b"k", 5, namespace=b"users"), b"i 5 1 5\nusersk")

    def test_encodes_tagged_incr(self):
        from nanocached._connection import _encode_incr

        self.assertEqual(_encode_incr(b"k", 5, tag=7), b"i 0 1 5 7\nk")

    def test_encodes_tagged_namespaced_incr(self):
        from nanocached._connection import _encode_incr

        self.assertEqual(
            _encode_incr(b"k", 5, tag=7, namespace=b"users"), b"i 5 1 5 7\nusersk"
        )

    def test_namespace_may_contain_arbitrary_bytes(self):
        from nanocached._connection import _encode_incr

        self.assertEqual(_encode_incr(b"beta", -1, namespace=b"\xff\x00"), b"i 2 4 -1\n\xff\x00beta")


class IncrTests(unittest.IsolatedAsyncioTestCase):
    # Counters (issue #129): single-node coverage of incr()/decr() and
    # the I/T/N response decode — see IncrReplicationTests below for the
    # cluster fan-out contract (primary-only i, replica gets a set).

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_incr_on_a_missing_key_returns_none(self):
        # Matches get()'s own miss convention — no exception.
        client = await self.connect()
        try:
            self.assertIsNone(await client.incr("missing"))
            self.assertEqual(self.node.incr_count, 1)
        finally:
            await client.close()

    async def test_incr_on_a_non_numeric_value_raises_not_numeric_error(self):
        client = await self.connect()
        try:
            await client.set("k", "not-a-number")
            with self.assertRaises(NotNumericError):
                await client.incr("k")
        finally:
            await client.close()

    async def test_successful_incr_returns_the_new_value(self):
        client = await self.connect()
        try:
            await client.set("counter", "10")
            self.assertEqual(await client.incr("counter"), 11)  # delta defaults to 1
            self.assertEqual(await client.incr("counter", 5), 16)
            self.assertEqual(await client.incr("counter", -20), -4)
        finally:
            await client.close()

    async def test_incr_on_a_compress_enabled_client_raises_before_any_io(self):
        # Issue #321: compress has no marker byte on incr's wire result,
        # so this must be rejected up front rather than corrupt the
        # keyspace — no wire op should even be attempted.
        client = await self.connect(compress=True)
        try:
            with self.assertRaises(CompressionIncompatibleError):
                await client.incr("counter")
            self.assertEqual(self.node.incr_count, 0)
        finally:
            await client.close()

    async def test_decr_on_a_compress_enabled_client_raises_before_any_io(self):
        client = await self.connect(compress=True)
        try:
            with self.assertRaises(CompressionIncompatibleError):
                await client.decr("counter")
            self.assertEqual(self.node.incr_count, 0)
        finally:
            await client.close()

    async def test_decr_sends_the_same_wire_op_with_a_negated_delta(self):
        client = await self.connect()
        try:
            await client.set("counter", "10")
            self.assertEqual(await client.decr("counter", 3), 7)
            # decr() never sends a separate wire op — just `i` with -3.
            self.assertEqual(self.node.incr_count, 1)
        finally:
            await client.close()

    async def test_decr_default_amount_matches_incr_with_negative_one(self):
        client = await self.connect()
        try:
            await client.set("a", "5")
            await client.set("b", "5")
            self.assertEqual(await client.decr("a"), await client.incr("b", -1))
        finally:
            await client.close()

    async def test_delta_out_of_i64_range_is_rejected_before_touching_the_connection(self):
        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.incr("k", 2**63)
            with self.assertRaises(ValueError):
                await client.incr("k", -(2**63) - 1)
            self.assertEqual(self.node.incr_count, 0)
        finally:
            await client.close()

    async def test_empty_key_is_rejected_before_touching_the_connection(self):
        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.incr("")
            self.assertEqual(self.node.incr_count, 0)
        finally:
            await client.close()


class IncrTaggedTests(unittest.IsolatedAsyncioTestCase):
    # Counters (issue #129): the I/T/N decode path on a tagged connection
    # (echoed response tags) — see IncrTests above for the untagged path.

    async def test_incr_round_trips_over_a_tagged_connection(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("counter", "1")
                self.assertEqual(await client.incr("counter", 4), 5)
                self.assertIsNone(await client.incr("missing"))
                await client.set("bad", "nope")
                with self.assertRaises(NotNumericError):
                    await client.incr("bad")
            finally:
                await client.close()
        finally:
            await node.close()


class IncrReplicationTests(unittest.IsolatedAsyncioTestCase):
    # Counters (issue #129): the cluster contract this feature exists
    # for — the primary alone runs `i`, and forwards its literal result
    # to the remaining owners as a `set`, never replaying the increment.

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

    async def test_the_primary_runs_incr_and_the_replica_only_ever_gets_a_set(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "counter"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            await client.set(key, "10")
            # set() itself fans out to both owners — reset the counters
            # so only the incr() call below is being measured.
            primary.incr_count = 0
            replica.incr_count = 0

            self.assertEqual(await client.incr(key, 5), 15)

            # The critical assertion: not just that both stores agree on
            # the final value (a buggy replay-on-replica implementation
            # would produce that too, starting from the same seed), but
            # that the replica never received an `i` frame at all.
            self.assertEqual(primary.incr_count, 1)
            self.assertEqual(replica.incr_count, 0)
            self.assertEqual(primary.store[key.encode()], b"15")
            self.assertEqual(replica.store[key.encode()], b"15")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_the_replicated_set_carries_the_entrys_ttl(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "counter-with-ttl"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            await client.set(key, "10", ttl_seconds=120)
            self.assertEqual(await client.incr(key, 1), 11)

            # Proves the optional TTL field in the `I` response was
            # actually decoded — a decode bug that dropped it would
            # forward TTL 0 (no expiry) to the replica instead.
            self.assertEqual(replica.last_set_ttl, 120)
            self.assertEqual(replica.entry_ttl[(b"", key.encode())], 120)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_missing_key_on_the_primary_never_touches_the_replica(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "missing-counter"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            self.assertIsNone(await client.incr(key))
            self.assertEqual(primary.incr_count, 1)
            self.assertEqual(replica.incr_count, 0)
            self.assertNotIn(key.encode(), replica.store)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_non_numeric_value_on_the_primary_never_touches_the_replica(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "not-a-counter"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            await client.set(key, "not-numeric")
            primary.incr_count = 0
            replica.incr_count = 0
            with self.assertRaises(NotNumericError):
                await client.incr(key)
            self.assertEqual(primary.incr_count, 1)
            self.assertEqual(replica.incr_count, 0)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_wrong_node_triggers_refresh_and_one_retry(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "some-counter"
            await client.set(key, "1")
            primary_name, _ = self.owners_of(key)
            primary = nodes[primary_name]

            primary.answer_wrong_node_once()
            self.assertEqual(await client.incr(key), 2)

            primary.answer_wrong_node_once()
            primary.answer_wrong_node_once()
            with self.assertRaises(WrongNodeError):
                await client.incr(key)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_lost_reply_after_the_primary_applies_the_increment_is_not_retried_via_ring_refresh(self):
        # issue #225: _with_wrong_node_retry is the *only* retry layer
        # incr() has — it both redials/reconnects and, in cluster mode,
        # refreshes the ring and resends. A `W` (test above) always
        # retries there; a ConnectionLostError must not, or the
        # refresh-and-retry would resend `i` to the primary a second
        # time and double the delta.
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "counter"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            await client.set(key, "10")
            primary.incr_count = 0
            replica.incr_count = 0

            # The primary fully applies this increment, then closes the
            # connection instead of ever answering.
            primary.apply_and_drop_next_incr()
            with self.assertRaises(ConnectionLostError):
                await client.incr(key, 5)

            # Applied exactly once: a ring-refresh-and-retry would have
            # sent a second `i` (incr_count == 2) and left the primary at
            # 20, not 15 — the replica must also never see an `i` at all.
            self.assertEqual(primary.incr_count, 1)
            self.assertEqual(replica.incr_count, 0)
            self.assertEqual(primary.store[key.encode()], b"15")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()


class ContentDigestTests(unittest.TestCase):
    # Compare-and-set (issue #141): docs/protocol.html "k / x" is the
    # authoritative wire spec content_digest() implements against.

    def test_pinned_cross_language_vector(self):
        # SHA-256 of the UTF-8 bytes "nanocached-cas-vector", truncated to
        # the first 16 bytes, lowercase hex — the same fixed input/output
        # pair pinned into the Rust server and every other SDK. A
        # mismatch here means CAS silently breaks across languages.
        self.assertEqual(
            content_digest(b"nanocached-cas-vector"), "36287141940ca57acbd7695ccdde9d43"
        )

    def test_produces_a_32_character_lowercase_hex_string(self):
        digest = content_digest(b"some value")
        self.assertEqual(len(digest), 32)
        self.assertEqual(digest, digest.lower())
        int(digest, 16)  # raises ValueError if it isn't hex

    def test_is_deterministic_and_content_sensitive(self):
        self.assertEqual(content_digest(b"same"), content_digest(b"same"))
        self.assertNotEqual(content_digest(b"a"), content_digest(b"b"))

    def test_empty_value_has_a_digest_too(self):
        digest = content_digest(b"")
        self.assertEqual(len(digest), 32)


class CasEncodingTests(unittest.TestCase):
    # Compare-and-set (issue #141): docs/protocol.html "k / x" is the
    # authoritative wire spec these pin — like `i`, neither op has a
    # legacy uppercase form, so even the default namespace still carries
    # <namespace-length> 0.

    def test_encodes_absent_conditioned_set_without_ttl(self):
        from nanocached._connection import CAS_ABSENT, _encode_cas_set

        self.assertEqual(_encode_cas_set(b"k", b"v", CAS_ABSENT), b"k 0 1 1 A\nkv")

    def test_encodes_present_conditioned_set_without_ttl(self):
        from nanocached._connection import CAS_PRESENT, _encode_cas_set

        self.assertEqual(_encode_cas_set(b"k", b"v", CAS_PRESENT), b"k 0 1 1 P\nkv")

    def test_encodes_digest_conditioned_set_without_ttl(self):
        from nanocached._connection import _encode_cas_set

        digest = content_digest(b"old").encode()
        self.assertEqual(
            _encode_cas_set(b"k", b"v", digest), b"k 0 1 1 %b\nkv" % digest
        )

    def test_encodes_set_with_ttl(self):
        from nanocached._connection import CAS_ABSENT, _encode_cas_set

        self.assertEqual(
            _encode_cas_set(b"k", b"v", CAS_ABSENT, ttl_seconds=60), b"k 0 1 1 A 60\nkv"
        )

    def test_encodes_namespaced_set(self):
        from nanocached._connection import CAS_ABSENT, _encode_cas_set

        self.assertEqual(
            _encode_cas_set(b"k", b"v", CAS_ABSENT, namespace=b"users"),
            b"k 5 1 1 A\nusers" + b"kv",
        )

    def test_encodes_tagged_set(self):
        from nanocached._connection import CAS_ABSENT, _encode_cas_set

        self.assertEqual(
            _encode_cas_set(b"k", b"v", CAS_ABSENT, tag=7), b"k 0 1 1 A 7\nkv"
        )

    def test_encodes_tagged_set_with_ttl(self):
        from nanocached._connection import CAS_ABSENT, _encode_cas_set

        self.assertEqual(
            _encode_cas_set(b"k", b"v", CAS_ABSENT, ttl_seconds=60, tag=7),
            b"k 0 1 1 A 60 7\nkv",
        )

    def test_encodes_digest_conditioned_delete(self):
        from nanocached._connection import _encode_cas_delete

        digest = content_digest(b"old").encode()
        self.assertEqual(_encode_cas_delete(b"k", digest), b"x 0 1 %b\nk" % digest)

    def test_encodes_tagged_delete(self):
        from nanocached._connection import _encode_cas_delete

        digest = content_digest(b"old").encode()
        self.assertEqual(
            _encode_cas_delete(b"k", digest, tag=3), b"x 0 1 %b 3\nk" % digest
        )

    def test_encodes_namespaced_delete(self):
        from nanocached._connection import _encode_cas_delete

        digest = content_digest(b"old").encode()
        self.assertEqual(
            _encode_cas_delete(b"k", digest, namespace=b"users"),
            b"x 5 1 %b\nusers" % digest + b"k",
        )


class MultiEncodingTests(unittest.TestCase):
    # Batched get/set (issues #128/#150/#151): docs/protocol.html "m / o
    # — batched get and set" is the authoritative wire spec these pin —
    # like `i`/`k`/`x`, neither op has a legacy uppercase form, so even
    # the default namespace still carries <namespace-length> 0.

    def test_encodes_untagged_multi_get(self):
        from nanocached._connection import _encode_multi_get

        self.assertEqual(_encode_multi_get([b"a", b"bb"]), b"m 0 2 1 2\nabb")

    def test_encodes_namespaced_multi_get(self):
        from nanocached._connection import _encode_multi_get

        self.assertEqual(
            _encode_multi_get([b"a", b"bb"], namespace=b"users"),
            b"m 5 2 1 2\nusersabb",
        )

    def test_encodes_tagged_multi_get(self):
        from nanocached._connection import _encode_multi_get

        self.assertEqual(_encode_multi_get([b"a", b"bb"], tag=7), b"m 0 2 1 2 7\nabb")

    def test_encodes_a_single_key_multi_get(self):
        from nanocached._connection import _encode_multi_get

        self.assertEqual(_encode_multi_get([b"k"]), b"m 0 1 1\nk")

    def test_namespace_may_contain_arbitrary_bytes(self):
        from nanocached._connection import _encode_multi_get

        self.assertEqual(
            _encode_multi_get([b"k"], namespace=b"\xff\x00"), b"m 2 1 1\n\xff\x00k"
        )

    def test_encodes_untagged_multi_set_without_ttl(self):
        from nanocached._connection import _encode_multi_set

        self.assertEqual(
            _encode_multi_set([b"a", b"bb"], [b"1", b"22"]), b"o 0 2 1 1 2 2\na1bb22"
        )

    def test_encodes_multi_set_with_ttl(self):
        from nanocached._connection import _encode_multi_set

        self.assertEqual(
            _encode_multi_set([b"k"], [b"v"], ttl_seconds=60), b"o 0 1 1 1 60\nkv"
        )

    def test_encodes_namespaced_multi_set(self):
        from nanocached._connection import _encode_multi_set

        self.assertEqual(
            _encode_multi_set([b"k"], [b"v"], namespace=b"users"),
            b"o 5 1 1 1\nuserskv",
        )

    def test_encodes_tagged_multi_set_with_ttl(self):
        from nanocached._connection import _encode_multi_set

        self.assertEqual(
            _encode_multi_set([b"k"], [b"v"], ttl_seconds=60, tag=7),
            b"o 0 1 1 1 60 7\nkv",
        )


class CasTests(unittest.IsolatedAsyncioTestCase):
    # Compare-and-set (issue #141): single-node coverage of
    # put_if_absent()/replace_if_present()/replace()/delete_if_matches()
    # and get_with_token() — see CasReplicationTests below for the
    # cluster fan-out contract (primary-only k/x, replica gets a plain
    # set/delete).

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_put_if_absent_succeeds_on_a_missing_key(self):
        client = await self.connect()
        try:
            self.assertTrue(await client.put_if_absent("k", "v"))
            self.assertEqual(await client.get("k"), "v")
            self.assertEqual(self.node.cas_set_count, 1)
        finally:
            await client.close()

    async def test_put_if_absent_fails_when_the_key_already_exists(self):
        client = await self.connect()
        try:
            await client.set("k", "original")
            self.assertFalse(await client.put_if_absent("k", "new"))
            self.assertEqual(await client.get("k"), "original")
        finally:
            await client.close()

    async def test_put_if_absent_honors_ttl(self):
        client = await self.connect()
        try:
            self.assertTrue(await client.put_if_absent("k", "v", ttl_seconds=60))
            self.assertEqual(self.node.entry_ttl[(b"", b"k")], 60)
        finally:
            await client.close()

    async def test_replace_if_present_succeeds_when_the_key_exists(self):
        client = await self.connect()
        try:
            await client.set("k", "original")
            self.assertTrue(await client.replace_if_present("k", "new"))
            self.assertEqual(await client.get("k"), "new")
        finally:
            await client.close()

    async def test_replace_if_present_fails_on_a_missing_key(self):
        client = await self.connect()
        try:
            self.assertFalse(await client.replace_if_present("missing", "new"))
            self.assertIsNone(await client.get("missing"))
        finally:
            await client.close()

    async def test_replace_succeeds_when_the_token_matches(self):
        client = await self.connect()
        try:
            await client.set("k", "original")
            result = await client.get_with_token("k")
            assert result is not None
            _, token = result
            self.assertTrue(await client.replace("k", token, "new"))
            self.assertEqual(await client.get("k"), "new")
        finally:
            await client.close()

    async def test_replace_fails_when_the_token_is_stale(self):
        client = await self.connect()
        try:
            await client.set("k", "original")
            result = await client.get_with_token("k")
            assert result is not None
            _, stale_token = result
            await client.set("k", "changed-out-from-under-us")
            self.assertFalse(await client.replace("k", stale_token, "new"))
            self.assertEqual(await client.get("k"), "changed-out-from-under-us")
        finally:
            await client.close()

    async def test_replace_fails_on_a_missing_key(self):
        client = await self.connect()
        try:
            token = content_digest(b"anything")
            self.assertFalse(await client.replace("missing", token, "new"))
        finally:
            await client.close()

    async def test_delete_if_matches_succeeds_when_the_token_matches(self):
        client = await self.connect()
        try:
            await client.set("k", "v")
            result = await client.get_with_token("k")
            assert result is not None
            _, token = result
            self.assertTrue(await client.delete_if_matches("k", token))
            self.assertIsNone(await client.get("k"))
        finally:
            await client.close()

    async def test_delete_if_matches_fails_when_the_token_is_stale(self):
        client = await self.connect()
        try:
            await client.set("k", "v")
            result = await client.get_with_token("k")
            assert result is not None
            _, stale_token = result
            await client.set("k", "changed")
            self.assertFalse(await client.delete_if_matches("k", stale_token))
            self.assertEqual(await client.get("k"), "changed")
        finally:
            await client.close()

    async def test_delete_if_matches_fails_on_a_missing_key(self):
        client = await self.connect()
        try:
            token = content_digest(b"anything")
            self.assertFalse(await client.delete_if_matches("missing", token))
        finally:
            await client.close()

    async def test_get_with_token_returns_none_on_a_miss(self):
        client = await self.connect()
        try:
            self.assertIsNone(await client.get_with_token("missing"))
        finally:
            await client.close()

    async def test_get_with_token_matches_content_digest_of_the_value(self):
        client = await self.connect()
        try:
            await client.set("k", "hello")
            result = await client.get_with_token("k")
            assert result is not None
            value, token = result
            self.assertEqual(value, b"hello")
            self.assertEqual(token, content_digest(b"hello"))
        finally:
            await client.close()

    async def test_malformed_token_is_rejected_before_touching_the_connection(self):
        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.replace("k", "not-a-digest", "new")
            with self.assertRaises(ValueError):
                await client.replace("k", "ABCDEF0123456789ABCDEF0123456789", "new")  # uppercase
            with self.assertRaises(ValueError):
                await client.delete_if_matches("k", "short")
            self.assertEqual(self.node.cas_set_count, 0)
            self.assertEqual(self.node.cas_delete_count, 0)
        finally:
            await client.close()

    async def test_empty_key_is_rejected_before_touching_the_connection(self):
        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.put_if_absent("", "v")
            self.assertEqual(self.node.cas_set_count, 0)
        finally:
            await client.close()


class NonIdempotentReplayTests(unittest.IsolatedAsyncioTestCase):
    # issue #225: incr()/replace() (and the rest of the CAS family) must
    # never be blindly replayed after a redial the way get/set/delete
    # are — only when the connection died *before* the request's own
    # frame was ever written is a retry safe (the idle-FIN case below);
    # once write()+drain() has returned, the primary may already have
    # applied the request and only the reply was lost, so the client
    # raises ConnectionLostError instead of resending it.

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_incr_redials_and_retries_when_the_connection_was_already_dead(self):
        # The idle-FIN case: the connection was already gone before incr()
        # even tried to use it (nothing was ever sent), so the usual
        # lazy-redial-and-retry still applies, exactly like get/set/delete.
        client = await self.connect()
        try:
            await client.set("counter", "10")
            self.node.drop_connections()
            await wait_for(
                lambda: client._single is not None and client._single.closed,
                "the client to see the FIN",
            )
            self.assertEqual(await client.incr("counter", 5), 15)
            self.assertEqual(self.node.connection_count, 2)
            self.assertEqual(self.node.incr_count, 1)
        finally:
            await client.close()

    async def test_incr_raises_connection_lost_instead_of_replaying_an_applied_increment(self):
        client = await self.connect()
        try:
            await client.set("counter", "10")
            # The node fully applies this increment, then closes the
            # connection instead of ever answering — the reply, not the
            # request, is what gets lost.
            self.node.apply_and_drop_next_incr()
            with self.assertRaises(ConnectionLostError):
                await client.incr("counter", 5)
            # Applied exactly once: a blind retry would have sent a
            # second `i` and landed on 20, not 15.
            self.assertEqual(await client.get("counter"), "15")
            self.assertEqual(self.node.incr_count, 1)
        finally:
            await client.close()

    async def test_a_lost_reply_arriving_while_drain_is_still_suspended_is_not_replayed(self):
        # Issue #412(a): write() can hand a frame's bytes to the OS
        # socket buffer before the following `await writer.drain()`
        # returns. If the connection is poisoned while _send() is still
        # suspended inside that drain() — not after it returns —
        # slot.sent must already read True at that moment, or
        # _error_for classifies the failure as "never left the client"
        # and _with_wrong_node_retry blindly resends it, double-applying
        # the increment. Normally there's no way to observe this window:
        # drain() for a small frame over loopback returns essentially
        # immediately, long before any server-side reaction could poison
        # the connection. So this patches StreamWriter.drain to hold the
        # coroutine open on an Event — an artificial slow-drain
        # transport, mirroring this module's own flaky_write pattern
        # (see test_a_reset_while_writing_the_extended_auth_frame_also_
        # falls_back_to_untagged) for simulating transport-level faults
        # — while the real (unpatched) write() has already handed the
        # frame to the kernel for real, so the node genuinely receives
        # and applies it.
        client = await self.connect()
        try:
            await client.set("counter", "10")
            # The node fully applies the increment, then closes the
            # connection instead of ever sending the `I` reply — so
            # nothing but the read loop noticing that close can resolve
            # the pending future while drain() is held open below.
            self.node.apply_and_drop_next_incr()

            real_drain = asyncio.StreamWriter.drain
            drain_entered = asyncio.Event()
            release_drain = asyncio.Event()

            async def slow_drain(writer):
                drain_entered.set()
                await release_drain.wait()
                await real_drain(writer)

            with mock.patch.object(asyncio.StreamWriter, "drain", slow_drain):
                incr_task = asyncio.ensure_future(client.incr("counter", 5))
                await wait_for(lambda: drain_entered.is_set(), "drain() to be entered")
                # The node's drop must poison the connection while this
                # coroutine is still parked in drain() above — proving
                # the write already reached the server even though
                # drain() itself hasn't returned to _send() yet.
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the dropped connection to be noticed while drain() is still suspended",
                )
                release_drain.set()
                with self.assertRaises(ConnectionLostError):
                    await incr_task

            # Applied exactly once: a blind retry (the pre-fix behavior,
            # since the old code only marked the slot sent after drain()
            # returned — too late here) would have sent a second `i` and
            # landed on 20, not 15.
            self.assertEqual(await client.get("counter"), "15")
            self.assertEqual(self.node.incr_count, 1)
        finally:
            await client.close()

    async def test_replace_redials_and_retries_when_the_connection_was_already_dead(self):
        client = await self.connect()
        try:
            await client.set("k", "original")
            result = await client.get_with_token("k")
            assert result is not None
            _, token = result
            self.node.drop_connections()
            await wait_for(
                lambda: client._single is not None and client._single.closed,
                "the client to see the FIN",
            )
            self.assertTrue(await client.replace("k", token, "new"))
            self.assertEqual(await client.get("k"), "new")
            self.assertEqual(self.node.connection_count, 2)
            self.assertEqual(self.node.cas_set_count, 1)
        finally:
            await client.close()

    async def test_replace_raises_connection_lost_instead_of_replaying_an_applied_cas(self):
        client = await self.connect()
        try:
            await client.set("k", "original")
            result = await client.get_with_token("k")
            assert result is not None
            _, token = result
            # The node fully applies this replace, then closes the
            # connection instead of ever answering `S` — a blind retry
            # would re-evaluate the (now stale) token against the new
            # value and misreport the already-successful CAS as a
            # mismatch (`False`) instead of raising.
            self.node.apply_and_drop_next_cas_set()
            with self.assertRaises(ConnectionLostError):
                await client.replace("k", token, "new")
            self.assertEqual(await client.get("k"), "new")
            self.assertEqual(self.node.cas_set_count, 1)
        finally:
            await client.close()


class CasTaggedTests(unittest.IsolatedAsyncioTestCase):
    # Compare-and-set (issue #141): the S/N/D decode path on a tagged
    # connection (echoed response tags) — see CasTests above for the
    # untagged path.

    async def test_cas_round_trips_over_a_tagged_connection(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                self.assertTrue(await client.put_if_absent("k", "v"))
                self.assertFalse(await client.put_if_absent("k", "v2"))
                result = await client.get_with_token("k")
                assert result is not None
                _, token = result
                self.assertTrue(await client.replace("k", token, "v3"))
                self.assertTrue(await client.delete_if_matches("k", content_digest(b"v3")))
                self.assertFalse(await client.delete_if_matches("k", content_digest(b"v3")))
            finally:
                await client.close()
        finally:
            await node.close()


class CasCompressionTests(unittest.IsolatedAsyncioTestCase):
    # Compare-and-set (issue #141) x value compression: the digest MUST be
    # computed over the raw, marker-prefixed wire bytes get_bytes() taps
    # before decompression — never the decompressed value get()/get_bytes()
    # return — since the server never decompresses and so could never
    # match a digest computed over bytes it never saw.

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def test_token_is_computed_from_the_raw_marker_prefixed_wire_bytes(self):
        client = await NanocachedClient.connect(
            [("127.0.0.1", self.node.port)], compress=True, compression_threshold=1
        )
        try:
            await client.set("k", "value")
            raw_wire_bytes = self.node.store[b"k"]
            self.assertEqual(raw_wire_bytes[0], 0x00)  # too small to shrink under DEFLATE

            result = await client.get_with_token("k")
            assert result is not None
            value, token = result
            self.assertEqual(value, b"value")  # decompressed
            # The token must match a digest of the raw wire bytes, not of
            # the decompressed value it's returned alongside.
            self.assertEqual(token, content_digest(raw_wire_bytes))
            self.assertNotEqual(token, content_digest(b"value"))
        finally:
            await client.close()

    async def test_replace_writes_a_compressed_value_a_plain_client_can_still_read(self):
        client = await NanocachedClient.connect(
            [("127.0.0.1", self.node.port)], compress=True, compression_threshold=1
        )
        try:
            await client.set("k", "original")
            result = await client.get_with_token("k")
            assert result is not None
            _, token = result
            new_value = "y" * 1000  # comfortably above the threshold
            self.assertTrue(await client.replace("k", token, new_value))

            stored = self.node.store[b"k"]
            self.assertEqual(stored[0], 0x01)  # DEFLATE-marked, not raw bytes
            self.assertEqual(await client.get("k"), new_value)
        finally:
            await client.close()


class CasReplicationTests(unittest.IsolatedAsyncioTestCase):
    # Compare-and-set (issue #141): the cluster contract this feature
    # exists for — the primary alone evaluates <cond> via `k`/`x`, and
    # forwards its literal result to the remaining owners as an ordinary
    # set()/delete(), never replaying `k`/`x`.

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

    async def test_the_primary_runs_k_and_the_replica_only_ever_gets_a_set(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "cas-key"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            self.assertTrue(await client.put_if_absent(key, "v1"))

            # The critical assertion: not just that both stores agree on
            # the final value (a buggy replay-on-replica implementation
            # would produce that too), but that the replica never
            # received a `k` frame at all.
            self.assertEqual(primary.cas_set_count, 1)
            self.assertEqual(replica.cas_set_count, 0)
            self.assertEqual(primary.store[key.encode()], b"v1")
            self.assertEqual(replica.store[key.encode()], b"v1")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_condition_mismatch_on_the_primary_never_touches_the_replica(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "already-there"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            await client.set(key, "original")
            primary.cas_set_count = 0
            replica.cas_set_count = 0

            self.assertFalse(await client.put_if_absent(key, "new"))

            self.assertEqual(primary.cas_set_count, 1)
            self.assertEqual(replica.cas_set_count, 0)
            self.assertEqual(primary.store[key.encode()], b"original")
            self.assertEqual(replica.store[key.encode()], b"original")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_the_replicated_set_carries_the_new_ttl(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "cas-key-with-ttl"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            self.assertTrue(await client.put_if_absent(key, "v1", ttl_seconds=120))

            self.assertEqual(replica.last_set_ttl, 120)
            self.assertEqual(replica.entry_ttl[(b"", key.encode())], 120)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_the_primary_runs_x_and_the_replica_only_ever_gets_a_delete(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "cas-delete-key"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            self.assertTrue(await client.put_if_absent(key, "v1"))
            result = await client.get_with_token(key)
            assert result is not None
            _, token = result
            primary.cas_set_count = 0
            replica.cas_set_count = 0

            self.assertTrue(await client.delete_if_matches(key, token))

            self.assertEqual(primary.cas_delete_count, 1)
            self.assertEqual(replica.cas_delete_count, 0)
            self.assertNotIn(key.encode(), primary.store)
            self.assertNotIn(key.encode(), replica.store)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_delete_mismatch_on_the_primary_never_touches_the_replica(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "stale-delete-key"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            self.assertTrue(await client.put_if_absent(key, "v1"))
            stale_token = content_digest(b"not-the-real-value")
            primary.cas_delete_count = 0
            replica.cas_delete_count = 0

            self.assertFalse(await client.delete_if_matches(key, stale_token))

            self.assertEqual(primary.cas_delete_count, 1)
            self.assertEqual(replica.cas_delete_count, 0)
            self.assertEqual(primary.store[key.encode()], b"v1")
            self.assertEqual(replica.store[key.encode()], b"v1")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_wrong_node_triggers_refresh_and_one_retry(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "some-cas-key"
            primary_name, _ = self.owners_of(key)
            primary = nodes[primary_name]

            primary.answer_wrong_node_once()
            self.assertTrue(await client.put_if_absent(key, "v1"))

            primary.answer_wrong_node_once()
            primary.answer_wrong_node_once()
            with self.assertRaises(WrongNodeError):
                await client.put_if_absent(key, "v2")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_lost_reply_after_the_primary_applies_the_cas_is_not_retried_via_ring_refresh(self):
        # issue #225: see IncrReplicationTests' own version of this test
        # — _with_wrong_node_retry is the only retry layer replace() has,
        # and a ConnectionLostError must skip its ring-refresh-and-retry
        # entirely, or a stale token re-evaluated against the value the
        # primary already stored would misreport the just-succeeded CAS
        # as a mismatch instead of raising.
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "cas-key"
            primary_name, replica_name = self.owners_of(key)
            primary, replica = nodes[primary_name], nodes[replica_name]

            await client.set(key, "original")
            result = await client.get_with_token(key)
            assert result is not None
            _, token = result
            primary.cas_set_count = 0
            replica.cas_set_count = 0

            # The primary fully applies this replace, then closes the
            # connection instead of ever answering `S`.
            primary.apply_and_drop_next_cas_set()
            with self.assertRaises(ConnectionLostError):
                await client.replace(key, token, "new")

            self.assertEqual(primary.cas_set_count, 1)
            self.assertEqual(replica.cas_set_count, 0)
            self.assertEqual(primary.store[key.encode()], b"new")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()


class CasNamespaceTests(unittest.IsolatedAsyncioTestCase):
    # Compare-and-set (issue #141): Namespace.put_if_absent()/
    # replace_if_present()/replace()/delete_if_matches()/get_with_token()
    # scope to the namespace the same way get/set/delete already do.

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_round_trips_cas_operations_within_a_namespace(self):
        client = await self.connect()
        try:
            users = client.namespace("users")
            self.assertTrue(await users.put_if_absent("k", "v1"))
            # The default namespace's own "k" is a separate entry.
            self.assertIsNone(await client.get("k"))

            self.assertTrue(await users.replace_if_present("k", "v2"))
            result = await users.get_with_token("k")
            assert result is not None
            value, token = result
            self.assertEqual(value, b"v2")

            self.assertTrue(await users.replace("k", token, "v3"))
            self.assertEqual(await users.get("k"), "v3")

            self.assertTrue(await users.delete_if_matches("k", content_digest(b"v3")))
            self.assertIsNone(await users.get("k"))
        finally:
            await client.close()


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
                await client.close()
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
                await client.close()
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
                await client.close()
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
                await client.close()
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
            await client.close()
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
            await client.close()
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
            await client.close()
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
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


class FireAndForgetReplicaWritesTests(unittest.IsolatedAsyncioTestCase):
    # Fire-and-forget replica writes

    # A "did it wait for the mock's delay" assertion can't compare the
    # measured elapsed time against the delay exactly: asyncio.sleep()'s
    # wakeup and loop.time()'s measurement can land within one
    # clock_resolution of each other, so a 0.08s delay can be observed as
    # slightly under 0.08s. Slack the lower bound by this much rather than
    # asserting on the boundary; still miles away from the ~0s an
    # immediate return would show.
    TIMING_TOLERANCE_S = 0.02

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
                self.assertGreaterEqual(elapsed, 0.08 - self.TIMING_TOLERANCE_S)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_cancelling_a_write_does_not_wait_for_the_replica_leg(self):
        # asyncio.wait_for around set() must bound the call: a cancellation
        # delivered while the primary write is in flight used to be caught
        # (CancelledError is a BaseException) and held until every
        # synchronous replica leg had finished too.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                primary, replica = self.owners_of("k")
                nodes[primary].delay_sets(0.3)
                nodes[replica].delay_sets(0.3)

                start = asyncio.get_running_loop().time()
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.set("k", "v"), timeout=0.05)
                elapsed = asyncio.get_running_loop().time() - start
                self.assertLess(elapsed, 0.3)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_close_still_drains_a_replica_leg_orphaned_by_a_cancelled_write(self):
        # Issue #189: the CancelledError path above (previous test) used
        # to re-raise immediately without registering the still-running
        # synchronous replica leg anywhere, leaving it a true orphan —
        # nothing referenced it, so close()'s _teardown() could yank the
        # connection out from under a write that was already on its way
        # to succeeding. It must instead land on the replica, same as if
        # the cancellation had never happened, with close() waiting for
        # it via the same background-replica-write pool fire_and_forget
        # dispatch uses.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            primary, replica = self.owners_of("k")
            nodes[primary].delay_sets(0.3)
            nodes[replica].delay_sets(0.08)

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(client.set("k", "v"), timeout=0.05)

            # The replica leg is still in flight at this point (0.08s
            # hasn't elapsed) with nothing awaiting it yet — before the
            # fix this raced _teardown() below and warned "Task was
            # destroyed but it is pending!" once the loop tore down.
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                await client.close()
                # Give a torn-down connection's socket-close callbacks
                # (and any leftover task destruction) a chance to run and
                # emit their warning before it's checked below.
                await asyncio.sleep(0)

            self.assertIn(
                b"k",
                nodes[replica].store,
                "close() tore down the connection before the orphaned replica leg finished",
            )
            self.assertNotIn("destroyed", captured.getvalue())
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
                await client.close()
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

                self.assertTrue(any(e >= 0.15 - self.TIMING_TOLERANCE_S for e in elapsed), elapsed)
                self.assertTrue(any(e < 0.15 for e in elapsed), elapsed)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_close_does_not_spin_when_every_background_write_has_already_finished(self):
        # Regression (issue #65): close() drained _background_replica_writes
        # with "while set: await gather(set)" and relied on each task's
        # add_done_callback(discard) hook to empty the set — but that hook
        # runs via call_soon, only once the loop gets control back, while
        # awaiting already-finished tasks completes without ever yielding.
        # With every member finished and its hook still queued, close()
        # re-checked the same non-empty set forever, synchronously,
        # freezing the entire event loop. Reproduced exactly: a finished
        # task whose discard hook is registered after the fact (so it sits
        # in the loop's ready queue), then close() called straight away.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], fire_and_forget_replicas=True
            )

            async def finished_write() -> None:
                pass

            task = asyncio.ensure_future(finished_write())
            await task
            client._background_replica_writes.add(task)
            # On a finished future this schedules the hook with call_soon
            # rather than running it — the state the spin needed.
            task.add_done_callback(client._background_replica_writes.discard)

            # A synchronous spin can't be interrupted by asyncio.wait_for
            # (the loop never runs again), but Python still delivers
            # signals between bytecodes — so a SIGALRM turns a hang into a
            # failure instead of a stuck test run.
            import signal

            def alarm(_signum, _frame):
                raise AssertionError("close() spun without yielding to the event loop (issue #65)")

            previous = signal.signal(signal.SIGALRM, alarm)
            signal.setitimer(signal.ITIMER_REAL, 2.0)
            try:
                await client.close()
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous)

            self.assertEqual(client._background_replica_writes, set())
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_write_starting_after_close_falls_back_to_synchronous_instead_of_registering(self):
        # Regression (issue #47 audit item 4): the fire_and_forget_replicas
        # admission check in _write() must recheck self._closed immediately
        # before registering a background task. Without it, a write that
        # reaches this point after a concurrent close() has already set
        # self._closed (but is still draining, or has already finished
        # draining and moved on to teardown) could add an entry close()
        # will never await — leaking it past teardown instead of falling
        # back to the synchronous path close() doesn't need to wait for.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], fire_and_forget_replicas=True
            )
            client._closed = True  # simulate close() having already started
            try:
                await client._write(b"", b"k", lambda connection: connection.set(b"k", b"v", 0))
            finally:
                client._closed = False  # let the real close() below run cleanly

            self.assertEqual(client._background_replica_writes, set())
            _, replica = self.owners_of("k")
            self.assertIn(b"k", nodes[replica].store, "the replica leg did not run synchronously")
        finally:
            await client.close()
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
            # The drain contract (fire-and-forget replica writes as amended by issue #47 item
            # 3): close() returns only after the in-flight replica write
            # finished.
            await client.close()
            self.assertIn(
                b"k",
                nodes[replica].store,
                "close() returned before the background replica write finished",
            )
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


class ReadRepairTests(unittest.IsolatedAsyncioTestCase):
    # Read repair

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

    async def test_by_default_a_clean_miss_on_the_primary_is_not_repaired(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                primary, replica = self.owners_of("k")
                nodes[replica].store[b"k"] = b"from-replica"

                self.assertIsNone(await client.get_bytes("k"))
                self.assertNotIn(b"k", nodes[primary].store)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_finds_a_value_on_a_replica_and_repairs_the_primary(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_repair=True
            )
            try:
                primary, replica = self.owners_of("k")
                nodes[replica].store[b"k"] = b"from-replica"

                self.assertEqual(await client.get_bytes("k"), b"from-replica")
                await wait_for(lambda: b"k" in nodes[primary].store, "the primary to be repaired")
                # The original TTL can't be recovered from a GET; a
                # repair must not use TTL 0 (no expiry), which would
                # permanently resurrect already-expired data — see
                # _READ_REPAIR_TTL in client.py.
                self.assertEqual(nodes[primary].last_set_ttl, 60)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_stays_a_clean_miss_when_no_owner_has_the_value(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_repair=True
            )
            try:
                self.assertIsNone(await client.get_bytes("nowhere"))
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_close_drains_an_in_flight_read_repair_write_back(self):
        # The write-back is detached from get_bytes()'s caller, but it must
        # still be tracked so close() can drain it (same drain contract as
        # fire_and_forget_replicas — fire-and-forget replica writes as amended by issue
        # #47 item 3) instead of leaving it dangling past teardown.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_repair=True
            )
            primary, replica = self.owners_of("k")
            nodes[replica].store[b"k"] = b"from-replica"
            nodes[primary].delay_sets(0.08)

            self.assertEqual(await client.get_bytes("k"), b"from-replica")
            await client.close()
            self.assertIn(
                b"k",
                nodes[primary].store,
                "close() returned before the read-repair write-back finished",
            )
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_repair_starting_after_close_is_skipped_instead_of_registering(self):
        # Regression (issue #47 audit item 4), the _try_read_repair()
        # counterpart of FireAndForgetReplicaWritesTests's equivalent test:
        # a read-repair write-back has no synchronous fallback (it's
        # opportunistic), so once self._closed is set — rechecked
        # immediately before registering the background task — the repair
        # for this miss must simply be skipped rather than adding an entry
        # a concurrent close() would never await.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_repair=True
            )
            primary, replica = self.owners_of("k")
            nodes[replica].store[b"k"] = b"from-replica"

            client._closed = True  # simulate close() having already started
            try:
                value = await client._try_read_repair(b"", b"k")
            finally:
                client._closed = False  # let the real close() below run cleanly

            self.assertEqual(value, b"from-replica")  # the probe itself still ran
            self.assertEqual(client._background_replica_writes, set())
            self.assertNotIn(b"k", nodes[primary].store, "the repair write-back should have been skipped")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()


class SharedBackgroundReplicaPoolTests(unittest.IsolatedAsyncioTestCase):
    # Fire-and-forget replica writes as extended to read repair (issue #47 audit
    # item 5): fire_and_forget_replicas writes and read-repair write-backs
    # draw from ONE shared admission pool of at most
    # _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES, not one independent cap per
    # category.

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

    async def test_a_full_fire_and_forget_pool_also_blocks_a_read_repair_write_back(self):
        self._client_module._MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES = 1

        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)],
                fire_and_forget_replicas=True,
                read_repair=True,
            )
            try:
                _, replica1 = self.owners_of("k1")
                primary2, replica2 = self.owners_of("k2")
                nodes[replica1].delay_sets(0.15)
                nodes[replica2].store[b"k2"] = b"from-replica"

                # Occupies the single shared slot with a fire_and_forget
                # replica write that won't finish for 0.15s.
                await client.set("k1", "v")
                self.assertEqual(len(client._background_replica_writes), 1)

                # A concurrent read-repair write-back finds a value fine
                # (the owner probe doesn't need a slot), but must not get a
                # slot of its own to write it back — if the two categories
                # had independent caps, this would still succeed.
                self.assertEqual(await client.get_bytes("k2"), b"from-replica")
                self.assertNotIn(
                    b"k2",
                    nodes[primary2].store,
                    "read-repair write-back ran despite the shared pool being full",
                )
                self.assertEqual(len(client._background_replica_writes), 1)

                # Once the slot frees up, the same miss is opportunistically
                # repairable again on a later read.
                _, replica1 = self.owners_of("k1")
                await wait_for(
                    lambda: b"k1" in nodes[replica1].store,
                    "the fire-and-forget replica write to finish and free its slot",
                )
                self.assertEqual(await client.get_bytes("k2"), b"from-replica")
                await wait_for(
                    lambda: b"k2" in nodes[primary2].store,
                    "the read-repair write-back to land once the shared pool had room",
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


class HedgedReadTests(unittest.IsolatedAsyncioTestCase):
    """Hedged reads (issue #64): read_hedge_after sends a read to the next
    owner when the primary hasn't answered in time — a slow node no longer
    bounds every read that touches it."""

    TIMING_TOLERANCE_S = 0.03

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

    async def timed(self, coroutine):
        start = asyncio.get_running_loop().time()
        result = await coroutine
        return result, asyncio.get_running_loop().time() - start

    async def test_rejects_a_non_positive_hedge(self):
        for bad in (0, -0.1):
            with self.assertRaises(ValueError):
                await NanocachedClient.connect([("127.0.0.1", 1)], read_hedge_after=bad)

    async def test_a_hit_from_the_replica_wins_over_a_slow_primary(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.05
            )
            try:
                await client.set("k", "v")
                primary, replica = self.owners_of("k")
                nodes[primary].delay_gets(0.4)

                value, elapsed = await self.timed(client.get("k"))

                self.assertEqual(value, "v")
                self.assertLess(elapsed, 0.4 - self.TIMING_TOLERANCE_S, elapsed)
                self.assertGreaterEqual(elapsed, 0.05 - self.TIMING_TOLERANCE_S, elapsed)
                self.assertEqual(nodes[replica].get_count, 1, "the replica should have been hedged to")
            finally:
                await client.close()
            # The slow primary's leg was left to finish, not cancelled, and
            # close() drained it.
            self.assertEqual(client._hedged_reads, set())
            self.assertEqual(nodes[primary].get_count, 1)
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_fast_primary_is_never_hedged(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.05
            )
            try:
                await client.set("k", "v")
                _, replica = self.owners_of("k")
                for _ in range(5):
                    self.assertEqual(await client.get("k"), "v")
                self.assertEqual(nodes[replica].get_count, 0)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_replica_miss_waits_for_the_primary(self):
        # Hedging must never turn a hit into a miss: the replica lacks the
        # copy and answers first, but the primary's answer is what counts.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.05
            )
            try:
                await client.set("k", "v")
                primary, replica = self.owners_of("k")
                del nodes[replica].store[b"k"]
                nodes[primary].delay_gets(0.2)

                value, elapsed = await self.timed(client.get("k"))

                self.assertEqual(value, "v")
                self.assertGreaterEqual(elapsed, 0.2 - self.TIMING_TOLERANCE_S, elapsed)
                self.assertEqual(nodes[replica].get_count, 1)

                # A key nobody has: the miss is accepted once the primary
                # has answered it too.
                self.assertIsNone(await client.get("absent"))
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_off_by_default_a_slow_primary_bounds_the_read(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                await client.set("k", "v")
                primary, replica = self.owners_of("k")
                nodes[primary].delay_gets(0.2)

                value, elapsed = await self.timed(client.get("k"))

                self.assertEqual(value, "v")
                self.assertGreaterEqual(elapsed, 0.2 - self.TIMING_TOLERANCE_S, elapsed)
                self.assertEqual(nodes[replica].get_count, 0)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_dead_primary_fails_over_immediately(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect(
            [("127.0.0.1", discovery.port)], read_hedge_after=0.5
        )
        try:
            await client.set("k", "v")
            primary, _ = self.owners_of("k")
            await nodes[primary].close()
            await wait_for(
                lambda: client._members[primary].connection.closed,
                "the client to see the FIN",
            )

            value, elapsed = await self.timed(client.get("k"))

            self.assertEqual(value, "v")
            self.assertLess(elapsed, 0.5 - self.TIMING_TOLERANCE_S, elapsed)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_hedge_leg_racing_close_is_refused_not_registered(self):
        # Issue #91: a read that passed its own closed-check can reach the
        # hedge-leg registration only after close() has set _closed and
        # drained _hedged_reads (e.g. it yielded in _maybe_refresh while
        # close() ran). start() must recheck _closed so it never registers a
        # leg the drain has already passed — which would then dial against a
        # connection teardown is closing. Set _closed directly to reproduce
        # exactly the state start() sees at that point.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.05
            )
            await client.set("k", "v")
            names = client._owner_names(b"", b"k")
            self.assertGreater(len(names), 1)

            async def op(connection):
                raise AssertionError("the leg must never be dialed after close() began")

            client._closed = True
            with self.assertRaises(AlreadyClosedError):
                await client._read_hedged(b"k", op, names)
            self.assertEqual(
                client._hedged_reads,
                set(),
                "no hedge leg may be registered after close() began",
            )

            # Restore so close() runs its real teardown rather than the
            # already-closed warning-and-return path.
            client._closed = False
            await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_both_legs_done_in_the_same_wait_leaves_no_leak(self):
        # Issue #229: asyncio.wait(FIRST_COMPLETED) can hand back more than
        # one finished task in `done` in a single call — e.g. the primary
        # and the hedge leg both failing around the same event-loop tick,
        # as a dying node can. The old loop discarded each task from
        # self._hedged_reads at the top of its own iteration, but a
        # decisive outcome (a raise, or a return) exited the for loop
        # immediately — skipping the discard for any other already-done
        # task the for loop hadn't reached yet. Both legs here are made
        # individually decisive (one returns, one raises WrongNodeError) so
        # the leak reproduces regardless of the set's (unspecified)
        # iteration order: whichever is visited first ends the loop before
        # the other is ever discarded, in the pre-fix code.
        #
        # asyncio.wait's FIRST_COMPLETED still only reports tasks that were
        # *already done* at the moment it runs its final done-check, so
        # asyncio.wait itself is patched to await both legs to completion
        # before delegating to the real asyncio.wait — this makes "both
        # legs finish in the same wait() call" deterministic instead of
        # depending on a real-time race between two sleeps.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.01
            )
            try:
                await client.set("k", "v")
                names = client._owner_names(b"", b"k")
                self.assertGreater(len(names), 1)

                hedge_started = asyncio.Event()

                async def op(connection):
                    if not hedge_started.is_set():
                        # The primary leg: block until the hedge leg has
                        # started, then answer with a genuine hit — decisive
                        # because it's the primary (index 0).
                        await hedge_started.wait()
                        return "v"
                    # The hedge leg: answer immediately with a decisive
                    # failure of its own.
                    raise WrongNodeError()

                real_wait = asyncio.wait

                async def patched_wait(fs, **kwargs):
                    fs = list(fs)
                    if len(fs) == 2:
                        hedge_started.set()
                        await asyncio.gather(*fs, return_exceptions=True)
                    return await real_wait(fs, **kwargs)

                # Which of the two decisive outcomes actually wins (the
                # primary's hit, or the hedge leg's WrongNodeError) depends
                # on the set's iteration order, which this test doesn't
                # control — both are decisive so the leak reproduces either
                # way; only the post-call _hedged_reads state matters here.
                with mock.patch("asyncio.wait", patched_wait):
                    try:
                        await client._read_hedged(b"k", op, names)
                    except WrongNodeError:
                        pass

                self.assertEqual(
                    client._hedged_reads,
                    set(),
                    "both legs finished in the same wait() call, but one "
                    "was left behind uncounted",
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_replica_hit_beats_the_primarys_miss_in_the_same_wait(self):
        # Issue #387: when the primary's miss and a replica's hit land in
        # the same asyncio.wait(FIRST_COMPLETED) batch, the old loop
        # decided on whichever task the set happened to iterate first —
        # visiting the primary first returned None for a key a replica had
        # just answered, violating the "hedging never turns a hit into a
        # miss" contract (a primary that lost the key to eviction/restart
        # while a replica still holds it). The whole batch must be
        # evaluated first, with a hit from any owner winning. Same
        # patched-wait construction as
        # test_both_legs_done_in_the_same_wait_leaves_no_leak, so both
        # legs deterministically finish in one wait() call.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.01
            )
            try:
                await client.set("k", "v")
                names = client._owner_names(b"", b"k")
                self.assertGreater(len(names), 1)

                hedge_started = asyncio.Event()

                async def op(connection):
                    if not hedge_started.is_set():
                        # The primary leg: block until the hedge leg has
                        # started, then answer with a miss — decisive on
                        # its own (index 0), but it must not outrank the
                        # replica's hit below.
                        await hedge_started.wait()
                        return None
                    # The hedge leg: a genuine hit.
                    return "v"

                real_wait = asyncio.wait

                async def patched_wait(fs, **kwargs):
                    fs = list(fs)
                    if len(fs) == 2:
                        hedge_started.set()
                        await asyncio.gather(*fs, return_exceptions=True)
                    return await real_wait(fs, **kwargs)

                with mock.patch("asyncio.wait", patched_wait):
                    self.assertEqual(
                        await client._read_hedged(b"k", op, names), "v"
                    )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_cancelling_a_hedged_read_does_not_leak_the_running_legs(self):
        # Issue #324: asyncio.wait_for(client.get(...), timeout) (or any
        # other cancellation of the caller's await) used to be caught by
        # nothing in _read_hedged's main loop — CancelledError propagated
        # straight out, skipping the detach()/resolve_losers() call that is
        # the only thing that ever arranges for a still-running leg to be
        # discarded from self._hedged_reads once it finishes. Both legs
        # here are still running when the cancellation lands; once each
        # eventually finishes on its own, it must still be gone from
        # self._hedged_reads instead of sitting there forever.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.02
            )
            try:
                await client.set("k", "v")
                primary, replica = self.owners_of("k")
                nodes[primary].delay_gets(0.3)
                nodes[replica].delay_gets(0.3)

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.get("k"), timeout=0.05)

                # Both legs are still in flight at this point (0.3s hasn't
                # elapsed) but already registered by start() above.
                self.assertEqual(len(client._hedged_reads), 2)

                await wait_for(
                    lambda: client._hedged_reads == set(),
                    "both orphaned legs to finish and be discarded",
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_close_still_drains_a_leg_orphaned_by_a_cancelled_hedged_read(self):
        # Companion to the previous test, mirroring
        # test_close_still_drains_a_replica_leg_orphaned_by_a_cancelled_write
        # (issue #189) for hedged reads: close() must still wait for the
        # orphaned legs via _hedged_reads' drain instead of racing
        # _teardown() against a task nothing else references.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.02
            )
            primary, replica = self.owners_of("k")
            await client.set("k", "v")
            nodes[primary].delay_gets(0.3)
            nodes[replica].delay_gets(0.08)

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(client.get("k"), timeout=0.05)

            # The replica leg is still in flight at this point (0.08s
            # hasn't elapsed) with nothing awaiting it yet.
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                await client.close()
                await asyncio.sleep(0)

            self.assertNotIn("destroyed", captured.getvalue())
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


class HedgedReadInflightCapTests(unittest.IsolatedAsyncioTestCase):
    """issue #192: _MAX_INFLIGHT_HEDGE_LOSER_LEGS bounds how many losing
    hedge legs run detached in the background, mirroring
    _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES for fire-and-forget replica
    writes — past the cap, a read's remaining legs are awaited
    synchronously right there instead of being left running unbounded."""

    TIMING_TOLERANCE_S = 0.03

    def setUp(self):
        from nanocached import client as client_module

        self._client_module = client_module
        self._default_cap = client_module._MAX_INFLIGHT_HEDGE_LOSER_LEGS

    def tearDown(self):
        self._client_module._MAX_INFLIGHT_HEDGE_LOSER_LEGS = self._default_cap

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

    async def test_falls_back_to_synchronous_past_the_cap(self):
        self._client_module._MAX_INFLIGHT_HEDGE_LOSER_LEGS = 0

        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.05
            )
            try:
                await client.set("k", "v")
                primary, replica = self.owners_of("k")
                nodes[primary].delay_gets(0.3)

                start = asyncio.get_running_loop().time()
                value = await client.get("k")
                elapsed = asyncio.get_running_loop().time() - start

                self.assertEqual(value, "v")
                self.assertEqual(nodes[replica].get_count, 1, "the replica should have been hedged to")
                # With no room under the cap, the slow primary's losing
                # leg is awaited right here instead of being detached —
                # the call doesn't return until it finishes, unlike the
                # default-cap case (HedgedReadTests, same scenario) where
                # it's left running in the background.
                self.assertGreaterEqual(elapsed, 0.3 - self.TIMING_TOLERANCE_S, elapsed)
                self.assertEqual(nodes[primary].get_count, 1)
                self.assertEqual(
                    client._hedged_reads,
                    set(),
                    "the awaited loser must already be gone, not left for close() to drain",
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_cancelling_the_synchronous_wait_past_the_cap_does_not_leak_the_leg(self):
        # issue #364, follow-up to #324's fix (PR #352): resolve_losers's
        # over-cap branch discards its tasks from self._hedged_reads
        # *before* synchronously awaiting them. If that wait is itself
        # what gets cancelled (the caller gave up while resolve_losers was
        # joining an over-cap batch), the outer loop's own
        # `except asyncio.CancelledError: detach(pending)` finds those
        # tasks already missing from self._hedged_reads — add_done_callback
        # there is a no-op on a set that no longer holds them — so the
        # still-running leg never makes it into close()'s drain. The fix
        # re-adds the tasks to self._hedged_reads before re-raising so
        # detach() has something to act on, same as an ordinary loser.
        self._client_module._MAX_INFLIGHT_HEDGE_LOSER_LEGS = 0

        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], read_hedge_after=0.02
            )
            try:
                await client.set("k", "v")
                primary, replica = self.owners_of("k")
                nodes[primary].delay_gets(0.3)

                # The replica answers right away once hedged to (a
                # decisive outcome), leaving the still-slow primary as the
                # loser resolve_losers's over-cap branch synchronously
                # joins — that join is what asyncio.wait_for below
                # interrupts, well before the primary's 0.3s delay elapses.
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.get("k"), timeout=0.08)

                self.assertEqual(
                    len(client._hedged_reads),
                    1,
                    "the orphaned over-cap leg must still be registered for close() to drain",
                )

                await wait_for(
                    lambda: client._hedged_reads == set(),
                    "the orphaned leg to finish on its own and be discarded",
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


class TolerantBootstrapTests(unittest.IsolatedAsyncioTestCase):
    """Issue #67: connect() must tolerate a node that discovery still lists
    but that can't be reached (dead, not yet evicted), the way steady-state
    requests already do — and fail only when no node is reachable."""

    async def start_cluster(self, dead: set[str]):
        nodes = {}
        entries = []
        for name in NAMES:
            if name in dead:
                entries.append((name, f"127.0.0.1:{await unused_port()}"))
            else:
                node = await MockNode().start()
                nodes[name] = node
                entries.append((name, node.address))
        discovery = await MockDiscovery(entries, replication=2).start()
        return nodes, discovery

    def owners_of(self, key: str):
        return HashRing(NAMES).owners(key.encode(), 2)

    def key_with_primary(self, name: str) -> str:
        for i in range(1000):
            key = f"key-{i}"
            if self.owners_of(key)[0] == name:
                return key
        raise AssertionError("no key routes to " + name)

    async def test_connect_succeeds_with_one_unreachable_node(self):
        dead, live = NAMES[0], NAMES[1]
        nodes, discovery = await self.start_cluster({dead})
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], reconnect_cooldown=0.05
            )
            try:
                self.assertEqual(client.replication, 2)
                self.assertIsNone(client._members[dead].connection)
                self.assertIsNotNone(client._members[live].connection)

                # A key whose primary is alive: the write lands, the dead
                # replica leg is swallowed and counted, the read hits.
                key = self.key_with_primary(live)
                await client.set(key, "v")
                self.assertEqual(await client.get(key), "v")
                self.assertEqual(client.stats().replica_write_failures, 1)

                # A key whose primary is the dead node: the read fails over
                # to the live replica right away (cooldown, no dial).
                other = self.key_with_primary(dead)
                nodes[live].store[other.encode()] = b"replica copy"
                start = asyncio.get_running_loop().time()
                self.assertEqual(await client.get(other), "replica copy")
                self.assertLess(asyncio.get_running_loop().time() - start, 0.5)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_connect_fails_only_when_every_node_is_unreachable(self):
        nodes, discovery = await self.start_cluster(set(NAMES))
        try:
            with self.assertRaises((ConnectionError, OSError)):
                await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_an_unreachable_node_is_redialed_once_the_cooldown_has_passed(self):
        dead, live = NAMES[0], NAMES[1]
        nodes, discovery = await self.start_cluster({dead})
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], reconnect_cooldown=0.05
            )
            try:
                # Bring the "dead" node up on the address discovery listed.
                dead_address = client._members[dead].address
                host, port = dead_address.rsplit(":", 1)
                revived = await MockNode().start(port=int(port))
                nodes[dead] = revived
                await asyncio.sleep(0.1)

                key = self.key_with_primary(dead)
                await client.set(key, "v")
                self.assertIn(key.encode(), revived.store)
                self.assertIsNotNone(client._members[dead].connection)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_refresh_purges_cooldowns_for_departed_addresses(self):
        # #96: a node that leaves the cluster must not leave its per-address
        # reconnect-cooldown entry behind — in a churny deployment (fresh
        # IP:port per restart) those would accumulate unboundedly.
        dead, live = NAMES[0], NAMES[1]
        nodes, discovery = await self.start_cluster({dead})
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], reconnect_cooldown=100
            )
            try:
                dead_address = client._members[dead].address
                # The unreachable node armed its cooldown at bootstrap.
                self.assertIn(dead_address, client._redial_cooldowns)

                # Discovery now drops the dead node from the roster; the next
                # refresh reconciles membership and must purge its cooldown.
                discovery.nodes = [(live, nodes[live].address)]
                await client._maybe_refresh(force=True)

                self.assertNotIn(dead, client._members)
                self.assertNotIn(dead_address, client._redial_cooldowns)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_cooldown_reraise_does_not_grow_the_traceback(self):
        # #96: re-raising the *stored* exception on every cooldown hit
        # splices a fresh traceback segment onto it each time, growing it
        # without bound for the life of the cooldown. The fix resets the
        # traceback before re-raising, so its depth stays flat.
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                address = "127.0.0.1:1"
                stored = ConnectionError("boom")
                loop = asyncio.get_running_loop()
                client._redial_cooldowns[address] = (loop.time() + 100, stored)

                # A plain try/except, not assertRaises: assertRaises clears
                # the caught exception's __traceback__ on exit (to break a
                # reference cycle), which would also reset the growth this
                # test is meant to observe.
                depths = []
                for _ in range(50):
                    try:
                        await client._redial("some-slot", address)
                    except ConnectionError as error:
                        self.assertIs(error, stored)
                        depths.append(len(traceback.extract_tb(error.__traceback__)))

                # Without the fix, each hit adds frames; with it, depth is flat.
                self.assertLessEqual(max(depths) - min(depths), 1, depths)
            finally:
                await client.close()
        finally:
            await node.close()


class StatsTests(unittest.IsolatedAsyncioTestCase):
    # stats()/ClientStats: observability for failures swallowed by design
    # (client-side replication / fire-and-forget replica writes / read repair).

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

    async def test_starts_at_zero(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                self.assertEqual(
                    client.stats(),
                    ClientStats(
                        replica_write_failures=0,
                        read_repair_failures=0,
                        refresh_failures=0,
                        transient_retries=0,
                    ),
                )
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_counts_a_swallowed_replica_write_failure_when_a_replica_is_dead(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "written-despite-dead-replica"
            primary, replica = self.owners_of(key)
            await nodes[replica].close()
            await wait_for(
                lambda: client._members[replica].connection.closed,
                "the client to see the FIN",
            )

            self.assertEqual(client.stats().replica_write_failures, 0)
            await client.set(key, "v")
            self.assertIn(key.encode(), nodes[primary].store)
            self.assertEqual(client.stats().replica_write_failures, 1)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_a_failed_owner_probe_is_swallowed_but_not_counted(self):
        # Issue #43: read_repair_failures counts failed repair
        # *write-backs* only, matching the other five SDKs — a failed
        # owner probe during the repair scan is swallowed silently.
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect(
            [("127.0.0.1", discovery.port)], read_repair=True
        )
        try:
            key = "read-repair-swallow"
            primary, replica = self.owners_of(key)
            await nodes[replica].close()
            await wait_for(
                lambda: client._members[replica].connection.closed,
                "the client to see the FIN",
            )

            # The primary reports a clean miss, then read repair probes
            # the (dead) replica and swallows the resulting connection
            # failure — without counting it.
            self.assertIsNone(await client.get_bytes(key))
            self.assertEqual(client.stats().read_repair_failures, 0)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_a_failed_repair_write_back_increments_read_repair_failures(self):
        # Issue #43: the write-back leg is what the counter measures —
        # the replica's value is still returned to the caller, but the
        # background repair write to the primary fails and is counted.
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect(
            [("127.0.0.1", discovery.port)], read_repair=True
        )
        try:
            key = "read-repair-write-back"
            primary, replica = self.owners_of(key)
            nodes[replica].store[key.encode()] = b"from-replica"
            # GETs against the primary keep missing normally; only the
            # repair's background S is answered with W and swallowed.
            nodes[primary].answer_wrong_node_on_set_once()

            self.assertEqual(client.stats().read_repair_failures, 0)
            self.assertEqual(await client.get_bytes(key), b"from-replica")
            await wait_for(
                lambda: client.stats().read_repair_failures >= 1,
                "the failed repair write-back to be counted",
            )
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_counts_a_swallowed_refresh_failure_for_an_unreachable_discovery_seed(self):
        node = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], node.address)]).start()
        dead = await unused_port()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", dead), ("127.0.0.1", discovery.port)]
            )
            try:
                self.assertEqual(client.stats().refresh_failures, 0)
                # Forces a fresh _fetch_node_list walk: the dead port fails
                # first, counted as a refresh failure, before discovery
                # answers.
                await client._refresh_node_list()
                self.assertEqual(client.stats().refresh_failures, 1)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await node.close()

    async def test_a_programming_error_from_a_replica_leg_does_not_swallow_or_clobber_a_successful_write(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "boom-key"
            primary, replica = self.owners_of(key)

            # Establish real connections to both owners first...
            await client.set(key, "v")
            # ...then stub the replica's own connection to simulate a bug
            # in this SDK's own code, e.g. a TypeError from a bad internal
            # call — this must NOT be swallowed the same way a dead
            # replica is, but it also must not clobber the primary's own
            # successful result: the write already completed at the
            # primary by the time the replica leg's bug surfaces, so
            # set() must return normally rather than raise.
            replica_connection = client._members[replica].connection

            async def boom(key: bytes, value: bytes, ttl_seconds: int, namespace: bytes = b"") -> None:
                raise TypeError("injected programming bug")

            replica_connection.set = boom

            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                await client.set(key, "v2")  # must not raise despite the replica bug
            self.assertIn("injected programming bug", captured.getvalue())
            self.assertEqual(
                client.stats().replica_write_failures,
                0,
                "a programming error must not be counted as a swallow",
            )
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_a_replica_bug_propagates_when_the_primary_also_fails(self):
        # Issue #3 (audit finding): when the primary leg fails with a
        # swallowable error (here, a dead node the retry layer would
        # otherwise just redial around) *and* a replica leg raises a
        # genuine programming bug, the bug must not be buried in a
        # stderr warning behind the primary's own — comparatively
        # mundane — failure; it must propagate instead. Mirrors the
        # TypeScript SDK's writeToOwners (`replicaBug ? replicaBug.reason
        # : primary.error` — see its own comment for the reasoning: a
        # replica-leg bug is a strictly worse sign than an error this SDK
        # already treats as swallowable at every other by-design swallow
        # site).
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            key = "both-legs-fail"
            primary, replica = self.owners_of(key)

            # Establish real connections to both owners first...
            await client.set(key, "v")

            # ...then kill the primary's node outright, so the primary
            # leg fails with a swallowable connection error (ECONNREFUSED
            # on redial) instead of succeeding...
            await nodes[primary].close()
            await wait_for(
                lambda: client._members[primary].connection.closed,
                "the client to see the FIN",
            )

            # ...and stub the replica's own connection to simulate a bug
            # in this SDK's own code.
            replica_connection = client._members[replica].connection

            async def boom(key: bytes, value: bytes, ttl_seconds: int, namespace: bytes = b"") -> None:
                raise TypeError("injected programming bug")

            replica_connection.set = boom

            with self.assertRaisesRegex(TypeError, "injected programming bug"):
                await client.set(key, "v2")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass


class NamespaceTests(unittest.IsolatedAsyncioTestCase):
    # Namespaces (issue #105): NanocachedClient.namespace() and the
    # Namespace handle it returns — single-node coverage. See
    # NamespaceClusterTests below for routing/replication/W
    # refresh-and-retry, which need an actual cluster.

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_namespace_accessor_exposes_the_encoded_bytes(self):
        client = await self.connect()
        try:
            self.assertEqual(client.namespace("users").namespace, b"users")
            self.assertEqual(client.namespace(b"\xff\x00").namespace, b"\xff\x00")
            self.assertEqual(client.namespace("").namespace, b"")
        finally:
            await client.close()

    async def test_round_trips_set_get_delete_within_a_namespace(self):
        client = await self.connect()
        try:
            users = client.namespace("users")
            await users.set("greeting", "hello")
            self.assertEqual(await users.get("greeting"), "hello")
            self.assertTrue(await users.delete("greeting"))
            self.assertIsNone(await users.get("greeting"))
            self.assertFalse(await users.delete("greeting"))
        finally:
            await client.close()

    async def test_round_trips_incr_decr_within_a_namespace(self):
        # Counters (issue #129): Namespace.incr()/decr() scope to the
        # namespace the same way get/set/delete already do.
        client = await self.connect()
        try:
            users = client.namespace("users")
            await users.set("counter", "10")
            self.assertEqual(await users.incr("counter"), 11)
            self.assertEqual(await users.decr("counter", 3), 8)
            # The default namespace's own "counter" is a separate entry.
            self.assertIsNone(await client.incr("counter"))
        finally:
            await client.close()

    async def test_namespace_incr_on_a_compress_enabled_client_raises_before_any_io(self):
        # Issue #321: Namespace.incr()/decr() forward to the same
        # client._incr() the top-level incr()/decr() use, so the guard
        # applies here too.
        client = await self.connect(compress=True)
        try:
            users = client.namespace("users")
            with self.assertRaises(CompressionIncompatibleError):
                await users.incr("counter")
            with self.assertRaises(CompressionIncompatibleError):
                await users.decr("counter")
        finally:
            await client.close()

    async def test_get_bytes_never_decodes_and_ttl_is_forwarded(self):
        client = await self.connect()
        try:
            bin_ns = client.namespace("bin")
            await bin_ns.set(b"k", b"\x00\xff", ttl_seconds=60)
            self.assertEqual(await bin_ns.get_bytes(b"k"), b"\x00\xff")
            self.assertEqual(self.node.last_set_ttl, 60)
        finally:
            await client.close()

    async def test_binary_namespace_round_trips(self):
        client = await self.connect()
        try:
            ns = client.namespace(b"\xff\x00")
            await ns.set("k", "v")
            self.assertEqual(await ns.get("k"), "v")
        finally:
            await client.close()

    async def test_a_non_empty_namespace_speaks_the_lowercase_frames_on_the_wire(self):
        client = await self.connect()
        try:
            users = client.namespace("users")
            await users.set("k", "v")
            await users.get("k")
            await users.delete("k")
            self.assertEqual(self.node.namespaced_command_count, 3)
        finally:
            await client.close()

    async def test_the_default_namespace_still_speaks_the_legacy_frames_on_the_wire(self):
        # SDK rule (issue #105 spec): the default (empty) namespace must
        # keep sending legacy G/S/D, never g/s/d, so an unchanged client
        # talking to an old, pre-namespace server keeps working.
        client = await self.connect()
        try:
            root = client.namespace("")
            await root.set("k", "v")
            await root.get("k")
            await root.delete("k")
            self.assertEqual(self.node.namespaced_command_count, 0)
            self.assertEqual(self.node.get_count, 1)
        finally:
            await client.close()

    async def test_namespace_isolates_a_shared_key_name(self):
        # Same key name in two namespaces plus the default namespace: 3
        # independent entries, none of which observe each other's writes
        # or deletes.
        client = await self.connect()
        try:
            users = client.namespace("users")
            orders = client.namespace("orders")
            await client.set("shared", "default-value")
            await users.set("shared", "users-value")
            await orders.set("shared", "orders-value")

            self.assertEqual(await client.get("shared"), "default-value")
            self.assertEqual(await users.get("shared"), "users-value")
            self.assertEqual(await orders.get("shared"), "orders-value")
            self.assertEqual(self.node.store[b"shared"], b"default-value")
            self.assertEqual(self.node.ns_store[(b"users", b"shared")], b"users-value")
            self.assertEqual(self.node.ns_store[(b"orders", b"shared")], b"orders-value")

            self.assertTrue(await users.delete("shared"))
            self.assertIsNone(await users.get("shared"))
            # Deleting from one namespace must not touch the others.
            self.assertEqual(await client.get("shared"), "default-value")
            self.assertEqual(await orders.get("shared"), "orders-value")
        finally:
            await client.close()

    async def test_a_handle_survives_the_client_it_is_bound_to_being_open_but_raises_after_close(self):
        client = await self.connect()
        users = client.namespace("users")
        await users.set("k", "v")  # the handle works fine before close()
        await client.close()
        with self.assertRaises(AlreadyClosedError):
            await users.get("k")
        with self.assertRaises(AlreadyClosedError):
            await users.get_bytes("k")
        with self.assertRaises(AlreadyClosedError):
            await users.set("k", "v")
        with self.assertRaises(AlreadyClosedError):
            await users.delete("k")

    async def test_namespaced_requests_participate_in_tagged_mode(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                users = client.namespace("users")
                await users.set("k", "v")
                self.assertEqual(await users.get("k"), "v")
                self.assertTrue(await users.delete("k"))
            finally:
                await client.close()
        finally:
            await node.close()


class NamespaceClusterTests(unittest.IsolatedAsyncioTestCase):
    # Namespaces (issue #105) enter HRW routing (client.py's _owner_names),
    # so a namespaced key's owners aren't necessarily a key-alone lookup's
    # owners — these need an actual cluster, unlike NamespaceTests above.

    async def start_cluster(self):
        node_a = await MockNode().start()
        node_b = await MockNode().start()
        nodes = {NAMES[0]: node_a, NAMES[1]: node_b}
        discovery = await MockDiscovery(
            [(name, node.address) for name, node in nodes.items()], replication=2
        ).start()
        return nodes, discovery

    def owners_of(self, namespace: bytes, key: str):
        return HashRing(NAMES).owners(key.encode(), 2, namespace=namespace)

    async def test_fans_namespaced_writes_out_to_every_owner(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                tenant = client.namespace("tenant-a")
                await tenant.set("k", "v")
                for name, node in nodes.items():
                    self.assertIn((b"tenant-a", b"k"), node.ns_store, f"missing from {name}")
                    # The default namespace's own store must stay empty —
                    # this write never touched it.
                    self.assertNotIn(b"k", node.store)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_w_refresh_and_retry_routes_a_namespaced_key_by_namespace_and_key(self):
        # Mirrors ReplicationTests.test_writes_route_around_a_dead_
        # primary_once_discovery_drops_it, but for a namespaced key: the
        # retry after a forced refresh must re-rank using (namespace,
        # key) — using the key alone could pick a different, wrong
        # primary for this namespace.
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            namespace = b"tenant-a"
            tenant = client.namespace(namespace)
            key = "written-after-primary-death"
            primary, replica = self.owners_of(namespace, key)

            await nodes[primary].close()
            discovery.nodes = [(replica, nodes[replica].address)]
            await wait_for(
                lambda: client._members[primary].connection.closed,
                "the client to see the FIN",
            )

            await tenant.set(key, "v")
            self.assertEqual(await tenant.get(key), "v")
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass


class ClearTests(unittest.IsolatedAsyncioTestCase):
    # Clear / flush (issue #106): single-node coverage. See
    # ClearClusterTests below for the fan-out and refresh-once-and-retry
    # path, which needs an actual cluster.

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_clear_drops_only_its_own_namespace(self):
        client = await self.connect()
        try:
            users = client.namespace("users")
            orders = client.namespace("orders")
            await client.set("k", "default-value")
            await users.set("k", "users-value")
            await orders.set("k", "orders-value")

            await users.clear()

            self.assertIsNone(await users.get("k"))
            self.assertEqual(await client.get("k"), "default-value")
            self.assertEqual(await orders.get("k"), "orders-value")
            self.assertEqual(self.node.clear_count, 1)
        finally:
            await client.close()

    async def test_clear_on_the_empty_namespace_handle_clears_the_default_namespace(self):
        client = await self.connect()
        try:
            await client.set("k", "v")
            await client.namespace("").clear()
            self.assertIsNone(await client.get("k"))
            self.assertEqual(self.node.clear_count, 1)
        finally:
            await client.close()

    async def test_clear_all_empties_every_namespace_including_the_default(self):
        client = await self.connect()
        try:
            await client.set("k", "default-value")
            await client.namespace("users").set("k", "users-value")
            await client.namespace("orders").set("k", "orders-value")

            await client.clear_all()

            self.assertIsNone(await client.get("k"))
            self.assertIsNone(await client.namespace("users").get("k"))
            self.assertIsNone(await client.namespace("orders").get("k"))
            self.assertEqual(self.node.flush_count, 1)
        finally:
            await client.close()

    async def test_clear_is_idempotent_on_an_already_empty_namespace(self):
        client = await self.connect()
        try:
            await client.namespace("empty").clear()  # must not raise
        finally:
            await client.close()

    async def test_after_close_both_raise_already_closed(self):
        client = await self.connect()
        users = client.namespace("users")
        await client.close()
        with self.assertRaises(AlreadyClosedError):
            await users.clear()
        with self.assertRaises(AlreadyClosedError):
            await client.clear_all()

    async def test_clear_participates_in_tagged_mode(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                users = client.namespace("users")
                await users.set("k", "v")
                await users.clear()
                self.assertIsNone(await users.get("k"))
                await client.clear_all()
            finally:
                await client.close()
        finally:
            await node.close()


class ClearClusterTests(unittest.IsolatedAsyncioTestCase):
    # Clear / flush (issue #106) is not routed by HRW like get/set/delete —
    # a namespace's keys are spread over every node, so a clear fans out to
    # every currently-known node instead (client.py's _fan_out_clear) —
    # these need an actual cluster, unlike ClearTests above.

    async def start_cluster(self):
        node_a = await MockNode().start()
        node_b = await MockNode().start()
        nodes = {NAMES[0]: node_a, NAMES[1]: node_b}
        discovery = await MockDiscovery(
            [(name, node.address) for name, node in nodes.items()], replication=2
        ).start()
        return nodes, discovery

    async def test_fans_a_namespace_clear_out_to_every_node(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                tenant = client.namespace("tenant-a")
                for i in range(20):
                    await tenant.set(f"k{i}", "v")

                await tenant.clear()

                for name, node in nodes.items():
                    self.assertEqual(node.clear_count, 1, f"{name} was not sent the clear")
                    self.assertEqual(
                        [k for (ns, k) in node.ns_store if ns == b"tenant-a"], [], name
                    )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_clear_all_fans_out_and_flushes_every_node(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                for i in range(20):
                    await client.set(f"k{i}", "v")
                    await client.namespace("tenant-a").set(f"k{i}", "v")

                await client.clear_all()

                for name, node in nodes.items():
                    self.assertEqual(node.flush_count, 1, f"{name} was not sent the flush")
                    self.assertEqual(node.store, {}, name)
                    self.assertEqual(node.ns_store, {}, name)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_node_failing_once_is_healed_by_the_refresh_and_retry(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            failing = NAMES[0]
            nodes[failing].fail_next_clear_once()

            await client.clear_all()  # must not raise: the retry after the refresh acks

            # Every node is retried on the refreshed list, not just the one
            # that failed (the operation is idempotent) — so the healthy
            # node also sees two flushes, and the failing one is healed by
            # its second (post-refresh) attempt.
            self.assertEqual(nodes[failing].flush_count, 2)
            self.assertEqual(nodes[NAMES[1]].flush_count, 2)
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass

    async def test_a_persistently_failing_node_raises_naming_it(self):
        nodes, discovery = await self.start_cluster()
        client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
        try:
            failing = NAMES[0]
            nodes[failing].fail_next_clear_once()
            nodes[failing].fail_next_clear_once()  # also fails the post-refresh retry

            with self.assertRaises(NanocachedError) as ctx:
                await client.clear_all()
            self.assertIn(failing, str(ctx.exception))
        finally:
            await client.close()
            await discovery.close()
            for node in nodes.values():
                try:
                    await node.close()
                except Exception:
                    pass


class ResponseTagTests(unittest.IsolatedAsyncioTestCase):
    # Echoed response tags: echoed response tags close the pipeline desync
    # window request pipelining left open. Mirrors the TypeScript SDK's own
    # "NanocachedClient response tags" suite.

    async def test_negotiates_tags_and_round_trips_pipelined_requests(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await asyncio.gather(*(client.set(f"key-{i}", f"value-{i}") for i in range(20)))
                values = await asyncio.gather(*(client.get(f"key-{i}") for i in range(20)))
                for i, value in enumerate(values):
                    self.assertEqual(value, f"value-{i}")

                self.assertTrue(await client.delete("key-0"))
                self.assertFalse(await client.delete("key-0"))
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_desynced_stream_is_caught_by_the_tag_check_before_any_caller_sees_wrong_data(self):
        # The exact misdelivery request pipelining left open: the server (as a
        # stand-in for any off-by-one stream corruption) never answers
        # the first GET, so the second GET's response arrives at the
        # first GET's pending slot. Without tags the first caller would
        # receive the second's value as a plausible, exception-free wrong
        # answer; the tag check must poison the connection before either
        # caller sees anything.
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")

                node.swallow_get_once()
                first = asyncio.ensure_future(client.get("a"))
                second = asyncio.ensure_future(client.get("k"))
                with self.assertRaisesRegex(ConnectionError, "desynced"):
                    await first
                with self.assertRaisesRegex(ConnectionError, "desynced"):
                    await second

                # The poisoned connection redials transparently on next use.
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 2)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_response_echoing_the_wrong_tag_poisons_the_connection(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                node.answer_wrong_tag_once()
                with self.assertRaisesRegex(ConnectionError, "desynced"):
                    await client.get("k")
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_falls_back_to_the_untagged_protocol_against_a_pre_0019_server(self):
        # An old server treats `A ... T` (and the further-extended `A ...
        # T R` — issue #125) as a parse error and closes without
        # replying; the client must redial through both extended stages
        # down to the plain form and run untagged — transparently, with
        # the same results.
        node = await MockNode(close_on_extended_auth=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                # Three dials: the `T R` attempt the server slammed shut,
                # then the `T`-only attempt it also slammed shut, then the
                # plain fallback that stuck.
                self.assertEqual(node.connection_count, 3)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_reset_while_writing_the_extended_auth_frame_also_falls_back_to_untagged(self):
        # Regression (issue #47 audit item 2): the legacy-server-fallback
        # guard used to wrap only the ack *read* (_identify.py), not the
        # `A ... T` write that precedes it. A pre-0019 server can slam the
        # door as soon as it sees the extended frame — a reaction to what
        # we sent, not to us waiting for a reply — so the door-slam can
        # just as well show up as a write-time ConnectionResetError as a
        # read-time one. MockNode's close_on_extended_auth (used by the
        # test above) closes after reading the header, which in practice
        # surfaces on the read side regardless of where the guard starts;
        # to pin down the write-time path specifically, patch
        # StreamWriter.write so sending the extended `A ... T` frame
        # itself raises ConnectionResetError, exactly as if the peer had
        # already reset by the time the client tried to write.
        node = await MockNode().start()
        try:
            real_write = asyncio.StreamWriter.write

            def flaky_write(writer, data):
                if data.startswith(b"A ") and b" T" in data.split(b"\n", 1)[0]:
                    raise ConnectionResetError("simulated reset reacting to the extended A frame")
                return real_write(writer, data)

            with mock.patch.object(asyncio.StreamWriter, "write", flaky_write):
                client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                # Transparently retried untagged — still fully usable.
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                # Three dials: both extended attempts (`T R`, then `T`)
                # reset on write — flaky_write matches any header
                # containing ` T` — then the plain fallback that stuck.
                self.assertEqual(node.connection_count, 3)
            finally:
                await client.close()
        finally:
            await node.close()


class RetryableErrorTests(unittest.IsolatedAsyncioTestCase):
    # Retryable-error status `R` (issue #125): the proxy's transient-
    # failure signal — retry the same request on the SAME connection, up
    # to 2 retries (3 attempts total), never a connection error/W/E.

    async def test_the_connect_probe_sends_the_retryable_capability_token(self):
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                self.assertEqual(node.last_auth_header, b"A 1 T R")
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_falls_back_from_retryable_to_tagged_against_a_server_that_predates_r(self):
        # A server that understands `T` but not the further-extended `T
        # R` closes on the retryable probe only — the client must redial
        # once with plain `A <len> T` and end up on a tagged connection.
        node = await MockNode(support_tags=True, close_on_retryable_auth=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                # Two dials: the `T R` attempt the server slammed shut,
                # then the `T`-only fallback that stuck.
                self.assertEqual(node.connection_count, 2)
                self.assertEqual(node.auth_headers, [b"A 1 T R", b"A 1 T"])
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_falls_all_the_way_back_to_plain_against_a_pre_tag_server(self):
        # Mirrors ResponseTagTests' own pre-0019-server fallback test: a
        # server that predates tagging entirely closes on any extended
        # `A`, so the client must walk all three stages (`T R` -> `T` ->
        # plain) before it sticks.
        node = await MockNode(close_on_extended_auth=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 3)
                self.assertEqual(node.auth_headers, [b"A 1 T R", b"A 1 T", b"A 1"])
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_a_single_r_is_retried_transparently_on_the_same_connection(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                requests_before = node.get_count
                node.answer_retryable(1)
                self.assertEqual(await client.get("k"), "v")
                # Exactly one retry: the original G plus one retried G.
                self.assertEqual(node.get_count - requests_before, 2)
                # No new connection was dialed for the retry.
                self.assertEqual(node.connection_count, 1)
                self.assertEqual(client.stats().transient_retries, 1)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_three_rs_in_a_row_raise_retryableerror_without_poisoning_the_connection(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                requests_before = node.get_count
                node.answer_retryable(3)
                with self.assertRaises(RetryableError):
                    await client.get("k")
                # 1 original + 2 retries = 3 attempts, all answered R.
                self.assertEqual(node.get_count - requests_before, 3)
                self.assertEqual(client.stats().transient_retries, 3)

                # The same connection still serves a following op.
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(node.connection_count, 1)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_tagged_r_pairs_with_the_right_in_flight_request(self):
        # A pipelined request answered `R` must not desync the other
        # requests already outstanding on the same tagged connection: the
        # retried request's response has to keep pairing with its own
        # slot even though other responses were dispatched in between.
        node = await MockNode(support_tags=True).start()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", node.port)])
            try:
                await client.set("k", "v")
                requests_before = node.get_count

                node.answer_retryable(1)
                first = asyncio.ensure_future(client.get("a"))
                second = asyncio.ensure_future(client.get("k"))

                self.assertIsNone(await first)
                self.assertEqual(await second, "v")
                self.assertEqual(node.get_count - requests_before, 3)
                self.assertEqual(node.connection_count, 1)
                self.assertEqual(client.stats().transient_retries, 1)
            finally:
                await client.close()
        finally:
            await node.close()

    async def test_r_is_retried_transparently_via_proxy(self):
        proxy = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[(NAMES[0], "127.0.0.1:1")], proxies=[(NAMES[1], proxy.address)]
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True
            )
            try:
                await client.set("k", "v")
                requests_before = proxy.get_count
                proxy.answer_retryable(1)
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(proxy.get_count - requests_before, 2)
                self.assertEqual(proxy.connection_count, 1)
                self.assertEqual(client.stats().transient_retries, 1)
            finally:
                await client.close()
        finally:
            await proxy.close()
            await discovery.close()


class ProxyModeTests(unittest.IsolatedAsyncioTestCase):
    """SDK proxy mode (issue #122): via_proxy fetches the proxy roster
    (`Q`) from a discovery seed instead of the node roster (`L`), and
    lands this client on a single proxy connection instead of a ring — a
    proxy looks exactly like one node that owns every key, so a mock
    proxy is just a MockNode (see mock_servers.py's own doc comment)."""

    async def test_every_op_lands_on_the_proxy_and_the_node_roster_is_never_touched(self):
        # The node roster names a port nobody listens on — if via_proxy
        # ever mistakenly dialed it (instead of the separate proxy
        # roster), the connect or the first operation would blow up with
        # a connection error, failing this test loudly rather than
        # silently passing.
        unreachable_node_port = await unused_port()
        proxy = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[(NAMES[0], f"127.0.0.1:{unreachable_node_port}")],
            proxies=[(NAMES[1], proxy.address)],
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                self.assertTrue(await client.delete("k"))

                users = client.namespace("users")
                await users.set("alice", "admin")
                self.assertEqual(await users.get("alice"), "admin")

                await client.clear_all()
                self.assertIsNone(await users.get("alice"))

                # `L` (the node roster) is never asked for in via_proxy
                # mode — only `Q` is.
                self.assertEqual(discovery.list_count, 0)
                self.assertGreaterEqual(discovery.list_proxies_count, 1)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await proxy.close()

    async def test_random_spread_across_multiple_proxies(self):
        # Deterministic despite being statistical (issue #122 spec): a
        # fixed seed pins client.py's random.shuffle() calls, so this
        # can't flake even though it's asserting about randomness.
        proxy_a = await MockNode().start()
        proxy_b = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[],
            proxies=[(NAMES[0], proxy_a.address), (NAMES[1], proxy_b.address)],
        ).start()
        try:
            random.seed(1234)
            for _ in range(20):
                client = await NanocachedClient.connect(
                    [("127.0.0.1", discovery.port)], via_proxy=True
                )
                await client.close()
            self.assertGreater(proxy_a.connection_count, 0)
            self.assertGreater(proxy_b.connection_count, 0)
        finally:
            await discovery.close()
            await proxy_a.close()
            await proxy_b.close()

    async def test_failover_when_the_first_chosen_proxy_is_down(self):
        dead_port = await unused_port()
        live = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[],
            proxies=[(NAMES[0], f"127.0.0.1:{dead_port}"), (NAMES[1], live.address)],
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(client._single_address, live.address)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await live.close()

    async def test_busy_first_seed_falls_over_to_a_second_seed_serving_q(self):
        proxy = await MockNode().start()
        warming = await MockDiscovery(nodes=[], proxies=[]).start()
        healthy = await MockDiscovery(nodes=[], proxies=[(NAMES[0], proxy.address)]).start()
        warming.warming_up = True
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", warming.port), ("127.0.0.1", healthy.port)], via_proxy=True
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(client._single_address, proxy.address)
            finally:
                await client.close()
        finally:
            await warming.close()
            await healthy.close()
            await proxy.close()

    async def test_empty_proxy_roster_is_a_clear_connect_error(self):
        discovery = await MockDiscovery(nodes=[], proxies=[]).start()
        try:
            with self.assertRaisesRegex(NanocachedError, "no proxies registered"):
                await NanocachedClient.connect(
                    [("127.0.0.1", discovery.port)], via_proxy=True
                )
        finally:
            await discovery.close()

    async def test_via_proxy_pointed_at_a_node_address_is_a_clear_error(self):
        node = await MockNode().start()
        try:
            with self.assertRaisesRegex(NanocachedError, "identifies as a cache node"):
                await NanocachedClient.connect([("127.0.0.1", node.port)], via_proxy=True)
        finally:
            await node.close()

    async def test_reconnect_after_the_proxy_dies_refetches_q_and_lands_on_the_survivor(self):
        proxy_a = await MockNode().start()
        proxy_b = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[],
            proxies=[(NAMES[0], proxy_a.address), (NAMES[1], proxy_b.address)],
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True, reconnect_cooldown=0.01
            )
            try:
                connected_address = client._single_address
                dead, survivor = (
                    (proxy_a, proxy_b) if connected_address == proxy_a.address else (proxy_b, proxy_a)
                )

                # The proxy this client is pinned to goes away for good
                # (not just a blip) — discovery's roster is updated to
                # match, exactly as it would once discovery notices.
                await dead.close()
                discovery.set_proxies([(NAMES[0], survivor.address)])
                # Wait for the FIN to be observed (mirrors ReconnectTests'
                # own test_transparently_reconnects_after_a_server_fin)
                # instead of racing client.set() against it: hitting the
                # connection while it still looks open would raise a raw
                # ConnectionError with no retry (via_proxy is single-
                # connection mode, so _with_wrong_node_retry has no ring
                # to refresh and fail over through — the same as
                # standalone single-node mode).
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the client to see the dead proxy's FIN",
                )

                await client.set("k", "v")
                self.assertEqual(await client.get("k"), "v")
                self.assertEqual(client._single_address, survivor.address)
                self.assertGreaterEqual(discovery.list_proxies_count, 2)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await proxy_a.close()
            await proxy_b.close()

    async def test_reconnect_purges_cooldowns_for_addresses_other_than_the_new_proxy(self):
        # #363: _maybe_refresh no-ops in via_proxy mode (self._ring stays
        # None forever, since via_proxy never builds a ring), so the #96
        # purge that _refresh_node_list runs for departed cluster members
        # never fires here — without a purge somewhere in the proxy
        # redial path, _redial_cooldowns would grow one stale entry per
        # proxy this client is ever swapped away from, for the life of a
        # churning proxy fleet. The fix prunes down to at most the
        # currently pinned proxy's own address every time
        # _dial_one_proxy_randomly lands on one, mirroring the
        # TypeScript SDK's refreshProxyTarget (issue #296).
        proxy_a = await MockNode().start()
        proxy_b = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[],
            proxies=[(NAMES[0], proxy_a.address), (NAMES[1], proxy_b.address)],
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True, reconnect_cooldown=0.01
            )
            try:
                connected_address = client._single_address
                dead, survivor = (
                    (proxy_a, proxy_b) if connected_address == proxy_a.address else (proxy_b, proxy_a)
                )

                # Plant a stale cooldown entry for an address that is
                # neither the dead proxy nor the survivor — standing in
                # for a proxy this client was pinned to even earlier in
                # a churny fleet, before the currently-dying one.
                stale_address = "127.0.0.1:1"
                loop = asyncio.get_running_loop()
                client._redial_cooldowns[stale_address] = (
                    loop.time() + 100,
                    ConnectionError("boom"),
                )

                await dead.close()
                discovery.set_proxies([(NAMES[0], survivor.address)])
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the client to see the dead proxy's FIN",
                )

                await client.set("k", "v")
                self.assertEqual(client._single_address, survivor.address)

                # Reconnecting to the survivor must prune every cooldown
                # entry that isn't the newly pinned proxy's own address —
                # both the pre-existing stale entry and (once the dead
                # proxy's own direct-dial failure armed one) the dead
                # proxy's entry itself.
                self.assertNotIn(stale_address, client._redial_cooldowns)
                self.assertNotIn(dead.address, client._redial_cooldowns)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await proxy_a.close()
            await proxy_b.close()

    async def test_get_many_and_set_many_ride_the_single_proxy_connection(self):
        # No owner grouping in proxy mode — the proxy splits/reassembles
        # server-side (docs/protocol.html "m / o"); this client just
        # sends one (possibly chunked) sub-frame straight to it.
        proxy = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[], proxies=[(NAMES[0], proxy.address)]
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True
            )
            try:
                await client.set_many({"a": "1", "b": "2"})
                self.assertEqual(await client.get_many(["a", "b"]), {"a": "1", "b": "2"})
                self.assertEqual(proxy.multi_set_count, 1)
                self.assertEqual(proxy.multi_get_count, 1)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await proxy.close()

    async def test_hedge_option_is_inert_in_proxy_mode(self):
        proxy = await MockNode().start()
        discovery = await MockDiscovery(
            nodes=[], proxies=[(NAMES[0], proxy.address)]
        ).start()
        try:
            client = await NanocachedClient.connect(
                [("127.0.0.1", discovery.port)], via_proxy=True, read_hedge_after=0.01
            )
            try:
                await client.set("k", "v")
                proxy.get_count = 0
                self.assertEqual(await client.get("k"), "v")
                # No hedge leg — exactly one G reached the wire.
                self.assertEqual(proxy.get_count, 1)
            finally:
                await client.close()
        finally:
            await discovery.close()
            await proxy.close()


class MultiGetSetTests(unittest.IsolatedAsyncioTestCase):
    # Batched get/set (issues #128/#150/#151), single-node coverage — see
    # MultiClusterTests below for owner grouping, replication, and the
    # per-key wrong-node refresh-and-retry contract.

    async def asyncSetUp(self):
        self.node = await MockNode().start()

    async def asyncTearDown(self):
        await self.node.close()

    async def connect(self, **kwargs):
        return await NanocachedClient.connect([("127.0.0.1", self.node.port)], **kwargs)

    async def test_get_many_mixes_hits_and_misses(self):
        client = await self.connect()
        try:
            await client.set("a", "va")
            await client.set("b", "vb")
            self.assertEqual(
                await client.get_many(["a", "b", "missing"]), {"a": "va", "b": "vb"}
            )
            self.assertEqual(
                await client.get_many_bytes(["a", "b", "missing"]),
                {"a": b"va", "b": b"vb"},
            )
        finally:
            await client.close()

    async def test_get_many_bytes_enforces_a_cumulative_decompressed_budget(self):
        # Regression (pass-7 audit): the per-value decompression cap can be
        # amplified across a batch. Patched low so the guard fires without
        # allocating the real 256 MiB bound.
        client = await self.connect(compress=True)
        try:
            await client.set("a", "12")
            await client.set("c", "34")
            with mock.patch("nanocached.client._MAX_MULTIGET_DECOMPRESSED_BYTES", 1):
                with self.assertRaises(DecompressionError):
                    await client.get_many_bytes(["a", "c"])
        finally:
            await client.close()

    async def test_get_many_bytes_budget_catches_the_crossing_entry_even_when_last(self):
        # Regression (pass-9 audit, issue #410a): the cumulative budget used
        # to be checked BEFORE charging the current entry, so the entry
        # that actually crosses the cap always slipped through uncaught —
        # and if it was the last hit in the response, the guard never fired
        # at all. Only one key here, so the crossing entry is necessarily
        # the last (and only) one; charge-then-check must still catch it.
        client = await self.connect(compress=True)
        try:
            await client.set("a", "12")
            with mock.patch("nanocached.client._MAX_MULTIGET_DECOMPRESSED_BYTES", 1):
                with self.assertRaises(DecompressionError):
                    await client.get_many_bytes(["a"])
        finally:
            await client.close()

    async def test_get_many_bytes_does_not_charge_the_budget_when_compress_is_disabled(self):
        # Regression (pass-9 audit, issue #410b): the cumulative budget used
        # to be charged and enforced even when the client has compression
        # disabled, so a large uncompressed batch could fail with a
        # misleading "decompression bomb" error. The budget is patched far
        # below what this batch would need if it were (wrongly) charged.
        client = await self.connect()
        try:
            await client.set("a", "12")
            await client.set("c", "34")
            with mock.patch("nanocached.client._MAX_MULTIGET_DECOMPRESSED_BYTES", 1):
                result = await client.get_many_bytes(["a", "c"])
            self.assertEqual(result, {"a": b"12", "c": b"34"})
        finally:
            await client.close()

    async def test_str_and_bytes_keys_may_be_mixed_and_are_keyed_by_the_original_object(self):
        client = await self.connect()
        try:
            await client.set("a", "va")
            await client.set(b"bb", "vbb")
            result = await client.get_many(["a", b"bb"])
            self.assertEqual(result, {"a": "va", b"bb": "vbb"})
            self.assertIn("a", result)
            self.assertIn(b"bb", result)
        finally:
            await client.close()

    async def test_duplicate_key_after_encoding_is_rejected(self):
        client = await self.connect()
        try:
            with self.assertRaisesRegex(ValueError, "duplicate key after encoding"):
                await client.get_many(["a", b"a"])
            with self.assertRaisesRegex(ValueError, "duplicate key after encoding"):
                await client.set_many({"a": "1", b"a": "2"})
        finally:
            await client.close()

    async def test_empty_batch_is_rejected(self):
        client = await self.connect()
        try:
            with self.assertRaises(ValueError):
                await client.get_many([])
            with self.assertRaises(ValueError):
                await client.get_many_bytes([])
            with self.assertRaises(ValueError):
                await client.set_many({})
        finally:
            await client.close()

    async def test_set_many_shares_one_ttl_across_the_batch(self):
        client = await self.connect()
        try:
            await client.set_many({"a": "1", "b": "2"}, ttl_seconds=60)
            self.assertEqual(self.node.last_set_ttl, 60)
            self.assertEqual(await client.get_many(["a", "b"]), {"a": "1", "b": "2"})
        finally:
            await client.close()

    async def test_a_batch_larger_than_max_batch_keys_is_transparently_chunked(self):
        from nanocached.client import _MAX_BATCH_KEYS

        client = await self.connect()
        try:
            values = {f"k{i}": f"v{i}" for i in range(_MAX_BATCH_KEYS + 1)}
            await client.set_many(values)
            self.assertEqual(self.node.multi_set_count, 2)
            self.assertEqual(await client.get_many(list(values.keys())), values)
            self.assertEqual(self.node.multi_get_count, 2)
        finally:
            await client.close()

    async def test_set_many_bytes_over_the_byte_bound_splits_by_bytes_not_just_count(self):
        # Issue #222: a handful of individually-valid pairs (nowhere near
        # _MAX_BATCH_KEYS) whose combined namespace+key+value bytes
        # exceed _MAX_REQUEST_BYTES must still be split into multiple
        # `o` sub-frames — otherwise the server's request_is_too_large
        # check would reject the whole frame with no reply at all,
        # closing the shared connection.
        from nanocached.client import _MAX_REQUEST_BYTES

        client = await self.connect()
        try:
            # Four values, each individually well under _MAX_REQUEST_BYTES,
            # but four of them summed comfortably exceed it — while the
            # count (4) is nowhere near _MAX_BATCH_KEYS (400), so only the
            # new cumulative-bytes bound can be responsible for a split.
            value_size = _MAX_REQUEST_BYTES // 3
            values = {f"k{i}": b"v" * value_size for i in range(4)}
            await client.set_many(values)

            self.assertGreater(self.node.multi_set_count, 1)
            self.assertLess(self.node.multi_set_count, len(values))
            for size in self.node.multi_set_frame_sizes:
                self.assertLessEqual(size, 1024 * 1024)

            result = await client.get_many_bytes(list(values.keys()))
            self.assertEqual(result, values)
        finally:
            await client.close()

    async def test_get_many_with_large_keys_over_the_byte_bound_splits_by_bytes(self):
        # get_many's own m-frame twin of the set_many test above (issue
        # #222): large keys alone (no values on the wire for `m`) can
        # still sum past _MAX_REQUEST_BYTES well before _MAX_BATCH_KEYS
        # keys are reached.
        from nanocached.client import _MAX_REQUEST_BYTES

        client = await self.connect()
        try:
            key_size = _MAX_REQUEST_BYTES // 3
            keys = [b"k" * key_size + str(i).encode() for i in range(4)]
            for i, key in enumerate(keys):
                await client.set(key, f"v{i}")

            result = await client.get_many_bytes(keys)

            self.assertGreater(self.node.multi_get_count, 1)
            self.assertLess(self.node.multi_get_count, len(keys))
            for size in self.node.multi_get_frame_sizes:
                self.assertLessEqual(size, 1024 * 1024)
            self.assertEqual(len(result), len(keys))
        finally:
            await client.close()

    async def test_a_persisting_wrong_node_propagates_immediately_in_single_mode(self):
        # No ring to refresh against in single-node mode — mirrors get()'s
        # own single-mode behavior (_read/_write's `self._ring is None`
        # branch).
        client = await self.connect()
        try:
            await client.set("a", "va")
            self.node.answer_wrong_node_for_keys({b"a"})
            with self.assertRaises(WrongNodeError):
                await client.get_many(["a"])

            self.node.answer_wrong_node_for_keys({b"a"})
            with self.assertRaises(WrongNodeError):
                await client.set_many({"a": "v2"})
        finally:
            await client.close()

    async def test_a_multi_get_reply_with_too_few_entries_is_rejected_as_a_desync(self):
        # Issue #181: a well-formed `M` header whose count doesn't match
        # len(keys) must not silently resize `entries` via the slice
        # assignment in _multi_get_chunked, shifting every later value
        # onto the wrong key — it must raise and poison the connection,
        # exactly like _mismatch's own wrong-marker desync.
        client = await self.connect()
        try:
            await client.set("a", "va")
            await client.set("b", "vb")
            self.node.answer_multi_get_bad_count_once(1)
            with self.assertRaises(ConnectionError):
                await client.get_many(["a", "b"])

            # The connection was poisoned, so the redial that follows
            # proves the client recovers cleanly instead of staying
            # desynced.
            self.assertEqual(await client.get_many(["a", "b"]), {"a": "va", "b": "vb"})
        finally:
            await client.close()

    async def test_a_multi_get_reply_with_too_many_entries_is_rejected_as_a_desync(self):
        client = await self.connect()
        try:
            await client.set("a", "va")
            await client.set("b", "vb")
            self.node.answer_multi_get_bad_count_once(3)
            with self.assertRaises(ConnectionError):
                await client.get_many(["a", "b"])

            self.assertEqual(await client.get_many(["a", "b"]), {"a": "va", "b": "vb"})
        finally:
            await client.close()

    async def test_a_multi_set_reply_with_a_mismatched_entry_count_is_rejected_as_a_desync(self):
        # Same contract as multi_get above, for `o`/multi_set (issue #181).
        client = await self.connect()
        try:
            self.node.answer_multi_set_bad_count_once(1)
            with self.assertRaises(ConnectionError):
                await client.set_many({"a": "va", "b": "vb"})

            await client.set_many({"a": "va", "b": "vb"})
            self.assertEqual(await client.get_many(["a", "b"]), {"a": "va", "b": "vb"})
        finally:
            await client.close()

    async def test_get_many_raises_partial_connection_lost_error_when_a_later_chunk_fails(self):
        # Issue #411: get_many/get_many_bytes previously discarded an
        # earlier, already-succeeded chunk's hits when a later chunk's
        # own connection failure propagated — single-node/proxy mode
        # never retries a chunk failure (no ring to refresh against), so
        # this failure mode reaches the caller directly, and used to
        # arrive as a bare connection error with no trace of the earlier
        # chunk's hits.
        from nanocached.client import _MAX_BATCH_KEYS

        client = await self.connect()
        try:
            values = {f"k{i}": f"v{i}" for i in range(_MAX_BATCH_KEYS + 1)}
            await client.set_many(values)
            self.assertEqual(self.node.multi_set_count, 2)

            self.node.fail_multi_get_after(1)
            with self.assertRaises(PartialConnectionLostError) as ctx:
                await client.get_many(list(values.keys()))

            error = ctx.exception
            self.assertEqual(self.node.multi_get_count, 2)
            first_chunk_keys = list(values.keys())[:_MAX_BATCH_KEYS]
            self.assertEqual(set(error.partial_values), set(first_chunk_keys))
            for key in first_chunk_keys:
                self.assertEqual(error.partial_values[key], values[key])
            self.assertIsInstance(error.__cause__, ConnectionError)

            # The connection was poisoned, not left desynced — a fresh
            # call recovers cleanly.
            self.assertEqual(await client.get_many(list(values.keys())), values)
        finally:
            await client.close()

    async def test_get_many_bytes_partial_connection_lost_error_carries_raw_bytes(self):
        # get_many_bytes' own companion to the test above: partial_values
        # must carry raw bytes, not decoded str, exactly like a plain
        # successful get_many_bytes() result would.
        from nanocached.client import _MAX_BATCH_KEYS

        client = await self.connect()
        try:
            values = {f"k{i}": f"v{i}" for i in range(_MAX_BATCH_KEYS + 1)}
            await client.set_many(values)

            self.node.fail_multi_get_after(1)
            with self.assertRaises(PartialConnectionLostError) as ctx:
                await client.get_many_bytes(list(values.keys()))

            error = ctx.exception
            first_chunk_keys = list(values.keys())[:_MAX_BATCH_KEYS]
            self.assertEqual(set(error.partial_values), set(first_chunk_keys))
            for key in first_chunk_keys:
                self.assertEqual(error.partial_values[key], values[key].encode())
        finally:
            await client.close()

    async def test_get_many_propagates_the_bare_error_when_the_first_chunk_fails(self):
        # The other half of the #411 fix: a failure on the very first
        # chunk has no partial data yet to attach, so it must still
        # raise the original connection error unwrapped, exactly as
        # before this fix — not PartialConnectionLostError.
        from nanocached.client import _MAX_BATCH_KEYS

        client = await self.connect()
        try:
            values = {f"k{i}": f"v{i}" for i in range(_MAX_BATCH_KEYS + 1)}
            await client.set_many(values)

            self.node.fail_multi_get_after(0)
            with self.assertRaises(ConnectionError) as ctx:
                await client.get_many(list(values.keys()))
            self.assertNotIsInstance(ctx.exception, PartialConnectionLostError)
        finally:
            await client.close()

    async def test_set_many_raises_partial_set_connection_lost_error_when_a_later_chunk_fails(self):
        # set_many's own version of the fix above (issue #411): unlike
        # a wrong-node retry exhausting (which has no value to report),
        # a connection failure mid-batch DOES have something meaningful
        # to attach — which keys an earlier, already-succeeded chunk
        # actually stored.
        from nanocached.client import _MAX_BATCH_KEYS

        client = await self.connect()
        try:
            values = {f"k{i}": f"v{i}" for i in range(_MAX_BATCH_KEYS + 1)}
            self.node.fail_multi_set_after(1)
            with self.assertRaises(PartialSetConnectionLostError) as ctx:
                await client.set_many(values)

            error = ctx.exception
            self.assertEqual(self.node.multi_set_count, 2)
            first_chunk_keys = set(list(values.keys())[:_MAX_BATCH_KEYS])
            self.assertEqual(error.partial_keys, first_chunk_keys)
            self.assertIsInstance(error.__cause__, ConnectionError)

            # The keys the failed chunk never reached are genuinely
            # absent — only the first chunk's keys actually landed.
            stored = await client.get_many(list(first_chunk_keys))
            self.assertEqual(stored, {k: values[k] for k in first_chunk_keys})
            last_key = list(values.keys())[_MAX_BATCH_KEYS]
            self.assertNotIn(last_key, await client.get_many_bytes([last_key]))
        finally:
            await client.close()

    async def test_set_many_propagates_the_bare_error_when_the_first_chunk_fails(self):
        from nanocached.client import _MAX_BATCH_KEYS

        client = await self.connect()
        try:
            values = {f"k{i}": f"v{i}" for i in range(_MAX_BATCH_KEYS + 1)}
            self.node.fail_multi_set_after(0)
            with self.assertRaises(ConnectionError) as ctx:
                await client.set_many(values)
            self.assertNotIsInstance(ctx.exception, PartialSetConnectionLostError)
        finally:
            await client.close()

    async def test_a_multi_get_reply_over_the_cumulative_bytes_bound_is_rejected_as_a_desync(self):
        # Issue #207 (follow-up to #179's Java fix, PR #201): each
        # individual entry's declared length is already capped at
        # _MAX_VALUE_LENGTH, but nothing previously bounded the SUM of
        # every hit's length across one `M` reply — a node answering a
        # 400-key multi-get with 400 x (per-value cap) hits could force
        # hundreds of MB of allocation from a single reply. Shrinks the
        # module-level bound so two small (real, 2-byte) values are
        # enough to trip it without moving tens of megabytes over
        # loopback, mirroring RequestTimeoutTests' own pattern for
        # _REQUEST_TIMEOUT.
        from nanocached import _connection as connection_module

        default_bound = connection_module._MAX_MULTIGET_RESPONSE_BYTES
        connection_module._MAX_MULTIGET_RESPONSE_BYTES = 3
        try:
            client = await self.connect()
            try:
                await client.set("a", "xy")
                await client.set("b", "zw")
                with self.assertRaisesRegex(NanocachedError, "exceeds"):
                    await client.get_many(["a", "b"])

                # The connection was poisoned, so the redial that follows
                # proves the client recovers cleanly instead of staying
                # desynced — restore the real-world bound first so the
                # redial's own (still 4-byte) reply isn't rejected too.
                connection_module._MAX_MULTIGET_RESPONSE_BYTES = default_bound
                self.assertEqual(await client.get_many(["a", "b"]), {"a": "xy", "b": "zw"})
            finally:
                await client.close()
        finally:
            connection_module._MAX_MULTIGET_RESPONSE_BYTES = default_bound

    async def test_a_multi_get_reply_just_under_the_cumulative_bytes_bound_succeeds(self):
        # Companion to the test above: a reply whose cumulative size is
        # right at (not over) the shrunk bound must still succeed
        # normally, proving the check is a strict "greater than", not an
        # off-by-one that rejects legitimate replies.
        from nanocached import _connection as connection_module

        default_bound = connection_module._MAX_MULTIGET_RESPONSE_BYTES
        connection_module._MAX_MULTIGET_RESPONSE_BYTES = 4
        try:
            client = await self.connect()
            try:
                await client.set("a", "xy")
                await client.set("b", "zw")
                self.assertEqual(await client.get_many(["a", "b"]), {"a": "xy", "b": "zw"})
            finally:
                await client.close()
        finally:
            connection_module._MAX_MULTIGET_RESPONSE_BYTES = default_bound


class MultiClusterTests(unittest.IsolatedAsyncioTestCase):
    # Batched get/set (issues #128/#150/#151), cluster coverage: owner
    # grouping (get: primary only; set: every rank), the node-that-is-
    # primary-for-one-key-and-replica-for-another case (#150's
    # asymmetry), and the per-key wrong-node refresh-and-retry contract
    # (_multi_get_pass/_multi_set_pass) — mirrors ReplicationTests'/
    # ClusterTests' own start_cluster/owners_of pattern.

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

    async def test_get_many_reassembles_hits_split_across_owners(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                keys = [f"key-{i}" for i in range(30)]
                values = {key: f"value of {key}" for key in keys}
                await client.set_many(values)
                self.assertEqual(await client.get_many(keys), values)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_set_many_stores_on_every_owner_under_replication(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                keys = [f"key-{i}" for i in range(30)]
                await client.set_many({key: "v" for key in keys})
                for key in keys:
                    for owner in self.owners_of(key):
                        self.assertIn(
                            key.encode(), nodes[owner].store, f"{key} missing from {owner}"
                        )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_node_primary_for_one_key_and_replica_for_another_gets_one_subframe(self):
        # #150's asymmetry: within one set_many batch, the same node can
        # own one key as primary and another as replica at once — it must
        # still receive exactly one `o` sub-frame, never two.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                candidates = [f"k{i}" for i in range(200)]
                owner_sets = {key: self.owners_of(key) for key in candidates}
                pair = next(
                    (key1, key2, owner_sets[key1][0])
                    for key1 in candidates
                    for key2 in candidates
                    if key1 != key2
                    and owner_sets[key1][0] == owner_sets[key2][1]
                    and owner_sets[key1][0] != owner_sets[key2][0]
                )
                key1, key2, shared_owner = pair
                before = nodes[shared_owner].multi_set_count
                await client.set_many({key1: "v1", key2: "v2"})
                self.assertEqual(nodes[shared_owner].multi_set_count - before, 1)
                self.assertEqual(await client.get_many([key1, key2]), {key1: "v1", key2: "v2"})
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_replica_leg_wrong_node_is_swallowed_and_counted(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key = "replica-fails"
                primary, replica = self.owners_of(key)
                nodes[replica].answer_wrong_node_for_keys({key.encode()})
                await client.set_many({key: "v"})  # must not raise
                self.assertEqual(client.stats().replica_write_failures, 1)
                self.assertIn(key.encode(), nodes[primary].store)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_per_key_wrong_node_is_healed_by_one_refresh_and_retry(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key, other = "some-key", "other-key"
                await client.set_many({key: "v", other: "v2"})
                owner = nodes[self.owners_of(key)[0]]

                owner.answer_wrong_node_for_keys({key.encode()}, times=1)
                self.assertEqual(
                    await client.get_many([key, other]), {key: "v", other: "v2"}
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_persisting_per_key_wrong_node_raises_partial_wrong_node_error(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key, other = "some-key", "other-key"
                await client.set_many({key: "v", other: "v2"})
                owner = nodes[self.owners_of(key)[0]]

                owner.answer_wrong_node_for_keys({key.encode()}, times=10)
                with self.assertRaises(PartialWrongNodeError) as ctx:
                    await client.get_many([key, other])
                self.assertEqual(ctx.exception.partial_values, {other: "v2"})
                # A subclass of WrongNodeError — existing handling keeps
                # working unchanged.
                self.assertIsInstance(ctx.exception, WrongNodeError)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_partial_wrong_node_survives_a_non_utf8_partial_value(self):
        # Issue #412(c): the PartialWrongNodeError handler in _get_many()
        # used to build partial_values with a plain .decode(), which
        # raises UnicodeDecodeError on a non-UTF-8 stored value — masking
        # the PartialWrongNodeError (the wrong-node/partial-failure
        # information) it was meant to construct and propagate. Stores
        # raw non-UTF-8 bytes for the key that stays healthy so the
        # decode inside that handler is actually exercised, and asserts
        # PartialWrongNodeError still comes through (with a lossy
        # errors="replace" decode) instead of UnicodeDecodeError.
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key, other = "some-key", "other-key"
                non_utf8 = b"\xff\xfe\x00\x01"
                await client.set_many({key: "v", other: non_utf8})
                owner = nodes[self.owners_of(key)[0]]

                owner.answer_wrong_node_for_keys({key.encode()}, times=10)
                with self.assertRaises(PartialWrongNodeError) as ctx:
                    await client.get_many([key, other])
                self.assertEqual(
                    ctx.exception.partial_values, {other: non_utf8.decode(errors="replace")}
                )
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_set_many_persisting_wrong_node_raises_plain_wrong_node_error(self):
        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                key, other = "some-key", "other-key"
                owner = nodes[self.owners_of(key)[0]]
                owner.answer_wrong_node_for_keys({key.encode()}, times=10)

                with self.assertRaises(WrongNodeError) as ctx:
                    await client.set_many({key: "v", other: "v2"})
                self.assertNotIsInstance(ctx.exception, PartialWrongNodeError)
                # The other key was still stored — a batch never fails as
                # a whole.
                self.assertEqual(await client.get_many([other]), {other: "v2"})
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()

    async def test_a_leg_chunk_failure_does_not_overcount_already_succeeded_keys(self):
        # Issue #411 (cluster-mode sibling of the single-node partial-
        # write fix above): a leg's own o-frame can itself be chunked
        # (issue #222) when it holds more than _MAX_BATCH_KEYS keys —
        # replication=2 on this 2-node cluster means every key's owner
        # list is both nodes, so each node's leg holds every key. A
        # connection failure on that leg's second sub-frame must not
        # re-blame the keys the first sub-frame already resolved: not by
        # retrying an already-primary-stored key, and not by
        # double-counting an already-resolved replica key into
        # replica_write_failures — the same class of overcounting bug
        # already fixed in the other SDKs' leg-runners.
        from nanocached.client import _MAX_BATCH_KEYS

        nodes, discovery = await self.start_cluster()
        try:
            client = await NanocachedClient.connect([("127.0.0.1", discovery.port)])
            try:
                keys = [f"key-{i}" for i in range(_MAX_BATCH_KEYS + 1)]
                values = {key: f"v{i}" for i, key in enumerate(keys)}

                nodes[NAMES[1]].fail_multi_set_after(1)
                await client.set_many(values)  # must not raise

                self.assertGreaterEqual(nodes[NAMES[1]].multi_set_count, 2)
                # Before the fix, every key this leg holds in a replica
                # role (roughly half of _MAX_BATCH_KEYS + 1) would have
                # been double-counted as a replica failure even though
                # the first sub-frame already stored them — only the
                # single key the failing sub-frame actually held can
                # legitimately count here, and only if this leg wasn't
                # that key's primary.
                self.assertLessEqual(client.stats().replica_write_failures, 1)
                self.assertEqual(await client.get_many(keys), values)
            finally:
                await client.close()
        finally:
            await discovery.close()
            for node in nodes.values():
                await node.close()


if __name__ == "__main__":
    unittest.main()
