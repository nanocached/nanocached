import asyncio
import unittest

from nanocached import (
    AlreadyClosedError,
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
        return await NanocachedClient.connect("127.0.0.1", self.node.port, **kwargs)

    async def test_round_trips_set_get_delete(self):
        client = await self.connect()
        try:
            await client.set("greeting", "hello")
            self.assertEqual(await client.get("greeting"), b"hello")
            self.assertTrue(await client.delete("greeting"))
            self.assertIsNone(await client.get("greeting"))
            self.assertFalse(await client.delete("greeting"))
        finally:
            client.close()

    async def test_handles_binary_and_empty_values(self):
        client = await self.connect()
        try:
            await client.set(b"\x01\x02", b"\x00\xff")
            self.assertEqual(await client.get(b"\x01\x02"), b"\x00\xff")
            await client.set("empty", "")
            self.assertEqual(await client.get("empty"), b"")
        finally:
            client.close()

    async def test_ttl_validation_is_synchronous(self):
        client = await self.connect()
        try:
            await client.set("k", "v", ttl_seconds=60)
            self.assertEqual(await client.get("k"), b"v")
            with self.assertRaises(ValueError):
                await client.set("k", "v", ttl_seconds=-1)
            # The rejected set must not have poisoned the connection.
            self.assertEqual(await client.get("k"), b"v")
        finally:
            client.close()

    async def test_authentication(self):
        secure = await MockNode(required_secret=b"s3cret").start()
        try:
            client = await NanocachedClient.connect(
                "127.0.0.1", secure.port, auth_secret="s3cret"
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), b"v")
            finally:
                client.close()

            with self.assertRaisesRegex(Exception, "requires authentication"):
                await NanocachedClient.connect("127.0.0.1", secure.port)
            with self.assertRaisesRegex(Exception, "authentication failed"):
                await NanocachedClient.connect("127.0.0.1", secure.port, auth_secret="wrong")
        finally:
            await secure.close()

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


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_transparently_reconnects_after_a_server_fin(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect("127.0.0.1", node.port)
            try:
                await client.set("k", "v")
                node.drop_connections()
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the client to see the FIN",
                )
                self.assertEqual(await client.get("k"), b"v")
                self.assertEqual(node.connection_count, 2)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_concurrent_requests_share_one_redial(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect("127.0.0.1", node.port)
            try:
                await client.set("k", "v")
                node.drop_connections()
                await wait_for(
                    lambda: client._single is not None and client._single.closed,
                    "the client to see the FIN",
                )
                values = await asyncio.gather(*[client.get("k") for _ in range(10)])
                self.assertTrue(all(value == b"v" for value in values))
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
            client = await NanocachedClient.connect("127.0.0.1", node.port)
            try:
                await client.set("k", "v")
                node.answer_malformed_value_once()
                with self.assertRaises(ConnectionError):
                    await client.get("k")

                self.assertEqual(await client.get("k"), b"v")
                self.assertEqual(node.connection_count, 2)
            finally:
                client.close()
        finally:
            await node.close()

    async def test_a_refresh_finishing_after_close_installs_no_connections(self):
        # Regression for issue #10.
        node = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], node.address)]).start()
        try:
            client = await NanocachedClient.connect("127.0.0.1", discovery.port)
            before = node.connection_count
            client.close()
            await client._refresh_node_list()
            self.assertEqual(node.connection_count, before)
        finally:
            await discovery.close()
            await node.close()


class KeepAliveTests(unittest.IsolatedAsyncioTestCase):
    async def test_pings_an_idle_connection(self):
        node = await MockNode().start()
        try:
            client = await NanocachedClient.connect(
                "127.0.0.1", node.port, keep_alive_interval=0.04
            )
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
            client = await NanocachedClient.connect(
                "127.0.0.1", node.port, keep_alive_interval=0.02
            )
            await wait_for(lambda: node.get_count >= 1, "a keep-alive ping")
            client.close()
            pings = node.get_count
            await asyncio.sleep(0.1)
            self.assertEqual(node.get_count, pings)
        finally:
            await node.close()

    async def test_rejects_a_non_positive_interval(self):
        with self.assertRaises(ValueError):
            await NanocachedClient.connect("127.0.0.1", 1, keep_alive_interval=0)


class SeedTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_missing_target(self):
        with self.assertRaisesRegex(ValueError, "needs either host/port"):
            await NanocachedClient.connect()

    async def test_fails_over_to_the_second_seed(self):
        node = await MockNode().start()
        discovery = await MockDiscovery([(NAMES[0], node.address)]).start()
        dead = await unused_port()
        try:
            client = await NanocachedClient.connect(
                seeds=[("127.0.0.1", dead), ("127.0.0.1", discovery.port)]
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), b"v")
            finally:
                client.close()
        finally:
            await discovery.close()
            await node.close()

    async def test_skips_a_warming_up_seed(self):
        node = await MockNode().start()
        warming = await MockDiscovery([(NAMES[0], node.address)]).start()
        healthy = await MockDiscovery([(NAMES[0], node.address)]).start()
        warming.warming_up = True
        try:
            client = await NanocachedClient.connect(
                seeds=[("127.0.0.1", warming.port), ("127.0.0.1", healthy.port)]
            )
            try:
                await client.set("k", "v")
                self.assertEqual(await client.get("k"), b"v")
            finally:
                client.close()
        finally:
            await warming.close()
            await healthy.close()
            await node.close()

    async def test_raises_busy_when_every_seed_is_warming(self):
        first = await MockDiscovery([]).start()
        second = await MockDiscovery([]).start()
        first.warming_up = True
        second.warming_up = True
        try:
            with self.assertRaises(DiscoveryBusyError):
                await NanocachedClient.connect(
                    seeds=[("127.0.0.1", first.port), ("127.0.0.1", second.port)]
                )
        finally:
            await first.close()
            await second.close()


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
            client = await NanocachedClient.connect("127.0.0.1", discovery.port)
            try:
                keys = [f"key-{i}" for i in range(50)]
                for key in keys:
                    await client.set(key, f"value of {key}")
                for key in keys:
                    self.assertEqual(await client.get(key), f"value of {key}".encode())

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
            client = await NanocachedClient.connect("127.0.0.1", discovery.port)
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
            client = await NanocachedClient.connect("127.0.0.1", discovery.port)
            try:
                key = "some-key"
                await client.set(key, "v")
                owner = dict(nodes)[HashRing([n for n, _ in nodes]).route(key.encode())]
                owner.answer_wrong_node_once()
                self.assertEqual(await client.get(key), b"v")

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
            client = await NanocachedClient.connect("127.0.0.1", discovery.port)
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
        client = await NanocachedClient.connect("127.0.0.1", discovery.port)
        try:
            key = "survives"
            await client.set(key, "still here")
            primary = self.owners_of(key)[0]
            await nodes[primary].close()
            await wait_for(
                lambda: client._members[primary].connection.closed,
                "the client to see the FIN",
            )
            self.assertEqual(await client.get(key), b"still here")
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
        client = await NanocachedClient.connect("127.0.0.1", discovery.port)
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
            self.assertEqual(await client.get(key), b"v")
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
        client = await NanocachedClient.connect("127.0.0.1", discovery.port)
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
            self.assertEqual(await client.get(key), b"v")
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
            client = await NanocachedClient.connect("127.0.0.1", discovery.port)
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


if __name__ == "__main__":
    unittest.main()
