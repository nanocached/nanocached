package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.nanocached.MockServers.MockDiscovery;
import org.nanocached.MockServers.MockNode;

@Timeout(30)
class NanocachedClientTest {
    private static final List<String> NAMES = List.of(
            "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
            "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47");

    private static void waitFor(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(5);
        }
    }

    // ── 単一ノード ────────────────────────────────────────────────

    @Test
    void roundTripsSetGetDelete() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect("127.0.0.1", node.port())) {
                client.set("greeting", "hello");
                assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), client.get("greeting"));
                assertTrue(client.delete("greeting"));
                assertNull(client.get("greeting"));
                assertFalse(client.delete("greeting"));
                assertEquals(1, client.replication());
            }
        }
    }

    @Test
    void validatesTtlSynchronously() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect("127.0.0.1", node.port())) {
                client.set("k", "v", 60);
                assertThrows(IllegalArgumentException.class,
                        () -> client.set("k".getBytes(), "v".getBytes(), -1L));
                // The rejected set must not have poisoned the connection.
                assertArrayEquals("v".getBytes(), client.get("k"));
            }
        }
    }

    @Test
    void authenticates() throws Exception {
        try (MockNode node = new MockNode("s3cret".getBytes(StandardCharsets.UTF_8))) {
            try (NanocachedClient client = NanocachedClient.connect(
                    NanocachedClient.builder().host("127.0.0.1", node.port()).authSecret("s3cret"))) {
                client.set("k", "v");
                assertArrayEquals("v".getBytes(), client.get("k"));
            }

            NanocachedException missing = assertThrows(NanocachedException.class,
                    () -> NanocachedClient.connect("127.0.0.1", node.port()));
            assertTrue(missing.getMessage().contains("requires authentication"));

            NanocachedException wrong = assertThrows(NanocachedException.class,
                    () -> NanocachedClient.connect(NanocachedClient.builder()
                            .host("127.0.0.1", node.port()).authSecret("wrong")));
            assertTrue(wrong.getMessage().contains("authentication failed"));
        }
    }

    @Test
    void wrongNodePropagatesInSingleMode() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect("127.0.0.1", node.port())) {
                node.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> client.get("k"));
            }
        }
    }

    @Test
    void rejectsUseAfterClose() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedClient client = NanocachedClient.connect("127.0.0.1", node.port());
            client.close();
            client.close(); // idempotent
            assertTrue(client.isClosed());
            assertThrows(NanocachedException.AlreadyClosed.class, () -> client.get("k"));
        }
    }

    // ── 遅延再接続と keep-alive ───────────────────────────────────

    @Test
    void transparentlyReconnectsAfterAServerFin() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.dropConnections();
                Thread.sleep(50); // let the FIN land
                assertArrayEquals("v".getBytes(), client.get("k"));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void keepAlivePingsAnIdleConnection() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .host("127.0.0.1", node.port())
                    .keepAliveInterval(Duration.ofMillis(40)))) {
                waitFor(() -> node.getCount.get() >= 2, "keep-alive pings");
                assertEquals(1, node.connectionCount.get());
            }
        }
    }

    @Test
    void rejectsANonPositiveKeepAliveInterval() {
        assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.builder().keepAliveInterval(Duration.ZERO));
    }

    // ── seeds ─────────────────────────────────────────────────────

    @Test
    void rejectsAMissingTarget() {
        assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.connect(NanocachedClient.builder()));
    }

    @Test
    void failsOverToTheSecondSeed() throws Exception {
        try (MockNode node = new MockNode();
                MockDiscovery discovery = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1)) {
            int dead = MockServers.unusedPort();
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .host("127.0.0.1", dead)
                    .host("127.0.0.1", discovery.port()))) {
                client.set("k", "v");
                assertArrayEquals("v".getBytes(), client.get("k"));
            }
        }
    }

    @Test
    void skipsAWarmingUpSeed() throws Exception {
        try (MockNode node = new MockNode();
                MockDiscovery warming = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1);
                MockDiscovery healthy = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1)) {
            warming.warmingUp = true;
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .host("127.0.0.1", warming.port())
                    .host("127.0.0.1", healthy.port()))) {
                client.set("k", "v");
                assertArrayEquals("v".getBytes(), client.get("k"));
            }
        }
    }

    @Test
    void raisesBusyWhenEverySeedIsWarming() throws Exception {
        try (MockDiscovery first = new MockDiscovery(List.of(), 1);
                MockDiscovery second = new MockDiscovery(List.of(), 1)) {
            first.warmingUp = true;
            second.warmingUp = true;
            assertThrows(NanocachedException.DiscoveryBusy.class,
                    () -> NanocachedClient.connect(NanocachedClient.builder()
                            .host("127.0.0.1", first.port())
                            .host("127.0.0.1", second.port())));
        }
    }

    // ── クラスタと複製 ────────────────────────────────────────────

    private record Cluster(Map<String, MockNode> nodes, MockDiscovery discovery)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            discovery.close();
            for (MockNode node : nodes.values()) node.close();
        }
    }

    private static Cluster startCluster(int replication) throws Exception {
        MockNode nodeA = new MockNode();
        MockNode nodeB = new MockNode();
        Map<String, MockNode> nodes = Map.of(NAMES.get(0), nodeA, NAMES.get(1), nodeB);
        MockDiscovery discovery = new MockDiscovery(
                List.of(new DiscoveredNode(NAMES.get(0), nodeA.address()),
                        new DiscoveredNode(NAMES.get(1), nodeB.address())),
                replication);
        return new Cluster(nodes, discovery);
    }

    @Test
    void routesAndReadsItsOwnWrites() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                for (int i = 0; i < 50; i++) {
                    client.set("key-" + i, "value of key-" + i);
                }
                for (int i = 0; i < 50; i++) {
                    assertArrayEquals(("value of key-" + i).getBytes(), client.get("key-" + i));
                }
                int total = cluster.nodes().values().stream().mapToInt(n -> n.store.size()).sum();
                assertEquals(50, total);
                assertTrue(cluster.nodes().values().stream().allMatch(n -> !n.store.isEmpty()));
            }
        }
    }

    @Test
    void wrongNodeTriggersRefreshAndOneRetry() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                String key = "some-key";
                client.set(key, "v");
                MockNode owner = cluster.nodes()
                        .get(new HashRing(NAMES).route(key.getBytes(StandardCharsets.UTF_8)));

                owner.answerWrongNodeOnce();
                assertArrayEquals("v".getBytes(), client.get(key));

                owner.answerWrongNodeOnce();
                owner.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> client.get(key));
            }
        }
    }

    @Test
    void fansWritesOutToEveryOwner() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                assertEquals(2, client.replication());
                for (int i = 0; i < 20; i++) {
                    client.set("key-" + i, "v");
                }
                for (int i = 0; i < 20; i++) {
                    String key = MockNode.keyOf(("key-" + i).getBytes(StandardCharsets.UTF_8));
                    for (MockNode node : cluster.nodes().values()) {
                        assertTrue(node.store.containsKey(key), key + " missing from a node");
                    }
                }
            }
        }
    }

    @Test
    void readsFailOverWhenThePrimaryDies() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                String key = "survives";
                client.set(key, "still here");
                String primary = new HashRing(NAMES)
                        .owners(key.getBytes(StandardCharsets.UTF_8), 2).get(0);
                cluster.nodes().get(primary).close();
                Thread.sleep(50);
                assertArrayEquals("still here".getBytes(), client.get(key));
            }
        }
    }

    @Test
    void aDeadReplicaDoesNotFailWrites() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                String key = "written-anyway";
                List<String> owners =
                        new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                cluster.nodes().get(owners.get(1)).close();
                Thread.sleep(50);
                client.set(key, "v");
                assertTrue(cluster.nodes().get(owners.get(0)).store
                        .containsKey(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8))));
                assertArrayEquals("v".getBytes(), client.get(key));
            }
        }
    }

    @Test
    void writesRouteAroundADeadPrimaryOnceDiscoveryDropsIt() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                String key = "written-after-primary-death";
                List<String> owners =
                        new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);

                // The primary dies AND discovery has already noticed: the
                // first write attempt fails on the dead primary, forcing a
                // refresh that re-ranks onto the survivor, and the retry
                // succeeds.
                cluster.nodes().get(owners.get(0)).close();
                cluster.discovery().nodes = List.of(new DiscoveredNode(
                        owners.get(1), cluster.nodes().get(owners.get(1)).address()));
                Thread.sleep(50);

                client.set(key, "v");
                assertArrayEquals("v".getBytes(), client.get(key));
            }
        }
    }

    @Test
    void fansDeletesOutToEveryOwner() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client =
                    NanocachedClient.connect("127.0.0.1", cluster.discovery().port())) {
                String key = "gone-everywhere";
                client.set(key, "v");
                assertTrue(client.delete(key));
                String stored = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));
                for (MockNode node : cluster.nodes().values()) {
                    assertFalse(node.store.containsKey(stored));
                }
            }
        }
    }
}
