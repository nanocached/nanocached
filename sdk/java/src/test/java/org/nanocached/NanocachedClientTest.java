package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.nanocached.MockServers.MockDiscovery;
import org.nanocached.MockServers.MockNode;
import org.nanocached.MockServers.Tls;
import org.nanocached.NanocachedClient.Address;

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

    /** Calls {@code get} until it fails with a {@link
     * NanocachedException.ConnectionFailed} (see
     * reconnectCooldownSkipsARedialToAKnownDeadAddress for why the first
     * call after a node closes is not asserted on directly), returning
     * that failure. */
    private static NanocachedException.ConnectionFailed firstConnectionFailure(NanocachedClient client)
            throws InterruptedException {
        return firstConnectionFailure(client, -1);
    }

    private static NanocachedException.ConnectionFailed firstConnectionFailure(NanocachedClient client, int port)
            throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        Object lastResult = null;
        while (true) {
            try {
                lastResult = client.get("k");
            } catch (NanocachedException.ConnectionFailed failure) {
                return failure;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for a get to fail against the closed node on port "
                        + port + "; last get returned " + lastResult + "; stats=" + client.stats());
            }
            Thread.sleep(5);
        }
    }

    /** Single-target connect, mirroring the removed connect(host, port) shorthand. */
    private static NanocachedClient connect(String host, int port) {
        return NanocachedClient.connect(single(host, port));
    }

    private static NanocachedClient.Options single(String host, int port) {
        return NanocachedClient.builder().addresses(List.of(new Address(host, port)));
    }

    /** Captures whatever is printed to stderr while {@code action} runs. */
    private static String captureStderr(Runnable action) {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    // ── 単一ノード ────────────────────────────────────────────────

    @Test
    void roundTripsSetGetDelete() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("greeting", "hello");
                assertEquals(Optional.of("hello"), client.get("greeting"));
                assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8),
                        client.getBytes("greeting").orElseThrow());
                assertTrue(client.delete("greeting"));
                assertEquals(Optional.empty(), client.get("greeting"));
                assertEquals(Optional.empty(), client.getBytes("greeting"));
                assertFalse(client.delete("greeting"));
                assertEquals(1, client.replication());
            }
        }
    }

    @Test
    void getBytesRoundTripsArbitraryBytes() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[] value = {0, 1, 2, (byte) 0xFF, 0x7F};
                client.set("raw".getBytes(StandardCharsets.UTF_8), value);
                assertArrayEquals(value, client.getBytes("raw").orElseThrow());
            }
        }
    }

    @Test
    void getRejectsNonUtf8Values() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[] invalidUtf8 = {(byte) 0xFF, (byte) 0xFE};
                client.set("bad".getBytes(StandardCharsets.UTF_8), invalidUtf8);
                assertThrows(UncheckedIOException.class, () -> client.get("bad"));
                // The raw bytes are still retrievable via getBytes.
                assertArrayEquals(invalidUtf8, client.getBytes("bad").orElseThrow());
            }
        }
    }

    @Test
    void ttlZeroMeansNoExpiryAndNegativeIsRejected() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 0);
                assertEquals(Optional.of("v"), client.get("k"));

                client.set("k2", "v2"); // defaults to ttlSeconds = 0
                assertEquals(Optional.of("v2"), client.get("k2"));

                assertThrows(IllegalArgumentException.class, () -> client.set("k", "v", -1L));
                assertThrows(IllegalArgumentException.class,
                        () -> client.set("k".getBytes(StandardCharsets.UTF_8),
                                "v".getBytes(StandardCharsets.UTF_8), -1L));
                // The rejected set must not have poisoned the connection.
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    // ── バッチ get/set (issue #151) ──────────────────────────────

    @Test
    void getManyReturnsHitsAndOmitsMisses() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("a", "1");
                client.set("b", "2");
                Map<String, String> values = client.getMany(List.of("a", "b", "missing"));
                assertEquals(Map.of("a", "1", "b", "2"), values);
                assertEquals(1, node.multiGetCount.get());
            }
        }
    }

    // issue #386: the per-value decompression cap alone lets a batch
    // amplify it by the key count (batch × 64 MiB from one small wire
    // reply); the cumulative budget must abort the batch instead. The cap
    // is lowered so the test doesn't allocate the real 256 MiB bound. The
    // budget only applies when compress is enabled (issue #410b).
    @Test
    void getManyCapsCumulativeDecompressedBytesAcrossTheBatch() throws Exception {
        long saved = Compression.maxMultiGetDecompressedBytes;
        Compression.maxMultiGetDecompressedBytes = 4;
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client =
                    NanocachedClient.connect(single("127.0.0.1", node.port()).compress(true))) {
                client.set("a", "12345678");
                client.set("b", "12345678");
                NanocachedException.DecompressionFailed error =
                        assertThrows(NanocachedException.DecompressionFailed.class,
                                () -> client.getMany(List.of("a", "b")));
                assertTrue(error.getMessage().contains("across the batch"));
                // A single-key read is untouched by the batch bound.
                assertEquals("12345678", client.get("a").orElseThrow());
            }
        } finally {
            Compression.maxMultiGetDecompressedBytes = saved;
        }
    }

    // issue #410a: the cumulative budget used to be checked BEFORE
    // charging the current entry, so the entry that actually crosses the
    // cap always slipped through uncaught — and if it was the last hit in
    // the response, the guard never fired at all. Only one key here, so
    // the crossing entry is necessarily the last (and only) one;
    // charge-then-check must still catch it.
    @Test
    void getManyBudgetCatchesTheCrossingEntryEvenWhenLast() throws Exception {
        long saved = Compression.maxMultiGetDecompressedBytes;
        Compression.maxMultiGetDecompressedBytes = 1;
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client =
                    NanocachedClient.connect(single("127.0.0.1", node.port()).compress(true))) {
                client.set("a", "12");
                assertThrows(NanocachedException.DecompressionFailed.class,
                        () -> client.getMany(List.of("a")));
            }
        } finally {
            Compression.maxMultiGetDecompressedBytes = saved;
        }
    }

    // issue #410b: the budget used to be charged and enforced even when
    // the client has compression disabled, so a large uncompressed batch
    // could fail with a misleading "decompression bomb" error. The budget
    // is lowered far below what this batch would need if it were
    // (wrongly) charged.
    @Test
    void getManyDoesNotChargeTheBudgetWhenCompressIsDisabled() throws Exception {
        long saved = Compression.maxMultiGetDecompressedBytes;
        Compression.maxMultiGetDecompressedBytes = 1;
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("a", "12345678");
                client.set("b", "12345678");
                Map<String, String> values = client.getMany(List.of("a", "b"));
                assertEquals(Map.of("a", "12345678", "b", "12345678"), values);
            }
        } finally {
            Compression.maxMultiGetDecompressedBytes = saved;
        }
    }

    @Test
    void getManyBytesRoundTripsArbitraryBytes() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[] value = {0, 1, 2, (byte) 0xFF, 0x7F};
                client.set("raw".getBytes(StandardCharsets.UTF_8), value);
                Map<String, byte[]> values = client.getManyBytes(List.of("raw"));
                assertArrayEquals(value, values.get("raw"));
            }
        }
    }

    @Test
    void getManyRejectsAnEmptyKeyList() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertThrows(IllegalArgumentException.class, () -> client.getMany(List.of()));
                assertThrows(IllegalArgumentException.class, () -> client.getManyBytes(List.of()));
            }
        }
    }

    @Test
    void setManyStoresEveryPairAndGetManyReadsThemBack() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.setMany(Map.of("a", "1", "b", "2", "c", "3"));
                assertEquals(Map.of("a", "1", "b", "2", "c", "3"),
                        client.getMany(List.of("a", "b", "c")));
                assertEquals(1, node.multiSetCount.get());
            }
        }
    }

    @Test
    void setManyTtlZeroMeansNoExpiryAndNegativeIsRejected() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.setMany(Map.of("k", "v"), 0);
                assertEquals(Optional.of("v"), client.get("k"));
                assertThrows(IllegalArgumentException.class, () -> client.setMany(Map.of("k", "v"), -1L));
                assertThrows(IllegalArgumentException.class, () -> client.setManyBytes(
                        Map.of("k", "v".getBytes(StandardCharsets.UTF_8)), -1L));
            }
        }
    }

    @Test
    void setManyRejectsAnEmptyValueMap() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertThrows(IllegalArgumentException.class, () -> client.setMany(Map.of()));
                assertThrows(IllegalArgumentException.class, () -> client.setManyBytes(Map.of()));
            }
        }
    }

    @Test
    void batchedGetSetAreScopedByNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace ns = client.namespace("tenant-a");
                ns.setMany(Map.of("k", "namespaced"));
                client.setMany(Map.of("k", "default"));
                assertEquals(Map.of("k", "namespaced"), ns.getMany(List.of("k")));
                assertEquals(Map.of("k", "default"), client.getMany(List.of("k")));
            }
        }
    }

    @Test
    void wrongNodePropagatesForBatchedOpsInSingleMode() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                node.answerWrongNodeOnce();
                assertThrows(NanocachedException.PartialWrongNode.class,
                        () -> client.getManyBytes(List.of("a", "b")));
                node.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class,
                        () -> client.setMany(Map.of("a", "1")));
            }
        }
    }

    // ── byte[]-keyed バッチ get/set (issue #160) ──────────────────

    @Test
    void positionalGetManyBytesHandlesNonUtf8KeysAndMisses() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[] opaque = {(byte) 0xFF, (byte) 0xFE, 0, 1};
                byte[] magic = {(byte) 0xAC, (byte) 0xED, 0, 5};
                client.set(opaque, "one".getBytes(StandardCharsets.UTF_8));
                client.set(magic, "two".getBytes(StandardCharsets.UTF_8));
                byte[][] values = client.getManyBytes(new byte[][] {
                        opaque, "missing".getBytes(StandardCharsets.UTF_8), magic});
                assertEquals(3, values.length);
                assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), values[0]);
                assertNull(values[1]);
                assertArrayEquals("two".getBytes(StandardCharsets.UTF_8), values[2]);
                assertEquals(1, node.multiGetCount.get());
            }
        }
    }

    @Test
    void positionalSetManyBytesStoresEveryPair() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[][] keys = {{(byte) 0xFF, 1}, {(byte) 0xFF, 2}};
                byte[][] values = {{10}, {20}};
                client.setManyBytes(keys, values);
                assertEquals(1, node.multiSetCount.get());
                assertArrayEquals(new byte[] {10}, client.getBytes(keys[0]).orElseThrow());
                assertArrayEquals(new byte[] {20}, client.getBytes(keys[1]).orElseThrow());
            }
        }
    }

    @Test
    void positionalBulkOpsValidateTheirArguments() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertThrows(IllegalArgumentException.class, () -> client.getManyBytes(new byte[0][]));
                assertThrows(IllegalArgumentException.class,
                        () -> client.setManyBytes(new byte[0][], new byte[0][]));
                assertThrows(IllegalArgumentException.class,
                        () -> client.setManyBytes(new byte[][] {{1}, {2}}, new byte[][] {{1}}));
                assertThrows(IllegalArgumentException.class,
                        () -> client.setManyBytes(new byte[][] {{1}}, new byte[][] {{1}}, -1L));
            }
        }
    }

    @Test
    void positionalBulkOpsAreNamespaced() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace ns = client.namespace("tenant");
                byte[][] key = {{7}};
                ns.setManyBytes(key, new byte[][] {{1}});
                client.setManyBytes(key, new byte[][] {{2}});
                assertArrayEquals(new byte[] {1}, ns.getManyBytes(key)[0]);
                assertArrayEquals(new byte[] {2}, client.getManyBytes(key)[0]);
            }
        }
    }

    @Test
    void positionalWrongNodeNamesTheUnresolvedIndices() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                node.answerWrongNodeOnce();
                NanocachedException.PartialWrongNodeRaw failure = assertThrows(
                        NanocachedException.PartialWrongNodeRaw.class,
                        () -> client.getManyBytes(new byte[][] {{1}, {2}}));
                assertEquals(2, failure.partialValues.length);
                assertTrue(failure.unresolvedIndices.length > 0);
            }
        }
    }

    // ── multi-get/multi-set request cumulative byte bound (issue #222) ──

    @Test
    void setManyBytesSplitsAByteOversizedBatchIntoSeveralSubFrames() throws Exception {
        // Regression (issue #222): multiSetChunked split purely by key
        // count (MAX_BATCH_KEYS), so a handful of individually valid
        // pairs — each comfortably under MAX_REQUEST_BYTES alone — could
        // still sum past it in one `o` frame. 7 x 300 KB values is
        // ~2.1 MB, well past the server's ~1 MiB MAX_REQUEST_SIZE, which
        // would otherwise make the server drop the connection with no
        // response (request_is_too_large, src/server.rs).
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int pairCount = 7;
                byte[][] keys = new byte[pairCount][];
                byte[][] values = new byte[pairCount][];
                for (int i = 0; i < pairCount; i++) {
                    keys[i] = ("k" + i).getBytes(StandardCharsets.UTF_8);
                    values[i] = new byte[300_000];
                    java.util.Arrays.fill(values[i], (byte) i);
                }

                client.setManyBytes(keys, values);

                assertTrue(node.multiSetCount.get() > 1,
                        "a batch summing past MAX_REQUEST_BYTES must split into more than one `o` frame, got "
                                + node.multiSetCount.get());
                for (int bodyBytes : node.multiSetFrameBodyBytes) {
                    assertTrue(bodyBytes < 1024 * 1024,
                            "each `o` sub-frame's body must stay under the server's 1 MiB frame cap, got "
                                    + bodyBytes);
                }

                for (int i = 0; i < pairCount; i++) {
                    assertArrayEquals(values[i], client.getBytes(keys[i]).orElseThrow());
                }
            }
        }
    }

    @Test
    void getManySplitsABatchOfByteOversizedKeysIntoSeveralSubFrames() throws Exception {
        // getMany's counterpart to the setManyBytes regression above:
        // multiGetChunked split purely by key count too, so a handful of
        // individually valid but large keys could sum past
        // MAX_REQUEST_BYTES in one `m` frame.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int pairCount = 7;
                List<String> keys = new ArrayList<>();
                java.util.Map<String, String> expected = new java.util.LinkedHashMap<>();
                for (int i = 0; i < pairCount; i++) {
                    String key = "k" + i + "-" + "x".repeat(300_000);
                    String value = "v" + i;
                    client.set(key, value);
                    keys.add(key);
                    expected.put(key, value);
                }

                Map<String, String> values = client.getMany(keys);
                assertEquals(expected, values);

                assertTrue(node.multiGetCount.get() > 1,
                        "a batch of keys summing past MAX_REQUEST_BYTES must split into more than one `m` frame, got "
                                + node.multiGetCount.get());
                for (int bodyBytes : node.multiGetFrameBodyBytes) {
                    assertTrue(bodyBytes < 1024 * 1024,
                            "each `m` sub-frame's body must stay under the server's 1 MiB frame cap, got "
                                    + bodyBytes);
                }
            }
        }
    }

    // ── chunked multi-get/multi-set partial success on connection
    //    failure (issue #411) ───────────────────────────────────────

    @Test
    void getManyBytesChunkedConnectionFailureCarriesFirstChunksValues() throws Exception {
        // Regression for issue #411: multiGetChunked used to let a
        // connection failure on the SECOND `m` sub-frame — after the
        // first already succeeded, and after the client's own built-in
        // reconnect-and-retry also failed (the node is fully torn down
        // by then) — propagate as a bare ConnectionFailed, discarding
        // everything the first sub-frame had already resolved.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int keyCount = 401; // MAX_BATCH_KEYS(400) + 1 forces exactly two `m` sub-frames
                byte[][] keys = new byte[keyCount][];
                for (int i = 0; i < keyCount; i++) {
                    keys[i] = ("k" + i).getBytes(StandardCharsets.UTF_8);
                }
                byte[] firstValue = "first-chunk-value".getBytes(StandardCharsets.UTF_8);
                client.set(keys[0], firstValue);

                node.closeAfterNextMultiGet();
                NanocachedException.PartialConnectionFailedRaw failure = assertThrows(
                        NanocachedException.PartialConnectionFailedRaw.class, () -> client.getManyBytes(keys));

                assertEquals(1, node.multiGetCount.get(),
                        "the node must have answered exactly the first sub-frame before it went down");
                assertEquals(keyCount, failure.partialValues.length);
                assertArrayEquals(firstValue, failure.partialValues[0]);
                assertEquals(1, failure.unresolvedIndices.length);
                assertEquals(400, failure.unresolvedIndices[0],
                        "only the second sub-frame's own key (index 400) should be unresolved");
            }
        }
    }

    @Test
    void getManyBytesStringKeyedChunkedConnectionFailureCarriesFirstChunksValues() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int keyCount = 401;
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < keyCount; i++) keys.add("k" + i);
                client.set("k0", "first-chunk-value");

                node.closeAfterNextMultiGet();
                NanocachedException.PartialConnectionFailed failure = assertThrows(
                        NanocachedException.PartialConnectionFailed.class, () -> client.getManyBytes(keys));

                assertEquals("first-chunk-value",
                        new String(failure.partialValues.get("k0"), StandardCharsets.UTF_8));
                assertEquals(1, failure.partialValues.size(),
                        "k0 is the only key ever actually set — every other key in the first chunk is "
                                + "a genuine miss, and the second chunk's own key never even ran");
            }
        }
    }

    @Test
    void getManyChunkedConnectionFailureCarriesFirstChunksValues() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int keyCount = 401;
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < keyCount; i++) keys.add("k" + i);
                client.set("k0", "first-chunk-value");

                node.closeAfterNextMultiGet();
                NanocachedException.PartialConnectionFailedStrings failure = assertThrows(
                        NanocachedException.PartialConnectionFailedStrings.class, () -> client.getMany(keys));

                assertEquals(Map.of("k0", "first-chunk-value"), failure.partialValues);
            }
        }
    }

    @Test
    void setManyBytesChunkedConnectionFailureCarriesFirstChunksSucceededIndices() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int pairCount = 401; // MAX_BATCH_KEYS(400) + 1 forces exactly two `o` sub-frames
                byte[][] keys = new byte[pairCount][];
                byte[][] values = new byte[pairCount][];
                for (int i = 0; i < pairCount; i++) {
                    keys[i] = ("k" + i).getBytes(StandardCharsets.UTF_8);
                    values[i] = new byte[] {(byte) i};
                }

                node.closeAfterNextMultiSet();
                NanocachedException.PartialConnectionFailedSetRaw failure = assertThrows(
                        NanocachedException.PartialConnectionFailedSetRaw.class,
                        () -> client.setManyBytes(keys, values));

                assertEquals(1, node.multiSetCount.get(),
                        "the node must have answered exactly the first sub-frame before it went down");
                assertEquals(400, failure.succeededIndices.length);
                for (int i = 0; i < 400; i++) {
                    assertEquals(i, failure.succeededIndices[i]);
                }
                // The first chunk's own writes must really have landed,
                // not just been reported as succeeded.
                assertArrayEquals(values[0], node.store.get(MockNode.keyOf(keys[0])));
                assertArrayEquals(values[399], node.store.get(MockNode.keyOf(keys[399])));
            }
        }
    }

    @Test
    void setManyBytesMapKeyedChunkedConnectionFailureCarriesFirstChunksSucceededKeys() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int pairCount = 401;
                Map<String, byte[]> values = new java.util.LinkedHashMap<>();
                for (int i = 0; i < pairCount; i++) {
                    values.put("k" + i, new byte[] {(byte) i});
                }

                node.closeAfterNextMultiSet();
                NanocachedException.PartialConnectionFailedSet failure = assertThrows(
                        NanocachedException.PartialConnectionFailedSet.class, () -> client.setManyBytes(values));

                assertEquals(400, failure.succeededKeys.size());
                assertTrue(failure.succeededKeys.contains("k0"));
                assertTrue(failure.succeededKeys.contains("k399"));
                assertFalse(failure.succeededKeys.contains("k400"),
                        "the second sub-frame's own key must not be reported as succeeded");
            }
        }
    }

    @Test
    void chunkedConnectionFailureOnTheFirstSubFrameStaysAPlainConnectionFailed() throws Exception {
        // issue #411's fix only wraps a chunk's connection failure into a
        // Partial* exception once an earlier chunk has already succeeded
        // — the very first (and, here, only) chunk failing must keep
        // throwing the exact same plain ConnectionFailed it always has,
        // not some Partial* subclass.
        try (MockNode node = new MockNode()) {
            int port = node.port();
            try (NanocachedClient client = connect("127.0.0.1", port)) {
                node.close();
                firstConnectionFailure(client, port); // establishes the node is reliably down

                NanocachedException.ConnectionFailed getFailure = assertThrows(
                        NanocachedException.ConnectionFailed.class,
                        () -> client.getManyBytes(new byte[][] {{1}, {2}}));
                assertSame(NanocachedException.ConnectionFailed.class, getFailure.getClass());

                NanocachedException.ConnectionFailed setFailure = assertThrows(
                        NanocachedException.ConnectionFailed.class,
                        () -> client.setManyBytes(new byte[][] {{1}, {2}}, new byte[][] {{9}, {9}}));
                assertSame(NanocachedException.ConnectionFailed.class, setFailure.getClass());
            }
        }
    }

    @Test
    void clusterMultiSetLegOnlyCountsTheFailedChunksReplicaKeysAsFailures() throws Exception {
        // Regression for issue #411's cluster-leg overcount (mirroring
        // the same bug already found and fixed in Rust's
        // run_multi_set_leg): runMultiSetLeg used to mark EVERY key in
        // its whole OwnerBatch as failed on any connection failure, even
        // keys an earlier, already-succeeded `o` sub-frame had already
        // confirmed stored — overcounting replicaWriteFailures for keys
        // that in fact landed just fine.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                HashRing ring = new HashRing(NAMES);
                // 5 keys that all route primary to NAMES.get(0), so
                // NAMES.get(1) is a pure-replica leg for the whole batch:
                // a pure-replica leg never contributes to the
                // client-visible retry list, so replicaWriteFailures is
                // the only observable signal of its connection failure.
                List<String> keys = new ArrayList<>();
                int candidate = 0;
                while (keys.size() < 5) {
                    String key = "k" + candidate++;
                    if (ring.route(key.getBytes(StandardCharsets.UTF_8)).equals(NAMES.get(0))) {
                        keys.add(key);
                    }
                }
                Map<String, byte[]> values = new java.util.LinkedHashMap<>();
                for (String key : keys) {
                    byte[] value = new byte[300_000];
                    java.util.Arrays.fill(value, (byte) 7);
                    values.put(key, value);
                }

                MockNode replicaLeg = cluster.nodes().get(NAMES.get(1));
                replicaLeg.closeAfterNextMultiSet();

                long before = client.stats().replicaWriteFailures();
                // Must NOT throw: a pure-replica leg's failure is
                // logged-and-swallowed into stats, never surfaced to the
                // caller.
                client.setManyBytes(values);
                long delta = client.stats().replicaWriteFailures() - before;

                assertTrue(delta > 0,
                        "the failed chunk's own replica-held keys must still be counted, got " + delta);
                assertTrue(delta < keys.size(),
                        "only the failed chunk's own keys should count as new failures — got " + delta
                                + " for a " + keys.size() + "-key batch whose first `o` sub-frame already "
                                + "succeeded");
            }
        }
    }

    // ── INCR/DECR (issue #129) ────────────────────────────────────

    @Test
    void incrOnAMissingKeyReturnsEmpty() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertEquals(java.util.OptionalLong.empty(), client.incr("missing", 1));
                assertEquals(java.util.OptionalLong.empty(), client.decr("missing"));
            }
        }
    }

    @Test
    void incrOnANonNumericStoredValueThrowsNotNumeric() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("word", "hello");
                assertThrows(NanocachedException.NotNumeric.class, () -> client.incr("word", 1));
                // The failed INCR must not have touched the stored value.
                assertEquals(Optional.of("hello"), client.get("word"));
            }
        }
    }

    @Test
    void aSuccessfulIncrReturnsTheNewValue() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("counter", "10");
                assertEquals(java.util.OptionalLong.of(15), client.incr("counter", 5));
                assertEquals(java.util.OptionalLong.of(12), client.incr("counter", -3));
                assertEquals(java.util.OptionalLong.of(13), client.incr("counter")); // defaults to delta 1
                assertEquals(Optional.of("13"), client.get("counter"));
            }
        }
    }

    @Test
    void decrWithAPositiveAmountMatchesIncrWithTheNegatedDelta() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("counter", "20");
                assertEquals(java.util.OptionalLong.of(15), client.decr("counter", 5));
                assertEquals(java.util.OptionalLong.of(14), client.decr("counter")); // defaults to amount 1
                assertEquals(Optional.of("14"), client.get("counter"));

                assertThrows(IllegalArgumentException.class, () -> client.decr("counter", Long.MIN_VALUE));
            }
        }
    }

    @Test
    void incrOverflowAnswersNotNumeric() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("huge", String.valueOf(Long.MAX_VALUE));
                assertThrows(NanocachedException.NotNumeric.class, () -> client.incr("huge", 1));
            }
        }
    }

    @Test
    void incrAndDecrAreScopedByNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace tenant = client.namespace("tenant-a");
                tenant.set("counter", "1");
                assertEquals(java.util.OptionalLong.of(2), tenant.incr("counter"));
                // The default (un-namespaced) keyspace is untouched.
                assertEquals(Optional.empty(), client.get("counter"));
                assertEquals(java.util.OptionalLong.empty(), client.incr("counter"));
                assertEquals(java.util.OptionalLong.of(1), tenant.decr("counter"));
            }
        }
    }

    // issue #321: compress has no marker byte distinguishing a compressed
    // value from INCR's plain-decimal-ASCII result, so a compress-enabled
    // client must refuse incr/decr outright, before any I/O.
    @Test
    void incrAndDecrRejectCompressEnabledClientsBeforeAnyIO() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", node.port()).compress(true))) {
                int connectionsAfterConnect = node.connectionCount.get();
                assertThrows(NanocachedException.CompressionIncompatible.class,
                        () -> client.incr("counter", 1));
                assertThrows(NanocachedException.CompressionIncompatible.class,
                        () -> client.decr("counter", 1));
                assertThrows(NanocachedException.CompressionIncompatible.class,
                        () -> client.namespace("tenant-a").incr("counter"));
                assertThrows(NanocachedException.CompressionIncompatible.class,
                        () -> client.namespace("tenant-a").decr("counter"));
                // No new connection was opened for any of the rejected calls.
                assertEquals(connectionsAfterConnect, node.connectionCount.get());
            }
        }
    }

    // issue #225: INCR is not idempotent, so unlike get/set/delete the
    // built-in redial-and-retry must never resend it once its bytes may
    // already have reached the server. These two tests cover the two
    // outcomes the fix distinguishes.

    @Test
    void incrIsRetriedAfterRedialWhenTheConnectionWasAlreadyDead() throws Exception {
        // The connection dies (mirrors the server's own idle timeout, see
        // transparentlyReconnectsAfterAServerFin) before this call's `i`
        // frame is ever written — nothing was sent, so the client's
        // lazy-reconnect-on-use redials and retries transparently, same
        // as get/set/delete.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("counter", "10");
                node.dropConnections();
                Thread.sleep(50); // let the FIN land
                assertEquals(java.util.OptionalLong.of(15), client.incr("counter", 5));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void incrAppliedButAcknowledgementLostIsNeverReplayed() throws Exception {
        // The primary reads the `i` request and applies it, but the
        // connection dies before its `I` reply arrives — the request
        // definitely reached the server, so a blind retry here would
        // double-apply the increment. The client must surface
        // ConnectionFailed instead of resending it, and the counter must
        // have moved exactly once.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("counter", "10");
                node.dropReplyAfterNextIncr();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.incr("counter", 5));
                assertEquals(Optional.of("15"), client.get("counter"));
            }
        }
    }

    // ── Touch / get-and-touch (issue #825) ──────────────────────────

    @Test
    void touchFramesCarryTheTtlAndTheGetFlag() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                client.touch("k", 30);
                client.touch("k", NanocachedClient.REMOVE_TTL);
                client.getAndTouch("k", 45);
                client.getAndTouch("k", NanocachedClient.REMOVE_TTL);
                client.namespace("tenant").touch("k", 7);
                assertEquals(
                        List.of("t 0 1 30", "t 0 1", "t 0 1 45 G", "t 0 1 G", "t 6 1 7"),
                        node.touchFrames);
            }
        }
    }

    @Test
    void touchReplacesTheTtlInPlaceAndNeverCreatesAKey() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 100);
                assertEquals(Long.valueOf(100), node.ttls.get("k"));

                assertTrue(client.touch("k", 30));
                assertEquals(Long.valueOf(30), node.ttls.get("k"));
                assertEquals(Optional.of("v"), client.get("k"));

                assertTrue(client.touch("k", NanocachedClient.REMOVE_TTL));
                assertNull(node.ttls.get("k"));

                assertFalse(client.touch("missing", 30));
                assertFalse(node.store.containsKey("missing"));

                // 0 expires the entry at once — the opposite of set(), where 0 means no expiry.
                assertTrue(client.touch("k", 0));
                assertEquals(Optional.empty(), client.get("k"));
            }
        }
    }

    @Test
    void getAndTouchReturnsTheValueAndMissesWithoutCreatingAnything() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "value", 100);
                assertEquals(Optional.of("value"), client.getAndTouch("k", 30));
                assertEquals(Long.valueOf(30), node.ttls.get("k"));
                assertArrayEquals("value".getBytes(StandardCharsets.UTF_8),
                        client.getAndTouchBytes("k", NanocachedClient.REMOVE_TTL).orElseThrow());
                assertNull(node.ttls.get("k"));

                assertEquals(Optional.empty(), client.getAndTouch("missing", 30));
                assertFalse(node.store.containsKey("missing"));
            }
        }
    }

    @Test
    void touchIsScopedToItsNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace tenant = client.namespace("tenant");
                tenant.set("k", "in-tenant", 100);
                client.set("k", "in-default", 100);

                assertTrue(tenant.touch("k", 5));
                assertEquals(Optional.of("in-tenant"), tenant.getAndTouch("k", 6));
                assertArrayEquals("in-tenant".getBytes(StandardCharsets.UTF_8),
                        tenant.getAndTouchBytes("k", 7).orElseThrow());
                assertFalse(tenant.touch("only-default", 5));

                assertEquals(Long.valueOf(7), node.namespacedTtls.get("tenant").get("k"));
                assertEquals(Long.valueOf(100), node.ttls.get("k"), "the default keyspace was not touched");
            }
        }
    }

    @Test
    void getAndTouchDecompressesWhenCompressionIsEnabled() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", node.port()).compress(true))) {
                String value = "compressible ".repeat(200);
                client.set("k", value);
                assertEquals(Optional.of(value), client.getAndTouch("k", 30));
            }
        }
    }

    @Test
    void touchRejectsAnInvalidTtlBeforeAnyIo() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                assertThrows(IllegalArgumentException.class, () -> client.touch("k", -2));
                assertThrows(IllegalArgumentException.class, () -> client.touch("k", Long.MIN_VALUE));
                assertThrows(IllegalArgumentException.class, () -> client.getAndTouch("k", -2));
                assertTrue(node.touchFrames.isEmpty());
            }
        }
    }

    @Test
    void touchWorksOnATaggedConnection() throws Exception {
        try (MockNode node = MockNode.withTagSupport()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 100);
                assertTrue(client.touch("k", 30));
                assertEquals(Optional.of("v"), client.getAndTouch("k", 40));
                assertFalse(client.touch("missing", 5));
                assertEquals(Long.valueOf(40), node.ttls.get("k"));
                assertEquals(3, node.touchFrames.size());
            }
        }
    }

    @Test
    void anOldServerIsReportedAsServerTooOldAndTheClientStaysUsable() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.rejectTouchLikeAnOldServer();
                assertThrows(NanocachedException.ServerTooOld.class, () -> client.touch("k", 30));
                assertThrows(NanocachedException.ServerTooOld.class, () -> client.getAndTouch("k", 30));
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void touchAppliedButAcknowledgementLostIsResentUnlikeIncr() throws Exception {
        // A touch is idempotent, so a lost reply is resent like get/set/
        // delete — where INCR throws ConnectionFailed instead.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 100);
                node.dropReplyAfterNextTouch();
                assertTrue(client.touch("k", 30));
                assertEquals(2, node.touchFrames.size());

                client.set("counter", "10");
                node.dropReplyAfterNextIncr();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.incr("counter", 5));
            }
        }
    }

    // Issue #901: ttl 0 expires the entry, so a resend after the node applied
    // it (reply lost) finds the key gone.

    @Test
    void anExpiringTouchWhoseReplyWasLostIsNotResent() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));

                client.set(key, "payload", 100);
                primary.dropReplyAfterNextTouch();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.touch(key, 0));
                assertEquals(1, primary.touchFrames.size(), "the applied touch was not sent again");
            }
        }
    }

    @Test
    void anExpiringGetAndTouchWhoseReplyWasLostIsNotResent() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));

                client.set(key, "payload", 100);
                primary.dropReplyAfterNextTouch();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.getAndTouch(key, 0));
                assertEquals(1, primary.touchFrames.size(), "the applied get-and-touch was not sent again");
            }
        }
    }

    @Test
    void anExpiringTouchOnASingleNodeIsNotResentAfterALostReply() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "payload", 100);
                node.dropReplyAfterNextTouch();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.getAndTouch("k", 0));
                assertEquals(1, node.touchFrames.size());
            }
        }
    }

    @Test
    void anExpiringTouchStillRetriesARequestThatWasNeverSent() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));

                client.set(key, "payload", 100);
                primary.answerWrongNodeOnce();
                assertEquals(Optional.of("payload"), client.getAndTouch(key, 0));
                assertEquals(2, primary.touchFrames.size(), "the W answer was followed by one resend");

                client.set(key, "payload", 100);
                primary.answerWrongNodeOnce();
                assertTrue(client.touch(key, 0));
                assertEquals(4, primary.touchFrames.size());
            }
        }
    }

    @Test
    void aTouchThatRemovesTheTtlIsStillResentAfterALostReply() throws Exception {
        // Only ttl 0 changed: REMOVE_TTL is not "expire now" (issue #893).
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 100);
                node.dropReplyAfterNextTouch();
                assertTrue(client.touch("k", NanocachedClient.REMOVE_TTL));
                assertEquals(2, node.touchFrames.size());
            }
        }
    }

    // A touch fans out to every owner like a set; get-and-touch asks the
    // primary alone for the value.

    @Test
    void touchGoesToEveryOwner() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                client.set(key, "v", 100);
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertTrue(client.touch(key, 30));

                for (String owner : owners) {
                    MockNode node = cluster.nodes().get(owner);
                    assertEquals(List.of("t 0 10 30"), node.touchFrames);
                    assertEquals(Long.valueOf(30), node.ttls.get(storedKey));
                }
            }
        }
    }

    @Test
    void getAndTouchSendsGToThePrimaryOnlyAndThePlainTouchToReplicas() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                client.set(key, "v", 100);
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertEquals(Optional.of("v"), client.getAndTouch(key, 30));

                assertEquals(List.of("t 0 10 30 G"), primary.touchFrames);
                assertEquals(List.of("t 0 10 30"), replica.touchFrames);
                assertEquals(Long.valueOf(30), replica.ttls.get(storedKey));
            }
        }
    }

    @Test
    void getAndTouchDoesNotFallBackToAReplicaOnAPrimaryMiss() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                // Only the replica holds the value.
                replica.store.put(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8)), new byte[] {'v'});

                assertEquals(Optional.empty(), client.getAndTouch(key, 30));

                assertEquals(List.of("t 0 10 30 G"), primary.touchFrames);
                assertEquals(List.of("t 0 10 30"), replica.touchFrames, "the replica is never asked for the value");
            }
        }
    }

    @Test
    void touchRecoversFromAWrongNodeAnswer() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                client.set(key, "v", 100);
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                primary.answerWrongNodeOnce();

                assertTrue(client.touch(key, 30));
                assertEquals(Optional.of("v"), client.getAndTouch(key, 31));
                assertTrue(primary.touchFrames.size() >= 2, "the touch was re-sent after the refresh");
            }
        }
    }

    @Test
    void aReplicaThatPredatesTouchDoesNotFailThePrimarysAnswer() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                client.set(key, "v", 100);
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                replica.rejectTouchLikeAnOldServer();
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertTrue(client.touch(key, 30));
                assertEquals(Optional.of("v"), client.getAndTouch(key, 40));
                assertEquals(Long.valueOf(40), primary.ttls.get(storedKey));
                assertTrue(client.stats().replicaWriteFailures() > 0);
            }
        }
    }

    @Test
    void aReplicaThatPredatesTouchIsAskedOnceNotRedialledAndResent() throws Exception {
        // Issue #924: an `En` is final, so the replica leg that gets one
        // gives up at once, as the primary leg does, instead of paying a
        // second dial for the same answer on every touch.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-key";
                client.set(key, "v", 100);
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode replica = cluster.nodes().get(owners.get(1));
                replica.rejectTouchLikeAnOldServer();

                assertTrue(client.touch(key, 30));
                assertEquals(1, replica.touchFrames.size(), "the replica was asked once");
                assertEquals(Optional.of("v"), client.getAndTouch(key, 40));
                assertEquals(2, replica.touchFrames.size(), "and once more for the second call");
            }
        }
    }

    // ── Batched touch / get-and-touch (issue #871) ──────────────────

    private static List<String> keysWhosePrimaryIs(String name, int count) {
        HashRing ring = new HashRing(NAMES);
        List<String> keys = new ArrayList<>();
        for (int i = 0; keys.size() < count; i++) {
            String key = "batch-" + i;
            if (ring.owners(key.getBytes(StandardCharsets.UTF_8), 2).get(0).equals(name)) keys.add(key);
        }
        return keys;
    }

    private static List<String> concat(List<String> first, List<String> second) {
        List<String> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }

    private static List<MockNode.MultiTouchFrame> framesWithGet(MockNode node, boolean get) {
        List<MockNode.MultiTouchFrame> frames = new ArrayList<>();
        for (MockNode.MultiTouchFrame frame : node.multiTouchFrames) {
            if (frame.get() == get) frames.add(frame);
        }
        return frames;
    }

    private static byte[][] utf8Keys(String... keys) {
        byte[][] raw = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) raw[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        return raw;
    }

    @Test
    void batchTouchFramesCarryTheTtlAndTheGetFlag() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                List<String> keys = List.of("a", "bb");
                client.touchMany(keys, 30);
                client.touchMany(keys, NanocachedClient.REMOVE_TTL);
                client.getManyAndTouch(keys, 45);
                client.getManyAndTouchBytes(keys, NanocachedClient.REMOVE_TTL);
                client.namespace("tenant").touchMany(keys, 7);
                assertEquals(
                        List.of(new MockNode.MultiTouchFrame("", keys, 30L, false),
                                new MockNode.MultiTouchFrame("", keys, null, false),
                                new MockNode.MultiTouchFrame("", keys, 45L, true),
                                new MockNode.MultiTouchFrame("", keys, null, true),
                                new MockNode.MultiTouchFrame("tenant", keys, 7L, false)),
                        node.multiTouchFrames);
            }
        }
    }

    @Test
    void touchManyReportsEachKeyAndNeverCreatesOne() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("a", "1", 100);
                client.set("b", "2", 100);

                Map<String, Boolean> touched = client.touchMany(List.of("a", "missing", "b"), 30);
                assertEquals(Map.of("a", true, "missing", false, "b", true), touched);
                assertEquals(Long.valueOf(30), node.ttls.get("a"));
                assertEquals(Long.valueOf(30), node.ttls.get("b"));
                assertFalse(node.store.containsKey("missing"));

                client.touchMany(List.of("a"), NanocachedClient.REMOVE_TTL);
                assertNull(node.ttls.get("a"));

                // 0 expires at once — the opposite of set(), where 0 means no expiry.
                assertEquals(Map.of("a", true, "b", true), client.touchMany(List.of("a", "b"), 0));
                assertEquals(Optional.empty(), client.get("a"));
                assertEquals(Optional.empty(), client.get("b"));
            }
        }
    }

    @Test
    void getManyAndTouchReturnsTheHitsAndSetsTheTtl() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("a", "one", 100);
                client.set("b", "two", 100);

                assertEquals(Map.of("a", "one", "b", "two"),
                        client.getManyAndTouch(List.of("a", "missing", "b"), 30));
                assertEquals(Long.valueOf(30), node.ttls.get("a"));
                assertFalse(node.store.containsKey("missing"));

                Map<String, byte[]> raw = client.getManyAndTouchBytes(List.of("a", "missing"), 40);
                assertEquals(Set.of("a"), raw.keySet());
                assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), raw.get("a"));
                assertEquals(Long.valueOf(40), node.ttls.get("a"));

                byte[][] positional = client.getManyAndTouchBytes(utf8Keys("b", "missing", "a"), 50);
                assertArrayEquals("two".getBytes(StandardCharsets.UTF_8), positional[0]);
                assertNull(positional[1]);
                assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), positional[2]);

                boolean[] touched = client.touchMany(utf8Keys("b", "missing", "a"), 60);
                assertArrayEquals(new boolean[] {true, false, true}, touched);
            }
        }
    }

    @Test
    void batchTouchCollapsesDuplicateKeys() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("a", "one", 100);
                client.set("b", "two", 100);

                assertEquals(Map.of("a", true, "b", true), client.touchMany(List.of("a", "b", "a"), 30));
                byte[][] values = client.getManyAndTouchBytes(utf8Keys("a", "b", "a", "missing", "missing"), 30);
                assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), values[0]);
                assertArrayEquals("two".getBytes(StandardCharsets.UTF_8), values[1]);
                assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), values[2]);
                assertNull(values[3]);
                assertNull(values[4]);
                assertArrayEquals(new boolean[] {true, true, true},
                        client.touchMany(utf8Keys("a", "b", "a"), 30));

                for (MockNode.MultiTouchFrame frame : node.multiTouchFrames) {
                    assertEquals(frame.get() ? 3 : 2, frame.keys().size(), "duplicates are sent once: " + frame);
                }
            }
        }
    }

    @Test
    void batchTouchIsScopedToItsNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace tenant = client.namespace("tenant");
                tenant.set("k", "in-tenant", 100);
                client.set("k", "in-default", 100);

                assertEquals(Map.of("k", true, "only-default", false), tenant.touchMany(List.of("k", "only-default"), 5));
                assertEquals(Map.of("k", "in-tenant"), tenant.getManyAndTouch(List.of("k", "only-default"), 6));
                assertArrayEquals("in-tenant".getBytes(StandardCharsets.UTF_8),
                        tenant.getManyAndTouchBytes(List.of("k"), 7).get("k"));
                assertArrayEquals(new boolean[] {true}, tenant.touchMany(utf8Keys("k"), 8));
                assertArrayEquals("in-tenant".getBytes(StandardCharsets.UTF_8),
                        tenant.getManyAndTouchBytes(utf8Keys("k"), 9)[0]);

                assertEquals(Long.valueOf(9), node.namespacedTtls.get("tenant").get("k"));
                assertEquals(Long.valueOf(100), node.ttls.get("k"), "the default keyspace was not touched");
            }
        }
    }

    @Test
    void getManyAndTouchDecompressesWhenCompressionIsEnabled() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", node.port()).compress(true))) {
                String value = "compressible ".repeat(200);
                client.set("k", value);
                assertEquals(Map.of("k", value), client.getManyAndTouch(List.of("k"), 30));
            }
        }
    }

    @Test
    void batchTouchRejectsBadArgumentsBeforeAnyIo() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                List<String> keys = List.of("k");
                assertThrows(IllegalArgumentException.class, () -> client.touchMany(keys, -2));
                assertThrows(IllegalArgumentException.class, () -> client.getManyAndTouch(keys, -2));
                assertThrows(IllegalArgumentException.class, () -> client.getManyAndTouchBytes(utf8Keys("k"), -2));
                assertThrows(IllegalArgumentException.class, () -> client.touchMany(List.of(), 30));
                assertThrows(IllegalArgumentException.class, () -> client.getManyAndTouch(List.of(), 30));
                assertThrows(IllegalArgumentException.class, () -> client.touchMany(new byte[0][], 30));
                assertTrue(node.multiTouchFrames.isEmpty());
            }
        }
    }

    @Test
    void batchTouchSplitsAtTheKeyLimit() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 401; i++) keys.add("k" + i);
                client.set("k0", "first");
                client.set("k400", "last");

                Map<String, Boolean> touched = client.touchMany(keys, 30);
                assertEquals(401, touched.size());
                assertTrue(touched.get("k0"));
                assertTrue(touched.get("k400"));
                assertFalse(touched.get("k1"));
                assertEquals(List.of(400, 1),
                        node.multiTouchFrames.stream().map(frame -> frame.keys().size()).toList());

                node.multiTouchFrames.clear();
                assertEquals(Map.of("k0", "first", "k400", "last"), client.getManyAndTouch(keys, 30));
                assertEquals(List.of(400, 1),
                        node.multiTouchFrames.stream().map(frame -> frame.keys().size()).toList());
            }
        }
    }

    @Test
    void batchTouchWorksOnATaggedConnection() throws Exception {
        try (MockNode node = MockNode.withTagSupport()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 100);
                assertEquals(Map.of("k", true, "missing", false), client.touchMany(List.of("k", "missing"), 30));
                assertEquals(Map.of("k", "v"), client.getManyAndTouch(List.of("k", "missing"), 40));
                assertEquals(Long.valueOf(40), node.ttls.get("k"));
                assertEquals(2, node.multiTouchFrames.size());
            }
        }
    }

    @Test
    void batchTouchAgainstAnOldServerIsServerTooOldAndTheClientStaysUsable() throws Exception {
        for (boolean predatesTouchToo : new boolean[] {false, true}) {
            try (MockNode node = new MockNode()) {
                try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                    client.set("k", "v");
                    if (predatesTouchToo) node.rejectTouchLikeAnOldServer();
                    else node.rejectMultiTouchLikeAnOldServer();

                    NanocachedException.ServerTooOld failure = assertThrows(
                            NanocachedException.ServerTooOld.class, () -> client.touchMany(List.of("k"), 30));
                    assertEquals("y", failure.command());
                    assertEquals("y", assertThrows(NanocachedException.ServerTooOld.class,
                            () -> client.getManyAndTouch(List.of("k"), 30)).command());
                    assertEquals(Optional.of("v"), client.get("k"));
                    // The single-key touch keeps the `t` wording it always had.
                    if (!predatesTouchToo) {
                        assertTrue(client.touch("k", 30));
                    } else {
                        assertEquals("t", assertThrows(NanocachedException.ServerTooOld.class,
                                () -> client.touch("k", 30)).command());
                    }
                }
            }
        }
    }

    @Test
    void batchTouchKeepsEarlierChunksWhenALaterChunkFails() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[][] keys = new byte[401][];
                for (int i = 0; i < keys.length; i++) keys[i] = ("k" + i).getBytes(StandardCharsets.UTF_8);
                byte[] first = "first-chunk-value".getBytes(StandardCharsets.UTF_8);
                client.set(keys[0], first);

                node.failMultiTouchAfter(1);
                NanocachedException.PartialConnectionFailedRaw failure = assertThrows(
                        NanocachedException.PartialConnectionFailedRaw.class,
                        () -> client.getManyAndTouchBytes(keys, 30));
                assertEquals(keys.length, failure.partialValues.length);
                assertArrayEquals(first, failure.partialValues[0]);
                assertArrayEquals(new int[] {400}, failure.unresolvedIndices);

                // touchMany carries no partial result: the whole batch is idempotent, send it again.
                NanocachedException.ConnectionFailed bare = assertThrows(
                        NanocachedException.ConnectionFailed.class, () -> client.touchMany(keys, 30));
                assertFalse(bare instanceof NanocachedException.PartialConnectionFailedRaw);
            }
        }
    }

    @Test
    void batchTouchEndsOnAWholeFrameWrongNodeOnASingleNode() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v", 100);
                node.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> client.touchMany(List.of("k"), 30));

                node.answerWrongNodeOnce();
                NanocachedException.PartialWrongNodeStrings failure = assertThrows(
                        NanocachedException.PartialWrongNodeStrings.class,
                        () -> client.getManyAndTouch(List.of("k"), 30));
                assertTrue(failure.partialValues.isEmpty());
                assertEquals(Long.valueOf(100), node.ttls.get("k"), "a wrong-node answer touches nothing");
            }
        }
    }

    @Test
    void aBatchTouchReplyWithTheWrongEntryCountIsNeverTrusted() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("a", "1", 100);
                client.set("b", "2", 100);
                client.set("c", "3", 100);
                List<String> keys = List.of("a", "b", "c");

                // The connection is dropped as desynced and the idempotent
                // request is sent again on a fresh one, so the caller sees
                // the right answer rather than a shifted one.
                node.answerMultiTouchWithEntryCountOnce(1);
                assertEquals(Map.of("a", true, "b", true, "c", true), client.touchMany(keys, 30));
                assertEquals(2, node.multiTouchFrames.size());

                node.multiTouchFrames.clear();
                node.answerMultiTouchWithEntryCountOnce(2);
                assertEquals(Map.of("a", "1", "b", "2", "c", "3"), client.getManyAndTouch(keys, 31));
                assertEquals(2, node.multiTouchFrames.size());
            }
        }
    }

    @Test
    void batchTouchGoesToEveryOwnerAndGoesToThePrimaryInTheGetForm() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                MockNode nodeB = cluster.nodes().get(NAMES.get(1));
                List<String> primaryA = keysWhosePrimaryIs(NAMES.get(0), 3);
                List<String> primaryB = keysWhosePrimaryIs(NAMES.get(1), 3);
                List<String> all = concat(primaryA, primaryB);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);

                assertEquals(Map.of(primaryA.get(0), true, primaryB.get(0), true),
                        client.touchMany(List.of(primaryA.get(0), primaryB.get(0)), 30));
                for (MockNode node : List.of(nodeA, nodeB)) {
                    assertEquals(1, node.multiTouchFrames.size(), "one plain frame per node");
                    assertFalse(node.multiTouchFrames.get(0).get());
                    assertEquals(2, node.multiTouchFrames.get(0).keys().size());
                }
                nodeA.multiTouchFrames.clear();
                nodeB.multiTouchFrames.clear();

                assertEquals(values, client.getManyAndTouch(all, 40));
                // A node is the primary of some keys and a replica of the
                // others, so it gets the G frame for the first and a plain
                // one for the second.
                assertEquals(2, nodeA.multiTouchFrames.size());
                assertEquals(2, nodeB.multiTouchFrames.size());
                assertEquals(primaryA.stream().map(k -> MockNode.keyOf(k.getBytes(StandardCharsets.UTF_8))).toList(),
                        framesWithGet(nodeA, true).get(0).keys());
                assertEquals(primaryB.stream().map(k -> MockNode.keyOf(k.getBytes(StandardCharsets.UTF_8))).toList(),
                        framesWithGet(nodeA, false).get(0).keys());
                assertEquals(primaryB, framesWithGet(nodeB, true).get(0).keys());
                assertEquals(primaryA, framesWithGet(nodeB, false).get(0).keys());
                for (String key : all) {
                    assertEquals(Long.valueOf(40), nodeA.ttls.get(key));
                    assertEquals(Long.valueOf(40), nodeB.ttls.get(key));
                }
            }
        }
    }

    @Test
    void getManyAndTouchDoesNotFallBackToAReplicaOnAPrimaryMiss() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode primary = cluster.nodes().get(NAMES.get(0));
                MockNode replica = cluster.nodes().get(NAMES.get(1));
                String key = keysWhosePrimaryIs(NAMES.get(0), 1).get(0);
                // Only the replica holds the value.
                replica.store.put(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8)), new byte[] {'v'});

                assertEquals(Map.of(), client.getManyAndTouch(List.of(key), 30));

                assertEquals(1, framesWithGet(primary, true).size());
                assertEquals(0, framesWithGet(replica, true).size(), "the replica is never asked for the value");
                assertEquals(1, framesWithGet(replica, false).size());
            }
        }
    }

    @Test
    void batchTouchRecoversFromAWrongNodeAnswer() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                List<String> all = concat(keysWhosePrimaryIs(NAMES.get(0), 3), keysWhosePrimaryIs(NAMES.get(1), 3));
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);

                nodeA.answerWrongNodeOnce();
                Map<String, Boolean> touched = client.touchMany(all, 30);
                assertEquals(6, touched.size());
                assertTrue(touched.values().stream().allMatch(t -> t));
                assertEquals(2, nodeA.multiTouchFrames.size(), "the keys A answered W for were sent again");

                nodeA.answerWrongNodeOnce();
                assertEquals(values, client.getManyAndTouch(all, 31));
            }
        }
    }

    @Test
    void batchTouchStillWrongNodeAfterTheRetryThrowsWhatDidResolve() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                List<String> primaryA = keysWhosePrimaryIs(NAMES.get(0), 3);
                List<String> primaryB = keysWhosePrimaryIs(NAMES.get(1), 3);
                List<String> all = concat(primaryA, primaryB);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);

                for (int i = 0; i < 8; i++) nodeA.answerWrongNodeOnce();
                NanocachedException.WrongNode failure =
                        assertThrows(NanocachedException.WrongNode.class, () -> client.touchMany(all, 30));
                assertFalse(failure instanceof NanocachedException.PartialWrongNodeStrings);

                NanocachedException.PartialWrongNodeStrings partial = assertThrows(
                        NanocachedException.PartialWrongNodeStrings.class, () -> client.getManyAndTouch(all, 31));
                Map<String, String> resolved = new java.util.LinkedHashMap<>();
                for (String key : primaryB) resolved.put(key, values.get(key));
                assertEquals(resolved, partial.partialValues);
            }
        }
    }

    @Test
    void aBatchTouchWhoseReplyWasLostIsSentAgain() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                List<String> keys = keysWhosePrimaryIs(NAMES.get(0), 3);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                client.setMany(values);

                nodeA.dropReplyAfterNextTouch();
                Map<String, Boolean> touched = client.touchMany(keys, 30);
                assertTrue(touched.values().stream().allMatch(t -> t));
                assertEquals(2, nodeA.multiTouchFrames.size());
            }
        }
    }

    @Test
    void anExpiringBatchTouchWhoseReplyWasLostIsNotResent() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                List<String> keysA = keysWhosePrimaryIs(NAMES.get(0), 3);
                List<String> keysB = keysWhosePrimaryIs(NAMES.get(1), 3);
                List<String> all = concat(keysA, keysB);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);

                nodeA.dropReplyAfterNextTouch();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.touchMany(all, 0));
                assertEquals(1, nodeA.multiTouchFrames.size(), "the applied frame was not sent again");
            }
        }
    }

    @Test
    void anExpiringGetManyAndTouchWhoseReplyWasLostIsNotResent() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                List<String> keysA = keysWhosePrimaryIs(NAMES.get(0), 3);
                List<String> keysB = keysWhosePrimaryIs(NAMES.get(1), 3);
                List<String> all = concat(keysA, keysB);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);

                nodeA.dropReplyAfterNextTouch();
                NanocachedException.PartialConnectionFailedStrings partial = assertThrows(
                        NanocachedException.PartialConnectionFailedStrings.class,
                        () -> client.getManyAndTouch(all, 0));
                Map<String, String> resolved = new java.util.LinkedHashMap<>();
                for (String key : keysB) resolved.put(key, values.get(key));
                assertEquals(resolved, partial.partialValues, "the other primary's hits are kept");
                assertEquals(1, nodeA.multiTouchFrames.size(), "the applied frame was not sent again");
            }
        }
    }

    @Test
    void anExpiringBatchTouchStillRetriesKeysThatWereNeverSent() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                List<String> all = concat(keysWhosePrimaryIs(NAMES.get(0), 3), keysWhosePrimaryIs(NAMES.get(1), 3));
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);

                nodeA.answerWrongNodeOnce();
                Map<String, Boolean> touched = client.touchMany(all, 0);
                assertEquals(6, touched.size());
                assertTrue(touched.values().stream().allMatch(t -> t));
                assertEquals(2, nodeA.multiTouchFrames.size(), "the keys A answered W for were sent again");

                client.setMany(values);
                nodeA.answerWrongNodeOnce();
                assertEquals(values, client.getManyAndTouch(all, 0));
            }
        }
    }

    @Test
    void aReplicaThatPredatesBatchTouchIsCountedPerKeyAndDoesNotFailThePrimarysAnswer() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode primary = cluster.nodes().get(NAMES.get(0));
                MockNode replica = cluster.nodes().get(NAMES.get(1));
                List<String> keys = keysWhosePrimaryIs(NAMES.get(0), 3);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                client.setMany(values);
                replica.rejectMultiTouchLikeAnOldServer();
                long before = client.stats().replicaWriteFailures();

                assertTrue(client.touchMany(keys, 30).values().stream().allMatch(t -> t));
                assertEquals(before + 3, client.stats().replicaWriteFailures());
                assertEquals(values, client.getManyAndTouch(keys, 40));
                assertEquals(before + 6, client.stats().replicaWriteFailures());
                for (String key : keys) {
                    assertEquals(Long.valueOf(40), primary.ttls.get(key));
                }
            }
        }
    }

    @Test
    void aPrimaryThatPredatesBatchTouchIsServerTooOldNotAConnectionFailure() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                List<String> keys = keysWhosePrimaryIs(NAMES.get(0), 2);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                client.setMany(values);
                cluster.nodes().get(NAMES.get(0)).rejectMultiTouchLikeAnOldServer();

                assertEquals("y", assertThrows(NanocachedException.ServerTooOld.class,
                        () -> client.touchMany(keys, 30)).command());
                assertEquals("y", assertThrows(NanocachedException.ServerTooOld.class,
                        () -> client.getManyAndTouch(keys, 30)).command());
            }
        }
    }

    @Test
    void aPrimaryThatPredatesBatchTouchHandsBackTheHitsOtherPrimariesReturned() throws Exception {
        // getManyAndTouch(ttl=0) consumes the values the other primaries
        // return, so an `En` from one primary must not throw them away.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                List<String> oldKeys = keysWhosePrimaryIs(NAMES.get(0), 2);
                List<String> newKeys = keysWhosePrimaryIs(NAMES.get(1), 2);
                List<String> all = new java.util.ArrayList<>(oldKeys);
                all.addAll(newKeys);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                Map<String, String> expected = new java.util.LinkedHashMap<>();
                for (String key : newKeys) expected.put(key, "value of " + key);
                client.setMany(values);
                cluster.nodes().get(NAMES.get(0)).rejectMultiTouchLikeAnOldServer();

                NanocachedException.PartialServerTooOldStrings strings = assertThrows(
                        NanocachedException.PartialServerTooOldStrings.class, () -> client.getManyAndTouch(all, 0));
                assertEquals("y", strings.command());
                assertEquals(expected, strings.partialValues);
                // The ttl=0 call above consumed them: this is the loss.
                assertTrue(client.getMany(newKeys).isEmpty());
                client.setMany(values);

                NanocachedException.PartialServerTooOld bytes = assertThrows(
                        NanocachedException.PartialServerTooOld.class, () -> client.getManyAndTouchBytes(all, 30));
                assertEquals(expected.keySet(), bytes.partialValues.keySet());
                for (String key : newKeys) {
                    assertArrayEquals(expected.get(key).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                            bytes.partialValues.get(key));
                }

                // Positional, with a repeated key: every caller position is mapped.
                byte[][] positional = new byte[all.size() + 1][];
                for (int i = 0; i < all.size(); i++) positional[i] = all.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                positional[all.size()] = positional[0];
                NanocachedException.PartialServerTooOldRaw raw = assertThrows(
                        NanocachedException.PartialServerTooOldRaw.class,
                        () -> client.getManyAndTouchBytes(positional, 30));
                assertArrayEquals(new int[] {0, 1, all.size()}, raw.unresolvedIndices);
                assertNull(raw.partialValues[0]);
                assertTrue(raw.partialValues[2] != null);

                // A plain touch has nothing partial worth attaching, and a call
                // where nothing resolved elsewhere throws the bare error.
                assertEquals(NanocachedException.ServerTooOld.class,
                        assertThrows(NanocachedException.ServerTooOld.class, () -> client.touchMany(all, 30)).getClass());
                assertEquals(NanocachedException.ServerTooOld.class,
                        assertThrows(NanocachedException.ServerTooOld.class, () -> client.getManyAndTouch(oldKeys, 30)).getClass());
            }
        }
    }

    @Test
    void aPrimaryThatAnswersEnToBatchTouchIsNotRedialledOrAskedAgain() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                List<String> keys = keysWhosePrimaryIs(NAMES.get(0), 3);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                client.setMany(values);
                MockNode old = cluster.nodes().get(NAMES.get(0));
                old.rejectMultiTouchLikeAnOldServer();

                // `En` is final: the same node would answer it again, so each
                // call sends one frame, with no resend and no second pass.
                assertEquals("y", assertThrows(NanocachedException.ServerTooOld.class,
                        () -> client.touchMany(keys, 30)).command());
                assertEquals(1, old.multiTouchFrames.size());
                assertEquals("y", assertThrows(NanocachedException.ServerTooOld.class,
                        () -> client.getManyAndTouch(keys, 30)).command());
                assertEquals(2, old.multiTouchFrames.size());
            }
        }
    }

    @Test
    void aPrimaryThatAnswersEnToTouchIsNotRedialledOrAskedAgain() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = keysWhosePrimaryIs(NAMES.get(0), 1).get(0);
                client.set(key, "v", 100);
                MockNode old = cluster.nodes().get(NAMES.get(0));
                old.rejectTouchLikeAnOldServer();

                // `En` is final (issue #902): one frame per call, no resend
                // after a redial and none after a refresh.
                assertThrows(NanocachedException.ServerTooOld.class, () -> client.touch(key, 30));
                assertEquals(1, old.touchFrames.size());
                assertThrows(NanocachedException.ServerTooOld.class, () -> client.getAndTouch(key, 30));
                assertEquals(2, old.touchFrames.size());
                assertThrows(NanocachedException.ServerTooOld.class, () -> client.touch(key, 0));
                assertEquals(3, old.touchFrames.size());
            }
        }
    }

    @Test
    void aReplicaThatAnswersEnToBatchTouchDoesNotFailTheCall() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                List<String> keys = keysWhosePrimaryIs(NAMES.get(0), 3);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                client.setMany(values);
                MockNode old = cluster.nodes().get(NAMES.get(1));
                old.rejectMultiTouchLikeAnOldServer();
                long before = client.stats().replicaWriteFailures();

                assertTrue(client.touchMany(keys, 30).values().stream().allMatch(t -> t));
                assertEquals(1, old.multiTouchFrames.size());
                assertEquals(before + keys.size(), client.stats().replicaWriteFailures());
            }
        }
    }

    @Test
    void batchTouchReportsAnUnreachablePrimaryAsAConnectionFailureNotWrongNode() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String dead = NAMES.get(0);
                List<String> deadKeys = keysWhosePrimaryIs(dead, 3);
                List<String> liveKeys = keysWhosePrimaryIs(NAMES.get(1), 3);
                List<String> all = concat(deadKeys, liveKeys);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : all) values.put(key, "value of " + key);
                client.setMany(values);
                cluster.nodes().get(dead).close();
                Thread.sleep(50); // let the FIN land

                NanocachedException touchFailure = assertThrows(
                        NanocachedException.ConnectionFailed.class, () -> client.touchMany(all, 30));
                assertFalse(touchFailure instanceof NanocachedException.WrongNode);

                NanocachedException.PartialConnectionFailedStrings failure = assertThrows(
                        NanocachedException.PartialConnectionFailedStrings.class,
                        () -> client.getManyAndTouch(all, 30));
                Map<String, String> live = new java.util.LinkedHashMap<>();
                for (String key : liveKeys) live.put(key, values.get(key));
                assertEquals(live, failure.partialValues);
                assertTrue(failure.getCause() instanceof NanocachedException.ConnectionFailed,
                        "cause = " + failure.getCause());
            }
        }
    }

    @Test
    void batchTouchGoesToTheOwnersOfANamespacedKey() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                NanocachedClient.Namespace tenant = client.namespace("tenant");
                List<String> keys = List.of("a", "b", "c", "d", "e", "f");
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                tenant.setMany(values);

                assertEquals(values, tenant.getManyAndTouch(keys, 30));
                assertTrue(tenant.touchMany(keys, 31).values().stream().allMatch(t -> t));
                for (MockNode node : cluster.nodes().values()) {
                    for (MockNode.MultiTouchFrame frame : node.multiTouchFrames) {
                        assertEquals("tenant", frame.namespace());
                    }
                    for (String key : keys) {
                        assertEquals(Long.valueOf(31), node.namespacedTtls.get("tenant").get(key));
                    }
                }
            }
        }
    }

    @Test
    void batchTouchReachesAFireAndForgetReplicaInTheBackground() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connectFireAndForget(cluster.discovery().port())) {
                MockNode replica = cluster.nodes().get(NAMES.get(1));
                List<String> keys = keysWhosePrimaryIs(NAMES.get(0), 3);
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (String key : keys) values.put(key, "value of " + key);
                client.setMany(values);
                // The replica's copy of the set is a background write too: a touch that reaches the
                // replica first finds nothing to touch, and the set then lands without a TTL.
                waitFor(() -> keys.stream().allMatch(
                                key -> replica.store.containsKey(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8)))),
                        "the background set to land on the replica");

                assertTrue(client.touchMany(keys, 30).values().stream().allMatch(t -> t));
                waitFor(() -> !replica.multiTouchFrames.isEmpty(), "the background touch to reach the replica");
                for (String key : keys) {
                    waitFor(() -> Long.valueOf(30).equals(replica.ttls.get(key)), "the replica's TTL");
                }
            }
        }
    }

    // ── Compare-and-set (issue #141) ───────────────────────────────

    // The pinned cross-language digest vector (docs/protocol.html#cas):
    // SHA-256 of the UTF-8 bytes "nanocached-cas-vector", truncated to its
    // first 16 bytes, lowercase hex. Independently pinned into the Rust
    // server and every other SDK too — a mismatch here means CAS silently
    // breaks across languages.
    @Test
    void contentDigestMatchesThePinnedCrossLanguageVector() {
        assertEquals("36287141940ca57acbd7695ccdde9d43",
                NanocachedClient.contentDigest("nanocached-cas-vector".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void putIfAbsentStoresOnlyWhenTheKeyIsAbsent() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertTrue(client.putIfAbsent("k", "first"));
                assertEquals(Optional.of("first"), client.get("k"));

                // The key now exists: a second putIfAbsent is a no-op.
                assertFalse(client.putIfAbsent("k", "second"));
                assertEquals(Optional.of("first"), client.get("k"));

                assertTrue(client.delete("k"));
                // Lazily expired/absent again: putIfAbsent succeeds.
                assertTrue(client.putIfAbsent("k", "third"));
                assertEquals(Optional.of("third"), client.get("k"));
            }
        }
    }

    @Test
    void replaceIfPresentStoresOnlyWhenTheKeyExists() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertFalse(client.replaceIfPresent("k", "v"));
                assertEquals(Optional.empty(), client.get("k"));

                client.set("k", "original");
                assertTrue(client.replaceIfPresent("k", "replaced"));
                assertEquals(Optional.of("replaced"), client.get("k"));
            }
        }
    }

    @Test
    void replaceSucceedsOnlyWhenTheTokenMatchesTheCurrentContent() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v1");
                NanocachedClient.CasEntry entry = client.getWithToken("k").orElseThrow();
                assertEquals("v1", new String(entry.value(), StandardCharsets.UTF_8));
                assertEquals(NanocachedClient.contentDigest("v1".getBytes(StandardCharsets.UTF_8)), entry.token());

                // A stale token (from before someone else's concurrent
                // write) is rejected.
                client.set("k", "v2");
                assertFalse(client.replace("k", entry.token(), "v3"));
                assertEquals(Optional.of("v2"), client.get("k"));

                // The current token succeeds.
                String currentToken = client.getWithToken("k").orElseThrow().token();
                assertTrue(client.replace("k", currentToken, "v3"));
                assertEquals(Optional.of("v3"), client.get("k"));

                // A missing key never matches any digest.
                assertTrue(client.delete("k"));
                assertFalse(client.replace("k", currentToken, "v4"));
            }
        }
    }

    @Test
    void replaceRejectsAMalformedToken() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                assertThrows(IllegalArgumentException.class, () -> client.replace("k", "not-a-digest", "v2"));
                assertThrows(IllegalArgumentException.class, () -> client.replace("k", "A", "v2"));
                assertThrows(IllegalArgumentException.class, () -> client.deleteIfMatches("k", "P"));
                assertThrows(IllegalArgumentException.class, () -> client.deleteIfMatches("k", null));
                // Rejected client-side: the value must be untouched.
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void deleteIfMatchesRemovesOnlyOnAMatchingToken() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                String staleToken = NanocachedClient.contentDigest("wrong".getBytes(StandardCharsets.UTF_8));
                assertFalse(client.deleteIfMatches("k", staleToken));
                assertEquals(Optional.of("v"), client.get("k"));

                String token = client.getWithToken("k").orElseThrow().token();
                assertTrue(client.deleteIfMatches("k", token));
                assertEquals(Optional.empty(), client.get("k"));

                // Already gone: the same token no longer matches anything.
                assertFalse(client.deleteIfMatches("k", token));
            }
        }
    }

    @Test
    void getWithTokenReturnsEmptyOnAMissingKey() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertEquals(Optional.empty(), client.getWithToken("missing"));
            }
        }
    }

    @Test
    void casTtlZeroMeansNoExpiryAndNegativeIsRejected() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertTrue(client.putIfAbsent("k", "v", 60));
                assertEquals(Long.valueOf(60), node.ttls.get("k"));

                assertThrows(IllegalArgumentException.class, () -> client.putIfAbsent("k2", "v", -1L));
                assertThrows(IllegalArgumentException.class, () -> client.replaceIfPresent("k", "v2", -1L));
                String token = client.getWithToken("k").orElseThrow().token();
                assertThrows(IllegalArgumentException.class, () -> client.replace("k", token, "v3", -1L));
            }
        }
    }

    @Test
    void casOperationsAreScopedByNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace tenant = client.namespace("tenant-a");
                assertTrue(tenant.putIfAbsent("k", "tenant-value"));
                // The default (un-namespaced) keyspace is untouched.
                assertEquals(Optional.empty(), client.get("k"));
                assertTrue(client.putIfAbsent("k", "default-value"));

                String tenantToken = tenant.getWithToken("k").orElseThrow().token();
                assertTrue(tenant.replace("k", tenantToken, "tenant-value-2"));
                assertEquals(Optional.of("default-value"), client.get("k"));
                assertEquals(Optional.of("tenant-value-2"), tenant.get("k"));

                assertTrue(tenant.deleteIfMatches("k",
                        tenant.getWithToken("k").orElseThrow().token()));
                assertEquals(Optional.empty(), tenant.get("k"));
                assertEquals(Optional.of("default-value"), client.get("k"));
            }
        }
    }

    // issue #225: same coverage as incrIsRetriedAfterRedialWhenTheConnectionWasAlreadyDead/
    // incrAppliedButAcknowledgementLostIsNeverReplayed above, for `k`
    // (CAS store) — replace/putIfAbsent/replaceIfPresent all share the
    // same casPrimaryThenReplicate driver.

    @Test
    void replaceIsRetriedAfterRedialWhenTheConnectionWasAlreadyDead() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v1");
                String token = client.getWithToken("k").orElseThrow().token();
                node.dropConnections();
                Thread.sleep(50); // let the FIN land
                assertTrue(client.replace("k", token, "v2"));
                assertEquals(Optional.of("v2"), client.get("k"));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void replaceAppliedButAcknowledgementLostIsNeverReplayed() throws Exception {
        // The primary applies the store but the connection dies before
        // its `S` reply arrives. A blind retry would resend `k` with the
        // *old* token, which the just-written value no longer matches —
        // reporting an already-succeeded replace as a mismatch (`false`).
        // The client must surface ConnectionFailed instead, and the
        // value must have been written exactly once.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v1");
                String token = client.getWithToken("k").orElseThrow().token();
                node.dropReplyAfterNextCasSet();
                assertThrows(NanocachedException.ConnectionFailed.class,
                        () -> client.replace("k", token, "v2"));
                assertEquals(Optional.of("v2"), client.get("k"));
            }
        }
    }

    @Test
    void getWithTokenComputesTheDigestFromRawWireBytesUnderCompression() throws Exception {
        // Correctness note (issue #141): with compress enabled, the value
        // on the wire carries a marker byte and the server never
        // decompresses — so the digest must be computed over those raw,
        // marker-prefixed bytes, never over the decompressed value this
        // same call also returns, or a subsequent k/x conditioned on this
        // token would never match what the server actually stores.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(single("127.0.0.1", node.port())
                    .compress(true)
                    .compressionThreshold(4))) {
                String value = "x".repeat(1000);
                client.set("k", value);

                byte[] rawWireBytes = node.store.get("k");
                assertEquals(0x01, rawWireBytes[0]); // actually compressed

                NanocachedClient.CasEntry entry = client.getWithToken("k").orElseThrow();
                assertEquals(value, new String(entry.value(), StandardCharsets.UTF_8));
                assertEquals(NanocachedClient.contentDigest(rawWireBytes), entry.token());
                assertNotEquals(NanocachedClient.contentDigest(value.getBytes(StandardCharsets.UTF_8)), entry.token());

                // The token from getWithToken must actually work against
                // the real server-side condition check.
                String replacement = "y".repeat(1000);
                assertTrue(client.replace("k".getBytes(StandardCharsets.UTF_8), entry.token(),
                        replacement.getBytes(StandardCharsets.UTF_8)));
                assertEquals(Optional.of(replacement), client.get("k"));
                // The replacement value must itself go through the same
                // compression pipeline set() uses, so a later get (from
                // this or any other compress-enabled client) still
                // decompresses correctly rather than reading raw
                // uncompressed bytes as if they were marker-prefixed.
                byte[] newRaw = node.store.get("k");
                assertEquals(0x01, newRaw[0]);
                assertTrue(newRaw.length < replacement.length());
            }
        }
    }

    // Audit finding J2: an empty key, or a key+value pair large enough to
    // risk the server's MAX_REQUEST_SIZE (src/server.rs, 1 MiB), must be
    // rejected synchronously — before any bytes reach the connection —
    // exactly like the ttlSeconds check above.
    @Test
    void rejectsEmptyKeysOnGetSetDelete() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                byte[] emptyKey = new byte[0];
                assertThrows(IllegalArgumentException.class, () -> client.get(emptyKey));
                assertThrows(IllegalArgumentException.class, () -> client.getBytes(emptyKey));
                assertThrows(IllegalArgumentException.class,
                        () -> client.set(emptyKey, "v".getBytes(StandardCharsets.UTF_8)));
                assertThrows(IllegalArgumentException.class, () -> client.delete(emptyKey));
                assertThrows(IllegalArgumentException.class, () -> client.get(""));
                assertThrows(IllegalArgumentException.class, () -> client.set("", "v"));
                assertThrows(IllegalArgumentException.class, () -> client.delete(""));

                // Rejected client-side, before any request frame — no
                // second connection beyond the initial connect() above.
                assertEquals(1, node.connectionCount.get());
            }
        }
    }

    @Test
    void rejectsOversizeKeyOrKeyPlusValueBeforeTouchingTheConnection() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                // Force the connection open first so connectionCount only
                // reflects the (successful) warm-up, not the rejections
                // below.
                client.set("warm", "up");
                int connectionsBeforeRejections = node.connectionCount.get();

                byte[] oversizeKey = new byte[1024 * 1024];
                assertThrows(IllegalArgumentException.class, () -> client.get(oversizeKey));
                assertThrows(IllegalArgumentException.class, () -> client.delete(oversizeKey));
                assertThrows(IllegalArgumentException.class,
                        () -> client.set(oversizeKey, "v".getBytes(StandardCharsets.UTF_8)));

                // A modest key with a value large enough to push key+value
                // over the limit must be rejected too.
                byte[] smallKey = "k".getBytes(StandardCharsets.UTF_8);
                byte[] oversizeValue = new byte[1024 * 1024];
                assertThrows(IllegalArgumentException.class, () -> client.set(smallKey, oversizeValue));

                // None of the above should have opened another connection.
                assertEquals(connectionsBeforeRejections, node.connectionCount.get());
                // And the connection already open must still be usable.
                assertEquals(Optional.of("up"), client.get("warm"));
            }
        }
    }

    // Audit finding J3: a null authSecret must fail the same way every
    // other invalid Options value does (IllegalArgumentException), not
    // with a raw NullPointerException from inside String#getBytes.
    @Test
    void authSecretRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> NanocachedClient.builder().authSecret(null));
    }

    @Test
    void reconnectCooldownStillRejectsNullOrNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.builder().reconnectCooldown(null));
        assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.builder().reconnectCooldown(Duration.ofMillis(-1)));
    }

    @Test
    void pipelinesConcurrentRequestsOnOneConnection() throws Exception {
        // Same shape as the TypeScript SDK's own pipelining test: N
        // concurrent requests on a single connection, each independently
        // verified to round-trip its own value (request pipelining) — a
        // bug in matching responses to the right caller in send order
        // would show up as swapped or wrong values here.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int n = 20;
                ExecutorService pool = Executors.newFixedThreadPool(n);
                try {
                    List<Future<?>> sets = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        int index = i;
                        sets.add(pool.submit(() -> client.set("key-" + index, "value-" + index)));
                    }
                    for (Future<?> future : sets) future.get();

                    List<Future<Optional<String>>> gets = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        int index = i;
                        gets.add(pool.submit(() -> client.get("key-" + index)));
                    }
                    for (int i = 0; i < n; i++) {
                        assertEquals(Optional.of("value-" + i), gets.get(i).get());
                    }
                } finally {
                    pool.shutdown();
                }
            }
        }
    }

    @Test
    void authenticates() throws Exception {
        try (MockNode node = new MockNode("s3cret".getBytes(StandardCharsets.UTF_8))) {
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", node.port()).authSecret("s3cret"))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }

            // Both shapes are matchable as AuthenticationFailed (issue
            // #47 item 5), not just by message.
            NanocachedException missing = assertThrows(NanocachedException.AuthenticationFailed.class,
                    () -> connect("127.0.0.1", node.port()));
            assertTrue(missing.getMessage().contains("requires authentication"));

            NanocachedException wrong = assertThrows(NanocachedException.AuthenticationFailed.class,
                    () -> NanocachedClient.connect(
                            single("127.0.0.1", node.port()).authSecret("wrong")));
            assertTrue(wrong.getMessage().contains("authentication failed"));
        }
    }

    @Test
    void emptyAuthSecretIsTreatedAsNoSecret() throws Exception {
        // An empty authSecret is the same as none, matching the other
        // SDKs (issue: audit finding): sent literally, an empty string
        // would reach the wire as an explicit zero-length secret, which
        // the server rejects (EmptySecret) and closes without replying —
        // turning what should be "no auth configured" into an opaque
        // ConnectionFailed instead of a normal connect against a
        // no-auth-required server.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", node.port()).authSecret(""))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void wrongNodePropagatesInSingleMode() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                node.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> client.get("k"));
            }
        }
    }

    @Test
    void rejectsUseAfterClose() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedClient client = connect("127.0.0.1", node.port());
            client.close();
            client.close(); // idempotent
            assertTrue(client.isClosed());
            assertThrows(NanocachedException.AlreadyClosed.class, () -> client.get("k"));
        }
    }

    @Test
    void warnsOnceOnDoubleClose() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedClient client = connect("127.0.0.1", node.port());
            client.close();

            String output = captureStderr(client::close);
            assertTrue(output.contains("nanocached: close() called again on an already-closed client"),
                    "unexpected stderr: " + output);
        }
    }

    @Test
    void warnsOnAForgottenClose() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedClient first = connect("127.0.0.1", node.port());
            try {
                NanocachedClient[] second = new NanocachedClient[1];
                String output = captureStderr(() -> second[0] = connect("127.0.0.1", node.port()));
                try {
                    assertTrue(output.contains("was close() forgotten?"), "unexpected stderr: " + output);
                } finally {
                    second[0].close();
                }
            } finally {
                first.close();
            }
        }
    }

    // ── 値の圧縮 (value compression) ────────────────────────────────

    @Test
    void wireFormatIsUntouchedWhenCompressIsOff() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                String value = "x".repeat(1000);
                client.set("k", value);
                assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), node.store.get("k"));
                assertEquals(Optional.of(value), client.get("k"));
            }
        }
    }

    @Test
    void compressesAtOrAboveTheThresholdAndDecompressesBack() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(single("127.0.0.1", node.port())
                    .compress(true)
                    .compressionThreshold(64))) {
                String value = "x".repeat(1000);
                client.set("k", value);

                byte[] stored = node.store.get("k");
                assertEquals(0x01, stored[0]);
                assertTrue(stored.length < value.length());

                assertEquals(Optional.of(value), client.get("k"));
                assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), client.getBytes("k").orElseThrow());
            }
        }
    }

    @Test
    void rejectsANegativeCompressionThresholdAtConnect() throws Exception {
        // A negative threshold would otherwise silently force every
        // set() to attempt compression regardless of value size, since
        // no value's length is ever less than a negative number (issue:
        // audit finding; mirrors the Go SDK's identical Connect-time
        // check). Rejected at connect() time, before any socket is
        // touched, exactly like the empty-addresses-list check.
        try (MockNode node = new MockNode()) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> NanocachedClient.connect(single("127.0.0.1", node.port())
                            .compress(true)
                            .compressionThreshold(-1)));
            assertTrue(error.getMessage().contains("compressionThreshold"), error.getMessage());
        }
    }

    @Test
    void belowThresholdValueIsPrefixedButNotCompressed() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(single("127.0.0.1", node.port())
                    .compress(true)
                    .compressionThreshold(256))) {
                client.set("k", "short");
                byte[] expected = new byte[6];
                expected[0] = 0x00;
                System.arraycopy("short".getBytes(StandardCharsets.UTF_8), 0, expected, 1, 5);
                assertArrayEquals(expected, node.store.get("k"));
                assertEquals(Optional.of("short"), client.get("k"));
            }
        }
    }

    @Test
    void incompressibleDataPassesThroughUnbloated() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = NanocachedClient.connect(single("127.0.0.1", node.port())
                    .compress(true)
                    .compressionThreshold(16))) {
                byte[] value = new byte[512];
                new java.security.SecureRandom().nextBytes(value);
                client.set("k".getBytes(StandardCharsets.UTF_8), value);

                byte[] stored = node.store.get("k");
                assertEquals(0x00, stored[0]);
                assertArrayEquals(value, client.getBytes("k").orElseThrow());
            }
        }
    }

    @Test
    void readingALegacyValueWithCompressEnabledThrowsClearly() throws Exception {
        try (MockNode node = new MockNode()) {
            // A legacy/uncompressed writer's value whose first byte happens
            // to collide with the DEFLATE marker (0x01) — value compression's
            // documented hazard of enabling compress against a keyspace
            // other clients still touch without it.
            try (NanocachedClient writer = connect("127.0.0.1", node.port())) {
                writer.set("k".getBytes(StandardCharsets.UTF_8), new byte[] {0x01, 2, 3, 4});
            }

            try (NanocachedClient reader = NanocachedClient.connect(
                    single("127.0.0.1", node.port()).compress(true))) {
                assertThrows(NanocachedException.DecompressionFailed.class, () -> reader.getBytes("k"));
            }
        }
    }

    // ── 遅延再接続と keep-alive ───────────────────────────────────

    @Test
    void aMalformedValueLengthPoisonsTheConnectionAndRetriesTransparently() throws Exception {
        // Regression for issue #8: a garbage `V <len>` header desyncs the
        // stream; the error must be connection-classified so the built-in
        // redial-and-retry-once makes the SAME call succeed, and the
        // poisoned connection must never serve stray bytes to a later
        // request.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.answerMalformedValueOnce();
                assertEquals(Optional.of("v"), client.get("k"));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void aPlusPrefixedValueLengthPoisonsTheConnectionAndRetriesTransparently() throws Exception {
        // Issue #462: the wire's integer grammar is ASCII digits only
        // (see src/command.rs's parse_length) — a leading '+' is protocol
        // garbage even though Integer.parseInt alone would silently
        // accept it. Same shape as
        // aMalformedValueLengthPoisonsTheConnectionAndRetriesTransparently:
        // connection-classified, so the built-in redial-and-retry-once
        // makes the SAME call succeed.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.answerValueLengthOnce("+5");
                assertEquals(Optional.of("v"), client.get("k"));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void aMismatchedResponseKindPoisonsTheConnection() throws Exception {
        // A well-formed response of the wrong kind (`S` answering a G)
        // means the request/response streams are off by one; reusing the
        // connection would answer every later request with the previous
        // one's response. The mismatch poisons the connection, and the
        // connection-classified error is healed by the built-in
        // redial-and-retry-once — never by reusing the desynced stream.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.answerStoredToGetOnce();
                assertEquals(Optional.of("v"), client.get("k"));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void transparentlyReconnectsAfterAServerFin() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.dropConnections();
                Thread.sleep(50); // let the FIN land
                assertEquals(Optional.of("v"), client.get("k"));
                assertEquals(2, node.connectionCount.get());
            }
        }
    }

    @Test
    void reconnectCooldownSkipsARedialToAKnownDeadAddress() throws Exception {
        try (MockNode node = new MockNode()) {
            int port = node.port();
            // Timing: a wide cooldown window and fast-rejection bound keep this
            // from flaking on loaded CI runners.
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", port).reconnectCooldown(Duration.ofMillis(1000)))) {
                client.set("k", "v");
                node.close();

                // Nothing listens on `port` anymore, so this redial fails
                // fast with a connection-refused error and starts the
                // cooldown window for that address.
                //
                // Polled rather than asserted on the very first get: the
                // first call after node.close() intermittently returned
                // normally on GitHub's ubuntu runners (~1 in 3 runs on
                // main, 2026-08-21) because MockNode.close() used to
                // close the listener *after* the accepted sockets, leaving
                // a connection accepted in between alive and answering.
                // MockNode.close() now closes the listener first; the
                // poll stays as a belt-and-braces guard, since all the
                // cooldown assertions below need is that the failure
                // shows up promptly.
                NanocachedException.ConnectionFailed firstError = firstConnectionFailure(client, port);

                // A listener now sits on the same port and answers
                // immediately with bytes the identify handshake rejects
                // outright — deliberately not a bare close/reset (the
                // shape that triggers connectAndIdentify's legacy-server
                // fallback redial, Identify.java), so each dial against it
                // fails after exactly one connection, letting
                // `connections` below tell "cooldown skipped the dial"
                // apart from "cooldown let it through" unambiguously.
                java.util.concurrent.atomic.AtomicInteger connections =
                        new java.util.concurrent.atomic.AtomicInteger();
                java.util.Set<java.net.Socket> acceptedSockets =
                        java.util.concurrent.ConcurrentHashMap.newKeySet();
                try (java.net.ServerSocket garbage = new java.net.ServerSocket(port)) {
                    Thread acceptor = new Thread(() -> {
                        while (!garbage.isClosed()) {
                            try {
                                java.net.Socket socket = garbage.accept();
                                connections.incrementAndGet();
                                acceptedSockets.add(socket);
                                socket.getOutputStream().write("XXX".getBytes(StandardCharsets.US_ASCII));
                                socket.getOutputStream().flush();
                            } catch (java.io.IOException stop) {
                                return;
                            }
                        }
                    }, "garbage-accept");
                    acceptor.setDaemon(true);
                    acceptor.start();

                    // Still within the cooldown window: rejects with the
                    // cached failure — the very same exception instance —
                    // near-instantly, without dialing the listener at all.
                    long started = System.nanoTime();
                    NanocachedException.ConnectionFailed secondError = assertThrows(
                            NanocachedException.ConnectionFailed.class, () -> client.get("k"));
                    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                    assertTrue(elapsedMillis < 500,
                            "expected a cooldown-fast rejection, took " + elapsedMillis + "ms");
                    assertEquals(0, connections.get(), "the cooldown did not prevent a redial");
                    assertTrue(firstError == secondError,
                            "expected the exact same cached exception, not a fresh one");

                    // Once the cooldown window has passed, the address is
                    // dialed again, this time reaching the listener.
                    Thread.sleep(1200);
                    NanocachedException thirdError = assertThrows(
                            NanocachedException.class, () -> client.get("k"));
                    assertTrue(thirdError.getMessage().contains("unexpected response to A"),
                            thirdError.getMessage());
                    assertEquals(1, connections.get(),
                            "the address was never redialed after the cooldown elapsed");

                    for (java.net.Socket socket : acceptedSockets) socket.close();
                }
            }
        }
    }

    @Test
    void reconnectCooldownZeroMeansDefaultInsteadOfDisablingIt() throws Exception {
        // Cross-SDK contract (issue: audit finding): Duration.ZERO means
        // "use the default", matching the Go SDK's zero-value Config
        // (whose ReconnectCooldown field, left unset, can't distinguish
        // "not specified" from "explicitly zero") and the Rust SDK's
        // Options::reconnect_cooldown. Previously, zero fell straight
        // through to reconnectCooldownNanos, which effectively disabled
        // the cooldown by accident (the cached entry's deadline was
        // already in the past the moment it was recorded) rather than by
        // the explicit opt-out disableReconnectCooldown() now provides.
        try (MockNode node = new MockNode()) {
            int port = node.port();
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", port).reconnectCooldown(Duration.ZERO))) {
                client.set("k", "v");
                node.close();
                firstConnectionFailure(client, port);

                java.util.concurrent.atomic.AtomicInteger connections =
                        new java.util.concurrent.atomic.AtomicInteger();
                try (java.net.ServerSocket garbage = new java.net.ServerSocket(port)) {
                    Thread acceptor = new Thread(() -> {
                        while (!garbage.isClosed()) {
                            try {
                                java.net.Socket socket = garbage.accept();
                                connections.incrementAndGet();
                                socket.close();
                            } catch (java.io.IOException stop) {
                                return;
                            }
                        }
                    }, "garbage-accept-zero-cooldown");
                    acceptor.setDaemon(true);
                    acceptor.start();

                    // Immediately after the failure: with the default (not
                    // disabled) 1s cooldown, this must be rejected from the
                    // cached failure without a redial at all.
                    assertThrows(NanocachedException.ConnectionFailed.class, () -> client.get("k"));
                    assertEquals(0, connections.get(),
                            "Duration.ZERO must select the default cooldown, not disable it");
                }
            }
        }
    }

    @Test
    void disableReconnectCooldownNeverCachesADialFailure() throws Exception {
        // The Go SDK's equivalent of disableReconnectCooldown() is a
        // negative Config.ReconnectCooldown; both mean every call that
        // finds a dead connection pays its own full dial attempt,
        // instead of ever reusing a cached failure the way the (now
        // default-selecting) Duration.ZERO does.
        try (MockNode node = new MockNode()) {
            int port = node.port();
            try (NanocachedClient client = NanocachedClient.connect(
                    single("127.0.0.1", port).disableReconnectCooldown())) {
                client.set("k", "v");
                node.close();
                // Timing: the closed node's FIN may not have reached the
                // client's live connection yet on a loaded CI runner, in
                // which case one more get can still be answered from the
                // kernel buffers before the redial path (and its
                // connection-refused failure) is ever taken — so poll
                // until the failure shows up instead of asserting on the
                // very first call.
                firstConnectionFailure(client, port);

                java.util.Set<java.net.Socket> acceptedSockets =
                        java.util.concurrent.ConcurrentHashMap.newKeySet();
                java.util.concurrent.atomic.AtomicInteger connections =
                        new java.util.concurrent.atomic.AtomicInteger();
                try (java.net.ServerSocket garbage = new java.net.ServerSocket(port)) {
                    Thread acceptor = new Thread(() -> {
                        while (!garbage.isClosed()) {
                            try {
                                java.net.Socket socket = garbage.accept();
                                connections.incrementAndGet();
                                acceptedSockets.add(socket);
                                socket.getOutputStream().write("XXX".getBytes(StandardCharsets.US_ASCII));
                                socket.getOutputStream().flush();
                            } catch (java.io.IOException stop) {
                                return;
                            }
                        }
                    }, "garbage-accept-disabled-cooldown");
                    acceptor.setDaemon(true);
                    acceptor.start();

                    assertThrows(NanocachedException.class, () -> client.get("k"));
                    assertThrows(NanocachedException.class, () -> client.get("k"));
                    waitFor(() -> connections.get() >= 2,
                            "every call to redial instead of reusing a cached failure");

                    for (java.net.Socket socket : acceptedSockets) socket.close();
                }
            }
        }
    }

    @Test
    void keepAlivePingsAnIdleConnection() throws Exception {
        // Keep-alive is always on with an internal interval (issue #27);
        // the package-visible field exists only so tests can shorten it.
        long defaultInterval = NanocachedClient.keepAliveIntervalMillis;
        NanocachedClient.keepAliveIntervalMillis = 40;
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                waitFor(() -> node.getCount.get() >= 2, "keep-alive pings");
                assertEquals(1, node.connectionCount.get());
            }
        } finally {
            NanocachedClient.keepAliveIntervalMillis = defaultInterval;
        }
    }

    @Test
    void aRequestToAHalfOpenServerFailsWithinTheTimeoutInsteadOfHanging() throws Exception {
        // Regression (issue #42): a server that completes the A handshake
        // but then never answers a G/S/D used to hang get/set/delete
        // forever in future.join() — there was no in-flight request
        // timeout at all. The package-visible field exists only so tests
        // can shorten it.
        long defaultTimeout = Connection.requestTimeoutMillis;
        Connection.requestTimeoutMillis = 150;
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.goSilentAfterHandshake();

                long started = System.nanoTime();
                // A request timeout is surfaced as-is: the client's retry
                // layer does not redial and resend after one (the peer is
                // reachable and simply not answering), so this settles
                // after one window.
                NanocachedException error = assertThrows(NanocachedException.class,
                        () -> client.get("k"));
                assertTrue(error.getMessage().contains("request timed out"),
                        "unexpected failure: " + error.getMessage());
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                assertTrue(elapsedMillis < 2_000, "get() took " + elapsedMillis + "ms, want well under 2s");
            }
        } finally {
            Connection.requestTimeoutMillis = defaultTimeout;
        }
    }

    @Test
    void steadyNewRequestsDoNotPostponeHalfOpenDetection() throws Exception {
        // The deadline is progress-based: new sends must not extend it
        // while an older request is still waiting (mirrors the Go SDK's
        // regression test of the same name).
        long defaultTimeout = Connection.requestTimeoutMillis;
        Connection.requestTimeoutMillis = 200;
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                node.goSilentAfterHandshake();

                // New requests keep arriving well inside every deadline
                // window (once the connection is poisoned they just fail
                // fast).
                Thread ticker = new Thread(() -> {
                    try {
                        while (!Thread.interrupted()) {
                            Thread.sleep(50);
                            try {
                                client.get("more");
                            } catch (RuntimeException ignored) {
                                // Expected once the connection is poisoned.
                            }
                        }
                    } catch (InterruptedException done) {
                        // Test finished.
                    }
                }, "test-steady-traffic");
                ticker.setDaemon(true);
                ticker.start();
                try {
                    long started = System.nanoTime();
                    NanocachedException error = assertThrows(NanocachedException.class,
                            () -> client.get("k"));
                    assertTrue(error.getMessage().contains("request timed out"),
                            "unexpected failure: " + error.getMessage());
                    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                    assertTrue(elapsedMillis < 2_000,
                            "get() took " + elapsedMillis + "ms, want well under 2s");
                } finally {
                    ticker.interrupt();
                    ticker.join(5_000);
                }
            }
        } finally {
            Connection.requestTimeoutMillis = defaultTimeout;
        }
    }

    @Test
    void closeFiresOnCloseExactlyOnceUnderConcurrency() throws Exception {
        // Java's Connection.poison() already gates on `synchronized (this)
        // { if (closed) return; closed = true; ... }`, so this documents
        // the existing correct-by-construction guarantee (the .NET port
        // had the analogous bug: a non-atomic check-then-set let
        // concurrent Close() calls both pass and double-fire onClose,
        // corrupting the open-target counter).
        try (java.net.Socket socket = new java.net.Socket()) {
            try (MockNode node = new MockNode()) {
                socket.connect(new java.net.InetSocketAddress("127.0.0.1", node.port()));
                java.util.concurrent.atomic.AtomicInteger closedCount = new java.util.concurrent.atomic.AtomicInteger();
                Connection connection = new Connection(socket, false, closedCount::incrementAndGet);

                ExecutorService pool = Executors.newFixedThreadPool(8);
                try {
                    List<Future<?>> futures = new ArrayList<>();
                    for (int i = 0; i < 50; i++) {
                        futures.add(pool.submit(connection::close));
                    }
                    for (Future<?> future : futures) future.get();
                } finally {
                    pool.shutdown();
                }

                assertEquals(1, closedCount.get());
            }
        }
    }

    @Test
    void newTrackedConnectionUntracksAndClosesSocketWhenConstructorFails() throws Exception {
        // Regression: Connection's constructor calls
        // socket.getInputStream()/getOutputStream(), which throws
        // IOException on an already-closed/never-connected socket.
        // newTrackedConnection() used to call trackOpenTarget() before
        // that constructor call, with nothing undoing it (or closing the
        // socket) on failure — leaking both the open-target counter and
        // the socket on every occurrence. Exercised directly via
        // reflection since forcing the constructor to fail from the
        // normal connect path isn't otherwise reachable deterministically.
        try (MockNode node = new MockNode()) {
            NanocachedClient client = connect("127.0.0.1", node.port());
            try {
                java.lang.reflect.Field targetKeyField = NanocachedClient.class.getDeclaredField("targetKey");
                targetKeyField.setAccessible(true);
                String targetKey = (String) targetKeyField.get(client);

                java.lang.reflect.Field openTargetsField = NanocachedClient.class.getDeclaredField("OPEN_TARGETS");
                openTargetsField.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.concurrent.ConcurrentHashMap<String, Integer> openTargets =
                        (java.util.concurrent.ConcurrentHashMap<String, Integer>) openTargetsField.get(null);
                int before = openTargets.getOrDefault(targetKey, 0);

                // Never connected: getInputStream()/getOutputStream() (and
                // thus Connection's constructor) throw IOException on it.
                java.net.Socket neverConnected = new java.net.Socket();

                java.lang.reflect.Method newTrackedConnection = NanocachedClient.class.getDeclaredMethod(
                        "newTrackedConnection", java.net.Socket.class, boolean.class);
                newTrackedConnection.setAccessible(true);
                assertThrows(java.lang.reflect.InvocationTargetException.class,
                        () -> newTrackedConnection.invoke(client, neverConnected, false));

                assertEquals(before, openTargets.getOrDefault(targetKey, 0),
                        "the open-target counter must not leak when the Connection constructor fails");
                assertTrue(neverConnected.isClosed(), "the socket must be closed when the Connection constructor fails");
            } finally {
                client.close();
            }
        }
    }

    // ── addresses ─────────────────────────────────────────────────

    @Test
    void rejectsAMissingTarget() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.connect(NanocachedClient.builder()));
        assertTrue(error.getMessage().contains("non-empty addresses list"));
    }

    @Test
    void failsOverToTheSecondAddress() throws Exception {
        try (MockNode node = new MockNode();
                MockDiscovery discovery = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1)) {
            int dead = MockServers.unusedPort();
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(
                            new Address("127.0.0.1", dead),
                            new Address("127.0.0.1", discovery.port()))))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void failsOverPastASilentAddressInsteadOfHangingForever() throws Exception {
        // Regression (issue #40): the identify exchange had no read
        // timeout, so an address that accepts the TCP connection but
        // never answers `A` hung connect() forever instead of failing
        // over. It must now time out (bounded by CONNECT_TIMEOUT_MS) —
        // and a timeout must NOT be mistaken for a legacy pre-tag server,
        // which would trigger a second, equally doomed untagged dial.
        try (MockNode node = new MockNode();
                MockDiscovery discovery = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1);
                java.net.ServerSocket silent = new java.net.ServerSocket(0)) {
            List<java.net.Socket> acceptedSockets =
                    java.util.Collections.synchronizedList(new ArrayList<>());
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) acceptedSockets.add(silent.accept());
                } catch (java.io.IOException ignored) {
                    // Server socket closed.
                }
            }, "test-silent-accept");
            acceptor.setDaemon(true);
            acceptor.start();

            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(
                            new Address("127.0.0.1", silent.getLocalPort()),
                            new Address("127.0.0.1", discovery.port()))))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
            assertEquals(1, acceptedSockets.size(),
                    "a silent peer's read timeout must not trigger the legacy-server redial");
            synchronized (acceptedSockets) {
                for (java.net.Socket socket : acceptedSockets) socket.close();
            }
        }
    }

    @Test
    void skipsAWarmingUpAddress() throws Exception {
        try (MockNode node = new MockNode();
                MockDiscovery warming = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1);
                MockDiscovery healthy = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1)) {
            warming.warmingUp = true;
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(
                            new Address("127.0.0.1", warming.port()),
                            new Address("127.0.0.1", healthy.port()))))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void raisesBusyWhenEveryAddressIsWarming() throws Exception {
        try (MockDiscovery first = new MockDiscovery(List.of(), 1);
                MockDiscovery second = new MockDiscovery(List.of(), 1)) {
            first.warmingUp = true;
            second.warmingUp = true;
            assertThrows(NanocachedException.DiscoveryBusy.class,
                    () -> NanocachedClient.connect(NanocachedClient.builder()
                            .addresses(List.of(
                                    new Address("127.0.0.1", first.port()),
                                    new Address("127.0.0.1", second.port())))));
        }
    }

    // ── discovery response limits ────────────────────────────────

    @Test
    void rejectsANodeCountBeyondTheMaximum() throws Exception {
        // Regression: `N 2000000001 3` used to drive
        // `new ArrayList<>(count)` straight from the wire — a
        // multi-gigabyte allocation from an untrusted server. The header
        // alone must be rejected before any entry is read.
        try (MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.rawListResponse = "N 2000000001 3\n";
            NanocachedException error = assertThrows(NanocachedException.class,
                    () -> connect("127.0.0.1", discovery.port()));
            assertTrue(error.getMessage().contains("node count"), error.getMessage());
        }
    }

    @Test
    void rejectsANodeListResponseBeyondTheAggregateCap() throws Exception {
        // Regression: a within-cap node count can still declare an
        // absurd per-entry name/address length. A single entry near the
        // 16 MiB aggregate cap must be rejected before its body is even
        // read — otherwise a malicious server could make the client
        // allocate gigabytes without ever sending that many bytes.
        try (MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            int hugeNameLength = 20 * 1024 * 1024; // > 16 MiB alone
            discovery.rawListResponse = "N 1 1\n" + hugeNameLength + " 0\n";
            NanocachedException error = assertThrows(NanocachedException.class,
                    () -> connect("127.0.0.1", discovery.port()));
            assertTrue(error.getMessage().contains("exceeds"), error.getMessage());
        }
    }

    @Test
    void rejectsAMalformedNodeCountHeaderInsteadOfLeakingANumberFormatException() throws Exception {
        try (MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.rawListResponse = "N x 1\n";
            assertThrows(NanocachedException.class, () -> connect("127.0.0.1", discovery.port()));
        }
    }

    @Test
    void rejectsAMalformedNodeEntryLengthHeaderInsteadOfLeakingANumberFormatException() throws Exception {
        try (MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.rawListResponse = "N 1 1\nx y\n";
            assertThrows(NanocachedException.class, () -> connect("127.0.0.1", discovery.port()));
        }
    }

    @Test
    void rejectsAMalformedNodeAddressPortInsteadOfLeakingANumberFormatException() throws Exception {
        // Regression: NanocachedClient.openNodeConnection used to call
        // Integer.parseInt directly on a discovered node's port.
        try (MockDiscovery discovery = new MockDiscovery(
                List.of(new DiscoveredNode(NAMES.get(0), "127.0.0.1:notaport")), 1)) {
            NanocachedException error = assertThrows(NanocachedException.class,
                    () -> connect("127.0.0.1", discovery.port()));
            assertTrue(error.getMessage().contains("invalid node address"), error.getMessage());
        }
    }

    @Test
    void rejectsADiscoveryHeaderLineThatNeverTerminates() throws Exception {
        // Regression (issue: audit finding): Identify.readLine had no cap
        // at all — unlike Connection.java's own readLine, bounded by
        // MAX_HEADER_LINE_LENGTH — so a malicious or buggy discovery
        // server that streams bytes with no '\n' would grow the client's
        // line buffer without bound (an OOM risk) instead of failing
        // fast. Now shares Connection.MAX_HEADER_LINE_LENGTH (4096) and
        // the same connection-lost exception type.
        try (MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.rawListResponse = "N " + "9".repeat(8192); // never terminates
            NanocachedException.ConnectionFailed error = assertThrows(
                    NanocachedException.ConnectionFailed.class,
                    () -> connect("127.0.0.1", discovery.port()));
            assertTrue(error.getMessage().contains("header line too long"), error.getMessage());
        }
    }

    // ── bootstrap dialer pool cap ────────────────────────────────

    @Test
    void capsTheBootstrapDialerPoolRegardlessOfNodeCount() throws Exception {
        // Regression (issue #178): openCluster used to size its dialer
        // pool to nodes.size() with no cap — the only bound on a
        // discovery reply's node count is Identify.MAX_NODE_COUNT
        // (65536), so a large or malicious reply could make bootstrap
        // try to spawn tens of thousands of native threads. Uses more
        // "silent" addresses (accept the TCP connection, never answer —
        // see failsOverPastASilentAddressInsteadOfHangingForever) than
        // the cap, so every dial blocks for the full identify read
        // timeout: that gives this test a wide, deterministic window in
        // which to sample how many "nanocached-bootstrap-dial" threads
        // are alive at once, instead of racing a fast-failing dial.
        int nodeCount = 20; // > the 16-thread cap (MAX_BOOTSTRAP_DIALER_THREADS)
        List<java.net.ServerSocket> silentServers = new ArrayList<>();
        List<DiscoveredNode> nodes = new ArrayList<>();
        try {
            for (int i = 0; i < nodeCount; i++) {
                java.net.ServerSocket silent = new java.net.ServerSocket(0);
                silentServers.add(silent);
                Thread acceptor = new Thread(() -> {
                    try {
                        while (true) silent.accept();
                    } catch (java.io.IOException ignored) {
                        // Server socket closed.
                    }
                }, "test-silent-accept-" + i);
                acceptor.setDaemon(true);
                acceptor.start();
                nodes.add(new DiscoveredNode(NAMES.get(0) + "-" + i, "127.0.0.1:" + silent.getLocalPort()));
            }

            try (MockDiscovery discovery = new MockDiscovery(nodes, 1)) {
                java.util.concurrent.atomic.AtomicInteger maxObserved =
                        new java.util.concurrent.atomic.AtomicInteger();
                Thread sampler = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        long alive = Thread.getAllStackTraces().keySet().stream()
                                .filter(t -> t.getName().equals("nanocached-bootstrap-dial"))
                                .count();
                        maxObserved.updateAndGet(current -> (int) Math.max(current, alive));
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException interrupted) {
                            return;
                        }
                    }
                }, "test-thread-sampler");
                sampler.setDaemon(true);
                sampler.start();
                try {
                    // Every node is silent, so bootstrap can't reach any
                    // of them — it's expected to fail once every dial
                    // has timed out; that failure isn't what's under
                    // test here, only how many dialer threads it took.
                    assertThrows(RuntimeException.class, () -> connect("127.0.0.1", discovery.port()));
                } finally {
                    sampler.interrupt();
                    sampler.join();
                }

                assertTrue(maxObserved.get() > 0, "test never observed a dialer thread running");
                assertTrue(maxObserved.get() <= 16,
                        "bootstrap dialer pool must never exceed its cap, observed " + maxObserved.get());
            }
        } finally {
            for (java.net.ServerSocket silent : silentServers) silent.close();
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
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                for (int i = 0; i < 50; i++) {
                    client.set("key-" + i, "value of key-" + i);
                }
                for (int i = 0; i < 50; i++) {
                    assertEquals(Optional.of("value of key-" + i), client.get("key-" + i));
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
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "some-key";
                client.set(key, "v");
                MockNode owner = cluster.nodes()
                        .get(new HashRing(NAMES).route(key.getBytes(StandardCharsets.UTF_8)));

                owner.answerWrongNodeOnce();
                assertEquals(Optional.of("v"), client.get(key));

                owner.answerWrongNodeOnce();
                owner.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> client.get(key));
            }
        }
    }

    @Test
    void fansWritesOutToEveryOwner() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
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
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "survives";
                client.set(key, "still here");
                String primary = new HashRing(NAMES)
                        .owners(key.getBytes(StandardCharsets.UTF_8), 2).get(0);
                cluster.nodes().get(primary).close();
                Thread.sleep(50);
                assertEquals(Optional.of("still here"), client.get(key));
            }
        }
    }

    @Test
    void getManyReadsFromTheNextOwnerWhenAPrimaryIsUnreachable() throws Exception {
        // A primary that cannot be reached at the connection level (here a
        // closed port; in the field a dead node, or a certificate this
        // client cannot verify) must not fail the keys it owns: with R=2
        // the replica holds them, and a single get would already have
        // fallen over to it. Discovery is left as it is — the client's
        // ring still names the dead node as primary.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (int i = 0; i < 24; i++) values.put("key-" + i, "value-" + i);
                client.setMany(values);
                String dead = NAMES.get(0);
                HashRing ring = new HashRing(NAMES);
                assertTrue(values.keySet().stream().anyMatch(
                        key -> ring.owners(key.getBytes(StandardCharsets.UTF_8), 2).get(0).equals(dead)),
                        "the key set must give the dead node some primaries");
                cluster.nodes().get(dead).close();
                Thread.sleep(50); // let the FIN land

                assertEquals(values, client.getMany(new ArrayList<>(values.keySet())));
            }
        }
    }

    @Test
    void getManyReportsAnUnreachableOwnerAsAConnectionFailureNotWrongNode() throws Exception {
        // R=1: the only owner of some keys is gone. Nothing answered `W`,
        // so the failure must not be the wrong-node one; the keys the live
        // node owns are still returned, attached to the exception.
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (int i = 0; i < 24; i++) values.put("key-" + i, "value-" + i);
                client.setMany(values);
                String dead = NAMES.get(0);
                String live = NAMES.get(1);
                HashRing ring = new HashRing(NAMES);
                Map<String, String> liveValues = new java.util.LinkedHashMap<>();
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    if (ring.owners(entry.getKey().getBytes(StandardCharsets.UTF_8), 1).get(0).equals(live)) {
                        liveValues.put(entry.getKey(), entry.getValue());
                    }
                }
                assertTrue(!liveValues.isEmpty() && liveValues.size() < values.size(),
                        "both nodes must own some keys");
                cluster.nodes().get(dead).close();
                Thread.sleep(50); // let the FIN land

                NanocachedException failure = assertThrows(
                        NanocachedException.PartialConnectionFailedStrings.class,
                        () -> client.getMany(new ArrayList<>(values.keySet())));
                assertFalse(failure instanceof NanocachedException.WrongNode);
                assertEquals(liveValues,
                        ((NanocachedException.PartialConnectionFailedStrings) failure).partialValues);
                assertTrue(failure.getCause() instanceof NanocachedException.ConnectionFailed,
                        "cause = " + failure.getCause());
            }
        }
    }

    @Test
    void aDeadReplicaDoesNotFailWrites() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "written-anyway";
                List<String> owners =
                        new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                cluster.nodes().get(owners.get(1)).close();
                Thread.sleep(50);
                client.set(key, "v");
                assertTrue(cluster.nodes().get(owners.get(0)).store
                        .containsKey(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8))));
                assertEquals(Optional.of("v"), client.get(key));
            }
        }
    }

    @Test
    void writesRouteAroundADeadPrimaryOnceDiscoveryDropsIt() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
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
                assertEquals(Optional.of("v"), client.get(key));
            }
        }
    }

    @Test
    void fansDeletesOutToEveryOwner() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
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

    // ── 重複ノード名の重複排除 (issue #461) ──────────────────────────

    @Test
    void aDuplicatedDiscoveryEntryIsDeduplicatedOnInitialConnect() throws Exception {
        // Regression for issue #461 (mirrors Go's #389 fix, which built
        // on HashRing's own #360 dedupe): a discovery roster listing the
        // same node name twice used to be dialed twice, and
        // openCluster's members.put(node.name(), ...) write for the
        // name's second occurrence silently overwrote the first —
        // leaking that connection (never closed, invisible to stats()
        // and the no-longer-listed close sweep). Discovery here hands
        // back NAMES.get(0) twice; the ring and member map must end up
        // with only the deduplicated names, and the node must be dialed
        // once.
        try (MockNode nodeA = new MockNode(); MockNode nodeB = new MockNode()) {
            try (MockDiscovery discovery = new MockDiscovery(
                    List.of(new DiscoveredNode(NAMES.get(0), nodeA.address()),
                            new DiscoveredNode(NAMES.get(0), nodeA.address()),
                            new DiscoveredNode(NAMES.get(1), nodeB.address())),
                    2)) {
                try (NanocachedClient client = connect("127.0.0.1", discovery.port())) {
                    Field membersField = NanocachedClient.class.getDeclaredField("members");
                    membersField.setAccessible(true);
                    Map<?, ?> members = (Map<?, ?>) membersField.get(client);
                    assertEquals(Set.of(NAMES.get(0), NAMES.get(1)), members.keySet());
                    assertEquals(1, nodeA.connectionCount.get());

                    Field ringField = NanocachedClient.class.getDeclaredField("ring");
                    ringField.setAccessible(true);
                    HashRing ring = (HashRing) ringField.get(client);
                    for (int replicas = 1; replicas <= 2; replicas++) {
                        List<String> owners = ring.owners("some-key".getBytes(StandardCharsets.UTF_8), replicas);
                        assertEquals(owners.size(), new java.util.HashSet<>(owners).size(), owners.toString());
                        assertTrue(owners.size() <= 2);
                    }
                }
            }
        }
    }

    @Test
    void aDuplicatedDiscoveryEntryIsDeduplicatedOnRefresh() throws Exception {
        // Same regression as above, but for refreshNodeList's own dial
        // loop: a repeated name in the refreshed roster must not replace
        // a member's live connection with a second, redundant dial of
        // the same node.
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String name = NAMES.get(0);
                Connection before = memberConnectionOf(client, name);

                MockNode node = cluster.nodes().get(name);
                cluster.discovery().nodes = List.of(
                        new DiscoveredNode(name, node.address()),
                        new DiscoveredNode(name, node.address()),
                        new DiscoveredNode(NAMES.get(1), cluster.nodes().get(NAMES.get(1)).address()));

                Method refreshNodeList = NanocachedClient.class.getDeclaredMethod("refreshNodeList");
                refreshNodeList.setAccessible(true);
                refreshNodeList.invoke(client);

                Connection after = memberConnectionOf(client, name);
                assertSame(before, after,
                        "a repeated node name replaced the live connection (leaking it) instead of being deduped");
            }
        }
    }

    // ── クラスタでのバッチ get/set (issue #151) ─────────────────────

    @Test
    void batchedGetSetRouteAcrossOwnersAndReassembleInCallerOrder() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                List<String> keys = new ArrayList<>();
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (int i = 0; i < 20; i++) {
                    keys.add("key-" + i);
                    values.put("key-" + i, "value-" + i);
                }
                client.setMany(values);
                assertEquals(values, client.getMany(keys));
                // Every key routed to exactly one owner: both nodes together
                // received every `o`/`m` sub-frame, and each key landed on
                // exactly the node HashRing itself would route it to.
                int totalStored = cluster.nodes().values().stream().mapToInt(n -> n.store.size()).sum();
                assertEquals(20, totalStored);
                assertTrue(cluster.nodes().values().stream().allMatch(n -> !n.store.isEmpty()),
                        "20 keys hashed across 2 owners should touch both nodes");
            }
        }
    }

    @Test
    void batchedWritesFanOutToEveryOwnerWhenReplicated() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                Map<String, String> values = new java.util.LinkedHashMap<>();
                for (int i = 0; i < 10; i++) values.put("key-" + i, "v");
                client.setMany(values);
                for (String key : values.keySet()) {
                    String stored = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));
                    for (MockNode node : cluster.nodes().values()) {
                        assertTrue(node.store.containsKey(stored), key + " missing from a node");
                    }
                }
                assertEquals(values.keySet().stream().map(k -> "v").toList().size(),
                        client.getMany(new ArrayList<>(values.keySet())).size());
            }
        }
    }

    @Test
    void aDeadReplicaDoesNotFailABatchedWrite() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "written-anyway";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                cluster.nodes().get(owners.get(1)).close();
                Thread.sleep(50);
                client.setMany(Map.of(key, "v"));
                assertTrue(cluster.nodes().get(owners.get(0)).store
                        .containsKey(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8))));
                assertEquals(Map.of(key, "v"), client.getMany(List.of(key)));
            }
        }
    }

    @Test
    void batchedGetWrongNodeTriggersRefreshAndOneRetry() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "some-key";
                client.setMany(Map.of(key, "v"));
                MockNode owner = cluster.nodes()
                        .get(new HashRing(NAMES).route(key.getBytes(StandardCharsets.UTF_8)));

                owner.answerWrongNodeOnce();
                assertEquals(Map.of(key, "v"), client.getMany(List.of(key)));

                owner.answerWrongNodeOnce();
                owner.answerWrongNodeOnce();
                NanocachedException.PartialWrongNode failure = assertThrows(
                        NanocachedException.PartialWrongNode.class, () -> client.getManyBytes(List.of(key)));
                assertTrue(failure.partialValues.isEmpty());
            }
        }
    }

    @Test
    void batchedSetWrongNodeTriggersRefreshAndOneRetry() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "some-key";
                MockNode owner = cluster.nodes()
                        .get(new HashRing(NAMES).route(key.getBytes(StandardCharsets.UTF_8)));

                owner.answerWrongNodeOnce();
                client.setMany(Map.of(key, "v"));
                assertEquals(Map.of(key, "v"), client.getMany(List.of(key)));

                owner.answerWrongNodeOnce();
                owner.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> client.setMany(Map.of(key, "v2")));
            }
        }
    }

    @Test
    void multiGetDrainsEveryLegAndDoesNotCountASecondLegsDecompressionFailureAsABackgroundBug() throws Exception {
        // Regression for issue #230: multiGetPass used to rethrow on the
        // first failing leg's join() without draining the remaining
        // legs, so a second, independent leg's bug would vanish silently
        // instead of being observed — unlike multiSetPass/write(), which
        // always drain every leg first. Two owners each hold a corrupted
        // "compressed" value (an unrecognized marker byte, which escapes
        // runMultiGetLeg as a DecompressionFailed rather than being fed
        // into the retry pass, per that method's own doc comment).
        //
        // Issue #413: this used to also assert the second leg's
        // DecompressionFailed was counted via backgroundWriteBugs — but
        // that counter is documented to "never increment for a legitimate
        // reason", and a client-side compress mismatch/decompression bomb
        // is exactly that: a legitimate, expected failure, not a
        // programming bug in this SDK's background-write handling. Both
        // legs racing the same corrupt-data condition concurrently must
        // still leave exactly one DecompressionFailed propagating to the
        // caller (whichever leg drains first) and must NOT bump
        // backgroundWriteBugs for the other.
        try (Cluster cluster = startCluster(1)) {
            String keyOnNodeA = null;
            String keyOnNodeB = null;
            for (int i = 0; i < 1000 && (keyOnNodeA == null || keyOnNodeB == null); i++) {
                String candidate = "gk-" + i;
                String owner = new HashRing(NAMES).route(candidate.getBytes(StandardCharsets.UTF_8));
                if (owner.equals(NAMES.get(0)) && keyOnNodeA == null) keyOnNodeA = candidate;
                if (owner.equals(NAMES.get(1)) && keyOnNodeB == null) keyOnNodeB = candidate;
            }
            assertTrue(keyOnNodeA != null && keyOnNodeB != null, "need one key routing to each node");
            String finalKeyOnNodeA = keyOnNodeA;
            String finalKeyOnNodeB = keyOnNodeB;

            byte[] corrupt = {0x02, 1, 2, 3}; // unrecognized compression marker byte
            cluster.nodes().get(NAMES.get(0)).store.put(
                    MockNode.keyOf(keyOnNodeA.getBytes(StandardCharsets.UTF_8)), corrupt);
            cluster.nodes().get(NAMES.get(1)).store.put(
                    MockNode.keyOf(keyOnNodeB.getBytes(StandardCharsets.UTF_8)), corrupt);

            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(new Address("127.0.0.1", cluster.discovery().port())))
                    .compress(true))) {
                long before = client.stats().backgroundWriteBugs();
                assertThrows(NanocachedException.DecompressionFailed.class,
                        () -> client.getManyBytes(List.of(finalKeyOnNodeA, finalKeyOnNodeB)));
                assertEquals(before, client.stats().backgroundWriteBugs(),
                        "a concurrent leg's DecompressionFailed is a legitimate, expected failure — "
                                + "it must never be counted as a background write bug");
            }
        }
    }

    @Test
    void drainLegsKeepingFirstBugReportsEveryBugPastTheFirst() throws Exception {
        // Regression for issue #233: multiSetPass and clearFanOutOnce used
        // to just overwrite a single tracked "legBug" variable on every
        // failing leg in turn, so only the LAST leg's bug ever reached the
        // caller and every earlier one vanished without even being
        // counted — unlike multiGetPass (issue #230), which always drained
        // every leg and reported every bug past the first via
        // reportBackgroundWriteBug, even before this method existed. All
        // three now share drainLegsKeepingFirstBug (multiGetPass's own
        // duplicate loop was folded in as part of issue #277), exercised
        // directly here with synthetic failing legs: every real protocol-level failure this
        // SDK produces is already wrapped as a NanocachedException and
        // caught well before this method ever sees it, so a genuine,
        // uncaught bug isn't reproducible by feeding a real server bad
        // data the way issue #230's own regression test could for GET.
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                RuntimeException first = new IllegalStateException("first leg's bug");
                RuntimeException second = new IllegalStateException("second leg's bug");
                RuntimeException third = new IllegalStateException("third leg's bug");
                List<CompletableFuture<Void>> legs = List.of(
                        CompletableFuture.completedFuture(null),
                        CompletableFuture.failedFuture(first),
                        CompletableFuture.failedFuture(second),
                        CompletableFuture.failedFuture(third));

                long before = client.stats().backgroundWriteBugs();
                RuntimeException returned = client.drainLegsKeepingFirstBug(legs);

                assertSame(first, returned, "the first leg's bug must be the one returned to the caller");
                assertEquals(before + 2, client.stats().backgroundWriteBugs(),
                        "the second and third legs' bugs must still be counted, not discarded");
            }
        }
    }

    @Test
    void drainLegsKeepingFirstBugDoesNotCountAFurtherDecompressionFailedAsABackgroundBug() throws Exception {
        // Regression for issue #413(b): runMultiGetLeg deliberately lets a
        // DecompressionFailed from decompressForBatch escape uncaught
        // (per that method's own doc comment — a decompression failure
        // must abort the whole batch, never be fed into the retry pass),
        // so unlike every other leg failure this SDK produces, it is NOT
        // already caught before reaching drainLegsKeepingFirstBug. Since
        // every owner's leg runs concurrently, more than one leg can hit
        // it at once (e.g. every leg racing the same shared
        // decompression-budget check). Only the first such bug reaching
        // here needs to propagate to the caller; any further one is a
        // redundant echo of the same client-side condition, not an
        // independent programming bug, and must be dropped rather than
        // counted via backgroundWriteBugs — that counter is documented to
        // "never increment for a legitimate reason".
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                RuntimeException first = new NanocachedException.DecompressionFailed("first leg's decompression bug");
                RuntimeException second = new NanocachedException.DecompressionFailed("second leg's decompression bug");
                RuntimeException third = new IllegalStateException("third leg's genuine bug");
                List<CompletableFuture<Void>> legs = List.of(
                        CompletableFuture.completedFuture(null),
                        CompletableFuture.failedFuture(first),
                        CompletableFuture.failedFuture(second),
                        CompletableFuture.failedFuture(third));

                long before = client.stats().backgroundWriteBugs();
                RuntimeException returned = client.drainLegsKeepingFirstBug(legs);

                assertSame(first, returned, "the first leg's bug must be the one returned to the caller");
                assertEquals(before + 1, client.stats().backgroundWriteBugs(),
                        "the second leg's DecompressionFailed must be dropped, not counted as a background "
                                + "bug — only the third leg's genuine bug may bump the counter");
            }
        }
    }

    // ── クラスタでの INCR/DECR (issue #129) ─────────────────────────
    // The one thing that must never happen: a replica replaying the
    // increment itself. Comparing final stored values between primary and
    // replica would NOT prove this — a buggy implementation that mistakenly
    // replays `i` on the replica would produce the same final bytes from
    // the same seed value. incrCount is the actual proof.

    @Test
    void incrSendsIOnlyToThePrimaryAndReplicatesTheResultAsSet() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-counter";
                client.set(key, "10"); // fans out to both owners, seeding them identically
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertEquals(java.util.OptionalLong.of(15), client.incr(key, 5));

                assertEquals(1, primary.incrCount.get(), "primary must receive exactly one `i` frame");
                assertEquals(0, replica.incrCount.get(), "a replica must never receive an `i` frame");
                assertEquals("15", new String(replica.store.get(storedKey), StandardCharsets.UTF_8),
                        "the replica must hold the primary's literal resulting value");
                assertEquals("15", new String(primary.store.get(storedKey), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void incrDoesNotTouchReplicasOnAMissOrNonNumericValue() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String missing = "never-set";
                List<String> missingOwners = new HashRing(NAMES).owners(missing.getBytes(StandardCharsets.UTF_8), 2);
                assertEquals(java.util.OptionalLong.empty(), client.incr(missing, 1));
                // Only the primary is ever asked; a miss touches no replica.
                assertEquals(1, cluster.nodes().get(missingOwners.get(0)).incrCount.get());
                assertEquals(0, cluster.nodes().get(missingOwners.get(1)).incrCount.get());
                assertEquals(0, cluster.nodes().get(missingOwners.get(1)).store.size());

                String word = "not-a-number";
                client.set(word, "hello");
                List<String> wordOwners = new HashRing(NAMES).owners(word.getBytes(StandardCharsets.UTF_8), 2);
                // Owners are just a permutation of this 2-node cluster's
                // two names, so the "word" primary may be the same
                // physical node as the "missing" case's above — compare
                // against its count just before this call, not an
                // absolute value.
                int wordPrimaryIncrCountBefore = cluster.nodes().get(wordOwners.get(0)).incrCount.get();
                long replicaWriteFailuresBefore = client.stats().replicaWriteFailures();
                assertThrows(NanocachedException.NotNumeric.class, () -> client.incr(word, 1));
                assertEquals(wordPrimaryIncrCountBefore + 1, cluster.nodes().get(wordOwners.get(0)).incrCount.get());
                // No replica `set` was triggered by the failed incr — the
                // replica-write-failure counter (a completely separate
                // mechanism) must not have moved either.
                assertEquals(replicaWriteFailuresBefore, client.stats().replicaWriteFailures());
            }
        }
    }

    @Test
    void incrAppliedButAcknowledgementLostIsNeverReplayedByTheOuterWrongNodeRetry() throws Exception {
        // issue #225: applyReconnectingNonIdempotent's own redial-and-
        // retry isn't the only layer that could double-apply INCR — the
        // outer withWrongNodeRetry (which refreshes the ring and re-runs
        // the WHOLE incr call on a W or a dead primary) must also refuse
        // to retry once a ConnectionFailed reaches it, since by then the
        // primary may already have applied the op. This is a cluster (2
        // owners, replication 2) counterpart of the single-node
        // incrAppliedButAcknowledgementLostIsNeverReplayed above,
        // specifically targeting that outer layer rather than the inner
        // one.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-counter";
                client.set(key, "10");
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                primary.dropReplyAfterNextIncr();
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.incr(key, 5));

                // The outer withWrongNodeRetry must not have re-run
                // incrPrimaryThenReplicate: only the one `i` frame the
                // primary already applied, never a second one — and the
                // value it holds reflects exactly one +5, not two.
                assertEquals(1, primary.incrCount.get(),
                        "the primary must receive exactly one `i` frame, not a replayed one");
                assertEquals("15", new String(primary.store.get(storedKey), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void incrReplicatesTheEntrysLiveTtlToTheReplica() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "counter-with-ttl";
                client.set(key, "10", 60);
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertEquals(java.util.OptionalLong.of(11), client.incr(key, 1));

                assertEquals("11", new String(replica.store.get(storedKey), StandardCharsets.UTF_8));
                assertEquals(Long.valueOf(60), replica.ttls.get(storedKey),
                        "the replica's set leg must carry the entry's live TTL");
                assertEquals(Long.valueOf(60), primary.ttls.get(storedKey));
            }
        }
    }

    // ── クラスタでの Compare-and-set (issue #141) ────────────────────
    // The one thing that must never happen: a replica evaluating <cond>
    // itself. Comparing final stored values between primary and replica
    // would NOT prove this — a buggy implementation that mistakenly
    // replayed k/x on the replica could still land on the same bytes from
    // the same seed. casSetCount/casDeleteCount are the actual proof.

    @Test
    void casSendsKOnlyToThePrimaryAndReplicatesTheResultAsSet() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-cas-key";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertTrue(client.putIfAbsent(key, "v1"));

                assertEquals(1, primary.casSetCount.get(), "primary must receive exactly one `k` frame");
                assertEquals(0, replica.casSetCount.get(), "a replica must never receive a `k` frame");
                assertEquals("v1", new String(replica.store.get(storedKey), StandardCharsets.UTF_8),
                        "the replica must hold the primary's literal resulting value");
                assertEquals("v1", new String(primary.store.get(storedKey), StandardCharsets.UTF_8));

                // A mismatch (key already present) never touches any
                // replica, and the primary's own casSetCount still moves —
                // only the fan-out is skipped.
                assertFalse(client.putIfAbsent(key, "v2"));
                assertEquals(2, primary.casSetCount.get());
                assertEquals(0, replica.casSetCount.get());
                assertEquals("v1", new String(replica.store.get(storedKey), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void casReplicatesTheLiteralTtlNeverRecomputingIt() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "cas-with-ttl";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));

                assertTrue(client.putIfAbsent(key, "v1", 60));

                assertEquals(Long.valueOf(60), primary.ttls.get(storedKey));
                assertEquals(Long.valueOf(60), replica.ttls.get(storedKey),
                        "the replica's set leg must carry the exact ttl the caller supplied");
            }
        }
    }

    @Test
    void deleteIfMatchesSendsXOnlyToThePrimaryAndReplicatesTheResultAsDelete() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "shared-cas-delete-key";
                client.set(key, "v1"); // fans out to both owners, seeding them identically
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));
                String token = client.getWithToken(key).orElseThrow().token();

                assertTrue(client.deleteIfMatches(key, token));

                assertEquals(1, primary.casDeleteCount.get(), "primary must receive exactly one `x` frame");
                assertEquals(0, replica.casDeleteCount.get(), "a replica must never receive an `x` frame");
                assertFalse(primary.store.containsKey(storedKey));
                assertFalse(replica.store.containsKey(storedKey),
                        "the replica must have the key removed as an ordinary delete");
            }
        }
    }

    @Test
    void casDoesNotTouchReplicasOnAMismatch() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "cas-mismatch-key";
                client.set(key, "v1");
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                MockNode primary = cluster.nodes().get(owners.get(0));
                MockNode replica = cluster.nodes().get(owners.get(1));
                String storedKey = MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8));
                String staleToken = NanocachedClient.contentDigest("stale".getBytes(StandardCharsets.UTF_8));

                assertFalse(client.deleteIfMatches(key, staleToken));

                assertEquals(1, primary.casDeleteCount.get());
                assertEquals(0, replica.casDeleteCount.get());
                assertTrue(primary.store.containsKey(storedKey));
                assertTrue(replica.store.containsKey(storedKey));
            }
        }
    }

    // ── fire-and-forget レプリカ書き込み (fire-and-forget replica writes) ──────────

    private static NanocachedClient connectFireAndForget(int port) {
        return NanocachedClient.connect(NanocachedClient.builder()
                .addresses(List.of(new Address("127.0.0.1", port)))
                .fireAndForgetReplicas(true));
    }

    // A "did it wait for the mock's delay" assertion can't compare the
    // measured elapsed time against the delay exactly: Thread.sleep()'s
    // wakeup is only approximate and nanoTime()/1_000_000 truncates, so an
    // 80ms delay can be observed as 79ms. Slack the lower bound by this
    // much rather than asserting on the boundary; still miles away from
    // the ~0ms an immediate return would show.
    private static final long TIMING_TOLERANCE_MILLIS = 20;

    @org.junit.jupiter.api.AfterEach
    void resetMaxInFlightBackgroundReplicaWrites() {
        NanocachedClient.maxInFlightBackgroundReplicaWrites = 32;
    }

    @Test
    void byDefaultAWriteStillWaitsForTheReplicaLeg() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);
            cluster.nodes().get(replica).delaySets(80);

            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                long start = System.nanoTime();
                client.set("k", "v");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                assertTrue(elapsedMillis >= 80 - TIMING_TOLERANCE_MILLIS, "set() should have waited for the replica, took " + elapsedMillis + "ms");
            }
        }
    }

    @Test
    void fireAndForgetReplicasReturnsAsSoonAsThePrimaryAcks() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);
            cluster.nodes().get(replica).delaySets(200);

            try (NanocachedClient client = connectFireAndForget(cluster.discovery().port())) {
                long start = System.nanoTime();
                client.set("k", "v");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                assertTrue(elapsedMillis < 200, "set() should not have waited for the replica, took " + elapsedMillis + "ms");

                String stored = MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8));
                waitFor(() -> cluster.nodes().get(replica).store.containsKey(stored),
                        "the background write to land on the replica");
            }
        }
    }

    @Test
    void fireAndForgetReplicasStillReturnImmediatelyWhenTheSharedLegPoolIsSaturated() throws Exception {
        // Regression for issue #500: #486 bounded the shared leg pool's
        // queue and made its overflow policy "the submitter runs the task
        // itself". That pool also carried the fire-and-forget legs, so a
        // burst of legs no permit bounds (synchronous fallbacks, batched
        // get/set owner legs) filling it made a permit-holding
        // fire-and-forget set() run its replica leg inline — blocking a
        // caller the option promises to return to as soon as the primary
        // acks. The fire-and-forget legs now run on their own pool
        // (backgroundLegs), which the permits keep from ever filling.
        // Saturate every thread *and* every queue slot of replicaWriters
        // with unrelated blocking work, then prove set() still returns
        // well inside the replica's delay.
        NanocachedClient.maxInFlightBackgroundReplicaWrites = 1;

        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);
            cluster.nodes().get(replica).delaySets(300);

            try (NanocachedClient client = connectFireAndForget(cluster.discovery().port())) {
                Field replicaWritersField = NanocachedClient.class.getDeclaredField("replicaWriters");
                replicaWritersField.setAccessible(true);
                ExecutorService replicaWriters = (ExecutorService) replicaWritersField.get(client);

                // 1 (maxInFlightBackgroundReplicaWrites) + REPLICA_WRITER_POOL_HEADROOM (16)
                // threads over a queue of the same depth: 34 slots in all. One
                // more submit would run on this thread (the overflow policy) —
                // exactly what the fire-and-forget leg must no longer risk.
                int threads = 17;
                java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.CountDownLatch occupied = new java.util.concurrent.CountDownLatch(threads);
                for (int i = 0; i < 2 * threads; i++) {
                    replicaWriters.submit(() -> {
                        occupied.countDown();
                        release.await();
                        return null;
                    });
                }
                assertTrue(occupied.await(5, java.util.concurrent.TimeUnit.SECONDS),
                        "every replicaWriters thread should have started its blocking dummy task");
                try {
                    long start = System.nanoTime();
                    client.set("k", "v");
                    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                    assertTrue(elapsedMillis < 300,
                            "set() must not run its replica leg inline just because the shared pool is full, took "
                                    + elapsedMillis + "ms");

                    String stored = MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8));
                    waitFor(() -> cluster.nodes().get(replica).store.containsKey(stored),
                            "the background write to land on the replica");
                } finally {
                    release.countDown();
                }
            }
        }
    }

    @Test
    void fireAndForgetReplicaLegDoesNotAliasTheCallersArray() throws Exception {
        // Issue #326: with compress off, set()'s `outgoing` used to be the
        // caller's own `value` array, not a copy — a fire-and-forget
        // replica leg closing over it could still be waiting to run once
        // set() has already returned, so a caller that reused/mutated
        // `value` in place right after the call could silently change
        // what the replica ends up storing. The fix defensively copies
        // the array for a leg that actually goes into the background,
        // synchronously before set() returns — so it must never be
        // affected by a mutation the caller makes afterwards, no matter
        // how late the leg itself actually runs.
        //
        // To make "how late the leg actually runs" deterministic instead
        // of a real-time race, permits/pool size are shrunk to 1 replica
        // writer slot and the one backgroundLegs thread (issue #500: the
        // fire-and-forget legs' own pool, sized to the permit count) is
        // occupied with a blocking dummy task before set() is even called
        // — so the real leg is guaranteed to still be sitting in that
        // pool's queue, not yet read `outgoing`, at the point this test
        // mutates `value`.
        NanocachedClient.maxInFlightBackgroundReplicaWrites = 1;

        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);

            try (NanocachedClient client = connectFireAndForget(cluster.discovery().port())) {
                Field backgroundLegsField = NanocachedClient.class.getDeclaredField("backgroundLegs");
                backgroundLegsField.setAccessible(true);
                ExecutorService backgroundLegs = (ExecutorService) backgroundLegsField.get(client);

                // maxInFlightBackgroundReplicaWrites (1) threads.
                int poolSize = 1;
                java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.CountDownLatch occupied = new java.util.concurrent.CountDownLatch(poolSize);
                for (int i = 0; i < poolSize; i++) {
                    backgroundLegs.submit(() -> {
                        occupied.countDown();
                        release.await();
                        return null;
                    });
                }
                assertTrue(occupied.await(5, java.util.concurrent.TimeUnit.SECONDS),
                        "every backgroundLegs thread should have started its blocking dummy task");

                byte[] value = "original".getBytes(StandardCharsets.UTF_8);
                client.set("k".getBytes(StandardCharsets.UTF_8), value);
                // The real replica leg is now queued behind the poolSize
                // blocking tasks above — it cannot have read `value` yet.
                Arrays.fill(value, (byte) 'X'); // mutate the caller's array in place, as a buffer-reuse caller would

                release.countDown(); // let every dummy task finish, then the real leg runs

                String stored = MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8));
                waitFor(() -> cluster.nodes().get(replica).store.containsKey(stored),
                        "the background write to land on the replica");
                assertArrayEquals("original".getBytes(StandardCharsets.UTF_8),
                        cluster.nodes().get(replica).store.get(stored),
                        "the replica must store the original bytes, not the caller's later in-place mutation");
            }
        }
    }

    @Test
    void fireAndForgetReplicasFallsBackToSynchronousPastTheCap() throws Exception {
        NanocachedClient.maxInFlightBackgroundReplicaWrites = 2;

        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);
            cluster.nodes().get(replica).delaySets(150);

            try (NanocachedClient client = connectFireAndForget(cluster.discovery().port())) {
                long[] elapsedMillis = new long[3];
                Thread[] threads = new Thread[3];
                for (int i = 0; i < threads.length; i++) {
                    int index = i;
                    threads[i] = new Thread(() -> {
                        long start = System.nanoTime();
                        client.set("k", "v");
                        elapsedMillis[index] = (System.nanoTime() - start) / 1_000_000;
                    });
                    threads[i].start();
                }
                for (Thread thread : threads) thread.join();

                boolean anySlow = false;
                boolean anyFast = false;
                for (long ms : elapsedMillis) {
                    if (ms >= 150 - TIMING_TOLERANCE_MILLIS) anySlow = true;
                    else anyFast = true;
                }
                assertTrue(anySlow, "expected at least one call to fall back to synchronous past the cap");
                assertTrue(anyFast, "expected at least one call to return fast (below the cap)");
            }
        }
    }

    @Test
    void closeDrainsInFlightBackgroundReplicaWrites() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);
            cluster.nodes().get(replica).delaySets(80);

            NanocachedClient client = connectFireAndForget(cluster.discovery().port());
            client.set("k", "v");
            client.close(); // should block until the still-in-flight replica write lands

            String stored = MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8));
            assertTrue(cluster.nodes().get(replica).store.containsKey(stored),
                    "close() returned before the background replica write finished");
        }
    }

    @Test
    void closeWaitsForAnInFlightSynchronousReplicaLeg() throws Exception {
        // Issue #97: a synchronous (non-fire-and-forget) replica leg runs
        // on replicaWriters and is awaited by the calling thread. A close()
        // from ANOTHER thread used to wait only a fixed 5s for that pool,
        // then proceed into teardown() and close the connection the leg was
        // still reading from — returning while a leg it claims to have
        // drained was in flight. close() must instead wait for the leg
        // (bounded by the request timeout), so this exercises a leg whose
        // duration is well past the (here shortened) teardown margin the
        // pre-fix code bounded on.
        long defaultTimeout = Connection.requestTimeoutMillis;
        long defaultTeardown = NanocachedClient.executorTerminationTimeoutMillis;
        Connection.requestTimeoutMillis = 2000;
        // Shrink the thread-teardown margin so the pre-fix fixed bound
        // (== this margin) is well under the leg's 800ms; the fix's bound is
        // requestTimeout + this margin, comfortably above it.
        NanocachedClient.executorTerminationTimeoutMillis = 200;
        try (Cluster cluster = startCluster(2)) {
            String replica = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(1);
            cluster.nodes().get(replica).delaySets(800);

            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                Thread writer = new Thread(() -> client.set("k", "v"));
                writer.start();
                // Let the synchronous leg get in flight on replicaWriters
                // before close() runs, without waiting for it to finish.
                Thread.sleep(100);

                long start = System.nanoTime();
                client.close();
                long closeMillis = (System.nanoTime() - start) / 1_000_000;
                writer.join();

                // close() waited out the remaining ~700ms of the leg, not
                // just the 200ms teardown margin the pre-fix code bounded on.
                assertTrue(closeMillis >= 500,
                        "close() returned in " + closeMillis + "ms — before the in-flight "
                                + "synchronous replica leg finished");
            }
        } finally {
            Connection.requestTimeoutMillis = defaultTimeout;
            NanocachedClient.executorTerminationTimeoutMillis = defaultTeardown;
        }
    }

    @Test
    void closeRacingConcurrentWritesNeverThrowsARawExecutorException() throws Exception {
        // Issue: audit finding — a set() straggling past close() (which
        // has already shut down replicaWriters) must never let a raw
        // RejectedExecutionException escape from the replica-write
        // submission, whether it went through the fire-and-forget path or
        // the synchronous-fallback one: every failure this SDK throws
        // extends NanocachedException. A tight burst of concurrent
        // writers overlapping a concurrent close() reliably lands at
        // least one call in the race window this fix closes.
        try (Cluster cluster = startCluster(2)) {
            NanocachedClient client = connectFireAndForget(cluster.discovery().port());

            int threadCount = 8;
            Thread[] threads = new Thread[threadCount];
            List<Throwable> unexpected = java.util.Collections.synchronizedList(new ArrayList<>());
            java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            for (int i = 0; i < threadCount; i++) {
                int index = i;
                threads[i] = new Thread(() -> {
                    while (!stop.get()) {
                        try {
                            client.set("k" + index, "v");
                        } catch (NanocachedException expected) {
                            // AlreadyClosed/ConnectionFailed once close() wins — fine.
                        } catch (Throwable other) {
                            unexpected.add(other);
                            return;
                        }
                    }
                }, "test-racing-writer-" + i);
                threads[i].start();
            }

            Thread.sleep(20); // let the writers get going before close() lands among them
            client.close();
            stop.set(true);
            for (Thread thread : threads) thread.join(5_000);

            assertTrue(unexpected.isEmpty(), "unexpected exception(s): " + unexpected);
        }
    }

    @Test
    void multiGetFallsBackToInlineExecutionWhenReplicaWritersIsShutDownWithoutClosingTheClient() throws Exception {
        // Regression for issue #277: multiGetPass used to submit each
        // owner-group leg straight to replicaWriters via a raw
        // CompletableFuture.supplyAsync, bypassing submitReplicaWrite —
        // the helper every other replicaWriters call site relies on to
        // catch RejectedExecutionException and fall back to running the
        // leg inline. close()'s own shutdown of replicaWriters is only
        // ever raced probabilistically (see
        // closeRacingConcurrentWritesNeverThrowsARawExecutorException's
        // set() equivalent above), so this reproduces the exact race
        // deterministically instead: shut replicaWriters down directly,
        // without going through close() (so beforeOperation()'s `closed`
        // check still passes and getMany() actually reaches
        // multiGetPass). Before the fix, this raised a raw
        // RejectedExecutionException past the public API; the fix must
        // instead run the leg synchronously on this thread and return the
        // normal result.
        try (Cluster cluster = startCluster(2);
                NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
            client.set("k", "v");

            Field replicaWritersField = NanocachedClient.class.getDeclaredField("replicaWriters");
            replicaWritersField.setAccessible(true);
            ExecutorService replicaWriters = (ExecutorService) replicaWritersField.get(client);
            replicaWriters.shutdown();
            assertTrue(replicaWriters.isShutdown());

            // No try/catch here on purpose: any exception at all — not
            // just a RejectedExecutionException specifically — fails this
            // test, since the fix's whole point is that getMany() returns
            // normally instead of throwing anything.
            Map<String, String> values = client.getMany(List.of("k"));
            assertEquals(Map.of("k", "v"), values);
        }
    }

    @Test
    void closeRacingConcurrentReadRepairNeverThrowsARawExecutorException() throws Exception {
        // Same race as above, for read repair's background write-back
        // (~line 600's replicaWriters.execute before this fix) — every
        // get() below finds the value only on the replica, so every call
        // triggers a repair write-back racing close().
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            cluster.nodes().get(owners.get(1)).store.put(
                    MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8)),
                    "from-replica".getBytes(StandardCharsets.UTF_8));

            NanocachedClient client = connectWithReadRepair(cluster.discovery().port());

            int threadCount = 8;
            Thread[] threads = new Thread[threadCount];
            List<Throwable> unexpected = java.util.Collections.synchronizedList(new ArrayList<>());
            java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    while (!stop.get()) {
                        try {
                            client.getBytes("k");
                        } catch (NanocachedException expected) {
                            // AlreadyClosed/ConnectionFailed once close() wins — fine.
                        } catch (Throwable other) {
                            unexpected.add(other);
                            return;
                        }
                    }
                }, "test-racing-reader-" + i);
                threads[i].start();
            }

            Thread.sleep(20);
            client.close();
            stop.set(true);
            for (Thread thread : threads) thread.join(5_000);

            assertTrue(unexpected.isEmpty(), "unexpected exception(s): " + unexpected);
        }
    }

    @Test
    void connectFailureSurfacesTheRealFailureInsteadOfHanging() throws Exception {
        // Part one of the teardown() regression below: force
        // startKeepAlive() itself to throw (an invalid <= 0 period is
        // rejected by scheduleAtFixedRate) after openCluster() has
        // already succeeded and built replicaWriters/
        // backgroundReplicaWritePermits — connect() must still surface
        // that failure (not hang, not swallow it).
        try (Cluster cluster = startCluster(1)) {
            long defaultInterval = NanocachedClient.keepAliveIntervalMillis;
            NanocachedClient.keepAliveIntervalMillis = 0;
            try {
                assertThrows(IllegalArgumentException.class,
                        () -> connect("127.0.0.1", cluster.discovery().port()));
            } finally {
                NanocachedClient.keepAliveIntervalMillis = defaultInterval;
            }
        }
    }

    @Test
    void teardownAlsoShutsDownReplicaWritersAndKeepAlive() throws Exception {
        // Part two: connect()'s catch blocks have no way to hand back a
        // half-built instance once they've thrown (see the previous
        // test), so this exercises teardown() itself — the exact method
        // those catch blocks call — directly on a normally-connected
        // client. Regression (issue: audit finding): teardown() used to
        // close only the connections, leaking replicaWriters (its daemon
        // threads survive until the executor is GC'd) and its
        // backgroundReplicaWritePermits whenever something after
        // openCluster() failed, e.g. the startKeepAlive() failure above.
        try (Cluster cluster = startCluster(1)) {
            NanocachedClient client = connect("127.0.0.1", cluster.discovery().port());
            try {
                java.lang.reflect.Field replicaWritersField =
                        NanocachedClient.class.getDeclaredField("replicaWriters");
                replicaWritersField.setAccessible(true);
                ExecutorService replicaWriters = (ExecutorService) replicaWritersField.get(client);
                assertFalse(replicaWriters.isShutdown());

                java.lang.reflect.Field keepAliveField = NanocachedClient.class.getDeclaredField("keepAlive");
                keepAliveField.setAccessible(true);
                ExecutorService keepAlive = (ExecutorService) keepAliveField.get(client);
                assertFalse(keepAlive.isShutdown());

                java.lang.reflect.Method teardown = NanocachedClient.class.getDeclaredMethod("teardown");
                teardown.setAccessible(true);
                teardown.invoke(client);

                assertTrue(replicaWriters.isShutdown(),
                        "teardown() must shut down replicaWriters too, not just the connections");
                assertTrue(keepAlive.isShutdown(),
                        "teardown() must shut down keepAlive too, not just the connections");
            } finally {
                client.close();
            }
        }
    }

    // ── read repair (read repair) ────────────────────────────

    private static NanocachedClient connectWithReadRepair(int port) {
        return NanocachedClient.connect(NanocachedClient.builder()
                .addresses(List.of(new Address("127.0.0.1", port)))
                .readRepair(true));
    }

    @Test
    void byDefaultACleanMissOnThePrimaryIsNotRepaired() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            cluster.nodes().get(owners.get(1)).store.put(
                    MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8)),
                    "from-replica".getBytes(StandardCharsets.UTF_8));

            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                assertEquals(Optional.empty(), client.getBytes("k"));
                assertFalse(cluster.nodes().get(owners.get(0)).store
                        .containsKey(MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8))));
            }
        }
    }

    @Test
    void findsAValueOnAReplicaAndRepairsThePrimary() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            cluster.nodes().get(owners.get(1)).store.put(
                    MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8)),
                    "from-replica".getBytes(StandardCharsets.UTF_8));

            try (NanocachedClient client = connectWithReadRepair(cluster.discovery().port())) {
                assertArrayEquals("from-replica".getBytes(StandardCharsets.UTF_8),
                        client.getBytes("k").orElseThrow());

                String stored = MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8));
                waitFor(() -> cluster.nodes().get(owners.get(0)).store.containsKey(stored),
                        "the primary to be repaired");
                // The original TTL can't be recovered from a GET; a
                // repair must not use TTL 0 (no expiry), which would
                // permanently resurrect already-expired data — see
                // READ_REPAIR_TTL_SECONDS.
                assertEquals(60, cluster.nodes().get(owners.get(0)).lastSetTtl);
                // Regression (issue: audit finding): tryReadRepair used to
                // re-probe the primary too — the one owner already known
                // to have missed by the normal read path — wasting a
                // redundant GET on it. The primary must be probed exactly
                // once: by getBytes()'s own read(), never again by
                // tryReadRepair.
                assertEquals(1, cluster.nodes().get(owners.get(0)).getCount.get(),
                        "read repair must not re-probe the primary that just missed");
            }
        }
    }

    @Test
    void staysACleanMissWhenNoOwnerHasTheValue() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connectWithReadRepair(cluster.discovery().port())) {
                assertEquals(Optional.empty(), client.getBytes("nowhere"));
            }
        }
    }

    // ── hedged reads (issue #64) ─────────────────────────────────────

    // A "did it wait/not wait" assertion can't compare measured elapsed
    // time against a delay exactly (Thread.sleep()'s wakeup is only
    // approximate); generous so CI (ubuntu) is never flaky.
    private static final long HEDGE_TIMING_TOLERANCE_MILLIS = 30;

    private static NanocachedClient connectWithReadHedgeAfter(int port, long hedgeAfterMillis) {
        return NanocachedClient.connect(NanocachedClient.builder()
                .addresses(List.of(new Address("127.0.0.1", port)))
                .readHedgeAfter(Duration.ofMillis(hedgeAfterMillis)));
    }

    @Test
    void readHedgeAfterRejectsANonPositiveDuration() {
        assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.builder().readHedgeAfter(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> NanocachedClient.builder().readHedgeAfter(Duration.ofMillis(-100)));
    }

    @Test
    void aHitFromTheReplicaWinsOverASlowPrimary() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            String primary = owners.get(0);
            String replica = owners.get(1);

            try (NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 50)) {
                client.set("k", "v");
                cluster.nodes().get(primary).delayGets(400);

                long start = System.nanoTime();
                Optional<String> value = client.get("k");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

                assertEquals(Optional.of("v"), value);
                assertTrue(elapsedMillis < 400 - HEDGE_TIMING_TOLERANCE_MILLIS,
                        "expected the replica's fast answer to win, took " + elapsedMillis + "ms");
                assertTrue(elapsedMillis >= 50 - HEDGE_TIMING_TOLERANCE_MILLIS,
                        "expected to wait out the hedge interval first, took " + elapsedMillis + "ms");
                assertEquals(1, cluster.nodes().get(replica).getCount.get(),
                        "the replica should have been hedged to");

                // The slow primary's leg was left to finish, not
                // cancelled, and close() (the try-with-resources below)
                // blocks until it has.
            }
            assertEquals(1, cluster.nodes().get(primary).getCount.get(),
                    "close() should have drained the slow primary's hedge leg");
        }
    }

    @Test
    void aFastPrimaryIsNeverHedged() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            String replica = owners.get(1);

            try (NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 50)) {
                client.set("k", "v");
                for (int i = 0; i < 5; i++) {
                    assertEquals(Optional.of("v"), client.get("k"));
                }
                assertEquals(0, cluster.nodes().get(replica).getCount.get());
            }
        }
    }

    @Test
    void aReplicaMissWaitsForThePrimary() throws Exception {
        // Hedging must never turn a hit into a miss: the replica lacks the
        // copy and answers first, but the primary's answer is what counts.
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            String primary = owners.get(0);
            String replica = owners.get(1);

            try (NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 50)) {
                client.set("k", "v");
                cluster.nodes().get(replica).store.remove(MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8)));
                cluster.nodes().get(primary).delayGets(200);

                long start = System.nanoTime();
                Optional<String> value = client.get("k");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

                assertEquals(Optional.of("v"), value);
                assertTrue(elapsedMillis >= 200 - HEDGE_TIMING_TOLERANCE_MILLIS,
                        "expected to wait for the primary, took " + elapsedMillis + "ms");
                assertEquals(1, cluster.nodes().get(replica).getCount.get());

                // A key nobody has: the miss is accepted once the primary
                // has answered it too.
                assertEquals(Optional.empty(), client.get("absent"));
            }
        }
    }

    @Test
    void offByDefaultASlowPrimaryBoundsTheRead() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            String primary = owners.get(0);
            String replica = owners.get(1);

            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                client.set("k", "v");
                cluster.nodes().get(primary).delayGets(200);

                long start = System.nanoTime();
                Optional<String> value = client.get("k");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

                assertEquals(Optional.of("v"), value);
                assertTrue(elapsedMillis >= 200 - HEDGE_TIMING_TOLERANCE_MILLIS,
                        "expected the slow primary to bound the read, took " + elapsedMillis + "ms");
                assertEquals(0, cluster.nodes().get(replica).getCount.get());
            }
        }
    }

    @Test
    void aDeadPrimaryFailsOverImmediately() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            String primary = owners.get(0);

            NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 500);
            try {
                client.set("k", "v");
                cluster.nodes().get(primary).close();
                Thread.sleep(50); // give the FIN a moment to be observable

                long start = System.nanoTime();
                Optional<String> value = client.get("k");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

                assertEquals(Optional.of("v"), value);
                assertTrue(elapsedMillis < 500 - HEDGE_TIMING_TOLERANCE_MILLIS,
                        "expected an immediate failover, took " + elapsedMillis + "ms");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void aHedgeLegRacingCloseIsRefusedNotRegistered() throws Exception {
        // Issue #91: a read that passed its own closed-check can reach
        // hedge-leg registration only after close() set `closed` and drained
        // hedgedReads. startHedgeLeg must recheck `closed` under
        // hedgedReadsLock so it never registers — and dials against a
        // connection teardown is closing — a leg the drain already passed.
        // Setting `closed` directly (reflection) reproduces exactly the
        // state startHedgeLeg sees at that point; readHedged/ConnectionOp are
        // private, so the whole path is driven reflectively.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 50)) {
                client.set("k", "v");

                Field closedField = NanocachedClient.class.getDeclaredField("closed");
                closedField.setAccessible(true);
                Field hedgedReadsField = NanocachedClient.class.getDeclaredField("hedgedReads");
                hedgedReadsField.setAccessible(true);

                Class<?> opInterface = Class.forName("org.nanocached.NanocachedClient$ConnectionOp");
                Object op = Proxy.newProxyInstance(
                        opInterface.getClassLoader(), new Class<?>[] {opInterface},
                        (proxy, method, args) -> {
                            throw new AssertionError("the leg must never be dialed after close() began");
                        });
                Method readHedged =
                        NanocachedClient.class.getDeclaredMethod("readHedged", opInterface, List.class);
                readHedged.setAccessible(true);

                closedField.setBoolean(client, true);
                try {
                    InvocationTargetException thrown = assertThrows(
                            InvocationTargetException.class,
                            () -> readHedged.invoke(client, op, List.of("a", "b")));
                    assertTrue(thrown.getCause() instanceof NanocachedException.AlreadyClosed,
                            "expected AlreadyClosed, got " + thrown.getCause());
                    Set<?> hedgedReads = (Set<?>) hedgedReadsField.get(client);
                    assertTrue(hedgedReads.isEmpty(),
                            "no hedge leg may be registered after close() began");
                } finally {
                    // Restore so close() runs its real teardown.
                    closedField.setBoolean(client, false);
                }
            }
        }
    }

    @org.junit.jupiter.api.AfterEach
    void resetMaxInFlightHedgeLoserLegs() {
        NanocachedClient.maxInFlightHedgeLoserLegs = 32;
    }

    @Test
    void hedgeLosersFallBackToSynchronousPastTheCap() throws Exception {
        // Issue #276: with no room under maxInFlightHedgeLoserLegs, the
        // slow primary's losing leg is joined right here instead of being
        // left detached in hedgedReads for close() to drain later —
        // mirroring fireAndForgetReplicasFallsBackToSynchronousPastTheCap
        // for background replica writes.
        NanocachedClient.maxInFlightHedgeLoserLegs = 0;

        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            String primary = owners.get(0);
            String replica = owners.get(1);

            try (NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 50)) {
                client.set("k", "v");
                cluster.nodes().get(primary).delayGets(300);

                long start = System.nanoTime();
                Optional<String> value = client.get("k");
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

                assertEquals(Optional.of("v"), value);
                assertEquals(1, cluster.nodes().get(replica).getCount.get(),
                        "the replica should have been hedged to");
                assertTrue(elapsedMillis >= 300 - HEDGE_TIMING_TOLERANCE_MILLIS,
                        "expected get() to wait for the slow primary's loser leg past the cap, took "
                                + elapsedMillis + "ms");
                assertEquals(1, cluster.nodes().get(primary).getCount.get());

                Field hedgedReadsField = NanocachedClient.class.getDeclaredField("hedgedReads");
                hedgedReadsField.setAccessible(true);
                Set<?> hedgedReads = (Set<?>) hedgedReadsField.get(client);
                assertTrue(hedgedReads.isEmpty(),
                        "the awaited loser must already be gone, not left for close() to drain");
            }
        }
    }

    // ── stats() — counters for by-design swallows ──────────────────

    @Test
    void aDeadReplicaIncrementsReplicaWriteFailuresStat() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "counted-replica-failure";
                List<String> owners =
                        new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                cluster.nodes().get(owners.get(1)).close();
                Thread.sleep(50);

                assertEquals(0, client.stats().replicaWriteFailures());
                client.set(key, "v");
                assertEquals(1, client.stats().replicaWriteFailures());
            }
        }
    }

    @Test
    void anUnreachableAddressDuringRefreshIncrementsRefreshFailuresStat() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            int dead = MockServers.unusedPort();
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(
                            new Address("127.0.0.1", dead),
                            new Address("127.0.0.1", cluster.discovery().port()))))) {
                assertEquals(0, client.stats().refreshFailures());

                // Force a refresh directly rather than waiting out
                // NODE_LIST_STALE_AFTER — the dead first address is still
                // walked (and still fails) on every refresh, exactly as it
                // was on connect().
                java.lang.reflect.Method refreshNodeList =
                        NanocachedClient.class.getDeclaredMethod("refreshNodeList");
                refreshNodeList.setAccessible(true);
                refreshNodeList.invoke(client);

                assertTrue(client.stats().refreshFailures() >= 1);
            }
        }
    }

    @Test
    void aSlowNewNodeDuringRefreshDoesNotStallOperations() throws Exception {
        // Regression: refreshNodeList used to dial newly listed nodes
        // while holding stateLock — the same lock every get/set/delete
        // needs for routing — so one unresponsive new node stalled all
        // traffic for the whole dial. Dials now happen outside the lock.
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                client.set("k", "v");

                // A "new" node that accepts the TCP connection but never
                // answers identify: the refresh's dial to it blocks until
                // the socket is closed below (or the issue-#40 identify
                // read timeout fires, whichever comes first).
                try (java.net.ServerSocket silent = new java.net.ServerSocket(0)) {
                    // The dial may connect more than once (the echoed response tags
                    // legacy fallback redials after the first connection
                    // dies), so accept in a loop and track every socket.
                    List<java.net.Socket> acceptedSockets =
                            java.util.Collections.synchronizedList(new ArrayList<>());
                    // Issue #130: once the test starts tearing down, a
                    // connection accept() hands back *after* the close loop
                    // below ran would otherwise stay open until the SDK's
                    // own identify read timeout — so from then on the
                    // acceptor closes whatever it accepts itself.
                    java.util.concurrent.atomic.AtomicBoolean tearingDown =
                            new java.util.concurrent.atomic.AtomicBoolean(false);
                    Thread acceptor = new Thread(() -> {
                        try {
                            while (true) {
                                java.net.Socket accepted = silent.accept();
                                if (tearingDown.get()) {
                                    accepted.close();
                                    continue;
                                }
                                acceptedSockets.add(accepted);
                            }
                        } catch (java.io.IOException ignored) {
                            // Server socket closed.
                        }
                    }, "test-silent-accept");
                    acceptor.setDaemon(true);
                    acceptor.start();

                    List<DiscoveredNode> extended = new ArrayList<>(cluster.discovery().nodes);
                    extended.add(new DiscoveredNode(
                            "11111111-2222-3333-4444-555555555555",
                            "127.0.0.1:" + silent.getLocalPort()));
                    cluster.discovery().nodes = extended;

                    java.lang.reflect.Method refreshNodeList =
                            NanocachedClient.class.getDeclaredMethod("refreshNodeList");
                    refreshNodeList.setAccessible(true);
                    Thread refresher = new Thread(() -> {
                        try {
                            refreshNodeList.invoke(client);
                        } catch (ReflectiveOperationException ignored) {
                            // The dial's IOException is swallowed by
                            // refreshNodeList itself; anything else fails
                            // the elapsed-time assertion below anyway.
                        }
                    }, "test-refresher");
                    refresher.setDaemon(true);
                    refresher.start();

                    // Only once the refresher is provably parked in the
                    // dial does the timing assertion mean anything.
                    waitFor(() -> !acceptedSockets.isEmpty(), "the refresh to reach the silent node");

                    long started = System.nanoTime();
                    assertEquals(Optional.of("v"), client.get("k"));
                    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                    assertTrue(elapsedMillis < 2_000,
                            "get() stalled " + elapsedMillis + "ms behind the refresh's dial");

                    // Unblock the refresher so it finishes inside this
                    // test: stop further redials first, then kill the
                    // in-flight connections.
                    tearingDown.set(true);
                    silent.close();
                    synchronized (acceptedSockets) {
                        for (java.net.Socket socket : acceptedSockets) socket.close();
                    }
                    // Issue #130 (reproduced 1-in-25 on Linux): this join
                    // used to be 5s — exactly Identify's CONNECT_TIMEOUT_MS
                    // read timeout — so a connection the close loop missed
                    // left the refresher alive at the very moment the
                    // assertion ran. The budget now comfortably covers one
                    // full identify timeout, and the acceptor above closes
                    // late arrivals so that path is not normally taken.
                    refresher.join(20_000);
                    assertFalse(refresher.isAlive(), "refresher did not finish");
                }
            }
        }
    }

    @Test
    void aFailedRepairWriteIncrementsReadRepairFailuresStat() throws Exception {
        try (Cluster cluster = startCluster(2)) {
            List<String> owners = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2);
            MockNode primary = cluster.nodes().get(owners.get(0));
            cluster.nodes().get(owners.get(1)).store.put(
                    MockNode.keyOf("k".getBytes(StandardCharsets.UTF_8)),
                    "from-replica".getBytes(StandardCharsets.UTF_8));
            // The primary stays up and misses the initial G (its store is
            // empty), so read-repair triggers and aims its write back at
            // the primary — but every S there is dropped (connection reset)
            // rather than acked, so the repair deterministically fails and
            // is counted. Deterministic in place of the previous
            // delaySets()+close() race, which was timing-flaky on loaded
            // CI runners.
            primary.failSets();

            try (NanocachedClient client = connectWithReadRepair(cluster.discovery().port())) {
                assertArrayEquals("from-replica".getBytes(StandardCharsets.UTF_8),
                        client.getBytes("k").orElseThrow());

                waitFor(() -> client.stats().readRepairFailures() >= 1,
                        "the repair write to the primary to fail and be counted");
            }
        }
    }

    // ── write() の結果優先順位 (issue: audit finding, finally-join) ──

    // ConnectionOp is private, so write() is driven reflectively with a
    // dynamic-proxy op — the same technique the pre-fix version of this
    // suite used, kept and shared across the three tests below since
    // they only differ in what the proxy does on each thread. The
    // primary leg always runs on the caller's own thread; a replica leg
    // (this cluster's replication is 2, so there is exactly one) runs on
    // "nanocached-replica-writer" when it takes the synchronous-fallback
    // path (fireAndForgetReplicas is off in every test below).

    private static Class<?> connectionOpClass() throws ClassNotFoundException {
        return Class.forName("org.nanocached.NanocachedClient$ConnectionOp");
    }

    private static java.lang.reflect.Method writeMethod() throws Exception {
        // Namespaces (issue #105): write() gained a leading namespace
        // parameter — EMPTY_NAMESPACE (via the empty byte[] literal below)
        // exercises exactly the un-namespaced path these tests target.
        java.lang.reflect.Method write = NanocachedClient.class.getDeclaredMethod(
                "write", byte[].class, byte[].class, connectionOpClass());
        write.setAccessible(true);
        return write;
    }

    private static boolean onReplicaWriterThread() {
        return Thread.currentThread().getName().equals("nanocached-replica-writer");
    }

    @Test
    void aReplicaLegBugDoesNotFailAWriteWhenThePrimarySucceeds() throws Exception {
        // Regression: write()'s old `finally { pending.join(); }` let a
        // replica leg's uncaught bug replace an already-successful
        // primary result — turning a completed write into a thrown
        // CompletionException. A genuine bug on a replica leg must never
        // fail a write whose primary already succeeded; it's recorded
        // instead (see aReplicaLegBugPropagatesWhenThePrimaryAlsoFails
        // below for when it does propagate). Mirrors the TypeScript SDK's
        // writeToOwners / the Python SDK's _write().
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                byte[] key = "primary-ok-replica-bug".getBytes(StandardCharsets.UTF_8);
                byte[] value = "v".getBytes(StandardCharsets.UTF_8);
                Object op = java.lang.reflect.Proxy.newProxyInstance(
                        connectionOpClass().getClassLoader(),
                        new Class<?>[] {connectionOpClass()},
                        (proxy, method, methodArgs) -> {
                            if (onReplicaWriterThread()) {
                                throw new ClassCastException("injected programming bug");
                            }
                            Connection connection = (Connection) methodArgs[0];
                            connection.set(key, value, null);
                            return null;
                        });

                long before = client.stats().backgroundWriteBugs();
                writeMethod().invoke(client, new byte[0], key, op); // must not throw
                assertEquals(before + 1, client.stats().backgroundWriteBugs(),
                        "the replica leg's bug must still be recorded even though the write succeeded");
            }
        }
    }

    @Test
    void aReplicaLegBugPropagatesWhenThePrimaryAlsoFails() throws Exception {
        // The other half of the same fix: when the primary ALSO fails, a
        // genuine replica-leg bug takes precedence over the primary's own
        // (expected) failure and propagates raw — as the bug itself
        // (RuntimeException), never wrapped in a CompletionException, the
        // one thing every exception this SDK throws must not be.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                byte[] key = "primary-fails-replica-bug".getBytes(StandardCharsets.UTF_8);
                Object op = java.lang.reflect.Proxy.newProxyInstance(
                        connectionOpClass().getClassLoader(),
                        new Class<?>[] {connectionOpClass()},
                        (proxy, method, methodArgs) -> {
                            if (onReplicaWriterThread()) {
                                throw new ClassCastException("injected programming bug");
                            }
                            throw new NanocachedException.ConnectionFailed(
                                    "nanocached: simulated primary failure", null);
                        });

                java.lang.reflect.InvocationTargetException thrown = assertThrows(
                        java.lang.reflect.InvocationTargetException.class,
                        () -> writeMethod().invoke(client, new byte[0], key, op));
                assertTrue(thrown.getCause() instanceof ClassCastException,
                        "expected the raw replica-leg bug to propagate directly, got: " + thrown.getCause());
            }
        }
    }

    @Test
    void primaryErrorPropagatesWhenTheReplicaIsJustDeadNotBuggy() throws Exception {
        // The primary's own error is still what propagates when nothing
        // about the replica leg is a bug — a dead replica is an expected
        // failure, swallowed inside the replica leg itself (counted via
        // replicaWriteFailures) and never reaches the join-loop's bug
        // handling at all.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "primary-fails-replica-dead";
                List<String> owners = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2);
                cluster.nodes().get(owners.get(0)).close();
                cluster.nodes().get(owners.get(1)).close();
                Thread.sleep(50);

                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.set(key, "v"));
            }
        }
    }

    // ── 寛容なブートストラップ (issue #67) ──────────────────────────
    // connect() must tolerate a node that discovery still lists but that
    // can't be reached (dead, not yet evicted) the way steady-state
    // requests already do, failing only when no listed node is reachable
    // at all. Mirrors the Python SDK's TolerantBootstrapTests.

    private record TolerantCluster(Map<String, MockNode> liveNodes, MockDiscovery discovery)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            discovery.close();
            for (MockNode node : liveNodes.values()) node.close();
        }
    }

    /** Starts a 2-node (replication 2) discovery-fronted cluster where
     * every name in {@code deadNames} is listed with an address nobody
     * listens on, and every other name gets a real {@link MockNode}. */
    private static TolerantCluster startClusterWithDeadNodes(List<String> deadNames) throws Exception {
        // unusedPort() frees the port the instant it returns it, and on
        // Linux that port can be handed straight back out to the very
        // next bind — so it must be the last socket operation here,
        // after every real listener (MockNode, MockDiscovery) is already
        // bound. Doing it earlier let the discovery listener land on a
        // "dead" port, turning an intentionally-unreachable node into a
        // live non-node one (the Java twin of #527).
        Map<String, MockNode> liveNodes = new java.util.LinkedHashMap<>();
        for (String name : NAMES) {
            if (!deadNames.contains(name)) {
                liveNodes.put(name, new MockNode());
            }
        }
        MockDiscovery discovery = new MockDiscovery(List.of(), 2);
        List<DiscoveredNode> entries = new ArrayList<>();
        for (String name : NAMES) {
            MockNode node = liveNodes.get(name);
            String address = node == null ? "127.0.0.1:" + MockServers.unusedPort() : node.address();
            entries.add(new DiscoveredNode(name, address));
        }
        discovery.nodes = entries;
        return new TolerantCluster(liveNodes, discovery);
    }

    private static String keyWithPrimary(String name) {
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            if (new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 2).get(0).equals(name)) {
                return key;
            }
        }
        throw new AssertionError("no key routes to " + name);
    }

    // NanocachedClient.Member is private, so its `connection`/`address`
    // fields are reached reflectively — the same technique already used
    // elsewhere in this file (e.g. newTrackedConnectionUntracksAndClosesSocketWhenConstructorFails).
    private static Object memberOf(NanocachedClient client, String name) throws Exception {
        java.lang.reflect.Field membersField = NanocachedClient.class.getDeclaredField("members");
        membersField.setAccessible(true);
        Map<?, ?> members = (Map<?, ?>) membersField.get(client);
        Object member = members.get(name);
        assertTrue(member != null, name + " is missing from the member map");
        return member;
    }

    private static Connection memberConnectionOf(NanocachedClient client, String name) throws Exception {
        java.lang.reflect.Field connectionField = memberOf(client, name).getClass().getDeclaredField("connection");
        connectionField.setAccessible(true);
        return (Connection) connectionField.get(memberOf(client, name));
    }

    private static String memberAddressOf(NanocachedClient client, String name) throws Exception {
        Object member = memberOf(client, name);
        java.lang.reflect.Field addressField = member.getClass().getDeclaredField("address");
        addressField.setAccessible(true);
        return (String) addressField.get(member);
    }

    @Test
    void connectSucceedsWithOneUnreachableNode() throws Exception {
        String dead = NAMES.get(0);
        String live = NAMES.get(1);
        try (TolerantCluster cluster = startClusterWithDeadNodes(List.of(dead))) {
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(new Address("127.0.0.1", cluster.discovery().port())))
                    // Wide on purpose (see reconnectCooldownSkipsARedialToAKnownDeadAddress):
                    // this test's writes/reads must land well inside the
                    // window the bootstrap dial failure armed, not race it.
                    .reconnectCooldown(Duration.ofSeconds(5)))) {
                assertEquals(2, client.replication());
                assertTrue(memberConnectionOf(client, dead) == null,
                        "a node unreachable at bootstrap must have no connection");
                assertTrue(memberConnectionOf(client, live) != null);

                // A key whose primary is alive: the write lands, the dead
                // replica leg is swallowed and counted, the read hits.
                String key = keyWithPrimary(live);
                client.set(key, "v");
                assertEquals(Optional.of("v"), client.get(key));
                assertEquals(1, client.stats().replicaWriteFailures());

                // A key whose primary is the dead node: the read fails
                // over to the live replica right away — the cooldown
                // skips the dial entirely, so this must be fast, not
                // bounded by Identify's connect timeout.
                String other = keyWithPrimary(dead);
                cluster.liveNodes().get(live).store.put(
                        MockNode.keyOf(other.getBytes(StandardCharsets.UTF_8)),
                        "replica copy".getBytes(StandardCharsets.UTF_8));
                long started = System.nanoTime();
                assertEquals(Optional.of("replica copy"), client.get(other));
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                assertTrue(elapsedMillis < 2000,
                        "expected a cooldown-fast failover, took " + elapsedMillis + "ms");
            }
        }
    }

    @Test
    void connectFailsOnlyWhenEveryNodeIsUnreachable() throws Exception {
        try (TolerantCluster cluster = startClusterWithDeadNodes(NAMES)) {
            assertThrows(NanocachedException.ConnectionFailed.class,
                    () -> connect("127.0.0.1", cluster.discovery().port()));
        }
    }

    @Test
    void anUnreachableNodeIsRedialedOnceTheCooldownHasPassed() throws Exception {
        String dead = NAMES.get(0);
        try (TolerantCluster cluster = startClusterWithDeadNodes(List.of(dead))) {
            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(new Address("127.0.0.1", cluster.discovery().port())))
                    .reconnectCooldown(Duration.ofMillis(300)))) {
                String deadAddress = memberAddressOf(client, dead);
                int port = Integer.parseInt(deadAddress.substring(deadAddress.lastIndexOf(':') + 1));

                // Bring the "dead" node up on the exact address discovery
                // already listed, then outlast the cooldown.
                try (MockNode revived = MockNode.onPort(port)) {
                    Thread.sleep(500);

                    String key = keyWithPrimary(dead);
                    client.set(key, "v");
                    assertTrue(revived.store.containsKey(MockNode.keyOf(key.getBytes(StandardCharsets.UTF_8))));
                    assertTrue(memberConnectionOf(client, dead) != null);
                }
            }
        }
    }

    // A node over --max-connections answers a bare `B\n` and closes: two
    // bytes, not the three-byte identity. Seen for what it is on the
    // first probe — not retried with the lesser `A` forms the way a
    // close-before-reply door-slam is.
    @Test
    void connectReportsANodeAtItsConnectionLimitAsBusyOnTheFirstDial() throws Exception {
        java.util.concurrent.atomic.AtomicInteger dials = new java.util.concurrent.atomic.AtomicInteger();
        try (java.net.ServerSocket server = new java.net.ServerSocket(0)) {
            Thread accepting = new Thread(() -> {
                while (true) {
                    try (java.net.Socket accepted = server.accept()) {
                        dials.incrementAndGet();
                        accepted.getOutputStream().write("B\n".getBytes(StandardCharsets.US_ASCII));
                        accepted.getOutputStream().flush();
                        // Wait (bounded) for the client to hang up before closing: a close
                        // with the client's identify frame still unread is a reset on Linux,
                        // and a reset discards the B it is queued behind.
                        accepted.setSoTimeout(2000);
                        byte[] sink = new byte[256];
                        try {
                            while (accepted.getInputStream().read(sink) >= 0) {
                            }
                        } catch (java.net.SocketTimeoutException ignored) {
                        }
                    } catch (java.io.IOException closed) {
                        return;
                    }
                }
            }, "busy-node");
            accepting.setDaemon(true);
            accepting.start();

            NanocachedException.ConnectionFailed error = assertThrows(
                    NanocachedException.ConnectionFailed.class,
                    () -> connect("127.0.0.1", server.getLocalPort()));
            assertTrue(error.getMessage().contains("connection limit"), error.getMessage());
            assertEquals(1, dials.get(), "a busy reply is not a legacy door-slam to retry");
        }
    }

    // One request-timeout watchdog thread for the whole process, however
    // many connections hold an armed deadline — each connection's own
    // thread is its reader alone (two per connection used to make a
    // 10,000-node roster cost 20,000 threads).
    @Test
    void connectionsShareOneRequestWatchdogThread() throws Exception {
        int count = 16;
        List<Connection> connections = new ArrayList<>();
        List<java.net.Socket> serverSides = new ArrayList<>();
        ExecutorService callers = Executors.newFixedThreadPool(count);
        try (java.net.ServerSocket server = new java.net.ServerSocket(0)) {
            for (int i = 0; i < count; i++) {
                java.net.Socket clientSocket = new java.net.Socket("127.0.0.1", server.getLocalPort());
                serverSides.add(server.accept());
                connections.add(new Connection(clientSocket, false, () -> {}));
            }
            // A request in flight on every connection: every deadline is
            // armed, so the watchdog is at its busiest.
            List<Future<byte[]>> inFlight = new ArrayList<>();
            for (Connection connection : connections) {
                inFlight.add(callers.submit(() -> connection.get("k".getBytes(StandardCharsets.UTF_8))));
            }
            for (java.net.Socket serverSide : serverSides) {
                byte[] frame = "G 1\nk".getBytes(StandardCharsets.US_ASCII);
                assertArrayEquals(frame, serverSide.getInputStream().readNBytes(frame.length));
            }

            java.util.Map<String, Long> threads = Thread.getAllStackTraces().keySet().stream()
                    .map(Thread::getName)
                    .filter(name -> name.startsWith("nanocached-"))
                    .collect(java.util.stream.Collectors.groupingBy(
                            name -> name, java.util.stream.Collectors.counting()));
            assertEquals(Long.valueOf(count), threads.get("nanocached-connection-reader"), threads.toString());
            assertTrue(threads.getOrDefault("nanocached-request-watchdog", 0L) <= 1, threads.toString());

            for (Connection connection : connections) connection.close();
            for (Future<byte[]> call : inFlight) {
                assertThrows(ExecutionException.class, call::get);
            }
        } finally {
            callers.shutdownNow();
            for (java.net.Socket serverSide : serverSides) serverSide.close();
        }
    }

    // ── INCR エンコード/デコード (issue #129) ────────────────────────
    // Exercised directly against Connection (bypassing NanocachedClient)
    // so the exact wire bytes and every response shape (with/without ttl,
    // with/without tag) are observable in isolation.

    @Test
    void incrRequestFrameBytesAndUntaggedResponseDecoding() throws Exception {
        try (java.net.ServerSocket server = new java.net.ServerSocket(0);
                java.net.Socket clientSocket = new java.net.Socket("127.0.0.1", server.getLocalPort());
                java.net.Socket serverSocket = server.accept()) {
            Connection connection = new Connection(clientSocket, false, () -> {});
            try {
                java.io.InputStream serverIn = serverSocket.getInputStream();
                java.io.OutputStream serverOut = serverSocket.getOutputStream();
                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                    // Namespaced, negative delta: `i <ns-len> <key-len>
                    // <delta>\n<namespace><key>` — the exact wire bytes.
                    Future<Connection.IncrResult> withTtl = pool.submit(() -> connection.incr(
                            "ns".getBytes(StandardCharsets.UTF_8), "key".getBytes(StandardCharsets.UTF_8), -42));
                    byte[] expectedFrame = "i 2 3 -42\nnskey".getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(expectedFrame, serverIn.readNBytes(expectedFrame.length));
                    serverOut.write("I 2 100\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.write("42".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    Connection.IncrResult result = withTtl.get();
                    assertEquals(42L, result.value());
                    assertEquals(Long.valueOf(100), result.ttlSeconds());

                    // Default namespace (namespace-length 0, still always
                    // sent — INCR has no separate legacy frame), no ttl.
                    byte[] frameNoTtl = "i 0 1 1\nk".getBytes(StandardCharsets.US_ASCII);

                    Future<Connection.IncrResult> noTtl = pool.submit(
                            () -> connection.incr(new byte[0], "k".getBytes(StandardCharsets.UTF_8), 1));
                    assertArrayEquals(frameNoTtl, serverIn.readNBytes(frameNoTtl.length));
                    serverOut.write("I 1\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.write("9".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    Connection.IncrResult result2 = noTtl.get();
                    assertEquals(9L, result2.value());
                    assertNull(result2.ttlSeconds());

                    // Miss.
                    Future<Connection.IncrResult> missing = pool.submit(
                            () -> connection.incr(new byte[0], "k".getBytes(StandardCharsets.UTF_8), 1));
                    assertArrayEquals(frameNoTtl, serverIn.readNBytes(frameNoTtl.length));
                    serverOut.write("N\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertNull(missing.get());

                    // Non-numeric stored value / overflow.
                    Future<Object> notNumeric = pool.submit(() -> {
                        try {
                            return connection.incr(new byte[0], "k".getBytes(StandardCharsets.UTF_8), 1);
                        } catch (NanocachedException.NotNumeric caught) {
                            return caught;
                        }
                    });
                    assertArrayEquals(frameNoTtl, serverIn.readNBytes(frameNoTtl.length));
                    serverOut.write("T\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertTrue(notNumeric.get() instanceof NanocachedException.NotNumeric);

                    // Stale routing.
                    Future<Object> wrongNode = pool.submit(() -> {
                        try {
                            return connection.incr(new byte[0], "k".getBytes(StandardCharsets.UTF_8), 1);
                        } catch (NanocachedException.WrongNode caught) {
                            return caught;
                        }
                    });
                    assertArrayEquals(frameNoTtl, serverIn.readNBytes(frameNoTtl.length));
                    serverOut.write("W\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertTrue(wrongNode.get() instanceof NanocachedException.WrongNode);
                } finally {
                    pool.shutdown();
                }
            } finally {
                connection.close();
            }
        }
    }

    // ── 整数フィールドのグラマー検査 (issue #462) ────────────────────
    // Every integer field the SDK parses off the wire must accept ASCII
    // digits only (^[0-9]+$) — src/command.rs's parse_length loops
    // byte-by-byte over ASCII digits with no leading-zero restriction but
    // no leading '+' either. Integer.parseInt/Long.parseLong alone would
    // silently accept a leading '+', which the wire grammar never
    // permits.

    @Test
    void isDigitsOnlyAcceptsOnlyTheWireIntegerGrammar() {
        // Connection.isDigitsOnly is package-private — callable directly,
        // no reflection needed, since this test is in the same package.
        Map<String, Boolean> cases = new java.util.LinkedHashMap<>();
        cases.put("5", true);
        cases.put("007", true); // leading zeros are allowed — matches parse_length
        cases.put("0", true);
        cases.put("", false);
        cases.put("+5", false);
        cases.put(" 5", false);
        cases.put("5 ", false);
        cases.put("1_000", false);
        cases.put("1e2", false);
        cases.put("-5", false);
        for (Map.Entry<String, Boolean> testCase : cases.entrySet()) {
            assertEquals(testCase.getValue(), Connection.isDigitsOnly(testCase.getKey()),
                    "isDigitsOnly(\"" + testCase.getKey() + "\")");
        }
    }

    @Test
    void parseNonNegativeIntAcceptsOnlyTheWireIntegerGrammar() {
        Map<String, Integer> cases = new java.util.LinkedHashMap<>();
        cases.put("5", 5);
        cases.put("007", 7); // leading zeros are allowed — matches parse_length
        cases.put("0", 0);
        cases.put("", -1);
        cases.put("+5", -1); // Integer.parseInt("+5") alone would accept this
        cases.put(" 5", -1);
        cases.put("5 ", -1);
        cases.put("1_000", -1);
        cases.put("1e2", -1);
        cases.put("-5", -1);
        for (Map.Entry<String, Integer> testCase : cases.entrySet()) {
            assertEquals(testCase.getValue(), Connection.parseNonNegativeInt(testCase.getKey()),
                    "parseNonNegativeInt(\"" + testCase.getKey() + "\")");
        }
    }

    /** Table-driven coverage for the counter helper (INCR's `I` response
     * body, {@code Connection.parseCounterValue}) — the one place a
     * leading '-' is allowed, unlike every other wire-derived integer
     * field, but a leading '+' is still rejected (Integer/Long's parsers
     * would otherwise silently accept it). Exercised directly against a
     * real Connection over a loopback socket pair, mirroring
     * incrRequestFrameBytesAndUntaggedResponseDecoding above; a rejected
     * body must poison the connection (subsequent use throws
     * ConnectionFailed) rather than silently coercing. */
    @Test
    void parseCounterValueAcceptsALeadingMinusButRejectsEverythingElseNonDigit() throws Exception {
        try (java.net.ServerSocket server = new java.net.ServerSocket(0);
                java.net.Socket clientSocket = new java.net.Socket("127.0.0.1", server.getLocalPort());
                java.net.Socket serverSocket = server.accept()) {
            Connection connection = new Connection(clientSocket, false, () -> {});
            try {
                Method parseCounterValue = Connection.class.getDeclaredMethod("parseCounterValue", byte[].class);
                parseCounterValue.setAccessible(true);

                Map<String, Long> accepted = new java.util.LinkedHashMap<>();
                accepted.put("5", 5L);
                accepted.put("007", 7L); // leading zeros allowed, matches parse_length
                accepted.put("0", 0L);
                accepted.put("-5", -5L); // the one exception: counters may go negative
                for (Map.Entry<String, Long> testCase : accepted.entrySet()) {
                    byte[] body = testCase.getKey().getBytes(StandardCharsets.US_ASCII);
                    assertEquals(testCase.getValue(), parseCounterValue.invoke(connection, (Object) body),
                            "parseCounterValue(\"" + testCase.getKey() + "\")");
                }

                List<String> rejected =
                        List.of("+5", " 5", "5 ", "1_000", "1e2", "");
                for (String testCase : rejected) {
                    byte[] body = testCase.getBytes(StandardCharsets.US_ASCII);
                    InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                            () -> parseCounterValue.invoke(connection, (Object) body),
                            "parseCounterValue(\"" + testCase + "\") should reject");
                    assertTrue(thrown.getCause() instanceof NanocachedException.ConnectionFailed,
                            "expected ConnectionFailed, got " + thrown.getCause());
                }
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void incrRoundTripsOnATaggedConnectionWithAndWithoutTtl() throws Exception {
        try (MockNode node = MockNode.withTagSupport()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertTrue(target.tagged());
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                // Missing key.
                assertNull(connection.incr(new byte[0], "counter".getBytes(StandardCharsets.UTF_8), 5));

                // Hit, no TTL.
                connection.set("counter".getBytes(StandardCharsets.UTF_8), "10".getBytes(StandardCharsets.UTF_8), null);
                Connection.IncrResult result = connection.incr(new byte[0], "counter".getBytes(StandardCharsets.UTF_8), 5);
                assertEquals(15L, result.value());
                assertNull(result.ttlSeconds());

                // Hit, with a TTL.
                connection.set("timed".getBytes(StandardCharsets.UTF_8), "100".getBytes(StandardCharsets.UTF_8), 30L);
                Connection.IncrResult timed = connection.incr(new byte[0], "timed".getBytes(StandardCharsets.UTF_8), 1);
                assertEquals(101L, timed.value());
                assertEquals(Long.valueOf(30), timed.ttlSeconds());

                // Non-numeric.
                connection.set("word".getBytes(StandardCharsets.UTF_8), "hi".getBytes(StandardCharsets.UTF_8), null);
                assertThrows(NanocachedException.NotNumeric.class,
                        () -> connection.incr(new byte[0], "word".getBytes(StandardCharsets.UTF_8), 1));
            } finally {
                connection.close();
            }
        }
    }

    // ── Compare-and-set エンコード/デコード (issue #141) ─────────────
    // Exercised directly against Connection (bypassing NanocachedClient)
    // so the exact wire bytes and every response shape are observable in
    // isolation, mirroring the INCR tests immediately above.

    @Test
    void casRequestFrameBytesAndUntaggedResponseDecoding() throws Exception {
        try (java.net.ServerSocket server = new java.net.ServerSocket(0);
                java.net.Socket clientSocket = new java.net.Socket("127.0.0.1", server.getLocalPort());
                java.net.Socket serverSocket = server.accept()) {
            Connection connection = new Connection(clientSocket, false, () -> {});
            try {
                java.io.InputStream serverIn = serverSocket.getInputStream();
                java.io.OutputStream serverOut = serverSocket.getOutputStream();
                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                    // `k <ns-len> <key-len> <value-len> <cond> [<ttl>]\n
                    // <namespace><key><value>` — cond A, namespaced, with a
                    // ttl: the exact wire bytes.
                    Future<Boolean> absent = pool.submit(() -> connection.casSet(
                            "ns".getBytes(StandardCharsets.UTF_8), "key".getBytes(StandardCharsets.UTF_8),
                            "val".getBytes(StandardCharsets.UTF_8), 60L, "A"));
                    byte[] absentFrame = "k 2 3 3 A 60\nnskeyval".getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(absentFrame, serverIn.readNBytes(absentFrame.length));
                    serverOut.write("S\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertTrue(absent.get());

                    // cond P, default namespace (namespace-length 0, still
                    // always sent), no ttl; mismatch (N).
                    Future<Boolean> present = pool.submit(() -> connection.casSet(
                            new byte[0], "k".getBytes(StandardCharsets.UTF_8),
                            "v".getBytes(StandardCharsets.UTF_8), null, "P"));
                    byte[] presentFrame = "k 0 1 1 P\nkv".getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(presentFrame, serverIn.readNBytes(presentFrame.length));
                    serverOut.write("N\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertFalse(present.get());

                    // cond is a 32-character digest, not length-prefixed —
                    // its own shape identifies it, exactly like A/P.
                    String digest = "d41d8cd98f00b204e9800998ecf8427e";
                    Future<Boolean> digestMatch = pool.submit(() -> connection.casSet(
                            new byte[0], "k".getBytes(StandardCharsets.UTF_8),
                            "v2".getBytes(StandardCharsets.UTF_8), null, digest));
                    byte[] digestFrame = ("k 0 1 2 " + digest + "\nkv2").getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(digestFrame, serverIn.readNBytes(digestFrame.length));
                    serverOut.write("S\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertTrue(digestMatch.get());

                    // Stale routing.
                    Future<Object> wrongNode = pool.submit(() -> {
                        try {
                            return connection.casSet(new byte[0], "k".getBytes(StandardCharsets.UTF_8),
                                    "v".getBytes(StandardCharsets.UTF_8), null, "A");
                        } catch (NanocachedException.WrongNode caught) {
                            return caught;
                        }
                    });
                    byte[] wrongNodeFrame = "k 0 1 1 A\nkv".getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(wrongNodeFrame, serverIn.readNBytes(wrongNodeFrame.length));
                    serverOut.write("W\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertTrue(wrongNode.get() instanceof NanocachedException.WrongNode);

                    // `x <ns-len> <key-len> <cond>\n<namespace><key>` —
                    // cond is always a digest here.
                    Future<Boolean> deleted = pool.submit(() -> connection.casDelete(
                            new byte[0], "k".getBytes(StandardCharsets.UTF_8), digest));
                    byte[] deleteFrame = ("x 0 1 " + digest + "\nk").getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(deleteFrame, serverIn.readNBytes(deleteFrame.length));
                    serverOut.write("D\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertTrue(deleted.get());

                    // Mismatch or missing key: N, exactly like a plain D's
                    // own miss.
                    Future<Boolean> notDeleted = pool.submit(() -> connection.casDelete(
                            new byte[0], "k".getBytes(StandardCharsets.UTF_8), digest));
                    assertArrayEquals(deleteFrame, serverIn.readNBytes(deleteFrame.length));
                    serverOut.write("N\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();
                    assertFalse(notDeleted.get());
                } finally {
                    pool.shutdown();
                }
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void casRoundTripsOnATaggedConnectionForEveryConditionKind() throws Exception {
        try (MockNode node = MockNode.withTagSupport()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertTrue(target.tagged());
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                byte[] key = "k".getBytes(StandardCharsets.UTF_8);

                // A: absent succeeds, then mismatches once present.
                assertTrue(connection.casSet(new byte[0], key, "v1".getBytes(StandardCharsets.UTF_8), null, "A"));
                assertFalse(connection.casSet(new byte[0], key, "v2".getBytes(StandardCharsets.UTF_8), null, "A"));

                // P: present succeeds, carrying a ttl through untouched.
                assertTrue(connection.casSet(new byte[0], key, "v3".getBytes(StandardCharsets.UTF_8), 30L, "P"));

                // Digest: matches the real current content (cross-checked
                // against this SDK's own independent digest implementation
                // via the mock's own — see MockNode.digestOf).
                String currentDigest = NanocachedClient.contentDigest(connection.get(new byte[0], key));
                assertTrue(connection.casSet(
                        new byte[0], key, "v4".getBytes(StandardCharsets.UTF_8), null, currentDigest));
                assertArrayEquals("v4".getBytes(StandardCharsets.UTF_8), connection.get(new byte[0], key));

                // The same (now stale) digest mismatches.
                assertFalse(connection.casSet(
                        new byte[0], key, "v5".getBytes(StandardCharsets.UTF_8), null, currentDigest));

                // x: digest-conditioned delete.
                String latestDigest = NanocachedClient.contentDigest(connection.get(new byte[0], key));
                assertTrue(connection.casDelete(new byte[0], key, latestDigest));
                assertNull(connection.get(new byte[0], key));

                // A missing key never matches any digest.
                assertFalse(connection.casDelete(new byte[0], key, latestDigest));
            } finally {
                connection.close();
            }
        }
    }

    // ── multi-get 応答の累積バイト上限 (issue #179) ───────────────────

    @Test
    void multiGetReplyPoisonsTheConnectionWhenTheCumulativeSizeExceedsTheBound() throws Exception {
        // Regression (issue #179): each M entry's declared length was
        // already capped at MAX_VALUE_LENGTH, but nothing bounded the
        // sum across a reply's many entries — a node answering a
        // 400-key multi-get with 400 × 2 MiB hits could force ~800 MB
        // of allocation from a single reply. Shrinks
        // Connection.maxMultiGetResponseBytes — mutable only so a test
        // can shrink it, mirroring requestTimeoutMillis — so this test
        // can trip the bound with a couple of tiny values instead of
        // actually moving hundreds of megabytes over the loopback
        // socket.
        long defaultBound = Connection.maxMultiGetResponseBytes;
        Connection.maxMultiGetResponseBytes = 3;
        try (java.net.ServerSocket server = new java.net.ServerSocket(0);
                java.net.Socket clientSocket = new java.net.Socket("127.0.0.1", server.getLocalPort());
                java.net.Socket serverSocket = server.accept()) {
            Connection connection = new Connection(clientSocket, false, () -> {});
            try {
                java.io.InputStream serverIn = serverSocket.getInputStream();
                java.io.OutputStream serverOut = serverSocket.getOutputStream();
                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                    byte[][] keys = {"a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8)};
                    Future<List<Connection.MultiEntry>> future =
                            pool.submit(() -> connection.multiGet(new byte[0], keys));

                    byte[] expectedFrame = "m 0 2 1 1\nab".getBytes(StandardCharsets.US_ASCII);
                    assertArrayEquals(expectedFrame, serverIn.readNBytes(expectedFrame.length));

                    // Two 2-byte hits: the first alone (2 bytes) is
                    // within the shrunk 3-byte bound, but the second
                    // pushes the running total to 4 — over the bound —
                    // so it must be rejected before its body is ever
                    // read; only the first entry's body is sent.
                    serverOut.write("M 2 2 2\n".getBytes(StandardCharsets.US_ASCII));
                    serverOut.write("xy".getBytes(StandardCharsets.US_ASCII));
                    serverOut.flush();

                    ExecutionException wrapped = assertThrows(ExecutionException.class, future::get);
                    assertTrue(wrapped.getCause() instanceof NanocachedException.ConnectionFailed,
                            String.valueOf(wrapped.getCause()));
                    assertTrue(wrapped.getCause().getMessage().contains("exceeds"),
                            wrapped.getCause().getMessage());
                    assertTrue(connection.isClosed(),
                            "an oversized multi-get reply must poison the connection");
                } finally {
                    pool.shutdown();
                }
            } finally {
                connection.close();
            }
        } finally {
            Connection.maxMultiGetResponseBytes = defaultBound;
        }
    }

    // ── 応答タグ (echoed response tags) ──────────────────────────────

    @Test
    void negotiatesTagsAndRoundTripsPipelinedRequests() throws Exception {
        // Same shape as pipelinesConcurrentRequestsOnOneConnection, but
        // against a tag-negotiating server: N concurrent set/get on one
        // tagged connection, each independently verified to round-trip
        // its own value.
        try (MockNode node = MockNode.withTagSupport()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                int n = 20;
                ExecutorService pool = Executors.newFixedThreadPool(n);
                try {
                    List<Future<?>> sets = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        int index = i;
                        sets.add(pool.submit(() -> client.set("key-" + index, "value-" + index, index)));
                    }
                    for (Future<?> future : sets) future.get();

                    List<Future<Optional<String>>> gets = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        int index = i;
                        gets.add(pool.submit(() -> client.get("key-" + index)));
                    }
                    for (int i = 0; i < n; i++) {
                        assertEquals(Optional.of("value-" + i), gets.get(i).get());
                    }
                } finally {
                    pool.shutdown();
                }

                assertTrue(client.delete("key-0"));
                assertFalse(client.delete("key-0"));
            }
        }
    }

    @Test
    void aDesyncedStreamIsCaughtByTheTagCheckBeforeAnyCallerSeesWrongData() throws Exception {
        // The exact misdelivery request pipelining left open: the server
        // (as a stand-in for any off-by-one stream corruption) never
        // answers the first GET, so the second GET's response arrives at
        // the first GET's pending slot. Without tags the first caller
        // would receive the second's value as a plausible, exception-free
        // wrong answer; the tag check must poison the connection before
        // either caller sees anything. Exercised directly against
        // Connection (bypassing NanocachedClient's own redial-and-retry,
        // which would otherwise mask the desync behind a transparently
        // healed retry) so the raw per-call outcome is observable.
        try (MockNode node = MockNode.withTagSupport()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertTrue(target.tagged());
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                connection.set("k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8), null);

                node.swallowGetOnce();
                ExecutorService pool = Executors.newFixedThreadPool(2);
                try {
                    Future<byte[]> first = pool.submit(() -> connection.get("a".getBytes(StandardCharsets.UTF_8)));
                    waitFor(() -> node.getCount.get() >= 1, "the swallowed GET to reach the server");
                    Future<byte[]> second = pool.submit(() -> connection.get("k".getBytes(StandardCharsets.UTF_8)));

                    ExecutionException firstError = assertThrows(ExecutionException.class, first::get);
                    assertTrue(firstError.getCause().getMessage().contains("desynced"), firstError.getCause().getMessage());
                    ExecutionException secondError = assertThrows(ExecutionException.class, second::get);
                    assertTrue(secondError.getCause().getMessage().contains("desynced"), secondError.getCause().getMessage());
                } finally {
                    pool.shutdown();
                }

                assertTrue(connection.isClosed());
            } finally {
                connection.close();
            }

            // The poisoned connection redials transparently through
            // NanocachedClient on next use.
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertEquals(Optional.of("v"), client.get("k"));
            }
            assertEquals(2, node.connectionCount.get());
        }
    }

    @Test
    void aResponseEchoingTheWrongTagPoisonsTheConnection() throws Exception {
        // Exercised directly against Connection — see the desync test
        // above for why NanocachedClient's own transparent retry would
        // otherwise mask this.
        try (MockNode node = MockNode.withTagSupport()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                node.answerWrongTagOnce();
                NanocachedException error = assertThrows(NanocachedException.class,
                        () -> connection.get("k".getBytes(StandardCharsets.UTF_8)));
                assertTrue(error.getMessage().contains("desynced"), error.getMessage());
                assertTrue(connection.isClosed());
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void aResponseHeaderThatNeverTerminatesFailsFast() throws Exception {
        // MAX_HEADER_LINE_LENGTH (issue: audit finding): a malicious or
        // buggy node streaming a `V` header with no '\n' must not be able
        // to grow readLine()'s buffer without bound — it must fail fast
        // instead, gated only by this cap rather than the (much longer)
        // request timeout.
        try (MockNode node = new MockNode()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                node.answerRunawayHeaderOnce();
                NanocachedException error = assertThrows(NanocachedException.class,
                        () -> connection.get("k".getBytes(StandardCharsets.UTF_8)));
                assertTrue(error.getMessage().contains("too long"), error.getMessage());
                assertTrue(connection.isClosed());
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void anUnexpectedByteAfterAnUntaggedResponseMarkerDesyncsTheConnection() throws Exception {
        // Issue: audit finding — the untagged `N`/`S`/`D`/`W`/`B` forms
        // are always exactly two bytes on the wire (marker + '\n'); the
        // second byte was never actually verified to be '\n' before this
        // fix, silently accepting a desynced stream instead of poisoning
        // the connection.
        try (MockNode node = new MockNode()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                node.answerBadTrailerOnce();
                NanocachedException error = assertThrows(NanocachedException.class,
                        () -> connection.get("missing".getBytes(StandardCharsets.UTF_8)));
                assertTrue(error.getMessage().contains("desynced"), error.getMessage());
                assertTrue(connection.isClosed());
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void fallsBackToTheUntaggedProtocolAgainstAPre0019Server() throws Exception {
        // An old server treats any extended `A` (`T R` or plain `T`) as a
        // parse error and closes without replying; the client must step
        // back one probe stage at a time and run untagged — transparently,
        // with the same results.
        try (MockNode node = MockNode.legacyServer()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
                // Three dials (issue #125 added a probe stage in front of
                // the pre-existing one): the full `T R` attempt the server
                // slammed shut, then the `T`-only attempt it also slammed
                // shut, then the plain fallback that stuck.
                assertEquals(3, node.connectionCount.get());
                assertEquals(List.of("A 1 T R", "A 1 T", "A 1"), node.authHeaders);
            }
        }
    }

    // ── issue #125: retryable-error status `R` ───────────────────────

    @Test
    void connectProbeSendsTheFullCapabilityHeader() throws Exception {
        // Every dial's first attempt asks for both `T` and `R`, in that
        // fixed order. A modern server — this suite's default MockNode —
        // simply ignores the trailing `R` it doesn't act on and acks
        // normally, so this is the only dial that ever happens.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                assertEquals(List.of("A 1 T R"), node.authHeaders);
                assertEquals(1, node.connectionCount.get());
            }
        }
    }

    @Test
    void fallsBackToTheTagOnlyProtocolAgainstAServerThatPredatesRetryCapability() throws Exception {
        // A server that supports `T` (issue #19) but predates `R` (issue
        // #125): the doubly-extended `A <len> T R` is a parse error to
        // it, so the client steps back exactly one probe stage — to `T`
        // alone — rather than falling all the way to the untagged form.
        try (MockNode node = MockNode.predatesRetryCapability()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
                assertEquals(2, node.connectionCount.get());
                assertEquals(List.of("A 1 T R", "A 1 T"), node.authHeaders);
            }

            // The stuck connection did negotiate tags — confirmed with a
            // second, independent probe against the same mock.
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertTrue(target.tagged());
            target.socket().close();
        }
    }

    @Test
    void aTransientRResponseIsTransparentlyRetriedOnTheSameConnection() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                assertEquals(0, client.stats().transientRetries());

                node.answerRetryableFor(1);
                assertEquals(Optional.of("v"), client.get("k"));

                assertEquals(1, client.stats().transientRetries());
                assertEquals(1, node.connectionCount.get(), "no new connection was dialed");
                assertEquals(2, node.getCount.get(), "the R'd attempt plus the retry that succeeded");
            }
        }
    }

    @Test
    void threeTransientRResponsesExhaustTheRetryBudgetButLeaveTheConnectionUsable() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");

                node.answerRetryableFor(3);
                long started = System.nanoTime();
                assertThrows(NanocachedException.RetryableError.class, () -> client.get("k"));
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                // The spec's fixed backoff: 50ms before the first retry,
                // 100ms before the second.
                assertTrue(elapsedMillis >= 150,
                        "expected at least the 50ms+100ms retry backoff, took " + elapsedMillis + "ms");

                assertEquals(3, client.stats().transientRetries());
                assertEquals(1, node.connectionCount.get(), "still the same connection — no redial");

                // The SAME connection still serves a following op.
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void aTaggedRResponsePairsWithItsOwnRequestAmongPipelinedOps() throws Exception {
        // Tagged mode (issue #125): `R <tag>` must pair with the exact
        // in-flight request it answers, not just whichever happens to be
        // oldest — proven by letting a transient retry interleave with a
        // second pipelined get for a different key.
        try (MockNode node = MockNode.withTagSupport()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k1", "v1");
                client.set("k2", "v2");

                node.delayGets(80);
                node.answerRetryableFor(1); // exactly one of the two G's below

                ExecutorService pool = Executors.newFixedThreadPool(2);
                try {
                    Future<Optional<String>> a = pool.submit(() -> client.get("k1"));
                    Future<Optional<String>> b = pool.submit(() -> client.get("k2"));
                    assertEquals(Optional.of("v1"), a.get());
                    assertEquals(Optional.of("v2"), b.get());
                } finally {
                    pool.shutdown();
                }

                assertEquals(1, client.stats().transientRetries());
                assertEquals(1, node.connectionCount.get());
            }
        }
    }

    @Test
    void viaProxyRetriesATransientRResponseOnTheSameProxyConnection() throws Exception {
        // The R path works identically in viaProxy mode — a proxy
        // connection is exactly a single-node connection under the hood,
        // so one confirmation test is enough (mirrors the spec's own
        // "one test is enough" for this mode).
        try (MockNode proxy = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.proxies = List.of(new DiscoveredNode("proxy-a", proxy.address()));

            try (NanocachedClient client =
                    NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                client.set("k", "v");

                proxy.answerRetryableFor(1);
                assertEquals(Optional.of("v"), client.get("k"));

                assertEquals(1, client.stats().transientRetries());
                assertEquals(1, proxy.connectionCount.get());
            }
        }
    }

    // ── TLS hostname verification (audit finding J1) ────────────────
    //
    // Certificates are generated per-test with the JDK's own `keytool`
    // (see MockServers.Tls) rather than a bundled/pre-generated PEM: that
    // keeps the certificate's notBefore/notAfter always valid without a
    // maintenance burden, and needs no TLS/crypto test dependency this
    // SDK doesn't otherwise have.

    @Test
    void tlsRejectsACertificateForADifferentHostname(@TempDir Path tempDir) throws Exception {
        // A cert that is otherwise perfectly valid (self-signed, but
        // trusted directly via ca()) except its SAN names a host the
        // client never dialed. Before the J1 fix this was accepted
        // outright — SSLContext verifies the chain but never checked the
        // identity it was issued to.
        Tls.Generated cert = Tls.generate(tempDir, "wrong-host", "dns:wrong.example.test");
        try (MockNode node = MockNode.withTls(cert.serverContext())) {
            NanocachedException error = assertThrows(NanocachedException.class, () ->
                    NanocachedClient.connect(single("127.0.0.1", node.port())
                            .tls(true)
                            .ca(cert.pemCert())));
            // Surfaces as an ordinary connection failure — never a silent
            // fallback to an unverified connection.
            assertTrue(error instanceof NanocachedException.ConnectionFailed, error.toString());
        }
    }

    @Test
    void tlsAcceptsACertificateForTheMatchingHostname(@TempDir Path tempDir) throws Exception {
        // The client dials "127.0.0.1"; the JDK's HTTPS endpoint
        // identification checks a numeric host against the cert's
        // iPAddress SAN entries (RFC 2818), so the cert must carry one.
        Tls.Generated cert = Tls.generate(tempDir, "127.0.0.1", "ip:127.0.0.1");
        try (MockNode node = MockNode.withTls(cert.serverContext())) {
            try (NanocachedClient client = NanocachedClient.connect(single("127.0.0.1", node.port())
                    .tls(true)
                    .ca(cert.pemCert()))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    // ── 名前空間 (namespaces, issue #105) ──────────────────────────

    @Test
    void namespacedSetGetDeleteRoundTrips() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace ns = client.namespace("tenant-a");
                assertArrayEquals("tenant-a".getBytes(StandardCharsets.UTF_8), ns.namespace());

                ns.set("greeting", "hello", 60);
                assertEquals(Optional.of("hello"), ns.get("greeting"));
                assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8),
                        ns.getBytes("greeting").orElseThrow());
                assertTrue(ns.delete("greeting"));
                assertEquals(Optional.empty(), ns.get("greeting"));
                assertFalse(ns.delete("greeting"));
            }
        }
    }

    @Test
    void namespaceIsolatesTheSameKeyNameFromTheDefaultAndOtherNamespaces() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace ns1 = client.namespace("ns1");
                NanocachedClient.Namespace ns2 = client.namespace("ns2");

                client.set("shared", "default-value");
                ns1.set("shared", "ns1-value");
                ns2.set("shared", "ns2-value");

                assertEquals(Optional.of("default-value"), client.get("shared"));
                assertEquals(Optional.of("ns1-value"), ns1.get("shared"));
                assertEquals(Optional.of("ns2-value"), ns2.get("shared"));

                // Three genuinely independent entries: the default
                // keyspace and each namespace's own store hold exactly
                // one each, not one shared entry the last write clobbered.
                assertEquals(1, node.store.size());
                assertEquals(2, node.namespacedStores.size());
                assertEquals(1, node.namespacedStores.get("ns1").size());
                assertEquals(1, node.namespacedStores.get("ns2").size());
            }
        }
    }

    @Test
    void namespaceEmptyUsesLegacyFramesAndIsEquivalentToTheRootClient() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                NanocachedClient.Namespace root = client.namespace("");
                assertArrayEquals(new byte[0], root.namespace());

                root.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k")); // same keyspace as the client itself
                assertEquals(Optional.of("v"), root.get("k"));
                assertTrue(root.delete("k"));
                assertEquals(Optional.empty(), client.get("k"));

                // Every request above went out as a legacy G/S/D frame,
                // never g/s/d — MockNode.namespacedCommandCount only
                // counts the latter (the SDK rule: the default namespace
                // must keep sending legacy frames byte-for-byte).
                assertEquals(0, node.namespacedCommandCount.get());
            }
        }
    }

    @Test
    void namespacedHandleIsInvalidAfterClose() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedClient client = connect("127.0.0.1", node.port());
            NanocachedClient.Namespace ns = client.namespace("ns");
            client.close();
            assertThrows(NanocachedException.AlreadyClosed.class, () -> ns.get("k"));
            assertThrows(NanocachedException.AlreadyClosed.class, () -> ns.set("k", "v"));
            assertThrows(NanocachedException.AlreadyClosed.class, () -> ns.delete("k"));
            assertThrows(NanocachedException.AlreadyClosed.class, ns::clear);
        }
    }

    @Test
    void rejectsOversizeNamespacePlusKeyBeforeTouchingTheConnection() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("warm", "up");
                int connectionsBeforeRejections = node.connectionCount.get();

                byte[] oversizeNamespace = new byte[1024 * 1024];
                NanocachedClient.Namespace ns = client.namespace(oversizeNamespace);
                byte[] key = "k".getBytes(StandardCharsets.UTF_8);
                byte[] value = "v".getBytes(StandardCharsets.UTF_8);
                assertThrows(IllegalArgumentException.class, () -> ns.get(key));
                assertThrows(IllegalArgumentException.class, () -> ns.delete(key));
                assertThrows(IllegalArgumentException.class, () -> ns.set(key, value));

                assertEquals(connectionsBeforeRejections, node.connectionCount.get());
                assertEquals(Optional.of("up"), client.get("warm"));
            }
        }
    }

    @Test
    void wrongNodeOnANamespacedKeyTriggersRefreshAndRoutesByNamespaceAndKey() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = "key-0";
                byte[] namespace = "ns".getBytes(StandardCharsets.UTF_8);
                NanocachedClient.Namespace ns = client.namespace(namespace);

                // Routing must key off (namespace, key), not the key
                // alone — this pair was picked because its plain-key
                // owner and its namespaced owner are different nodes in
                // this 2-node ring, so the test fails if namespace were
                // silently dropped from routing.
                List<String> plainOwner = new HashRing(NAMES).owners(key.getBytes(StandardCharsets.UTF_8), 1);
                List<String> nsOwner =
                        new HashRing(NAMES).owners(namespace, key.getBytes(StandardCharsets.UTF_8), 1);
                assertNotEquals(plainOwner, nsOwner);

                ns.set(key, "v");
                MockNode owner = cluster.nodes().get(nsOwner.get(0));

                owner.answerWrongNodeOnce();
                assertEquals(Optional.of("v"), ns.get(key));

                owner.answerWrongNodeOnce();
                owner.answerWrongNodeOnce();
                assertThrows(NanocachedException.WrongNode.class, () -> ns.get(key));
            }
        }
    }

    @Test
    void connectionEncodesNamespacedFramesIncludingTaggedAndBinaryForms() throws Exception {
        // Exercised directly against Connection (as the tagged-mode tests
        // above do) so the exact frame shape — the ttl+tag `s` form
        // (<ns-len> <key-len> <val-len> <ttl> <tag>), a binary (non-UTF-8)
        // namespace, and the tagged connection's `g`/`d` forms — is
        // proven, not just the higher-level round trip.
        try (MockNode node = MockNode.withTagSupport()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertTrue(target.tagged());
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                byte[] namespace = {(byte) 0xff, 0x00, 0x01}; // binary, not valid UTF-8
                byte[] key = "k".getBytes(StandardCharsets.UTF_8);
                byte[] value = "v".getBytes(StandardCharsets.UTF_8);

                connection.set(namespace, key, value, 60L); // the ttl+tag `s` form
                assertArrayEquals(value, connection.get(namespace, key));
                assertTrue(connection.delete(namespace, key));
                assertNull(connection.get(namespace, key));

                // The default namespace on a tagged connection still uses
                // the legacy tagged G/S/D form (no namespace-length field).
                connection.set(new byte[0], key, value, null);
                assertArrayEquals(value, connection.get(new byte[0], key));
            } finally {
                connection.close();
            }
        }
    }

    // ── CLEAR namespace / flush everything (issue #106) ─────────────

    @Test
    void connectionEncodesClearAndClearAllFramesIncludingTaggedForm() throws Exception {
        // Exercised directly against Connection, mirroring
        // connectionEncodesNamespacedFramesIncludingTaggedAndBinaryForms:
        // the exact frame shape (the `c <ns-len> <tag>` form, the bare
        // `F <tag>` form with no body at all, and a binary namespace) is
        // proven, not just the higher-level round trip. A returning
        // clear()/clearAll() call with no exception is itself proof the
        // `C` response parses correctly.
        try (MockNode node = MockNode.withTagSupport()) {
            Identify.NodeTarget target =
                    (Identify.NodeTarget) Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertTrue(target.tagged());
            Connection connection = new Connection(target.socket(), target.tagged(), () -> {});
            try {
                byte[] namespace = {(byte) 0xff, 0x00, 0x01}; // binary, not valid UTF-8
                connection.clear(namespace); // c <len> <tag>\n<namespace>
                connection.clear(new byte[0]); // c 0 <tag>\n — the default namespace, not rejected
                connection.clearAll(); // F <tag>\n — no body at all
                assertEquals(2, node.clearCount.get());
                assertEquals(1, node.clearAllCount.get());
            } finally {
                connection.close();
            }
        }

        // Untagged connection: the legacy bare `c <len>\n<ns>` / `F\n` forms.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.namespace("ns").clear();
                client.clearAll();
                assertEquals(1, node.clearCount.get());
                assertEquals(1, node.clearAllCount.get());
            }
        }
    }

    @Test
    void namespaceClearDropsOnlyThatNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("default-key", "default-value");
                NanocachedClient.Namespace ns1 = client.namespace("ns1");
                NanocachedClient.Namespace ns2 = client.namespace("ns2");
                ns1.set("k", "ns1-value");
                ns2.set("k", "ns2-value");

                ns1.clear();

                assertEquals(Optional.empty(), ns1.get("k"));
                assertEquals(Optional.of("ns2-value"), ns2.get("k")); // a different namespace survives
                assertEquals(Optional.of("default-value"), client.get("default-key")); // the default survives
            }
        }
    }

    @Test
    void clearOnAnEmptyNamespaceHandleClearsOnlyTheDefaultNamespace() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("default-key", "v");
                NanocachedClient.Namespace ns = client.namespace("ns");
                ns.set("k", "ns-value");

                client.namespace("").clear(); // `c 0` — the default namespace only, not `F`

                assertEquals(Optional.empty(), client.get("default-key"));
                assertEquals(Optional.of("ns-value"), ns.get("k")); // untouched — this is not clearAll()
            }
        }
    }

    @Test
    void clearAllEmptiesEveryNamespaceIncludingTheDefault() throws Exception {
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("default-key", "v");
                NanocachedClient.Namespace ns1 = client.namespace("ns1");
                NanocachedClient.Namespace ns2 = client.namespace("ns2");
                ns1.set("k", "v1");
                ns2.set("k", "v2");

                client.clearAll();

                assertEquals(Optional.empty(), client.get("default-key"));
                assertEquals(Optional.empty(), ns1.get("k"));
                assertEquals(Optional.empty(), ns2.get("k"));
                assertTrue(node.store.isEmpty());
                assertTrue(node.namespacedStores.isEmpty());
            }
        }
    }

    @Test
    void clearAllThrowsAfterClose() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedClient client = connect("127.0.0.1", node.port());
            client.close();
            assertThrows(NanocachedException.AlreadyClosed.class, client::clearAll);
        }
    }

    @Test
    void clearAllFansOutToEveryNodeInTheCluster() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                for (int i = 0; i < 20; i++) {
                    client.set("key-" + i, "v"); // scattered across both nodes by HRW
                }
                assertTrue(cluster.nodes().values().stream().anyMatch(n -> !n.store.isEmpty()));

                client.clearAll();

                for (MockNode node : cluster.nodes().values()) {
                    assertEquals(1, node.clearAllCount.get(), "every node must receive the F frame");
                    assertTrue(node.store.isEmpty());
                }
            }
        }
    }

    @Test
    void namespaceClearFansOutToEveryNodeInTheCluster() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                NanocachedClient.Namespace ns = client.namespace("tenant");
                for (int i = 0; i < 20; i++) {
                    ns.set("key-" + i, "v"); // scattered across both nodes by HRW
                }

                ns.clear();

                for (MockNode node : cluster.nodes().values()) {
                    assertEquals(1, node.clearCount.get(), "every node must receive the c frame");
                    assertEquals(Map.of(), node.namespacedStores.getOrDefault("tenant", Map.of()));
                }
            }
        }
    }

    @Test
    void clearAllRetriesOnceAfterANodeFailsThenSucceedsOnTheRefreshedList() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode owner = cluster.nodes().get(NAMES.get(0));
                MockNode survivor = cluster.nodes().get(NAMES.get(1));
                // Two credits: applyReconnecting's own single redial-retry
                // (Connection-level healing, beneath the client's
                // refresh-and-retry) already absorbs one dropped
                // connection per fan-out pass, so both attempts in the
                // first pass must fail for the node to still be down by
                // the time fanOutClear checks — the third attempt, in the
                // retried pass, then acks normally.
                owner.failClearOnce();
                owner.failClearOnce();

                client.clearAll(); // must not throw — the retried pass succeeds

                // The retry resends F to *every* node of the refreshed
                // list, not just the one that failed (the spec's own
                // wording) — so the survivor sees it twice (once per
                // pass) and the owner three times (two dropped, one acked).
                assertEquals(3, owner.clearAllCount.get());
                assertEquals(2, survivor.clearAllCount.get());
                for (MockNode node : cluster.nodes().values()) {
                    assertTrue(node.store.isEmpty());
                }
            }
        }
    }

    @Test
    void clearAllRaisesNamingTheNodeWhenItIsStillDownAfterTheRetry() throws Exception {
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String deadName = NAMES.get(0);
                cluster.nodes().get(deadName).close();
                Thread.sleep(50);

                NanocachedException error = assertThrows(NanocachedException.class, client::clearAll);
                assertTrue(error.getMessage().contains(deadName),
                        "error should name the failing node: " + error.getMessage());
            }
        }
    }

    // ── SDK proxy mode (issue #122, viaProxy) ────────────────────────

    private static NanocachedClient.Options viaProxyOptions(int discoveryPort) {
        return NanocachedClient.builder()
                .addresses(List.of(new Address("127.0.0.1", discoveryPort)))
                .viaProxy(true);
    }

    @Test
    void viaProxyRoutesEveryOperationThroughTheChosenProxyAndNeverDialsANode() throws Exception {
        try (MockNode proxy = new MockNode();
                MockNode node = new MockNode();
                MockDiscovery discovery = new MockDiscovery(
                        List.of(new DiscoveredNode(NAMES.get(0), node.address())), 1)) {
            // A cluster node is registered too, so "never dials a node"
            // below is an actual assertion, not vacuously true.
            discovery.proxies = List.of(new DiscoveredNode("proxy-a", proxy.address()));

            try (NanocachedClient client =
                    NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
                assertTrue(client.delete("k"));
                assertEquals(Optional.empty(), client.get("k"));

                NanocachedClient.Namespace ns = client.namespace("tenant-a");
                ns.set("k2", "v2");
                assertEquals(Optional.of("v2"), ns.get("k2"));

                client.clearAll();
                assertEquals(Optional.empty(), ns.get("k2"));

                assertTrue(proxy.connectionCount.get() >= 1);
                assertEquals(0, node.connectionCount.get(),
                        "viaProxy must never open a connection to a cluster node");
            }
        }
    }

    @Test
    void viaProxySpreadsClientsAcrossProxiesAtRandom() throws Exception {
        // Statistical, not seeded, but with p=0.5 per trial and 40 fresh
        // clients the odds of either proxy going entirely unpicked are
        // astronomically small (2 * 0.5^40) — deterministic enough in
        // practice not to flake.
        try (MockNode proxyA = new MockNode();
                MockNode proxyB = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.proxies = List.of(
                    new DiscoveredNode("proxy-a", proxyA.address()),
                    new DiscoveredNode("proxy-b", proxyB.address()));

            for (int i = 0; i < 40; i++) {
                try (NanocachedClient client =
                        NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                    client.set("k", "v");
                }
            }

            assertTrue(proxyA.connectionCount.get() > 0, "proxy-a was never picked across 40 connects");
            assertTrue(proxyB.connectionCount.get() > 0, "proxy-b was never picked across 40 connects");
        }
    }

    @Test
    void viaProxyFailsOverToALiveProxyWhenTheChosenOneIsDown() throws Exception {
        try (MockNode live = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            int deadPort = MockServers.unusedPort();
            // Order matters for nothing here: connectToOneProxy shuffles
            // the roster, so the dead entry is picked first about half the
            // time — the outcome (landing on the live one) must hold
            // either way.
            discovery.proxies = List.of(
                    new DiscoveredNode("proxy-dead", "127.0.0.1:" + deadPort),
                    new DiscoveredNode("proxy-live", live.address()));

            try (NanocachedClient client =
                    NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
                assertEquals(1, live.connectionCount.get());
            }
        }
    }

    @Test
    void viaProxySkipsAWarmingUpDiscoverySeedForTheProxyRoster() throws Exception {
        // Same `B`-then-close startup-grace shape L already has (skipsAWarmingUpAddress
        // above) — the SDK's existing address fail-over handles it for Q too.
        try (MockNode proxy = new MockNode();
                MockDiscovery warming = new MockDiscovery(List.of(), 1);
                MockDiscovery healthy = new MockDiscovery(List.of(), 1)) {
            warming.warmingUp = true;
            healthy.proxies = List.of(new DiscoveredNode("proxy-a", proxy.address()));

            try (NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
                    .addresses(List.of(
                            new Address("127.0.0.1", warming.port()),
                            new Address("127.0.0.1", healthy.port())))
                    .viaProxy(true))) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
            }
        }
    }

    @Test
    void viaProxyRaisesAClearErrorWhenNoProxiesAreRegistered() throws Exception {
        try (MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            NanocachedException error = assertThrows(NanocachedException.class,
                    () -> NanocachedClient.connect(viaProxyOptions(discovery.port())));
            assertTrue(error.getMessage().contains("no proxies registered"), error.getMessage());
        }
    }

    @Test
    void viaProxyPointedAtANodeAddressFailsFastNamingTheAddress() throws Exception {
        try (MockNode node = new MockNode()) {
            NanocachedException error = assertThrows(NanocachedException.class,
                    () -> NanocachedClient.connect(viaProxyOptions(node.port())));
            assertTrue(error.getMessage().contains("viaProxy") && error.getMessage().contains("cache node"),
                    error.getMessage());
            assertEquals(0, node.getCount.get(), "the rejected node connection must never be used");
        }
    }

    @Test
    void viaProxyReconnectsToASurvivorAfterTheConnectedProxyDies() throws Exception {
        try (MockNode proxyA = new MockNode();
                MockNode proxyB = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.proxies = List.of(
                    new DiscoveredNode("proxy-a", proxyA.address()),
                    new DiscoveredNode("proxy-b", proxyB.address()));

            try (NanocachedClient client =
                    NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                client.set("k", "v");

                MockNode connected = proxyA.connectionCount.get() > 0 ? proxyA : proxyB;
                MockNode survivor = connected == proxyA ? proxyB : proxyA;
                connected.close(); // full teardown — this address can never come back
                Thread.sleep(50); // let the failure land

                // Retries the dead proxy first (fails), then re-fetches Q
                // and lands on the survivor — transparently, within this
                // one call, exactly like any other single-mode redial.
                assertEquals(Optional.empty(), client.get("k2"));
                assertTrue(survivor.connectionCount.get() >= 1,
                        "the survivor must have been dialed after reconnect");
            }
        }
    }

    @Test
    void viaProxyReconnectPurgesTheDepartedProxysCooldownEntry() throws Exception {
        // Issue #296: maybeRefresh's own cooldown prune (refreshNodeList)
        // never runs in proxy mode -- it early-returns while ring stays
        // null forever -- so without reconnectProxy's own purge (added
        // for #296) the failed same-address retry against the dead proxy
        // below would arm a reconnectCooldowns entry that then sits in
        // the map forever: that address is never dialed again once
        // singleAddress has moved on to the survivor. Mirrors
        // viaProxyReconnectsToASurvivorAfterTheConnectedProxyDies's own
        // setup.
        try (MockNode proxyA = new MockNode();
                MockNode proxyB = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.proxies = List.of(
                    new DiscoveredNode("proxy-a", proxyA.address()),
                    new DiscoveredNode("proxy-b", proxyB.address()));

            try (NanocachedClient client =
                    NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                client.set("k", "v");

                Field reconnectCooldownsField =
                        NanocachedClient.class.getDeclaredField("reconnectCooldowns");
                reconnectCooldownsField.setAccessible(true);
                Map<?, ?> reconnectCooldowns = (Map<?, ?>) reconnectCooldownsField.get(client);
                assertTrue(reconnectCooldowns.isEmpty());

                MockNode connected = proxyA.connectionCount.get() > 0 ? proxyA : proxyB;
                MockNode survivor = connected == proxyA ? proxyB : proxyA;
                String deadAddress = connected.address();
                connected.close(); // full teardown -- this address can never come back
                Thread.sleep(50); // let the failure land

                // Retries the dead proxy first (arming its cooldown entry
                // on failure), then re-fetches Q and lands on the
                // survivor -- transparently, within this one call.
                assertEquals(Optional.empty(), client.get("k2"));
                assertTrue(survivor.connectionCount.get() >= 1,
                        "the survivor must have been dialed after reconnect");

                // The swap must have purged the dead proxy's
                // now-unreachable-forever cooldown entry rather than
                // leaving it behind.
                assertTrue(reconnectCooldowns.keySet().stream().noneMatch(deadAddress::equals),
                        "a departed proxy's reconnect-cooldown entry must not linger "
                                + "after a proxy-mode failover swap");
            }
        }
    }

    @Test
    void viaProxyFailoverPrunesCooldownsForProxiesNoLongerInTheRoster() throws Exception {
        // Regression (pass-7 audit): #296 purges only the single
        // previousAddress on a swap. A cooldown armed for any other
        // address the proxy tier has since autoscaled away -- e.g. a
        // candidate the failover loop tried and failed before landing on a
        // survivor -- is never dialed again and, with refreshNodeList inert
        // in proxy mode, would linger in reconnectCooldowns for the
        // client's whole life. A failover must prune every cooldown entry
        // whose address is no longer in the freshly fetched roster.
        try (MockNode proxyA = new MockNode();
                MockNode proxyB = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.proxies = List.of(
                    new DiscoveredNode("proxy-a", proxyA.address()),
                    new DiscoveredNode("proxy-b", proxyB.address()));

            try (NanocachedClient client =
                    NanocachedClient.connect(viaProxyOptions(discovery.port()))) {
                client.set("k", "v");

                Field reconnectCooldownsField =
                        NanocachedClient.class.getDeclaredField("reconnectCooldowns");
                reconnectCooldownsField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String, Object> reconnectCooldowns =
                        (Map<String, Object>) reconnectCooldownsField.get(client);

                // Stand in for a retired proxy's leftover cooldown: an
                // address that is NOT in the roster and is NOT the
                // previousAddress the #296 purge would catch.
                String retiredAddress = "10.255.255.1:9999";
                Class<?> cooldownEntryClass = Class.forName(
                        "org.nanocached.NanocachedClient$CooldownEntry");
                var ctor = cooldownEntryClass.getDeclaredConstructor(long.class, RuntimeException.class);
                ctor.setAccessible(true);
                reconnectCooldowns.put(
                        retiredAddress,
                        ctor.newInstance(System.nanoTime() + Duration.ofHours(1).toNanos(),
                                new RuntimeException("stale")));
                assertTrue(reconnectCooldowns.containsKey(retiredAddress));

                MockNode connected = proxyA.connectionCount.get() > 0 ? proxyA : proxyB;
                MockNode survivor = connected == proxyA ? proxyB : proxyA;
                connected.close();
                Thread.sleep(50);

                assertEquals(Optional.empty(), client.get("k2"));
                assertTrue(survivor.connectionCount.get() >= 1,
                        "the survivor must have been dialed after reconnect");

                assertFalse(reconnectCooldowns.containsKey(retiredAddress),
                        "a failover must prune cooldown entries for addresses no longer "
                                + "in the proxy roster, not just the previousAddress");
            }
        }
    }

    @Test
    void connectRejectsReadHedgeAfterWithViaProxy() throws Exception {
        // Issue #488: a proxy connection has no replicas to hedge to, so a
        // configured readHedgeAfter can never take effect — connect()
        // rejects the combination instead of ignoring it.
        try (MockNode proxy = new MockNode();
                MockDiscovery discovery = new MockDiscovery(List.of(), 1)) {
            discovery.proxies = List.of(new DiscoveredNode("proxy-a", proxy.address()));
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                    NanocachedClient.connect(
                            viaProxyOptions(discovery.port()).readHedgeAfter(Duration.ofMillis(10))));
            assertTrue(error.getMessage().contains("readHedgeAfter has no effect with viaProxy"), error.getMessage());
        }
    }

    @Test
    void connectRejectsCaWithoutTls() throws Exception {
        // Issue #488: a CA file is only ever read over TLS; setting one
        // without tls(true) is almost always a forgotten tls(true).
        try (MockNode node = new MockNode()) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                    NanocachedClient.connect(
                            NanocachedClient.builder()
                                    .addresses(List.of(new Address("127.0.0.1", node.port())))
                                    .ca("/nonexistent/ca.pem")));
            assertTrue(error.getMessage().contains("ca is only used when tls is enabled"), error.getMessage());
        }
    }

    // ── hedge legs never run a read under hedgedReadsLock ──────────

    @Test
    void aSaturatedLegPoolDoesNotRunAHedgeLegInlineUnderTheRegistrationLock() throws Exception {
        // startHedgeLeg used to submit its leg to replicaWriters inside
        // synchronized (hedgedReadsLock). When that pool's queue is full its
        // overflow policy runs the task on the submitting thread, so a whole
        // read round trip ran holding the lock: every other startHedgeLeg
        // and close()'s drain blocked behind it. Saturate the pool, start a
        // hedged read whose primary is slow, and prove another thread can
        // still take the lock while that read is in flight. (The class's
        // @AfterEach restores the permit count.)
        NanocachedClient.maxInFlightBackgroundReplicaWrites = 1;
        try (Cluster cluster = startCluster(2)) {
            String primary = new HashRing(NAMES).owners("k".getBytes(StandardCharsets.UTF_8), 2).get(0);
            try (NanocachedClient client = connectWithReadHedgeAfter(cluster.discovery().port(), 5_000)) {
                client.set("k", "v");

                Field replicaWritersField = NanocachedClient.class.getDeclaredField("replicaWriters");
                replicaWritersField.setAccessible(true);
                ExecutorService replicaWriters = (ExecutorService) replicaWritersField.get(client);
                // 1 + REPLICA_WRITER_POOL_HEADROOM (16) threads over a queue of the same depth.
                int threads = 17;
                java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.CountDownLatch occupied = new java.util.concurrent.CountDownLatch(threads);
                // Whatever happens below, the blocked tasks must be released:
                // client.close() waits for them, and would otherwise turn a
                // failed assertion into a hang.
                try {
                    java.util.concurrent.Callable<Object> block = () -> {
                        occupied.countDown();
                        release.await();
                        return null;
                    };
                    // First occupy every thread, and only then fill the queue:
                    // submitting all 2 * threads at once races a worker that
                    // is idle (set() above used one) dequeuing its first task,
                    // and an overflowing submit would run the blocking task on
                    // this very thread.
                    for (int i = 0; i < threads; i++) replicaWriters.submit(block);
                    assertTrue(occupied.await(5, java.util.concurrent.TimeUnit.SECONDS));
                    for (int i = 0; i < threads; i++) replicaWriters.submit(block);

                    Field lockField = NanocachedClient.class.getDeclaredField("hedgedReadsLock");
                    lockField.setAccessible(true);
                    Object hedgedReadsLock = lockField.get(client);

                    cluster.nodes().get(primary).delayGets(1_500);
                    java.util.concurrent.atomic.AtomicReference<Optional<String>> read =
                            new java.util.concurrent.atomic.AtomicReference<>();
                    Thread reader = new Thread(() -> read.set(client.get("k")), "test-hedged-reader");
                    reader.start();
                    try {
                        // The primary leg is now running inline on the reader thread.
                        waitFor(() -> cluster.nodes().get(primary).getCount.get() >= 1,
                                "the saturated-pool read to reach its primary");

                        java.util.concurrent.CountDownLatch acquired = new java.util.concurrent.CountDownLatch(1);
                        Thread probe = new Thread(() -> {
                            synchronized (hedgedReadsLock) {
                                acquired.countDown();
                            }
                        }, "test-lock-probe");
                        probe.setDaemon(true);
                        probe.start();
                        assertTrue(acquired.await(500, java.util.concurrent.TimeUnit.MILLISECONDS),
                                "hedgedReadsLock is held while a hedge leg runs inline on a saturated pool");
                        assertTrue(reader.isAlive(), "the read should still be in flight, or this proves nothing");
                    } finally {
                        reader.join(10_000);
                    }
                    assertEquals(Optional.of("v"), read.get());
                } finally {
                    release.countDown();
                }
            }
        }
    }

    // ── refresh dials new nodes concurrently and keeps the failed ones ──

    @Test
    void refreshDialsBlackholedNewNodesConcurrently() throws Exception {
        // refreshNodeList dialed the new nodes one at a time under
        // refreshLock, so k blackholed nodes held every caller of
        // beforeOperation for k connect timeouts.
        int originalTimeout = Identify.connectTimeoutMillis;
        List<java.net.ServerSocket> silentServers = new ArrayList<>();
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                List<DiscoveredNode> roster = new ArrayList<>(cluster.discovery().nodes);
                for (int i = 0; i < 4; i++) {
                    java.net.ServerSocket silent = new java.net.ServerSocket(0);
                    silentServers.add(silent);
                    Thread acceptor = new Thread(() -> {
                        try {
                            while (true) silent.accept(); // accepts, never answers
                        } catch (java.io.IOException closed) {
                            // test over
                        }
                    }, "test-silent-accept-" + i);
                    acceptor.setDaemon(true);
                    acceptor.start();
                    roster.add(new DiscoveredNode("silent-" + i, "127.0.0.1:" + silent.getLocalPort()));
                }
                cluster.discovery().nodes = roster;

                Identify.connectTimeoutMillis = 400;
                long start = System.nanoTime();
                forceRefresh(client);
                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

                // Serial: 4 x 400ms = 1.6s+. Concurrent: about one timeout.
                assertTrue(elapsedMillis < 1_200,
                        "4 blackholed new nodes should be dialed concurrently, refresh took " + elapsedMillis + "ms");
                assertEquals(4, client.stats().refreshFailures());
            }
        } finally {
            Identify.connectTimeoutMillis = originalTimeout;
            for (java.net.ServerSocket silent : silentServers) silent.close();
        }
    }

    @Test
    void refreshKeepsAnUnreachableNewNodeInTheRingAsAMemberWithoutAConnection() throws Exception {
        // A new node whose dial failed used to be dropped from members, so
        // the ring was rebuilt without it and this client's primary/replica
        // choices disagreed with every peer that did reach it (issue #67
        // keeps such a node as a null-connection member everywhere else).
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String deadAddress = "127.0.0.1:" + MockServers.unusedPort();
                List<DiscoveredNode> roster = new ArrayList<>(cluster.discovery().nodes);
                roster.add(new DiscoveredNode("dead-new", deadAddress));
                cluster.discovery().nodes = roster;

                forceRefresh(client);

                Field membersField = NanocachedClient.class.getDeclaredField("members");
                membersField.setAccessible(true);
                Map<?, ?> members = (Map<?, ?>) membersField.get(client);
                assertTrue(members.containsKey("dead-new"), "unreachable new node must stay a member");
                assertNull(memberConnectionOf(client, "dead-new"));

                Field ringField = NanocachedClient.class.getDeclaredField("ring");
                ringField.setAccessible(true);
                HashRing ring = (HashRing) ringField.get(client);
                assertTrue(ring.owners("some-key".getBytes(StandardCharsets.UTF_8), 3).contains("dead-new"),
                        "the ring must be built with the unreachable node, like every peer's");

                Field cooldownsField = NanocachedClient.class.getDeclaredField("reconnectCooldowns");
                cooldownsField.setAccessible(true);
                assertTrue(((Map<?, ?>) cooldownsField.get(client)).containsKey(deadAddress),
                        "its reconnect cooldown must be armed");
                assertEquals(1, client.stats().refreshFailures());
            }
        }
    }

    private static void forceRefresh(NanocachedClient client) throws Exception {
        Method refreshNodeList = NanocachedClient.class.getDeclaredMethod("refreshNodeList");
        refreshNodeList.setAccessible(true);
        refreshNodeList.invoke(client);
    }

    // ── keep-alive isolates a hung node (issue #192) ───────────────

    @Test
    void keepAlivePingsTheOtherNodesWhileOneNodeIsHung() throws Exception {
        // The pings ran one after another on one scheduler thread, so a
        // half-open node (its G reply held 10s here; 30s request timeout in
        // the field) delayed the ping to every member after it until ~60s
        // idle, the server's idle limit.
        long originalInterval = NanocachedClient.keepAliveIntervalMillis;
        NanocachedClient.keepAliveIntervalMillis = 100;
        try (MockNode hung = new MockNode(); MockNode healthyA = new MockNode(); MockNode healthyB = new MockNode()) {
            hung.delayGets(10_000);
            // members iterates in roster order, so the hung node is pinged first.
            try (MockDiscovery discovery = new MockDiscovery(
                    List.of(new DiscoveredNode(NAMES.get(0), hung.address()),
                            new DiscoveredNode(NAMES.get(1), healthyA.address()),
                            new DiscoveredNode("healthy-b", healthyB.address())),
                    1)) {
                try (NanocachedClient client = connect("127.0.0.1", discovery.port())) {
                    waitFor(() -> healthyA.getCount.get() >= 3 && healthyB.getCount.get() >= 3,
                            "keep-alive pings to the healthy nodes behind the hung one");
                    // One ping is outstanding to the hung node; later ticks skip a
                    // connection whose previous ping has not returned.
                    assertEquals(1, hung.getCount.get());
                }
            }
        } finally {
            NanocachedClient.keepAliveIntervalMillis = originalInterval;
        }
    }

    // Issue #486: the replica-writer pool is bounded in both threads and
    // queue depth. Past both, the submitter runs the task itself (the same
    // synchronous fallback every call site has for the permit-exhausted
    // case) — and a pool that has been shut down still rejects, so the
    // call sites' existing "close() raced us" handling keeps working.
    @Test
    void replicaWriterPoolRunsOverflowInlineAndStillRejectsAfterShutdown() throws Exception {
        java.util.concurrent.ExecutorService pool = NanocachedClient.newReplicaWriterPool(1);
        try {
            java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch occupied = new java.util.concurrent.CountDownLatch(1);
            Runnable block = () -> {
                occupied.countDown();
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            };
            pool.execute(block);                       // occupies the single thread
            assertTrue(occupied.await(5, java.util.concurrent.TimeUnit.SECONDS));
            pool.execute(block);                       // fills the single queue slot

            Thread[] ranOn = new Thread[1];
            pool.execute(() -> ranOn[0] = Thread.currentThread());
            assertSame(Thread.currentThread(), ranOn[0], "overflow must run on the submitter, not be queued or dropped");

            release.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
            assertThrows(java.util.concurrent.RejectedExecutionException.class, () -> pool.execute(() -> {}),
                    "a shut-down pool must still reject rather than silently discard");
        } finally {
            pool.shutdownNow();
        }
    }

    // A request frame blocked mid-write (the server isn't reading) used to
    // hold the connection monitor, which the reader thread needs to
    // dispatch every response. With big values in both directions on one
    // pipelined connection that deadlocks: the server (which, like the real
    // one, reads a request and then writes its response) stops reading
    // requests while it is blocked writing responses, the reader (holding a
    // full response, waiting for the monitor) stops reading them, and
    // nothing moves until the request deadline poisons the connection --
    // failing every request pending on it, non-idempotent ones as "possibly
    // sent". Mirrors the Go and Rust SDKs' regression of the same shape.
    @Test
    void largeRequestsAndResponsesPipelinedTogetherDoNotDeadlock() throws Exception {
        byte[] big = new byte[1_000_000];
        Arrays.fill(big, (byte) 'x');
        String value = new String(big, StandardCharsets.ISO_8859_1);
        long original = Connection.requestTimeoutMillis;
        Connection.requestTimeoutMillis = 5_000;
        try (MockNode node = new MockNode()) {
            for (int i = 0; i < 4; i++) {
                node.store.put(MockNode.keyOf(("big-" + i).getBytes(StandardCharsets.UTF_8)), big);
            }
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                ExecutorService callers = Executors.newFixedThreadPool(96);
                try {
                    long start = System.nanoTime();
                    List<Future<?>> calls = new ArrayList<>();
                    for (int i = 0; i < 48; i++) {
                        int n = i;
                        calls.add(callers.submit(() -> client.get("big-" + (n % 4))));
                        calls.add(callers.submit(() -> client.set("set-" + n, value)));
                    }
                    for (Future<?> call : calls) {
                        call.get(20, java.util.concurrent.TimeUnit.SECONDS); // a failed request throws here
                    }
                    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                    // Loopback moves 96 MB in well under a second; a stall lasts
                    // until the request timeout.
                    assertTrue(elapsedMillis < 2_500,
                            "96 large requests took " + elapsedMillis + "ms, want far less than the 5000ms request timeout");
                } finally {
                    callers.shutdownNow();
                }
            }
        } finally {
            Connection.requestTimeoutMillis = original;
        }
    }

    // ── Reader thread: a RuntimeException poisons, not just IOException (issue #600) ──

    @Test
    void aRuntimeExceptionOnTheReaderThreadPoisonsTheConnectionAtOnce() throws Exception {
        // readLoop caught IOException and NanocachedException only; any
        // other RuntimeException out of the read path killed the reader
        // thread without poison(), so every pending request waited for
        // the watchdog (30 s) and the socket stayed open. Python's read
        // loop guards the same hole explicitly (issue #8). No networking:
        // a Socket whose input stream throws the moment it is read.
        java.io.InputStream throwing = new java.io.InputStream() {
            @Override
            public int read() {
                throw new IllegalStateException("simulated reader-thread failure");
            }
        };
        java.net.Socket socket = new java.net.Socket() {
            @Override
            public java.io.InputStream getInputStream() {
                return throwing;
            }

            @Override
            public java.io.OutputStream getOutputStream() {
                return java.io.OutputStream.nullOutputStream();
            }
        };
        java.util.concurrent.CountDownLatch closed = new java.util.concurrent.CountDownLatch(1);
        // Quiet the JVM's default handler for the reader thread's death on
        // the unfixed code, which is not what this test is about.
        Thread.UncaughtExceptionHandler original = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {});
        try {
            Connection connection = new Connection(socket, false, closed::countDown);
            assertTrue(closed.await(5, java.util.concurrent.TimeUnit.SECONDS),
                    "the reader thread's RuntimeException did not poison the connection");
            assertTrue(socket.isClosed(), "poison() closes the socket");
            NanocachedException.ConnectionFailed error = assertThrows(
                    NanocachedException.ConnectionFailed.class,
                    () -> connection.get(new byte[0], "k".getBytes(StandardCharsets.UTF_8)));
            // A poisoned connection refuses at once (notSent=true: the frame
            // never touched the wire) — no 30 s watchdog wait.
            assertTrue(error.getMessage().contains("closed"), error.getMessage());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(original);
        }
    }

    // ── a String with a lone surrogate is rejected, not encoded as '?' ──

    @Test
    void aStringWithALoneSurrogateIsRejectedInsteadOfCollidingWithAQuestionMark() throws Exception {
        // String.getBytes(UTF_8) silently turns a lone surrogate into '?',
        // so "x\uD800" and "x?" used to address the same entry: a read
        // under one key returned the other key's value.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("x?", "question");
                assertThrows(IllegalArgumentException.class, () -> client.set("x\uD800", "surrogate"));
                assertThrows(IllegalArgumentException.class, () -> client.get("x\uD800"));
                assertThrows(IllegalArgumentException.class, () -> client.set("k", "v\uDC00"));
                assertThrows(IllegalArgumentException.class, () -> client.namespace("ns\uD800"));
                assertThrows(IllegalArgumentException.class, () -> client.setMany(Map.of("x\uD800", "v")));
                assertThrows(IllegalArgumentException.class, () -> client.getMany(List.of("x\uD800")));
                assertEquals(Optional.of("question"), client.get("x?"));
                assertEquals(1, node.store.size());

                // A well-formed pair is an ordinary key.
                client.set("x😀", "emoji");
                assertEquals(Optional.of("emoji"), client.get("x😀"));
            }
        }
    }

    // ── getMany reads a repeated key once ───────────────────────────

    @Test
    void getManyReadsARepeatedKeyOnce() throws Exception {
        // A list of many copies of one key used to be sent as one `m`
        // frame per 400 copies with every frame's reply retained until
        // the call returned — hundreds of MB from a 60 KB request.
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "value");
                Map<String, String> values = client.getMany(java.util.Collections.nCopies(2_000, "k"));
                assertEquals(Map.of("k", "value"), values);
                assertEquals(1, node.multiGetCount.get());
                assertEquals(1, node.multiGetFrameBodyBytes.size());

                // The positional form keeps every position, sharing one
                // result between the copies of a key.
                byte[] k = "k".getBytes(StandardCharsets.UTF_8);
                byte[] missing = "missing".getBytes(StandardCharsets.UTF_8);
                byte[][] positional = client.getManyBytes(new byte[][] {k, missing, k});
                assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), positional[0]);
                assertNull(positional[1]);
                assertSame(positional[0], positional[2]);
                assertEquals(2, node.multiGetCount.get());
            }
        }
    }

    @Test
    void getManyMapsAPartialFailureBackOntoEveryPositionOfARepeatedKey() throws Exception {
        // The de-duplicated batch's unresolved indices and partial values
        // are expanded back to the caller's positions, so a repeated key
        // that stayed wrong-node is reported at each of its positions.
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String onA = keyOwnedBy(NAMES.get(0));
                String onB = keyOwnedBy(NAMES.get(1));
                client.set(onA, "a");
                client.set(onB, "b");
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                nodeA.answerWrongNodeOnce();
                nodeA.answerWrongNodeOnce();

                byte[][] keys = {
                    onA.getBytes(StandardCharsets.UTF_8),
                    onB.getBytes(StandardCharsets.UTF_8),
                    onA.getBytes(StandardCharsets.UTF_8),
                };
                NanocachedException.PartialWrongNodeRaw partial = assertThrows(
                        NanocachedException.PartialWrongNodeRaw.class, () -> client.getManyBytes(keys));
                assertArrayEquals(new int[] {0, 2}, partial.unresolvedIndices);
                assertEquals(3, partial.partialValues.length);
                assertNull(partial.partialValues[0]);
                assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), partial.partialValues[1]);
                assertNull(partial.partialValues[2]);
            }
        }
    }

    /** A key whose primary under {@link #NAMES} (R=1) is {@code owner}. */
    private static String keyOwnedBy(String owner) {
        HashRing ring = new HashRing(NAMES);
        for (int i = 0; ; i++) {
            String candidate = "key-" + i;
            if (ring.route(candidate.getBytes(StandardCharsets.UTF_8)).equals(owner)) return candidate;
        }
    }

    // ── setMany retries only the legs that failed ───────────────────

    @Test
    void batchedSetRetriesOnlyTheLegsThatFailedAndReportsAnUnreachablePrimaryAsAConnectionFailure()
            throws Exception {
        // With R=2 every key goes to both nodes. Node B closes on every
        // `o` frame: the retry pass used to re-send every rank of each
        // key whose primary (B) leg failed, including node A, which had
        // already stored it — A received 19 key-sends for 12 keys. And
        // the exhausted retry used to be reported as WrongNode although B
        // never answered `W`: it was unreachable.
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                MockNode nodeA = cluster.nodes().get(NAMES.get(0));
                MockNode nodeB = cluster.nodes().get(NAMES.get(1));
                nodeB.failAllMultiSets();

                Map<String, String> batch = new java.util.LinkedHashMap<>();
                Set<String> primaryOnA = new java.util.HashSet<>();
                HashRing ring = new HashRing(NAMES);
                for (int i = 0; i < 12; i++) {
                    String key = "key-" + i;
                    batch.put(key, "v" + i);
                    if (ring.route(key.getBytes(StandardCharsets.UTF_8)).equals(NAMES.get(0))) primaryOnA.add(key);
                }
                assertTrue(!primaryOnA.isEmpty() && primaryOnA.size() < 12, "want keys with each primary");

                NanocachedException.PartialConnectionFailedSet failure = assertThrows(
                        NanocachedException.PartialConnectionFailedSet.class, () -> client.setMany(batch));
                assertEquals(primaryOnA, failure.succeededKeys);
                assertEquals(12, nodeA.multiSetKeyCount.get(), "every key sent to A exactly once");
                assertEquals(12, nodeA.store.size());
            }
        }
    }

    // ── forced refreshes are single-flight ──────────────────────────

    @Test
    void concurrentCallersOnADeadNodeShareOneForcedRefresh() throws Exception {
        // Four callers that find their node dead at the same time used to
        // run four full refreshes one after another behind refreshLock,
        // the last caller waiting through all of them.
        try (Cluster cluster = startCluster(1)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = keyOwnedBy(NAMES.get(0));
                client.set(key, "v");
                cluster.nodes().get(NAMES.get(0)).close();
                cluster.discovery().laterListDelayMillis = 400;
                assertEquals(1, cluster.discovery().connectionCount.get());

                ExecutorService callers = Executors.newFixedThreadPool(4);
                try {
                    List<Future<Long>> elapsed = new ArrayList<>();
                    for (int i = 0; i < 4; i++) {
                        elapsed.add(callers.submit(() -> {
                            long started = System.nanoTime();
                            assertThrows(NanocachedException.class, () -> client.get(key));
                            return (System.nanoTime() - started) / 1_000_000;
                        }));
                    }
                    for (Future<Long> caller : elapsed) {
                        long millis = caller.get(10, java.util.concurrent.TimeUnit.SECONDS);
                        assertTrue(millis < 1_000, "a caller waited " + millis + "ms (serialized refreshes?)");
                    }
                } finally {
                    callers.shutdownNow();
                }
                assertEquals(2, cluster.discovery().connectionCount.get(), "one shared refresh");
            }
        }
    }

    // ── concurrent decompressions share one budget ──────────────────

    @Test
    void concurrentDecompressionsAreBoundedByTheClientWideBudget() throws Exception {
        // N concurrent reads of a compressed value each decompressed in
        // full at once, N × the per-value cap. With the budget lowered
        // to one byte, every reservation clamps to the whole budget, so
        // decompressions run one at a time.
        long savedBudget = Compression.maxInflightDecompressedBytes;
        Compression.maxInflightDecompressedBytes = 1;
        java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger peak = new java.util.concurrent.atomic.AtomicInteger();
        Compression.inflateTestHook = phase -> {
            if (phase.equals("start")) {
                int now = inFlight.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(30);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            } else {
                inFlight.decrementAndGet();
            }
        };
        try (MockNode node = new MockNode()) {
            try (NanocachedClient client =
                    NanocachedClient.connect(single("127.0.0.1", node.port()).compress(true))) {
                String value = "0123456789".repeat(200);
                client.set("k", value);
                ExecutorService readers = Executors.newFixedThreadPool(8);
                try {
                    List<Future<Optional<String>>> reads = new ArrayList<>();
                    for (int i = 0; i < 8; i++) reads.add(readers.submit(() -> client.get("k")));
                    for (Future<Optional<String>> read : reads) {
                        assertEquals(Optional.of(value), read.get(10, java.util.concurrent.TimeUnit.SECONDS));
                    }
                } finally {
                    readers.shutdownNow();
                }
                assertEquals(1, peak.get(), "decompressions ran concurrently past the budget");
            }
        } finally {
            Compression.inflateTestHook = null;
            Compression.maxInflightDecompressedBytes = savedBudget;
        }
    }

    // ── the identify exchange has one absolute deadline ─────────────

    @Test
    void aPeerThatTricklesTheRosterFailsTheDialAtTheDeadline() throws Exception {
        // The identify read timeout used to be per read (SO_TIMEOUT), so
        // a discovery server that sent one byte every few seconds held
        // connect() — and every refresh behind refreshLock — for as long
        // as the roster was long.
        int originalTimeout = Identify.connectTimeoutMillis;
        Identify.connectTimeoutMillis = 500;
        try (Cluster cluster = startCluster(1)) {
            cluster.discovery().listByteDelayMillis = 100; // the roster is >100 bytes: >10 s untreated
            long started = System.nanoTime();
            NanocachedException.ConnectionFailed error = assertThrows(
                    NanocachedException.ConnectionFailed.class,
                    () -> connect("127.0.0.1", cluster.discovery().port()));
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
            assertTrue(elapsedMillis < 2_000, "connect took " + elapsedMillis + "ms");
            assertTrue(error.getMessage().contains("identify exchange"), "unexpected failure: " + error.getMessage());
        } finally {
            Identify.connectTimeoutMillis = originalTimeout;
        }
    }

    // ── a stalled replica costs one request timeout, not two ────────

    @Test
    void aStalledReplicaBoundsAClusterSetByOneRequestTimeout() throws Exception {
        // A replica that acknowledged the handshake and then stopped
        // reading timed out the replica leg, which was then redialed and
        // re-sent for a second full timeout — set() returned after twice
        // the request deadline although the primary had answered at once.
        long defaultTimeout = Connection.requestTimeoutMillis;
        Connection.requestTimeoutMillis = 300;
        try (Cluster cluster = startCluster(2)) {
            try (NanocachedClient client = connect("127.0.0.1", cluster.discovery().port())) {
                String key = keyOwnedBy(NAMES.get(0));
                client.set(key, "warm");
                MockNode replica = cluster.nodes().get(NAMES.get(1));
                int connectionsBefore = replica.connectionCount.get();
                replica.goSilentAfterHandshake();

                long started = System.nanoTime();
                client.set(key, "v");
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                assertTrue(elapsedMillis >= 250 && elapsedMillis < 550,
                        "set took " + elapsedMillis + "ms, want about one timeout");
                assertEquals(connectionsBefore, replica.connectionCount.get(), "no redial after a timeout");
                assertEquals(1, client.stats().replicaWriteFailures());
            }
        } finally {
            Connection.requestTimeoutMillis = defaultTimeout;
        }
    }
}
