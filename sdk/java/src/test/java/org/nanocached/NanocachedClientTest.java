package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        // verified to round-trip its own value (doc/adr/0016-*.md) — a
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

    // ── 値の圧縮 (doc/adr/0013-*.md) ────────────────────────────────

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
            // to collide with the DEFLATE marker (0x01) — doc/adr/0013-*.md's
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
                NanocachedException.ConnectionFailed firstError = assertThrows(
                        NanocachedException.ConnectionFailed.class, () -> client.get("k"));

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
                assertThrows(NanocachedException.ConnectionFailed.class, () -> client.get("k"));

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
                waitFor(() -> {
                    try {
                        client.get("k");
                        return false;
                    } catch (NanocachedException.ConnectionFailed expected) {
                        return true;
                    }
                }, "the redial to the closed node to fail with ConnectionFailed");

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
                // The client's retry layer redials once after the first
                // timeout; the redialed connection times out too, so this
                // settles after roughly two windows — still bounded.
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
        // and a timeout must NOT be mistaken for a pre-ADR-0019 server,
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

    // ── fire-and-forget レプリカ書き込み (doc/adr/0014-*.md) ──────────

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

    // ── read repair (doc/adr/0015-*.md) ────────────────────────────

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
                    // The dial may connect more than once (the ADR-0019
                    // legacy fallback redials after the first connection
                    // dies), so accept in a loop and track every socket.
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
                    silent.close();
                    synchronized (acceptedSockets) {
                        for (java.net.Socket socket : acceptedSockets) socket.close();
                    }
                    refresher.join(5_000);
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
        java.lang.reflect.Method write =
                NanocachedClient.class.getDeclaredMethod("write", byte[].class, connectionOpClass());
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
                writeMethod().invoke(client, key, op); // must not throw
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
                        () -> writeMethod().invoke(client, key, op));
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

    // ── 応答タグ (doc/adr/0019-*.md) ──────────────────────────────

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
        // The exact misdelivery doc/adr/0016-*.md left open: the server
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
        // An old server treats `A ... T` as a parse error and closes
        // without replying; the client must redial once with the plain
        // form and run untagged — transparently, with the same results.
        try (MockNode node = MockNode.legacyServer()) {
            try (NanocachedClient client = connect("127.0.0.1", node.port())) {
                client.set("k", "v");
                assertEquals(Optional.of("v"), client.get("k"));
                // Two dials: the extended attempt the server slammed
                // shut, then the plain fallback that stuck.
                assertEquals(2, node.connectionCount.get());
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
}
