package org.nanocached;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/**
 * In-process stand-ins for nanocached-node and nanocached-discovery,
 * speaking just enough of the wire protocol for client tests to run over
 * real TCP without the Rust binaries. Mirrors the TypeScript/Python mocks.
 */
final class MockServers {

    private MockServers() {}

    static final class MockNode implements AutoCloseable {
        final Map<String, byte[]> store = new ConcurrentHashMap<>();
        /** Namespaces (issue #105): one store per non-empty namespace,
         * keyed by the namespace's {@link #keyOf} string form — the
         * default (empty) namespace keeps using {@link #store} above, so
         * every pre-existing test that inspects {@code store} directly is
         * unaffected. Isolation between namespaces (and between a
         * namespace and the default keyspace) falls out of these simply
         * being different maps. */
        final Map<String, Map<String, byte[]>> namespacedStores = new ConcurrentHashMap<>();
        /** Per-key TTL (whole seconds; absent = no expiry), default
         * namespace — populated by `S`/`s` and read back by `i` (issue
         * #129) so a mock node can answer INCR's own optional
         * {@code <ttl-seconds>} trailing field truthfully. Nothing else
         * (get/delete/clear included) needs a key's TTL, so this is the
         * only place it's tracked. */
        final Map<String, Long> ttls = new ConcurrentHashMap<>();
        /** As {@link #ttls}, one map per non-empty namespace — mirrors
         * {@link #namespacedStores}. */
        final Map<String, Map<String, Long>> namespacedTtls = new ConcurrentHashMap<>();
        final AtomicInteger connectionCount = new AtomicInteger();
        final AtomicInteger getCount = new AtomicInteger();
        /** Counts every `i` (INCR, issue #129) frame received — lets a
         * test prove a replica never receives one (only the primary owner
         * ever should; a replica gets the result forwarded as an ordinary
         * `set` instead). */
        final AtomicInteger incrCount = new AtomicInteger();
        /** Counts every `k` (compare-and-set store, issue #141) frame
         * received — lets a test prove a replica never receives one (only
         * the primary owner ever should; a replica gets the result
         * forwarded as an ordinary `set` instead). */
        final AtomicInteger casSetCount = new AtomicInteger();
        /** As {@link #casSetCount}, for `x` (compare-and-set delete, issue
         * #141) frames. */
        final AtomicInteger casDeleteCount = new AtomicInteger();
        /** Counts every `m` (multi-get, issue #151) frame received. */
        final AtomicInteger multiGetCount = new AtomicInteger();
        /** Counts every `o` (multi-set, issue #151) frame received. */
        final AtomicInteger multiSetCount = new AtomicInteger();
        /** Every `m` frame's body byte count (namespace + every key's
         * bytes, header line excluded) in receipt order (issue #222) —
         * lets a test prove request chunking really did keep each
         * sub-frame under the server's MAX_REQUEST_SIZE rather than just
         * counting how many sub-frames arrived. */
        final List<Integer> multiGetFrameBodyBytes = new CopyOnWriteArrayList<>();
        /** As {@link #multiGetFrameBodyBytes}, for `o` (issue #222):
         * namespace + every key's and value's bytes. */
        final List<Integer> multiSetFrameBodyBytes = new CopyOnWriteArrayList<>();
        /** Counts every `g`/`s`/`d` frame received — never incremented by
         * `G`/`S`/`D`. Lets a test prove the empty (default) namespace
         * really does send the legacy frame, not `g 0 ...`/etc (issue
         * #105's SDK rule). */
        final AtomicInteger namespacedCommandCount = new AtomicInteger();
        /** Counts every `c` (clear one namespace) frame received (issue
         * #106) — lets a test prove a clear/clearAll fanned out reaches
         * every node. */
        final AtomicInteger clearCount = new AtomicInteger();
        /** Counts every `F` (flush everything) frame received (issue
         * #106). */
        final AtomicInteger clearAllCount = new AtomicInteger();
        private final AtomicInteger wrongNodeReplies = new AtomicInteger();
        /** echoed response tags: queued one-off replies that echo the WRONG tag (the
         * request's tag + 1) — the desync a pre-tag stream
         * misalignment would produce. Only takes effect on a tagged
         * connection. */
        private final AtomicInteger wrongTagReplies = new AtomicInteger();
        /** echoed response tags: swallows the next G entirely (no reply) — the
         * off-by-one stream desync where every later response answers
         * the previous request. */
        private final AtomicInteger swallowedGets = new AtomicInteger();
        private final AtomicInteger malformedValueReplies = new AtomicInteger();
        private final AtomicInteger storedToGetReplies = new AtomicInteger();
        /** Queues a one-off G reply whose `V` header never terminates with
         * '\n' — regression coverage for the unbounded readLine() growth
         * fix (issue: audit finding, MAX_HEADER_LINE_LENGTH). */
        private final AtomicInteger runawayHeaderReplies = new AtomicInteger();
        /** Queues a one-off untagged fixed-shape (`N`) reply whose second
         * byte is something other than '\n' — regression coverage for the
         * unverified-trailing-byte fix (issue: audit finding, "connection
         * desynced" on a bad trailer). */
        private final AtomicInteger badTrailerReplies = new AtomicInteger();
        /** Every `A` header line this node received verbatim (issue
         * #125) — e.g. {@code "A 1 T R"} — across every dial attempt
         * including any fallback redials, so a test can assert the exact
         * probe form the client sent. */
        final List<String> authHeaders = new CopyOnWriteArrayList<>();
        /** Countdown of data requests (`G`/`S`/`D`/`g`/`s`/`d`/`c`/`F`) to
         * answer with a transient `R` instead of their real reply (issue
         * #125), tagged correctly on a tagged connection. */
        private final AtomicInteger retryableReplies = new AtomicInteger();
        private volatile long setDelayMillis = 0;
        private volatile long getDelayMillis = 0;
        private volatile boolean failSets = false;
        /** One-off connection resets queued for the next `c`/`F` frame(s)
         * (issue #106), mirroring {@link #wrongNodeReplies}/{@link
         * #takeWrongNode()} rather than {@link #failSets}'s permanent
         * switch — a test needs to arm exactly N failures (e.g. 2, to
         * outlast a single fan-out pass's own applyReconnecting redial)
         * and then let a later attempt succeed. */
        private final AtomicInteger clearFailures = new AtomicInteger();
        private volatile boolean silent = false;
        /** The TTL (whole seconds; 0 if omitted on the wire) from the
         * most recent S request this server received. */
        volatile long lastSetTtl = 0;
        private final byte[] requiredSecret;
        /** echoed response tags: acknowledge an extended `A ... T` with `OnT\n` and
         * echo tags on that connection's G/S/D replies. Off by default so
         * the bulk of the suite keeps exercising the legacy untagged
         * path. */
        private final boolean supportTags;
        /** echoed response tags: behave like a pre-0019 server — an extended
         * `A ... T` is a parse error, so close the connection without
         * replying. */
        private final boolean closeOnExtendedAuth;
        /** issue #125: behave like a server that supports the `T` tag
         * capability but predates the further-extended `R` retryable-error
         * token — accepts `A <len> T` normally, but the doubly-extended
         * `A <len> T R` is a parse error to it, so close without replying
         * (forcing the client's middle fallback stage: full → tags-only).
         * Independent of {@link #closeOnExtendedAuth}, which closes on
         * either extension. */
        private final boolean closeOnRetryToken;
        private final ServerSocket server;
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();

        MockNode() throws IOException {
            this(null, false, false, null, 0);
        }

        MockNode(byte[] requiredSecret) throws IOException {
            this(requiredSecret, false, false, null, 0);
        }

        /** Listens on a caller-chosen port instead of an ephemeral one —
         * for tests that need a node to come back up on the exact address
         * discovery already listed (issue #67, redial after a bootstrap
         * dial failed and the address's cooldown has passed). */
        static MockNode onPort(int port) throws IOException {
            return new MockNode(null, false, false, null, port);
        }

        /** echoed response tags: a node that negotiates tags — accepts `A ... T`
         * with `OnT\n` and echoes tags on that connection's replies. */
        static MockNode withTagSupport() throws IOException {
            return new MockNode(null, true, false, null);
        }

        /** echoed response tags: a pre-0019 node — the extended `A ... T` is a parse
         * error, so it closes the connection without replying, forcing
         * the caller's transparent untagged fallback. */
        static MockNode legacyServer() throws IOException {
            return new MockNode(null, false, true, null);
        }

        /** issue #125: a node that supports `T` (issue #19) but predates
         * `R` — accepts `A <len> T` exactly like {@link #withTagSupport},
         * but closes without replying on the further-extended `A <len> T
         * R`, forcing the client's middle fallback stage (full →
         * tags-only) rather than all the way down to plain. */
        static MockNode predatesRetryCapability() throws IOException {
            return new MockNode(null, true, false, null, 0, true);
        }

        /** J1: a node that speaks TLS, presenting whatever certificate
         * {@code serverTls} was built with (see {@link Tls#generate}).
         * Everything past the handshake (A/G/S/D) is identical to a plain
         * MockNode — an {@link javax.net.ssl.SSLSocket} is a {@link
         * Socket}, so {@link #serve} needs no TLS-specific code at all. */
        static MockNode withTls(SSLContext serverTls) throws IOException {
            return new MockNode(null, false, false, serverTls);
        }

        private MockNode(byte[] requiredSecret, boolean supportTags, boolean closeOnExtendedAuth,
                SSLContext serverTls) throws IOException {
            this(requiredSecret, supportTags, closeOnExtendedAuth, serverTls, 0);
        }

        private MockNode(byte[] requiredSecret, boolean supportTags, boolean closeOnExtendedAuth,
                SSLContext serverTls, int port) throws IOException {
            this(requiredSecret, supportTags, closeOnExtendedAuth, serverTls, port, false);
        }

        private MockNode(byte[] requiredSecret, boolean supportTags, boolean closeOnExtendedAuth,
                SSLContext serverTls, int port, boolean closeOnRetryToken) throws IOException {
            this.requiredSecret = requiredSecret;
            this.supportTags = supportTags;
            this.closeOnExtendedAuth = closeOnExtendedAuth;
            this.closeOnRetryToken = closeOnRetryToken;
            this.server = serverTls == null
                    ? new ServerSocket(port)
                    : serverTls.getServerSocketFactory().createServerSocket(port);
            Thread acceptor = new Thread(this::acceptLoop, "mock-node-accept");
            acceptor.setDaemon(true);
            acceptor.start();
            threads.add(acceptor);
        }

        int port() {
            return server.getLocalPort();
        }

        String address() {
            return "127.0.0.1:" + port();
        }

        void answerWrongNodeOnce() {
            wrongNodeReplies.incrementAndGet();
        }

        /** Queue a one-off reply for the next G request on a tagged
         * connection that echoes the WRONG tag (echoed response tags). */
        void answerWrongTagOnce() {
            wrongTagReplies.incrementAndGet();
        }

        /** Swallow the next G request entirely (no reply) — the
         * off-by-one stream desync where every later response answers
         * the previous request (echoed response tags). */
        void swallowGetOnce() {
            swallowedGets.incrementAndGet();
        }

        /** Queue a one-off garbage `V` header for the next G request. */
        void answerMalformedValueOnce() {
            malformedValueReplies.incrementAndGet();
        }

        /** Reply {@code S} to the next G — a well-formed frame of the
         * wrong kind, as a desynced (off-by-one) stream would produce. */
        void answerStoredToGetOnce() {
            storedToGetReplies.incrementAndGet();
        }

        /** Queue a one-off G reply that streams a `V` marker followed by
         * bytes with no '\n' ever — a malicious/buggy node's header that
         * would otherwise grow the client's line buffer without bound. */
        void answerRunawayHeaderOnce() {
            runawayHeaderReplies.incrementAndGet();
        }

        /** Queue a one-off G miss (`N`) reply whose second byte is not
         * '\n' — the untagged fixed-shape response is always exactly two
         * bytes on the wire, so anything else here is a desync. */
        void answerBadTrailerOnce() {
            badTrailerReplies.incrementAndGet();
        }

        /** Answer the next {@code count} data requests
         * (`G`/`S`/`D`/`g`/`s`/`d`/`c`/`F`) with a transient `R` instead
         * of their real reply — issue #125's retryable-error status,
         * tagged correctly (`R <tag>`) on a tagged connection. */
        void answerRetryableFor(int count) {
            retryableReplies.addAndGet(count);
        }

        private boolean takeRetryable() {
            while (true) {
                int pending = retryableReplies.get();
                if (pending == 0) return false;
                if (retryableReplies.compareAndSet(pending, pending - 1)) return true;
            }
        }

        /** Holds every future S reply for {@code millis} first — for tests
         * proving a caller isn't blocked on a slow replica leg
         * (fire-and-forget replica writes). */
        void delaySets(long millis) {
            setDelayMillis = millis;
        }

        /** Holds every future G reply for {@code millis} first — a
         * slow-but-alive node, for hedged-read tests (issue #64). */
        void delayGets(long millis) {
            getDelayMillis = millis;
        }

        /** Drops the connection (server-side reset) on every S instead of
         * acking it, so a write here fails with a connection error — for
         * tests that need a repair/replica write to deterministically
         * fail without racing a close(). Reads (G) are unaffected, so a
         * node can still miss the initial lookup that triggers the repair. */
        void failSets() {
            failSets = true;
        }

        /** Queue one connection reset (server-side, no reply) for the
         * next `c`/`F` frame instead of acking it — for tests of
         * clear()/clearAll()'s partial-failure and refresh-and-retry
         * paths (issue #106). Call it twice to outlast a single fan-out
         * pass's own {@code applyReconnecting} redial-retry. */
        void failClearOnce() {
            clearFailures.incrementAndGet();
        }

        private boolean takeClearFailure() {
            while (true) {
                int pending = clearFailures.get();
                if (pending == 0) return false;
                if (clearFailures.compareAndSet(pending, pending - 1)) return true;
            }
        }

        /** Makes this node a half-open server from this point on: it
         * still accepts and completes the A handshake, and still reads
         * every request frame off the wire (so the TCP stream stays
         * well-formed), but never writes a reply — regression coverage
         * for the request timeout (issue #42), mirroring the Go suite's
         * hook of the same name. */
        void goSilentAfterHandshake() {
            silent = true;
        }

        /** Server-side FIN on every open connection, like the idle timeout. */
        void dropConnections() {
            for (Socket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Best-effort.
                }
            }
        }

        @Override
        public void close() throws IOException {
            // Listener first, then the accepted sockets: in the other
            // order a connection accepted between the two steps outlives
            // close() and its serve thread keeps answering from the
            // (intact) store — seen on GitHub's ubuntu runners as a get
            // against a "closed" node returning the stored value for
            // seconds (reconnectCooldown* tests, 2026-08-21).
            server.close();
            dropConnections();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    sockets.add(socket);
                    if (server.isClosed()) {
                        // Raced close(): dropConnections() may already
                        // have run, so this socket is ours to close.
                        socket.close();
                        return;
                    }
                    connectionCount.incrementAndGet();
                    Thread worker = new Thread(() -> serve(socket), "mock-node-conn");
                    worker.setDaemon(true);
                    worker.start();
                    threads.add(worker);
                } catch (IOException stop) {
                    return;
                }
            }
        }

        private void serve(Socket socket) {
            try (socket) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                // Echoed response tags: set when this connection's `A ... T` was
                // acknowledged — its requests then carry a trailing tag
                // the replies must echo.
                boolean tagged = false;
                while (true) {
                    String[] parts = readLine(in).split(" ");
                    // On a tagged connection every request's last header
                    // field is its tag, echoed back as each reply's own
                    // last field.
                    String tagSuffix = tagged ? " " + parts[parts.length - 1] : "";

                    switch (parts[0]) {
                        case "A" -> {
                            // issue #125: record the exact probe form
                            // received, across every dial attempt this
                            // connection's caller makes (including
                            // fallback redials to a fresh MockNode
                            // connection — each gets its own `serve`
                            // call, so this always reflects that one
                            // attempt's header).
                            authHeaders.add(String.join(" ", parts));
                            if (parts.length > 2 && closeOnExtendedAuth) {
                                return; // pre-0019 behavior: close without replying
                            }
                            if (parts.length > 3 && closeOnRetryToken) {
                                // Predates the `R` capability token (issue
                                // #125): the plain `T` extension it
                                // understands is fine, but the further
                                // `T R` form is a parse error to it.
                                return;
                            }
                            byte[] secret = in.readNBytes(Integer.parseInt(parts[1]));
                            boolean accepted = requiredSecret == null
                                    ? secret.length > 0
                                    : java.util.Arrays.equals(secret, requiredSecret);
                            tagged = accepted && supportTags && parts.length > 2 && parts[2].equals("T");
                            out.write((accepted ? (tagged ? "OnT\n" : "On\n") : "En\n")
                                    .getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            if (!accepted) return;
                        }
                        case "G" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            getCount.incrementAndGet();

                            if (getDelayMillis > 0) {
                                try {
                                    Thread.sleep(getDelayMillis);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (swallowedGets.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                break; // no reply — simulates an off-by-one stream desync
                            }
                            if (tagged && takeWrongTag()) {
                                // Echo the WRONG tag (the request's tag +
                                // 1) — the desync a pre-tag stream
                                // misalignment would produce.
                                long wrongTag = Long.parseLong(parts[2]) + 1;
                                out.write(("N " + wrongTag + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (malformedValueReplies.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                out.write("V x\n".getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (storedToGetReplies.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                out.write(("S" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (runawayHeaderReplies.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                // 'V' followed by 5 KiB with no '\n' — never
                                // a legal header, so the client must fail
                                // fast (MAX_HEADER_LINE_LENGTH) instead of
                                // growing its line buffer forever.
                                byte[] junk = new byte[5 * 1024];
                                java.util.Arrays.fill(junk, (byte) 'x');
                                out.write('V');
                                out.write(junk);
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                byte[] value = store.get(key);
                                if (value == null) {
                                    if (badTrailerReplies.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                        // Two bytes, as the untagged form
                                        // always is, but the second isn't
                                        // '\n' — a desync the client must
                                        // catch rather than silently accept.
                                        out.write(("N" + tagSuffix).getBytes(StandardCharsets.US_ASCII));
                                        out.write('X');
                                    } else {
                                        out.write(("N" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                    }
                                } else {
                                    out.write(("V " + value.length + tagSuffix + "\n")
                                            .getBytes(StandardCharsets.US_ASCII));
                                    out.write(value);
                                }
                            }
                            out.flush();
                        }
                        case "S" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            byte[] value = in.readNBytes(Integer.parseInt(parts[2]));
                            // The TTL, when present, is the field after
                            // the two lengths (omitted on the wire means
                            // "no expiry", i.e. 0); on a tagged connection
                            // the tag sits after it as the last field.
                            int ttlFieldCount = parts.length - (tagged ? 4 : 3);
                            lastSetTtl = ttlFieldCount > 0 ? Long.parseLong(parts[3]) : 0;
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (failSets) {
                                // Reset the connection instead of acking:
                                // the frame is fully consumed above, so the
                                // client sees a clean connection error on
                                // the write, not a desync.
                                return;
                            }
                            if (setDelayMillis > 0) {
                                try {
                                    Thread.sleep(setDelayMillis);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                store.put(key, value);
                                namespacedPutTtl("", key, lastSetTtl);
                                out.write(("S" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        case "D" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                out.write((store.remove(key) != null ? "D" + tagSuffix + "\n" : "N" + tagSuffix + "\n")
                                        .getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        // Namespaces (issue #105): the lowercase counterparts
                        // of G/S/D — one extra leading <namespace-length>
                        // header field, namespace bytes leading the body,
                        // everything else (including the response markers)
                        // identical. Only the happy path plus takeWrongNode
                        // is reproduced here — the exotic desync-injection
                        // hooks above are exercised exclusively via the
                        // uppercase commands already, and namespaces don't
                        // change any of that machinery.
                        case "g" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[2])));
                            getCount.incrementAndGet();
                            namespacedCommandCount.incrementAndGet();

                            if (getDelayMillis > 0) {
                                try {
                                    Thread.sleep(getDelayMillis);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                byte[] value = namespacedGet(ns, key);
                                if (value == null) {
                                    out.write(("N" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                } else {
                                    out.write(("V " + value.length + tagSuffix + "\n")
                                            .getBytes(StandardCharsets.US_ASCII));
                                    out.write(value);
                                }
                            }
                            out.flush();
                        }
                        case "s" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[2])));
                            byte[] value = in.readNBytes(Integer.parseInt(parts[3]));
                            namespacedCommandCount.incrementAndGet();
                            // As the "S" case above: the TTL, when present,
                            // is the field after the three lengths; the tag
                            // (on a tagged connection) sits after that as
                            // the last field.
                            int ttlFieldCount = parts.length - (tagged ? 5 : 4);
                            lastSetTtl = ttlFieldCount > 0 ? Long.parseLong(parts[4]) : 0;
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (failSets) {
                                return;
                            }
                            if (setDelayMillis > 0) {
                                try {
                                    Thread.sleep(setDelayMillis);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                namespacedPut(ns, key, value);
                                namespacedPutTtl(ns, key, lastSetTtl);
                                out.write(("S" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        // INCR/DECR (issue #129): always namespaced on the
                        // wire (0-length namespace = default), unlike G/S/D
                        // there is no separate uppercase form. A missing
                        // key answers `N`; a non-numeric stored value or an
                        // overflowing delta answers `T`; a hit answers `I
                        // <value-length> [<ttl-seconds>] [<tag>]` — the ttl
                        // field mirrors this entry's own live TTL (tracked
                        // by S/s above), present only when one was set.
                        case "i" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[2])));
                            long delta = Long.parseLong(parts[3]);
                            incrCount.incrementAndGet();
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            byte[] existing = namespacedGet(ns, key);
                            if (existing == null) {
                                out.write(("N" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            long updated;
                            try {
                                updated = Math.addExact(
                                        Long.parseLong(new String(existing, StandardCharsets.US_ASCII)), delta);
                            } catch (NumberFormatException | ArithmeticException notNumeric) {
                                out.write(("T" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            byte[] updatedValue = Long.toString(updated).getBytes(StandardCharsets.US_ASCII);
                            namespacedPut(ns, key, updatedValue);
                            Long ttl = namespacedTtl(ns, key);
                            String ttlField = ttl == null ? "" : " " + ttl;
                            out.write(("I " + updatedValue.length + ttlField + tagSuffix + "\n")
                                    .getBytes(StandardCharsets.US_ASCII));
                            out.write(updatedValue);
                            out.flush();
                        }
                        // Compare-and-set (issue #141): always namespaced on
                        // the wire (0-length namespace = default), like
                        // INCR. <cond> is a bare, non-length-prefixed token
                        // — "A" (absent), "P" (present), or a 32-character
                        // lowercase hex digest (exact content match,
                        // computed the same way NanocachedClient.contentDigest
                        // does, independently here so a bug in one isn't
                        // masked by the other). `k` reuses S/N (no new
                        // marker); `x` reuses D/N.
                        case "k" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[2])));
                            byte[] value = in.readNBytes(Integer.parseInt(parts[3]));
                            String cond = parts[4];
                            // <ttl-seconds>, when present, is the field
                            // after <cond>; the tag (on a tagged
                            // connection) sits after that as the last
                            // field — mirrors the "s" case's own layout.
                            int ttlFieldCount = parts.length - (tagged ? 6 : 5);
                            long ttlSeconds = ttlFieldCount > 0 ? Long.parseLong(parts[5]) : 0;
                            casSetCount.incrementAndGet();
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            byte[] existing = namespacedGet(ns, key);
                            boolean matches = switch (cond) {
                                case "A" -> existing == null;
                                case "P" -> existing != null;
                                default -> existing != null && digestOf(existing).equals(cond);
                            };
                            if (matches) {
                                namespacedPut(ns, key, value);
                                namespacedPutTtl(ns, key, ttlSeconds);
                                out.write(("S" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                out.write(("N" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        case "x" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[2])));
                            String cond = parts[3];
                            casDeleteCount.incrementAndGet();
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            byte[] existing = namespacedGet(ns, key);
                            if (existing != null && digestOf(existing).equals(cond)) {
                                namespacedRemove(ns, key);
                                out.write(("D" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                out.write(("N" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        // Batched get/set (issue #151): always namespaced on
                        // the wire (0-length namespace = default), like
                        // INCR/CAS — no legacy pre-namespace form. `m`
                        // answers `M`; `o` answers `O`. A single mock node's
                        // whole received frame answers `W` uniformly when
                        // takeWrongNode() is armed, since a real node never
                        // owns some-but-not-all of a frame's keys the client
                        // itself already grouped by owner (only the client's
                        // routing table could be stale for the entire
                        // group at once).
                        case "m" -> {
                            int nsLen = Integer.parseInt(parts[1]);
                            String ns = keyOf(in.readNBytes(nsLen));
                            int n = Integer.parseInt(parts[2]);
                            String[] keys = new String[n];
                            int bodyBytes = nsLen;
                            for (int i = 0; i < n; i++) {
                                int keyLen = Integer.parseInt(parts[3 + i]);
                                bodyBytes += keyLen;
                                keys[i] = keyOf(in.readNBytes(keyLen));
                            }
                            multiGetCount.incrementAndGet();
                            multiGetFrameBodyBytes.add(bodyBytes);
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            boolean wrongNode = takeWrongNode();
                            StringBuilder header = new StringBuilder("M ").append(n);
                            ByteArrayOutputStream body = new ByteArrayOutputStream();
                            for (String key : keys) {
                                if (wrongNode) {
                                    header.append(" W");
                                    continue;
                                }
                                byte[] value = namespacedGet(ns, key);
                                if (value == null) {
                                    header.append(" -");
                                } else {
                                    header.append(' ').append(value.length);
                                    body.write(value, 0, value.length);
                                }
                            }
                            header.append(tagSuffix).append('\n');
                            out.write(header.toString().getBytes(StandardCharsets.US_ASCII));
                            out.write(body.toByteArray());
                            out.flush();
                        }
                        case "o" -> {
                            int nsLen = Integer.parseInt(parts[1]);
                            String ns = keyOf(in.readNBytes(nsLen));
                            int n = Integer.parseInt(parts[2]);
                            int[] keyLens = new int[n];
                            int[] valueLens = new int[n];
                            int bodyBytes = nsLen;
                            for (int i = 0; i < n; i++) {
                                keyLens[i] = Integer.parseInt(parts[3 + i * 2]);
                                valueLens[i] = Integer.parseInt(parts[4 + i * 2]);
                                bodyBytes += keyLens[i] + valueLens[i];
                            }
                            int fixedFieldCount = 3 + n * 2;
                            int ttlFieldCount = parts.length - fixedFieldCount - (tagged ? 1 : 0);
                            long ttlSeconds = ttlFieldCount > 0 ? Long.parseLong(parts[fixedFieldCount]) : 0;
                            String[] keys = new String[n];
                            byte[][] values = new byte[n][];
                            for (int i = 0; i < n; i++) {
                                keys[i] = keyOf(in.readNBytes(keyLens[i]));
                                values[i] = in.readNBytes(valueLens[i]);
                            }
                            multiSetCount.incrementAndGet();
                            multiSetFrameBodyBytes.add(bodyBytes);
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            boolean wrongNode = takeWrongNode();
                            StringBuilder header = new StringBuilder("O ").append(n);
                            for (int i = 0; i < n; i++) {
                                if (wrongNode) {
                                    header.append(" W");
                                } else {
                                    namespacedPut(ns, keys[i], values[i]);
                                    namespacedPutTtl(ns, keys[i], ttlSeconds);
                                    header.append(" S");
                                }
                            }
                            header.append(tagSuffix).append('\n');
                            out.write(header.toString().getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                        }
                        case "d" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[2])));
                            namespacedCommandCount.incrementAndGet();
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                out.write((namespacedRemove(ns, key) != null
                                        ? "D" + tagSuffix + "\n" : "N" + tagSuffix + "\n")
                                        .getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        // CLEAR namespace / flush everything (issue #106):
                        // an O(1) sub-map drop, never key-addressed, so
                        // unlike G/S/D/g/s/d there is no W to answer with —
                        // only C (or, via failClearOnce(), a dropped
                        // connection).
                        case "c" -> {
                            String ns = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            clearCount.incrementAndGet();
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeClearFailure()) {
                                return; // server-side reset instead of acking
                            }
                            if (ns.isEmpty()) {
                                store.clear();
                                ttls.clear();
                            } else {
                                namespacedStores.remove(ns);
                                namespacedTtls.remove(ns);
                            }
                            out.write(("C" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                        }
                        case "F" -> {
                            clearAllCount.incrementAndGet();
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeRetryable()) {
                                out.write(("R" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeClearFailure()) {
                                return; // server-side reset instead of acking
                            }
                            store.clear();
                            namespacedStores.clear();
                            ttls.clear();
                            namespacedTtls.clear();
                            out.write(("C" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                        }
                        default -> {
                            return;
                        }
                    }
                }
            } catch (IOException | RuntimeException done) {
                // Connection closed — normal end of a mock session.
            } finally {
                sockets.remove(socket);
            }
        }

        private boolean takeWrongNode() {
            while (true) {
                int pending = wrongNodeReplies.get();
                if (pending == 0) return false;
                if (wrongNodeReplies.compareAndSet(pending, pending - 1)) return true;
            }
        }

        private boolean takeWrongTag() {
            while (true) {
                int pending = wrongTagReplies.get();
                if (pending == 0) return false;
                if (wrongTagReplies.compareAndSet(pending, pending - 1)) return true;
            }
        }

        // Namespaces (issue #105): an empty namespace addresses the same
        // default keyspace `G`/`S`/`D` do (protocol.html) — routed to
        // `store` directly rather than `namespacedStores.get("")`, so it's
        // the exact same map a legacy-frame test already asserts against.
        private byte[] namespacedGet(String ns, String key) {
            if (ns.isEmpty()) return store.get(key);
            Map<String, byte[]> nsStore = namespacedStores.get(ns);
            return nsStore == null ? null : nsStore.get(key);
        }

        private void namespacedPut(String ns, String key, byte[] value) {
            if (ns.isEmpty()) {
                store.put(key, value);
                return;
            }
            namespacedStores.computeIfAbsent(ns, ignored -> new ConcurrentHashMap<>()).put(key, value);
        }

        private byte[] namespacedRemove(String ns, String key) {
            if (ns.isEmpty()) {
                ttls.remove(key);
                return store.remove(key);
            }
            Map<String, byte[]> nsStore = namespacedStores.get(ns);
            Map<String, Long> nsTtls = namespacedTtls.get(ns);
            if (nsTtls != null) nsTtls.remove(key);
            return nsStore == null ? null : nsStore.remove(key);
        }

        /** As {@link #namespacedPut}, for a key's TTL (issue #129) —
         * {@code ttlSeconds <= 0} means no expiry, matching the wire's own
         * "omitted means no TTL" convention (see {@link #lastSetTtl}). */
        private void namespacedPutTtl(String ns, String key, long ttlSeconds) {
            if (ns.isEmpty()) {
                if (ttlSeconds > 0) ttls.put(key, ttlSeconds); else ttls.remove(key);
                return;
            }
            Map<String, Long> nsTtls = namespacedTtls.computeIfAbsent(ns, ignored -> new ConcurrentHashMap<>());
            if (ttlSeconds > 0) nsTtls.put(key, ttlSeconds); else nsTtls.remove(key);
        }

        /** As {@link #namespacedGet}, for a key's TTL (issue #129) —
         * {@code null} means no TTL. */
        private Long namespacedTtl(String ns, String key) {
            if (ns.isEmpty()) return ttls.get(key);
            Map<String, Long> nsTtls = namespacedTtls.get(ns);
            return nsTtls == null ? null : nsTtls.get(key);
        }

        static String keyOf(byte[] key) {
            return new String(key, StandardCharsets.ISO_8859_1);
        }

        /** The CAS digest (issue #141): SHA-256 of {@code value}, truncated
         * to its first 16 bytes, lowercase hex-encoded. Deliberately its
         * own independent implementation rather than a call to {@link
         * NanocachedClient#contentDigest} — this mock stands in for the
         * server, and a real server implementation is independent of this
         * SDK's own, so a bug shared between the SDK and this mock
         * wouldn't be caught by any test that only compares the two
         * against each other. */
        private static String digestOf(byte[] value) {
            try {
                java.security.MessageDigest sha256 = java.security.MessageDigest.getInstance("SHA-256");
                byte[] digest = sha256.digest(value);
                StringBuilder hex = new StringBuilder(32);
                for (int i = 0; i < 16; i++) {
                    hex.append(Character.forDigit((digest[i] >> 4) & 0xF, 16));
                    hex.append(Character.forDigit(digest[i] & 0xF, 16));
                }
                return hex.toString();
            } catch (java.security.NoSuchAlgorithmException impossible) {
                throw new AssertionError(impossible);
            }
        }
    }

    static final class MockDiscovery implements AutoCloseable {
        volatile List<DiscoveredNode> nodes;
        /** SDK proxy mode (issue #122): the roster `Q` answers with —
         * reuses {@link DiscoveredNode} exactly like a real proxy
         * announce does on the wire (name/address, no replication field),
         * even though a proxy's "name" has no routing meaning to a
         * client. Empty by default; a test sets this directly (mirrors
         * {@link #warmingUp}/{@link #rawListResponse} below) — including
         * mid-test, for a "roster changed under a live client" case. A
         * "proxy" here is nothing more than a {@link MockNode}: that is
         * literally what a proxy looks like to a client (full G/S/D,
         * never W). */
        volatile List<DiscoveredNode> proxies = List.of();
        volatile boolean warmingUp = false;
        final int replication;
        private final ServerSocket server;
        /** When set, an L request gets this exact text instead of the
         * normally generated frame — for tests that need to claim things
         * about the node list a real registry couldn't (an over-the-cap
         * count, a malformed header, an entry whose declared length
         * would blow the aggregate response cap) without actually
         * holding that much node data in memory. */
        volatile String rawListResponse;

        MockDiscovery(List<DiscoveredNode> nodes, int replication) throws IOException {
            this.nodes = nodes;
            this.replication = replication;
            this.server = new ServerSocket(0);
            Thread acceptor = new Thread(this::acceptLoop, "mock-discovery-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return server.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            server.close();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread worker = new Thread(() -> serve(socket), "mock-discovery-conn");
                    worker.setDaemon(true);
                    worker.start();
                } catch (IOException stop) {
                    return;
                }
            }
        }

        private void serve(Socket socket) {
            try (socket) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                while (true) {
                    String[] parts = readLine(in).split(" ");
                    if (parts[0].equals("A")) {
                        in.readNBytes(Integer.parseInt(parts[1]));
                        // Echoed response tags: echo the tag capability — clients send
                        // the extended A before knowing which kind of
                        // server answered. Discovery itself never tags
                        // requests (L is a one-shot), but the ack must
                        // still parse.
                        boolean requestedTags = parts.length > 2 && parts[2].equals("T");
                        out.write((requestedTags ? "OdT\n" : "Od\n").getBytes(StandardCharsets.US_ASCII));
                        out.flush();
                    } else if (parts[0].equals("L")) {
                        if (warmingUp) {
                            out.write("B\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            return;
                        }
                        String raw = rawListResponse;
                        if (raw != null) {
                            out.write(raw.getBytes(StandardCharsets.UTF_8));
                            out.flush();
                            return;
                        }
                        List<DiscoveredNode> snapshot = nodes;
                        StringBuilder frame = new StringBuilder(
                                "N " + snapshot.size() + " " + replication + "\n");
                        for (DiscoveredNode node : snapshot) {
                            frame.append(node.name().length()).append(' ')
                                    .append(node.address().length()).append('\n')
                                    .append(node.name()).append(node.address()).append('\n');
                        }
                        out.write(frame.toString().getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    } else if (parts[0].equals("Q")) {
                        // SDK proxy mode (issue #122): same B-on-startup-grace
                        // shape as L above; the header carries only the
                        // count (no replication field — a proxy client
                        // needs no R).
                        if (warmingUp) {
                            out.write("B\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            return;
                        }
                        List<DiscoveredNode> snapshot = proxies;
                        StringBuilder frame = new StringBuilder("N " + snapshot.size() + "\n");
                        for (DiscoveredNode proxy : snapshot) {
                            frame.append(proxy.name().length()).append(' ')
                                    .append(proxy.address().length()).append('\n')
                                    .append(proxy.name()).append(proxy.address()).append('\n');
                        }
                        out.write(frame.toString().getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    } else {
                        return;
                    }
                }
            } catch (IOException | RuntimeException done) {
                // Connection closed — normal end of a mock session.
            }
        }
    }

    /**
     * J1 test support: throwaway self-signed TLS certificates, generated
     * with the JDK's own {@code keytool} binary (shipped with every JDK
     * install — this adds no Maven/Gradle test dependency) rather than a
     * TLS/crypto library this SDK doesn't otherwise need. Each certificate
     * is self-signed and used as its own trust anchor, mirroring how a
     * real deployment's private CA is passed to {@code Options.ca(Path)}.
     */
    static final class Tls {
        private static final char[] PASSWORD = "changeit".toCharArray();
        private Tls() {}

        record Generated(SSLContext serverContext, Path pemCert) {}

        /** {@code subjectAltName} is a keytool {@code -ext SAN=...} value,
         * e.g. {@code "ip:127.0.0.1"} or {@code "dns:wrong.example.test"} —
         * this is exactly the value the JDK's HTTPS endpoint-identification
         * algorithm (issue J1) checks the connected-to host against. */
        static Generated generate(Path dir, String commonName, String subjectAltName) throws Exception {
            Path keystore = dir.resolve("node.p12");
            Path pem = dir.resolve("node.pem");

            runKeytool(dir,
                    "-genkeypair",
                    "-alias", "node",
                    "-keyalg", "RSA",
                    "-keysize", "2048",
                    "-validity", "3650",
                    "-storetype", "PKCS12",
                    "-keystore", keystore.toString(),
                    "-storepass", new String(PASSWORD),
                    "-keypass", new String(PASSWORD),
                    "-dname", "CN=" + commonName,
                    "-ext", "SAN=" + subjectAltName);
            runKeytool(dir,
                    "-exportcert",
                    "-alias", "node",
                    "-keystore", keystore.toString(),
                    "-storepass", new String(PASSWORD),
                    "-rfc",
                    "-file", pem.toString());

            KeyStore serverKeystore = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(keystore)) {
                serverKeystore.load(in, PASSWORD);
            }
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(serverKeystore, PASSWORD);
            SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(keyManagerFactory.getKeyManagers(), null, null);
            return new Generated(serverContext, pem);
        }

        private static void runKeytool(Path workingDir, String... args) throws IOException, InterruptedException {
            List<String> command = new java.util.ArrayList<>();
            command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "keytool");
            command.addAll(List.of(args));
            Process process = new ProcessBuilder(command)
                    .directory(workingDir.toFile())
                    .redirectErrorStream(true)
                    .start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IOException("keytool timed out: " + command);
            }
            if (process.exitValue() != 0) {
                throw new IOException("keytool failed (exit " + process.exitValue() + "): " + output);
            }
        }
    }

    static int unusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b == -1) throw new IOException("closed");
            if (b == '\n') return line.toString(StandardCharsets.US_ASCII);
            line.write(b);
        }
    }
}
