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
        final AtomicInteger connectionCount = new AtomicInteger();
        final AtomicInteger getCount = new AtomicInteger();
        private final AtomicInteger wrongNodeReplies = new AtomicInteger();
        /** ADR-0019: queued one-off replies that echo the WRONG tag (the
         * request's tag + 1) — the desync a pre-ADR-0019 stream
         * misalignment would produce. Only takes effect on a tagged
         * connection. */
        private final AtomicInteger wrongTagReplies = new AtomicInteger();
        /** ADR-0019: swallows the next G entirely (no reply) — the
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
        private volatile long setDelayMillis = 0;
        private volatile boolean failSets = false;
        private volatile boolean silent = false;
        /** The TTL (whole seconds; 0 if omitted on the wire) from the
         * most recent S request this server received. */
        volatile long lastSetTtl = 0;
        private final byte[] requiredSecret;
        /** ADR-0019: acknowledge an extended `A ... T` with `OnT\n` and
         * echo tags on that connection's G/S/D replies. Off by default so
         * the bulk of the suite keeps exercising the legacy untagged
         * path. */
        private final boolean supportTags;
        /** ADR-0019: behave like a pre-0019 server — an extended
         * `A ... T` is a parse error, so close the connection without
         * replying. */
        private final boolean closeOnExtendedAuth;
        private final ServerSocket server;
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();

        MockNode() throws IOException {
            this(null, false, false, null);
        }

        MockNode(byte[] requiredSecret) throws IOException {
            this(requiredSecret, false, false, null);
        }

        /** ADR-0019: a node that negotiates tags — accepts `A ... T`
         * with `OnT\n` and echoes tags on that connection's replies. */
        static MockNode withTagSupport() throws IOException {
            return new MockNode(null, true, false, null);
        }

        /** ADR-0019: a pre-0019 node — the extended `A ... T` is a parse
         * error, so it closes the connection without replying, forcing
         * the caller's transparent untagged fallback. */
        static MockNode legacyServer() throws IOException {
            return new MockNode(null, false, true, null);
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
            this.requiredSecret = requiredSecret;
            this.supportTags = supportTags;
            this.closeOnExtendedAuth = closeOnExtendedAuth;
            this.server = serverTls == null
                    ? new ServerSocket(0)
                    : serverTls.getServerSocketFactory().createServerSocket(0);
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
         * connection that echoes the WRONG tag (ADR-0019). */
        void answerWrongTagOnce() {
            wrongTagReplies.incrementAndGet();
        }

        /** Swallow the next G request entirely (no reply) — the
         * off-by-one stream desync where every later response answers
         * the previous request (ADR-0019). */
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

        /** Holds every future S reply for {@code millis} first — for tests
         * proving a caller isn't blocked on a slow replica leg
         * (doc/adr/0014-*.md). */
        void delaySets(long millis) {
            setDelayMillis = millis;
        }

        /** Drops the connection (server-side reset) on every S instead of
         * acking it, so a write here fails with a connection error — for
         * tests that need a repair/replica write to deterministically
         * fail without racing a close(). Reads (G) are unaffected, so a
         * node can still miss the initial lookup that triggers the repair. */
        void failSets() {
            failSets = true;
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
            dropConnections();
            server.close();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    connectionCount.incrementAndGet();
                    sockets.add(socket);
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
                // ADR-0019: set when this connection's `A ... T` was
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
                            if (parts.length > 2 && closeOnExtendedAuth) {
                                return; // pre-0019 behavior: close without replying
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

                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (swallowedGets.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                break; // no reply — simulates an off-by-one stream desync
                            }
                            if (tagged && takeWrongTag()) {
                                // Echo the WRONG tag (the request's tag +
                                // 1) — the desync a pre-ADR-0019 stream
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
                                out.write(("S" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        case "D" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            if (silent) {
                                break; // half-open: frame consumed, never answered
                            }
                            if (takeWrongNode()) {
                                out.write(("W" + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
                            } else {
                                out.write((store.remove(key) != null ? "D" + tagSuffix + "\n" : "N" + tagSuffix + "\n")
                                        .getBytes(StandardCharsets.US_ASCII));
                            }
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

        static String keyOf(byte[] key) {
            return new String(key, StandardCharsets.ISO_8859_1);
        }
    }

    static final class MockDiscovery implements AutoCloseable {
        volatile List<DiscoveredNode> nodes;
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
                        // ADR-0019: echo the tag capability — clients send
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
