package org.nanocached;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * The public client. An address (or an addresses list) may name either a
 * single nanocached-node or discovery server(s) fronting a cluster —
 * {@code connect()} finds out from the server's own handshake response
 * (doc/adr/0007-*.md), so calling code is identical either way.
 *
 * <p>Cluster mode implements ADR-0011 client-side replication: writes fan
 * out to each key's top-R owners (the primary's result decides; a dead
 * replica never fails a write), reads ask the primary and fall over to
 * the next owner only when the holder is unreachable. Dead connections
 * are redialed lazily on use, and an opt-in keep-alive can hold
 * connections open across the server's 60s idle timeout.
 *
 * <p>Thread-safe. Requests are serialized per connection (see
 * {@link Connection}); concurrent callers queue.
 */
public final class NanocachedClient implements AutoCloseable {

    public record Address(String host, int port) {}

    /** Options for {@link #connect(Options)}; build with {@link #builder()}. */
    public static final class Options {
        private final List<Address> addresses = new ArrayList<>();
        private byte[] authSecret;
        private boolean tls;
        private Path ca;

        /** Discovery replicas (ADR-0010), tried in order for connect and every
         * refresh; a one-element list is the single-target case. */
        public Options addresses(List<Address> addresses) {
            this.addresses.addAll(addresses);
            return this;
        }

        /** Shared secret matching NANOCACHED_AUTH_SECRET on the server. */
        public Options authSecret(String secret) {
            this.authSecret = secret.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        /** Connect over TLS. Without {@link #ca}, verifies against the
         * platform trust store; with it, a private CA replaces the default
         * store. */
        public Options tls(boolean enabled) {
            this.tls = enabled;
            return this;
        }

        /** A PEM file of trusted root certificate(s). Meaningful only when
         * {@link #tls} is enabled — silently ignored otherwise. */
        public Options ca(Path path) {
            this.ca = path;
            return this;
        }

        /** Convenience overload of {@link #ca(Path)}. */
        public Options ca(String path) {
            return ca(Path.of(path));
        }

        /** Convenience overload of {@link #ca(Path)}. */
        public Options ca(File file) {
            return ca(file.toPath());
        }
    }

    public static Options builder() {
        return new Options();
    }

    private static final Duration NODE_LIST_STALE_AFTER = Duration.ofSeconds(30);
    // The server rejects empty keys, so the keep-alive G needs one byte.
    private static final byte[] KEEPALIVE_KEY = {0};
    static volatile long keepAliveIntervalMillis = 30_000;

    // Tracks, per connect() target (not per instance — mirrors
    // sdk/typescript/src/client.ts's openTargets), how many open sockets
    // this process still holds for a given "host:port". Purely a
    // programming-error guard: catches "connect() called again for the
    // same target before the previous one was ever released" without
    // affecting behavior — connecting again still works, this only warns.
    private static final ConcurrentHashMap<String, Integer> OPEN_TARGETS = new ConcurrentHashMap<>();

    private static void trackOpenTarget(String key) {
        OPEN_TARGETS.merge(key, 1, Integer::sum);
    }

    private static void untrackOpenTarget(String key) {
        OPEN_TARGETS.computeIfPresent(key, (ignoredKey, count) -> count <= 1 ? null : count - 1);
    }

    private final Object stateLock = new Object();
    private final Object refreshLock = new Object();
    private final ConcurrentHashMap<String, Object> redialLocks = new ConcurrentHashMap<>();
    private final List<Address> addresses;
    private final byte[] authSecret;
    private final SSLContext tls;

    /** The address that answered connect() — used both to redial in single
     * mode and as the key for the open-sockets tracker in every mode
     * (mirrors TypeScript's {@code this.url}). Set once, before this
     * client ever opens a socket. */
    private String targetKey;

    private volatile boolean closed = false;
    private Connection single;              // single-node mode
    private String singleAddress;
    private final Map<String, Member> members = new LinkedHashMap<>(); // cluster mode
    private HashRing ring;
    private int replication = 1;
    private long lastFetchNanos = System.nanoTime();

    private ExecutorService replicaWriters;
    private ScheduledExecutorService keepAlive;

    private static final class Member {
        String address;
        Connection connection;

        Member(String address, Connection connection) {
            this.address = address;
            this.connection = connection;
        }
    }

    private NanocachedClient(List<Address> addresses, byte[] authSecret, SSLContext tls) {
        this.addresses = List.copyOf(addresses);
        this.authSecret = authSecret;
        this.tls = tls;
    }

    public static NanocachedClient connect(Options options) {
        if (options.addresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "nanocached: connect() needs a non-empty addresses list");
        }

        SSLContext sslContext = buildSslContext(options.tls, options.ca);
        NanocachedClient client =
                new NanocachedClient(options.addresses, options.authSecret, sslContext);

        // Walk the addresses until one yields a working target; an address
        // that is unreachable, warming up (B, ADR-0010), or knows no live
        // nodes is skipped — the next replica may do better.
        RuntimeException lastError = null;
        for (Address address : client.addresses) {
            String key = address.host() + ":" + address.port();

            // Only meaningful for a single explicit target: with an
            // addresses list, another client instance legitimately holding
            // connections to the same address makes this heuristic
            // false-positive (issue #12).
            if (client.addresses.size() == 1 && OPEN_TARGETS.containsKey(key)) {
                System.err.println("nanocached: connect() called for " + key
                        + " while a previous connection to it is still open — was close() forgotten?");
            }

            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(
                        address.host(), address.port(), client.authSecret, client.tls);
            } catch (IOException | RuntimeException error) {
                lastError = error instanceof RuntimeException runtime
                        ? runtime
                        : new NanocachedException.ConnectionFailed(error.getMessage(), error);
                continue;
            }

            try {
                client.targetKey = key;

                if (identified instanceof Identify.NodeTarget node) {
                    if (client.addresses.size() > 1) {
                        System.err.println("nanocached: " + key
                                + " is a cache node, so this client is pinned to that single server —"
                                + " the " + (client.addresses.size() - 1) + " remaining address(es)"
                                + " will not be used. Point addresses at discovery servers for cluster"
                                + " routing and failover.");
                    }
                    client.single = client.newTrackedConnection(node.socket());
                    client.singleAddress = key;
                    client.startKeepAlive();
                    return client;
                }

                Identify.ClusterTarget cluster = (Identify.ClusterTarget) identified;
                if (cluster.nodes().isEmpty()) {
                    lastError = new NanocachedException(
                            "nanocached: no live nodes registered with the discovery server at " + key);
                    continue;
                }

                client.openCluster(cluster);
                client.startKeepAlive();
                return client;
            } catch (IOException error) {
                client.teardown();
                throw new NanocachedException.ConnectionFailed(error.getMessage(), error);
            } catch (RuntimeException error) {
                client.teardown();
                throw error;
            }
        }

        throw lastError != null
                ? lastError
                : new NanocachedException("nanocached: could not connect to any address");
    }

    /** Builds the TLS context to dial with, or {@code null} for a plain
     * connection. {@code ca} is silently ignored when {@code tlsEnabled} is
     * false; an unreadable/unparseable CA file is a connect-time error. */
    private static SSLContext buildSslContext(boolean tlsEnabled, Path ca) {
        if (!tlsEnabled) return null;

        if (ca == null) {
            try {
                return SSLContext.getDefault();
            } catch (NoSuchAlgorithmException error) {
                throw new NanocachedException(
                        "nanocached: no default SSL context available: " + error.getMessage());
            }
        }

        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Collection<? extends Certificate> certificates;
            try (InputStream in = Files.newInputStream(ca)) {
                certificates = factory.generateCertificates(in);
            }
            if (certificates.isEmpty()) {
                throw new NanocachedException("nanocached: ca file " + ca + " contains no certificates");
            }

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            int index = 0;
            for (Certificate certificate : certificates) {
                trustStore.setCertificateEntry("ca-" + index++, certificate);
            }

            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagerFactory.getTrustManagers(), null);
            return context;
        } catch (IOException | GeneralSecurityException error) {
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: could not load ca file " + ca + ": " + error.getMessage(), error);
        }
    }

    private void openCluster(Identify.ClusterTarget cluster) throws IOException {
        List<String> names = new ArrayList<>();
        for (DiscoveredNode node : cluster.nodes()) {
            members.put(node.name(), new Member(node.address(), openNodeConnection(node.address())));
            names.add(node.name());
        }
        ring = new HashRing(names);
        replication = cluster.replication();
        replicaWriters = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "nanocached-replica-writer");
            thread.setDaemon(true);
            return thread;
        });
    }

    // ── 公開 API ──────────────────────────────────────────────────

    /** How many nodes hold each key (ADR-0011) — 1 against a single node. */
    public int replication() {
        return ring != null ? replication : 1;
    }

    public boolean isClosed() {
        return closed;
    }

    /** Returns the value decoded as strict UTF-8, or {@code Optional.empty()}
     * when the key is missing.
     * @throws UncheckedIOException if the stored value is not valid UTF-8 */
    public Optional<String> get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns the value decoded as strict UTF-8, or {@code Optional.empty()}
     * when the key is missing.
     * @throws UncheckedIOException if the stored value is not valid UTF-8 */
    public Optional<String> get(byte[] key) {
        return getBytes(key).map(NanocachedClient::decodeUtf8Strict);
    }

    /** Returns the raw value, or {@code Optional.empty()} when the key is
     * missing. */
    public Optional<byte[]> getBytes(String key) {
        return getBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns the raw value, or {@code Optional.empty()} when the key is
     * missing. */
    public Optional<byte[]> getBytes(byte[] key) {
        beforeOperation();
        return Optional.ofNullable(withWrongNodeRetry(() -> read(key, connection -> connection.get(key))));
    }

    private static String decodeUtf8Strict(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new UncheckedIOException(malformed);
        }
    }

    public void set(String key, String value) {
        set(key, value, 0L);
    }

    public void set(String key, String value, long ttlSeconds) {
        set(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), ttlSeconds);
    }

    public void set(byte[] key, byte[] value) {
        set(key, value, 0L);
    }

    /** {@code ttlSeconds == 0} means no expiry. */
    public void set(byte[] key, byte[] value, long ttlSeconds) {
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException(
                    "nanocached: ttlSeconds must be non-negative, got " + ttlSeconds);
        }
        beforeOperation();
        Long wireTtlSeconds = ttlSeconds == 0 ? null : ttlSeconds;
        withWrongNodeRetry(() -> {
            write(key, connection -> {
                connection.set(key, value, wireTtlSeconds);
                return null;
            });
            return null;
        });
    }

    public boolean delete(String key) {
        return delete(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns whether the key existed before this call. */
    public boolean delete(byte[] key) {
        beforeOperation();
        return withWrongNodeRetry(() -> write(key, connection -> connection.delete(key)));
    }

    /** Idempotent (later get/set/delete throw {@link NanocachedException.AlreadyClosed}),
     * but a second call warns to stderr — usually a sign the caller lost
     * track of this instance's lifecycle. */
    @Override
    public void close() {
        if (closed) {
            System.err.println("nanocached: close() called again on an already-closed client");
            return;
        }
        closed = true;
        if (keepAlive != null) keepAlive.shutdownNow();
        if (replicaWriters != null) replicaWriters.shutdown();
        teardown();
    }

    private void teardown() {
        synchronized (stateLock) {
            if (single != null) single.close();
            for (Member member : members.values()) member.connection.close();
        }
    }

    // ── ルーティングと複製 ────────────────────────────────────────

    private interface ConnectionOp<T> {
        T apply(Connection connection);
    }

    private void beforeOperation() {
        if (closed) throw new NanocachedException.AlreadyClosed();
        maybeRefresh(false);
    }

    /**
     * Runs the operation; on a {@code W} answer (stale routing) <em>or</em>
     * a connection-level failure that exhausted the current ranking (e.g.
     * the key's primary died), forces a node-list refresh and retries the
     * whole operation once against the fresh ranking. The retry window for
     * a dead node is therefore bounded by discovery's liveness timeout —
     * once discovery drops the node, the refreshed ranking routes around
     * it. A second failure after a fresh refresh propagates.
     */
    private <T> T withWrongNodeRetry(java.util.function.Supplier<T> operation) {
        try {
            return operation.get();
        } catch (NanocachedException.WrongNode | NanocachedException.ConnectionFailed error) {
            if (ring == null) throw error;
            maybeRefresh(true);
            return operation.get();
        }
    }

    private List<String> ownerNames(byte[] key) {
        synchronized (stateLock) {
            return ring.owners(key, replication);
        }
    }

    /**
     * Runs {@code op} against {@code source}'s connection, retrying once
     * on a connection-level failure. Unlike Node or Python, a Java socket
     * doesn't learn about a peer FIN (e.g. the server's 60s idle timeout)
     * until an I/O call fails — so lazy reconnect-on-use here means: the
     * failed request poisons the connection, {@code source} redials, and
     * the operation runs again. Safe because get/set/delete are all
     * idempotent.
     */
    private <T> T applyReconnecting(
            java.util.function.Supplier<Connection> source, ConnectionOp<T> op) {
        try {
            return op.apply(source.get());
        } catch (NanocachedException.ConnectionFailed retryable) {
            return op.apply(source.get());
        }
    }

    private <T> T read(byte[] key, ConnectionOp<T> op) {
        if (ring == null) {
            return applyReconnecting(this::singleConnection, op);
        }

        // Owners in rank order; fall through only on connection-level
        // failure — a replica hedges against a dead holder, not a miss.
        RuntimeException lastError = null;
        for (String name : ownerNames(key)) {
            try {
                return applyReconnecting(() -> memberConnection(name), op);
            } catch (NanocachedException.WrongNode error) {
                throw error;
            } catch (RuntimeException error) {
                lastError = error;
            }
        }
        throw lastError != null
                ? lastError
                : new NanocachedException("nanocached: no owner is reachable for this key");
    }

    private <T> T write(byte[] key, ConnectionOp<T> op) {
        if (ring == null) {
            return applyReconnecting(this::singleConnection, op);
        }

        List<String> names = ownerNames(key);
        if (names.isEmpty()) {
            throw new NanocachedException("nanocached: no owner is reachable for this key");
        }

        List<CompletableFuture<Void>> replicaWrites = new ArrayList<>();
        for (int i = 1; i < names.size(); i++) {
            String replica = names.get(i);
            replicaWrites.add(CompletableFuture.runAsync(() -> {
                try {
                    applyReconnecting(() -> memberConnection(replica), op);
                } catch (RuntimeException ignored) {
                    // Swallowed by design (ADR-0011): a dead or disagreeing
                    // replica leaves the key under-replicated until the next
                    // node-list refresh, never fails the write.
                }
            }, replicaWriters));
        }

        try {
            return applyReconnecting(() -> memberConnection(names.get(0)), op);
        } finally {
            for (CompletableFuture<Void> pending : replicaWrites) {
                pending.join();
            }
        }
    }

    // ── 遅延再接続 ────────────────────────────────────────────────

    private Connection singleConnection() {
        Connection current = single;
        if (!current.isClosed()) return current;

        synchronized (redialLocks.computeIfAbsent("", slot -> new Object())) {
            if (single.isClosed()) {
                single = openNodeConnectionOrThrow(singleAddress);
            }
            return single;
        }
    }

    private Connection memberConnection(String name) {
        Member member;
        synchronized (stateLock) {
            member = members.get(name);
        }
        if (member == null) {
            // Connection-classified (issue #8): the usual cause is a
            // refresh racing this operation, which the refresh-and-retry
            // layer heals.
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: " + name + " has no open connection", null);
        }
        if (!member.connection.isClosed()) return member.connection;

        // Concurrent requests finding the same dead connection share one
        // dial: the first thread in redials, the rest wait then reuse.
        synchronized (redialLocks.computeIfAbsent(name, slot -> new Object())) {
            synchronized (stateLock) {
                Member current = members.get(name);
                if (current == null) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: " + name + " left the cluster while reconnecting", null);
                }
                member = current;
            }
            if (!member.connection.isClosed()) return member.connection;
            Connection connection = openNodeConnectionOrThrow(member.address);
            member.connection = connection;
            return connection;
        }
    }

    private Connection openNodeConnectionOrThrow(String address) {
        try {
            return openNodeConnection(address);
        } catch (IOException error) {
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: could not connect to " + address + ": " + error.getMessage(), error);
        }
    }

    private Connection openNodeConnection(String address) throws IOException {
        int separator = address.lastIndexOf(':');
        if (separator == -1) {
            throw new NanocachedException("nanocached: invalid node address from discovery server: " + address);
        }
        String host = address.substring(0, separator);
        int port = Integer.parseInt(address.substring(separator + 1));

        Identify.Result identified = Identify.connectAndIdentify(host, port, authSecret, tls);
        if (!(identified instanceof Identify.NodeTarget node)) {
            // Connection-classified (issue #8): a topology change, healed
            // by the refresh-and-retry layer — unlike an auth failure,
            // which stays a plain (non-retryable) exception.
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: " + address + " no longer identifies as a cache node", null);
        }
        if (closed) {
            try {
                node.socket().close();
            } catch (IOException ignored) {
                // Best-effort cleanup on the close race.
            }
            throw new NanocachedException.AlreadyClosed();
        }
        return newTrackedConnection(node.socket());
    }

    /** Wraps {@code socket} in a {@link Connection}, incrementing this
     * client's open-sockets count under {@link #targetKey} and arranging
     * for it to be decremented the moment that connection closes for any
     * reason — self-poisoning on a protocol error, a refresh dropping the
     * node, a lazy redial discarding it, or {@link #close()}. */
    private Connection newTrackedConnection(Socket socket) throws IOException {
        trackOpenTarget(targetKey);
        return new Connection(socket, () -> untrackOpenTarget(targetKey));
    }

    // ── ノードリスト更新 ──────────────────────────────────────────

    private void maybeRefresh(boolean force) {
        if (ring == null) return;
        if (!force && System.nanoTime() - lastFetchNanos < NODE_LIST_STALE_AFTER.toNanos()) return;

        synchronized (refreshLock) {
            if (!force && System.nanoTime() - lastFetchNanos < NODE_LIST_STALE_AFTER.toNanos()) return;
            refreshNodeList();
        }
    }

    private void refreshNodeList() {
        Identify.ClusterTarget cluster = fetchNodeList();
        lastFetchNanos = System.nanoTime();
        if (cluster == null) return;

        synchronized (stateLock) {
            Map<String, DiscoveredNode> byName = new LinkedHashMap<>();
            for (DiscoveredNode node : cluster.nodes()) byName.put(node.name(), node);

            members.entrySet().removeIf(entry -> {
                if (!byName.containsKey(entry.getKey())) {
                    entry.getValue().connection.close();
                    // Node names are per-process UUIDs; a departed node's
                    // redial gate would otherwise leak forever (issue #12).
                    redialLocks.remove(entry.getKey());
                    return true;
                }
                return false;
            });

            for (DiscoveredNode node : cluster.nodes()) {
                Member existing = members.get(node.name());
                if (existing != null) {
                    existing.address = node.address();
                    continue;
                }
                try {
                    members.put(node.name(), new Member(node.address(), openNodeConnection(node.address())));
                } catch (IOException | RuntimeException error) {
                    // Kept silent by design (issue #12): behavior is
                    // unchanged (this node is retried on the next refresh),
                    // it just no longer narrates to stderr.
                }
            }

            ring = new HashRing(new ArrayList<>(members.keySet()));
            replication = cluster.replication();
        }
    }

    /** Walks every address (ADR-0010); {@code null} means keep the
     * last-known list. */
    private Identify.ClusterTarget fetchNodeList() {
        for (Address address : addresses) {
            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(address.host(), address.port(), authSecret, tls);
            } catch (IOException | RuntimeException error) {
                continue;
            }
            if (identified instanceof Identify.NodeTarget node) {
                try {
                    node.socket().close();
                } catch (IOException ignored) {
                    // One-shot probe cleanup.
                }
                continue;
            }
            Identify.ClusterTarget cluster = (Identify.ClusterTarget) identified;
            if (cluster.nodes().isEmpty()) {
                continue;
            }
            return cluster;
        }
        return null;
    }

    // ── keep-alive ────────────────────────────────────────────────

    private void startKeepAlive() {
        // Always on, with an internal interval (issue #27): half the
        // server's 60s idle timeout, so it never severs a healthy
        // client. Package-visible only so tests can shorten it.
        Duration interval = Duration.ofMillis(keepAliveIntervalMillis);

        keepAlive = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "nanocached-keepalive");
            thread.setDaemon(true);
            return thread;
        });
        keepAlive.scheduleAtFixedRate(() -> {
            List<Connection> connections = new ArrayList<>();
            synchronized (stateLock) {
                if (single != null) connections.add(single);
                for (Member member : members.values()) connections.add(member.connection);
            }
            for (Connection connection : connections) {
                if (connection.isClosed()) continue; // dead ones stay lazy
                if (connection.idleNanos() < interval.toNanos()) continue;
                try {
                    // Any parseable reply proves liveness — N, or W from a
                    // non-owner — and resets the server's idle timer.
                    connection.get(KEEPALIVE_KEY);
                } catch (RuntimeException ignored) {
                    // Keep-alive failures never surface; use redials lazily.
                }
            }
        }, interval.toNanos(), interval.toNanos(), TimeUnit.NANOSECONDS);
    }
}
