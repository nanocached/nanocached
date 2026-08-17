package org.nanocached;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

/**
 * The public client. A host/port (or a seeds list) may name either a
 * single nanocached-node or discovery server(s) fronting a cluster —
 * {@code connect()} finds out from the server's own handshake response
 * (doc/adr/0007-*.md), so calling code is identical either way.
 *
 * <p>Cluster mode implements ADR-0011 client-side replication: writes fan
 * out to each key's top-R owners (the primary's result decides; a dead
 * replica never fails a write), reads ask the primary and fall over to
 * the next owner only when the holder is unreachable. Dead connections
 * are redialed lazily on use, and an opt-in keep-alive can hold
 * connections open across the server's 30s idle timeout.
 *
 * <p>Thread-safe. Requests are serialized per connection (see
 * {@link Connection}); concurrent callers queue.
 */
public final class NanocachedClient implements AutoCloseable {

    public record Seed(String host, int port) {}

    /** Options for {@link #connect(Options)}; build with {@link #builder()}. */
    public static final class Options {
        private final List<Seed> seeds = new ArrayList<>();
        private byte[] authSecret;
        private SSLContext tls;
        private Duration keepAliveInterval;

        public Options host(String host, int port) {
            seeds.add(new Seed(host, port));
            return this;
        }

        /** Discovery replicas (ADR-0010), tried in order for connect and every refresh. */
        public Options seeds(List<Seed> seeds) {
            this.seeds.addAll(seeds);
            return this;
        }

        /** Shared secret matching NANOCACHED_AUTH_SECRET on the server. */
        public Options authSecret(String secret) {
            this.authSecret = secret.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        /** Connect over TLS with this context (use {@code SSLContext.getDefault()}
         * for publicly-trusted CAs, or a custom context for a private CA). */
        public Options tls(SSLContext context) {
            this.tls = context;
            return this;
        }

        /** Opt-in keep-alive; pick something below the server's 30s idle timeout. */
        public Options keepAliveInterval(Duration interval) {
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("nanocached: keepAliveInterval must be positive");
            }
            this.keepAliveInterval = interval;
            return this;
        }
    }

    public static Options builder() {
        return new Options();
    }

    private static final Duration NODE_LIST_STALE_AFTER = Duration.ofSeconds(30);
    // The server rejects empty keys, so the keep-alive G needs one byte.
    private static final byte[] KEEPALIVE_KEY = {0};

    private final Object stateLock = new Object();
    private final Object refreshLock = new Object();
    private final ConcurrentHashMap<String, Object> redialLocks = new ConcurrentHashMap<>();
    private final List<Seed> seeds;
    private final byte[] authSecret;
    private final SSLContext tls;

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

    private NanocachedClient(List<Seed> seeds, byte[] authSecret, SSLContext tls) {
        this.seeds = List.copyOf(seeds);
        this.authSecret = authSecret;
        this.tls = tls;
    }

    public static NanocachedClient connect(String host, int port) {
        return connect(builder().host(host, port));
    }

    public static NanocachedClient connect(Options options) {
        if (options.seeds.isEmpty()) {
            throw new IllegalArgumentException(
                    "nanocached: connect() needs either host/port or a non-empty seeds list");
        }

        NanocachedClient client =
                new NanocachedClient(options.seeds, options.authSecret, options.tls);

        // Walk the seeds until one yields a working target; a seed that is
        // unreachable, warming up (B, ADR-0010), or knows no live nodes is
        // skipped — the next replica may do better.
        RuntimeException lastError = null;
        for (Seed seed : client.seeds) {
            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(
                        seed.host(), seed.port(), client.authSecret, client.tls);
            } catch (IOException | RuntimeException error) {
                lastError = error instanceof RuntimeException runtime
                        ? runtime
                        : new NanocachedException.ConnectionFailed(error.getMessage(), error);
                continue;
            }

            try {
                if (identified instanceof Identify.NodeTarget node) {
                    if (client.seeds.size() > 1) {
                        System.err.println("nanocached: " + seed.host() + ":" + seed.port()
                                + " is a cache node, so this client is pinned to that single server —"
                                + " the remaining seed(s) will not be used. Point seeds at discovery"
                                + " servers for cluster routing and failover.");
                    }
                    client.single = new Connection(node.socket());
                    client.singleAddress = seed.host() + ":" + seed.port();
                    client.startKeepAlive(options.keepAliveInterval);
                    return client;
                }

                Identify.ClusterTarget cluster = (Identify.ClusterTarget) identified;
                if (cluster.nodes().isEmpty()) {
                    lastError = new NanocachedException(
                            "nanocached: no live nodes registered with the discovery server at "
                                    + seed.host() + ":" + seed.port());
                    continue;
                }

                client.openCluster(cluster);
                client.startKeepAlive(options.keepAliveInterval);
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
                : new NanocachedException("nanocached: could not connect to any seed");
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

    public byte[] get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns the value, or {@code null} when the key is missing. */
    public byte[] get(byte[] key) {
        beforeOperation();
        return withWrongNodeRetry(() -> read(key, connection -> connection.get(key)));
    }

    public void set(String key, String value) {
        set(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), null);
    }

    public void set(String key, String value, long ttlSeconds) {
        set(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), ttlSeconds);
    }

    public void set(byte[] key, byte[] value, Long ttlSeconds) {
        if (ttlSeconds != null && ttlSeconds < 0) {
            throw new IllegalArgumentException(
                    "nanocached: ttlSeconds must be non-negative, got " + ttlSeconds);
        }
        beforeOperation();
        withWrongNodeRetry(() -> {
            write(key, connection -> {
                connection.set(key, value, ttlSeconds);
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

    /** Idempotent; later get/set/delete throw {@link NanocachedException.AlreadyClosed}. */
    @Override
    public void close() {
        if (closed) return;
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
     * doesn't learn about a peer FIN (e.g. the server's 30s idle timeout)
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
            throw new NanocachedException("nanocached: " + name + " has no open connection");
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
            throw new NanocachedException("nanocached: " + address + " no longer identifies as a cache node");
        }
        if (closed) {
            try {
                node.socket().close();
            } catch (IOException ignored) {
                // Best-effort cleanup on the close race.
            }
            throw new NanocachedException.AlreadyClosed();
        }
        return new Connection(node.socket());
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
                    System.err.println("nanocached: could not connect to new node "
                            + node.address() + ", will retry: " + error.getMessage());
                }
            }

            ring = new HashRing(new ArrayList<>(members.keySet()));
            replication = cluster.replication();
        }
    }

    /** Walks every seed (ADR-0010); {@code null} means keep the last-known list. */
    private Identify.ClusterTarget fetchNodeList() {
        for (Seed seed : seeds) {
            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(seed.host(), seed.port(), authSecret, tls);
            } catch (IOException | RuntimeException error) {
                System.err.println("nanocached: could not refresh the node list from "
                        + seed.host() + ":" + seed.port() + ": " + error.getMessage());
                continue;
            }
            if (identified instanceof Identify.NodeTarget node) {
                try {
                    node.socket().close();
                } catch (IOException ignored) {
                    // One-shot probe cleanup.
                }
                System.err.println("nanocached: " + seed.host() + ":" + seed.port()
                        + " no longer identifies as a discovery server");
                continue;
            }
            Identify.ClusterTarget cluster = (Identify.ClusterTarget) identified;
            if (cluster.nodes().isEmpty()) {
                System.err.println("nanocached: discovery at " + seed.host() + ":" + seed.port()
                        + " returned no live nodes, skipping");
                continue;
            }
            return cluster;
        }
        System.err.println("nanocached: no discovery seed could provide a node list, keeping the last-known list");
        return null;
    }

    // ── keep-alive ────────────────────────────────────────────────

    private void startKeepAlive(Duration interval) {
        if (interval == null) return;

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
