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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * The public client. An address (or an addresses list) may name either a
 * single nanocached-node or discovery server(s) fronting a cluster —
 * {@code connect()} finds out from the server's own handshake response
 * (the server type in the auth response), so calling code is identical either way.
 *
 * <p>Cluster mode implements client-side replication client-side replication: writes fan
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
        private boolean compress;
        private int compressionThreshold = DEFAULT_COMPRESSION_THRESHOLD;
        private boolean fireAndForgetReplicas;
        private boolean readRepair;
        private Duration reconnectCooldown = DEFAULT_RECONNECT_COOLDOWN;
        private boolean reconnectCooldownDisabled;
        private Duration readHedgeAfter;

        /** Discovery replicas (discovery HA), tried in order for connect and every
         * refresh; a one-element list is the single-target case. */
        public Options addresses(List<Address> addresses) {
            this.addresses.addAll(addresses);
            return this;
        }

        /** Shared secret matching NANOCACHED_AUTH_SECRET on the server. An
         * empty secret is the same as none, matching the other SDKs: sent
         * literally, an empty string would reach the wire as an explicit
         * zero-length secret, which the server rejects (EmptySecret) and
         * closes without replying — turning what should be "no auth
         * configured" into an opaque {@link NanocachedException.ConnectionFailed}
         * instead of the clear {@link NanocachedException.AuthenticationFailed}
         * a missing secret against a server that requires one already
         * gives. */
        public Options authSecret(String secret) {
            // A raw NullPointerException here reads like a bug in this
            // SDK, not a caller mistake — IllegalArgumentException matches
            // every other validation in this class (issue: audit finding
            // J3).
            if (secret == null) {
                throw new IllegalArgumentException("nanocached: authSecret must not be null");
            }
            this.authSecret = secret.isEmpty() ? null : secret.getBytes(StandardCharsets.UTF_8);
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

        /** Transparently compress values above {@link #compressionThreshold}
         * on set and decompress them on get/getBytes (value compression).
         * Off by default. <b>Every client that reads or writes a given set
         * of keys must agree on this setting</b> — it is a per-keyspace
         * format decision, not a per-client preference; take care before enabling this against an existing keyspace
         * another client may still touch with {@code compress} off. */
        public Options compress(boolean enabled) {
            this.compress = enabled;
            return this;
        }

        /** Values shorter than this (in bytes) are never compressed — the
         * per-value overhead of attempting it outweighs the savings. Only
         * meaningful when {@link #compress} is enabled. Default {@value
         * #DEFAULT_COMPRESSION_THRESHOLD}. Negative is rejected by {@link
         * #connect} — a negative threshold would otherwise silently force
         * every set() to attempt compression regardless of value size,
         * since no value's length is ever less than a negative number
         * (issue: audit finding; mirrors the Go SDK's identical
         * Connect-time check). */
        public Options compressionThreshold(int bytes) {
            this.compressionThreshold = bytes;
            return this;
        }

        /** Let set/delete return as soon as the primary owner acks, letting
         * replica legs finish in the background instead of waiting for them
         * too (fire-and-forget replica writes). Off by default. Unlike {@link #compress},
         * this is a pure latency/durability trade for this client's own
         * writes — it carries no wire format and needs no agreement with
         * other clients. */
        public Options fireAndForgetReplicas(boolean enabled) {
            this.fireAndForgetReplicas = enabled;
            return this;
        }

        /** On a clean miss (the key's first-reached owner reports it
         * missing), probe the remaining owners before accepting that, and
         * repair the primary in the background if one still has the value
         * (read repair). Off by default. Costs extra reads only on
         * the misses this actually applies to. */
        public Options readRepair(boolean enabled) {
            this.readRepair = enabled;
            return this;
        }

        /** How long, after a reconnect dial to an address fails, that
         * address is treated as still down — a call routed to it during
         * this window fails immediately with the original dial error
         * instead of paying another full connect timeout redialing an
         * address that just proved unreachable. Default {@code
         * DEFAULT_RECONNECT_COOLDOWN} (1s). Keep well under the node-list
         * staleness window ({@code NODE_LIST_STALE_AFTER}, 30s) so a node
         * that genuinely recovers isn't shut out for long.
         *
         * <p>{@link Duration#ZERO} means "use the default", not "disable
         * it" — this matches the Go SDK's zero-value {@code Config}
         * (whose {@code ReconnectCooldown} field, simply left unset,
         * can't distinguish "not specified" from "explicitly zero", so
         * zero has to mean "default" there) and the Rust SDK's {@code
         * Options::reconnect_cooldown}, for cross-SDK consistency even
         * though this builder can tell "never called" apart on its own.
         * Call {@link #disableReconnectCooldown()} to actually disable
         * the cooldown — the Go SDK's equivalent is a negative {@code
         * Config.ReconnectCooldown}. */
        public Options reconnectCooldown(Duration cooldown) {
            // Matches authSecret's null handling above (issue: audit
            // finding J3) — a raw NullPointerException here would read
            // like a bug in this SDK, not a caller mistake. A negative
            // duration is rejected too: it would mean "never cools down
            // by the time check", i.e. every dial to a known-dead address
            // would be treated as already past its cooldown instantly —
            // silently defeating the whole feature rather than failing
            // loudly at configuration time. (Disabling the feature
            // outright is still possible — see disableReconnectCooldown()
            // — just not as a side effect of a duration that reads like
            // "immediately".)
            if (cooldown == null || cooldown.isNegative()) {
                throw new IllegalArgumentException(
                        "nanocached: reconnectCooldown must not be null or negative");
            }
            this.reconnectCooldown = cooldown.isZero() ? DEFAULT_RECONNECT_COOLDOWN : cooldown;
            this.reconnectCooldownDisabled = false;
            return this;
        }

        /** Disables the per-address reconnect cooldown entirely: every
         * call that finds an address's connection dead pays its own full
         * dial attempt instead of reusing a cached failure. See
         * {@link #reconnectCooldown} for what the cooldown is; the Go
         * SDK's equivalent of this method is a negative {@code
         * Config.ReconnectCooldown}, and the Rust SDK's is {@code
         * Options::disable_reconnect_cooldown()}. */
        public Options disableReconnectCooldown() {
            this.reconnectCooldownDisabled = true;
            return this;
        }

        /** Sends the same read to the next owner as well once the primary
         * has been silent for this long (and so on, one more owner per
         * interval, until every owner is in flight) — a slow-but-alive
         * owner (a saturated host, a bad link) no longer bounds every read
         * that touches it at its full round trip (hedged reads). {@code
         * null} (the default) is off.
         *
         * <p>The first answer decides: a hit from any owner is final; a
         * miss is final only from the primary — a replica's miss may
         * simply mean it lacks the copy, so hedging never turns a hit
         * into a miss. A connection-level (or other SDK) failure hedges
         * onward immediately; a {@link NanocachedException.WrongNode}
         * answer propagates exactly as the non-hedged read path's does.
         * Applies only once a ring is known and the key has at least two
         * owners — with a single copy there is nobody to hedge to, so
         * this is simply inert against a single node or {@code
         * replication == 1}.
         *
         * <p>The losing leg of a hedge is never cancelled — interrupting a
         * request mid-write could desync that connection — but left to
         * finish on this client's background executor, its result
         * discarded, and is drained by {@link #close()} exactly like a
         * {@code fireAndForgetReplicas} write. Writes are unaffected. */
        public Options readHedgeAfter(Duration interval) {
            if (interval != null && interval.compareTo(Duration.ZERO) <= 0) {
                throw new IllegalArgumentException(
                        "nanocached: readHedgeAfter must be a positive duration, got " + interval);
            }
            this.readHedgeAfter = interval;
            return this;
        }
    }

    /**
     * Point-in-time snapshot returned by {@link #stats()}: counters for
     * failures this client swallows by design instead of surfacing to the
     * caller — a dead replica leg on a write (client-side replication,
     * Fire-and-forget replica writes), a failed background repair of the primary after
     * read-repair found a value on another owner (read repair), and
     * a failed node-list refresh attempt or per-node reconnect during one.
     * None of these ever fail an operation; this is purely observability
     * so an operator who only watches for thrown exceptions can still
     * notice replication silently degrading or a node-list refresh stuck
     * failing.
     *
     * @param backgroundWriteBugs a genuine programming bug (never an
     * expected failure — those are already counted above) that escaped a
     * background replica write or read-repair write-back. Unlike the
     * other counters, this must never legitimately increase; it exists so
     * a bug in this SDK's own background-write handling can't vanish
     * silently the way an ignored {@code CompletableFuture.whenComplete}
     * error previously could (issue: audit finding). Also logged to
     * stderr when it happens — see {@link #reportBackgroundWriteBug}.
     */
    public record ClientStats(long replicaWriteFailures, long readRepairFailures, long refreshFailures,
            long backgroundWriteBugs) {}

    private static final int DEFAULT_COMPRESSION_THRESHOLD = 256;

    // See Options.reconnectCooldown. Mirrors sdk/typescript/src/client.ts's
    // DEFAULT_RECONNECT_COOLDOWN_MS and sdk/python/src/nanocached/client.py's
    // _DEFAULT_RECONNECT_COOLDOWN.
    private static final Duration DEFAULT_RECONNECT_COOLDOWN = Duration.ofMillis(1_000);

    // The server rejects (and drops the connection for) any request frame
    // over MAX_REQUEST_SIZE (src/server.rs), 1 MiB — a hard cap on the
    // *whole* frame, header included. Validating key/value length against
    // that exact number would still let a caller build a frame that trips
    // it once the "G "/"S "/"D "/lengths/ttl/tag header text and framing
    // are added, so this constant carries headroom for that header —
    // comfortably more than any header this SDK ever writes (issue: audit
    // finding J2). 256 bytes, standardized across every SDK (Go/Rust's
    // original value; TypeScript's and .NET's headroom constants match).
    // Catching an oversize request here, before it ever reaches Connection,
    // avoids the confusing alternative of the server silently closing the
    // connection with no response (a bare key+value length rejection is
    // never sent back — see request_is_too_large in server.rs).
    private static final int MAX_REQUEST_BYTES = 1024 * 1024 - 256;

    public static Options builder() {
        return new Options();
    }

    private static final Duration NODE_LIST_STALE_AFTER = Duration.ofSeconds(30);
    // Reserved by the SDKs so a real application key can never collide
    // with it: a GET refreshes the pinged key's server-side LRU recency,
    // which is exactly why collision would matter — an app using key
    // {0x00} would previously have had its recency silently refreshed on
    // every keep-alive tick. The leading 0x00 also keeps this out of any
    // plausible printable-string keyspace.
    private static final byte[] KEEPALIVE_KEY = keepAliveKey();

    private static byte[] keepAliveKey() {
        byte[] name = "nanocached-keepalive".getBytes(StandardCharsets.US_ASCII);
        byte[] key = new byte[1 + name.length];
        System.arraycopy(name, 0, key, 1, name.length);
        return key;
    }

    // Namespaces (issue #105): the default namespace, shared by every
    // un-namespaced get/set/delete overload below — routing, the wire
    // encoding (Connection.get/set/delete), and the hash ring (HashRing)
    // all treat a zero-length namespace as "no namespace at all", so
    // threading this through those call sites instead of duplicating them
    // costs nothing on the un-namespaced path.
    private static final byte[] EMPTY_NAMESPACE = new byte[0];

    // TTL a read-repair write uses (read repair), in whole seconds —
    // the protocol's TTL unit throughout (see set()'s ttlSeconds). The
    // original TTL isn't recoverable from a GET response, and repairing
    // with TTL 0 (no expiry) would permanently resurrect data that was
    // legitimately expiring; 60s bounds the overshoot instead — an
    // immortal key just gets re-repaired on a later miss. Cross-SDK
    // policy decision, applied identically across all SDKs.
    private static final long READ_REPAIR_TTL_SECONDS = 60;
    static volatile long keepAliveIntervalMillis = 30_000;
    // Fire-and-forget replica writes: bounds how many replica writes a single client
    // may have running in the background at once when
    // fireAndForgetReplicas is enabled — once the cap is reached, further
    // replica legs fall back to running synchronously, the same as with
    // the option off. Mutable only so tests can shrink it, mirroring
    // keepAliveIntervalMillis.
    static volatile int maxInFlightBackgroundReplicaWrites = 32;
    // Headroom above maxInFlightBackgroundReplicaWrites for replicaWriters'
    // fixed thread count (see openCluster): background legs are capped by
    // backgroundReplicaWritePermits, but synchronous-fallback legs (option
    // off, or the cap already reached) are not permit-gated and can pile up
    // from many concurrent write() calls — this headroom lets a burst of
    // them run with real parallelism instead of only ever queueing, without
    // reintroducing newCachedThreadPool's unbounded thread growth. Chosen,
    // not derived: no formula makes this precise, just generous enough in
    // practice.
    private static final int REPLICA_WRITER_POOL_HEADROOM = 16;

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
    private final boolean compress;
    private final int compressionThreshold;
    private final boolean fireAndForgetReplicas;
    private final boolean readRepair;
    private final long reconnectCooldownNanos;
    /** {@code true} when {@link Options#disableReconnectCooldown()} was
     * called: {@link #dialWithCooldown} then never records a cooldown
     * entry at all, so every dial to a known-dead address still pays its
     * own full connect attempt (see {@link Options#reconnectCooldown}'s
     * doc for the Duration.ZERO-means-default rule this complements). */
    private final boolean reconnectCooldownDisabled;
    /** Hedged reads (issue #64): 0 means off. See {@link Options#readHedgeAfter}. */
    private final long readHedgeAfterNanos;
    /** Hedge legs still in flight after a read has already returned (the
     * losers): finished detached on {@link #replicaWriters}, their result
     * discarded, drained by {@link #close()} exactly like {@link
     * #backgroundReplicaWritePermits}'s writes. Unlike that pool, hedge
     * legs are not permit-gated — a read may only ever have at most
     * {@code replication} legs in flight at once, so no separate cap is
     * needed. */
    private final Set<CompletableFuture<?>> hedgedReads = ConcurrentHashMap.newKeySet();
    /** Serializes a hedge leg's "check closed, then register" against
     * {@link #close()}'s "observe the set empty, then stop draining"
     * (issue #91). Without it a leg could be registered — and dialed
     * against a connection {@link #teardown()} is closing — after the drain
     * had already found the set empty. Held only briefly on both sides
     * (never across a leg's own {@code join()}), so it doesn't serialize
     * the reads themselves. */
    private final Object hedgedReadsLock = new Object();
    /** Per-address reconnect cooldown (see {@link Options#reconnectCooldown}):
     * the address of the most recently failed dial, and how long it stays
     * "down" before another dial to it is attempted. Keyed by address, not
     * slot — {@link #memberConnection}'s slot (node name) can be
     * reassigned to a different address by a refresh, but the address
     * itself is what's actually unreachable. Mirrors TypeScript's
     * reconnectCooldowns. */
    private final ConcurrentHashMap<String, CooldownEntry> reconnectCooldowns = new ConcurrentHashMap<>();

    private static final class CooldownEntry {
        final long untilNanos;
        final RuntimeException error;

        CooldownEntry(long untilNanos, RuntimeException error) {
            this.untilNanos = untilNanos;
            this.error = error;
        }
    }
    // Observability for failures this client swallows by design — see
    // stats(). AtomicLong because they're incremented from whichever
    // thread happens to hit the swallow site (foreground calls,
    // background replica writes, a refresh running on any caller's
    // thread).
    private final AtomicLong replicaWriteFailures = new AtomicLong();
    private final AtomicLong readRepairFailures = new AtomicLong();
    private final AtomicLong refreshFailures = new AtomicLong();
    private final AtomicLong backgroundWriteBugs = new AtomicLong();
    private java.util.concurrent.Semaphore backgroundReplicaWritePermits;
    // The permit count backgroundReplicaWritePermits was built with,
    // captured so close() can acquire exactly all of them even if a test
    // mutates the static maxInFlightBackgroundReplicaWrites afterwards.
    private int backgroundReplicaWritePermitCount;

    /** The address that answered connect() — used both to redial in single
     * mode and as the key for the open-sockets tracker in every mode
     * (mirrors TypeScript's {@code this.url}). Set once, before this
     * client ever opens a socket. */
    private String targetKey;

    private volatile boolean closed = false;
    // Gates close() atomically: the volatile boolean above gives
    // visibility but not atomicity, so two concurrent close() calls could
    // both pass a plain check and both run the teardown body (the same
    // check-then-set race Connection.poison() avoids via synchronized).
    private final java.util.concurrent.atomic.AtomicBoolean closeCalled =
            new java.util.concurrent.atomic.AtomicBoolean();
    // volatile so a reader sees a redial's new Connection right away.
    // redialLocks (per-slot monitors) give mutual exclusion for the
    // redial itself, but a monitor only guarantees visibility to threads
    // that acquire that *same* lock — and single/Member.connection are
    // read under different locks in different places (stateLock for the
    // keep-alive sweep, no lock at all on singleConnection()'s/
    // memberConnection()'s fast path), so a plain field could let a
    // thread keep seeing a stale, already-closed connection after
    // another thread redialed. volatile fixes the cross-thread
    // visibility on top of the locking that already exists — correct
    // double-checked locking. .NET's Connection sidesteps this by
    // routing every access through _stateLock instead of splitting it
    // across a lock and an unlocked fast path.
    private volatile Connection single;              // single-node mode
    private String singleAddress;
    private final Map<String, Member> members = new LinkedHashMap<>(); // cluster mode
    private HashRing ring;
    private int replication = 1;
    private long lastFetchNanos = System.nanoTime();

    private ExecutorService replicaWriters;
    private ScheduledExecutorService keepAlive;

    private static final class Member {
        String address;
        // volatile for the same cross-thread visibility reason as
        // `single` above — see that field's comment. null for a member
        // that discovery listed but that this client couldn't reach when
        // it bootstrapped (issue #67): it stays routable — a request for
        // one of its keys fails over the same way it would after a
        // mid-life node death — and the next request after the reconnect
        // cooldown redials it (see memberConnection).
        volatile Connection connection;

        Member(String address, Connection connection) {
            this.address = address;
            this.connection = connection;
        }
    }

    private NanocachedClient(
            List<Address> addresses, byte[] authSecret, SSLContext tls,
            boolean compress, int compressionThreshold, boolean fireAndForgetReplicas,
            boolean readRepair, Duration reconnectCooldown, boolean reconnectCooldownDisabled,
            Duration readHedgeAfter) {
        this.addresses = List.copyOf(addresses);
        this.authSecret = authSecret;
        this.tls = tls;
        this.compress = compress;
        this.compressionThreshold = compressionThreshold;
        this.fireAndForgetReplicas = fireAndForgetReplicas;
        this.readRepair = readRepair;
        this.reconnectCooldownNanos = reconnectCooldown.toNanos();
        this.reconnectCooldownDisabled = reconnectCooldownDisabled;
        this.readHedgeAfterNanos = readHedgeAfter != null ? readHedgeAfter.toNanos() : 0;
    }

    public static NanocachedClient connect(Options options) {
        if (options.addresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "nanocached: connect() needs a non-empty addresses list");
        }
        // Mirrors the Go SDK's Connect-time rejection: a negative
        // threshold would otherwise silently force every set() to
        // attempt compression regardless of value size (issue: audit
        // finding).
        if (options.compressionThreshold < 0) {
            throw new IllegalArgumentException(
                    "nanocached: compressionThreshold must not be negative, got "
                            + options.compressionThreshold);
        }

        SSLContext sslContext = buildSslContext(options.tls, options.ca);
        NanocachedClient client = new NanocachedClient(
                options.addresses, options.authSecret, sslContext,
                options.compress, options.compressionThreshold, options.fireAndForgetReplicas,
                options.readRepair, options.reconnectCooldown, options.reconnectCooldownDisabled,
                options.readHedgeAfter);

        // Walk the addresses until one yields a working target; an address
        // that is unreachable, warming up (B, discovery HA), or knows no live
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
                    client.single = client.newTrackedConnection(node.socket(), node.tagged());
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

    /** One bootstrap dial's outcome (issue #67): a live connection, or a
     * connection-level failure to tolerate — exactly one of {@link
     * #connection}/{@link #error} is set. A hard failure (an unparseable
     * address, or an address that identifies as something other than a
     * cache node) is a distinct, non-tolerated case — see {@link #hard}
     * and {@link #dialBootstrapNode}. */
    private static final class DialOutcome {
        final Connection connection;
        final RuntimeException error;
        final boolean hard;

        private DialOutcome(Connection connection, RuntimeException error, boolean hard) {
            this.connection = connection;
            this.error = error;
            this.hard = hard;
        }

        static DialOutcome success(Connection connection) {
            return new DialOutcome(connection, null, false);
        }

        static DialOutcome tolerable(RuntimeException error) {
            return new DialOutcome(null, error, false);
        }

        static DialOutcome hard(RuntimeException error) {
            return new DialOutcome(null, error, true);
        }
    }

    /**
     * Dials and identifies one node discovery listed, for {@link
     * #openCluster}'s concurrent bootstrap (issue #67). Never throws: a
     * connection-level failure — the dial itself failing, or the
     * identify exchange failing or being rejected — is tolerated and
     * comes back as {@link DialOutcome#tolerable}, exactly the class of
     * failure {@link #dialWithCooldown} already tolerates on a lazy
     * redial. An unparseable address, or an address that identifies as
     * something other than a cache node, is a hard error today and stays
     * one — reported as {@link DialOutcome#hard} rather than thrown
     * directly, so {@link #openCluster} can close whatever the other
     * concurrent dials already opened before it aborts the whole
     * bootstrap.
     */
    private DialOutcome dialBootstrapNode(DiscoveredNode node) {
        String address = node.address();
        int separator = address.lastIndexOf(':');
        Integer port = separator == -1 ? null : parsePort(address.substring(separator + 1));
        if (separator == -1 || port == null) {
            return DialOutcome.hard(new NanocachedException(
                    "nanocached: invalid node address from discovery server: " + address));
        }
        String host = address.substring(0, separator);

        Identify.Result identified;
        try {
            identified = Identify.connectAndIdentify(host, port, authSecret, tls);
        } catch (IOException | RuntimeException error) {
            return DialOutcome.tolerable(error instanceof RuntimeException runtime
                    ? runtime
                    : new NanocachedException.ConnectionFailed(
                            "nanocached: could not connect to " + address + ": " + error.getMessage(), error));
        }

        if (!(identified instanceof Identify.NodeTarget nodeTarget)) {
            return DialOutcome.hard(new NanocachedException(
                    "nanocached: discovery server returned a non-node address: " + address));
        }

        try {
            return DialOutcome.success(newTrackedConnection(nodeTarget.socket(), nodeTarget.tagged()));
        } catch (IOException error) {
            return DialOutcome.tolerable(new NanocachedException.ConnectionFailed(
                    "nanocached: could not connect to " + address + ": " + error.getMessage(), error));
        }
    }

    /**
     * Dials every node discovery listed, concurrently, on a short-lived
     * pool sized to the node count — {@link #replicaWriters} doesn't
     * exist yet at this point, since building it needs {@link #replication}
     * from this same cluster response, so bootstrap dialing gets its own
     * throwaway executor rather than reusing it. Every dial still honors
     * the same per-dial connect timeout as any other identify exchange
     * (see {@code Identify.CONNECT_TIMEOUT_MS}).
     *
     * <p>A node that can't be reached (issue #67: typically one that just
     * died and discovery hasn't evicted yet — its liveness window is
     * seconds long, and every key is still served by another owner once
     * replication &gt; 1) is installed as a member without a connection
     * and with its reconnect cooldown armed — exactly the state a member
     * is in after dying mid-life (see {@link #dialWithCooldown}) — so
     * requests for its keys fail over per request instead of the whole
     * {@code connect()} call failing, and the next request after the
     * cooldown redials it. Only a cluster with <em>no</em> reachable node
     * fails, with the last dial error.
     *
     * <p>A hard failure — an unparseable address, or an address that
     * identifies as something other than a cache node — aborts the whole
     * bootstrap regardless of how any other node fared, exactly as a
     * sequential dial hitting the same condition always has; every
     * connection another concurrent dial already opened is closed first
     * so this doesn't leak a socket for a node this client will now never
     * adopt.
     */
    private void openCluster(Identify.ClusterTarget cluster) {
        List<DiscoveredNode> nodes = cluster.nodes();

        ExecutorService dialers = Executors.newFixedThreadPool(
                Math.max(1, nodes.size()), runnable -> {
                    Thread thread = new Thread(runnable, "nanocached-bootstrap-dial");
                    thread.setDaemon(true);
                    return thread;
                });
        List<CompletableFuture<DialOutcome>> futures = new ArrayList<>(nodes.size());
        List<DialOutcome> outcomes = new ArrayList<>(nodes.size());
        try {
            for (DiscoveredNode node : nodes) {
                futures.add(CompletableFuture.supplyAsync(() -> dialBootstrapNode(node), dialers));
            }
            for (CompletableFuture<DialOutcome> future : futures) {
                outcomes.add(future.join());
            }
        } finally {
            dialers.shutdown();
        }

        RuntimeException hardError = null;
        for (DialOutcome outcome : outcomes) {
            if (outcome.hard && hardError == null) hardError = outcome.error;
        }
        if (hardError != null) {
            for (DialOutcome outcome : outcomes) {
                if (outcome.connection != null) outcome.connection.close();
            }
            throw hardError;
        }

        List<String> names = new ArrayList<>(nodes.size());
        int reachable = 0;
        RuntimeException lastError = null;
        for (int i = 0; i < nodes.size(); i++) {
            DiscoveredNode node = nodes.get(i);
            DialOutcome outcome = outcomes.get(i);
            names.add(node.name());
            if (outcome.connection != null) {
                members.put(node.name(), new Member(node.address(), outcome.connection));
                reachable++;
            } else {
                members.put(node.name(), new Member(node.address(), null));
                armReconnectCooldown(node.address(), outcome.error);
                lastError = outcome.error;
            }
        }

        if (reachable == 0) {
            throw lastError != null
                    ? lastError
                    : new NanocachedException("nanocached: could not reach any node in the cluster");
        }

        ring = new HashRing(names);
        replication = cluster.replication();
        backgroundReplicaWritePermitCount = maxInFlightBackgroundReplicaWrites;
        backgroundReplicaWritePermits = new java.util.concurrent.Semaphore(backgroundReplicaWritePermitCount);
        // Bounded (not newCachedThreadPool, which grows one thread per
        // submitted task with no cap) — see REPLICA_WRITER_POOL_HEADROOM.
        // The queue backing a fixed-size pool is unbounded, so a burst
        // beyond the fixed thread count simply queues rather than being
        // rejected or blocking the submitter; only the thread count itself
        // is bounded (issue: audit finding, unbounded replica-writer
        // threads).
        replicaWriters = Executors.newFixedThreadPool(
                backgroundReplicaWritePermitCount + REPLICA_WRITER_POOL_HEADROOM, runnable -> {
                    Thread thread = new Thread(runnable, "nanocached-replica-writer");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    // ── 公開 API ──────────────────────────────────────────────────

    /** How many nodes hold each key (client-side replication) — 1 against a single node. */
    public int replication() {
        return ring != null ? replication : 1;
    }

    public boolean isClosed() {
        return closed;
    }

    /** A namespace-scoped handle (namespaces, issue #105) — see {@link
     * Namespace}. {@code namespace} is UTF-8 encoded, matching every other
     * key-ish {@code String} overload in this class. */
    public Namespace namespace(String namespace) {
        return namespace(namespace.getBytes(StandardCharsets.UTF_8));
    }

    /** As {@link #namespace(String)}. An empty {@code namespace} is not
     * rejected — it returns a handle equivalent to this client itself
     * (the same legacy wire frames and routing, per the namespaces spec). */
    public Namespace namespace(byte[] namespace) {
        return new Namespace(namespace);
    }

    /**
     * A lightweight, namespace-scoped view of this client (namespaces,
     * issue #105): the same {@code get}/{@code getBytes}/{@code set}/
     * {@code delete} surface {@link NanocachedClient} itself exposes,
     * every key scoped to {@link #namespace()} — the same key name in two
     * namespaces (or the default, un-namespaced keyspace) is three
     * independent entries, since the namespace enters the HRW routing
     * hash alongside the key (see {@link HashRing}) and leads the body of
     * the {@code g}/{@code s}/{@code d} wire frames (see {@link
     * Connection}).
     *
     * <p>Obtained via {@link #namespace(byte[])}/{@link
     * #namespace(String)}; cheap (holds only the namespace bytes and this
     * client's own reference — no networking of its own), shares this
     * client's connections, and forwards every call to this client's
     * internal (namespace, key) methods rather than duplicating them —
     * routing, replication fan-out, hedged reads, {@code W}
     * refresh-and-retry, response tags, and compression all behave
     * exactly as they do for the un-namespaced API. Invalid once {@link
     * #close()} has run, raising the same {@link
     * NanocachedException.AlreadyClosed} a direct call on this client
     * would.
     */
    public final class Namespace {
        private final byte[] namespace;

        private Namespace(byte[] namespace) {
            this.namespace = namespace;
        }

        /** The namespace this handle scopes every call to. A defensive
         * copy — unlike a key or value, which this SDK never clones on
         * the way in or out, this array is reused for every future call
         * this handle makes, so handing back the live reference would let
         * a caller who mutates it corrupt this handle's routing from then
         * on. */
        public byte[] namespace() {
            return namespace.clone();
        }

        /** Returns the value decoded as strict UTF-8, or {@code
         * Optional.empty()} when the key is missing.
         * @throws UncheckedIOException if the stored value is not valid UTF-8 */
        public Optional<String> get(String key) {
            return get(key.getBytes(StandardCharsets.UTF_8));
        }

        /** As {@link #get(String)}. */
        public Optional<String> get(byte[] key) {
            return getBytes(key).map(NanocachedClient::decodeUtf8Strict);
        }

        /** Returns the raw value, or {@code Optional.empty()} when the key
         * is missing. */
        public Optional<byte[]> getBytes(String key) {
            return getBytes(key.getBytes(StandardCharsets.UTF_8));
        }

        /** As {@link #getBytes(String)}. Same semantics as {@link
         * NanocachedClient#getBytes(byte[])} — compression, read repair,
         * hedged reads — just scoped to {@link #namespace()}. */
        public Optional<byte[]> getBytes(byte[] key) {
            return NanocachedClient.this.getBytes(namespace, key);
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

        /** As {@link NanocachedClient#set(byte[], byte[], long)}, scoped
         * to {@link #namespace()}. */
        public void set(byte[] key, byte[] value, long ttlSeconds) {
            NanocachedClient.this.set(namespace, key, value, ttlSeconds);
        }

        public boolean delete(String key) {
            return delete(key.getBytes(StandardCharsets.UTF_8));
        }

        /** Returns whether the key existed before this call, scoped to
         * {@link #namespace()}. */
        public boolean delete(byte[] key) {
            return NanocachedClient.this.delete(namespace, key);
        }
    }

    /**
     * A snapshot of counters for failures this client swallows by design
     * (replica-leg writes, read repair, and
     * node-list refresh) — see {@link ClientStats} for exactly what each
     * counts. Nothing here ever fails an operation; this exists purely so
     * an operator can detect replication silently degrading or a
     * node-list refresh that is stuck failing.
     */
    public ClientStats stats() {
        return new ClientStats(
                replicaWriteFailures.get(), readRepairFailures.get(), refreshFailures.get(),
                backgroundWriteBugs.get());
    }

    /** Inspects a background replica write's ({@code fireAndForgetReplicas},
     * or read-repair's write-back to the primary) outcome. Both Runnables
     * already swallow every expected failure internally (the replica
     * leg's {@code NanocachedException} catch in {@link #write}, counted
     * via {@link #replicaWriteFailures}; the read-repair leg's identical
     * catch in {@link #tryReadRepair}, counted via {@link
     * #readRepairFailures}), so {@code error} here is only ever non-null
     * for a genuine programming bug that escaped one of those catches —
     * it must not vanish silently the way an ignored {@code
     * CompletableFuture.whenComplete} error previously did (issue: audit
     * finding). Counted in {@link #backgroundWriteBugs}, a stat distinct
     * from the expected-failure counters above, and logged to stderr —
     * this class's existing way of surfacing something a caller can't
     * otherwise observe (see the forgotten-close warning in {@link
     * #connect}). Mirrors .NET's {@code NanocachedClient.cs} background-task
     * {@code ContinueWith} handling of the same case, though .NET folds it
     * back into its single expected-failure counter for lack of a logger
     * to hand the bug to instead; this SDK has one, so it gets both. */
    private void reportBackgroundWriteBug(Throwable error) {
        if (error == null) return;
        backgroundWriteBugs.incrementAndGet();
        System.err.println("nanocached: a background write raised an unexpected error: " + error);
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
     * missing. Transparently decompresses when {@code compress} is
     * enabled (value compression). With {@code readRepair}, a clean miss
     * probes the remaining owners before being accepted as final
     * (read repair). */
    public Optional<byte[]> getBytes(byte[] key) {
        return getBytes(EMPTY_NAMESPACE, key);
    }

    /** The namespaced counterpart of every {@code get}/{@code getBytes}
     * overload above (issue #105) — see {@link #namespace}. {@code
     * namespace} empty is exactly the un-namespaced form: same routing,
     * same legacy wire frame. */
    Optional<byte[]> getBytes(byte[] namespace, byte[] key) {
        validateKey(namespace, key);
        beforeOperation();
        byte[] value = withWrongNodeRetry(
                () -> read(namespace, key, connection -> connection.get(namespace, key)));
        if (value == null && readRepair && ring != null) {
            value = tryReadRepair(namespace, key);
        }
        if (value == null) return Optional.empty();
        return Optional.of(compress ? Compression.decompressValue(value) : value);
    }

    /** read repair: probes the remaining owners of {@code key} —
     * every owner but the primary, which the normal read path already
     * probed and got a clean miss from — in rank order, for a value
     * (issue: audit finding, tryReadRepair used to re-probe the primary
     * too, wasting a redundant GET on the one owner already known to
     * have missed; mirrors the Rust/Go SDKs' identical fix). The first
     * one that has it wins: its value is returned, and — detached, not
     * awaited, no tracking — that same value repairs the true primary in
     * the background, with TTL READ_REPAIR_TTL_SECONDS (the original TTL
     * can't be recovered from a GET, and TTL 0 would permanently
     * resurrect already-expired data). Every failure along the way
     * (connection lost, WrongNode, another miss) is swallowed; nothing
     * here may turn an already-accepted miss into an error. A failure
     * repairing the primary specifically is counted via {@link
     * #stats()}'s readRepairFailures. The write-back is bounded by — and
     * drained on {@link #close()} through — the same {@link
     * #backgroundReplicaWritePermits} pool as a fireAndForgetReplicas
     * replica leg (fire-and-forget replica writes): past the cap, the repair for this
     * miss is simply skipped, since read repair is opportunistic and a
     * later miss on the same key repairs it (issue: audit finding,
     * unbounded/undrained read-repair write-backs). */
    private byte[] tryReadRepair(byte[] namespace, byte[] key) {
        List<String> names = ownerNames(namespace, key);
        if (names.isEmpty()) return null;
        String primary = names.get(0);
        for (String name : names.subList(1, names.size())) {
            byte[] value;
            try {
                value = applyReconnecting(() -> memberConnection(name), connection -> connection.get(namespace, key));
            } catch (RuntimeException ignored) {
                continue;
            }
            if (value == null) continue;

            if (backgroundReplicaWritePermits.tryAcquire()) {
                byte[] repairValue = value;
                Runnable repair = () -> {
                    try {
                        applyReconnecting(() -> memberConnection(primary), connection -> {
                            connection.set(namespace, key, repairValue, READ_REPAIR_TTL_SECONDS);
                            return null;
                        });
                    } catch (NanocachedException ignored) {
                        // Swallowed by design — see the doc comment; now
                        // counted via stats().readRepairFailures. Narrowed
                        // to the connection layer's own failure types, so
                        // a programming bug here (e.g. a
                        // NullPointerException) propagates instead of
                        // being treated identically to a dead primary.
                        readRepairFailures.incrementAndGet();
                    }
                };
                try {
                    CompletableFuture.runAsync(repair, replicaWriters)
                            .whenComplete((ignoredResult, error) -> {
                                backgroundReplicaWritePermits.release();
                                // error is non-null only for a genuine bug
                                // that escaped repair's own catch above —
                                // see reportBackgroundWriteBug (issue:
                                // audit finding, background writes
                                // silently discarding this).
                                reportBackgroundWriteBug(error);
                            });
                } catch (RejectedExecutionException rejected) {
                    // close() shut replicaWriters down concurrently — see
                    // the matching handling in write(). The repair is
                    // opportunistic, so simply release the permit and skip
                    // it rather than run it inline; a later miss repairs
                    // the primary anyway.
                    backgroundReplicaWritePermits.release();
                }
            }
            return value;
        }
        return null;
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

    /** {@code ttlSeconds == 0} means no expiry. Transparently compresses
     * values at or above {@code compressionThreshold} when {@code
     * compress} is enabled (value compression). */
    public void set(byte[] key, byte[] value, long ttlSeconds) {
        set(EMPTY_NAMESPACE, key, value, ttlSeconds);
    }

    /** The namespaced counterpart of {@link #set(byte[], byte[], long)}
     * (issue #105) — see {@link #namespace}. */
    void set(byte[] namespace, byte[] key, byte[] value, long ttlSeconds) {
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException(
                    "nanocached: ttlSeconds must be non-negative, got " + ttlSeconds);
        }
        validateKeyAndValue(namespace, key, value);
        beforeOperation();
        byte[] outgoing = compress ? Compression.compressValue(value, compressionThreshold) : value;
        Long wireTtlSeconds = ttlSeconds == 0 ? null : ttlSeconds;
        withWrongNodeRetry(() -> {
            write(namespace, key, connection -> {
                connection.set(namespace, key, outgoing, wireTtlSeconds);
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
        return delete(EMPTY_NAMESPACE, key);
    }

    /** The namespaced counterpart of {@link #delete(byte[])} (issue #105) —
     * see {@link #namespace}. */
    boolean delete(byte[] namespace, byte[] key) {
        validateKey(namespace, key);
        beforeOperation();
        return withWrongNodeRetry(
                () -> write(namespace, key, connection -> connection.delete(namespace, key)));
    }

    /** Rejects an empty key or a (namespace, key) pair so large that {@code
     * "G "}/{@code "D "}/{@code "g "}/{@code "d "} plus the namespace and
     * key alone would already risk tripping the server's MAX_REQUEST_SIZE
     * (issue: audit finding J2; namespace bytes folded in for issue #105 —
     * they lead the body exactly like the key does, so they count toward
     * the same frame-size risk) — checked synchronously, before any
     * connection is touched, mirroring {@link #set(byte[], byte[],
     * byte[], long)}'s ttlSeconds check. The namespace itself has no
     * length limit beyond this shared one (namespaces spec): there is no
     * separate namespace-only cap. */
    private static void validateKey(byte[] namespace, byte[] key) {
        if (key.length == 0) {
            throw new IllegalArgumentException("nanocached: key must not be empty");
        }
        long total = (long) namespace.length + key.length;
        if (total > MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException(
                    "nanocached: namespace (" + namespace.length + " bytes) + key (" + key.length
                            + " bytes) = " + total + " bytes, which exceeds the " + MAX_REQUEST_BYTES
                            + "-byte request limit (server MAX_REQUEST_SIZE, src/server.rs, is 1 MiB)");
        }
    }

    /** As {@link #validateKey}, plus rejects a namespace+key+value triple
     * too large for a single {@code S}/{@code s} request to have any
     * chance of fitting under the server's MAX_REQUEST_SIZE (issue: audit
     * finding J2; namespace folded in for issue #105). Checked against the
     * caller-supplied value, before compression — compression only ever
     * shrinks what actually goes on the wire, so this is the conservative
     * (never falsely permissive) side to check. */
    private static void validateKeyAndValue(byte[] namespace, byte[] key, byte[] value) {
        validateKey(namespace, key);
        long total = (long) namespace.length + key.length + value.length;
        if (total > MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException(
                    "nanocached: namespace (" + namespace.length + " bytes) + key (" + key.length
                            + " bytes) + value (" + value.length + " bytes) = " + total
                            + " bytes, which exceeds the " + MAX_REQUEST_BYTES
                            + "-byte request limit (server MAX_REQUEST_SIZE, src/server.rs, is 1 MiB)");
        }
    }

    /** Idempotent (later get/set/delete throw {@link NanocachedException.AlreadyClosed}),
     * but a second call warns to stderr — usually a sign the caller lost
     * track of this instance's lifecycle. */
    @Override
    public void close() {
        if (!closeCalled.compareAndSet(false, true)) {
            System.err.println("nanocached: close() called again on an already-closed client");
            return;
        }
        closed = true;
        if (keepAlive != null) {
            keepAlive.shutdownNow();
            awaitTerminationQuietly(keepAlive);
        }
        // Fire-and-forget replica writes: give background replica writes a chance to
        // finish before their connections are torn out from under them.
        // Acquiring every permit — rather than snapshotting the future
        // set — closes the registration race: a write() that passed its
        // closed check before this call can still be about to start a
        // background leg, and a snapshot taken in that window would miss
        // it. Once all permits are held here no new leg can start
        // (tryAcquire fails, so the leg falls back to the synchronous
        // path), and each permit is only released after its leg
        // completed. Bounded by the permit count, so this is a short wait
        // in practice.
        if (backgroundReplicaWritePermits != null) {
            backgroundReplicaWritePermits.acquireUninterruptibly(backgroundReplicaWritePermitCount);
        }
        // Hedged reads (issue #64): the losing leg of a hedge is never
        // cancelled (see Options.readHedgeAfter's doc for why), so it's
        // still running on replicaWriters here — drain it exactly like the
        // background replica writes just above, before that pool is shut
        // down.
        drainHedgedReads();
        if (replicaWriters != null) {
            replicaWriters.shutdown();
            // A longer bound than keepAlive's: a synchronous (or
            // replication-factor-exceeded fallback) replica leg is neither
            // permit-tracked nor interrupted, so — unlike the fire-and-forget
            // and hedged legs already drained above — it can still be doing
            // real work here, blocked in Connection.request()'s future.join()
            // for up to the connection request timeout (issue #97). Wait it
            // out (plus the thread-teardown margin) rather than proceed into
            // teardown() and close the connection it's reading from, which
            // would poison its future and surface a spurious I/O exception on
            // a leg the caller never saw — contradicting "close() drains
            // everything". The leg self-bounds at requestTimeoutMillis, so
            // this is bounded regardless of how unresponsive the node is.
            awaitTerminationQuietly(replicaWriters, replicaWriterDrainTimeoutMillis());
        }
        teardown();
    }

    /** How long {@link #close()} waits for {@link #replicaWriters} to
     * terminate: the connection request timeout (the longest a still-running
     * synchronous replica leg can be blocked in {@code future.join()}) plus
     * the same thread-teardown margin keepAlive gets — see the call site and
     * {@link #EXECUTOR_TERMINATION_TIMEOUT_SECONDS} (issue #97). Reads the
     * live {@code Connection.requestTimeoutMillis}, so a test that shortens
     * it shortens this too. */
    private static long replicaWriterDrainTimeoutMillis() {
        return Connection.requestTimeoutMillis + executorTerminationTimeoutMillis;
    }

    /** Blocks until every hedge leg still tracked in {@link #hedgedReads}
     * (the losing legs of reads that already returned via their winning
     * leg) has finished, its outcome discarded either way. Looped, not a
     * single pass over one snapshot: a read racing this close() call can
     * still register a new leg after a snapshot was taken but before this
     * method returns, so re-checking until the set is genuinely empty keeps
     * one from leaking past close() undrained. The emptiness check is taken
     * under {@link #hedgedReadsLock} (issue #91): {@link #close()} sets
     * {@code closed} before calling this, and {@link #startHedgeLeg} checks
     * {@code closed} under the same lock before registering, so once this
     * observes the set empty while holding the lock no further leg can be
     * added — {@code startHedgeLeg} would see {@code closed} and throw. The
     * blocking {@code join()}s happen outside the lock so they never block a
     * concurrent (doomed) registration. */
    private void drainHedgedReads() {
        while (true) {
            List<CompletableFuture<?>> snapshot;
            synchronized (hedgedReadsLock) {
                if (hedgedReads.isEmpty()) {
                    return;
                }
                snapshot = List.copyOf(hedgedReads);
            }
            for (CompletableFuture<?> future : snapshot) {
                try {
                    future.join();
                } catch (RuntimeException ignored) {
                    // A losing leg's own failure (expected or a genuine
                    // bug) is irrelevant now — the read it belonged to
                    // already returned via its winning leg.
                }
            }
        }
    }

    // The thread-teardown margin for close()'s executor awaits — how long
    // to wait for an already-shutdown executor's worker threads to actually
    // finish exiting once the real work they were doing is accounted for,
    // without which close() could return (and teardown() tear the
    // connections out from under a task) before every task genuinely
    // stopped running (issue: audit finding, close() not awaiting
    // termination). A few seconds is generous for keepAlive (shutdownNow()
    // interrupted it) and for replicaWriters' *tracked* work (the
    // permit-drain above already waited it out). It is NOT enough on its
    // own for a still-running synchronous replica leg, which is neither
    // permit-tracked nor interrupted — replicaWriters is awaited for
    // requestTimeout + this margin instead (see replicaWriterDrainTimeoutMillis,
    // issue #97). Non-final only so tests can shorten it, mirroring
    // Connection.requestTimeoutMillis.
    static volatile long executorTerminationTimeoutMillis = 5_000;

    private static void awaitTerminationQuietly(ExecutorService executor) {
        awaitTerminationQuietly(executor, executorTerminationTimeoutMillis);
    }

    private static void awaitTerminationQuietly(ExecutorService executor, long timeoutMillis) {
        try {
            executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Closes every connection this attempt opened. Called both from
     * {@link #close()} (after it has already drained/shut down {@link
     * #replicaWriters}/{@link #keepAlive} itself) and from {@link
     * #connect}'s catch blocks whenever anything after {@link
     * #openCluster} fails — including {@link #startKeepAlive} itself
     * throwing, which can happen after openCluster already built {@link
     * #replicaWriters} and {@link #backgroundReplicaWritePermits}. That
     * second caller used to leave replicaWriters running (its daemon
     * threads survive until the executor is GC'd) and its permits
     * un-drained, because this method only ever closed connections
     * (issue: audit finding, connect()-failure teardown missing
     * replicaWriters/keepAlive). Shutting them down here too — redundant
     * but harmless when {@link #close()} already did it, a no-op when
     * they were never created — makes this method a complete teardown of
     * everything a partially-succeeded connect() attempt could have
     * built, not just its sockets. */
    private void teardown() {
        synchronized (stateLock) {
            if (single != null) single.close();
            for (Member member : members.values()) {
                if (member.connection != null) member.connection.close();
            }
        }
        if (keepAlive != null) {
            keepAlive.shutdownNow();
        }
        if (replicaWriters != null) {
            replicaWriters.shutdown();
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

    /** {@code namespace} empty is exactly the un-namespaced form — {@link
     * HashRing#owners(byte[], byte[], int)} treats it identically to
     * {@link HashRing#owners(byte[], int)} (namespaces, issue #105). */
    private List<String> ownerNames(byte[] namespace, byte[] key) {
        synchronized (stateLock) {
            return ring.owners(namespace, key, replication);
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

    private <T> T read(byte[] namespace, byte[] key, ConnectionOp<T> op) {
        if (ring == null) {
            return applyReconnecting(this::singleConnection, op);
        }

        List<String> names = ownerNames(namespace, key);
        if (readHedgeAfterNanos > 0 && names.size() > 1) {
            return readHedged(op, names);
        }

        // Owners in rank order; fall through only on connection-level
        // failure — a replica hedges against a dead holder, not a miss.
        // Narrowed to NanocachedException, matching write()'s replicaWrite
        // catch (~line 836): WrongNode is already peeled off by the catch
        // above, so this only ever sees ConnectionFailed/AlreadyClosed/etc,
        // and a genuine programming bug (e.g. a NullPointerException)
        // propagates instead of being treated identically to a dead owner
        // and silently retried against the next one (issue: audit finding,
        // overbroad RuntimeException catch).
        RuntimeException lastError = null;
        for (String name : names) {
            try {
                return applyReconnecting(() -> memberConnection(name), op);
            } catch (NanocachedException.WrongNode error) {
                throw error;
            } catch (NanocachedException error) {
                lastError = error;
            }
        }
        throw lastError != null
                ? lastError
                : new NanocachedException("nanocached: no owner is reachable for this key");
    }

    /** One hedge leg's outcome, however it happened — see {@link
     * #readHedged}. Exactly one of {@code wrongNode}/{@code failure}/
     * {@code bug} is set for anything but a hit ({@code value} may
     * legitimately be {@code null} too, for a miss). */
    private static final class LegOutcome<T> {
        final T value;
        final NanocachedException.WrongNode wrongNode;
        final NanocachedException failure;
        final RuntimeException bug;

        private LegOutcome(T value, NanocachedException.WrongNode wrongNode,
                NanocachedException failure, RuntimeException bug) {
            this.value = value;
            this.wrongNode = wrongNode;
            this.failure = failure;
            this.bug = bug;
        }

        static <T> LegOutcome<T> hit(T value) {
            return new LegOutcome<>(value, null, null, null);
        }

        static <T> LegOutcome<T> wrongNode(NanocachedException.WrongNode error) {
            return new LegOutcome<>(null, error, null, null);
        }

        static <T> LegOutcome<T> failure(NanocachedException error) {
            return new LegOutcome<>(null, null, error, null);
        }

        static <T> LegOutcome<T> bug(RuntimeException error) {
            return new LegOutcome<>(null, null, null, error);
        }
    }

    /**
     * Hedged reads (issue #64): one slow — not dead — owner otherwise
     * bounds every read that touches it at its full round trip, since the
     * sequential path above only moves on to the next owner when the
     * current one *fails*. Here the read starts at the primary ({@code
     * names.get(0)}), and if no answer has arrived within {@code
     * readHedgeAfterNanos} the same read is also sent to the next owner
     * (and so on, one more owner per interval, until every owner is in
     * flight); the first answer decides:
     *
     * <ul>
     * <li>a hit from any owner is final;
     * <li>a miss is final only from the primary — a replica's miss is
     * provisional (it may simply lack the copy) and the primary is still
     * waited for, so hedging never turns a hit into a miss; it is
     * accepted only once every owner has answered or failed;
     * <li>a failure ({@link NanocachedException} other than {@link
     * NanocachedException.WrongNode}, or a connection-level one) hedges
     * onward immediately — the moment nothing else is still in flight,
     * not after waiting out another full interval;
     * <li>{@link NanocachedException.WrongNode} propagates exactly as the
     * sequential path's does.
     * </ul>
     *
     * A losing leg is never cancelled (interrupting a request mid-write
     * could desync that connection — see {@link Connection}) but left to
     * run to completion, detached, on {@link #replicaWriters}; its result
     * is discarded and {@link #close()} drains it via {@link
     * #hedgedReads} exactly like a {@code fireAndForgetReplicas} write.
     */
    private <T> T readHedged(ConnectionOp<T> op, List<String> names) {
        BlockingQueue<Integer> completions = new LinkedBlockingQueue<>();
        Map<Integer, LegOutcome<T>> results = new ConcurrentHashMap<>();

        int nextIndex = 1;
        startHedgeLeg(0, names.get(0), op, completions, results);
        int pendingCount = 1;

        RuntimeException lastError = null;
        boolean replicaMissed = false;

        while (pendingCount > 0) {
            Integer index;
            try {
                index = nextIndex < names.size()
                        ? completions.poll(readHedgeAfterNanos, TimeUnit.NANOSECONDS)
                        : completions.take();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NanocachedException(
                        "nanocached: interrupted while waiting for a hedged read", interrupted);
            }

            if (index == null) {
                // Hedge interval elapsed with no answer: one more owner,
                // without waiting for any leg already in flight.
                startHedgeLeg(nextIndex, names.get(nextIndex), op, completions, results);
                pendingCount++;
                nextIndex++;
                continue;
            }

            pendingCount--;
            LegOutcome<T> outcome = results.remove(index);
            if (outcome.wrongNode != null) {
                throw outcome.wrongNode;
            } else if (outcome.bug != null) {
                throw outcome.bug;
            } else if (outcome.failure != null) {
                lastError = outcome.failure;
            } else if (outcome.value != null || index == 0) {
                return outcome.value;
            } else {
                replicaMissed = true;
            }

            if (pendingCount == 0 && nextIndex < names.size()) {
                // Everything started so far has failed or missed
                // provisionally: the next owner gets its turn right away,
                // rather than waiting out another full interval.
                startHedgeLeg(nextIndex, names.get(nextIndex), op, completions, results);
                pendingCount++;
                nextIndex++;
            }
        }

        if (replicaMissed) return null;
        throw lastError != null
                ? lastError
                : new NanocachedException("nanocached: no owner is reachable for this key");
    }

    /** Starts one hedge leg against {@code names.get(index)} (via {@code
     * name}), running it on {@link #replicaWriters} — falling back to
     * running it inline if that pool was concurrently shut down by {@link
     * #close()}, mirroring {@link #submitReplicaWrite} — and reporting its
     * outcome by putting it in {@code results} and offering {@code index}
     * to {@code completions}, rather than through the {@link
     * CompletableFuture}'s own result: {@link #readHedged} needs to learn
     * of a completion the instant it happens, including one that races in
     * while it's blocked waiting on an earlier leg, which a queue gives
     * for free and a bare future does not. The future itself exists only
     * so {@link #hedgedReads}/{@link #drainHedgedReads} can still block
     * {@link #close()} until this leg — win or lose — actually finishes. */
    private <T> void startHedgeLeg(int index, String name, ConnectionOp<T> op,
            BlockingQueue<Integer> completions, Map<Integer, LegOutcome<T>> results) {
        Runnable task = () -> {
            LegOutcome<T> outcome;
            try {
                T value = applyReconnecting(() -> memberConnection(name), op);
                outcome = LegOutcome.hit(value);
            } catch (NanocachedException.WrongNode error) {
                outcome = LegOutcome.wrongNode(error);
            } catch (NanocachedException error) {
                outcome = LegOutcome.failure(error);
            } catch (RuntimeException error) {
                outcome = LegOutcome.bug(error);
            }
            results.put(index, outcome);
            completions.add(index);
        };

        // Check closed and register under hedgedReadsLock so this can't
        // interleave with close()'s drain observing the set empty (issue
        // #91): close() sets `closed` before its drain takes this lock, so
        // a leg that finds `closed` here must not start — it would run
        // against connections teardown is about to close and never be
        // awaited. A leg that passes the check is added to hedgedReads
        // before the lock is released, so the drain's next locked snapshot
        // sees it.
        synchronized (hedgedReadsLock) {
            if (closed) {
                throw new NanocachedException.AlreadyClosed();
            }
            CompletableFuture<Void> started;
            try {
                started = CompletableFuture.runAsync(task, replicaWriters);
            } catch (RejectedExecutionException rejected) {
                // close() shut replicaWriters down concurrently: run it inline
                // rather than losing it (mirrors submitReplicaWrite).
                task.run();
                started = CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> future = started;
            hedgedReads.add(future);
            future.whenComplete((ignoredResult, ignoredError) -> hedgedReads.remove(future));
        }
    }

    private <T> T write(byte[] namespace, byte[] key, ConnectionOp<T> op) {
        if (ring == null) {
            return applyReconnecting(this::singleConnection, op);
        }

        List<String> names = ownerNames(namespace, key);
        if (names.isEmpty()) {
            throw new NanocachedException("nanocached: no owner is reachable for this key");
        }

        List<CompletableFuture<Void>> replicaWrites = new ArrayList<>();
        for (int i = 1; i < names.size(); i++) {
            String replica = names.get(i);
            Runnable replicaWrite = () -> {
                try {
                    applyReconnecting(() -> memberConnection(replica), op);
                } catch (NanocachedException ignored) {
                    // Swallowed by design (client-side replication): a dead or disagreeing
                    // replica leaves the key under-replicated until the next
                    // node-list refresh, never fails the write. Counted via
                    // stats().replicaWriteFailures so operators can spot
                    // silently degrading replication. Narrowed to the
                    // connection layer's own failure types, covering both
                    // the fire-and-forget and synchronous-fallback callers
                    // of this lambda, so a programming bug doesn't get
                    // treated the same way as a dead replica.
                    replicaWriteFailures.incrementAndGet();
                }
            };

            // Fire-and-forget replica writes: with fireAndForgetReplicas, up to
            // maxInFlightBackgroundReplicaWrites legs run in the
            // background instead of being waited for below — past that
            // cap, further legs fall back to the synchronous path exactly
            // as with the option off.
            if (fireAndForgetReplicas && backgroundReplicaWritePermits.tryAcquire()) {
                try {
                    CompletableFuture.runAsync(replicaWrite, replicaWriters)
                            .whenComplete((ignoredResult, error) -> {
                                backgroundReplicaWritePermits.release();
                                // error is non-null only for a genuine bug
                                // that escaped replicaWrite's own catch
                                // above — see reportBackgroundWriteBug
                                // (issue: audit finding, background writes
                                // silently discarding this).
                                reportBackgroundWriteBug(error);
                            });
                } catch (RejectedExecutionException rejected) {
                    // close() shut replicaWriters down concurrently: the
                    // permit was already acquired, but the task was never
                    // submitted, so whenComplete would never run to release
                    // it — release it here instead of leaking it, and run
                    // the leg inline rather than losing it (issue: audit
                    // finding, background-write permit leak on close race).
                    backgroundReplicaWritePermits.release();
                    replicaWrite.run();
                }
                continue;
            }

            replicaWrites.add(submitReplicaWrite(replicaWrite));
        }

        T value = null;
        RuntimeException primaryError = null;
        try {
            value = applyReconnecting(() -> memberConnection(names.get(0)), op);
        } catch (RuntimeException error) {
            primaryError = error;
        }

        // Always drain the synchronous replica legs — for close()'s
        // tracking, and so a genuine replica-leg bug (a RuntimeException
        // that escaped replicaWrite's own NanocachedException-only catch
        // above) doesn't linger as an unobserved CompletableFuture
        // failure — but never let one override an already-successful
        // primary write: the write happened, so throwing here despite
        // that would misreport a completed write as failed. This used to
        // be a plain `finally { pending.join(); }`, whose
        // CompletionException — thrown even when the primary's try block
        // had already returned normally — silently replaced a successful
        // result, or masked the primary's own exception, with a raw,
        // un-narrowed CompletionException that (unlike everything else
        // this SDK throws) didn't even extend NanocachedException (issue:
        // audit finding). Mirrors the TypeScript SDK's writeToOwners
        // (client.ts) / the Python SDK's _write() (client.py): a genuine
        // replica bug is only ever surfaced by throwing when the primary
        // itself also failed — any other one is still counted/logged via
        // reportBackgroundWriteBug, the same as a fireAndForgetReplicas
        // leg's bug above.
        RuntimeException replicaBug = null;
        for (CompletableFuture<Void> pending : replicaWrites) {
            try {
                pending.join();
            } catch (CompletionException wrapped) {
                RuntimeException bug = unwrapReplicaBug(wrapped);
                if (primaryError != null && replicaBug == null) {
                    replicaBug = bug;
                } else {
                    reportBackgroundWriteBug(bug);
                }
            }
        }

        if (primaryError != null) {
            throw replicaBug != null ? replicaBug : primaryError;
        }
        return value;
    }

    /** Unwraps a replica leg's {@link CompletionException} (from {@link
     * CompletableFuture#join}) down to the raw bug that caused it — a
     * {@code Runnable} run via {@link CompletableFuture#runAsync} can
     * only ever throw unchecked, and replicaWrite's own catch already
     * narrows to {@code NanocachedException}, so whatever reaches here is
     * a genuine programming bug. Every exception escaping this SDK's
     * public API must extend {@link NanocachedException} or be the raw
     * bug itself — never a {@link CompletionException} (issue: audit
     * finding, see {@link #write}). */
    private static RuntimeException unwrapReplicaBug(CompletionException wrapped) {
        Throwable cause = wrapped.getCause();
        return cause instanceof RuntimeException runtime ? runtime : wrapped;
    }

    /** Submits {@code replicaWrite} to {@link #replicaWriters}, falling
     * back to running it inline on this thread if the pool was
     * concurrently shut down by {@link #close()} — the synchronous-fallback
     * counterpart of the fire-and-forget path's own
     * {@code RejectedExecutionException} handling in {@link #write}, so a
     * write racing close() never throws a raw
     * {@link RejectedExecutionException} out to the caller (issue: audit
     * finding, background-write permit leak on close race). */
    private CompletableFuture<Void> submitReplicaWrite(Runnable replicaWrite) {
        try {
            return CompletableFuture.runAsync(replicaWrite, replicaWriters);
        } catch (RejectedExecutionException rejected) {
            replicaWrite.run();
            return CompletableFuture.completedFuture(null);
        }
    }

    // ── 遅延再接続 ────────────────────────────────────────────────

    private Connection singleConnection() {
        Connection current = single;
        if (!current.isClosed()) return current;

        synchronized (redialLocks.computeIfAbsent("", slot -> new Object())) {
            if (single.isClosed()) {
                single = dialWithCooldown(singleAddress);
            }
            return single;
        }
    }

    /** Redials {@code address}, honoring the per-address reconnect
     * cooldown (see {@link #reconnectCooldowns}): an address whose dial
     * just failed stays "down" for {@code reconnectCooldownNanos}, so a
     * burst of calls routed to it — or one call every keep-alive tick —
     * fails immediately with the same error the dial itself produced,
     * instead of each paying another full connect timeout in turn.
     * Callers must hold the redial lock for the relevant slot. */
    private Connection dialWithCooldown(String address) {
        CooldownEntry cooldown = reconnectCooldowns.get(address);
        if (cooldown != null && System.nanoTime() < cooldown.untilNanos) {
            throw cooldown.error;
        }

        try {
            Connection connection = openNodeConnectionOrThrow(address);
            reconnectCooldowns.remove(address);
            return connection;
        } catch (RuntimeException error) {
            armReconnectCooldown(address, error);
            throw error;
        }
    }

    /** Arms {@code address}'s reconnect cooldown with {@code error}: a
     * call routed to it during {@link #reconnectCooldownNanos} fails
     * immediately with this same error instead of paying another full
     * connect timeout. Shared by {@link #dialWithCooldown}'s own failed
     * redial and, for issue #67, {@link #openCluster} installing a member
     * whose bootstrap dial failed — the same mechanism either way, so a
     * node discovery still lists but that couldn't be reached is treated
     * identically whether it died before or after this client bootstrapped. */
    private void armReconnectCooldown(String address, RuntimeException error) {
        // disableReconnectCooldown() (Options): never record a cooldown
        // entry at all, so every dial to this address keeps paying its
        // own full connect attempt instead of ever hitting the
        // fast-rejection branch in dialWithCooldown.
        if (!reconnectCooldownDisabled) {
            reconnectCooldowns.put(
                    address, new CooldownEntry(System.nanoTime() + reconnectCooldownNanos, error));
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
        // member.connection is null for a member listed by discovery but
        // unreachable when this client bootstrapped (issue #67) — treated
        // exactly like a closed connection: fall through to redial it,
        // honoring the same reconnect cooldown a mid-life death would.
        if (member.connection != null && !member.connection.isClosed()) return member.connection;

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
            if (member.connection != null && !member.connection.isClosed()) return member.connection;
            Connection connection = dialWithCooldown(member.address);
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

    /** {@code null} on anything {@link Integer#parseInt} would reject —
     * every SDK exception must extend {@link NanocachedException}, so a
     * raw {@link NumberFormatException} must never escape a discovery
     * response's port field. Out-of-range ports are rejected here too:
     * {@code new InetSocketAddress(host, port)} in {@code Identify.open}
     * throws a raw {@link IllegalArgumentException} outside 0-65535,
     * which the get/set/delete reconnect path does not catch. Mirrors
     * .NET's {@code int.TryParse} handling at the same spot
     * (Identify.SplitHostPort). */
    private static Integer parsePort(String text) {
        try {
            int port = Integer.parseInt(text);
            return port < 0 || port > 65535 ? null : port;
        } catch (NumberFormatException malformed) {
            return null;
        }
    }

    private Connection openNodeConnection(String address) throws IOException {
        int separator = address.lastIndexOf(':');
        Integer port = separator == -1 ? null : parsePort(address.substring(separator + 1));
        if (separator == -1 || port == null) {
            throw new NanocachedException("nanocached: invalid node address from discovery server: " + address);
        }
        String host = address.substring(0, separator);

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
        return newTrackedConnection(node.socket(), node.tagged());
    }

    /** Wraps {@code socket} in a {@link Connection}, incrementing this
     * client's open-sockets count under {@link #targetKey} and arranging
     * for it to be decremented the moment that connection closes for any
     * reason — self-poisoning on a protocol error, a refresh dropping the
     * node, a lazy redial discarding it, or {@link #close()}.
     *
     * <p>{@link Connection}'s constructor calls {@code socket
     * .getInputStream()}/{@code getOutputStream()}, which can throw
     * {@link IOException} (e.g. the socket died between accept and here).
     * If that happens after {@link #trackOpenTarget} already ran, both the
     * open-target counter and the socket itself would otherwise leak —
     * nothing else ever closes a socket whose {@code Connection} never got
     * built. On that failure, undo the tracking and close the socket
     * (suppressing a close error — the constructor's is the interesting
     * one) before rethrowing. */
    private Connection newTrackedConnection(Socket socket, boolean tagged) throws IOException {
        trackOpenTarget(targetKey);
        try {
            return new Connection(socket, tagged, () -> untrackOpenTarget(targetKey));
        } catch (IOException | RuntimeException error) {
            untrackOpenTarget(targetKey);
            try {
                socket.close();
            } catch (IOException ignored) {
                // The original failure is the interesting one.
            }
            throw error;
        }
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

        // Dialing newly listed nodes happens *outside* stateLock
        // (mirroring .NET's RefreshNodeListAsync): every get/set/delete
        // routes through stateLock (ownerNames/memberConnection), so a
        // blocking connect held under it — up to CONNECT_TIMEOUT_MS per
        // new node — would stall all traffic for the whole dial phase.
        // Under the lock we only reconcile the member map and collect
        // which nodes still need a connection.
        List<DiscoveredNode> toOpen = new ArrayList<>();
        synchronized (stateLock) {
            Map<String, DiscoveredNode> byName = new LinkedHashMap<>();
            for (DiscoveredNode node : cluster.nodes()) byName.put(node.name(), node);

            members.entrySet().removeIf(entry -> {
                if (!byName.containsKey(entry.getKey())) {
                    // null for a member that was never reached (issue #67)
                    // — nothing to close in that case.
                    Connection stale = entry.getValue().connection;
                    if (stale != null) stale.close();
                    // Node names are per-process UUIDs; a departed node's
                    // redial gate would otherwise leak forever (issue #12).
                    redialLocks.remove(entry.getKey());
                    // Same leak, for the same reason, on the per-address
                    // cooldown map: a departed node's address is never
                    // reused, so its cooldown entry (if any) would
                    // otherwise linger forever (issue: audit finding,
                    // unpruned reconnectCooldowns).
                    reconnectCooldowns.remove(entry.getValue().address);
                    return true;
                }
                return false;
            });

            for (DiscoveredNode node : cluster.nodes()) {
                Member existing = members.get(node.name());
                if (existing != null) {
                    existing.address = node.address();
                } else {
                    toOpen.add(node);
                }
            }
        }

        for (DiscoveredNode node : toOpen) {
            try {
                Connection connection = openNodeConnection(node.address());
                synchronized (stateLock) {
                    if (closed) {
                        // close() ran while we were dialing (issue #10):
                        // installing this socket now would leak it.
                        connection.close();
                        return;
                    }
                    members.put(node.name(), new Member(node.address(), connection));
                }
            } catch (IOException | RuntimeException error) {
                // Left out of the ring for now; the next refresh
                // retries it. Silent by design: the stderr narration
                // this once had was removed by the #25/#27
                // API-unification work — not issue #12, which is only
                // the redial-gate pruning above — since a per-node
                // connect failure here changes no behavior and isn't
                // worth a warning on every refresh. Counted via
                // stats().refreshFailures instead.
                refreshFailures.incrementAndGet();
            }
        }

        synchronized (stateLock) {
            ring = new HashRing(new ArrayList<>(members.keySet()));
            replication = cluster.replication();
        }
    }

    /** Walks every address (discovery HA); {@code null} means keep the
     * last-known list. */
    private Identify.ClusterTarget fetchNodeList() {
        for (Address address : addresses) {
            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(address.host(), address.port(), authSecret, tls);
            } catch (IOException | RuntimeException error) {
                // Silent by design — refresh is opportunistic/best-effort
                // and must never fail the caller's operation, consistent
                // with client-side replication's eventual-consistency model. The next
                // refresh retries; counted via stats().refreshFailures.
                refreshFailures.incrementAndGet();
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
                for (Member member : members.values()) {
                    // null for a member never reached (issue #67) — stays
                    // lazy, exactly like a closed connection, until a
                    // foreground request redials it.
                    if (member.connection != null) connections.add(member.connection);
                }
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
