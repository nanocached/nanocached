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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import java.util.regex.Pattern;
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

    /** A value read alongside its CAS token (issue #141) — returned by
     * {@link #getWithToken}/{@link Namespace#getWithToken}. {@code value}
     * is the same decompressed bytes {@link #getBytes} would return;
     * {@code token} is {@link #contentDigest} of the raw wire bytes
     * (before decompression, when {@code compress} is enabled — the
     * server never decompresses either), ready to pass straight to
     * {@link #replace(byte[], String, byte[], long)}/{@link
     * #deleteIfMatches(byte[], String)}. */
    public record CasEntry(byte[] value, String token) {}

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
        private boolean viaProxy;

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

        /** SDK proxy mode (issue #122). Off by default. Only meaningful
         * when {@code addresses} names discovery server(s) — {@code
         * connect()} fails fast, naming the address, if the first one
         * reached turns out to be a cache node instead (proxy mode needs a
         * ring to fan a roster fetch out to, not a single node's identity).
         *
         * <p>Routes every request through exactly one {@code
         * nanocached-proxy} instead of joining the cluster ring: no
         * per-node connections, no ring view, and no hedged reads — a
         * proxy is the only owner on this one connection, so there is
         * nobody to hedge to, and a configured {@link #readHedgeAfter} is
         * simply inert here (never rejected — it may still apply to a
         * different client sharing these {@code Options}). Namespaces,
         * clear/clearAll, tags, keep-alive, and compression all work
         * unchanged; {@code close()} is unchanged too. From here on this
         * client behaves exactly like single-node mode, because a proxy
         * answers the identify handshake exactly like a cache node does
         * (full G/S/D, never {@code W} — it owns every key) and this SDK's
         * existing single-connection path already speaks to it correctly.
         *
         * <p>The proxy itself is chosen at random from the roster
         * discovery serves via {@code Q} — spreads a client fleet across
         * the whole proxy tier instead of piling every client onto
         * whichever proxy answers first — with random fail-over across the
         * rest of the roster if the chosen one is unreachable. On a later
         * disconnect the client first retries that same proxy (it may
         * simply have restarted) before re-fetching the roster and picking
         * another; both paths reuse the existing single-node reconnect
         * machinery ({@link #reconnectCooldown} included) rather than a
         * second one built for this mode. */
        public Options viaProxy(boolean enabled) {
            this.viaProxy = enabled;
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
     * @param transientRetries every {@code R} (transient-failure, issue
     * #125) response this client has received across every connection,
     * including the final one on any request that went on to exhaust its
     * bounded retry budget and raise {@link
     * NanocachedException.RetryableError}. Unlike the other counters
     * here, a transient retry is never silently swallowed on its own — a
     * request either transparently succeeds after one or more, or the
     * caller sees {@code RetryableError} — this counter exists purely so
     * an operator can see how often it's happening even when every
     * individual occurrence resolved fine.
     */
    public record ClientStats(long replicaWriteFailures, long readRepairFailures, long refreshFailures,
            long backgroundWriteBugs, long transientRetries) {}

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

    // Batched get/set (issue #151): bounds how many keys getMany/
    // getManyBytes/setMany/setManyBytes pack into a single `m`/`o`
    // sub-frame per owner before splitting into more than one (batch
    // chunking) — a reply header (Connection.MAX_HEADER_LINE_LENGTH, 4
    // KiB) must fit every key's/value's decimal length field plus
    // separators. 400 keys' worth of "9999 9999 " fields (worst case, a
    // 4-digit length pair) is ~4000 bytes, comfortably under that cap
    // with room for the count/tag fields — same value the Go and
    // TypeScript SDKs use.
    private static final int MAX_BATCH_KEYS = 400;

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

    // Cap on the throwaway dialer pool openCluster spins up to bootstrap-
    // dial every discovered node concurrently (issue #178). Node counts are
    // only bounded by Identify.MAX_NODE_COUNT (65536) — sizing the pool to
    // nodes.size() directly lets a large or malicious discovery reply make
    // the client try to spawn tens of thousands of native threads and die
    // with OutOfMemoryError: unable to create native thread. Dials are I/O-
    // bound (each one is a short connect-and-identify, individually bounded
    // by Identify.CONNECT_TIMEOUT_MS), so a small fixed pool still dials a
    // huge cluster promptly — it just does it in waves instead of all at
    // once.
    private static final int MAX_BOOTSTRAP_DIALER_THREADS = 16;

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
    /** SDK proxy mode (issue #122): {@code true} routes {@link
     * #singleConnection}'s reconnect through {@link #reconnectProxy}
     * (retry-same-then-re-fetch-and-pick-another) instead of {@link
     * #dialWithCooldown}'s plain single-address redial. See {@link
     * Options#viaProxy}. */
    private final boolean viaProxy;
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
    /** Issue #125: every {@code R} (transient-failure) response any
     * {@link Connection} this client owns has received — wired into each
     * one via {@link #newTrackedConnection}'s {@code onTransientRetry}
     * callback. See {@link ClientStats#transientRetries()}. */
    private final AtomicLong transientRetries = new AtomicLong();
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
            Duration readHedgeAfter, boolean viaProxy) {
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
        this.viaProxy = viaProxy;
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
                options.readHedgeAfter, options.viaProxy);

        // SDK proxy mode (issue #122): a wholly different connect flow —
        // fetch a proxy roster via `Q` rather than a node list via `L`,
        // then dial one proxy at random — so it gets its own top-level
        // branch rather than being threaded through every arm of the
        // node/cluster loop below.
        if (client.viaProxy) {
            return connectViaProxy(client);
        }

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

    /**
     * SDK proxy mode (issue #122) connect: walks {@code client.addresses}
     * exactly like {@link #connect}'s own loop does for the non-proxy
     * path — same per-address tolerance for a busy ({@code B}, discovery
     * HA startup grace) or unreachable seed, same {@code lastError}
     * fallback message — but fetches the proxy roster ({@code Q}) instead
     * of the node list ({@code L}), and stops at the first seed that
     * serves a non-empty one: unlike {@link #openCluster}, this never
     * goes back to a different discovery seed for a second opinion on the
     * roster once one seed has answered.
     *
     * <p>An address that identifies as a cache node is a hard,
     * non-tolerated error (unlike a busy/unreachable seed) — proxy mode
     * needs a ring to fetch a roster from, and pointing it at a node
     * instead is a configuration mistake no amount of address fail-over
     * fixes, so this fails fast naming the address rather than silently
     * trying the next one.
     */
    private static NanocachedClient connectViaProxy(NanocachedClient client) {
        RuntimeException lastError = null;
        for (Address address : client.addresses) {
            String key = address.host() + ":" + address.port();

            if (client.addresses.size() == 1 && OPEN_TARGETS.containsKey(key)) {
                System.err.println("nanocached: connect() called for " + key
                        + " while a previous connection to it is still open — was close() forgotten?");
            }

            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(
                        address.host(), address.port(), client.authSecret, client.tls, true);
            } catch (IOException | RuntimeException error) {
                lastError = error instanceof RuntimeException runtime
                        ? runtime
                        : new NanocachedException.ConnectionFailed(error.getMessage(), error);
                continue;
            }

            client.targetKey = key;

            if (identified instanceof Identify.NodeTarget node) {
                try {
                    node.socket().close();
                } catch (IOException ignored) {
                    // Best-effort cleanup — the config error below is what matters.
                }
                throw new NanocachedException(
                        "nanocached: viaProxy requires a discovery address, but " + key + " is a cache node");
            }

            List<DiscoveredNode> proxies = ((Identify.ProxyRosterTarget) identified).proxies();
            if (proxies.isEmpty()) {
                lastError = new NanocachedException(
                        "nanocached: no proxies registered with the discovery server at " + key);
                continue;
            }

            try {
                return connectToOneProxy(client, proxies);
            } catch (RuntimeException error) {
                client.teardown();
                throw error;
            }
        }

        throw lastError != null
                ? lastError
                : new NanocachedException("nanocached: could not connect to any address");
    }

    /**
     * Dials one proxy from {@code proxies}, in random order — spreads a
     * client fleet across the whole proxy tier instead of piling
     * everyone onto whichever entry happens to be first — returning as
     * soon as one accepts. A proxy identifies exactly like a cache node
     * on the wire (full G/S/D, never {@code W}: it owns every key), so
     * {@link #openNodeConnectionOrThrow} — the same dial this class uses
     * for any ordinary node — is exactly right here too; every entry
     * unreachable is this SDK's normal connect error.
     */
    private static NanocachedClient connectToOneProxy(NanocachedClient client, List<DiscoveredNode> proxies) {
        List<DiscoveredNode> shuffled = new ArrayList<>(proxies);
        java.util.Collections.shuffle(shuffled);

        RuntimeException lastError = null;
        for (DiscoveredNode proxy : shuffled) {
            try {
                client.single = client.openNodeConnectionOrThrow(proxy.address());
            } catch (RuntimeException error) {
                lastError = error;
                continue;
            }
            client.singleAddress = proxy.address();
            client.startKeepAlive();
            return client;
        }

        throw lastError != null
                ? lastError
                : new NanocachedException("nanocached: no proxies registered with discovery");
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
     * pool capped at {@link #MAX_BOOTSTRAP_DIALER_THREADS} —
     * {@link #replicaWriters} doesn't exist yet at this point, since
     * building it needs {@link #replication} from this same cluster
     * response, so bootstrap dialing gets its own throwaway executor
     * rather than reusing it. Every dial still honors the same per-dial
     * connect timeout as any other identify exchange (see
     * {@code Identify.CONNECT_TIMEOUT_MS}).
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
                Math.max(1, Math.min(nodes.size(), MAX_BOOTSTRAP_DIALER_THREADS)), runnable -> {
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

    // Compare-and-set (issue #141): "A"/"P" are the two bare, non-digest
    // <cond> tokens k understands — see cas()/casSet below. A digest is
    // always exactly 32 lowercase hex characters (CAS_TOKEN_PATTERN),
    // never confusable with either.
    private static final String CAS_ABSENT = "A";
    private static final String CAS_PRESENT = "P";
    private static final Pattern CAS_TOKEN_PATTERN = Pattern.compile("[0-9a-f]{32}");

    /** SHA-256 of {@code value}, truncated to its first 16 bytes (128
     * bits), lowercase hex-encoded (32 characters) — the CAS content
     * digest (issue #141), computed identically by the server and every
     * SDK; see docs/protocol.html#cas for the pinned cross-language test
     * vector. Public and static so code that already holds a value (a
     * future JCache adapter, issue #118, computing an expected token
     * without a prior GET) can compute one directly — see {@link
     * #replace(byte[], String, byte[], long)}'s doc for why that path is
     * only safe when the reconstruction is byte-identical to what the
     * server actually stores, unlike a token taken from {@link
     * #getWithToken}, which always is. */
    public static String contentDigest(byte[] value) {
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            // Every JDK ships SHA-256 (a JLS-mandated MessageDigest algorithm).
            throw new AssertionError(impossible);
        }
        byte[] digest = sha256.digest(value);
        StringBuilder hex = new StringBuilder(32);
        for (int i = 0; i < 16; i++) {
            hex.append(Character.forDigit((digest[i] >> 4) & 0xF, 16));
            hex.append(Character.forDigit(digest[i] & 0xF, 16));
        }
        return hex.toString();
    }

    /** Rejects a {@code token} that isn't a well-formed CAS digest (issue
     * #141) — exactly 32 lowercase hex characters, the only shape {@link
     * #contentDigest}/{@link #getWithToken} ever produce. Catches an
     * obviously wrong value (a stale/truncated copy-paste, or accidentally
     * passing something other than a digest) before it reaches the wire,
     * where — unlike a length-prefixed field — a malformed {@code <cond>}
     * has no way to be distinguished from a bare token like the digest's
     * own {@code "a"}-prefixed hex happening to collide with neither
     * {@code "A"} nor {@code "P"}. */
    private static void validateToken(String token) {
        if (token == null || !CAS_TOKEN_PATTERN.matcher(token).matches()) {
            throw new IllegalArgumentException(
                    "nanocached: token must be a 32-character lowercase hex digest "
                            + "(from contentDigest/getWithToken), got: " + token);
        }
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

        /** As {@link #getWithToken(byte[])}. */
        public Optional<CasEntry> getWithToken(String key) {
            return getWithToken(key.getBytes(StandardCharsets.UTF_8));
        }

        /** As {@link NanocachedClient#getWithToken(byte[])}, scoped to
         * {@link #namespace()} (issue #141). */
        public Optional<CasEntry> getWithToken(byte[] key) {
            return NanocachedClient.this.getWithToken(namespace, key);
        }

        /** As {@link NanocachedClient#getMany(List)}, scoped to
         * {@link #namespace()} (issue #151). */
        public Map<String, String> getMany(List<String> keys) {
            return NanocachedClient.this.getManyDecoded(namespace, keys);
        }

        /** As {@link NanocachedClient#getManyBytes(List)}, scoped to
         * {@link #namespace()} (issue #151). */
        public Map<String, byte[]> getManyBytes(List<String> keys) {
            return NanocachedClient.this.getManyBytes(namespace, keys);
        }

        /** As {@link NanocachedClient#getManyBytes(byte[][])}, scoped to
         * {@link #namespace()} (issue #160). */
        public byte[][] getManyBytes(byte[][] keys) {
            return NanocachedClient.this.getManyBytes(namespace, keys);
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

        public void setMany(Map<String, String> values) {
            setMany(values, 0L);
        }

        /** As {@link NanocachedClient#setMany(Map, long)}, scoped to
         * {@link #namespace()} (issue #151). */
        public void setMany(Map<String, String> values, long ttlSeconds) {
            NanocachedClient.this.setManyString(namespace, values, ttlSeconds);
        }

        public void setManyBytes(Map<String, byte[]> values) {
            setManyBytes(values, 0L);
        }

        /** As {@link NanocachedClient#setManyBytes(Map, long)}, scoped
         * to {@link #namespace()} (issue #151). */
        public void setManyBytes(Map<String, byte[]> values, long ttlSeconds) {
            NanocachedClient.this.setManyBytes(namespace, values, ttlSeconds);
        }

        public void setManyBytes(byte[][] keys, byte[][] values) {
            setManyBytes(keys, values, 0L);
        }

        /** As {@link NanocachedClient#setManyBytes(byte[][], byte[][], long)},
         * scoped to {@link #namespace()} (issue #160). */
        public void setManyBytes(byte[][] keys, byte[][] values, long ttlSeconds) {
            NanocachedClient.this.setManyBytes(namespace, keys, values, ttlSeconds);
        }

        public OptionalLong incr(String key, long delta) {
            return incr(key.getBytes(StandardCharsets.UTF_8), delta);
        }

        /** As {@link #incr(String, long)} with a delta of 1. */
        public OptionalLong incr(String key) {
            return incr(key, 1L);
        }

        /** As {@link NanocachedClient#incr(byte[], long)}, scoped to
         * {@link #namespace()} (issue #129). */
        public OptionalLong incr(byte[] key, long delta) {
            return NanocachedClient.this.incr(namespace, key, delta);
        }

        /** As {@link #incr(byte[], long)} with a delta of 1. */
        public OptionalLong incr(byte[] key) {
            return incr(key, 1L);
        }

        public OptionalLong decr(String key, long delta) {
            return decr(key.getBytes(StandardCharsets.UTF_8), delta);
        }

        /** As {@link #decr(String, long)} with an amount of 1. */
        public OptionalLong decr(String key) {
            return decr(key, 1L);
        }

        /** As {@link NanocachedClient#decr(byte[], long)}, scoped to
         * {@link #namespace()} — the same negated-delta {@code i}, never a
         * separate wire op. */
        public OptionalLong decr(byte[] key, long delta) {
            if (delta == Long.MIN_VALUE) {
                throw new IllegalArgumentException(
                        "nanocached: decr delta must not be Long.MIN_VALUE (has no positive negation)");
            }
            return incr(key, -delta);
        }

        /** As {@link #decr(byte[], long)} with an amount of 1. */
        public OptionalLong decr(byte[] key) {
            return decr(key, 1L);
        }

        // Compare-and-set (issue #141): see NanocachedClient's own
        // putIfAbsent/replaceIfPresent/replace/deleteIfMatches for the
        // full doc — these simply scope every call to namespace().

        public boolean putIfAbsent(String key, String value) {
            return putIfAbsent(key, value, 0L);
        }

        public boolean putIfAbsent(String key, String value, long ttlSeconds) {
            return putIfAbsent(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
                    ttlSeconds);
        }

        public boolean putIfAbsent(byte[] key, byte[] value) {
            return putIfAbsent(key, value, 0L);
        }

        /** As {@link NanocachedClient#putIfAbsent(byte[], byte[], long)},
         * scoped to {@link #namespace()}. */
        public boolean putIfAbsent(byte[] key, byte[] value, long ttlSeconds) {
            return NanocachedClient.this.putIfAbsent(namespace, key, value, ttlSeconds);
        }

        public boolean replaceIfPresent(String key, String value) {
            return replaceIfPresent(key, value, 0L);
        }

        public boolean replaceIfPresent(String key, String value, long ttlSeconds) {
            return replaceIfPresent(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
                    ttlSeconds);
        }

        public boolean replaceIfPresent(byte[] key, byte[] value) {
            return replaceIfPresent(key, value, 0L);
        }

        /** As {@link NanocachedClient#replaceIfPresent(byte[], byte[],
         * long)}, scoped to {@link #namespace()}. */
        public boolean replaceIfPresent(byte[] key, byte[] value, long ttlSeconds) {
            return NanocachedClient.this.replaceIfPresent(namespace, key, value, ttlSeconds);
        }

        public boolean replace(String key, String token, String newValue) {
            return replace(key, token, newValue, 0L);
        }

        public boolean replace(String key, String token, String newValue, long ttlSeconds) {
            return replace(key.getBytes(StandardCharsets.UTF_8), token, newValue.getBytes(StandardCharsets.UTF_8),
                    ttlSeconds);
        }

        public boolean replace(byte[] key, String token, byte[] newValue) {
            return replace(key, token, newValue, 0L);
        }

        /** As {@link NanocachedClient#replace(byte[], String, byte[],
         * long)}, scoped to {@link #namespace()}. */
        public boolean replace(byte[] key, String token, byte[] newValue, long ttlSeconds) {
            return NanocachedClient.this.replace(namespace, key, token, newValue, ttlSeconds);
        }

        public boolean deleteIfMatches(String key, String token) {
            return deleteIfMatches(key.getBytes(StandardCharsets.UTF_8), token);
        }

        /** As {@link NanocachedClient#deleteIfMatches(byte[], String)},
         * scoped to {@link #namespace()}. */
        public boolean deleteIfMatches(byte[] key, String token) {
            return NanocachedClient.this.deleteIfMatches(namespace, key, token);
        }

        /** Drops every entry in this namespace (CLEAR, issue #106) — see
         * {@link NanocachedClient#clearAll()} for the whole-store
         * counterpart. An empty {@link #namespace()} (this handle's own
         * {@code namespace("")}) clears the default namespace ({@code c
         * 0}), never rejected. */
        public void clear() {
            NanocachedClient.this.clear(namespace);
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
                backgroundWriteBugs.get(), transientRetries.get());
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
        byte[] raw = readRawBytes(namespace, key);
        if (raw == null) return Optional.empty();
        return Optional.of(compress ? Compression.decompressValue(raw) : raw);
    }

    /** As {@link #get(String)}, but returning a {@link CasEntry} carrying
     * a token ({@link #contentDigest}) alongside the value — the low-level
     * read half of compare-and-set (issue #141); see {@link
     * #putIfAbsent}/{@link #replaceIfPresent}/{@link #replace}/{@link
     * #deleteIfMatches}. */
    public Optional<CasEntry> getWithToken(String key) {
        return getWithToken(key.getBytes(StandardCharsets.UTF_8));
    }

    /** As {@link #getWithToken(String)}. <b>Correctness note</b>: the
     * token is computed from the exact raw wire bytes this connection
     * received — the same bytes a compression-enabled client's marker
     * byte is part of, since the server never decompresses — never from
     * the decompressed value this method also returns, so it always
     * matches what a subsequent {@code k}/{@code x} on the same key would
     * see server-side, compression or not. */
    public Optional<CasEntry> getWithToken(byte[] key) {
        return getWithToken(EMPTY_NAMESPACE, key);
    }

    /** The namespaced counterpart of {@link #getWithToken(byte[])} (issue
     * #141) — see {@link #namespace}. */
    Optional<CasEntry> getWithToken(byte[] namespace, byte[] key) {
        byte[] raw = readRawBytes(namespace, key);
        if (raw == null) return Optional.empty();
        byte[] value = compress ? Compression.decompressValue(raw) : raw;
        return Optional.of(new CasEntry(value, contentDigest(raw)));
    }

    /** Shared by {@link #getBytes(byte[], byte[])} and {@link
     * #getWithToken(byte[], byte[])}: the raw wire bytes for {@code key}
     * (including a compression-enabled client's marker byte — decompressed
     * by neither caller, which each do at the point that matches their own
     * contract), or {@code null} on a miss. Routing, {@code W}
     * refresh-and-retry, and read repair all happen here exactly once, so
     * the two callers can't drift on any of it. */
    private byte[] readRawBytes(byte[] namespace, byte[] key) {
        validateKey(namespace, key);
        beforeOperation();
        byte[] value = withWrongNodeRetry(
                () -> read(namespace, key, connection -> connection.get(namespace, key)));
        if (value == null && readRepair && ring != null) {
            value = tryReadRepair(namespace, key);
        }
        return value;
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

    // ── Batched get/set (issue #151) ─────────────────────────────────
    // m/o — see docs/protocol.html#multi. Every requested key's owner is
    // still resolved via HashRing/ownerNames, exactly like a single
    // getBytes/set: getManyBytes groups keys by primary owner and issues
    // one `m` sub-frame per owner (batch chunking splits an
    // over-MAX_BATCH_KEYS group further); setManyBytes groups by every
    // owner across every rank, since one batch's keys can place the same
    // node as primary for one key and a replica for another. A batch
    // never fails as a whole (docs/protocol.html#multi): getManyBytes
    // returns every key that resolved, throwing
    // NanocachedException.PartialWrongNode (carrying that partial map)
    // only if some keys are still wrong-node after one bounded
    // refresh-and-retry — the same policy getBytes' own withWrongNodeRetry
    // applies, generalized to a per-key roster instead of an
    // all-or-nothing retry. setManyBytes has nothing to return on
    // success, so it just throws a plain NanocachedException.WrongNode on
    // the same condition.

    /** As {@link #getManyBytes(List)}, decoding every hit as strict
     * UTF-8.
     * @throws UncheckedIOException if a stored value is not valid UTF-8 */
    public Map<String, String> getMany(List<String> keys) {
        return getManyDecoded(EMPTY_NAMESPACE, keys);
    }

    /** Shared by {@link #getMany(List)} and {@link Namespace#getMany}:
     * decodes {@link #getManyBytes(byte[], byte[][])}'s raw result, or — on a
     * partial failure — decodes the {@link
     * NanocachedException.PartialWrongNode}'s own partial map and
     * rethrows the UTF-8-decoded counterpart ({@link
     * NanocachedException.PartialWrongNodeStrings}) instead. */
    private Map<String, String> getManyDecoded(byte[] namespace, List<String> keys) {
        try {
            return decodeMany(getManyBytes(namespace, keys));
        } catch (NanocachedException.PartialWrongNode partial) {
            throw new NanocachedException.PartialWrongNodeStrings(decodeMany(partial.partialValues));
        }
    }

    private static Map<String, String> decodeMany(Map<String, byte[]> raw) {
        Map<String, String> values = new LinkedHashMap<>(raw.size());
        for (Map.Entry<String, byte[]> entry : raw.entrySet()) {
            values.put(entry.getKey(), decodeUtf8Strict(entry.getValue()));
        }
        return values;
    }

    /** Returns every requested key's raw value in one round trip per
     * owner (batched get, docs/protocol.html#multi) — a missing key is
     * simply absent from the returned map, never an error, the same "a
     * miss is not an error" contract {@link #getBytes(byte[])} itself
     * has. {@code keys} must be non-empty.
     *
     * <p>A batch never fails as a whole: if some keys are still
     * wrong-node after one bounded refresh-and-retry, throws {@link
     * NanocachedException.PartialWrongNode} whose {@code partialValues}
     * holds every key that DID resolve, rather than discarding a
     * mostly-successful batch over a handful of stale placements. In
     * single-node/proxy mode a {@code W} propagates immediately, exactly
     * as {@link #getBytes(byte[])}'s own single-mode behavior does —
     * there is no ring to refresh against.
     *
     * <p>Larger batches are transparently split into more than one
     * {@code m} sub-frame per owner (batch chunking, see {@link
     * #MAX_BATCH_KEYS}) — callers never need to think about this. */
    public Map<String, byte[]> getManyBytes(List<String> keys) {
        return getManyBytes(EMPTY_NAMESPACE, keys);
    }

    /** The namespaced counterpart of {@link #getManyBytes(List)} (issue
     * #151) — see {@link #namespace}. */
    Map<String, byte[]> getManyBytes(byte[] namespace, List<String> keys) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("nanocached: getMany/getManyBytes requires at least one key");
        }
        byte[][] keyBytes = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            keyBytes[i] = keys.get(i).getBytes(StandardCharsets.UTF_8);
        }
        try {
            return spliceMany(keys, getManyBytes(namespace, keyBytes));
        } catch (NanocachedException.PartialWrongNodeRaw partial) {
            throw new NanocachedException.PartialWrongNode(spliceMany(keys, partial.partialValues));
        }
    }

    /** Re-keys a positional {@link #getManyBytes(byte[], byte[][])}
     * result by its original {@code String} keys, dropping misses
     * ({@code null} slots) so "a miss is simply absent" holds for the
     * map-shaped API. */
    private static Map<String, byte[]> spliceMany(List<String> keys, byte[][] positional) {
        Map<String, byte[]> values = new LinkedHashMap<>(keys.size());
        for (int i = 0; i < positional.length; i++) {
            if (positional[i] != null) values.put(keys.get(i), positional[i]);
        }
        return values;
    }

    /** The {@code byte[]}-keyed counterpart of {@link
     * #getManyBytes(List)} (issue #160), for callers whose keys are not
     * UTF-8 text — the bulk analogue of {@link #getBytes(byte[])}. The
     * result is positional: {@code result[i]} is {@code keys[i]}'s raw
     * value, or {@code null} for a miss (a {@code byte[]} cannot key a
     * {@code Map} by content, so a {@code Map<byte[], byte[]>} would be
     * useless). {@code keys} must be non-empty.
     *
     * <p>Same batch semantics as the {@code String}-keyed form —
     * chunking, one bounded refresh-and-retry, single-mode {@code W}
     * propagation — except the partial-failure exception is the
     * positional {@link NanocachedException.PartialWrongNodeRaw}, whose
     * {@code partialValues} is this same positional array and whose
     * {@code unresolvedIndices} names the keys that are still
     * wrong-node (a {@code null} slot alone can't tell a miss from an
     * unresolved key). */
    public byte[][] getManyBytes(byte[][] keys) {
        return getManyBytes(EMPTY_NAMESPACE, keys);
    }

    /** The namespaced, positional core every {@code getMany} variant
     * is built on — see {@link #getManyBytes(byte[][])}. */
    byte[][] getManyBytes(byte[] namespace, byte[][] keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("nanocached: getMany/getManyBytes requires at least one key");
        }
        for (byte[] key : keys) {
            validateKey(namespace, key);
        }
        beforeOperation();

        byte[][] values = new byte[keys.length][];

        boolean single;
        synchronized (stateLock) {
            single = ring == null;
        }

        if (single) {
            List<Connection.MultiEntry> entries = multiGetChunked(this::singleConnection, namespace, keys);
            List<Integer> unresolved = new ArrayList<>();
            for (int i = 0; i < entries.size(); i++) {
                Connection.MultiEntry entry = entries.get(i);
                if (entry.ok()) {
                    values[i] = maybeDecompress(entry.value());
                } else if (entry.wrongNode()) {
                    unresolved.add(i);
                }
            }
            if (!unresolved.isEmpty()) throw new NanocachedException.PartialWrongNodeRaw(values, unresolved);
            return values;
        }

        List<Integer> retry = multiGetPass(namespace, keys, values, null);
        if (retry.isEmpty()) return values;
        maybeRefresh(true);
        retry = multiGetPass(namespace, keys, values, retry);
        if (!retry.isEmpty()) throw new NanocachedException.PartialWrongNodeRaw(values, retry);
        return values;
    }

    /** {@code compress}'s decompression step (see {@link
     * #getBytes(byte[], byte[])}), generalized so {@link
     * #getManyBytes(byte[], byte[][])}'s per-entry splicing can share it: a
     * no-op when {@code compress} is off. */
    private byte[] maybeDecompress(byte[] value) {
        return compress ? Compression.decompressValue(value) : value;
    }

    /** Issues one or more {@code m} sub-frames against whatever {@code
     * connectionFor} resolves to — already grouped to one owner (or the
     * single/proxy target) by the caller — splitting into {@link
     * #MAX_BATCH_KEYS}-sized chunks (batch chunking) so no reply header
     * risks exceeding {@link Connection#MAX_HEADER_LINE_LENGTH}. */
    private List<Connection.MultiEntry> multiGetChunked(
            java.util.function.Supplier<Connection> connectionFor, byte[] namespace, byte[][] keys) {
        List<Connection.MultiEntry> entries = new ArrayList<>(Collections.nCopies(keys.length, null));
        for (int start = 0; start < keys.length; start += MAX_BATCH_KEYS) {
            int end = Math.min(start + MAX_BATCH_KEYS, keys.length);
            byte[][] chunk = Arrays.copyOfRange(keys, start, end);
            List<Connection.MultiEntry> chunkEntries = applyReconnecting(
                    connectionFor, connection -> connection.multiGet(namespace, chunk));
            for (int i = start; i < end; i++) {
                entries.set(i, chunkEntries.get(i - start));
            }
        }
        return entries;
    }

    /** One pass of {@link #getManyBytes(byte[], byte[][])}'s cluster routing:
     * group the given indices (every key, when {@code retryIndices} is
     * {@code null} — the initial pass — or just the keys a previous pass
     * left unresolved) by their current primary owner (matching plain
     * {@code get}'s own primary-first stance), dispatch one (possibly
     * chunked) {@code m} exchange per owner concurrently, splice hits
     * into {@code values}, and return the indices still unresolved: a
     * per-key {@code W}, or a whole owner group whose call failed
     * outright. Called once for the initial pass and once more, if
     * needed, after a single forced refresh. */
    private List<Integer> multiGetPass(
            byte[] namespace, byte[][] keyBytes, byte[][] values, List<Integer> retryIndices) {
        List<Integer> indices = retryIndices;
        if (indices == null) {
            indices = new ArrayList<>(keyBytes.length);
            for (int i = 0; i < keyBytes.length; i++) indices.add(i);
        }

        Map<String, List<Integer>> groups = new LinkedHashMap<>();
        List<Integer> retry = new ArrayList<>();
        for (int idx : indices) {
            List<String> owners = ownerNames(namespace, keyBytes[idx]);
            if (owners.isEmpty()) {
                retry.add(idx);
                continue;
            }
            groups.computeIfAbsent(owners.get(0), name -> new ArrayList<>()).add(idx);
        }

        List<CompletableFuture<List<Integer>>> legs = new ArrayList<>(groups.size());
        for (Map.Entry<String, List<Integer>> group : groups.entrySet()) {
            String owner = group.getKey();
            List<Integer> groupIndices = group.getValue();
            legs.add(CompletableFuture.supplyAsync(
                    () -> runMultiGetLeg(namespace, owner, groupIndices, keyBytes, values), replicaWriters));
        }
        for (CompletableFuture<List<Integer>> leg : legs) {
            try {
                retry.addAll(leg.join());
            } catch (CompletionException wrapped) {
                throw unwrapReplicaBug(wrapped);
            }
        }
        return retry;
    }

    /** One owner group's {@code m} exchange, run on {@link
     * #replicaWriters} by {@link #multiGetPass}: a connection-level
     * failure retries the whole group (indistinguishable from a
     * possibly-idle-closed connection, same stance {@link
     * #applyReconnecting}'s own callers take elsewhere); a per-key {@code
     * W} retries just that key; a hit is spliced into {@code values}
     * (decompression failures propagate, aborting the batch immediately —
     * never fed into the retry pass, since they're a client-side {@code
     * compress} mismatch, not a routing outcome). */
    private List<Integer> runMultiGetLeg(
            byte[] namespace, String owner, List<Integer> groupIndices,
            byte[][] keyBytes, byte[][] values) {
        byte[][] groupKeys = new byte[groupIndices.size()][];
        for (int i = 0; i < groupIndices.size(); i++) {
            groupKeys[i] = keyBytes[groupIndices.get(i)];
        }

        List<Connection.MultiEntry> entries;
        try {
            entries = multiGetChunked(() -> memberConnection(owner), namespace, groupKeys);
        } catch (NanocachedException connectionFailure) {
            return new ArrayList<>(groupIndices);
        }

        List<Integer> retry = new ArrayList<>();
        for (int i = 0; i < groupIndices.size(); i++) {
            int idx = groupIndices.get(i);
            Connection.MultiEntry entry = entries.get(i);
            if (entry.wrongNode()) {
                retry.add(idx);
            } else if (entry.ok()) {
                values[idx] = maybeDecompress(entry.value());
            }
        }
        return retry;
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

    public void setMany(Map<String, String> values) {
        setMany(values, 0L);
    }

    /** As {@link #setManyBytes(Map, long)}, encoding every value as UTF-8. */
    public void setMany(Map<String, String> values, long ttlSeconds) {
        setManyString(EMPTY_NAMESPACE, values, ttlSeconds);
    }

    private void setManyString(byte[] namespace, Map<String, String> values, long ttlSeconds) {
        Map<String, byte[]> raw = new LinkedHashMap<>(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) {
            raw.put(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
        setManyBytes(namespace, raw, ttlSeconds);
    }

    public void setManyBytes(Map<String, byte[]> values) {
        setManyBytes(values, 0L);
    }

    /** Stores every raw value in {@code values} in one round trip per
     * involved node (batched set, docs/protocol.html#multi).
     * {@code ttlSeconds == 0} means no expiry, shared by the whole batch
     * — not per key, since every real caller of a batched set (Django's
     * {@code set_many}, cache-manager's {@code mset}) already passes one
     * TTL per call. {@code values} must be non-empty. Transparently
     * compresses values at or above {@code compressionThreshold} when
     * {@code compress} is enabled, exactly like {@link #set(byte[],
     * byte[], long)}.
     *
     * <p>Within one batch, the same node can be a key's primary and
     * another key's replica at once — it receives exactly one {@code o}
     * sub-frame either way, and only its answer for the keys it is
     * primary for decides that key's outcome; a replica-held key's
     * failure or {@code W} is logged-and-swallowed into {@link
     * #stats()}'s {@code replicaWriteFailures}, exactly like {@link
     * #set(byte[], byte[], long)}'s own replica legs ({@link #write}). A
     * batch never fails as a whole: if some keys' primaries are still
     * wrong-node after one bounded refresh-and-retry, this throws {@link
     * NanocachedException.WrongNode} — every other key in the batch was
     * still stored. In single-node/proxy mode a {@code W} propagates
     * immediately, exactly as {@link #set(byte[], byte[], long)}'s own
     * single-mode behavior does.
     *
     * <p>Larger batches are transparently split into more than one
     * {@code o} sub-frame per node (batch chunking, see {@link
     * #MAX_BATCH_KEYS}). */
    public void setManyBytes(Map<String, byte[]> values, long ttlSeconds) {
        setManyBytes(EMPTY_NAMESPACE, values, ttlSeconds);
    }

    /** The namespaced counterpart of {@link #setManyBytes(Map, long)}
     * (issue #151) — see {@link #namespace}. */
    void setManyBytes(byte[] namespace, Map<String, byte[]> values, long ttlSeconds) {
        byte[][] keyBytes = new byte[values.size()][];
        byte[][] valueBytes = new byte[values.size()][];
        int i = 0;
        for (Map.Entry<String, byte[]> entry : values.entrySet()) {
            keyBytes[i] = entry.getKey().getBytes(StandardCharsets.UTF_8);
            valueBytes[i] = entry.getValue();
            i++;
        }
        setManyBytes(namespace, keyBytes, valueBytes, ttlSeconds);
    }

    public void setManyBytes(byte[][] keys, byte[][] values) {
        setManyBytes(keys, values, 0L);
    }

    /** The {@code byte[]}-keyed counterpart of {@link
     * #setManyBytes(Map, long)} (issue #160): stores {@code values[i]}
     * under {@code keys[i]} for every position, with the same batch
     * semantics (shared {@code ttlSeconds}, chunking, one bounded
     * refresh-and-retry, plain {@link NanocachedException.WrongNode} if
     * some keys are still wrong-node afterwards). {@code keys} and
     * {@code values} must be non-empty and the same length. */
    public void setManyBytes(byte[][] keys, byte[][] values, long ttlSeconds) {
        setManyBytes(EMPTY_NAMESPACE, keys, values, ttlSeconds);
    }

    /** The namespaced, positional core every {@code setMany} variant is
     * built on — see {@link #setManyBytes(byte[][], byte[][], long)}. */
    void setManyBytes(byte[] namespace, byte[][] keys, byte[][] values, long ttlSeconds) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("nanocached: setMany/setManyBytes requires at least one key");
        }
        if (keys.length != values.length) {
            throw new IllegalArgumentException(
                    "nanocached: setManyBytes got " + keys.length + " keys but " + values.length + " values");
        }
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException(
                    "nanocached: ttlSeconds must be non-negative, got " + ttlSeconds);
        }
        byte[][] valueBytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            validateKeyAndValue(namespace, keys[i], values[i]);
            valueBytes[i] = compress ? Compression.compressValue(values[i], compressionThreshold) : values[i];
        }
        beforeOperation();

        Long wireTtlSeconds = ttlSeconds == 0 ? null : ttlSeconds;

        boolean single;
        synchronized (stateLock) {
            single = ring == null;
        }

        if (single) {
            List<Connection.MultiEntry> entries =
                    multiSetChunked(this::singleConnection, namespace, keys, valueBytes, wireTtlSeconds);
            for (Connection.MultiEntry entry : entries) {
                if (entry.wrongNode()) throw new NanocachedException.WrongNode();
            }
            return;
        }

        List<Integer> retry = multiSetPass(namespace, keys, valueBytes, wireTtlSeconds, null);
        if (retry.isEmpty()) return;
        maybeRefresh(true);
        retry = multiSetPass(namespace, keys, valueBytes, wireTtlSeconds, retry);
        if (!retry.isEmpty()) throw new NanocachedException.WrongNode();
    }

    /** {@link #multiGetChunked}'s write-side twin: one or more {@code o}
     * sub-frames against whatever {@code connectionFor} resolves to,
     * split into {@link #MAX_BATCH_KEYS}-sized chunks the same way. */
    private List<Connection.MultiEntry> multiSetChunked(
            java.util.function.Supplier<Connection> connectionFor, byte[] namespace,
            byte[][] keys, byte[][] values, Long ttlSeconds) {
        List<Connection.MultiEntry> entries = new ArrayList<>(Collections.nCopies(keys.length, null));
        for (int start = 0; start < keys.length; start += MAX_BATCH_KEYS) {
            int end = Math.min(start + MAX_BATCH_KEYS, keys.length);
            byte[][] keyChunk = Arrays.copyOfRange(keys, start, end);
            byte[][] valueChunk = Arrays.copyOfRange(values, start, end);
            List<Connection.MultiEntry> chunkEntries = applyReconnecting(
                    connectionFor, connection -> connection.multiSet(namespace, keyChunk, valueChunk, ttlSeconds));
            for (int i = start; i < end; i++) {
                entries.set(i, chunkEntries.get(i - start));
            }
        }
        return entries;
    }

    /** One owner's key/isPrimary membership across one {@link
     * #multiSetPass} call — see that method's own doc comment for why a
     * key can appear here with {@code isPrimary} false: the same node
     * can be primary for one key in the batch and a replica for
     * another. */
    private static final class OwnerBatch {
        final List<Integer> indices = new ArrayList<>();
        final List<Boolean> isPrimary = new ArrayList<>();
    }

    /** One pass of {@link #setManyBytes(byte[], byte[][], byte[][], long)}'s cluster
     * routing: for every key still needing resolution (every key, when
     * {@code retryIndices} is {@code null}, or just what a previous pass
     * left unresolved), build one sub-batch per <b>owner name across
     * every rank</b> — not just primaries, unlike {@link
     * #multiGetPass} — because within one batch the same node can be
     * primary for one key and a replica for another; each owner
     * therefore gets exactly one {@code o} sub-frame covering every key
     * it holds in any role. Only a leg's <em>primary</em> keys can end
     * up in the returned retry list; a leg's replica-held keys are
     * logged-and-swallowed into {@link #replicaWriteFailures} instead,
     * mirroring {@link #write}'s stance for single-key set. A leg that is
     * a pure replica for every key it holds is eligible for {@code
     * fireAndForgetReplicas}, exactly like a single-key replica write —
     * see {@link #runMultiSetLeg}. */
    private List<Integer> multiSetPass(
            byte[] namespace, byte[][] keyBytes, byte[][] valueBytes,
            Long ttlSeconds, List<Integer> retryIndices) {
        List<Integer> indices = retryIndices;
        if (indices == null) {
            indices = new ArrayList<>(keyBytes.length);
            for (int i = 0; i < keyBytes.length; i++) indices.add(i);
        }

        Map<String, OwnerBatch> owners = new LinkedHashMap<>();
        List<Integer> retry = Collections.synchronizedList(new ArrayList<>());
        for (int idx : indices) {
            List<String> names = ownerNames(namespace, keyBytes[idx]);
            if (names.isEmpty()) {
                retry.add(idx);
                continue;
            }
            for (int rank = 0; rank < names.size(); rank++) {
                OwnerBatch batch = owners.computeIfAbsent(names.get(rank), name -> new OwnerBatch());
                batch.indices.add(idx);
                batch.isPrimary.add(rank == 0);
            }
        }

        List<CompletableFuture<Void>> legs = new ArrayList<>();
        for (Map.Entry<String, OwnerBatch> ownerEntry : owners.entrySet()) {
            String name = ownerEntry.getKey();
            OwnerBatch batch = ownerEntry.getValue();
            Runnable leg = () -> runMultiSetLeg(namespace, name, batch, keyBytes, valueBytes, ttlSeconds, retry);

            boolean pureReplica = !batch.isPrimary.contains(Boolean.TRUE);
            // Fire-and-forget replica writes: with fireAndForgetReplicas, up to
            // maxInFlightBackgroundReplicaWrites legs run in the
            // background instead of being waited for below — mirrors
            // write()'s own fire-and-forget branch exactly, including its
            // close()-race fallbacks.
            if (fireAndForgetReplicas && pureReplica && backgroundReplicaWritePermits.tryAcquire()) {
                try {
                    CompletableFuture.runAsync(leg, replicaWriters)
                            .whenComplete((ignoredResult, error) -> {
                                backgroundReplicaWritePermits.release();
                                reportBackgroundWriteBug(error);
                            });
                } catch (RejectedExecutionException rejected) {
                    backgroundReplicaWritePermits.release();
                    leg.run();
                }
                continue;
            }

            legs.add(submitReplicaWrite(leg));
        }

        RuntimeException legBug = null;
        for (CompletableFuture<Void> pending : legs) {
            try {
                pending.join();
            } catch (CompletionException wrapped) {
                legBug = unwrapReplicaBug(wrapped);
            }
        }
        if (legBug != null) throw legBug;
        return retry;
    }

    /** Dispatches one owner's {@code o} sub-batch (via {@link
     * #multiSetChunked}) and applies its result to {@code retry}/{@link
     * #replicaWriteFailures}: only primary-held keys can end up appended
     * to {@code retry}; every replica-held key's failure or {@code W} is
     * counted in {@link #replicaWriteFailures} instead, mirroring {@link
     * #write}'s own stance for single-key set. A connection-level
     * failure for the whole leg is treated the same way, key by key,
     * since the SAME sub-frame can carry both primary- and replica-held
     * keys and a transport failure doesn't distinguish between them.
     * {@code retry} must already be a thread-safe list — this runs
     * concurrently with every other owner's leg. */
    private void runMultiSetLeg(
            byte[] namespace, String name, OwnerBatch batch, byte[][] keyBytes, byte[][] valueBytes,
            Long ttlSeconds, List<Integer> retry) {
        byte[][] groupKeys = new byte[batch.indices.size()][];
        byte[][] groupValues = new byte[batch.indices.size()][];
        for (int i = 0; i < batch.indices.size(); i++) {
            int idx = batch.indices.get(i);
            groupKeys[i] = keyBytes[idx];
            groupValues[i] = valueBytes[idx];
        }

        List<Connection.MultiEntry> entries;
        try {
            entries = multiSetChunked(() -> memberConnection(name), namespace, groupKeys, groupValues, ttlSeconds);
        } catch (NanocachedException connectionFailure) {
            for (int i = 0; i < batch.indices.size(); i++) {
                if (batch.isPrimary.get(i)) {
                    retry.add(batch.indices.get(i));
                } else {
                    replicaWriteFailures.incrementAndGet();
                }
            }
            return;
        }

        for (int i = 0; i < batch.indices.size(); i++) {
            boolean primary = batch.isPrimary.get(i);
            Connection.MultiEntry entry = entries.get(i);
            if (!primary) {
                if (entry.wrongNode()) replicaWriteFailures.incrementAndGet();
                continue;
            }
            if (entry.wrongNode()) retry.add(batch.indices.get(i));
        }
    }

    // INCR/DECR (issue #129): as volatile as set — LRU eviction and TTL
    // expiry reclaim an incremented value exactly like any other entry,
    // so this is for rate limiting/approximate counters, never durable
    // counts. Unlike a missing key on get (an empty Optional carrying no
    // further meaning), INCR's own "not found" is a real protocol answer
    // (`N`, distinct from "not numeric" (`T`)), so it gets the same
    // empty-on-miss convention getBytes' Optional already uses rather
    // than throwing — OptionalLong is that convention's long-typed
    // counterpart. `decr` is never a separate wire op — see {@link
    // #decr(byte[], long)}.

    public OptionalLong incr(String key, long delta) {
        return incr(key.getBytes(StandardCharsets.UTF_8), delta);
    }

    /** As {@link #incr(String, long)} with a delta of 1. */
    public OptionalLong incr(String key) {
        return incr(key, 1L);
    }

    public OptionalLong incr(byte[] key, long delta) {
        return incr(EMPTY_NAMESPACE, key, delta);
    }

    /** As {@link #incr(byte[], long)} with a delta of 1. */
    public OptionalLong incr(byte[] key) {
        return incr(key, 1L);
    }

    public OptionalLong decr(String key, long delta) {
        return decr(key.getBytes(StandardCharsets.UTF_8), delta);
    }

    /** As {@link #decr(String, long)} with an amount of 1. */
    public OptionalLong decr(String key) {
        return decr(key, 1L);
    }

    /** Sends the exact same {@code i} op as {@link #incr(byte[], long)}
     * with the delta negated — there is no separate decrement opcode on
     * the wire (see {@link Connection#incr}). {@code delta} is the
     * (non-negative in spirit, but unchecked beyond this) amount to
     * subtract; {@code Long.MIN_VALUE} is rejected since two's complement
     * has no positive value to negate it to. */
    public OptionalLong decr(byte[] key, long delta) {
        if (delta == Long.MIN_VALUE) {
            throw new IllegalArgumentException(
                    "nanocached: decr delta must not be Long.MIN_VALUE (has no positive negation)");
        }
        return incr(key, -delta);
    }

    /** As {@link #decr(byte[], long)} with an amount of 1. */
    public OptionalLong decr(byte[] key) {
        return decr(key, 1L);
    }

    /** The namespaced counterpart of every incr/decr overload above
     * (issue #105-style scoping applied to issue #129) — see {@link
     * #namespace}. {@code namespace} empty is exactly the un-namespaced
     * form: INCR always carries an explicit namespace length on the wire
     * (0 meaning default), so — unlike {@code get}/{@code set}/{@code
     * delete} — there is no separate legacy frame to fall back to; this
     * is simply the one and only frame shape. */
    OptionalLong incr(byte[] namespace, byte[] key, long delta) {
        validateKey(namespace, key);
        beforeOperation();
        Connection.IncrResult result = withWrongNodeRetry(() -> incrPrimaryThenReplicate(namespace, key, delta));
        return result == null ? OptionalLong.empty() : OptionalLong.of(result.value());
    }

    // ── Compare-and-set (issue #141) ─────────────────────────────────
    // k/x — see docs/protocol.html#cas. A condition mismatch is a normal
    // `false` return, never an exception, exactly like delete() returning
    // false for a miss; genuine errors (connection failure, exhausted
    // W-retries, etc.) still throw NanocachedException as usual.
    //
    // NOT A DISTRIBUTED LOCK: LRU eviction reclaims a CAS-written key
    // exactly as it would after a plain set, CAS or not — a key used as a
    // lock (putIfAbsent to acquire, a TTL to release) can be silently
    // double-acquired if it's evicted under memory pressure between one
    // caller's acquire and its release. k/x are atomic against concurrent
    // requests on the node that owns the key, the same guarantee INCR
    // makes and no stronger.

    public boolean putIfAbsent(String key, String value) {
        return putIfAbsent(key, value, 0L);
    }

    public boolean putIfAbsent(String key, String value, long ttlSeconds) {
        return putIfAbsent(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), ttlSeconds);
    }

    public boolean putIfAbsent(byte[] key, byte[] value) {
        return putIfAbsent(key, value, 0L);
    }

    /** Stores {@code value} at {@code key} only if the key is currently
     * absent — including lazily expired — the {@code A}-conditioned
     * {@code k} ({@code add}/{@code putIfAbsent}). Returns whether it was
     * stored: {@code false} means the key already existed and nothing
     * changed. {@code ttlSeconds == 0} means no expiry, same as {@link
     * #set(byte[], byte[], long)}. See this section's own doc comment for
     * why this is not a distributed lock. */
    public boolean putIfAbsent(byte[] key, byte[] value, long ttlSeconds) {
        return putIfAbsent(EMPTY_NAMESPACE, key, value, ttlSeconds);
    }

    /** The namespaced counterpart of {@link #putIfAbsent(byte[], byte[],
     * long)} (issue #105-style scoping applied to issue #141) — see
     * {@link #namespace}. */
    boolean putIfAbsent(byte[] namespace, byte[] key, byte[] value, long ttlSeconds) {
        return cas(namespace, key, value, ttlSeconds, CAS_ABSENT);
    }

    public boolean replaceIfPresent(String key, String value) {
        return replaceIfPresent(key, value, 0L);
    }

    public boolean replaceIfPresent(String key, String value, long ttlSeconds) {
        return replaceIfPresent(
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), ttlSeconds);
    }

    public boolean replaceIfPresent(byte[] key, byte[] value) {
        return replaceIfPresent(key, value, 0L);
    }

    /** Stores {@code value} at {@code key} only if the key currently holds
     * any (unexpired) value, whatever it is — the {@code P}-conditioned
     * {@code k}, the two-argument {@code replace(key, value)}. Returns
     * whether it was stored: {@code false} means the key was absent and
     * nothing changed. {@code ttlSeconds == 0} means no expiry. */
    public boolean replaceIfPresent(byte[] key, byte[] value, long ttlSeconds) {
        return replaceIfPresent(EMPTY_NAMESPACE, key, value, ttlSeconds);
    }

    /** The namespaced counterpart of {@link #replaceIfPresent(byte[],
     * byte[], long)} (issue #141) — see {@link #namespace}. */
    boolean replaceIfPresent(byte[] namespace, byte[] key, byte[] value, long ttlSeconds) {
        return cas(namespace, key, value, ttlSeconds, CAS_PRESENT);
    }

    public boolean replace(String key, String token, String newValue) {
        return replace(key, token, newValue, 0L);
    }

    public boolean replace(String key, String token, String newValue, long ttlSeconds) {
        return replace(key.getBytes(StandardCharsets.UTF_8), token, newValue.getBytes(StandardCharsets.UTF_8),
                ttlSeconds);
    }

    public boolean replace(byte[] key, String token, byte[] newValue) {
        return replace(key, token, newValue, 0L);
    }

    /** Stores {@code newValue} at {@code key} only if the key currently
     * holds an unexpired value whose {@link #contentDigest} equals {@code
     * token} exactly — the digest-conditioned {@code k}, the
     * three-argument {@code replace(key, old, new)}. Returns whether it
     * was stored: {@code false} means the stored value's digest didn't
     * match (including if the key was absent) and nothing changed.
     * {@code ttlSeconds == 0} means no expiry.
     *
     * <p>{@code token} is normally taken from a real prior read ({@link
     * #getWithToken}) — that path is always correct. A token instead
     * reconstructed by re-serializing/re-compressing a value the caller
     * already holds is <em>content</em>-based CAS's version of memcached's
     * own value-based CAS hazard: it's only correct if that reconstruction
     * produces byte-identical output to what the server actually stores —
     * true within one client sharing one serializer/compressor, not
     * guaranteed across languages with client-side compression enabled.
     *
     * @throws IllegalArgumentException if {@code token} isn't a
     * well-formed 32-character lowercase hex digest */
    public boolean replace(byte[] key, String token, byte[] newValue, long ttlSeconds) {
        return replace(EMPTY_NAMESPACE, key, token, newValue, ttlSeconds);
    }

    /** The namespaced counterpart of {@link #replace(byte[], String,
     * byte[], long)} (issue #141) — see {@link #namespace}. */
    boolean replace(byte[] namespace, byte[] key, String token, byte[] newValue, long ttlSeconds) {
        validateToken(token);
        return cas(namespace, key, newValue, ttlSeconds, token);
    }

    /** Shared validation/dispatch for {@link #putIfAbsent}/{@link
     * #replaceIfPresent}/{@link #replace} — {@code cond} is {@link
     * #CAS_ABSENT}, {@link #CAS_PRESENT}, or (already validated by {@link
     * #replace}) a digest. Mirrors {@link #set(byte[], byte[], byte[],
     * long)}'s own ttl/size validation and {@code withWrongNodeRetry}
     * wrapping. */
    private boolean cas(byte[] namespace, byte[] key, byte[] value, long ttlSeconds, String cond) {
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException(
                    "nanocached: ttlSeconds must be non-negative, got " + ttlSeconds);
        }
        validateKeyAndValue(namespace, key, value);
        beforeOperation();
        // Correctness (issue #141): a new value written via `k` must go
        // through the exact same compression pipeline set() already uses
        // — writing raw uncompressed bytes here would make a subsequent
        // plain get() from any compress-enabled client fail to
        // decompress it.
        byte[] outgoing = compress ? Compression.compressValue(value, compressionThreshold) : value;
        Long wireTtlSeconds = ttlSeconds == 0 ? null : ttlSeconds;
        return withWrongNodeRetry(() -> casPrimaryThenReplicate(namespace, key, outgoing, wireTtlSeconds, cond));
    }

    public boolean deleteIfMatches(String key, String token) {
        return deleteIfMatches(key.getBytes(StandardCharsets.UTF_8), token);
    }

    /** Removes {@code key} only if its current stored value's {@link
     * #contentDigest} equals {@code token} exactly — the two-argument
     * {@code remove(key, old)}. Returns whether it was removed: {@code
     * false} means a mismatch or a missing key, not an exception. See
     * {@link #replace(byte[], String, byte[], long)}'s doc for the same
     * token-reconstruction caveat.
     *
     * @throws IllegalArgumentException if {@code token} isn't a
     * well-formed 32-character lowercase hex digest */
    public boolean deleteIfMatches(byte[] key, String token) {
        return deleteIfMatches(EMPTY_NAMESPACE, key, token);
    }

    /** The namespaced counterpart of {@link #deleteIfMatches(byte[],
     * String)} (issue #141) — see {@link #namespace}. */
    boolean deleteIfMatches(byte[] namespace, byte[] key, String token) {
        validateToken(token);
        validateKey(namespace, key);
        beforeOperation();
        return withWrongNodeRetry(() -> casDeletePrimaryThenReplicate(namespace, key, token));
    }

    /**
     * CLEAR (issue #106): flushes every namespace, the default one
     * included, on every node in this client's current node list — a
     * single sub-map drop per node, not key-addressed (protocol.html: "c
     * / F — clear a namespace, flush everything"), so there is no owner
     * ranking to consult and no {@code W} to react to; the same {@code F}
     * frame goes to every node. Succeeds only once every node has acked
     * {@code C}: a node that failed (a dead connection, a bad/missing
     * ack, a timeout) gets one node-list refresh and one retry against
     * the refreshed list — the same refresh-and-retry path a
     * {@code W}/dead primary get/set/delete uses (see {@link
     * #withWrongNodeRetry}) — so this never silently reports success on a
     * partial clear: a node still failing after that retry fails this
     * call, naming it. The operation is idempotent, so a caller that sees
     * this throw can simply call it again.
     *
     * <p>Standalone (single-node) mode sends {@code F} to that one node.
     */
    public void clearAll() {
        beforeOperation();
        fanOutClear(null);
    }

    /** The namespaced counterpart of {@link #clearAll()} (issue #106) —
     * see {@link #namespace} and {@link Namespace#clear()}. An empty
     * {@code namespace} clears the default namespace ({@code c 0}) rather
     * than every namespace — never rejected. */
    void clear(byte[] namespace) {
        validateNamespace(namespace);
        beforeOperation();
        fanOutClear(namespace);
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

    /** As {@link #validateKey}, but for {@link #clear(byte[])} (issue
     * #106): no key is involved — the namespace alone is the entire body
     * of a {@code c} frame — so only it needs to stay clear of the same
     * MAX_REQUEST_BYTES headroom. {@link #clearAll()} needs no such
     * check: an {@code F} frame's body is always empty. */
    private static void validateNamespace(byte[] namespace) {
        if (namespace.length > MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException(
                    "nanocached: namespace (" + namespace.length + " bytes) exceeds the "
                            + MAX_REQUEST_BYTES
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

    // ── INCR/DECR (issue #129) ───────────────────────────────────────
    // Deliberately NOT write()'s same-op-to-every-owner fan-out: replaying
    // the increment on a replica could let it drift from the primary
    // (e.g. an earlier replica-leg write dropped after a transient
    // failure, or the replica separately evicted/reset the key), so the
    // `i` op goes to the primary owner only, and — only once that leg has
    // actually succeeded — its literal resulting value (and TTL) is
    // forwarded to the remaining owners as an ordinary `set`, keeping
    // every replica byte-identical to the primary rather than merely
    // aiming for the same arithmetic.

    /**
     * INCR's own write driver (issue #129). Sends {@code i} to the
     * primary owner only and awaits it; a miss ({@code null}, mirroring
     * {@link #getBytes(byte[], byte[])}'s own null-on-miss convention) or
     * {@link NanocachedException.NotNumeric} is returned/thrown directly
     * without touching any replica, since nothing was written. A hit's
     * result is fanned out to the remaining owners via {@link
     * #replicateIncrResult} before being returned. {@link
     * #withWrongNodeRetry} already wraps a call to this whole method (see
     * {@link #incr(byte[], byte[], long)}) and retries it on a {@code
     * W}/dead primary — since replication only ever runs after the
     * primary leg above has already returned successfully, that retry
     * only ever re-runs the primary leg again (a second, freshly routed
     * primary attempt), never a duplicate replica fan-out.
     */
    private Connection.IncrResult incrPrimaryThenReplicate(byte[] namespace, byte[] key, long delta) {
        if (ring == null) {
            return applyReconnecting(this::singleConnection, connection -> connection.incr(namespace, key, delta));
        }

        List<String> names = ownerNames(namespace, key);
        if (names.isEmpty()) {
            throw new NanocachedException("nanocached: no owner is reachable for this key");
        }

        Connection.IncrResult result = applyReconnecting(
                () -> memberConnection(names.get(0)), connection -> connection.incr(namespace, key, delta));
        if (result == null) return null;

        replicateIncrResult(namespace, key, names, result);
        return result;
    }

    /**
     * Fans {@code result} — the primary's own literal INCR outcome, never
     * replayed as another {@code i} — out to {@code names}' remaining
     * owners as a {@code set} carrying that exact value and TTL. Mirrors
     * {@link #write}'s own replica-leg scheduling ({@code
     * fireAndForgetReplicas} runs up to the configured cap in the
     * background; the rest run concurrently and are joined here), and the
     * same expected-failure handling (swallowed, counted via {@link
     * #replicaWriteFailures}). Unlike {@link #write}, there is no primary
     * failure for a genuine replica-leg bug to attach to — the primary
     * already succeeded by the time this runs — so any bug that escapes a
     * leg is simply logged via {@link #reportBackgroundWriteBug}, exactly
     * as a {@code fireAndForgetReplicas} leg's own bug already is.
     */
    private void replicateIncrResult(
            byte[] namespace, byte[] key, List<String> names, Connection.IncrResult result) {
        byte[] valueBytes = Long.toString(result.value()).getBytes(StandardCharsets.US_ASCII);
        replicateResultToReplicas(names, connection -> {
            connection.set(namespace, key, valueBytes, result.ttlSeconds());
            return null;
        });
    }

    /**
     * Fans {@code op} — an already-decided result, never a condition or
     * computation to redo — out to {@code names}' remaining owners (index
     * 1 onward). Shared by {@link #replicateIncrResult} (issue #129) and
     * compare-and-set's {@link #casPrimaryThenReplicate}/{@link
     * #casDeletePrimaryThenReplicate} (issue #141): all three run this
     * only after a primary op has already succeeded, so — unlike {@link
     * #write}, which fans out concurrently with a still-pending primary —
     * there is never a primary failure for a replica-leg bug to attach to
     * here; one that escapes {@code op}'s own {@link
     * NanocachedException}-only catch is simply logged via {@link
     * #reportBackgroundWriteBug}, exactly as a {@code
     * fireAndForgetReplicas} leg's own bug already is. Mirrors {@link
     * #write}'s replica-leg scheduling otherwise: {@code
     * fireAndForgetReplicas} runs up to the configured cap in the
     * background; the rest run concurrently and are joined here, with
     * every expected failure swallowed and counted via {@link
     * #replicaWriteFailures}.
     */
    private void replicateResultToReplicas(List<String> names, ConnectionOp<Void> op) {
        List<CompletableFuture<Void>> replicaWrites = new ArrayList<>();
        for (int i = 1; i < names.size(); i++) {
            String replica = names.get(i);
            Runnable replicaWrite = () -> {
                try {
                    applyReconnecting(() -> memberConnection(replica), op);
                } catch (NanocachedException ignored) {
                    // Swallowed by design, exactly like write()'s own
                    // replica leg — see that method's matching catch for
                    // the full rationale (issue: audit finding covers
                    // both identically).
                    replicaWriteFailures.incrementAndGet();
                }
            };

            if (fireAndForgetReplicas && backgroundReplicaWritePermits.tryAcquire()) {
                try {
                    CompletableFuture.runAsync(replicaWrite, replicaWriters)
                            .whenComplete((ignoredResult, error) -> {
                                backgroundReplicaWritePermits.release();
                                reportBackgroundWriteBug(error);
                            });
                } catch (RejectedExecutionException rejected) {
                    // close() shut replicaWriters down concurrently — see
                    // write()'s identical handling.
                    backgroundReplicaWritePermits.release();
                    replicaWrite.run();
                }
                continue;
            }

            replicaWrites.add(submitReplicaWrite(replicaWrite));
        }

        for (CompletableFuture<Void> pending : replicaWrites) {
            try {
                pending.join();
            } catch (CompletionException wrapped) {
                reportBackgroundWriteBug(unwrapReplicaBug(wrapped));
            }
        }
    }

    // ── Compare-and-set (issue #141) ──────────────────────────────────
    // Same driver shape as INCR immediately above: only the key's primary
    // owner ever evaluates <cond>, and only once that leg has actually
    // succeeded is its result forwarded to the remaining owners — as an
    // ordinary set/delete, via replicateResultToReplicas — never by
    // replaying k/x itself. A replica evaluating the same condition
    // against its own possibly-different copy could reach a different
    // outcome than the primary just did.

    /** putIfAbsent/replaceIfPresent/replace's write driver (issue #141).
     * Sends {@code k} to the primary owner only; a condition mismatch
     * ({@code false}) is returned directly without touching any replica,
     * since nothing was written. A success is fanned out to the remaining
     * owners via {@link #replicateResultToReplicas}, carrying the exact
     * same {@code value}/{@code ttlSeconds} just written to the primary,
     * before returning {@code true}. {@link #withWrongNodeRetry} already
     * wraps a call to this whole method (see {@link #cas}) and retries it
     * on a {@code W}/dead primary — since replication only ever runs
     * after the primary leg above has already returned successfully, that
     * retry only ever re-runs the primary leg again, never a duplicate
     * replica fan-out. */
    private boolean casPrimaryThenReplicate(
            byte[] namespace, byte[] key, byte[] value, Long ttlSeconds, String cond) {
        if (ring == null) {
            return applyReconnecting(
                    this::singleConnection, connection -> connection.casSet(namespace, key, value, ttlSeconds, cond));
        }

        List<String> names = ownerNames(namespace, key);
        if (names.isEmpty()) {
            throw new NanocachedException("nanocached: no owner is reachable for this key");
        }

        boolean stored = applyReconnecting(() -> memberConnection(names.get(0)),
                connection -> connection.casSet(namespace, key, value, ttlSeconds, cond));
        if (!stored) return false;

        replicateResultToReplicas(names, connection -> {
            connection.set(namespace, key, value, ttlSeconds);
            return null;
        });
        return true;
    }

    /** deleteIfMatches's write driver (issue #141) — as {@link
     * #casPrimaryThenReplicate}, but for {@code x}/{@code remove(key,
     * old)}: a success's replica leg is an ordinary (unconditional)
     * delete, since the primary's own successful digest match already
     * proved the key existed. */
    private boolean casDeletePrimaryThenReplicate(byte[] namespace, byte[] key, String cond) {
        if (ring == null) {
            return applyReconnecting(this::singleConnection, connection -> connection.casDelete(namespace, key, cond));
        }

        List<String> names = ownerNames(namespace, key);
        if (names.isEmpty()) {
            throw new NanocachedException("nanocached: no owner is reachable for this key");
        }

        boolean deleted = applyReconnecting(
                () -> memberConnection(names.get(0)), connection -> connection.casDelete(namespace, key, cond));
        if (!deleted) return false;

        replicateResultToReplicas(names, connection -> {
            connection.delete(namespace, key);
            return null;
        });
        return true;
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

    // ── CLEAR namespace / flush everything (issue #106) ─────────────

    /** Fans a clear out to every node — {@code namespace == null} means
     * {@code F} ({@link #clearAll()}), otherwise {@code c <namespace>}
     * ({@link #clear(byte[])}) — shared by both. Single mode sends
     * straight to the one connection, exactly like {@link #write}'s own
     * {@code ring == null} branch (no discovery to refresh from, so a
     * failure here is only ever {@link #applyReconnecting}'s one redial).
     * Cluster mode sends to every node concurrently (client-side
     * replication's own {@link #replicaWriters} pool — a clear is exactly
     * the kind of per-node fan-out that pool already exists for), then —
     * unlike a replica leg's failure, silently tolerated there — retries
     * once against a freshly refreshed node list on any failure, exactly
     * as {@link #withWrongNodeRetry} does for a {@code W}/dead-primary
     * get/set/delete; a node still failing after that retry fails this
     * call, naming it, so a clear can never silently report success while
     * one node's data survives. */
    private void fanOutClear(byte[] namespace) {
        if (ring == null) {
            applyReconnecting(this::singleConnection, connection -> {
                sendClear(connection, namespace);
                return null;
            });
            return;
        }

        Set<String> failed = clearFanOutOnce(allMemberNames(), namespace);
        if (failed.isEmpty()) return;

        maybeRefresh(true);
        failed = clearFanOutOnce(allMemberNames(), namespace);
        if (!failed.isEmpty()) {
            throw new NanocachedException(
                    "nanocached: clear failed on node(s): " + String.join(", ", failed));
        }
    }

    private static void sendClear(Connection connection, byte[] namespace) {
        if (namespace == null) connection.clearAll();
        else connection.clear(namespace);
    }

    /** Every member name in this client's current node list, snapshotted
     * under {@link #stateLock} (mirrors {@link #refreshNodeList}'s own
     * {@code new ArrayList<>(members.keySet())}) — {@link #fanOutClear}'s
     * fan-out target. Distinct from {@link #ownerNames}, which ranks only
     * a key's few owners by HRW: a clear touches every node regardless of
     * what it currently owns. */
    private List<String> allMemberNames() {
        synchronized (stateLock) {
            return new ArrayList<>(members.keySet());
        }
    }

    /** Sends one clear pass concurrently to every name in {@code names},
     * on {@link #replicaWriters}, returning the names that failed rather
     * than throwing — {@link #fanOutClear} decides whether that's grounds
     * for a refresh-and-retry or a final error. A connection-level
     * failure counts as a failure here; a genuine programming bug is not
     * caught and propagates once every leg has been joined, mirroring
     * {@link #write}'s replicaBug handling via {@link
     * #unwrapReplicaBug}. */
    private Set<String> clearFanOutOnce(List<String> names, byte[] namespace) {
        Set<String> failed = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<Void>> futures = new ArrayList<>(names.size());
        for (String name : names) {
            futures.add(submitReplicaWrite(() -> {
                try {
                    applyReconnecting(() -> memberConnection(name), connection -> {
                        sendClear(connection, namespace);
                        return null;
                    });
                } catch (NanocachedException ignored) {
                    failed.add(name);
                }
            }));
        }

        RuntimeException bug = null;
        for (CompletableFuture<Void> future : futures) {
            try {
                future.join();
            } catch (CompletionException wrapped) {
                bug = unwrapReplicaBug(wrapped);
            }
        }
        if (bug != null) throw bug;
        return failed;
    }

    // ── 遅延再接続 ────────────────────────────────────────────────

    private Connection singleConnection() {
        Connection current = single;
        if (!current.isClosed()) return current;

        synchronized (redialLocks.computeIfAbsent("", slot -> new Object())) {
            if (single.isClosed()) {
                // SDK proxy mode (issue #122): a dead proxy connection gets
                // its own reconnect strategy (retry the same proxy, then
                // re-fetch the roster) instead of the plain single-address
                // redial every other single-mode target uses.
                single = viaProxy ? reconnectProxy() : dialWithCooldown(singleAddress);
            }
            return single;
        }
    }

    /**
     * SDK proxy mode reconnect (issue #122). Called with the single-mode
     * redial lock already held, exactly like {@link #dialWithCooldown}'s
     * other callers. First retries {@link #singleAddress} — the same
     * proxy, which may simply have restarted — through {@link
     * #dialWithCooldown} itself, so a proxy that just failed stays
     * "down" for the usual cooldown window instead of being redialed on
     * every call. Only once that fails does this re-fetch the roster
     * ({@code Q}) from discovery and, in random order, dial the rest of
     * it through {@link #dialWithCooldown} too — reusing the exact same
     * per-address cooldown bookkeeping {@link #connectToOneProxy}'s
     * initial pick and every ordinary node redial already use, rather
     * than a second reconnect machine built for this mode.
     */
    private Connection reconnectProxy() {
        try {
            return dialWithCooldown(singleAddress);
        } catch (RuntimeException sameProxyFailed) {
            List<DiscoveredNode> proxies = fetchProxyRoster();
            if (proxies == null || proxies.isEmpty()) {
                throw sameProxyFailed;
            }

            List<DiscoveredNode> shuffled = new ArrayList<>(proxies);
            java.util.Collections.shuffle(shuffled);
            RuntimeException lastError = sameProxyFailed;
            for (DiscoveredNode proxy : shuffled) {
                try {
                    Connection connection = dialWithCooldown(proxy.address());
                    singleAddress = proxy.address();
                    return connection;
                } catch (RuntimeException error) {
                    lastError = error;
                }
            }
            throw lastError;
        }
    }

    /**
     * SDK proxy mode (issue #122): re-fetches the proxy roster by walking
     * {@link #addresses} exactly like {@link #fetchNodeList} walks them
     * for a cluster-mode node-list refresh — same per-address tolerance
     * for a busy/unreachable discovery seed, counted the same way via
     * {@link #refreshFailures} (this is a reconnect, so — like that
     * method — it must never throw past its caller). {@code null} means
     * every address failed outright; a caller must still treat a
     * successfully-fetched but empty roster ({@code proxies.isEmpty()})
     * as "nothing to fail over to" itself, since that's a legitimate —
     * if inconvenient — answer, not a fetch failure.
     */
    private List<DiscoveredNode> fetchProxyRoster() {
        for (Address address : addresses) {
            Identify.Result identified;
            try {
                identified = Identify.connectAndIdentify(address.host(), address.port(), authSecret, tls, true);
            } catch (IOException | RuntimeException error) {
                refreshFailures.incrementAndGet();
                continue;
            }
            if (identified instanceof Identify.NodeTarget node) {
                // The same misconfiguration connect() rejects outright —
                // but a reconnect must never throw past its caller, so
                // just close the socket and treat this seed as unusable.
                try {
                    node.socket().close();
                } catch (IOException ignored) {
                    // Best-effort cleanup.
                }
                refreshFailures.incrementAndGet();
                continue;
            }
            List<DiscoveredNode> proxies = ((Identify.ProxyRosterTarget) identified).proxies();
            if (!proxies.isEmpty()) return proxies;
            // An empty roster from this seed: try the next one, mirroring
            // fetchNodeList's identical treatment of an empty node list.
        }
        return null;
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
            return new Connection(
                    socket, tagged, () -> untrackOpenTarget(targetKey), transientRetries::incrementAndGet);
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
