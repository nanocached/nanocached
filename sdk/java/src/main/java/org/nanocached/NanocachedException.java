package org.nanocached;

/** Base class for every error this SDK raises on its own behalf. */
public class NanocachedException extends RuntimeException {
    public NanocachedException(String message) {
        super(message);
    }

    public NanocachedException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Raised by get/set/delete after close(); close() itself is idempotent. */
    public static final class AlreadyClosed extends NanocachedException {
        public AlreadyClosed() {
            super("nanocached: this client is closed");
        }
    }

    /**
     * A node answered {@code W} (staged node join): per its own view of cluster
     * membership it doesn't hold this key — the caller's routing table is
     * stale. The client catches this internally to refresh the node list
     * and retry once; it only escapes when that retry also fails, or in
     * single-node mode where there is no discovery to refresh from.
     */
    public static class WrongNode extends NanocachedException {
        public WrongNode() {
            super("nanocached: this node does not hold the requested key");
        }
    }

    /**
     * The server rejected the {@code A} handshake's secret — either no
     * {@code authSecret} was configured for a server that requires one,
     * or the configured secret is wrong. Never transient: retrying with
     * the same configuration cannot succeed.
     */
    public static final class AuthenticationFailed extends NanocachedException {
        public AuthenticationFailed(String message) {
            super(message);
        }
    }

    /**
     * A discovery server answered {@code L} with {@code B} — it is inside
     * its startup grace (discovery HA), re-learning membership after a
     * restart. Try another address, or retry shortly.
     */
    public static final class DiscoveryBusy extends NanocachedException {
        public DiscoveryBusy() {
            super("nanocached: the discovery server is busy: warming up after a restart, or its replication factor disagrees with the cluster's");
        }
    }

    /** A connection-level failure; the client redials lazily on the next use. */
    public static final class ConnectionFailed extends NanocachedException {
        /**
         * Issue #225: {@code true} only when this connection is known to
         * have never sent the failed request's bytes at all — {@link
         * Connection#request}'s pre-write {@code isClosed()} checks are
         * the only place this is set, and it means the connection was
         * already marked closed before this call ever attempted to write,
         * almost always because the reader thread had already noticed the
         * peer's FIN (e.g. its idle timeout) moments earlier. Only then is
         * redialing and resending a <em>non-idempotent</em> request
         * (INCR/CAS/delete-if-matches) safe — see {@link
         * NanocachedClient}'s {@code applyReconnectingNonIdempotent}.
         * Every other {@code ConnectionFailed} (the write itself failing
         * partway, a request timeout, or the reply simply never arriving
         * after a successful write) leaves the request's fate genuinely
         * unknown, so it must not be replayed. Irrelevant to get/set/
         * delete/clear, which stay safe to retry unconditionally because
         * they're idempotent regardless of this flag — always {@code
         * false} on the public two-argument constructor those (and every
         * caller outside {@link Connection}) use.
         */
        private final boolean notSent;

        public ConnectionFailed(String message, Throwable cause) {
            this(message, cause, false);
        }

        ConnectionFailed(String message, Throwable cause, boolean notSent) {
            super(message, cause);
            this.notSent = notSent;
        }

        /** See {@link #notSent}. */
        public boolean notSent() {
            return notSent;
        }
    }

    /**
     * Raised by get/getBytes when a value with {@code compress} enabled
     * can't be interpreted — almost always a {@code compress} mismatch
     * between clients sharing this key (value compression's compatibility
     * caveat: every client touching a given keyspace must agree on
     * {@code compress}), not a transient failure.
     */
    public static final class DecompressionFailed extends NanocachedException {
        public DecompressionFailed(String message) {
            super(message);
        }

        public DecompressionFailed(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A request answered {@code R} (issue #125's retryable-error status)
     * three times running — bounded transparent retry on the SAME
     * connection (up to 2 retries, 3 attempts total, 50ms then 100ms
     * between them) exhausted without a real answer. Today only
     * {@code nanocached-proxy} sends {@code R}, when its upstream node was
     * briefly unreachable and survived its own one refresh-and-retry; this
     * is the signal that even that didn't resolve in time.
     *
     * <p>Unlike {@link ConnectionFailed} or {@link WrongNode}, this never
     * closes or redials the connection — it stays open and usable for the
     * next call. Every {@code R} answer, including the three that led
     * here, is counted in {@link NanocachedClient.ClientStats#transientRetries()}.
     */
    public static final class RetryableError extends NanocachedException {
        public RetryableError() {
            super("nanocached: request failed transiently (R) after 3 attempts on this connection");
        }
    }

    /**
     * A node answered {@code T} to an {@code i} (INCR/DECR, issue #129)
     * request: the key exists but its stored value isn't INCR's counter
     * grammar (plain decimal ASCII), or applying the delta would overflow
     * a 64-bit counter. Never transient — retrying the same request
     * answers the same way until the key is overwritten with a numeric
     * value (or deleted).
     */
    public static final class NotNumeric extends NanocachedException {
        public NotNumeric() {
            super("nanocached: the stored value is not an integer INCR can operate on");
        }
    }

    /**
     * Raised by {@link NanocachedClient#getManyBytes} (issue #151) when
     * some keys are still wrong-node after the one bounded
     * refresh-and-retry every batch gets (the per-key analogue of {@code
     * getBytes}' own {@code W} refresh-and-retry) — a subclass of {@link
     * WrongNode}, so existing {@code catch (WrongNode)} handling keeps
     * working unchanged. {@code partialValues} holds every key that DID
     * resolve — a batch never fails as a whole
     * (docs/protocol.html#multi), so a handful of stale placements
     * shouldn't force discarding an otherwise successful batch. {@link
     * NanocachedClient#setManyBytes} has nothing to return on success, so
     * it just throws a plain {@link WrongNode} on the same condition —
     * there's no partial payload worth attaching.
     *
     * <p>A separate, non-generic class from {@link PartialWrongNodeStrings}
     * rather than one class parameterized over the value type: the JLS
     * forbids a generic class from being a (direct or indirect) subclass
     * of {@link Throwable}.
     */
    public static final class PartialWrongNode extends WrongNode {
        public final java.util.Map<String, byte[]> partialValues;

        public PartialWrongNode(java.util.Map<String, byte[]> partialValues) {
            this.partialValues = partialValues;
        }
    }

    /**
     * As {@link PartialWrongNode}, but raised by {@link
     * NanocachedClient#getMany} — the UTF-8-decoded counterpart, thrown
     * once the raw {@link PartialWrongNode} its {@code getManyBytes} call
     * underneath threw has had its {@code partialValues} decoded the same
     * way a successful {@code getMany} decodes {@code getManyBytes}' own
     * result.
     */
    public static final class PartialWrongNodeStrings extends WrongNode {
        public final java.util.Map<String, String> partialValues;

        public PartialWrongNodeStrings(java.util.Map<String, String> partialValues) {
            this.partialValues = partialValues;
        }
    }

    /**
     * As {@link PartialWrongNode}, but raised by the positional,
     * {@code byte[]}-keyed {@link NanocachedClient#getManyBytes(byte[][])}
     * (issue #160). {@code partialValues} is the same positional array a
     * successful call would have returned ({@code null} for a miss);
     * since a {@code null} slot alone cannot tell a miss from a key that
     * is still wrong-node, {@code unresolvedIndices} lists (ascending)
     * the positions that did NOT resolve.
     */
    public static final class PartialWrongNodeRaw extends WrongNode {
        public final byte[][] partialValues;
        public final int[] unresolvedIndices;

        public PartialWrongNodeRaw(byte[][] partialValues, java.util.List<Integer> unresolvedIndices) {
            this.partialValues = partialValues;
            this.unresolvedIndices = unresolvedIndices.stream().mapToInt(Integer::intValue).sorted().toArray();
        }
    }
}
