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
    public static final class WrongNode extends NanocachedException {
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
        public ConnectionFailed(String message, Throwable cause) {
            super(message, cause);
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
}
