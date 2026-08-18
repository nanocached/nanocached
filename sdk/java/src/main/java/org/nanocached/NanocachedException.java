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
     * A node answered {@code W} (ADR-0008): per its own view of cluster
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
     * A discovery server answered {@code L} with {@code B} — it is inside
     * its startup grace (ADR-0010), re-learning membership after a
     * restart. Try another address, or retry shortly.
     */
    public static final class DiscoveryBusy extends NanocachedException {
        public DiscoveryBusy() {
            super("nanocached: the discovery server is warming up after a restart");
        }
    }

    /** A connection-level failure; the client redials lazily on the next use. */
    public static final class ConnectionFailed extends NanocachedException {
        public ConnectionFailed(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
