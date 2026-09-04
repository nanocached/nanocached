package org.nanocached;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * One already-identified connection to a single nanocached-node, speaking
 * the cache protocol ({@code G}/{@code S}/{@code D} — the {@code A}
 * identify exchange happens in {@link Identify} before a Connection
 * exists). Requests are pipelined onto the socket and matched to
 * responses in send order (request pipelining): a dedicated reader thread
 * consumes responses and dispatches each to the oldest still-pending
 * request, since nanocached-node itself only ever answers in the order it
 * received requests. Enqueuing the pending slot and writing the frame
 * happen under one monitor, so concurrent callers' queue order always
 * matches the order their frames actually hit the wire.
 */
final class Connection {
    /** Bounds how long the connection may go without progress while
     * requests are outstanding (issue #42) — each response must arrive
     * within this window of the previous one (or of its own send, when
     * the queue was empty): without it, a half-open server that accepts
     * the TCP connection but never writes back — or stops mid-stream —
     * would hang get/set/delete forever in {@code future.join()}.
     * Generous versus the server's own 10s outbound timeouts, and the
     * same 30s the Go and Rust SDKs use. Non-final only so tests can
     * shorten it, mirroring {@code keepAliveIntervalMillis}. */
    static volatile long requestTimeoutMillis = 30_000;

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final Runnable onClose;
    /** Fired once for every {@code R} (transient-failure, issue #125)
     * response this connection sees, win or lose — {@link
     * NanocachedClient} wires its own {@code transientRetries} counter
     * here; every test that constructs a {@code Connection} directly via
     * the 3-arg constructor gets a no-op instead. */
    private final Runnable onTransientRetry;
    /** echoed response tags: negotiated during identify — when true, every request
     * carries a tag the server echoes, and {@link #readLoop} verifies the
     * echo against the oldest pending request before dispatching it. */
    private final boolean tagged;
    private int nextTag = 0;
    private final Deque<Pending> pending = new ArrayDeque<>();
    private volatile boolean closed = false;
    private volatile long lastUsedNanos = System.nanoTime();

    /** The progress-based request deadline (issue #42), guarded by
     * {@link #deadlineLock} — its own lock, not this connection's
     * monitor, because the watchdog must be able to fire even while a
     * caller is blocked in {@code out.write} holding the monitor (a
     * half-open peer can stall the write side too, and a monitor-based
     * watchdog could never reacquire it to act). Lock order is always
     * monitor → deadlineLock; the watchdog takes deadlineLock alone and
     * releases it before poisoning. 0 means unarmed. */
    private final Object deadlineLock = new Object();
    private long requestDeadlineNanos = 0;
    /** Set by the watchdog just before it closes the socket, so the
     * poison() that actually wins — often the reader's, reacting to the
     * close with a generic IOException — still reports the timeout. */
    private volatile NanocachedException timedOutError;

    /** {@code onClose} fires exactly once, the first time this connection
     * closes for any reason — used by {@link NanocachedClient} to keep its
     * forgotten-close open-sockets tracker accurate without every call
     * site remembering to decrement it by hand. */
    Connection(Socket socket, boolean tagged, Runnable onClose) throws IOException {
        this(socket, tagged, onClose, () -> {});
    }

    /** As {@link #Connection(Socket, boolean, Runnable)}, additionally
     * reporting every {@code R} response via {@code onTransientRetry} (see
     * that field's doc). */
    Connection(Socket socket, boolean tagged, Runnable onClose, Runnable onTransientRetry) throws IOException {
        this.socket = socket;
        this.tagged = tagged;
        this.onClose = onClose;
        this.onTransientRetry = onTransientRetry;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
        Thread reader = new Thread(this::readLoop, "nanocached-connection-reader");
        reader.setDaemon(true);
        reader.start();
        Thread watchdog = new Thread(this::watchdogLoop, "nanocached-request-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
    }

    /** Called with the monitor held (see {@link #deadlineLock}'s lock
     * order). Arms — or, on progress, re-arms — the request deadline. */
    private void armDeadline() {
        synchronized (deadlineLock) {
            requestDeadlineNanos = System.nanoTime() + requestTimeoutMillis * 1_000_000L;
            deadlineLock.notifyAll();
        }
    }

    /** Called from readLoop's dispatch (monitor held) and from poison()
     * (monitor released) — safe either way; see {@link #armDeadline}. */
    private void clearDeadline() {
        synchronized (deadlineLock) {
            requestDeadlineNanos = 0;
            deadlineLock.notifyAll();
        }
    }

    /** Sleeps until the armed deadline expires (poisoning the connection,
     * which unblocks the reader — and any blocked writer — with a socket
     * error) or the connection closes for another reason. Parked in
     * {@code wait()} whenever no deadline is armed, so an idle connection
     * is never closed by this. */
    private void watchdogLoop() {
        synchronized (deadlineLock) {
            while (!closed) {
                try {
                    if (requestDeadlineNanos == 0) {
                        deadlineLock.wait();
                    } else {
                        long remainingMillis = (requestDeadlineNanos - System.nanoTime()) / 1_000_000;
                        if (remainingMillis <= 0) break;
                        deadlineLock.wait(remainingMillis + 1);
                    }
                } catch (InterruptedException interrupted) {
                    return;
                }
            }
            if (closed) return;
        }
        // The deadline expired with requests still pending. Record the
        // timeout error first (so whichever thread's poison() wins the
        // race reports the timeout, not a bare "Socket closed"), then
        // close the socket *before* trying to poison: poison() takes the
        // monitor, and a writer wedged in out.write holds it — only the
        // close itself can unblock that writer (and the blocked reader).
        timedOutError = new NanocachedException.ConnectionFailed(
                "nanocached: no response from server within " + requestTimeoutMillis
                        + "ms (request timed out)",
                null);
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing an already-broken socket is fine.
        }
        poison(timedOutError);
    }

    boolean isClosed() {
        return closed || socket.isClosed();
    }

    long idleNanos() {
        return System.nanoTime() - lastUsedNanos;
    }

    void close() {
        poison(new NanocachedException.ConnectionFailed("nanocached: connection closed", null));
    }

    byte[] get(byte[] key) {
        Response response = request(tag -> frame("G " + key.length + tagSuffix(tag) + "\n", key, null));
        return switch (response.marker) {
            case 'V' -> response.value;
            case 'N' -> null;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    void set(byte[] key, byte[] value, Long ttlSeconds) {
        Response response = request(tag -> {
            String header = ttlSeconds == null
                    ? "S " + key.length + " " + value.length + tagSuffix(tag) + "\n"
                    : "S " + key.length + " " + value.length + " " + ttlSeconds + tagSuffix(tag) + "\n";
            return frame(header, key, value);
        });
        if (response.marker == 'W') throw new NanocachedException.WrongNode();
        if (response.marker != 'S') throw mismatch(response.marker);
    }

    boolean delete(byte[] key) {
        Response response = request(tag -> frame("D " + key.length + tagSuffix(tag) + "\n", key, null));
        return switch (response.marker) {
            case 'D' -> true;
            case 'N' -> false;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    // Namespaces (issue #105): `g`/`s`/`d` are the namespaced counterparts
    // of `G`/`S`/`D` — one extra leading `<namespace-length>` header field,
    // namespace bytes leading the body, everything else (including the
    // response markers) identical. SDK rule: the default (empty) namespace
    // must keep sending the legacy `G`/`S`/`D` frames byte-for-byte, so an
    // unchanged client talking to an old server keeps working — delegating
    // to the untagged-namespace overload above rather than reimplementing
    // it guarantees that byte-for-byte equivalence rather than merely
    // aiming for it.

    byte[] get(byte[] namespace, byte[] key) {
        if (namespace.length == 0) return get(key);
        Response response = request(tag -> frame(
                "g " + namespace.length + " " + key.length + tagSuffix(tag) + "\n", namespace, key, null));
        return switch (response.marker) {
            case 'V' -> response.value;
            case 'N' -> null;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    void set(byte[] namespace, byte[] key, byte[] value, Long ttlSeconds) {
        if (namespace.length == 0) {
            set(key, value, ttlSeconds);
            return;
        }
        Response response = request(tag -> {
            String header = ttlSeconds == null
                    ? "s " + namespace.length + " " + key.length + " " + value.length + tagSuffix(tag) + "\n"
                    : "s " + namespace.length + " " + key.length + " " + value.length + " " + ttlSeconds
                            + tagSuffix(tag) + "\n";
            return frame(header, namespace, key, value);
        });
        if (response.marker == 'W') throw new NanocachedException.WrongNode();
        if (response.marker != 'S') throw mismatch(response.marker);
    }

    boolean delete(byte[] namespace, byte[] key) {
        if (namespace.length == 0) return delete(key);
        Response response = request(tag -> frame(
                "d " + namespace.length + " " + key.length + tagSuffix(tag) + "\n", namespace, key, null));
        return switch (response.marker) {
            case 'D' -> true;
            case 'N' -> false;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    // CLEAR namespace / flush everything (issue #106): `c`/`F` — unlike
    // g/s/d, these have no legacy pre-namespace equivalent to fall back to
    // (they're new in #106), so an empty namespace on `clear` always sends
    // `c 0` rather than some other frame — that IS how the default
    // namespace is addressed. Neither is key-addressed (protocol.html: "c
    // / F — clear a namespace, flush everything"), so a node never answers
    // either with `W`; only `C`.

    /** Drops every entry in one namespace on this connection's node —
     * {@code namespace.length == 0} clears the default namespace
     * ({@code c 0}). */
    void clear(byte[] namespace) {
        Response response = request(tag -> frame(
                "c " + namespace.length + tagSuffix(tag) + "\n", namespace, null));
        if (response.marker != 'C') throw mismatch(response.marker);
    }

    /** Drops every namespace on this connection's node, the default one
     * included. */
    void clearAll() {
        Response response = request(tag -> frame("F" + tagSuffix(tag) + "\n"));
        if (response.marker != 'C') throw mismatch(response.marker);
    }

    // INCR (issue #129): `i` — unlike G/S/D there is no separate
    // uppercase/legacy form; it always carries an explicit
    // <namespace-length> (0 = default namespace). `<delta>` is a signed
    // decimal long (a negative delta decrements — see
    // NanocachedClient.decr, which sends this same `i` op with a negated
    // delta rather than a separate wire opcode). A hit answers `I
    // <value-length> [<ttl-seconds>] [<tag>]` — the ttl field is optional
    // exactly like `S`'s own trailing TTL is on the request side,
    // disambiguated purely by whether this connection is tagged (see
    // readResponse's 'I' case); a miss answers `N` (mirroring get's own
    // null-on-miss convention below); a non-numeric stored value or an
    // overflowing delta answers `T`, a marker no other op uses.

    /** The outcome of a successful INCR: the new counter value, and —
     * only when the entry carries a TTL — its remaining seconds. {@code
     * null} on a miss (see {@link #incr}), never on a hit. */
    record IncrResult(long value, Long ttlSeconds) {}

    IncrResult incr(byte[] namespace, byte[] key, long delta) {
        Response response = request(tag -> frame(
                "i " + namespace.length + " " + key.length + " " + delta + tagSuffix(tag) + "\n",
                namespace, key, null));
        return switch (response.marker) {
            case 'I' -> new IncrResult(parseCounterValue(response.value), response.ttlSeconds);
            case 'N' -> null;
            case 'T' -> throw new NanocachedException.NotNumeric();
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    /** Parses an {@code I} response's value body — decimal ASCII, the
     * same grammar as {@code delta} (see {@link #incr}) — back to a
     * {@code long}. A non-canonical body is protocol garbage: the
     * connection is desynced mid-frame and must be poisoned, exactly like
     * {@link #readResponse}'s own malformed-length checks (this one runs
     * on the caller's thread rather than the reader thread, so — unlike
     * those — it must poison explicitly rather than rely on {@link
     * #readLoop}'s catch). A leading {@code '-'} is the one exception to
     * {@link #isDigitsOnly}'s grammar allowed here — INCR/DECR deltas (and
     * so counter values) can go negative, unlike every other wire-derived
     * integer field — but a leading {@code '+'} is still rejected: {@link
     * Long#parseLong} would otherwise accept it (issue #462), and the
     * wire grammar never permits it even here. Mirrors .NET's
     * {@code TryParseWireCounter}'s explicit {@code body[0] == '+'}
     * check. */
    private long parseCounterValue(byte[] value) {
        String text = new String(value, StandardCharsets.US_ASCII);
        boolean negative = text.startsWith("-");
        String digits = negative ? text.substring(1) : text;
        if (isDigitsOnly(digits)) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException overflow) {
                // fall through to poison below
            }
        }
        NanocachedException error = new NanocachedException.ConnectionFailed(
                "nanocached: invalid INCR value in response (connection desynced)", null);
        poison(error);
        throw error;
    }

    // Compare-and-set (issue #141): `k`/`x` — same always-namespaced
    // shape as INCR (an explicit <namespace-length>, 0 = default
    // namespace; no legacy pre-namespace form). <cond> is a bare,
    // non-length-prefixed token: "A" (absent), "P" (present), or a
    // 32-character lowercase hex digest (exact content match) — see
    // NanocachedClient.contentDigest. Both reuse existing response
    // markers rather than introducing new ones: `k` answers `S`
    // (stored) or `N` (condition mismatch), exactly S/N's own
    // bare-or-tagged shape; `x` answers `D` (deleted) or `N`
    // (mismatch/missing), exactly D/N's shape. Replication is the
    // caller's job (NanocachedClient): only the primary evaluates
    // <cond>, and a success's literal result is forwarded to the
    // remaining owners as an ordinary set/delete, never by replaying
    // k/x itself.

    /** Sends {@code k} — stores {@code value} at {@code key} only if
     * {@code cond} holds against the key's current stored bytes.
     * Returns {@code true} on success ({@code S}), {@code false} on a
     * condition mismatch ({@code N}) — a normal outcome, not an
     * exception. */
    boolean casSet(byte[] namespace, byte[] key, byte[] value, Long ttlSeconds, String cond) {
        Response response = request(tag -> {
            String header = ttlSeconds == null
                    ? "k " + namespace.length + " " + key.length + " " + value.length + " " + cond
                            + tagSuffix(tag) + "\n"
                    : "k " + namespace.length + " " + key.length + " " + value.length + " " + cond + " "
                            + ttlSeconds + tagSuffix(tag) + "\n";
            return frame(header, namespace, key, value);
        });
        return switch (response.marker) {
            case 'S' -> true;
            case 'N' -> false;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    /** Sends {@code x} — removes {@code key} only if {@code cond} (always
     * a digest — an absent/present-only conditioned delete is already the
     * plain, unconditional {@code D}) holds. Returns {@code true} on
     * success ({@code D}), {@code false} on a mismatch or missing key
     * ({@code N}). */
    boolean casDelete(byte[] namespace, byte[] key, String cond) {
        Response response = request(tag -> frame(
                "x " + namespace.length + " " + key.length + " " + cond + tagSuffix(tag) + "\n",
                namespace, key, null));
        return switch (response.marker) {
            case 'D' -> true;
            case 'N' -> false;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    // Echoed response tags: on a tagged-mode connection every request header carries
    // the client's tag as its last field, and the server echoes it in the
    // response — `tag == null` is the untagged (pre-0019) form, which
    // must serialize byte-for-byte as it always has.
    private static String tagSuffix(Integer tag) {
        return tag == null ? "" : " " + Integer.toUnsignedString(tag);
    }

    /**
     * A well-formed response of the wrong kind (a {@code S} answering a G)
     * means the request/response streams are misaligned — every later
     * response would answer the wrong request, silently returning other
     * keys' data. Poison the connection, and classify as connection-level
     * so the client's retry layer redials and retries once. Requests
     * still pending behind this one may already have been resolved with
     * misaligned data by the time this runs — an inherent limitation of
     * matching-by-order pipelining shared with the TypeScript SDK's
     * Connection (request pipelining), not something this SDK introduces.
     */
    private NanocachedException mismatch(int marker) {
        NanocachedException error = new NanocachedException.ConnectionFailed(
                "nanocached: response '" + (char) marker + "' does not match the request (connection desynced)",
                null);
        poison(error);
        return error;
    }

    /**
     * An {@code M}/{@code O} response whose result-roster length doesn't
     * match the request's key count: the streams are just as desynced as
     * a kind mismatch ({@link #mismatch}), so this poisons the connection
     * the same way.
     */
    private NanocachedException desyncedEntryCount(String op, int got, int want) {
        NanocachedException error = new NanocachedException.ConnectionFailed(
                "nanocached: " + op + " response roster length " + got
                        + " does not match request key count " + want + " (connection desynced)",
                null);
        poison(error);
        return error;
    }

    /** True iff every character of {@code field} is an ASCII digit and
     * the string is non-empty — the wire's own integer grammar (issue
     * #462; see {@code src/command.rs}'s {@code parse_length}, which
     * loops byte-by-byte over ASCII digits with no leading-zero
     * restriction but no leading {@code '+'} either). {@link
     * Integer#parseInt}/{@link Long#parseLong} are looser than this grammar
     * — both accept an optional leading {@code '+'} — so every
     * wire-derived integer field (except the one exception, {@link
     * #parseCounterValue}'s leading {@code '-'}) must pass this check
     * before being handed to them. Package-private: shared with {@link
     * Identify} and {@link NanocachedClient}, both in this package. */
    static boolean isDigitsOnly(String field) {
        if (field.isEmpty()) {
            return false;
        }
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /** Parses a decimal field as {@code readResponse}'s length/count
     * fields do throughout: a field that isn't {@link #isDigitsOnly} (in
     * particular one carrying a leading {@code '+'}, which {@link
     * Integer#parseInt} would otherwise silently accept — issue #462) or
     * that overflows an {@code int} is protocol garbage, reported here as
     * -1 so every caller's existing {@code < 0} bounds check catches it
     * uniformly. Package-private: shared with {@link Identify}. */
    static int parseNonNegativeInt(String field) {
        if (!isDigitsOnly(field)) {
            return -1;
        }
        try {
            return Integer.parseInt(field);
        } catch (NumberFormatException overflow) {
            return -1;
        }
    }

    /** {@link #parseNonNegativeInt}, but for a {@code long}-sized field
     * (the {@code I} response's optional TTL — issue #462). */
    private static long parseNonNegativeLong(String field) {
        if (!isDigitsOnly(field)) {
            return -1;
        }
        try {
            return Long.parseLong(field);
        } catch (NumberFormatException overflow) {
            return -1;
        }
    }

    // Batched get/set (issue #151): `m`/`o` — same always-namespaced
    // shape as INCR/CAS (an explicit <namespace-length>, 0 = default
    // namespace; no legacy pre-namespace form — batching has no
    // pre-batching wire form to stay compatible with). Restricted to one
    // namespace per frame (docs/protocol.html#multi): rendezvous hashing
    // routes on (namespace, key), so a frame mixing namespaces couldn't
    // route as a single unit anyway. Routing (grouping keys by owner,
    // wrong-node retry, replica fan-out for multiSet) is
    // NanocachedClient's job, same split as every other op here.

    /** Sends {@code m} — one round trip for every key in {@code keys}
     * (docs/protocol.html#multi). {@code entries[i]} answers
     * {@code keys[i]}, in request order. A reply whose roster length
     * doesn't match {@code keys.length} is treated as a desynced
     * connection, same stance as {@link #mismatch} — a malformed reply
     * can't be trusted key-for-key. */
    List<MultiEntry> multiGet(byte[] namespace, byte[][] keys) {
        Response response = request(tag -> buildMultiGetFrame(namespace, keys, tag));
        if (response.marker() != 'M') throw mismatch(response.marker());
        if (response.entries().size() != keys.length) {
            throw desyncedEntryCount("multi-get", response.entries().size(), keys.length);
        }
        return response.entries();
    }

    /** Builds an {@code m} request frame: {@code m <ns-len> <n>
     * <key-len-1> ... <key-len-n>[ <tag>]\n<ns><key-1>...<key-n>}
     * (docs/protocol.html#multi). */
    private static byte[] buildMultiGetFrame(byte[] namespace, byte[][] keys, Integer tag) {
        StringBuilder header = new StringBuilder("m ").append(namespace.length).append(' ').append(keys.length);
        for (byte[] key : keys) {
            header.append(' ').append(key.length);
        }
        header.append(tagSuffix(tag)).append('\n');
        byte[] headerBytes = header.toString().getBytes(StandardCharsets.US_ASCII);

        int bodyLength = namespace.length;
        for (byte[] key : keys) {
            bodyLength += key.length;
        }
        byte[] frame = new byte[headerBytes.length + bodyLength];
        int offset = 0;
        System.arraycopy(headerBytes, 0, frame, offset, headerBytes.length);
        offset += headerBytes.length;
        System.arraycopy(namespace, 0, frame, offset, namespace.length);
        offset += namespace.length;
        for (byte[] key : keys) {
            System.arraycopy(key, 0, frame, offset, key.length);
            offset += key.length;
        }
        return frame;
    }

    /** Sends {@code o} — stores every key/value pair in one round trip,
     * one shared {@code ttlSeconds} (null means no expiry) for the whole
     * batch rather than per key (docs/protocol.html#multi).
     * {@code entries[i]} answers {@code keys[i]}/{@code values[i]}, in
     * request order; see {@link #multiGet} for the same "only a desynced
     * roster is an error" stance. */
    List<MultiEntry> multiSet(byte[] namespace, byte[][] keys, byte[][] values, Long ttlSeconds) {
        Response response = request(tag -> buildMultiSetFrame(namespace, keys, values, ttlSeconds, tag));
        if (response.marker() != 'O') throw mismatch(response.marker());
        if (response.entries().size() != keys.length) {
            throw desyncedEntryCount("multi-set", response.entries().size(), keys.length);
        }
        return response.entries();
    }

    /** Builds an {@code o} request frame: {@code o <ns-len> <n>
     * <key-len-1> <value-len-1> ... <key-len-n> <value-len-n> [<ttl>][
     * <tag>]\n<ns><key-1><value-1>...<key-n><value-n>}
     * (docs/protocol.html#multi). The optional TTL sits ahead of the tag,
     * same convention {@link #casSet}'s own {@code [ttl]} uses. */
    private static byte[] buildMultiSetFrame(
            byte[] namespace, byte[][] keys, byte[][] values, Long ttlSeconds, Integer tag) {
        StringBuilder header = new StringBuilder("o ").append(namespace.length).append(' ').append(keys.length);
        for (int i = 0; i < keys.length; i++) {
            header.append(' ').append(keys[i].length).append(' ').append(values[i].length);
        }
        if (ttlSeconds != null) {
            header.append(' ').append(ttlSeconds);
        }
        header.append(tagSuffix(tag)).append('\n');
        byte[] headerBytes = header.toString().getBytes(StandardCharsets.US_ASCII);

        int bodyLength = namespace.length;
        for (int i = 0; i < keys.length; i++) {
            bodyLength += keys[i].length + values[i].length;
        }
        byte[] frame = new byte[headerBytes.length + bodyLength];
        int offset = 0;
        System.arraycopy(headerBytes, 0, frame, offset, headerBytes.length);
        offset += headerBytes.length;
        System.arraycopy(namespace, 0, frame, offset, namespace.length);
        offset += namespace.length;
        for (int i = 0; i < keys.length; i++) {
            System.arraycopy(keys[i], 0, frame, offset, keys[i].length);
            offset += keys[i].length;
            System.arraycopy(values[i], 0, frame, offset, values[i].length);
            offset += values[i].length;
        }
        return frame;
    }

    /**
     * Marks the connection closed, closes the socket, and rejects every
     * still-pending request with error. Safe to call more than once —
     * from a writer noticing a failed write, the reader thread noticing a
     * failed read, or an explicit close() — only the first call has any
     * effect.
     */
    private void poison(NanocachedException error) {
        // A watchdog-recorded timeout is the root cause — the generic
        // IOException another thread saw is just the closed socket's echo
        // of it (see watchdogLoop).
        NanocachedException timeout = timedOutError;
        if (timeout != null) error = timeout;
        List<Pending> drained;
        synchronized (this) {
            if (closed) return;
            closed = true;
            drained = new ArrayList<>(pending);
            pending.clear();
        }
        clearDeadline();
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing an already-broken socket is fine.
        }
        for (Pending p : drained) {
            p.future().completeExceptionally(error);
        }
        onClose.run();
    }

    /** A header-only frame with no key/namespace/value bytes to follow
     * (issue #106's {@code F}, the only request this connection ever
     * sends with an empty body). */
    private static byte[] frame(String header) {
        return header.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] frame(String header, byte[] key, byte[] value) {
        byte[] headerBytes = header.getBytes(StandardCharsets.US_ASCII);
        byte[] frame = new byte[headerBytes.length + key.length + (value == null ? 0 : value.length)];
        System.arraycopy(headerBytes, 0, frame, 0, headerBytes.length);
        System.arraycopy(key, 0, frame, headerBytes.length, key.length);
        if (value != null) {
            System.arraycopy(value, 0, frame, headerBytes.length + key.length, value.length);
        }
        return frame;
    }

    /** As {@link #frame(String, byte[], byte[])}, with the namespace bytes
     * leading the body ({@code <namespace><key>[<value>]}) — the `g`/`s`/`d`
     * frame shape (namespaces, issue #105). */
    private static byte[] frame(String header, byte[] namespace, byte[] key, byte[] value) {
        byte[] headerBytes = header.getBytes(StandardCharsets.US_ASCII);
        int valueLength = value == null ? 0 : value.length;
        byte[] frame = new byte[headerBytes.length + namespace.length + key.length + valueLength];
        int offset = 0;
        System.arraycopy(headerBytes, 0, frame, offset, headerBytes.length);
        offset += headerBytes.length;
        System.arraycopy(namespace, 0, frame, offset, namespace.length);
        offset += namespace.length;
        System.arraycopy(key, 0, frame, offset, key.length);
        offset += key.length;
        if (value != null) {
            System.arraycopy(value, 0, frame, offset, value.length);
        }
        return frame;
    }

    /** {@code ttlSeconds} is only ever non-null for an {@code I} response
     * that carried one (issue #129) — every other marker leaves it {@code
     * null}. {@code entries} is only ever non-null for an {@code M}
     * (multi-get) or {@code O} (multi-set) response (issue #151,
     * docs/protocol.html#multi) — every other marker leaves it {@code
     * null}, {@code value} unused for those two markers instead. */
    private record Response(int marker, byte[] value, int tag, Long ttlSeconds, List<MultiEntry> entries) {}

    /** One key's outcome inside an {@code M} (multi-get) or {@code O}
     * (multi-set) response (issue #151, docs/protocol.html#multi) — a
     * batch never fails as a whole, so each key's result is independent
     * of every other key's, same as the server's own multi-ack. Reused
     * for both response kinds rather than two near-identical types:
     * <ul>
     * <li>{@code M}: {@link #ok} true is a hit ({@link #value} holds the
     * bytes, possibly empty); {@link #wrongNode} is a per-key {@code W};
     * neither set is a clean miss ({@code -}).
     * <li>{@code O}: {@link #ok} true is {@code S} (stored);
     * {@link #wrongNode} is {@code W}; {@link #value} is always
     * {@code null} — a set has nothing to echo back.
     * </ul> */
    record MultiEntry(byte[] value, boolean ok, boolean wrongNode) {
        static MultiEntry ofHit(byte[] value) {
            return new MultiEntry(value, true, false);
        }

        static MultiEntry ofMiss() {
            return new MultiEntry(null, false, false);
        }

        static MultiEntry ofWrongNode() {
            return new MultiEntry(null, false, true);
        }

        static MultiEntry ofStored() {
            return new MultiEntry(null, true, false);
        }
    }

    /** A pending request's future paired with the tag its response must
     * echo (echoed response tags) — meaningless (and never compared) on an untagged
     * connection. */
    private record Pending(CompletableFuture<Response> future, int tag) {}

    /** A request's frame, fixed for the whole call including any
     * transient ({@code R}) retries (issue #125) — a retry resends this
     * exact same {@code frame}/{@code tag} pair, since it is the same
     * logical request, not a new one; only the pending slot and its
     * future are recreated per attempt. */
    private record PreparedRequest(byte[] frame, int tag) {}

    // Transient-error retry (issue #125): an `R` answer means THIS
    // request failed transiently on the server (today only
    // nanocached-proxy sends it, when its upstream node briefly survived
    // its own one refresh-and-retry) — the connection itself is fine, so
    // the same request is resent on it up to twice more before giving up.
    // 3 total attempts (the original send plus these 2 retries), 50ms
    // before the first retry and 100ms before the second — fixed by the
    // spec, not caller-configurable.
    private static final int MAX_TRANSIENT_ATTEMPTS = 3;
    private static final long[] TRANSIENT_RETRY_DELAYS_MILLIS = {50, 100};

    /** Enqueues a pending slot and writes frame under one monitor — see
     * the class doc comment — then blocks this caller's own thread on
     * its own future, not the socket. {@code build} receives this
     * request's claimed tag ({@code null} on an untagged connection) and
     * must return the frame to write.
     *
     * <p>An {@code R} response (issue #125) is never handed back to the
     * caller: it is retried here, transparently, on this same connection
     * — bounded by {@link #MAX_TRANSIENT_ATTEMPTS} — until either a real
     * answer arrives or the budget is exhausted, at which point this
     * throws {@link NanocachedException.RetryableError} without touching
     * the connection's open/closed state. {@code R} is therefore never
     * seen by {@link #readResponse}'s callers (the {@code get}/{@code
     * set}/{@code delete}/etc. marker switches), exactly like a normal
     * marker mismatch never is. */
    private Response request(Function<Integer, byte[]> build) {
        if (isClosed()) {
            // notSent=true (issue #225): this call's frame never touched
            // the wire — the connection was already dead (e.g. an
            // idle-timeout FIN the reader thread already noticed), so a
            // non-idempotent caller may safely redial and resend.
            throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null, true);
        }

        PreparedRequest prepared = null;
        for (int attempt = 1; ; attempt++) {
            CompletableFuture<Response> future = new CompletableFuture<>();
            synchronized (this) {
                if (isClosed()) {
                    // As above (notSent=true) — this attempt's frame still
                    // hasn't been written; only a concurrent poison() (or
                    // an 'R' retry racing a close) landed between the
                    // check above and this one.
                    throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null, true);
                }
                lastUsedNanos = System.nanoTime();
                if (prepared == null) {
                    // Echoed response tags: the tag is claimed in the same synchronous span
                    // that enqueues the pending slot and writes the frame
                    // (request pipelining's enqueue+write atomicity), so tag order
                    // can never skew from queue/wire order. Built before
                    // enqueueing: a builder that fails (e.g. an invalid TTL) must
                    // fail with nothing queued, or the next response would
                    // resolve an orphaned slot and desync the stream. Computed
                    // once — a transient retry below reuses this same frame/tag
                    // rather than calling build again.
                    Integer tag = tagged ? claimTag() : null;
                    prepared = new PreparedRequest(build.apply(tag), tag == null ? -1 : tag);
                }
                pending.addLast(new Pending(future, prepared.tag()));
                // Armed only on the empty→non-empty transition: arming on
                // *every* request would let a continuous stream of new
                // requests push the deadline forever ahead of a server that
                // has stopped answering — exactly the half-open hang the
                // timeout exists to catch (issue #42).
                if (pending.size() == 1) armDeadline();
                try {
                    out.write(prepared.frame());
                    out.flush();
                } catch (IOException error) {
                    // The stream state after a failed write is unknown —
                    // poison the connection so the client redials lazily.
                    // notSent stays false (issue #225): out.write may have
                    // handed some or all of this frame's bytes to the OS
                    // send buffer before failing, so a non-idempotent
                    // caller must NOT treat this as "never sent" and
                    // replay it.
                    poison(new NanocachedException.ConnectionFailed(
                            "nanocached: connection failed: " + error.getMessage(), error));
                }
            }

            Response response;
            try {
                response = future.join();
            } catch (CompletionException wrapped) {
                Throwable cause = wrapped.getCause();
                if (cause instanceof NanocachedException nanocachedError) throw nanocachedError;
                throw new NanocachedException.ConnectionFailed("nanocached: connection failed", cause);
            }

            if (response.marker != 'R') return response;

            // Every R seen counts toward transient_retries (issue #125),
            // including the final one that exhausts the budget.
            onTransientRetry.run();
            if (attempt >= MAX_TRANSIENT_ATTEMPTS) {
                throw new NanocachedException.RetryableError();
            }
            sleepBeforeTransientRetry(TRANSIENT_RETRY_DELAYS_MILLIS[attempt - 1]);
        }
    }

    /** Sleeps between transient-error retries (issue #125) — an
     * interruption here is treated like any other wait this SDK does on
     * the caller's own thread (e.g. {@link
     * NanocachedClient#readHedged}'s queue poll): the interrupt flag is
     * restored and a {@link NanocachedException} surfaces rather than a
     * raw {@link InterruptedException}, since every exception this SDK
     * throws must extend it. */
    private static void sleepBeforeTransientRetry(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new NanocachedException(
                    "nanocached: interrupted while waiting to retry a transient (R) failure", interrupted);
        }
    }

    /** A connection-scoped u32 wrapping counter (echoed response tags), 0-based —
     * only ever called from within the {@code synchronized (this)} block
     * in {@link #request}, so no separate synchronization is needed here.
     * Encoded/decoded as unsigned decimal text (see {@link #tagSuffix}
     * and {@link #parseTag}) since a Java {@code int} wraps at the same
     * 2^32 width as the wire's u32, just with a different sign
     * interpretation. */
    private int claimTag() {
        int tag = nextTag;
        nextTag++;
        return tag;
    }

    // The server's own request cap is 1 MiB; this constant doubles that
    // as headroom, so a claimed length beyond it is definitely a corrupt
    // or malicious frame, never just a legitimately large value.
    private static final int MAX_VALUE_LENGTH = 2 * 1024 * 1024;

    // Bounds the sum of every hit's declared length across one multi-get
    // ('M') reply (issue #179) — each individual length is already
    // capped at MAX_VALUE_LENGTH above, but that alone doesn't bound the
    // reply as a whole: a node answering a 400-key multi-get with 400 ×
    // 2 MiB hits would force ~800 MB of allocation from a single reply.
    // NanocachedClient's own chunking (MAX_BATCH_KEYS, 400) never asks a
    // single M for more keys than that, so 400 × MAX_VALUE_LENGTH
    // (~800 MB) would still be a "faithful to the protocol's own limits"
    // bound — but it's much too loose a defense: it lets almost the
    // entire attack in issue #179 through unchanged. A single wire
    // reply legitimately needing more than 64 MiB is already an
    // unreasonable shape for one round trip — a real caller batching
    // that much data can and should ask for it in more, smaller
    // multi-gets — so this reuses Compression.MAX_DECOMPRESSED_LENGTH's
    // 64 MiB cap (issue #41, shared with the other five SDKs) instead of
    // deriving from MAX_BATCH_KEYS × MAX_VALUE_LENGTH. Checked before
    // each readExactly below, the same way Identify.readNodeEntries
    // tracks totalBytes against MAX_NODE_LIST_RESPONSE_BYTES — so an
    // oversized claim poisons the connection before the allocation
    // happens, not after. Mutable only so a test can shrink it and
    // exercise the bound without actually moving tens of megabytes over
    // a loopback socket, mirroring NanocachedClient's
    // keepAliveIntervalMillis/maxInFlightBackgroundReplicaWrites.
    static volatile long maxMultiGetResponseBytes = 64L * 1024 * 1024;

    // Header/tag lines (the marker line ahead of a V's body, or the whole
    // line for S/D/N/W) are always a handful of bytes in the real
    // protocol. Without a cap, a malicious or buggy node that streams
    // bytes with no '\n' would grow readLine()'s buffer without bound,
    // gated only by requestTimeoutMillis rather than failing fast — mirrors
    // .NET's Connection.MaxHeaderLineLength and Rust's 4 KiB (issue: audit
    // finding, unbounded readLine). Package-private (not private): shared
    // with Identify.java's own readLine, which bounds the discovery
    // server's `N <count> <r>`/entry header lines against the same
    // failure mode (issue: audit finding J-readLine — Identify's readLine
    // had no cap at all).
    static final int MAX_HEADER_LINE_LENGTH = 4096;

    /** This connection's only reader, for its whole lifetime — nothing
     * else may read from {@code in}. Consumes responses off the wire and
     * dispatches each to the oldest pending request (FIFO —
     * Request pipelining). */
    private void readLoop() {
        while (true) {
            Response response;
            try {
                response = readResponse();
            } catch (IOException error) {
                poison(new NanocachedException.ConnectionFailed(
                        "nanocached: connection failed: " + error.getMessage(), error));
                return;
            } catch (NanocachedException error) {
                poison(error);
                return;
            }

            Pending waiter;
            boolean wasEmpty;
            synchronized (this) {
                wasEmpty = pending.isEmpty();
                waiter = wasEmpty ? null : pending.pollFirst();
                // Progress-based deadline (see request()): a dispatched
                // response is progress, so the next-oldest request gets a
                // fresh window; with nothing left waiting, clear it so an
                // otherwise-idle connection is never closed by it. Under
                // the monitor so this can't race a concurrent request()
                // arming the deadline for a request this section didn't
                // see.
                if (waiter != null) {
                    if (pending.isEmpty()) clearDeadline();
                    else armDeadline();
                }
            }

            // An unsolicited "busy" response means the server hit its
            // connection limit right after accept and is about to close
            // the connection; it isn't an answer to anything we sent
            // (mirrors the TypeScript SDK's Connection.onData).
            if (response.marker == 'B' && wasEmpty) {
                poison(new NanocachedException.ConnectionFailed(
                        "nanocached: server rejected the connection (connection limit reached)", null));
                return;
            }
            if (waiter == null) {
                poison(new NanocachedException.ConnectionFailed(
                        "nanocached: unsolicited response '" + (char) response.marker
                                + "' from server (connection desynced)",
                        null));
                return;
            }

            // Echoed response tags: verify the echoed tag against the request this
            // response is about to answer — *before* it can reach any
            // caller. A mismatch means the streams are misaligned; unlike
            // the caller-side kind check (mismatch()), catching it here
            // stops the misdelivery instead of merely noticing it later.
            if (tagged && response.tag != waiter.tag()) {
                NanocachedException error = new NanocachedException.ConnectionFailed(
                        "nanocached: response tag " + response.tag + " does not answer request tag "
                                + waiter.tag() + " (connection desynced)",
                        null);
                poison(error);
                // The polled waiter is no longer in `pending`, so poison()
                // won't reach it — complete it here; the rest drain there.
                waiter.future().completeExceptionally(error);
                return;
            }

            waiter.future().complete(response);
        }
    }

    private Response readResponse() throws IOException {
        int marker = readByte();
        switch (marker) {
            case 'V' -> {
                // Untagged: `V <len>`. Tagged: `V <len> <tag>` (echoed response tags).
                String[] fields = readLine().split(" ");
                if (fields.length != (tagged ? 2 : 1)) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid value header in response", null);
                }
                // A non-numeric (issue #462: including a leading '+'),
                // negative, or absurd length is protocol garbage: the
                // connection is desynced mid-frame and must be poisoned,
                // and the error must be connection-classified so the
                // redial/retry layer handles it (issue #8).
                int length = parseNonNegativeInt(fields[0]);
                if (length < 0 || length > MAX_VALUE_LENGTH) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid value length in response", null);
                }
                int tag = tagged ? parseTag(fields[1]) : -1;
                return new Response(marker, readExactly(length), tag, null, null);
            }
            // 'I' is INCR's (issue #129) hit reply: `I <value-length>
            // [<ttl-seconds>] [<tag>]`. The ttl field is optional exactly
            // like S's own trailing TTL is on the request side —
            // disambiguated purely by whether this connection is tagged,
            // never guessed frame by frame: untagged, 1 field means no
            // ttl and 2 means ttl present; tagged, 1 means just the tag
            // and 2 means ttl-then-tag.
            case 'I' -> {
                String[] fields = readLine().split(" ");
                int fieldsWithoutTtl = tagged ? 2 : 1;
                int fieldsWithTtl = tagged ? 3 : 2;
                if (fields.length != fieldsWithoutTtl && fields.length != fieldsWithTtl) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid incr value header in response", null);
                }
                int length = parseNonNegativeInt(fields[0]);
                if (length < 0 || length > MAX_VALUE_LENGTH) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid value length in response", null);
                }
                Long ttlSeconds = null;
                if (fields.length == fieldsWithTtl) {
                    // long-sized, so it can't route through
                    // parseNonNegativeInt — same digits-only grammar
                    // (issue #462) via parseNonNegativeLong instead.
                    long parsedTtl = parseNonNegativeLong(fields[1]);
                    if (parsedTtl < 0) {
                        throw new NanocachedException.ConnectionFailed(
                                "nanocached: invalid ttl in response", null);
                    }
                    ttlSeconds = parsedTtl;
                }
                int tag = tagged ? parseTag(fields[fields.length - 1]) : -1;
                return new Response(marker, readExactly(length), tag, ttlSeconds, null);
            }
            // Busy is always bare (echoed response tags): it's an unsolicited
            // pre-auth response, never an answer to a tagged request.
            case 'B' -> {
                expectLf(); // the trailing '\n'
                return new Response(marker, null, -1, null, null);
            }
            // 'C' is CLEAR namespace / flush everything's (issue #106) only
            // reply — same bare-or-tagged shape as S/D/N/W. 'R' (issue
            // #125's retryable-error status) is possible on any data
            // command (G/S/D/g/s/d/c/F this connection sends) and shares
            // that exact bare-or-tagged shape too: `R\n` untagged, `R
            // <tag>\n` tagged — request() intercepts it before any caller
            // ever sees it. 'T' is INCR's (issue #129) non-numeric-value
            // reply — same bare-or-tagged shape too.
            case 'S', 'D', 'N', 'W', 'C', 'R', 'T' -> {
                if (!tagged) {
                    expectLf(); // the trailing '\n'
                    return new Response(marker, null, -1, null, null);
                }
                return new Response(marker, null, parseTag(readLine()), null, null);
            }
            // 'M' is multi-get's (issue #151, docs/protocol.html#multi)
            // reply: `M <n> <result-1> ... <result-n>[ <tag>]\n<hit
            // values, concatenated in request order>`. Each result token
            // is "-" (miss), "W" (wrong node), or a decimal byte length
            // (a hit — that many trailing body bytes belong to this key,
            // read here, inline, in token order, since only hit tokens
            // consume body bytes).
            case 'M' -> {
                String[] fields = readLine().split(" ");
                if (fields.length < 1) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid multi-get header in response", null);
                }
                int count = parseNonNegativeInt(fields[0]);
                if (count < 0) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid multi-get count in response", null);
                }
                int wantFields = 1 + count + (tagged ? 1 : 0);
                if (fields.length != wantFields) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid multi-get header in response", null);
                }
                List<MultiEntry> entries = new ArrayList<>(count);
                long totalBytes = 0;
                for (int i = 0; i < count; i++) {
                    String token = fields[1 + i];
                    switch (token) {
                        case "-" -> entries.add(MultiEntry.ofMiss());
                        case "W" -> entries.add(MultiEntry.ofWrongNode());
                        default -> {
                            int length = parseNonNegativeInt(token);
                            if (length < 0 || length > MAX_VALUE_LENGTH) {
                                throw new NanocachedException.ConnectionFailed(
                                        "nanocached: invalid multi-get result length in response", null);
                            }
                            totalBytes += length;
                            if (totalBytes > maxMultiGetResponseBytes) {
                                throw new NanocachedException.ConnectionFailed(
                                        "nanocached: multi-get response exceeds "
                                                + maxMultiGetResponseBytes + " bytes", null);
                            }
                            entries.add(MultiEntry.ofHit(readExactly(length)));
                        }
                    }
                }
                int tag = tagged ? parseTag(fields[1 + count]) : -1;
                return new Response(marker, null, tag, null, entries);
            }
            // 'O' is multi-set's (issue #151, docs/protocol.html#multi)
            // reply: `O <n> <result-1> ... <result-n>[ <tag>]\n` — no
            // body, unlike M's hit values (a set has nothing to echo
            // back). Each token is "S" (stored) or "W" (wrong node).
            // Never confused with the `On`/`OnT` identify reply:
            // Identify.java handles that before a Connection exists, and
            // no other request ever answers with a leading 'O'.
            //
            // No maxMultiGetResponseBytes-style cumulative bound is
            // needed here (issue #179): every ack token is one of two
            // fixed one-character strings with no length-prefixed body
            // to read, so this loop's cost is already O(count) and
            // count is already bounded — this header line's own length
            // (capped like every other header, see readLine) limits how
            // many single-character tokens can fit on it in the first
            // place.
            case 'O' -> {
                String[] fields = readLine().split(" ");
                if (fields.length < 1) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid multi-set header in response", null);
                }
                int count = parseNonNegativeInt(fields[0]);
                if (count < 0) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid multi-set count in response", null);
                }
                int wantFields = 1 + count + (tagged ? 1 : 0);
                if (fields.length != wantFields) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid multi-set header in response", null);
                }
                List<MultiEntry> entries = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String token = fields[1 + i];
                    switch (token) {
                        case "S" -> entries.add(MultiEntry.ofStored());
                        case "W" -> entries.add(MultiEntry.ofWrongNode());
                        default -> throw new NanocachedException.ConnectionFailed(
                                "nanocached: invalid multi-set result token in response", null);
                    }
                }
                int tag = tagged ? parseTag(fields[1 + count]) : -1;
                return new Response(marker, null, tag, null, entries);
            }
            default -> throw new NanocachedException.ConnectionFailed(
                    "nanocached: unexpected response from server: " + (char) marker, null);
        }
    }

    /** Parses a tag field (echoed response tags) as unsigned decimal text, matching
     * the wire's u32 width — see {@link #claimTag}/{@link #tagSuffix}. A
     * non-numeric (issue #462: including a leading {@code '+'}, which
     * {@link Integer#parseUnsignedInt} would otherwise silently accept —
     * confirmed by inspection, not just javadoc) or out-of-range field is
     * protocol garbage: the connection is desynced and must be poisoned,
     * connection-classified so the redial/retry layer handles it. */
    private static int parseTag(String field) {
        if (!isDigitsOnly(field)) {
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: invalid response tag", null);
        }
        try {
            return Integer.parseUnsignedInt(field);
        } catch (NumberFormatException malformed) {
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: invalid response tag", null);
        }
    }

    private int readByte() throws IOException {
        int value = in.read();
        if (value == -1) throw new IOException("connection closed by the server");
        return value;
    }

    /** Consumes one byte and verifies it is '\n' — used for the untagged
     * fixed-shape responses (`S`/`D`/`N`/`W`/`B`), which are exactly two
     * bytes on the wire. A byte other than '\n' here means the streams
     * are desynced (e.g. a server that unexpectedly tagged a response on
     * an untagged connection) and every later response would be
     * misaligned too, so this must poison the connection rather than
     * silently ignore the extra byte. */
    private void expectLf() throws IOException {
        int value = readByte();
        if (value != '\n') {
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: unexpected byte after response marker (connection desynced)", null);
        }
    }

    /** Reads up to (and consuming) the next '\n', returning what preceded
     * it. Bounded by {@link #MAX_HEADER_LINE_LENGTH}: a malicious or
     * buggy node streaming bytes with no '\n' must fail fast instead of
     * growing this buffer without bound. */
    private String readLine() throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        for (int b = readByte(); b != '\n'; b = readByte()) {
            if (line.size() >= MAX_HEADER_LINE_LENGTH) {
                throw new NanocachedException.ConnectionFailed(
                        "nanocached: response header line too long", null);
            }
            line.write(b);
        }
        return line.toString(StandardCharsets.US_ASCII).trim();
    }

    private byte[] readExactly(int length) throws IOException {
        byte[] data = in.readNBytes(length);
        if (data.length != length) throw new IOException("connection closed mid-value");
        return data;
    }
}
