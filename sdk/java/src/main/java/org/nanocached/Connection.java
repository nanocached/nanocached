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
        this.socket = socket;
        this.tagged = tagged;
        this.onClose = onClose;
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

    private record Response(int marker, byte[] value, int tag) {}

    /** A pending request's future paired with the tag its response must
     * echo (echoed response tags) — meaningless (and never compared) on an untagged
     * connection. */
    private record Pending(CompletableFuture<Response> future, int tag) {}

    /** Enqueues a pending slot and writes frame under one monitor — see
     * the class doc comment — then blocks this caller's own thread on
     * its own future, not the socket. {@code build} receives this
     * request's claimed tag ({@code null} on an untagged connection) and
     * must return the frame to write. */
    private Response request(Function<Integer, byte[]> build) {
        if (isClosed()) {
            throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null);
        }

        CompletableFuture<Response> future = new CompletableFuture<>();
        synchronized (this) {
            if (isClosed()) {
                throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null);
            }
            lastUsedNanos = System.nanoTime();
            // Echoed response tags: the tag is claimed in the same synchronous span
            // that enqueues the pending slot and writes the frame
            // (request pipelining's enqueue+write atomicity), so tag order
            // can never skew from queue/wire order. Built before
            // enqueueing: a builder that fails (e.g. an invalid TTL) must
            // fail with nothing queued, or the next response would
            // resolve an orphaned slot and desync the stream.
            Integer tag = tagged ? claimTag() : null;
            byte[] frame = build.apply(tag);
            pending.addLast(new Pending(future, tag == null ? -1 : tag));
            // Armed only on the empty→non-empty transition: arming on
            // *every* request would let a continuous stream of new
            // requests push the deadline forever ahead of a server that
            // has stopped answering — exactly the half-open hang the
            // timeout exists to catch (issue #42).
            if (pending.size() == 1) armDeadline();
            try {
                out.write(frame);
                out.flush();
            } catch (IOException error) {
                // The stream state after a failed write is unknown —
                // poison the connection so the client redials lazily.
                poison(new NanocachedException.ConnectionFailed(
                        "nanocached: connection failed: " + error.getMessage(), error));
            }
        }

        try {
            return future.join();
        } catch (CompletionException wrapped) {
            Throwable cause = wrapped.getCause();
            if (cause instanceof NanocachedException nanocachedError) throw nanocachedError;
            throw new NanocachedException.ConnectionFailed("nanocached: connection failed", cause);
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
                // A non-numeric, negative, or absurd length is protocol
                // garbage: the connection is desynced mid-frame and must
                // be poisoned, and the error must be connection-classified
                // so the redial/retry layer handles it (issue #8).
                int length;
                try {
                    length = Integer.parseInt(fields[0]);
                } catch (NumberFormatException malformed) {
                    length = -1;
                }
                if (length < 0 || length > MAX_VALUE_LENGTH) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid value length in response", null);
                }
                int tag = tagged ? parseTag(fields[1]) : -1;
                return new Response(marker, readExactly(length), tag);
            }
            // Busy is always bare (echoed response tags): it's an unsolicited
            // pre-auth response, never an answer to a tagged request.
            case 'B' -> {
                expectLf(); // the trailing '\n'
                return new Response(marker, null, -1);
            }
            case 'S', 'D', 'N', 'W' -> {
                if (!tagged) {
                    expectLf(); // the trailing '\n'
                    return new Response(marker, null, -1);
                }
                return new Response(marker, null, parseTag(readLine()));
            }
            default -> throw new NanocachedException.ConnectionFailed(
                    "nanocached: unexpected response from server: " + (char) marker, null);
        }
    }

    /** Parses a tag field (echoed response tags) as unsigned decimal text, matching
     * the wire's u32 width — see {@link #claimTag}/{@link #tagSuffix}. A
     * non-numeric or out-of-range field is protocol garbage: the
     * connection is desynced and must be poisoned, connection-classified
     * so the redial/retry layer handles it. */
    private static int parseTag(String field) {
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
