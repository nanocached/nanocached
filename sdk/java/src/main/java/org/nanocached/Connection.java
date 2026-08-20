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
 * responses in send order (doc/adr/0016-*.md): a dedicated reader thread
 * consumes responses and dispatches each to the oldest still-pending
 * request, since nanocached-node itself only ever answers in the order it
 * received requests. Enqueuing the pending slot and writing the frame
 * happen under one monitor, so concurrent callers' queue order always
 * matches the order their frames actually hit the wire.
 */
final class Connection {
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final Runnable onClose;
    /** ADR-0019: negotiated during identify — when true, every request
     * carries a tag the server echoes, and {@link #readLoop} verifies the
     * echo against the oldest pending request before dispatching it. */
    private final boolean tagged;
    private int nextTag = 0;
    private final Deque<Pending> pending = new ArrayDeque<>();
    private volatile boolean closed = false;
    private volatile long lastUsedNanos = System.nanoTime();

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

    // ADR-0019: on a tagged-mode connection every request header carries
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
     * Connection (doc/adr/0016-*.md), not something this SDK introduces.
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
        List<Pending> drained;
        synchronized (this) {
            if (closed) return;
            closed = true;
            drained = new ArrayList<>(pending);
            pending.clear();
        }
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
     * echo (ADR-0019) — meaningless (and never compared) on an untagged
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
            // ADR-0019: the tag is claimed in the same synchronous span
            // that enqueues the pending slot and writes the frame
            // (doc/adr/0016-*.md's enqueue+write atomicity), so tag order
            // can never skew from queue/wire order. Built before
            // enqueueing: a builder that fails (e.g. an invalid TTL) must
            // fail with nothing queued, or the next response would
            // resolve an orphaned slot and desync the stream.
            Integer tag = tagged ? claimTag() : null;
            byte[] frame = build.apply(tag);
            pending.addLast(new Pending(future, tag == null ? -1 : tag));
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

    /** A connection-scoped u32 wrapping counter (ADR-0019), 0-based —
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

    /** This connection's only reader, for its whole lifetime — nothing
     * else may read from {@code in}. Consumes responses off the wire and
     * dispatches each to the oldest pending request (FIFO —
     * doc/adr/0016-*.md). */
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

            // ADR-0019: verify the echoed tag against the request this
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
                // Untagged: `V <len>`. Tagged: `V <len> <tag>` (ADR-0019).
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
            // Busy is always bare (ADR-0019): it's an unsolicited
            // pre-auth response, never an answer to a tagged request.
            case 'B' -> {
                readByte(); // the trailing '\n'
                return new Response(marker, null, -1);
            }
            case 'S', 'D', 'N', 'W' -> {
                if (!tagged) {
                    readByte(); // the trailing '\n'
                    return new Response(marker, null, -1);
                }
                return new Response(marker, null, parseTag(readLine()));
            }
            default -> throw new NanocachedException.ConnectionFailed(
                    "nanocached: unexpected response from server: " + (char) marker, null);
        }
    }

    /** Parses a tag field (ADR-0019) as unsigned decimal text, matching
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

    /** Reads up to (and consuming) the next '\n', returning what preceded it. */
    private String readLine() throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        for (int b = readByte(); b != '\n'; b = readByte()) {
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
