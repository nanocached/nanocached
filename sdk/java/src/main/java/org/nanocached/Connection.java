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
    private final Deque<CompletableFuture<Response>> pending = new ArrayDeque<>();
    private volatile boolean closed = false;
    private volatile long lastUsedNanos = System.nanoTime();

    /** {@code onClose} fires exactly once, the first time this connection
     * closes for any reason — used by {@link NanocachedClient} to keep its
     * forgotten-close open-sockets tracker accurate without every call
     * site remembering to decrement it by hand. */
    Connection(Socket socket, Runnable onClose) throws IOException {
        this.socket = socket;
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
        byte[] frame = frame("G " + key.length + "\n", key, null);
        Response response = request(frame);
        return switch (response.marker) {
            case 'V' -> response.value;
            case 'N' -> null;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
    }

    void set(byte[] key, byte[] value, Long ttlSeconds) {
        String header = ttlSeconds == null
                ? "S " + key.length + " " + value.length + "\n"
                : "S " + key.length + " " + value.length + " " + ttlSeconds + "\n";
        Response response = request(frame(header, key, value));
        if (response.marker == 'W') throw new NanocachedException.WrongNode();
        if (response.marker != 'S') throw mismatch(response.marker);
    }

    boolean delete(byte[] key) {
        Response response = request(frame("D " + key.length + "\n", key, null));
        return switch (response.marker) {
            case 'D' -> true;
            case 'N' -> false;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw mismatch(response.marker);
        };
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
        List<CompletableFuture<Response>> drained;
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
        for (CompletableFuture<Response> future : drained) {
            future.completeExceptionally(error);
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

    private record Response(int marker, byte[] value) {}

    /** Enqueues a pending slot and writes frame under one monitor — see
     * the class doc comment — then blocks this caller's own thread on
     * its own future, not the socket. */
    private Response request(byte[] frame) {
        if (isClosed()) {
            throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null);
        }

        CompletableFuture<Response> future = new CompletableFuture<>();
        synchronized (this) {
            if (isClosed()) {
                throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null);
            }
            lastUsedNanos = System.nanoTime();
            pending.addLast(future);
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

    // The server never stores values above its 1 MiB request limit, so a
    // claimed length beyond this is a corrupt or malicious frame.
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

            CompletableFuture<Response> future;
            boolean wasEmpty;
            synchronized (this) {
                wasEmpty = pending.isEmpty();
                future = wasEmpty ? null : pending.pollFirst();
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
            if (future == null) {
                poison(new NanocachedException.ConnectionFailed(
                        "nanocached: unsolicited response '" + (char) response.marker
                                + "' from server (connection desynced)",
                        null));
                return;
            }
            future.complete(response);
        }
    }

    private Response readResponse() throws IOException {
        int marker = readByte();
        switch (marker) {
            case 'V' -> {
                // A non-numeric, negative, or absurd length is protocol
                // garbage: the connection is desynced mid-frame and must
                // be poisoned, and the error must be connection-classified
                // so the redial/retry layer handles it (issue #8).
                int length;
                try {
                    length = Integer.parseInt(readLine());
                } catch (NumberFormatException malformed) {
                    length = -1;
                }
                if (length < 0 || length > MAX_VALUE_LENGTH) {
                    throw new NanocachedException.ConnectionFailed(
                            "nanocached: invalid value length in response", null);
                }
                return new Response(marker, readExactly(length));
            }
            case 'S', 'D', 'N', 'W', 'B' -> {
                readByte(); // the trailing '\n'
                return new Response(marker, null);
            }
            default -> throw new NanocachedException.ConnectionFailed(
                    "nanocached: unexpected response from server: " + (char) marker, null);
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
