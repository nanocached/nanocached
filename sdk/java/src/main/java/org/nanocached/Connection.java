package org.nanocached;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * One already-identified connection to a single nanocached-node, speaking
 * the cache protocol ({@code G}/{@code S}/{@code D} — the {@code A}
 * identify exchange happens in {@link Identify} before a Connection
 * exists). Requests are serialized (synchronized) per connection — a
 * deliberate v1 simplification over the TypeScript SDK's pipelining:
 * nanocached-node answers in arrival order, so serializing is always
 * correct, just less concurrent. Concurrent callers queue on the monitor.
 */
final class Connection {
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private volatile boolean closed = false;
    private volatile long lastUsedNanos = System.nanoTime();

    Connection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    boolean isClosed() {
        return closed || socket.isClosed();
    }

    long idleNanos() {
        return System.nanoTime() - lastUsedNanos;
    }

    void close() {
        closed = true;
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing an already-broken socket is fine.
        }
    }

    byte[] get(byte[] key) {
        byte[] frame = frame("G " + key.length + "\n", key, null);
        Response response = request(frame);
        return switch (response.marker) {
            case 'V' -> response.value;
            case 'N' -> null;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw unexpected(response.marker);
        };
    }

    void set(byte[] key, byte[] value, Long ttlSeconds) {
        String header = ttlSeconds == null
                ? "S " + key.length + " " + value.length + "\n"
                : "S " + key.length + " " + value.length + " " + ttlSeconds + "\n";
        Response response = request(frame(header, key, value));
        if (response.marker == 'W') throw new NanocachedException.WrongNode();
        if (response.marker != 'S') throw unexpected(response.marker);
    }

    boolean delete(byte[] key) {
        Response response = request(frame("D " + key.length + "\n", key, null));
        return switch (response.marker) {
            case 'D' -> true;
            case 'N' -> false;
            case 'W' -> throw new NanocachedException.WrongNode();
            default -> throw unexpected(response.marker);
        };
    }

    private static NanocachedException unexpected(int marker) {
        return new NanocachedException("nanocached: unexpected response from server: " + (char) marker);
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

    private synchronized Response request(byte[] frame) {
        if (isClosed()) {
            throw new NanocachedException.ConnectionFailed("nanocached: connection is closed", null);
        }

        lastUsedNanos = System.nanoTime();
        try {
            out.write(frame);
            out.flush();
            return readResponse();
        } catch (IOException error) {
            // The stream state after a failed round trip is unknown —
            // poison the connection so the client redials lazily.
            close();
            throw new NanocachedException.ConnectionFailed(
                    "nanocached: connection failed: " + error.getMessage(), error);
        }
    }

    private Response readResponse() throws IOException {
        int marker = readByte();
        switch (marker) {
            case 'V' -> {
                int length = Integer.parseInt(readLine());
                return new Response(marker, readExactly(length));
            }
            case 'S', 'D', 'N', 'W' -> {
                readByte(); // the trailing '\n'
                return new Response(marker, null);
            }
            case 'B' -> {
                // Unsolicited busy: connection-limit rejection, server closing.
                close();
                throw new NanocachedException.ConnectionFailed(
                        "nanocached: server rejected the connection (connection limit reached)", null);
            }
            default -> {
                close();
                throw unexpected(marker);
            }
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
