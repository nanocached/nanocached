package org.nanocached.jcache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal in-process nanocached node speaking just the slice of the
 * wire protocol this adapter's traffic produces: the {@code A ... T}
 * handshake (the SDK always negotiates tagged mode), namespaced {@code
 * g}/{@code s}/{@code d}/{@code c}, and the compare-and-set {@code k}/
 * {@code x} (issue #141) this adapter's {@code putIfAbsent}/{@code
 * replace}/{@code deleteIfMatches}/{@code getWithToken} rely on, and the
 * batched {@code m} (issue #152) this adapter's {@code getAll} uses for
 * its bulk-safe keys. A
 * trimmed, independent reimplementation — see the adapters' shared
 * "duplicate per module rather than share a test double" convention
 * (e.g. {@code nanocached-spring}'s own {@code MockNode}).
 */
final class MockNode implements AutoCloseable {

    record Entry(byte[] value, long ttlSeconds) {}

    private final ServerSocket server;
    private final Thread acceptLoop;
    final Map<ByteBuffer, Map<ByteBuffer, Entry>> stores = new ConcurrentHashMap<>();
    final AtomicInteger clearCount = new AtomicInteger();
    final AtomicInteger casSetCount = new AtomicInteger();
    final AtomicInteger casDeleteCount = new AtomicInteger();
    final AtomicInteger multiGetCount = new AtomicInteger();
    final AtomicInteger multiSetCount = new AtomicInteger();
    /** When set, the next {@code k}/{@code x} request answers a
     * mismatch ({@code N}) regardless of whether the condition actually
     * holds — a one-shot fault, cleared on use. Exists purely to prove
     * the adapter's {@code getAnd*} CAS retry loops actually retry
     * instead of only ever exercising the happy path. */
    final AtomicBoolean forceCasMismatchOnce = new AtomicBoolean(false);

    MockNode() throws IOException {
        server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
        acceptLoop = new Thread(() -> {
            while (true) {
                try {
                    Socket socket = server.accept();
                    Thread serve = new Thread(() -> serve(socket));
                    serve.setDaemon(true);
                    serve.start();
                } catch (IOException stop) {
                    return;
                }
            }
        });
        acceptLoop.setDaemon(true);
        acceptLoop.start();
    }

    int port() {
        return server.getLocalPort();
    }

    String address() {
        return "127.0.0.1:" + port();
    }

    Map<ByteBuffer, Entry> store(String namespace) {
        return stores.computeIfAbsent(
                ByteBuffer.wrap(namespace.getBytes(StandardCharsets.UTF_8)),
                unused -> new ConcurrentHashMap<>());
    }

    Entry entry(String namespace, byte[] key) {
        return store(namespace).get(ByteBuffer.wrap(key));
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    private void serve(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            boolean tagged = false;
            while (true) {
                String[] parts = readLine(in).split(" ");
                String tagSuffix = tagged ? " " + parts[parts.length - 1] : "";
                switch (parts[0]) {
                    case "A" -> {
                        byte[] secret = in.readNBytes(Integer.parseInt(parts[1]));
                        boolean accepted = secret.length > 0;
                        tagged = accepted && parts.length > 2 && parts[2].equals("T");
                        reply(out, accepted ? (tagged ? "OnT\n" : "On\n") : "En\n");
                        if (!accepted) {
                            return;
                        }
                    }
                    case "g" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        get(in, out, ns(namespace), Integer.parseInt(parts[2]), tagSuffix);
                    }
                    case "s" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        set(in, out, ns(namespace), parts, tagged, tagSuffix);
                    }
                    case "d" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        delete(in, out, ns(namespace), Integer.parseInt(parts[2]), tagSuffix);
                    }
                    case "c" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        stores.remove(ByteBuffer.wrap(namespace));
                        clearCount.incrementAndGet();
                        reply(out, "C" + tagSuffix + "\n");
                    }
                    case "k" -> casSet(in, out, parts, tagged, tagSuffix);
                    case "x" -> casDelete(in, out, parts, tagSuffix);
                    case "m" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        multiGet(in, out, ns(namespace), parts, tagSuffix);
                    }
                    case "o" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        multiSet(in, out, ns(namespace), parts, tagged, tagSuffix);
                    }
                    default -> throw new IOException("unexpected command " + parts[0]);
                }
            }
        } catch (IOException done) {
            // connection closed by the client (or test teardown)
        }
    }

    private void get(InputStream in, OutputStream out, String namespace, int keyLength, String tagSuffix)
            throws IOException {
        byte[] key = in.readNBytes(keyLength);
        Entry entry = entry(namespace, key);
        if (entry == null) {
            reply(out, "N" + tagSuffix + "\n");
            return;
        }
        out.write(("V " + entry.value().length + tagSuffix + "\n").getBytes(StandardCharsets.US_ASCII));
        out.write(entry.value());
        out.flush();
    }

    private void set(
            InputStream in, OutputStream out, String namespace, String[] parts, boolean tagged, String tagSuffix)
            throws IOException {
        int keyLength = Integer.parseInt(parts[2]);
        int valueLength = Integer.parseInt(parts[3]);
        int remaining = parts.length - 4 - (tagged ? 1 : 0);
        long ttlSeconds = remaining > 0 ? Long.parseLong(parts[4]) : 0;
        byte[] key = in.readNBytes(keyLength);
        byte[] value = in.readNBytes(valueLength);
        store(namespace).put(ByteBuffer.wrap(key), new Entry(value, ttlSeconds));
        reply(out, "S" + tagSuffix + "\n");
    }

    private void delete(InputStream in, OutputStream out, String namespace, int keyLength, String tagSuffix)
            throws IOException {
        byte[] key = in.readNBytes(keyLength);
        boolean existed = store(namespace).remove(ByteBuffer.wrap(key)) != null;
        reply(out, (existed ? "D" : "N") + tagSuffix + "\n");
    }

    /** {@code m <ns-len> <n> <key-len-1> ... <key-len-n> [tag]}, docs/protocol.html
     * "m / o". Reply: {@code M <n> <result-1> ... <result-n> [tag]\n<hit values>},
     * each result a decimal byte length (hit) or {@code -} (miss) — no {@code W}
     * from a single-node mock. */
    private void multiGet(InputStream in, OutputStream out, String namespace, String[] parts, String tagSuffix)
            throws IOException {
        int n = Integer.parseInt(parts[2]);
        int[] keyLengths = new int[n];
        for (int i = 0; i < n; i++) {
            keyLengths[i] = Integer.parseInt(parts[3 + i]);
        }
        multiGetCount.incrementAndGet();
        byte[][] values = new byte[n][];
        for (int i = 0; i < n; i++) {
            byte[] key = in.readNBytes(keyLengths[i]);
            Entry entry = entry(namespace, key);
            values[i] = entry == null ? null : entry.value();
        }
        StringBuilder header = new StringBuilder("M ").append(n);
        for (byte[] value : values) {
            header.append(' ').append(value == null ? "-" : String.valueOf(value.length));
        }
        header.append(tagSuffix).append('\n');
        out.write(header.toString().getBytes(StandardCharsets.US_ASCII));
        for (byte[] value : values) {
            if (value != null) {
                out.write(value);
            }
        }
        out.flush();
    }

    /** {@code o <ns-len> <n> <key-len-1> <val-len-1> ... <key-len-n> <val-len-n>
     * [ttl] [tag]}, docs/protocol.html "m / o" — one shared TTL for the whole
     * batch, omitted from the wire when 0. Reply: {@code O <n> <result-1> ...
     * <result-n> [tag]\n}, every result {@code S} — no {@code W} from a
     * single-node mock. */
    private void multiSet(
            InputStream in, OutputStream out, String namespace, String[] parts, boolean tagged, String tagSuffix)
            throws IOException {
        int n = Integer.parseInt(parts[2]);
        int[] keyLengths = new int[n];
        int[] valueLengths = new int[n];
        for (int i = 0; i < n; i++) {
            keyLengths[i] = Integer.parseInt(parts[3 + 2 * i]);
            valueLengths[i] = Integer.parseInt(parts[4 + 2 * i]);
        }
        int trailing = parts.length - (3 + 2 * n) - (tagged ? 1 : 0);
        long ttlSeconds = trailing > 0 ? Long.parseLong(parts[3 + 2 * n]) : 0;
        multiSetCount.incrementAndGet();
        for (int i = 0; i < n; i++) {
            byte[] key = in.readNBytes(keyLengths[i]);
            byte[] value = in.readNBytes(valueLengths[i]);
            store(namespace).put(ByteBuffer.wrap(key), new Entry(value, ttlSeconds));
        }
        StringBuilder header = new StringBuilder("O ").append(n);
        for (int i = 0; i < n; i++) {
            header.append(" S");
        }
        header.append(tagSuffix).append('\n');
        out.write(header.toString().getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    /** {@code k <ns-len> <key-len> <val-len> <cond> [ttl] [tag]}. {@code
     * <cond>} always sits right after the three lengths; the optional
     * {@code <ttl>} then the tag (on a tagged connection) follow, mirroring
     * {@code s}'s own trailing-field layout. */
    private void casSet(InputStream in, OutputStream out, String[] parts, boolean tagged, String tagSuffix)
            throws IOException {
        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
        int keyLength = Integer.parseInt(parts[2]);
        int valueLength = Integer.parseInt(parts[3]);
        String cond = parts[4];
        byte[] key = in.readNBytes(keyLength);
        byte[] value = in.readNBytes(valueLength);
        casSetCount.incrementAndGet();

        Entry existing = entry(ns(namespace), key);
        boolean matches = !forceCasMismatchOnce.compareAndSet(true, false)
                && switch (cond) {
                    case "A" -> existing == null;
                    case "P" -> existing != null;
                    default -> existing != null && digestOf(existing.value()).equals(cond);
                };
        if (matches) {
            int ttlFieldCount = parts.length - 5 - (tagged ? 1 : 0);
            long ttlSeconds = ttlFieldCount > 0 ? Long.parseLong(parts[5]) : 0;
            store(ns(namespace)).put(ByteBuffer.wrap(key), new Entry(value, ttlSeconds));
            reply(out, "S" + tagSuffix + "\n");
        } else {
            reply(out, "N" + tagSuffix + "\n");
        }
    }

    /** {@code x <ns-len> <key-len> <cond> [tag]}. */
    private void casDelete(InputStream in, OutputStream out, String[] parts, String tagSuffix) throws IOException {
        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
        int keyLength = Integer.parseInt(parts[2]);
        String cond = parts[3];
        byte[] key = in.readNBytes(keyLength);
        casDeleteCount.incrementAndGet();

        Entry existing = entry(ns(namespace), key);
        boolean matches = !forceCasMismatchOnce.compareAndSet(true, false)
                && existing != null
                && digestOf(existing.value()).equals(cond);
        if (matches) {
            store(ns(namespace)).remove(ByteBuffer.wrap(key));
            reply(out, "D" + tagSuffix + "\n");
        } else {
            reply(out, "N" + tagSuffix + "\n");
        }
    }

    /** Same algorithm as {@code NanocachedClient.contentDigest} —
     * SHA-256 truncated to 16 bytes, lowercase hex — reimplemented
     * independently so a bug in one isn't masked by the other. */
    private static String digestOf(byte[] value) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] hash = sha256.digest(value);
            StringBuilder hex = new StringBuilder(32);
            for (int i = 0; i < 16; i++) {
                hex.append(String.format("%02x", hash[i]));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String ns(byte[] namespace) {
        return new String(namespace, StandardCharsets.UTF_8);
    }

    private void reply(OutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("connection closed");
            }
            if (b == '\n') {
                return line.toString();
            }
            line.append((char) b);
        }
    }
}
