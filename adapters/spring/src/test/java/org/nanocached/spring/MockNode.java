package org.nanocached.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal in-process nanocached node speaking just the slice of the
 * wire protocol the Spring adapter's client traffic produces: the
 * {@code A ... T} handshake, the namespaced {@code g}/{@code s}/{@code
 * d}/{@code c} commands, their legacy default-namespace forms (the
 * SDK's keep-alive probe), the compare-and-set {@code k} (issue #141)
 * {@link NanocachedCache#putIfAbsent} relies on, and {@code F}. A
 * trimmed re-implementation of the SDK's own test double (that one is
 * package-private to the SDK), not a real store: no TTL expiry, no LRU —
 * the adapter tests only assert what reaches the wire and what comes
 * back.
 */
final class MockNode implements AutoCloseable {

    record Entry(byte[] value, long ttlSeconds) {}

    private final ServerSocket server;
    private final Thread acceptLoop;
    /** namespace (UTF-8-ish, raw bytes wrapped) → key → entry. */
    final Map<ByteBuffer, Map<ByteBuffer, Entry>> stores = new ConcurrentHashMap<>();
    final AtomicInteger clearCount = new AtomicInteger();
    final AtomicInteger flushCount = new AtomicInteger();
    final AtomicInteger casSetCount = new AtomicInteger();

    MockNode() throws IOException {
        server = new ServerSocket(0, 16, java.net.InetAddress.getLoopbackAddress());
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
                        reply(out, (accepted ? (tagged ? "OnT\n" : "On\n") : "En\n"));
                        if (!accepted) {
                            return;
                        }
                    }
                    case "G" -> get(in, out, "", Integer.parseInt(parts[1]), tagSuffix);
                    case "g" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        get(in, out, ns(namespace), Integer.parseInt(parts[2]), tagSuffix);
                    }
                    case "S" -> set(in, out, "", parts, 1, tagged, tagSuffix);
                    case "s" -> {
                        // Body order is namespace-first, but the length
                        // headers put the namespace length first too, so
                        // read it before delegating for the key/value.
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        set(in, out, ns(namespace), parts, 2, tagged, tagSuffix);
                    }
                    case "D" -> delete(in, out, "", Integer.parseInt(parts[1]), tagSuffix);
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
                    case "F" -> {
                        stores.clear();
                        flushCount.incrementAndGet();
                        reply(out, "C" + tagSuffix + "\n");
                    }
                    case "k" -> casSet(in, out, parts, tagged, tagSuffix);
                    default -> throw new IOException("unexpected command " + parts[0]);
                }
            }
        } catch (IOException done) {
            // connection closed by the client (or test teardown)
        }
    }

    private void get(InputStream in, OutputStream out, String namespace, int keyLength,
            String tagSuffix) throws IOException {
        byte[] key = in.readNBytes(keyLength);
        Entry entry = entry(namespace, key);
        if (entry == null) {
            reply(out, "N" + tagSuffix + "\n");
            return;
        }
        out.write(("V " + entry.value().length + tagSuffix + "\n")
                .getBytes(StandardCharsets.US_ASCII));
        out.write(entry.value());
        out.flush();
    }

    private void set(InputStream in, OutputStream out, String namespace, String[] parts,
            int firstLengthIndex, boolean tagged, String tagSuffix) throws IOException {
        int keyLength = Integer.parseInt(parts[firstLengthIndex]);
        int valueLength = Integer.parseInt(parts[firstLengthIndex + 1]);
        // Remaining numeric fields: [ttl] in untagged mode, [ttl] tag in
        // tagged mode — the tag is always last.
        int remaining = parts.length - (firstLengthIndex + 2) - (tagged ? 1 : 0);
        long ttlSeconds = remaining > 0 ? Long.parseLong(parts[firstLengthIndex + 2]) : 0;
        byte[] key = in.readNBytes(keyLength);
        byte[] value = in.readNBytes(valueLength);
        store(namespace).put(ByteBuffer.wrap(key), new Entry(value, ttlSeconds));
        reply(out, "S" + tagSuffix + "\n");
    }

    private void delete(InputStream in, OutputStream out, String namespace, int keyLength,
            String tagSuffix) throws IOException {
        byte[] key = in.readNBytes(keyLength);
        boolean existed = store(namespace).remove(ByteBuffer.wrap(key)) != null;
        reply(out, (existed ? "D" : "N") + tagSuffix + "\n");
    }

    /** {@code k <ns-len> <key-len> <val-len> <cond> [ttl] [tag]},
     * docs/protocol.html "k / x" — only {@code <cond> == "A"} (add if
     * absent) is implemented, the only condition {@link
     * NanocachedCache#putIfAbsent} ever sends. The store's per-namespace
     * map is the lock: check-then-put is synchronized on it so two
     * connections racing on the same key really do serialize the way the
     * real server's CAS does, not just "usually" thanks to test timing. */
    private void casSet(InputStream in, OutputStream out, String[] parts, boolean tagged, String tagSuffix)
            throws IOException {
        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
        int keyLength = Integer.parseInt(parts[2]);
        int valueLength = Integer.parseInt(parts[3]);
        String cond = parts[4];
        byte[] key = in.readNBytes(keyLength);
        byte[] value = in.readNBytes(valueLength);
        casSetCount.incrementAndGet();

        if (!"A".equals(cond)) {
            throw new IOException("MockNode: only cond \"A\" is implemented, got " + cond);
        }
        Map<ByteBuffer, Entry> map = store(ns(namespace));
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        boolean stored;
        synchronized (map) {
            stored = !map.containsKey(keyBuffer);
            if (stored) {
                int ttlFieldCount = parts.length - 5 - (tagged ? 1 : 0);
                long ttlSeconds = ttlFieldCount > 0 ? Long.parseLong(parts[5]) : 0;
                map.put(keyBuffer, new Entry(value, ttlSeconds));
            }
        }
        reply(out, (stored ? "S" : "N") + tagSuffix + "\n");
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
