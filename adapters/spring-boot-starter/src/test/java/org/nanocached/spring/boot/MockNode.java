package org.nanocached.spring.boot;

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
 * wire protocol the starter's client traffic produces: the {@code A ...}
 * handshake and the namespaced {@code g}/{@code s}/{@code d}/{@code c}
 * commands. A trimmed re-implementation of {@code
 * nanocached-spring}'s own test double (package-private to that module,
 * see the shared adapters spec) built without response tags — this
 * module's tests never configure anything that would request them.
 */
final class MockNode implements AutoCloseable {

    record Entry(byte[] value, long ttlSeconds) {}

    private final ServerSocket server;
    private final Thread acceptLoop;
    /** namespace (UTF-8-ish, raw bytes wrapped) → key → entry. */
    final Map<ByteBuffer, Map<ByteBuffer, Entry>> stores = new ConcurrentHashMap<>();
    final AtomicInteger clearCount = new AtomicInteger();

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
            while (true) {
                String[] parts = readLine(in).split(" ");
                switch (parts[0]) {
                    case "A" -> {
                        byte[] secret = in.readNBytes(Integer.parseInt(parts[1]));
                        boolean accepted = secret.length > 0;
                        reply(out, accepted ? "On\n" : "En\n");
                        if (!accepted) {
                            return;
                        }
                    }
                    case "g" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        get(in, out, ns(namespace), Integer.parseInt(parts[2]));
                    }
                    case "s" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        set(in, out, ns(namespace), parts);
                    }
                    case "d" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        delete(in, out, ns(namespace), Integer.parseInt(parts[2]));
                    }
                    case "c" -> {
                        byte[] namespace = in.readNBytes(Integer.parseInt(parts[1]));
                        stores.remove(ByteBuffer.wrap(namespace));
                        clearCount.incrementAndGet();
                        reply(out, "C\n");
                    }
                    default -> throw new IOException("unexpected command " + parts[0]);
                }
            }
        } catch (IOException done) {
            // connection closed by the client (or test teardown)
        }
    }

    private void get(InputStream in, OutputStream out, String namespace, int keyLength)
            throws IOException {
        byte[] key = in.readNBytes(keyLength);
        Entry entry = entry(namespace, key);
        if (entry == null) {
            reply(out, "N\n");
            return;
        }
        out.write(("V " + entry.value().length + "\n").getBytes(StandardCharsets.US_ASCII));
        out.write(entry.value());
        out.flush();
    }

    private void set(InputStream in, OutputStream out, String namespace, String[] parts)
            throws IOException {
        int keyLength = Integer.parseInt(parts[2]);
        int valueLength = Integer.parseInt(parts[3]);
        long ttlSeconds = parts.length > 4 ? Long.parseLong(parts[4]) : 0;
        byte[] key = in.readNBytes(keyLength);
        byte[] value = in.readNBytes(valueLength);
        store(namespace).put(ByteBuffer.wrap(key), new Entry(value, ttlSeconds));
        reply(out, "S\n");
    }

    private void delete(InputStream in, OutputStream out, String namespace, int keyLength)
            throws IOException {
        byte[] key = in.readNBytes(keyLength);
        boolean existed = store(namespace).remove(ByteBuffer.wrap(key)) != null;
        reply(out, (existed ? "D" : "N") + "\n");
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
