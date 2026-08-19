package org.nanocached;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-process stand-ins for nanocached-node and nanocached-discovery,
 * speaking just enough of the wire protocol for client tests to run over
 * real TCP without the Rust binaries. Mirrors the TypeScript/Python mocks.
 */
final class MockServers {

    private MockServers() {}

    static final class MockNode implements AutoCloseable {
        final Map<String, byte[]> store = new ConcurrentHashMap<>();
        final AtomicInteger connectionCount = new AtomicInteger();
        final AtomicInteger getCount = new AtomicInteger();
        private final AtomicInteger wrongNodeReplies = new AtomicInteger();
        private final AtomicInteger malformedValueReplies = new AtomicInteger();
        private final AtomicInteger storedToGetReplies = new AtomicInteger();
        private volatile long setDelayMillis = 0;
        private final byte[] requiredSecret;
        private final ServerSocket server;
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();

        MockNode() throws IOException {
            this(null);
        }

        MockNode(byte[] requiredSecret) throws IOException {
            this.requiredSecret = requiredSecret;
            this.server = new ServerSocket(0);
            Thread acceptor = new Thread(this::acceptLoop, "mock-node-accept");
            acceptor.setDaemon(true);
            acceptor.start();
            threads.add(acceptor);
        }

        int port() {
            return server.getLocalPort();
        }

        String address() {
            return "127.0.0.1:" + port();
        }

        void answerWrongNodeOnce() {
            wrongNodeReplies.incrementAndGet();
        }

        /** Queue a one-off garbage `V` header for the next G request. */
        void answerMalformedValueOnce() {
            malformedValueReplies.incrementAndGet();
        }

        /** Reply {@code S} to the next G — a well-formed frame of the
         * wrong kind, as a desynced (off-by-one) stream would produce. */
        void answerStoredToGetOnce() {
            storedToGetReplies.incrementAndGet();
        }

        /** Holds every future S reply for {@code millis} first — for tests
         * proving a caller isn't blocked on a slow replica leg
         * (doc/adr/0014-*.md). */
        void delaySets(long millis) {
            setDelayMillis = millis;
        }

        /** Server-side FIN on every open connection, like the idle timeout. */
        void dropConnections() {
            for (Socket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Best-effort.
                }
            }
        }

        @Override
        public void close() throws IOException {
            dropConnections();
            server.close();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    connectionCount.incrementAndGet();
                    sockets.add(socket);
                    Thread worker = new Thread(() -> serve(socket), "mock-node-conn");
                    worker.setDaemon(true);
                    worker.start();
                    threads.add(worker);
                } catch (IOException stop) {
                    return;
                }
            }
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
                            boolean accepted = requiredSecret == null
                                    ? secret.length > 0
                                    : java.util.Arrays.equals(secret, requiredSecret);
                            out.write((accepted ? "On\n" : "En\n").getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            if (!accepted) return;
                        }
                        case "G" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            getCount.incrementAndGet();
                            if (malformedValueReplies.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                out.write("V x\n".getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (storedToGetReplies.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                out.write("S\n".getBytes(StandardCharsets.US_ASCII));
                                out.flush();
                                break;
                            }
                            if (takeWrongNode()) {
                                out.write("W\n".getBytes(StandardCharsets.US_ASCII));
                            } else {
                                byte[] value = store.get(key);
                                if (value == null) {
                                    out.write("N\n".getBytes(StandardCharsets.US_ASCII));
                                } else {
                                    out.write(("V " + value.length + "\n").getBytes(StandardCharsets.US_ASCII));
                                    out.write(value);
                                }
                            }
                            out.flush();
                        }
                        case "S" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            byte[] value = in.readNBytes(Integer.parseInt(parts[2]));
                            if (setDelayMillis > 0) {
                                try {
                                    Thread.sleep(setDelayMillis);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            if (takeWrongNode()) {
                                out.write("W\n".getBytes(StandardCharsets.US_ASCII));
                            } else {
                                store.put(key, value);
                                out.write("S\n".getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        case "D" -> {
                            String key = keyOf(in.readNBytes(Integer.parseInt(parts[1])));
                            if (takeWrongNode()) {
                                out.write("W\n".getBytes(StandardCharsets.US_ASCII));
                            } else {
                                out.write((store.remove(key) != null ? "D\n" : "N\n")
                                        .getBytes(StandardCharsets.US_ASCII));
                            }
                            out.flush();
                        }
                        default -> {
                            return;
                        }
                    }
                }
            } catch (IOException | RuntimeException done) {
                // Connection closed — normal end of a mock session.
            } finally {
                sockets.remove(socket);
            }
        }

        private boolean takeWrongNode() {
            while (true) {
                int pending = wrongNodeReplies.get();
                if (pending == 0) return false;
                if (wrongNodeReplies.compareAndSet(pending, pending - 1)) return true;
            }
        }

        static String keyOf(byte[] key) {
            return new String(key, StandardCharsets.ISO_8859_1);
        }
    }

    static final class MockDiscovery implements AutoCloseable {
        volatile List<DiscoveredNode> nodes;
        volatile boolean warmingUp = false;
        final int replication;
        private final ServerSocket server;

        MockDiscovery(List<DiscoveredNode> nodes, int replication) throws IOException {
            this.nodes = nodes;
            this.replication = replication;
            this.server = new ServerSocket(0);
            Thread acceptor = new Thread(this::acceptLoop, "mock-discovery-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return server.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            server.close();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread worker = new Thread(() -> serve(socket), "mock-discovery-conn");
                    worker.setDaemon(true);
                    worker.start();
                } catch (IOException stop) {
                    return;
                }
            }
        }

        private void serve(Socket socket) {
            try (socket) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                while (true) {
                    String[] parts = readLine(in).split(" ");
                    if (parts[0].equals("A")) {
                        in.readNBytes(Integer.parseInt(parts[1]));
                        out.write("Od\n".getBytes(StandardCharsets.US_ASCII));
                        out.flush();
                    } else if (parts[0].equals("L")) {
                        if (warmingUp) {
                            out.write("B\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            return;
                        }
                        List<DiscoveredNode> snapshot = nodes;
                        StringBuilder frame = new StringBuilder(
                                "N " + snapshot.size() + " " + replication + "\n");
                        for (DiscoveredNode node : snapshot) {
                            frame.append(node.name().length()).append(' ')
                                    .append(node.address().length()).append('\n')
                                    .append(node.name()).append(node.address()).append('\n');
                        }
                        out.write(frame.toString().getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    } else {
                        return;
                    }
                }
            } catch (IOException | RuntimeException done) {
                // Connection closed — normal end of a mock session.
            }
        }
    }

    static int unusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b == -1) throw new IOException("closed");
            if (b == '\n') return line.toString(StandardCharsets.US_ASCII);
            line.write(b);
        }
    }
}
