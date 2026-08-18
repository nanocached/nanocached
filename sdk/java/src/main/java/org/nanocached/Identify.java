package org.nanocached;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

/**
 * Connect-and-identify: dials {@code host:port}, authenticates, and
 * figures out from the server's own {@code A} response whether it reached
 * a cache node ({@code On}) or a discovery server ({@code Od}) — the
 * caller never says which it expects (doc/adr/0007-*.md). A node's socket
 * is handed back live; a discovery connection is used once for {@code L}
 * and closed, returning the name/address list and the cluster's
 * replication factor R (doc/adr/0009, 0010, 0011).
 */
final class Identify {
    // A server with no secret accepts any non-empty secret; one that
    // requires a real secret correctly rejects this placeholder.
    private static final byte[] NO_SECRET_PLACEHOLDER = {0};

    sealed interface Result permits NodeTarget, ClusterTarget {}

    record NodeTarget(Socket socket) implements Result {}

    record ClusterTarget(List<DiscoveredNode> nodes, int replication) implements Result {}

    private Identify() {}

    static Result connectAndIdentify(String host, int port, byte[] authSecret, SSLContext tls)
            throws IOException {
        Socket socket = open(host, port, tls);
        try {
            byte[] secret = authSecret != null ? authSecret : NO_SECRET_PLACEHOLDER;
            OutputStream out = socket.getOutputStream();
            out.write(("A " + secret.length + "\n").getBytes(StandardCharsets.US_ASCII));
            out.write(secret);
            out.flush();

            InputStream in = socket.getInputStream();
            byte[] ack = readExactly(in, 3);
            if (ack[2] != '\n' || (ack[0] != 'O' && ack[0] != 'E') || (ack[1] != 'n' && ack[1] != 'd')) {
                throw new NanocachedException("nanocached: unexpected response to A");
            }
            if (ack[0] == 'E') {
                if (authSecret == null) {
                    throw new NanocachedException("nanocached: " + host + ":" + port
                            + " requires authentication, but no authSecret was given");
                }
                throw new NanocachedException("nanocached: authentication failed");
            }

            if (ack[1] == 'n') {
                return new NodeTarget(socket);
            }

            // A discovery server: one-shot L, then this connection is done.
            out.write("L\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            ClusterTarget cluster = readNodeList(in);
            socket.close();
            return cluster;
        } catch (IOException | RuntimeException error) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The original failure is the interesting one.
            }
            throw error;
        }
    }

    private static final int CONNECT_TIMEOUT_MS = 10_000;

    private static Socket open(String host, int port, SSLContext tls) throws IOException {
        // Both paths bound the TCP connect (issue #11): the TLS factory's
        // own connect(host, port) has no timeout, so an unresponsive
        // (packet-dropping) address would hang connect()/refresh forever
        // instead of failing over. Layer SSL over a pre-connected,
        // timeout-bounded plain socket instead, and bound the handshake
        // with a read timeout.
        Socket plain = new Socket();
        try {
            plain.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            plain.setTcpNoDelay(true);
            if (tls == null) {
                return plain;
            }
            SSLSocket socket =
                    (SSLSocket) tls.getSocketFactory().createSocket(plain, host, port, true);
            socket.setSoTimeout(CONNECT_TIMEOUT_MS);
            socket.startHandshake();
            socket.setSoTimeout(0);
            return socket;
        } catch (IOException error) {
            plain.close();
            throw error;
        }
    }

    private static ClusterTarget readNodeList(InputStream in) throws IOException {
        String header = readLine(in);
        if (header.startsWith("B")) {
            throw new NanocachedException.DiscoveryBusy();
        }
        if (!header.startsWith("N ")) {
            throw new NanocachedException(
                    "nanocached: unexpected response from discovery server: " + header);
        }

        // `N <count> <r>\n` (ADR-0011) — the replication factor rides along.
        String[] fields = header.substring(2).split(" ");
        if (fields.length != 2) {
            throw new NanocachedException("nanocached: invalid node-list header in discovery response");
        }
        int count = Integer.parseInt(fields[0]);
        int replication = Integer.parseInt(fields[1]);
        if (replication < 1) {
            throw new NanocachedException("nanocached: invalid replication factor in discovery response");
        }

        List<DiscoveredNode> nodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String[] lengths = readLine(in).split(" ");
            if (lengths.length != 2) {
                throw new NanocachedException("nanocached: invalid node entry header in discovery response");
            }
            int nameLength = Integer.parseInt(lengths[0]);
            int addrLength = Integer.parseInt(lengths[1]);

            byte[] body = readExactly(in, nameLength + addrLength + 1); // +1: trailing '\n'
            if (body[body.length - 1] != '\n') {
                throw new NanocachedException("nanocached: malformed node entry in discovery response");
            }
            nodes.add(new DiscoveredNode(
                    new String(body, 0, nameLength, StandardCharsets.UTF_8),
                    new String(body, nameLength, addrLength, StandardCharsets.UTF_8)));
        }
        return new ClusterTarget(nodes, replication);
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        for (int b = read(in); b != '\n'; b = read(in)) {
            line.append((char) b);
        }
        return line.toString();
    }

    private static int read(InputStream in) throws IOException {
        int value = in.read();
        if (value == -1) throw new IOException("connection closed by the server");
        return value;
    }

    private static byte[] readExactly(InputStream in, int length) throws IOException {
        byte[] data = in.readNBytes(length);
        if (data.length != length) throw new IOException("connection closed mid-frame");
        return data;
    }
}
