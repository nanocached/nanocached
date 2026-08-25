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
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * Connect-and-identify: dials {@code host:port}, authenticates, and
 * figures out from the server's own {@code A} response whether it reached
 * a cache node ({@code On}) or a discovery server ({@code Od}) — the
 * caller never says which it expects (the server type in the auth response). A node's socket
 * is handed back live; a discovery connection is used once for {@code L}
 * and closed, returning the name/address list and the cluster's
 * replication factor R (node identity, discovery HA, replication).
 */
final class Identify {
    // A server with no secret accepts any non-empty secret; one that
    // requires a real secret correctly rejects this placeholder.
    private static final byte[] NO_SECRET_PLACEHOLDER = {0};

    sealed interface Result permits NodeTarget, ClusterTarget, ProxyRosterTarget {}

    // `tagged` (echoed response tags): the node accepted the extended `A ... T`, so
    // this socket's G/S/D traffic must carry tags and its responses echo
    // them; false means an older node answered the plain-`A` fallback.
    record NodeTarget(Socket socket, boolean tagged) implements Result {}

    record ClusterTarget(List<DiscoveredNode> nodes, int replication) implements Result {}

    /** SDK proxy mode (issue #122, {@code viaProxy}): the roster a
     * discovery server answers `Q` with — the same name/address shape as
     * {@link ClusterTarget#nodes()}, reusing {@link DiscoveredNode} even
     * though a proxy's "name" carries no routing meaning to this client
     * (unlike a cache node's, it's never hashed) — there is simply no
     * separate replication field on this response for a proxy client to
     * need (a proxy owns every key). */
    record ProxyRosterTarget(List<DiscoveredNode> proxies) implements Result {}

    private Identify() {}

    /** Thrown only when writing/reading the extended {@code A ... T}
     * exchange fails in a way that looks like a legacy pre-tag server
     * slamming the connection shut without replying (close/EOF/RST,
     * never a malformed-but-present reply) — the one case
     * {@link #connectAndIdentify} retries with the plain form. Never
     * escapes this class. */
    private static final class LegacyServerSignal extends IOException {
        LegacyServerSignal(IOException cause) {
            super(cause);
        }
    }

    static Result connectAndIdentify(String host, int port, byte[] authSecret, SSLContext tls)
            throws IOException {
        return connectAndIdentify(host, port, authSecret, tls, false);
    }

    /** The extended {@code A} capability tokens this client asks for,
     * from most to least capable — fixed wire order {@code [T] [R]}
     * (issue #125's retryable-error probe is layered in front of the
     * existing {@code T} probe, not a new mechanism of its own). {@link
     * #connectAndIdentify} starts at {@link #FULL} and steps back one
     * stage at a time on the legacy-server signal: a server that predates
     * a token it was sent treats the whole extended {@code A} as a parse
     * error and closes without replying, rather than rejecting just the
     * unrecognized token. */
    private enum ProbeStage {
        FULL(true, true), // "A <len> T R"
        TAGS_ONLY(true, false), // "A <len> T"
        LEGACY(false, false); // "A <len>"

        final boolean requestTags;
        final boolean requestRetryCapability;

        ProbeStage(boolean requestTags, boolean requestRetryCapability) {
            this.requestTags = requestTags;
            this.requestRetryCapability = requestRetryCapability;
        }
    }

    /** As {@link #connectAndIdentify(String, int, byte[], SSLContext)}, but
     * when {@code host:port} identifies as a discovery server, fetches its
     * proxy roster (`Q`, SDK proxy mode issue #122) instead of its node
     * list (`L`) — used both for {@code viaProxy}'s initial connect and its
     * reconnect-time re-fetch. A NODE identity is still returned as a plain
     * {@link NodeTarget} either way (matches `L`'s own identify-based
     * dispatch): whether that's a hard "viaProxy needs a discovery address"
     * error or simply an unusable seed to skip is the caller's call, since
     * only it knows whether this is the top-level connect address or a
     * background re-fetch that must never throw. */
    static Result connectAndIdentify(
            String host, int port, byte[] authSecret, SSLContext tls, boolean listProxies) throws IOException {
        try {
            return identifyOnce(host, port, authSecret, tls, ProbeStage.FULL, listProxies);
        } catch (LegacyServerSignal signal) {
            try {
                // Echoed response tags transparent fallback, stage 1: a server that
                // predates the `R` capability token (issue #125) treats
                // the doubly-extended `A ... T R` as a parse error and
                // closes/resets without replying — redial once with just
                // `T` and see whether it understands that.
                return identifyOnce(host, port, authSecret, tls, ProbeStage.TAGS_ONLY, listProxies);
            } catch (LegacyServerSignal signal2) {
                // Stage 2: a pre-0019 server treats even the plain `T`
                // extension as a parse error — fall all the way back to
                // the original untagged form.
                return identifyOnce(host, port, authSecret, tls, ProbeStage.LEGACY, listProxies);
            }
        }
    }

    private static Result identifyOnce(
            String host, int port, byte[] authSecret, SSLContext tls, ProbeStage stage, boolean listProxies)
            throws IOException {
        Socket socket = open(host, port, tls);
        try {
            byte[] secret = authSecret != null ? authSecret : NO_SECRET_PLACEHOLDER;
            OutputStream out = socket.getOutputStream();

            AuthIdentity identity;
            try {
                out.write(("A " + secret.length
                                + (stage.requestTags ? " T" : "")
                                + (stage.requestRetryCapability ? " R" : "")
                                + "\n")
                        .getBytes(StandardCharsets.US_ASCII));
                out.write(secret);
                out.flush();
                identity = readIdentity(socket.getInputStream());
            } catch (IOException error) {
                // Only an extended attempt is worth stepping back a stage
                // for — a failure while already running the plain (LEGACY)
                // form is a real failure. A read timeout is not a
                // legacy-server signal either: the server kept the
                // connection open, it just didn't answer (issue #40) —
                // stepping back a stage would only hang another
                // CONNECT_TIMEOUT_MS.
                boolean legacyShaped = stage.requestTags && !(error instanceof java.io.InterruptedIOException);
                throw legacyShaped ? new LegacyServerSignal(error) : error;
            }

            if (!identity.accepted()) {
                if (authSecret == null) {
                    throw new NanocachedException.AuthenticationFailed("nanocached: " + host + ":" + port
                            + " requires authentication, but no authSecret was given");
                }
                throw new NanocachedException.AuthenticationFailed("nanocached: authentication failed");
            }

            if (identity.kind() == 'n') {
                // Identify succeeded — hand the socket to Connection with
                // the exchange deadline cleared, mirroring Go's
                // SetDeadline(time.Time{}) on success (issue #40).
                socket.setSoTimeout(0);
                return new NodeTarget(socket, identity.tagged());
            }

            // A discovery server: one-shot L (or, for viaProxy, Q), then
            // this connection is done.
            if (listProxies) {
                out.write("Q\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                ProxyRosterTarget roster = readProxyList(socket.getInputStream());
                socket.close();
                return roster;
            }
            out.write("L\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            ClusterTarget cluster = readNodeList(socket.getInputStream());
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

    private record AuthIdentity(boolean accepted, char kind, boolean tagged) {}

    /** Parses the reply to `A`: {@code On\n}/{@code En\n} from a cache
     * node, {@code Od\n}/{@code Ed\n} from a discovery server (see
     * The server type in the auth response), each stretched to four bytes by a {@code T}
     * before the LF when the server is echoing the tag capability our
     * extended {@code A} asked for (echoed response tags). The second byte
     * is what identifies which kind of server this is — it isn't an
     * accident of the auth outcome, it's present whether accepted or
     * rejected. */
    private static AuthIdentity readIdentity(InputStream in) throws IOException {
        byte[] ack = readExactly(in, 3);
        boolean accepted = ack[0] == 'O';
        if (!accepted && ack[0] != 'E') {
            throw new NanocachedException("nanocached: unexpected response to A");
        }
        if (ack[1] != 'n' && ack[1] != 'd') {
            throw new NanocachedException("nanocached: unexpected response to A");
        }

        boolean tagged;
        if (ack[2] == '\n') {
            tagged = false;
        } else if (ack[2] == 'T') {
            byte[] rest = readExactly(in, 1);
            if (rest[0] != '\n') {
                throw new NanocachedException("nanocached: unexpected response to A");
            }
            tagged = true;
        } else {
            throw new NanocachedException("nanocached: unexpected response to A");
        }

        return new AuthIdentity(accepted, (char) ack[1], tagged);
    }

    private static final int CONNECT_TIMEOUT_MS = 5_000;

    private static Socket open(String host, int port, SSLContext tls) throws IOException {
        // Both paths bound the TCP connect (issue #11): the TLS factory's
        // own connect(host, port) has no timeout, so an unresponsive
        // (packet-dropping) address would hang connect()/refresh forever
        // instead of failing over. Layer SSL over a pre-connected,
        // timeout-bounded plain socket instead. The read timeout stays
        // armed through the TLS handshake AND the whole identify exchange
        // (issue #40): a peer that accepts the connection but never
        // answers must surface as a connect failure, not hang the caller
        // in readExactly forever. identifyOnce clears it only when
        // handing a node socket to Connection.
        Socket plain = new Socket();
        try {
            plain.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            plain.setTcpNoDelay(true);
            plain.setSoTimeout(CONNECT_TIMEOUT_MS);
            if (tls == null) {
                return plain;
            }
            SSLSocket socket =
                    (SSLSocket) tls.getSocketFactory().createSocket(plain, host, port, true);
            socket.setSoTimeout(CONNECT_TIMEOUT_MS);
            // Without this, the JDK verifies the certificate chain but
            // never checks it was actually issued to `host` — a valid
            // cert for any other name would be accepted, defeating TLS
            // entirely (audit finding J1). HTTPS-style endpoint
            // identification is exactly hostname verification without
            // pulling in HTTPS itself.
            SSLParameters params = socket.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(params);
            socket.startHandshake();
            return socket;
        } catch (IOException error) {
            plain.close();
            throw error;
        }
    }

    // Bounds a discovery `N <count> <r>` response before allocation,
    // mirroring MAX_VALUE_LENGTH on the `V` path: a malicious or MITM'd
    // discovery server must not be able to make the client pre-allocate
    // arbitrary memory from an unverified length prefix (`N 2000000000 3`
    // would otherwise ask for a multi-GB list). 65536 mirrors the cap the
    // Go and Rust SDKs already enforce.
    private static final int MAX_NODE_COUNT = 1 << 16;

    // A within-cap node count can still carry an absurd per-entry
    // name/address length, so this bounds the whole response's on-wire
    // size too: comfortably fits a full MAX_NODE_COUNT-node registry of
    // ordinary lengths while still bounding a malicious discovery
    // server's memory pressure on this client. The same constant is
    // being added to all six SDKs.
    private static final long MAX_NODE_LIST_RESPONSE_BYTES = 16L * 1024 * 1024;

    private static ClusterTarget readNodeList(InputStream in) throws IOException {
        String header = readLine(in);
        if (header.startsWith("B")) {
            throw new NanocachedException.DiscoveryBusy();
        }
        if (!header.startsWith("N ")) {
            throw new NanocachedException(
                    "nanocached: unexpected response from discovery server: " + header);
        }

        // `N <count> <r>\n` (client-side replication) — the replication factor rides along.
        String[] fields = header.substring(2).split(" ");
        if (fields.length != 2) {
            throw new NanocachedException("nanocached: invalid node-list header in discovery response");
        }
        int count;
        int replication;
        try {
            count = Integer.parseInt(fields[0]);
            replication = Integer.parseInt(fields[1]);
        } catch (NumberFormatException malformed) {
            // Every SDK exception must extend NanocachedException — never
            // let a raw parse failure escape (issue tracked alongside the
            // NanocachedClient port-parse fix).
            throw new NanocachedException("nanocached: invalid node-list header in discovery response");
        }
        if (count < 0 || count > MAX_NODE_COUNT) {
            throw new NanocachedException("nanocached: invalid node count in discovery response");
        }
        if (replication < 1) {
            throw new NanocachedException("nanocached: invalid replication factor in discovery response");
        }

        return new ClusterTarget(readNodeEntries(in, count), replication);
    }

    // Issue #122: a `Q` response's header carries only the count — no
    // replication field, since a proxy client needs no R (a proxy owns
    // every key). Otherwise identical framing to `L`'s `N <count> <r>`,
    // entries included, so this shares readNodeEntries/MAX_NODE_COUNT/
    // MAX_NODE_LIST_RESPONSE_BYTES with readNodeList rather than
    // duplicating them (the spec: "entry cap and name/addr length caps
    // mirror the L fetch's").
    private static ProxyRosterTarget readProxyList(InputStream in) throws IOException {
        String header = readLine(in);
        if (header.startsWith("B")) {
            throw new NanocachedException.DiscoveryBusy();
        }
        if (!header.startsWith("N ")) {
            throw new NanocachedException(
                    "nanocached: unexpected response from discovery server: " + header);
        }

        int count;
        try {
            count = Integer.parseInt(header.substring(2));
        } catch (NumberFormatException malformed) {
            throw new NanocachedException("nanocached: invalid proxy-list header in discovery response");
        }
        if (count < 0 || count > MAX_NODE_COUNT) {
            throw new NanocachedException("nanocached: invalid proxy count in discovery response");
        }

        return new ProxyRosterTarget(readNodeEntries(in, count));
    }

    /** The `<name-len> <addr-len>\n<name><addr>\n` entries shared,
     * byte-for-byte, by both `L`'s node list and `Q`'s proxy roster
     * (issue #122) — {@code count} is already validated against {@link
     * #MAX_NODE_COUNT} by the caller before this runs. */
    private static List<DiscoveredNode> readNodeEntries(InputStream in, int count) throws IOException {
        // count is bounded by MAX_NODE_COUNT by the caller, so sizing the
        // list's capacity from it is safe — never trust an unvalidated
        // wire value for a pre-allocation, validated or not.
        List<DiscoveredNode> entries = new ArrayList<>(count);
        long totalBytes = 0;
        for (int i = 0; i < count; i++) {
            String[] lengths = readLine(in).split(" ");
            if (lengths.length != 2) {
                throw new NanocachedException("nanocached: invalid node entry header in discovery response");
            }
            int nameLength;
            int addrLength;
            try {
                nameLength = Integer.parseInt(lengths[0]);
                addrLength = Integer.parseInt(lengths[1]);
            } catch (NumberFormatException malformed) {
                throw new NanocachedException("nanocached: invalid node entry header in discovery response");
            }
            if (nameLength < 0 || addrLength < 0) {
                throw new NanocachedException("nanocached: invalid node entry header in discovery response");
            }

            // Checked before allocating: a single absurd entry (or many
            // ordinary ones) must not be able to exhaust memory even
            // though count itself is within MAX_NODE_COUNT.
            totalBytes += (long) nameLength + addrLength + 1;
            if (totalBytes > MAX_NODE_LIST_RESPONSE_BYTES) {
                throw new NanocachedException(
                        "nanocached: discovery node-list response exceeds " + MAX_NODE_LIST_RESPONSE_BYTES + " bytes");
            }

            byte[] body = readExactly(in, nameLength + addrLength + 1); // +1: trailing '\n'
            if (body[body.length - 1] != '\n') {
                throw new NanocachedException("nanocached: malformed node entry in discovery response");
            }
            entries.add(new DiscoveredNode(
                    new String(body, 0, nameLength, StandardCharsets.UTF_8),
                    new String(body, nameLength, addrLength, StandardCharsets.UTF_8)));
        }
        return entries;
    }

    /** Reads up to (and consuming) the next '\n', returning what preceded
     * it. Bounded by {@link Connection#MAX_HEADER_LINE_LENGTH} — the same
     * cap and rationale as {@link Connection}'s own readLine, shared
     * rather than duplicated: every real discovery header line (`N
     * <count> <r>`, a `<nameLen> <addrLen>` entry line) is a handful of
     * bytes, so a malicious or buggy discovery server that streams bytes
     * with no '\n' must fail fast — connection-classified, so the
     * built-in redial/retry layer (and fetchNodeList's per-address
     * fallback) handles it exactly like any other dead/misbehaving
     * address — instead of growing this buffer without bound (issue:
     * audit finding, unbounded readLine in Identify; Connection.java's
     * own readLine already had this cap). */
    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        for (int b = read(in); b != '\n'; b = read(in)) {
            if (line.length() >= Connection.MAX_HEADER_LINE_LENGTH) {
                throw new NanocachedException.ConnectionFailed(
                        "nanocached: response header line too long", null);
            }
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
