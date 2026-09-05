package nanocached

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"
)

// noSecretPlaceholder is sent as the A secret when the caller didn't
// configure one: a server with no secret accepts any non-empty secret,
// and one that requires a real secret correctly rejects this placeholder.
var noSecretPlaceholder = []byte{0}

// discoveredNode pairs a node's hash-ring identity (a random per-process
// UUID) with its network address — two different things since
// Node identity decoupled from address: Name is what routing hashes; Address is only for
// opening a connection. Unexported (issue #47): no public API ever
// returns or accepts it, so exporting it was dead public surface.
type discoveredNode struct {
	Name    string
	Address string
}

type identified struct {
	// Exactly one of conn / cluster is set.
	conn net.Conn
	// nodes holds a discovery connection's roster: the node list (`L`)
	// or, in proxy mode (issue #122), the registered nanocached-proxy
	// list (`Q`) — the two share the exact same name/address entry
	// shape, so one field serves both; which command produced it is a
	// property of how identify() was called (see discoveryListCommand),
	// not of this struct.
	nodes []discoveredNode
	// list records which roster command produced nodes (issue #486) —
	// listNodes or listProxies; zero on a node (`conn`) result.
	list discoveryListCommand
	// replication is only meaningful for a node-list (`L`) result — a
	// proxy roster (`Q`) carries no replication field on the wire (a
	// proxy client needs no R; see nanocached-discovery.rs's
	// ListProxies). Read it through nodeReplication, which refuses a `Q`
	// result instead of handing back a meaningless zero.
	replication int
	// tagged (echoed response tags): the node accepted the extended `A ... T`,
	// so this connection's G/S/D traffic must carry tags and its
	// responses echo them; false means an older node answered the plain-`A`
	// fallback. Meaningless on a cluster result.
	tagged bool
}

// discoveryListCommand selects which one-shot roster command an identify
// exchange sends once the peer reveals itself as a discovery server
// (issue #122): listNodes for the ordinary `L` node roster every
// non-proxy caller wants, or listProxies for `Q`, the registered
// nanocached-proxy roster ViaProxy's connect/reconnect flow wants
// instead. Same single-connection-then-close shape either way (see
// identify) — only the command byte and the reply parser differ
// (readNodeList vs. readProxyList).
type discoveryListCommand byte

const (
	listNodes   discoveryListCommand = 'L'
	listProxies discoveryListCommand = 'Q'
)

// authProbe is one stage of the connect/identify handshake's auth
// capability probe (issue #47's echoed response tags `T`, extended by
// issue #125's retryable-error status `R`). The client always tries the
// richest form first and falls back a stage at a time on the legacy
// signal (isLegacyServerSignal) — see connectAndIdentifyAs. Token order
// on the wire is fixed: `[T] [R]`.
type authProbe int

const (
	// probeRetryable sends `A <len> T R` — echoed response tags plus the
	// retryable-error capability, the form every connection tries first.
	probeRetryable authProbe = iota
	// probeTagged sends `A <len> T` — a server that predates `R` but
	// still understands `T` (issue #47).
	probeTagged
	// probeLegacy sends the bare `A <len>` — a pre-0019 server that
	// predates both extensions. The final stage: no further fallback.
	probeLegacy
)

// wireSuffix is the extra header text authProbe appends after `A <len>`.
func (p authProbe) wireSuffix() string {
	switch p {
	case probeRetryable:
		return " T R"
	case probeTagged:
		return " T"
	default:
		return ""
	}
}

// fallback reports the next, less-capable stage to retry with after this
// one's extended `A` drew the legacy-server signal, and whether there is
// one at all (probeLegacy has none — it's already the most basic form).
func (p authProbe) fallback() (authProbe, bool) {
	switch p {
	case probeRetryable:
		return probeTagged, true
	case probeTagged:
		return probeLegacy, true
	default:
		return probeLegacy, false
	}
}

// connectDeadline bounds one whole connect attempt — dial, TLS
// handshake, and the identify exchange share a single 5s budget, the
// same shape as the other five SDKs (issue #47 item 1: the previous
// 10s-dial + 5s-handshake staging made Go's worst-case failover ~3x
// the others'). A server that accepts the TCP connection but never
// answers (a blackholed address behaves the same way) must not hang
// the caller. The echoed response tags legacy fallback's redial gets a fresh
// budget, matching the per-attempt deadlines elsewhere. A variable
// only so tests can shorten it.
var connectDeadline = 5 * time.Second

// connectAndIdentify dials host:port, authenticates, and figures out from
// the server's own A response whether it reached a cache node (On) or a
// discovery server (Od) — the caller never says which it expects
// (the server type in the auth response). A node's conn is handed back live; a discovery
// connection is used once for L and closed, returning the name/address
// list and the cluster's replication factor R (node identity, discovery HA, replication).
// Equivalent to connectAndIdentifyProxies with listNodes — see that
// function's doc for the proxy-mode (`Q`) counterpart.
func connectAndIdentify(address string, authSecret []byte, tlsConfig *tls.Config) (*identified, error) {
	return connectAndIdentifyAs(address, authSecret, tlsConfig, listNodes)
}

// connectAndIdentifyProxies is connectAndIdentify's proxy-mode counterpart
// (issue #122, Config.ViaProxy): identical dial/auth/legacy-fallback
// handling, but a discovery peer is asked for `Q` (the registered
// nanocached-proxy roster) instead of `L` (the node roster) — see
// readProxyList. A node peer answers exactly as connectAndIdentify's
// does; ViaProxy's caller is the one that decides that's a
// misconfiguration, not this function.
func connectAndIdentifyProxies(address string, authSecret []byte, tlsConfig *tls.Config) (*identified, error) {
	return connectAndIdentifyAs(address, authSecret, tlsConfig, listProxies)
}

func connectAndIdentifyAs(
	address string, authSecret []byte, tlsConfig *tls.Config, list discoveryListCommand,
) (*identified, error) {
	// Capability-probe fallback chain (issue #125 adds a stage in front
	// of issue #47's existing one): `A <len> T R`, then `A <len> T`, then
	// plain `A <len>` — each stage retried, on a fresh dial, only when the
	// previous one drew the legacy-server signal (a pre-that-capability
	// server rejects the extended `A` as a parse error and closes without
	// replying; see isLegacyServerSignal). Every connection this SDK
	// dials (per-node, proxy, discovery, hedge, reconnect) goes through
	// this same probe, since connectAndIdentifyAs is the sole dial path.
	for probe := probeRetryable; ; {
		deadline := time.Now().Add(connectDeadline)
		conn, err := open(address, tlsConfig, deadline)
		if err != nil {
			return nil, connectionLost("could not connect to "+address, err)
		}

		_ = conn.SetDeadline(deadline)
		result, err := identify(conn, address, authSecret, probe, list)
		if err == nil {
			if result.conn != nil {
				// The deadline only bounds the handshake; a live node
				// connection must not inherit it.
				_ = result.conn.SetDeadline(time.Time{})
			}
			return result, nil
		}
		_ = conn.Close()

		next, ok := probe.fallback()
		if !ok || !isLegacyServerSignal(err) {
			return nil, err
		}
		probe = next
	}
}

// isLegacyServerSignal reports whether err looks like a pre-tag
// server slamming the door on the extended `A ... T` — closing, EOF, or
// resetting the connection before any reply — the only failure worth
// retrying with the plain form. A timeout is not one: the server kept the
// connection open, it just didn't answer. identify only ever returns a raw
// (unwrapped) error of this shape from the tagged handshake's write/read
// step, so checking it here can't misclassify an unrelated connectionLost-
// wrapped failure (e.g. a later L/node-list read) as a legacy signal.
// io.ErrUnexpectedEOF is included alongside io.EOF: readFull now goes
// through io.ReadFull (issue #47 audit), which reports a
// partial-then-closed read as ErrUnexpectedEOF rather than plain EOF —
// still the same "closed before any reply" signature, matching Rust's
// classify_auth_io_error treating UnexpectedEof the same as a clean EOF.
func isLegacyServerSignal(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// open dials (and, with TLS, handshakes) within the attempt's shared
// absolute deadline — see connectDeadline. tls.DialWithDialer applies
// the dialer's deadline to the TLS handshake too, so the whole attempt
// stays inside one budget.
func open(address string, tlsConfig *tls.Config, deadline time.Time) (net.Conn, error) {
	dialer := net.Dialer{Deadline: deadline}
	if tlsConfig == nil {
		conn, err := dialer.Dial("tcp", address)
		if err != nil {
			return nil, err
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetNoDelay(true)
		}
		return conn, nil
	}

	config := tlsConfig
	if config.ServerName == "" {
		host, _, err := net.SplitHostPort(address)
		if err == nil {
			config = config.Clone()
			config.ServerName = host
		}
	}
	conn, err := tls.DialWithDialer(&dialer, "tcp", address, config)
	if err != nil {
		return nil, err
	}
	// Same reasoning as the plaintext path above: small request/response
	// frames pay Nagle's-algorithm latency unless nodelay is set. The
	// *tls.Conn wraps the raw *net.TCPConn rather than being one, so the
	// type assertion above can't reach it directly; NetConn() (Go 1.18+,
	// well under this module's go 1.22 floor) unwraps to the underlying
	// net.Conn so we can set nodelay on it here too.
	if tcp, ok := conn.NetConn().(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return conn, nil
}

// identify runs the `A` handshake on conn. probe (issue #47's echoed
// response tags `T`, extended by issue #125's retryable-error status `R`)
// selects which capability tokens the extended form asks for
// (`A <len>[ T][ R]\n<secret>`) — the client always tries probeRetryable
// first (connectAndIdentifyAs falls back a stage at a time on the
// legacy-server signal below). Server replies are unchanged either way
// (`On`/`OnT`/`Od`/`OdT`): `R` is a purely client-declared capability,
// never echoed back on the ack. A write or read failure
// on the ack itself is returned raw (not connectionLost-wrapped) unless
// probe is already probeLegacy, so the caller can tell a too-old
// server's closed/EOF/reset door apart from an ordinary connection
// failure and retry with a less capable probe. list (issue #122) selects
// `L` or `Q` for the one-shot roster command sent when the peer turns
// out to be a discovery server — see discoveryListCommand.
func identify(conn net.Conn, address string, authSecret []byte, probe authProbe, list discoveryListCommand) (*identified, error) {
	secret := authSecret
	if secret == nil {
		secret = noSecretPlaceholder
	}
	extended := probe != probeLegacy
	frame := append([]byte(fmt.Sprintf("A %d%s\n", len(secret), probe.wireSuffix())), secret...)
	if _, err := conn.Write(frame); err != nil {
		if extended && isLegacyServerSignal(err) {
			return nil, err
		}
		return nil, connectionLost("handshake write failed", err)
	}

	reader := bufio.NewReader(conn)
	ack := make([]byte, 3)
	if _, err := readFull(reader, ack); err != nil {
		if extended && isLegacyServerSignal(err) {
			return nil, err
		}
		return nil, connectionLost("handshake read failed", err)
	}
	shapedPrefix := (ack[0] == 'O' || ack[0] == 'E') && (ack[1] == 'n' || ack[1] == 'd')
	if !shapedPrefix {
		return nil, fmt.Errorf("nanocached: unexpected response to A")
	}

	// Echoed response tags stretches the reply to four bytes by a `T` before
	// the LF when the server is echoing the tag capability our extended
	// `A` asked for (`OnT\n`/`EnT\n`/`OdT\n`/`EdT\n`); a bare `\n` in that
	// third position is the traditional, untagged reply.
	var tagged bool
	switch ack[2] {
	case '\n':
		tagged = false
	case 'T':
		fourth := make([]byte, 1)
		if _, err := readFull(reader, fourth); err != nil {
			return nil, connectionLost("handshake read failed", err)
		}
		if fourth[0] != '\n' {
			return nil, fmt.Errorf("nanocached: unexpected response to A")
		}
		tagged = true
	default:
		return nil, fmt.Errorf("nanocached: unexpected response to A")
	}

	if ack[0] == 'E' {
		if authSecret == nil {
			return nil, errors.Join(ErrAuthenticationFailed, fmt.Errorf(
				"nanocached: %s requires authentication, but no AuthSecret was given", address))
		}
		return nil, ErrAuthenticationFailed
	}

	if ack[1] == 'n' {
		// Hand the live node connection over, buffered reader included:
		// the buffer may already hold bytes that must not be lost.
		return &identified{conn: &bufferedConn{Conn: conn, reader: reader}, tagged: tagged}, nil
	}

	// A discovery server: one-shot L or Q, then this connection is done.
	// Tags have no meaning here (discovery answers exactly one roster
	// request per connection), but the reply above still had to be
	// parsed either way.
	if _, writeErr := conn.Write([]byte{byte(list), '\n'}); writeErr != nil {
		return nil, connectionLost(fmt.Sprintf("%c write failed", list), writeErr)
	}
	var nodes []discoveredNode
	var replication int
	var err error
	if list == listProxies {
		nodes, err = readProxyList(reader)
	} else {
		nodes, replication, err = readNodeList(reader)
	}
	if err != nil {
		return nil, err
	}
	_ = conn.Close()
	return &identified{nodes: nodes, replication: replication, list: list}, nil
}

// nodeReplication returns the replication factor a node-list (`L`) result
// carried. A proxy roster (`Q`) has none, and a cluster-mode caller that
// reads one is a bug, so this reports it instead of returning zero
// (issue #486).
func (r *identified) nodeReplication() (int, error) {
	if r.list != listNodes {
		return 0, fmt.Errorf("nanocached: replication factor requested from a %c roster", byte(r.list))
	}
	return r.replication, nil
}

// bufferedConn keeps the identify-time bufio.Reader attached to the
// connection so no already-buffered bytes are dropped when the node
// connection is handed to newConnection (which wraps it again).
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

// maxNodeCount and maxNodeFieldLength bound a discovery `N` response
// before allocation, mirroring maxValueLength on the `V` path: a
// malicious or MITM'd discovery server must not be able to make the
// client pre-allocate arbitrary memory from an unverified length prefix.
// Per-field caps alone still leave the aggregate unbounded in practice
// (maxNodeCount * 2 * maxNodeFieldLength is ~8.5GB) — maxNodeListResponseBytes
// caps the total, bounding a malicious discovery server's memory
// pressure on the client while comfortably fitting a full 65536-node
// registry of ordinary name/address lengths.
const (
	maxNodeCount             = 1 << 16
	maxNodeFieldLength       = 64 * 1024
	maxNodeListResponseBytes = 16 * 1024 * 1024
)

func readNodeList(reader *bufio.Reader) ([]discoveredNode, int, error) {
	header, err := readLine(reader)
	if err != nil {
		return nil, 0, connectionLost("node-list read failed", err)
	}
	header = strings.TrimSuffix(header, "\n")

	if strings.HasPrefix(header, "B") {
		return nil, 0, ErrDiscoveryBusy
	}
	rest, ok := strings.CutPrefix(header, "N ")
	if !ok {
		return nil, 0, fmt.Errorf("nanocached: unexpected response from discovery server: %s", header)
	}

	// `N <count> <r>\n` (client-side replication) — the replication factor rides along.
	fields := strings.Split(rest, " ")
	if len(fields) != 2 {
		return nil, 0, fmt.Errorf("nanocached: invalid node-list header in discovery response")
	}
	count, err := parseStrictInt(fields[0])
	if err != nil || count < 0 || count > maxNodeCount {
		return nil, 0, fmt.Errorf("nanocached: invalid node count in discovery response")
	}
	replication, err := parseStrictInt(fields[1])
	if err != nil || replication < 1 {
		return nil, 0, fmt.Errorf("nanocached: invalid replication factor in discovery response")
	}

	nodes, err := readListEntries(reader, count)
	if err != nil {
		return nil, 0, err
	}
	return nodes, replication, nil
}

// readProxyList reads a discovery `Q` reply (issue #122): `N <count>\n`
// then, per proxy, the exact same `<name-len> <addr-len>\n<name><addr>\n`
// entry shape readNodeList's `L` reply uses — nanocached-discovery.rs's
// ListProxies command mirrors List's roster shape, just without the
// trailing replication field on the header line (a proxy client needs no
// R: it looks like a single node that owns every key).
func readProxyList(reader *bufio.Reader) ([]discoveredNode, error) {
	header, err := readLine(reader)
	if err != nil {
		return nil, connectionLost("proxy-list read failed", err)
	}
	header = strings.TrimSuffix(header, "\n")

	if strings.HasPrefix(header, "B") {
		return nil, ErrDiscoveryBusy
	}
	rest, ok := strings.CutPrefix(header, "N ")
	if !ok {
		return nil, fmt.Errorf("nanocached: unexpected response from discovery server: %s", header)
	}

	count, err := parseStrictInt(rest)
	if err != nil || count < 0 || count > maxNodeCount {
		return nil, fmt.Errorf("nanocached: invalid proxy count in discovery response")
	}

	return readListEntries(reader, count)
}

// readListEntries reads count `<name-len> <addr-len>\n<name><addr>\n`
// entries off reader — the body shared by a discovery `N` reply, node
// roster (`L`, readNodeList) or proxy roster (`Q`, readProxyList) alike
// (issue #122): the same per-field caps (maxNodeFieldLength) and the same
// running-total cap (maxNodeListResponseBytes) apply either way, since a
// hostile or MITM'd `Q` reply can exhaust client memory exactly as an `L`
// one can.
func readListEntries(reader *bufio.Reader, count int) ([]discoveredNode, error) {
	nodes := make([]discoveredNode, 0, count)
	total := 0
	for i := 0; i < count; i++ {
		entry, err := readLine(reader)
		if err != nil {
			return nil, connectionLost("node-list read failed", err)
		}
		total += len(entry)
		lengths := strings.Split(strings.TrimSuffix(entry, "\n"), " ")
		if len(lengths) != 2 {
			return nil, fmt.Errorf("nanocached: invalid node entry header in discovery response")
		}
		nameLength, err1 := parseStrictInt(lengths[0])
		addrLength, err2 := parseStrictInt(lengths[1])
		if err1 != nil || err2 != nil || nameLength < 0 || addrLength < 0 ||
			nameLength > maxNodeFieldLength || addrLength > maxNodeFieldLength {
			return nil, fmt.Errorf("nanocached: invalid node entry lengths in discovery response")
		}

		bodyLength := nameLength + addrLength + 1 // +1: trailing '\n'
		total += bodyLength
		if total > maxNodeListResponseBytes {
			return nil, fmt.Errorf(
				"nanocached: discovery node-list response exceeds %d bytes", maxNodeListResponseBytes)
		}

		body := make([]byte, bodyLength)
		if _, err := readFull(reader, body); err != nil {
			return nil, connectionLost("node-list read failed", err)
		}
		if body[len(body)-1] != '\n' {
			return nil, fmt.Errorf("nanocached: malformed node entry in discovery response")
		}
		nodes = append(nodes, discoveredNode{
			Name:    string(body[:nameLength]),
			Address: string(body[nameLength : nameLength+addrLength]),
		})
	}
	return nodes, nil
}
