package nanocached

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
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
	conn        net.Conn
	nodes       []discoveredNode
	replication int
	// tagged (echoed response tags): the node accepted the extended `A ... T`,
	// so this connection's G/S/D traffic must carry tags and its
	// responses echo them; false means an older node answered the plain-`A`
	// fallback. Meaningless on a cluster result.
	tagged bool
}

// connectAndIdentify dials host:port, authenticates, and figures out from
// the server's own A response whether it reached a cache node (On) or a
// discovery server (Od) — the caller never says which it expects
// (the server type in the auth response). A node's conn is handed back live; a discovery
// connection is used once for L and closed, returning the name/address
// list and the cluster's replication factor R (node identity, discovery HA, replication).
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

func connectAndIdentify(address string, authSecret []byte, tlsConfig *tls.Config) (*identified, error) {
	deadline := time.Now().Add(connectDeadline)
	conn, err := open(address, tlsConfig, deadline)
	if err != nil {
		return nil, connectionLost("could not connect to "+address, err)
	}

	_ = conn.SetDeadline(deadline)
	result, err := identify(conn, address, authSecret, true)
	if err != nil {
		_ = conn.Close()
		if !isLegacyServerSignal(err) {
			return nil, err
		}
		// Echoed response tags transparent fallback: a pre-0019 server rejects
		// the extended `A ... T` as a parse error and closes without
		// replying — redial once with the plain form and run the
		// connection untagged (the pre-0019 behavior, desync window
		// included).
		deadline = time.Now().Add(connectDeadline)
		conn, err = open(address, tlsConfig, deadline)
		if err != nil {
			return nil, connectionLost("could not connect to "+address, err)
		}
		_ = conn.SetDeadline(deadline)
		result, err = identify(conn, address, authSecret, false)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	if result.conn != nil {
		// The deadline only bounds the handshake; a live node connection
		// must not inherit it.
		_ = result.conn.SetDeadline(time.Time{})
	}
	return result, nil
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
	return tls.DialWithDialer(&dialer, "tcp", address, config)
}

// identify runs the `A` handshake on conn. requestTags (echoed response tags)
// says whether to send the extended form (`A <len> T\n<secret>`), asking
// the server to echo tags on this connection's G/S/D traffic; the client
// always tries this first (connectAndIdentify falls back to the plain form
// on the legacy-server signal below). A write or read failure on the ack
// itself is returned raw (not connectionLost-wrapped) when requestTags is
// true, so the caller can tell a pre-0019 server's closed/EOF/reset door
// apart from an ordinary connection failure and retry untagged.
func identify(conn net.Conn, address string, authSecret []byte, requestTags bool) (*identified, error) {
	secret := authSecret
	if secret == nil {
		secret = noSecretPlaceholder
	}
	tagSuffix := ""
	if requestTags {
		tagSuffix = " T"
	}
	frame := append([]byte(fmt.Sprintf("A %d%s\n", len(secret), tagSuffix)), secret...)
	if _, err := conn.Write(frame); err != nil {
		if requestTags && isLegacyServerSignal(err) {
			return nil, err
		}
		return nil, connectionLost("handshake write failed", err)
	}

	reader := bufio.NewReader(conn)
	ack := make([]byte, 3)
	if _, err := readFull(reader, ack); err != nil {
		if requestTags && isLegacyServerSignal(err) {
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

	// A discovery server: one-shot L, then this connection is done. Tags
	// have no meaning here (discovery is a single L per connection), but
	// the reply above still had to be parsed either way.
	if _, err := conn.Write([]byte("L\n")); err != nil {
		return nil, connectionLost("L write failed", err)
	}
	nodes, replication, err := readNodeList(reader)
	if err != nil {
		return nil, err
	}
	_ = conn.Close()
	return &identified{nodes: nodes, replication: replication}, nil
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
	count, err := strconv.Atoi(fields[0])
	if err != nil || count < 0 || count > maxNodeCount {
		return nil, 0, fmt.Errorf("nanocached: invalid node count in discovery response")
	}
	replication, err := strconv.Atoi(fields[1])
	if err != nil || replication < 1 {
		return nil, 0, fmt.Errorf("nanocached: invalid replication factor in discovery response")
	}

	nodes := make([]discoveredNode, 0, count)
	total := 0
	for i := 0; i < count; i++ {
		entry, err := readLine(reader)
		if err != nil {
			return nil, 0, connectionLost("node-list read failed", err)
		}
		total += len(entry)
		lengths := strings.Split(strings.TrimSuffix(entry, "\n"), " ")
		if len(lengths) != 2 {
			return nil, 0, fmt.Errorf("nanocached: invalid node entry header in discovery response")
		}
		nameLength, err1 := strconv.Atoi(lengths[0])
		addrLength, err2 := strconv.Atoi(lengths[1])
		if err1 != nil || err2 != nil || nameLength < 0 || addrLength < 0 ||
			nameLength > maxNodeFieldLength || addrLength > maxNodeFieldLength {
			return nil, 0, fmt.Errorf("nanocached: invalid node entry lengths in discovery response")
		}

		bodyLength := nameLength + addrLength + 1 // +1: trailing '\n'
		total += bodyLength
		if total > maxNodeListResponseBytes {
			return nil, 0, fmt.Errorf(
				"nanocached: discovery node-list response exceeds %d bytes", maxNodeListResponseBytes)
		}

		body := make([]byte, bodyLength)
		if _, err := readFull(reader, body); err != nil {
			return nil, 0, connectionLost("node-list read failed", err)
		}
		if body[len(body)-1] != '\n' {
			return nil, 0, fmt.Errorf("nanocached: malformed node entry in discovery response")
		}
		nodes = append(nodes, discoveredNode{
			Name:    string(body[:nameLength]),
			Address: string(body[nameLength : nameLength+addrLength]),
		})
	}
	return nodes, replication, nil
}
