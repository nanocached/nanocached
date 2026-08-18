package nanocached

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// noSecretPlaceholder is sent as the A secret when the caller didn't
// configure one: a server with no secret accepts any non-empty secret,
// and one that requires a real secret correctly rejects this placeholder.
var noSecretPlaceholder = []byte{0}

// DiscoveredNode pairs a node's hash-ring identity (a random per-process
// UUID) with its network address — two different things since
// doc/adr/0009-*.md: Name is what routing hashes; Address is only for
// opening a connection.
type DiscoveredNode struct {
	Name    string
	Address string
}

type identified struct {
	// Exactly one of conn / cluster is set.
	conn        net.Conn
	nodes       []DiscoveredNode
	replication int
}

// connectAndIdentify dials host:port, authenticates, and figures out from
// the server's own A response whether it reached a cache node (On) or a
// discovery server (Od) — the caller never says which it expects
// (doc/adr/0007-*.md). A node's conn is handed back live; a discovery
// connection is used once for L and closed, returning the name/address
// list and the cluster's replication factor R (doc/adr/0009, 0010, 0011).
// handshakeDeadline bounds the identify exchange after the dial: a
// server that accepts the TCP connection but never answers (a blackholed
// address behaves the same way) must not hang the caller. A variable
// only so tests can shorten it.
var handshakeDeadline = 5 * time.Second

func connectAndIdentify(address string, authSecret []byte, tlsConfig *tls.Config) (*identified, error) {
	conn, err := open(address, tlsConfig)
	if err != nil {
		return nil, connectionLost("could not connect to "+address, err)
	}

	_ = conn.SetDeadline(time.Now().Add(handshakeDeadline))
	result, err := identify(conn, address, authSecret)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if result.conn != nil {
		// The deadline only bounds the handshake; a live node connection
		// must not inherit it.
		_ = result.conn.SetDeadline(time.Time{})
	}
	return result, nil
}

func open(address string, tlsConfig *tls.Config) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
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

func identify(conn net.Conn, address string, authSecret []byte) (*identified, error) {
	secret := authSecret
	if secret == nil {
		secret = noSecretPlaceholder
	}
	frame := append([]byte(fmt.Sprintf("A %d\n", len(secret))), secret...)
	if _, err := conn.Write(frame); err != nil {
		return nil, connectionLost("handshake write failed", err)
	}

	reader := bufio.NewReader(conn)
	ack := make([]byte, 3)
	if _, err := readFull(reader, ack); err != nil {
		return nil, connectionLost("handshake read failed", err)
	}
	shaped := ack[2] == '\n' &&
		(ack[0] == 'O' || ack[0] == 'E') &&
		(ack[1] == 'n' || ack[1] == 'd')
	if !shaped {
		return nil, fmt.Errorf("nanocached: unexpected response to A")
	}
	if ack[0] == 'E' {
		if authSecret == nil {
			return nil, fmt.Errorf(
				"nanocached: %s requires authentication, but no AuthSecret was given", address)
		}
		return nil, fmt.Errorf("nanocached: authentication failed")
	}

	if ack[1] == 'n' {
		// Hand the live node connection over, buffered reader included:
		// the buffer may already hold bytes that must not be lost.
		return &identified{conn: &bufferedConn{Conn: conn, reader: reader}}, nil
	}

	// A discovery server: one-shot L, then this connection is done.
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

func readNodeList(reader *bufio.Reader) ([]DiscoveredNode, int, error) {
	header, err := reader.ReadString('\n')
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

	// `N <count> <r>\n` (ADR-0011) — the replication factor rides along.
	fields := strings.Split(rest, " ")
	if len(fields) != 2 {
		return nil, 0, fmt.Errorf("nanocached: invalid node-list header in discovery response")
	}
	count, err := strconv.Atoi(fields[0])
	if err != nil || count < 0 {
		return nil, 0, fmt.Errorf("nanocached: invalid node count in discovery response")
	}
	replication, err := strconv.Atoi(fields[1])
	if err != nil || replication < 1 {
		return nil, 0, fmt.Errorf("nanocached: invalid replication factor in discovery response")
	}

	nodes := make([]DiscoveredNode, 0, count)
	for i := 0; i < count; i++ {
		entry, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, connectionLost("node-list read failed", err)
		}
		lengths := strings.Split(strings.TrimSuffix(entry, "\n"), " ")
		if len(lengths) != 2 {
			return nil, 0, fmt.Errorf("nanocached: invalid node entry header in discovery response")
		}
		nameLength, err1 := strconv.Atoi(lengths[0])
		addrLength, err2 := strconv.Atoi(lengths[1])
		if err1 != nil || err2 != nil || nameLength < 0 || addrLength < 0 {
			return nil, 0, fmt.Errorf("nanocached: invalid node entry lengths in discovery response")
		}

		body := make([]byte, nameLength+addrLength+1) // +1: trailing '\n'
		if _, err := readFull(reader, body); err != nil {
			return nil, 0, connectionLost("node-list read failed", err)
		}
		if body[len(body)-1] != '\n' {
			return nil, 0, fmt.Errorf("nanocached: malformed node entry in discovery response")
		}
		nodes = append(nodes, DiscoveredNode{
			Name:    string(body[:nameLength]),
			Address: string(body[nameLength : nameLength+addrLength]),
		})
	}
	return nodes, replication, nil
}
