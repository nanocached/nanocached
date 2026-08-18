package nanocached

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// connection is one already-identified connection to a single
// nanocached-node, speaking the cache protocol (G/S/D — the A identify
// exchange happens in identify.go before a connection exists). Requests
// are serialized per connection (a mutex around each round trip) — a
// deliberate v1 simplification over the TypeScript SDK's pipelining:
// nanocached-node answers in arrival order, so serializing is always
// correct, just less concurrent. Concurrent callers queue on the mutex.
// maxValueLength bounds a `V <len>` response before allocation: the
// server never stores values above its 1 MiB request limit, so anything
// larger is a corrupt or malicious frame.
const maxValueLength = 2 * 1024 * 1024

type connection struct {
	mu       sync.Mutex
	conn     net.Conn // nil only for the pre-poisoned placeholder
	reader   *bufio.Reader
	closed   bool
	lastUsed time.Time
	// onClose, when set, fires exactly once — the moment this connection
	// transitions from open to closed — so callers can keep an external
	// open-connection count (the forgotten-close tracker in client.go)
	// accurate no matter which of the several close() call sites fires.
	onClose func()
}

func newConnection(conn net.Conn) *connection {
	return &connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		lastUsed: time.Now(),
	}
}

// deadConnection is a pre-poisoned placeholder for a newly discovered
// node — the first request through it fails as connection-lost and
// triggers the lazy redial.
func deadConnection() *connection {
	return &connection{closed: true}
}

func (c *connection) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *connection) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.conn != nil {
		_ = c.conn.Close()
	}
	onClose := c.onClose
	c.mu.Unlock()
	if onClose != nil {
		onClose()
	}
}

func (c *connection) idle() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastUsed)
}

func (c *connection) get(key []byte) ([]byte, bool, error) {
	frame := append([]byte(fmt.Sprintf("G %d\n", len(key))), key...)
	marker, value, err := c.request(frame)
	if err != nil {
		return nil, false, err
	}
	switch marker {
	case 'V':
		return value, true, nil
	case 'N':
		return nil, false, nil
	case 'W':
		return nil, false, ErrWrongNode
	default:
		return nil, false, c.mismatch(marker)
	}
}

func (c *connection) set(key, value []byte, ttlSeconds int64) error {
	var header string
	if ttlSeconds < 0 {
		header = fmt.Sprintf("S %d %d\n", len(key), len(value))
	} else {
		header = fmt.Sprintf("S %d %d %d\n", len(key), len(value), ttlSeconds)
	}
	frame := append(append([]byte(header), key...), value...)
	marker, _, err := c.request(frame)
	if err != nil {
		return err
	}
	switch marker {
	case 'S':
		return nil
	case 'W':
		return ErrWrongNode
	default:
		return c.mismatch(marker)
	}
}

func (c *connection) delete(key []byte) (bool, error) {
	frame := append([]byte(fmt.Sprintf("D %d\n", len(key))), key...)
	marker, _, err := c.request(frame)
	if err != nil {
		return false, err
	}
	switch marker {
	case 'D':
		return true, nil
	case 'N':
		return false, nil
	case 'W':
		return false, ErrWrongNode
	default:
		return false, c.mismatch(marker)
	}
}

// mismatch handles a well-formed response of the wrong kind (a `S`
// answering a G): the request/response streams are misaligned — every
// later response would answer the wrong request, silently returning
// other keys' data. Poison the connection, and classify as
// connection-lost so the client's retry layer redials and retries once.
func (c *connection) mismatch(marker byte) error {
	c.close()
	return connectionLost(
		fmt.Sprintf("response %q does not match the request (connection desynced)", marker), nil)
}

func (c *connection) request(frame []byte) (byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.conn == nil {
		return 0, nil, connectionLost("connection is closed", nil)
	}
	c.lastUsed = time.Now()

	marker, value, err := c.roundTrip(frame)
	if err != nil {
		// The stream state after a failed round trip is unknown — poison
		// the connection so the client redials lazily.
		c.closed = true
		_ = c.conn.Close()
		return 0, nil, connectionLost("connection failed", err)
	}
	return marker, value, nil
}

func (c *connection) roundTrip(frame []byte) (byte, []byte, error) {
	if _, err := c.conn.Write(frame); err != nil {
		return 0, nil, err
	}

	marker, err := c.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	switch marker {
	case 'V':
		header, err := c.reader.ReadString('\n')
		if err != nil {
			return 0, nil, err
		}
		// The wire is `V <len>\n`; after the marker byte the header still
		// carries the leading space. Lengths beyond the server's own 1 MiB
		// request cap are protocol garbage — reject before allocating.
		length, err := strconv.Atoi(strings.TrimSpace(header))
		if err != nil || length < 0 || length > maxValueLength {
			return 0, nil, fmt.Errorf("invalid value length in response")
		}
		value := make([]byte, length)
		if _, err := readFull(c.reader, value); err != nil {
			return 0, nil, err
		}
		return marker, value, nil
	case 'S', 'D', 'N', 'W':
		if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
			return 0, nil, err
		}
		return marker, nil, nil
	case 'B':
		// Unsolicited busy: connection-limit rejection, server closing.
		return 0, nil, fmt.Errorf("server rejected the connection (connection limit reached)")
	default:
		return 0, nil, fmt.Errorf("unexpected response from server: %c", marker)
	}
}

func readFull(reader *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := reader.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
