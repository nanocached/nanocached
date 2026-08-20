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
// are pipelined onto the socket and matched to responses in send order
// (doc/adr/0016-*.md): a dedicated read loop consumes responses and
// dispatches each to the oldest still-pending request, since
// nanocached-node itself only ever answers in the order it received
// requests. mu also serializes the push-onto-pending-queue-and-write
// sequence across concurrent callers, so queue order always matches wire
// send order.
// maxValueLength bounds a `V <len>` response before allocation: the
// server never stores values above its 1 MiB request limit, so anything
// larger is a corrupt or malicious frame.
const maxValueLength = 2 * 1024 * 1024

// requestTimeout bounds each outstanding request's full round trip
// (write + wait for its matched response): without it, a half-open
// server that accepts the TCP connection but never writes back — or
// stops mid-stream — would hang Get/Set/Delete forever in readLoop's
// blocking Read, wedging every other pending caller behind it (and,
// transitively, Close(), which waits on background replica writes).
// Generous versus the server's own 10s outbound timeouts. A variable
// only so tests can shorten it.
var requestTimeout = 30 * time.Second

type roundTripResult struct {
	marker byte
	value  []byte
	err    error
}

type connection struct {
	mu       sync.Mutex
	conn     net.Conn // nil only for the pre-poisoned placeholder
	reader   *bufio.Reader
	pending  []chan roundTripResult
	closed   bool
	lastErr  error
	lastUsed time.Time
	// onClose, when set, fires exactly once — the moment this connection
	// transitions from open to closed — so callers can keep an external
	// open-connection count (the forgotten-close tracker in client.go)
	// accurate no matter which of the several close() call sites fires.
	onClose func()
}

// onClose is taken here, not assigned afterward, so it's fully set
// before the read loop goroutine — started before newConnection even
// returns — can possibly read it in poison().
func newConnection(conn net.Conn, onClose func()) *connection {
	c := &connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		lastUsed: time.Now(),
		onClose:  onClose,
	}
	go c.readLoop()
	return c
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
	c.poison(connectionLost("connection closed", nil))
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
// Requests still pending behind this one may already have been resolved
// with misaligned data by the time this runs (the read loop doesn't wait
// for a caller to notice a mismatch before dispatching the next parsed
// response) — an inherent limitation of matching-by-order pipelining
// shared with the TypeScript SDK's Connection, not something this
// SDK introduces.
func (c *connection) mismatch(marker byte) error {
	err := connectionLost(
		fmt.Sprintf("response %q does not match the request (connection desynced)", marker), nil)
	c.poison(err)
	return err
}

// poison marks the connection closed, closes the socket, and rejects
// every still-pending request with err. Safe to call more than once —
// from a writer noticing a failed Write, the read loop noticing a failed
// Read, or an explicit close() — only the first call has any effect.
func (c *connection) poison(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.lastErr = err
	pending := c.pending
	c.pending = nil
	onClose := c.onClose
	c.mu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
	}
	for _, ch := range pending {
		ch <- roundTripResult{err: err}
	}
	if onClose != nil {
		onClose()
	}
}

// request sends frame and waits for its matched response. Pushing onto
// the pending queue and writing the frame happen under the same lock, so
// concurrent callers' queue order always matches the order their frames
// actually hit the wire — required for the read loop's FIFO dispatch to
// stay correct.
func (c *connection) request(frame []byte) (byte, []byte, error) {
	resultCh := make(chan roundTripResult, 1)

	c.mu.Lock()
	if c.closed {
		err := c.lastErr
		c.mu.Unlock()
		if err == nil {
			err = connectionLost("connection is closed", nil)
		}
		return 0, nil, err
	}
	c.lastUsed = time.Now()
	c.pending = append(c.pending, resultCh)
	// requestTimeout bounds this request while it's outstanding; reset
	// on every new request so the deadline always reflects the newest
	// thing waiting on an answer. readLoop clears it once nothing is
	// outstanding, so an idle connection is never closed by this alone.
	_ = c.conn.SetDeadline(time.Now().Add(requestTimeout))
	_, writeErr := c.conn.Write(frame)
	c.mu.Unlock()

	if writeErr != nil {
		err := connectionLost("connection failed", writeErr)
		c.poison(err)
		return 0, nil, err
	}

	result := <-resultCh
	if result.err != nil {
		return 0, nil, result.err
	}
	return result.marker, result.value, nil
}

// readLoop consumes responses off the wire for as long as the connection
// stays open, dispatching each to the oldest pending request (FIFO —
// doc/adr/0016-*.md). It is this connection's only reader; nothing else
// may read from conn.
func (c *connection) readLoop() {
	for {
		marker, value, err := c.readOneResponse()
		if err != nil {
			c.poison(connectionLost("connection failed", err))
			return
		}

		c.mu.Lock()
		wasEmpty := len(c.pending) == 0
		var ch chan roundTripResult
		if !wasEmpty {
			ch = c.pending[0]
			c.pending = c.pending[1:]
		}
		noneOutstanding := len(c.pending) == 0
		c.mu.Unlock()

		// An unsolicited "busy" response means the server hit its
		// connection limit right after accept and is about to close the
		// connection; it isn't an answer to anything we sent (mirrors
		// the TypeScript SDK's Connection.onData).
		if marker == 'B' && wasEmpty {
			c.poison(fmt.Errorf("nanocached: server rejected the connection (connection limit reached)"))
			return
		}
		if ch == nil {
			c.poison(fmt.Errorf("nanocached: unsolicited response %q from server (connection desynced)", marker))
			return
		}
		if noneOutstanding {
			// Nothing left waiting on an answer: clear requestTimeout's
			// deadline so an otherwise-idle connection is never closed
			// by it (keep-alive pings excepted — they set their own
			// deadline via request() like any other call).
			_ = c.conn.SetDeadline(time.Time{})
		}
		ch <- roundTripResult{marker: marker, value: value}
	}
}

func (c *connection) readOneResponse() (byte, []byte, error) {
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
	case 'S', 'D', 'N', 'W', 'B':
		if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
			return 0, nil, err
		}
		return marker, nil, nil
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
