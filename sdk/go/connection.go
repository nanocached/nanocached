package nanocached

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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
// server's own request cap is 1 MiB; this constant doubles that as
// headroom, so a claimed length beyond it is definitely a corrupt or
// malicious frame, never just a legitimately large value.
const maxValueLength = 2 * 1024 * 1024

// maxHeaderLineLength caps a response header line (readLine, shared with
// identify.go's discovery node-list headers) before it can grow without
// bound: every real header line (`V <len> <tag>`, `S <tag>`, a discovery
// `N <count> <r>`/entry line, ...) is a few dozen bytes at most, so a
// peer that never sends the terminating '\n' is corrupt or hostile, not
// just slow — 4 KiB is generous headroom while still bounding its memory
// pressure on the client (issue #47 audit; mirrors Rust's
// MAX_HEADER_LINE_LENGTH in connection.rs:36 and maxValueLength's
// rationale for the `V` body above).
const maxHeaderLineLength = 4 * 1024

// requestTimeout bounds how long the connection may go without progress
// while requests are outstanding — each response must arrive within
// this window of the previous one (or of its own send, when the queue
// was empty): without it, a half-open server that accepts the TCP
// connection but never writes back — or stops mid-stream — would hang
// Get/Set/Delete forever in readLoop's blocking Read, wedging every
// other pending caller behind it (and, transitively, Close(), which
// waits on background replica writes).
// Generous versus the server's own 10s outbound timeouts. A variable
// only so tests can shorten it.
var requestTimeout = 30 * time.Second

type roundTripResult struct {
	marker byte
	value  []byte
	err    error
}

// pendingRequest is one still-outstanding request: the channel its result
// is delivered on, paired with the tag (doc/adr/0019-*.md) its response
// must echo. tag is meaningless when the connection is untagged.
type pendingRequest struct {
	ch  chan roundTripResult
	tag uint32
}

type connection struct {
	mu     sync.Mutex
	conn   net.Conn // nil only for the pre-poisoned placeholder
	reader *bufio.Reader
	// tagged (doc/adr/0019-*.md): negotiated during identify — when true,
	// every request carries a tag the server echoes, and readLoop verifies
	// the echo against the oldest pending request before dispatching it.
	tagged   bool
	nextTag  uint32
	pending  []pendingRequest
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
func newConnection(conn net.Conn, onClose func(), tagged bool) *connection {
	c := &connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		tagged:   tagged,
		lastUsed: time.Now(),
		onClose:  onClose,
	}
	go c.readLoop()
	return c
}

// appendTagField appends a request's doc/adr/0019-*.md tag as its
// trailing header field (" <tag>") to buf on a tagged connection, or
// nothing on an untagged one — the same wire shape a fmt.Sprintf(" %d",
// tag) would produce, built here with strconv.AppendUint straight into
// the frame buffer instead, to avoid the allocate-a-string-then-copy-it-in
// round trip fmt.Sprintf costs on every single request (issue #47 audit
// item G2). Untagged mode remains byte-for-byte identical to the
// pre-0019 wire format.
func appendTagField(buf []byte, tagged bool, tag uint32) []byte {
	if !tagged {
		return buf
	}
	buf = append(buf, ' ')
	return strconv.AppendUint(buf, uint64(tag), 10)
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
	marker, value, err := c.request(func(tag uint32) []byte {
		frame := append([]byte("G "), strconv.AppendInt(nil, int64(len(key)), 10)...)
		frame = appendTagField(frame, c.tagged, tag)
		frame = append(frame, '\n')
		return append(frame, key...)
	})
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
	marker, _, err := c.request(func(tag uint32) []byte {
		frame := append([]byte("S "), strconv.AppendInt(nil, int64(len(key)), 10)...)
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(value)), 10)
		if ttlSeconds >= 0 {
			frame = append(frame, ' ')
			frame = strconv.AppendInt(frame, ttlSeconds, 10)
		}
		frame = appendTagField(frame, c.tagged, tag)
		frame = append(frame, '\n')
		frame = append(frame, key...)
		return append(frame, value...)
	})
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
	marker, _, err := c.request(func(tag uint32) []byte {
		frame := append([]byte("D "), strconv.AppendInt(nil, int64(len(key)), 10)...)
		frame = appendTagField(frame, c.tagged, tag)
		frame = append(frame, '\n')
		return append(frame, key...)
	})
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
	for _, req := range pending {
		req.ch <- roundTripResult{err: err}
	}
	if onClose != nil {
		onClose()
	}
}

// request builds a frame via build and waits for its matched response.
// build receives this request's doc/adr/0019-*.md tag (claimed here,
// meaningless when the connection is untagged) so it can render the
// frame's trailing tag field. Claiming the tag, pushing onto the pending
// queue, and writing the frame all happen under the same lock, so
// concurrent callers' tag order and queue order always match the order
// their frames actually hit the wire — required for the read loop's FIFO
// dispatch, and the tag echo it verifies before dispatching, to stay
// correct.
func (c *connection) request(build func(tag uint32) []byte) (byte, []byte, error) {
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
	tag := c.nextTag
	c.nextTag++ // wraps at the uint32's own width, matching the wire
	frame := build(tag)
	c.pending = append(c.pending, pendingRequest{ch: resultCh, tag: tag})
	// requestTimeout is progress-based: the deadline is armed when the
	// queue goes from empty to non-empty, re-armed by readLoop each time
	// a response is dispatched with more still outstanding, and cleared
	// once nothing is. Arming it here on *every* request instead would
	// let a continuous stream of new requests push the deadline forever
	// ahead of a server that has stopped answering — exactly the
	// half-open hang requestTimeout exists to catch. An idle connection
	// is never closed by this alone.
	if len(c.pending) == 1 {
		_ = c.conn.SetDeadline(time.Now().Add(requestTimeout))
	}
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
		marker, value, tag, err := c.readOneResponse()
		if err != nil {
			// A malformed/unexpected frame is already classified as
			// ErrProtocol by readOneResponse — pass it through as-is
			// instead of also rewrapping it as ErrConnectionLost, so
			// callers can tell a protocol violation apart from a genuine
			// I/O failure (issue #47 audit item G4). Everything else here
			// is a real I/O error (EOF, reset, timeout, ...) and keeps the
			// existing ErrConnectionLost classification.
			if errors.Is(err, ErrProtocol) {
				c.poison(err)
			} else {
				c.poison(connectionLost("connection failed", err))
			}
			return
		}

		c.mu.Lock()
		wasEmpty := len(c.pending) == 0
		var req pendingRequest
		haveReq := !wasEmpty
		if haveReq {
			req = c.pending[0]
			c.pending = c.pending[1:]
		}
		noneOutstanding := len(c.pending) == 0
		if haveReq {
			// Progress-based deadline (see request()): a dispatched
			// response is progress, so the next-oldest request gets a
			// fresh window; with nothing left waiting, clear it so an
			// otherwise-idle connection is never closed by it
			// (keep-alive pings excepted — they arm their own deadline
			// via request() like any other call). Under c.mu so this
			// can't race a concurrent request() arming the deadline for
			// a request this locked section didn't see.
			if noneOutstanding {
				_ = c.conn.SetDeadline(time.Time{})
			} else {
				_ = c.conn.SetDeadline(time.Now().Add(requestTimeout))
			}
		}
		c.mu.Unlock()

		// An unsolicited "busy" response means the server hit its
		// connection limit right after accept and is about to close the
		// connection; it isn't an answer to anything we sent (mirrors
		// the TypeScript SDK's Connection.onData).
		if marker == 'B' && wasEmpty {
			c.poison(fmt.Errorf("nanocached: server rejected the connection (connection limit reached)"))
			return
		}
		if !haveReq {
			c.poison(fmt.Errorf("nanocached: unsolicited response %q from server (connection desynced)", marker))
			return
		}

		// doc/adr/0019-*.md: on a tagged connection, verify the echoed tag
		// against the request this response is about to answer — *before*
		// it can reach any caller. A mismatch means the streams are
		// misaligned; unlike the caller-side kind check (mismatch()),
		// catching it here stops the misdelivery instead of merely
		// noticing it later. Busy is always untagged, so it's exempt.
		if c.tagged && marker != 'B' && tag != req.tag {
			err := connectionLost(
				fmt.Sprintf("response tag %d does not answer request tag %d (connection desynced)", tag, req.tag), nil)
			// req has already been shifted out of c.pending, so poison()'s
			// own rejection sweep won't reach it — reject it here; the
			// rest drain when poison() runs.
			c.poison(err)
			req.ch <- roundTripResult{err: err}
			return
		}

		req.ch <- roundTripResult{marker: marker, value: value}
	}
}

// readOneResponse reads one response frame off the wire. tag is only
// meaningful (and only present on the wire at all — doc/adr/0019-*.md) for
// non-busy responses on a tagged connection; callers gate on c.tagged the
// same way readOneResponse itself does.
func (c *connection) readOneResponse() (marker byte, value []byte, tag uint32, err error) {
	marker, err = c.reader.ReadByte()
	if err != nil {
		return 0, nil, 0, err
	}
	switch marker {
	case 'V':
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, err
		}
		// Untagged wire: `V <len>\n`. Tagged: `V <len> <seq>\n`
		// (doc/adr/0019-*.md). After the marker byte the header still
		// carries the leading space.
		fields := strings.Fields(header)
		wantFields := 1
		if c.tagged {
			wantFields = 2
		}
		if len(fields) != wantFields {
			return 0, nil, 0, protocolError("invalid value header in response")
		}
		// Lengths beyond the server's own 1 MiB request cap are protocol
		// garbage — reject before allocating.
		length, err := strconv.Atoi(fields[0])
		if err != nil || length < 0 || length > maxValueLength {
			return 0, nil, 0, protocolError("invalid value length in response")
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(fields[1])
			if err != nil {
				return 0, nil, 0, err
			}
		}
		value := make([]byte, length)
		if _, err := readFull(c.reader, value); err != nil {
			return 0, nil, 0, err
		}
		return marker, value, responseTag, nil
	case 'B':
		// Busy is always untagged (doc/adr/0019-*.md) — it's an
		// unsolicited response sent whether or not this connection
		// negotiated tags.
		if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
			return 0, nil, 0, err
		}
		return marker, nil, 0, nil
	case 'S', 'D', 'N', 'W':
		if !c.tagged {
			if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
				return 0, nil, 0, err
			}
			return marker, nil, 0, nil
		}
		// Tagged wire: `S <seq>\n` etc. (doc/adr/0019-*.md).
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, err
		}
		header = strings.TrimSuffix(header, "\n")
		field, ok := strings.CutPrefix(header, " ")
		if !ok {
			return 0, nil, 0, protocolError("response is missing its tag (connection desynced)")
		}
		responseTag, err := parseTag(field)
		if err != nil {
			return 0, nil, 0, err
		}
		return marker, nil, responseTag, nil
	default:
		return 0, nil, 0, protocolError(fmt.Sprintf("unexpected response from server: %c", marker))
	}
}

// parseTag parses a response's doc/adr/0019-*.md echoed tag — a u32
// written in decimal — matching protocol.ts's parseTag.
func parseTag(field string) (uint32, error) {
	tag, err := strconv.ParseUint(field, 10, 32)
	if err != nil {
		return 0, protocolError("invalid response tag")
	}
	return uint32(tag), nil
}

// readLine reads one '\n'-terminated line from reader, like
// bufio.Reader.ReadString('\n') — the returned string includes the
// trailing '\n' on success, and holds whatever was read so far when err
// is non-nil, matching ReadString's own contract — but bounded: a peer
// that never sends '\n' would otherwise make ReadString's internal
// buffer grow without limit. Mirrors Rust's read_line in connection.rs
// (issue #47 audit); see maxHeaderLineLength above.
func readLine(reader *bufio.Reader) (string, error) {
	var line []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return string(line), err
		}
		line = append(line, b)
		if b == '\n' {
			return string(line), nil
		}
		if len(line) > maxHeaderLineLength {
			return string(line), protocolError(fmt.Sprintf(
				"nanocached: response header line exceeds %d bytes without a terminator", maxHeaderLineLength))
		}
	}
}

// readFull reads exactly len(buf) bytes from reader, wrapping
// io.ReadFull so a final read that delivers the last bytes and an EOF
// (or ErrUnexpectedEOF) together still counts as success — io.ReadFull's
// io.ReadAtLeast forces err to nil once enough bytes have been read,
// regardless of what the underlying Read also returned alongside them.
// The hand-rolled loop this replaced didn't have that: it returned
// whatever error the final Read produced even when that Read had
// delivered every remaining byte (issue #47 audit), which wrongly failed
// a peer that writes the last bytes and closes in the same flush.
func readFull(reader *bufio.Reader, buf []byte) (int, error) {
	return io.ReadFull(reader, buf)
}
