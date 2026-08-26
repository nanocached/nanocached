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
	"sync/atomic"
	"time"
)

// connection is one already-identified connection to a single
// nanocached-node, speaking the cache protocol (G/S/D — the A identify
// exchange happens in identify.go before a connection exists). Requests
// are pipelined onto the socket and matched to responses in send order
// (request pipelining): a dedicated read loop consumes responses and
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

// transientRetryDelays is the retryable-error status `R` (issue #125)
// bounded retry budget: up to 2 retries (3 attempts total) at a single
// request, sleeping 50ms before the first retry and 100ms before the
// second. len(transientRetryDelays) is the number of retries available;
// index i is the sleep before retry i+1. A var only so tests can shorten
// it, matching connectDeadline/requestTimeout's own convention.
var transientRetryDelays = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}

type roundTripResult struct {
	marker byte
	value  []byte
	// ttlSeconds is only meaningful for an `I` (INCR, issue #129) response:
	// -1 means the entry has no TTL, N >= 0 is its remaining TTL in whole
	// seconds. Unused (zero value) for every other marker.
	ttlSeconds int64
	err        error
}

// pendingRequest is one still-outstanding request: the channel its result
// is delivered on, paired with the tag (echoed response tags) its response
// must echo. tag is meaningless when the connection is untagged.
type pendingRequest struct {
	ch  chan roundTripResult
	tag uint32
}

type connection struct {
	mu     sync.Mutex
	conn   net.Conn // nil only for the pre-poisoned placeholder
	reader *bufio.Reader
	// tagged (echoed response tags): negotiated during identify — when true,
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
	// transientRetries, when set, is incremented once for every `R`
	// response this connection receives (issue #125) — the Client's
	// Stats().TransientRetries counter. nil in tests that build a
	// connection directly without a Client (deadConnection, some unit
	// tests), where retries still work, just uncounted.
	transientRetries *atomic.Uint64
}

// onClose is taken here, not assigned afterward, so it's fully set
// before the read loop goroutine — started before newConnection even
// returns — can possibly read it in poison(). transientRetries (issue
// #125) may be nil — see the connection field's doc.
func newConnection(conn net.Conn, onClose func(), tagged bool, transientRetries *atomic.Uint64) *connection {
	c := &connection{
		conn:             conn,
		reader:           bufio.NewReader(conn),
		tagged:           tagged,
		lastUsed:         time.Now(),
		onClose:          onClose,
		transientRetries: transientRetries,
	}
	go c.readLoop()
	return c
}

// appendTagField appends a request's echoed response tags tag as its
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

// get sends the default-namespace `G` frame — equivalent to
// getNS(nil, key), kept as its own method purely so the many call sites
// that predate namespaces (issue #105) don't have to pass one.
func (c *connection) get(key []byte) ([]byte, bool, error) {
	return c.getNS(nil, key)
}

// getNS is get scoped to namespace: a `g` frame when namespace is
// non-empty, the byte-for-byte legacy `G` frame otherwise (see
// appendGetFrame — the SDK rule that the default namespace must never
// change the wire format at all, so an unmodified server keeps working).
// The response markers (V/N/W) are identical either way — namespaced
// commands answer exactly like their uppercase counterparts.
func (c *connection) getNS(namespace, key []byte) ([]byte, bool, error) {
	marker, value, _, err := c.request(func(tag uint32) []byte {
		return appendGetFrame(namespace, key, c.tagged, tag)
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

// appendGetFrame builds a G/g request frame. An empty namespace emits
// `G <key-len>[ <tag>]\n<key>` — byte-for-byte what a pre-namespace
// client sends, so the default namespace never depends on the server
// having learned about namespaces at all. A non-empty namespace emits
// `g <ns-len> <key-len>[ <tag>]\n<ns><key>` (docs/protocol.html's
// "g / s / d — namespaced get, set, delete").
func appendGetFrame(namespace, key []byte, tagged bool, tag uint32) []byte {
	var frame []byte
	if len(namespace) == 0 {
		frame = append([]byte("G "), strconv.AppendInt(nil, int64(len(key)), 10)...)
	} else {
		frame = append([]byte("g "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(key)), 10)
	}
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	frame = append(frame, namespace...)
	return append(frame, key...)
}

// set sends the default-namespace `S` frame — equivalent to
// setNS(nil, key, value, ttlSeconds).
func (c *connection) set(key, value []byte, ttlSeconds int64) error {
	return c.setNS(nil, key, value, ttlSeconds)
}

// setNS is set scoped to namespace (issue #105) — see getNS.
func (c *connection) setNS(namespace, key, value []byte, ttlSeconds int64) error {
	marker, _, _, err := c.request(func(tag uint32) []byte {
		return appendSetFrame(namespace, key, value, ttlSeconds, c.tagged, tag)
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

// appendSetFrame builds an S/s request frame; see appendGetFrame for the
// legacy-vs-namespaced split. The optional TTL field sits ahead of the
// tag in both forms — `s <ns-len> <key-len> <val-len> [<ttl>] [<tag>]`.
func appendSetFrame(namespace, key, value []byte, ttlSeconds int64, tagged bool, tag uint32) []byte {
	var frame []byte
	if len(namespace) == 0 {
		frame = append([]byte("S "), strconv.AppendInt(nil, int64(len(key)), 10)...)
	} else {
		frame = append([]byte("s "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(key)), 10)
	}
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(value)), 10)
	if ttlSeconds >= 0 {
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, ttlSeconds, 10)
	}
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	frame = append(frame, namespace...)
	frame = append(frame, key...)
	return append(frame, value...)
}

// delete sends the default-namespace `D` frame — equivalent to
// deleteNS(nil, key).
func (c *connection) delete(key []byte) (bool, error) {
	return c.deleteNS(nil, key)
}

// deleteNS is delete scoped to namespace (issue #105) — see getNS.
func (c *connection) deleteNS(namespace, key []byte) (bool, error) {
	marker, _, _, err := c.request(func(tag uint32) []byte {
		return appendDeleteFrame(namespace, key, c.tagged, tag)
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

// appendDeleteFrame builds a D/d request frame; see appendGetFrame.
func appendDeleteFrame(namespace, key []byte, tagged bool, tag uint32) []byte {
	var frame []byte
	if len(namespace) == 0 {
		frame = append([]byte("D "), strconv.AppendInt(nil, int64(len(key)), 10)...)
	} else {
		frame = append([]byte("d "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(key)), 10)
	}
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	frame = append(frame, namespace...)
	return append(frame, key...)
}

// incrNS sends the `i` frame (issue #129) — INCR has no legacy/uppercase
// form the way G/S/D do (see appendGetFrame): it is always namespaced,
// even the default namespace, exactly like clear's `c`/`F`
// (appendClearFrame), so there is no unnamespaced incr wrapper to pair
// with it. ok is false with a nil err on a clean miss (`N`) — the same
// "not found" shape getNS returns. A non-numeric stored value or an
// overflowing delta answers `T`, surfaced as ErrNotNumeric. ttlSeconds
// mirrors setNS's own wire convention (-1 means no TTL, N >= 0 is the
// remaining TTL in whole seconds), so a hit's ttlSeconds can be handed
// straight to a replica's setNS call to forward the literal result
// (client.go's Incr — replicas never replay `i`, see its doc comment).
func (c *connection) incrNS(namespace, key []byte, delta int64) (value []byte, ttlSeconds int64, ok bool, err error) {
	marker, raw, ttl, err := c.request(func(tag uint32) []byte {
		return appendIncrFrame(namespace, key, delta, c.tagged, tag)
	})
	if err != nil {
		return nil, -1, false, err
	}
	switch marker {
	case 'I':
		return raw, ttl, true, nil
	case 'N':
		return nil, -1, false, nil
	case 'T':
		return nil, -1, false, ErrNotNumeric
	case 'W':
		return nil, -1, false, ErrWrongNode
	default:
		return nil, -1, false, c.mismatch(marker)
	}
}

// appendIncrFrame builds an `i` request frame: `i <ns-len> <key-len>
// <delta>[ <tag>]\n<ns><key>` — always namespaced (ns-len 0 for the
// default namespace), unlike appendGetFrame/appendSetFrame/
// appendDeleteFrame's legacy-vs-namespaced split (see incrNS). delta is
// signed decimal — strconv.AppendInt already emits the canonical form the
// wire contract requires (optional leading '-', no leading zeros, no
// '+').
func appendIncrFrame(namespace, key []byte, delta int64, tagged bool, tag uint32) []byte {
	frame := append([]byte("i "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(key)), 10)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, delta, 10)
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	frame = append(frame, namespace...)
	return append(frame, key...)
}

// clear drops every entry in namespace (issue #106) — an empty namespace
// clears the default namespace (`c 0\n`), never rejected: see
// appendClearFrame. Unlike get/set/delete there is no key involved and
// no `W` ever answers (docs/protocol.html's "c / F — clear a namespace,
// flush everything": neither command is key-addressed), so the only
// well-formed reply is `C`; anything else is a mismatch.
func (c *connection) clear(namespace []byte) error {
	marker, _, _, err := c.request(func(tag uint32) []byte {
		return appendClearFrame(namespace, c.tagged, tag)
	})
	if err != nil {
		return err
	}
	if marker != 'C' {
		return c.mismatch(marker)
	}
	return nil
}

// clearAll drops every namespace, the default one included (issue #106's
// `F` — see clear).
func (c *connection) clearAll() error {
	marker, _, _, err := c.request(func(tag uint32) []byte {
		return appendClearAllFrame(c.tagged, tag)
	})
	if err != nil {
		return err
	}
	if marker != 'C' {
		return c.mismatch(marker)
	}
	return nil
}

// appendClearFrame builds a `c` request frame: `c <ns-len>[ <tag>]\n<ns>`.
// There is no separate legacy/uppercase form the way G/S/D have — clear
// isn't key-addressed, so it always names its namespace explicitly, even
// the default one (ns-len 0), and never had a pre-namespace wire shape to
// preserve.
func appendClearFrame(namespace []byte, tagged bool, tag uint32) []byte {
	frame := append([]byte("c "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	return append(frame, namespace...)
}

// appendClearAllFrame builds an `F` request frame: `F[ <tag>]\n` — no
// body, since it addresses every namespace at once.
func appendClearAllFrame(tagged bool, tag uint32) []byte {
	frame := appendTagField([]byte("F"), tagged, tag)
	return append(frame, '\n')
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

// request builds a frame via build, sends it, and waits for its matched
// response — transparently retrying on the retryable-error status `R`
// (issue #125): up to len(transientRetryDelays) retries of the SAME
// request on the SAME connection (no redial, no poison — see attemptRequest's
// callers for why `R` never reaches the marker switches in get/set/
// delete/clear), sleeping transientRetryDelays[attempt] before each
// retry. build is called again for each retry (a fresh tag on a tagged
// connection); the request/response semantics are otherwise identical to
// a single round trip, so every caller (G/S/D/g/s/d/c/F alike, including
// a hedge leg's own connection — issue #64) gets the bounded retry for
// free just by going through request(). Every `R` received — whether the
// retry that follows it succeeds or not — bumps transientRetries. If the
// budget is exhausted (every attempt answered `R`), returns ErrRetryable
// without touching the connection's open/closed state.
func (c *connection) request(build func(tag uint32) []byte) (byte, []byte, int64, error) {
	for attempt := 0; ; attempt++ {
		marker, value, ttlSeconds, err := c.attemptRequest(build)
		if err != nil {
			return 0, nil, 0, err
		}
		if marker != 'R' {
			return marker, value, ttlSeconds, nil
		}
		if c.transientRetries != nil {
			c.transientRetries.Add(1)
		}
		if attempt >= len(transientRetryDelays) {
			return 0, nil, 0, retryableFailed(fmt.Sprintf(
				"request failed transiently (server answered R) %d times in a row", attempt+1))
		}
		time.Sleep(transientRetryDelays[attempt])
	}
}

// attemptRequest is a single request/response round trip — request's
// retry-on-`R` loop body. See request for the retry semantics built on
// top of this.
func (c *connection) attemptRequest(build func(tag uint32) []byte) (byte, []byte, int64, error) {
	resultCh := make(chan roundTripResult, 1)

	c.mu.Lock()
	if c.closed {
		err := c.lastErr
		c.mu.Unlock()
		if err == nil {
			err = connectionLost("connection is closed", nil)
		}
		return 0, nil, 0, err
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
		return 0, nil, 0, err
	}

	result := <-resultCh
	if result.err != nil {
		return 0, nil, 0, result.err
	}
	return result.marker, result.value, result.ttlSeconds, nil
}

// readLoop consumes responses off the wire for as long as the connection
// stays open, dispatching each to the oldest pending request (FIFO —
// Request pipelining). It is this connection's only reader; nothing else
// may read from conn.
func (c *connection) readLoop() {
	for {
		marker, value, tag, ttlSeconds, err := c.readOneResponse()
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

		// Echoed response tags: on a tagged connection, verify the echoed tag
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

		req.ch <- roundTripResult{marker: marker, value: value, ttlSeconds: ttlSeconds}
	}
}

// readOneResponse reads one response frame off the wire. tag is only
// meaningful (and only present on the wire at all — echoed response tags) for
// non-busy responses on a tagged connection; callers gate on c.tagged the
// same way readOneResponse itself does. ttlSeconds is only meaningful for
// an `I` response (issue #129's INCR) — see roundTripResult's doc comment.
func (c *connection) readOneResponse() (marker byte, value []byte, tag uint32, ttlSeconds int64, err error) {
	marker, err = c.reader.ReadByte()
	if err != nil {
		return 0, nil, 0, 0, err
	}
	switch marker {
	case 'V':
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, err
		}
		// Untagged wire: `V <len>\n`. Tagged: `V <len> <seq>\n`
		// (echoed response tags). After the marker byte the header still
		// carries the leading space.
		fields := strings.Fields(header)
		wantFields := 1
		if c.tagged {
			wantFields = 2
		}
		if len(fields) != wantFields {
			return 0, nil, 0, 0, protocolError("invalid value header in response")
		}
		// Lengths beyond the server's own 1 MiB request cap are protocol
		// garbage — reject before allocating.
		length, err := strconv.Atoi(fields[0])
		if err != nil || length < 0 || length > maxValueLength {
			return 0, nil, 0, 0, protocolError("invalid value length in response")
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(fields[1])
			if err != nil {
				return 0, nil, 0, 0, err
			}
		}
		value := make([]byte, length)
		if _, err := readFull(c.reader, value); err != nil {
			return 0, nil, 0, 0, err
		}
		return marker, value, responseTag, 0, nil
	// `I` is INCR's success response (issue #129): `I <value-length>
	// [<ttl-seconds>] [<tag>]\n<value>` — the same "trailing optional
	// field(s), tagged-mode-aware" shape S's own request-side [ttl] [tag]
	// ordering has (appendSetFrame), mirrored here for parsing: on an
	// untagged connection 0 trailing fields after <value-length> means no
	// TTL, 1 means TTL present; on a tagged connection 1 trailing field
	// means "just the tag, no TTL", 2 means "ttl then tag" — disambiguated
	// purely by whether the connection is tagged, never guessed frame by
	// frame.
	case 'I':
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, err
		}
		fields := strings.Fields(header)
		minFields, maxFields := 1, 2
		if c.tagged {
			minFields, maxFields = 2, 3
		}
		if len(fields) < minFields || len(fields) > maxFields {
			return 0, nil, 0, 0, protocolError("invalid incr header in response")
		}
		length, err := strconv.Atoi(fields[0])
		if err != nil || length < 0 || length > maxValueLength {
			return 0, nil, 0, 0, protocolError("invalid incr value length in response")
		}
		trailing := fields[1:]
		hasTTL := len(trailing) == maxFields-1
		responseTTL := int64(-1)
		if hasTTL {
			responseTTL, err = strconv.ParseInt(trailing[0], 10, 64)
			if err != nil || responseTTL < 0 {
				return 0, nil, 0, 0, protocolError("invalid incr ttl in response")
			}
			trailing = trailing[1:]
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(trailing[0])
			if err != nil {
				return 0, nil, 0, 0, err
			}
		}
		value := make([]byte, length)
		if _, err := readFull(c.reader, value); err != nil {
			return 0, nil, 0, 0, err
		}
		return marker, value, responseTag, responseTTL, nil
	case 'B':
		// Busy is always untagged (echoed response tags) — it's an
		// unsolicited response sent whether or not this connection
		// negotiated tags.
		if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
			return 0, nil, 0, 0, err
		}
		return marker, nil, 0, 0, nil
	case 'S', 'D', 'N', 'W', 'C', 'R', 'T':
		if !c.tagged {
			if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
				return 0, nil, 0, 0, err
			}
			return marker, nil, 0, 0, nil
		}
		// Tagged wire: `S <seq>\n` etc. (echoed response tags) — `C`
		// (issue #106's clear/flush ack), `R` (issue #125's retryable-error
		// status — `R <tag>\n`, pairing it to the request it answers
		// exactly like any other response), and `T` (issue #129's INCR
		// not-numeric/overflow status) included.
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, err
		}
		header = strings.TrimSuffix(header, "\n")
		field, ok := strings.CutPrefix(header, " ")
		if !ok {
			return 0, nil, 0, 0, protocolError("response is missing its tag (connection desynced)")
		}
		responseTag, err := parseTag(field)
		if err != nil {
			return 0, nil, 0, 0, err
		}
		return marker, nil, responseTag, 0, nil
	default:
		return 0, nil, 0, 0, protocolError(fmt.Sprintf("unexpected response from server: %c", marker))
	}
}

// parseTag parses a response's echoed response tags echoed tag — a u32
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
