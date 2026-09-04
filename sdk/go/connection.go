package nanocached

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
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

// maxMultiGetResponseBytes bounds the sum of every hit's declared length
// across one `M` (multi-get) reply (issue #207, a follow-up to #179's
// Java fix in PR #201). Each individual hit's length is already capped
// at maxValueLength above, but that alone doesn't bound the reply as a
// whole: a node answering a 400-key multi-get with 400 x maxValueLength
// hits would force ~800 MB of allocation from a single reply. Reuses
// compression.go's maxDecompressedLength figure (issue #41) rather than
// inventing a new one. A var, not a const, only so a test can shrink it
// and exercise the bound without actually moving tens of megabytes over
// a loopback socket — matching requestTimeout/transientRetryDelays'
// own test-overridable convention above.
var maxMultiGetResponseBytes int64 = 64 * 1024 * 1024

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
	// entries is only meaningful for an `M` (multi-get) or `O` (multi-set)
	// response (issues #128/#150/#151) — see multiEntry's own doc comment.
	// nil for every other marker.
	entries []multiEntry
	err     error
}

// multiEntry is one key's outcome inside an `M` (multi-get) or `O`
// (multi-set) response (issues #128/#150/#151,
// docs/protocol.html#multi) — a batch never fails as a whole, so each
// key's result is independent of every other key's, same as the
// server's own Response::Multi/Response::MultiAck. Reused for both
// response kinds rather than two near-identical types:
//   - `M`: ok+value is a hit (value holds the bytes, possibly empty);
//     wrongNode is a per-key `W`; neither set is a clean miss (`-`).
//   - `O`: ok is `S` (stored), wrongNode is `W`; value is always nil —
//     a set has nothing to echo back.
type multiEntry struct {
	value     []byte
	ok        bool
	wrongNode bool
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
	marker, value, _, _, err := c.request(func(tag uint32) []byte {
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
	marker, _, _, _, err := c.request(func(tag uint32) []byte {
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
	marker, _, _, _, err := c.request(func(tag uint32) []byte {
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
	marker, raw, ttl, _, err := c.request(func(tag uint32) []byte {
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

// casNS sends the `k` frame (issue #141: compare-and-set) — like INCR
// (see incrNS), `k` has no legacy/uppercase form: it is always
// namespaced, even for the default namespace. cond is one of
// casCondAbsent ("A"), casCondPresent ("P"), or a 32-character lowercase
// hex content digest (see ContentDigest) — a bare token, never
// length-prefixed; its own shape identifies it
// (docs/protocol.html#cas). Success answers `S` (true, the same
// acknowledgement a plain Set gives); a condition mismatch answers `N`
// (false) — the very same "nothing here to act on" shape a miss already
// uses elsewhere, so `k` introduces no new response marker. ttlSeconds
// mirrors setNS's own wire convention (-1 means no expiry, N > 0 sets
// one); unlike incrNS there is no old TTL to preserve, since the caller
// supplies the whole new value.
func (c *connection) casNS(namespace, key, value []byte, cond string, ttlSeconds int64) (bool, error) {
	marker, _, _, _, err := c.request(func(tag uint32) []byte {
		return appendCasFrame(namespace, key, value, cond, ttlSeconds, c.tagged, tag)
	})
	if err != nil {
		return false, err
	}
	switch marker {
	case 'S':
		return true, nil
	case 'N':
		return false, nil
	case 'W':
		return false, ErrWrongNode
	default:
		return false, c.mismatch(marker)
	}
}

// appendCasFrame builds a `k` request frame: `k <ns-len> <key-len>
// <val-len> <cond> [<ttl>][ <tag>]\n<ns><key><value>` (docs/protocol.html#cas).
// cond is a bare token (A, P, or a digest) appended as-is — the one field
// in this frame that isn't length-prefixed, since its own shape (a single
// letter versus 32 hex characters) identifies which kind it is.
func appendCasFrame(namespace, key, value []byte, cond string, ttlSeconds int64, tagged bool, tag uint32) []byte {
	frame := append([]byte("k "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(key)), 10)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(value)), 10)
	frame = append(frame, ' ')
	frame = append(frame, cond...)
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

// deleteIfMatchesNS sends the `x` frame (issue #141): the two-argument
// remove(key, old). digest is always a 32-character lowercase hex content
// digest here — never A/P, since an absent- or present-only conditioned
// delete is already the plain, unconditional D/d
// (docs/protocol.html#cas). Success answers `D` (true, the same
// acknowledgement a plain delete gives for a key that existed); a
// mismatch or missing key answers `N` (false) — the same status a plain
// delete already gives when there was nothing to delete.
func (c *connection) deleteIfMatchesNS(namespace, key []byte, digest string) (bool, error) {
	marker, _, _, _, err := c.request(func(tag uint32) []byte {
		return appendDeleteIfMatchesFrame(namespace, key, digest, c.tagged, tag)
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

// appendDeleteIfMatchesFrame builds an `x` request frame: `x <ns-len>
// <key-len> <cond>[ <tag>]\n<ns><key>` — cond is always a digest here, the
// one non-length-prefixed field, exactly like appendCasFrame's own cond.
func appendDeleteIfMatchesFrame(namespace, key []byte, digest string, tagged bool, tag uint32) []byte {
	frame := append([]byte("x "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(key)), 10)
	frame = append(frame, ' ')
	frame = append(frame, digest...)
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
	marker, _, _, _, err := c.request(func(tag uint32) []byte {
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
	marker, _, _, _, err := c.request(func(tag uint32) []byte {
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

// multiGetNS sends the `m` frame — batched get (issues #128/#150/#151):
// n keys under one round trip through the cache, instead of n
// independent g/G frames. entries[i] answers keys[i], in request order
// (docs/protocol.html#multi) — a batch never fails as a whole, so a
// per-key `W` never turns into an error here; only a malformed reply or
// a roster whose length disagrees with the request does (the streams
// are desynced at that point, same as mismatch()).
func (c *connection) multiGetNS(namespace []byte, keys [][]byte) ([]multiEntry, error) {
	marker, _, _, entries, err := c.request(func(tag uint32) []byte {
		return appendMultiGetFrame(namespace, keys, c.tagged, tag)
	})
	if err != nil {
		return nil, err
	}
	if marker != 'M' {
		return nil, c.mismatch(marker)
	}
	if len(entries) != len(keys) {
		desyncErr := connectionLost(fmt.Sprintf(
			"multi-get response roster length %d does not match request key count %d (connection desynced)",
			len(entries), len(keys)), nil)
		c.poison(desyncErr)
		return nil, desyncErr
	}
	return entries, nil
}

// appendMultiGetFrame builds an `m` request frame: `m <ns-len> <n>
// <key-len-1> ... <key-len-n>[ <tag>]\n<ns><key-1>...<key-n>`
// (docs/protocol.html#multi) — always namespaced, like appendIncrFrame,
// never appendGetFrame's legacy-vs-namespaced split: `m` has no
// pre-batching wire form to stay compatible with.
func appendMultiGetFrame(namespace []byte, keys [][]byte, tagged bool, tag uint32) []byte {
	frame := append([]byte("m "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(keys)), 10)
	for _, key := range keys {
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(key)), 10)
	}
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	frame = append(frame, namespace...)
	for _, key := range keys {
		frame = append(frame, key...)
	}
	return frame
}

// multiSetNS sends the `o` frame — batched set (issues #150/#151): n
// keys stored under one round trip, one shared ttlSeconds for the whole
// batch rather than per key (docs/protocol.html#multi). entries[i]
// answers keys[i]/values[i], in request order; see multiGetNS for the
// same "only a desynced roster is an error" stance.
func (c *connection) multiSetNS(namespace []byte, keys, values [][]byte, ttlSeconds int64) ([]multiEntry, error) {
	marker, _, _, entries, err := c.request(func(tag uint32) []byte {
		return appendMultiSetFrame(namespace, keys, values, ttlSeconds, c.tagged, tag)
	})
	if err != nil {
		return nil, err
	}
	if marker != 'O' {
		return nil, c.mismatch(marker)
	}
	if len(entries) != len(keys) {
		desyncErr := connectionLost(fmt.Sprintf(
			"multi-set response roster length %d does not match request key count %d (connection desynced)",
			len(entries), len(keys)), nil)
		c.poison(desyncErr)
		return nil, desyncErr
	}
	return entries, nil
}

// appendMultiSetFrame builds an `o` request frame: `o <ns-len> <n>
// <key-len-1> <value-len-1> ... <key-len-n> <value-len-n> [<ttl>][ <tag>]
// \n<ns><key-1><value-1>...<key-n><value-n>` (docs/protocol.html#multi).
// Always namespaced, same class as appendMultiGetFrame. The optional TTL
// sits ahead of the tag, same convention as appendSetFrame's own [ttl].
func appendMultiSetFrame(namespace []byte, keys, values [][]byte, ttlSeconds int64, tagged bool, tag uint32) []byte {
	frame := append([]byte("o "), strconv.AppendInt(nil, int64(len(namespace)), 10)...)
	frame = append(frame, ' ')
	frame = strconv.AppendInt(frame, int64(len(keys)), 10)
	for i, key := range keys {
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(key)), 10)
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, int64(len(values[i])), 10)
	}
	if ttlSeconds >= 0 {
		frame = append(frame, ' ')
		frame = strconv.AppendInt(frame, ttlSeconds, 10)
	}
	frame = appendTagField(frame, tagged, tag)
	frame = append(frame, '\n')
	frame = append(frame, namespace...)
	for i, key := range keys {
		frame = append(frame, key...)
		frame = append(frame, values[i]...)
	}
	return frame
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
func (c *connection) request(build func(tag uint32) []byte) (byte, []byte, int64, []multiEntry, error) {
	for attempt := 0; ; attempt++ {
		marker, value, ttlSeconds, entries, err := c.attemptRequest(build)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		if marker != 'R' {
			return marker, value, ttlSeconds, entries, nil
		}
		if c.transientRetries != nil {
			c.transientRetries.Add(1)
		}
		if attempt >= len(transientRetryDelays) {
			return 0, nil, 0, nil, retryableFailed(fmt.Sprintf(
				"request failed transiently (server answered R) %d times in a row", attempt+1))
		}
		time.Sleep(transientRetryDelays[attempt])
	}
}

// attemptRequest is a single request/response round trip — request's
// retry-on-`R` loop body. See request for the retry semantics built on
// top of this.
//
// Issue #225: every failure this returns is classified as either
// "definitely not sent" (errRequestNotSent, via notSent below — the
// connection was already closed before this attempt even tried to
// write, or the Write() call itself failed) or "possibly sent" (the
// frame was handed to the socket successfully and some later failure —
// no reply, a desynced/malformed response — lost the outcome). Only
// applyNonIdempotent (client.go) reads this distinction; every other
// caller of request()/attemptRequest still just sees ErrConnectionLost/
// ErrProtocol exactly as before.
func (c *connection) attemptRequest(build func(tag uint32) []byte) (byte, []byte, int64, []multiEntry, error) {
	resultCh := make(chan roundTripResult, 1)

	c.mu.Lock()
	if c.closed {
		err := c.lastErr
		c.mu.Unlock()
		if err == nil {
			err = connectionLost("connection is closed", nil)
		}
		// This attempt's frame was never written — regardless of what
		// originally poisoned the connection (which may itself have been
		// a "possibly sent" failure for a DIFFERENT, earlier request),
		// THIS request never got a chance to reach the wire.
		return 0, nil, 0, nil, notSent(err)
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
		// The Write() call itself failed: no complete frame reached the
		// server, so it could not have executed this request. Safe to
		// retry after a redial.
		return 0, nil, 0, nil, notSent(err)
	}

	// From here on the frame was fully handed to the socket — Write()'s
	// all-or-error contract means the server may already have received
	// and acted on it. result.err (poisoned by readLoop, a mismatch, or a
	// tag desync) is therefore surfaced WITHOUT the notSent marker: a
	// non-idempotent caller must not replay it.
	result := <-resultCh
	if result.err != nil {
		return 0, nil, 0, nil, result.err
	}
	return result.marker, result.value, result.ttlSeconds, result.entries, nil
}

// readLoop consumes responses off the wire for as long as the connection
// stays open, dispatching each to the oldest pending request (FIFO —
// Request pipelining). It is this connection's only reader; nothing else
// may read from conn.
func (c *connection) readLoop() {
	for {
		marker, value, tag, ttlSeconds, entries, err := c.readOneResponse()
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

		// A "busy" response means the server hit its connection limit
		// right after accept and is about to close the connection; it
		// isn't an answer to anything we sent (mirrors the TypeScript
		// SDK's Connection.onData). This holds regardless of whether a
		// request happens to be pending: `B` is always an untagged,
		// protocol-level signal, never an echoed answer (issue #334) —
		// treating it as one only when wasEmpty let a `B` arriving
		// mid-stream, with a request already pending, be misdelivered to
		// the oldest pending request below and poison the connection via
		// the generic mismatch path instead of this dedicated one.
		if marker == 'B' {
			err := fmt.Errorf("nanocached: server rejected the connection (connection limit reached)")
			c.poison(err)
			if haveReq {
				// req has already been shifted out of c.pending above, so
				// poison()'s own rejection sweep won't reach it — reject it
				// here, same as the tag-mismatch case below.
				req.ch <- roundTripResult{err: err}
			}
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

		req.ch <- roundTripResult{marker: marker, value: value, ttlSeconds: ttlSeconds, entries: entries}
	}
}

// readOneResponse reads one response frame off the wire. tag is only
// meaningful (and only present on the wire at all — echoed response tags) for
// non-busy responses on a tagged connection; callers gate on c.tagged the
// same way readOneResponse itself does. ttlSeconds is only meaningful for
// an `I` response (issue #129's INCR) — see roundTripResult's doc comment.
func (c *connection) readOneResponse() (marker byte, value []byte, tag uint32, ttlSeconds int64, entries []multiEntry, err error) {
	marker, err = c.reader.ReadByte()
	if err != nil {
		return 0, nil, 0, 0, nil, err
	}
	switch marker {
	case 'V':
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, nil, err
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
			return 0, nil, 0, 0, nil, protocolError("invalid value header in response")
		}
		// Lengths beyond the server's own 1 MiB request cap are protocol
		// garbage — reject before allocating.
		length, err := parseStrictInt(fields[0])
		if err != nil || length < 0 || length > maxValueLength {
			return 0, nil, 0, 0, nil, protocolError("invalid value length in response")
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(fields[1])
			if err != nil {
				return 0, nil, 0, 0, nil, err
			}
		}
		value := make([]byte, length)
		if _, err := readFull(c.reader, value); err != nil {
			return 0, nil, 0, 0, nil, err
		}
		return marker, value, responseTag, 0, nil, nil
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
			return 0, nil, 0, 0, nil, err
		}
		fields := strings.Fields(header)
		minFields, maxFields := 1, 2
		if c.tagged {
			minFields, maxFields = 2, 3
		}
		if len(fields) < minFields || len(fields) > maxFields {
			return 0, nil, 0, 0, nil, protocolError("invalid incr header in response")
		}
		length, err := parseStrictInt(fields[0])
		if err != nil || length < 0 || length > maxValueLength {
			return 0, nil, 0, 0, nil, protocolError("invalid incr value length in response")
		}
		trailing := fields[1:]
		hasTTL := len(trailing) == maxFields-1
		responseTTL := int64(-1)
		if hasTTL {
			responseTTL, err = parseStrictInt64(trailing[0])
			if err != nil || responseTTL < 0 {
				return 0, nil, 0, 0, nil, protocolError("invalid incr ttl in response")
			}
			trailing = trailing[1:]
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(trailing[0])
			if err != nil {
				return 0, nil, 0, 0, nil, err
			}
		}
		value := make([]byte, length)
		if _, err := readFull(c.reader, value); err != nil {
			return 0, nil, 0, 0, nil, err
		}
		return marker, value, responseTag, responseTTL, nil, nil
	// `M` is multi-get's response (issues #128/#150/#151): `M <n>
	// <result-1> ... <result-n>[ <tag>]\n<hit values, concatenated in
	// request order>` (docs/protocol.html#multi). Unlike V/I this has a
	// variable number of header fields (n of them), so it can't reuse
	// those cases' fixed wantFields shape: n is read first, then exactly
	// n roster tokens are expected, then (tagged mode) one more for the
	// tag — anything else is a malformed header. Each token is a decimal
	// byte length (a hit — read that many body bytes next, in order),
	// "-" (a clean miss), or "W" (this key's own wrong-node). The header
	// line is read whole before any body byte, so a lying n can never
	// cause an out-of-bounds read — it only ever fails the wantFields
	// check against the fields readLine's bounded read actually
	// delivered.
	case 'M':
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, nil, err
		}
		fields := strings.Fields(header)
		if len(fields) < 1 {
			return 0, nil, 0, 0, nil, protocolError("invalid multi-get header in response")
		}
		count, err := parseStrictInt(fields[0])
		if err != nil || count < 0 {
			return 0, nil, 0, 0, nil, protocolError("invalid multi-get count in response")
		}
		wantFields := 1 + count
		if c.tagged {
			wantFields++
		}
		if len(fields) != wantFields {
			return 0, nil, 0, 0, nil, protocolError("invalid multi-get header in response")
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(fields[1+count])
			if err != nil {
				return 0, nil, 0, 0, nil, err
			}
		}
		results := make([]multiEntry, count)
		// totalBytes accumulates every hit's declared length across this
		// whole reply (issue #207) — the per-token check above already
		// rejects any single length beyond maxValueLength, but 400 keys
		// each just under that cap would still force ~800 MB of
		// allocation from one reply. Checked before the body is
		// read/allocated, so an oversized claim poisons the connection
		// on the length that pushes the running total over the bound,
		// not after however many bytes it would have taken to read it.
		var totalBytes int64
		for i, token := range fields[1 : 1+count] {
			switch token {
			case "-":
				results[i] = multiEntry{}
			case "W":
				results[i] = multiEntry{wrongNode: true}
			default:
				length, err := parseStrictInt(token)
				if err != nil || length < 0 || length > maxValueLength {
					return 0, nil, 0, 0, nil, protocolError("invalid multi-get result length in response")
				}
				totalBytes += int64(length)
				if totalBytes > maxMultiGetResponseBytes {
					return 0, nil, 0, 0, nil, protocolError(fmt.Sprintf(
						"multi-get response exceeds %d bytes", maxMultiGetResponseBytes))
				}
				hit := make([]byte, length)
				if _, err := readFull(c.reader, hit); err != nil {
					return 0, nil, 0, 0, nil, err
				}
				results[i] = multiEntry{value: hit, ok: true}
			}
		}
		return marker, nil, responseTag, 0, results, nil
	// `O` is multi-set's response (issues #150/#151): `O <n> <result-1>
	// ... <result-n>[ <tag>]\n` — no body, unlike `M`'s hit values (a set
	// has nothing to echo back). Each token is "S" (stored) or "W"
	// (wrong node); parsing otherwise mirrors `M` above. Never confused
	// with the `On`/`OnT` identify reply: identify.go handles that before
	// a connection exists, and no other request ever answers with a
	// leading 'O'.
	//
	// Unlike `M` above, this loop carries no cumulative-bytes bound
	// (issue #207, following #179's Java fix): every token is a
	// fixed-width "S" or "W" with no length-prefixed body to allocate,
	// so the loop is already O(count) regardless of what count is — and
	// count itself is already bounded, by wantFields against the header
	// line's own maxHeaderLineLength cap above, before this loop ever
	// runs.
	case 'O':
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, nil, err
		}
		fields := strings.Fields(header)
		if len(fields) < 1 {
			return 0, nil, 0, 0, nil, protocolError("invalid multi-set header in response")
		}
		count, err := parseStrictInt(fields[0])
		if err != nil || count < 0 {
			return 0, nil, 0, 0, nil, protocolError("invalid multi-set count in response")
		}
		wantFields := 1 + count
		if c.tagged {
			wantFields++
		}
		if len(fields) != wantFields {
			return 0, nil, 0, 0, nil, protocolError("invalid multi-set header in response")
		}
		var responseTag uint32
		if c.tagged {
			responseTag, err = parseTag(fields[1+count])
			if err != nil {
				return 0, nil, 0, 0, nil, err
			}
		}
		results := make([]multiEntry, count)
		for i, token := range fields[1 : 1+count] {
			switch token {
			case "S":
				results[i] = multiEntry{ok: true}
			case "W":
				results[i] = multiEntry{wrongNode: true}
			default:
				return 0, nil, 0, 0, nil, protocolError("invalid multi-set result token in response")
			}
		}
		return marker, nil, responseTag, 0, results, nil
	case 'B':
		// Busy is always untagged (echoed response tags) — it's an
		// unsolicited response sent whether or not this connection
		// negotiated tags.
		if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
			return 0, nil, 0, 0, nil, err
		}
		return marker, nil, 0, 0, nil, nil
	case 'S', 'D', 'N', 'W', 'C', 'R', 'T':
		if !c.tagged {
			if _, err := c.reader.ReadByte(); err != nil { // the trailing '\n'
				return 0, nil, 0, 0, nil, err
			}
			return marker, nil, 0, 0, nil, nil
		}
		// Tagged wire: `S <seq>\n` etc. (echoed response tags) — `C`
		// (issue #106's clear/flush ack), `R` (issue #125's retryable-error
		// status — `R <tag>\n`, pairing it to the request it answers
		// exactly like any other response), and `T` (issue #129's INCR
		// not-numeric/overflow status) included.
		header, err := readLine(c.reader)
		if err != nil {
			return 0, nil, 0, 0, nil, err
		}
		header = strings.TrimSuffix(header, "\n")
		field, ok := strings.CutPrefix(header, " ")
		if !ok {
			return 0, nil, 0, 0, nil, protocolError("response is missing its tag (connection desynced)")
		}
		responseTag, err := parseTag(field)
		if err != nil {
			return 0, nil, 0, 0, nil, err
		}
		return marker, nil, responseTag, 0, nil, nil
	default:
		return 0, nil, 0, 0, nil, protocolError(fmt.Sprintf("unexpected response from server: %c", marker))
	}
}

// parseStrictUint parses s as an unsigned base-10 integer using the wire
// protocol's own grammar for every non-negative integer field it sends
// — length prefixes, item counts, response tags, TTLs: ASCII digits
// only, matching ^[0-9]+$ exactly (issue #462), rejecting a leading `+`,
// leading/trailing whitespace, `_` digit-group separators, an exponent,
// or a leading `-`. Leading zeros ("007") ARE allowed — the server's own
// grammar (src/command.rs's parse_length) loops byte-by-byte over ASCII
// digits with no leading-zero restriction, so this has to match.
// strconv.ParseUint(s, 10, bitSize) already refuses every one of the
// above on its own: unlike strconv.Atoi/ParseInt, ParseUint never
// strips or accepts a sign character at all (only ParseInt does that,
// then delegates to the same digit-only loop) — confirmed empirically
// (ParseUint("+5", 10, 64) errors), not just assumed from the doc
// comment. So this is a deliberate, documented pass-through kept as one
// named function so every call site states its intent instead of each
// one re-deriving that ParseUint alone is already strict enough.
func parseStrictUint(s string, bitSize int) (uint64, error) {
	return strconv.ParseUint(s, 10, bitSize)
}

// parseStrictInt is parseStrictUint for callers that want a plain int
// (slice lengths, item counts) instead of picking a wire bit width —
// same digits-only grammar, with an ErrRange failure for anything that
// wouldn't fit in an int, mirroring strconv.Atoi's own overflow
// contract.
func parseStrictInt(s string) (int, error) {
	v, err := parseStrictUint(s, 64)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt {
		return 0, strconv.ErrRange
	}
	return int(v), nil
}

// parseStrictInt64 is parseStrictInt for the one field carried as int64
// rather than int — the `I` response's TTL — same digits-only grammar
// and overflow contract, bounded to int64 instead.
func parseStrictInt64(s string) (int64, error) {
	v, err := parseStrictUint(s, 64)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, strconv.ErrRange
	}
	return int64(v), nil
}

// parseCounterValue parses an `I` response's <value> body — decimal
// ASCII int64 with an optional single leading `-` (never `+`) — the one
// field in the whole wire protocol allowed to be negative, since it's
// the same grammar the request's own <delta> field uses
// (appendIncrFrame). Matches Python's
// `_INCR_VALUE_RE = re.compile(rb"-?[0-9]{1,19}")` and .NET's
// TryParseWireCounter/ParseTag split (issue #462). strconv.ParseInt
// alone isn't strict enough here either: unlike parseStrictUint's
// callers, this field does need ParseInt's minus-sign handling, but
// ParseInt also accepts a leading `+` that this grammar must still
// reject — so that's checked explicitly before delegating to ParseInt
// for the actual sign/digit/range parsing.
func parseCounterValue(s string) (int64, error) {
	if strings.HasPrefix(s, "+") {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(s, 10, 64)
}

// parseTag parses a response's echoed response tags echoed tag — a u32
// written in decimal — matching protocol.ts's parseTag.
func parseTag(field string) (uint32, error) {
	tag, err := parseStrictUint(field, 32)
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
