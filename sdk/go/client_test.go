package nanocached

// Integration tests against in-process mock servers speaking just enough
// of the wire protocol — mirrors the other SDKs' mock-based suites.

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testNames = []string{
	"5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
	"0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
}

// addr parses a "host:port" string (as returned by a mock listener's
// Addr().String()) into an Address, for building Config.Addresses in
// tests.
func addr(hostPort string) Address {
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	return Address{Host: host, Port: port}
}

// captureStderr redirects os.Stderr for the duration of fn and returns
// everything written to it. Tests in this package run sequentially (none
// call t.Parallel), so swapping the package-level os.Stderr is safe.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = w
	fn()
	os.Stderr = original
	_ = w.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	_ = r.Close()
	return buf.String()
}

// ── モックノード ──────────────────────────────────────────────────

// storeKey is a mock node's storage key: (namespace, key) pairs are
// stored separately from each other and from an unnamespaced key of the
// same name (issue #105: first-class namespaces) — ns == "" is the
// default namespace, exactly the entry a legacy G/S/D frame (or a `g`/
// `s`/`d` frame with a zero-length namespace) addresses.
type storeKey struct {
	ns  string
	key string
}

type mockNode struct {
	listener         net.Listener
	requiredSecret   []byte
	opts             mockNodeOpts
	store            sync.Map // storeKey -> []byte
	connectionCount  atomic.Int32
	getCount         atomic.Int32
	wrongNodeLeft    atomic.Int32
	setWrongNodeLeft atomic.Int32 // like wrongNodeLeft, but only consumed by S (for isolating a repair write's failure from an unrelated G)
	malformedLeft    atomic.Int32
	storedToGetLeft  atomic.Int32
	wrongTagLeft     atomic.Int32 // echoed response tags: echo the wrong tag on the next G on a tagged connection
	swallowLeft      atomic.Int32 // echoed response tags: swallow the next G entirely (no reply)
	lastSetTTL       atomic.Value // string: the TTL field of the last S, or "none"
	setDelay         atomic.Int64 // nanoseconds; sleep this long before every S reply
	getDelay         atomic.Int64 // nanoseconds; sleep this long before every G reply
	conns            sync.Map     // net.Conn -> struct{}
	silent           atomic.Bool  // once true, every G/S/D is read but never answered
	failClearLeft    atomic.Int32 // issue #106: fail the next N c/F requests (read, then drop the connection with no reply)
	clearCount       atomic.Int32 // issue #106: how many c/F requests this node has received, failed or not
	// retryableLeft is issue #125's "answer the next N data requests with
	// R" knob — consumed by any of G/g/S/s/D/d/c/F, tagged correctly like
	// every other reply.
	retryableLeft atomic.Int32
	// dataRequestCount counts every data command (G/g/S/s/D/d/c/F) this
	// node has read off the wire, R-answered or not — issue #125's tests
	// assert the exact number of attempts a retry sequence made,
	// regardless of which command type they used.
	dataRequestCount atomic.Int32
	// authHeaders records, in order, every `A` header line this node has
	// received (issue #125: lets a test assert the exact probe form —
	// `A <len> T R`, `A <len> T`, or plain `A <len>` — a connect/fallback
	// attempt sent), trailing '\n' stripped.
	authHeadersMu sync.Mutex
	authHeaders   []string
	// ttls records the TTL field (in seconds) of the last S/s that stored
	// each key, keyed the same way store is — storeKey{"", ...} for the
	// default namespace. Absent means the entry has no TTL. Used to answer
	// an `i` (INCR, issue #129) success with its own optional
	// [ttl-seconds] header field, exactly like a real node's INCR reports
	// the entry's remaining TTL.
	ttls sync.Map // storeKey -> int64
	// iCount counts every `i` (INCR) frame this node has received —
	// issue #129's replication test uses it to assert a replica never
	// receives one (only the primary ever runs the increment; replicas
	// get the literal result via an ordinary Set).
	iCount atomic.Int32
	// kCount/xCount count every `k`/`x` (compare-and-set, issue #141)
	// frame this node has received — the CAS replication test uses these
	// to assert a replica never receives either (only the primary ever
	// evaluates <cond>; replicas get the literal result via an ordinary
	// Set/Delete).
	kCount atomic.Int32
	xCount atomic.Int32
	// mCount/oCount count every `m`/`o` (batched get/set, issues
	// #128/#150/#151) frame this node has received.
	mCount atomic.Int32
	oCount atomic.Int32
	// multiWrongNodeKey/multiWrongNodeLeft (issues #128/#150/#151):
	// when multiWrongNodeKey is set, every `m`/`o` roster containing
	// that exact key answers just that key `W` for as long as
	// multiWrongNodeLeft has budget left (consumed one per match,
	// exactly like wrongNodeLeft's own count) — the batched analogue of
	// wrongNodeLeft/setWrongNodeLeft, which answer a whole G/S `W`
	// instead of naming a single key inside a batch.
	multiWrongNodeKey  atomic.Value // string
	multiWrongNodeLeft atomic.Int32
}

// mockNodeOpts configures a startMockNode server's echoed response tags
// (response tags) behavior. Immutable for the server's whole lifetime —
// set once at construction, like requiredSecret — so acceptLoop's
// goroutine never races a test goroutine mutating it later.
type mockNodeOpts struct {
	// supportTags: acknowledge an extended `A ... T` with `OnT\n` and echo
	// tags on that connection's G/S/D replies. Off by default so the bulk
	// of the suite keeps exercising the legacy untagged path.
	supportTags bool
	// closeOnExtendedAuth: behave like a legacy pre-tag server — an
	// extended `A ... T` (or `A ... T R`) is a parse error, so close the
	// connection without replying. The oldest generation: rejects every
	// extension, so the SDK's probe falls all the way back to plain `A`.
	closeOnExtendedAuth bool
	// closeOnRetryableCapability: behave like a server that predates
	// issue #125's `R` capability token but still understands issue #47's
	// `T` — accepts `A <len> T` normally, but treats any `A` with more
	// fields than that (i.e. a trailing `R`) as a parse error and closes
	// without replying. A middle generation, distinct from
	// closeOnExtendedAuth: exercises the new front fallback stage
	// (`A <len> T R` -> `A <len> T`) in isolation, without also falling
	// all the way back to the untagged form.
	closeOnRetryableCapability bool
}

// delaySets makes every future S reply from this node wait d first — for
// tests proving a caller isn't blocked on a slow replica leg
// (fire-and-forget replica writes).
func (m *mockNode) delaySets(d time.Duration) { m.setDelay.Store(int64(d)) }

// delayGets makes every future G reply from this node wait d first — a
// slow-but-alive node, for hedged-read tests (issue #64).
func (m *mockNode) delayGets(d time.Duration) { m.getDelay.Store(int64(d)) }

// goSilentAfterHandshake makes this node a half-open server from this
// point on: it still accepts and completes the A handshake, and still
// reads every request frame off the wire (so the TCP stream stays
// well-formed), but never writes a reply — regression coverage for a
// request-level I/O timeout (issue tracked alongside request pipelining).
func (m *mockNode) goSilentAfterHandshake() { m.silent.Store(true) }

// answerWrongTagOnce queues a one-off reply for the next G request on a
// tagged connection that echoes the wrong tag (the request's tag + 1) —
// the desync a pre-tag stream misalignment would produce.
func (m *mockNode) answerWrongTagOnce() { m.wrongTagLeft.Add(1) }

// answerRetryableTimes queues n one-off `R` replies (issue #125),
// consumed by the next n data requests (G/g/S/s/D/d/c/F, in whatever
// order they arrive) on any connection to this node.
func (m *mockNode) answerRetryableTimes(n int32) { m.retryableLeft.Add(n) }

// dataRequestsReceived reports how many data commands (G/g/S/s/D/d/c/F)
// this node has read off the wire in total, R-answered or not — issue
// #125's tests use this to assert the exact number of attempts a bounded
// retry sequence made.
func (m *mockNode) dataRequestsReceived() int32 { return m.dataRequestCount.Load() }

// authHeadersSeen returns, in order, every `A` header line this node has
// received so far (trailing '\n' stripped) — issue #125's tests use this
// to assert the exact probe form (`A <len> T R`, `A <len> T`, or plain
// `A <len>`) a connect/fallback attempt sent.
func (m *mockNode) authHeadersSeen() []string {
	m.authHeadersMu.Lock()
	defer m.authHeadersMu.Unlock()
	return append([]string(nil), m.authHeaders...)
}

// swallowGetOnce swallows the next G request entirely (no reply) — the
// off-by-one stream desync where every later response answers the
// previous request.
func (m *mockNode) swallowGetOnce() { m.swallowLeft.Add(1) }

// failClearTimes makes this node's next n c/F requests fail: the request
// is still read off the wire (so the stream stays well-formed for
// whatever request follows on a fresh connection), but answered by
// dropping the connection instead of replying — a connection-level
// failure (issue #106's clearFanout retry path), not a `W` or malformed
// frame, since neither ever applies to a clear.
func (m *mockNode) failClearTimes(n int32) { m.failClearLeft.Add(n) }

// clearCountReceived reports how many c/F requests this node has
// received so far (failed attempts counted too) — used to assert a
// clear actually fanned out to every node (issue #106).
func (m *mockNode) clearCountReceived() int32 { return m.clearCount.Load() }

// iRequestsReceived reports how many `i` (INCR) frames this node has
// received so far — issue #129's replication test uses this to assert a
// replica never received one.
func (m *mockNode) iRequestsReceived() int32 { return m.iCount.Load() }

// kRequestsReceived/xRequestsReceived report how many `k`/`x`
// (compare-and-set, issue #141) frames this node has received so far —
// the CAS replication test uses these to assert a replica never received
// either.
func (m *mockNode) kRequestsReceived() int32 { return m.kCount.Load() }
func (m *mockNode) xRequestsReceived() int32 { return m.xCount.Load() }

// mockDigestHex computes the same content digest hex a real
// nanocached-node evaluates a `k`/`x` <cond> digest against
// (docs/protocol.html#cas) — reusing this SDK's own ContentDigest/
// CasToken (same package) so the mock server's evaluation stays
// byte-for-byte aligned with what GetWithToken's caller computes.
func mockDigestHex(value []byte) string {
	return CasToken{digest: ContentDigest(value)}.Hex()
}

func startMockNode(t *testing.T, requiredSecret []byte) *mockNode {
	return startMockNodeOpts(t, requiredSecret, mockNodeOpts{})
}

func startMockNodeOpts(t *testing.T, requiredSecret []byte, opts mockNodeOpts) *mockNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &mockNode{listener: listener, requiredSecret: requiredSecret, opts: opts}
	go node.acceptLoop()
	t.Cleanup(node.close)
	return node
}

// startMockNodeAt binds to a specific address instead of an ephemeral
// port — for a node that comes back up on the address discovery already
// advertised (issue #67's redial-after-cooldown test).
func startMockNodeAt(t *testing.T, requiredSecret []byte, address string) *mockNode {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	node := &mockNode{listener: listener, requiredSecret: requiredSecret}
	go node.acceptLoop()
	t.Cleanup(node.close)
	return node
}

func (m *mockNode) address() string { return m.listener.Addr().String() }

func (m *mockNode) storeLen() int {
	count := 0
	m.store.Range(func(_, _ any) bool { count++; return true })
	return count
}

func (m *mockNode) hasKey(key string) bool {
	return m.hasNSKey("", key)
}

// hasNSKey is hasKey scoped to namespace ns (issue #105).
func (m *mockNode) hasNSKey(ns, key string) bool {
	_, ok := m.store.Load(storeKey{ns, key})
	return ok
}

// storedValue returns key's raw stored value as a string within the
// default namespace, and whether it was present at all — issue #129's
// Incr tests use this to confirm a replica's stored value is the
// primary's literal result, not something the replica computed itself.
func (m *mockNode) storedValue(key string) (string, bool) {
	v, ok := m.store.Load(storeKey{"", key})
	if !ok {
		return "", false
	}
	return string(v.([]byte)), true
}

// storedTTL returns the TTL (seconds) recorded by the most recent S/s
// that stored key within the default namespace, and whether one is
// recorded at all — issue #129's Incr TTL round-trip test uses this to
// confirm the replica leg's Set carried the primary's reported TTL.
func (m *mockNode) storedTTL(key string) (int64, bool) {
	v, ok := m.ttls.Load(storeKey{"", key})
	if !ok {
		return 0, false
	}
	return v.(int64), true
}

func (m *mockNode) dropConnections() {
	m.conns.Range(func(conn, _ any) bool {
		_ = conn.(net.Conn).Close()
		return true
	})
}

func (m *mockNode) close() {
	m.dropConnections()
	_ = m.listener.Close()
}

func (m *mockNode) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			return
		}
		m.connectionCount.Add(1)
		m.conns.Store(conn, struct{}{})
		go m.serve(conn)
	}
}

func (m *mockNode) serve(conn net.Conn) {
	defer func() {
		m.conns.Delete(conn)
		_ = conn.Close()
	}()
	reader := bufio.NewReader(conn)
	// Echoed response tags: set once this connection's `A ... T` is
	// acknowledged — its requests then carry a trailing tag the replies
	// must echo.
	tagged := false
	for {
		header, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		parts := strings.Split(strings.TrimSuffix(header, "\n"), " ")
		// On a tagged connection every request's last header field is its
		// tag, echoed back as each reply's own last field.
		tagSuffix := ""
		if tagged {
			tagSuffix = " " + parts[len(parts)-1]
		}
		switch parts[0] {
		case "A":
			m.authHeadersMu.Lock()
			m.authHeaders = append(m.authHeaders, strings.TrimSuffix(header, "\n"))
			m.authHeadersMu.Unlock()
			if len(parts) > 2 && m.opts.closeOnExtendedAuth {
				return
			}
			// issue #125: a server that understands `T` but predates the
			// trailing `R` capability token treats anything past
			// `A <len> T` as a parse error too.
			if len(parts) > 3 && m.opts.closeOnRetryableCapability {
				return
			}
			secret := mustRead(reader, atoiOrPanic(parts[1]))
			accepted := len(secret) > 0
			if m.requiredSecret != nil {
				accepted = bytes.Equal(secret, m.requiredSecret)
			}
			tagged = accepted && m.opts.supportTags && len(parts) > 2 && parts[2] == "T"
			reply := "On\n"
			if !accepted {
				reply = "En\n"
			} else if tagged {
				reply = "OnT\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil || !accepted {
				return
			}
		case "G":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			if m.silent.Load() {
				continue
			}
			m.getCount.Add(1)
			m.dataRequestCount.Add(1)
			if delay := time.Duration(m.getDelay.Load()); delay > 0 {
				time.Sleep(delay)
			}
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.swallowLeft) {
				continue // no reply at all — the off-by-one desync injection
			}
			if tagged && m.takeOne(&m.wrongTagLeft) {
				// Echo the wrong tag (the request's tag + 1) — the desync
				// a pre-tag stream misalignment would
				// produce.
				requestTag := atoiOrPanic(parts[len(parts)-1])
				if _, err := conn.Write([]byte(fmt.Sprintf("N %d\n", requestTag+1))); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.malformedLeft) {
				if _, err := conn.Write([]byte("V x\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.storedToGetLeft) {
				// A well-formed frame of the wrong kind, as a desynced
				// (off-by-one) stream would produce.
				if _, err := conn.Write([]byte("S" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			var reply []byte
			if m.takeWrongNode() {
				reply = []byte("W" + tagSuffix + "\n")
			} else if value, ok := m.store.Load(storeKey{"", key}); ok {
				stored := value.([]byte)
				reply = append([]byte(fmt.Sprintf("V %d%s\n", len(stored), tagSuffix)), stored...)
			} else {
				reply = []byte("N" + tagSuffix + "\n")
			}
			if _, err := conn.Write(reply); err != nil {
				return
			}
		// Namespaced get (issue #105): `g <ns-len> <key-len>
		// [<tag>]\n<ns><key>` — the same body-field order as G, with the
		// namespace leading the key and one extra ns-len header field.
		// Everything past reading the namespace bytes mirrors G exactly,
		// including the fault-injection knobs (shared with G's tests,
		// which exercise the same store and counters through either
		// command).
		case "g":
			nsLen := atoiOrPanic(parts[1])
			keyLen := atoiOrPanic(parts[2])
			namespace := string(mustRead(reader, nsLen))
			key := string(mustRead(reader, keyLen))
			if m.silent.Load() {
				continue
			}
			m.getCount.Add(1)
			m.dataRequestCount.Add(1)
			if delay := time.Duration(m.getDelay.Load()); delay > 0 {
				time.Sleep(delay)
			}
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.swallowLeft) {
				continue
			}
			if tagged && m.takeOne(&m.wrongTagLeft) {
				requestTag := atoiOrPanic(parts[len(parts)-1])
				if _, err := conn.Write([]byte(fmt.Sprintf("N %d\n", requestTag+1))); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.malformedLeft) {
				if _, err := conn.Write([]byte("V x\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.storedToGetLeft) {
				if _, err := conn.Write([]byte("S" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			var nsReply []byte
			if m.takeWrongNode() {
				nsReply = []byte("W" + tagSuffix + "\n")
			} else if value, ok := m.store.Load(storeKey{namespace, key}); ok {
				stored := value.([]byte)
				nsReply = append([]byte(fmt.Sprintf("V %d%s\n", len(stored), tagSuffix)), stored...)
			} else {
				nsReply = []byte("N" + tagSuffix + "\n")
			}
			if _, err := conn.Write(nsReply); err != nil {
				return
			}
		case "S":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			value := mustRead(reader, atoiOrPanic(parts[2]))
			if m.silent.Load() {
				continue
			}
			m.dataRequestCount.Add(1)
			// The TTL, when present, is the field after the two lengths
			// (omitted on the wire means "no expiry", i.e. 0); on a
			// tagged connection the tag sits after it as the last field.
			ttlBase := 3
			if tagged {
				ttlBase = 4
			}
			hasTTL := len(parts) > ttlBase
			if hasTTL {
				m.lastSetTTL.Store(parts[3])
			} else {
				m.lastSetTTL.Store("none")
			}
			if delay := time.Duration(m.setDelay.Load()); delay > 0 {
				time.Sleep(delay)
			}
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			reply := "S" + tagSuffix + "\n"
			if m.takeOne(&m.setWrongNodeLeft) || m.takeWrongNode() {
				reply = "W" + tagSuffix + "\n"
			} else {
				sk := storeKey{"", key}
				m.store.Store(sk, value)
				if hasTTL {
					m.ttls.Store(sk, int64(atoiOrPanic(parts[3])))
				} else {
					m.ttls.Delete(sk)
				}
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		// Namespaced set (issue #105): `s <ns-len> <key-len> <val-len>
		// [<ttl>] [<tag>]\n<ns><key><value>` — the ttl+tag `s` form from
		// the issue #105 SDK port spec. ns-len shifts every later
		// length field by one position versus S, so the TTL-presence
		// arithmetic below is S's own shifted by that same one field.
		case "s":
			nsLen := atoiOrPanic(parts[1])
			keyLen := atoiOrPanic(parts[2])
			valLen := atoiOrPanic(parts[3])
			namespace := string(mustRead(reader, nsLen))
			key := string(mustRead(reader, keyLen))
			value := mustRead(reader, valLen)
			if m.silent.Load() {
				continue
			}
			m.dataRequestCount.Add(1)
			ttlBase := 4
			if tagged {
				ttlBase = 5
			}
			hasTTL := len(parts) > ttlBase
			if hasTTL {
				m.lastSetTTL.Store(parts[4])
			} else {
				m.lastSetTTL.Store("none")
			}
			if delay := time.Duration(m.setDelay.Load()); delay > 0 {
				time.Sleep(delay)
			}
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			reply := "S" + tagSuffix + "\n"
			if m.takeOne(&m.setWrongNodeLeft) || m.takeWrongNode() {
				reply = "W" + tagSuffix + "\n"
			} else {
				sk := storeKey{namespace, key}
				m.store.Store(sk, value)
				if hasTTL {
					m.ttls.Store(sk, int64(atoiOrPanic(parts[4])))
				} else {
					m.ttls.Delete(sk)
				}
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		// Batched get (issues #128/#150/#151): `m <ns-len> <n>
		// <key-len-1> ... <key-len-n>[ <tag>]\n<ns><key-1>...<key-n>` —
		// always namespaced, no legacy uppercase form. Answers `M <n>
		// <result-1> ... <result-n>[ <tag>]\n<hit values, concatenated
		// in request order>` (docs/protocol.html#multi): a decimal byte
		// length for a hit, "-" for a clean miss, "W" for a per-key
		// wrong-node (multiWrongNodeKey — the batched analogue of
		// wrongNodeLeft). mCount counts every `m` this node has
		// received.
		case "m":
			nsLen := atoiOrPanic(parts[1])
			count := atoiOrPanic(parts[2])
			namespace := string(mustRead(reader, nsLen))
			keys := make([]string, count)
			for i := 0; i < count; i++ {
				keys[i] = string(mustRead(reader, atoiOrPanic(parts[3+i])))
			}
			if m.silent.Load() {
				continue
			}
			m.mCount.Add(1)
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			header := "M " + strconv.Itoa(count)
			var body []byte
			for _, key := range keys {
				if m.takeMultiWrongNode(key) {
					header += " W"
					continue
				}
				if value, ok := m.store.Load(storeKey{namespace, key}); ok {
					stored := value.([]byte)
					header += " " + strconv.Itoa(len(stored))
					body = append(body, stored...)
				} else {
					header += " -"
				}
			}
			if _, err := conn.Write(append([]byte(header+tagSuffix+"\n"), body...)); err != nil {
				return
			}
		// Batched set (issues #150/#151): `o <ns-len> <n> <key-len-1>
		// <value-len-1> ... <key-len-n> <value-len-n> [<ttl>][ <tag>]
		// \n<ns><key-1><value-1>...<key-n><value-n>` — one shared TTL
		// for the whole batch, not per key. Answers `O <n> <result-1>
		// ... <result-n>[ <tag>]\n` (no body): "S" (stored) or "W"
		// (per-key wrong-node, same multiWrongNodeKey knob `m` uses).
		// oCount counts every `o` this node has received. The
		// ttl-vs-tag field disambiguation mirrors `s`'s own (see its
		// case above): the ttl field, when present, always sits
		// immediately after the last length field, regardless of
		// whether the connection is tagged.
		case "o":
			nsLen := atoiOrPanic(parts[1])
			count := atoiOrPanic(parts[2])
			namespace := string(mustRead(reader, nsLen))
			keys := make([]string, count)
			values := make([][]byte, count)
			ttlIndex := 3 + 2*count
			ttlBase := ttlIndex
			if tagged {
				ttlBase++
			}
			hasTTL := len(parts) > ttlBase
			for i := 0; i < count; i++ {
				keyLen := atoiOrPanic(parts[3+2*i])
				valLen := atoiOrPanic(parts[3+2*i+1])
				keys[i] = string(mustRead(reader, keyLen))
				values[i] = mustRead(reader, valLen)
			}
			if m.silent.Load() {
				continue
			}
			m.oCount.Add(1)
			m.dataRequestCount.Add(1)
			var ttlValue int64
			if hasTTL {
				ttlValue = int64(atoiOrPanic(parts[ttlIndex]))
			}
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			header := "O " + strconv.Itoa(count)
			for i, key := range keys {
				if m.takeMultiWrongNode(key) {
					header += " W"
					continue
				}
				sk := storeKey{namespace, key}
				m.store.Store(sk, values[i])
				if hasTTL {
					m.ttls.Store(sk, ttlValue)
				} else {
					m.ttls.Delete(sk)
				}
				header += " S"
			}
			if _, err := conn.Write([]byte(header + tagSuffix + "\n")); err != nil {
				return
			}
		// Incr/Decr (issue #129): `i <ns-len> <key-len> <delta>[ <tag>]
		// \n<ns><key>` — always namespaced (ns-len 0 for the default
		// namespace), no legacy uppercase form. Parses the stored value
		// (if any) as a signed decimal int64, adds delta, and answers `N`
		// on a miss, `T` on a non-numeric stored value or an overflowing
		// add, or `I <value-length> [<ttl-seconds>][ <tag>]\n<value>` on
		// success — the ttl field, when this key has one recorded (see
		// ttls, set by S/s above), mirrors a real node reporting the
		// entry's remaining TTL. iCount counts every `i` this node has
		// seen, failed or not — issue #129's replication test asserts a
		// replica never receives one.
		case "i":
			nsLen := atoiOrPanic(parts[1])
			keyLen := atoiOrPanic(parts[2])
			deltaStr := parts[3]
			namespace := string(mustRead(reader, nsLen))
			key := string(mustRead(reader, keyLen))
			if m.silent.Load() {
				continue
			}
			m.iCount.Add(1)
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeWrongNode() {
				if _, err := conn.Write([]byte("W" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			sk := storeKey{namespace, key}
			stored, found := m.store.Load(sk)
			if !found {
				if _, err := conn.Write([]byte("N" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			current, currErr := strconv.ParseInt(string(stored.([]byte)), 10, 64)
			delta, deltaErr := strconv.ParseInt(deltaStr, 10, 64)
			next := current + delta
			overflowed := (delta > 0 && next < current) || (delta < 0 && next > current)
			if currErr != nil || deltaErr != nil || overflowed {
				if _, err := conn.Write([]byte("T" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			nextBytes := []byte(strconv.FormatInt(next, 10))
			m.store.Store(sk, nextBytes)
			ttlField := ""
			if ttl, ok := m.ttls.Load(sk); ok {
				ttlField = fmt.Sprintf(" %d", ttl.(int64))
			}
			reply := append([]byte(fmt.Sprintf("I %d%s%s\n", len(nextBytes), ttlField, tagSuffix)), nextBytes...)
			if _, err := conn.Write(reply); err != nil {
				return
			}
		// Compare-and-set store (issue #141): `k <ns-len> <key-len>
		// <val-len> <cond> [<ttl-seconds>] [<tag>]\n<ns><key><value>` —
		// always namespaced, no legacy uppercase form, mirroring `i`.
		// <cond> is a bare token: "A" (absent expected), "P" (present
		// expected), or a 32-char lowercase hex content digest (exact
		// match expected) — see mockDigestHex. Success stores value and
		// answers `S`; a mismatch answers `N`, reusing the same markers a
		// plain Set/miss already use (no new response marker). kCount
		// counts every `k` this node has seen, matched or not — the CAS
		// replication test asserts a replica never receives one.
		case "k":
			nsLen := atoiOrPanic(parts[1])
			keyLen := atoiOrPanic(parts[2])
			valLen := atoiOrPanic(parts[3])
			cond := parts[4]
			namespace := string(mustRead(reader, nsLen))
			key := string(mustRead(reader, keyLen))
			value := mustRead(reader, valLen)
			if m.silent.Load() {
				continue
			}
			m.kCount.Add(1)
			m.dataRequestCount.Add(1)
			ttlBase := 5
			if tagged {
				ttlBase = 6
			}
			hasTTL := len(parts) > ttlBase
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeWrongNode() {
				if _, err := conn.Write([]byte("W" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			sk := storeKey{namespace, key}
			existing, found := m.store.Load(sk)
			condOK := false
			switch cond {
			case "A":
				condOK = !found
			case "P":
				condOK = found
			default:
				condOK = found && mockDigestHex(existing.([]byte)) == cond
			}
			if !condOK {
				if _, err := conn.Write([]byte("N" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			m.store.Store(sk, value)
			if hasTTL {
				m.ttls.Store(sk, int64(atoiOrPanic(parts[5])))
			} else {
				m.ttls.Delete(sk)
			}
			if _, err := conn.Write([]byte("S" + tagSuffix + "\n")); err != nil {
				return
			}
		// Compare-and-set remove (issue #141): `x <ns-len> <key-len>
		// <cond> [<tag>]\n<ns><key>` — <cond> here is always a digest
		// (never A/P). Success deletes the key and answers `D`; a
		// mismatch or missing key answers `N`, the same markers a plain
		// Delete already uses. xCount mirrors kCount.
		case "x":
			nsLen := atoiOrPanic(parts[1])
			keyLen := atoiOrPanic(parts[2])
			cond := parts[3]
			namespace := string(mustRead(reader, nsLen))
			key := string(mustRead(reader, keyLen))
			if m.silent.Load() {
				continue
			}
			m.xCount.Add(1)
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeWrongNode() {
				if _, err := conn.Write([]byte("W" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			sk := storeKey{namespace, key}
			existing, found := m.store.Load(sk)
			reply := "N" + tagSuffix + "\n"
			if found && mockDigestHex(existing.([]byte)) == cond {
				m.store.Delete(sk)
				m.ttls.Delete(sk)
				reply = "D" + tagSuffix + "\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		case "D":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			if m.silent.Load() {
				continue
			}
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			reply := "N" + tagSuffix + "\n"
			if m.takeWrongNode() {
				reply = "W" + tagSuffix + "\n"
			} else if _, existed := m.store.LoadAndDelete(storeKey{"", key}); existed {
				reply = "D" + tagSuffix + "\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		// Namespaced delete (issue #105): `d <ns-len> <key-len>
		// [<tag>]\n<ns><key>` — mirrors D exactly past the extra ns-len
		// field and namespace bytes.
		case "d":
			nsLen := atoiOrPanic(parts[1])
			keyLen := atoiOrPanic(parts[2])
			namespace := string(mustRead(reader, nsLen))
			key := string(mustRead(reader, keyLen))
			if m.silent.Load() {
				continue
			}
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			reply := "N" + tagSuffix + "\n"
			if m.takeWrongNode() {
				reply = "W" + tagSuffix + "\n"
			} else if _, existed := m.store.LoadAndDelete(storeKey{namespace, key}); existed {
				reply = "D" + tagSuffix + "\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		// Clear one namespace (issue #106): `c <ns-len>[ <tag>]\n<ns>` —
		// drops every stored entry whose namespace matches (an empty
		// namespace is the default one, stored under storeKey{"", ...}
		// exactly like G/S/D's unnamespaced form). Always acks `C`
		// unless failClearTimes queued a failure.
		case "c":
			nsLen := atoiOrPanic(parts[1])
			namespace := string(mustRead(reader, nsLen))
			m.clearCount.Add(1)
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.failClearLeft) {
				return // simulate a connection-level failure: no reply, drop
			}
			m.store.Range(func(k, _ any) bool {
				if k.(storeKey).ns == namespace {
					m.store.Delete(k)
				}
				return true
			})
			if _, err := conn.Write([]byte("C" + tagSuffix + "\n")); err != nil {
				return
			}
		// Flush everything (issue #106): `F[ <tag>]\n` — drops every
		// namespace, default included.
		case "F":
			m.clearCount.Add(1)
			m.dataRequestCount.Add(1)
			if m.takeOne(&m.retryableLeft) { // issue #125
				if _, err := conn.Write([]byte("R" + tagSuffix + "\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.failClearLeft) {
				return
			}
			m.store.Range(func(k, _ any) bool {
				m.store.Delete(k)
				return true
			})
			if _, err := conn.Write([]byte("C" + tagSuffix + "\n")); err != nil {
				return
			}
		default:
			return
		}
	}
}

func (m *mockNode) takeWrongNode() bool {
	return m.takeOne(&m.wrongNodeLeft)
}

// takeMultiWrongNode reports whether key matches multiWrongNodeKey and
// multiWrongNodeLeft still has budget (issues #128/#150/#151) — a test
// sets multiWrongNodeKey once and controls how many consecutive `m`/`o`
// requests answer that key `W` via multiWrongNodeLeft's count, exactly
// like wrongNodeLeft governs a whole G/S's own `W` count.
func (m *mockNode) takeMultiWrongNode(key string) bool {
	target, _ := m.multiWrongNodeKey.Load().(string)
	if target == "" || target != key {
		return false
	}
	return m.takeOne(&m.multiWrongNodeLeft)
}

func (m *mockNode) takeOne(counter *atomic.Int32) bool {
	for {
		pending := counter.Load()
		if pending == 0 {
			return false
		}
		if counter.CompareAndSwap(pending, pending-1) {
			return true
		}
	}
}

// ── モック discovery ──────────────────────────────────────────────

type mockDiscovery struct {
	listener    net.Listener
	replication int
	mu          sync.Mutex
	nodes       []discoveredNode
	// proxies is issue #122's registered-proxy roster, served by `Q` —
	// a mock "proxy" is just a mockNode, exactly as ViaProxy's spec
	// notes: that is literally what a proxy looks like to a client.
	proxies   []discoveredNode
	warmingUp bool
	// lCount/qCount let a test assert which roster command a client
	// actually sent — in particular, that ViaProxy never issues `L` at
	// all (issue #122's "no connection is ever made to a node address").
	lCount atomic.Int32
	qCount atomic.Int32
}

func startMockDiscovery(t *testing.T, nodes []discoveredNode, replication int) *mockDiscovery {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	discovery := &mockDiscovery{listener: listener, replication: replication, nodes: nodes}
	go discovery.acceptLoop()
	t.Cleanup(func() { _ = listener.Close() })
	return discovery
}

func (m *mockDiscovery) address() string { return m.listener.Addr().String() }

func (m *mockDiscovery) setNodes(nodes []discoveredNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = nodes
}

// setProxies updates the registered-proxy roster `Q` serves (issue #122)
// — usable both before Connect and mid-test, exactly like setNodes,
// since nanocached-discovery.rs's roster is itself live-updatable
// (ProxyAnnounce) rather than fixed at startup.
func (m *mockDiscovery) setProxies(proxies []discoveredNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.proxies = proxies
}

func (m *mockDiscovery) setWarming(warming bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warmingUp = warming
}

func (m *mockDiscovery) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			return
		}
		go m.serve(conn)
	}
}

func (m *mockDiscovery) serve(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		header, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		parts := strings.Split(strings.TrimSuffix(header, "\n"), " ")
		switch parts[0] {
		case "A":
			mustRead(reader, atoiOrPanic(parts[1]))
			// Echoed response tags: echo the tag capability — clients send
			// the extended A before knowing which kind of server
			// answered. Discovery itself never uses tags (a single L per
			// connection), but still has to parse this reply either way.
			reply := "Od\n"
			if len(parts) > 2 && parts[2] == "T" {
				reply = "OdT\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		case "L":
			m.lCount.Add(1)
			m.mu.Lock()
			warming, nodes := m.warmingUp, append([]discoveredNode(nil), m.nodes...)
			m.mu.Unlock()
			if warming {
				_, _ = conn.Write([]byte("B\n"))
				return
			}
			var frame strings.Builder
			fmt.Fprintf(&frame, "N %d %d\n", len(nodes), m.replication)
			for _, node := range nodes {
				fmt.Fprintf(&frame, "%d %d\n%s%s\n",
					len(node.Name), len(node.Address), node.Name, node.Address)
			}
			if _, err := conn.Write([]byte(frame.String())); err != nil {
				return
			}
		// Issue #122: `Q`, ListProxies — same reply shape as `L` above,
		// minus the replication field on the header line (see
		// nanocached-discovery.rs's ListProxies and readProxyList).
		case "Q":
			m.qCount.Add(1)
			m.mu.Lock()
			warming, proxies := m.warmingUp, append([]discoveredNode(nil), m.proxies...)
			m.mu.Unlock()
			if warming {
				_, _ = conn.Write([]byte("B\n"))
				return
			}
			var frame strings.Builder
			fmt.Fprintf(&frame, "N %d\n", len(proxies))
			for _, proxy := range proxies {
				fmt.Fprintf(&frame, "%d %d\n%s%s\n",
					len(proxy.Name), len(proxy.Address), proxy.Name, proxy.Address)
			}
			if _, err := conn.Write([]byte(frame.String())); err != nil {
				return
			}
		default:
			return
		}
	}
}

func mustRead(reader *bufio.Reader, length int) []byte {
	data := make([]byte, length)
	if _, err := readFull(reader, data); err != nil {
		panic(err)
	}
	return data
}

func atoiOrPanic(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return n
}

func unusedPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitFor(t *testing.T, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ── 単一ノード ────────────────────────────────────────────────────

func TestRoundTripsSetGetDelete(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("greeting", "hello", 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.Get("greeting")
	if err != nil || !ok || value != "hello" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if existed, err := client.Delete("greeting"); err != nil || !existed {
		t.Fatalf("Delete = %v, %v", existed, err)
	}
	if _, ok, err := client.Get("greeting"); err != nil || ok {
		t.Fatalf("Get after delete: ok=%v err=%v", ok, err)
	}
	if existed, err := client.Delete("greeting"); err != nil || existed {
		t.Fatalf("second Delete = %v, %v", existed, err)
	}
	if client.Replication() != 1 {
		t.Fatalf("Replication = %d", client.Replication())
	}
}

func TestGetBytesRoundTripsRawValues(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	raw := []byte{0x00, 0xff, 0x10, 0x80, 0x7f, 'h', 'i'}
	if err := client.SetBytes("binary", raw, 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.GetBytes("binary")
	if err != nil || !ok || !bytes.Equal(value, raw) {
		t.Fatalf("GetBytes = %v, %v, %v", value, ok, err)
	}
}

func TestRejectsANegativeTtl(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative ttl accepted, err = %v, want ErrInvalidArgument", err)
	}
	if err := client.SetBytes("k", []byte("v"), -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative ttl accepted (SetBytes), err = %v, want ErrInvalidArgument", err)
	}
	if err := client.Set("k", "v", 60); err != nil {
		t.Fatal(err)
	}
}

// ── 引数検証 (issue #47 audit item G1) ────────────────────────────────

func TestRejectsAnEmptyKeyAndOversizedRequestWithoutTouchingTheNetwork(t *testing.T) {
	// The server has no way to answer an empty-key request except by
	// closing the connection outright — poisoning every other request
	// already pipelined behind it on that connection. Catching this (and
	// a key+value that could never fit the server's own request cap)
	// client-side must happen before any bytes hit the wire — verified
	// below by checking no extra connection was ever dialed beyond
	// Connect's own.
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, _, err := client.GetBytes(""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key accepted by GetBytes, err = %v, want ErrInvalidArgument", err)
	}
	if err := client.SetBytes("", []byte("v"), 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key accepted by SetBytes, err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.Delete(""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key accepted by Delete, err = %v, want ErrInvalidArgument", err)
	}

	oversized := make([]byte, 1024*1024)
	if err := client.SetBytes("k", oversized, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized key+value accepted by SetBytes, err = %v, want ErrInvalidArgument", err)
	}

	if got := node.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (validation must reject before any network I/O)", got)
	}
}

// TestRejectsAnOversizedKeyOnGetAndDelete is the GET/DELETE half of the
// fix for issue #47 audit item G1's follow-up: validateKey previously only
// rejected an empty key, so an oversized key (with no value to trip
// validateKeyAndValue's combined check) sailed past client-side validation
// on GetBytes/Delete, got serialized onto the wire, and only then hit the
// server's own request cap — which rejects it by silently closing the
// connection, poisoning every other request pipelined behind it. This must
// be caught before any network I/O, exactly like the empty-key and
// oversized-key+value cases above.
func TestRejectsAnOversizedKeyOnGetAndDelete(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	oversizedKey := string(make([]byte, maxRequestBytes+1))

	if _, _, err := client.GetBytes(oversizedKey); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized key accepted by GetBytes, err = %v, want ErrInvalidArgument", err)
	}
	if _, _, err := client.Get(oversizedKey); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized key accepted by Get, err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.Delete(oversizedKey); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized key accepted by Delete, err = %v, want ErrInvalidArgument", err)
	}

	if got := node.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (validation must reject before any network I/O)", got)
	}
}

func TestAnInvalidRequestDoesNotPoisonConcurrentValidRequestsOnTheSameConnection(t *testing.T) {
	// Key/size validation runs before any network I/O, so an invalid call
	// (empty key) never touches the wire at all — and so can never desync
	// or poison a connection that other, valid, concurrent requests are
	// pipelined on (request pipelining).
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := client.Set(key, value, 0); err != nil {
				t.Error(err)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := client.GetBytes(""); err == nil {
				t.Error("empty key accepted")
			}
		}()
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		value, ok, err := client.Get(fmt.Sprintf("key-%d", i))
		if err != nil || !ok {
			t.Fatalf("Get key-%d: value=%q ok=%v err=%v", i, value, ok, err)
		}
		if want := fmt.Sprintf("value-%d", i); value != want {
			t.Fatalf("key-%d = %q, want %q", i, value, want)
		}
	}
}

// ── Config.String/GoString (issue #47 audit item G3) ─────────────────

func TestConfigStringAndGoStringRedactAuthSecret(t *testing.T) {
	cfg := Config{
		Addresses:  []Address{addr("127.0.0.1:8357")},
		AuthSecret: "s3cret",
		TLS:        true,
	}
	for _, rendered := range []string{
		fmt.Sprintf("%v", cfg),
		fmt.Sprintf("%s", cfg),
		fmt.Sprintf("%#v", cfg),
	} {
		if strings.Contains(rendered, "s3cret") {
			t.Fatalf("Config format leaked AuthSecret: %s", rendered)
		}
		if !strings.Contains(rendered, "REDACTED") {
			t.Fatalf("Config format did not redact a set AuthSecret: %s", rendered)
		}
	}

	cfg.AuthSecret = ""
	if got := cfg.String(); strings.Contains(got, "REDACTED") {
		t.Fatalf("Config.String() redacted an unset AuthSecret: %s", got)
	}
}

func TestTtlZeroMeansNoExpiryAndAPositiveTtlIsSentAsIs(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if got := node.lastSetTTL.Load(); got != "none" {
		t.Fatalf("zero TTL sent as %v, want none", got)
	}
	if err := client.Set("k", "v", 2); err != nil {
		t.Fatal(err)
	}
	if got := node.lastSetTTL.Load(); got != "2" {
		t.Fatalf("2s TTL sent as %v, want \"2\"", got)
	}
}

// TestPipelinesConcurrentRequestsOnOneConnection is the same shape as
// the TypeScript SDK's own pipelining test: N concurrent requests on a
// single connection, each independently verified to round-trip its own
// value (request pipelining) — a bug in matching responses to the right
// caller in send order would show up as swapped or wrong values here.
func TestPipelinesConcurrentRequestsOnOneConnection(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := client.Set(key, value, 0); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	errs := make([]error, n)
	values := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value, ok, err := client.Get(fmt.Sprintf("key-%d", i))
			if err != nil || !ok {
				errs[i] = fmt.Errorf("Get key-%d: value=%q ok=%v err=%v", i, value, ok, err)
				return
			}
			values[i] = value
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Error(errs[i])
			continue
		}
		if want := fmt.Sprintf("value-%d", i); values[i] != want {
			t.Errorf("key-%d = %q, want %q", i, values[i], want)
		}
	}
}

// ── echoed response tags 応答タグ ───────────────────────────────────────

// TestPipelinesConcurrentRequestsOnOneConnectionTagged is
// TestPipelinesConcurrentRequestsOnOneConnection's shape against a
// tag-supporting node: N concurrent set/get on one tagged connection each
// independently round-trip their own value, verifying the tag-echo check
// doesn't itself misdispatch a busy pipeline.
func TestPipelinesConcurrentRequestsOnOneConnectionTagged(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := client.Set(key, value, 0); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	errs := make([]error, n)
	values := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value, ok, err := client.Get(fmt.Sprintf("key-%d", i))
			if err != nil || !ok {
				errs[i] = fmt.Errorf("Get key-%d: value=%q ok=%v err=%v", i, value, ok, err)
				return
			}
			values[i] = value
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Error(errs[i])
			continue
		}
		if want := fmt.Sprintf("value-%d", i); values[i] != want {
			t.Errorf("key-%d = %q, want %q", i, values[i], want)
		}
	}

	if existed, err := client.Delete("key-0"); err != nil || !existed {
		t.Fatalf("Delete key-0 = %v, %v, want true, nil", existed, err)
	}
	if existed, err := client.Delete("key-0"); err != nil || existed {
		t.Fatalf("Delete key-0 (again) = %v, %v, want false, nil", existed, err)
	}
}

// TestASwallowedResponseDesyncsAndIsCaughtBeforeDispatchTagged is the
// exact misdelivery request pipelining left open: the server never answers
// the first GET (swallowGetOnce), so the second GET's response arrives at
// the first GET's pending slot. Without tags the first caller would
// receive the second's value as a plausible, exception-free wrong answer;
// the tag check must poison the connection before either caller sees
// anything, and the next request must transparently redial and succeed.
// This is exercised directly against a *connection* (this package's
// internal, unexported per-socket type — this test file is part of package
// nanocached), not through Client: Client's Get/Set/Delete redial and
// retry once on any connection-classified failure (see
// TestAMismatchedResponseKindPoisonsTheConnection), which would silently
// absorb the very desync this test exists to catch before that retry ever
// gets a chance to mask it.
func TestASwallowedResponseDesyncsAndIsCaughtBeforeDispatchTagged(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})

	result, err := connectAndIdentify(node.address(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.tagged {
		t.Fatal("expected the mock node to negotiate tags")
	}
	conn := newConnection(result.conn, func() {}, result.tagged, nil)
	defer conn.close()

	if err := conn.set([]byte("k"), []byte("v"), -1); err != nil {
		t.Fatal(err)
	}

	node.swallowGetOnce()

	type getResult struct {
		value []byte
		ok    bool
		err   error
	}
	firstCh := make(chan getResult, 1)
	secondCh := make(chan getResult, 1)
	go func() {
		value, ok, err := conn.get([]byte("a"))
		firstCh <- getResult{value, ok, err}
	}()
	// Give the first GET a moment to actually hit the wire before the
	// second is sent, so the server sees them in this order (the swallow
	// only ever consumes the next G, whichever arrives first).
	time.Sleep(20 * time.Millisecond)
	go func() {
		value, ok, err := conn.get([]byte("k"))
		secondCh <- getResult{value, ok, err}
	}()

	first := <-firstCh
	second := <-secondCh
	if first.err == nil || !strings.Contains(first.err.Error(), "desynced") {
		t.Fatalf("first get error = %v, want a desynced error", first.err)
	}
	if second.err == nil || !strings.Contains(second.err.Error(), "desynced") {
		t.Fatalf("second get error = %v, want a desynced error", second.err)
	}
	if !conn.isClosed() {
		t.Fatal("expected the tag mismatch to have poisoned (closed) the connection")
	}

	// A fresh connection round-trips the real value correctly — the
	// desync above never touched the store.
	result2, err := connectAndIdentify(node.address(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn2 := newConnection(result2.conn, func() {}, result2.tagged, nil)
	defer conn2.close()
	value, ok, err := conn2.get([]byte("k"))
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("get after desync = %q, %v, %v, want \"v\", true, nil", value, ok, err)
	}
	if got := node.connectionCount.Load(); got != 2 {
		t.Fatalf("connectionCount = %d, want 2", got)
	}
}

// TestAWrongResponseTagPoisonsTheConnection mirrors
// TestAMismatchedResponseKindPoisonsTheConnection's wrong-kind desync, but
// for a tagged connection whose response echoes the wrong tag outright —
// caught by the tag-echo check itself rather than the caller-side kind
// check. Like that test, the connection-classified failure is healed
// transparently by Client's built-in redial-and-retry-once.
func TestAWrongResponseTagPoisonsTheConnection(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.answerWrongTagOnce()
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get after wrong-tag response = %q, %v, %v, want \"v\", true, nil", value, ok, err)
	}
	if got := node.connectionCount.Load(); got != 2 {
		t.Fatalf("connectionCount = %d, want 2 (poison + redial)", got)
	}
}

// TestConnectFallsBackToTheUntaggedProtocolAgainstALegacyServer: an old
// (pre-tag) server treats any extended `A` (`A ... T R` or `A ... T`) as
// a parse error and closes without replying; the client must fall all
// the way back to the plain form and run untagged — transparently, with
// the same results. Issue #125 adds a probe stage in front of issue
// #47's own (`A <len> T R` tried first, then `A <len> T`), so a fully
// legacy server now draws two closed dials before the plain fallback
// sticks, not one — see TestConnectFallsBackFromRetryableCapabilityToTaggedOnly
// for the middle stage in isolation.
func TestConnectFallsBackToTheUntaggedProtocolAgainstALegacyServer(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{closeOnExtendedAuth: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v, want \"v\", true, nil", value, ok, err)
	}
	// Three dials: the `T R` attempt the server slammed shut, then the
	// `T`-only attempt it slammed shut too, then the plain fallback that
	// stuck.
	if got := node.connectionCount.Load(); got != 3 {
		t.Fatalf("connectionCount = %d, want 3 (T R attempt + T attempt + plain fallback)", got)
	}
	headers := node.authHeadersSeen()
	if len(headers) != 3 {
		t.Fatalf("authHeadersSeen() = %v, want 3 headers", headers)
	}
	if !strings.HasSuffix(headers[0], " T R") {
		t.Fatalf("first A header = %q, want a trailing \" T R\"", headers[0])
	}
	if !strings.HasSuffix(headers[1], " T") || strings.HasSuffix(headers[1], " T R") {
		t.Fatalf("second A header = %q, want a trailing \" T\" (not \" T R\")", headers[1])
	}
	if strings.Contains(headers[2], "T") {
		t.Fatalf("third A header = %q, want the plain form", headers[2])
	}
}

// TestConnectProbesWithTheRetryableCapabilityFirst: the very first thing
// every connect/identify exchange sends is the full `A <len> T R` probe
// (issue #125) — asserted here against a normal (issue #47 tag-capable)
// mock node that accepts it outright, so there's exactly one dial and
// one recorded header to check.
func TestConnectProbesWithTheRetryableCapabilityFirst(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}

	headers := node.authHeadersSeen()
	if len(headers) != 1 {
		t.Fatalf("authHeadersSeen() = %v, want exactly 1 header", headers)
	}
	if !strings.HasSuffix(headers[0], " T R") {
		t.Fatalf("A header = %q, want a trailing \" T R\"", headers[0])
	}
	if got := node.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (the R capability was accepted outright)", got)
	}
}

// TestConnectFallsBackFromRetryableCapabilityToTaggedOnly: a server that
// understands issue #47's `T` but predates issue #125's `R` rejects
// `A <len> T R` as a parse error and closes without replying; the client
// must fall back one stage, to `A <len> T`, and run tagged (not all the
// way to the untagged form) — the front-of-chain fallback stage in
// isolation, distinct from TestConnectFallsBackToTheUntaggedProtocolAgainstALegacyServer's
// fully-legacy server.
func TestConnectFallsBackFromRetryableCapabilityToTaggedOnly(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true, closeOnRetryableCapability: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v, want \"v\", true, nil", value, ok, err)
	}
	if got := node.connectionCount.Load(); got != 2 {
		t.Fatalf("connectionCount = %d, want 2 (T R attempt + T fallback)", got)
	}
	headers := node.authHeadersSeen()
	if len(headers) != 2 {
		t.Fatalf("authHeadersSeen() = %v, want 2 headers", headers)
	}
	if !strings.HasSuffix(headers[0], " T R") {
		t.Fatalf("first A header = %q, want a trailing \" T R\"", headers[0])
	}
	if !strings.HasSuffix(headers[1], " T") || strings.HasSuffix(headers[1], " T R") {
		t.Fatalf("second A header = %q, want a trailing \" T\" (not \" T R\")", headers[1])
	}
}

// TestRetryableStatusTransparentlyRetriesOnTheSameConnection: a single
// `R` answers a request, then a second attempt succeeds — the retry must
// be invisible to the caller (the op just succeeds), happen on the same
// connection (no redial), and bump Stats().TransientRetries by exactly 1
// (issue #125).
func TestRetryableStatusTransparentlyRetriesOnTheSameConnection(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	node.answerRetryableTimes(1)
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatalf("Set() with one R = %v, want it to transparently succeed", err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get() = %q, %v, %v, want \"v\", true, nil", value, ok, err)
	}

	if got := node.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (the R retry must not redial)", got)
	}
	// One S that drew R, one S retry, one G: 3 data requests total.
	if got := node.dataRequestsReceived(); got != 3 {
		t.Fatalf("dataRequestsReceived() = %d, want 3 (1 failed S + 1 retried S + 1 G)", got)
	}
	if got := client.Stats().TransientRetries; got != 1 {
		t.Fatalf("Stats().TransientRetries = %d, want 1", got)
	}
}

// TestRetryableStatusExhaustsToRetryableErrorButLeavesTheConnectionUsable:
// 3 straight `R` answers exhaust the bounded retry budget (2 retries) —
// the op must surface ErrRetryable, WITHOUT closing or re-dialing the
// connection, which must still serve a following op normally (issue
// #125).
func TestRetryableStatusExhaustsToRetryableErrorButLeavesTheConnectionUsable(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	node.answerRetryableTimes(3)
	err = client.Set("k", "v", 0)
	if err == nil || !errors.Is(err, ErrRetryable) {
		t.Fatalf("Set() with 3 R = %v, want ErrRetryable", err)
	}

	if got := node.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (exhausting the retry budget must not redial)", got)
	}
	if got := node.dataRequestsReceived(); got != 3 {
		t.Fatalf("dataRequestsReceived() = %d, want 3 (the bounded budget: 1 attempt + 2 retries)", got)
	}
	if got := client.Stats().TransientRetries; got != 3 {
		t.Fatalf("Stats().TransientRetries = %d, want 3", got)
	}

	// The same connection still works for a following op.
	if err := client.Set("k", "v2", 0); err != nil {
		t.Fatalf("Set() after ErrRetryable = %v, want it to succeed", err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v2" {
		t.Fatalf("Get() after ErrRetryable = %q, %v, %v, want \"v2\", true, nil", value, ok, err)
	}
	if got := node.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (still the original connection)", got)
	}
}

// TestRetryableStatusTaggedPairsWithTheRightPipelinedRequest: on a
// tagged connection, an `R <tag>` reply must pair with the in-flight
// request it actually answers, even with another request outstanding at
// the same time (issue #125) — mirrors the tag-pairing coverage the
// existing echoed-response-tags tests give V/S/D/N/W/C.
func TestRetryableStatusTaggedPairsWithTheRightPipelinedRequest(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	node.delaySets(20 * time.Millisecond) // keeps the S outstanding while the G below is also in flight
	result, err := connectAndIdentify(node.address(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.tagged {
		t.Fatal("expected the mock node to negotiate tags")
	}
	conn := newConnection(result.conn, func() {}, result.tagged, nil)
	defer conn.close()

	node.answerRetryableTimes(1) // consumed by whichever data request reaches the server first

	type setResult struct{ err error }
	type getResult struct {
		value []byte
		ok    bool
		err   error
	}
	setCh := make(chan setResult, 1)
	go func() {
		err := conn.set([]byte("k"), []byte("v"), -1)
		setCh <- setResult{err}
	}()
	time.Sleep(5 * time.Millisecond) // let the S claim the queued R and hit the wire first
	getCh := make(chan getResult, 1)
	go func() {
		value, ok, err := conn.get([]byte("other"))
		getCh <- getResult{value, ok, err}
	}()

	set := <-setCh
	get := <-getCh
	if set.err != nil {
		t.Fatalf("set() = %v, want the R to have been transparently retried", set.err)
	}
	if get.err != nil || get.ok {
		t.Fatalf("get() = %v, %v, %v, want a clean miss (not desynced by the R)", get.value, get.ok, get.err)
	}
	if conn.isClosed() {
		t.Fatal("expected the connection to remain open")
	}
	if got := node.dataRequestsReceived(); got != 3 {
		t.Fatalf("dataRequestsReceived() = %d, want 3 (1 failed S + 1 retried S + 1 G)", got)
	}
}

func TestAuthenticates(t *testing.T) {
	node := startMockNode(t, []byte("s3cret"))

	client, err := Connect(Config{Addresses: []Address{addr(node.address())}, AuthSecret: "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Both shapes are matchable via ErrAuthenticationFailed (issue #47
	// item 5), not just by message.
	if _, err := Connect(Config{Addresses: []Address{addr(node.address())}}); !errors.Is(err, ErrAuthenticationFailed) ||
		!strings.Contains(err.Error(), "requires authentication") {
		t.Fatalf("missing-secret error = %v", err)
	}
	if _, err := Connect(Config{Addresses: []Address{addr(node.address())}, AuthSecret: "wrong"}); !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("wrong-secret error = %v", err)
	}
}

func TestWrongNodePropagatesInSingleMode(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	node.wrongNodeLeft.Add(1)
	if _, _, err := client.Get("k"); !errors.Is(err, ErrWrongNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestRejectsUseAfterClose(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	client.Close() // idempotent
	if !client.IsClosed() {
		t.Fatal("not closed")
	}
	if _, _, err := client.Get("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v", err)
	}
}

func TestClosingTwiceWarnsOnStderr(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	output := captureStderr(t, func() {
		client.Close()
	})
	if !strings.Contains(output, "close() called again on an already-closed client") {
		t.Fatalf("expected double-close warning, got %q", output)
	}
	if !client.IsClosed() {
		t.Fatal("not closed")
	}
}

func TestConnectWarnsWhenAPreviousConnectionToTheSameAddressIsStillOpen(t *testing.T) {
	node := startMockNode(t, nil)
	first, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	var second *Client
	output := captureStderr(t, func() {
		second, err = Connect(Config{Addresses: []Address{addr(node.address())}})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	if !strings.Contains(output, "was close() forgotten") {
		t.Fatalf("expected forgotten-close warning, got %q", output)
	}
}

// ── 値の圧縮 (value compression) ────────────────────────────────────

func TestWireFormatIsUntouchedWhenCompressIsOff(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value := strings.Repeat("x", 1000)
	if err := client.Set("k", value, 0); err != nil {
		t.Fatal(err)
	}

	stored, ok := node.store.Load(storeKey{"", "k"})
	if !ok || !bytes.Equal(stored.([]byte), []byte(value)) {
		t.Fatalf("stored = %v, %v", stored, ok)
	}
	got, ok, err := client.Get("k")
	if err != nil || !ok || got != value {
		t.Fatalf("Get = %q, %v, %v", got, ok, err)
	}
}

func TestConnectRejectsANegativeCompressionThreshold(t *testing.T) {
	node := startMockNode(t, nil)
	_, err := Connect(Config{
		Addresses:            []Address{addr(node.address())},
		Compress:             true,
		CompressionThreshold: -1,
	})
	if err == nil || !strings.Contains(err.Error(), "CompressionThreshold must not be negative") {
		t.Fatalf("Connect with a negative CompressionThreshold = %v, want a rejection", err)
	}
}

func TestCompressesAtOrAboveTheThresholdAndDecompressesBack(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{
		Addresses:            []Address{addr(node.address())},
		Compress:             true,
		CompressionThreshold: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value := strings.Repeat("x", 1000)
	if err := client.Set("k", value, 0); err != nil {
		t.Fatal(err)
	}

	storedAny, ok := node.store.Load(storeKey{"", "k"})
	if !ok {
		t.Fatal("value not stored")
	}
	stored := storedAny.([]byte)
	if stored[0] != compressionMarkerDeflate {
		t.Fatalf("marker = %d, want %d", stored[0], compressionMarkerDeflate)
	}
	if len(stored) >= len(value) {
		t.Fatalf("compressed length %d >= original length %d", len(stored), len(value))
	}

	got, ok, err := client.Get("k")
	if err != nil || !ok || got != value {
		t.Fatalf("Get = %q, %v, %v", got, ok, err)
	}
	gotBytes, ok, err := client.GetBytes("k")
	if err != nil || !ok || !bytes.Equal(gotBytes, []byte(value)) {
		t.Fatalf("GetBytes = %v, %v, %v", gotBytes, ok, err)
	}
}

func TestBelowThresholdValueIsPrefixedButNotCompressed(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{
		Addresses: []Address{addr(node.address())},
		Compress:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "short", 0); err != nil {
		t.Fatal(err)
	}

	storedAny, ok := node.store.Load(storeKey{"", "k"})
	if !ok {
		t.Fatal("value not stored")
	}
	want := append([]byte{compressionMarkerRaw}, []byte("short")...)
	if !bytes.Equal(storedAny.([]byte), want) {
		t.Fatalf("stored = %v, want %v", storedAny, want)
	}

	got, ok, err := client.Get("k")
	if err != nil || !ok || got != "short" {
		t.Fatalf("Get = %q, %v, %v", got, ok, err)
	}
}

func TestIncompressibleDataPassesThroughUnbloatedIntegration(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{
		Addresses:            []Address{addr(node.address())},
		Compress:             true,
		CompressionThreshold: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value := make([]byte, 512)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	if err := client.SetBytes("k", value, 0); err != nil {
		t.Fatal(err)
	}

	storedAny, ok := node.store.Load(storeKey{"", "k"})
	if !ok {
		t.Fatal("value not stored")
	}
	want := append([]byte{compressionMarkerRaw}, value...)
	if !bytes.Equal(storedAny.([]byte), want) {
		t.Fatalf("stored = %v, want %v", storedAny, want)
	}

	got, ok, err := client.GetBytes("k")
	if err != nil || !ok || !bytes.Equal(got, value) {
		t.Fatalf("GetBytes = %v, %v, %v", got, ok, err)
	}
}

func TestReadingALegacyValueWithCompressEnabledErrorsClearly(t *testing.T) {
	node := startMockNode(t, nil)

	// A legacy/uncompressed writer's value whose first byte happens to
	// collide with the DEFLATE marker (0x01) — value compression's
	// documented hazard of enabling Compress against a keyspace other
	// clients still touch without it. The remaining bytes are chosen to
	// reliably fail DEFLATE decoding (raw DEFLATE has no checksum, so not
	// every garbage body does — see compression_test.go's own pinned
	// test).
	node.store.Store(storeKey{"", "k"}, []byte{compressionMarkerDeflate, 0xFF, 0xFF, 0xFF, 0xFF})

	reader, err := Connect(Config{Addresses: []Address{addr(node.address())}, Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, _, err := reader.GetBytes("k"); !errors.Is(err, ErrDecompression) {
		t.Fatalf("err = %v, want ErrDecompression", err)
	}
}

// ── 遅延再接続と keep-alive ───────────────────────────────────────

func TestAMismatchedResponseKindPoisonsTheConnection(t *testing.T) {
	// A well-formed response of the wrong kind (`S` answering a G) means
	// the request/response streams are off by one; reusing the connection
	// would answer every later request with the previous one's response.
	// The mismatch poisons the connection, and the connection-classified
	// error is healed by the built-in redial-and-retry-once — never by
	// reusing the desynced stream.
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.storedToGetLeft.Add(1)
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get after mismatched response = %q, %v, %v", value, ok, err)
	}
	if got := node.connectionCount.Load(); got != 2 {
		t.Fatalf("connectionCount = %d, want 2 (poison + redial)", got)
	}
}

func TestConnectingToASilentServerFailsWithinTheDeadline(t *testing.T) {
	// A server that accepts the TCP connection but never answers the
	// handshake (a blackholed address behaves the same way) must fail the
	// connect within the deadline instead of hanging.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	original := connectDeadline
	connectDeadline = 100 * time.Millisecond
	defer func() { connectDeadline = original }()

	started := time.Now()
	_, err = Connect(Config{Addresses: []Address{addr(listener.Addr().String())}})
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Connect against a silent server = %v, want ErrConnectionLost", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Connect took %v, want well under the kernel timeout", elapsed)
	}
}

func TestAMalformedValueLengthPoisonsTheConnectionAndRetriesTransparently(t *testing.T) {
	// Regression for issue #8/#12: a garbage V header is protocol-
	// classified (issue #47 audit item G4 — see ErrProtocol), and
	// applyReconnecting's redial-and-retry-once treats that the same as a
	// connection-level failure, so the same call still succeeds, never
	// serving stray bytes.
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.malformedLeft.Add(1)
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if node.connectionCount.Load() != 2 {
		t.Fatalf("connections = %d", node.connectionCount.Load())
	}
}

// TestAMalformedFrameSurfacesAsErrProtocolNotErrConnectionLost is the
// error-taxonomy half of issue #47 audit item G4: a malformed/unexpected
// response frame is a protocol violation, not a genuine connection loss
// (EOF, reset, timeout). Queuing two malformed replies exhausts
// applyReconnecting's single redial-and-retry (see the transparent-retry
// test above), so the second failure is what the caller actually sees —
// it must be classified as ErrProtocol, not ErrConnectionLost, even though
// internally both poison the connection identically.
func TestAMalformedFrameSurfacesAsErrProtocolNotErrConnectionLost(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.malformedLeft.Add(2) // one for the original connection, one for the redial
	_, _, err = client.Get("k")
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v, want ErrProtocol", err)
	}
	if errors.Is(err, ErrConnectionLost) {
		t.Fatalf("err = %v, must not also be ErrConnectionLost", err)
	}
}

// TestAnAbruptCloseSurfacesAsErrConnectionLostNotErrProtocol is the
// counterpart to the malformed-frame test above: a genuine I/O failure
// (here, the whole node going away so both the original attempt and the
// redial fail to even connect) must keep its existing ErrConnectionLost
// classification, unaffected by the new ErrProtocol taxonomy.
func TestAnAbruptCloseSurfacesAsErrConnectionLostNotErrProtocol(t *testing.T) {
	node := startMockNode(t, nil)
	hostPort := node.address()
	client, err := Connect(Config{Addresses: []Address{addr(hostPort)}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.close()                      // nothing listens on hostPort anymore
	time.Sleep(50 * time.Millisecond) // let the FIN land and the listener release the port

	_, _, err = client.Get("k")
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("err = %v, want ErrConnectionLost", err)
	}
	if errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v, must not also be ErrProtocol", err)
	}
}

func TestTransparentlyReconnectsAfterAServerFin(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.dropConnections()
	time.Sleep(50 * time.Millisecond) // let the FIN land

	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get after FIN = %q, %v, %v", value, ok, err)
	}
	if node.connectionCount.Load() != 2 {
		t.Fatalf("connections = %d", node.connectionCount.Load())
	}
}

func TestReconnectCooldownSkipsAKnownDeadAddress(t *testing.T) {
	node := startMockNode(t, nil)
	hostPort := node.address()
	client, err := Connect(Config{
		Addresses:         []Address{addr(hostPort)},
		ReconnectCooldown: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.close()
	time.Sleep(50 * time.Millisecond) // let the FIN land and the listener release the port

	// Nothing listens on hostPort anymore, so this redial fails fast
	// (connection refused) and starts the cooldown window for that
	// address.
	if _, _, err := client.Get("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Get after node close = %v, want ErrConnectionLost", err)
	}

	// A listener now sits on the same address and answers immediately
	// with bytes the identify handshake rejects outright — deliberately
	// not the closed/EOF/reset-before-any-reply shape that signals a
	// legacy server (identify.go's isLegacyServerSignal), so each dial
	// against it fails after exactly one connection, letting connections
	// below tell "cooldown skipped the dial" apart from "cooldown let it
	// through" unambiguously.
	var garbage net.Listener
	for attempt := 0; ; attempt++ {
		l, listenErr := net.Listen("tcp", hostPort)
		if listenErr == nil {
			garbage = l
			break
		}
		if attempt >= 50 {
			t.Fatalf("could not rebind %s: %v", hostPort, listenErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer garbage.Close()

	var connections atomic.Int32
	go func() {
		for {
			conn, acceptErr := garbage.Accept()
			if acceptErr != nil {
				return
			}
			connections.Add(1)
			_, _ = conn.Write([]byte("XXX"))
		}
	}()

	// Still within the cooldown window: rejects with the cached failure
	// near-instantly, without dialing the listener at all.
	started := time.Now()
	if _, _, err := client.Get("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Get within the cooldown window = %v, want ErrConnectionLost", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("expected a cooldown-fast rejection, took %v", elapsed)
	}
	if got := connections.Load(); got != 0 {
		t.Fatalf("the cooldown did not prevent a redial: connections = %d", got)
	}

	// Once the cooldown window has passed, the address is dialed again,
	// this time reaching the listener.
	time.Sleep(250 * time.Millisecond)
	_, _, err = client.Get("k")
	if err == nil || !strings.Contains(err.Error(), "unexpected response to A") {
		t.Fatalf("Get after the cooldown elapsed = %v, want an unexpected-response-to-A error", err)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("the address was never redialed after the cooldown elapsed: connections = %d", got)
	}
}

func TestNegativeReconnectCooldownDisablesTheCooldown(t *testing.T) {
	node := startMockNode(t, nil)
	hostPort := node.address()
	client, err := Connect(Config{
		Addresses:         []Address{addr(hostPort)},
		ReconnectCooldown: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	node.close()
	time.Sleep(50 * time.Millisecond) // let the FIN land and the listener release the port

	if _, _, err := client.Get("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Get after node close = %v, want ErrConnectionLost", err)
	}

	var garbage net.Listener
	for attempt := 0; ; attempt++ {
		l, listenErr := net.Listen("tcp", hostPort)
		if listenErr == nil {
			garbage = l
			break
		}
		if attempt >= 50 {
			t.Fatalf("could not rebind %s: %v", hostPort, listenErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer garbage.Close()

	var connections atomic.Int32
	go func() {
		for {
			conn, acceptErr := garbage.Accept()
			if acceptErr != nil {
				return
			}
			connections.Add(1)
			_, _ = conn.Write([]byte("XXX"))
		}
	}()

	// With the cooldown disabled (negative ReconnectCooldown), this
	// redials immediately instead of reusing the cached failure.
	_, _, err = client.Get("k")
	if err == nil || !strings.Contains(err.Error(), "unexpected response to A") {
		t.Fatalf("Get with the cooldown disabled = %v, want an unexpected-response-to-A error", err)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("a disabled cooldown should redial immediately: connections = %d", got)
	}
}

func TestKeepAlivePingsAnIdleConnection(t *testing.T) {
	// Keep-alive is always on with an internal interval (issue #27); the
	// package variable exists only so tests can shorten it.
	defaultInterval := keepAliveInterval
	keepAliveInterval = 40 * time.Millisecond
	defer func() { keepAliveInterval = defaultInterval }()

	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	waitFor(t, func() bool { return node.getCount.Load() >= 2 }, "keep-alive pings")
	if node.connectionCount.Load() != 1 {
		t.Fatalf("connections = %d", node.connectionCount.Load())
	}
}

// TestKeepAlivePingsIdleConnectionsInParallel is a regression for issue
// #192: pings used to run sequentially, one connection at a time, so a
// slow or hung node delayed the ping reaching every other member by its
// own response time. Driving pingIdleConnections directly (rather than
// through startKeepalive's ticker) makes this deterministic — connection
// order no longer matters, since with N connections all delayed by d,
// sequential pinging takes ~N*d while parallel pinging takes ~d
// regardless of N.
func TestKeepAlivePingsIdleConnectionsInParallel(t *testing.T) {
	const (
		nodeCount = 5
		delay     = 150 * time.Millisecond
	)

	connections := make([]*connection, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		node := startMockNode(t, nil)
		node.delayGets(delay)
		result, err := connectAndIdentify(node.address(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		conn := newConnection(result.conn, func() {}, result.tagged, nil)
		defer conn.close()
		connections = append(connections, conn)
	}

	// idle() must already exceed the interval for pingIdleConnections to
	// probe a connection at all.
	interval := time.Millisecond
	time.Sleep(2 * time.Millisecond)

	started := time.Now()
	pingIdleConnections(connections, interval)
	elapsed := time.Since(started)

	// Sequential pinging of 5 connections each delayed 150ms would take
	// ~750ms; parallel pinging takes ~150ms regardless of how many.
	if elapsed > 400*time.Millisecond {
		t.Fatalf("pingIdleConnections(%d connections, %v delay each) took %v, want well under %v (sequential would take ~%v)",
			nodeCount, delay, elapsed, 400*time.Millisecond, time.Duration(nodeCount)*delay)
	}
}

// TestCloseWaitsForTheKeepaliveGoroutine is a regression for issue #192:
// Close() used to only signal the keepalive goroutine to stop
// (close(stopKeepalive)) without waiting for it to actually exit — a ping
// already in flight against a connection could still be running when
// teardown() closed that same connection out from under it.
func TestCloseWaitsForTheKeepaliveGoroutine(t *testing.T) {
	defaultInterval := keepAliveInterval
	keepAliveInterval = 20 * time.Millisecond
	defer func() { keepAliveInterval = defaultInterval }()

	node := startMockNode(t, nil)
	node.delayGets(150 * time.Millisecond)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a keep-alive ping to actually reach the node — getCount is
	// incremented before the mock node sleeps out the delay, so this
	// confirms a ping is now blocked in flight.
	waitFor(t, func() bool { return node.getCount.Load() >= 1 }, "a keep-alive ping to start")

	started := time.Now()
	client.Close()
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("Close() returned after %v, want it to wait out the in-flight keep-alive ping (~150ms)", elapsed)
	}
}

func TestARequestToAHalfOpenServerFailsWithinTheTimeoutAndCloseReturns(t *testing.T) {
	// Regression: a server that completes the A handshake but then never
	// answers a G/S/D (accepts the TCP connection and goes silent — a
	// blackholed peer behaves the same way once the deadline is cleared
	// after the handshake) must not hang Get/Set/Delete, or transitively
	// Close(), forever.
	original := requestTimeout
	requestTimeout = 100 * time.Millisecond
	defer func() { requestTimeout = original }()

	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	node.goSilentAfterHandshake()

	started := time.Now()
	if _, _, err := client.Get("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Get against a half-open connection = %v, want ErrConnectionLost", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Get took %v, want well under 2s", elapsed)
	}

	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return")
	}
}

func TestSteadyNewRequestsDoNotPostponeHalfOpenDetection(t *testing.T) {
	// Regression: the deadline used to be re-armed on *every* new
	// request, so steady traffic against a server that had gone silent
	// pushed it forever ahead and the oldest pending request was never
	// timed out. The deadline is progress-based now — new sends must not
	// extend it while an older request is still waiting.
	original := requestTimeout
	requestTimeout = 200 * time.Millisecond
	defer func() { requestTimeout = original }()

	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	node.goSilentAfterHandshake()

	// New requests keep arriving well inside every deadline window
	// (once the connection is poisoned they just fail fast).
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				_, _, _ = client.Get("more")
			}
		}
	}()

	started := time.Now()
	if _, _, err := client.Get("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Get against a half-open connection under steady traffic = %v, want ErrConnectionLost", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Get took %v, want well under 2s", elapsed)
	}
}

// TestAnUnterminatedResponseHeaderFailsInsteadOfGrowingWithoutBound is a
// regression for the unbounded bufio.Reader.ReadString('\n') this SDK
// used to read every response header: a peer that never sends the '\n'
// terminator made the client's read buffer grow without bound (issue
// #47 audit; mirrors Rust's MAX_HEADER_LINE_LENGTH regression coverage
// in rust/tests/client.rs). readLine's maxHeaderLineLength cap must
// fail the request instead.
func TestAnUnterminatedResponseHeaderFailsInsteadOfGrowingWithoutBound(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// Loops accepting connections (mirrors mockNode.acceptLoop): every
	// connection gets the same treatment, so the client's built-in
	// redial-and-retry-once — which also fires on ErrProtocol, not just
	// ErrConnectionLost (see
	// TestAMalformedValueLengthPoisonsTheConnectionAndRetriesTransparently)
	// — hits an oversized header again on the retry instead of a dead
	// listener backlog entry, keeping this test fast regardless of that
	// retry.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)

				// The `A` handshake.
				header, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				parts := strings.Fields(header)
				_ = mustRead(reader, atoiOrPanic(parts[1])) // the secret
				if _, err := conn.Write([]byte("On\n")); err != nil {
					return
				}

				// The `G` request.
				header, err = reader.ReadString('\n')
				if err != nil {
					return
				}
				parts = strings.Fields(header)
				_ = mustRead(reader, atoiOrPanic(parts[1])) // the key

				// A `V` header that streams 5 KiB and never terminates
				// with '\n'.
				_, _ = conn.Write(append([]byte("V"), bytes.Repeat([]byte("9"), 5*1024)...))
			}(conn)
		}
	}()

	client, err := Connect(Config{Addresses: []Address{addr(listener.Addr().String())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	started := time.Now()
	// An unterminated header is a corrupt/hostile-peer condition, not a
	// genuine I/O failure — issue #47 audit item G4 classifies it as
	// ErrProtocol, not ErrConnectionLost (see errors.go's protocolError).
	if _, _, err := client.Get("k"); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Get against a peer streaming an unterminated header = %v, want ErrProtocol", err)
	} else if errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Get against a peer streaming an unterminated header = %v, must not also be ErrConnectionLost", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Get took %v, want well under 2s (bounded by maxHeaderLineLength, not requestTimeout)", elapsed)
	}
}

// fakeFinalChunkWithEOF is an io.Reader that delivers its entire
// remaining payload and io.EOF in the same Read call — the shape a real
// net.Conn can produce when a peer writes its last bytes and closes in
// the same flush.
type fakeFinalChunkWithEOF struct {
	data []byte
}

func (f *fakeFinalChunkWithEOF) Read(p []byte) (int, error) {
	n := copy(p, f.data)
	f.data = f.data[n:]
	return n, io.EOF
}

// TestReadFullSucceedsWhenTheFinalReadDeliversAllBytesAndEOFTogether is a
// regression for the hand-rolled readFull this SDK used to have: it
// returned whatever error the final underlying Read produced even when
// that Read had delivered every remaining byte (issue #47 audit),
// wrongly failing a peer that writes its last bytes and closes in the
// same flush. readFull now wraps io.ReadFull, whose io.ReadAtLeast
// forces the error to nil once enough bytes have been read regardless
// of what accompanied them.
func TestReadFullSucceedsWhenTheFinalReadDeliversAllBytesAndEOFTogether(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 32)
	source := &fakeFinalChunkWithEOF{data: append([]byte(nil), payload...)}
	// A bufio.Reader whose internal buffer (bufio's own 16-byte floor)
	// is no larger than the destination slice below forces Read to
	// bypass its own buffering and call source.Read directly with the
	// full-sized p — the only way to observe a single Read that both
	// completes the request and reports an error together.
	reader := bufio.NewReaderSize(source, 16)
	buf := make([]byte, len(payload))
	n, err := readFull(reader, buf)
	if err != nil {
		t.Fatalf("readFull = _, %v, want nil", err)
	}
	if n != len(payload) || !bytes.Equal(buf, payload) {
		t.Fatalf("readFull = %q, %d, want %q, %d", buf, n, payload, len(payload))
	}
}

// ── addresses ─────────────────────────────────────────────────────

func TestRejectsAMissingTarget(t *testing.T) {
	if _, err := Connect(Config{}); err == nil {
		t.Fatal("empty addresses accepted")
	}
}

func TestFailsOverToTheSecondAddress(t *testing.T) {
	node := startMockNode(t, nil)
	discovery := startMockDiscovery(t,
		[]discoveredNode{{Name: testNames[0], Address: node.address()}}, 1)

	client, err := Connect(Config{Addresses: []Address{addr(unusedPort(t)), addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Get("k"); err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
}

func TestRaisesBusyWhenEveryAddressIsWarming(t *testing.T) {
	first := startMockDiscovery(t, nil, 1)
	second := startMockDiscovery(t, nil, 1)
	first.setWarming(true)
	second.setWarming(true)

	if _, err := Connect(Config{Addresses: []Address{addr(first.address()), addr(second.address())}}); !errors.Is(err, ErrDiscoveryBusy) {
		t.Fatalf("err = %v", err)
	}
}

func TestDiscoveryNodeListExceedingTheAggregateCapIsRejected(t *testing.T) {
	// Regression: maxNodeCount and maxNodeFieldLength bound each field of
	// an N response, but not its aggregate size — a discovery server
	// claiming many maxNodeFieldLength-sized entries could otherwise make
	// the client accumulate tens of megabytes (~8.5GB at the theoretical
	// extreme: maxNodeCount * 2 * maxNodeFieldLength) from a single L
	// response. This test's entries hit the (legal) per-field max, just
	// enough of them to cross maxNodeListResponseBytes — well short of
	// maxNodeCount — so the aggregate cap specifically is what trips.
	const fieldLen = maxNodeFieldLength
	entryBytes := 2*fieldLen + 1 // name + address + trailing '\n'
	count := maxNodeListResponseBytes/entryBytes + 2

	name := strings.Repeat("n", fieldLen)
	address := strings.Repeat("a", fieldLen)
	nodes := make([]discoveredNode, count)
	for i := range nodes {
		nodes[i] = discoveredNode{Name: name, Address: address}
	}

	discovery := startMockDiscovery(t, nodes, 1)
	_, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err == nil {
		t.Fatal("expected an error for a node-list response exceeding the aggregate cap")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want an aggregate-cap error", err)
	}
}

// ── クラスタと複製 ────────────────────────────────────────────────

func startCluster(t *testing.T, replication int) (map[string]*mockNode, *mockDiscovery) {
	t.Helper()
	nodes := map[string]*mockNode{
		testNames[0]: startMockNode(t, nil),
		testNames[1]: startMockNode(t, nil),
	}
	listed := make([]discoveredNode, 0, len(nodes))
	for _, name := range testNames {
		listed = append(listed, discoveredNode{Name: name, Address: nodes[name].address()})
	}
	return nodes, startMockDiscovery(t, listed, replication)
}

func ownersOf(key string) []string {
	return NewHashRing(testNames).Owners([]byte(key), 2)
}

func TestRoutesAndReadsItsOwnWrites(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i := 0; i < 50; i++ {
		if err := client.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		value, ok, err := client.Get(fmt.Sprintf("key-%d", i))
		if err != nil || !ok || value != fmt.Sprintf("value-%d", i) {
			t.Fatalf("key-%d = %q, %v, %v", i, value, ok, err)
		}
	}

	total := 0
	for name, node := range nodes {
		size := node.storeLen()
		if size == 0 {
			t.Errorf("%s holds nothing", name)
		}
		total += size
	}
	if total != 50 {
		t.Fatalf("total stored = %d", total)
	}
}

func TestWrongNodeTriggersRefreshAndOneRetry(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("some-key", "v", 0); err != nil {
		t.Fatal(err)
	}
	primary, err := NewHashRing(testNames).Route([]byte("some-key"))
	if err != nil {
		t.Fatal(err)
	}
	owner := nodes[primary]

	owner.wrongNodeLeft.Add(1)
	if value, ok, err := client.Get("some-key"); err != nil || !ok || value != "v" {
		t.Fatalf("Get after one W = %q, %v, %v", value, ok, err)
	}

	owner.wrongNodeLeft.Add(2)
	if _, _, err := client.Get("some-key"); !errors.Is(err, ErrWrongNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestFansWritesOutToEveryOwner(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if client.Replication() != 2 {
		t.Fatalf("Replication = %d", client.Replication())
	}
	for i := 0; i < 20; i++ {
		if err := client.Set(fmt.Sprintf("key-%d", i), "v", 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		for name, node := range nodes {
			if !node.hasKey(fmt.Sprintf("key-%d", i)) {
				t.Errorf("key-%d missing from %s", i, name)
			}
		}
	}
}

func TestReadsFailOverWhenThePrimaryDies(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("survives", "still here", 0); err != nil {
		t.Fatal(err)
	}
	nodes[ownersOf("survives")[0]].close()
	time.Sleep(50 * time.Millisecond)

	value, ok, err := client.Get("survives")
	if err != nil || !ok || value != "still here" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
}

func TestADeadReplicaDoesNotFailWrites(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	owners := ownersOf("written-anyway")
	nodes[owners[1]].close()
	time.Sleep(50 * time.Millisecond)

	if err := client.Set("written-anyway", "v", 0); err != nil {
		t.Fatal(err)
	}
	if !nodes[owners[0]].hasKey("written-anyway") {
		t.Fatal("primary missing the key")
	}
}

func TestWritesRouteAroundADeadPrimaryOnceDiscoveryDropsIt(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const key = "written-after-primary-death"
	owners := ownersOf(key)

	// The primary dies AND discovery has already noticed: the first write
	// attempt fails on the dead primary, forcing a refresh that re-ranks
	// onto the survivor, and the retry succeeds.
	nodes[owners[0]].close()
	discovery.setNodes([]discoveredNode{{Name: owners[1], Address: nodes[owners[1]].address()}})
	time.Sleep(50 * time.Millisecond)

	if err := client.Set(key, "v", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Get(key); err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
}

func TestFansDeletesOutToEveryOwner(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("gone-everywhere", "v", 0); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Delete("gone-everywhere"); err != nil || !existed {
		t.Fatalf("Delete = %v, %v", existed, err)
	}
	for name, node := range nodes {
		if node.hasKey("gone-everywhere") {
			t.Errorf("still present on %s", name)
		}
	}
}

// ── batched get/set, single node (issues #128/#150/#151) ─────────────

func TestGetManyReturnsHitsAndMissesInOneCall(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("a", "1", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.Set("c", "3", 0); err != nil {
		t.Fatal(err)
	}

	values, err := client.GetMany([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("GetMany err = %v", err)
	}
	want := map[string]string{"a": "1", "c": "3"}
	if len(values) != len(want) || values["a"] != "1" || values["c"] != "3" {
		t.Fatalf("GetMany = %v, want %v (with \"b\" absent)", values, want)
	}
	if _, missing := values["b"]; missing {
		t.Fatal("expected \"b\" to be absent from the map, not present with a zero value")
	}
	if got := node.mCount.Load(); got != 1 {
		t.Fatalf("mCount = %d, want 1", got)
	}
}

func TestSetManyThenGetManyRoundTripWithTTL(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 60); err != nil {
		t.Fatalf("SetMany err = %v", err)
	}
	if ttl, ok := node.storedTTL("a"); !ok || ttl != 60 {
		t.Fatalf("storedTTL(a) = %d, %v, want 60, true", ttl, ok)
	}
	if got := node.oCount.Load(); got != 1 {
		t.Fatalf("oCount = %d, want 1", got)
	}

	values, err := client.GetMany([]string{"a", "b"})
	if err != nil {
		t.Fatalf("GetMany err = %v", err)
	}
	if values["a"] != "1" || values["b"] != "2" {
		t.Fatalf("GetMany = %v, want {a:1 b:2}", values)
	}
}

func TestGetManyBytesRequiresAtLeastOneKey(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	values, err := client.GetMany(nil)
	if values != nil || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("GetMany(nil) = %v, %v, want nil, ErrInvalidArgument", values, err)
	}
}

func TestSetManyBytesRequiresAtLeastOneKey(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.SetMany(map[string]string{}, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SetMany({}) err = %v, want ErrInvalidArgument", err)
	}
}

// TestBatchLargerThanMaxBatchKeysSplitsIntoMultipleSubFrames covers
// batch chunking (maxBatchKeys, issues #128/#150/#151): a call with
// more keys than fit in one `m`/`o` sub-frame transparently becomes
// more than one, invisible to the caller beyond the extra sub-frames on
// the wire.
func TestBatchLargerThanMaxBatchKeysSplitsIntoMultipleSubFrames(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const total = maxBatchKeys + 50
	values := make(map[string]string, total)
	keys := make([]string, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys[i] = key
		values[key] = fmt.Sprintf("value-%d", i)
	}

	if err := client.SetMany(values, 0); err != nil {
		t.Fatalf("SetMany err = %v", err)
	}
	if got := node.oCount.Load(); got != 2 {
		t.Fatalf("oCount = %d, want 2 (%d keys split at maxBatchKeys=%d)", got, total, maxBatchKeys)
	}

	got, err := client.GetMany(keys)
	if err != nil {
		t.Fatalf("GetMany err = %v", err)
	}
	if got2 := node.mCount.Load(); got2 != 2 {
		t.Fatalf("mCount = %d, want 2", got2)
	}
	if len(got) != total {
		t.Fatalf("GetMany returned %d keys, want %d", len(got), total)
	}
	for key, want := range values {
		if got[key] != want {
			t.Fatalf("GetMany[%q] = %q, want %q", key, got[key], want)
		}
	}
}

// ── batched get/set, cluster (issues #128/#150/#151) ─────────────────

func TestClusterGetManySplitsAcrossOwnersAndReassembles(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	values := make(map[string]string, 20)
	keys := make([]string, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys[i] = key
		values[key] = fmt.Sprintf("value-%d", i)
	}
	if err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}

	got, err := client.GetMany(keys)
	if err != nil {
		t.Fatalf("GetMany err = %v", err)
	}
	for key, want := range values {
		if got[key] != want {
			t.Fatalf("GetMany[%q] = %q, want %q", key, got[key], want)
		}
	}

	// With 20 keys spread over 2 owners by HRW, both nodes should have
	// answered at least one `m` — proving the batch really was split by
	// owner rather than all sent to a single node.
	for name, node := range nodes {
		if node.mCount.Load() == 0 {
			t.Errorf("%s received no m frames at all", name)
		}
	}
}

func TestClusterSetManyStoresOnEveryOwnerWithReplication2(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	values := make(map[string]string, 10)
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys[i] = key
		values[key] = fmt.Sprintf("value-%d", i)
	}
	if err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		for name, node := range nodes {
			if !node.hasKey(key) {
				t.Errorf("%s missing %s", name, key)
			}
		}
	}
}

// TestClusterSetManySendsExactlyOneSubFrameToANodeThatIsPrimaryForOneKeyAndReplicaForAnother
// exercises SetManyBytes' owner-address grouping (docs/protocol.html#multi):
// within one batch the same node can be primary for one key and a
// replica for another, and it must receive exactly one `o` sub-frame
// covering both roles, not two. Two nodes with replication 2 is enough
// to force this (every key's owners are both nodes, in an order that
// differs per key) — mirroring the Rust proxy's own equivalent test.
func TestClusterSetManySendsExactlyOneSubFrameToANodeThatIsPrimaryForOneKeyAndReplicaForAnother(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	keyA := keyWithPrimary(t, testNames[0])
	keyB := keyWithPrimary(t, testNames[1])

	if err := client.SetMany(map[string]string{keyA: "va", keyB: "vb"}, 0); err != nil {
		t.Fatal(err)
	}

	for name, node := range nodes {
		if got := node.oCount.Load(); got != 1 {
			t.Fatalf("%s oCount = %d, want 1 (one sub-frame covering both its primary and replica key)", name, got)
		}
		if !node.hasKey(keyA) || !node.hasKey(keyB) {
			t.Fatalf("%s missing one of keyA/keyB", name)
		}
	}
}

func TestClusterGetManyRecoversAPerKeyWrongNodeAfterARefresh(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const wrongKey, okKey = "wrong-key", "ok-key"
	if err := client.Set(wrongKey, "w", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(okKey, "ok", 0); err != nil {
		t.Fatal(err)
	}

	primary, err := NewHashRing(testNames).Route([]byte(wrongKey))
	if err != nil {
		t.Fatal(err)
	}
	owner := nodes[primary]
	owner.multiWrongNodeKey.Store(wrongKey)
	owner.multiWrongNodeLeft.Add(1)

	values, err := client.GetMany([]string{wrongKey, okKey})
	if err != nil {
		t.Fatalf("GetMany after one per-key W = %v, %v, want success", values, err)
	}
	if values[wrongKey] != "w" || values[okKey] != "ok" {
		t.Fatalf("GetMany = %v, want both keys resolved", values)
	}
}

func TestClusterGetManyDegradesToAPartialMapWithErrWrongNodeWhenWrongNodePersists(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const wrongKey, okKey = "wrong-key", "ok-key"
	if err := client.Set(wrongKey, "w", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(okKey, "ok", 0); err != nil {
		t.Fatal(err)
	}

	primary, err := NewHashRing(testNames).Route([]byte(wrongKey))
	if err != nil {
		t.Fatal(err)
	}
	owner := nodes[primary]
	owner.multiWrongNodeKey.Store(wrongKey)
	owner.multiWrongNodeLeft.Add(2) // survives the initial pass AND the one retry

	values, err := client.GetMany([]string{wrongKey, okKey})
	if !errors.Is(err, ErrWrongNode) {
		t.Fatalf("GetMany err = %v, want ErrWrongNode", err)
	}
	if values[okKey] != "ok" {
		t.Fatalf("GetMany = %v, want okKey still resolved despite wrongKey's persistent W", values)
	}
	if _, present := values[wrongKey]; present {
		t.Fatalf("GetMany = %v, want wrongKey absent (never resolved)", values)
	}
}

func TestClusterSetManyRecoversAPerKeyWrongNodeAfterARefresh(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const wrongKey, okKey = "wrong-key", "ok-key"
	primary, err := NewHashRing(testNames).Route([]byte(wrongKey))
	if err != nil {
		t.Fatal(err)
	}
	owner := nodes[primary]
	owner.multiWrongNodeKey.Store(wrongKey)
	owner.multiWrongNodeLeft.Add(1)

	if err := client.SetMany(map[string]string{wrongKey: "w", okKey: "ok"}, 0); err != nil {
		t.Fatalf("SetMany after one per-key W = %v, want success", err)
	}
	if !owner.hasKey(wrongKey) || !owner.hasKey(okKey) {
		t.Fatal("expected both keys stored after the retry recovered from the per-key W")
	}
}

// ── issue #67: connect() bootstrap tolerates an unreachable node ──

// keyWithPrimary finds a key whose primary owner is name, against
// testNames/replication 2 — the same ring startCluster's discovery mocks
// use.
func keyWithPrimary(t *testing.T, name string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if ownersOf(key)[0] == name {
			return key
		}
	}
	t.Fatalf("no key routes to %s as primary", name)
	return ""
}

// startClusterWithDeadNode is startCluster, except every name in dead is
// listed by discovery at an address nobody listens on instead of a real
// mockNode — issue #67's bootstrap-tolerance tests. The returned map only
// holds mockNodes for the live names; deadAddresses gives back the
// unreachable address discovery listed for each dead name, so a test can
// later start a real listener on that same address.
func startClusterWithDeadNode(t *testing.T, replication int, dead map[string]bool) (
	nodes map[string]*mockNode, deadAddresses map[string]string, discovery *mockDiscovery,
) {
	t.Helper()
	nodes = map[string]*mockNode{}
	deadAddresses = map[string]string{}
	listed := make([]discoveredNode, 0, len(testNames))
	for _, name := range testNames {
		if dead[name] {
			address := unusedPort(t)
			deadAddresses[name] = address
			listed = append(listed, discoveredNode{Name: name, Address: address})
			continue
		}
		node := startMockNode(t, nil)
		nodes[name] = node
		listed = append(listed, discoveredNode{Name: name, Address: node.address()})
	}
	return nodes, deadAddresses, startMockDiscovery(t, listed, replication)
}

func TestConnectSucceedsWithOneUnreachableNode(t *testing.T) {
	dead, live := testNames[0], testNames[1]
	nodes, _, discovery := startClusterWithDeadNode(t, 2, map[string]bool{dead: true})

	client, err := Connect(Config{
		Addresses:         []Address{addr(discovery.address())},
		ReconnectCooldown: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Connect with one unreachable node = %v, want success", err)
	}
	defer client.Close()

	if got := client.Replication(); got != 2 {
		t.Fatalf("Replication = %d, want 2", got)
	}
	if !client.members[dead].connection.isClosed() {
		t.Fatal("the unreachable node's member has a live connection, want none")
	}
	if client.members[live].connection.isClosed() {
		t.Fatal("the reachable node's member has no live connection")
	}

	// A key whose primary is alive: the write lands, the dead replica leg
	// is swallowed and counted, and the read hits.
	key := keyWithPrimary(t, live)
	if err := client.Set(key, "v", 0); err != nil {
		t.Fatalf("Set on a live primary (dead replica) = %v", err)
	}
	if value, ok, err := client.Get(key); err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if got := client.Stats().ReplicaWriteFailures; got != 1 {
		t.Fatalf("ReplicaWriteFailures = %d, want 1", got)
	}

	// A key whose primary is the dead node: the read fails over to the
	// live replica right away (cooldown armed at bootstrap, no dial).
	other := keyWithPrimary(t, dead)
	nodes[live].store.Store(storeKey{"", other}, []byte("replica copy"))
	start := time.Now()
	value, ok, err := client.Get(other)
	elapsed := time.Since(start)
	if err != nil || !ok || value != "replica copy" {
		t.Fatalf("Get with a dead primary = %q, %v, %v", value, ok, err)
	}
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("elapsed = %v, want well under the dial timeout (failover via the armed cooldown)", elapsed)
	}
}

func TestConnectFailsOnlyWhenEveryNodeIsUnreachable(t *testing.T) {
	_, _, discovery := startClusterWithDeadNode(t, 2, map[string]bool{testNames[0]: true, testNames[1]: true})

	_, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Connect with every node unreachable = %v, want ErrConnectionLost", err)
	}
}

func TestRefreshPurgesCooldownsForDepartedAddresses(t *testing.T) {
	// #96: a node that leaves the cluster must not leave its per-address
	// reconnect-cooldown entry behind — in a churny deployment (a fresh
	// IP:port per restart) those would accumulate unboundedly.
	dead, live := testNames[0], testNames[1]
	nodes, deadAddresses, discovery := startClusterWithDeadNode(t, 2, map[string]bool{dead: true})

	client, err := Connect(Config{
		Addresses:         []Address{addr(discovery.address())},
		ReconnectCooldown: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	deadAddr := deadAddresses[dead]
	hasCooldown := func(address string) bool {
		client.redialCooldownMu.Lock()
		defer client.redialCooldownMu.Unlock()
		_, ok := client.redialCooldowns[address]
		return ok
	}
	// The unreachable node armed its cooldown at bootstrap.
	if !hasCooldown(deadAddr) {
		t.Fatalf("no cooldown armed for the unreachable node %s", deadAddr)
	}

	// Discovery drops the dead node from the roster; the next refresh
	// reconciles membership and must purge its cooldown alongside it.
	discovery.setNodes([]discoveredNode{{Name: live, Address: nodes[live].address()}})
	client.maybeRefresh(true)

	if _, ok := client.members[dead]; ok {
		t.Fatalf("departed node %s still present in members", dead)
	}
	if hasCooldown(deadAddr) {
		t.Fatalf("cooldown for departed address %s was not purged", deadAddr)
	}
}

func TestAnUnreachableNodeIsRedialedOnceTheCooldownHasPassed(t *testing.T) {
	dead := testNames[0]
	_, deadAddresses, discovery := startClusterWithDeadNode(t, 2, map[string]bool{dead: true})

	client, err := Connect(Config{
		Addresses:         []Address{addr(discovery.address())},
		ReconnectCooldown: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Connect with one unreachable node = %v, want success", err)
	}
	defer client.Close()

	// Bring the "dead" node up on the address discovery listed.
	revived := startMockNodeAt(t, nil, deadAddresses[dead])

	key := keyWithPrimary(t, dead)
	if !waitUntil(t, 2*time.Second, func() bool {
		return client.Set(key, "v", 0) == nil
	}) {
		t.Fatal("Set never succeeded once the revived node came up")
	}
	if !revived.hasKey(key) {
		t.Fatal("the revived node never received the write")
	}
	if client.members[dead].connection.isClosed() {
		t.Fatal("the revived member has no live connection after a successful write to it")
	}
}

// ── fire-and-forget レプリカ書き込み (fire-and-forget replica writes) ──────────────

// A "did it wait for the mock's delay" assertion can't compare the
// measured elapsed time against the delay exactly: time.Sleep only
// guarantees *at least* the requested duration, but scheduling jitter
// around the boundary makes an exact-equality-style check flaky in
// spirit even when it's technically one-sided. Slack the lower bound by
// this much rather than asserting on the boundary; still miles away from
// the ~0ms an immediate return would show.
const timingToleranceMs = 20 * time.Millisecond

func TestByDefaultAWriteStillWaitsForTheReplicaLeg(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].delaySets(80 * time.Millisecond)

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	start := time.Now()
	if err := client.Set(key, "v", 0); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond-timingToleranceMs {
		t.Fatalf("Set returned after %v, want >= 80ms (should have waited for the replica)", elapsed)
	}
}

func TestFireAndForgetReplicasReturnsAsSoonAsThePrimaryAcks(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].delaySets(200 * time.Millisecond)

	client, err := Connect(Config{
		Addresses:             []Address{addr(discovery.address())},
		FireAndForgetReplicas: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	start := time.Now()
	if err := client.Set(key, "v", 0); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= 200*time.Millisecond {
		t.Fatalf("Set returned after %v, want well under the replica's 200ms delay", elapsed)
	}

	// The background write still lands eventually.
	if !waitUntil(t, 2*time.Second, func() bool { return nodes[owners[1]].hasKey(key) }) {
		t.Fatal("replica never received the background write")
	}
}

func TestFireAndForgetReplicasFallsBackToSynchronousPastTheCap(t *testing.T) {
	original := maxInFlightBackgroundReplicaWrites
	maxInFlightBackgroundReplicaWrites = 2
	defer func() { maxInFlightBackgroundReplicaWrites = original }()

	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].delaySets(150 * time.Millisecond)

	client, err := Connect(Config{
		Addresses:             []Address{addr(discovery.address())},
		FireAndForgetReplicas: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	elapsed := make([]time.Duration, 3)
	var wg sync.WaitGroup
	for i := range elapsed {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			if err := client.Set(key, "v", 0); err != nil {
				t.Error(err)
			}
			elapsed[i] = time.Since(start)
		}(i)
	}
	wg.Wait()

	fast, slow := 0, 0
	for _, e := range elapsed {
		if e >= 150*time.Millisecond-timingToleranceMs {
			slow++
		} else {
			fast++
		}
	}
	if slow == 0 {
		t.Fatal("expected at least one call to fall back to synchronous past the cap")
	}
	if fast == 0 {
		t.Fatal("expected at least one call to return fast (below the cap)")
	}
}

func TestCloseDrainsInFlightBackgroundReplicaWrites(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].delaySets(80 * time.Millisecond)

	client, err := Connect(Config{
		Addresses:             []Address{addr(discovery.address())},
		FireAndForgetReplicas: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Set(key, "v", 0); err != nil {
		t.Fatal(err)
	}
	client.Close() // should block until the still-in-flight replica write lands

	if !nodes[owners[1]].hasKey(key) {
		t.Fatal("Close() returned before the background replica write finished")
	}
}

// ── read repair (read repair) ────────────────────────────────

func TestByDefaultACleanMissOnThePrimaryIsNotRepaired(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].store.Store(storeKey{"", key}, []byte("from-replica"))

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, ok, err := client.GetBytes(key)
	if err != nil || ok {
		t.Fatalf("GetBytes = ok=%v err=%v, want a clean miss", ok, err)
	}
	if nodes[owners[0]].hasKey(key) {
		t.Fatal("primary was repaired despite ReadRepair being off")
	}
}

func TestReadRepairFindsAValueOnAReplicaAndRepairsThePrimary(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].store.Store(storeKey{"", key}, []byte("from-replica"))

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ReadRepair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value, ok, err := client.GetBytes(key)
	if err != nil || !ok || string(value) != "from-replica" {
		t.Fatalf("GetBytes = %q, %v, %v", value, ok, err)
	}

	// The primary must not be re-probed by read repair — it already
	// missed once on the normal read path.
	if got := nodes[owners[0]].getCount.Load(); got != 1 {
		t.Fatalf("primary getCount = %d, want 1 (read repair must not re-probe it)", got)
	}

	if !waitUntil(t, 2*time.Second, func() bool { return nodes[owners[0]].hasKey(key) }) {
		t.Fatal("the primary was never repaired")
	}
	if got := nodes[owners[0]].lastSetTTL.Load(); got != "60" {
		t.Fatalf("repair TTL = %v, want %d (readRepairTTL, not immortal)", got, readRepairTTL)
	}
}

func TestReadRepairStaysACleanMissWhenNoOwnerHasTheValue(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	owners := ownersOf("nowhere")
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ReadRepair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, ok, err := client.GetBytes("nowhere")
	if err != nil || ok {
		t.Fatalf("GetBytes = ok=%v err=%v, want a clean miss", ok, err)
	}

	// Every owner is probed exactly once: the primary by the normal read
	// path, the rest by read repair — never the primary twice.
	for _, name := range owners {
		if got := nodes[name].getCount.Load(); got != 1 {
			t.Fatalf("owner %s getCount = %d, want exactly 1", name, got)
		}
	}
}

// ── Stats() (client-side replication / fire-and-forget replica writes / read repair swallowed-failure counters) ────────

func TestADeadReplicaCountsAReplicaWriteFailureInStats(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	owners := ownersOf("k")
	nodes[owners[1]].close() // the replica is unreachable
	time.Sleep(50 * time.Millisecond)

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err) // the primary still succeeds; the dead replica must not fail the write
	}
	waitFor(t, func() bool { return client.Stats().ReplicaWriteFailures > 0 },
		"ReplicaWriteFailures to be counted")
}

func TestAFailedRepairWriteCountsAReadRepairFailureInStats(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].store.Store(storeKey{"", key}, []byte("from-replica"))
	// The repair write back to the primary fails; setWrongNodeLeft only
	// affects S, so the G probes leading up to it are unaffected.
	nodes[owners[0]].setWrongNodeLeft.Add(1)

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ReadRepair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value, ok, err := client.GetBytes(key)
	if err != nil || !ok || string(value) != "from-replica" {
		t.Fatalf("GetBytes = %q, %v, %v", value, ok, err)
	}
	waitFor(t, func() bool { return client.Stats().ReadRepairFailures > 0 },
		"ReadRepairFailures to be counted")
}

func TestARefreshAgainstAnUnreachableDiscoverySeedCountsARefreshFailureInStats(t *testing.T) {
	_, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_ = discovery.listener.Close() // the only configured address is now unreachable
	client.maybeRefresh(true)

	if got := client.Stats().RefreshFailures; got == 0 {
		t.Fatalf("RefreshFailures = %d, want > 0", got)
	}
}

// ── Hedged reads (issue #64) ─────────────────────────────────────────
//
// ReadHedgeAfter sends a read to the next owner when the primary hasn't
// answered in time — a slow node no longer bounds every read that
// touches it. Generous timing tolerances (well over 30ms) keep these
// tests from flaking under CI's ubuntu runners.

const hedgeTimingTolerance = 30 * time.Millisecond

func TestHedgedReadRejectsANegativeHedge(t *testing.T) {
	for _, bad := range []time.Duration{-1, -time.Second} {
		if _, err := Connect(Config{Addresses: []Address{addr(unusedPort(t))}, ReadHedgeAfter: bad}); err == nil {
			t.Fatalf("ReadHedgeAfter=%v: expected an error", bad)
		}
	}
}

func TestHedgedReadZeroMeansOff(t *testing.T) {
	// Unlike Python's None-sentinel, Go's zero value can't distinguish
	// "unset" from "explicitly zero" — so, per Config.ReadHedgeAfter's own
	// doc comment, zero must mean "off" rather than being rejected like a
	// negative value.
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}, ReadHedgeAfter: 0})
	if err != nil {
		t.Fatalf("ReadHedgeAfter=0: expected no error, got %v", err)
	}
	client.Close()
}

func TestAHitFromTheReplicaWinsOverASlowPrimary(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{
		Addresses:      []Address{addr(discovery.address())},
		ReadHedgeAfter: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	owners := ownersOf("k")
	primary, replica := owners[0], owners[1]
	nodes[primary].delayGets(400 * time.Millisecond)

	start := time.Now()
	value, ok, err := client.Get("k")
	elapsed := time.Since(start)

	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if elapsed >= 400*time.Millisecond-hedgeTimingTolerance {
		t.Fatalf("elapsed = %v, want well under the primary's 400ms delay", elapsed)
	}
	if elapsed < 50*time.Millisecond-hedgeTimingTolerance {
		t.Fatalf("elapsed = %v, want at least the 50ms hedge interval", elapsed)
	}
	if got := nodes[replica].getCount.Load(); got != 1 {
		t.Fatalf("replica getCount = %d, want 1 (the replica should have been hedged to)", got)
	}

	client.Close() // idempotent-looking, but proves close() drains the still-running slow leg
	if got := nodes[primary].getCount.Load(); got != 1 {
		t.Fatalf("primary getCount = %d, want 1", got)
	}
}

func TestAFastPrimaryIsNeverHedged(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{
		Addresses:      []Address{addr(discovery.address())},
		ReadHedgeAfter: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	replica := ownersOf("k")[1]
	for i := 0; i < 5; i++ {
		value, ok, err := client.Get("k")
		if err != nil || !ok || value != "v" {
			t.Fatalf("Get = %q, %v, %v", value, ok, err)
		}
	}
	if got := nodes[replica].getCount.Load(); got != 0 {
		t.Fatalf("replica getCount = %d, want 0 (a fast primary must never be hedged)", got)
	}
}

func TestAReplicaMissWaitsForThePrimary(t *testing.T) {
	// Hedging must never turn a hit into a miss: the replica lacks the
	// copy and answers first, but the primary's answer is what counts.
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{
		Addresses:      []Address{addr(discovery.address())},
		ReadHedgeAfter: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	owners := ownersOf("k")
	primary, replica := owners[0], owners[1]
	nodes[replica].store.Delete(storeKey{"", "k"})
	nodes[primary].delayGets(200 * time.Millisecond)

	start := time.Now()
	value, ok, err := client.Get("k")
	elapsed := time.Since(start)

	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if elapsed < 200*time.Millisecond-hedgeTimingTolerance {
		t.Fatalf("elapsed = %v, want at least the primary's 200ms delay", elapsed)
	}
	if got := nodes[replica].getCount.Load(); got != 1 {
		t.Fatalf("replica getCount = %d, want 1", got)
	}

	// A key nobody has: the miss is accepted once the primary has
	// answered it too.
	_, ok, err = client.Get("absent")
	if err != nil || ok {
		t.Fatalf("Get(absent) = ok=%v err=%v, want a clean miss", ok, err)
	}
}

func TestHedgingOffByDefaultASlowPrimaryBoundsTheRead(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	owners := ownersOf("k")
	primary, replica := owners[0], owners[1]
	nodes[primary].delayGets(200 * time.Millisecond)

	start := time.Now()
	value, ok, err := client.Get("k")
	elapsed := time.Since(start)

	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if elapsed < 200*time.Millisecond-hedgeTimingTolerance {
		t.Fatalf("elapsed = %v, want at least the primary's 200ms delay (hedging is off)", elapsed)
	}
	if got := nodes[replica].getCount.Load(); got != 0 {
		t.Fatalf("replica getCount = %d, want 0 (hedging is off by default)", got)
	}
}

func TestADeadPrimaryFailsOverImmediatelyWhenHedgingIsOn(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{
		Addresses:      []Address{addr(discovery.address())},
		ReadHedgeAfter: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	primary := ownersOf("k")[0]
	nodes[primary].close()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	value, ok, err := client.Get("k")
	elapsed := time.Since(start)

	if err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if elapsed >= 500*time.Millisecond-hedgeTimingTolerance {
		t.Fatalf("elapsed = %v, want well under the 500ms hedge interval (a dead primary fails over immediately)", elapsed)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return condition()
}

// ── namespaces (issue #105) ──────────────────────────────────────────

// ── encoder tests: appendGetFrame/appendSetFrame/appendDeleteFrame ────

func TestAppendGetFrameEmitsTheLegacyFormForTheDefaultNamespace(t *testing.T) {
	// The SDK rule: the default (empty) namespace must keep sending the
	// legacy G frame byte-for-byte, both nil and "" alike, so an
	// unmodified server keeps working.
	for _, ns := range [][]byte{nil, []byte("")} {
		if got, want := string(appendGetFrame(ns, []byte("key"), false, 0)), "G 3\nkey"; got != want {
			t.Fatalf("appendGetFrame(%v, ...) = %q, want %q", ns, got, want)
		}
	}
}

func TestAppendGetFrameEmitsTheLowercaseFormForANonEmptyNamespace(t *testing.T) {
	if got, want := string(appendGetFrame([]byte("ns"), []byte("key"), false, 0)), "g 2 3\nnskey"; got != want {
		t.Fatalf("appendGetFrame = %q, want %q", got, want)
	}
}

func TestAppendGetFrameCarriesTheTagFieldLast(t *testing.T) {
	if got, want := string(appendGetFrame([]byte("ns"), []byte("key"), true, 7)), "g 2 3 7\nnskey"; got != want {
		t.Fatalf("appendGetFrame (tagged) = %q, want %q", got, want)
	}
	// Legacy form stays tagged the same way G always has.
	if got, want := string(appendGetFrame(nil, []byte("key"), true, 7)), "G 3 7\nkey"; got != want {
		t.Fatalf("appendGetFrame (legacy, tagged) = %q, want %q", got, want)
	}
}

func TestAppendSetFrameLegacyAndNamespacedForms(t *testing.T) {
	if got, want := string(appendSetFrame(nil, []byte("k"), []byte("v"), -1, false, 0)), "S 1 1\nkv"; got != want {
		t.Fatalf("legacy S, no ttl = %q, want %q", got, want)
	}
	if got, want := string(appendSetFrame(nil, []byte("k"), []byte("v"), 60, false, 0)), "S 1 1 60\nkv"; got != want {
		t.Fatalf("legacy S, ttl = %q, want %q", got, want)
	}
	if got, want := string(appendSetFrame([]byte("ns"), []byte("k"), []byte("v"), -1, false, 0)),
		"s 2 1 1\nnskv"; got != want {
		t.Fatalf("namespaced s, no ttl = %q, want %q", got, want)
	}
	if got, want := string(appendSetFrame([]byte("ns"), []byte("k"), []byte("v"), -1, true, 9)),
		"s 2 1 1 9\nnskv"; got != want {
		t.Fatalf("namespaced s, tag only = %q, want %q", got, want)
	}
	// The ttl+tag `s` form from the issue #105 spec: TTL, then tag, both
	// trailing the three length fields — `s <ns-len> <key-len> <val-len>
	// [<ttl>] [<tag>]`.
	if got, want := string(appendSetFrame([]byte("ns"), []byte("k"), []byte("v"), 60, true, 9)),
		"s 2 1 1 60 9\nnskv"; got != want {
		t.Fatalf("namespaced s, ttl+tag = %q, want %q", got, want)
	}
}

func TestAppendDeleteFrameLegacyAndNamespacedForms(t *testing.T) {
	if got, want := string(appendDeleteFrame(nil, []byte("k"), false, 0)), "D 1\nk"; got != want {
		t.Fatalf("legacy D = %q, want %q", got, want)
	}
	if got, want := string(appendDeleteFrame([]byte("ns"), []byte("k"), true, 3)), "d 2 1 3\nnsk"; got != want {
		t.Fatalf("namespaced d = %q, want %q", got, want)
	}
}

// TestAppendFramesAcceptBinaryNamespaces exercises a namespace that
// isn't valid UTF-8 (issue #105: a namespace is a flat, opaque byte
// string — no delimiter, no escaping, may contain any bytes).
func TestAppendFramesAcceptBinaryNamespaces(t *testing.T) {
	ns := []byte{0xff, 0x00}
	key := []byte("beta")

	got := appendGetFrame(ns, key, false, 0)
	want := append([]byte("g 2 4\n"), append(append([]byte{}, ns...), key...)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("appendGetFrame with binary namespace = %v, want %v", got, want)
	}

	gotD := appendDeleteFrame(ns, key, false, 0)
	wantD := append([]byte("d 2 4\n"), append(append([]byte{}, ns...), key...)...)
	if !bytes.Equal(gotD, wantD) {
		t.Fatalf("appendDeleteFrame with binary namespace = %v, want %v", gotD, wantD)
	}

	value := []byte("v")
	gotS := appendSetFrame(ns, key, value, -1, false, 0)
	wantS := append([]byte("s 2 4 1\n"), append(append(append([]byte{}, ns...), key...), value...)...)
	if !bytes.Equal(gotS, wantS) {
		t.Fatalf("appendSetFrame with binary namespace = %v, want %v", gotS, wantS)
	}
}

// ── clear/flush frame encoders (issue #106) ──────────────────────────

func TestAppendClearFrameUntaggedAndTagged(t *testing.T) {
	if got, want := string(appendClearFrame([]byte("ns"), false, 0)), "c 2\nns"; got != want {
		t.Fatalf("appendClearFrame = %q, want %q", got, want)
	}
	if got, want := string(appendClearFrame([]byte("ns"), true, 7)), "c 2 7\nns"; got != want {
		t.Fatalf("appendClearFrame (tagged) = %q, want %q", got, want)
	}
}

// TestAppendClearFrameEmptyNamespaceClearsTheDefaultNamespace covers both
// nil and "" the way appendGetFrame's legacy-form test does: the default
// namespace is `c 0\n`, never rejected.
func TestAppendClearFrameEmptyNamespaceClearsTheDefaultNamespace(t *testing.T) {
	for _, ns := range [][]byte{nil, []byte("")} {
		if got, want := string(appendClearFrame(ns, false, 0)), "c 0\n"; got != want {
			t.Fatalf("appendClearFrame(%v, ...) = %q, want %q", ns, got, want)
		}
	}
}

func TestAppendClearAllFrameUntaggedAndTagged(t *testing.T) {
	if got, want := string(appendClearAllFrame(false, 0)), "F\n"; got != want {
		t.Fatalf("appendClearAllFrame = %q, want %q", got, want)
	}
	if got, want := string(appendClearAllFrame(true, 3)), "F 3\n"; got != want {
		t.Fatalf("appendClearAllFrame (tagged) = %q, want %q", got, want)
	}
}

// TestAppendIncrFrameDefaultAndNamespacedUntaggedAndTagged covers
// appendIncrFrame's exact wire bytes (issue #129): always namespaced, even
// for the default namespace (ns-len 0), unlike appendGetFrame/
// appendSetFrame/appendDeleteFrame's legacy-vs-namespaced split — there is
// no separate uppercase form to also test.
func TestAppendIncrFrameDefaultAndNamespacedUntaggedAndTagged(t *testing.T) {
	for _, ns := range [][]byte{nil, []byte("")} {
		if got, want := string(appendIncrFrame(ns, []byte("k"), 5, false, 0)), "i 0 1 5\nk"; got != want {
			t.Fatalf("appendIncrFrame(%v, ...) = %q, want %q", ns, got, want)
		}
	}
	if got, want := string(appendIncrFrame([]byte("ns"), []byte("k"), 5, false, 0)), "i 2 1 5\nnsk"; got != want {
		t.Fatalf("appendIncrFrame (namespaced) = %q, want %q", got, want)
	}
	if got, want := string(appendIncrFrame([]byte("ns"), []byte("k"), -5, false, 0)), "i 2 1 -5\nnsk"; got != want {
		t.Fatalf("appendIncrFrame (negative delta) = %q, want %q", got, want)
	}
	if got, want := string(appendIncrFrame([]byte("ns"), []byte("k"), 5, true, 9)), "i 2 1 5 9\nnsk"; got != want {
		t.Fatalf("appendIncrFrame (tagged) = %q, want %q", got, want)
	}
}

// ── batched get/set frame encoders (issues #128/#150/#151) ───────────

// TestAppendMultiGetFrameIsAlwaysNamespacedUntaggedAndTagged covers
// appendMultiGetFrame's exact wire bytes: always namespaced, even for
// the default namespace (ns-len 0), same class as appendIncrFrame —
// there is no separate uppercase form to also test.
func TestAppendMultiGetFrameIsAlwaysNamespacedUntaggedAndTagged(t *testing.T) {
	for _, ns := range [][]byte{nil, []byte("")} {
		got := string(appendMultiGetFrame(ns, [][]byte{[]byte("a"), []byte("bb")}, false, 0))
		if want := "m 0 2 1 2\nabb"; got != want {
			t.Fatalf("appendMultiGetFrame(%v, ...) = %q, want %q", ns, got, want)
		}
	}
	got := string(appendMultiGetFrame([]byte("ns"), [][]byte{[]byte("a"), []byte("bb")}, false, 0))
	if want := "m 2 2 1 2\nnsabb"; got != want {
		t.Fatalf("appendMultiGetFrame (namespaced) = %q, want %q", got, want)
	}
	got = string(appendMultiGetFrame([]byte("ns"), [][]byte{[]byte("a")}, true, 9))
	if want := "m 2 1 1 9\nnsa"; got != want {
		t.Fatalf("appendMultiGetFrame (tagged) = %q, want %q", got, want)
	}
}

// TestAppendMultiSetFrameWithAndWithoutTTLUntaggedAndTagged covers
// appendMultiSetFrame's exact wire bytes, including the [ttl] field's
// position ahead of the tag — the same convention appendSetFrame's own
// [ttl] uses.
func TestAppendMultiSetFrameWithAndWithoutTTLUntaggedAndTagged(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("bb")}
	values := [][]byte{[]byte("x"), []byte("yy")}

	got := string(appendMultiSetFrame(nil, keys, values, -1, false, 0))
	if want := "o 0 2 1 1 2 2\naxbbyy"; got != want {
		t.Fatalf("appendMultiSetFrame (no ttl) = %q, want %q", got, want)
	}

	got = string(appendMultiSetFrame([]byte("ns"), keys, values, 60, false, 0))
	if want := "o 2 2 1 1 2 2 60\nnsaxbbyy"; got != want {
		t.Fatalf("appendMultiSetFrame (with ttl) = %q, want %q", got, want)
	}

	got = string(appendMultiSetFrame([]byte("ns"), keys, values, -1, true, 9))
	if want := "o 2 2 1 1 2 2 9\nnsaxbbyy"; got != want {
		t.Fatalf("appendMultiSetFrame (tagged, no ttl) = %q, want %q", got, want)
	}

	got = string(appendMultiSetFrame([]byte("ns"), keys, values, 60, true, 9))
	if want := "o 2 2 1 1 2 2 60 9\nnsaxbbyy"; got != want {
		t.Fatalf("appendMultiSetFrame (tagged, with ttl) = %q, want %q", got, want)
	}
}

// TestAppendMultiFramesAcceptBinaryNamespaces mirrors
// TestAppendFramesAcceptBinaryNamespaces for the batched encoders.
func TestAppendMultiFramesAcceptBinaryNamespaces(t *testing.T) {
	ns := []byte{0xff, 0x00}
	keys := [][]byte{[]byte("a")}

	gotM := appendMultiGetFrame(ns, keys, false, 0)
	wantM := append([]byte("m 2 1 1\n"), append(append([]byte{}, ns...), keys[0]...)...)
	if !bytes.Equal(gotM, wantM) {
		t.Fatalf("appendMultiGetFrame with binary namespace = %v, want %v", gotM, wantM)
	}

	values := [][]byte{[]byte("v")}
	gotO := appendMultiSetFrame(ns, keys, values, -1, false, 0)
	wantO := append([]byte("o 2 1 1 1\n"), append(append(append([]byte{}, ns...), keys[0]...), values[0]...)...)
	if !bytes.Equal(gotO, wantO) {
		t.Fatalf("appendMultiSetFrame with binary namespace = %v, want %v", gotO, wantO)
	}
}

// ── clear/flush client round trips, single node (issue #106) ─────────

// TestNamespaceClearRoundTrip covers the issue #106 spec's namespaced
// clear scenario directly: set in two namespaces plus the default one,
// clear a single namespace, and confirm only that namespace emptied.
func TestNamespaceClearRoundTrip(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	users := client.Namespace("users")
	orders := client.Namespace("orders")
	if err := client.Set("k", "default", 0); err != nil {
		t.Fatal(err)
	}
	if err := users.Set("k", "in-users", 0); err != nil {
		t.Fatal(err)
	}
	if err := orders.Set("k", "in-orders", 0); err != nil {
		t.Fatal(err)
	}

	if err := users.Clear(); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := users.Get("k"); err != nil || ok {
		t.Fatalf("users.Get after Clear: ok=%v err=%v", ok, err)
	}
	if value, ok, err := orders.Get("k"); err != nil || !ok || value != "in-orders" {
		t.Fatalf("orders.Get after users.Clear = %q, %v, %v", value, ok, err)
	}
	if value, ok, err := client.Get("k"); err != nil || !ok || value != "default" {
		t.Fatalf("default Get after users.Clear = %q, %v, %v", value, ok, err)
	}
}

// TestNamespaceClearOnTheEmptyStringHandleClearsTheDefaultNamespace
// covers the spec's "clear() on namespace(\"\") clears the default
// namespace, never rejected" rule.
func TestNamespaceClearOnTheEmptyStringHandleClearsTheDefaultNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.Namespace("").Clear(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := client.Get("k"); err != nil || ok {
		t.Fatalf("Get after namespace(\"\").Clear: ok=%v err=%v", ok, err)
	}
}

// TestClearAllEmptiesEveryNamespace covers the spec's "clearAll() empties
// everything, default included" scenario.
func TestClearAllEmptiesEveryNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.Namespace("users").Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.Namespace("orders").Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if got := node.storeLen(); got != 3 {
		t.Fatalf("storeLen before ClearAll = %d, want 3", got)
	}

	if err := client.ClearAll(); err != nil {
		t.Fatal(err)
	}
	if got := node.storeLen(); got != 0 {
		t.Fatalf("storeLen after ClearAll = %d, want 0", got)
	}
}

func TestClearMethodsErrorAfterClientClose(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	ns := client.Namespace("users")
	client.Close()

	if err := ns.Clear(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Clear err = %v, want ErrClosed", err)
	}
	if err := client.ClearAll(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ClearAll err = %v, want ErrClosed", err)
	}
}

// ── clear/flush cluster fan-out and failure handling (issue #106) ────

// TestClearFansOutToEveryNode proves a namespaced Clear reaches every
// member node, not just one owner — clear isn't key-addressed the way
// Get/Set/Delete are (docs/protocol.html's "c / F").
func TestClearFansOutToEveryNode(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Namespace("users").Clear(); err != nil {
		t.Fatal(err)
	}
	for name, node := range nodes {
		if node.clearCountReceived() != 1 {
			t.Errorf("%s received %d clear requests, want 1", name, node.clearCountReceived())
		}
	}
}

// TestClearAllFansOutToEveryNode is TestClearFansOutToEveryNode for
// ClearAll's `F`.
func TestClearAllFansOutToEveryNode(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.ClearAll(); err != nil {
		t.Fatal(err)
	}
	for name, node := range nodes {
		if node.clearCountReceived() != 1 {
			t.Errorf("%s received %d clear requests, want 1", name, node.clearCountReceived())
		}
	}
}

// TestAClearThatFailsTwiceIsRetriedAfterARefreshAndSucceeds: a node
// fails the first two attempts — the request itself, and
// applyReconnecting's own one-shot redial-retry of it — so the failure
// surfaces to clearFanout's own refresh-and-retry loop; the node
// eventually acks on the very next attempt, in the refreshed list's
// round, and the overall Clear succeeds.
func TestAClearThatFailsTwiceIsRetriedAfterARefreshAndSucceeds(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var flaky *mockNode
	for _, node := range nodes {
		flaky = node
		break
	}
	flaky.failClearTimes(2)

	if err := client.Namespace("users").Clear(); err != nil {
		t.Fatalf("Clear = %v, want success after refresh-and-retry", err)
	}
	if got := client.Stats().RefreshFailures; got != 0 {
		t.Fatalf("RefreshFailures = %d, want 0 (discovery stayed reachable)", got)
	}
}

// TestAPersistentlyFailingNodeFailsClearNamingIt: the failing node never
// recovers, so both rounds fail on it and Clear raises an error naming
// it, never silently succeeding on a partial clear.
func TestAPersistentlyFailingNodeFailsClearNamingIt(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var failingName string
	var failing *mockNode
	for name, node := range nodes {
		failingName, failing = name, node
		break
	}
	failing.failClearTimes(1000)

	err = client.ClearAll()
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("ClearAll err = %v, want ErrConnectionLost", err)
	}
	if !strings.Contains(err.Error(), failingName) {
		t.Fatalf("ClearAll err = %v, want it to name %s", err, failingName)
	}
}

// ── single-node round trip and isolation ──────────────────────────────

func TestNamespaceRoundTripsAndIsolatesFromTheDefaultNamespaceAndOtherNamespaces(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	users := client.Namespace("users")
	orders := client.Namespace("orders")
	if got := users.Name(); got != "users" {
		t.Fatalf("Name() = %q, want %q", got, "users")
	}
	if got := orders.Name(); got != "orders" {
		t.Fatalf("Name() = %q, want %q", got, "orders")
	}

	// Same key name in the default namespace and two others: three
	// independent entries (issue #105 spec item 4).
	if err := client.Set("shared-key", "default", 0); err != nil {
		t.Fatal(err)
	}
	if err := users.Set("shared-key", "in-users", 0); err != nil {
		t.Fatal(err)
	}
	if err := orders.Set("shared-key", "in-orders", 0); err != nil {
		t.Fatal(err)
	}

	if value, ok, err := client.Get("shared-key"); err != nil || !ok || value != "default" {
		t.Fatalf("default Get = %q, %v, %v", value, ok, err)
	}
	if value, ok, err := users.Get("shared-key"); err != nil || !ok || value != "in-users" {
		t.Fatalf("users Get = %q, %v, %v", value, ok, err)
	}
	if value, ok, err := orders.Get("shared-key"); err != nil || !ok || value != "in-orders" {
		t.Fatalf("orders Get = %q, %v, %v", value, ok, err)
	}
	if got := node.storeLen(); got != 3 {
		t.Fatalf("storeLen = %d, want 3 (isolation)", got)
	}

	if raw, ok, err := users.GetBytes("shared-key"); err != nil || !ok || string(raw) != "in-users" {
		t.Fatalf("users GetBytes = %q, %v, %v", raw, ok, err)
	}
	if err := users.SetBytes("shared-key", []byte{0x00, 0xff}, 0); err != nil {
		t.Fatal(err)
	}
	if raw, ok, err := users.GetBytes("shared-key"); err != nil || !ok || !bytes.Equal(raw, []byte{0x00, 0xff}) {
		t.Fatalf("users GetBytes after SetBytes = %v, %v, %v", raw, ok, err)
	}

	if existed, err := users.Delete("shared-key"); err != nil || !existed {
		t.Fatalf("users Delete = %v, %v", existed, err)
	}
	if _, ok, err := users.Get("shared-key"); err != nil || ok {
		t.Fatalf("users Get after delete: ok=%v err=%v", ok, err)
	}
	// Deleting from one namespace must not touch the others.
	if value, ok, err := client.Get("shared-key"); err != nil || !ok || value != "default" {
		t.Fatalf("default Get after users delete = %q, %v, %v", value, ok, err)
	}
	if value, ok, err := orders.Get("shared-key"); err != nil || !ok || value != "in-orders" {
		t.Fatalf("orders Get after users delete = %q, %v, %v", value, ok, err)
	}
}

func TestNamespaceEmptyStringUsesLegacyFramesAndTheDefaultNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	root := client.Namespace("")
	if got := root.Name(); got != "" {
		t.Fatalf("Name() = %q, want \"\"", got)
	}
	if err := root.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	// namespace("") must be indistinguishable on the wire from the
	// client's own namespace-less methods (issue #105's SDK rule): the
	// mock node stores it as the unnamespaced entry, so the plain client
	// reads it straight back.
	if value, ok, err := client.Get("k"); err != nil || !ok || value != "v" {
		t.Fatalf("client.Get after root.Set = %q, %v, %v", value, ok, err)
	}
	if !node.hasKey("k") {
		t.Fatal("expected the legacy (unnamespaced) store entry")
	}

	if err := client.Set("k2", "v2", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := root.Get("k2"); err != nil || !ok || value != "v2" {
		t.Fatalf("root.Get after client.Set = %q, %v, %v", value, ok, err)
	}
}

func TestNamespaceMethodsErrorAfterClientClose(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	ns := client.Namespace("users")
	client.Close()

	if _, _, err := ns.Get("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get err = %v, want ErrClosed", err)
	}
	if _, _, err := ns.GetBytes("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("GetBytes err = %v, want ErrClosed", err)
	}
	if err := ns.Set("k", "v", 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set err = %v, want ErrClosed", err)
	}
	if _, err := ns.Delete("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete err = %v, want ErrClosed", err)
	}
}

// ── cluster mode: routing and W refresh-and-retry by (ns, key) ────────

func TestNamespacedWrongNodeTriggersRefreshAndOneRetry(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ns := client.Namespace("users")
	if err := ns.Set("some-key", "v", 0); err != nil {
		t.Fatal(err)
	}
	primary, err := NewHashRing(testNames).RouteNS([]byte("users"), []byte("some-key"))
	if err != nil {
		t.Fatal(err)
	}
	owner := nodes[primary]

	owner.wrongNodeLeft.Add(1)
	if value, ok, err := ns.Get("some-key"); err != nil || !ok || value != "v" {
		t.Fatalf("Get after one W = %q, %v, %v", value, ok, err)
	}

	owner.wrongNodeLeft.Add(2)
	if _, _, err := ns.Get("some-key"); !errors.Is(err, ErrWrongNode) {
		t.Fatalf("err = %v", err)
	}
}

// TestNamespacedRoutingCanDifferFromTheDefaultNamespace proves the
// namespace actually enters routing (issue #105) rather than every
// namespace simply reusing the default keyspace's placement: for a fixed
// key, at least one of a handful of namespaces routes to a different
// primary than the default namespace does.
func TestNamespacedRoutingCanDifferFromTheDefaultNamespace(t *testing.T) {
	ring := NewHashRing([]string{"node-a", "node-b", "node-c"})
	defaultPrimary, err := ring.Route([]byte("some-key"))
	if err != nil {
		t.Fatal(err)
	}
	differed := false
	for _, ns := range []string{"users", "orders", "sessions", "carts", "invoices"} {
		primary, err := ring.RouteNS([]byte(ns), []byte("some-key"))
		if err != nil {
			t.Fatal(err)
		}
		if primary != defaultPrimary {
			differed = true
			break
		}
	}
	if !differed {
		t.Fatal("every namespace routed the same key to the same primary as the default namespace")
	}
}

// ── Incr/Decr (issue #129) ─────────────────────────────────────────

func TestIncrRoundTripsAndReturnsTheNewValue(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("counter", "10", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Incr("counter", 5); err != nil || !ok || value != 15 {
		t.Fatalf("Incr(+5) = %d, %v, %v, want 15, true, nil", value, ok, err)
	}
	if value, ok, err := client.Incr("counter", -3); err != nil || !ok || value != 12 {
		t.Fatalf("Incr(-3) = %d, %v, %v, want 12, true, nil", value, ok, err)
	}
	if got, _ := node.storedValue("counter"); got != "12" {
		t.Fatalf("node's stored value = %q, want \"12\"", got)
	}
}

// TestIncrRoundTripsOverATaggedConnection covers the `I` response's
// tagged-mode decode (issue #47's echoed response tags plus issue #129's
// INCR) alongside the untagged path TestIncrRoundTripsAndReturnsTheNewValue
// already covers.
func TestIncrRoundTripsOverATaggedConnection(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("counter", "100", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Incr("counter", 23); err != nil || !ok || value != 123 {
		t.Fatalf("Incr = %d, %v, %v, want 123, true, nil", value, ok, err)
	}
}

func TestIncrOnAMissingKeyReturnsNotFound(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value, ok, err := client.Incr("never-set", 1)
	if err != nil || ok || value != 0 {
		t.Fatalf("Incr on a missing key = %d, %v, %v, want 0, false, nil", value, ok, err)
	}
}

func TestIncrOnANonNumericStoredValueReturnsErrNotNumeric(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("not-a-number", "hello", 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := client.Incr("not-a-number", 1); ok || !errors.Is(err, ErrNotNumeric) {
		t.Fatalf("Incr on a non-numeric value: ok=%v err=%v, want ok=false err=ErrNotNumeric", ok, err)
	}
}

// TestDecrSendsTheNegatedDelta confirms Decr is a thin wrapper: it must
// never send a separate wire opcode, only Incr with delta negated (issue
// #129's spec).
func TestDecrSendsTheNegatedDelta(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("counter", "20", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Decr("counter", 5); err != nil || !ok || value != 15 {
		t.Fatalf("Decr(5) = %d, %v, %v, want 15, true, nil", value, ok, err)
	}
	// Decr must send the same `i` opcode as Incr, just with delta negated
	// — never a separate wire command.
	if node.iRequestsReceived() != 1 {
		t.Fatalf("iRequestsReceived = %d, want 1 (Decr must reuse `i`, not a separate opcode)", node.iRequestsReceived())
	}
}

// TestDecrRejectsMinInt64Delta covers issue #182: math.MinInt64 has no
// valid int64 negation (two's complement wraps it back to itself), so a
// naive `-delta` would silently turn Decr(math.MinInt64) into an Incr by
// +2^63 instead of failing. Decr must reject it client-side, before any
// `i` frame is sent.
func TestDecrRejectsMinInt64Delta(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("counter", "20", 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := client.Decr("counter", math.MinInt64); ok || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Decr(math.MinInt64) = ok=%v err=%v, want ok=false err=ErrInvalidArgument", ok, err)
	}
	if node.iRequestsReceived() != 0 {
		t.Fatalf("iRequestsReceived = %d, want 0 (math.MinInt64 must be rejected before any wire I/O)", node.iRequestsReceived())
	}

	ns := client.Namespace("counters")
	if err := ns.Set("hits", "20", 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ns.Decr("hits", math.MinInt64); ok || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Namespace.Decr(math.MinInt64) = ok=%v err=%v, want ok=false err=ErrInvalidArgument", ok, err)
	}
}

func TestNamespaceIncrAndDecrScopeToTheirNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ns := client.Namespace("counters")
	if err := ns.Set("hits", "1", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := ns.Incr("hits", 1); err != nil || !ok || value != 2 {
		t.Fatalf("Namespace.Incr = %d, %v, %v, want 2, true, nil", value, ok, err)
	}
	if value, ok, err := ns.Decr("hits", 1); err != nil || !ok || value != 1 {
		t.Fatalf("Namespace.Decr = %d, %v, %v, want 1, true, nil", value, ok, err)
	}
	// The default namespace never saw this key at all — Incr/Decr, like
	// Get/Set/Delete, must stay scoped to the namespace handle they were
	// called through.
	if node.hasKey("hits") {
		t.Fatal("Namespace.Incr/Decr leaked into the default namespace")
	}
	if !node.hasNSKey("counters", "hits") {
		t.Fatal("Namespace.Incr/Decr did not write into its own namespace")
	}
}

// TestClusterIncrRunsOnlyOnThePrimaryAndReplicatesTheLiteralResult is the
// single most important test for issue #129's replication rule: a
// successful Incr must run `i` against the primary owner ONLY, then fan
// the primary's literal resulting value (and TTL) out to the remaining
// owners as an ordinary Set — never replay `i` on a replica (see
// (*Client).incr's own doc comment for why: replaying would let a replica
// drift from the primary instead of staying byte-identical to it).
//
// Both nodes start from the same seeded value (10, with a TTL) precisely
// so that a buggy implementation which mistakenly replays `i` on the
// replica would still land on the same final stored value (15) as a
// correct one — comparing only final values would not catch that bug.
// The frame-count assertions below are the actual proof: the replica must
// have received zero `i` frames, and its TTL must match what the
// primary's `I` response reported (proving the replica's new value
// arrived via a Set that carried that TTL field, not independently).
func TestClusterIncrRunsOnlyOnThePrimaryAndReplicatesTheLiteralResult(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const key = "shared-counter"
	owners := ownersOf(key)
	primary, replica := nodes[owners[0]], nodes[owners[1]]

	// Seed both owners, via the ordinary replicated Set path, with a TTL —
	// so the mock's per-key ttls state (see storedTTL) starts identical on
	// both, and only a real replicate-the-result Set proves it stays that
	// way through the Incr below.
	if err := client.Set(key, "10", 60); err != nil {
		t.Fatal(err)
	}

	value, ok, err := client.Incr(key, 5)
	if err != nil || !ok || value != 15 {
		t.Fatalf("Incr = %d, %v, %v, want 15, true, nil", value, ok, err)
	}

	if got := primary.iRequestsReceived(); got != 1 {
		t.Fatalf("primary received %d `i` frames, want exactly 1", got)
	}
	if got := replica.iRequestsReceived(); got != 0 {
		t.Fatalf("replica received %d `i` frames, want exactly 0 (it must never see `i`)", got)
	}

	if got, ok := replica.storedValue(key); !ok || got != "15" {
		t.Fatalf("replica's stored value = %q, %v, want \"15\", true (must equal the primary's literal result)", got, ok)
	}
	if got, ok := primary.storedValue(key); !ok || got != "15" {
		t.Fatalf("primary's stored value = %q, %v, want \"15\", true", got, ok)
	}

	// TTL round-trip: the replica's Set-leg must have carried the same TTL
	// the primary's own `I` response reported.
	if got, ok := replica.storedTTL(key); !ok || got != 60 {
		t.Fatalf("replica's recorded TTL = %d, %v, want 60, true", got, ok)
	}
}

// ── Compare-and-set (issue #141) ─────────────────────────────────────

// TestAppendCasFrameAllConditionFormsUntaggedAndTagged covers appendCasFrame's
// exact wire bytes: always namespaced, even the default namespace (like
// appendIncrFrame), all three <cond> shapes (A, P, a digest), with and
// without a TTL, untagged and tagged.
func TestAppendCasFrameAllConditionFormsUntaggedAndTagged(t *testing.T) {
	if got, want := string(appendCasFrame(nil, []byte("k"), []byte("v"), casCondAbsent, -1, false, 0)),
		"k 0 1 1 A\nkv"; got != want {
		t.Fatalf("appendCasFrame (absent, no ttl) = %q, want %q", got, want)
	}
	if got, want := string(appendCasFrame([]byte("ns"), []byte("k"), []byte("v"), casCondPresent, -1, false, 0)),
		"k 2 1 1 P\nnskv"; got != want {
		t.Fatalf("appendCasFrame (present, namespaced) = %q, want %q", got, want)
	}
	digest := "36287141940ca57acbd7695ccdde9d43"
	if got, want := string(appendCasFrame([]byte("ns"), []byte("k"), []byte("v"), digest, -1, false, 0)),
		"k 2 1 1 "+digest+"\nnskv"; got != want {
		t.Fatalf("appendCasFrame (digest) = %q, want %q", got, want)
	}
	if got, want := string(appendCasFrame(nil, []byte("k"), []byte("v"), casCondAbsent, 60, false, 0)),
		"k 0 1 1 A 60\nkv"; got != want {
		t.Fatalf("appendCasFrame (ttl) = %q, want %q", got, want)
	}
	if got, want := string(appendCasFrame([]byte("ns"), []byte("k"), []byte("v"), casCondPresent, 60, true, 9)),
		"k 2 1 1 P 60 9\nnskv"; got != want {
		t.Fatalf("appendCasFrame (ttl+tag) = %q, want %q", got, want)
	}
	if got, want := string(appendCasFrame(nil, []byte("k"), []byte("v"), digest, -1, true, 5)),
		"k 0 1 1 "+digest+" 5\nkv"; got != want {
		t.Fatalf("appendCasFrame (digest, tag only) = %q, want %q", got, want)
	}
}

// TestAppendDeleteIfMatchesFrameUntaggedAndTagged covers
// appendDeleteIfMatchesFrame's exact wire bytes: always namespaced, cond
// is always a digest, untagged and tagged.
func TestAppendDeleteIfMatchesFrameUntaggedAndTagged(t *testing.T) {
	digest := "36287141940ca57acbd7695ccdde9d43"
	if got, want := string(appendDeleteIfMatchesFrame(nil, []byte("k"), digest, false, 0)),
		"x 0 1 "+digest+"\nk"; got != want {
		t.Fatalf("appendDeleteIfMatchesFrame = %q, want %q", got, want)
	}
	if got, want := string(appendDeleteIfMatchesFrame([]byte("ns"), []byte("k"), digest, true, 3)),
		"x 2 1 "+digest+" 3\nnsk"; got != want {
		t.Fatalf("appendDeleteIfMatchesFrame (namespaced, tagged) = %q, want %q", got, want)
	}
}

// TestContentDigestMatchesTheCrossLanguagePinnedVector pins ContentDigest
// against the fixed test vector docs/protocol.html#cas specifies — the
// same vector every other nanocached SDK and the Rust server pin, so a
// mismatch here means CAS silently breaks across languages.
func TestContentDigestMatchesTheCrossLanguagePinnedVector(t *testing.T) {
	const wantHex = "36287141940ca57acbd7695ccdde9d43"
	digest := ContentDigest([]byte("nanocached-cas-vector"))
	if got := hex.EncodeToString(digest[:]); got != wantHex {
		t.Fatalf("ContentDigest(%q) = %s, want %s", "nanocached-cas-vector", got, wantHex)
	}
	if got := (CasToken{digest: digest}).Hex(); got != wantHex {
		t.Fatalf("CasToken.Hex() = %s, want %s", got, wantHex)
	}
}

func TestPutIfAbsentStoresOnlyWhenTheKeyIsAbsent(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	stored, err := client.PutIfAbsent("k", []byte("first"), 0)
	if err != nil || !stored {
		t.Fatalf("PutIfAbsent (absent) = %v, %v, want true, nil", stored, err)
	}
	if got, ok := node.storedValue("k"); !ok || got != "first" {
		t.Fatalf("stored value = %q, %v, want \"first\", true", got, ok)
	}

	stored, err = client.PutIfAbsent("k", []byte("second"), 0)
	if err != nil || stored {
		t.Fatalf("PutIfAbsent (present) = %v, %v, want false, nil", stored, err)
	}
	if got, _ := node.storedValue("k"); got != "first" {
		t.Fatalf("stored value changed to %q, want unchanged %q", got, "first")
	}
}

func TestReplaceIfPresentStoresOnlyWhenTheKeyIsPresent(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	stored, err := client.ReplaceIfPresent("k", []byte("v1"), 0)
	if err != nil || stored {
		t.Fatalf("ReplaceIfPresent (absent) = %v, %v, want false, nil", stored, err)
	}

	if err := client.Set("k", "v0", 0); err != nil {
		t.Fatal(err)
	}
	stored, err = client.ReplaceIfPresent("k", []byte("v1"), 0)
	if err != nil || !stored {
		t.Fatalf("ReplaceIfPresent (present) = %v, %v, want true, nil", stored, err)
	}
	if got, _ := node.storedValue("k"); got != "v1" {
		t.Fatalf("stored value = %q, want %q", got, "v1")
	}
}

func TestReplaceStoresOnlyWhenTheDigestMatches(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v0", 0); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := client.GetWithToken("k")
	if err != nil || !ok {
		t.Fatalf("GetWithToken = ok=%v err=%v, want ok", ok, err)
	}

	stored, err := client.Replace("k", token, []byte("v1"), 0)
	if err != nil || !stored {
		t.Fatalf("Replace (matching digest) = %v, %v, want true, nil", stored, err)
	}
	if got, _ := node.storedValue("k"); got != "v1" {
		t.Fatalf("stored value = %q, want %q", got, "v1")
	}

	// token digests "v0"; the key now holds "v1", so this must mismatch.
	stored, err = client.Replace("k", token, []byte("v2"), 0)
	if err != nil || stored {
		t.Fatalf("Replace (stale digest) = %v, %v, want false, nil", stored, err)
	}
	if got, _ := node.storedValue("k"); got != "v1" {
		t.Fatalf("stored value changed to %q, want unchanged %q", got, "v1")
	}
}

func TestReplaceOnAMissingKeyIsAMismatch(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	token := TokenFromDigest(ContentDigest([]byte("whatever")))
	stored, err := client.Replace("missing", token, []byte("v"), 0)
	if err != nil || stored {
		t.Fatalf("Replace on a missing key = %v, %v, want false, nil", stored, err)
	}
}

func TestDeleteIfMatchesRemovesOnlyWhenTheDigestMatches(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v0", 0); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := client.GetWithToken("k")
	if err != nil || !ok {
		t.Fatalf("GetWithToken = ok=%v err=%v, want ok", ok, err)
	}

	wrongToken := TokenFromDigest(ContentDigest([]byte("not-the-stored-value")))
	deleted, err := client.DeleteIfMatches("k", wrongToken)
	if err != nil || deleted {
		t.Fatalf("DeleteIfMatches (mismatch) = %v, %v, want false, nil", deleted, err)
	}
	if !node.hasKey("k") {
		t.Fatal("DeleteIfMatches deleted the key on a digest mismatch")
	}

	deleted, err = client.DeleteIfMatches("k", token)
	if err != nil || !deleted {
		t.Fatalf("DeleteIfMatches (match) = %v, %v, want true, nil", deleted, err)
	}
	if node.hasKey("k") {
		t.Fatal("DeleteIfMatches did not delete the key on a digest match")
	}
}

func TestDeleteIfMatchesOnAMissingKeyIsAMismatch(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	token := TokenFromDigest(ContentDigest([]byte("whatever")))
	deleted, err := client.DeleteIfMatches("missing", token)
	if err != nil || deleted {
		t.Fatalf("DeleteIfMatches on a missing key = %v, %v, want false, nil", deleted, err)
	}
}

func TestNamespaceCasScopesToItsNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ns := client.Namespace("cas-ns")
	stored, err := ns.PutIfAbsent("k", []byte("v0"), 0)
	if err != nil || !stored {
		t.Fatalf("Namespace.PutIfAbsent = %v, %v, want true, nil", stored, err)
	}
	if node.hasKey("k") {
		t.Fatal("Namespace.PutIfAbsent leaked into the default namespace")
	}
	if !node.hasNSKey("cas-ns", "k") {
		t.Fatal("Namespace.PutIfAbsent did not write into its own namespace")
	}

	_, token, ok, err := ns.GetWithToken("k")
	if err != nil || !ok {
		t.Fatalf("Namespace.GetWithToken = ok=%v err=%v, want ok", ok, err)
	}
	stored, err = ns.Replace("k", token, []byte("v1"), 0)
	if err != nil || !stored {
		t.Fatalf("Namespace.Replace = %v, %v, want true, nil", stored, err)
	}

	// token still digests v0; k now holds v1, so this must mismatch.
	deleted, err := ns.DeleteIfMatches("k", token)
	if err != nil || deleted {
		t.Fatalf("Namespace.DeleteIfMatches (stale token) = %v, %v, want false, nil", deleted, err)
	}
}

// TestGetWithTokenDigestIsComputedFromRawWireBytesNotDecompressedValue is
// the critical compression-correctness check (issue #141): with
// Config.Compress enabled, GetWithToken's token must be the digest of the
// raw, marker-prefixed wire bytes the server actually stores — never the
// decompressed value this SDK hands back to the caller, since the server
// itself never decompresses and so could never produce a matching digest
// from decompressed bytes.
func TestGetWithTokenDigestIsComputedFromRawWireBytesNotDecompressedValue(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{
		Addresses:            []Address{addr(node.address())},
		Compress:             true,
		CompressionThreshold: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value := strings.Repeat("y", 500)
	if err := client.Set("k", value, 0); err != nil {
		t.Fatal(err)
	}

	storedAny, ok := node.store.Load(storeKey{"", "k"})
	if !ok {
		t.Fatal("value not stored")
	}
	raw := storedAny.([]byte)
	if raw[0] != compressionMarkerDeflate {
		t.Fatalf("marker = %d, want %d (test assumes this value compresses)", raw[0], compressionMarkerDeflate)
	}

	got, token, ok, err := client.GetWithToken("k")
	if err != nil || !ok || string(got) != value {
		t.Fatalf("GetWithToken = %q, %v, %v, want %q, true, nil", got, ok, err, value)
	}

	wantDigest := ContentDigest(raw)
	if token.Digest() != wantDigest {
		t.Fatalf("token digest = %x, want %x (must be computed from the raw wire bytes, marker included)",
			token.Digest(), wantDigest)
	}
	// Sanity check that this test would actually catch the bug it's aimed
	// at: the digest of the decompressed value must differ from the raw
	// one, or a wrong implementation computing it from the wrong bytes
	// would slip through undetected.
	if wrongDigest := ContentDigest([]byte(value)); token.Digest() == wrongDigest {
		t.Fatal("token digest equals the decompressed value's digest — computed from the wrong bytes")
	}

	// The value Replace writes back must go through the same compression
	// pipeline Set uses, so a later plain Get still decompresses cleanly.
	stored, err := client.Replace("k", token, []byte("a-new-value-thats-long-enough-to-maybe-compress"), 0)
	if err != nil || !stored {
		t.Fatalf("Replace = %v, %v, want true, nil", stored, err)
	}
	got2, ok, err := client.Get("k")
	if err != nil || !ok || got2 != "a-new-value-thats-long-enough-to-maybe-compress" {
		t.Fatalf("Get after Replace = %q, %v, %v", got2, ok, err)
	}
}

// TestClusterCasRunsOnlyOnThePrimaryAndReplicatesTheLiteralResult is the
// CAS equivalent of TestClusterIncrRunsOnlyOnThePrimaryAndReplicatesTheLiteralResult
// (issue #141, following #129's replication rule exactly): a successful
// Replace/DeleteIfMatches must run `k`/`x` against the primary owner
// ONLY, then fan the primary's literal result out to the remaining
// owners as an ordinary Set/Delete — never replay `k`/`x` on a replica (a
// replica evaluating the same condition against its own possibly
// different copy could reach a different outcome than the primary).
func TestClusterCasRunsOnlyOnThePrimaryAndReplicatesTheLiteralResult(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const key = "shared-cas-key"
	owners := ownersOf(key)
	primary, replica := nodes[owners[0]], nodes[owners[1]]

	if err := client.Set(key, "v0", 0); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := client.GetWithToken(key)
	if err != nil || !ok {
		t.Fatalf("GetWithToken = ok=%v err=%v, want ok", ok, err)
	}

	stored, err := client.Replace(key, token, []byte("v1"), 0)
	if err != nil || !stored {
		t.Fatalf("Replace = %v, %v, want true, nil", stored, err)
	}

	if got := primary.kRequestsReceived(); got != 1 {
		t.Fatalf("primary received %d `k` frames, want exactly 1", got)
	}
	if got := replica.kRequestsReceived(); got != 0 {
		t.Fatalf("replica received %d `k` frames, want exactly 0 (it must never see `k`)", got)
	}
	if got, ok := replica.storedValue(key); !ok || got != "v1" {
		t.Fatalf("replica's stored value = %q, %v, want \"v1\", true (must equal the primary's literal result)", got, ok)
	}
	if got, ok := primary.storedValue(key); !ok || got != "v1" {
		t.Fatalf("primary's stored value = %q, %v, want \"v1\", true", got, ok)
	}

	// DeleteIfMatches follows the exact same rule for `x`.
	_, token2, ok, err := client.GetWithToken(key)
	if err != nil || !ok {
		t.Fatalf("GetWithToken (2) = ok=%v err=%v, want ok", ok, err)
	}
	deleted, err := client.DeleteIfMatches(key, token2)
	if err != nil || !deleted {
		t.Fatalf("DeleteIfMatches = %v, %v, want true, nil", deleted, err)
	}
	if got := primary.xRequestsReceived(); got != 1 {
		t.Fatalf("primary received %d `x` frames, want exactly 1", got)
	}
	if got := replica.xRequestsReceived(); got != 0 {
		t.Fatalf("replica received %d `x` frames, want exactly 0 (it must never see `x`)", got)
	}
	if primary.hasKey(key) {
		t.Fatal("primary still has the key after DeleteIfMatches succeeded")
	}
	if replica.hasKey(key) {
		t.Fatal("replica still has the key — the delete result must have been fanned out as an ordinary Delete")
	}
}
