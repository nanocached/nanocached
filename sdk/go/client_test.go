package nanocached

// Integration tests against in-process mock servers speaking just enough
// of the wire protocol — mirrors the other SDKs' mock-based suites.

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
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

type mockNode struct {
	listener         net.Listener
	requiredSecret   []byte
	opts             mockNodeOpts
	store            sync.Map // string -> []byte
	connectionCount  atomic.Int32
	getCount         atomic.Int32
	wrongNodeLeft    atomic.Int32
	setWrongNodeLeft atomic.Int32 // like wrongNodeLeft, but only consumed by S (for isolating a repair write's failure from an unrelated G)
	malformedLeft    atomic.Int32
	storedToGetLeft  atomic.Int32
	wrongTagLeft     atomic.Int32 // doc/adr/0019-*.md: echo the wrong tag on the next G on a tagged connection
	swallowLeft      atomic.Int32 // doc/adr/0019-*.md: swallow the next G entirely (no reply)
	lastSetTTL       atomic.Value // string: the TTL field of the last S, or "none"
	setDelay         atomic.Int64 // nanoseconds; sleep this long before every S reply
	conns            sync.Map     // net.Conn -> struct{}
	silent           atomic.Bool  // once true, every G/S/D is read but never answered
}

// mockNodeOpts configures a startMockNode server's doc/adr/0019-*.md
// (response tags) behavior. Immutable for the server's whole lifetime —
// set once at construction, like requiredSecret — so acceptLoop's
// goroutine never races a test goroutine mutating it later.
type mockNodeOpts struct {
	// supportTags: acknowledge an extended `A ... T` with `OnT\n` and echo
	// tags on that connection's G/S/D replies. Off by default so the bulk
	// of the suite keeps exercising the legacy untagged path.
	supportTags bool
	// closeOnExtendedAuth: behave like a pre-doc/adr/0019-*.md server — an
	// extended `A ... T` is a parse error, so close the connection without
	// replying.
	closeOnExtendedAuth bool
}

// delaySets makes every future S reply from this node wait d first — for
// tests proving a caller isn't blocked on a slow replica leg
// (doc/adr/0014-*.md).
func (m *mockNode) delaySets(d time.Duration) { m.setDelay.Store(int64(d)) }

// goSilentAfterHandshake makes this node a half-open server from this
// point on: it still accepts and completes the A handshake, and still
// reads every request frame off the wire (so the TCP stream stays
// well-formed), but never writes a reply — regression coverage for a
// request-level I/O timeout (issue tracked alongside doc/adr/0016-*.md).
func (m *mockNode) goSilentAfterHandshake() { m.silent.Store(true) }

// answerWrongTagOnce queues a one-off reply for the next G request on a
// tagged connection that echoes the wrong tag (the request's tag + 1) —
// the desync a pre-doc/adr/0019-*.md stream misalignment would produce.
func (m *mockNode) answerWrongTagOnce() { m.wrongTagLeft.Add(1) }

// swallowGetOnce swallows the next G request entirely (no reply) — the
// off-by-one stream desync where every later response answers the
// previous request.
func (m *mockNode) swallowGetOnce() { m.swallowLeft.Add(1) }

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

func (m *mockNode) address() string { return m.listener.Addr().String() }

func (m *mockNode) storeLen() int {
	count := 0
	m.store.Range(func(_, _ any) bool { count++; return true })
	return count
}

func (m *mockNode) hasKey(key string) bool {
	_, ok := m.store.Load(key)
	return ok
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
	// doc/adr/0019-*.md: set once this connection's `A ... T` is
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
			if len(parts) > 2 && m.opts.closeOnExtendedAuth {
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
			if m.takeOne(&m.swallowLeft) {
				continue // no reply at all — the off-by-one desync injection
			}
			if tagged && m.takeOne(&m.wrongTagLeft) {
				// Echo the wrong tag (the request's tag + 1) — the desync
				// a pre-doc/adr/0019-*.md stream misalignment would
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
			} else if value, ok := m.store.Load(key); ok {
				stored := value.([]byte)
				reply = append([]byte(fmt.Sprintf("V %d%s\n", len(stored), tagSuffix)), stored...)
			} else {
				reply = []byte("N" + tagSuffix + "\n")
			}
			if _, err := conn.Write(reply); err != nil {
				return
			}
		case "S":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			value := mustRead(reader, atoiOrPanic(parts[2]))
			if m.silent.Load() {
				continue
			}
			// The TTL, when present, is the field after the two lengths
			// (omitted on the wire means "no expiry", i.e. 0); on a
			// tagged connection the tag sits after it as the last field.
			ttlBase := 3
			if tagged {
				ttlBase = 4
			}
			if len(parts) > ttlBase {
				m.lastSetTTL.Store(parts[3])
			} else {
				m.lastSetTTL.Store("none")
			}
			if delay := time.Duration(m.setDelay.Load()); delay > 0 {
				time.Sleep(delay)
			}
			reply := "S" + tagSuffix + "\n"
			if m.takeOne(&m.setWrongNodeLeft) || m.takeWrongNode() {
				reply = "W" + tagSuffix + "\n"
			} else {
				m.store.Store(key, value)
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		case "D":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			if m.silent.Load() {
				continue
			}
			reply := "N" + tagSuffix + "\n"
			if m.takeWrongNode() {
				reply = "W" + tagSuffix + "\n"
			} else if _, existed := m.store.LoadAndDelete(key); existed {
				reply = "D" + tagSuffix + "\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
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
	warmingUp   bool
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
			// doc/adr/0019-*.md: echo the tag capability — clients send
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

	if err := client.Set("k", "v", -1); err == nil {
		t.Fatal("negative ttl accepted")
	}
	if err := client.SetBytes("k", []byte("v"), -1); err == nil {
		t.Fatal("negative ttl accepted (SetBytes)")
	}
	if err := client.Set("k", "v", 60); err != nil {
		t.Fatal(err)
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
// value (doc/adr/0016-*.md) — a bug in matching responses to the right
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

// ── doc/adr/0019-*.md 応答タグ ───────────────────────────────────────

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
// exact misdelivery doc/adr/0016-*.md left open: the server never answers
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
	conn := newConnection(result.conn, func() {}, result.tagged)
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
	conn2 := newConnection(result2.conn, func() {}, result2.tagged)
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
// (pre-doc/adr/0019-*.md) server treats the extended `A ... T` as a parse
// error and closes without replying; the client must redial once with the
// plain form and run untagged — transparently, with the same results.
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
	// Two dials: the extended attempt the server slammed shut, then the
	// plain fallback that stuck.
	if got := node.connectionCount.Load(); got != 2 {
		t.Fatalf("connectionCount = %d, want 2 (extended attempt + plain fallback)", got)
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

// ── 値の圧縮 (doc/adr/0013-*.md) ────────────────────────────────────

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

	stored, ok := node.store.Load("k")
	if !ok || !bytes.Equal(stored.([]byte), []byte(value)) {
		t.Fatalf("stored = %v, %v", stored, ok)
	}
	got, ok, err := client.Get("k")
	if err != nil || !ok || got != value {
		t.Fatalf("Get = %q, %v, %v", got, ok, err)
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

	storedAny, ok := node.store.Load("k")
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

	storedAny, ok := node.store.Load("k")
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

	storedAny, ok := node.store.Load("k")
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
	// collide with the DEFLATE marker (0x01) — doc/adr/0013-*.md's
	// documented hazard of enabling Compress against a keyspace other
	// clients still touch without it. The remaining bytes are chosen to
	// reliably fail DEFLATE decoding (raw DEFLATE has no checksum, so not
	// every garbage body does — see compression_test.go's own pinned
	// test).
	node.store.Store("k", []byte{compressionMarkerDeflate, 0xFF, 0xFF, 0xFF, 0xFF})

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
	// Regression for issue #8/#12: a garbage V header is
	// connection-classified, so the built-in redial-and-retry-once makes
	// the same call succeed, never serving stray bytes.
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
	owner := nodes[NewHashRing(testNames).Route([]byte("some-key"))]

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

// ── fire-and-forget レプリカ書き込み (doc/adr/0014-*.md) ──────────────

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

// ── read repair (doc/adr/0015-*.md) ────────────────────────────────

func TestByDefaultACleanMissOnThePrimaryIsNotRepaired(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	const key = "k"
	owners := ownersOf(key)
	nodes[owners[1]].store.Store(key, []byte("from-replica"))

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
	nodes[owners[1]].store.Store(key, []byte("from-replica"))

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ReadRepair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value, ok, err := client.GetBytes(key)
	if err != nil || !ok || string(value) != "from-replica" {
		t.Fatalf("GetBytes = %q, %v, %v", value, ok, err)
	}

	if !waitUntil(t, 2*time.Second, func() bool { return nodes[owners[0]].hasKey(key) }) {
		t.Fatal("the primary was never repaired")
	}
	if got := nodes[owners[0]].lastSetTTL.Load(); got != "60" {
		t.Fatalf("repair TTL = %v, want %d (readRepairTTL, not immortal)", got, readRepairTTL)
	}
}

func TestReadRepairStaysACleanMissWhenNoOwnerHasTheValue(t *testing.T) {
	_, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ReadRepair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, ok, err := client.GetBytes("nowhere")
	if err != nil || ok {
		t.Fatalf("GetBytes = ok=%v err=%v, want a clean miss", ok, err)
	}
}

// ── Stats() (ADR-0011/0014/0015 swallowed-failure counters) ────────

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
	nodes[owners[1]].store.Store(key, []byte("from-replica"))
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
