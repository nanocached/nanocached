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
	listener        net.Listener
	requiredSecret  []byte
	store           sync.Map // string -> []byte
	connectionCount atomic.Int32
	getCount        atomic.Int32
	wrongNodeLeft   atomic.Int32
	malformedLeft   atomic.Int32
	storedToGetLeft atomic.Int32
	lastSetTTL      atomic.Value // string: the TTL field of the last S, or "none"
	setDelay        atomic.Int64 // nanoseconds; sleep this long before every S reply
	conns           sync.Map     // net.Conn -> struct{}
}

// delaySets makes every future S reply from this node wait d first — for
// tests proving a caller isn't blocked on a slow replica leg
// (doc/adr/0014-*.md).
func (m *mockNode) delaySets(d time.Duration) { m.setDelay.Store(int64(d)) }

func startMockNode(t *testing.T, requiredSecret []byte) *mockNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
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
	for {
		header, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		parts := strings.Split(strings.TrimSuffix(header, "\n"), " ")
		switch parts[0] {
		case "A":
			secret := mustRead(reader, atoiOrPanic(parts[1]))
			accepted := len(secret) > 0
			if m.requiredSecret != nil {
				accepted = bytes.Equal(secret, m.requiredSecret)
			}
			reply := "On\n"
			if !accepted {
				reply = "En\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil || !accepted {
				return
			}
		case "G":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			m.getCount.Add(1)
			if m.takeOne(&m.malformedLeft) {
				if _, err := conn.Write([]byte("V x\n")); err != nil {
					return
				}
				continue
			}
			if m.takeOne(&m.storedToGetLeft) {
				// A well-formed frame of the wrong kind, as a desynced
				// (off-by-one) stream would produce.
				if _, err := conn.Write([]byte("S\n")); err != nil {
					return
				}
				continue
			}
			var reply []byte
			if m.takeWrongNode() {
				reply = []byte("W\n")
			} else if value, ok := m.store.Load(key); ok {
				stored := value.([]byte)
				reply = append([]byte(fmt.Sprintf("V %d\n", len(stored))), stored...)
			} else {
				reply = []byte("N\n")
			}
			if _, err := conn.Write(reply); err != nil {
				return
			}
		case "S":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			value := mustRead(reader, atoiOrPanic(parts[2]))
			if len(parts) == 4 {
				m.lastSetTTL.Store(parts[3])
			} else {
				m.lastSetTTL.Store("none")
			}
			if delay := time.Duration(m.setDelay.Load()); delay > 0 {
				time.Sleep(delay)
			}
			reply := "S\n"
			if m.takeWrongNode() {
				reply = "W\n"
			} else {
				m.store.Store(key, value)
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		case "D":
			key := string(mustRead(reader, atoiOrPanic(parts[1])))
			reply := "N\n"
			if m.takeWrongNode() {
				reply = "W\n"
			} else if _, existed := m.store.LoadAndDelete(key); existed {
				reply = "D\n"
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
	nodes       []DiscoveredNode
	warmingUp   bool
}

func startMockDiscovery(t *testing.T, nodes []DiscoveredNode, replication int) *mockDiscovery {
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

func (m *mockDiscovery) setNodes(nodes []DiscoveredNode) {
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
			if _, err := conn.Write([]byte("Od\n")); err != nil {
				return
			}
		case "L":
			m.mu.Lock()
			warming, nodes := m.warmingUp, append([]DiscoveredNode(nil), m.nodes...)
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

	if _, err := Connect(Config{Addresses: []Address{addr(node.address())}}); err == nil ||
		!strings.Contains(err.Error(), "requires authentication") {
		t.Fatalf("missing-secret error = %v", err)
	}
	if _, err := Connect(Config{Addresses: []Address{addr(node.address())}, AuthSecret: "wrong"}); err == nil ||
		!strings.Contains(err.Error(), "authentication failed") {
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

	original := handshakeDeadline
	handshakeDeadline = 100 * time.Millisecond
	defer func() { handshakeDeadline = original }()

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

// ── addresses ─────────────────────────────────────────────────────

func TestRejectsAMissingTarget(t *testing.T) {
	if _, err := Connect(Config{}); err == nil {
		t.Fatal("empty addresses accepted")
	}
}

func TestFailsOverToTheSecondAddress(t *testing.T) {
	node := startMockNode(t, nil)
	discovery := startMockDiscovery(t,
		[]DiscoveredNode{{Name: testNames[0], Address: node.address()}}, 1)

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

// ── クラスタと複製 ────────────────────────────────────────────────

func startCluster(t *testing.T, replication int) (map[string]*mockNode, *mockDiscovery) {
	t.Helper()
	nodes := map[string]*mockNode{
		testNames[0]: startMockNode(t, nil),
		testNames[1]: startMockNode(t, nil),
	}
	listed := make([]DiscoveredNode, 0, len(nodes))
	for _, name := range testNames {
		listed = append(listed, DiscoveredNode{Name: name, Address: nodes[name].address()})
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
	discovery.setNodes([]DiscoveredNode{{Name: owners[1], Address: nodes[owners[1]].address()}})
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
