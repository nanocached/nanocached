package nanocached

// Integration tests against in-process mock servers speaking just enough
// of the wire protocol — mirrors the other SDKs' mock-based suites.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
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
	conns           sync.Map     // net.Conn -> struct{}
}

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
	client, err := Connect(Config{Seeds: []string{node.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("greeting", []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.Get("greeting")
	if err != nil || !ok || string(value) != "hello" {
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

func TestRejectsANegativeTtl(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Seeds: []string{node.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", []byte("v"), -time.Second); err == nil {
		t.Fatal("negative ttl accepted")
	}
	if err := client.Set("k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticates(t *testing.T) {
	node := startMockNode(t, []byte("s3cret"))

	client, err := Connect(Config{Seeds: []string{node.address()}, AuthSecret: "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	client.Close()

	if _, err := Connect(Config{Seeds: []string{node.address()}}); err == nil ||
		!strings.Contains(err.Error(), "requires authentication") {
		t.Fatalf("missing-secret error = %v", err)
	}
	if _, err := Connect(Config{Seeds: []string{node.address()}, AuthSecret: "wrong"}); err == nil ||
		!strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("wrong-secret error = %v", err)
	}
}

func TestWrongNodePropagatesInSingleMode(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Seeds: []string{node.address()}})
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
	client, err := Connect(Config{Seeds: []string{node.address()}})
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

// ── 遅延再接続と keep-alive ───────────────────────────────────────

func TestSubSecondTtlRoundsUpToOneSecond(t *testing.T) {
	// Regression for issue #9: 300ms must not truncate to an explicit
	// 0-second TTL (near-immediate expiry).
	node := startMockNode(t, nil)
	client, err := Connect(Config{Seeds: []string{node.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", []byte("v"), 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := node.lastSetTTL.Load(); got != "1" {
		t.Fatalf("300ms TTL sent as %v, want \"1\"", got)
	}
	if err := client.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if got := node.lastSetTTL.Load(); got != "none" {
		t.Fatalf("zero TTL sent as %v, want none", got)
	}
	if err := client.Set("k", []byte("v"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := node.lastSetTTL.Load(); got != "2" {
		t.Fatalf("2s TTL sent as %v, want \"2\"", got)
	}
}

func TestAMismatchedResponseKindPoisonsTheConnection(t *testing.T) {
	// A well-formed response of the wrong kind (`S` answering a G) means
	// the request/response streams are off by one; reusing the connection
	// would answer every later request with the previous one's response.
	// The mismatch poisons the connection, and the connection-classified
	// error is healed by the built-in redial-and-retry-once — never by
	// reusing the desynced stream.
	node := startMockNode(t, nil)
	client, err := Connect(Config{Seeds: []string{node.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	node.storedToGetLeft.Add(1)
	value, ok, err := client.Get("k")
	if err != nil || !ok || string(value) != "v" {
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
	_, err = Connect(Config{Seeds: []string{listener.Addr().String()}})
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
	client, err := Connect(Config{Seeds: []string{node.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	node.malformedLeft.Add(1)
	value, ok, err := client.Get("k")
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if node.connectionCount.Load() != 2 {
		t.Fatalf("connections = %d", node.connectionCount.Load())
	}
}

func TestTransparentlyReconnectsAfterAServerFin(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{Seeds: []string{node.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	node.dropConnections()
	time.Sleep(50 * time.Millisecond) // let the FIN land

	value, ok, err := client.Get("k")
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get after FIN = %q, %v, %v", value, ok, err)
	}
	if node.connectionCount.Load() != 2 {
		t.Fatalf("connections = %d", node.connectionCount.Load())
	}
}

func TestKeepAlivePingsAnIdleConnection(t *testing.T) {
	node := startMockNode(t, nil)
	client, err := Connect(Config{
		Seeds:             []string{node.address()},
		KeepAliveInterval: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	waitFor(t, func() bool { return node.getCount.Load() >= 2 }, "keep-alive pings")
	if node.connectionCount.Load() != 1 {
		t.Fatalf("connections = %d", node.connectionCount.Load())
	}
}

// ── seeds ─────────────────────────────────────────────────────────

func TestRejectsAMissingTarget(t *testing.T) {
	if _, err := Connect(Config{}); err == nil {
		t.Fatal("empty seeds accepted")
	}
}

func TestFailsOverToTheSecondSeed(t *testing.T) {
	node := startMockNode(t, nil)
	discovery := startMockDiscovery(t,
		[]DiscoveredNode{{Name: testNames[0], Address: node.address()}}, 1)

	client, err := Connect(Config{Seeds: []string{unusedPort(t), discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Get("k"); err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
}

func TestRaisesBusyWhenEverySeedIsWarming(t *testing.T) {
	first := startMockDiscovery(t, nil, 1)
	second := startMockDiscovery(t, nil, 1)
	first.setWarming(true)
	second.setWarming(true)

	if _, err := Connect(Config{Seeds: []string{first.address(), second.address()}}); !errors.Is(err, ErrDiscoveryBusy) {
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
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i := 0; i < 50; i++ {
		if err := client.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		value, ok, err := client.Get(fmt.Sprintf("key-%d", i))
		if err != nil || !ok || string(value) != fmt.Sprintf("value-%d", i) {
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
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("some-key", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	owner := nodes[NewHashRing(testNames).Route([]byte("some-key"))]

	owner.wrongNodeLeft.Add(1)
	if value, ok, err := client.Get("some-key"); err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get after one W = %q, %v, %v", value, ok, err)
	}

	owner.wrongNodeLeft.Add(2)
	if _, _, err := client.Get("some-key"); !errors.Is(err, ErrWrongNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestFansWritesOutToEveryOwner(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if client.Replication() != 2 {
		t.Fatalf("Replication = %d", client.Replication())
	}
	for i := 0; i < 20; i++ {
		if err := client.Set(fmt.Sprintf("key-%d", i), []byte("v"), 0); err != nil {
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
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("survives", []byte("still here"), 0); err != nil {
		t.Fatal(err)
	}
	nodes[ownersOf("survives")[0]].close()
	time.Sleep(50 * time.Millisecond)

	value, ok, err := client.Get("survives")
	if err != nil || !ok || string(value) != "still here" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
}

func TestADeadReplicaDoesNotFailWrites(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	owners := ownersOf("written-anyway")
	nodes[owners[1]].close()
	time.Sleep(50 * time.Millisecond)

	if err := client.Set("written-anyway", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if !nodes[owners[0]].hasKey("written-anyway") {
		t.Fatal("primary missing the key")
	}
}

func TestWritesRouteAroundADeadPrimaryOnceDiscoveryDropsIt(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
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

	if err := client.Set(key, []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.Get(key); err != nil || !ok || string(value) != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
}

func TestFansDeletesOutToEveryOwner(t *testing.T) {
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Seeds: []string{discovery.address()}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("gone-everywhere", []byte("v"), 0); err != nil {
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
