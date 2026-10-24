package nanocached

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// Touch / GetAndTouch (issue #825).

func TestAppendTouchFrameEveryCombinationOfTTLAndGet(t *testing.T) {
	cases := []struct {
		name      string
		namespace []byte
		ttl       int64
		get       bool
		tagged    bool
		tag       uint32
		want      string
	}{
		{"no ttl", nil, -1, false, false, 0, "t 0 1\nk"},
		{"ttl", nil, 30, false, false, 0, "t 0 1 30\nk"},
		{"zero ttl is sent", nil, 0, false, false, 0, "t 0 1 0\nk"},
		{"get without ttl", nil, -1, true, false, 0, "t 0 1 G\nk"},
		{"get with ttl", nil, 30, true, false, 0, "t 0 1 30 G\nk"},
		{"namespace and tag", []byte("users"), 30, true, true, 7, "t 5 1 30 G 7\nusersk"},
		{"tag without ttl", nil, -1, false, true, 7, "t 0 1 7\nk"},
	}
	for _, tc := range cases {
		if got := string(appendTouchFrame(tc.namespace, []byte("k"), tc.ttl, tc.get, tc.tagged, tc.tag)); got != tc.want {
			t.Errorf("%s: appendTouchFrame = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func connectTo(t *testing.T, node *mockNode, config Config) *Client {
	t.Helper()
	config.Addresses = []Address{addr(node.address())}
	client, err := Connect(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestTouchExtendsShortensAndRemovesATTLWithoutTouchingTheValue(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 10); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		ttl     int64
		wantTTL int64
		hasTTL  bool
	}{{100, 100, true}, {5, 5, true}, {RemoveTTL, 0, false}} {
		if existed, err := client.Touch("k", step.ttl); err != nil || !existed {
			t.Fatalf("Touch(%d) = %v, %v, want true, nil", step.ttl, existed, err)
		}
		got, ok := node.storedTTL("k")
		if ok != step.hasTTL || (ok && got != step.wantTTL) {
			t.Fatalf("after Touch(%d): TTL = %d, %v, want %d, %v", step.ttl, got, ok, step.wantTTL, step.hasTTL)
		}
	}
	if value, ok, err := client.Get("k"); err != nil || !ok || value != "v" {
		t.Fatalf("Get after the touches = %q, %v, %v, want v, true, nil", value, ok, err)
	}
}

func TestTouchWithZeroExpiresTheEntryAtOnce(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Touch("k", 0); err != nil || !existed {
		t.Fatalf("Touch(0) = %v, %v, want true, nil", existed, err)
	}
	if _, ok, err := client.Get("k"); err != nil || ok {
		t.Fatalf("Get after Touch(0) = ok %v, err %v, want a miss", ok, err)
	}
}

func TestTouchOfAMissingKeyIsFalseAndCreatesNothing(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if existed, err := client.Touch("nope", 30); err != nil || existed {
		t.Fatalf("Touch(missing) = %v, %v, want false, nil", existed, err)
	}
	if _, ok, err := client.GetAndTouch("nope", 30); err != nil || ok {
		t.Fatalf("GetAndTouch(missing) = ok %v, err %v, want a miss", ok, err)
	}
	if node.storeLen() != 0 {
		t.Fatalf("a touch created %d keys", node.storeLen())
	}
}

func TestTouchNeverOverwritesALaterSet(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Touch("k", 30); err != nil {
		t.Fatal(err)
	}
	if err := client.Set("k", "newer", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Touch("k", 60); err != nil {
		t.Fatal(err)
	}
	if value, _, _ := client.Get("k"); value != "newer" {
		t.Fatalf("Get = %q, want newer", value)
	}
}

func TestTouchIsScopedToItsNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	users := client.Namespace("users")
	if err := users.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Touch("k", 30); err != nil || existed {
		t.Fatalf("default-namespace Touch = %v, %v, want false, nil", existed, err)
	}
	if existed, err := users.Touch("k", 30); err != nil || !existed {
		t.Fatalf("namespaced Touch = %v, %v, want true, nil", existed, err)
	}
	if value, ok, err := users.GetAndTouch("k", 5); err != nil || !ok || value != "v" {
		t.Fatalf("namespaced GetAndTouch = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if _, ok, _ := client.GetAndTouch("k", 5); ok {
		t.Fatal("the default namespace saw the namespaced key")
	}
}

func TestGetAndTouchReturnsTheValueAndSetsTheTTL(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.GetAndTouch("k", 60); err != nil || !ok || value != "v" {
		t.Fatalf("GetAndTouch = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if ttl, ok := node.storedTTL("k"); !ok || ttl != 60 {
		t.Fatalf("TTL = %d, %v, want 60, true", ttl, ok)
	}
	// No TTL: the reply carries no TTL field, and the TTL is removed.
	if value, ok, err := client.GetAndTouchBytes("k", RemoveTTL); err != nil || !ok || !bytes.Equal(value, []byte("v")) {
		t.Fatalf("GetAndTouchBytes(RemoveTTL) = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if _, ok := node.storedTTL("k"); ok {
		t.Fatal("the TTL was not removed")
	}
}

func TestGetAndTouchWithZeroReturnsTheValueThenTheEntryIsGone(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.GetAndTouch("k", 0); err != nil || !ok || value != "v" {
		t.Fatalf("GetAndTouch(0) = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if _, ok, _ := client.Get("k"); ok {
		t.Fatal("the entry survived GetAndTouch(0)")
	}
}

func TestGetAndTouchDecompressesOnACompressEnabledClient(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{Compress: true, CompressionThreshold: 16})
	value := strings.Repeat("x", 200)
	if err := client.Set("k", value, 0); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := client.GetAndTouch("k", 30); err != nil || !ok || got != value {
		t.Fatalf("GetAndTouch = %d bytes, %v, %v, want the original %d bytes", len(got), ok, err, len(value))
	}
}

func TestTouchRejectsAnInvalidTTLBeforeAnyIO(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.Touch("k", -2); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Touch(-2) err = %v, want ErrInvalidArgument", err)
	}
	if _, _, err := client.GetAndTouch("k", -2); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("GetAndTouch(-2) err = %v, want ErrInvalidArgument", err)
	}
	if got := node.touchesSeen(); len(got) != 0 {
		t.Fatalf("an invalid TTL reached the node: %v", got)
	}
}

func TestTouchRoundTripsOverATaggedConnection(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Touch("k", 30); err != nil || !existed {
		t.Fatalf("Touch = %v, %v, want true, nil", existed, err)
	}
	if value, ok, err := client.GetAndTouch("k", 60); err != nil || !ok || value != "v" {
		t.Fatalf("GetAndTouch = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if value, ok, err := client.GetAndTouchBytes("k", RemoveTTL); err != nil || !ok || string(value) != "v" {
		t.Fatalf("GetAndTouchBytes(RemoveTTL) = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if existed, err := client.Touch("nope", 30); err != nil || existed {
		t.Fatalf("Touch(missing) = %v, %v, want false, nil", existed, err)
	}
}

func TestTouchReturnsErrServerTooOldAgainstANodeThatPredatesTheCommand(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{withoutTouch: true})
	client := connectTo(t, node, Config{})
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Touch("k", 30); !errors.Is(err, ErrServerTooOld) {
		t.Fatalf("Touch err = %v, want ErrServerTooOld", err)
	}
	if _, _, err := client.GetAndTouch("k", 30); !errors.Is(err, ErrServerTooOld) {
		t.Fatalf("GetAndTouch err = %v, want ErrServerTooOld", err)
	}
	// Everything else keeps working: the client redials.
	if value, ok, err := client.Get("k"); err != nil || !ok || value != "v" {
		t.Fatalf("Get after the rejections = %q, %v, %v, want v, true, nil", value, ok, err)
	}
}

func TestErrServerTooOldKeepsTheErrProtocolItIsBuiltOn(t *testing.T) {
	// ErrServerTooOld is joined onto the ErrProtocol an `En` always
	// produced, so a caller matching ErrProtocol sees no change.
	node := startMockNodeOpts(t, nil, mockNodeOpts{withoutTouch: true})
	client := connectTo(t, node, Config{})
	_, err := client.Touch("k", 30)
	if !errors.Is(err, ErrServerTooOld) || !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v, want both ErrServerTooOld and ErrProtocol", err)
	}
}

// ── cluster ──────────────────────────────────────────────────────────

func startTouchCluster(t *testing.T, opts [2]mockNodeOpts) (map[string]*mockNode, *mockDiscovery) {
	t.Helper()
	nodes := map[string]*mockNode{
		testNames[0]: startMockNodeOpts(t, nil, opts[0]),
		testNames[1]: startMockNodeOpts(t, nil, opts[1]),
	}
	listed := make([]discoveredNode, 0, len(nodes))
	for _, name := range testNames {
		listed = append(listed, discoveredNode{Name: name, Address: nodes[name].address()})
	}
	return nodes, startMockDiscovery(t, listed, 2)
}

func TestClusterTouchReachesEveryOwner(t *testing.T) {
	nodes, discovery := startTouchCluster(t, [2]mockNodeOpts{})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 10); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Touch("k", 77); err != nil || !existed {
		t.Fatalf("Touch = %v, %v, want true, nil", existed, err)
	}
	for name, node := range nodes {
		if ttl, ok := node.storedTTL("k"); !ok || ttl != 77 {
			t.Errorf("%s: TTL = %d, %v, want 77, true", name, ttl, ok)
		}
		if got := node.touchesSeen(); len(got) != 1 || got[0] != (touchRecord{"", "k", 77, false}) {
			t.Errorf("%s: touches = %v, want one plain touch with ttl 77", name, got)
		}
	}
}

func TestClusterGetAndTouchAsksOnlyThePrimaryForTheValue(t *testing.T) {
	nodes, discovery := startTouchCluster(t, [2]mockNodeOpts{})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	owners := ownersOf("k")
	primary, replica := nodes[owners[0]], nodes[owners[1]]
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := client.GetAndTouch("k", 40); err != nil || !ok || value != "v" {
		t.Fatalf("GetAndTouch = %q, %v, %v, want v, true, nil", value, ok, err)
	}
	if got := primary.touchesSeen(); len(got) != 1 || got[0] != (touchRecord{"", "k", 40, true}) {
		t.Errorf("primary touches = %v, want one touch with G", got)
	}
	if got := replica.touchesSeen(); len(got) != 1 || got[0] != (touchRecord{"", "k", 40, false}) {
		t.Errorf("replica touches = %v, want one plain touch", got)
	}
	if ttl, ok := replica.storedTTL("k"); !ok || ttl != 40 {
		t.Errorf("replica TTL = %d, %v, want 40, true", ttl, ok)
	}
}

func TestClusterGetAndTouchDoesNotFallBackToAReplicaWhenThePrimaryMisses(t *testing.T) {
	nodes, discovery := startTouchCluster(t, [2]mockNodeOpts{})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	owners := ownersOf("k")
	replica := nodes[owners[1]]
	replica.store.Store(storeKey{"", "k"}, []byte("v"))
	if _, ok, err := client.GetAndTouch("k", 40); err != nil || ok {
		t.Fatalf("GetAndTouch = ok %v, err %v, want a miss (answered by the primary alone)", ok, err)
	}
	// The replica was still sent the plain touch, not asked for the value.
	if got := replica.touchesSeen(); len(got) != 1 || got[0].get {
		t.Fatalf("replica touches = %v, want one plain touch", got)
	}
}

func TestClusterTouchRefreshesOnceAfterAWrongNodeAnswer(t *testing.T) {
	nodes, discovery := startTouchCluster(t, [2]mockNodeOpts{})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	primary := nodes[ownersOf("k")[0]]
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	primary.wrongNodeLeft.Add(1)
	if value, ok, err := client.GetAndTouch("k", 40); err != nil || !ok || value != "v" {
		t.Fatalf("GetAndTouch after a W = %q, %v, %v, want v, true, nil", value, ok, err)
	}
}

func TestClusterTouchWhoseReplyWasLostIsResentUnlikeAnIncr(t *testing.T) {
	nodes, discovery := startTouchCluster(t, [2]mockNodeOpts{})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	primary := nodes[ownersOf("k")[0]]
	if err := client.Set("k", "10", 0); err != nil {
		t.Fatal(err)
	}
	// The primary applies the touch and the reply never arrives; resending
	// it changes nothing, so the client does.
	primary.applyThenCloseTouchTimes(1)
	if existed, err := client.Touch("k", 30); err != nil || !existed {
		t.Fatalf("Touch after a lost reply = %v, %v, want true, nil", existed, err)
	}
	resent := 0
	for _, seen := range primary.touchesSeen() {
		if seen.ttl == 30 {
			resent++
		}
	}
	if resent != 2 {
		t.Fatalf("primary saw %d touches with ttl 30, want 2 (one lost reply, one resend)", resent)
	}

	primary.applyThenCloseTimes(1)
	if _, _, err := client.Incr("k", 1); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Incr after a lost reply err = %v, want ErrConnectionLost (never resent)", err)
	}
}

func TestClusterTouchSurvivesAReplicaThatPredatesTheCommand(t *testing.T) {
	var opts [2]mockNodeOpts
	opts[1].withoutTouch = true // the node of testNames[1]
	_, discovery := startTouchCluster(t, opts)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Pick a key whose primary is the new node, so the old one is a replica.
	key := ""
	for i := 0; i < 200; i++ {
		candidate := "k" + strings.Repeat("x", i)
		if ownersOf(candidate)[0] == testNames[0] {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Skip("no key found whose primary is the new node")
	}
	if err := client.Set(key, "v", 0); err != nil {
		t.Fatal(err)
	}
	if existed, err := client.Touch(key, 30); err != nil || !existed {
		t.Fatalf("Touch with an old replica = %v, %v, want true, nil", existed, err)
	}
	if got := client.Stats().ReplicaWriteFailures; got < 1 {
		t.Fatalf("ReplicaWriteFailures = %d, want at least 1 (the old replica)", got)
	}
}
