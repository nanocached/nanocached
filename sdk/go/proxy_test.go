package nanocached

// Issue #122: SDK proxy mode (Config.ViaProxy) — a client fetches the
// discovery-registered nanocached-proxy roster (`Q`) instead of the node
// roster (`L`) and runs in single-connection mode against one proxy,
// chosen at random. A mock "proxy" is just a mockNode (see client_test.go)
// — that is literally what a proxy looks like to a client (the spec's own
// framing): it answers the identify handshake `On`/`OnT` and speaks full
// G/S/D/g/s/d/c/F, never W.

import (
	"testing"
	"time"
)

func TestViaProxyRoutesAllOperationsThroughTheChosenProxy(t *testing.T) {
	proxy := startMockNode(t, nil)
	node := startMockNode(t, nil) // never touched — proves L/nodes are irrelevant to ViaProxy
	discovery := startMockDiscovery(t, []discoveredNode{{Name: "node-a", Address: node.address()}}, 2)
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxy.address()}})

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get() = %q, %v, %v", value, ok, err)
	}
	existed, err := client.Delete("k")
	if err != nil || !existed {
		t.Fatalf("Delete() = %v, %v", existed, err)
	}

	users := client.Namespace("users")
	if err := users.Set("42", "alice", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := users.Get("42"); err != nil || !ok || value != "alice" {
		t.Fatalf("namespaced Get() = %q, %v, %v", value, ok, err)
	}
	if err := users.Clear(); err != nil {
		t.Fatal(err)
	}
	if proxy.hasNSKey("users", "42") {
		t.Fatal("expected users.Clear() to have dropped the namespaced key")
	}

	if proxy.connectionCount.Load() == 0 {
		t.Fatal("expected the client to have connected to the proxy")
	}
	if node.connectionCount.Load() != 0 {
		t.Fatalf("expected the client to never dial the node address, got %d connections",
			node.connectionCount.Load())
	}
	if discovery.lCount.Load() != 0 {
		t.Fatalf("expected the client to never send L, got %d", discovery.lCount.Load())
	}
	if discovery.qCount.Load() == 0 {
		t.Fatal("expected the client to have sent Q")
	}
}

func TestViaProxySpreadsAcrossProxiesAtRandom(t *testing.T) {
	proxyA := startMockNode(t, nil)
	proxyB := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 2)
	discovery.setProxies([]discoveredNode{
		{Name: "proxy-a", Address: proxyA.address()},
		{Name: "proxy-b", Address: proxyB.address()},
	})

	// Statistical, not deterministic per-run — but with 40 independent
	// fresh clients across 2 proxies, the odds every single one lands on
	// the same proxy are 2^-39; this doesn't flake in practice.
	for i := 0; i < 40; i++ {
		client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
		if err != nil {
			t.Fatal(err)
		}
		client.Close()
	}

	if proxyA.connectionCount.Load() == 0 {
		t.Fatal("expected proxy A to have received at least one connection")
	}
	if proxyB.connectionCount.Load() == 0 {
		t.Fatal("expected proxy B to have received at least one connection")
	}
}

func TestViaProxyFailsOverToTheLiveProxy(t *testing.T) {
	dead := unusedPort(t) // nothing listens here — every dial to it fails
	live := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 2)
	discovery.setProxies([]discoveredNode{
		{Name: "proxy-dead", Address: dead},
		{Name: "proxy-live", Address: live.address()},
	})

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if !live.hasKey("k") {
		t.Fatal("expected the client to have failed over to the live proxy")
	}
}

func TestViaProxyServesFromTheSecondDiscoverySeedWhenTheFirstIsWarmingUp(t *testing.T) {
	proxy := startMockNode(t, nil)
	first := startMockDiscovery(t, nil, 2)
	first.setWarming(true)
	second := startMockDiscovery(t, nil, 2)
	second.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxy.address()}})

	client, err := Connect(Config{
		Addresses: []Address{addr(first.address()), addr(second.address())},
		ViaProxy:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if !proxy.hasKey("k") {
		t.Fatal("expected the write to land on the proxy served by the second discovery seed")
	}
}

func TestViaProxyEmptyRosterFailsConnect(t *testing.T) {
	discovery := startMockDiscovery(t, nil, 2)
	// No setProxies call: the roster is empty.

	_, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err == nil {
		t.Fatal("expected Connect to fail with an empty proxy roster")
	}
}

func TestViaProxyPointedAtANodeFailsConnect(t *testing.T) {
	node := startMockNode(t, nil)

	_, err := Connect(Config{Addresses: []Address{addr(node.address())}, ViaProxy: true})
	if err == nil {
		t.Fatal("expected Connect to fail when ViaProxy points at a node address")
	}
}

func TestViaProxyReconnectsToAnotherProxyOnLoss(t *testing.T) {
	proxyA := startMockNode(t, nil)
	proxyB := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 2)
	// Only proxy A is registered at connect time, so the client
	// deterministically lands on it.
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxyA.address()}})

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("k", "v1", 0); err != nil {
		t.Fatal(err)
	}
	if !proxyA.hasKey("k") {
		t.Fatal("expected the initial write to land on proxy A")
	}

	// Proxy A "dies"; discovery's roster is refreshed to reflect only the
	// survivor — exactly what a real discovery's liveness eviction would
	// eventually produce.
	proxyA.dropConnections()
	proxyA.close()
	discovery.setProxies([]discoveredNode{{Name: "proxy-b", Address: proxyB.address()}})

	if err := client.Set("k", "v2", 0); err != nil {
		t.Fatalf("expected the client to reconnect via a fresh Q fetch, got %v", err)
	}
	if !proxyB.hasKey("k") {
		t.Fatal("expected the retried write to land on proxy B")
	}
	if discovery.qCount.Load() < 2 {
		t.Fatalf("expected at least 2 Q fetches (initial connect + reconnect), got %d",
			discovery.qCount.Load())
	}
}

// TestViaProxyTransientRetryStatusWorksAgainstAProxy: issue #125's `R`
// path isn't node-specific — a proxy connection retries transparently
// the same way (spec: "one test is enough" for via_proxy coverage).
func TestViaProxyTransientRetryStatusWorksAgainstAProxy(t *testing.T) {
	proxy := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	discovery := startMockDiscovery(t, nil, 2)
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxy.address()}})

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	proxy.answerRetryableTimes(1)
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatalf("Set() with one R via proxy = %v, want it to transparently succeed", err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get() via proxy = %q, %v, %v, want \"v\", true, nil", value, ok, err)
	}
	if got := proxy.connectionCount.Load(); got != 1 {
		t.Fatalf("connectionCount = %d, want 1 (the R retry must not redial)", got)
	}
	if got := client.Stats().TransientRetries; got != 1 {
		t.Fatalf("Stats().TransientRetries = %d, want 1", got)
	}
}

// TestViaProxyGetManyAndSetManyRideTheSingleConnection covers
// GetMany/SetMany (issues #128/#150/#151) in ViaProxy mode: a single
// connection has no owners to group by, so both ops fall straight
// through to the proxy on one `m`/`o` sub-frame each, exactly like
// Get/Set's own single-connection behavior.
func TestViaProxyGetManyAndSetManyRideTheSingleConnection(t *testing.T) {
	proxy := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 2)
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxy.address()}})

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatalf("SetMany via proxy = %v", err)
	}
	if got := proxy.oCount.Load(); got != 1 {
		t.Fatalf("oCount = %d, want 1", got)
	}

	values, err := client.GetMany([]string{"a", "b", "missing"})
	if err != nil {
		t.Fatalf("GetMany via proxy = %v", err)
	}
	if values["a"] != "1" || values["b"] != "2" {
		t.Fatalf("GetMany = %v, want {a:1 b:2}", values)
	}
	if _, present := values["missing"]; present {
		t.Fatal("expected \"missing\" to be absent")
	}
	if got := proxy.mCount.Load(); got != 1 {
		t.Fatalf("mCount = %d, want 1", got)
	}
}

func TestViaProxyHedgedReadOptionIsInert(t *testing.T) {
	proxy := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 2)
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxy.address()}})

	client, err := Connect(Config{
		Addresses:      []Address{addr(discovery.address())},
		ViaProxy:       true,
		ReadHedgeAfter: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	proxy.delayGets(20 * time.Millisecond) // long past ReadHedgeAfter
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get() = %q, %v, %v", value, ok, err)
	}
	// A single owner (the one proxy connection) means there is nothing to
	// hedge to even if the option were somehow live — one G is the only
	// possible outcome, hedged or not. This asserts it explicitly rather
	// than relying on that structural argument alone.
	if got := proxy.getCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 G on the wire (no hedge attempted), got %d", got)
	}
}
