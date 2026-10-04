package nanocached

import (
	"net"
	"strconv"
	"testing"
)

// TestNodeReplicationRefusesAProxyRoster (issue #486): a `Q` roster carries
// no replication factor, and reading one off it is a caller bug — the
// accessor must say so rather than hand back a meaningless zero.
func TestNodeReplicationRefusesAProxyRoster(t *testing.T) {
	proxies := &identified{nodes: []discoveredNode{{Name: "p", Address: "127.0.0.1:1"}}, list: listProxies}
	if _, err := proxies.nodeReplication(); err == nil {
		t.Fatal("expected an error reading the replication factor off a Q roster")
	}
	nodes := &identified{nodes: []discoveredNode{{Name: "n", Address: "127.0.0.1:1"}}, replication: 2, list: listNodes}
	if r, err := nodes.nodeReplication(); err != nil || r != 2 {
		t.Fatalf("expected replication 2 from an L roster, got %d, %v", r, err)
	}
}

// Discovery registers an IPv6 node as `{ip}:{port}` with no brackets
// (`2001:db8::1:8356`), which net.Dial rejects ("too many colons"):
// dialAddress splits on the last ':' like every other SDK and rebuilds the
// address with net.JoinHostPort.
func TestDialAddress(t *testing.T) {
	cases := []struct{ in, want string }{
		{"127.0.0.1:8356", "127.0.0.1:8356"},
		{"node-1.example.com:8356", "node-1.example.com:8356"},
		{"::1:8356", "[::1]:8356"},
		{"2001:db8::1:8356", "[2001:db8::1]:8356"},
		{"fe80::1ff:fe23:4567:890a:8356", "[fe80::1ff:fe23:4567:890a]:8356"},
		{"[::1]:8356", "[::1]:8356"},
		{"[2001:db8::1]:8356", "[2001:db8::1]:8356"},
		// What net.Dial would reject on its own is passed through for it
		// to reject, not mangled.
		{"no-port-here", "no-port-here"},
		{"[::1]", "[::1]"},
		{"", ""},
	}
	for _, c := range cases {
		if got := dialAddress(c.in); got != c.want {
			t.Errorf("dialAddress(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// The TLS ServerName open() derives from the address must be the bare
// host: with brackets it would never match a certificate.
func TestDialAddressSplitsToTheBareHostForTLSServerName(t *testing.T) {
	for in, wantHost := range map[string]string{
		"::1:8356":        "::1",
		"[::1]:8356":      "::1",
		"2001:db8::1:443": "2001:db8::1",
		"127.0.0.1:8356":  "127.0.0.1",
		"example.com:443": "example.com",
	} {
		host, _, err := net.SplitHostPort(dialAddress(in))
		if err != nil || host != wantHost {
			t.Errorf("ServerName host for %q = %q, %v, want %q", in, host, err, wantHost)
		}
	}
}

// unbracketed rewrites a listener address `[::1]:port` the way discovery
// registers it: `::1:port`.
func unbracketed(t *testing.T, address string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	return host + ":" + port
}

// requireIPv6Loopback skips the test on a host without ::1.
func requireIPv6Loopback(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback on this host: %v", err)
	}
	_ = l.Close()
}

func startMockNodeIPv6(t *testing.T) *mockNode {
	t.Helper()
	return startMockNodeAt(t, nil, "[::1]:0")
}

func TestConnectDialsAnUnbracketedIPv6RosterEntry(t *testing.T) {
	requireIPv6Loopback(t)
	node := startMockNodeIPv6(t)
	discovery := startMockDiscovery(t,
		[]discoveredNode{{Name: "node-a", Address: unbracketed(t, node.address())}}, 1)

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatalf("Connect with an IPv6 roster entry = %v", err)
	}
	defer client.Close()

	if client.members["node-a"].connection.isClosed() {
		t.Fatal("the IPv6 node has no live connection after bootstrap")
	}
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatalf("Set = %v", err)
	}
	if value, ok, err := client.Get("k"); err != nil || !ok || value != "v" {
		t.Fatalf("Get = %q, %v, %v", value, ok, err)
	}
	if !node.hasKey("k") {
		t.Fatal("the write did not reach the IPv6 node")
	}
}

func TestRefreshAndLazyRedialDialAnUnbracketedIPv6RosterEntry(t *testing.T) {
	requireIPv6Loopback(t)
	first := startMockNodeIPv6(t)
	second := startMockNodeIPv6(t)
	discovery := startMockDiscovery(t,
		[]discoveredNode{{Name: "node-a", Address: unbracketed(t, first.address())}}, 1)

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Refresh: a newly listed IPv6 node joins the roster; Go connects new
	// members lazily, so its first request dials it from the stored
	// (unbracketed) address. The existing connection is dropped as well, so
	// that redial path is covered too.
	discovery.setNodes([]discoveredNode{
		{Name: "node-a", Address: unbracketed(t, first.address())},
		{Name: "node-b", Address: unbracketed(t, second.address())},
	})
	client.maybeRefresh(true)
	if len(client.members) != 2 {
		t.Fatalf("members after refresh = %d, want 2", len(client.members))
	}
	client.members["node-a"].connection.close()
	for i := 0; i < 20; i++ {
		if err := client.Set("k"+strconv.Itoa(i), "v", 0); err != nil {
			t.Fatalf("Set after dropping connections = %v", err)
		}
	}
	if first.storeLen() == 0 || second.storeLen() == 0 {
		t.Fatalf("writes after the redial reached first=%d second=%d keys, want both nonzero",
			first.storeLen(), second.storeLen())
	}
}

func TestViaProxyDialsAnUnbracketedIPv6ProxyEntry(t *testing.T) {
	requireIPv6Loopback(t)
	proxy := startMockNodeIPv6(t)
	discovery := startMockDiscovery(t, nil, 1)
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: unbracketed(t, proxy.address())}})

	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatalf("Connect via an IPv6 proxy entry = %v", err)
	}
	defer client.Close()
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	if !proxy.hasKey("k") {
		t.Fatal("the write did not reach the IPv6 proxy")
	}
}
