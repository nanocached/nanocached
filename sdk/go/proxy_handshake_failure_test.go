package nanocached

import (
	"errors"
	"strings"
	"testing"
)

// proxyClientWithRoster connects in proxy mode through a live proxy, then
// replaces the roster discovery serves.
func proxyClientWithRoster(t *testing.T, roster func(discovery *mockDiscovery) []discoveredNode) *Client {
	t.Helper()
	live := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 2)
	discovery.setProxies([]discoveredNode{{Name: "proxy-live", Address: live.address()}})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	discovery.setProxies(roster(discovery))
	return client
}

// The proxy that failed the handshake is the one the error names, not the
// proxy the client was moving away from.
func TestReconnectAnotherProxyNamesTheProxyThatFailedTheHandshake(t *testing.T) {
	garbage := startGarbageListener(t)
	client := proxyClientWithRoster(t, func(*mockDiscovery) []discoveredNode {
		return []discoveredNode{{Name: "proxy-garbage", Address: garbage}}
	})

	const dead = "dead-proxy.invalid:1"
	_, _, err := client.reconnectAnotherProxy(dead)
	if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) {
		t.Fatalf("reconnectAnotherProxy = %v, want ErrConnectionLost marked not sent", err)
	}
	if !strings.Contains(err.Error(), garbage) {
		t.Errorf("error %q does not name the proxy that failed the handshake (%s)", err, garbage)
	}
	if strings.Contains(err.Error(), dead) {
		t.Errorf("error %q names the proxy that was left (%s)", err, dead)
	}
}

// A candidate that no longer identifies as a proxy is not a failed
// handshake reply, but it is still a lost connection that was never used
// (the client moves on to another proxy), with a message that says so.
func TestReconnectAnotherProxyReportsANoLongerAProxyAccurately(t *testing.T) {
	client := proxyClientWithRoster(t, func(discovery *mockDiscovery) []discoveredNode {
		// A discovery server identifies as discovery, not as a proxy.
		return []discoveredNode{{Name: "proxy-not-a-proxy", Address: discovery.address()}}
	})

	_, _, err := client.reconnectAnotherProxy("dead-proxy.invalid:1")
	if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) {
		t.Fatalf("reconnectAnotherProxy = %v, want ErrConnectionLost marked not sent", err)
	}
	if !strings.Contains(err.Error(), "no longer identifies as a proxy") {
		t.Errorf("error %q lost its own message", err)
	}
	if strings.Contains(err.Error(), "did not complete the handshake") {
		t.Errorf("error %q calls it a failed handshake", err)
	}
}
