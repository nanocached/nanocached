package nanocached

import (
	"errors"
	"testing"
)

// Caller mistakes caught by Connect before anything is dialed carry
// ErrInvalidArgument, like every other client-side argument check.
func TestConnectOptionErrorsAreInvalidArgument(t *testing.T) {
	cases := map[string]Config{
		"no addresses":       {},
		"negative threshold": {Addresses: []Address{{Host: "127.0.0.1", Port: 1}}, CompressionThreshold: -1},
		"negative hedge":     {Addresses: []Address{{Host: "127.0.0.1", Port: 1}}, ReadHedgeAfter: -1},
		"hedge with proxy":   {Addresses: []Address{{Host: "127.0.0.1", Port: 1}}, ViaProxy: true, ReadHedgeAfter: 1},
		"CA without TLS":     {Addresses: []Address{{Host: "127.0.0.1", Port: 1}}, CA: "ca.pem"},
	}
	for name, config := range cases {
		_, err := Connect(config)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: Connect() error = %v, want ErrInvalidArgument", name, err)
		}
	}
}

// ViaProxy pointed at an address that turns out to be a cache node is a
// configuration mistake, like the option checks above (issue #1454).
func TestViaProxyAgainstANodeAddressIsInvalidArgument(t *testing.T) {
	node := startMockNode(t, nil)
	_, err := Connect(Config{Addresses: []Address{addr(node.address())}, ViaProxy: true})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Connect() error = %v, want ErrInvalidArgument", err)
	}
}

// A discovery server that answers but lists nothing to connect to is a
// protocol-level failure, not an untyped one (issue #1454).
func TestAnEmptyDiscoveryRosterIsAProtocolError(t *testing.T) {
	discovery := startMockDiscovery(t, nil, 1)

	_, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("no nodes: Connect() error = %v, want ErrProtocol", err)
	}

	_, err = Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("no proxies: Connect() error = %v, want ErrProtocol", err)
	}
}

// listNodeAtANonNodeAddress returns a connected client whose ring contains
// a node (bad, the primary of key) listed at an address that answers as a
// discovery server, with discovery's roster already corrected so a
// refresh drops it.
func listNodeAtANonNodeAddress(t *testing.T, key string) (client *Client, discovery *mockDiscovery, bad string) {
	t.Helper()
	nodes, discovery := startCluster(t, 1)
	other := ownersOf(key)[1]
	bad = ownersOf(key)[0]
	discovery.setNodes([]discoveredNode{{Name: other, Address: nodes[other].address()}})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	// A second discovery server stands in for the service now at the
	// address; the first roster lists the bad node at it, and a refresh
	// installs that (newly listed nodes are dialed lazily).
	impostor := startMockDiscovery(t, nil, 1)
	discovery.setNodes([]discoveredNode{
		{Name: other, Address: nodes[other].address()},
		{Name: bad, Address: impostor.address()},
	})
	client.refreshNodeList()
	discovery.setNodes([]discoveredNode{{Name: other, Address: nodes[other].address()}})
	return client, discovery, bad
}

// A node that discovery lists at an address which answers as discovery (an
// address another service took over) used to fail every operation routed to
// it with ErrProtocol, which the cluster retry does not refresh on: Set and
// Incr kept failing until the 30 s staleness refresh. It is a lost, unsent
// connection now, so the operation refreshes the node list and reroutes.
func TestANonNodeAddressIsALostUnsentConnectionAndItsCooldownKeepsTheType(t *testing.T) {
	client, _, bad := listNodeAtANonNodeAddress(t, "k")

	for attempt := 1; attempt <= 2; attempt++ { // the second one is the cooldown entry
		_, err := client.slotConnection(bad)
		if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) || errors.Is(err, ErrProtocol) {
			t.Fatalf("attempt %d: slotConnection = %v, want ErrConnectionLost marked not sent", attempt, err)
		}
	}
}

func TestOperationsRefreshAndRerouteWhenTheirPrimaryAnswersAsANonNode(t *testing.T) {
	const key = "k"
	cases := map[string]func(c *Client) error{
		"Set": func(c *Client) error { return c.Set(key, "v", 0) },
		"Incr": func(c *Client) error {
			_, _, err := c.Incr(key, 1)
			return err
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			client, discovery, _ := listNodeAtANonNodeAddress(t, key)
			before := discovery.lCount.Load()
			if err := run(client); err != nil {
				t.Fatalf("%s = %v, want success after a refresh", name, err)
			}
			if got := discovery.lCount.Load() - before; got < 1 {
				t.Fatalf("%s sent %d L requests to discovery, want a forced refresh", name, got)
			}
		})
	}
}
