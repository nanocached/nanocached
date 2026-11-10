package nanocached

import (
	"errors"
	"net"
	"testing"
	"time"
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
	// A second discovery server stands in for the service now at the
	// address.
	return listNodeAtAddress(t, key, startMockDiscovery(t, nil, 1).address())
}

// listNodeAtAddress is listNodeAtANonNodeAddress for any address: the
// primary of key is listed at impostorAddress.
func listNodeAtAddress(t *testing.T, key, impostorAddress string) (client *Client, discovery *mockDiscovery, bad string) {
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

	// The first roster lists the bad node at the impostor, and a refresh
	// installs that (newly listed nodes are dialed lazily).
	discovery.setNodes([]discoveredNode{
		{Name: other, Address: nodes[other].address()},
		{Name: bad, Address: impostorAddress},
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

// startGarbageListener returns the address of a listener that answers
// whatever it is sent with an HTTP 400, the way a web server on a reused
// port does, and then closes the connection.
func startGarbageListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				buf := make([]byte, 64)
				_, _ = conn.Read(buf)
				_, _ = conn.Write([]byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"))
			}()
		}
	}()
	return listener.Addr().String()
}

// An address that answers the handshake with something that is not a
// handshake reply (an HTTP server on a reused port) is the same cure as a
// non-node: the redial fails as a lost connection that was never used, and
// so does the reconnect-cooldown entry it arms.
func TestAGarbageHandshakeIsALostUnsentConnectionAndItsCooldownKeepsTheType(t *testing.T) {
	client, _, bad := listNodeAtAddress(t, "k", startGarbageListener(t))

	for attempt := 1; attempt <= 2; attempt++ { // the second one is the cooldown entry
		_, err := client.slotConnection(bad)
		if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) || errors.Is(err, ErrProtocol) {
			t.Fatalf("attempt %d: slotConnection = %v, want ErrConnectionLost marked not sent", attempt, err)
		}
	}
}

func TestOperationsRefreshAndRerouteWhenTheirPrimaryAnswersTheHandshakeWithGarbage(t *testing.T) {
	const key = "k"
	cases := map[string]func(c *Client) error{
		"Set": func(c *Client) error { return c.Set(key, "v", 0) },
		"Incr": func(c *Client) error {
			_, _, err := c.Incr(key, 1)
			return err
		},
		"Touch ttl 0": func(c *Client) error {
			_, err := c.Touch(key, 0)
			return err
		},
		"TouchMany ttl 0": func(c *Client) error {
			_, err := c.TouchMany([]string{key}, 0)
			return err
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			client, discovery, _ := listNodeAtAddress(t, key, startGarbageListener(t))
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

// A ttl-0 batch whose chunk could not be sent because the dial found
// garbage must not count that chunk as written: only a chunk whose frame
// reached the node may have expired its keys.
func TestATtlZeroTouchChunkWhoseDialFoundGarbageIsNotSent(t *testing.T) {
	client, _, bad := listNodeAtAddress(t, "k", startGarbageListener(t))

	_, resolved, sentEnd, err := client.multiTouchChunked(bad, nil, [][]byte{[]byte("k")}, 0, false, false)
	if err == nil {
		t.Fatal("multiTouchChunked succeeded against a garbage address")
	}
	if resolved != 0 || sentEnd != 0 {
		t.Fatalf("resolved = %d, sentEnd = %d, want 0 and 0: the chunk never left (err %v)", resolved, sentEnd, err)
	}
}

// In proxy mode the redial falls over to a freshly listed proxy; one that
// answers the handshake with garbage is a failure to dial, so a ttl-0 batch
// does not count its chunk as written.
func TestAProxyThatAnswersTheHandshakeWithGarbageLeavesATtlZeroTouchChunkUnsent(t *testing.T) {
	proxy := startMockNode(t, nil)
	discovery := startMockDiscovery(t, nil, 1)
	discovery.setProxies([]discoveredNode{{Name: "proxy-a", Address: proxy.address()}})
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, ViaProxy: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	proxy.dropConnections()
	proxy.close()
	discovery.setProxies([]discoveredNode{{Name: "proxy-b", Address: startGarbageListener(t)}})
	// The chunk must find the connection already closed, so that the attempt
	// starts with a redial: wait for the read loop to see the drop.
	deadline := time.Now().Add(5 * time.Second)
	for !client.single.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("the dropped proxy connection was never noticed")
		}
		time.Sleep(time.Millisecond)
	}

	_, resolved, sentEnd, err := client.multiTouchChunked("", nil, [][]byte{[]byte("k")}, 0, false, false)
	if err == nil {
		t.Fatal("multiTouchChunked succeeded with no working proxy")
	}
	if resolved != 0 || sentEnd != 0 {
		t.Fatalf("resolved = %d, sentEnd = %d, want 0 and 0: the chunk never left (err %v)", resolved, sentEnd, err)
	}
}
