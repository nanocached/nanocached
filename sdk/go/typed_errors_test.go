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
