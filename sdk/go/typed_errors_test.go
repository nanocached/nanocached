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
