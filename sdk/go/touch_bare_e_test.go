package nanocached

import (
	"errors"
	"net"
	"strings"
	"testing"
)

// Only a node's `En` is "server too old" (issue #939). The bare `E` (or the
// tagged `E <tag>`) nanocached-proxy answers when a request's outcome is
// unknown is a connection lost after the frame was written: a ttl=0 touch is
// not resent, and it is not a protocol error a redial would replay.
func TestABareEToATTLZeroTouchIsAConnectionLossAndTheFrameIsNotResent(t *testing.T) {
	for _, reply := range []string{"E 7\n", "E\n"} {
		for _, call := range []struct {
			name string
			do   func(*Client) error
		}{
			{"Touch", func(c *Client) error { _, err := c.Touch("k", 0); return err }},
			{"GetAndTouch", func(c *Client) error { _, _, err := c.GetAndTouch("k", 0); return err }},
		} {
			t.Run(call.name+" "+strings.TrimSpace(reply), func(t *testing.T) {
				node := startScriptedNode(t, func(index int, conn net.Conn, next func() (scriptedFrame, bool)) {
					if _, ok := next(); !ok {
						return
					}
					_, _ = conn.Write([]byte(reply))
					next() // hold the connection until the client closes it
				})
				client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(client.Close)
				err = call.do(client)
				if !errors.Is(err, ErrConnectionLost) || errors.Is(err, ErrServerTooOld) || errors.Is(err, ErrProtocol) {
					t.Fatalf("err = %v, want ErrConnectionLost only (not ErrServerTooOld, not ErrProtocol)", err)
				}
				if errors.Is(err, errRequestNotSent) {
					t.Fatalf("err = %v, want it marked as sent", err)
				}
				if got := node.framesStarting("t "); got != 1 {
					t.Fatalf("t frames = %d, want exactly 1 (no resend)", got)
				}
				if got := node.connectionCount(); got != 1 {
					t.Fatalf("connections = %d, want 1 (no redial)", got)
				}
			})
		}
	}
}

// The batch form: a bare `E` to a ttl=0 `y` is a connection loss, and the
// frame is not resent.
func TestABareEToATTLZeroBatchTouchIsAConnectionLossAndTheFrameIsNotResent(t *testing.T) {
	for _, reply := range []string{"E 7\n", "E\n"} {
		for _, call := range []struct {
			name string
			do   func(*Client) error
		}{
			{"TouchMany", func(c *Client) error { _, err := c.TouchMany([]string{"a", "b"}, 0); return err }},
			{"GetManyAndTouch", func(c *Client) error { _, err := c.GetManyAndTouch([]string{"a", "b"}, 0); return err }},
		} {
			t.Run(call.name+" "+strings.TrimSpace(reply), func(t *testing.T) {
				node := startScriptedNode(t, func(index int, conn net.Conn, next func() (scriptedFrame, bool)) {
					if _, ok := next(); !ok {
						return
					}
					_, _ = conn.Write([]byte(reply))
					next()
				})
				client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(client.Close)
				err = call.do(client)
				if !errors.Is(err, ErrConnectionLost) || errors.Is(err, ErrServerTooOld) || errors.Is(err, ErrProtocol) {
					t.Fatalf("err = %v, want ErrConnectionLost only", err)
				}
				if got := node.framesStarting("y "); got != 1 {
					t.Fatalf("y frames = %d, want exactly 1 (no resend)", got)
				}
			})
		}
	}
}

// An idempotent request keeps today's replay: the lost reply is resent on a
// fresh connection, for a bare `E` as for any other lost reply.
func TestABareEToAnIdempotentRequestIsReplayedOnAFreshConnection(t *testing.T) {
	for _, reply := range []string{"E 7\n", "E\n"} {
		t.Run("Get "+strings.TrimSpace(reply), func(t *testing.T) {
			node := startScriptedNode(t, func(index int, conn net.Conn, next func() (scriptedFrame, bool)) {
				for {
					if _, ok := next(); !ok {
						return
					}
					if index == 0 {
						_, _ = conn.Write([]byte(reply))
						return
					}
					_, _ = conn.Write([]byte("V 1\nv"))
				}
			})
			client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			if value, ok, err := client.Get("k"); err != nil || !ok || value != "v" {
				t.Fatalf("Get = %q, %v, %v, want v, true, nil (replayed)", value, ok, err)
			}
		})
		// A positive-TTL touch is idempotent too.
		t.Run("Touch(30) "+strings.TrimSpace(reply), func(t *testing.T) {
			node := startScriptedNode(t, func(index int, conn net.Conn, next func() (scriptedFrame, bool)) {
				for {
					if _, ok := next(); !ok {
						return
					}
					if index == 0 {
						_, _ = conn.Write([]byte(reply))
						return
					}
					_, _ = conn.Write([]byte("S\n"))
				}
			})
			client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			if existed, err := client.Touch("k", 30); err != nil || !existed {
				t.Fatalf("Touch(30) = %v, %v, want true, nil (replayed)", existed, err)
			}
			if got := node.framesStarting("t "); got != 2 {
				t.Fatalf("t frames = %d, want 2 (one replay)", got)
			}
		})
	}
}

// `En\n` stays "server too old", with the ErrProtocol it is built on.
func TestAnEnToATTLZeroTouchIsStillServerTooOld(t *testing.T) {
	node := startScriptedNode(t, func(index int, conn net.Conn, next func() (scriptedFrame, bool)) {
		if _, ok := next(); !ok {
			return
		}
		_, _ = conn.Write([]byte("En\n"))
	})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	_, err = client.Touch("k", 0)
	if !errors.Is(err, ErrServerTooOld) || !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v, want ErrServerTooOld and ErrProtocol", err)
	}
	if got := node.framesStarting("t "); got != 1 {
		t.Fatalf("t frames = %d, want 1", got)
	}
}
