package nanocached

import (
	"errors"
	"net"
	"strings"
	"testing"
)

// An `En` poisons the connection, but only the request the node refused
// carries the rejection (issue #940): a plain Get pipelined behind it merely
// lost the connection, so it is redialed and answered.
func TestAnEnRejectsOnlyTheRequestItAnsweredAndTheOthersAreRedialed(t *testing.T) {
	touchRead := make(chan struct{})
	node := startScriptedNode(t, func(index int, conn net.Conn, next func() (scriptedFrame, bool)) {
		if index == 0 {
			if _, ok := next(); !ok { // the touch
				return
			}
			close(touchRead)
			if _, ok := next(); !ok { // the Get pipelined behind it
				return
			}
			_, _ = conn.Write([]byte("En\n"))
			return
		}
		for {
			f, ok := next()
			if !ok {
				return
			}
			if strings.HasPrefix(f.header, "G ") {
				_, _ = conn.Write([]byte("V 1\nv"))
			}
		}
	})
	client, err := Connect(Config{Addresses: []Address{addr(node.address())}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	touchErr := make(chan error, 1)
	go func() {
		_, err := client.Touch("k", 30)
		touchErr <- err
	}()
	<-touchRead
	value, ok, err := client.Get("k")
	if err != nil || !ok || value != "v" {
		t.Fatalf("Get pipelined behind the rejected touch = %q, %v, %v, want v, true, nil (redialed)", value, ok, err)
	}
	if err := <-touchErr; !errors.Is(err, ErrServerTooOld) {
		t.Fatalf("Touch err = %v, want ErrServerTooOld", err)
	}
	if got := node.connectionCount(); got != 2 {
		t.Fatalf("connections = %d, want 2 (one redial for the Get, none for the touch)", got)
	}
	if got := node.framesStarting("t "); got != 1 {
		t.Fatalf("t frames = %d, want 1 (the rejected touch is not resent)", got)
	}
}
