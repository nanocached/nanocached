package nanocached

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// startHandshakeThenSilentNode is a node whose first connection completes
// the `A` handshake and then never answers a request, and whose every later
// connection never completes the handshake at all — a dial to it can only
// end at the 5 s connect deadline.
func startHandshakeThenSilentNode(t *testing.T) (address string, firstFrame <-chan struct{}) {
	t.Helper()
	listener := listenLoopback(t)
	t.Cleanup(func() { _ = listener.Close() })
	got := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	accepted := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			first := accepted == 0
			accepted++
			mu.Unlock()
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				header, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if first {
					parts := strings.Split(strings.TrimSuffix(header, "\n"), " ")
					mustRead(reader, atoiOrPanic(parts[1]))
					if _, err := conn.Write([]byte("On\n")); err != nil {
						return
					}
					if _, err := reader.ReadString('\n'); err == nil {
						once.Do(func() { close(got) })
					}
				}
				// Hold the connection open and say nothing until the peer
				// closes it.
				_, _ = io.Copy(io.Discard, reader)
			}()
		}
	}()
	return listener.Addr().String(), got
}

// A request whose connection was poisoned by Close() is redialed by
// applyReconnecting; slotConnection did not check the closed flag before
// that dial, so a node that never completes the handshake held the request
// for the whole connect deadline (5 s) and it then failed with
// ErrConnectionLost instead of ErrClosed.
func TestARequestRetriedAfterCloseFailsAsClosedInsteadOfRedialing(t *testing.T) {
	address, firstFrame := startHandshakeThenSilentNode(t)
	client, err := Connect(Config{Addresses: []Address{addr(address)}})
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, _, err := client.Get("k")
		result <- err
	}()
	select {
	case <-firstFrame:
	case <-time.After(10 * time.Second):
		t.Fatal("the node never saw the Get")
	}

	client.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Get after Close = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Get still waiting 3 s after Close: it redialed the node instead of failing as closed")
	}
}
