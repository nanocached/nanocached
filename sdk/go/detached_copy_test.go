package nanocached

import (
	"fmt"
	"sync"
	"testing"
)

// A replica leg that outlives its call (FireAndForgetReplicas) must send the
// bytes the call was made with. The leg is held back by taking its
// connection's writeMu, the call returns, the caller reuses its buffer, and
// only then is the leg let go. Compression is off, so the value the leg sends
// is the caller's own slice unless the SDK copied it when it started the leg.

// holdReplica makes the replica's connection busy, as if it were writing
// something else, and returns the function that lets queued writes through.
func holdReplica(t *testing.T, client *Client, replica string) (release func()) {
	t.Helper()
	client.mu.Lock()
	conn := client.members[replica].connection
	client.mu.Unlock()
	conn.writeMu.Lock()
	return conn.writeMu.Unlock
}

func storedValue(t *testing.T, node *mockNode, key string) string {
	t.Helper()
	v, ok := node.store.Load(storeKey{"", key})
	if !ok {
		t.Fatalf("%q is not stored", key)
	}
	return string(v.([]byte))
}

// connectFireAndForget returns the client, the nodes and a Close that is safe
// to call more than once (the test closes it to wait for the background legs;
// the cleanup closes it again if the test failed before that).
func connectFireAndForget(t *testing.T) (*Client, map[string]*mockNode, func()) {
	t.Helper()
	nodes, discovery := startCluster(t, 2)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}, FireAndForgetReplicas: true})
	if err != nil {
		t.Fatal(err)
	}
	closeClient := sync.OnceFunc(func() { client.Close() })
	t.Cleanup(closeClient)
	return client, nodes, closeClient
}

func TestDetachedReplicaLegsDoNotReadTheCallersValueAfterTheCallReturns(t *testing.T) {
	const key = "k"
	owners := ownersOf(key)

	t.Run("SetBytes", func(t *testing.T) {
		client, nodes, closeClient := connectFireAndForget(t)
		release := holdReplica(t, client, owners[1])
		buf := []byte("AAAAAAAA")
		if err := client.SetBytes(key, buf, 0); err != nil {
			release()
			t.Fatal(err)
		}
		copy(buf, "BBBBBBBB") // the caller reuses its buffer once SetBytes has returned
		release()
		closeClient() // waits for the background legs
		if got := storedValue(t, nodes[owners[1]], key); got != "AAAAAAAA" {
			t.Fatalf("replica stored %q, want AAAAAAAA", got)
		}
		if got := storedValue(t, nodes[owners[0]], key); got != "AAAAAAAA" {
			t.Fatalf("primary stored %q, want AAAAAAAA", got)
		}
	})

	t.Run("PutIfAbsent", func(t *testing.T) {
		client, nodes, closeClient := connectFireAndForget(t)
		release := holdReplica(t, client, owners[1])
		buf := []byte("AAAAAAAA")
		if stored, err := client.PutIfAbsent(key, buf, 0); err != nil || !stored {
			release()
			t.Fatalf("PutIfAbsent = %v, %v", stored, err)
		}
		copy(buf, "BBBBBBBB")
		release()
		closeClient()
		if got := storedValue(t, nodes[owners[1]], key); got != "AAAAAAAA" {
			t.Fatalf("replica stored %q, want AAAAAAAA", got)
		}
	})

	t.Run("SetManyBytes", func(t *testing.T) {
		client, nodes, closeClient := connectFireAndForget(t)
		// Keys that all have the same primary, so the other node's leg
		// holds only replica copies and is the one that runs detached.
		primary, replica := owners[0], owners[1]
		values := map[string][]byte{}
		for i := 0; len(values) < 3; i++ {
			k := fmt.Sprintf("many-%d", i)
			if o := ownersOf(k); o[0] == primary && o[1] == replica {
				values[k] = []byte(fmt.Sprintf("AAAA-%d", len(values)))
			}
		}
		want := map[string]string{}
		for k, v := range values {
			want[k] = string(v)
		}
		release := holdReplica(t, client, replica)
		if _, err := client.SetManyBytes(values, 0); err != nil {
			release()
			t.Fatal(err)
		}
		for _, v := range values {
			copy(v, "BBBBBB")
		}
		release()
		closeClient()
		for k, w := range want {
			if got := storedValue(t, nodes[replica], k); got != w {
				t.Fatalf("replica stored %q under %q, want %q", got, k, w)
			}
		}
	})
}
