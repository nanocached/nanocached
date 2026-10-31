package nanocached

import (
	"errors"
	"testing"
	"time"
)

// A request that never obtained a connection wrote nothing, so the
// non-idempotent path must mark that failure not-sent — the cluster layer
// refreshes and reroutes only on that marker — exactly as applyExpiring
// does.

func TestApplyNonIdempotentTreatsAFailedDialAsNotSent(t *testing.T) {
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	dead := testNames[0]
	nodes[dead].close()
	time.Sleep(50 * time.Millisecond)
	err = client.applyNonIdempotent(dead, func(conn *connection) error {
		_, _, _, opErr := conn.incrNS(nil, []byte("k"), 1)
		return opErr
	})
	if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) {
		t.Fatalf("applyNonIdempotent on a dead node = %v, want ErrConnectionLost marked not sent", err)
	}
}

func TestApplyNonIdempotentTreatsAFailedRedialAsNotSent(t *testing.T) {
	// The first attempt is refused before any byte left the process; the
	// redial that follows then fails too. Still nothing was written.
	nodes, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	slot := testNames[0]
	calls := 0
	err = client.applyNonIdempotent(slot, func(conn *connection) error {
		calls++
		nodes[slot].close()
		conn.close()
		return notSent(ErrConnectionLost)
	})
	if calls != 1 {
		t.Fatalf("op ran %d times, want 1 (the redial failed)", calls)
	}
	if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) {
		t.Fatalf("applyNonIdempotent with a failed redial = %v, want ErrConnectionLost marked not sent", err)
	}
}

func TestApplyNonIdempotentDoesNotMarkASentRequestAsNotSent(t *testing.T) {
	// A failure after the frame was written must stay unmarked: the primary
	// may have applied it.
	_, discovery := startCluster(t, 1)
	client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	err = client.applyNonIdempotent(testNames[0], func(conn *connection) error {
		return ErrConnectionLost
	})
	if !errors.Is(err, ErrConnectionLost) || errors.Is(err, errRequestNotSent) {
		t.Fatalf("applyNonIdempotent after a sent request = %v, want ErrConnectionLost not marked", err)
	}
}

// The audit reproduction: two nodes, replication 1; the primary of the key
// dies and discovery has already dropped it. Set reroutes via a forced
// refresh; the non-idempotent operations must too.
func TestNonIdempotentOpsRerouteAroundADeadPrimaryDroppedFromDiscovery(t *testing.T) {
	const key = "k"
	cases := []struct {
		name string
		run  func(t *testing.T, c *Client)
	}{
		{"Incr", func(t *testing.T, c *Client) {
			// The key was never set on the survivor: a miss, not an error.
			if v, ok, err := c.Incr(key, 1); err != nil || ok || v != 0 {
				t.Fatalf("Incr = %d, %v, %v, want 0, false, nil", v, ok, err)
			}
		}},
		{"Decr", func(t *testing.T, c *Client) {
			if v, ok, err := c.Decr(key, 1); err != nil || ok || v != 0 {
				t.Fatalf("Decr = %d, %v, %v, want 0, false, nil", v, ok, err)
			}
		}},
		{"PutIfAbsent", func(t *testing.T, c *Client) {
			if ok, err := c.PutIfAbsent(key, []byte("1"), 0); err != nil || !ok {
				t.Fatalf("PutIfAbsent = %v, %v, want true, nil", ok, err)
			}
			if v, ok, err := c.Get(key); err != nil || !ok || v != "1" {
				t.Fatalf("Get after PutIfAbsent = %q, %v, %v", v, ok, err)
			}
		}},
		{"ReplaceIfPresent", func(t *testing.T, c *Client) {
			if ok, err := c.ReplaceIfPresent(key, []byte("1"), 0); err != nil || ok {
				t.Fatalf("ReplaceIfPresent = %v, %v, want false, nil", ok, err)
			}
		}},
		{"Replace", func(t *testing.T, c *Client) {
			if ok, err := c.Replace(key, CasToken{}, []byte("1"), 0); err != nil {
				t.Fatalf("Replace = %v, %v, want no error", ok, err)
			}
		}},
		{"DeleteIfMatches", func(t *testing.T, c *Client) {
			if ok, err := c.DeleteIfMatches(key, CasToken{}); err != nil {
				t.Fatalf("DeleteIfMatches = %v, %v, want no error", ok, err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes, discovery := startCluster(t, 1)
			client, err := Connect(Config{Addresses: []Address{addr(discovery.address())}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			primary := ownersOf(key)[0]
			other := testNames[0]
			if primary == other {
				other = testNames[1]
			}
			nodes[primary].close()
			discovery.setNodes([]discoveredNode{{Name: other, Address: nodes[other].address()}})
			time.Sleep(100 * time.Millisecond)

			tc.run(t, client)
		})
	}
}
