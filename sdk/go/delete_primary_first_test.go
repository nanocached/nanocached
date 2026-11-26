package nanocached

import (
	"errors"
	"testing"
	"time"
)

// A delete reaches the other owners only after the primary answered, as a
// touch with ttlSeconds == 0 (issue #934): a primary that answered `W` or was
// unreachable is rerouted with no replica deleted yet, so the reroute does not
// report false from a copy this call already removed.

func TestDeleteOfAWrongNodePrimaryDeletesNoReplicaAndTheRerouteReportsItExisted(t *testing.T) {
	pc := startPrimaryFirstCluster(t, "k")
	if err := pc.client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	pc.primary.wrongNodeLeft.Store(1)
	existed, err := pc.client.Delete("k")
	if err != nil || !existed {
		t.Fatalf("Delete after a W = %v, %v, want true (the key existed)", existed, err)
	}
	// The one replica delete followed the primary's definitive answer.
	if got := pc.replica.deletesReceived.Load(); got != 1 {
		t.Fatalf("replica deletes = %d, want exactly 1", got)
	}
	if _, ok := pc.replica.store.Load(storeKey{"", "k"}); ok {
		t.Fatal("the replica's copy is still there")
	}
}

func TestDeleteRerouteAfterADeadPrimaryReportsThatTheKeyExisted(t *testing.T) {
	pc := startPrimaryFirstCluster(t, "k")
	if err := pc.client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	pc.killPrimaryAndShrinkTheRosterToTheReplica(t)
	existed, err := pc.client.Delete("k")
	if err != nil || !existed {
		t.Fatalf("Delete after the primary died = %v, %v, want true (not false from a replica deleted first)", existed, err)
	}
	// Only the reroute reached the replica, as the new primary.
	if got := pc.replica.deletesReceived.Load(); got != 1 {
		t.Fatalf("replica deletes = %d, want exactly the rerouted primary leg", got)
	}
}

// A primary that is dead but still in the roster (discovery has not dropped
// it yet) fails the delete even after the refresh. The other owners are then
// sent the delete before the error is returned, so a read that falls back to
// a replica does not find the deleted value.
func TestDeleteWhosePrimaryStaysUnreachableStillDeletesTheReplicas(t *testing.T) {
	pc := startPrimaryFirstCluster(t, "k")
	if err := pc.client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	pc.primary.close()
	time.Sleep(100 * time.Millisecond) // the client's read loop sees the FIN
	_, err := pc.client.Delete("k")
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Delete with an unreachable primary err = %v, want ErrConnectionLost", err)
	}
	if got := pc.replica.deletesReceived.Load(); got != 1 {
		t.Fatalf("replica deletes = %d, want 1", got)
	}
	if _, ok := pc.replica.store.Load(storeKey{"", "k"}); ok {
		t.Fatal("the replica still holds the value")
	}
	if value, ok, err := pc.client.Get("k"); err != nil || ok {
		t.Fatalf("Get after the failed delete = %q, %v, %v, want a miss", value, ok, err)
	}
}

// A primary that answers `W` on every attempt is not a lost connection: the
// replicas are left alone, as before.
func TestDeleteWhosePrimaryKeepsAnsweringWrongNodeDeletesNoReplica(t *testing.T) {
	pc := startPrimaryFirstCluster(t, "k")
	if err := pc.client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	pc.primary.wrongNodeLeft.Store(2)
	if _, err := pc.client.Delete("k"); !errors.Is(err, ErrWrongNode) {
		t.Fatalf("Delete err = %v, want ErrWrongNode", err)
	}
	if got := pc.replica.deletesReceived.Load(); got != 0 {
		t.Fatalf("replica deletes = %d, want 0", got)
	}
}
