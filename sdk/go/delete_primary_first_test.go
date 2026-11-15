package nanocached

import (
	"testing"
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
