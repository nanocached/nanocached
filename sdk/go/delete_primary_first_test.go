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

// The fallback deletes are awaited even with FireAndForgetReplicas: when
// Delete returns its error, the replica has already applied the delete.
func TestDeleteWhosePrimaryStaysUnreachableAwaitsTheReplicaDeletesWithFireAndForget(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{FireAndForgetReplicas: true})
	owners := ownersOf("k")
	primary, replica := nodes[owners[0]], nodes[owners[1]]
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return replica.hasKey("k") }, "the replica to store the value")
	primary.close()
	time.Sleep(100 * time.Millisecond) // the client's read loop sees the FIN
	replica.deleteDelay.Store(int64(200 * time.Millisecond))
	if _, err := client.Delete("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Delete err = %v, want ErrConnectionLost", err)
	}
	if replica.hasKey("k") {
		t.Fatal("Delete returned before the replica applied the fallback delete")
	}
}

// A dead replica among the fallback deletes is counted as a replica write
// failure; the caller still gets the primary's error.
func TestAFailedFallbackDeleteCountsAReplicaWriteFailure(t *testing.T) {
	pc := startPrimaryFirstCluster(t, "k")
	if err := pc.client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	pc.primary.close()
	pc.replica.close()
	time.Sleep(100 * time.Millisecond)
	if _, err := pc.client.Delete("k"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Delete err = %v, want ErrConnectionLost", err)
	}
	if got := pc.client.Stats().ReplicaWriteFailures; got == 0 {
		t.Fatal("ReplicaWriteFailures = 0, want the failed fallback delete counted")
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
