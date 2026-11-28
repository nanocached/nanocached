package nanocached

import (
	"errors"
	"testing"
	"time"
)

// A touch with ttlSeconds == 0 reaches the primary alone first and the other
// owners only after it answered (issue #934): a plain `t ... 0` expires the
// copy it finds, so a replica touched before the primary failed or answered
// `W` would have nothing left for the reroute to return.

// primaryFirstCluster is a two-node, two-copy cluster plus a client on it,
// and the primary/replica nodes of key.
type primaryFirstCluster struct {
	nodes     map[string]*mockNode
	discovery *mockDiscovery
	client    *Client
	primary   *mockNode
	replica   *mockNode
	// replicaName is the roster entry of the replica, for shrinking the
	// roster to it; primaryName the primary's.
	replicaName string
	primaryName string
}

func startPrimaryFirstCluster(t *testing.T, key string) *primaryFirstCluster {
	t.Helper()
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	owners := ownersOf(key)
	return &primaryFirstCluster{
		nodes: nodes, discovery: discovery, client: client,
		primary: nodes[owners[0]], replica: nodes[owners[1]], replicaName: owners[1],
		primaryName: owners[0],
	}
}

// killPrimaryAndShrinkTheRosterToTheReplica makes the primary unreachable and
// tells discovery it is gone, so the refresh a failed first pass triggers
// promotes the replica to primary.
func (pc *primaryFirstCluster) killPrimaryAndShrinkTheRosterToTheReplica(t *testing.T) {
	t.Helper()
	pc.primary.close()
	time.Sleep(100 * time.Millisecond) // the client's read loop sees the FIN
	pc.discovery.setNodes([]discoveredNode{{Name: pc.replicaName, Address: pc.replica.address()}})
}

// killPrimaryButKeepItListed makes the primary unreachable while discovery
// still lists it, and waits for the client to see its connection to the
// primary close: a request written to a socket the client still thinks is
// open would count as sent (its reply lost), not as never sent.
func (pc *primaryFirstCluster) killPrimaryButKeepItListed(t *testing.T) {
	t.Helper()
	pc.primary.close()
	waitFor(t, func() bool { return pc.client.memberConnectionClosed(pc.primaryName) },
		"the client to see its connection to the primary close")
}

// dropThePrimaryFromTheRoster tells discovery the dead primary is gone and
// waits out the forced-refresh spacing, so the next failure's refresh
// learns the new roster and promotes the replica.
func (pc *primaryFirstCluster) dropThePrimaryFromTheRoster() {
	pc.discovery.setNodes([]discoveredNode{{Name: pc.replicaName, Address: pc.replica.address()}})
	time.Sleep(forcedRefreshMinInterval + 50*time.Millisecond)
}

// memberConnectionClosed reports whether the client's connection to the
// cluster member name is closed (or absent).
func (c *Client) memberConnectionClosed(name string) bool {
	c.mu.Lock()
	m := c.members[name]
	c.mu.Unlock()
	return m == nil || m.connection == nil || m.connection.isClosed()
}

type expiringSingleCall struct {
	name string
	// do touches key with ttl and returns whether it found the key, the
	// value it returned (GetAndTouch only) and the error.
	do func(c *Client, key string, ttl int64) (found bool, value string, err error)
}

var expiringSingleCalls = []expiringSingleCall{
	{"Touch", func(c *Client, key string, ttl int64) (bool, string, error) {
		existed, err := c.Touch(key, ttl)
		return existed, "", err
	}},
	{"GetAndTouch", func(c *Client, key string, ttl int64) (bool, string, error) {
		value, ok, err := c.GetAndTouch(key, ttl)
		return ok, value, err
	}},
}

func TestExpiringTouchSendsNoReplicaLegBeforeThePrimaryAnswers(t *testing.T) {
	for _, call := range expiringSingleCalls {
		t.Run(call.name, func(t *testing.T) {
			pc := startPrimaryFirstCluster(t, "k")
			if err := pc.client.Set("k", "v", 0); err != nil {
				t.Fatal(err)
			}
			pc.primary.touchDelay.Store(int64(400 * time.Millisecond))
			type outcome struct {
				found bool
				value string
				err   error
			}
			done := make(chan outcome, 1)
			go func() {
				found, value, err := call.do(pc.client, "k", 0)
				done <- outcome{found, value, err}
			}()
			waitFor(t, func() bool { return len(pc.primary.touchesSeen()) == 1 }, "the primary to receive the touch")
			time.Sleep(150 * time.Millisecond)
			if got := pc.replica.touchesSeen(); len(got) != 0 {
				t.Fatalf("replica saw %v before the primary answered, want nothing", got)
			}
			res := <-done
			if res.err != nil || !res.found || (call.name == "GetAndTouch" && res.value != "v") {
				t.Fatalf("%s = %v, %q, %v, want found, nil", call.name, res.found, res.value, res.err)
			}
			if got := pc.replica.touchesSeen(); len(got) != 1 || got[0].ttl != 0 || got[0].get {
				t.Fatalf("replica touches after the answer = %v, want one plain touch with ttl 0", got)
			}
		})
	}
}

func TestPositiveTTLTouchStillReachesTheReplicaWithoutWaitingForThePrimary(t *testing.T) {
	pc := startPrimaryFirstCluster(t, "k")
	if err := pc.client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	pc.primary.touchDelay.Store(int64(500 * time.Millisecond))
	done := make(chan error, 1)
	go func() {
		_, err := pc.client.Touch("k", 30)
		done <- err
	}()
	waitFor(t, func() bool { return len(pc.replica.touchesSeen()) == 1 }, "the replica to be touched while the primary is still busy")
	select {
	case err := <-done:
		t.Fatalf("Touch returned (%v) before the delayed primary answered", err)
	default:
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestExpiringTouchOfAWrongNodePrimaryTouchesNoReplica(t *testing.T) {
	for _, call := range expiringSingleCalls {
		t.Run(call.name+" W on both passes", func(t *testing.T) {
			pc := startPrimaryFirstCluster(t, "k")
			if err := pc.client.Set("k", "v", 0); err != nil {
				t.Fatal(err)
			}
			pc.primary.wrongNodeLeft.Store(2)
			if _, _, err := call.do(pc.client, "k", 0); !errors.Is(err, ErrWrongNode) {
				t.Fatalf("%s err = %v, want ErrWrongNode", call.name, err)
			}
			if got := pc.replica.touchesSeen(); len(got) != 0 {
				t.Fatalf("replica saw %v although the primary never answered, want nothing", got)
			}
			if _, ok := pc.replica.store.Load(storeKey{"", "k"}); !ok {
				t.Fatal("the replica's copy was expired")
			}
		})
		t.Run(call.name+" W once, then the reroute succeeds", func(t *testing.T) {
			pc := startPrimaryFirstCluster(t, "k")
			if err := pc.client.Set("k", "v", 0); err != nil {
				t.Fatal(err)
			}
			pc.primary.wrongNodeLeft.Store(1)
			found, value, err := call.do(pc.client, "k", 0)
			if err != nil || !found || (call.name == "GetAndTouch" && value != "v") {
				t.Fatalf("%s after a W = %v, %q, %v, want found with the value", call.name, found, value, err)
			}
			// The one replica touch followed the primary's definitive answer.
			if got := pc.replica.touchesSeen(); len(got) != 1 {
				t.Fatalf("replica touches = %v, want exactly one", got)
			}
		})
	}
}

func TestExpiringTouchRerouteAfterADeadPrimaryFindsTheValueOnTheReplica(t *testing.T) {
	for _, call := range expiringSingleCalls {
		t.Run(call.name, func(t *testing.T) {
			pc := startPrimaryFirstCluster(t, "k")
			if err := pc.client.Set("k", "v", 0); err != nil {
				t.Fatal(err)
			}
			pc.killPrimaryAndShrinkTheRosterToTheReplica(t)
			found, value, err := call.do(pc.client, "k", 0)
			if err != nil || !found || (call.name == "GetAndTouch" && value != "v") {
				t.Fatalf("%s after the primary died = %v, %q, %v, want found with the value (not a miss)",
					call.name, found, value, err)
			}
			// Only the reroute touched the replica, and as the new primary.
			got := pc.replica.touchesSeen()
			if len(got) != 1 || got[0].get != (call.name == "GetAndTouch") {
				t.Fatalf("replica touches = %v, want exactly the rerouted primary leg", got)
			}
		})
	}
}

// Issue #2057: the primary applied the expiring touch and the reply was
// lost, which is not resent (issue #901). The replica is still sent its
// plain `t ... 0` before the error is returned, so it does not keep the
// value the caller expired.
func TestExpiringTouchWhosePrimaryReplyWasLostStillExpiresTheReplica(t *testing.T) {
	for _, call := range expiringSingleCalls {
		t.Run(call.name, func(t *testing.T) {
			pc := startPrimaryFirstCluster(t, "k")
			if err := pc.client.Set("k", "v", 0); err != nil {
				t.Fatal(err)
			}
			pc.primary.applyThenCloseTouchTimes(1)
			if _, _, err := call.do(pc.client, "k", 0); !errors.Is(err, ErrConnectionLost) {
				t.Fatalf("%s err = %v, want ErrConnectionLost", call.name, err)
			}
			got := pc.replica.touchesSeen()
			if len(got) != 1 || got[0].ttl != 0 || got[0].get {
				t.Fatalf("replica touches = %v, want exactly one plain t ... 0", got)
			}
			if pc.replica.hasKey("k") {
				t.Fatal("the replica still holds the value")
			}
		})
	}
}

// The primary is dead but still in the roster, so the retry's dial is
// refused too and the touch never left. No other owner is touched and the
// error is the not-sent kind, so the resend it invites, once discovery has
// dropped the primary, still finds the value on the replica.
func TestExpiringTouchWhosePrimaryStaysUnreachableLeavesTheReplica(t *testing.T) {
	for _, call := range expiringSingleCalls {
		t.Run(call.name, func(t *testing.T) {
			pc := startPrimaryFirstCluster(t, "k")
			if err := pc.client.Set("k", "v", 0); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return pc.replica.hasKey("k") }, "the replica to store the value")
			pc.killPrimaryButKeepItListed(t)
			_, _, err := call.do(pc.client, "k", 0)
			if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) {
				t.Fatalf("%s err = %v, want ErrConnectionLost marked not sent", call.name, err)
			}
			if got := pc.replica.touchesSeen(); len(got) != 0 {
				t.Fatalf("replica touches = %v although the primary was never sent the touch, want none", got)
			}
			if !pc.replica.hasKey("k") {
				t.Fatal("the replica's copy was expired")
			}

			pc.dropThePrimaryFromTheRoster()
			found, value, err := call.do(pc.client, "k", 0)
			if err != nil || !found || (call.name == "GetAndTouch" && value != "v") {
				t.Fatalf("%s resent after the primary left the roster = %v, %q, %v, want found with the value",
					call.name, found, value, err)
			}
			if got := pc.replica.touchesSeen(); len(got) != 1 || got[0].get != (call.name == "GetAndTouch") {
				t.Fatalf("replica touches = %v, want exactly the resent primary leg", got)
			}
			if pc.replica.hasKey("k") {
				t.Fatal("the replica still holds the value after the resend")
			}
		})
	}
}

// The fallback touch is awaited even with FireAndForgetReplicas: when the
// call returns its error, the replica has already applied it.
func TestExpiringTouchAwaitsItsFallbackReplicaTouchWithFireAndForget(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{FireAndForgetReplicas: true})
	owners := ownersOf("k")
	primary, replica := nodes[owners[0]], nodes[owners[1]]
	if err := client.Set("k", "v", 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return replica.hasKey("k") }, "the replica to store the value")
	primary.applyThenCloseTouchTimes(1) // sent, and the reply lost
	replica.touchDelay.Store(int64(200 * time.Millisecond))
	if _, err := client.Touch("k", 0); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Touch err = %v, want ErrConnectionLost", err)
	}
	if replica.hasKey("k") {
		t.Fatal("Touch returned before the replica applied the fallback touch")
	}
}

// ── batch ────────────────────────────────────────────────────────────

type expiringBatchCall struct {
	name string
	do   func(c *Client, keys []string, ttl int64) (found map[string]bool, values map[string]string, err error)
}

var expiringBatchCalls = []expiringBatchCall{
	{"TouchMany", func(c *Client, keys []string, ttl int64) (map[string]bool, map[string]string, error) {
		found, err := c.TouchMany(keys, ttl)
		return found, nil, err
	}},
	{"GetManyAndTouch", func(c *Client, keys []string, ttl int64) (map[string]bool, map[string]string, error) {
		values, err := c.GetManyAndTouch(keys, ttl)
		found := make(map[string]bool, len(values))
		for key := range values {
			found[key] = true
		}
		return found, values, err
	}},
}

// batchKeys returns count keys whose primary is the same node, and the
// cluster they live on, filled.
func startPrimaryFirstBatch(t *testing.T, count int) (*primaryFirstCluster, []string) {
	t.Helper()
	keys := keysWithPrimary(testNames[0], count, "pf")
	pc := startPrimaryFirstCluster(t, keys[0])
	if _, err := pc.client.SetMany(valuesFor(keys), 0); err != nil {
		t.Fatal(err)
	}
	return pc, keys
}

func TestExpiringBatchTouchSendsNoReplicaFrameBeforeThePrimaryAnswers(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			pc, keys := startPrimaryFirstBatch(t, 3)
			pc.primary.touchDelay.Store(int64(400 * time.Millisecond))
			type outcome struct {
				found  map[string]bool
				values map[string]string
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				found, values, err := call.do(pc.client, keys, 0)
				done <- outcome{found, values, err}
			}()
			waitFor(t, func() bool { return len(pc.primary.yFramesSeen()) == 1 }, "the primary to receive the batch")
			time.Sleep(150 * time.Millisecond)
			if got := pc.replica.yFramesSeen(); len(got) != 0 {
				t.Fatalf("replica saw %v before the primary answered, want nothing", got)
			}
			res := <-done
			if res.err != nil || len(res.found) != len(keys) {
				t.Fatalf("%s = %v, %v, want all %d keys found", call.name, res.found, res.err, len(keys))
			}
			// One plain fan-out for every key the primary answered.
			got := pc.replica.yFramesSeen()
			if len(got) != 1 || got[0].get || got[0].ttl != 0 || len(got[0].keys) != len(keys) {
				t.Fatalf("replica frames after the answer = %+v, want one plain ttl-0 frame of %d keys", got, len(keys))
			}
			if primaryFrames := pc.primary.yFramesSeen(); len(primaryFrames) != 1 || primaryFrames[0].get != (call.name == "GetManyAndTouch") {
				t.Fatalf("primary frames = %+v, want exactly one", primaryFrames)
			}
		})
	}
}

func TestExpiringBatchTouchLeavesTheReplicaOfAWrongNodeKeyAlone(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			pc, keys := startPrimaryFirstBatch(t, 2)
			wrong, answered := keys[0], keys[1]
			pc.primary.multiWrongNodeKey.Store(wrong)
			pc.primary.multiWrongNodeLeft.Store(2) // both passes
			found, _, err := call.do(pc.client, keys, 0)
			if !errors.Is(err, ErrWrongNode) || len(found) != 1 || !found[answered] {
				t.Fatalf("%s = %v, %v, want only %s and ErrWrongNode", call.name, found, err, answered)
			}
			got := pc.replica.yFramesSeen()
			if len(got) != 1 || len(got[0].keys) != 1 || got[0].keys[0] != answered {
				t.Fatalf("replica frames = %+v, want one frame holding only %s", got, answered)
			}
			if _, ok := pc.replica.store.Load(storeKey{"", wrong}); !ok {
				t.Fatalf("the replica's copy of %s was expired", wrong)
			}
		})
	}
}

func TestExpiringBatchTouchRerouteAfterAWrongNodeAnswerIsOneFanOut(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			pc, keys := startPrimaryFirstBatch(t, 2)
			pc.primary.multiWrongNodeKey.Store(keys[0])
			pc.primary.multiWrongNodeLeft.Store(1)
			found, values, err := call.do(pc.client, keys, 0)
			if err != nil || len(found) != len(keys) {
				t.Fatalf("%s after a W = %v, %v, want every key found", call.name, found, err)
			}
			if call.name == "GetManyAndTouch" && (values[keys[0]] != "v-"+keys[0] || values[keys[1]] != "v-"+keys[1]) {
				t.Fatalf("values = %v, want both values", values)
			}
			// Both keys' primary answered over the two passes; the replica is
			// touched once, for both.
			got := pc.replica.yFramesSeen()
			if len(got) != 1 || len(got[0].keys) != len(keys) {
				t.Fatalf("replica frames = %+v, want one frame of %d keys", got, len(keys))
			}
		})
	}
}

func TestExpiringBatchTouchRerouteAfterADeadPrimaryFindsTheValuesOnTheReplica(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			pc, keys := startPrimaryFirstBatch(t, 3)
			pc.killPrimaryAndShrinkTheRosterToTheReplica(t)
			found, values, err := call.do(pc.client, keys, 0)
			if err != nil || len(found) != len(keys) {
				t.Fatalf("%s after the primary died = %v, %v, want every key found (not a miss)", call.name, found, err)
			}
			for _, key := range keys {
				if !found[key] || (call.name == "GetManyAndTouch" && values[key] != "v-"+key) {
					t.Fatalf("%s: key %s = found %v value %q", call.name, key, found[key], values[key])
				}
			}
			// Only the reroute touched the replica, as the new primary.
			got := pc.replica.yFramesSeen()
			if len(got) != 1 || got[0].get != (call.name == "GetManyAndTouch") {
				t.Fatalf("replica frames = %+v, want exactly the rerouted primary frame", got)
			}
		})
	}
}

// Issue #2057, as for a single key: the keys whose primary lost the reply
// are still sent to the replica, in one plain frame.
func TestExpiringBatchTouchWhosePrimaryReplyWasLostStillExpiresTheReplica(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			pc, keys := startPrimaryFirstBatch(t, 2)
			pc.primary.applyThenCloseTouchTimes(1)
			if _, _, err := call.do(pc.client, keys, 0); !errors.Is(err, ErrConnectionLost) {
				t.Fatalf("%s err = %v, want ErrConnectionLost", call.name, err)
			}
			got := pc.replica.yFramesSeen()
			if len(got) != 1 || len(got[0].keys) != 2 || got[0].ttl != 0 || got[0].get {
				t.Fatalf("replica frames = %+v, want one plain y ... 0 holding both keys", got)
			}
			for _, key := range keys {
				if pc.replica.hasKey(key) {
					t.Fatalf("the replica still holds %q", key)
				}
			}
		})
	}
}

// The other owners of a key whose primary reply was lost are the owners on
// the ring in force minus the node that lost it, by name. Here the primary's
// first chunk is applied and its reply lost, the second chunk never leaves,
// and the refresh before the second pass drops the primary from the roster:
// the replica is now the keys' primary, and it must still be sent the lost
// chunk's expiring touch, or it keeps serving values the caller expired.
func TestExpiringBatchTouchExpiresTheNewPrimaryWhenTheRefreshDroppedTheNodeThatLostTheReply(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			keys := keysWithPrimary(testNames[0], 2*maxBatchKeys, "drop")
			pc := startPrimaryFirstCluster(t, keys[0])
			if _, err := pc.client.SetMany(valuesFor(keys), 0); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return pc.replica.hasKey(keys[0]) && pc.replica.hasKey(keys[len(keys)-1]) },
				"the replica to store the values")
			// Discovery no longer lists the primary; the client learns it on
			// the forced refresh the unresolved second chunk triggers.
			pc.discovery.setNodes([]discoveredNode{{Name: pc.replicaName, Address: pc.replica.address()}})
			time.Sleep(forcedRefreshMinInterval + 50*time.Millisecond)
			pc.primary.applyThenCloseTouchTimes(1)

			if _, _, err := call.do(pc.client, keys, 0); !errors.Is(err, ErrConnectionLost) {
				t.Fatalf("%s err = %v, want ErrConnectionLost", call.name, err)
			}
			if owners := pc.client.ownerNames(nil, []byte(keys[0])); len(owners) != 1 || owners[0] != pc.replicaName {
				t.Fatalf("owners after the call = %v, want the refresh to leave only the replica %s", owners, pc.replicaName)
			}
			left := 0
			for _, key := range keys {
				if pc.replica.hasKey(key) {
					left++
				}
			}
			if left != 0 {
				t.Fatalf("the replica, now the primary, still holds %d of %d keys; frames = %d", left, len(keys), len(pc.replica.yFramesSeen()))
			}
		})
	}
}

// As for a single key: the keys whose primary stayed unreachable on both
// passes never left, so the replica is sent nothing, the error is the
// not-sent kind, and the resend after discovery dropped the primary still
// finds every value.
func TestExpiringBatchTouchWhosePrimaryStaysUnreachableLeavesTheReplica(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			pc, keys := startPrimaryFirstBatch(t, 2)
			waitFor(t, func() bool { return pc.replica.hasKey(keys[0]) && pc.replica.hasKey(keys[1]) },
				"the replica to store the values")
			pc.killPrimaryButKeepItListed(t)
			_, _, err := call.do(pc.client, keys, 0)
			if !errors.Is(err, ErrConnectionLost) || !errors.Is(err, errRequestNotSent) {
				t.Fatalf("%s err = %v, want ErrConnectionLost marked not sent", call.name, err)
			}
			if got := pc.replica.yFramesSeen(); len(got) != 0 {
				t.Fatalf("replica frames = %+v although the primary was never sent the batch, want none", got)
			}
			for _, key := range keys {
				if !pc.replica.hasKey(key) {
					t.Fatalf("the replica's copy of %q was expired", key)
				}
			}

			pc.dropThePrimaryFromTheRoster()
			found, values, err := call.do(pc.client, keys, 0)
			if err != nil || len(found) != len(keys) {
				t.Fatalf("%s resent after the primary left the roster = %v, %v, want every key found", call.name, found, err)
			}
			for _, key := range keys {
				if !found[key] {
					t.Fatalf("%s: %s not found after the resend", call.name, key)
				}
				if call.name == "GetManyAndTouch" && values[key] != "v-"+key {
					t.Fatalf("%s: value of %s = %q, want %q", call.name, key, values[key], "v-"+key)
				}
				if pc.replica.hasKey(key) {
					t.Fatalf("the replica still holds %q after the resend", key)
				}
			}
		})
	}
}

// The rule is per key: in one batch, the keys whose primary was sent the
// frame and lost the reply have their other owners expired, while the keys
// whose primary never got it touch no other owner. The batch's error is the
// sent-and-lost one (not marked never sent), since some keys may have been
// expired.
func TestExpiringBatchTouchExpiresOnlyTheReplicasOfTheKeysWhosePrimaryWasSent(t *testing.T) {
	for _, call := range expiringBatchCalls {
		t.Run(call.name, func(t *testing.T) {
			nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
			client := connectCluster(t, discovery, Config{})
			lostKeys := keysWithPrimary(testNames[0], 2, "lost")
			neverSentKeys := keysWithPrimary(testNames[1], 2, "unsent")
			lostPrimary, deadPrimary := nodes[testNames[0]], nodes[testNames[1]]
			keys := append(append([]string{}, lostKeys...), neverSentKeys...)
			if _, err := client.SetMany(valuesFor(keys), 0); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return lostPrimary.hasKey(neverSentKeys[0]) && lostPrimary.hasKey(neverSentKeys[1]) },
				"the replica copies of the never-sent keys")
			deadPrimary.close()
			waitFor(t, func() bool { return client.memberConnectionClosed(testNames[1]) },
				"the client to see its connection to the dead primary close")
			lostPrimary.applyThenCloseTouchTimes(1)

			_, _, err := call.do(client, keys, 0)
			if !errors.Is(err, ErrConnectionLost) || errors.Is(err, errRequestNotSent) {
				t.Fatalf("%s err = %v, want ErrConnectionLost not marked never sent", call.name, err)
			}
			for _, frame := range lostPrimary.yFramesSeen() {
				for _, key := range frame.keys {
					if key == neverSentKeys[0] || key == neverSentKeys[1] {
						t.Fatalf("frames on the never-sent keys' replica = %+v, want none holding %v",
							lostPrimary.yFramesSeen(), neverSentKeys)
					}
				}
			}
			for _, key := range neverSentKeys {
				if !lostPrimary.hasKey(key) {
					t.Fatalf("the replica copy of never-sent key %q was expired", key)
				}
			}
		})
	}
}

// The fallback frame is awaited even with FireAndForgetReplicas.
func TestExpiringBatchTouchAwaitsItsFallbackReplicaFrameWithFireAndForget(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{FireAndForgetReplicas: true})
	keys := keysWithPrimary(testNames[0], 2, "pf")
	primary, replica := nodes[testNames[0]], nodes[testNames[1]]
	if _, err := client.SetMany(valuesFor(keys), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return replica.hasKey(keys[0]) && replica.hasKey(keys[1]) },
		"the replica to store the values")
	primary.applyThenCloseTouchTimes(1) // sent, and the reply lost
	replica.touchDelay.Store(int64(200 * time.Millisecond))
	if _, err := client.TouchMany(keys, 0); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("TouchMany err = %v, want ErrConnectionLost", err)
	}
	if replica.hasKey(keys[0]) || replica.hasKey(keys[1]) {
		t.Fatal("TouchMany returned before the replica applied the fallback frame")
	}
}

// A key the primary answered `N` for is still sent to the replica: the
// caller's intent is to expire it everywhere, and a fire-and-forget copy may
// sit there.
func TestExpiringBatchTouchSendsTheReplicaTheKeysItsPrimaryMissed(t *testing.T) {
	pc, keys := startPrimaryFirstBatch(t, 2)
	pc.primary.store.Delete(storeKey{"", keys[0]})
	found, err := pc.client.TouchMany(keys, 0)
	if err != nil || found[keys[0]] || !found[keys[1]] {
		t.Fatalf("TouchMany = %v, %v, want the first key missing and the second found", found, err)
	}
	got := pc.replica.yFramesSeen()
	if len(got) != 1 || len(got[0].keys) != 2 {
		t.Fatalf("replica frames = %+v, want one frame holding both keys", got)
	}
	if _, ok := pc.replica.store.Load(storeKey{"", keys[0]}); ok {
		t.Fatal("the replica's copy of the key the primary missed was not expired")
	}
}
