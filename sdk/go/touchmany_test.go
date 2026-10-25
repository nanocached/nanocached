package nanocached

import (
	"bufio"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// TouchMany / GetManyAndTouch (issue #871).

func TestAppendMultiTouchFrameEveryCombinationOfTTLAndGet(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("bb")}
	cases := []struct {
		name      string
		namespace []byte
		ttl       int64
		get       bool
		tagged    bool
		tag       uint32
		want      string
	}{
		{"no ttl", nil, -1, false, false, 0, "y 0 2 1 2\nabb"},
		{"ttl", nil, 30, false, false, 0, "y 0 2 1 2 30\nabb"},
		{"zero ttl is sent", nil, 0, false, false, 0, "y 0 2 1 2 0\nabb"},
		{"get without ttl", nil, -1, true, false, 0, "y 0 2 1 2 G\nabb"},
		{"get with ttl", nil, 30, true, false, 0, "y 0 2 1 2 30 G\nabb"},
		{"namespace and tag", []byte("users"), 30, true, true, 7, "y 5 2 1 2 30 G 7\nusersabb"},
		{"tag without ttl", nil, -1, false, true, 7, "y 0 2 1 2 7\nabb"},
	}
	for _, tc := range cases {
		if got := string(appendMultiTouchFrame(tc.namespace, keys, tc.ttl, tc.get, tc.tagged, tc.tag)); got != tc.want {
			t.Errorf("%s: appendMultiTouchFrame = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func newReaderOnlyConnection(wire string, tagged bool) *connection {
	return &connection{reader: bufio.NewReader(strings.NewReader(wire)), tagged: tagged}
}

func TestReadAckRosterParsesTheYReplyAndKeepsTheOReplyStrict(t *testing.T) {
	// The roster reader itself, over a hand-fed reader.
	parse := func(wire string, tagged bool, tokens map[string]multiEntry, what string) ([]multiEntry, uint32, error) {
		conn := newReaderOnlyConnection(wire, tagged)
		return conn.readAckRoster(what, tokens)
	}
	y := map[string]multiEntry{"S": {ok: true}, "N": {}, "W": {wrongNode: true}}
	o := map[string]multiEntry{"S": {ok: true}, "W": {wrongNode: true}}

	entries, tag, err := parse("3 S N W\n", false, y, "multi-touch")
	if err != nil || tag != 0 {
		t.Fatalf("parse = %v, tag %d, err %v", entries, tag, err)
	}
	want := []multiEntry{{ok: true}, {}, {wrongNode: true}}
	for i := range want {
		if entries[i].ok != want[i].ok || entries[i].wrongNode != want[i].wrongNode {
			t.Fatalf("entries[%d] = %+v, want %+v", i, entries[i], want[i])
		}
	}
	if _, tag, err := parse("2 N S 9\n", true, y, "multi-touch"); err != nil || tag != 9 {
		t.Fatalf("tagged parse: tag %d, err %v", tag, err)
	}
	for _, bad := range []string{"1 -\n", "3 S S\n", "x S\n"} {
		if _, _, err := parse(bad, false, y, "multi-touch"); !errors.Is(err, ErrProtocol) {
			t.Errorf("parse(%q) err = %v, want ErrProtocol", bad, err)
		}
	}
	// `O` shares the reader: its tokens stay S and W only.
	if _, _, err := parse("1 N\n", false, o, "multi-set"); !errors.Is(err, ErrProtocol) {
		t.Errorf("an O roster with N: err = %v, want ErrProtocol", err)
	}
}

func TestTouchManyReplacesEveryTTLAndReportsWhichKeysExisted(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 10); err != nil {
		t.Fatal(err)
	}
	got, err := client.TouchMany([]string{"a", "missing", "b"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"a": true, "missing": false, "b": true}
	if len(got) != len(want) {
		t.Fatalf("TouchMany = %v, want %v", got, want)
	}
	for key, existed := range want {
		if got[key] != existed {
			t.Fatalf("TouchMany[%q] = %v, want %v", key, got[key], existed)
		}
	}
	for _, key := range []string{"a", "b"} {
		if ttl, ok := node.storedTTL(key); !ok || ttl != 100 {
			t.Fatalf("TTL of %s = %d, %v, want 100, true", key, ttl, ok)
		}
	}
	// A touch never creates a key.
	if _, ok := node.store.Load(storeKey{"", "missing"}); ok {
		t.Fatal("a touch created a key")
	}
	// One plain frame for the whole batch.
	frames := node.yFramesSeen()
	if len(frames) != 1 || frames[0].get || frames[0].ttl != 100 || strings.Join(frames[0].keys, ",") != "a,missing,b" {
		t.Fatalf("frames = %+v, want one plain frame for a,missing,b with ttl 100", frames)
	}
	if got := node.touchesSeen(); len(got) != 0 {
		t.Fatalf("per-key touches = %v, want none", got)
	}
}

func TestTouchManyWithRemoveTTLRemovesItAndZeroExpiresTheKeysAtOnce(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := client.TouchMany([]string{"a", "b"}, RemoveTTL); err != nil {
		t.Fatal(err)
	}
	if _, ok := node.storedTTL("a"); ok {
		t.Fatal("the TTL was not removed")
	}
	if got := node.yFramesSeen()[0].ttl; got != -1 {
		t.Fatalf("ttl on the wire = %d, want the field omitted (-1)", got)
	}
	if got, err := client.TouchMany([]string{"a", "b"}, 0); err != nil || !got["a"] || !got["b"] {
		t.Fatalf("TouchMany(0) = %v, %v, want both true", got, err)
	}
	if values, err := client.GetMany([]string{"a", "b"}); err != nil || len(values) != 0 {
		t.Fatalf("GetMany after TouchMany(0) = %v, %v, want nothing", values, err)
	}
}

func TestGetManyAndTouchReturnsTheHitsAndSetsTheTTL(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 10); err != nil {
		t.Fatal(err)
	}
	values, err := client.GetManyAndTouch([]string{"a", "missing", "b"}, 60)
	if err != nil || len(values) != 2 || values["a"] != "1" || values["b"] != "2" {
		t.Fatalf("GetManyAndTouch = %v, %v, want a=1 b=2", values, err)
	}
	for _, key := range []string{"a", "b"} {
		if ttl, ok := node.storedTTL(key); !ok || ttl != 60 {
			t.Fatalf("TTL of %s = %d, %v, want 60, true", key, ttl, ok)
		}
	}
	if frames := node.yFramesSeen(); len(frames) != 1 || !frames[0].get || frames[0].ttl != 60 {
		t.Fatalf("frames = %+v, want one G frame with ttl 60", frames)
	}
	raw, err := client.GetManyAndTouchBytes([]string{"a", "b"}, RemoveTTL)
	if err != nil || string(raw["a"]) != "1" || string(raw["b"]) != "2" {
		t.Fatalf("GetManyAndTouchBytes = %v, %v", raw, err)
	}
	if _, ok := node.storedTTL("a"); ok {
		t.Fatal("the TTL was not removed")
	}
	if values, err := client.GetManyAndTouch([]string{"missing"}, 5); err != nil || len(values) != 0 {
		t.Fatalf("GetManyAndTouch(miss) = %v, %v, want an empty map", values, err)
	}
}

func TestGetManyAndTouchWithZeroReturnsTheValuesThenTheyAreGone(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatal(err)
	}
	if values, err := client.GetManyAndTouch([]string{"a", "b"}, 0); err != nil || len(values) != 2 {
		t.Fatalf("GetManyAndTouch(0) = %v, %v, want both values", values, err)
	}
	if values, _ := client.GetMany([]string{"a", "b"}); len(values) != 0 {
		t.Fatalf("the entries survived GetManyAndTouch(0): %v", values)
	}
}

func TestBatchTouchTreatsARepeatedKeyOnce(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if err := client.Set("a", "1", 0); err != nil {
		t.Fatal(err)
	}
	if got, err := client.TouchMany([]string{"a", "a", "a"}, 30); err != nil || len(got) != 1 || !got["a"] {
		t.Fatalf("TouchMany = %v, %v, want {a: true}", got, err)
	}
	if got, err := client.GetManyAndTouch([]string{"a", "a"}, 30); err != nil || len(got) != 1 || got["a"] != "1" {
		t.Fatalf("GetManyAndTouch = %v, %v, want {a: 1}", got, err)
	}
	for _, frame := range node.yFramesSeen() {
		if len(frame.keys) != 1 {
			t.Fatalf("a repeated key was sent %d times: %v", len(frame.keys), frame.keys)
		}
	}
}

func TestBatchTouchIsScopedToItsNamespace(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	users := client.Namespace("users")
	if _, err := users.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := client.TouchMany([]string{"a"}, 30); err != nil || got["a"] {
		t.Fatalf("default-namespace TouchMany = %v, %v, want a: false", got, err)
	}
	if got, err := users.TouchMany([]string{"a", "b"}, 30); err != nil || !got["a"] || !got["b"] {
		t.Fatalf("namespaced TouchMany = %v, %v, want both true", got, err)
	}
	if values, err := users.GetManyAndTouch([]string{"a", "b"}, 40); err != nil || len(values) != 2 {
		t.Fatalf("namespaced GetManyAndTouch = %v, %v", values, err)
	}
	if raw, err := users.GetManyAndTouchBytes([]string{"a"}, 40); err != nil || string(raw["a"]) != "1" {
		t.Fatalf("namespaced GetManyAndTouchBytes = %v, %v", raw, err)
	}
	seen := map[string]bool{}
	for _, frame := range node.yFramesSeen() {
		seen[frame.ns] = true
	}
	if !seen[""] || !seen["users"] {
		t.Fatalf("namespaces on the wire = %v, want both the default and users", seen)
	}
}

func TestGetManyAndTouchDecompressesOnACompressEnabledClient(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{Compress: true, CompressionThreshold: 16})
	long := strings.Repeat("x", 200)
	if _, err := client.SetMany(map[string]string{"a": long, "b": "short"}, 0); err != nil {
		t.Fatal(err)
	}
	values, err := client.GetManyAndTouch([]string{"a", "b"}, 30)
	if err != nil || values["a"] != long || values["b"] != "short" {
		t.Fatalf("GetManyAndTouch = %d keys, %v, want the original values", len(values), err)
	}
	// A plain touch never reads a value, so compression is moot.
	if got, err := client.TouchMany([]string{"a", "b"}, 30); err != nil || !got["a"] || !got["b"] {
		t.Fatalf("TouchMany = %v, %v", got, err)
	}
}

func TestBatchTouchSplitsALargeBatchIntoSubFramesAndReassemblesIt(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	const total = maxBatchKeys*2 + 5
	values := make(map[string]string, total)
	keys := make([]string, 0, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("k%d", i)
		values[key] = fmt.Sprintf("v%d", i)
		keys = append(keys, key)
	}
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	got, err := client.GetManyAndTouch(keys, 30)
	if err != nil || len(got) != total {
		t.Fatalf("GetManyAndTouch = %d keys, %v, want %d", len(got), err, total)
	}
	for key, want := range values {
		if got[key] != want {
			t.Fatalf("got[%q] = %q, want %q", key, got[key], want)
		}
	}
	var sizes []int
	for _, frame := range node.yFramesSeen() {
		sizes = append(sizes, len(frame.keys))
	}
	if fmt.Sprint(sizes) != fmt.Sprint([]int{maxBatchKeys, maxBatchKeys, 5}) {
		t.Fatalf("frame sizes = %v, want [%d %d 5]", sizes, maxBatchKeys, maxBatchKeys)
	}
	touched, err := client.TouchMany(append(keys, "missing"), 30)
	if err != nil || len(touched) != total+1 || touched["missing"] {
		t.Fatalf("TouchMany = %d keys, %v (missing=%v)", len(touched), err, touched["missing"])
	}
	if len(node.yFramesSeen()) != 6 {
		t.Fatalf("frames = %d, want 6", len(node.yFramesSeen()))
	}
}

func TestBatchTouchRejectsEmptyKeysAndABadTTLBeforeAnyIO(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.TouchMany(nil, 30); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("TouchMany(nil) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.GetManyAndTouch(nil, 30); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("GetManyAndTouch(nil) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.TouchMany([]string{"k"}, -2); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("TouchMany(-2) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.GetManyAndTouchBytes([]string{"k"}, -2); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("GetManyAndTouchBytes(-2) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.TouchMany([]string{""}, 30); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("TouchMany(empty key) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.Namespace("users").TouchMany(nil, 30); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Namespace.TouchMany(nil) err = %v, want ErrInvalidArgument", err)
	}
	if got := node.yFramesSeen(); len(got) != 0 {
		t.Fatalf("an invalid call reached the node: %v", got)
	}
}

func TestBatchTouchRoundTripsOverATaggedConnection(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{supportTags: true})
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := client.TouchMany([]string{"a", "nope"}, 30); err != nil || !got["a"] || got["nope"] {
		t.Fatalf("TouchMany = %v, %v", got, err)
	}
	if values, err := client.GetManyAndTouch([]string{"a", "b", "nope"}, 60); err != nil || len(values) != 2 {
		t.Fatalf("GetManyAndTouch = %v, %v", values, err)
	}
	frames := node.yFramesSeen()
	if last := frames[len(frames)-1]; !last.get || last.ttl != 60 || len(last.keys) != 3 {
		t.Fatalf("last frame = %+v", last)
	}
}

func TestBatchTouchReturnsErrServerTooOldBatchAgainstANodeThatPredatesY(t *testing.T) {
	for name, opts := range map[string]mockNodeOpts{
		"knows t but not y": {withoutMultiTouch: true},
		"predates t":        {withoutTouch: true},
	} {
		t.Run(name, func(t *testing.T) {
			node := startMockNodeOpts(t, nil, opts)
			client := connectTo(t, node, Config{})
			if _, err := client.SetMany(map[string]string{"a": "1"}, 0); err != nil {
				t.Fatal(err)
			}
			_, err := client.TouchMany([]string{"a"}, 30)
			if !errors.Is(err, ErrServerTooOldBatch) || !errors.Is(err, ErrServerTooOld) || !errors.Is(err, ErrProtocol) {
				t.Fatalf("TouchMany err = %v, want ErrServerTooOldBatch, ErrServerTooOld and ErrProtocol", err)
			}
			if !strings.Contains(err.Error(), "(y)") {
				t.Fatalf("TouchMany err = %q, want the message to name y", err)
			}
			if _, err := client.GetManyAndTouch([]string{"a"}, 30); !errors.Is(err, ErrServerTooOldBatch) {
				t.Fatalf("GetManyAndTouch err = %v, want ErrServerTooOldBatch", err)
			}
			// Everything else keeps working: the client redials.
			if value, ok, err := client.Get("a"); err != nil || !ok || value != "1" {
				t.Fatalf("Get after the rejections = %q, %v, %v", value, ok, err)
			}
		})
	}
}

func TestSingleTouchStillNamesItsOwnCommandWhenTooOld(t *testing.T) {
	node := startMockNodeOpts(t, nil, mockNodeOpts{withoutTouch: true})
	client := connectTo(t, node, Config{})
	_, err := client.Touch("k", 30)
	if !errors.Is(err, ErrServerTooOld) || errors.Is(err, ErrServerTooOldBatch) {
		t.Fatalf("Touch err = %v, want ErrServerTooOld and not ErrServerTooOldBatch", err)
	}
	if !strings.Contains(err.Error(), "(t)") {
		t.Fatalf("Touch err = %q, want the message to name t", err)
	}
}

func TestBatchTouchKeepsTheEarlierChunksOnAConnectionLossInSingleNodeMode(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	const total = maxBatchKeys + 1
	values := make(map[string]string, total)
	keys := make([]string, 0, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("k%d", i)
		values[key] = "v"
		keys = append(keys, key)
	}
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	// Single-node mode never retries a chunk failure, so what chunk 1
	// resolved is returned alongside the error, as GetMany does.
	node.closeAfterYFrame(1)
	got, err := client.GetManyAndTouch(keys, 30)
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("GetManyAndTouch err = %v, want ErrConnectionLost", err)
	}
	if len(got) != maxBatchKeys {
		t.Fatalf("GetManyAndTouch returned %d keys, want the first chunk's %d", len(got), maxBatchKeys)
	}
}

func TestBatchTouchPerKeyWrongNodeInSingleNodeModeIsErrWrongNode(t *testing.T) {
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatal(err)
	}
	node.multiWrongNodeKey.Store("a")
	node.multiWrongNodeLeft.Store(2)
	got, err := client.TouchMany([]string{"a", "b"}, 30)
	if !errors.Is(err, ErrWrongNode) || len(got) != 1 || !got["b"] {
		t.Fatalf("TouchMany = %v, %v, want {b: true} and ErrWrongNode", got, err)
	}
	values, err := client.GetManyAndTouch([]string{"a", "b"}, 30)
	if !errors.Is(err, ErrWrongNode) || len(values) != 1 || values["b"] != "2" {
		t.Fatalf("GetManyAndTouch = %v, %v, want {b: 2} and ErrWrongNode", values, err)
	}
}

func TestBatchTouchRosterOfTheWrongLengthIsADesyncAndIsResent(t *testing.T) {
	// The same count check multiGetNS/multiSetNS make (issue #181): a short
	// roster would otherwise shift every later key's answer onto another
	// key. The connection is poisoned and, a touch being idempotent, the
	// request is redialed and resent (applyReconnecting) — so the caller
	// sees the right answer, and the node saw the frame twice.
	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	if _, err := client.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatal(err)
	}
	node.answerMultiTouchBadCountOnce(1)
	got, err := client.TouchMany([]string{"a", "b"}, 30)
	if err != nil || len(got) != 2 || !got["a"] || !got["b"] {
		t.Fatalf("TouchMany = %v, %v, want both true after the resend", got, err)
	}
	if frames := node.yFramesSeen(); len(frames) != 2 {
		t.Fatalf("frames = %d, want 2 (the desynced reply, then the resend)", len(frames))
	}
	node.answerMultiTouchBadCountOnce(1)
	values, err := client.GetManyAndTouch([]string{"a", "b"}, 30)
	if err != nil || len(values) != 2 {
		t.Fatalf("GetManyAndTouch = %v, %v, want both values after the resend", values, err)
	}
	if frames := node.yFramesSeen(); len(frames) != 4 {
		t.Fatalf("frames = %d, want 4", len(frames))
	}
}

// ── cluster ──────────────────────────────────────────────────────────

func startBatchTouchCluster(t *testing.T, replication int, opts [2]mockNodeOpts) (map[string]*mockNode, *mockDiscovery) {
	t.Helper()
	nodes := map[string]*mockNode{
		testNames[0]: startMockNodeOpts(t, nil, opts[0]),
		testNames[1]: startMockNodeOpts(t, nil, opts[1]),
	}
	listed := make([]discoveredNode, 0, len(nodes))
	for _, name := range testNames {
		listed = append(listed, discoveredNode{Name: name, Address: nodes[name].address()})
	}
	return nodes, startMockDiscovery(t, listed, replication)
}

func connectCluster(t *testing.T, discovery *mockDiscovery, config Config) *Client {
	t.Helper()
	config.Addresses = []Address{addr(discovery.address())}
	client, err := Connect(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func keysWithPrimary(name string, count int, prefix string) []string {
	var keys []string
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if ownersOf(key)[0] == name {
			keys = append(keys, key)
		}
	}
	return keys
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func valuesFor(keys []string) map[string]string {
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		values[key] = "v-" + key
	}
	return values
}

func TestClusterBatchTouchReachesEveryOwnerWithOneFramePerNode(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	if _, err := client.SetMany(valuesFor(keys), 10); err != nil {
		t.Fatal(err)
	}
	got, err := client.TouchMany(append(append([]string(nil), keys...), "nope"), 77)
	if err != nil || len(got) != 21 || got["nope"] {
		t.Fatalf("TouchMany = %d keys, %v (nope=%v)", len(got), err, got["nope"])
	}
	for name, node := range nodes {
		// R=2 over two nodes: both hold every key, and each got a single
		// plain frame for all of them.
		frames := node.yFramesSeen()
		if len(frames) != 1 || frames[0].get || len(frames[0].keys) != 21 {
			t.Errorf("%s: frames = %+v, want one plain frame of 21 keys", name, frames)
		}
		for _, key := range keys {
			if ttl, ok := node.storedTTL(key); !ok || ttl != 77 {
				t.Errorf("%s: TTL of %s = %d, %v, want 77, true", name, key, ttl, ok)
			}
		}
	}
}

func TestClusterGetManyAndTouchAsksOnlyAKeysPrimaryForTheValueInTwoFramesPerNode(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	keys := make([]string, 24)
	primaries := map[string]bool{}
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
		primaries[ownersOf(keys[i])[0]] = true
	}
	if len(primaries) != 2 {
		t.Fatal("the keys must give both nodes primaries")
	}
	values := valuesFor(keys)
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	got, err := client.GetManyAndTouch(keys, 40)
	if err != nil || len(got) != len(values) {
		t.Fatalf("GetManyAndTouch = %d keys, %v", len(got), err)
	}
	for name, node := range nodes {
		// Each node is primary for some keys and replica for the others, so
		// it gets the G frame for the first and the plain frame for the
		// second.
		frames := node.yFramesSeen()
		if len(frames) != 2 {
			t.Fatalf("%s: %d frames, want 2", name, len(frames))
		}
		var withG, plain yRecord
		if frames[0].get {
			withG, plain = frames[0], frames[1]
		} else {
			withG, plain = frames[1], frames[0]
		}
		var primaryKeys, replicaKeys []string
		for _, key := range keys {
			if ownersOf(key)[0] == name {
				primaryKeys = append(primaryKeys, key)
			} else {
				replicaKeys = append(replicaKeys, key)
			}
		}
		if !withG.get || plain.get ||
			fmt.Sprint(sortedStrings(withG.keys)) != fmt.Sprint(sortedStrings(primaryKeys)) ||
			fmt.Sprint(sortedStrings(plain.keys)) != fmt.Sprint(sortedStrings(replicaKeys)) {
			t.Errorf("%s: G frame %v / plain frame %v, want primary keys %v / replica keys %v", name, withG, plain, primaryKeys, replicaKeys)
		}
		for _, key := range keys {
			if ttl, ok := node.storedTTL(key); !ok || ttl != 40 {
				t.Errorf("%s: TTL of %s = %d, %v, want 40, true", name, key, ttl, ok)
			}
		}
	}
}

func TestClusterGetManyAndTouchDoesNotFallBackToAReplicaOnAMiss(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	owners := ownersOf("k")
	replica := nodes[owners[1]]
	replica.store.Store(storeKey{"", "k"}, []byte("v"))
	if values, err := client.GetManyAndTouch([]string{"k"}, 40); err != nil || len(values) != 0 {
		t.Fatalf("GetManyAndTouch = %v, %v, want a miss (answered by the primary alone)", values, err)
	}
	if got, err := client.TouchMany([]string{"k"}, 40); err != nil || got["k"] {
		t.Fatalf("TouchMany = %v, %v, want k: false", got, err)
	}
	if ttl, ok := replica.storedTTL("k"); !ok || ttl != 40 {
		t.Fatalf("replica TTL = %d, %v, want 40, true", ttl, ok)
	}
}

func TestClusterBatchTouchRefreshesOnceAfterAWrongNodeAnswerForAKey(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	values := valuesFor(keys)
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	owner := nodes[ownersOf(keys[0])[0]]
	owner.multiWrongNodeKey.Store(keys[0])
	owner.multiWrongNodeLeft.Store(1)
	got, err := client.GetManyAndTouch(keys, 40)
	if err != nil || len(got) != len(values) {
		t.Fatalf("GetManyAndTouch after a W = %d keys, %v, want all %d", len(got), err, len(values))
	}
	owner.multiWrongNodeLeft.Store(1)
	touched, err := client.TouchMany(keys, 41)
	if err != nil || len(touched) != len(keys) {
		t.Fatalf("TouchMany after a W = %d keys, %v", len(touched), err)
	}
	for key, existed := range touched {
		if !existed {
			t.Fatalf("TouchMany[%q] = false", key)
		}
	}
}

func TestClusterBatchTouchWithAPersistingWrongNodeReturnsWhatResolved(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	key, other := "some-key", "other-key"
	if _, err := client.SetMany(map[string]string{key: "v", other: "v2"}, 0); err != nil {
		t.Fatal(err)
	}
	owner := nodes[ownersOf(key)[0]]
	owner.multiWrongNodeKey.Store(key)
	owner.multiWrongNodeLeft.Store(10)
	values, err := client.GetManyAndTouch([]string{key, other}, 30)
	if !errors.Is(err, ErrWrongNode) || len(values) != 1 || values[other] != "v2" {
		t.Fatalf("GetManyAndTouch = %v, %v, want {other: v2} and ErrWrongNode", values, err)
	}
	owner.multiWrongNodeLeft.Store(10)
	touched, err := client.TouchMany([]string{key, other}, 30)
	if !errors.Is(err, ErrWrongNode) || len(touched) != 1 || !touched[other] {
		t.Fatalf("TouchMany = %v, %v, want {other: true} and ErrWrongNode", touched, err)
	}
}

func TestClusterBatchWhoseReplyWasLostIsResentWhole(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	keys := keysWithPrimary(testNames[0], 4, "mt")
	primary := nodes[testNames[0]]
	values := valuesFor(keys)
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	// The primary applies the touch and the reply never arrives; resending
	// it changes nothing, so the client does.
	primary.applyThenCloseTouchTimes(1)
	got, err := client.GetManyAndTouch(keys, 30)
	if err != nil || len(got) != len(values) {
		t.Fatalf("GetManyAndTouch after a lost reply = %d keys, %v", len(got), err)
	}
	resent := 0
	for _, frame := range primary.yFramesSeen() {
		if frame.get {
			resent++
		}
	}
	if resent != 2 {
		t.Fatalf("primary saw %d G frames, want 2 (one lost reply, one resend)", resent)
	}
}

func TestClusterReplicaThatFailsDoesNotFailThePrimarysAnswer(t *testing.T) {
	var opts [2]mockNodeOpts
	opts[1].withoutMultiTouch = true // testNames[1], the replica of every key below
	_, discovery := startBatchTouchCluster(t, 2, opts)
	client := connectCluster(t, discovery, Config{})
	keys := keysWithPrimary(testNames[0], 3, "mt")
	values := valuesFor(keys)
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	// Counted per key, as SetMany counts a failed replica leg.
	if got, err := client.GetManyAndTouch(keys, 30); err != nil || len(got) != 3 {
		t.Fatalf("GetManyAndTouch = %d keys, %v", len(got), err)
	}
	if got := client.Stats().ReplicaWriteFailures; got != 3 {
		t.Fatalf("ReplicaWriteFailures = %d, want 3", got)
	}
	if got, err := client.TouchMany(keys, 30); err != nil || len(got) != 3 {
		t.Fatalf("TouchMany = %v, %v", got, err)
	}
	if got := client.Stats().ReplicaWriteFailures; got != 6 {
		t.Fatalf("ReplicaWriteFailures = %d, want 6", got)
	}
}

func TestClusterPrimaryThatPredatesYIsErrServerTooOldBatchNotAConnectionLoss(t *testing.T) {
	// A release that knows `t` but not `y`: its `En` closes the connection,
	// and the batch code sees it as a failed leg, not as a returned error
	// of its own — it must still come out as ErrServerTooOldBatch.
	var opts [2]mockNodeOpts
	opts[0].withoutMultiTouch = true // testNames[0], the primary of every key below
	_, discovery := startBatchTouchCluster(t, 2, opts)
	client := connectCluster(t, discovery, Config{})
	keys := keysWithPrimary(testNames[0], 3, "mt")
	if _, err := client.SetMany(valuesFor(keys), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetManyAndTouch(keys, 30); !errors.Is(err, ErrServerTooOldBatch) {
		t.Fatalf("GetManyAndTouch err = %v, want ErrServerTooOldBatch", err)
	}
	if _, err := client.TouchMany(keys, 30); !errors.Is(err, ErrServerTooOldBatch) {
		t.Fatalf("TouchMany err = %v, want ErrServerTooOldBatch", err)
	}
	// The single-key touch is untouched: that node knows `t`.
	if existed, err := client.Touch(keys[0], 30); err != nil || !existed {
		t.Fatalf("Touch = %v, %v, want true, nil", existed, err)
	}
}

func TestClusterBatchTouchWithAnUnreachablePrimaryIsAConnectionLossWithTheOtherHits(t *testing.T) {
	// R=1: the only owner of some keys is gone. Nothing answered `W`, so
	// the error is not the wrong-node one, and no replica stands in.
	nodes, discovery := startBatchTouchCluster(t, 1, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	keys := make([]string, 24)
	var liveKeys []string
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
		if NewHashRing(testNames).Owners([]byte(keys[i]), 1)[0] == testNames[1] {
			liveKeys = append(liveKeys, keys[i])
		}
	}
	if len(liveKeys) == 0 || len(liveKeys) == len(keys) {
		t.Fatal("the keys must give both nodes primaries")
	}
	if _, err := client.SetMany(valuesFor(keys), 0); err != nil {
		t.Fatal(err)
	}
	nodes[testNames[0]].close()
	time.Sleep(50 * time.Millisecond)

	values, err := client.GetManyAndTouch(keys, 30)
	if !errors.Is(err, ErrConnectionLost) || errors.Is(err, ErrWrongNode) {
		t.Fatalf("GetManyAndTouch err = %v, want ErrConnectionLost (not ErrWrongNode)", err)
	}
	got := make([]string, 0, len(values))
	for key := range values {
		got = append(got, key)
	}
	if fmt.Sprint(sortedStrings(got)) != fmt.Sprint(sortedStrings(liveKeys)) {
		t.Fatalf("resolved keys = %v, want the live node's %v", sortedStrings(got), sortedStrings(liveKeys))
	}
	if !strings.Contains(err.Error(), "could not reach the primary of") {
		t.Fatalf("err = %q, want the unreachable-primary message", err)
	}
	touched, err := client.TouchMany(keys, 30)
	if !errors.Is(err, ErrConnectionLost) || errors.Is(err, ErrWrongNode) || len(touched) != len(liveKeys) {
		t.Fatalf("TouchMany = %d keys, %v, want the live node's %d and ErrConnectionLost", len(touched), err, len(liveKeys))
	}
}

func TestClusterBatchTouchMovesOnlyAFailedChunksTailToTheRetry(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	primary := nodes[testNames[0]]
	keys := keysWithPrimary(testNames[0], maxBatchKeys+50, "chunk")
	values := valuesFor(keys)
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	before := len(primary.yFramesSeen())
	primary.failMultiTouchAfter(1)
	got, err := client.GetManyAndTouch(keys, 30)
	if err != nil || len(got) != len(values) {
		t.Fatalf("GetManyAndTouch = %d keys, %v, want all %d", len(got), err, len(values))
	}
	// Chunk 1 answered; chunk 2 (the last 50) died once and was sent again
	// alone — chunk 1's keys were not resent.
	var shape []string
	for _, frame := range primary.yFramesSeen()[before:] {
		shape = append(shape, fmt.Sprintf("%d/%v", len(frame.keys), frame.get))
	}
	want := fmt.Sprintf("[%d/true 50/true 50/true]", maxBatchKeys)
	if fmt.Sprint(shape) != want {
		t.Fatalf("primary frames = %v, want %s", shape, want)
	}
}

func TestClusterBatchTouchIsScopedToItsNamespace(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{})
	users := client.Namespace("users")
	if _, err := users.SetMany(map[string]string{"a": "1", "b": "2"}, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := client.TouchMany([]string{"a", "b"}, 30); err != nil || got["a"] || got["b"] {
		t.Fatalf("default-namespace TouchMany = %v, %v, want both false", got, err)
	}
	if values, err := users.GetManyAndTouch([]string{"a", "b"}, 30); err != nil || len(values) != 2 {
		t.Fatalf("namespaced GetManyAndTouch = %v, %v", values, err)
	}
	for name, node := range nodes {
		seen := map[string]bool{}
		for _, frame := range node.yFramesSeen() {
			seen[frame.ns] = true
		}
		if !seen[""] || !seen["users"] {
			t.Errorf("%s: namespaces on the wire = %v, want both", name, seen)
		}
	}
}

func TestClusterBatchTouchPureReplicaLegsMayRunInTheBackground(t *testing.T) {
	nodes, discovery := startBatchTouchCluster(t, 2, [2]mockNodeOpts{})
	client := connectCluster(t, discovery, Config{FireAndForgetReplicas: true})
	keys := keysWithPrimary(testNames[0], 3, "mt")
	values := valuesFor(keys)
	if _, err := client.SetMany(values, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := client.GetManyAndTouch(keys, 55); err != nil || len(got) != len(values) {
		t.Fatalf("GetManyAndTouch = %v, %v", got, err)
	}
	// The replica's plain frame is not awaited, but it does land.
	replica := nodes[testNames[1]]
	waitFor(t, func() bool {
		for _, key := range keys {
			if ttl, ok := replica.storedTTL(key); !ok || ttl != 55 {
				return false
			}
		}
		return true
	}, "the background replica touch")
}
