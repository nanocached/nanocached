package nanocached

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
)

func TestFnv1aMatchesPublishedVectors(t *testing.T) {
	cases := map[string]uint64{
		"":       0xcbf29ce484222325,
		"a":      0xaf63dc4c8601ec8c,
		"foobar": 0x85944171f73967e8,
	}
	for input, want := range cases {
		if got := fnv1a([]byte(input)); got != want {
			t.Errorf("fnv1a(%q) = %#x, want %#x", input, got, want)
		}
	}
}

func TestMatchesCrossLanguageScoreVectors(t *testing.T) {
	// Pinned outputs of the full client-side replication score pipeline — the Rust node
	// and the TypeScript/Python/Java/Rust/.NET SDKs assert these too.
	if got := fmix64(0); got != 0 {
		t.Errorf("fmix64(0) = %#x", got)
	}
	if got := fmix64(1); got != 0xb456bcfc34c2cb2c {
		t.Errorf("fmix64(1) = %#x", got)
	}
	if got := fmix64(0xcbf29ce484222325); got != 0xefd01f60ba992926 {
		t.Errorf("fmix64(offset basis) = %#x", got)
	}

	ring := NewHashRing([]string{"node-a", "node-b", "node-c"})
	cases := map[string][]string{
		"alpha": {"node-c", "node-b", "node-a"},
		"beta":  {"node-a", "node-c", "node-b"},
		"":      {"node-a", "node-b", "node-c"},
	}
	for key, want := range cases {
		if got := ring.Owners([]byte(key), 3); !reflect.DeepEqual(got, want) {
			t.Errorf("Owners(%q) = %v, want %v", key, got, want)
		}
	}
}

// ── namespaces (issue #105) ──────────────────────────────────────────

// TestNamespacedKeyHashMatchesCrossLanguageVectors pins keyHash's
// namespaced form (fnv1a(be32(len(ns)) || ns || key)) against the same
// vectors the server (src/hash_ring.rs) and the other five SDKs assert,
// next to the existing alpha/beta/"" vectors above.
func TestNamespacedKeyHashMatchesCrossLanguageVectors(t *testing.T) {
	cases := []struct {
		ns, key string
		want    uint64
	}{
		{"users", "alpha", 0xfd4ab55027c21df6},
		{"users", "", 0xa9e9bbca44bb502e}, // hash-only vector; the wire itself rejects an empty key
		{"\xff\x00", "beta", 0x8f7c097eccb8e792},
	}
	for _, c := range cases {
		if got := keyHash([]byte(c.ns), []byte(c.key)); got != c.want {
			t.Errorf("keyHash(%q, %q) = %#x, want %#x", c.ns, c.key, got, c.want)
		}
	}
}

// TestNamespacedOwnersMatchCrossLanguageVectors is the same vectors run
// through the full HRW pipeline, over the ring/replicas the issue #105
// spec pins them against.
func TestNamespacedOwnersMatchCrossLanguageVectors(t *testing.T) {
	ring := NewHashRing([]string{"node-a", "node-b", "node-c"})
	cases := []struct {
		ns, key string
		want    []string
	}{
		{"users", "alpha", []string{"node-a", "node-c", "node-b"}},
		{"users", "", []string{"node-b", "node-c", "node-a"}},
		{"\xff\x00", "beta", []string{"node-b", "node-a", "node-c"}},
	}
	for _, c := range cases {
		got := ring.OwnersNS([]byte(c.ns), []byte(c.key), 3)
		want := c.want
		if !reflect.DeepEqual(got, want) {
			t.Errorf("OwnersNS(%q, %q) = %v, want %v", c.ns, c.key, got, want)
		}
	}
}

// TestDefaultNamespaceHashesAndRoutesExactlyLikeTheLegacyForm is the
// rolling-upgrade invariant from hash_ring.rs's module docs: an
// unnamespaced key's placement must not move when namespaces enter the
// picture — nil and "" must both behave as "no namespace at all", and
// OwnersNS(nil, ...)/Owners(...) must agree byte-for-byte with the
// pre-#105 alpha vector above.
func TestDefaultNamespaceHashesAndRoutesExactlyLikeTheLegacyForm(t *testing.T) {
	if got, want := keyHash(nil, []byte("alpha")), fnv1a([]byte("alpha")); got != want {
		t.Errorf("keyHash(nil, \"alpha\") = %#x, want %#x", got, want)
	}
	if got, want := keyHash([]byte(""), []byte("alpha")), fnv1a([]byte("alpha")); got != want {
		t.Errorf("keyHash(\"\", \"alpha\") = %#x, want %#x", got, want)
	}

	ring := NewHashRing([]string{"node-a", "node-b", "node-c"})
	want := []string{"node-c", "node-b", "node-a"}
	if got := ring.OwnersNS(nil, []byte("alpha"), 3); !reflect.DeepEqual(got, want) {
		t.Errorf("OwnersNS(nil, \"alpha\") = %v, want %v", got, want)
	}
	if got := ring.OwnersNS([]byte(""), []byte("alpha"), 3); !reflect.DeepEqual(got, want) {
		t.Errorf("OwnersNS(\"\", \"alpha\") = %v, want %v", got, want)
	}
	if got := ring.Owners([]byte("alpha"), 3); !reflect.DeepEqual(got, want) {
		t.Errorf("Owners(\"alpha\") = %v, want %v", got, want)
	}
}

// TestNamespaceAndKeyBoundariesAreUnambiguous: length-prefixing the
// namespace is what keeps ("ab","c") and ("a","bc") from colliding, and
// keeps a namespaced key from ever colliding with the un-namespaced
// concatenation of the two — mirrors hash_ring.rs's own boundary test.
func TestNamespaceAndKeyBoundariesAreUnambiguous(t *testing.T) {
	if got, other := keyHash([]byte("ab"), []byte("c")), keyHash([]byte("a"), []byte("bc")); got == other {
		t.Errorf("keyHash(\"ab\",\"c\") == keyHash(\"a\",\"bc\") == %#x", got)
	}
	if got, legacy := keyHash([]byte("ab"), []byte("c")), keyHash(nil, []byte("abc")); got == legacy {
		t.Errorf("keyHash(\"ab\",\"c\") == keyHash(nil,\"abc\") == %#x", got)
	}
}

func TestOwnersAreDistinctAndCapped(t *testing.T) {
	ring := NewHashRing([]string{"node-a", "node-b", "node-c"})
	owners := ring.Owners([]byte("some-key"), 2)
	if len(owners) != 2 || owners[0] == owners[1] {
		t.Errorf("Owners = %v", owners)
	}
	if got := ring.Owners([]byte("some-key"), 10); len(got) != 3 {
		t.Errorf("capped Owners = %v", got)
	}
}

// TestOwnersMatchesANaiveFullSortReference pins Owners' output
// byte-identical to the straightforward "sort everything, then
// truncate" reference it replaced (the bounded top-R selection it now
// does instead must never change which nodes come back, or their
// order) across the edge cases in replicas (0, 1, exactly the node
// count, and over it) plus a run of pseudo-random keys.
func TestOwnersMatchesANaiveFullSortReference(t *testing.T) {
	type scored struct {
		score uint64
		node  string
	}
	naiveOwners := func(ring *HashRing, key []byte, replicas int) []string {
		keyHash := fnv1a(key)
		ranked := make([]scored, len(ring.nodes))
		for i, node := range ring.nodes {
			ranked[i] = scored{fmix64(ring.nodeHashes[i] ^ keyHash), node}
		}
		sort.Slice(ranked, func(a, b int) bool {
			if ranked[a].score != ranked[b].score {
				return ranked[a].score > ranked[b].score
			}
			return ranked[a].node < ranked[b].node
		})
		if replicas > len(ranked) {
			replicas = len(ranked)
		}
		if replicas <= 0 {
			return []string{}
		}
		owners := make([]string, replicas)
		for i := range owners {
			owners[i] = ranked[i].node
		}
		return owners
	}

	nodes := make([]string, 37)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("node-%d", i)
	}
	ring := NewHashRing(nodes)

	// A tiny deterministic PRNG (splitmix64) stands in for a "math/rand"
	// seeded sequence, keeping this test free of any flakiness tied to
	// the runtime's default source.
	state := uint64(0xC0FFEE)
	next := func() uint64 {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}

	for _, replicas := range []int{0, 1, len(nodes) / 2, len(nodes), len(nodes) + 1, len(nodes) + 10} {
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("fuzz-key-%d", next()))
			got := ring.Owners(key, replicas)
			want := naiveOwners(ring, key, replicas)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("replicas=%d key=%q: Owners = %v, want %v", replicas, key, got, want)
			}
		}
	}
}

func TestAddingANodeNeverReordersExistingNodes(t *testing.T) {
	before := NewHashRing([]string{"node-a", "node-b", "node-c"})
	after := NewHashRing([]string{"node-a", "node-b", "node-c", "node-d"})
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		var newOrder []string
		for _, node := range after.Owners(key, 4) {
			if node != "node-d" {
				newOrder = append(newOrder, node)
			}
		}
		if !reflect.DeepEqual(before.Owners(key, 3), newOrder) {
			t.Fatalf("reordered for key-%d", i)
		}
	}
}

func TestRouteErrorsOnAnEmptyRing(t *testing.T) {
	node, err := NewHashRing(nil).Route([]byte("k"))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if node != "" {
		t.Fatalf("node = %q, want empty", node)
	}
}

func TestSpreadsKeysEvenly(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	ring := NewHashRing(nodes)
	counts := map[string]int{}
	const total = 3000
	for i := 0; i < total; i++ {
		node, err := ring.Route([]byte(fmt.Sprintf("key-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		counts[node]++
	}
	fair := float64(total) / float64(len(nodes))
	for node, count := range counts {
		if math.Abs(float64(count)-fair)/fair >= 0.15 {
			t.Errorf("%s got %d/%d", node, count, total)
		}
	}
}
