package nanocached

import (
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
	// Pinned outputs of the full ADR-0011 score pipeline — the Rust node
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

func TestRoutePanicsOnAnEmptyRing(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Route on an empty ring did not panic")
		}
	}()
	NewHashRing(nil).Route([]byte("k"))
}

func TestSpreadsKeysEvenly(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	ring := NewHashRing(nodes)
	counts := map[string]int{}
	const total = 3000
	for i := 0; i < total; i++ {
		counts[ring.Route([]byte(fmt.Sprintf("key-%d", i)))]++
	}
	fair := float64(total) / float64(len(nodes))
	for node, count := range counts {
		if math.Abs(float64(count)-fair)/fair >= 0.15 {
			t.Errorf("%s got %d/%d", node, count, total)
		}
	}
}
