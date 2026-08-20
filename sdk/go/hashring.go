package nanocached

import "sort"

// HashRing is rendezvous (highest-random-weight) hashing over a fixed
// node list (see doc/adr/0011-*.md in the nanocached repository). This is
// deliberately a byte-for-byte port of the same computation every other
// nanocached participant uses (the Rust node, the TypeScript/Python/Java/
// Rust/.NET SDKs) — not just "a" rendezvous hash, but *this specific*
// one: if this SDK's ranking disagreed with a node's own copy, the two
// would disagree about which nodes hold a key. Cross-language test
// vectors pin the pipeline.
//
// For each (node, key) pair, score = fmix64(fnv1a(name) ^ fnv1a(key)); a
// key's owners are the `replicas` highest-scoring nodes in descending
// score order (ties — effectively impossible at 64 bits — break toward
// the lexicographically smaller name), and its primary is the top one.
//
// Built from node *names*, not addresses (doc/adr/0009-*.md).
type HashRing struct {
	nodes      []string
	nodeHashes []uint64
}

// fnv1a is FNV-1a over 64 bits; Go's uint64 arithmetic wraps like Rust's u64.
func fnv1a(data []byte) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 0x100000001b3
	}
	return hash
}

// fmix64 is MurmurHash3's 64-bit finalizer: the full-avalanche mix FNV-1a lacks.
func fmix64(value uint64) uint64 {
	value ^= value >> 33
	value *= 0xff51afd7ed558ccd
	value ^= value >> 33
	value *= 0xc4ceb9fe1a85ec53
	value ^= value >> 33
	return value
}

// NewHashRing builds a ranking over the given node names, once, from a
// discovery server's node list. Ranking never changes after construction.
func NewHashRing(nodes []string) *HashRing {
	ring := &HashRing{
		nodes:      append([]string(nil), nodes...),
		nodeHashes: make([]uint64, len(nodes)),
	}
	for i, node := range ring.nodes {
		ring.nodeHashes[i] = fnv1a([]byte(node))
	}
	return ring
}

// Owners returns the key's owners: the `replicas` highest-scoring nodes,
// primary first. Returns fewer when the cluster is smaller.
func (r *HashRing) Owners(key []byte, replicas int) []string {
	keyHash := fnv1a(key)

	type scored struct {
		score uint64
		node  string
	}
	ranked := make([]scored, len(r.nodes))
	for i, node := range r.nodes {
		ranked[i] = scored{fmix64(r.nodeHashes[i] ^ keyHash), node}
	}

	// Descending by score; ties toward the lexicographically smaller
	// name — a total order every implementation agrees on.
	sort.Slice(ranked, func(a, b int) bool {
		if ranked[a].score != ranked[b].score {
			return ranked[a].score > ranked[b].score
		}
		return ranked[a].node < ranked[b].node
	})

	if replicas > len(ranked) {
		replicas = len(ranked)
	}
	owners := make([]string, replicas)
	for i := range owners {
		owners[i] = ranked[i].node
	}
	return owners
}

// Route returns the key's primary — Owners(key, 1)[0]. Panics on an
// empty ring.
func (r *HashRing) Route(key []byte) string {
	if len(r.nodes) == 0 {
		panic("nanocached: HashRing.Route called on an empty ring")
	}
	return r.Owners(key, 1)[0]
}
