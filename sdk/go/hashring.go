package nanocached

import "encoding/binary"

// HashRing is rendezvous (highest-random-weight) hashing over a fixed
// node list (the client-side replication design). This is
// deliberately a byte-for-byte port of the same computation every other
// nanocached participant uses (the Rust node, the TypeScript/Python/Java/
// Rust/.NET SDKs) — not just "a" rendezvous hash, but *this specific*
// one: if this SDK's ranking disagreed with a node's own copy, the two
// would disagree about which nodes hold a key. Cross-language test
// vectors pin the pipeline.
//
// For each (node, key) pair, score = fmix64(fnv1a(name) ^ keyHash); a
// key's owners are the `replicas` highest-scoring nodes in descending
// score order (ties — effectively impossible at 64 bits — break toward
// the lexicographically smaller name), and its primary is the top one.
//
// Built from node *names*, not addresses (node identity decoupled from address).
type HashRing struct {
	nodes      []string
	nodeHashes []uint64
}

// fnv1aOffsetBasis and fnv1aPrime are FNV-1a's 64-bit constants; Go's
// uint64 arithmetic wraps like Rust's u64.
const (
	fnv1aOffsetBasis uint64 = 0xcbf29ce484222325
	fnv1aPrime       uint64 = 0x100000001b3
)

// fnv1a is FNV-1a over 64 bits, from the standard offset basis.
func fnv1a(data []byte) uint64 {
	return fnv1aContinue(fnv1aOffsetBasis, data)
}

// fnv1aContinue feeds data into an in-progress FNV-1a state, so a
// multi-part input (issue #105's namespaced key hash: length-prefix,
// then namespace, then key) can be hashed as one stream without ever
// materializing the concatenation — mirrors src/hash_ring.rs's
// fnv1a_continue, which every implementation shares this exact split
// with.
func fnv1aContinue(hash uint64, data []byte) uint64 {
	for _, b := range data {
		hash ^= uint64(b)
		hash *= fnv1aPrime
	}
	return hash
}

// keyHash is the canonical key-side hash input the HRW score is built
// from (issue #105: first-class namespaces) — consensus-critical across
// the server, this SDK, and the other five:
//
//   - default (empty) namespace: fnv1a(key) — byte-identical to the
//     pre-namespace form, so an existing key's placement never moves
//     across a rolling upgrade, and mixed-version clients still agree;
//   - non-empty namespace: fnv1a(be32(len(ns)) || ns || key) — the
//     namespace length as a 4-byte big-endian integer, then the
//     namespace bytes, then the key bytes, hashed as one stream.
//     Length-prefixed so ("ab","c") and ("a","bc") can never collide;
//     including the namespace at all is what keeps placement balanced —
//     hashing the key alone would pile every namespace's common
//     singleton keys (e.g. "config") onto the same nodes.
func keyHash(namespace, key []byte) uint64 {
	if len(namespace) == 0 {
		return fnv1a(key)
	}
	// namespace is bounded by the request-size limit (1 MiB) by the
	// time any caller in this package reaches here — validateKey/
	// validateKeyAndValue/validateNamespaceForClear in client.go fold
	// the namespace into that bound for every namespaced entry point
	// (issue #228) — so this cast never truncates; it's the canonical
	// encoding width every implementation uses, not a narrowing that
	// could actually occur.
	var lengthBytes [4]byte
	binary.BigEndian.PutUint32(lengthBytes[:], uint32(len(namespace)))
	hash := fnv1a(lengthBytes[:])
	hash = fnv1aContinue(hash, namespace)
	return fnv1aContinue(hash, key)
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
//
// Issue #360 (mirrors src/hash_ring.rs's HashRing::new dedupe, issue
// #328): deduplicates nodes, keeping the first occurrence so construction
// order is otherwise unaffected. A repeated name would otherwise score
// independently for each of its slots in OwnersNS's bounded insertion,
// occupying more than one place in the returned top-`replicas` set and
// inflating its effective share of the ring.
func NewHashRing(nodes []string) *HashRing {
	deduped := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		deduped = append(deduped, node)
	}

	ring := &HashRing{
		nodes:      deduped,
		nodeHashes: make([]uint64, len(deduped)),
	}
	for i, node := range ring.nodes {
		ring.nodeHashes[i] = fnv1a([]byte(node))
	}
	return ring
}

// Owners returns the key's owners in the default (unnamespaced) keyspace:
// the `replicas` highest-scoring nodes, primary first. Returns fewer when
// the cluster is smaller. Equivalent to OwnersNS(nil, key, replicas) —
// kept as its own method so existing callers (and this package's own
// pre-#105 tests) don't have to pass a namespace they don't have.
func (r *HashRing) Owners(key []byte, replicas int) []string {
	return r.OwnersNS(nil, key, replicas)
}

// OwnersNS is Owners scoped to namespace (issue #105: first-class
// namespaces) — an empty namespace is the same default keyspace Owners
// itself addresses, byte-identical placement included (see keyHash).
func (r *HashRing) OwnersNS(namespace, key []byte, replicas int) []string {
	keyHash := keyHash(namespace, key)

	type scored struct {
		score uint64
		node  string
	}

	if replicas > len(r.nodes) {
		replicas = len(r.nodes)
	}
	if replicas <= 0 {
		return []string{}
	}

	// less reports whether a ranks strictly ahead of b in owner order:
	// higher score wins; ties break toward the lexicographically smaller
	// name — the same total order every nanocached implementation uses.
	less := func(a, b scored) bool {
		if a.score != b.score {
			return a.score > b.score
		}
		return a.node < b.node
	}

	// This runs per key while the client's routing lock is held, so
	// avoid sort.Slice's O(n log n) full sort of every node when only
	// the top `replicas` are wanted: keep just the best `replicas`
	// candidates seen so far, in a slice sorted best-first, inserting
	// (and evicting the current worst kept candidate, if any) as better
	// candidates turn up. `replicas` is typically small (a handful) next
	// to the cluster size, so this bounded insertion — O(n * replicas) —
	// beats sorting everything.
	top := make([]scored, 0, replicas)
	for i, node := range r.nodes {
		cand := scored{fmix64(r.nodeHashes[i] ^ keyHash), node}
		if len(top) == replicas && !less(cand, top[len(top)-1]) {
			continue // no better than the worst candidate currently kept
		}
		pos := len(top)
		for pos > 0 && less(cand, top[pos-1]) {
			pos--
		}
		if len(top) < replicas {
			top = append(top, scored{})
		}
		copy(top[pos+1:], top[pos:len(top)-1])
		top[pos] = cand
	}

	owners := make([]string, len(top))
	for i, s := range top {
		owners[i] = s.node
	}
	return owners
}

// Route returns the key's primary in the default (unnamespaced) keyspace
// — Owners(key, 1)[0]. On an empty ring there is no primary to return, so
// it reports an error (wrapping ErrInvalidArgument, matching every other
// client-side validation failure caught before any network I/O) instead
// of panicking — a public API shouldn't crash its caller's process over a
// construction-time mistake it can hand back as an ordinary error
// instead.
func (r *HashRing) Route(key []byte) (string, error) {
	return r.RouteNS(nil, key)
}

// RouteNS is Route scoped to namespace (issue #105).
func (r *HashRing) RouteNS(namespace, key []byte) (string, error) {
	if len(r.nodes) == 0 {
		return "", invalidArgument("nanocached: HashRing.Route called on an empty ring")
	}
	return r.OwnersNS(namespace, key, 1)[0], nil
}
