//! Rendezvous (highest-random-weight) hashing over a fixed node list (see
//! Client-side replication, which replaced client-side consistent hashing with a discovery server's virtual-node ring: FNV-1a's weak
//! high-bit avalanche clustered each node's ring points into narrow bands,
//! skewing shares by up to ~2×; HRW measures within 2% of fair and yields
//! replica sets for free). A byte-for-byte port of the same algorithm all
//! six SDKs use — not shared with them via a common library (this project's
//! binaries don't share code that way, see TLS support), just the same
//! computation independently implemented, the way any two independent
//! nanocached clients must agree on it.
//!
//! For each (node, key) pair, `score = fmix64(fnv1a(name) ^ fnv1a(key))`;
//! a key's owners are the `replicas` highest-scoring nodes in descending
//! score order (ties — effectively impossible at 64 bits — break toward
//! the lexicographically smaller name), and its primary is the top one.
//! Adding a node never reorders the existing nodes relative to each other,
//! which is what keeps client-side replication's join handoff and replica cleanup local:
//! per affected key, exactly one node is displaced from the top-R.
//!
//! `nanocached-node` needs this for staged node join's join: an already-ready node
//! must compute, for each key it holds, how the key's top-R changes when a
//! new node is added — who sends the joining node its copy (the old
//! primary), and whether this node's own copy just became dead (displaced
//! from rank R, see client-side replication).
//!
//! Namespaces (issue #105) enter the key side of the score, and the
//! canonical hash input is consensus-critical across the server, the six
//! SDKs and `verify-staged-join`:
//!
//! - default (empty) namespace: `fnv1a(key)` — byte-identical to the
//!   pre-namespace form, so every existing key keeps its placement
//!   across a rolling upgrade (no cluster-wide hit-rate cliff, and
//!   mixed-version clients agree on placement);
//! - non-empty namespace: `fnv1a(be32(len(ns)) || ns || key)` — the
//!   namespace length as a 4-byte big-endian integer, then the namespace
//!   bytes, then the key bytes, hashed as one stream. Length-prefixed so
//!   `("ab", "c")` and `("a", "bc")` never share an input; including the
//!   namespace at all is what keeps the placement balanced — hashing the
//!   key alone would pile every namespace's common singleton keys (e.g.
//!   `config`) onto the same nodes.

use crate::key::Key;
use bytes::Bytes;
use std::collections::HashSet;

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_continue(FNV_OFFSET_BASIS, bytes)
}

/// Feeds `bytes` into an in-progress FNV-1a state, so a multi-part
/// input hashes exactly as its concatenation would, with no allocation.
fn fnv1a_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    #[cfg(test)]
    HASHED_BYTES.with(|count| count.set(count.get() + bytes.len()));
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// Test-only: bytes fed through FNV on this thread, to assert that per-key
// work does not scale with the namespace length (a thread-local because
// tests run in parallel).
#[cfg(test)]
thread_local! {
    static HASHED_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Test-only: bytes fed through FNV on this thread so far.
#[cfg(test)]
pub(crate) fn hashed_bytes() -> usize {
    HASHED_BYTES.with(|count| count.get())
}

/// The FNV-1a state after the namespace part of a key's canonical hash
/// input (see the module docs): `be32(len(ns)) || ns` for a non-empty
/// namespace, the plain offset basis for the default one. FNV-1a is a
/// byte stream, so continuing this with each key's name gives exactly
/// the one-shot hash — which lets a frame or loop over many keys of one
/// namespace pay for the namespace (up to ~1 MiB) once rather than per
/// key.
fn namespace_state(namespace: &[u8]) -> u64 {
    if namespace.is_empty() {
        return FNV_OFFSET_BASIS;
    }

    // Namespaces are bounded by the request-size limit (1 MiB), so the
    // length always fits; the `u32` cast is the canonical encoding width
    // every implementation uses, not a truncation that could ever occur.
    let namespace_length = u32::try_from(namespace.len())
        .expect("a namespace is bounded by the request-size limit")
        .to_be_bytes();

    let hash = fnv1a(&namespace_length);
    fnv1a_continue(hash, namespace)
}

/// A key's canonical hash (see the module docs for the two forms),
/// computed once and then reusable across every `HashRing` lookup for the
/// key (`is_owner_hashed`/`owners_hashed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyHash(u64);

impl KeyHash {
    pub fn of(key: &Key) -> Self {
        Self(fnv1a_continue(namespace_state(&key.namespace), &key.name))
    }
}

/// Hashes many keys, paying for each *distinct* namespace once: the state
/// after the last namespace seen is kept, keyed by the identity (pointer
/// and length) of its `Bytes`, so consecutive keys that share a namespace
/// allocation — a multi-key frame's keys, or `Cache::keys()`'s, which
/// arrive grouped by namespace — skip straight to hashing their own name.
/// A key whose namespace is a different allocation just recomputes, so
/// the result is always the one-shot hash, whatever the key order.
#[derive(Default)]
pub struct KeyHasher {
    /// Holds the namespace itself so its allocation, and so the pointer
    /// compared against, cannot be freed and reused while remembered.
    last: Option<(Bytes, u64)>,
}

impl KeyHasher {
    pub fn hash(&mut self, key: &Key) -> KeyHash {
        let state = match &self.last {
            Some((namespace, state))
                if namespace.as_ptr() == key.namespace.as_ptr()
                    && namespace.len() == key.namespace.len() =>
            {
                *state
            }
            _ => {
                let state = namespace_state(&key.namespace);
                self.last = Some((key.namespace.clone(), state));
                state
            }
        };

        KeyHash(fnv1a_continue(state, &key.name))
    }
}

/// MurmurHash3's 64-bit finalizer: a full-avalanche bijective mix, which
/// is what FNV-1a alone lacks (see the module docs).
fn fmix64(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

pub struct HashRing {
    nodes: Vec<String>,
    /// `fnv1a(name)` per node, precomputed — a lookup only mixes each with
    /// the key's hash.
    node_hashes: Vec<u64>,
}

impl HashRing {
    /// Issue #328 (defense in depth): every caller is expected to already
    /// hand this a deduplicated membership list (`adopt_membership`'s
    /// `sort_unstable` + `dedup`, and `migration_rings`'s copy of the
    /// same as of this issue) — a repeated name otherwise makes
    /// `is_owner` and `owners` silently disagree. `owners` scores every
    /// element of the backing list independently, so one name occupying
    /// more than one slot inflates its share of the top-`replicas`,
    /// while `is_owner` (via `self.nodes.iter().position(..)` and the
    /// `node == name` skip below it) treats every occurrence of a name
    /// as the same single member. Deduplicating here — keeping the
    /// first occurrence, so construction order is otherwise unaffected —
    /// guards against a duplicate slipping through some caller this
    /// doesn't know about, at a cost (one hash-set pass) construction
    /// wasn't on any per-request hot path to begin with.
    pub fn new(nodes: Vec<String>) -> Self {
        let mut seen = HashSet::with_capacity(nodes.len());
        let nodes: Vec<String> = nodes
            .into_iter()
            .filter(|node| seen.insert(node.clone()))
            .collect();
        let node_hashes = nodes.iter().map(|node| fnv1a(node.as_bytes())).collect();
        Self { nodes, node_hashes }
    }

    /// The member names this ring was built from, in construction order.
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    /// The key's owners: the `replicas` highest-scoring nodes, primary
    /// first. Returns fewer than `replicas` when the cluster is smaller.
    pub fn owners(&self, key: &Key, replicas: usize) -> Vec<&str> {
        self.owners_hashed(KeyHash::of(key), replicas)
    }

    /// `owners` for a key whose hash is already known.
    pub fn owners_hashed(&self, key_hash: KeyHash, replicas: usize) -> Vec<&str> {
        let key_hash = key_hash.0;

        let mut scored: Vec<(u64, &str)> = self
            .node_hashes
            .iter()
            .zip(&self.nodes)
            .map(|(node_hash, node)| (fmix64(node_hash ^ key_hash), node.as_str()))
            .collect();

        // Descending by score; ties toward the lexicographically smaller
        // name. Total order, so every implementation agrees.
        let by_score = |a: &(u64, &str), b: &(u64, &str)| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1));

        let top = replicas.min(scored.len());
        if top > 0 && top < scored.len() {
            // Only the top `replicas` are ever returned, so avoid paying
            // for a full O(n log n) sort of every node in the cluster on
            // every lookup: `select_nth_unstable_by` partitions in O(n)
            // so `scored[..top]` holds exactly the `top` best-ranked
            // nodes by `by_score` (in arbitrary order within that
            // prefix), leaving only those `top` elements to actually
            // sort below.
            scored.select_nth_unstable_by(top - 1, by_score);
        }
        scored.truncate(top);
        scored.sort_unstable_by(by_score);

        scored.into_iter().map(|(_, node)| node).collect()
    }

    /// Whether `name` is one of the key's `replicas` owners. Production
    /// code hashes once and uses `is_owner_hashed`; this stays for tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_owner(&self, key: &Key, name: &str, replicas: usize) -> bool {
        self.is_owner_hashed(KeyHash::of(key), name, replicas)
    }

    /// `is_owner` for a key whose hash is already known.
    pub fn is_owner_hashed(&self, key_hash: KeyHash, name: &str, replicas: usize) -> bool {
        if replicas == 0 {
            return false;
        }

        let Some(name_index) = self.nodes.iter().position(|node| node == name) else {
            return false;
        };

        // Avoids `owners`'s `Vec` allocations (the scored buffer and its
        // output) entirely: `name` is an owner iff fewer than `replicas`
        // other nodes outrank it by the same order `owners` sorts
        // by, which this can count directly and — unlike building and
        // sorting the whole scored list — give up on as soon as that
        // count is reached, without scoring the rest of the cluster.
        let key_hash = key_hash.0;
        let name_score = fmix64(self.node_hashes[name_index] ^ key_hash);

        let mut better_ranked = 0usize;
        for (node_hash, node) in self.node_hashes.iter().zip(&self.nodes) {
            if node == name {
                continue;
            }

            let score = fmix64(node_hash ^ key_hash);
            let ranks_better = score > name_score || (score == name_score && node.as_str() < name);

            if ranks_better {
                better_ranked += 1;
                if better_ranked >= replicas {
                    return false;
                }
            }
        }

        true
    }

    /// The key's primary — `owners(key, 1)[0]`. Panics on an empty ring,
    /// which no caller constructs. Production code works in top-R terms
    /// (`owners`/`is_owner`); this stays for tests and for parity with
    /// the SDKs' primary-routing.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn route(&self, key: &Key) -> &str {
        self.owners(key, 1)[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;

    fn ring(names: &[&str]) -> HashRing {
        HashRing::new(names.iter().map(|name| name.to_string()).collect())
    }

    fn key(name: &[u8]) -> Key {
        Key::unnamespaced(Bytes::copy_from_slice(name))
    }

    fn namespaced(namespace: &[u8], name: &[u8]) -> Key {
        Key::new(
            Bytes::copy_from_slice(namespace),
            Bytes::copy_from_slice(name),
        )
    }

    #[test]
    fn routes_a_key_to_one_of_the_given_nodes() {
        let ring = ring(&["a", "b"]);
        let node = ring.route(&key(b"hello"));
        assert!(node == "a" || node == "b");
    }

    #[test]
    fn routing_is_deterministic() {
        let ring = ring(&["a", "b", "c"]);
        let first = ring.route(&key(b"some-key")).to_string();
        for _ in 0..100 {
            assert_eq!(ring.route(&key(b"some-key")), first);
        }
    }

    #[test]
    fn different_ring_instances_from_the_same_node_list_agree() {
        let nodes = vec!["10.0.0.1:8356".to_string(), "10.0.0.2:8356".to_string()];
        let ring_a = HashRing::new(nodes.clone());
        let ring_b = HashRing::new(nodes);
        assert_eq!(ring_a.route(&key(b"x")), ring_b.route(&key(b"x")));
    }

    #[test]
    fn owners_are_distinct_and_capped_by_cluster_size() {
        let ring = ring(&["a", "b", "c"]);
        let owners = ring.owners(&key(b"some-key"), 2);
        assert_eq!(owners.len(), 2);
        assert_ne!(owners[0], owners[1]);
        assert_eq!(ring.owners(&key(b"some-key"), 10).len(), 3);
    }

    #[test]
    fn adding_a_node_never_reorders_the_existing_nodes() {
        // The HRW property client-side replication's handoff and cleanup rules rest on.
        let before = ring(&["a", "b", "c"]);
        let after = ring(&["a", "b", "c", "d"]);

        for i in 0..500 {
            let key = format!("key-{i}");
            let old: Vec<&str> = before.owners(&Key::from(Bytes::from(key.clone())), 3);
            let new: Vec<&str> = after
                .owners(&Key::from(Bytes::from(key.clone())), 4)
                .into_iter()
                .filter(|node| *node != "d")
                .collect();
            assert_eq!(old, new, "relative order changed for {key}");
        }
    }

    /// The original one-shot key hash (the form `KeyHash` replaced),
    /// kept as the reference the streaming forms must equal byte for byte.
    fn key_hash(key: &Key) -> u64 {
        if !key.is_namespaced() {
            return fnv1a(&key.name);
        }

        let namespace_length = u32::try_from(key.namespace.len()).unwrap().to_be_bytes();
        let hash = fnv1a(&namespace_length);
        let hash = fnv1a_continue(hash, &key.namespace);
        fnv1a_continue(hash, &key.name)
    }

    #[test]
    fn the_streaming_key_hash_equals_the_one_shot_hash() {
        let big = vec![0xa5u8; 5_000];
        let cases: [(&[u8], &[u8]); 7] = [
            (b"", b""),
            (b"", b"alpha"),
            (b"users", b""),
            (b"users", b"alpha"),
            (b"\xff\x00", b"be\x00ta"),
            (&big, b"k"),
            (b"a", &big),
        ];

        let mut hasher = KeyHasher::default();
        for (namespace, name) in cases {
            let k = namespaced(namespace, name);
            assert_eq!(KeyHash::of(&k).0, key_hash(&k), "{namespace:?}/{name:?}");
            assert_eq!(hasher.hash(&k).0, key_hash(&k), "{namespace:?}/{name:?}");
        }
        // The pinned vectors, through the new paths as well.
        assert_eq!(
            KeyHash::of(&namespaced(b"users", b"alpha")).0,
            NS_USERS_ALPHA
        );
        assert_eq!(KeyHash::of(&namespaced(b"users", b"")).0, NS_USERS_EMPTY);
        assert_eq!(
            KeyHash::of(&namespaced(b"\xff\x00", b"beta")).0,
            NS_BINARY_BETA
        );
    }

    #[test]
    fn hashed_lookups_agree_with_the_key_lookups() {
        let ring = ring(&["a", "b", "c", "d"]);
        let mut hasher = KeyHasher::default();

        for i in 0..200 {
            let k = namespaced(
                format!("ns-{}", i % 3).as_bytes(),
                format!("k{i}").as_bytes(),
            );
            let hash = hasher.hash(&k);
            assert_eq!(ring.owners_hashed(hash, 2), ring.owners(&k, 2));
            for node in ["a", "b", "c", "d", "e"] {
                assert_eq!(
                    ring.is_owner_hashed(hash, node, 2),
                    ring.is_owner(&k, node, 2)
                );
            }
        }
    }

    #[test]
    fn per_key_hash_work_does_not_scale_with_the_namespace_length() {
        // Count-based, not wall-clock: bytes fed through FNV for N keys of
        // one L-byte namespace must be L + sum(|name|) + the 4-byte
        // prefix, not N x L.
        const NAMESPACE_LEN: usize = 100_000;
        const KEYS: usize = 500;
        let namespace = Bytes::from(vec![b'n'; NAMESPACE_LEN]);
        let names: Vec<Bytes> = (0..KEYS)
            .map(|i| Bytes::from(format!("{i:04}").into_bytes()))
            .collect();
        let names_len: usize = names.iter().map(Bytes::len).sum();

        let start = hashed_bytes();
        let mut hasher = KeyHasher::default();
        for name in &names {
            hasher.hash(&Key::new(namespace.clone(), name.clone()));
        }
        assert_eq!(hashed_bytes() - start, 4 + NAMESPACE_LEN + names_len);

        // The per-key form pays the namespace every time — the cost the
        // hasher exists to avoid.
        let start = hashed_bytes();
        for name in &names {
            KeyHash::of(&Key::new(namespace.clone(), name.clone()));
        }
        assert_eq!(
            hashed_bytes() - start,
            KEYS * (4 + NAMESPACE_LEN) + names_len
        );
    }

    #[test]
    fn the_hasher_recomputes_when_the_namespace_changes() {
        let mut hasher = KeyHasher::default();
        let a = namespaced(b"aaa", b"k");
        let b = namespaced(b"bbb", b"k");

        assert_eq!(hasher.hash(&a).0, key_hash(&a));
        assert_eq!(hasher.hash(&b).0, key_hash(&b));
        assert_eq!(hasher.hash(&a).0, key_hash(&a));
        assert_ne!(hasher.hash(&a), hasher.hash(&b));
    }

    #[test]
    fn matches_the_known_fnv1a_of_an_empty_string() {
        // FNV-1a's published test vector for the empty byte string.
        assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
    }

    #[test]
    fn matches_the_cross_language_score_vectors() {
        // Pinned outputs of the full score pipeline
        // (fmix64(fnv1a(name) ^ fnv1a(key))) — the TypeScript SDK asserts
        // these exact values, so both implementations agree byte-for-byte
        // or one of these tests fails.
        assert_eq!(fmix64(0), 0);
        assert_eq!(fmix64(1), 0xb456bcfc34c2cb2c);
        assert_eq!(fmix64(0xcbf29ce484222325), 0xefd01f60ba992926);

        let ring = ring(&["node-a", "node-b", "node-c"]);
        assert_eq!(
            ring.owners(&key(b"alpha"), 3),
            vec!["node-c", "node-b", "node-a"]
        );
        assert_eq!(
            ring.owners(&key(b"beta"), 3),
            vec!["node-a", "node-c", "node-b"]
        );
        assert_eq!(
            ring.owners(&key(b""), 3),
            vec!["node-a", "node-b", "node-c"]
        );

        // Namespaced form (issue #105): `fnv1a(be32(len(ns)) || ns || key)`.
        // Every SDK asserts these same vectors.
        assert_eq!(key_hash(&namespaced(b"users", b"alpha")), NS_USERS_ALPHA);
        assert_eq!(key_hash(&namespaced(b"users", b"")), NS_USERS_EMPTY);
        assert_eq!(key_hash(&namespaced(b"\xff\x00", b"beta")), NS_BINARY_BETA);
        assert_eq!(
            ring.owners(&namespaced(b"users", b"alpha"), 3),
            NS_USERS_ALPHA_OWNERS
        );
        assert_eq!(
            ring.owners(&namespaced(b"users", b""), 3),
            NS_USERS_EMPTY_OWNERS
        );
        assert_eq!(
            ring.owners(&namespaced(b"\xff\x00", b"beta"), 3),
            NS_BINARY_BETA_OWNERS
        );
    }

    #[test]
    fn the_default_namespace_hashes_exactly_like_the_legacy_form() {
        // Rolling-upgrade invariant: an un-namespaced key's placement must
        // not move when the server learns about namespaces.
        assert_eq!(key_hash(&key(b"alpha")), fnv1a(b"alpha"));
        assert_eq!(key_hash(&key(b"")), fnv1a(b""));
    }

    #[test]
    fn namespace_and_key_boundaries_are_unambiguous() {
        // A delimiter-free split: the length prefix keeps `("ab","c")`
        // and `("a","bc")` apart, and a namespaced key never collides
        // with the un-namespaced concatenation.
        assert_ne!(
            key_hash(&namespaced(b"ab", b"c")),
            key_hash(&namespaced(b"a", b"bc"))
        );
        assert_ne!(key_hash(&namespaced(b"ab", b"c")), key_hash(&key(b"abc")));
    }

    #[test]
    fn namespaces_spread_a_shared_singleton_key_over_different_nodes() {
        // The reason the namespace is part of the hash input at all.
        let ring = ring(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        let primaries: std::collections::HashSet<&str> = (0..64)
            .map(|i| ring.route(&namespaced(format!("cache-{i}").as_bytes(), b"config")))
            .collect();
        assert!(primaries.len() > 1);
    }

    #[test]
    fn duplicate_names_are_deduplicated_at_construction() {
        // Issue #328: a repeated name in the input list must not survive
        // into `nodes` — that's what keeps `is_owner` and `owners`
        // agreeing (see `HashRing::new`'s doc comment).
        let ring = HashRing::new(
            ["a", "b", "b", "c", "a"]
                .iter()
                .map(|name| name.to_string())
                .collect(),
        );
        assert_eq!(
            ring.nodes(),
            &["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn owners_and_is_owner_agree_even_if_a_duplicate_name_slips_through() {
        // Regression (issue #328): before `HashRing::new` deduplicated,
        // `owners` scored every element of the backing `Vec`
        // independently — so a name repeated N times could occupy N
        // slots of the returned top-`replicas` — while `is_owner`
        // treated every occurrence of a name as the same single member
        // (it looks up one index, then skips every node whose name
        // equals it while counting how many others outrank it). The two
        // disagreed on whether a duplicated node was "an owner".
        let ring = HashRing::new(
            ["a", "b", "b", "b", "c", "d"]
                .iter()
                .map(|name| name.to_string())
                .collect(),
        );
        let k = key(b"some-key");

        for replicas in 0..=5 {
            let owners = ring.owners(&k, replicas);
            for name in ["a", "b", "c", "d"] {
                assert_eq!(
                    owners.contains(&name),
                    ring.is_owner(&k, name, replicas),
                    "name={name} replicas={replicas} owners={owners:?}"
                );
            }
            // No name should ever occupy more than one slot in the
            // returned owner set.
            let mut sorted = owners.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), owners.len(), "owners={owners:?}");
        }
    }

    // `key_hash` for (ns, key), computed independently in Python from the
    // spec's definition, then the top-3 over `node-a`/`node-b`/`node-c`.
    const NS_USERS_ALPHA: u64 = 0xfd4ab55027c21df6;
    const NS_USERS_EMPTY: u64 = 0xa9e9bbca44bb502e;
    const NS_BINARY_BETA: u64 = 0x8f7c097eccb8e792;
    const NS_USERS_ALPHA_OWNERS: [&str; 3] = ["node-a", "node-c", "node-b"];
    const NS_USERS_EMPTY_OWNERS: [&str; 3] = ["node-b", "node-c", "node-a"];
    const NS_BINARY_BETA_OWNERS: [&str; 3] = ["node-b", "node-a", "node-c"];
}
