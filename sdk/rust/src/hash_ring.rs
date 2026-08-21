//! Rendezvous (highest-random-weight) hashing over a fixed node list (see
//! doc/adr/0011-*.md in the nanocached repository). This is deliberately a
//! byte-for-byte port of the same computation every other nanocached
//! participant uses (the node's own `src/hash_ring.rs`, and the
//! TypeScript, Python, Java, Go, and .NET SDKs) — not just "a" rendezvous
//! hash, but *this specific* one: if this SDK's ranking disagreed with a
//! node's own copy, the two would disagree about which nodes hold a key.
//! Cross-language test vectors pin the pipeline.
//!
//! For each (node, key) pair, `score = fmix64(fnv1a(name) ^ fnv1a(key))`;
//! a key's owners are the `replicas` highest-scoring nodes in descending
//! score order (ties — effectively impossible at 64 bits — break toward
//! the lexicographically smaller name), and its primary is the top one.
//!
//! Built from node *names*, not addresses (doc/adr/0009-*.md).

use crate::error::{Error, Result};

pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// MurmurHash3's 64-bit finalizer: the full-avalanche mix FNV-1a lacks.
pub(crate) fn fmix64(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

/// A rendezvous-hash ranking over a fixed node list, built once from a
/// discovery server's node list. Ranking never changes once built.
pub struct HashRing {
    nodes: Vec<String>,
    node_hashes: Vec<u64>,
}

impl HashRing {
    pub fn new(nodes: Vec<String>) -> Self {
        let node_hashes = nodes.iter().map(|node| fnv1a(node.as_bytes())).collect();
        Self { nodes, node_hashes }
    }

    /// The key's owners: the `replicas` highest-scoring nodes, primary
    /// first. Returns fewer when the cluster is smaller.
    pub fn owners(&self, key: &[u8], replicas: usize) -> Vec<&str> {
        let key_hash = fnv1a(key);

        let mut scored: Vec<(u64, &str)> = self
            .node_hashes
            .iter()
            .zip(&self.nodes)
            .map(|(node_hash, node)| (fmix64(node_hash ^ key_hash), node.as_str()))
            .collect();

        // Descending by score; ties toward the lexicographically smaller
        // name — a total order every implementation agrees on.
        let cmp = |a: &(u64, &str), b: &(u64, &str)| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1));

        // This runs per key while the client's routing lock is held, so
        // avoid a full O(n log n) sort of every node when only the top
        // `replicas` are wanted: `select_nth_unstable_by` partitions the
        // `replicas` best candidates into the front of the slice (in
        // unspecified order among themselves) in expected linear time,
        // and only that small prefix then needs sorting. `nth` must be a
        // valid index (< len), hence the `r < scored.len()` guard — when
        // `replicas >= scored.len()` there's nothing to partition away,
        // every node is kept, and the sort below covers all of them.
        let r = replicas.min(scored.len());
        if r < scored.len() {
            scored.select_nth_unstable_by(r, cmp);
            scored.truncate(r);
        }
        scored.sort_unstable_by(cmp);
        scored.into_iter().map(|(_, node)| node).collect()
    }

    /// The key's primary — `owners(key, 1)[0]`. Returns
    /// `Err(Error::InvalidArgument)` on an empty ring instead of panicking
    /// (there is no node to name as primary), matching how every other
    /// public entry point in this crate reports "the caller passed
    /// something that could never be meant" rather than aborting.
    pub fn route(&self, key: &[u8]) -> Result<&str> {
        self.owners(key, 1).into_iter().next().ok_or_else(|| {
            Error::InvalidArgument(
                "nanocached: HashRing::route called on an empty ring".to_string(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(names: &[&str]) -> HashRing {
        HashRing::new(names.iter().map(|name| name.to_string()).collect())
    }

    #[test]
    fn matches_published_fnv1a_vectors() {
        assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn matches_the_cross_language_score_vectors() {
        // Pinned outputs of the full ADR-0011 score pipeline — the node,
        // TypeScript, Python, and Java implementations assert these too.
        assert_eq!(fmix64(0), 0);
        assert_eq!(fmix64(1), 0xb456bcfc34c2cb2c);
        assert_eq!(fmix64(0xcbf29ce484222325), 0xefd01f60ba992926);

        let ring = ring(&["node-a", "node-b", "node-c"]);
        assert_eq!(ring.owners(b"alpha", 3), vec!["node-c", "node-b", "node-a"]);
        assert_eq!(ring.owners(b"beta", 3), vec!["node-a", "node-c", "node-b"]);
        assert_eq!(ring.owners(b"", 3), vec!["node-a", "node-b", "node-c"]);
    }

    #[test]
    fn owners_are_distinct_and_capped() {
        let ring = ring(&["node-a", "node-b", "node-c"]);
        let owners = ring.owners(b"some-key", 2);
        assert_eq!(owners.len(), 2);
        assert_ne!(owners[0], owners[1]);
        assert_eq!(ring.owners(b"some-key", 10).len(), 3);
    }

    #[test]
    fn owners_matches_a_naive_full_sort_reference() {
        // `owners` now partitions out the top `replicas` instead of doing
        // a full sort of every node; this pins its output byte-identical
        // to the straightforward "sort everything, then truncate"
        // reference it replaced, across the edge cases in `replicas`
        // (0, 1, exactly the node count, and over it) plus a run of
        // pseudo-random keys. A small deterministic PRNG stands in for a
        // `rand` dev-dependency, which this is the only thing that would
        // need it.
        struct SplitMix64(u64);
        impl SplitMix64 {
            fn next(&mut self) -> u64 {
                self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = self.0;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^ (z >> 31)
            }
        }

        fn naive_owners<'a>(ring: &'a HashRing, key: &[u8], replicas: usize) -> Vec<&'a str> {
            let key_hash = fnv1a(key);
            let mut scored: Vec<(u64, &str)> = ring
                .node_hashes
                .iter()
                .zip(&ring.nodes)
                .map(|(node_hash, node)| (fmix64(node_hash ^ key_hash), node.as_str()))
                .collect();
            scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
            scored.truncate(replicas);
            scored.into_iter().map(|(_, node)| node).collect()
        }

        let node_names: Vec<String> = (0..37).map(|i| format!("node-{i}")).collect();
        let name_refs: Vec<&str> = node_names.iter().map(String::as_str).collect();
        let ring = ring(&name_refs);
        let len = node_names.len();

        let mut rng = SplitMix64(0xC0FFEE);
        for &replicas in &[0, 1, len / 2, len, len + 1, len + 10] {
            for _ in 0..200 {
                let key = format!("fuzz-key-{}", rng.next()).into_bytes();
                assert_eq!(
                    ring.owners(&key, replicas),
                    naive_owners(&ring, &key, replicas),
                    "mismatch for replicas={replicas}, key={key:?}"
                );
            }
        }
    }

    #[test]
    fn route_on_an_empty_ring_returns_invalid_argument_instead_of_panicking() {
        let empty = HashRing::new(vec![]);
        assert!(matches!(empty.route(b"k"), Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn route_on_a_non_empty_ring_returns_the_primary() {
        let ring = ring(&["node-a", "node-b", "node-c"]);
        assert_eq!(ring.route(b"alpha").unwrap(), ring.owners(b"alpha", 1)[0]);
    }

    #[test]
    fn adding_a_node_never_reorders_existing_nodes() {
        let before = ring(&["node-a", "node-b", "node-c"]);
        let after = ring(&["node-a", "node-b", "node-c", "node-d"]);
        for i in 0..500 {
            let key = format!("key-{i}");
            let new_order: Vec<&str> = after
                .owners(key.as_bytes(), 4)
                .into_iter()
                .filter(|node| *node != "node-d")
                .collect();
            assert_eq!(before.owners(key.as_bytes(), 3), new_order);
        }
    }
}
