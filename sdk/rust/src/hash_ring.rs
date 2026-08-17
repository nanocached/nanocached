//! Rendezvous (highest-random-weight) hashing over a fixed node list (see
//! doc/adr/0011-*.md in the nanocached repository). This is deliberately a
//! byte-for-byte port of the same computation every other nanocached
//! participant uses (the node's own `src/hash_ring.rs`, the TypeScript,
//! Python, and Java SDKs) — not just "a" rendezvous hash, but *this
//! specific* one: if this SDK's ranking disagreed with a node's own copy,
//! the two would disagree about which nodes hold a key. Cross-language
//! test vectors pin the pipeline.
//!
//! For each (node, key) pair, `score = fmix64(fnv1a(name) ^ fnv1a(key))`;
//! a key's owners are the `replicas` highest-scoring nodes in descending
//! score order (ties — effectively impossible at 64 bits — break toward
//! the lexicographically smaller name), and its primary is the top one.
//!
//! Built from node *names*, not addresses (doc/adr/0009-*.md).

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
        scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        scored.truncate(replicas);
        scored.into_iter().map(|(_, node)| node).collect()
    }

    /// The key's primary — `owners(key, 1)[0]`. Panics on an empty ring.
    pub fn route(&self, key: &[u8]) -> &str {
        self.owners(key, 1)[0]
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
