//! Rendezvous (highest-random-weight) hashing over a fixed node list (see
//! ADR 0011, which replaced ADR 0002's virtual-node ring: FNV-1a's weak
//! high-bit avalanche clustered each node's ring points into narrow bands,
//! skewing shares by up to ~2×; HRW measures within 2% of fair and yields
//! replica sets for free). A byte-for-byte port of the same algorithm all
//! six SDKs use — not shared with them via a common library (this project's
//! binaries don't share code that way, see ADR 0006), just the same
//! computation independently implemented, the way any two independent
//! nanocached clients must agree on it.
//!
//! For each (node, key) pair, `score = fmix64(fnv1a(name) ^ fnv1a(key))`;
//! a key's owners are the `replicas` highest-scoring nodes in descending
//! score order (ties — effectively impossible at 64 bits — break toward
//! the lexicographically smaller name), and its primary is the top one.
//! Adding a node never reorders the existing nodes relative to each other,
//! which is what keeps ADR 0011's join handoff and replica cleanup local:
//! per affected key, exactly one node is displaced from the top-R.
//!
//! `nanocached-node` needs this for ADR 0008's join: an already-ready node
//! must compute, for each key it holds, how the key's top-R changes when a
//! new node is added — who sends the joining node its copy (the old
//! primary), and whether this node's own copy just became dead (displaced
//! from rank R, see ADR 0011).

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
    pub fn new(nodes: Vec<String>) -> Self {
        let node_hashes = nodes.iter().map(|node| fnv1a(node.as_bytes())).collect();
        Self { nodes, node_hashes }
    }

    /// The key's owners: the `replicas` highest-scoring nodes, primary
    /// first. Returns fewer than `replicas` when the cluster is smaller.
    pub fn owners(&self, key: &[u8], replicas: usize) -> Vec<&str> {
        let key_hash = fnv1a(key);

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

    /// Whether `name` is one of the key's `replicas` owners.
    pub fn is_owner(&self, key: &[u8], name: &str, replicas: usize) -> bool {
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
        let key_hash = fnv1a(key);
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
    fn routes_a_key_to_one_of_the_given_nodes() {
        let ring = ring(&["a", "b"]);
        let node = ring.route(b"hello");
        assert!(node == "a" || node == "b");
    }

    #[test]
    fn routing_is_deterministic() {
        let ring = ring(&["a", "b", "c"]);
        let first = ring.route(b"some-key").to_string();
        for _ in 0..100 {
            assert_eq!(ring.route(b"some-key"), first);
        }
    }

    #[test]
    fn different_ring_instances_from_the_same_node_list_agree() {
        let nodes = vec!["10.0.0.1:8356".to_string(), "10.0.0.2:8356".to_string()];
        let ring_a = HashRing::new(nodes.clone());
        let ring_b = HashRing::new(nodes);
        assert_eq!(ring_a.route(b"x"), ring_b.route(b"x"));
    }

    #[test]
    fn owners_are_distinct_and_capped_by_cluster_size() {
        let ring = ring(&["a", "b", "c"]);
        let owners = ring.owners(b"some-key", 2);
        assert_eq!(owners.len(), 2);
        assert_ne!(owners[0], owners[1]);
        assert_eq!(ring.owners(b"some-key", 10).len(), 3);
    }

    #[test]
    fn adding_a_node_never_reorders_the_existing_nodes() {
        // The HRW property ADR-0011's handoff and cleanup rules rest on.
        let before = ring(&["a", "b", "c"]);
        let after = ring(&["a", "b", "c", "d"]);

        for i in 0..500 {
            let key = format!("key-{i}");
            let old: Vec<&str> = before.owners(key.as_bytes(), 3);
            let new: Vec<&str> = after
                .owners(key.as_bytes(), 4)
                .into_iter()
                .filter(|node| *node != "d")
                .collect();
            assert_eq!(old, new, "relative order changed for {key}");
        }
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
        assert_eq!(ring.owners(b"alpha", 3), vec!["node-c", "node-b", "node-a"]);
        assert_eq!(ring.owners(b"beta", 3), vec!["node-a", "node-c", "node-b"]);
        assert_eq!(ring.owners(b"", 3), vec!["node-a", "node-b", "node-c"]);
    }
}
