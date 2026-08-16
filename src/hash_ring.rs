//! Client-side consistent hashing over a fixed node list (see ADR 0002 and
//! 0008). A byte-for-byte port of the same algorithm `src/bin/bench.rs` and
//! the TypeScript client use (FNV-1a, 128 virtual nodes per real node) — not
//! shared with them via a common library (this project's binaries don't
//! share code that way, see ADR 0006), just the same computation
//! independently implemented, the way any two independent nanocached
//! clients must agree on it.
//!
//! `nanocached-node` needs this for ADR 0008's node join: an already-ready
//! node must compute, for each key it holds, whether a *newly joining* node
//! now owns it, by comparing the ring without the joining node against the
//! ring with it — the same computation a client does when a node is added,
//! just performed locally against the node's own key set instead of against
//! one key at a time from the outside.

const VIRTUAL_NODES_PER_NODE: u32 = 128;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub struct HashRing {
    nodes: Vec<String>,
    points: Vec<(u64, usize)>,
}

impl HashRing {
    pub fn new(nodes: Vec<String>) -> Self {
        let mut points = Vec::with_capacity(nodes.len() * VIRTUAL_NODES_PER_NODE as usize);

        for (index, node) in nodes.iter().enumerate() {
            for virtual_id in 0..VIRTUAL_NODES_PER_NODE {
                let point = fnv1a(format!("{node}#{virtual_id}").as_bytes());
                points.push((point, index));
            }
        }

        points.sort_unstable_by_key(|(point, _)| *point);

        Self { nodes, points }
    }

    pub fn route(&self, key: &[u8]) -> &str {
        let hash = fnv1a(key);
        let position = self.points.partition_point(|(point, _)| *point < hash);
        let position = if position == self.points.len() {
            0
        } else {
            position
        };

        &self.nodes[self.points[position].1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_a_key_to_one_of_the_given_nodes() {
        let ring = HashRing::new(vec!["a".to_string(), "b".to_string()]);
        let node = ring.route(b"hello");
        assert!(node == "a" || node == "b");
    }

    #[test]
    fn routing_is_deterministic() {
        let ring = HashRing::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
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
    fn matches_the_known_fnv1a_of_an_empty_string() {
        // FNV-1a's published test vector for the empty byte string.
        assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
    }
}
