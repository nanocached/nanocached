package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HashRingTest {
    private static final List<String> NODES = List.of("node-a", "node-b", "node-c");

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void matchesPublishedFnv1aVectors() {
        assertEquals(0xcbf29ce484222325L, HashRing.fnv1a(bytes("")));
        assertEquals(0xaf63dc4c8601ec8cL, HashRing.fnv1a(bytes("a")));
        assertEquals(0x85944171f73967e8L, HashRing.fnv1a(bytes("foobar")));
    }

    @Test
    void matchesCrossLanguageScoreVectors() {
        // Pinned outputs of the full client-side replication score pipeline — the Rust,
        // TypeScript, and Python implementations assert these too.
        assertEquals(0L, HashRing.fmix64(0));
        assertEquals(0xb456bcfc34c2cb2cL, HashRing.fmix64(1));
        assertEquals(0xefd01f60ba992926L, HashRing.fmix64(0xcbf29ce484222325L));

        HashRing ring = new HashRing(NODES);
        assertEquals(List.of("node-c", "node-b", "node-a"), ring.owners(bytes("alpha"), 3));
        assertEquals(List.of("node-a", "node-c", "node-b"), ring.owners(bytes("beta"), 3));
        assertEquals(List.of("node-a", "node-b", "node-c"), ring.owners(bytes(""), 3));
    }

    // Namespaces (issue #105): `key_hash = fnv1a(be32(len(ns)) || ns || key)`
    // for a non-empty namespace. Every SDK and the server pin these same
    // owner-order vectors (computed independently in Python from the
    // spec's definition) over the identical node-a/node-b/node-c ring, so
    // a disagreement here means this SDK's routing would silently diverge
    // from the rest of the cluster.
    @Test
    void matchesCrossLanguageNamespacedVectors() {
        HashRing ring = new HashRing(NODES);
        assertEquals(List.of("node-a", "node-c", "node-b"), ring.owners(bytes("users"), bytes("alpha"), 3));
        assertEquals(List.of("node-b", "node-c", "node-a"), ring.owners(bytes("users"), bytes(""), 3));
        assertEquals(List.of("node-b", "node-a", "node-c"),
                ring.owners(new byte[] {(byte) 0xff, 0x00}, bytes("beta"), 3));
    }

    @Test
    void theDefaultNamespaceHashesExactlyLikeTheLegacyForm() {
        // Rolling-upgrade invariant: an un-namespaced key's placement must
        // not move when the server (and this SDK) learns about namespaces.
        HashRing ring = new HashRing(NODES);
        assertEquals(ring.owners(bytes("alpha"), 3), ring.owners(bytes(""), bytes("alpha"), 3));
        assertEquals(ring.owners(bytes(""), 3), ring.owners(bytes(""), bytes(""), 3));
    }

    @Test
    void namespaceAndKeyBoundariesAreUnambiguous() {
        // A delimiter-free split: the length prefix keeps ("ab","c") and
        // ("a","bc") apart, and a namespaced key never collides with the
        // un-namespaced concatenation.
        HashRing ring = new HashRing(NODES);
        assertNotEquals(
                ring.owners(bytes("ab"), bytes("c"), 3),
                ring.owners(bytes("a"), bytes("bc"), 3));
        assertNotEquals(
                ring.owners(bytes("ab"), bytes("c"), 3),
                ring.owners(bytes(""), bytes("abc"), 3));
    }

    @Test
    void namespacesSpreadASharedSingletonKeyOverDifferentNodes() {
        // The reason the namespace is part of the hash input at all: two
        // namespaces sharing a common singleton key name (e.g. "config")
        // must not always land on the same node.
        List<String> nodes = new ArrayList<>();
        for (char c = 'a'; c <= 'h'; c++) nodes.add("node-" + c);
        HashRing ring = new HashRing(nodes);
        Map<String, Integer> primaries = new HashMap<>();
        for (int i = 0; i < 64; i++) {
            String primary = ring.owners(bytes("cache-" + i), bytes("config"), 1).get(0);
            primaries.merge(primary, 1, Integer::sum);
        }
        assertTrue(primaries.size() > 1);
    }

    @Test
    void ownersAreDistinctAndCapped() {
        HashRing ring = new HashRing(NODES);
        List<String> owners = ring.owners(bytes("some-key"), 2);
        assertEquals(2, owners.size());
        assertNotEquals(owners.get(0), owners.get(1));
        assertEquals(3, ring.owners(bytes("some-key"), 10).size());
    }

    @Test
    void addingANodeNeverReordersExistingNodes() {
        HashRing before = new HashRing(NODES);
        HashRing after = new HashRing(List.of("node-a", "node-b", "node-c", "node-d"));
        for (int i = 0; i < 500; i++) {
            byte[] key = bytes("key-" + i);
            List<String> newOrder = after.owners(key, 4).stream()
                    .filter(node -> !node.equals("node-d"))
                    .toList();
            assertEquals(before.owners(key, 3), newOrder, "reordered for key-" + i);
        }
    }

    // Pins owners()' bounded-heap top-R selection byte-identical to the
    // straightforward "score every node, sort descending, truncate"
    // reference it replaced (see HashRing.owners' doc comment) — the
    // bounded selection must never change which nodes come back, or
    // their order, versus a full sort using the identical comparator
    // (descending unsigned score; ties toward the lexicographically
    // smaller name). Mirrors the Go SDK's
    // TestOwnersMatchesANaiveFullSortReference.
    private static List<String> naiveOwners(HashRing ring, List<String> nodes, byte[] key, int replicas) {
        long keyHash = HashRing.fnv1a(key);
        record Scored(long score, String node) {}
        Comparator<Scored> order = (a, b) -> {
            int byScore = Long.compareUnsigned(b.score(), a.score());
            return byScore != 0 ? byScore : a.node().compareTo(b.node());
        };
        List<Scored> ranked = new ArrayList<>();
        for (String node : nodes) {
            ranked.add(new Scored(HashRing.fmix64(HashRing.fnv1a(bytes(node)) ^ keyHash), node));
        }
        ranked.sort(order);
        int limit = Math.min(replicas, ranked.size());
        if (limit <= 0) return List.of();
        List<String> result = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) result.add(ranked.get(i).node());
        return result;
    }

    @Test
    void ownersMatchesANaiveFullSortReference() {
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < 37; i++) nodes.add("node-" + i);
        HashRing ring = new HashRing(nodes);

        // A tiny deterministic PRNG (splitmix64) rather than java.util.Random,
        // keeping this test free of any flakiness tied to a seeded
        // default source (mirrors the Go SDK's reference test).
        long[] state = {0xC0FFEEL};
        java.util.function.LongSupplier next = () -> {
            state[0] += 0x9E3779B97F4A7C15L;
            long z = state[0];
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        };

        for (int replicas : new int[] {0, 1, nodes.size() / 2, nodes.size(), nodes.size() + 1, nodes.size() + 10}) {
            for (int i = 0; i < 200; i++) {
                byte[] key = bytes("fuzz-key-" + next.getAsLong());
                assertEquals(naiveOwners(ring, nodes, key, replicas), ring.owners(key, replicas),
                        "replicas=" + replicas + " key=" + new String(key, StandardCharsets.UTF_8));
            }
        }
    }

    // Regression for issue #461 (mirrors src/hash_ring.rs's HashRing::new
    // dedupe from issue #328, and the Python/Go SDK fixes for the same,
    // issues #360/#389): before this fix, a name repeated in the
    // constructor's node list scored independently for each of its
    // slots, so owners() could return the same node name more than once
    // in the top-`replicas` set — inflating its effective share of the
    // ring.
    @Test
    void duplicateNodeNamesAreDeduplicated() {
        HashRing ring = new HashRing(List.of("a", "b", "b", "b", "c", "d"));
        byte[] key = bytes("some-key");

        for (int replicas = 0; replicas <= 5; replicas++) {
            List<String> owners = ring.owners(key, replicas);
            assertEquals(owners.size(), new java.util.HashSet<>(owners).size(),
                    "replicas=" + replicas + " owners=" + owners);
        }

        // Construction order is otherwise unaffected: the first
        // occurrence of a repeated name is kept in place.
        HashRing deduped = new HashRing(List.of("a", "b", "c", "d"));
        for (int i = 0; i < 200; i++) {
            byte[] k = bytes("key-" + i);
            assertEquals(deduped.owners(k, 4), ring.owners(k, 4), "key-" + i);
        }
    }

    @Test
    void spreadsKeysEvenly() {
        HashRing ring = new HashRing(NODES);
        Map<String, Integer> counts = new HashMap<>();
        int total = 3000;
        for (int i = 0; i < total; i++) {
            counts.merge(ring.route(bytes("key-" + i)), 1, Integer::sum);
        }
        double fair = (double) total / NODES.size();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            assertTrue(Math.abs(entry.getValue() - fair) / fair < 0.15,
                    entry.getKey() + " got " + entry.getValue() + "/" + total);
        }
    }
}
