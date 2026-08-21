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
