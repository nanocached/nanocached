package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
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
        // Pinned outputs of the full ADR-0011 score pipeline — the Rust,
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
