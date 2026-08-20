package org.nanocached;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Rendezvous (highest-random-weight) hashing over a fixed node list (see
 * doc/adr/0011-*.md in the nanocached repository). This is deliberately a
 * byte-for-byte port of the same computation every other nanocached
 * participant uses (the Rust node, the Go, TypeScript, Python, Rust, and
 * .NET SDKs) — not just "a" rendezvous hash, but <em>this specific</em>
 * one: if this SDK's ranking disagreed with a node's own copy, the two
 * would disagree about which nodes hold a key. Cross-language test
 * vectors pin the pipeline.
 *
 * <p>For each (node, key) pair, {@code score = fmix64(fnv1a(name) ^
 * fnv1a(key))}; a key's owners are the {@code replicas} highest-scoring
 * nodes in descending <em>unsigned</em> score order (ties — effectively
 * impossible at 64 bits — break toward the lexicographically smaller
 * name), and its primary is the top one.
 *
 * <p>Built from node <em>names</em>, not addresses (doc/adr/0009-*.md).
 */
public final class HashRing {
    private final List<String> nodes;
    private final long[] nodeHashes;

    public HashRing(List<String> nodes) {
        this.nodes = List.copyOf(nodes);
        this.nodeHashes = new long[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            this.nodeHashes[i] = fnv1a(nodes.get(i).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** FNV-1a over 64 bits; Java's long arithmetic wraps exactly like Rust's u64. */
    static long fnv1a(byte[] data) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : data) {
            hash ^= (b & 0xffL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    /** MurmurHash3's 64-bit finalizer: the full-avalanche mix FNV-1a lacks. */
    static long fmix64(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return value;
    }

    /**
     * The key's owners: the {@code replicas} highest-scoring nodes,
     * primary first. Returns fewer when the cluster is smaller.
     *
     * <p>Top-{@code replicas} selection via a bounded max-heap of the
     * {@code replicas} best candidates seen so far (O(n log replicas))
     * instead of sorting every node (O(n log n)) — replicas is typically a
     * small constant (2-5) while a cluster can have many nodes, so this
     * avoids paying for a full ranking this call only ever uses the front
     * of. Produces the identical ordering a full sort would (same
     * comparator, {@link #order}), just without sorting the nodes this
     * call discards.
     */
    public List<String> owners(byte[] key, int replicas) {
        long keyHash = fnv1a(key);
        int limit = Math.min(replicas, nodes.size());
        if (limit <= 0) return List.of();

        // The heap holds the `limit` best candidates seen so far, ordered
        // by `order` REVERSED — so its root (PriorityQueue.peek(), the
        // "least" element under whatever comparator it's given) is always
        // the WORST of those kept, the one to evict when a better
        // candidate shows up.
        PriorityQueue<Scored> heap = new PriorityQueue<>(limit, ORDER.reversed());
        for (int i = 0; i < nodes.size(); i++) {
            Scored candidate = new Scored(fmix64(nodeHashes[i] ^ keyHash), nodes.get(i));
            if (heap.size() < limit) {
                heap.add(candidate);
            } else if (ORDER.compare(candidate, heap.peek()) < 0) {
                heap.poll();
                heap.add(candidate);
            }
        }

        List<Scored> best = new ArrayList<>(heap);
        best.sort(ORDER);
        List<String> result = new ArrayList<>(best.size());
        for (Scored s : best) result.add(s.node());
        return result;
    }

    private record Scored(long score, String node) {}

    // Descending by UNSIGNED score; ties toward the lexicographically
    // smaller name — a total order every implementation agrees on.
    private static final Comparator<Scored> ORDER = (a, b) -> {
        int byScore = Long.compareUnsigned(b.score(), a.score());
        return byScore != 0 ? byScore : a.node().compareTo(b.node());
    };

    /** The key's primary — {@code owners(key, 1).get(0)}. */
    public String route(byte[] key) {
        return owners(key, 1).get(0);
    }
}
