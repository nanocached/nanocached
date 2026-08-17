package org.nanocached;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Rendezvous (highest-random-weight) hashing over a fixed node list (see
 * doc/adr/0011-*.md in the nanocached repository). This is deliberately a
 * byte-for-byte port of the same computation every other nanocached
 * participant uses (the Rust node, the TypeScript and Python SDKs) — not
 * just "a" rendezvous hash, but <em>this specific</em> one: if this SDK's
 * ranking disagreed with a node's own copy, the two would disagree about
 * which nodes hold a key. Cross-language test vectors pin the pipeline.
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
     */
    public List<String> owners(byte[] key, int replicas) {
        long keyHash = fnv1a(key);

        record Scored(long score, String node) {}
        List<Scored> scored = new ArrayList<>(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            scored.add(new Scored(fmix64(nodeHashes[i] ^ keyHash), nodes.get(i)));
        }

        // Descending by UNSIGNED score; ties toward the lexicographically
        // smaller name — a total order every implementation agrees on.
        scored.sort((a, b) -> {
            int byScore = Long.compareUnsigned(b.score(), a.score());
            return byScore != 0 ? byScore : a.node().compareTo(b.node());
        });

        List<String> result = new ArrayList<>(Math.min(replicas, scored.size()));
        for (int i = 0; i < Math.min(replicas, scored.size()); i++) {
            result.add(scored.get(i).node());
        }
        return result;
    }

    /** The key's primary — {@code owners(key, 1).get(0)}. */
    public String route(byte[] key) {
        return owners(key, 1).get(0);
    }
}
