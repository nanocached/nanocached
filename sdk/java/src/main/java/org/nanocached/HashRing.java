package org.nanocached;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Rendezvous (highest-random-weight) hashing over a fixed node list (the client-side
 * replication design). This is deliberately a
 * byte-for-byte port of the same computation every other nanocached
 * participant uses (the Rust node, the Go, TypeScript, Python, Rust, and
 * .NET SDKs) — not just "a" rendezvous hash, but <em>this specific</em>
 * one: if this SDK's ranking disagreed with a node's own copy, the two
 * would disagree about which nodes hold a key. Cross-language test
 * vectors pin the pipeline.
 *
 * <p>For each (node, key) pair, {@code score = fmix64(fnv1a(name) ^
 * key_hash)}; a key's owners are the {@code replicas} highest-scoring
 * nodes in descending <em>unsigned</em> score order (ties — effectively
 * impossible at 64 bits — break toward the lexicographically smaller
 * name), and its primary is the top one.
 *
 * <p>Built from node <em>names</em>, not addresses (node identity decoupled from address).
 *
 * <p>Namespaces (issue #105) enter the key side of the score, and the
 * canonical hash input is consensus-critical across the server, the six
 * SDKs and {@code verify-staged-join} (see {@code src/hash_ring.rs}'s
 * module docs for the full rationale):
 *
 * <ul>
 * <li>default (empty) namespace: {@code fnv1a(key)} — byte-identical to
 * the pre-namespace form, so every existing key keeps its placement
 * across a rolling upgrade (no cluster-wide hit-rate cliff, and
 * mixed-version clients agree on placement);
 * <li>non-empty namespace: {@code fnv1a(be32(len(ns)) || ns || key)} — the
 * namespace length as a 4-byte big-endian integer, then the namespace
 * bytes, then the key bytes, hashed as one stream. Length-prefixed so
 * {@code ("ab", "c")} and {@code ("a", "bc")} never share an input;
 * including the namespace at all is what keeps the placement balanced —
 * hashing the key alone would pile every namespace's common singleton
 * keys (e.g. {@code config}) onto the same nodes.
 * </ul>
 */
public final class HashRing {
    /** The default namespace — every un-namespaced key routes as if
     * namespaced by this, and {@link #keyHash} treats it identically to
     * "no namespace at all" (the legacy {@code fnv1a(key)} form). */
    private static final byte[] EMPTY_NAMESPACE = new byte[0];

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
        return fnv1aContinue(0xcbf29ce484222325L, data);
    }

    /** Feeds {@code data} into an in-progress FNV-1a state, so a
     * multi-part input hashes exactly as its concatenation would, with no
     * allocation — used to fold the namespace length, namespace bytes,
     * and key bytes into one stream in {@link #keyHash}. */
    private static long fnv1aContinue(long hash, byte[] data) {
        for (byte b : data) {
            hash ^= (b & 0xffL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    /** The canonical key-side hash — see the class doc for the two forms. */
    private static long keyHash(byte[] namespace, byte[] key) {
        if (namespace.length == 0) {
            return fnv1a(key);
        }

        // Namespaces are bounded by the request-size limit (1 MiB), so
        // the length always fits in 4 bytes; this is the canonical
        // encoding width every implementation uses, not a truncation
        // that could ever occur.
        int length = namespace.length;
        byte[] namespaceLength = {
                (byte) (length >>> 24), (byte) (length >>> 16), (byte) (length >>> 8), (byte) length
        };

        long hash = fnv1aContinue(0xcbf29ce484222325L, namespaceLength);
        hash = fnv1aContinue(hash, namespace);
        return fnv1aContinue(hash, key);
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
        return owners(EMPTY_NAMESPACE, key, replicas);
    }

    /** As {@link #owners(byte[], int)}, but scoped to {@code namespace}
     * (issue #105): the same key name in two namespaces routes
     * independently, since it enters the key-side hash (see the class
     * doc). An empty namespace is exactly {@link #owners(byte[], int)} —
     * the same legacy hash, so un-namespaced keys never move. */
    public List<String> owners(byte[] namespace, byte[] key, int replicas) {
        long keyHash = keyHash(namespace, key);
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
        List<String> owners = owners(key, 1);
        if (owners.isEmpty()) {
            throw new IllegalStateException("nanocached: hash ring has no nodes");
        }
        return owners.get(0);
    }
}
