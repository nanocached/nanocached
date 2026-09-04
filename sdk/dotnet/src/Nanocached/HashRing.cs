using System.Buffers.Binary;
using System.Text;

namespace Nanocached;

/// <summary>
/// Rendezvous (highest-random-weight) hashing over a fixed node list (the
/// client-side replication design). This is deliberately a
/// byte-for-byte port of the same computation every other nanocached
/// participant uses (the Rust node, the Go/TypeScript/Python/Java/Rust SDKs)
/// — not just "a" rendezvous hash, but <em>this specific</em> one: if this
/// SDK's ranking disagreed with a node's own copy, the two would disagree
/// about which nodes hold a key. Cross-language test vectors pin the
/// pipeline.
///
/// For each (node, key) pair,
/// <c>score = fmix64(fnv1a(name) ^ key_hash)</c>; a key's owners are the
/// <c>replicas</c> highest-scoring nodes in descending (unsigned) score
/// order (ties — effectively impossible at 64 bits — break toward the
/// lexicographically smaller name), and its primary is the top one.
///
/// Built from node <em>names</em>, not addresses (node identity decoupled from address).
///
/// <para>Namespaces (issue #105) enter the key side of the score via
/// <see cref="KeyHash"/> — see that method's doc comment for the two
/// forms. Every <c>Owners</c>/<c>Route</c> overload below that omits a
/// namespace is exactly the empty-namespace case (a thin wrapper), so
/// existing callers are unaffected.</para>
/// </summary>
public sealed class HashRing
{
    private readonly string[] _nodes;
    private readonly ulong[] _nodeHashes;

    /// <summary>
    /// Builds the ring over <paramref name="nodes"/>, deduplicating
    /// repeated names first-occurrence-wins (issue #461, mirroring the
    /// server's <c>HashRing::new</c> dedupe (issue #328, src/hash_ring.rs)
    /// and the Go/Python SDKs' own fix, issue #360). Without
    /// this, a name repeated in <paramref name="nodes"/> would score
    /// independently for each of its slots in
    /// <see cref="Owners(ReadOnlySpan{byte}, int)"/>'s
    /// bounded top-<c>replicas</c> insertion, letting a duplicated node
    /// occupy more than one replica slot and inflating its effective
    /// share of the ring. Construction order is otherwise unaffected.
    /// </summary>
    public HashRing(IReadOnlyList<string> nodes)
    {
        var seen = new HashSet<string>(nodes.Count);
        var deduped = new List<string>(nodes.Count);
        foreach (string node in nodes)
        {
            if (seen.Add(node))
            {
                deduped.Add(node);
            }
        }

        _nodes = deduped.ToArray();
        _nodeHashes = _nodes.Select(node => Fnv1a(Encoding.UTF8.GetBytes(node))).ToArray();
    }

    /// <summary>FNV-1a over 64 bits; C#'s ulong arithmetic wraps like Rust's u64.</summary>
    internal static ulong Fnv1a(ReadOnlySpan<byte> data) => Fnv1aContinue(0xcbf29ce484222325, data);

    /// <summary>Feeds <paramref name="data"/> into an in-progress FNV-1a
    /// state, so a multi-part input hashes exactly as its concatenation
    /// would — with no allocation to actually build that concatenation.
    /// <see cref="KeyHash"/> uses this to stream namespace-length,
    /// namespace, and key through one FNV-1a pass (issue #105).</summary>
    internal static ulong Fnv1aContinue(ulong hash, ReadOnlySpan<byte> data)
    {
        foreach (byte b in data)
        {
            hash ^= b;
            hash *= 0x100000001b3;
        }
        return hash;
    }

    /// <summary>
    /// The canonical key-side hash (issue #105) — consensus-critical
    /// across the server, this SDK, and the other five: every
    /// implementation must agree byte-for-byte or two clients would
    /// disagree about which nodes hold a key.
    ///
    /// <list type="bullet">
    /// <item>default (empty) namespace: <c>fnv1a(key)</c> — byte-identical
    /// to the pre-namespace form, so every existing key keeps its
    /// placement across a rolling upgrade (no cluster-wide hit-rate cliff,
    /// and mixed-version clients agree on placement);</item>
    /// <item>non-empty namespace: <c>fnv1a(be32(len(ns)) || ns || key)</c>
    /// — the namespace length as a 4-byte big-endian integer, then the
    /// namespace bytes, then the key bytes, hashed as one stream.
    /// Length-prefixed so <c>("ab", "c")</c> and <c>("a", "bc")</c> never
    /// share an input; including the namespace at all is what keeps
    /// placement balanced — hashing the key alone would pile every
    /// namespace's common singleton keys (e.g. <c>config</c>) onto the
    /// same nodes.</item>
    /// </list>
    /// </summary>
    internal static ulong KeyHash(ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> key)
    {
        if (namespaceBytes.Length == 0)
        {
            return Fnv1a(key);
        }

        // Namespaces are bounded by the request-size limit (1 MiB), so the
        // length always fits in a u32 — the canonical encoding width every
        // implementation uses, not a truncation that could ever occur.
        Span<byte> namespaceLength = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(namespaceLength, (uint)namespaceBytes.Length);

        ulong hash = Fnv1a(namespaceLength);
        hash = Fnv1aContinue(hash, namespaceBytes);
        return Fnv1aContinue(hash, key);
    }

    /// <summary>MurmurHash3's 64-bit finalizer: the full-avalanche mix FNV-1a lacks.</summary>
    internal static ulong Fmix64(ulong value)
    {
        value ^= value >> 33;
        value *= 0xff51afd7ed558ccd;
        value ^= value >> 33;
        value *= 0xc4ceb9fe1a85ec53;
        value ^= value >> 33;
        return value;
    }

    /// <summary>
    /// The key's owners: the <c>replicas</c> highest-scoring nodes,
    /// primary first. Returns fewer when the cluster is smaller.
    ///
    /// <para>Top-<c>replicas</c> selection via a bounded insertion of the
    /// <c>replicas</c> best candidates seen so far (O(n * replicas))
    /// instead of sorting every node (O(n log n)) — this runs per key
    /// while the client's routing lock is held
    /// (<see cref="NanocachedClient"/>'s <c>_stateLock</c>), and
    /// <c>replicas</c> is typically small (a handful) next to the cluster
    /// size, so this avoids paying for a full ranking this call only ever
    /// uses the front of. Mirrors the Go SDK's <c>HashRing.Owners</c>
    /// (sdk/go/hashring.go) and the Java SDK's PriorityQueue-based
    /// approach (HashRing.java); produces the identical ordering a full
    /// sort would (same comparator, <see cref="Less"/>), just without
    /// sorting the nodes this call discards.</para>
    /// </summary>
    public IReadOnlyList<string> Owners(ReadOnlySpan<byte> key, int replicas) =>
        Owners(ReadOnlySpan<byte>.Empty, key, replicas);

    /// <summary>
    /// As <see cref="Owners(ReadOnlySpan{byte}, int)"/>, scoped to
    /// <paramref name="namespaceBytes"/> (issue #105): the same key name in
    /// two namespaces is two independent entries, routed via
    /// <see cref="KeyHash"/> and (possibly) owned by different nodes. The
    /// empty namespace routes identically to the single-span overload — see
    /// <see cref="KeyHash"/> for why that equivalence holds byte-for-byte.
    /// </summary>
    public IReadOnlyList<string> Owners(ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> key, int replicas)
    {
        ulong keyHash = KeyHash(namespaceBytes, key);
        int limit = Math.Min(replicas, _nodes.Length);
        if (limit <= 0) return Array.Empty<string>();

        // `top` holds the `limit` best candidates seen so far, sorted
        // best-first; a better candidate is inserted in place, evicting
        // the current worst kept candidate (top[^1]) once `top` is full.
        var top = new List<(ulong Score, string Node)>(limit);
        for (int i = 0; i < _nodes.Length; i++)
        {
            var candidate = (Fmix64(_nodeHashes[i] ^ keyHash), _nodes[i]);
            if (top.Count == limit && !Less(candidate, top[^1]))
            {
                continue; // no better than the worst candidate currently kept
            }

            int pos = top.Count;
            while (pos > 0 && Less(candidate, top[pos - 1]))
            {
                pos--;
            }
            if (top.Count < limit)
            {
                top.Add(default);
            }
            for (int j = top.Count - 1; j > pos; j--)
            {
                top[j] = top[j - 1];
            }
            top[pos] = candidate;
        }

        var owners = new string[top.Count];
        for (int i = 0; i < top.Count; i++) owners[i] = top[i].Node;
        return owners;
    }

    /// <summary>Whether <paramref name="a"/> ranks strictly ahead of
    /// <paramref name="b"/> in owner order: higher score wins; ties break
    /// toward the lexicographically smaller name (ordinal comparison,
    /// matching byte-wise ordering elsewhere) — a total order every
    /// nanocached implementation agrees on.</summary>
    private static bool Less((ulong Score, string Node) a, (ulong Score, string Node) b)
    {
        if (a.Score != b.Score) return a.Score > b.Score;
        return string.CompareOrdinal(a.Node, b.Node) < 0;
    }

    /// <summary>The key's primary — <c>Owners(key, 1)[0]</c>.</summary>
    public string Route(ReadOnlySpan<byte> key) => Route(ReadOnlySpan<byte>.Empty, key);

    /// <summary>As <see cref="Route(ReadOnlySpan{byte})"/>, scoped to
    /// <paramref name="namespaceBytes"/> (issue #105) — <c>Owners(namespaceBytes, key, 1)[0]</c>.</summary>
    public string Route(ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> key)
    {
        IReadOnlyList<string> owners = Owners(namespaceBytes, key, 1);
        if (owners.Count == 0)
        {
            throw new InvalidOperationException("nanocached: hash ring has no nodes");
        }
        return owners[0];
    }
}
