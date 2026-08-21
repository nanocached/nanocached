using System.Text;

namespace Nanocached;

/// <summary>
/// Rendezvous (highest-random-weight) hashing over a fixed node list (see
/// doc/adr/0011-*.md in the nanocached repository). This is deliberately a
/// byte-for-byte port of the same computation every other nanocached
/// participant uses (the Rust node, the Go/TypeScript/Python/Java/Rust SDKs)
/// — not just "a" rendezvous hash, but <em>this specific</em> one: if this
/// SDK's ranking disagreed with a node's own copy, the two would disagree
/// about which nodes hold a key. Cross-language test vectors pin the
/// pipeline.
///
/// For each (node, key) pair,
/// <c>score = fmix64(fnv1a(name) ^ fnv1a(key))</c>; a key's owners are the
/// <c>replicas</c> highest-scoring nodes in descending (unsigned) score
/// order (ties — effectively impossible at 64 bits — break toward the
/// lexicographically smaller name), and its primary is the top one.
///
/// Built from node <em>names</em>, not addresses (doc/adr/0009-*.md).
/// </summary>
public sealed class HashRing
{
    private readonly string[] _nodes;
    private readonly ulong[] _nodeHashes;

    public HashRing(IReadOnlyList<string> nodes)
    {
        _nodes = nodes.ToArray();
        _nodeHashes = _nodes.Select(node => Fnv1a(Encoding.UTF8.GetBytes(node))).ToArray();
    }

    /// <summary>FNV-1a over 64 bits; C#'s ulong arithmetic wraps like Rust's u64.</summary>
    internal static ulong Fnv1a(ReadOnlySpan<byte> data)
    {
        ulong hash = 0xcbf29ce484222325;
        foreach (byte b in data)
        {
            hash ^= b;
            hash *= 0x100000001b3;
        }
        return hash;
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
    public IReadOnlyList<string> Owners(ReadOnlySpan<byte> key, int replicas)
    {
        ulong keyHash = Fnv1a(key);
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
    public string Route(ReadOnlySpan<byte> key) => Owners(key, 1)[0];
}
