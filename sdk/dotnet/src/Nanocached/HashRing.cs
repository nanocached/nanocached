using System.Text;

namespace Nanocached;

/// <summary>
/// Rendezvous (highest-random-weight) hashing over a fixed node list (see
/// doc/adr/0011-*.md in the nanocached repository). This is deliberately a
/// byte-for-byte port of the same computation every other nanocached
/// participant uses (the Rust node, the TypeScript/Python/Java/Rust SDKs)
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
    /// </summary>
    public IReadOnlyList<string> Owners(ReadOnlySpan<byte> key, int replicas)
    {
        ulong keyHash = Fnv1a(key);

        var scored = new (ulong Score, string Node)[_nodes.Length];
        for (int i = 0; i < _nodes.Length; i++)
        {
            scored[i] = (Fmix64(_nodeHashes[i] ^ keyHash), _nodes[i]);
        }

        // Descending by score; ties toward the lexicographically smaller
        // name — a total order every implementation agrees on. Ordinal
        // comparison, matching byte-wise ordering elsewhere.
        Array.Sort(scored, (a, b) =>
        {
            int byScore = b.Score.CompareTo(a.Score);
            return byScore != 0 ? byScore : string.CompareOrdinal(a.Node, b.Node);
        });

        return scored.Take(Math.Min(replicas, scored.Length)).Select(pair => pair.Node).ToArray();
    }

    /// <summary>The key's primary — <c>Owners(key, 1)[0]</c>.</summary>
    public string Route(ReadOnlySpan<byte> key) => Owners(key, 1)[0];
}
