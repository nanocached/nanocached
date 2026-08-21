using System.Text;
using Xunit;

namespace Nanocached.Tests;

public class HashRingTests
{
    private static readonly string[] Nodes = { "node-a", "node-b", "node-c" };

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public void MatchesPublishedFnv1aVectors()
    {
        Assert.Equal(0xcbf29ce484222325UL, HashRing.Fnv1a(Bytes("")));
        Assert.Equal(0xaf63dc4c8601ec8cUL, HashRing.Fnv1a(Bytes("a")));
        Assert.Equal(0x85944171f73967e8UL, HashRing.Fnv1a(Bytes("foobar")));
    }

    [Fact]
    public void MatchesCrossLanguageScoreVectors()
    {
        // Pinned outputs of the full ADR-0011 score pipeline — the Rust
        // node and the TypeScript/Python/Java/Rust SDKs assert these too.
        Assert.Equal(0UL, HashRing.Fmix64(0));
        Assert.Equal(0xb456bcfc34c2cb2cUL, HashRing.Fmix64(1));
        Assert.Equal(0xefd01f60ba992926UL, HashRing.Fmix64(0xcbf29ce484222325UL));

        var ring = new HashRing(Nodes);
        Assert.Equal(new[] { "node-c", "node-b", "node-a" }, ring.Owners(Bytes("alpha"), 3));
        Assert.Equal(new[] { "node-a", "node-c", "node-b" }, ring.Owners(Bytes("beta"), 3));
        Assert.Equal(new[] { "node-a", "node-b", "node-c" }, ring.Owners(Bytes(""), 3));
    }

    [Fact]
    public void OwnersAreDistinctAndCapped()
    {
        var ring = new HashRing(Nodes);
        IReadOnlyList<string> owners = ring.Owners(Bytes("some-key"), 2);
        Assert.Equal(2, owners.Count);
        Assert.NotEqual(owners[0], owners[1]);
        Assert.Equal(3, ring.Owners(Bytes("some-key"), 10).Count);
    }

    [Fact]
    public void AddingANodeNeverReordersExistingNodes()
    {
        var before = new HashRing(Nodes);
        var after = new HashRing(new[] { "node-a", "node-b", "node-c", "node-d" });
        for (int i = 0; i < 500; i++)
        {
            byte[] key = Bytes($"key-{i}");
            string[] newOrder = after.Owners(key, 4).Where(node => node != "node-d").ToArray();
            Assert.Equal(before.Owners(key, 3), newOrder);
        }
    }

    [Fact]
    public void OwnersMatchesANaiveFullSortReferenceAcrossReplicaCounts()
    {
        // Owners now selects the top-R via bounded insertion instead of
        // sorting every node under NanocachedClient's routing lock on
        // every call (audit finding). This pins its output
        // byte-identical to the straightforward "score everything, sort,
        // then take" reference it replaced, across R's edge cases (0, 1,
        // half the cluster, exactly the cluster size, and one over it)
        // plus a run of pseudo-random keys. Mirrors the Rust SDK's
        // owners_matches_a_naive_full_sort_reference test
        // (sdk/rust/src/hash_ring.rs) and the Go SDK's TestOwnersMatchesNaiveFullSort.
        string[] nodeNames = Enumerable.Range(0, 37).Select(i => $"node-{i}").ToArray();
        var ring = new HashRing(nodeNames);
        int len = nodeNames.Length;

        var rng = new Random(0xC0FFEE);
        foreach (int replicas in new[] { 0, 1, len / 2, len, len + 1, len + 10 })
        {
            for (int i = 0; i < 200; i++)
            {
                byte[] key = Bytes($"fuzz-key-{rng.Next()}");
                IReadOnlyList<string> actual = ring.Owners(key, replicas);
                IReadOnlyList<string> expected = NaiveOwners(nodeNames, key, replicas);
                Assert.Equal(expected, actual);
            }
        }
    }

    /// <summary>The reference implementation Owners used to be — sort
    /// every node's score, then take the front — kept independent of
    /// HashRing.Owners' own bounded-insertion logic so it can serve as a
    /// ground truth for the property test above.</summary>
    private static IReadOnlyList<string> NaiveOwners(string[] nodeNames, byte[] key, int replicas)
    {
        ulong keyHash = HashRing.Fnv1a(key);
        List<(ulong Score, string Node)> scored = nodeNames
            .Select(node => (Score: HashRing.Fmix64(HashRing.Fnv1a(Bytes(node)) ^ keyHash), Node: node))
            .OrderByDescending(pair => pair.Score)
            .ThenBy(pair => pair.Node, StringComparer.Ordinal)
            .ToList();
        return scored.Take(Math.Min(replicas, scored.Count)).Select(pair => pair.Node).ToArray();
    }

    [Fact]
    public void SpreadsKeysEvenly()
    {
        var ring = new HashRing(Nodes);
        var counts = new Dictionary<string, int>();
        const int total = 3000;
        for (int i = 0; i < total; i++)
        {
            string owner = ring.Route(Bytes($"key-{i}"));
            counts[owner] = counts.GetValueOrDefault(owner) + 1;
        }
        double fair = (double)total / Nodes.Length;
        foreach (var (node, count) in counts)
        {
            Assert.True(Math.Abs(count - fair) / fair < 0.15, $"{node} got {count}/{total}");
        }
    }
}
