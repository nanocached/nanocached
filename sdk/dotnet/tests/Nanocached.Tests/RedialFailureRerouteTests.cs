using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// issue #936 — a failed redial of a dead primary is "request not sent"
/// (as the first connect already reports), so the not-sent retry paths
/// refresh the roster and reroute instead of surfacing the dial failure.
/// ttl 30 and the removed TTL always rerouted; ttl 0, Incr and the
/// batch touch did not.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class RedialFailureRerouteTests
{
    private static readonly string[] Names =
    {
        "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
        "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
    };

    private static List<string> KeysWhosePrimaryIs(string name, int count)
    {
        var keys = new List<string>();
        for (int i = 0; keys.Count < count; i++)
        {
            string key = "batch-" + i;
            if (new HashRing(Names).Owners(Encoding.UTF8.GetBytes(key), 2)[0] == name) keys.Add(key);
        }
        return keys;
    }

    // Two nodes, replication 1, the roster later shrunk to node B: a key
    // whose primary was A is gone with A and must come back as a miss
    // from B, not as the dial failure to A.
    private static async Task<(NanocachedClient Client, MockNode NodeB, MockDiscovery Discovery)> StartAsync(
        Func<NanocachedClient, Task> seed)
    {
        var nodeA = new MockNode();
        var nodeB = new MockNode();
        var nodes = new Dictionary<string, MockNode> { [Names[0]] = nodeA, [Names[1]] = nodeB };
        var discovery = new MockDiscovery(nodes.Select(p => (p.Key, p.Value.Address)).ToList(), 1);
        NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });
        await seed(client);
        nodeA.Dispose();
        await Task.Delay(100);
        discovery.SetNodes(new List<(string, string)> { (Names[1], nodeB.Address) });
        return (client, nodeB, discovery);
    }

    [Theory]
    [InlineData(30L)]
    [InlineData(0L)]
    [InlineData(-1L)]
    public async Task SingleTouchReroutesPastADeadPrimary(long ttl)
    {
        string key = KeysWhosePrimaryIs(Names[0], 1)[0];
        var (client, nodeB, discovery) = await StartAsync(c => c.SetAsync(key, "v", 100));
        using var _c = client;
        using var _b = nodeB;
        using var _d = discovery;

        Assert.False(await client.TouchAsync(key, ttl < 0 ? null : ttl));
    }

    [Theory]
    [InlineData(30L)]
    [InlineData(0L)]
    public async Task BatchTouchReroutesPastADeadPrimary(long ttl)
    {
        var keys = KeysWhosePrimaryIs(Names[0], 2).Concat(KeysWhosePrimaryIs(Names[1], 2)).ToList();
        var (client, nodeB, discovery) = await StartAsync(
            c => c.SetManyAsync(keys.ToDictionary(k => k, k => "v")));
        using var _c = client;
        using var _b = nodeB;
        using var _d = discovery;

        var result = await client.TouchManyAsync(keys, ttl);

        Assert.Equal(keys.Count, result.Count);
        foreach (string key in KeysWhosePrimaryIs(Names[0], 2)) Assert.False(result[key]);
        foreach (string key in KeysWhosePrimaryIs(Names[1], 2)) Assert.True(result[key]);
    }

    [Fact]
    public async Task IncrReroutesPastADeadPrimary()
    {
        string key = KeysWhosePrimaryIs(Names[0], 1)[0];
        var (client, nodeB, discovery) = await StartAsync(c => c.SetAsync(key, "5", 100));
        using var _c = client;
        using var _b = nodeB;
        using var _d = discovery;

        Assert.Null(await client.IncrAsync(key, 1));
    }
}
