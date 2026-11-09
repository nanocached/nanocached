using System.Reflection;
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

    // issue #1525 — while a refresh dials a newly listed node, the member
    // table has already lost the departed node but the ring still names
    // it. A non-idempotent op on one of its keys found no member: that is
    // a request that was never written, not a lost reply, so it waits for
    // the refresh and reroutes like an idempotent op does.
    private static Task StartRefresh(NanocachedClient client)
    {
        MethodInfo method = typeof(NanocachedClient)
            .GetMethod("MaybeRefreshAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)method.Invoke(client, new object[] { true })!;
    }

    private const string NewNodeName = "a3b6c9d2-4e5f-4a1b-8c7d-9e0f1a2b3c4d";

    private static string KeyMovingFromTo(string from, string to)
    {
        string[] after = { to, NewNodeName };
        for (int i = 0; ; i++)
        {
            string key = "window-" + i;
            byte[] bytes = Encoding.UTF8.GetBytes(key);
            if (new HashRing(Names).Owners(bytes, 1)[0] == from && new HashRing(after).Owners(bytes, 1)[0] == to)
            {
                return key;
            }
        }
    }

    [Fact]
    public async Task NonIdempotentOpsWaitOutARefreshThatDropsTheirPrimary()
    {
        string key = KeyMovingFromTo(Names[0], Names[1]);
        using var nodeA = new MockNode();
        using var nodeB = new MockNode();
        using var slow = new MockNode();
        using var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], nodeA.Address), (Names[1], nodeB.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });

        slow.DelayAuth(1500);
        nodeB.Store[MockNode.KeyOf(Encoding.UTF8.GetBytes(key))] = Encoding.UTF8.GetBytes("5");
        discovery.SetNodes(new List<(string, string)> { (Names[1], nodeB.Address), (NewNodeName, slow.Address) });

        // The refresh drops A at once, then spends 1.5 s dialing the slow node.
        Task refresh = StartRefresh(client);
        await Task.Delay(300);
        Assert.False(refresh.IsCompleted, "the refresh must still be dialing");

        Assert.Equal(6L, await client.IncrAsync(key, 1));
        Assert.True(refresh.IsCompleted);
        Assert.False(await client.PutIfAbsentAsync(key, "other"));
    }

    [Fact]
    public async Task ARedialThatFindsItsMemberGoneIsANotSentFailure()
    {
        using var nodeA = new MockNode();
        using var nodeB = new MockNode();
        using var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], nodeA.Address), (Names[1], nodeB.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });

        MethodInfo dial = typeof(NanocachedClient)
            .GetMethod("DialWithCooldownAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Connection fresh = await (Task<Connection>)dial.Invoke(client, new object?[] { nodeA.Address })!;
        MethodInfo install = typeof(NanocachedClient)
            .GetMethod("InstallRedialedConnection", BindingFlags.NonPublic | BindingFlags.Instance)!;

        var thrown = Assert.Throws<TargetInvocationException>(
            () => install.Invoke(client, new object?[] { "no-such-member", fresh }));
        var lost = Assert.IsType<ConnectionLostException>(thrown.InnerException);
        Assert.True(lost.RequestNotSent);
        Assert.True(fresh.IsClosed);
    }
}
