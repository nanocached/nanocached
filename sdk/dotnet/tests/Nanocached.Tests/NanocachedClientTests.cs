using System.Text;
using Xunit;

namespace Nanocached.Tests;

public class NanocachedClientTests
{
    private static readonly string[] Names =
    {
        "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
        "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
    };

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"timed out waiting for {what}");
            await Task.Delay(5);
        }
    }

    // ── 単一ノード ────────────────────────────────────────────────

    [Fact]
    public async Task RoundTripsSetGetDelete()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync("127.0.0.1", node.Port);

        await client.SetAsync("greeting", "hello");
        Assert.Equal(Bytes("hello"), await client.GetAsync("greeting"));
        Assert.True(await client.DeleteAsync("greeting"));
        Assert.Null(await client.GetAsync("greeting"));
        Assert.False(await client.DeleteAsync("greeting"));
        Assert.Equal(1, client.Replication);
    }

    [Fact]
    public async Task ValidatesTtlSynchronously()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync("127.0.0.1", node.Port);

        await client.SetAsync("k", "v", 60);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => client.SetAsync(Bytes("k"), Bytes("v"), -1));
        // The rejected set must not have poisoned the connection.
        Assert.Equal(Bytes("v"), await client.GetAsync("k"));
    }

    [Fact]
    public async Task Authenticates()
    {
        using var node = new MockNode(requiredSecret: "s3cret");

        using (NanocachedClient client = await NanocachedClient.ConnectAsync(
                   new NanocachedClient.Options().Host("127.0.0.1", node.Port).AuthSecret("s3cret")))
        {
            await client.SetAsync("k", "v");
            Assert.Equal(Bytes("v"), await client.GetAsync("k"));
        }

        NanocachedException missing = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync("127.0.0.1", node.Port));
        Assert.Contains("requires authentication", missing.Message);

        NanocachedException wrong = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(
                new NanocachedClient.Options().Host("127.0.0.1", node.Port).AuthSecret("wrong")));
        Assert.Contains("authentication failed", wrong.Message);
    }

    [Fact]
    public async Task WrongNodePropagatesInSingleMode()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync("127.0.0.1", node.Port);
        node.AnswerWrongNodeOnce();
        await Assert.ThrowsAsync<WrongNodeException>(() => client.GetAsync("k"));
    }

    [Fact]
    public async Task RejectsUseAfterClose()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync("127.0.0.1", node.Port);
        client.Close();
        client.Close(); // idempotent
        Assert.True(client.IsClosed);
        await Assert.ThrowsAsync<AlreadyClosedException>(() => client.GetAsync("k"));
    }

    // ── 遅延再接続と keep-alive ───────────────────────────────────

    [Fact]
    public async Task TransparentlyReconnectsAfterAServerFin()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync("127.0.0.1", node.Port);

        await client.SetAsync("k", "v");
        node.DropConnections();
        await Task.Delay(50); // let the FIN land
        Assert.Equal(Bytes("v"), await client.GetAsync("k"));
        Assert.Equal(2, node.ConnectionCount);
    }

    [Fact]
    public async Task KeepAlivePingsAnIdleConnection()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options()
                .Host("127.0.0.1", node.Port)
                .KeepAliveInterval(TimeSpan.FromMilliseconds(40)));

        await WaitForAsync(() => node.GetCount >= 2, "keep-alive pings");
        Assert.Equal(1, node.ConnectionCount);
    }

    [Fact]
    public void RejectsANonPositiveKeepAliveInterval()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NanocachedClient.Options().KeepAliveInterval(TimeSpan.Zero));
    }

    // ── seeds ─────────────────────────────────────────────────────

    [Fact]
    public async Task RejectsAMissingTarget()
    {
        await Assert.ThrowsAsync<ArgumentException>(
            () => NanocachedClient.ConnectAsync(new NanocachedClient.Options()));
    }

    [Fact]
    public async Task FailsOverToTheSecondSeed()
    {
        using var node = new MockNode();
        using var discovery = new MockDiscovery(new[] { (Names[0], node.Address) });
        int dead = Wire.UnusedPort();

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options().Host("127.0.0.1", dead).Host("127.0.0.1", discovery.Port));
        await client.SetAsync("k", "v");
        Assert.Equal(Bytes("v"), await client.GetAsync("k"));
    }

    [Fact]
    public async Task SkipsAWarmingUpSeed()
    {
        using var node = new MockNode();
        using var warming = new MockDiscovery(new[] { (Names[0], node.Address) });
        using var healthy = new MockDiscovery(new[] { (Names[0], node.Address) });
        warming.WarmingUp = true;

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options()
                .Host("127.0.0.1", warming.Port)
                .Host("127.0.0.1", healthy.Port));
        await client.SetAsync("k", "v");
        Assert.Equal(Bytes("v"), await client.GetAsync("k"));
    }

    [Fact]
    public async Task RaisesBusyWhenEverySeedIsWarming()
    {
        using var first = new MockDiscovery(Array.Empty<(string, string)>());
        using var second = new MockDiscovery(Array.Empty<(string, string)>());
        first.WarmingUp = true;
        second.WarmingUp = true;

        await Assert.ThrowsAsync<DiscoveryBusyException>(
            () => NanocachedClient.ConnectAsync(new NanocachedClient.Options()
                .Host("127.0.0.1", first.Port)
                .Host("127.0.0.1", second.Port)));
    }

    // ── クラスタと複製 ────────────────────────────────────────────

    private sealed record Cluster(
        IReadOnlyDictionary<string, MockNode> Nodes, MockDiscovery Discovery) : IDisposable
    {
        public void Dispose()
        {
            Discovery.Dispose();
            foreach (MockNode node in Nodes.Values) node.Dispose();
        }
    }

    private static Cluster StartCluster(int replication)
    {
        var nodeA = new MockNode();
        var nodeB = new MockNode();
        var nodes = new Dictionary<string, MockNode> { [Names[0]] = nodeA, [Names[1]] = nodeB };
        var discovery = new MockDiscovery(
            nodes.Select(pair => (pair.Key, pair.Value.Address)).ToList(), replication);
        return new Cluster(nodes, discovery);
    }

    private static IReadOnlyList<string> OwnersOf(string key) =>
        new HashRing(Names).Owners(Bytes(key), 2);

    [Fact]
    public async Task RoutesAndReadsItsOwnWrites()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);

        for (int i = 0; i < 50; i++) await client.SetAsync($"key-{i}", $"value-{i}");
        for (int i = 0; i < 50; i++)
        {
            Assert.Equal(Bytes($"value-{i}"), await client.GetAsync($"key-{i}"));
        }

        int[] sizes = cluster.Nodes.Values.Select(node => node.Store.Count).ToArray();
        Assert.Equal(50, sizes.Sum());
        Assert.All(sizes, size => Assert.True(size > 0));
    }

    [Fact]
    public async Task WrongNodeTriggersRefreshAndOneRetry()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);

        await client.SetAsync("some-key", "v");
        MockNode owner = cluster.Nodes[new HashRing(Names).Route(Bytes("some-key"))];

        owner.AnswerWrongNodeOnce();
        Assert.Equal(Bytes("v"), await client.GetAsync("some-key"));

        owner.AnswerWrongNodeOnce();
        owner.AnswerWrongNodeOnce();
        await Assert.ThrowsAsync<WrongNodeException>(() => client.GetAsync("some-key"));
    }

    [Fact]
    public async Task FansWritesOutToEveryOwner()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);
        Assert.Equal(2, client.Replication);

        for (int i = 0; i < 20; i++) await client.SetAsync($"key-{i}", "v");
        for (int i = 0; i < 20; i++)
        {
            string stored = MockNode.KeyOf(Bytes($"key-{i}"));
            foreach (var (name, node) in cluster.Nodes)
            {
                Assert.True(node.Store.ContainsKey(stored), $"key-{i} missing from {name}");
            }
        }
    }

    [Fact]
    public async Task ReadsFailOverWhenThePrimaryDies()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);

        await client.SetAsync("survives", "still here");
        cluster.Nodes[OwnersOf("survives")[0]].Dispose();
        await Task.Delay(50);

        Assert.Equal(Bytes("still here"), await client.GetAsync("survives"));
    }

    [Fact]
    public async Task ADeadReplicaDoesNotFailWrites()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);

        IReadOnlyList<string> owners = OwnersOf("written-anyway");
        cluster.Nodes[owners[1]].Dispose();
        await Task.Delay(50);

        await client.SetAsync("written-anyway", "v");
        Assert.True(cluster.Nodes[owners[0]].Store.ContainsKey(MockNode.KeyOf(Bytes("written-anyway"))));
        Assert.Equal(Bytes("v"), await client.GetAsync("written-anyway"));
    }

    [Fact]
    public async Task WritesRouteAroundADeadPrimaryOnceDiscoveryDropsIt()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);

        const string key = "written-after-primary-death";
        IReadOnlyList<string> owners = OwnersOf(key);

        // The primary dies AND discovery has already noticed: the first
        // write attempt fails on the dead primary, forcing a refresh that
        // re-ranks onto the survivor, and the retry succeeds.
        cluster.Nodes[owners[0]].Dispose();
        cluster.Discovery.SetNodes(new[] { (owners[1], cluster.Nodes[owners[1]].Address) });
        await Task.Delay(50);

        await client.SetAsync(key, "v");
        Assert.Equal(Bytes("v"), await client.GetAsync(key));
    }

    [Fact]
    public async Task FansDeletesOutToEveryOwner()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync("127.0.0.1", cluster.Discovery.Port);

        await client.SetAsync("gone-everywhere", "v");
        Assert.True(await client.DeleteAsync("gone-everywhere"));
        string stored = MockNode.KeyOf(Bytes("gone-everywhere"));
        foreach (MockNode node in cluster.Nodes.Values)
        {
            Assert.False(node.Store.ContainsKey(stored));
        }
    }
}
