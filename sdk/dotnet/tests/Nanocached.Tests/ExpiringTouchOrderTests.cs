using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// issue #934 — a touch with a TTL of 0 sends the primary's frame alone
/// and the plain touch to the other owners only after the primary
/// answered. Sent concurrently, a replica expired its copy while the
/// primary could still fail or answer <c>W</c>: the reroute then reached
/// the surviving replica (now the primary), which answered <c>N</c> for a
/// key that existed, and the value was gone. A positive or removed TTL
/// keeps the concurrent fan-out.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class ExpiringTouchOrderTests
{
    private static readonly string[] Names =
    {
        "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
        "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
    };

    private sealed class Cluster : IDisposable
    {
        public readonly Dictionary<string, MockNode> Nodes = new();
        public readonly MockDiscovery Discovery;
        public NanocachedClient Client = null!;

        public Cluster()
        {
            foreach (string name in Names) Nodes[name] = new MockNode();
            Discovery = new MockDiscovery(Nodes.Select(p => (p.Key, p.Value.Address)).ToList(), 2);
        }

        public async Task ConnectAsync() =>
            Client = await NanocachedClient.ConnectAsync(new() { Addresses = { ("127.0.0.1", Discovery.Port) } });

        public (string Name, MockNode Node) Primary(string key) => Owner(key, 0);

        public (string Name, MockNode Node) Replica(string key) => Owner(key, 1);

        private (string, MockNode) Owner(string key, int rank)
        {
            string name = new HashRing(Names).Owners(Encoding.UTF8.GetBytes(key), 2)[rank];
            return (name, Nodes[name]);
        }

        public void Dispose()
        {
            Client?.Dispose();
            foreach (MockNode node in Nodes.Values) node.Dispose();
            Discovery.Dispose();
        }
    }

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

    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("timed out waiting for " + what);
            await Task.Delay(5);
        }
    }

    // ── single key ───────────────────────────────────────────────────

    [Fact]
    public async Task ADeadPrimaryIsReroutedWithoutTheReplicaHavingBeenTouched()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        const string key = "shared-key";
        await cluster.Client.SetAsync(key, "v", 100);
        var (primaryName, primary) = cluster.Primary(key);
        var (_, replica) = cluster.Replica(key);
        primary.Dispose();
        await Task.Delay(100);

        // The roster still lists the dead primary: the call ends in its
        // connection failure, and the replica was never sent a touch.
        await Assert.ThrowsAsync<ConnectionLostException>(() => cluster.Client.GetAndTouchAsync(key, 0));
        await Assert.ThrowsAsync<ConnectionLostException>(() => cluster.Client.TouchAsync(key, 0));
        Assert.Empty(replica.TouchFrames);

        // Once discovery drops the primary the reroute finds the value on
        // the surviving replica, which is the primary now.
        cluster.Discovery.SetNodes(
            cluster.Nodes.Where(p => p.Key != primaryName).Select(p => (p.Key, p.Value.Address)).ToList());
        Assert.Equal("v", await cluster.Client.GetAndTouchAsync(key, 0));
        Assert.Single(replica.TouchFrames);
        Assert.Contains("G", replica.TouchFrames[0].Split(' ')); // the reroute asks the new primary for the value
    }

    [Fact]
    public async Task ADeadPrimaryDroppedFromTheRosterReroutesAPlainTouchToTheValue()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        const string key = "shared-key";
        await cluster.Client.SetAsync(key, "v", 100);
        var (primaryName, primary) = cluster.Primary(key);
        var (_, replica) = cluster.Replica(key);
        primary.Dispose();
        await Task.Delay(100);
        cluster.Discovery.SetNodes(
            cluster.Nodes.Where(p => p.Key != primaryName).Select(p => (p.Key, p.Value.Address)).ToList());

        Assert.True(await cluster.Client.TouchAsync(key, 0));
        Assert.Single(replica.TouchFrames);
    }

    [Theory]
    [InlineData("touch")]
    [InlineData("getAndTouch")]
    public async Task APrimaryThatAnswersWrongNodeIsReroutedWithoutTheReplicaHavingBeenTouched(string op)
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        const string key = "shared-key";
        await cluster.Client.SetAsync(key, "v", 100);
        var (_, primary) = cluster.Primary(key);
        var (_, replica) = cluster.Replica(key);
        // Both the first pass and the refresh-and-retry are refused.
        primary.AnswerWrongNodeOnce();
        primary.AnswerWrongNodeOnce();

        await Assert.ThrowsAsync<WrongNodeException>(
            () => op == "touch" ? cluster.Client.TouchAsync(key, 0) : cluster.Client.GetAndTouchAsync(key, 0));

        Assert.Empty(replica.TouchFrames);
        Assert.Equal(2, primary.TouchFrames.Count);
    }

    [Theory]
    [InlineData("touch")]
    [InlineData("getAndTouch")]
    public async Task AnExpiringTouchSendsTheReplicaItsFrameOnlyAfterThePrimaryAnswered(string op)
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        const string key = "shared-key";
        await cluster.Client.SetAsync(key, "v", 100);
        var (_, primary) = cluster.Primary(key);
        var (_, replica) = cluster.Replica(key);
        primary.DelayTouches(400);

        Task call = op == "touch" ? cluster.Client.TouchAsync(key, 0) : cluster.Client.GetAndTouchAsync(key, 0);
        await WaitForAsync(() => primary.TouchFrames.Count == 1, "the primary's touch frame");
        await Task.Delay(100);
        Assert.Empty(replica.TouchFrames); // the primary has not answered yet
        await call;

        Assert.Single(replica.TouchFrames);
        Assert.DoesNotContain("G", replica.TouchFrames[0].Split(' ')); // the replica gets the plain form
    }

    [Fact]
    public async Task APositiveTtlStillTouchesTheReplicaConcurrently()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        const string key = "shared-key";
        await cluster.Client.SetAsync(key, "v", 100);
        var (_, primary) = cluster.Primary(key);
        var (_, replica) = cluster.Replica(key);
        primary.DelayTouches(400);

        Task call = cluster.Client.TouchAsync(key, 30);
        await WaitForAsync(() => replica.TouchFrames.Count == 1, "the replica's touch frame");
        Assert.Single(primary.TouchFrames);
        await call;
    }

    // ── batch ────────────────────────────────────────────────────────

    [Fact]
    public async Task ABatchWithADeadPrimaryIsReroutedWithoutTheReplicaHavingBeenTouched()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        var deadPrimaryName = Names[0];
        var keys = KeysWhosePrimaryIs(deadPrimaryName, 3);
        await cluster.Client.SetManyAsync(keys.ToDictionary(k => k, k => "v"), 100);
        MockNode primary = cluster.Nodes[deadPrimaryName];
        MockNode replica = cluster.Nodes[Names[1]];
        primary.Dispose();
        await Task.Delay(100);

        await Assert.ThrowsAnyAsync<ConnectionLostException>(() => cluster.Client.GetManyAndTouchAsync(keys, 0));
        await Assert.ThrowsAnyAsync<ConnectionLostException>(() => cluster.Client.TouchManyAsync(keys, 0));
        Assert.Empty(replica.MultiTouchFrames);

        cluster.Discovery.SetNodes(new List<(string, string)> { (Names[1], replica.Address) });
        var hits = await cluster.Client.GetManyAndTouchAsync(keys, 0);
        Assert.Equal(keys.OrderBy(k => k), hits.Keys.OrderBy(k => k));
        // One frame per pass: the rerouted batch, the survivor being the primary.
        Assert.Single(replica.MultiTouchFrames);
        Assert.True(replica.MultiTouchFrames[0].Get);
    }

    [Fact]
    public async Task ABatchWhosePrimaryAnswersWrongNodeLeavesTheReplicaUntouched()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        var keys = KeysWhosePrimaryIs(Names[0], 3);
        await cluster.Client.SetManyAsync(keys.ToDictionary(k => k, k => "v"), 100);
        MockNode primary = cluster.Nodes[Names[0]];
        MockNode replica = cluster.Nodes[Names[1]];
        primary.AnswerWrongNodeOnce();
        primary.AnswerWrongNodeOnce();

        await Assert.ThrowsAnyAsync<WrongNodeException>(() => cluster.Client.TouchManyAsync(keys, 0));

        Assert.Empty(replica.MultiTouchFrames);
        Assert.Equal(2, primary.MultiTouchFrames.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnExpiringBatchSendsTheReplicasTheirFrameOnlyAfterThePrimariesAnswered(bool get)
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        var keysOfA = KeysWhosePrimaryIs(Names[0], 3);
        await cluster.Client.SetManyAsync(keysOfA.ToDictionary(k => k, k => "v"), 100);
        MockNode a = cluster.Nodes[Names[0]];
        MockNode b = cluster.Nodes[Names[1]];
        a.DelayTouches(400);

        Task call = get
            ? cluster.Client.GetManyAndTouchAsync(keysOfA, 0)
            : cluster.Client.TouchManyAsync(keysOfA, 0);
        await WaitForAsync(() => a.MultiTouchFrames.Count == 1, "the primary's batch frame");
        await Task.Delay(100);
        Assert.Empty(b.MultiTouchFrames); // the primary has not answered yet
        await call;

        // One plain fan-out afterwards, carrying every key the primary answered.
        Assert.Single(b.MultiTouchFrames);
        Assert.False(b.MultiTouchFrames[0].Get);
        Assert.Equal(keysOfA.OrderBy(k => k), b.MultiTouchFrames[0].Keys.OrderBy(k => k));
        Assert.Equal(0, b.MultiTouchFrames[0].Ttl);
        Assert.Single(a.MultiTouchFrames);
    }

    [Fact]
    public async Task AnExpiringBatchAcrossBothPrimariesCrossFansOutOnceEach()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        var keysOfA = KeysWhosePrimaryIs(Names[0], 2);
        var keysOfB = KeysWhosePrimaryIs(Names[1], 2);
        var keys = keysOfA.Concat(keysOfB).ToList();
        await cluster.Client.SetManyAsync(keys.ToDictionary(k => k, k => "v"), 100);
        MockNode a = cluster.Nodes[Names[0]];
        MockNode b = cluster.Nodes[Names[1]];

        var result = await cluster.Client.TouchManyAsync(keys, 0);

        Assert.All(keys, key => Assert.True(result[key]));
        Assert.Equal(2, a.MultiTouchFrames.Count);
        Assert.Equal(2, b.MultiTouchFrames.Count);
        // Primary frames first (own keys), then the other node's keys.
        Assert.Equal(keysOfA.OrderBy(k => k), a.MultiTouchFrames[0].Keys.OrderBy(k => k));
        Assert.Equal(keysOfB.OrderBy(k => k), a.MultiTouchFrames[1].Keys.OrderBy(k => k));
        Assert.Equal(keysOfB.OrderBy(k => k), b.MultiTouchFrames[0].Keys.OrderBy(k => k));
        Assert.Equal(keysOfA.OrderBy(k => k), b.MultiTouchFrames[1].Keys.OrderBy(k => k));
    }

    [Fact]
    public async Task APositiveTtlBatchStillTouchesTheReplicasConcurrently()
    {
        using var cluster = new Cluster();
        await cluster.ConnectAsync();
        var keysOfA = KeysWhosePrimaryIs(Names[0], 3);
        await cluster.Client.SetManyAsync(keysOfA.ToDictionary(k => k, k => "v"), 100);
        MockNode a = cluster.Nodes[Names[0]];
        MockNode b = cluster.Nodes[Names[1]];
        a.DelayTouches(400);

        Task call = cluster.Client.TouchManyAsync(keysOfA, 30);
        await WaitForAsync(() => b.MultiTouchFrames.Count == 1, "the replica's batch frame");
        Assert.Single(a.MultiTouchFrames);
        await call;
    }
}
