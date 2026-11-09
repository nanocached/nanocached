using System.Diagnostics;
using System.Reflection;
using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// issue #1583 — a forced node-list refresh is not taken from a refresh
/// that began before the failure it reacts to, and forced refreshes are
/// at least ForcedRefreshMinInterval apart.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class ForcedRefreshTests
{
    private static readonly string[] Names =
    {
        "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
        "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
    };

    private static string KeyWhosePrimaryIs(string name)
    {
        for (int i = 0; ; i++)
        {
            string key = "dead-" + i;
            if (new HashRing(Names).Owners(Encoding.UTF8.GetBytes(key), 1)[0] == name) return key;
        }
    }

    // Two nodes at R=1, the first stopped but still listed by discovery
    // (its liveness window): every operation on the returned key fails and
    // forces a refresh that installs the same roster again.
    private static async Task<(NanocachedClient Client, MockDiscovery Discovery, MockNode NodeB, string Key)> StartAsync()
    {
        var nodeA = new MockNode();
        var nodeB = new MockNode();
        var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], nodeA.Address), (Names[1], nodeB.Address) }, 1);
        NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });
        nodeA.Dispose();
        await Task.Delay(100);
        return (client, discovery, nodeB, KeyWhosePrimaryIs(Names[0]));
    }

    private static Task StartForcedRefresh(NanocachedClient client)
    {
        MethodInfo method = typeof(NanocachedClient)
            .GetMethod("MaybeRefreshAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)method.Invoke(client, new object[] { true })!;
    }

    [Fact]
    public async Task SequentialFailuresAgainstADeadListedPrimaryDoNotEachOpenADiscoveryConnection()
    {
        var (client, discovery, nodeB, key) = await StartAsync();
        using var _c = client;
        using var _d = discovery;
        using var _b = nodeB;

        int before = discovery.ConnectionCount;
        for (int i = 0; i < 40; i++)
        {
            await Assert.ThrowsAsync<ConnectionLostException>(() => client.SetAsync(key, "v"));
        }

        // One refresh for the first failure; the rest fall inside the interval.
        int fetches = discovery.ConnectionCount - before;
        Assert.InRange(fetches, 1, 2);
    }

    [Fact]
    public async Task AForcedRefreshRunsAgainOnceTheIntervalHasPassed()
    {
        var (client, discovery, nodeB, key) = await StartAsync();
        using var _c = client;
        using var _d = discovery;
        using var _b = nodeB;

        await Assert.ThrowsAsync<ConnectionLostException>(() => client.SetAsync(key, "v"));
        int afterFirst = discovery.ConnectionCount;
        await Task.Delay(NanocachedClient.ForcedRefreshMinInterval + TimeSpan.FromMilliseconds(150));
        await Assert.ThrowsAsync<ConnectionLostException>(() => client.SetAsync(key, "v"));

        Assert.Equal(afterFirst + 1, discovery.ConnectionCount);
    }

    [Fact]
    public async Task AForcedRefreshDoesNotReuseOneThatBeganBeforeItsFailure()
    {
        var (client, discovery, nodeB, key) = await StartAsync();
        using var _c = client;
        using var _d = discovery;
        using var _b = nodeB;

        // A refresh already waiting on discovery's list when the write fails.
        discovery.LaterListDelayMillis = 600;
        int before = discovery.ConnectionCount;
        Task inFlight = StartForcedRefresh(client);
        await Task.Delay(150);
        Assert.False(inFlight.IsCompleted);

        var watch = Stopwatch.StartNew();
        await Assert.ThrowsAsync<ConnectionLostException>(() => client.SetAsync(key, "v"));
        watch.Stop();
        await inFlight;

        // The in-flight refresh, then a second one that began after the
        // failure and after the interval that followed the first.
        Assert.Equal(before + 2, discovery.ConnectionCount);
        Assert.True(
            watch.Elapsed >= TimeSpan.FromMilliseconds(600 - 150) + NanocachedClient.ForcedRefreshMinInterval,
            $"the follow-up refresh started too early: {watch.Elapsed}");
    }
}
