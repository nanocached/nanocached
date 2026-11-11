using System.Reflection;
using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// A listed address that answers as discovery (or a proxy) instead of a
/// cache node is a topology change, not a lost reply: nothing was sent, so
/// the retry layers refresh the roster and reroute. A plain
/// <see cref="NanocachedException"/> in the reconnect cooldown failed every
/// operation on that member until the 30 s staleness refresh.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class NonNodeIdentificationRerouteTests
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
            string key = "non-node-" + i;
            if (new HashRing(Names).Owners(Encoding.UTF8.GetBytes(key), 1)[0] == name) return key;
        }
    }

    private static Task ForceRefresh(NanocachedClient client)
    {
        MethodInfo method = typeof(NanocachedClient)
            .GetMethod("MaybeRefreshAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)method.Invoke(client, new object[] { true })!;
    }

    [Theory]
    [InlineData("set")]
    [InlineData("incr")]
    public async Task AMemberThatAnswersAsDiscoveryIsReroutedOnceDiscoveryDropsIt(string operation)
    {
        string key = KeyWhosePrimaryIs(Names[0]);
        using var nodeB = new MockNode();
        using var notANode = new MockDiscovery(new List<(string, string)>(), 1);
        string notANodeAddress = $"127.0.0.1:{notANode.Port}";
        using var discovery = new MockDiscovery(new List<(string, string)> { (Names[1], nodeB.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });
        nodeB.Store[MockNode.KeyOf(Encoding.UTF8.GetBytes(key))] = Encoding.UTF8.GetBytes("5");

        // A refresh lists a node whose address answers as discovery: it is
        // installed with no connection and the dial error in its cooldown.
        discovery.SetNodes(new List<(string, string)> { (Names[0], notANodeAddress), (Names[1], nodeB.Address) });
        await ForceRefresh(client);
        // Discovery then drops it; the next forced refresh is due once the interval has passed.
        discovery.SetNodes(new List<(string, string)> { (Names[1], nodeB.Address) });
        await Task.Delay(NanocachedClient.ForcedRefreshMinInterval + TimeSpan.FromMilliseconds(150));

        if (operation == "set")
        {
            await client.SetAsync(key, "v");
            Assert.Equal("v", await client.GetAsync(key));
        }
        else
        {
            Assert.Equal(6L, await client.IncrAsync(key, 1));
        }
    }

    [Fact]
    public async Task ARedialThatFindsANonNodeIsANotSentFailure()
    {
        using var nodeB = new MockNode();
        using var notANode = new MockDiscovery(new List<(string, string)>(), 1);
        using var discovery = new MockDiscovery(new List<(string, string)> { (Names[1], nodeB.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });

        MethodInfo open = typeof(NanocachedClient)
            .GetMethod("OpenNodeConnectionAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var thrown = await Assert.ThrowsAnyAsync<ConnectionLostException>(
            () => (Task<Connection>)open.Invoke(client, new object[] { $"127.0.0.1:{notANode.Port}" })!);
        Assert.True(thrown.RequestNotSent);
    }

    [Fact]
    public async Task AListedAddressThatAnswersAsDiscoveryIsADeadMemberWhileAnotherNodeIsUp()
    {
        using var notANode = new MockDiscovery(new List<(string, string)>(), 1);
        using var node = new MockNode();
        using var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], $"127.0.0.1:{notANode.Port}"), (Names[1], node.Address) }, 2);

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });

        // A read of a key owned by the non-node address fails over to the
        // live replica; a write to it reports the not-sent lost connection.
        string key = KeyWhosePrimaryIs(Names[0]);
        node.Store[MockNode.KeyOf(Encoding.UTF8.GetBytes(key))] = Encoding.UTF8.GetBytes("replica copy");
        Assert.Equal("replica copy", await client.GetAsync(key));
        var lost = await Assert.ThrowsAnyAsync<ConnectionLostException>(() => client.SetAsync(key, "v"));
        Assert.True(lost.RequestNotSent);
        Assert.Contains("does not identify as a cache node", lost.Message);
    }

    [Fact]
    public async Task AListedNonNodeFailsTheInitialConnectWithAPlainExceptionWhenNoNodeIsReachable()
    {
        using var notANode = new MockDiscovery(new List<(string, string)>(), 1);
        using var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], $"127.0.0.1:{notANode.Port}") }, 1);

        var error = await Assert.ThrowsAsync<NanocachedException>(() => NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } }));
        Assert.IsNotType<ConnectionLostException>(error);
        Assert.Contains("does not identify as a cache node", error.Message);
    }
}
