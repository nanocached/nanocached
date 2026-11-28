using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// issue #2230 — a node's <c>En</c> is final for every request kind, as in
/// the Go and Rust SDKs: the same frame on a fresh connection (or after a
/// roster refresh) would be refused the same way, so it is not resent.
/// The requests pipelined behind it keep their not-sent replay
/// (<see cref="ServerRejectedTouchTests.AnIncrPipelinedBehindATouchAnsweredEnIsReplayed"/>).
/// </summary>
[Collection(DialSettings.CollectionName)]
public class ServerRejectedFinalTests
{
    private const string Name = "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6";

    [Fact]
    public async Task AnEnToAGetIsFinalAndNotResentOnANewConnection()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", node.Port) } });
        await client.SetAsync("k", "v", 100);
        int connections = node.ConnectionCount;
        int gets = node.GetCount;
        node.AnswerRawToGetOnce("En\n");

        await Assert.ThrowsAnyAsync<ConnectionLostException>(() => client.GetAsync("k"));

        Assert.Equal(gets + 1, node.GetCount);
        Assert.Equal(connections, node.ConnectionCount);
    }

    [Fact]
    public async Task AnEnToAGetInAClusterIsFinalAndNotResentAfterARefresh()
    {
        using var node = new MockNode();
        using var discovery = new MockDiscovery(new[] { (Name, node.Address) });
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });
        await client.SetAsync("k", "v", 100);
        int connections = node.ConnectionCount;
        int gets = node.GetCount;
        int fetches = discovery.ConnectionCount;
        node.AnswerRawToGetOnce("En\n");

        await Assert.ThrowsAnyAsync<ConnectionLostException>(() => client.GetAsync("k"));

        Assert.Equal(gets + 1, node.GetCount);
        Assert.Equal(connections, node.ConnectionCount);
        Assert.Equal(fetches, discovery.ConnectionCount);
    }
}
