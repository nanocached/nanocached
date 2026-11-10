using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// A proxy that discovery's <c>Q</c> roster lists but that answers as a
/// discovery server is a configuration error at <c>ConnectAsync</c>, reported
/// as the plain <see cref="NanocachedException"/> the cluster connect throws
/// for a listed non-node. The internal not-a-cache-node type is for the
/// retry layers and must never reach a caller.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class ProxyConnectNonNodeTests
{
    [Fact]
    public async Task AProxyRosterThatListsOnlyANonNodeFailsConnectWithAPlainException()
    {
        using var notAProxy = new MockDiscovery(new List<(string, string)>(), 1);
        using var discovery = new MockDiscovery(new List<(string, string)>(), 1);
        discovery.SetProxies(new[] { ("proxy-1", $"127.0.0.1:{notAProxy.Port}") });

        var error = await Assert.ThrowsAsync<NanocachedException>(() => NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) }, ViaProxy = true }));

        Assert.Equal(typeof(NanocachedException), error.GetType());
        Assert.Contains("no longer identifies as a cache node", error.Message);
    }
}
