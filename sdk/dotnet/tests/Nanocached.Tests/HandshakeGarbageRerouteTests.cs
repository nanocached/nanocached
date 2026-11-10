using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// A listed address that answers the <c>A</c> handshake with something that
/// is not a handshake reply at all (an HTTP server's error page) is a
/// connection failure that sent nothing, like a refused dial. As a plain
/// <see cref="NanocachedException"/> in the reconnect cooldown it failed
/// every operation whose primary was that member until the 30 s staleness
/// refresh.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class HandshakeGarbageRerouteTests
{
    private static readonly string[] Names =
    {
        "5f8a9c2e-1b3d-4e6f-8a90-c1d2e3f4a5b6",
        "0d47b1a9-7e2c-4f58-9b31-6a8d0c9e2f47",
    };

    /// <summary>Answers whatever dials it with an HTTP error and keeps the
    /// connection open (closing it would race the client's own handshake
    /// write into an EPIPE instead of the unexpected reply).</summary>
    private sealed class HttpGarbageListener : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<TcpClient> _held = new();
        private int _connections;

        public HttpGarbageListener(int port = 0)
        {
            _listener = new TcpListener(IPAddress.Loopback, port);
            _listener.Start();
            _ = AcceptLoopAsync();
        }

        public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        public string Address => $"127.0.0.1:{Port}";

        public int Connections => Volatile.Read(ref _connections);

        private async Task AcceptLoopAsync()
        {
            while (true)
            {
                TcpClient accepted;
                try
                {
                    accepted = await _listener.AcceptTcpClientAsync();
                }
                catch
                {
                    return;
                }
                lock (_held) _held.Add(accepted);
                Interlocked.Increment(ref _connections);
                try
                {
                    await accepted.GetStream().WriteAsync(
                        Encoding.ASCII.GetBytes("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"));
                }
                catch (IOException)
                {
                    // The client already hung up.
                }
            }
        }

        public void Dispose()
        {
            _listener.Stop();
            lock (_held)
            {
                foreach (TcpClient held in _held) held.Dispose();
            }
        }
    }

    private static string KeyWhosePrimaryIs(string name, params string[] except)
    {
        for (int i = 0; ; i++)
        {
            string key = "garbage-" + i;
            if (except.Contains(key)) continue;
            if (new HashRing(Names).Owners(Encoding.UTF8.GetBytes(key), 1)[0] == name) return key;
        }
    }

    private static Task ForceRefresh(NanocachedClient client)
    {
        MethodInfo method = typeof(NanocachedClient)
            .GetMethod("MaybeRefreshAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)method.Invoke(client, new object[] { true })!;
    }

    private static Task PastTheForcedRefreshInterval() =>
        Task.Delay(NanocachedClient.ForcedRefreshMinInterval + TimeSpan.FromMilliseconds(150));

    [Theory]
    [InlineData("set")]
    [InlineData("incr")]
    [InlineData("touch0")]
    public async Task AMemberThatAnswersGarbageIsReroutedOnceDiscoveryDropsIt(string operation)
    {
        string key = KeyWhosePrimaryIs(Names[0]);
        using var nodeB = new MockNode();
        using var http = new HttpGarbageListener();
        using var discovery = new MockDiscovery(new List<(string, string)> { (Names[1], nodeB.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });
        nodeB.Store[MockNode.KeyOf(Encoding.UTF8.GetBytes(key))] = Encoding.UTF8.GetBytes("5");

        // A refresh lists a node whose address answers with HTTP: it is
        // installed with no connection and the dial error in its cooldown.
        discovery.SetNodes(new List<(string, string)> { (Names[0], http.Address), (Names[1], nodeB.Address) });
        await ForceRefresh(client);
        Assert.True(http.Connections >= 1, "the refresh never dialed the garbage listener");
        // Discovery then drops it; the next forced refresh is due once the interval has passed.
        discovery.SetNodes(new List<(string, string)> { (Names[1], nodeB.Address) });
        await PastTheForcedRefreshInterval();

        switch (operation)
        {
            case "set":
                await client.SetAsync(key, "v");
                Assert.Equal("v", await client.GetAsync(key));
                break;
            case "incr":
                Assert.Equal(6L, await client.IncrAsync(key, 1));
                break;
            default:
                // ttl 0: the chunk whose dial failed was never sent, so it is rerouted.
                Assert.Equal(new Dictionary<string, bool> { [key] = true }, await client.TouchManyAsync(new[] { key }, 0));
                break;
        }
    }

    [Fact]
    public async Task ARedialThatMeetsGarbageIsANotSentConnectionLost()
    {
        using var nodeB = new MockNode();
        using var http = new HttpGarbageListener();
        using var discovery = new MockDiscovery(new List<(string, string)> { (Names[1], nodeB.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });

        MethodInfo open = typeof(NanocachedClient)
            .GetMethod("OpenNodeConnectionAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var thrown = await Assert.ThrowsAnyAsync<ConnectionLostException>(
            () => (Task<Connection>)open.Invoke(client, new object[] { http.Address })!);
        Assert.True(thrown.RequestNotSent);
        Assert.Contains("unexpected response to A", thrown.Message);
    }

    [Fact]
    public async Task AMemberRedialedIntoGarbageIsReroutedLikeADeadNode()
    {
        // A member whose connection died, its address then taken over by a
        // listener that speaks HTTP; discovery has since dropped it.
        string setKey = KeyWhosePrimaryIs(Names[0]);
        string counter = KeyWhosePrimaryIs(Names[0], setKey);
        using var nodeA = new MockNode();
        var nodeB = new MockNode();
        int portB = nodeB.Port;
        using var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], $"127.0.0.1:{portB}"), (Names[1], nodeA.Address) }, 1);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } });
        nodeA.Store[MockNode.KeyOf(Encoding.UTF8.GetBytes(counter))] = Encoding.UTF8.GetBytes("10");

        nodeB.Dispose();
        await Task.Delay(100); // let the FIN land
        using var http = new HttpGarbageListener(portB);
        discovery.SetNodes(new List<(string, string)> { (Names[1], nodeA.Address) });
        await PastTheForcedRefreshInterval();

        await client.SetAsync(setKey, "v");
        Assert.Equal("v", await client.GetAsync(setKey));
        Assert.True(http.Connections >= 1, "the redial never reached the garbage listener");
        Assert.Equal(11L, await client.IncrAsync(counter, 1));
    }

    [Fact]
    public async Task AFirstConnectToAnAddressThatAnswersGarbageStaysAPlainException()
    {
        using var http = new HttpGarbageListener();

        var single = await Assert.ThrowsAsync<NanocachedException>(() => NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", http.Port) } }));
        Assert.Contains("unexpected response to A", single.Message);

        // A listed node that answers garbage is the same configuration error.
        using var discovery = new MockDiscovery(new List<(string, string)> { (Names[0], http.Address) }, 1);
        var listed = await Assert.ThrowsAsync<NanocachedException>(() => NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) } }));
        Assert.Contains("unexpected response to A", listed.Message);

        // And a proxy roster that lists only such an address.
        discovery.SetProxies(new[] { ("proxy-1", http.Address) });
        var proxy = await Assert.ThrowsAsync<NanocachedException>(() => NanocachedClient.ConnectAsync(
            new() { Addresses = { ("127.0.0.1", discovery.Port) }, ViaProxy = true }));
        Assert.Contains("unexpected response to A", proxy.Message);
    }
}
