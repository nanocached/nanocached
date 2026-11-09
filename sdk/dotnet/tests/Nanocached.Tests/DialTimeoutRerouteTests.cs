using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// A dial that times out sent nothing, so it is a not-sent failure like a
/// refused dial: a non-idempotent operation whose primary stopped
/// answering the handshake (a vanished cloud VM drops the SYNs) refreshes
/// the roster and reroutes instead of failing as "outcome unknown".
/// </summary>
[Collection(DialSettings.CollectionName)]
public class DialTimeoutRerouteTests
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
            string key = "timeout-" + i;
            if (new HashRing(Names).Owners(Encoding.UTF8.GetBytes(key), 1)[0] == name) return key;
        }
    }

    /// <summary>Accepts connections and never answers: the handshake of
    /// every dial runs to the connect deadline.</summary>
    private sealed class BlackHole : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly List<TcpClient> _held = new();

        internal BlackHole()
        {
            _listener.Start();
            _ = AcceptAsync();
        }

        internal string Address => $"127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync();
                    lock (_held) _held.Add(client);
                }
            }
            catch
            {
                // Listener stopped.
            }
        }

        public void Dispose()
        {
            _listener.Stop();
            lock (_held)
            {
                foreach (TcpClient client in _held) client.Close();
            }
        }
    }

    // Node A is listed at an address that never completes the handshake
    // (so the client installs it with no connection and the dial timeout in
    // its cooldown); discovery then drops A. Each operation on A's key must
    // reroute to B.
    public static IEnumerable<object[]> Operations() => new[]
    {
        new object[] { "incr" },
        new object[] { "decr" },
        new object[] { "put-if-absent" },
        new object[] { "replace-if-present" },
        new object[] { "delete-if-matches" },
        new object[] { "touch-ttl-0" },
        new object[] { "get-and-touch-ttl-0" },
    };

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task ADialThatTimedOutIsRerouted(string operation)
    {
        string key = KeyWhosePrimaryIs(Names[0]);
        using var blackHole = new BlackHole();
        using var nodeB = new MockNode();
        using var discovery = new MockDiscovery(
            new List<(string, string)> { (Names[0], blackHole.Address), (Names[1], nodeB.Address) }, 1);
        nodeB.Store[MockNode.KeyOf(Encoding.UTF8.GetBytes(key))] = Encoding.UTF8.GetBytes("5");

        TimeSpan original = Identify.ConnectDeadline;
        Identify.ConnectDeadline = TimeSpan.FromMilliseconds(300);
        try
        {
            using NanocachedClient client = await NanocachedClient.ConnectAsync(
                new() { Addresses = { ("127.0.0.1", discovery.Port) } });
            discovery.SetNodes(new List<(string, string)> { (Names[1], nodeB.Address) });

            switch (operation)
            {
                case "incr":
                    Assert.Equal(6L, await client.IncrAsync(key, 1));
                    break;
                case "decr":
                    Assert.Equal(4L, await client.DecrAsync(key, 1));
                    break;
                case "put-if-absent":
                    Assert.False(await client.PutIfAbsentAsync(key, "other"));
                    break;
                case "replace-if-present":
                    Assert.True(await client.ReplaceIfPresentAsync(key, "other"));
                    break;
                case "delete-if-matches":
                    Assert.False(await client.DeleteIfMatchesAsync(key, new string('0', 32)));
                    break;
                case "touch-ttl-0":
                    Assert.True(await client.TouchAsync(key, 0));
                    break;
                case "get-and-touch-ttl-0":
                    Assert.NotNull(await client.GetAndTouchAsync(key, 0));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation));
            }
        }
        finally
        {
            Identify.ConnectDeadline = original;
        }
    }

    [Fact]
    public async Task TheDeadlineExceptionIsMarkedNotSent()
    {
        using var blackHole = new BlackHole();
        var (host, port) = Identify.SplitHostPort(blackHole.Address);

        TimeSpan original = Identify.ConnectDeadline;
        Identify.ConnectDeadline = TimeSpan.FromMilliseconds(200);
        try
        {
            var lost = await Assert.ThrowsAsync<ConnectionLostException>(
                () => Identify.ConnectAndIdentifyAsync(host, port, null, null));
            Assert.True(lost.RequestNotSent);
        }
        finally
        {
            Identify.ConnectDeadline = original;
        }
    }
}
