using System.Reflection;
using System.Security.Cryptography;
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

    private static NanocachedClient.Options SingleAddress(string host, int port) =>
        new() { Addresses = { (host, port) } };

    private static NanocachedClient.Options ManyAddresses(params (string Host, int Port)[] addresses)
    {
        var options = new NanocachedClient.Options();
        foreach (var address in addresses) options.Addresses.Add(address);
        return options;
    }

    private static async Task<string> CaptureStderrAsync(Func<Task> action)
    {
        TextWriter original = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            await action();
        }
        finally
        {
            Console.SetError(original);
        }
        return captured.ToString();
    }

    private static int CountOccurrences(string haystack, string needle) =>
        haystack.Split(needle, StringSplitOptions.None).Length - 1;

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
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("greeting", "hello");
        Assert.Equal("hello", await client.GetAsync("greeting"));
        Assert.Equal(Bytes("hello"), await client.GetBytesAsync("greeting"));
        Assert.True(await client.DeleteAsync("greeting"));
        Assert.Null(await client.GetAsync("greeting"));
        Assert.Null(await client.GetBytesAsync("greeting"));
        Assert.False(await client.DeleteAsync("greeting"));
        Assert.Equal(1, client.Replication);
    }

    [Fact]
    public async Task GetBytesRoundTripsRawByteValues()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        byte[] value = { 0, 1, 2, 254, 255 };
        await client.SetAsync(Bytes("k"), value);
        Assert.Equal(value, await client.GetBytesAsync("k"));
        Assert.Equal(value, await client.GetBytesAsync(Bytes("k")));
    }

    [Fact]
    public async Task GetRejectsANonUtf8ValueButGetBytesReturnsItRaw()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        byte[] invalid = { 0xFF, 0xFE, 0x00 };
        node.Store[MockNode.KeyOf(Bytes("bad"))] = invalid;

        await Assert.ThrowsAsync<DecoderFallbackException>(() => client.GetAsync("bad"));
        Assert.Equal(invalid, await client.GetBytesAsync("bad"));
    }

    [Fact]
    public async Task TtlZeroMeansNoExpiryAndNegativeTtlIsRejectedSynchronously()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v", 60);
        await client.SetAsync("no-expiry", "v"); // ttlSeconds defaults to 0
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => client.SetAsync(Bytes("k"), Bytes("v"), -1));
        // The rejected set must not have poisoned the connection.
        Assert.Equal("v", await client.GetAsync("k"));
    }

    [Fact]
    public async Task PipelinesConcurrentRequestsOnOneConnection()
    {
        // Same shape as the TypeScript SDK's own pipelining test: N
        // concurrent requests on a single connection, each independently
        // verified to round-trip its own value (doc/adr/0016-*.md) — a
        // bug in matching responses to the right caller in send order
        // would show up as swapped or wrong values here.
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        const int n = 20;
        await Task.WhenAll(Enumerable.Range(0, n).Select(i => client.SetAsync($"key-{i}", $"value-{i}")));

        string?[] values = await Task.WhenAll(Enumerable.Range(0, n).Select(i => client.GetAsync($"key-{i}")));
        for (int i = 0; i < n; i++)
        {
            Assert.Equal($"value-{i}", values[i]);
        }
    }

    [Fact]
    public async Task Authenticates()
    {
        using var node = new MockNode(requiredSecret: "s3cret");

        using (NanocachedClient client = await NanocachedClient.ConnectAsync(
                   new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) }, AuthSecret = "s3cret" }))
        {
            await client.SetAsync("k", "v");
            Assert.Equal("v", await client.GetAsync("k"));
        }

        NanocachedException missing = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port)));
        Assert.Contains("requires authentication", missing.Message);

        NanocachedException wrong = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(
                new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) }, AuthSecret = "wrong" }));
        Assert.Contains("authentication failed", wrong.Message);
    }

    [Fact]
    public async Task WrongNodePropagatesInSingleMode()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
        node.AnswerWrongNodeOnce();
        await Assert.ThrowsAsync<WrongNodeException>(() => client.GetAsync("k"));
    }

    [Fact]
    public async Task RejectsUseAfterClose()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
        client.Close();
        client.Close(); // idempotent
        Assert.True(client.IsClosed);
        await Assert.ThrowsAsync<AlreadyClosedException>(() => client.GetAsync("k"));
    }

    [Fact]
    public async Task WarnsOnceWhenCloseIsCalledASecondTime()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        string output = await CaptureStderrAsync(async () =>
        {
            client.Close();
            client.Close();
            await Task.CompletedTask;
        });

        Assert.Equal(
            1, CountOccurrences(output, "nanocached: close() called again on an already-closed client"));
    }

    [Fact]
    public async Task WarnsWhenConnectAsyncIsCalledAgainForAStillOpenSingleAddress()
    {
        using var node = new MockNode();
        using NanocachedClient first = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedClient? second = null;
        string output = await CaptureStderrAsync(async () =>
        {
            second = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
        });

        using (second)
        {
            Assert.Contains(
                $"nanocached: connect() called for 127.0.0.1:{node.Port} while a previous connection "
                + "to it is still open — was close() forgotten?",
                output);
        }
    }

    [Fact]
    public async Task DoesNotWarnAboutAForgottenCloseForMultiAddressConfigs()
    {
        // Legitimate concurrent clients against the same discovery replica
        // must not false-positive (issue #12).
        using var node = new MockNode();
        using var discovery = new MockDiscovery(new[] { (Names[0], node.Address) });
        int dead = Wire.UnusedPort();

        using NanocachedClient first = await NanocachedClient.ConnectAsync(
            ManyAddresses(("127.0.0.1", dead), ("127.0.0.1", discovery.Port)));

        NanocachedClient? second = null;
        string output = await CaptureStderrAsync(async () =>
        {
            second = await NanocachedClient.ConnectAsync(
                ManyAddresses(("127.0.0.1", dead), ("127.0.0.1", discovery.Port)));
        });

        using (second)
        {
            Assert.DoesNotContain("forgotten", output);
        }
    }

    // ── 遅延再接続と keep-alive ───────────────────────────────────

    [Fact]
    public async Task AMalformedValueLengthPoisonsTheConnectionAndRetriesTransparently()
    {
        // Regression for issue #8: a garbage V header must be
        // connection-classified so the built-in redial-and-retry-once
        // makes the same call succeed, never serving stray bytes.
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        node.AnswerMalformedValueOnce();
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal(2, node.ConnectionCount);
    }

    [Fact]
    public async Task AMismatchedResponseKindPoisonsTheConnection()
    {
        // A well-formed response of the wrong kind (`S` answering a G)
        // means the request/response streams are off by one; reusing the
        // connection would answer every later request with the previous
        // one's response. The mismatch poisons the connection, and the
        // connection-classified error is healed by the built-in
        // redial-and-retry-once — never by reusing the desynced stream.
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        node.AnswerStoredToGetOnce();
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal(2, node.ConnectionCount);
    }

    [Fact]
    public async Task ConnectingToASilentServerFailsWithinTheDeadline()
    {
        // A server that accepts the TCP connection but never answers the
        // handshake (a blackholed address behaves the same way) must fail
        // the connect within the deadline instead of hanging.
        var silent = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        silent.Start();
        int port = ((System.Net.IPEndPoint)silent.LocalEndpoint).Port;
        TimeSpan original = Identify.ConnectDeadline;
        Identify.ConnectDeadline = TimeSpan.FromMilliseconds(100);
        try
        {
            await Assert.ThrowsAsync<ConnectionLostException>(
                () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", port)));
        }
        finally
        {
            Identify.ConnectDeadline = original;
            silent.Stop();
        }
    }

    [Fact]
    public async Task RepeatedTlsHandshakeFailuresEachFailPromptlyWithoutLeaking()
    {
        // Regression: OpenAsync's catch block used to call only
        // socket.Dispose() on a failed TLS handshake, leaving the
        // SslStream/NetworkStream wrapping it undisposed — so repeated
        // failed handshakes accumulated undisposed streams. There's no
        // clean outside observation of stream disposal, so this proves
        // the fix's externally visible contract instead: many failed
        // handshakes against the same never-TLS-speaking listener each
        // fail promptly and independently, with no hang or crash from
        // accumulating undisposed streams.
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        int port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        _ = AcceptAndCloseForeverAsync(listener);

        try
        {
            for (int i = 0; i < 20; i++)
            {
                await Assert.ThrowsAnyAsync<Exception>(() => NanocachedClient.ConnectAsync(new NanocachedClient.Options
                {
                    Addresses = { ("127.0.0.1", port) },
                    Tls = true,
                }));
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task AcceptAndCloseForeverAsync(System.Net.Sockets.TcpListener listener)
    {
        while (true)
        {
            System.Net.Sockets.TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync();
            }
            catch
            {
                return;
            }
            _ = Task.Run(async () =>
            {
                try
                {
                    // Not a valid TLS record — the client's handshake fails.
                    await client.GetStream().WriteAsync(new byte[] { 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 });
                }
                catch
                {
                    // The client may have already given up.
                }
                finally
                {
                    client.Close();
                }
            });
        }
    }

    [Fact]
    public async Task TransparentlyReconnectsAfterAServerFin()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        node.DropConnections();
        await Task.Delay(50); // let the FIN land
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal(2, node.ConnectionCount);
    }

    [Fact]
    public async Task KeepAlivePingsAnIdleConnection()
    {
        // Keep-alive is always on with an internal interval (issue #27);
        // the internal field exists only so tests can shorten it.
        TimeSpan defaultInterval = NanocachedClient.KeepAliveInterval;
        NanocachedClient.KeepAliveInterval = TimeSpan.FromMilliseconds(40);
        try
        {
            using var node = new MockNode();
            using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

            await WaitForAsync(() => node.GetCount >= 2, "keep-alive pings");
            Assert.Equal(1, node.ConnectionCount);
        }
        finally
        {
            NanocachedClient.KeepAliveInterval = defaultInterval;
        }
    }

    [Fact]
    public async Task ARequestToAHalfOpenServerFailsWithinTheTimeoutInsteadOfHanging()
    {
        // Regression (issue #42): a server that completes the A handshake
        // but then never answers a G/S/D used to hang Get/Set/Delete
        // forever — there was no in-flight request timeout at all. The
        // internal field exists only so tests can shorten it.
        TimeSpan defaultTimeout = Connection.RequestTimeout;
        Connection.RequestTimeout = TimeSpan.FromMilliseconds(150);
        try
        {
            using var node = new MockNode();
            using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
            await client.SetAsync("k", "v");
            node.GoSilentAfterHandshake();

            var started = System.Diagnostics.Stopwatch.StartNew();
            // The client's retry layer redials once after the first
            // timeout; the redialed connection times out too, so this
            // settles after roughly two windows — still bounded.
            var error = await Assert.ThrowsAsync<ConnectionLostException>(() => client.GetAsync("k"));
            Assert.Contains("request timed out", error.Message);
            Assert.True(started.ElapsedMilliseconds < 2_000,
                $"GetAsync took {started.ElapsedMilliseconds}ms, want well under 2s");
        }
        finally
        {
            Connection.RequestTimeout = defaultTimeout;
        }
    }

    [Fact]
    public async Task SteadyNewRequestsDoNotPostponeHalfOpenDetection()
    {
        // The deadline is progress-based: new sends must not extend it
        // while an older request is still waiting (mirrors the Go SDK's
        // regression test of the same name).
        TimeSpan defaultTimeout = Connection.RequestTimeout;
        Connection.RequestTimeout = TimeSpan.FromMilliseconds(200);
        try
        {
            using var node = new MockNode();
            using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
            await client.SetAsync("k", "v");
            node.GoSilentAfterHandshake();

            // New requests keep arriving well inside every deadline
            // window (once the connection is poisoned they just fail
            // fast).
            using var stop = new CancellationTokenSource();
            Task ticker = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(50, stop.Token).ContinueWith(_ => { });
                    try { await client.GetAsync("more"); }
                    catch (Exception) { /* expected once poisoned */ }
                }
            });
            try
            {
                var started = System.Diagnostics.Stopwatch.StartNew();
                var error = await Assert.ThrowsAsync<ConnectionLostException>(() => client.GetAsync("k"));
                Assert.Contains("request timed out", error.Message);
                Assert.True(started.ElapsedMilliseconds < 2_000,
                    $"GetAsync took {started.ElapsedMilliseconds}ms, want well under 2s");
            }
            finally
            {
                stop.Cancel();
                await ticker;
            }
        }
        finally
        {
            Connection.RequestTimeout = defaultTimeout;
        }
    }

    [Fact]
    public async Task CloseFiresOnClosedExactlyOnceUnderConcurrency()
    {
        // Regression: the old "if (_closed) return; _closed = true;"
        // check-then-set let concurrent Close() calls both pass the
        // check, double-firing onClosed and corrupting the open-target
        // counter it decrements. Interlocked.Exchange makes the gate
        // atomic instead.
        using var node = new MockNode();
        using var raw = new System.Net.Sockets.TcpClient();
        await raw.ConnectAsync("127.0.0.1", node.Port);

        int closedCount = 0;
        var connection = new Connection(raw.GetStream(), onClosed: () => Interlocked.Increment(ref closedCount));

        await Task.WhenAll(Enumerable.Range(0, 50).Select(_ => Task.Run(connection.Close)));

        Assert.Equal(1, closedCount);
    }

    // ── addresses ─────────────────────────────────────────────────

    [Fact]
    public async Task RejectsAMissingTarget()
    {
        await Assert.ThrowsAsync<ArgumentException>(
            () => NanocachedClient.ConnectAsync(new NanocachedClient.Options()));
    }

    [Fact]
    public async Task FailsOverToTheSecondAddress()
    {
        using var node = new MockNode();
        using var discovery = new MockDiscovery(new[] { (Names[0], node.Address) });
        int dead = Wire.UnusedPort();

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ManyAddresses(("127.0.0.1", dead), ("127.0.0.1", discovery.Port)));
        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
    }

    [Fact]
    public async Task SkipsAWarmingUpAddress()
    {
        using var node = new MockNode();
        using var warming = new MockDiscovery(new[] { (Names[0], node.Address) });
        using var healthy = new MockDiscovery(new[] { (Names[0], node.Address) });
        warming.WarmingUp = true;

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ManyAddresses(("127.0.0.1", warming.Port), ("127.0.0.1", healthy.Port)));
        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
    }

    [Fact]
    public async Task RaisesBusyWhenEveryAddressIsWarming()
    {
        using var first = new MockDiscovery(Array.Empty<(string, string)>());
        using var second = new MockDiscovery(Array.Empty<(string, string)>());
        first.WarmingUp = true;
        second.WarmingUp = true;

        await Assert.ThrowsAsync<DiscoveryBusyException>(
            () => NanocachedClient.ConnectAsync(
                ManyAddresses(("127.0.0.1", first.Port), ("127.0.0.1", second.Port))));
    }

    // ── discovery response limits ────────────────────────────────

    [Fact]
    public async Task RejectsANodeCountBeyondTheMaximum()
    {
        // Regression: `N 2000000001 3` used to drive
        // `new List<DiscoveredNode>(count)` straight from the wire — a
        // multi-gigabyte allocation from an untrusted server. The header
        // alone must be rejected before any entry is read.
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.RawListResponse = "N 2000000001 3\n";

        NanocachedException error = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", discovery.Port)));
        Assert.Contains("node count", error.Message);
    }

    [Fact]
    public async Task RejectsANodeListResponseBeyondTheAggregateCap()
    {
        // Regression: a within-cap node count can still declare an
        // absurd per-entry name/address length. A single entry near the
        // 16 MiB aggregate cap must be rejected before its body is even
        // read — otherwise a malicious server could make the client
        // allocate gigabytes without ever sending that many bytes.
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        const int hugeNameLength = 20 * 1024 * 1024; // > 16 MiB alone
        discovery.RawListResponse = $"N 1 1\n{hugeNameLength} 0\n";

        NanocachedException error = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", discovery.Port)));
        Assert.Contains("exceeds", error.Message);
    }

    [Fact]
    public async Task RejectsAMalformedNodeCountHeader()
    {
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.RawListResponse = "N x 1\n";

        await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", discovery.Port)));
    }

    [Fact]
    public async Task RejectsAMalformedNodeEntryLengthHeader()
    {
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.RawListResponse = "N 1 1\nx y\n";

        await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", discovery.Port)));
    }

    [Fact]
    public async Task RejectsAMalformedNodeAddressPort()
    {
        using var discovery = new MockDiscovery(new[] { (Names[0], "127.0.0.1:notaport") });

        NanocachedException error = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", discovery.Port)));
        Assert.Contains("invalid node address", error.Message);
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
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        for (int i = 0; i < 50; i++) await client.SetAsync($"key-{i}", $"value-{i}");
        for (int i = 0; i < 50; i++)
        {
            Assert.Equal($"value-{i}", await client.GetAsync($"key-{i}"));
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
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        await client.SetAsync("some-key", "v");
        MockNode owner = cluster.Nodes[new HashRing(Names).Route(Bytes("some-key"))];

        owner.AnswerWrongNodeOnce();
        Assert.Equal("v", await client.GetAsync("some-key"));

        owner.AnswerWrongNodeOnce();
        owner.AnswerWrongNodeOnce();
        await Assert.ThrowsAsync<WrongNodeException>(() => client.GetAsync("some-key"));
    }

    [Fact]
    public async Task FansWritesOutToEveryOwner()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));
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
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        await client.SetAsync("survives", "still here");
        cluster.Nodes[OwnersOf("survives")[0]].Dispose();
        await Task.Delay(50);

        Assert.Equal("still here", await client.GetAsync("survives"));
    }

    [Fact]
    public async Task ADeadReplicaDoesNotFailWrites()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        IReadOnlyList<string> owners = OwnersOf("written-anyway");
        cluster.Nodes[owners[1]].Dispose();
        await Task.Delay(50);

        await client.SetAsync("written-anyway", "v");
        Assert.True(cluster.Nodes[owners[0]].Store.ContainsKey(MockNode.KeyOf(Bytes("written-anyway"))));
        Assert.Equal("v", await client.GetAsync("written-anyway"));
    }

    [Fact]
    public async Task WritesRouteAroundADeadPrimaryOnceDiscoveryDropsIt()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        const string key = "written-after-primary-death";
        IReadOnlyList<string> owners = OwnersOf(key);

        // The primary dies AND discovery has already noticed: the first
        // write attempt fails on the dead primary, forcing a refresh that
        // re-ranks onto the survivor, and the retry succeeds.
        cluster.Nodes[owners[0]].Dispose();
        cluster.Discovery.SetNodes(new[] { (owners[1], cluster.Nodes[owners[1]].Address) });
        await Task.Delay(50);

        await client.SetAsync(key, "v");
        Assert.Equal("v", await client.GetAsync(key));
    }

    [Fact]
    public async Task FansDeletesOutToEveryOwner()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        await client.SetAsync("gone-everywhere", "v");
        Assert.True(await client.DeleteAsync("gone-everywhere"));
        string stored = MockNode.KeyOf(Bytes("gone-everywhere"));
        foreach (MockNode node in cluster.Nodes.Values)
        {
            Assert.False(node.Store.ContainsKey(stored));
        }
    }

    // ── fire-and-forget レプリカ書き込み (doc/adr/0014-*.md) ──────────

    // A "did it wait for the mock's delay" assertion can't compare the
    // measured elapsed time against the delay exactly: Task.Delay's timer
    // tick and Stopwatch.ElapsedMilliseconds's truncation can each shave a
    // little off, so an 80ms delay can be observed as under 80ms. Slack
    // the lower bound by this much rather than asserting on the boundary;
    // still miles away from the ~0ms an immediate return would show.
    private const long TimingToleranceMillis = 20;

    [Fact]
    public async Task ByDefaultAWriteStillWaitsForTheReplicaLeg()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].DelaySets(80);

        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await client.SetAsync("k", "v");
        Assert.True(stopwatch.ElapsedMilliseconds >= 80 - TimingToleranceMillis, "SetAsync should have waited for the replica");
    }

    [Fact]
    public async Task FireAndForgetReplicasReturnsAsSoonAsThePrimaryAcks()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].DelaySets(200);

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            FireAndForgetReplicas = true,
        });

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await client.SetAsync("k", "v");
        Assert.True(stopwatch.ElapsedMilliseconds < 200, "SetAsync should not have waited for the replica");

        string stored = MockNode.KeyOf(Bytes("k"));
        await WaitForAsync(
            () => cluster.Nodes[owners[1]].Store.ContainsKey(stored),
            "the background write to land on the replica");
    }

    [Fact]
    public async Task FireAndForgetReplicasFallsBackToSynchronousPastTheCap()
    {
        int defaultCap = NanocachedClient.MaxInFlightBackgroundReplicaWrites;
        NanocachedClient.MaxInFlightBackgroundReplicaWrites = 2;
        try
        {
            using Cluster cluster = StartCluster(replication: 2);
            IReadOnlyList<string> owners = OwnersOf("k");
            cluster.Nodes[owners[1]].DelaySets(150);

            using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
            {
                Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
                FireAndForgetReplicas = true,
            });

            Task<long>[] tasks = Enumerable.Range(0, 3).Select(async _ =>
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                await client.SetAsync("k", "v");
                return stopwatch.ElapsedMilliseconds;
            }).ToArray();
            long[] elapsed = await Task.WhenAll(tasks);

            Assert.True(elapsed.Any(ms => ms >= 150 - TimingToleranceMillis), $"expected at least one call to fall back to synchronous, got [{string.Join(",", elapsed)}]");
            Assert.True(elapsed.Any(ms => ms < 150), $"expected at least one call to return fast, got [{string.Join(",", elapsed)}]");
        }
        finally
        {
            NanocachedClient.MaxInFlightBackgroundReplicaWrites = defaultCap;
        }
    }

    [Fact]
    public async Task CloseDrainsInFlightBackgroundReplicaWrites()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].DelaySets(80);

        NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            FireAndForgetReplicas = true,
        });

        await client.SetAsync("k", "v");
        client.Close(); // should block until the still-in-flight replica write lands

        string stored = MockNode.KeyOf(Bytes("k"));
        Assert.True(
            cluster.Nodes[owners[1]].Store.ContainsKey(stored),
            "Close() returned before the background replica write finished");
    }

    // ── read repair (doc/adr/0015-*.md) ────────────────────────────

    [Fact]
    public async Task ByDefaultACleanMissOnThePrimaryIsNotRepaired()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].Store[MockNode.KeyOf(Bytes("k"))] = Bytes("from-replica");

        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        Assert.Null(await client.GetBytesAsync("k"));
        Assert.False(cluster.Nodes[owners[0]].Store.ContainsKey(MockNode.KeyOf(Bytes("k"))));
    }

    [Fact]
    public async Task FindsAValueOnAReplicaAndRepairsThePrimary()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].Store[MockNode.KeyOf(Bytes("k"))] = Bytes("from-replica");

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadRepair = true,
        });

        Assert.Equal(Bytes("from-replica"), await client.GetBytesAsync("k"));

        string stored = MockNode.KeyOf(Bytes("k"));
        await WaitForAsync(
            () => cluster.Nodes[owners[0]].Store.ContainsKey(stored),
            "the primary to be repaired");
        // The original TTL can't be recovered from a GET; a repair must
        // not use TTL 0 (no expiry), which would permanently resurrect
        // already-expired data — see ReadRepairTtlSeconds.
        Assert.Equal(60, cluster.Nodes[owners[0]].LastSetTtl);
    }

    [Fact]
    public async Task StaysACleanMissWhenNoOwnerHasTheValue()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadRepair = true,
        });

        Assert.Null(await client.GetBytesAsync("nowhere"));
    }

    // ── Stats() と catch の絞り込み ─────────────────────────────────

    [Fact]
    public async Task ADeadReplicaIncrementsReplicaWriteFailures()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].Dispose();
        await Task.Delay(50);

        Assert.Equal(0, client.Stats().ReplicaWriteFailures);
        await client.SetAsync("k", "v");
        Assert.Equal(1, client.Stats().ReplicaWriteFailures);
    }

    [Fact]
    public async Task AFailedPrimaryRepairIncrementsReadRepairFailures()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].Store[MockNode.KeyOf(Bytes("k"))] = Bytes("from-replica");
        // The initial GET (and read-repair's own probe) against the
        // primary must still see a clean miss, so only the later repair
        // SET is made to fail.
        cluster.Nodes[owners[0]].AnswerWrongNodeOnSetOnce();

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadRepair = true,
        });

        Assert.Equal(Bytes("from-replica"), await client.GetBytesAsync("k"));

        await WaitForAsync(() => client.Stats().ReadRepairFailures > 0, "the failed repair to be counted");
        Assert.False(cluster.Nodes[owners[0]].Store.ContainsKey(MockNode.KeyOf(Bytes("k"))));
    }

    [Fact]
    public async Task AnUnreachableAddressDuringRefreshIncrementsRefreshFailures()
    {
        using Cluster cluster = StartCluster(replication: 1);
        int deadPort = Wire.UnusedPort();

        // The dead address is tried first on every address walk, so both
        // the initial connect (which falls over to the real discovery
        // server) and every later refresh attempt it and fail first.
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ManyAddresses(("127.0.0.1", deadPort), ("127.0.0.1", cluster.Discovery.Port)));

        const string key = "written-after-primary-death";
        IReadOnlyList<string> owners = OwnersOf(key);

        // Force a node-list refresh (as in WritesRouteAroundADeadPrimaryOnceDiscoveryDropsIt):
        // the primary dies, discovery already knows, so the first write
        // attempt fails and forces a refresh that walks _addresses —
        // hitting deadPort before the real discovery server.
        cluster.Nodes[owners[0]].Dispose();
        cluster.Discovery.SetNodes(new[] { (owners[1], cluster.Nodes[owners[1]].Address) });
        await Task.Delay(50);

        await client.SetAsync(key, "v");
        Assert.Equal("v", await client.GetAsync(key));
        Assert.True(client.Stats().RefreshFailures >= 1);
    }

    // A minimal Stream whose writes throw a plain programming-error
    // exception (not a connection failure) — for regression-testing that
    // the narrowed swallow-site catches let such bugs propagate instead
    // of treating them the same as a dead replica.
    private sealed class ThrowingStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() { }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => 0;

        // Never completes — this stream is never actually read from in
        // the regression test; only the write side needs to misbehave.
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            new(new TaskCompletionSource<int>().Task);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw Bug();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            throw Bug();

        private static InvalidOperationException Bug() =>
            new("nanocached test: synthetic programming-error bug, not a connection failure");
    }

    // Swaps a connected cluster member's live connection for one wired to
    // ThrowingStream. Member and its Connection property are private/
    // internal, so this reaches them via reflection — the only way to
    // inject a fault below the public API for this regression test.
    private static void ReplaceMemberConnection(NanocachedClient client, string name, Connection connection)
    {
        FieldInfo membersField = typeof(NanocachedClient)
            .GetField("_members", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var members = (System.Collections.IDictionary)membersField.GetValue(client)!;
        object member = members[name]!;
        PropertyInfo connectionProperty = member.GetType()
            .GetProperty("Connection", BindingFlags.NonPublic | BindingFlags.Instance)!;
        connectionProperty.SetValue(member, connection);
    }

    [Fact]
    public async Task ANonConnectionExceptionOnAReplicaLegPropagatesInsteadOfBeingSwallowed()
    {
        // Regression for the catch-narrowing at the replica-write swallow
        // site (WriteAsync's ReplicaWriteAsync): only the connection
        // layer's own failure types are caught there now, so a
        // programming bug — here, a stubbed connection whose stream
        // throws InvalidOperationException on write — must propagate
        // instead of being swallowed identically to a dead replica.
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        string replica = OwnersOf("k")[1];
        ReplaceMemberConnection(client, replica, new Connection(new ThrowingStream()));

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.SetAsync("k", "v"));
    }

    // ── 値の圧縮 (doc/adr/0013-*.md) ──────────────────────────────

    private static NanocachedClient.Options CompressingOptions(int port, int threshold = 256) =>
        new()
        {
            Addresses = { ("127.0.0.1", port) },
            Compress = true,
            CompressionThreshold = threshold,
        };

    [Fact]
    public async Task WireFormatIsUntouchedWhenCompressIsOff()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        string value = new string('x', 1000);
        await client.SetAsync("k", value);

        Assert.Equal(Bytes(value), node.Store[MockNode.KeyOf(Bytes("k"))]);
        Assert.Equal(value, await client.GetAsync("k"));
    }

    [Fact]
    public async Task CompressesAtOrAboveTheThresholdAndDecompressesBack()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(CompressingOptions(node.Port, 64));

        string value = new string('x', 1000);
        await client.SetAsync("k", value);

        byte[] stored = node.Store[MockNode.KeyOf(Bytes("k"))];
        Assert.Equal(0x01, stored[0]);
        Assert.True(stored.Length < Bytes(value).Length);

        Assert.Equal(value, await client.GetAsync("k"));
        Assert.Equal(Bytes(value), await client.GetBytesAsync("k"));
    }

    [Fact]
    public async Task BelowThresholdValueIsPrefixedButNotCompressed()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(CompressingOptions(node.Port));

        await client.SetAsync("k", "short");

        byte[] expected = new byte[] { 0x00 }.Concat(Bytes("short")).ToArray();
        Assert.Equal(expected, node.Store[MockNode.KeyOf(Bytes("k"))]);
        Assert.Equal("short", await client.GetAsync("k"));
    }

    [Fact]
    public async Task IncompressibleDataPassesThroughUnbloated()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(CompressingOptions(node.Port, 16));

        byte[] value = new byte[512];
        RandomNumberGenerator.Fill(value);

        await client.SetAsync(Bytes("k"), value);

        byte[] expected = new byte[] { 0x00 }.Concat(value).ToArray();
        Assert.Equal(expected, node.Store[MockNode.KeyOf(Bytes("k"))]);
        Assert.Equal(value, await client.GetBytesAsync("k"));
    }

    [Fact]
    public async Task ReadingALegacyValueWithCompressEnabledErrorsClearly()
    {
        using var node = new MockNode();

        // A legacy/uncompressed writer's value whose first byte happens to
        // collide with the DEFLATE marker (0x01) — doc/adr/0013-*.md's
        // documented hazard of enabling Compress against a keyspace other
        // clients still touch without it. The remaining bytes are chosen
        // to reliably fail DEFLATE decoding (raw DEFLATE has no checksum,
        // so not every garbage body does — see CompressionTests' own
        // pinned test).
        node.Store[MockNode.KeyOf(Bytes("k"))] = new byte[] { 0x01, 0xFF, 0xFF, 0xFF, 0xFF };

        using NanocachedClient reader = await NanocachedClient.ConnectAsync(CompressingOptions(node.Port));
        await Assert.ThrowsAsync<DecompressionException>(() => reader.GetBytesAsync("k"));
    }

    // ── response tags (doc/adr/0019-*.md) ───────────────────────────

    [Fact]
    public async Task PipelinesConcurrentRequestsOnATaggedConnection()
    {
        // Same shape as PipelinesConcurrentRequestsOnOneConnection, but
        // against a tag-supporting node — proves tagged requests/responses
        // round-trip correctly under concurrency, not just the untagged
        // wire format.
        using var node = new MockNode(supportTags: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        const int n = 20;
        await Task.WhenAll(Enumerable.Range(0, n).Select(i => client.SetAsync($"key-{i}", $"value-{i}")));

        string?[] values = await Task.WhenAll(Enumerable.Range(0, n).Select(i => client.GetAsync($"key-{i}")));
        for (int i = 0; i < n; i++)
        {
            Assert.Equal($"value-{i}", values[i]);
        }

        Assert.True(await client.DeleteAsync("key-0"));
        Assert.False(await client.DeleteAsync("key-0"));
    }

    [Fact]
    public async Task AWrongTagResponsePoisonsTheConnectionAndRetriesTransparently()
    {
        // A response echoing a tag other than the oldest pending request's
        // own tag means the streams are misaligned. The read loop must
        // catch this before handing the response to any caller, poison
        // the connection, and — like every other connection-classified
        // failure (see AMismatchedResponseKindPoisonsTheConnection) — the
        // built-in redial-and-retry-once heals it.
        using var node = new MockNode(supportTags: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        node.AnswerWrongTagOnce();
        Assert.Null(await client.GetAsync("k"));
        Assert.Equal(2, node.ConnectionCount);
    }

    [Fact]
    public async Task ASwallowedResponseDesyncIsCaughtBeforeAnyCallerSeesWrongData()
    {
        // The exact misdelivery ADR-0016 left open: the server (as a
        // stand-in for any off-by-one stream corruption) never answers the
        // first GET, so the second GET's response arrives at the first
        // GET's pending slot. Without the ADR-0019 tag check, the first
        // caller would receive the second's value as a plausible,
        // exception-free wrong answer — the classic desync. The tag check
        // must catch this before either caller sees anything wrong, and
        // this SDK's built-in redial-and-retry-once then transparently
        // heals both calls with their own correct results.
        using var node = new MockNode(supportTags: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");

        node.SwallowGetOnce();
        Task<string?> first = client.GetAsync("a");
        Task<string?> second = client.GetAsync("k");

        Assert.Null(await first);
        Assert.Equal("v", await second);
        Assert.Equal(2, node.ConnectionCount);
    }

    [Fact]
    public async Task FallsBackToTheUntaggedProtocolAgainstAPre0019Server()
    {
        // An old server treats `A ... T` as a parse error and closes
        // without replying; the client must redial once with the plain
        // form and run untagged — transparently, with the same results.
        using var node = new MockNode(closeOnExtendedAuth: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
        // Two dials: the extended attempt the server slammed shut, then
        // the plain fallback that stuck.
        Assert.Equal(2, node.ConnectionCount);
    }
}
