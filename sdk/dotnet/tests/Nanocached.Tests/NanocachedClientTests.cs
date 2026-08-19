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
        Assert.True(stopwatch.ElapsedMilliseconds >= 80, "SetAsync should have waited for the replica");
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

            Assert.True(elapsed.Any(ms => ms >= 150), $"expected at least one call to fall back to synchronous, got [{string.Join(",", elapsed)}]");
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
}
