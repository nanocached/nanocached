using System.Net;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
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

    // Audit finding D2: an empty key, or a key+value pair large enough to
    // risk the server's MAX_REQUEST_SIZE (src/server.rs, 1 MiB), must be
    // rejected synchronously (into the returned Task, before any bytes
    // reach the connection) — exactly like the ttlSeconds check above.
    [Fact]
    public async Task RejectsEmptyKeysOnGetSetDelete()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        byte[] emptyKey = Array.Empty<byte>();
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetAsync(emptyKey));
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetBytesAsync(emptyKey));
        await Assert.ThrowsAsync<ArgumentException>(() => client.SetAsync(emptyKey, Bytes("v")));
        await Assert.ThrowsAsync<ArgumentException>(() => client.DeleteAsync(emptyKey));
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetAsync(""));
        await Assert.ThrowsAsync<ArgumentException>(() => client.SetAsync("", "v"));
        await Assert.ThrowsAsync<ArgumentException>(() => client.DeleteAsync(""));

        // Rejected client-side, before any request frame — no second
        // connection beyond the initial ConnectAsync() above.
        Assert.Equal(1, node.ConnectionCount);
    }

    [Fact]
    public async Task RejectsOversizeKeyOrKeyPlusValueBeforeTouchingTheConnection()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        // Warm the connection up first so ConnectionCount below only
        // reflects that, not the rejections.
        await client.SetAsync("warm", "up");
        int connectionsBeforeRejections = node.ConnectionCount;

        byte[] oversizeKey = new byte[1024 * 1024];
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.GetAsync(oversizeKey));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.DeleteAsync(oversizeKey));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.SetAsync(oversizeKey, Bytes("v")));

        // A modest key with a value large enough to push key+value over
        // the limit must be rejected too.
        byte[] smallKey = Bytes("k");
        byte[] oversizeValue = new byte[1024 * 1024];
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.SetAsync(smallKey, oversizeValue));

        Assert.Equal(connectionsBeforeRejections, node.ConnectionCount);
        Assert.Equal("up", await client.GetAsync("warm"));
    }

    [Fact]
    public async Task PipelinesConcurrentRequestsOnOneConnection()
    {
        // Same shape as the TypeScript SDK's own pipelining test: N
        // concurrent requests on a single connection, each independently
        // verified to round-trip its own value (request pipelining) — a
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

        // Both shapes are matchable as AuthenticationFailedException
        // (issue #47 item 5), not just by message.
        AuthenticationFailedException missing = await Assert.ThrowsAsync<AuthenticationFailedException>(
            () => NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port)));
        Assert.Contains("requires authentication", missing.Message);

        AuthenticationFailedException wrong = await Assert.ThrowsAsync<AuthenticationFailedException>(
            () => NanocachedClient.ConnectAsync(
                new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) }, AuthSecret = "wrong" }));
        Assert.Contains("authentication failed", wrong.Message);
    }

    [Fact]
    public async Task EmptyAuthSecretBehavesAsNoSecret()
    {
        // Regression: AuthSecret = "" used to be sent literally, reaching
        // the wire as an explicit zero-length secret ("A 0\n") — which a
        // real server rejects (and this mock's `secret.Length > 0` check
        // mirrors) as EmptySecret and closes the connection without
        // replying, turning what should be "no auth configured" into an
        // opaque AuthenticationFailedException/ConnectionLostException.
        // An empty AuthSecret must behave exactly like a null one.
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) }, AuthSecret = "" });

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
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

    // Audit finding D4: Close() cancelled `_lifetime` but never disposed
    // it, leaking the CancellationTokenSource. Reflection is the only way
    // to reach the private field; disposal is observed indirectly by the
    // ObjectDisposedException a disposed CancellationTokenSource's own
    // members throw.
    [Fact]
    public async Task CloseDisposesTheLifetimeCancellationTokenSource()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        FieldInfo lifetimeField = typeof(NanocachedClient)
            .GetField("_lifetime", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var lifetime = (CancellationTokenSource)lifetimeField.GetValue(client)!;

        client.Close();

        Assert.Throws<ObjectDisposedException>(() => lifetime.Token);
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
    public async Task AnExtraByteAfterAnUntaggedResponseMarkerPoisonsTheConnectionAndRetriesTransparently()
    {
        // Regression: the untagged fast path (S/D/N/W) used to read the
        // trailing byte with ReadByteAsync() and discard it unchecked
        // instead of verifying it is '\n' — a byte other than '\n' here
        // means the streams are desynced (e.g. the server tagged a
        // response, "S1\n", on a connection that never asked for tags)
        // and every later response would be misaligned too. Mirrors
        // Java's expectLf() (Connection.java:492-498). The
        // connection-classified error is healed by the built-in
        // redial-and-retry-once, exactly like
        // AMismatchedResponseKindPoisonsTheConnection.
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        node.AnswerExtraByteOnSetOnce();
        await client.SetAsync("k", "v");
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
    public async Task AServerFinWhileARequestIsInFlightSurfacesAsConnectionLost()
    {
        // The FIN lands *after* the request was written but before its
        // reply: the read loop hits end-of-stream with a waiter pending.
        // That waiter must get a ConnectionLostException (the README's
        // "every failure extends NanocachedException" contract, and what
        // the retry layer redials on) — not the raw EndOfStreamException.
        var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
        await client.SetAsync("k", "v");
        node.GoSilentAfterHandshake();

        Task<string?> inFlight = client.GetAsync("k");
        await Task.Delay(50); // the G frame is on the wire, unanswered
        node.Dispose(); // FIN on the pending connection; nothing left to redial

        await Assert.ThrowsAsync<ConnectionLostException>(() => inFlight);
    }

    [Fact]
    public async Task ReconnectCooldownSkipsARedialToAKnownDeadAddress()
    {
        var node = new MockNode();
        int port = node.Port;
        var options = SingleAddress("127.0.0.1", port);
        // Timing: a wide cooldown window and fast-rejection bound keep this from flaking on loaded CI runners
        // (xUnit runs the other test classes in parallel with this one).
        options.ReconnectCooldown = TimeSpan.FromMilliseconds(1000);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(options);

        await client.SetAsync("k", "v");
        node.Dispose();
        await Task.Delay(50); // let the FIN land, as TransparentlyReconnectsAfterAServerFin does

        // Nothing listens on `port` anymore, so this redial fails fast
        // with a connection-refused error and starts the cooldown window
        // for that address.
        var firstError = await Assert.ThrowsAsync<ConnectionLostException>(() => client.GetAsync("k"));

        // A listener now sits on the same port and answers immediately
        // with bytes the identify handshake rejects outright —
        // deliberately not a bare close/reset (the shape that triggers
        // ConnectAndIdentifyAsync's legacy-server fallback redial,
        // Identify.cs), so each dial against it fails after exactly one
        // connection, letting `connections` below tell "cooldown skipped
        // the dial" apart from "cooldown let it through" unambiguously.
        int connections = 0;
        var garbage = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, port);
        garbage.Start();
        _ = AcceptAndAnswerGarbageAsync(garbage, () => Interlocked.Increment(ref connections));

        try
        {
            // Still within the cooldown window: rejects with the cached
            // failure — the very same exception instance — near-instantly,
            // without dialing the listener at all.
            DateTime started = DateTime.UtcNow;
            var secondError = await Assert.ThrowsAsync<ConnectionLostException>(() => client.GetAsync("k"));
            TimeSpan elapsed = DateTime.UtcNow - started;
            Assert.True(elapsed < TimeSpan.FromMilliseconds(500),
                $"expected a cooldown-fast rejection, took {elapsed.TotalMilliseconds}ms");
            Assert.Equal(0, connections);
            Assert.Same(firstError, secondError);

            // Once the cooldown window has passed, the address is dialed
            // again, this time reaching the listener.
            await Task.Delay(1200);
            NanocachedException thirdError = await Assert.ThrowsAsync<NanocachedException>(() => client.GetAsync("k"));
            Assert.Contains("unexpected response to A", thirdError.Message);
            Assert.Equal(1, connections);
        }
        finally
        {
            garbage.Stop();
        }
    }

    [Fact]
    public async Task RejectsANegativeReconnectCooldown()
    {
        // Cross-SDK contract (mirrors Rust/Go): a negative
        // ReconnectCooldown is invalid — DisableReconnectCooldown is the
        // only supported way to disable the cooldown.
        var options = SingleAddress("127.0.0.1", Wire.UnusedPort());
        options.ReconnectCooldown = TimeSpan.FromMilliseconds(-1);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => NanocachedClient.ConnectAsync(options));
    }

    [Fact]
    public async Task ZeroReconnectCooldownMeansTheDefaultNotDisabled()
    {
        // Cross-SDK contract change: TimeSpan.Zero used to disable the
        // cooldown entirely; it now means "use the default" (1s), matching
        // the Go SDK's zero-value Config.ReconnectCooldown and the Rust
        // SDK's Duration::ZERO. Same shape as
        // ReconnectCooldownSkipsARedialToAKnownDeadAddress, but with an
        // explicit TimeSpan.Zero standing in for "unset".
        var node = new MockNode();
        int port = node.Port;
        var options = SingleAddress("127.0.0.1", port);
        options.ReconnectCooldown = TimeSpan.Zero;
        using NanocachedClient client = await NanocachedClient.ConnectAsync(options);

        await client.SetAsync("k", "v");
        node.Dispose();
        await Task.Delay(50); // let the FIN land

        var firstError = await Assert.ThrowsAsync<ConnectionLostException>(() => client.GetAsync("k"));

        int connections = 0;
        var garbage = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, port);
        garbage.Start();
        _ = AcceptAndAnswerGarbageAsync(garbage, () => Interlocked.Increment(ref connections));
        try
        {
            // Still within the (default, ~1s) cooldown window: rejects
            // with the cached failure near-instantly, without dialing the
            // listener at all — proving zero resolved to the default
            // instead of disabling the cooldown.
            var secondError = await Assert.ThrowsAsync<ConnectionLostException>(() => client.GetAsync("k"));
            Assert.Equal(0, connections);
            Assert.Same(firstError, secondError);
        }
        finally
        {
            garbage.Stop();
        }
    }

    [Fact]
    public async Task DisableReconnectCooldownRedialsEveryTime()
    {
        // Options.DisableReconnectCooldown is this SDK's equivalent of
        // Rust's disable_reconnect_cooldown() / Java's
        // disableReconnectCooldown() / Go's negative
        // Config.ReconnectCooldown: every request that finds the
        // connection dead pays its own full dial attempt instead of
        // reusing a cached failure.
        var node = new MockNode();
        int port = node.Port;
        var options = SingleAddress("127.0.0.1", port);
        options.DisableReconnectCooldown = true;
        using NanocachedClient client = await NanocachedClient.ConnectAsync(options);

        await client.SetAsync("k", "v");
        node.Dispose();
        await Task.Delay(50); // let the FIN land

        int connections = 0;
        var garbage = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, port);
        garbage.Start();
        _ = AcceptAndAnswerGarbageAsync(garbage, () => Interlocked.Increment(ref connections));
        try
        {
            // Unlike the cooldown-enabled case, each call dials again —
            // no cooldown window is ever recorded.
            await Assert.ThrowsAsync<NanocachedException>(() => client.GetAsync("k"));
            await Assert.ThrowsAsync<NanocachedException>(() => client.GetAsync("k"));
            Assert.Equal(2, connections);
        }
        finally
        {
            garbage.Stop();
        }
    }

    private static async Task AcceptAndAnswerGarbageAsync(System.Net.Sockets.TcpListener listener, Action onAccepted)
    {
        // Accepted sockets are held open until the listener is stopped,
        // not disposed right after answering: closing them immediately
        // races the client's own `A` frame write, which on Linux then
        // fails with EPIPE (a ConnectionLostException) before the client
        // ever reads the garbage — whereas these tests want the
        // deterministic "unexpected response to A" protocol rejection
        // (a plain NanocachedException) on every dial.
        var held = new List<System.Net.Sockets.TcpClient>();
        while (true)
        {
            System.Net.Sockets.TcpClient accepted;
            try
            {
                accepted = await listener.AcceptTcpClientAsync();
            }
            catch
            {
                foreach (var client in held) client.Dispose();
                return;
            }
            held.Add(accepted);
            onAccepted();
            try
            {
                await accepted.GetStream().WriteAsync(Bytes("XXX"));
            }
            catch
            {
                // The client may have already given up.
            }
        }
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

    // ── namespaces (issue #105) ──────────────────────────────────

    [Fact]
    public async Task NamespaceIsolatesEntriesWithTheSameKeyName()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace users = client.Namespace("users");
        NanocachedNamespace orders = client.Namespace("orders");

        await users.SetAsync("k", "from users");
        await orders.SetAsync("k", "from orders");
        await client.SetAsync("k", "from default");

        // Same key name, three independent entries: the default
        // namespace and each of the two named ones.
        Assert.Equal("from users", await users.GetAsync("k"));
        Assert.Equal("from orders", await orders.GetAsync("k"));
        Assert.Equal("from default", await client.GetAsync("k"));
        Assert.Equal(3, node.Store.Count);

        Assert.True(await users.DeleteAsync("k"));
        Assert.Null(await users.GetAsync("k"));
        // Deleting from "users" must not touch the other two namespaces.
        Assert.Equal("from orders", await orders.GetAsync("k"));
        Assert.Equal("from default", await client.GetAsync("k"));
    }

    [Fact]
    public async Task NamespaceAcceptsRawBytesIncludingNonUtf8()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        byte[] binaryNamespace = { 0xff, 0x00, 0x10 };
        NanocachedNamespace ns = client.Namespace(binaryNamespace);
        Assert.Equal(binaryNamespace, ns.NamespaceBytes);

        await ns.SetAsync(Bytes("k"), Bytes("v"));
        Assert.Equal(Bytes("v"), await ns.GetBytesAsync(Bytes("k")));
        Assert.True(await ns.DeleteAsync(Bytes("k")));
        Assert.Null(await ns.GetBytesAsync(Bytes("k")));
    }

    [Fact]
    public async Task EmptyNamespaceIsEquivalentToTheRootClientAndSendsLegacyFrames()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace root = client.Namespace("");
        Assert.Equal("", root.Namespace);
        Assert.Empty(root.NamespaceBytes);

        await root.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k")); // same store entry as the root client
        Assert.True(await root.DeleteAsync("k"));
        Assert.Null(await client.GetAsync("k"));

        // Namespace("") never reaches the wire as an explicit
        // zero-length-namespace g/s/d frame — it is byte-for-byte the
        // legacy G/S/D form (docs/protocol.html's namespaces section).
        Assert.Equal(0, node.NamespacedRequestCount);
    }

    [Fact]
    public async Task NonEmptyNamespaceUsesTheNamespacedFrames()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace ns = client.Namespace("users");
        await ns.SetAsync("k", "v");
        Assert.Equal("v", await ns.GetAsync("k"));
        Assert.True(await ns.DeleteAsync("k"));

        Assert.Equal(3, node.NamespacedRequestCount); // one g/s/d frame apiece
    }

    [Fact]
    public async Task NamespacedSetWithTtlRoundTripsOnATaggedConnection()
    {
        // Exercises the tagged form of the namespaced s frame with a TTL
        // present: "s <ns-len> <key-len> <val-len> <ttl> <tag>\n<ns><key><value>".
        using var node = new MockNode(supportTags: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace ns = client.Namespace("users");
        await ns.SetAsync("k", "v", ttlSeconds: 60);
        Assert.Equal(60, node.LastSetTtl);
        Assert.Equal("v", await ns.GetAsync("k"));
        Assert.True(await ns.DeleteAsync("k"));
    }

    [Fact]
    public async Task NamespaceHandleThrowsAfterTheOwningClientCloses()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
        NanocachedNamespace ns = client.Namespace("users");
        client.Close();

        await Assert.ThrowsAsync<AlreadyClosedException>(() => ns.GetAsync("k"));
        await Assert.ThrowsAsync<AlreadyClosedException>(() => ns.SetAsync("k", "v"));
        await Assert.ThrowsAsync<AlreadyClosedException>(() => ns.DeleteAsync("k"));
    }

    [Fact]
    public async Task NamespacedKeysRouteByNamespaceAndKeyTogether()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        byte[] namespaceBytes = Bytes("users");
        NanocachedNamespace users = client.Namespace("users");
        for (int i = 0; i < 50; i++) await users.SetAsync($"key-{i}", $"value-{i}");
        for (int i = 0; i < 50; i++)
        {
            Assert.Equal($"value-{i}", await users.GetAsync($"key-{i}"));
        }

        // Routed by HRW over (namespace, key) — the same ring the un-
        // namespaced tests above pin, but with the namespace mixed into
        // the key-side hash, so this lands on whichever owner
        // HashRing.Owners(ns, key, ...) picks, not the un-namespaced key's
        // owner.
        var ring = new HashRing(Names);
        for (int i = 0; i < 50; i++)
        {
            byte[] key = Bytes($"key-{i}");
            string owner = ring.Owners(namespaceBytes, key, 1)[0];
            Assert.True(cluster.Nodes[owner].Store.ContainsKey(MockNode.KeyOf(namespaceBytes, key)));
        }
    }

    [Fact]
    public async Task NamespacedWrongNodeTriggersRefreshAndOneRetryRoutedByNamespaceAndKey()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        byte[] namespaceBytes = Bytes("users");
        NanocachedNamespace ns = client.Namespace("users");
        await ns.SetAsync("some-key", "v");
        MockNode owner = cluster.Nodes[new HashRing(Names).Route(namespaceBytes, Bytes("some-key"))];

        owner.AnswerWrongNodeOnce();
        Assert.Equal("v", await ns.GetAsync("some-key"));

        owner.AnswerWrongNodeOnce();
        owner.AnswerWrongNodeOnce();
        await Assert.ThrowsAsync<WrongNodeException>(() => ns.GetAsync("some-key"));
    }

    // ── clear / clearAll (issue #106) ────────────────────────────

    [Fact]
    public async Task NamespacedClearRemovesOnlyThatNamespaceLeavingOthersAndTheDefaultIntact()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace users = client.Namespace("users");
        NanocachedNamespace orders = client.Namespace("orders");
        await users.SetAsync("k", "from users");
        await orders.SetAsync("k", "from orders");
        await client.SetAsync("k", "from default");

        await users.ClearAsync();

        Assert.Null(await users.GetAsync("k"));
        Assert.Equal("from orders", await orders.GetAsync("k"));
        Assert.Equal("from default", await client.GetAsync("k"));
        Assert.Equal(1, node.ClearRequestCount);
    }

    [Fact]
    public async Task ClearOnTheEmptyNamespaceHandleClearsTheDefaultNamespaceAndIsNotRejected()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace root = client.Namespace("");
        await client.SetAsync("k", "v");

        // Namespace("").ClearAsync() must not be rejected — it sends
        // `c 0\n` (docs/protocol.html's "c / F" section), clearing the
        // default namespace exactly as client.ClearAsync() itself would.
        await root.ClearAsync();

        Assert.Null(await client.GetAsync("k"));
        Assert.Equal(1, node.ClearRequestCount);
    }

    [Fact]
    public async Task ClearAllEmptiesEveryNamespaceIncludingTheDefaultOne()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace users = client.Namespace("users");
        await users.SetAsync("k", "from users");
        await client.SetAsync("k", "from default");

        await client.ClearAllAsync();

        Assert.Null(await users.GetAsync("k"));
        Assert.Null(await client.GetAsync("k"));
        Assert.Equal(1, node.ClearAllRequestCount);
        Assert.Empty(node.Store);
    }

    [Fact]
    public async Task ClearAndClearAllRoundTripOnATaggedConnection()
    {
        // Exercises the tagged forms "c <ns-len> <tag>\n<ns>" and
        // "F <tag>\n" — the response parser must learn C's tagged shape
        // (same as S/D/N/W) alongside the untagged tests above.
        using var node = new MockNode(supportTags: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        NanocachedNamespace ns = client.Namespace("users");
        await ns.SetAsync("k", "v");
        await ns.ClearAsync();
        Assert.Null(await ns.GetAsync("k"));

        await client.SetAsync("k", "v");
        await client.ClearAllAsync();
        Assert.Null(await client.GetAsync("k"));
    }

    [Fact]
    public async Task ClearAllFansOutToEveryNodeRegardlessOfReplicationFactor()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        // Replication 1: each key lands on exactly one of the two nodes —
        // clear is never key-addressed, so it must still reach both.
        for (int i = 0; i < 20; i++) await client.SetAsync($"key-{i}", "v");

        await client.ClearAllAsync();

        foreach (MockNode node in cluster.Nodes.Values)
        {
            Assert.Equal(1, node.ClearAllRequestCount);
            Assert.Empty(node.Store);
        }
    }

    [Fact]
    public async Task NamespacedClearFansOutToEveryNode()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        NanocachedNamespace users = client.Namespace("users");
        for (int i = 0; i < 20; i++) await users.SetAsync($"key-{i}", "v");

        await users.ClearAsync();

        foreach (MockNode node in cluster.Nodes.Values)
        {
            Assert.Equal(1, node.ClearRequestCount);
        }
        for (int i = 0; i < 20; i++)
        {
            Assert.Null(await users.GetAsync($"key-{i}"));
        }
    }

    [Fact]
    public async Task AClearThatFailsOnOneNodeIsRefreshedAndRetriedOnceThenSucceeds()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        MockNode flaky = cluster.Nodes[Names[0]];
        // Two queued failures: the first absorbs the fan-out's own
        // attempt, the second absorbs the lazy reconnect-on-use retry
        // ApplyReconnectingAsync already does at the connection level
        // (SlotConnectionAsync) — only once *both* are exhausted does the
        // failure reach the outer fan-out, which then refreshes the node
        // list once and retries against *every* node of the refreshed
        // list (not just the one that failed), succeeding once the mock
        // has nothing left queued.
        flaky.FailClearOnce();
        flaky.FailClearOnce();

        await client.ClearAllAsync(); // must not throw

        Assert.Equal(3, flaky.ClearAllRequestCount); // 2 failures + the retry pass
        Assert.Equal(2, cluster.Nodes[Names[1]].ClearAllRequestCount); // first pass + retry pass
    }

    [Fact]
    public async Task AClearThatKeepsFailingRaisesAnErrorNamingTheNode()
    {
        using Cluster cluster = StartCluster(replication: 1);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        string deadName = Names[0];
        // Dies but discovery hasn't evicted it yet — its own membership
        // list is unchanged, so refresh still finds it and the retry
        // reaches the same dead node again.
        cluster.Nodes[deadName].Dispose();

        ConnectionLostException error = await Assert.ThrowsAsync<ConnectionLostException>(
            () => client.ClearAllAsync());
        Assert.Contains(deadName, error.Message);
    }

    [Fact]
    public async Task ClearAndClearAllThrowAfterClose()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));
        NanocachedNamespace ns = client.Namespace("users");
        client.Close();

        await Assert.ThrowsAsync<AlreadyClosedException>(() => ns.ClearAsync());
        await Assert.ThrowsAsync<AlreadyClosedException>(() => client.ClearAllAsync());
    }

    // ── fire-and-forget レプリカ書き込み (fire-and-forget replica writes) ──────────

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

    // ── read repair (read repair) ────────────────────────────

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
    public async Task ReadRepairProbesOnlyTheRemainingOwnersNotThePrimary()
    {
        // Regression: TryReadRepairAsync used to re-probe every owner,
        // including the primary that the normal read path had already
        // just probed for a clean miss — a redundant GET against a
        // connection that's about to be repaired anyway.
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[1]].Store[MockNode.KeyOf(Bytes("k"))] = Bytes("from-replica");

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadRepair = true,
        });

        Assert.Equal(Bytes("from-replica"), await client.GetBytesAsync("k"));

        // Exactly one GET on the primary: the normal read path's own
        // clean-miss probe. Read repair itself must not probe it again.
        Assert.Equal(1, cluster.Nodes[owners[0]].GetCount);
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
    public async Task ANonConnectionExceptionOnASynchronousReplicaLegIsLoggedButDoesNotFailASuccessfulPrimaryWrite()
    {
        // Regression for WriteAsync's primary/replica join (issue: audit
        // finding — a `finally { await Task.WhenAll(replicaWrites); }`
        // let an uncaught replica-leg bug REPLACE the try block's outcome,
        // turning a successful primary write into a thrown exception).
        // The catch-narrowing at the replica-write swallow site
        // (ReplicaWriteAsync) still only catches the connection layer's
        // own failure types, so a programming bug — here, a stubbed
        // connection whose stream throws InvalidOperationException on
        // write — still escapes it and must not be silently dropped
        // (counted identically to a dead replica's
        // Stats().ReplicaWriteFailures); but since the primary write
        // already succeeded, SetAsync must report success, with the bug
        // only logged to stderr. Mirrors the TypeScript SDK's
        // writeToOwners (client.ts, ~767-789) and the Python SDK's
        // equivalent _warn() path.
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        string replica = OwnersOf("k")[1];
        ReplaceMemberConnection(client, replica, new Connection(new ThrowingStream()));

        string output = await CaptureStderrAsync(async () => await client.SetAsync("k", "v"));

        Assert.Contains("nanocached: a replica write raised an unexpected error", output);
        Assert.Equal(0, client.Stats().ReplicaWriteFailures);
        Assert.Equal("v", await client.GetAsync("k"));
    }

    [Fact]
    public async Task WhenThePrimaryFailsAndAReplicaLegBugsTooTheReplicaBugIsThrown()
    {
        // Second of the three finally-join combinations (see the test
        // above and the one below): primary failure AND a replica leg
        // that throws a non-SDK exception (a programming bug, not one of
        // the connection-layer failure types ReplicaWriteAsync already
        // swallows) — the bug must win, since the primary write itself
        // never landed anywhere and this is the only signal that
        // something is badly wrong with the replica leg too.
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[0]].Dispose(); // the primary dies
        await Task.Delay(50); // let the FIN land
        ReplaceMemberConnection(client, owners[1], new Connection(new ThrowingStream()));

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.SetAsync("k", "v"));
    }

    [Fact]
    public async Task WhenThePrimaryFailsAndReplicasAreFineThePrimarysOwnErrorPropagates()
    {
        // Third combination: primary failure with no replica bug (the
        // replica leg is healthy) — the primary's own error must
        // propagate, not be masked or replaced.
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));

        IReadOnlyList<string> owners = OwnersOf("k");
        cluster.Nodes[owners[0]].Dispose(); // the primary dies; the replica stays healthy
        await Task.Delay(50); // let the FIN land

        await Assert.ThrowsAsync<ConnectionLostException>(() => client.SetAsync("k", "v"));
    }

    // Audit finding D3: when the failing replica leg instead runs in the
    // background (FireAndForgetReplicas), the same InvalidOperationException
    // now escapes ReplicaWriteAsync's own try/catch (it isn't one of the
    // connection-layer types that swallows) inside a fire-and-forget
    // Task.Run — there is no caller awaiting it to observe the fault.
    // Before the fix, the ContinueWith only released the semaphore, so
    // this fault was never read: exactly an *unobserved* Task exception,
    // which the .NET runtime reports via
    // TaskScheduler.UnobservedTaskException once the faulted Task is
    // finalized. This proves the fix actually reads `completed.Exception`
    // (nothing is ever raised as unobserved) and still counts the failure
    // via Stats(), the one diagnostic channel this SDK has for it.
    [Fact]
    public async Task AnEscapedExceptionOnAFireAndForgetReplicaLegIsObservedNotLeaked()
    {
        var unobserved = new List<Exception>();
        void OnUnobserved(object? _, UnobservedTaskExceptionEventArgs args)
        {
            unobserved.Add(args.Exception);
            args.SetObserved();
        }
        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            using Cluster cluster = StartCluster(replication: 2);
            using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
            {
                Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
                FireAndForgetReplicas = true,
            });

            string replica = OwnersOf("k")[1];
            ReplaceMemberConnection(client, replica, new Connection(new ThrowingStream()));

            Assert.Equal(0, client.Stats().ReplicaWriteFailures);
            // The primary write still succeeds; the replica leg's bug is
            // backgrounded, so this must not throw.
            await client.SetAsync("k", "v");
            await WaitForAsync(
                () => client.Stats().ReplicaWriteFailures > 0, "the escaped replica exception to be counted");

            // Force finalization: an exception left genuinely unobserved
            // (the pre-fix behavior) only gets reported by the runtime
            // once the faulted Task is collected.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }
        Assert.Empty(unobserved);
    }

    // ── 値の圧縮 (value compression) ──────────────────────────────

    private static NanocachedClient.Options CompressingOptions(int port, int threshold = 256) =>
        new()
        {
            Addresses = { ("127.0.0.1", port) },
            Compress = true,
            CompressionThreshold = threshold,
        };

    [Fact]
    public async Task RejectsANegativeCompressionThreshold()
    {
        // A negative CompressionThreshold used to be accepted silently,
        // always compressing (Compression.CompressValue treats "shorter
        // than a negative threshold" as never true). Reject it up front,
        // like the other invalid-option checks in ConnectAsync.
        var options = CompressingOptions(Wire.UnusedPort(), threshold: -1);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => NanocachedClient.ConnectAsync(options));
    }

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
        // collide with the DEFLATE marker (0x01) — value compression's
        // documented hazard of enabling Compress against a keyspace other
        // clients still touch without it. The remaining bytes are chosen
        // to reliably fail DEFLATE decoding (raw DEFLATE has no checksum,
        // so not every garbage body does — see CompressionTests' own
        // pinned test).
        node.Store[MockNode.KeyOf(Bytes("k"))] = new byte[] { 0x01, 0xFF, 0xFF, 0xFF, 0xFF };

        using NanocachedClient reader = await NanocachedClient.ConnectAsync(CompressingOptions(node.Port));
        await Assert.ThrowsAsync<DecompressionException>(() => reader.GetBytesAsync("k"));
    }

    // ── response tags (echoed response tags) ───────────────────────────

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
        // The exact misdelivery request pipelining left open: the server (as a
        // stand-in for any off-by-one stream corruption) never answers the
        // first GET, so the second GET's response arrives at the first
        // GET's pending slot. Without the echoed response tags tag check, the first
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
        // A genuinely pre-0019 server treats ANY extra field on A as a
        // parse error and closes without replying — both the `T R` probe
        // (issue #125) and the plain `T` one behind it — so the client
        // must fall all the way back to the bare `A <len>` form and run
        // untagged, transparently, with the same results.
        using var node = new MockNode(closeOnExtendedAuth: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
        // Three dials: the `T R` attempt the server slammed shut, then the
        // `T`-only attempt it slammed shut too, then the plain fallback
        // that stuck.
        Assert.Equal(3, node.ConnectionCount);
        Assert.Equal("A 1", node.LastAuthHeader);
    }

    // ── retryable-error status R (issue #125) ─────────────────────────

    [Fact]
    public async Task ProbesWithTaggedAndRetryableFirstAndTheMockRecordsIt()
    {
        // Every connect probes with the extended `A <len> T R` form first
        // — the mock, even one with no special R handling, just accepts it
        // (unrecognized trailing tokens are the server's problem, not
        // this test's) and records the exact header the SDK sent.
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        Assert.Equal("A 1 T R", node.LastAuthHeader);
        Assert.Equal(1, node.ConnectionCount);
    }

    [Fact]
    public async Task FallsBackFromTheRetryableProbeToTaggedOnlyAgainstAServerThatPredatesR()
    {
        // A server that understands `T` but not the newer `R` token
        // (issue #125) treats the longer `A <len> T R` as a parse error
        // and closes without replying, same legacy-fallback signal as a
        // pre-0019 server closing on `T` — the client falls back exactly
        // one stage, to `A <len> T`, and the connection ends up tagged.
        using var node = new MockNode(supportTags: true, closeOnRetryableAuth: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
        // Two dials: the `T R` attempt the server slammed shut, then the
        // `T`-only fallback that stuck.
        Assert.Equal(2, node.ConnectionCount);
        Assert.Equal("A 1 T", node.LastAuthHeader);
    }

    [Fact]
    public async Task RRespondedOnceThenSuccessRetriesTransparentlyOnTheSameConnection()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        node.AnswerRetryableTimes(1);

        Assert.Equal("v", await client.GetAsync("k"));
        // Exactly one retry: the mock saw two G frames for this one call
        // (the R-answered attempt, then the one that succeeded), no new
        // connection was dialed, and the retry is counted exactly once.
        Assert.Equal(2, node.GetCount);
        Assert.Equal(1, node.ConnectionCount);
        Assert.Equal(1, client.Stats().TransientRetries);
    }

    [Fact]
    public async Task RRespondedThreeTimesRaisesRetryableExceptionButKeepsTheConnectionUsable()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("k", "v");
        node.AnswerRetryableTimes(3);

        await Assert.ThrowsAsync<RetryableException>(() => client.GetAsync("k"));
        Assert.Equal(1, node.ConnectionCount);
        Assert.Equal(3, client.Stats().TransientRetries);

        // R is never a reason to close or redial: the same connection
        // must still serve a following operation correctly.
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal(1, node.ConnectionCount);
    }

    [Fact]
    public async Task ATaggedRRepliesToTheRightInFlightRequestWhenPipelined()
    {
        // Whichever of these two concurrent GETs the mock happens to
        // answer R to first, the pairing must not be by luck: R carries
        // this connection's per-request tag exactly like every other
        // reply (Connection's shared tag-verifying read path), so it can
        // only ever retry the request it actually belongs to — the other,
        // untouched, in-flight GET must still resolve with its own
        // correct value.
        using var node = new MockNode(supportTags: true);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", node.Port));

        await client.SetAsync("a", "va");
        await client.SetAsync("b", "vb");

        node.AnswerRetryableTimes(1);
        Task<string?> first = client.GetAsync("a");
        Task<string?> second = client.GetAsync("b");

        string?[] results = await Task.WhenAll(first, second);
        Assert.Equal(new[] { "va", "vb" }, results);
        Assert.Equal(1, node.ConnectionCount);
        Assert.Equal(1, client.Stats().TransientRetries);
    }

    [Fact]
    public async Task ViaProxyRRespondedOnceThenSuccessRetriesTransparently()
    {
        // SDK proxy mode (issue #122): the R path works the same way over
        // the single proxy connection — one test is enough (per the
        // issue #125 spec), since Connection's retry loop doesn't know or
        // care whether it's talking to a node or a proxy.
        using var proxy = new MockNode();
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.SetProxies(new[] { ("proxy-1", proxy.Address) });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ViaProxyAddress("127.0.0.1", discovery.Port));

        await client.SetAsync("k", "v");
        proxy.AnswerRetryableTimes(1);

        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal(1, proxy.ConnectionCount);
        Assert.Equal(1, client.Stats().TransientRetries);
    }

    // ── TLS hostname verification (audit finding D1) ─────────────────
    //
    // Certificates are generated per-test with the BCL's own
    // CertificateRequest API (see MockServers.Tls) rather than a
    // bundled/pre-generated PEM: that keeps the certificate's
    // notBefore/notAfter always valid without a maintenance burden, and
    // needs no TLS/crypto test dependency this SDK doesn't otherwise
    // have.

    [Fact]
    public async Task TlsRejectsACertificateForADifferentHostname()
    {
        // A cert that is otherwise perfectly valid (self-signed, but
        // trusted directly via Ca) except its SAN names a host the client
        // never dialed. Before the D1 fix this was accepted outright — the
        // custom RemoteCertificateValidationCallback re-validated the
        // chain of trust but never looked at sslPolicyErrors, so it never
        // noticed SslStream had already flagged a name mismatch.
        using X509Certificate2 cert = Tls.GenerateSelfSigned("wrong-host", sanDnsName: "wrong.example.test");
        string pemPath = Tls.WritePemCertificate(cert);
        try
        {
            using MockNode node = MockNode.WithTls(cert);

            await Assert.ThrowsAsync<AuthenticationException>(() =>
                NanocachedClient.ConnectAsync(new NanocachedClient.Options
                {
                    Addresses = { ("127.0.0.1", node.Port) },
                    Tls = true,
                    Ca = pemPath,
                }));
        }
        finally
        {
            File.Delete(pemPath);
        }
    }

    [Fact]
    public async Task TlsAcceptsACertificateForTheMatchingHostname()
    {
        // The client dials "127.0.0.1"; .NET's hostname verification
        // checks a numeric host against the cert's iPAddress SAN entries
        // (RFC 2818), so the cert must carry one.
        using X509Certificate2 cert = Tls.GenerateSelfSigned("127.0.0.1", sanIpAddress: IPAddress.Loopback);
        string pemPath = Tls.WritePemCertificate(cert);
        try
        {
            using MockNode node = MockNode.WithTls(cert);

            using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
            {
                Addresses = { ("127.0.0.1", node.Port) },
                Tls = true,
                Ca = pemPath,
            });
            await client.SetAsync("k", "v");
            Assert.Equal("v", await client.GetAsync("k"));
        }
        finally
        {
            File.Delete(pemPath);
        }
    }

    // ── ヘッジ読み取り (Hedged reads, issue #64) ─────────────────────

    // Generous versus the FireAndForgetReplicas suite's 20ms — hedging's
    // own assertions straddle both a hedge interval and a mock delay in
    // the same test, so CI (ubuntu) gets more slack against scheduling
    // noise on either side.
    private const long HedgeTimingToleranceMillis = 40;

    // _hedgedReads is private; reached via reflection the same way
    // ReplaceMemberConnection above reaches _members — the only way to
    // assert, from outside, that a losing hedge leg was actually drained.
    private static ICollection<Task> HedgedReads(NanocachedClient client)
    {
        FieldInfo field = typeof(NanocachedClient)
            .GetField("_hedgedReads", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var dictionary = (System.Collections.IDictionary)field.GetValue(client)!;
        var tasks = new List<Task>();
        foreach (System.Collections.DictionaryEntry entry in dictionary) tasks.Add((Task)entry.Key);
        return tasks;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-10)]
    public async Task RejectsANonPositiveReadHedgeAfter(int millis)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            NanocachedClient.ConnectAsync(new NanocachedClient.Options
            {
                Addresses = { ("127.0.0.1", 1) },
                ReadHedgeAfter = TimeSpan.FromMilliseconds(millis),
            }));
    }

    [Fact]
    public async Task AHitFromTheReplicaWinsOverASlowPrimary()
    {
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        string primary = owners[0], replica = owners[1];

        NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadHedgeAfter = TimeSpan.FromMilliseconds(50),
        });
        try
        {
            await client.SetAsync("k", "v");
            cluster.Nodes[primary].DelayGets(400);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            string? value = await client.GetAsync("k");
            stopwatch.Stop();

            Assert.Equal("v", value);
            Assert.True(stopwatch.ElapsedMilliseconds < 400 - HedgeTimingToleranceMillis,
                $"elapsed {stopwatch.ElapsedMilliseconds}ms should be well under the primary's 400ms delay");
            Assert.True(stopwatch.ElapsedMilliseconds >= 50 - HedgeTimingToleranceMillis,
                $"elapsed {stopwatch.ElapsedMilliseconds}ms should be at least the 50ms hedge interval");
            Assert.Equal(1, cluster.Nodes[replica].GetCount);
        }
        finally
        {
            client.Close();
        }
        // The slow primary's leg was left to finish, not cancelled, and
        // Close() drained it (waiting out its own 400ms delay).
        Assert.Empty(HedgedReads(client));
        Assert.Equal(1, cluster.Nodes[primary].GetCount);
    }

    [Fact]
    public async Task HedgeLegRacingCloseIsRefusedNotRegistered()
    {
        // Issue #91: a read that passed its own _closed check can reach
        // hedge-leg registration only after Close() set _closed and drained
        // _hedgedReads. StartLeg must recheck _closed under _hedgedReadsLock
        // so it never registers — and dials against a connection Teardown()
        // is closing — a leg the drain already passed. Setting _closed
        // directly (reflection) reproduces exactly that state; ReadHedgedAsync
        // is private, so the whole path is driven reflectively.
        using Cluster cluster = StartCluster(replication: 2);
        NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadHedgeAfter = TimeSpan.FromMilliseconds(50),
        });
        try
        {
            await client.SetAsync("k", "v");

            FieldInfo closedField = typeof(NanocachedClient)
                .GetField("_closed", BindingFlags.NonPublic | BindingFlags.Instance)!;
            MethodInfo readHedged = typeof(NanocachedClient)
                .GetMethod("ReadHedgedAsync", BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(typeof(string));

            Func<Connection, Task<string>> op = _ =>
                throw new InvalidOperationException("the leg must never be dialed after Close() began");

            closedField.SetValue(client, true);
            try
            {
                var task = (Task<string>)readHedged.Invoke(
                    client,
                    new object[] { op, new List<string> { "a", "b" }, TimeSpan.FromMilliseconds(50) })!;
                await Assert.ThrowsAsync<AlreadyClosedException>(async () => await task);
                Assert.Empty(HedgedReads(client));
            }
            finally
            {
                // Restore so Close() runs its real teardown.
                closedField.SetValue(client, false);
            }
        }
        finally
        {
            client.Close();
        }
    }

    [Fact]
    public async Task AFastPrimaryIsNeverHedged()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadHedgeAfter = TimeSpan.FromMilliseconds(50),
        });
        await client.SetAsync("k", "v");
        string replica = OwnersOf("k")[1];

        for (int i = 0; i < 5; i++)
        {
            Assert.Equal("v", await client.GetAsync("k"));
        }
        Assert.Equal(0, cluster.Nodes[replica].GetCount);
    }

    [Fact]
    public async Task AReplicaMissWaitsForThePrimary()
    {
        // Hedging must never turn a hit into a miss: the replica lacks the
        // copy and answers first, but the primary's answer is what counts.
        using Cluster cluster = StartCluster(replication: 2);
        IReadOnlyList<string> owners = OwnersOf("k");
        string primary = owners[0], replica = owners[1];

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadHedgeAfter = TimeSpan.FromMilliseconds(50),
        });
        await client.SetAsync("k", "v");
        cluster.Nodes[replica].Store.TryRemove(MockNode.KeyOf(Bytes("k")), out _);
        cluster.Nodes[primary].DelayGets(200);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        string? value = await client.GetAsync("k");
        stopwatch.Stop();

        Assert.Equal("v", value);
        Assert.True(stopwatch.ElapsedMilliseconds >= 200 - HedgeTimingToleranceMillis,
            $"elapsed {stopwatch.ElapsedMilliseconds}ms should have waited out the primary's 200ms delay");
        Assert.Equal(1, cluster.Nodes[replica].GetCount);

        // A key nobody has: the miss is accepted once the primary has
        // answered it too.
        Assert.Null(await client.GetAsync("absent"));
    }

    [Fact]
    public async Task DisposeRacingAFinishingHedgeLegDoesNotThrow()
    {
        // v0.3.0 regression: Close()'s drain checked _hedgedReads for
        // emptiness and then looked up First(); a losing leg's completion
        // callback removing itself in between made First() throw out of
        // Dispose(). Exercised by disposing right as the slow leg lands.
        for (int i = 0; i < 20; i++)
        {
            using Cluster cluster = StartCluster(replication: 2);
            IReadOnlyList<string> owners = OwnersOf("k");
            string primary = owners[0];
            NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
            {
                Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
                ReadHedgeAfter = TimeSpan.FromMilliseconds(5),
            });
            await client.SetAsync("k", "v");
            cluster.Nodes[primary].DelayGets(20);
            Assert.Equal("v", await client.GetAsync("k"));
            await Task.Delay(i % 5 * 5);
            client.Dispose();
        }
    }

    [Fact]
    public async Task OffByDefaultASlowPrimaryBoundsTheRead()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client =
            await NanocachedClient.ConnectAsync(SingleAddress("127.0.0.1", cluster.Discovery.Port));
        IReadOnlyList<string> owners = OwnersOf("k");
        string primary = owners[0], replica = owners[1];

        await client.SetAsync("k", "v");
        cluster.Nodes[primary].DelayGets(200);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        string? value = await client.GetAsync("k");
        stopwatch.Stop();

        Assert.Equal("v", value);
        Assert.True(stopwatch.ElapsedMilliseconds >= 200 - HedgeTimingToleranceMillis,
            $"elapsed {stopwatch.ElapsedMilliseconds}ms should have waited out the primary's 200ms delay");
        Assert.Equal(0, cluster.Nodes[replica].GetCount);
    }

    [Fact]
    public async Task ADeadPrimaryFailsOverImmediately()
    {
        using Cluster cluster = StartCluster(replication: 2);
        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReadHedgeAfter = TimeSpan.FromMilliseconds(500),
        });
        await client.SetAsync("k", "v");
        string primary = OwnersOf("k")[0];
        cluster.Nodes[primary].Dispose();
        await Task.Delay(50); // let the FIN land

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        string? value = await client.GetAsync("k");
        stopwatch.Stop();

        Assert.Equal("v", value);
        Assert.True(stopwatch.ElapsedMilliseconds < 500 - HedgeTimingToleranceMillis,
            $"elapsed {stopwatch.ElapsedMilliseconds}ms should be nowhere near the 500ms hedge interval");
    }

    // ── SDK proxy mode (issue #122) ──────────────────────────────

    private static NanocachedClient.Options ViaProxyAddress(string host, int port) =>
        new() { Addresses = { (host, port) }, ViaProxy = true };

    // _singleAddress is private; reached via reflection the same way
    // GetMemberConnection/GetMemberAddress (TolerantBootstrapTests, below)
    // reach _members' internals — the only way to assert, from outside,
    // which proxy a client landed on.
    private static string GetSingleAddress(NanocachedClient client)
    {
        FieldInfo field = typeof(NanocachedClient)
            .GetField("_singleAddress", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (string)field.GetValue(client)!;
    }

    [Fact]
    public async Task ViaProxyRoutesEveryOperationThroughTheChosenProxyAndNeverDialsANode()
    {
        // discovery lists node in its ordinary L roster (a real discovery
        // would never mix the two, but this proves ViaProxy never even
        // tries L/node — see the ConnectionCount assertion below) and
        // proxy in its Q roster.
        using var node = new MockNode();
        using var proxy = new MockNode();
        using var discovery = new MockDiscovery(new[] { (Names[0], node.Address) });
        discovery.SetProxies(new[] { ("proxy-1", proxy.Address) });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ViaProxyAddress("127.0.0.1", discovery.Port));

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.True(await client.DeleteAsync("k"));
        Assert.Null(await client.GetAsync("k"));

        NanocachedNamespace users = client.Namespace("users");
        await users.SetAsync("42", "alice");
        Assert.Equal("alice", await users.GetAsync("42"));
        await users.ClearAsync();
        Assert.Null(await users.GetAsync("42"));

        // A proxy owns every key: no ring, single connection.
        Assert.Equal(1, client.Replication);
        Assert.Equal(1, proxy.ConnectionCount);
        Assert.Equal(0, node.ConnectionCount);
    }

    [Fact]
    public async Task ViaProxySpreadsFreshClientsAcrossBothProxies()
    {
        using var proxyA = new MockNode();
        using var proxyB = new MockNode();
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.SetProxies(new[] { ("proxy-a", proxyA.Address), ("proxy-b", proxyB.Address) });

        var chosen = new HashSet<string>();
        for (int i = 0; i < 30; i++)
        {
            using NanocachedClient client = await NanocachedClient.ConnectAsync(
                ViaProxyAddress("127.0.0.1", discovery.Port));
            chosen.Add(GetSingleAddress(client));
        }

        // Statistical, not flaky in practice: 30 independent 50/50 picks
        // missing either proxy entirely has probability 2 * 0.5^30.
        Assert.Contains(proxyA.Address, chosen);
        Assert.Contains(proxyB.Address, chosen);
    }

    [Fact]
    public async Task ViaProxyFailsOverToTheLiveProxyWhenTheRandomlyChosenOneIsDown()
    {
        using var live = new MockNode();
        int dead = Wire.UnusedPort();
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.SetProxies(new[] { ("proxy-dead", $"127.0.0.1:{dead}"), ("proxy-live", live.Address) });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ViaProxyAddress("127.0.0.1", discovery.Port));

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal(live.Address, GetSingleAddress(client));
    }

    [Fact]
    public async Task ViaProxySkipsAWarmingUpDiscoverySeedAndFetchesQFromTheNext()
    {
        using var proxy = new MockNode();
        using var warming = new MockDiscovery(Array.Empty<(string, string)>());
        using var healthy = new MockDiscovery(Array.Empty<(string, string)>());
        warming.WarmingUp = true;
        healthy.SetProxies(new[] { ("proxy-1", proxy.Address) });

        var options = new NanocachedClient.Options { ViaProxy = true };
        options.Addresses.Add(("127.0.0.1", warming.Port));
        options.Addresses.Add(("127.0.0.1", healthy.Port));

        using NanocachedClient client = await NanocachedClient.ConnectAsync(options);
        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
    }

    [Fact]
    public async Task ViaProxyThrowsAClearErrorWhenTheProxyRosterIsEmpty()
    {
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());

        NanocachedException error = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(ViaProxyAddress("127.0.0.1", discovery.Port)));
        Assert.Contains("no proxies registered", error.Message);
    }

    [Fact]
    public async Task ViaProxyPointedAtANodeAddressFailsFast()
    {
        using var node = new MockNode();

        NanocachedException error = await Assert.ThrowsAsync<NanocachedException>(
            () => NanocachedClient.ConnectAsync(ViaProxyAddress("127.0.0.1", node.Port)));
        Assert.Contains("ViaProxy requires discovery addresses", error.Message);
    }

    [Fact]
    public async Task ViaProxyReconnectsThroughAFreshQFetchWhenTheConnectedProxyDies()
    {
        var proxyA = new MockNode();
        var proxyB = new MockNode();
        var proxies = new Dictionary<string, MockNode> { ["proxy-a"] = proxyA, ["proxy-b"] = proxyB };
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.SetProxies(new[] { ("proxy-a", proxyA.Address), ("proxy-b", proxyB.Address) });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(
            ViaProxyAddress("127.0.0.1", discovery.Port));
        await client.SetAsync("k", "v");

        string connectedAddress = GetSingleAddress(client);
        MockNode connected = proxies.Values.Single(p => p.Address == connectedAddress);
        MockNode survivor = proxies.Values.Single(p => p.Address != connectedAddress);
        try
        {
            connected.Dispose();
            await Task.Delay(50); // let the FIN land, as TransparentlyReconnectsAfterAServerFin does

            // The connected proxy's own address is retried first (it may
            // simply have restarted) and only then does the client
            // re-fetch Q and fail over — see DialProxyWithFailoverAsync.
            await client.SetAsync("k2", "v2");
            Assert.Equal("v2", await client.GetAsync("k2"));
            Assert.Equal(survivor.Address, GetSingleAddress(client));
            Assert.True(survivor.Store.ContainsKey(MockNode.KeyOf(Bytes("k2"))));
        }
        finally
        {
            survivor.Dispose();
        }
    }

    [Fact]
    public async Task ViaProxyIgnoresReadHedgeAfterSinceThereIsNoReplicaToHedgeTo()
    {
        using var proxy = new MockNode();
        using var discovery = new MockDiscovery(Array.Empty<(string, string)>());
        discovery.SetProxies(new[] { ("proxy-1", proxy.Address) });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", discovery.Port) },
            ViaProxy = true,
            ReadHedgeAfter = TimeSpan.FromMilliseconds(50),
        });

        await client.SetAsync("k", "v");
        Assert.Equal("v", await client.GetAsync("k"));
        Assert.Equal("v", await client.GetAsync("k"));

        // No ring in proxy mode, so ReadAsync never reaches
        // ReadHedgedAsync at all: exactly one G per GetAsync call, on the
        // one connection, whatever ReadHedgeAfter says.
        Assert.Equal(2, proxy.GetCount); // the two GetAsync calls above
    }
}

/// <summary>
/// Issue #67: <see cref="NanocachedClient.ConnectAsync(NanocachedClient.Options)"/>
/// must tolerate a node that discovery still lists but that can't be
/// reached (dead, not yet evicted) the same way steady-state requests
/// already do — installing it as a member with no connection instead of
/// failing the whole connect — and fail only when no listed node is
/// reachable.
/// </summary>
public class TolerantBootstrapTests
{
    private static readonly string[] Names =
    {
        "6c9f2a7e-4b1d-4f3a-9e5c-1a2b3c4d5e6f",
        "2e4d6f81-9a3b-4c5d-8e7f-0a1b2c3d4e5f",
    };

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static IReadOnlyList<string> OwnersOf(string key) =>
        new HashRing(Names).Owners(Bytes(key), 2);

    private static string KeyWithPrimary(string name)
    {
        for (int i = 0; i < 1000; i++)
        {
            string key = $"key-{i}";
            if (OwnersOf(key)[0] == name) return key;
        }
        throw new InvalidOperationException($"no key routes to {name}");
    }

    private sealed record Cluster(
        IReadOnlyDictionary<string, MockNode> Nodes, MockDiscovery Discovery) : IDisposable
    {
        public void Dispose()
        {
            Discovery.Dispose();
            foreach (MockNode node in Nodes.Values) node.Dispose();
        }
    }

    /// <summary>Starts a 2-node cluster (replication 2); every name in
    /// <paramref name="dead"/> is instead listed by discovery at an
    /// address nobody listens on.</summary>
    private static Cluster StartCluster(ISet<string> dead)
    {
        var nodes = new Dictionary<string, MockNode>();
        var entries = new List<(string Name, string Address)>();
        foreach (string name in Names)
        {
            if (dead.Contains(name))
            {
                entries.Add((name, $"127.0.0.1:{Wire.UnusedPort()}"));
            }
            else
            {
                var node = new MockNode();
                nodes[name] = node;
                entries.Add((name, node.Address));
            }
        }
        var discovery = new MockDiscovery(entries, replication: 2);
        return new Cluster(nodes, discovery);
    }

    // Member and its Connection/Address properties are private/internal —
    // reached via reflection, the same way ReplaceMemberConnection (above,
    // in NanocachedClientTests) does for its own regression test.
    private static object GetMember(NanocachedClient client, string name)
    {
        FieldInfo membersField = typeof(NanocachedClient)
            .GetField("_members", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var members = (System.Collections.IDictionary)membersField.GetValue(client)!;
        return members[name] ?? throw new InvalidOperationException($"no member named {name}");
    }

    private static Connection? GetMemberConnection(NanocachedClient client, string name)
    {
        object member = GetMember(client, name);
        PropertyInfo property = member.GetType()
            .GetProperty("Connection", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Connection?)property.GetValue(member);
    }

    private static string GetMemberAddress(NanocachedClient client, string name)
    {
        object member = GetMember(client, name);
        PropertyInfo property = member.GetType()
            .GetProperty("Address", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (string)property.GetValue(member)!;
    }

    private static bool HasMember(NanocachedClient client, string name)
    {
        FieldInfo membersField = typeof(NanocachedClient)
            .GetField("_members", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var members = (System.Collections.IDictionary)membersField.GetValue(client)!;
        return members.Contains(name);
    }

    private static System.Collections.IDictionary GetCooldowns(NanocachedClient client)
    {
        FieldInfo field = typeof(NanocachedClient)
            .GetField("_reconnectCooldowns", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (System.Collections.IDictionary)field.GetValue(client)!;
    }

    private static Task ForceRefreshAsync(NanocachedClient client)
    {
        MethodInfo method = typeof(NanocachedClient)
            .GetMethod("RefreshNodeListAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)method.Invoke(client, null)!;
    }

    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"timed out waiting for {what}");
            await Task.Delay(5);
        }
    }

    [Fact]
    public async Task ConnectSucceedsWithOneUnreachableNode()
    {
        string dead = Names[0], live = Names[1];
        using Cluster cluster = StartCluster(new HashSet<string> { dead });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReconnectCooldown = TimeSpan.FromSeconds(3),
        });

        Assert.Equal(2, client.Replication);
        Assert.Null(GetMemberConnection(client, dead));
        Assert.NotNull(GetMemberConnection(client, live));

        // A key whose primary is alive: the write lands, the dead
        // replica leg is swallowed and counted, the read hits.
        string key = KeyWithPrimary(live);
        await client.SetAsync(key, "v");
        Assert.Equal("v", await client.GetAsync(key));
        Assert.Equal(1, client.Stats().ReplicaWriteFailures);

        // A key whose primary is the dead node: the read fails over to
        // the live replica right away (cooldown still armed — no dial),
        // well under the 5s connect/dial timeout.
        string other = KeyWithPrimary(dead);
        cluster.Nodes[live].Store[MockNode.KeyOf(Bytes(other))] = Bytes("replica copy");
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        Assert.Equal("replica copy", await client.GetAsync(other));
        stopwatch.Stop();
        Assert.True(stopwatch.ElapsedMilliseconds < 2000,
            $"expected a fast failover, took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task ConnectThrowsAConnectionErrorWhenEveryListedNodeIsUnreachable()
    {
        using Cluster cluster = StartCluster(new HashSet<string>(Names));

        await Assert.ThrowsAsync<ConnectionLostException>(
            () => NanocachedClient.ConnectAsync(new NanocachedClient.Options
            {
                Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            }));
    }

    [Fact]
    public async Task AnUnreachableNodeIsRedialedOnceTheCooldownHasPassed()
    {
        string dead = Names[0], live = Names[1];
        using Cluster cluster = StartCluster(new HashSet<string> { dead });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReconnectCooldown = TimeSpan.FromMilliseconds(50),
        });

        // Bring the "dead" node up on the exact address discovery listed.
        string deadAddress = GetMemberAddress(client, dead);
        int port = int.Parse(deadAddress.Split(':')[1]);
        using var revived = new MockNode(port: port);
        await Task.Delay(150); // past the 50ms cooldown

        string key = KeyWithPrimary(dead);
        await client.SetAsync(key, "v");

        await WaitForAsync(
            () => revived.Store.ContainsKey(MockNode.KeyOf(Bytes(key))),
            "the revived node to receive the write");
        Assert.NotNull(GetMemberConnection(client, dead));
    }

    [Fact]
    public async Task RefreshPurgesCooldownsForDepartedAddresses()
    {
        // #96: a node that leaves the cluster must not leave its per-address
        // reconnect-cooldown entry behind — in a churny deployment (a fresh
        // IP:port per restart) those would accumulate unboundedly.
        string dead = Names[0], live = Names[1];
        using Cluster cluster = StartCluster(new HashSet<string> { dead });

        using NanocachedClient client = await NanocachedClient.ConnectAsync(new NanocachedClient.Options
        {
            Addresses = { ("127.0.0.1", cluster.Discovery.Port) },
            ReconnectCooldown = TimeSpan.FromMinutes(1),
        });

        string deadAddress = GetMemberAddress(client, dead);
        System.Collections.IDictionary cooldowns = GetCooldowns(client);
        // The unreachable node armed its cooldown at bootstrap.
        Assert.True(cooldowns.Contains(deadAddress), "no cooldown armed for the unreachable node");

        // Discovery drops the dead node from the roster; the refresh
        // reconciles membership and must purge its cooldown alongside it.
        cluster.Discovery.SetNodes(new[] { (live, cluster.Nodes[live].Address) });
        await ForceRefreshAsync(client);

        Assert.False(HasMember(client, dead), "departed node still present in members");
        Assert.False(cooldowns.Contains(deadAddress), "cooldown for departed address was not purged");
    }
}
