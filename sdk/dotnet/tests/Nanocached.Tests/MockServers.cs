using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Nanocached.Tests;

/// <summary>
/// In-process stand-ins for nanocached-node and nanocached-discovery,
/// speaking just enough of the wire protocol for client tests to run over
/// real TCP without the Rust binaries. Mirrors the other SDKs' mocks.
/// </summary>
public sealed class MockNode : IDisposable
{
    public ConcurrentDictionary<string, byte[]> Store { get; } = new();
    public int ConnectionCount => _connectionCount;
    public int GetCount => _getCount;

    /// <summary>The TTL (whole seconds; 0 if omitted on the wire) from
    /// the most recent S request this server received.</summary>
    public long LastSetTtl => _lastSetTtl;

    /// <summary>issue #105 — first-class namespaces: how many lowercase
    /// g/s/d (namespaced) requests this server has received — lets a test
    /// prove that <c>Namespace("")</c> sends the legacy G/S/D frames,
    /// byte-for-byte, rather than the namespaced form with an empty
    /// namespace length.</summary>
    public int NamespacedRequestCount => _namespacedRequestCount;

    /// <summary>issue #106: how many <c>c</c> (clear-one-namespace)
    /// requests this server has received — lets a fan-out test assert
    /// every node in a cluster was actually reached.</summary>
    public int ClearRequestCount => _clearRequestCount;

    /// <summary>issue #106: how many <c>F</c> (flush-everything)
    /// requests this server has received.</summary>
    public int ClearAllRequestCount => _clearAllRequestCount;

    /// <summary>issue #129: how many <c>i</c> (incr) requests this server
    /// has received — the critical assertion for cluster replication
    /// tests: a replica must see this stay 0 while a Set frame carries the
    /// primary's literal result, never replaying the increment itself.</summary>
    public int IncrRequestCount => _incrRequestCount;

    /// <summary>issue #129: the exact header line (everything up to, not
    /// including, the trailing '\n') of the most recent <c>i</c> request
    /// this server received — lets a test assert the wire frame's exact
    /// shape (namespace length, key length, delta, and — on a tagged
    /// connection — the tag), the same role <see cref="LastAuthHeader"/>
    /// plays for <c>A</c>.</summary>
    public string? LastIncrHeader => _lastIncrHeader;
    private volatile string? _lastIncrHeader;

    /// <summary>issue #141 — compare-and-set: how many <c>k</c> requests
    /// this server has received — the critical assertion for cluster
    /// replication tests, mirroring <see cref="IncrRequestCount"/>: a
    /// replica must see this stay 0 while a Set frame carries the
    /// primary's literal result, never replaying the CAS itself.</summary>
    public int CasRequestCount => _casRequestCount;

    /// <summary>issue #141: how many <c>x</c> (CAS delete) requests this
    /// server has received — the <see cref="CasRequestCount"/> counterpart
    /// for the digest-conditioned remove.</summary>
    public int CasDeleteRequestCount => _casDeleteRequestCount;

    /// <summary>issue #141: the exact header line (everything up to, not
    /// including, the trailing '\n') of the most recent <c>k</c> request
    /// this server received.</summary>
    public string? LastCasHeader => _lastCasHeader;
    private volatile string? _lastCasHeader;

    /// <summary>issue #141: the exact header line of the most recent
    /// <c>x</c> request this server received.</summary>
    public string? LastCasDeleteHeader => _lastCasDeleteHeader;
    private volatile string? _lastCasDeleteHeader;

    /// <summary>issue #151: how many <c>m</c> (multi-get) requests this
    /// server has received.</summary>
    public int MultiGetRequestCount => _multiGetRequestCount;
    private int _multiGetRequestCount;

    /// <summary>issue #151: how many <c>o</c> (multi-set) requests this
    /// server has received.</summary>
    public int MultiSetRequestCount => _multiSetRequestCount;
    private int _multiSetRequestCount;

    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<TcpClient, bool> _clients = new();
    private readonly byte[]? _requiredSecret;
    /// <summary>Speak echoed response tags: acknowledge `A ... T` with `OnT\n` and echo
    /// tags on that connection's replies. Off by default so the bulk of
    /// the suite keeps exercising the legacy untagged path.</summary>
    private readonly bool _supportTags;
    /// <summary>Behave like a legacy pre-tag server: an extended `A ... T`
    /// (or `A ... T R`) is a parse error — close the connection without
    /// replying.</summary>
    private readonly bool _closeOnExtendedAuth;
    /// <summary>issue #125: behave like a server that understands `T` but
    /// predates the `R` capability token — accepts a plain `A ... T`
    /// exactly like <see cref="_supportTags"/> would, but treats the
    /// longer `A ... T R` as a parse error and closes without replying.
    /// Exercises the SDK's middle fallback stage (extended-with-R ->
    /// extended-tags-only). Requires <see cref="_supportTags"/> to also be
    /// set, or there is no `T`-only form for the SDK to fall back
    /// to.</summary>
    private readonly bool _closeOnRetryableAuth;
    /// <summary>issue #125: the exact `A` header line (everything up to,
    /// not including, the trailing '\n' — the secret bytes that follow are
    /// never part of it) this mock most recently received, letting a test
    /// assert the probe form a connect actually sent (e.g. `"A 1 T R"`).
    /// Volatile: written on the mock's own accept-loop thread, read from
    /// the test thread after the SDK call that triggered it has already
    /// completed.</summary>
    public string? LastAuthHeader => _lastAuthHeader;
    private volatile string? _lastAuthHeader;
    private int _connectionCount;
    private int _getCount;
    private int _wrongNodeReplies;
    private int _wrongTagReplies;
    private int _swallowedGets;
    private int _malformedValueReplies;
    private int _storedToGetReplies;
    private int _wrongNodeOnSetReplies;
    private int _extraByteOnSetReplies;
    private volatile int _setDelayMillis;
    private volatile int _getDelayMillis;
    private volatile int _authDelayMillis;
    private volatile bool _silent;
    private long _lastSetTtl;
    private int _namespacedRequestCount;
    private int _clearRequestCount;
    private int _clearAllRequestCount;
    private int _failClearReplies;
    private int _incrRequestCount;
    private int _casRequestCount;
    private int _casDeleteRequestCount;
    /// <summary>issue #129: the TTL (whole seconds; 0 if none) each stored
    /// entry currently carries, keyed the same way <see cref="Store"/> is
    /// — recorded on every S/s and consulted (never modified — INCR never
    /// changes a key's TTL) by <c>i</c> so its <c>I</c> reply can report
    /// the entry's remaining TTL, the same way a real node would.</summary>
    private readonly ConcurrentDictionary<string, long> _ttls = new();
    /// <summary>issue #125: how many of the NEXT data requests (any of
    /// G/S/D/g/s/d/c/F) get answered `R` (tagged correctly for the
    /// connection they arrived on) instead of their normal reply — set via
    /// <see cref="AnswerRetryableTimes"/>.</summary>
    private int _retryableReplies;
    /// <summary>J1/D1: when set, every accepted connection is wrapped in
    /// an <see cref="SslStream"/> presenting this certificate before the
    /// A/G/S/D protocol loop runs — everything past the handshake is
    /// identical to a plain MockNode, since <see cref="ServeAsync"/> only
    /// ever talks to a generic <see cref="Stream"/>.</summary>
    private readonly X509Certificate2? _serverCertificate;

    public MockNode(
        string? requiredSecret = null,
        bool supportTags = false,
        bool closeOnExtendedAuth = false,
        bool closeOnRetryableAuth = false,
        X509Certificate2? serverCertificate = null,
        int port = 0)
    {
        _requiredSecret = requiredSecret is null ? null : Encoding.UTF8.GetBytes(requiredSecret);
        _supportTags = supportTags;
        _closeOnExtendedAuth = closeOnExtendedAuth;
        _closeOnRetryableAuth = closeOnRetryableAuth;
        _serverCertificate = serverCertificate;
        // port = 0 (the default) picks any free port, as before; a
        // caller that needs to revive a node on a specific, previously
        // advertised address (issue #67's redial-after-cooldown test)
        // passes the exact port instead.
        _listener = new TcpListener(IPAddress.Loopback, port);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    /// <summary>D1: a node that speaks TLS, presenting <paramref
    /// name="serverCertificate"/>.</summary>
    public static MockNode WithTls(X509Certificate2 serverCertificate) =>
        new(serverCertificate: serverCertificate);

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public string Address => $"127.0.0.1:{Port}";

    public void AnswerWrongNodeOnce() => Interlocked.Increment(ref _wrongNodeReplies);

    /// <summary>Queue a one-off reply for the next G request on a tagged
    /// connection that echoes the WRONG tag (the request's tag + 1) — the
    /// desync a pre-tag stream misalignment would produce.</summary>
    public void AnswerWrongTagOnce() => Interlocked.Increment(ref _wrongTagReplies);

    /// <summary>Swallow the next G request entirely (no reply) — the
    /// off-by-one stream desync where every later response answers the
    /// previous request.</summary>
    public void SwallowGetOnce() => Interlocked.Increment(ref _swallowedGets);

    /// <summary>Queue a one-off garbage V header for the next G request.</summary>
    public void AnswerMalformedValueOnce() => Interlocked.Increment(ref _malformedValueReplies);

    /// <summary>Reply <c>S</c> to the next G — a well-formed frame of the
    /// wrong kind, as a desynced (off-by-one) stream would produce.</summary>
    public void AnswerStoredToGetOnce() => Interlocked.Increment(ref _storedToGetReplies);

    /// <summary>Reply <c>W</c> to the next S specifically (not G/D) — for
    /// tests that need a node to keep answering GET normally while a
    /// later SET against it (e.g. a read-repair write) fails.</summary>
    public void AnswerWrongNodeOnSetOnce() => Interlocked.Increment(ref _wrongNodeOnSetReplies);

    /// <summary>Queue a one-off malformed reply to the next S on an
    /// untagged connection: <c>S1\n</c> instead of the well-formed
    /// two-byte <c>S\n</c> — an extra byte between the marker and the
    /// trailing '\n', as if the server had (bogusly) tagged this reply on
    /// a connection that never asked for tags. Regression coverage for
    /// the untagged fast path's trailing-byte validation
    /// (Connection.cs's ExpectLfAsync).</summary>
    public void AnswerExtraByteOnSetOnce() => Interlocked.Increment(ref _extraByteOnSetReplies);

    /// <summary>Holds every future S reply for <paramref name="millis"/>
    /// first — for tests proving a caller isn't blocked on a slow replica
    /// leg (fire-and-forget replica writes).</summary>
    public void DelaySets(int millis) => _setDelayMillis = millis;

    /// <summary>issue #125: the next <paramref name="count"/> data requests
    /// this node receives (any of G/S/D/g/s/d/c/F, in arrival order, across
    /// however many connections) are answered <c>R</c> — tagged correctly
    /// when the connection they arrive on is tagged — instead of their
    /// normal reply.</summary>
    public void AnswerRetryableTimes(int count) => Interlocked.Add(ref _retryableReplies, count);

    /// <summary>issue #106: makes the next <c>c</c>/<c>F</c> request this
    /// node receives fail at the connection level — the request is read
    /// (so the TCP stream stays well-formed) but the connection is then
    /// closed instead of answered <c>C</c>, giving the client an
    /// immediate <see cref="ConnectionLostException"/> rather than a
    /// 30-second request-timeout wait. This is what a client's
    /// fan-out-and-retry (<see cref="NanocachedClient.ClearAllAsync"/>'s
    /// doc comment) needs to see a node fail.</summary>
    public void FailClearOnce() => Interlocked.Increment(ref _failClearReplies);

    /// <summary>Holds every future G reply for <paramref name="millis"/>
    /// first — for hedged-reads tests proving a caller isn't bounded by a
    /// slow owner (Hedged reads).</summary>
    public void DelayGets(int millis) => _getDelayMillis = millis;

    /// <summary>Holds every future accepted A (auth/identify) reply for
    /// <paramref name="millis"/> first — issue #227: for tests proving a
    /// client dialing several newly discovered nodes on a refresh does so
    /// concurrently (Task.WhenAll) rather than one dial's connect time
    /// per node.</summary>
    public void DelayAuth(int millis) => _authDelayMillis = millis;

    /// <summary>Makes this node a half-open server from this point on: it
    /// still accepts and completes the A handshake, and still reads every
    /// request frame off the wire (so the TCP stream stays well-formed),
    /// but never writes a reply — regression coverage for the request
    /// timeout (issue #42), mirroring the Go suite's hook of the same
    /// name.</summary>
    public void GoSilentAfterHandshake() => _silent = true;

    /// <summary>Server-side FIN on every open connection, like the idle timeout.</summary>
    public void DropConnections()
    {
        foreach (TcpClient client in _clients.Keys) client.Close();
    }

    public void Dispose()
    {
        DropConnections();
        _listener.Stop();
    }

    internal static string KeyOf(byte[] key) => Convert.ToBase64String(key);

    /// <summary>issue #141 — compare-and-set: this mock's own
    /// implementation of the digest algorithm docs/protocol.html#cas
    /// specifies — SHA-256 truncated to the first 16 bytes, lowercase hex
    /// — so it can evaluate a 32-character-hex <c>&lt;cond&gt;</c> exactly
    /// as a real node would. Deliberately a second, independent
    /// implementation from <see cref="NanocachedClient.ContentDigest"/>
    /// (not shared code) — the cluster CAS tests below only prove anything
    /// if the mock and the SDK could disagree.</summary>
    internal static string ComputeDigest(byte[] value)
    {
        byte[] hash = SHA256.HashData(value);
        return Convert.ToHexString(hash, 0, 16).ToLowerInvariant();
    }

    /// <summary>issue #105 — first-class namespaces: the store key for a
    /// namespaced entry, distinct from every unnamespaced <see cref="KeyOf(byte[])"/>
    /// key (the "ns:" prefix can never collide with a bare base64 key,
    /// which contains only base64 alphabet characters) and from every
    /// other namespace's entries — the same key name in two namespaces
    /// (plus the default namespace) is three independent store entries.
    /// The empty namespace maps to the same store key as
    /// <see cref="KeyOf(byte[])"/>, matching the wire rule that an empty
    /// namespace addresses the default namespace.</summary>
    internal static string KeyOf(byte[] namespaceBytes, byte[] key) =>
        namespaceBytes.Length == 0
            ? KeyOf(key)
            : $"ns:{Convert.ToBase64String(namespaceBytes)}:{Convert.ToBase64String(key)}";

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (true)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync();
                Interlocked.Increment(ref _connectionCount);
                _clients[client] = true;
                _ = ServeAsync(client);
            }
        }
        catch (Exception)
        {
            // Listener stopped — normal shutdown.
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        try
        {
            Stream stream = client.GetStream();
            if (_serverCertificate is not null)
            {
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsServerAsync(_serverCertificate, clientCertificateRequired: false,
                    checkCertificateRevocation: false);
                stream = ssl;
            }
            // Echoed response tags: set when this connection's `A ... T` was
            // acknowledged — its requests then carry a trailing tag the
            // replies must echo.
            bool tagged = false;
            while (true)
            {
                string[] parts = (await Wire.ReadLineAsync(stream)).Split(' ');
                // On a tagged connection every request's last header field
                // is its tag, echoed back as each reply's own last field.
                string tag = tagged ? $" {parts[^1]}" : "";

                switch (parts[0])
                {
                    case "A":
                    {
                        // issue #125: record the exact header line (minus
                        // the secret bytes, which follow separately) so a
                        // test can assert the probe form a connect
                        // actually sent — before any close-without-reply
                        // branch below, since those are exactly the forms
                        // a test needs to see were tried.
                        _lastAuthHeader = string.Join(' ', parts);

                        if (parts.Length > 2 && _closeOnExtendedAuth)
                        {
                            client.Close();
                            return;
                        }
                        // issue #125: predates the R token specifically —
                        // `A <len> T R` (4 fields) is a parse error, but
                        // `A <len> T` (3 fields, or fewer) is fine.
                        if (parts.Length > 3 && _closeOnRetryableAuth)
                        {
                            client.Close();
                            return;
                        }

                        byte[] secret = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        bool accepted = _requiredSecret is null
                            ? secret.Length > 0
                            : secret.AsSpan().SequenceEqual(_requiredSecret);
                        tagged = accepted && _supportTags && parts.Length > 2 && parts[2] == "T";
                        if (accepted && _authDelayMillis > 0)
                        {
                            await Task.Delay(_authDelayMillis);
                        }
                        await Wire.WriteAsync(stream, accepted ? (tagged ? "OnT\n" : "On\n") : "En\n");
                        if (!accepted) return;
                        break;
                    }
                    case "G":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        Interlocked.Increment(ref _getCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (_getDelayMillis > 0)
                        {
                            await Task.Delay(_getDelayMillis);
                        }
                        if (TakeOne(ref _swallowedGets))
                        {
                            break;
                        }
                        if (tagged && TakeOne(ref _wrongTagReplies))
                        {
                            await Wire.WriteAsync(stream, $"N {int.Parse(parts[^1]) + 1}\n");
                            break;
                        }
                        if (TakeMalformedValue())
                        {
                            await Wire.WriteAsync(stream, "V x\n");
                            break;
                        }
                        if (TakeOne(ref _storedToGetReplies))
                        {
                            await Wire.WriteAsync(stream, $"S{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else if (Store.TryGetValue(KeyOf(key), out byte[]? value))
                        {
                            await Wire.WriteAsync(stream, $"V {value.Length}{tag}\n");
                            await stream.WriteAsync(value);
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, $"N{tag}\n");
                        }
                        break;
                    }
                    case "S":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] value = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        // The TTL, when present, is the field after the
                        // two lengths (omitted on the wire means "no
                        // expiry", i.e. 0); on a tagged connection the tag
                        // sits after it as the last field.
                        int ttlFieldCount = parts.Length - (tagged ? 4 : 3);
                        _lastSetTtl = ttlFieldCount > 0 ? long.Parse(parts[3]) : 0;
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (_setDelayMillis > 0)
                        {
                            await Task.Delay(_setDelayMillis);
                        }
                        if (!tagged && TakeOne(ref _extraByteOnSetReplies))
                        {
                            await Wire.WriteAsync(stream, "S1\n");
                            break;
                        }
                        if (TakeOne(ref _wrongNodeOnSetReplies) || TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else
                        {
                            Store[KeyOf(key)] = value;
                            _ttls[KeyOf(key)] = _lastSetTtl;
                            await Wire.WriteAsync(stream, $"S{tag}\n");
                        }
                        break;
                    }
                    case "D":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, Store.TryRemove(KeyOf(key), out _) ? $"D{tag}\n" : $"N{tag}\n");
                        }
                        break;
                    }
                    // issue #105 — first-class namespaces: the lowercase
                    // g/s/d frames gain one leading <namespace-length>
                    // header field, and the namespace bytes lead the body
                    // ahead of the key (and, for s, the value). Otherwise
                    // identical to G/S/D above, including every injected
                    // failure mode this mock supports — a namespaced
                    // client test can reuse the same hooks.
                    case "g":
                    {
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        Interlocked.Increment(ref _getCount);
                        Interlocked.Increment(ref _namespacedRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (_getDelayMillis > 0)
                        {
                            await Task.Delay(_getDelayMillis);
                        }
                        if (TakeOne(ref _swallowedGets))
                        {
                            break;
                        }
                        if (tagged && TakeOne(ref _wrongTagReplies))
                        {
                            await Wire.WriteAsync(stream, $"N {int.Parse(parts[^1]) + 1}\n");
                            break;
                        }
                        if (TakeMalformedValue())
                        {
                            await Wire.WriteAsync(stream, "V x\n");
                            break;
                        }
                        if (TakeOne(ref _storedToGetReplies))
                        {
                            await Wire.WriteAsync(stream, $"S{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else if (Store.TryGetValue(KeyOf(namespaceBytes, key), out byte[]? value))
                        {
                            await Wire.WriteAsync(stream, $"V {value.Length}{tag}\n");
                            await stream.WriteAsync(value);
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, $"N{tag}\n");
                        }
                        break;
                    }
                    case "s":
                    {
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        byte[] value = await Wire.ReadExactlyAsync(stream, int.Parse(parts[3]));
                        Interlocked.Increment(ref _namespacedRequestCount);
                        // Field layout: "s <ns-len> <key-len> <val-len>
                        // [<ttl>] [<tag>]" — one more leading field than
                        // legacy S, so the baseline field counts (no ttl)
                        // are 4/5 instead of 3/4.
                        int ttlFieldCount = parts.Length - (tagged ? 5 : 4);
                        _lastSetTtl = ttlFieldCount > 0 ? long.Parse(parts[4]) : 0;
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (_setDelayMillis > 0)
                        {
                            await Task.Delay(_setDelayMillis);
                        }
                        if (TakeOne(ref _wrongNodeOnSetReplies) || TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else
                        {
                            Store[KeyOf(namespaceBytes, key)] = value;
                            _ttls[KeyOf(namespaceBytes, key)] = _lastSetTtl;
                            await Wire.WriteAsync(stream, $"S{tag}\n");
                        }
                        break;
                    }
                    case "i":
                    {
                        // issue #129 — always namespaced (no legacy
                        // uppercase form): `i <ns-len> <key-len> <delta>[ <tag>]\n`.
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        long delta = long.Parse(parts[3], CultureInfo.InvariantCulture);
                        _lastIncrHeader = string.Join(' ', parts);
                        Interlocked.Increment(ref _incrRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                            break;
                        }

                        string storeKey = KeyOf(namespaceBytes, key);
                        if (!Store.TryGetValue(storeKey, out byte[]? existing))
                        {
                            await Wire.WriteAsync(stream, $"N{tag}\n");
                            break;
                        }
                        if (!long.TryParse(
                                Encoding.ASCII.GetString(existing), NumberStyles.Integer,
                                CultureInfo.InvariantCulture, out long current))
                        {
                            await Wire.WriteAsync(stream, $"T{tag}\n");
                            break;
                        }

                        long updated;
                        try
                        {
                            updated = checked(current + delta);
                        }
                        catch (OverflowException)
                        {
                            await Wire.WriteAsync(stream, $"T{tag}\n");
                            break;
                        }

                        byte[] updatedBytes = Encoding.ASCII.GetBytes(
                            updated.ToString(CultureInfo.InvariantCulture));
                        Store[storeKey] = updatedBytes;
                        // TTL is unaffected by an increment — only reported.
                        long ttlSeconds = _ttls.TryGetValue(storeKey, out long ttl) ? ttl : 0;
                        string ttlField = ttlSeconds > 0 ? $" {ttlSeconds}" : "";
                        await Wire.WriteAsync(stream, $"I {updatedBytes.Length}{ttlField}{tag}\n");
                        await stream.WriteAsync(updatedBytes);
                        break;
                    }
                    case "k":
                    {
                        // issue #141 — compare-and-set: always namespaced
                        // (no legacy uppercase form): `k <ns-len> <key-len>
                        // <val-len> <cond> [<ttl-seconds>][ <tag>]\n<ns><key><value>`.
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        byte[] value = await Wire.ReadExactlyAsync(stream, int.Parse(parts[3]));
                        string cond = parts[4];
                        // Field layout mirrors "s": baseline (no ttl) is 5
                        // fields (k, ns-len, key-len, val-len, cond), 6
                        // with a tag.
                        int ttlFieldCount = parts.Length - (tagged ? 6 : 5);
                        long ttlSeconds = ttlFieldCount > 0
                            ? long.Parse(parts[5], CultureInfo.InvariantCulture)
                            : 0;
                        _lastCasHeader = string.Join(' ', parts);
                        Interlocked.Increment(ref _casRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                            break;
                        }

                        string storeKey = KeyOf(namespaceBytes, key);
                        bool matches = cond switch
                        {
                            "A" => !Store.ContainsKey(storeKey),
                            "P" => Store.ContainsKey(storeKey),
                            _ => Store.TryGetValue(storeKey, out byte[]? existing)
                                && ComputeDigest(existing) == cond,
                        };
                        if (!matches)
                        {
                            await Wire.WriteAsync(stream, $"N{tag}\n");
                            break;
                        }
                        Store[storeKey] = value;
                        _ttls[storeKey] = ttlSeconds;
                        await Wire.WriteAsync(stream, $"S{tag}\n");
                        break;
                    }
                    case "x":
                    {
                        // issue #141: `x <ns-len> <key-len> <cond>[ <tag>]\n<ns><key>`
                        // — <cond> here is always a digest.
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        string digest = parts[3];
                        _lastCasDeleteHeader = string.Join(' ', parts);
                        Interlocked.Increment(ref _casDeleteRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                            break;
                        }

                        string storeKey = KeyOf(namespaceBytes, key);
                        bool matches = Store.TryGetValue(storeKey, out byte[]? existing)
                            && ComputeDigest(existing) == digest;
                        if (!matches)
                        {
                            await Wire.WriteAsync(stream, $"N{tag}\n");
                            break;
                        }
                        Store.TryRemove(storeKey, out _);
                        await Wire.WriteAsync(stream, $"D{tag}\n");
                        break;
                    }
                    case "d":
                    {
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        Interlocked.Increment(ref _namespacedRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else
                        {
                            await Wire.WriteAsync(
                                stream,
                                Store.TryRemove(KeyOf(namespaceBytes, key), out _) ? $"D{tag}\n" : $"N{tag}\n");
                        }
                        break;
                    }
                    // issue #151 — batched get/set: always namespaced (no
                    // legacy uppercase form), like INCR/CAS. This whole
                    // received frame answers W uniformly when
                    // TakeWrongNode() is armed, since a real node never
                    // owns some-but-not-all of a frame's keys the client
                    // itself already grouped by owner.
                    case "m":
                    {
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        int n = int.Parse(parts[2], CultureInfo.InvariantCulture);
                        var keys = new byte[n][];
                        for (int i = 0; i < n; i++)
                        {
                            keys[i] = await Wire.ReadExactlyAsync(stream, int.Parse(parts[3 + i], CultureInfo.InvariantCulture));
                        }
                        Interlocked.Increment(ref _multiGetRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }

                        bool wrongNode = TakeWrongNode();
                        var header = new StringBuilder("M ").Append(n);
                        using var body = new MemoryStream();
                        for (int i = 0; i < n; i++)
                        {
                            if (wrongNode)
                            {
                                header.Append(" W");
                                continue;
                            }
                            if (Store.TryGetValue(KeyOf(namespaceBytes, keys[i]), out byte[]? value))
                            {
                                header.Append(' ').Append(value.Length);
                                body.Write(value);
                            }
                            else
                            {
                                header.Append(" -");
                            }
                        }
                        header.Append(tag).Append('\n');
                        await Wire.WriteAsync(stream, header.ToString());
                        await stream.WriteAsync(body.ToArray());
                        break;
                    }
                    case "o":
                    {
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        int n = int.Parse(parts[2], CultureInfo.InvariantCulture);
                        var keyLens = new int[n];
                        var valueLens = new int[n];
                        for (int i = 0; i < n; i++)
                        {
                            keyLens[i] = int.Parse(parts[3 + i * 2], CultureInfo.InvariantCulture);
                            valueLens[i] = int.Parse(parts[4 + i * 2], CultureInfo.InvariantCulture);
                        }
                        int fixedFieldCount = 3 + n * 2;
                        int ttlFieldCount = parts.Length - fixedFieldCount - (tagged ? 1 : 0);
                        long ttlSeconds = ttlFieldCount > 0
                            ? long.Parse(parts[fixedFieldCount], CultureInfo.InvariantCulture)
                            : 0;
                        var keys = new byte[n][];
                        var values = new byte[n][];
                        for (int i = 0; i < n; i++)
                        {
                            keys[i] = await Wire.ReadExactlyAsync(stream, keyLens[i]);
                            values[i] = await Wire.ReadExactlyAsync(stream, valueLens[i]);
                        }
                        Interlocked.Increment(ref _multiSetRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }

                        bool wrongNode = TakeWrongNode();
                        var header = new StringBuilder("O ").Append(n);
                        for (int i = 0; i < n; i++)
                        {
                            if (wrongNode)
                            {
                                header.Append(" W");
                            }
                            else
                            {
                                string storeKey = KeyOf(namespaceBytes, keys[i]);
                                Store[storeKey] = values[i];
                                _ttls[storeKey] = ttlSeconds;
                                header.Append(" S");
                            }
                        }
                        header.Append(tag).Append('\n');
                        await Wire.WriteAsync(stream, header.ToString());
                        break;
                    }
                    // issue #106: c/F never have a legacy uppercase
                    // counterpart (they postdate namespaces) and are
                    // never key-addressed, so — unlike g/s/d above —
                    // there is no wrong-node injection here; W is never a
                    // valid reply to either. FailClearOnce() is this
                    // mock's stand-in for a node that fails a clear at
                    // the connection level (closing instead of replying),
                    // for the client's fan-out-and-retry to react to.
                    case "c":
                    {
                        byte[] namespaceBytes = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        Interlocked.Increment(ref _clearRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeOne(ref _failClearReplies))
                        {
                            return;
                        }
                        // Mirrors KeyOf(byte[], byte[])'s "ns:" store-key
                        // convention: the empty namespace shares the plain
                        // (unnamespaced) keyspace, so dropping it here must
                        // spare every "ns:"-prefixed entry, and dropping a
                        // named namespace must spare everything else.
                        string? prefix = namespaceBytes.Length == 0
                            ? null
                            : $"ns:{Convert.ToBase64String(namespaceBytes)}:";
                        foreach (string storeKey in Store.Keys)
                        {
                            bool inThisNamespace = prefix is null
                                ? !storeKey.StartsWith("ns:", StringComparison.Ordinal)
                                : storeKey.StartsWith(prefix, StringComparison.Ordinal);
                            if (inThisNamespace)
                            {
                                Store.TryRemove(storeKey, out _);
                            }
                        }
                        await Wire.WriteAsync(stream, $"C{tag}\n");
                        break;
                    }
                    case "F":
                    {
                        Interlocked.Increment(ref _clearAllRequestCount);
                        if (_silent)
                        {
                            break; // half-open: frame consumed, never answered
                        }
                        // issue #125: answered before any other
                        // failure-injection check — a retryable-status test
                        // doesn't need to reason about interaction with the
                        // rest of this mock's hooks.
                        if (TakeOne(ref _retryableReplies))
                        {
                            await Wire.WriteAsync(stream, $"R{tag}\n");
                            break;
                        }
                        if (TakeOne(ref _failClearReplies))
                        {
                            return;
                        }
                        Store.Clear();
                        await Wire.WriteAsync(stream, $"C{tag}\n");
                        break;
                    }
                    default:
                        return;
                }
            }
        }
        catch (Exception)
        {
            // Connection closed — normal end of a mock session.
        }
        finally
        {
            _clients.TryRemove(client, out _);
            client.Close();
        }
    }

    private static bool TakeOne(ref int counter)
    {
        while (true)
        {
            int pending = counter;
            if (pending == 0) return false;
            if (Interlocked.CompareExchange(ref counter, pending - 1, pending) == pending)
            {
                return true;
            }
        }
    }

    private bool TakeMalformedValue()
    {
        while (true)
        {
            int pending = _malformedValueReplies;
            if (pending == 0) return false;
            if (Interlocked.CompareExchange(ref _malformedValueReplies, pending - 1, pending) == pending)
            {
                return true;
            }
        }
    }

    private bool TakeWrongNode()
    {
        while (true)
        {
            int pending = _wrongNodeReplies;
            if (pending == 0) return false;
            if (Interlocked.CompareExchange(ref _wrongNodeReplies, pending - 1, pending) == pending)
            {
                return true;
            }
        }
    }
}

public sealed class MockDiscovery : IDisposable
{
    public volatile bool WarmingUp;

    private readonly TcpListener _listener;
    private readonly int _replication;
    private volatile IReadOnlyList<(string Name, string Address)> _nodes;
    // SDK proxy mode (issue #122): the roster `Q` serves — separate from
    // _nodes (what `L` serves), since a proxy is a different kind of
    // registration on the wire (Y, not J/P) even though a mock "proxy" is
    // just a MockNode like any other.
    private volatile IReadOnlyList<(string Name, string Address)> _proxies =
        Array.Empty<(string, string)>();

    public MockDiscovery(IReadOnlyList<(string Name, string Address)> nodes, int replication = 1)
    {
        _nodes = nodes;
        _replication = replication;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public void SetNodes(IReadOnlyList<(string Name, string Address)> nodes) => _nodes = nodes;

    /// <summary>SDK proxy mode: replaces the roster `Q` serves — lets a
    /// test change which proxies discovery knows about mid-test (e.g. a
    /// reconnect-failover test dropping a dead one), mirroring
    /// <see cref="SetNodes"/>.</summary>
    public void SetProxies(IReadOnlyList<(string Name, string Address)> proxies) => _proxies = proxies;

    /// <summary>When set, an L request gets this exact text instead of
    /// the normally generated frame — for tests that need to claim
    /// things about the node list a real registry couldn't (an
    /// over-the-cap count, a malformed header, an entry whose declared
    /// length would blow the aggregate response cap) without actually
    /// having to hold that much node data in memory.</summary>
    public string? RawListResponse { get; set; }

    public void Dispose() => _listener.Stop();

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (true)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync();
                _ = ServeAsync(client);
            }
        }
        catch (Exception)
        {
            // Listener stopped — normal shutdown.
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        try
        {
            NetworkStream stream = client.GetStream();
            while (true)
            {
                string[] parts = (await Wire.ReadLineAsync(stream)).Split(' ');
                if (parts[0] == "A")
                {
                    await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                    await Wire.WriteAsync(stream, "Od\n");
                }
                else if (parts[0] == "L")
                {
                    if (WarmingUp)
                    {
                        await Wire.WriteAsync(stream, "B\n");
                        return;
                    }
                    if (RawListResponse is not null)
                    {
                        await Wire.WriteAsync(stream, RawListResponse);
                        return;
                    }
                    IReadOnlyList<(string Name, string Address)> snapshot = _nodes;
                    var frame = new StringBuilder($"N {snapshot.Count} {_replication}\n");
                    foreach (var (name, address) in snapshot)
                    {
                        frame.Append($"{name.Length} {address.Length}\n{name}{address}\n");
                    }
                    await Wire.WriteAsync(stream, frame.ToString());
                }
                else if (parts[0] == "Q")
                {
                    // SDK proxy mode (issue #122): same startup-grace `B`
                    // and entry shape as `L`, but no replication field —
                    // a proxy owns every key, so R means nothing to it.
                    if (WarmingUp)
                    {
                        await Wire.WriteAsync(stream, "B\n");
                        return;
                    }
                    IReadOnlyList<(string Name, string Address)> snapshot = _proxies;
                    var frame = new StringBuilder($"N {snapshot.Count}\n");
                    foreach (var (name, address) in snapshot)
                    {
                        frame.Append($"{name.Length} {address.Length}\n{name}{address}\n");
                    }
                    await Wire.WriteAsync(stream, frame.ToString());
                }
                else
                {
                    return;
                }
            }
        }
        catch (Exception)
        {
            // Connection closed — normal end of a mock session.
        }
        finally
        {
            client.Close();
        }
    }
}

/// <summary>
/// D1 test support: throwaway self-signed TLS certificates, generated
/// entirely with the BCL's own <see cref="CertificateRequest"/> API — no
/// external TLS/crypto test dependency this SDK doesn't otherwise need,
/// and no dependency on an external `openssl`/`keytool`-style binary
/// being on PATH. Each certificate is self-signed and used as its own
/// trust anchor, mirroring how a real deployment's private CA is passed
/// to <c>Options.Ca</c>.
/// </summary>
internal static class Tls
{
    /// <summary>Builds a self-signed certificate for <paramref
    /// name="commonName"/> whose Subject Alternative Name is exactly
    /// <paramref name="sanDnsName"/> (if set) and/or <paramref
    /// name="sanIpAddress"/> (if set) — these are exactly what .NET's
    /// hostname verification (issue D1) checks the connected-to host
    /// against.</summary>
    internal static X509Certificate2 GenerateSelfSigned(
        string commonName, string? sanDnsName = null, IPAddress? sanIpAddress = null)
    {
        using RSA rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        var sanBuilder = new SubjectAlternativeNameBuilder();
        if (sanDnsName is not null) sanBuilder.AddDnsName(sanDnsName);
        if (sanIpAddress is not null) sanBuilder.AddIpAddress(sanIpAddress);
        request.CertificateExtensions.Add(sanBuilder.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));

        X509Certificate2 cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(3650));

        // Round-tripped through a PFX export/import: a certificate fresh
        // off CreateSelfSigned carries an *ephemeral* private key that
        // SslStream's server-side AuthenticateAsServerAsync can't
        // reliably use on every platform — re-importing gives it a normal
        // (if still in-memory/exportable) key.
        return X509CertificateLoader.LoadPkcs12(
            cert.Export(X509ContentType.Pfx), password: null,
            X509KeyStorageFlags.Exportable);
    }

    /// <summary>Writes just the public certificate (no private key) as a
    /// PEM file — the same shape <c>Options.Ca</c> expects from a real
    /// private CA's root certificate.</summary>
    internal static string WritePemCertificate(X509Certificate2 certificate)
    {
        string path = Path.Combine(Path.GetTempPath(), $"nanocached-test-ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, certificate.ExportCertificatePem());
        return path;
    }
}

internal static class Wire
{
    internal static async Task<string> ReadLineAsync(Stream stream)
    {
        var line = new StringBuilder();
        var single = new byte[1];
        while (true)
        {
            await stream.ReadExactlyAsync(single);
            if (single[0] == (byte)'\n') return line.ToString();
            line.Append((char)single[0]);
        }
    }

    internal static async Task<byte[]> ReadExactlyAsync(Stream stream, int length)
    {
        var data = new byte[length];
        await stream.ReadExactlyAsync(data);
        return data;
    }

    internal static Task WriteAsync(Stream stream, string text) =>
        stream.WriteAsync(Encoding.ASCII.GetBytes(text)).AsTask();

    internal static int UnusedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
