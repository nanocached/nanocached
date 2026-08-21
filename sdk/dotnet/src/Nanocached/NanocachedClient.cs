using System.Collections.Concurrent;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Nanocached;

/// <summary>
/// The public client. An address list names either a single
/// nanocached-node or discovery server(s) fronting a cluster —
/// <see cref="ConnectAsync(Options)"/> finds out from the server's own handshake
/// response (doc/adr/0007-*.md), so calling code is identical either way.
///
/// Cluster mode implements ADR-0011 client-side replication: writes fan
/// out to each key's top-R owners (the primary's result decides; a dead
/// replica never fails a write), reads ask the primary and fall over to
/// the next owner only when the holder is unreachable. Dead connections
/// are redialed lazily on use (with one transparent retry — a socket only
/// learns of a peer FIN on I/O, and every operation is idempotent), and an
/// opt-in keep-alive can hold connections open across the server's 60s
/// idle timeout.
///
/// Thread-safe. Requests are serialized per connection; concurrent
/// callers queue.
/// </summary>
public sealed class NanocachedClient : IDisposable
{
    /// <summary>Options for <see cref="ConnectAsync(Options)"/>.</summary>
    public sealed class Options
    {
        /// <summary>Targets to try, in order — a one-element list is the
        /// single-target case. Everything is either a single
        /// nanocached-node or a discovery replica (ADR-0010) fronting a
        /// cluster; both the initial connect and every later node-list
        /// refresh walk this list until one yields a working target.</summary>
        public List<(string Host, int Port)> Addresses { get; } = new();

        /// <summary>Shared secret matching NANOCACHED_AUTH_SECRET on the server.</summary>
        public string? AuthSecret { get; set; }

        /// <summary>Connect over TLS. Defaults to the platform/system
        /// trust store; set <see cref="Ca"/> for a private CA. Ignored
        /// (silently) when false, even if <see cref="Ca"/> is set.</summary>
        public bool Tls { get; set; }

        /// <summary>Path to a PEM file of trusted root certificate(s),
        /// replacing the default trust store. Only meaningful when
        /// <see cref="Tls"/> is true; an unreadable or unparseable file is
        /// a connect-time error.</summary>
        public string? Ca { get; set; }

        /// <summary>Transparently compress values above
        /// <see cref="CompressionThreshold"/> on set and decompress them
        /// on get/getBytes (doc/adr/0013-*.md). Off by default. <b>Every
        /// client that reads or writes a given set of keys must agree on
        /// this setting</b> — it is a per-keyspace format decision, not a
        /// per-client preference; see the ADR's Consequences before
        /// enabling this against an existing keyspace another client may
        /// still touch with <see cref="Compress"/> off.</summary>
        public bool Compress { get; set; }

        /// <summary>Values shorter than this (in bytes) are never
        /// compressed — the per-value overhead of attempting it outweighs
        /// the savings. Only meaningful when <see cref="Compress"/> is
        /// true. Default 256.</summary>
        public int CompressionThreshold { get; set; } = 256;

        /// <summary>Let SetAsync/DeleteAsync return as soon as the primary
        /// owner acks, letting replica legs finish in the background
        /// instead of waiting for them too (doc/adr/0014-*.md). Off by
        /// default. Unlike <see cref="Compress"/>, this is a pure
        /// latency/durability trade for this client's own writes — it
        /// carries no wire format and needs no agreement with other
        /// clients.</summary>
        public bool FireAndForgetReplicas { get; set; }

        /// <summary>On a clean miss (the key's first-reached owner reports
        /// it missing), probe the remaining owners before accepting that,
        /// and repair the primary in the background if one still has the
        /// value (doc/adr/0015-*.md). Off by default. Costs extra reads
        /// only on the misses this actually applies to.</summary>
        public bool ReadRepair { get; set; }

        /// <summary>How long, after a reconnect dial to an address fails,
        /// that address is treated as still down — a call routed to it
        /// during this window fails immediately with the original dial
        /// error instead of paying another full connect timeout redialing
        /// an address that just proved unreachable. Default 1 second. Keep
        /// well under <c>NodeListStaleAfter</c> (30s) so a node that
        /// genuinely recovers isn't shut out for long.</summary>
        public TimeSpan ReconnectCooldown { get; set; } = TimeSpan.FromMilliseconds(1000);
    }

    /// <summary>
    /// Point-in-time snapshot returned by <see cref="Stats"/>: counters
    /// for failures this client swallows by design instead of surfacing
    /// to the caller — a dead replica leg on a write (doc/adr/0011-*.md,
    /// doc/adr/0014-*.md), a failed background repair of the primary
    /// after read-repair found a value on another owner (doc/adr/0015-*.md),
    /// and a failed node-list refresh attempt or per-node reconnect
    /// during one. None of these ever fail an operation; this is purely
    /// observability so an operator who only watches for thrown
    /// exceptions can still notice replication silently degrading or a
    /// node-list refresh stuck failing.
    /// </summary>
    public readonly record struct ClientStats(
        long ReplicaWriteFailures, long ReadRepairFailures, long RefreshFailures);

    // The server rejects (and drops the connection for) any request frame
    // over MAX_REQUEST_SIZE (src/server.rs), 1 MiB — a hard cap on the
    // *whole* frame, header included. Validating key/value length against
    // that exact number would still let a caller build a frame that trips
    // it once the "G "/"S "/"D "/lengths/ttl/tag header text and framing
    // are added, so this constant carries headroom for that header —
    // comfortably more than any header this SDK ever writes (audit finding
    // D2). 256 bytes, standardized across every SDK (Go/Rust's original
    // value; Java's and TypeScript's headroom constants match). Catching
    // an oversize request here, before it ever reaches Connection, avoids
    // the confusing alternative of the server silently closing the
    // connection with no response (see request_is_too_large in server.rs
    // — an over-limit frame gets no reply at all).
    private const int MaxRequestBytes = 1024 * 1024 - 256;

    private static readonly TimeSpan NodeListStaleAfter = TimeSpan.FromSeconds(30);
    // Reserved by the SDKs so a real application key can never collide
    // with it: a GET refreshes the pinged key's server-side LRU recency,
    // which is exactly why collision would matter — an app using key
    // {0x00} would previously have had its recency silently refreshed on
    // every keep-alive tick. The leading 0x00 also keeps this out of any
    // plausible printable-string keyspace.
    private static readonly byte[] KeepaliveKey =
        new byte[] { 0x00 }.Concat(Encoding.ASCII.GetBytes("nanocached-keepalive")).ToArray();

    // TTL a read-repair write uses (doc/adr/0015-*.md), in whole seconds —
    // the protocol's TTL unit throughout (see SetAsync's ttlSeconds). The
    // original TTL isn't recoverable from a GET response, and repairing
    // with TTL 0 (no expiry) would permanently resurrect data that was
    // legitimately expiring; 60s bounds the overshoot instead — an
    // immortal key just gets re-repaired on a later miss. Cross-SDK
    // policy decision, applied identically across all SDKs.
    private const long ReadRepairTtlSeconds = 60;

    private sealed class Member
    {
        internal Member(string address, Connection connection)
        {
            Address = address;
            Connection = connection;
        }

        internal string Address { get; set; }
        internal Connection Connection { get; set; }
    }

    // Process-global: how many open sockets exist right now for a given
    // connect target ("host:port"), across every NanocachedClient instance
    // in this process. Purely a programming-error guard (issue #12) — it
    // never affects behavior, only whether connect()/close() warn. Mirrors
    // sdk/typescript/src/client.ts's openTargets.
    private static readonly ConcurrentDictionary<string, int> OpenTargets = new();

    private static bool HasOpenTarget(string key) =>
        OpenTargets.TryGetValue(key, out int count) && count > 0;

    private static void IncrementOpenTarget(string key) =>
        OpenTargets.AddOrUpdate(key, 1, (_, count) => count + 1);

    private static void DecrementOpenTarget(string key) =>
        OpenTargets.AddOrUpdate(key, 0, (_, count) => Math.Max(0, count - 1));

    private readonly object _stateLock = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly Dictionary<string, SemaphoreSlim> _redialGates = new();
    private readonly List<(string Host, int Port)> _addresses;
    private readonly byte[]? _authSecret;
    private readonly SslClientAuthenticationOptions? _tls;
    private readonly bool _compress;
    private readonly int _compressionThreshold;
    private readonly bool _fireAndForgetReplicas;
    private readonly bool _readRepair;
    private readonly TimeSpan _reconnectCooldown;
    /// <summary>Per-address reconnect cooldown (see
    /// <see cref="Options.ReconnectCooldown"/>): the address of the most
    /// recently failed dial, and how long it stays "down" before another
    /// dial to it is attempted. Keyed by address, not slot — a member's
    /// slot (node name) can be reassigned to a different address by a
    /// refresh, but the address itself is what's actually unreachable.
    /// Mirrors TypeScript's reconnectCooldowns.</summary>
    private readonly ConcurrentDictionary<string, (DateTime Until, Exception Error)> _reconnectCooldowns = new();
    // doc/adr/0014-*.md: bounds in-flight background replica writes and
    // lets Close() drain them before tearing down connections.
    private readonly SemaphoreSlim _backgroundReplicaPermits;
    // The permit count _backgroundReplicaPermits was built with, captured
    // so Close() can acquire exactly all of them even if a test mutates
    // the static MaxInFlightBackgroundReplicaWrites afterwards.
    private readonly int _backgroundReplicaPermitCount;
    // Same mechanism as _backgroundReplicaPermits, but for the background
    // primary-repair write TryReadRepairAsync fires (doc/adr/0015-*.md):
    // bounds how many may run at once and lets Close() drain them too,
    // instead of the unbounded, untracked Task.Run this used to be.
    private readonly SemaphoreSlim _backgroundReadRepairPermits;
    private readonly int _backgroundReadRepairPermitCount;
    private readonly CancellationTokenSource _lifetime = new();

    // Observability for failures this client swallows by design — see
    // Stats(). Interlocked because they're incremented from whichever
    // thread happens to hit the swallow site (foreground calls,
    // background replica writes, the keep-alive loop's redials).
    private long _replicaWriteFailures;
    private long _readRepairFailures;
    private long _refreshFailures;

    private volatile bool _closed;
    // 0 = open, 1 = closed. Gates Close() with Interlocked.Exchange the
    // same way Connection._closedFlag does: the volatile _closed alone
    // gives visibility but not atomicity, so two concurrent Close() calls
    // could both pass a plain check and both run the teardown body.
    private int _closeCalled;
    private Connection? _single;
    private string? _singleAddress;
    private readonly Dictionary<string, Member> _members = new();
    private HashRing? _ring;
    private int _replication = 1;
    private DateTime _lastFetch = DateTime.UtcNow;

    // The address that answered connect() — a node's own address in
    // single mode, the winning discovery server's address in cluster
    // mode. Fixed for the client's lifetime; keys every socket this
    // client ever opens in OpenTargets (mirrors TS's `this.url`).
    private string? _targetKey;

    private NanocachedClient(Options options)
    {
        _addresses = options.Addresses.ToList();
        _authSecret = options.AuthSecret is null ? null : Encoding.UTF8.GetBytes(options.AuthSecret);
        _tls = BuildTlsOptions(options);
        _compress = options.Compress;
        _compressionThreshold = options.CompressionThreshold;
        _fireAndForgetReplicas = options.FireAndForgetReplicas;
        _backgroundReplicaPermitCount = MaxInFlightBackgroundReplicaWrites;
        _backgroundReplicaPermits = new SemaphoreSlim(_backgroundReplicaPermitCount, _backgroundReplicaPermitCount);
        _backgroundReadRepairPermitCount = MaxInFlightBackgroundReadRepairs;
        _backgroundReadRepairPermits = new SemaphoreSlim(_backgroundReadRepairPermitCount, _backgroundReadRepairPermitCount);
        _readRepair = options.ReadRepair;
        _reconnectCooldown = options.ReconnectCooldown;
    }

    /// <summary>Builds the internal TLS options for every dial this client
    /// makes. <c>null</c> means plaintext. <see cref="Options.Ca"/> is
    /// silently ignored when <see cref="Options.Tls"/> is false; an
    /// unreadable/unparseable CA file when true is a connect-time
    /// error.</summary>
    private static SslClientAuthenticationOptions? BuildTlsOptions(Options options)
    {
        if (!options.Tls) return null;
        if (options.Ca is null) return new SslClientAuthenticationOptions();

        X509Certificate2Collection roots;
        try
        {
            roots = new X509Certificate2Collection();
            roots.ImportFromPemFile(options.Ca);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or CryptographicException)
        {
            throw new NanocachedException(
                $"nanocached: could not read CA file {options.Ca}: {error.Message}", error);
        }

        return new SslClientAuthenticationOptions
        {
            RemoteCertificateValidationCallback = (_, certificate, _, sslPolicyErrors) =>
            {
                if (certificate is null) return false;

                // D1: SslStream already checked the presented certificate's
                // identity against SslClientAuthenticationOptions.TargetHost
                // (set to the dialed host in Identify.OpenAsync) before
                // calling this callback — a name mismatch is reported here,
                // not thrown, precisely so a callback like this one gets a
                // chance to override it. The custom chain build below only
                // ever re-validates the *trust* leg (does this leaf chain to
                // our private CA?); it says nothing about whether the leaf
                // was issued to the host we actually connected to. Without
                // this check, any certificate this private CA ever issued —
                // for any hostname — would be accepted for every host,
                // silently defeating hostname verification. Only the
                // trust-chain error is ours to override; a name mismatch (or
                // no certificate at all) stays fatal.
                if ((sslPolicyErrors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0
                    || (sslPolicyErrors & SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
                {
                    return false;
                }

                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Clear();
                chain.ChainPolicy.CustomTrustStore.AddRange(roots);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                using var leaf = new X509Certificate2(certificate);
                return chain.Build(leaf);
            },
        };
    }

    public static async Task<NanocachedClient> ConnectAsync(Options options)
    {
        if (options.Addresses.Count == 0)
        {
            throw new ArgumentException(
                "nanocached: connect() needs a non-empty addresses list", nameof(options));
        }
        if (options.ReconnectCooldown < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), "nanocached: ReconnectCooldown must not be negative");
        }

        var client = new NanocachedClient(options);

        // Walk the addresses until one yields a working target; an
        // address that is unreachable, warming up (B, ADR-0010), or knows
        // no live nodes is skipped — the next replica may do better.
        Exception? lastError = null;
        foreach (var (host, port) in client._addresses)
        {
            string key = $"{host}:{port}";
            // Only meaningful for a single configured address: with
            // multiple addresses, another client instance legitimately
            // holding connections to the same address makes this
            // heuristic false-positive (issue #12).
            if (client._addresses.Count == 1 && HasOpenTarget(key))
            {
                Console.Error.WriteLine(
                    $"nanocached: connect() called for {key} while a previous connection to it "
                    + "is still open — was close() forgotten?");
            }

            Identify.Result identified;
            try
            {
                identified = await Identify
                    .ConnectAndIdentifyAsync(host, port, client._authSecret, client._tls)
                    .ConfigureAwait(false);
            }
            catch (Exception error) when (error is NanocachedException or IOException or System.Net.Sockets.SocketException)
            {
                lastError = error;
                continue;
            }

            try
            {
                switch (identified)
                {
                    case Identify.NodeTarget node:
                        if (client._addresses.Count > 1)
                        {
                            Console.Error.WriteLine(
                                $"nanocached: {host}:{port} is a cache node, so this client is pinned "
                                + $"to that single server — the {client._addresses.Count - 1} remaining "
                                + "address(es) will not be used. Point addresses at discovery servers "
                                + "for cluster routing and failover.");
                        }
                        client._targetKey = key;
                        client._single = client.NewConnection(node.Stream, node.Tagged);
                        client._singleAddress = key;
                        client.StartKeepAlive();
                        return client;

                    case Identify.ClusterTarget cluster when cluster.Nodes.Count == 0:
                        lastError = new NanocachedException(
                            $"nanocached: no live nodes registered with the discovery server at {host}:{port}");
                        continue;

                    case Identify.ClusterTarget cluster:
                        client._targetKey = key;
                        await client.OpenClusterAsync(cluster).ConfigureAwait(false);
                        client.StartKeepAlive();
                        return client;
                }
            }
            catch
            {
                client.Teardown();
                throw;
            }
        }

        throw lastError ?? new NanocachedException("nanocached: could not connect to any address");
    }

    /// <summary>Wraps a freshly identified node stream into a tracked
    /// <see cref="Connection"/>: increments <see cref="OpenTargets"/> for
    /// this client's <see cref="_targetKey"/> now, and decrements it
    /// exactly once whenever this connection eventually closes — however
    /// that happens (client.Close(), a refresh reconciling a departed
    /// node, a dead-connection replacement, or discarding a redial that
    /// raced a concurrent Close()).</summary>
    private Connection NewConnection(Stream stream, bool tagged)
    {
        string key = _targetKey!;
        IncrementOpenTarget(key);
        return new Connection(stream, tagged, () => DecrementOpenTarget(key));
    }

    private async Task OpenClusterAsync(Identify.ClusterTarget cluster)
    {
        foreach (DiscoveredNode node in cluster.Nodes)
        {
            _members[node.Name] = new Member(
                node.Address, await OpenNodeConnectionAsync(node.Address).ConfigureAwait(false));
        }
        _ring = new HashRing(cluster.Nodes.Select(node => node.Name).ToList());
        _replication = cluster.Replication;
    }

    // ── 公開 API ──────────────────────────────────────────────────

    /// <summary>How many nodes hold each key (ADR-0011) — 1 against a single node.</summary>
    public int Replication => _ring is not null ? _replication : 1;

    public bool IsClosed => _closed;

    /// <summary>
    /// A snapshot of counters for failures this client swallows by
    /// design (ADR-0011/0014's replica-leg writes, ADR-0015's
    /// read-repair, and node-list refresh) — see <see cref="ClientStats"/>
    /// for exactly what each counts. Nothing here ever fails an
    /// operation; this exists purely so an operator can detect
    /// replication silently degrading or a node-list refresh that is
    /// stuck failing.
    /// </summary>
    public ClientStats Stats() => new(
        Interlocked.Read(ref _replicaWriteFailures),
        Interlocked.Read(ref _readRepairFailures),
        Interlocked.Read(ref _refreshFailures));

    // Strict — never silently replaces a malformed byte with U+FFFD; a
    // non-UTF-8 value raises DecoderFallbackException instead.
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public Task<string?> GetAsync(string key) => GetAsync(Encoding.UTF8.GetBytes(key));

    /// <summary>Returns the value decoded as UTF-8, or <c>null</c> when
    /// the key is missing. A value that is not valid UTF-8 raises
    /// <see cref="System.Text.DecoderFallbackException"/> — use
    /// <see cref="GetBytesAsync(byte[])"/> for the raw bytes instead.</summary>
    public async Task<string?> GetAsync(byte[] key)
    {
        byte[]? value = await GetBytesAsync(key).ConfigureAwait(false);
        return value is null ? null : StrictUtf8.GetString(value);
    }

    public Task<byte[]?> GetBytesAsync(string key) => GetBytesAsync(Encoding.UTF8.GetBytes(key));

    /// <summary>Returns the raw value, or <c>null</c> when the key is
    /// missing. Transparently decompresses when <c>Compress</c> is
    /// enabled (doc/adr/0013-*.md). With <c>ReadRepair</c>, a clean miss
    /// probes the remaining owners before being accepted as final
    /// (doc/adr/0015-*.md).</summary>
    public async Task<byte[]?> GetBytesAsync(byte[] key)
    {
        ValidateKey(key);
        await BeforeOperationAsync().ConfigureAwait(false);
        byte[]? value = await WithClusterRetryAsync(
            () => ReadAsync(key, connection => connection.GetAsync(key))).ConfigureAwait(false);
        if (value is null && _readRepair && _ring is not null)
        {
            value = await TryReadRepairAsync(key).ConfigureAwait(false);
        }
        return value is null || !_compress ? value : Compression.DecompressValue(value);
    }

    /// <summary>doc/adr/0015-*.md: probes every owner of <paramref
    /// name="key"/>, in rank order, for a value the normal read path
    /// already reported missing. The first owner that has it wins: its
    /// value is returned, and — detached, not awaited, but bounded and
    /// tracked via <see cref="_backgroundReadRepairPermits"/> the same way
    /// <see cref="_backgroundReplicaPermits"/> bounds replica writes —
    /// that same value repairs the true primary in the background, with
    /// TTL <see cref="ReadRepairTtlSeconds"/> (the original TTL can't be
    /// recovered from a GET, and TTL 0 would permanently resurrect
    /// already-expired data). Every failure along the way (connection
    /// lost, WrongNode, another miss) is swallowed; nothing here may turn
    /// an already-accepted miss into an error. A failure repairing the
    /// primary specifically is counted via <see cref="Stats"/>'s
    /// <c>ReadRepairFailures</c>.</summary>
    private async Task<byte[]?> TryReadRepairAsync(byte[] key)
    {
        IReadOnlyList<string> names = OwnerNames(key);
        foreach (string name in names)
        {
            byte[]? value;
            try
            {
                value = await ApplyReconnectingAsync(name, connection => connection.GetAsync(key))
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                continue;
            }
            if (value is null) continue;

            if (names.Count > 0 && _backgroundReadRepairPermits.Wait(0))
            {
                string primary = names[0];
                byte[] repairValue = value;
                Task background = Task.Run(async () =>
                {
                    try
                    {
                        await ApplyReconnectingAsync<object?>(primary, async connection =>
                        {
                            await connection.SetAsync(key, repairValue, ReadRepairTtlSeconds).ConfigureAwait(false);
                            return null;
                        }).ConfigureAwait(false);
                    }
                    catch (Exception error) when (error is NanocachedException or IOException
                        or System.Net.Sockets.SocketException or ObjectDisposedException)
                    {
                        // Swallowed by design — see the doc comment; now
                        // counted via Stats().ReadRepairFailures. Narrowed
                        // to the connection layer's own failure types, so
                        // a programming bug here (e.g. a
                        // NullReferenceException) propagates instead of
                        // being treated identically to a dead primary.
                        // OperationCanceledException is deliberately not
                        // caught here either — it propagates too.
                        Interlocked.Increment(ref _readRepairFailures);
                    }
                });
                // Bounded and tracked (doc/adr/0014-*.md's mechanism,
                // reused here): past MaxInFlightBackgroundReadRepairs
                // in-flight repairs, Wait(0) above just fails and this
                // particular repair is skipped rather than queued — a
                // missed repair is harmless, so there is no synchronous
                // fallback to await here the way replica writes have.
                _ = background.ContinueWith(
                    completed =>
                    {
                        // D3: the try/catch inside `background` already
                        // swallows every expected failure and counts it via
                        // ReadRepairFailures — this observes whatever
                        // *escaped* that (a real bug, or the deliberately
                        // uncaught OperationCanceledException from Close()).
                        // Reading .Exception both marks it observed (an
                        // unfaulted background Task's exception would
                        // otherwise vanish silently — no logger callback
                        // exists in this SDK to hand it to instead) and
                        // gets it counted in the one diagnostic channel
                        // Stats() already exposes for this failure mode.
                        if (completed.Exception is not null)
                        {
                            Interlocked.Increment(ref _readRepairFailures);
                        }
                        _backgroundReadRepairPermits.Release();
                    },
                    TaskScheduler.Default);
            }
            return value;
        }
        return null;
    }

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no expiry.</summary>
    public Task SetAsync(string key, string value, long ttlSeconds = 0) =>
        SetAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ttlSeconds);

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no
    /// expiry. Transparently compresses values at or above
    /// <c>CompressionThreshold</c> when <c>Compress</c> is enabled
    /// (doc/adr/0013-*.md).</summary>
    public async Task SetAsync(byte[] key, byte[] value, long ttlSeconds = 0)
    {
        if (ttlSeconds < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ttlSeconds), $"nanocached: ttlSeconds must be non-negative, got {ttlSeconds}");
        }
        ValidateKeyAndValue(key, value);
        byte[] outgoing = _compress ? Compression.CompressValue(value, _compressionThreshold) : value;
        await BeforeOperationAsync().ConfigureAwait(false);
        await WithClusterRetryAsync<object?>(async () =>
        {
            await WriteAsync<object?>(key, async connection =>
            {
                await connection.SetAsync(key, outgoing, ttlSeconds).ConfigureAwait(false);
                return null;
            }).ConfigureAwait(false);
            return null;
        }).ConfigureAwait(false);
    }

    public Task<bool> DeleteAsync(string key) => DeleteAsync(Encoding.UTF8.GetBytes(key));

    /// <summary>Returns whether the key existed before this call.</summary>
    public async Task<bool> DeleteAsync(byte[] key)
    {
        ValidateKey(key);
        await BeforeOperationAsync().ConfigureAwait(false);
        return await WithClusterRetryAsync(
            () => WriteAsync(key, connection => connection.DeleteAsync(key))).ConfigureAwait(false);
    }

    /// <summary>Rejects an empty key, or one so large that a bare
    /// <c>"G "</c>/<c>"D "</c> header plus the key alone would already risk
    /// tripping the server's MAX_REQUEST_SIZE (audit finding D2) — checked
    /// synchronously, before any connection is touched, mirroring
    /// <see cref="SetAsync(byte[], byte[], long)"/>'s ttlSeconds
    /// check.</summary>
    private static void ValidateKey(byte[] key)
    {
        if (key.Length == 0)
        {
            throw new ArgumentException("nanocached: key must not be empty", nameof(key));
        }
        if (key.Length > MaxRequestBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(key),
                $"nanocached: key is {key.Length} bytes, which exceeds the {MaxRequestBytes}-byte "
                + "request limit (server MAX_REQUEST_SIZE, src/server.rs, is 1 MiB)");
        }
    }

    /// <summary>As <see cref="ValidateKey"/>, plus rejects a key+value pair
    /// too large for a single <c>S</c> request to have any chance of
    /// fitting under the server's MAX_REQUEST_SIZE (audit finding D2).
    /// Checked against the caller-supplied value, before compression —
    /// compression only ever shrinks what actually goes on the wire, so
    /// this is the conservative (never falsely permissive) side to
    /// check.</summary>
    private static void ValidateKeyAndValue(byte[] key, byte[] value)
    {
        ValidateKey(key);
        long total = (long)key.Length + value.Length;
        if (total > MaxRequestBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                $"nanocached: key ({key.Length} bytes) + value ({value.Length} bytes) = {total} bytes, "
                + $"which exceeds the {MaxRequestBytes}-byte request limit (server MAX_REQUEST_SIZE, "
                + "src/server.rs, is 1 MiB)");
        }
    }

    /// <summary>Idempotent; later operations throw <see cref="AlreadyClosedException"/>.
    /// A second call warns to stderr instead of erroring — usually a sign
    /// the caller lost track of this instance's lifecycle.</summary>
    public void Close()
    {
        if (Interlocked.Exchange(ref _closeCalled, 1) != 0)
        {
            Console.Error.WriteLine("nanocached: close() called again on an already-closed client");
            return;
        }
        _closed = true;
        _lifetime.Cancel();
        // doc/adr/0014-*.md: give background replica writes a chance to
        // finish before their connections are torn out from under them.
        // Acquiring every permit — rather than snapshotting the task set —
        // closes the registration race: a WriteAsync that passed its
        // _closed check before this call can still be about to start a
        // background leg, and a snapshot taken in that window would miss
        // it. Once all permits are held here no new leg can start
        // (Wait(0) fails, so the leg falls back to the synchronous path),
        // and each permit is only released after its leg completed.
        // Bounded by the permit count, so this is a short wait in
        // practice.
        for (int i = 0; i < _backgroundReplicaPermitCount; i++)
        {
            _backgroundReplicaPermits.Wait();
        }
        // Same drain, for the background read-repair writes fired by
        // TryReadRepairAsync.
        for (int i = 0; i < _backgroundReadRepairPermitCount; i++)
        {
            _backgroundReadRepairPermits.Wait();
        }
        Teardown();
    }

    public void Dispose() => Close();

    private void Teardown()
    {
        // D4: Cancel() alone never released the CancellationTokenSource
        // itself — only cancelling the tokens issued from it — so every
        // Close()/failed-connect Teardown() leaked one. Cancel() first
        // (idempotent, and a no-op if Close() already called it above) so
        // a token still in flight observes cancellation before the source
        // beneath it goes away.
        _lifetime.Cancel();
        _lifetime.Dispose();
        lock (_stateLock)
        {
            _single?.Close();
            foreach (Member member in _members.Values) member.Connection.Close();
        }
    }

    // ── ルーティングと複製 ────────────────────────────────────────

    private async Task BeforeOperationAsync()
    {
        if (_closed) throw new AlreadyClosedException();
        await MaybeRefreshAsync(force: false).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs the operation; on a <c>W</c> answer (stale routing) or a
    /// connection-level failure that exhausted the current ranking (e.g.
    /// the key's primary died), forces a node-list refresh and retries the
    /// whole operation once against the fresh ranking. The retry window
    /// for a dead node is therefore bounded by discovery's liveness
    /// timeout. A second failure after a fresh refresh propagates.
    /// </summary>
    private async Task<T> WithClusterRetryAsync<T>(Func<Task<T>> operation)
    {
        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (Exception error) when (error is WrongNodeException or ConnectionLostException)
        {
            if (_ring is null) throw;
            await MaybeRefreshAsync(force: true).ConfigureAwait(false);
            return await operation().ConfigureAwait(false);
        }
    }

    private IReadOnlyList<string> OwnerNames(byte[] key)
    {
        lock (_stateLock)
        {
            return _ring!.Owners(key, _replication);
        }
    }

    /// <summary>
    /// Runs <paramref name="op"/> against the slot's connection, retrying
    /// once on a connection-level failure: a socket only learns of a peer
    /// FIN (e.g. the server's 60s idle timeout) on I/O, so lazy
    /// reconnect-on-use means the failed request poisons the connection,
    /// the redial replaces it, and the operation runs again. Safe because
    /// get/set/delete are all idempotent.
    /// </summary>
    private async Task<T> ApplyReconnectingAsync<T>(string? slot, Func<Connection, Task<T>> op)
    {
        try
        {
            return await op(await SlotConnectionAsync(slot).ConfigureAwait(false)).ConfigureAwait(false);
        }
        catch (ConnectionLostException)
        {
            return await op(await SlotConnectionAsync(slot).ConfigureAwait(false)).ConfigureAwait(false);
        }
    }

    private async Task<T> ReadAsync<T>(byte[] key, Func<Connection, Task<T>> op)
    {
        if (_ring is null)
        {
            return await ApplyReconnectingAsync(null, op).ConfigureAwait(false);
        }

        // Owners in rank order; fall through only on connection-level
        // failure — a replica hedges against a dead holder, not a miss.
        Exception? lastError = null;
        foreach (string name in OwnerNames(key))
        {
            try
            {
                return await ApplyReconnectingAsync(name, op).ConfigureAwait(false);
            }
            catch (WrongNodeException)
            {
                throw;
            }
            catch (Exception error) when (error is NanocachedException)
            {
                // A Close() racing this read is included here (issue #47
                // item 4, superseding issue #12's fail-fast): keep
                // walking the remaining owners like Rust/Python/TS do.
                // Every later owner throws AlreadyClosedException too, so
                // the caller still surfaces it via lastError — the walk
                // just no longer takes a different path than the other
                // SDKs in the same race.
                lastError = error;
            }
        }
        throw lastError
              ?? new ConnectionLostException("nanocached: no owner is reachable for this key");
    }

    private async Task<T> WriteAsync<T>(byte[] key, Func<Connection, Task<T>> op)
    {
        if (_ring is null)
        {
            return await ApplyReconnectingAsync(null, op).ConfigureAwait(false);
        }

        IReadOnlyList<string> names = OwnerNames(key);
        if (names.Count == 0)
        {
            throw new ConnectionLostException("nanocached: no owner is reachable for this key");
        }

        // Fan out to the replicas concurrently with the primary write. The
        // primary's outcome decides; replica failures are swallowed by
        // design (ADR-0011) — a dead or disagreeing replica leaves the key
        // under-replicated until the next node-list refresh, never fails
        // the write. Now counted via Stats().ReplicaWriteFailures.
        async Task ReplicaWriteAsync(string name)
        {
            try
            {
                await ApplyReconnectingAsync(name, op).ConfigureAwait(false);
            }
            catch (Exception error) when (error is NanocachedException or IOException
                or System.Net.Sockets.SocketException or ObjectDisposedException)
            {
                // Swallowed by design — see above; counted via
                // Stats().ReplicaWriteFailures. Narrowed to the
                // connection layer's own failure types (a dead replica, a
                // lost socket), covering both the fire-and-forget and
                // synchronous-fallback callers of this local function, so
                // a programming bug doesn't get treated the same way as a
                // dead replica. OperationCanceledException is
                // deliberately not caught here either — it propagates.
                Interlocked.Increment(ref _replicaWriteFailures);
            }
        }

        var replicaWrites = new List<Task>();
        foreach (string name in names.Skip(1))
        {
            // doc/adr/0014-*.md: with FireAndForgetReplicas, up to
            // MaxInFlightBackgroundReplicaWrites legs run in the
            // background instead of being waited for below — past that
            // cap, further legs fall back to the synchronous path exactly
            // as with the option off.
            if (_fireAndForgetReplicas && _backgroundReplicaPermits.Wait(0))
            {
                Task background = Task.Run(() => ReplicaWriteAsync(name));
                _ = background.ContinueWith(
                    completed =>
                    {
                        // D3: ReplicaWriteAsync's own try/catch already
                        // swallows and counts every expected failure — this
                        // observes whatever escaped it (a real bug, or the
                        // deliberately uncaught OperationCanceledException
                        // from Close()) instead of letting it vanish as an
                        // unobserved task exception, and counts it in the
                        // same ReplicaWriteFailures Stats() already exposes.
                        if (completed.Exception is not null)
                        {
                            Interlocked.Increment(ref _replicaWriteFailures);
                        }
                        _backgroundReplicaPermits.Release();
                    },
                    TaskScheduler.Default);
                continue;
            }

            replicaWrites.Add(ReplicaWriteAsync(name));
        }

        try
        {
            return await ApplyReconnectingAsync(names[0], op).ConfigureAwait(false);
        }
        finally
        {
            await Task.WhenAll(replicaWrites).ConfigureAwait(false);
        }
    }

    // ── 遅延再接続 ────────────────────────────────────────────────

    private async Task<Connection> SlotConnectionAsync(string? slot)
    {
        (string slotKey, string address, Connection current) = SnapshotSlot(slot);
        if (!current.IsClosed) return current;

        // Concurrent requests finding the same dead connection share one
        // dial: the first caller redials, the rest wait then reuse.
        SemaphoreSlim gate;
        lock (_stateLock)
        {
            if (!_redialGates.TryGetValue(slotKey, out gate!))
            {
                gate = new SemaphoreSlim(1, 1);
                _redialGates[slotKey] = gate;
            }
        }

        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            (_, address, current) = SnapshotSlot(slot);
            if (!current.IsClosed) return current;

            Connection fresh = await DialWithCooldownAsync(address).ConfigureAwait(false);
            lock (_stateLock)
            {
                if (slot is null)
                {
                    _single = fresh;
                }
                else if (_members.TryGetValue(slot, out Member? member))
                {
                    member.Connection = fresh;
                }
                else
                {
                    fresh.Close();
                    throw new ConnectionLostException(
                        $"nanocached: {slot} left the cluster while reconnecting");
                }
            }
            return fresh;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Redials <paramref name="address"/>, honoring the
    /// per-address reconnect cooldown (see <see cref="_reconnectCooldowns"/>):
    /// an address whose dial just failed stays "down" for
    /// <see cref="_reconnectCooldown"/>, so a burst of calls routed to it —
    /// or one call every keep-alive tick — fails immediately with the same
    /// error the dial itself produced, instead of each paying another full
    /// connect timeout in turn. Callers must hold the slot's redial
    /// gate.</summary>
    private async Task<Connection> DialWithCooldownAsync(string address)
    {
        if (_reconnectCooldowns.TryGetValue(address, out var cooldown) && DateTime.UtcNow < cooldown.Until)
        {
            throw cooldown.Error;
        }

        try
        {
            Connection connection = await OpenNodeConnectionAsync(address).ConfigureAwait(false);
            _reconnectCooldowns.TryRemove(address, out _);
            return connection;
        }
        catch (NanocachedException error)
        {
            _reconnectCooldowns[address] = (DateTime.UtcNow + _reconnectCooldown, error);
            throw;
        }
    }

    private (string SlotKey, string Address, Connection Connection) SnapshotSlot(string? slot)
    {
        lock (_stateLock)
        {
            if (slot is null)
            {
                return ("", _singleAddress!, _single!);
            }
            if (!_members.TryGetValue(slot, out Member? member))
            {
                throw new ConnectionLostException($"nanocached: {slot} has no open connection");
            }
            return (slot, member.Address, member.Connection);
        }
    }

    private async Task<Connection> OpenNodeConnectionAsync(string address)
    {
        var (host, port) = Identify.SplitHostPort(address);
        Identify.Result identified;
        try
        {
            identified = await Identify
                .ConnectAndIdentifyAsync(host, port, _authSecret, _tls)
                .ConfigureAwait(false);
        }
        catch (Exception error) when (error is IOException or System.Net.Sockets.SocketException)
        {
            throw new ConnectionLostException(
                $"nanocached: could not connect to {address}: {error.Message}", error);
        }

        if (identified is not Identify.NodeTarget node)
        {
            throw new NanocachedException(
                $"nanocached: {address} no longer identifies as a cache node");
        }
        if (_closed)
        {
            node.Stream.Dispose();
            throw new AlreadyClosedException();
        }
        return NewConnection(node.Stream, node.Tagged);
    }

    // ── ノードリスト更新 ──────────────────────────────────────────

    private async Task MaybeRefreshAsync(bool force)
    {
        if (_ring is null) return;
        if (!force && DateTime.UtcNow - _lastFetch < NodeListStaleAfter) return;

        await _refreshGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!force && DateTime.UtcNow - _lastFetch < NodeListStaleAfter) return;
            await RefreshNodeListAsync().ConfigureAwait(false);
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private async Task RefreshNodeListAsync()
    {
        Identify.ClusterTarget? cluster = await FetchNodeListAsync().ConfigureAwait(false);
        _lastFetch = DateTime.UtcNow;
        if (cluster is null) return;

        var toOpen = new List<DiscoveredNode>();
        lock (_stateLock)
        {
            var byName = cluster.Nodes.ToDictionary(node => node.Name);

            foreach (string name in _members.Keys.Where(name => !byName.ContainsKey(name)).ToList())
            {
                _members[name].Connection.Close();
                _members.Remove(name);
                // Node names are per-process UUIDs; a departed node's
                // redial gate would otherwise leak forever (issue #12).
                _redialGates.Remove(name);
            }

            foreach (DiscoveredNode node in cluster.Nodes)
            {
                if (_members.TryGetValue(node.Name, out Member? existing))
                {
                    existing.Address = node.Address;
                }
                else
                {
                    toOpen.Add(node);
                }
            }
        }

        foreach (DiscoveredNode node in toOpen)
        {
            try
            {
                Connection connection = await OpenNodeConnectionAsync(node.Address).ConfigureAwait(false);
                lock (_stateLock)
                {
                    if (_closed)
                    {
                        // Close() ran while we were dialing (issue #10):
                        // installing this socket now would leak it.
                        connection.Close();
                        return;
                    }
                    _members[node.Name] = new Member(node.Address, connection);
                }
            }
            catch (NanocachedException)
            {
                Interlocked.Increment(ref _refreshFailures);
                // Left out of the ring for now; the next refresh retries
                // it. Silent by design — refresh is opportunistic/
                // best-effort and must never fail the caller's operation,
                // consistent with ADR-0011's eventual-consistency model.
                // Counted via Stats().RefreshFailures.
            }
        }

        lock (_stateLock)
        {
            _ring = new HashRing(_members.Keys.ToList());
            _replication = cluster.Replication;
        }
    }

    /// <summary>Walks every configured address (ADR-0010); <c>null</c>
    /// means keep the last-known list.</summary>
    private async Task<Identify.ClusterTarget?> FetchNodeListAsync()
    {
        foreach (var (host, port) in _addresses)
        {
            try
            {
                Identify.Result identified = await Identify
                    .ConnectAndIdentifyAsync(host, port, _authSecret, _tls)
                    .ConfigureAwait(false);
                switch (identified)
                {
                    case Identify.ClusterTarget cluster when cluster.Nodes.Count > 0:
                        return cluster;
                    case Identify.ClusterTarget:
                        continue;
                    case Identify.NodeTarget node:
                        node.Stream.Dispose();
                        continue;
                }
            }
            catch (Exception error) when (error is NanocachedException or IOException or System.Net.Sockets.SocketException)
            {
                Interlocked.Increment(ref _refreshFailures);
                // Silent by design — refresh is opportunistic/best-effort
                // and must never fail the caller's operation, consistent
                // with ADR-0011's eventual-consistency model. The next
                // refresh retries; counted via Stats().RefreshFailures.
            }
        }
        return null;
    }

    // ── keep-alive ────────────────────────────────────────────────

    // Always on, with an internal interval (issue #27): half the
    // server's 60s idle timeout, so it never severs a healthy client.
    // Internal and mutable only so tests can shorten it.
    internal static TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(30);

    // doc/adr/0014-*.md: bounds how many replica writes a single client
    // may have running in the background at once when
    // FireAndForgetReplicas is enabled — once the cap is reached, further
    // replica legs fall back to running synchronously, the same as with
    // the option off. Internal and mutable only so tests can shrink it,
    // mirroring KeepAliveInterval. Read once per constructor call, so
    // tests must set it before ConnectAsync().
    internal static int MaxInFlightBackgroundReplicaWrites = 32;

    // Same idea as MaxInFlightBackgroundReplicaWrites, but bounds
    // TryReadRepairAsync's background primary-repair write instead: once
    // the cap is reached, further repair attempts are skipped rather than
    // queued, since a missed repair is harmless (the key is simply
    // repaired on some later miss) and this path must never add latency
    // to the read it rides along with. Internal and mutable only so tests
    // can shrink it. Read once per constructor call, so tests must set it
    // before ConnectAsync().
    internal static int MaxInFlightBackgroundReadRepairs = 32;

    private void StartKeepAlive()
    {
        TimeSpan every = KeepAliveInterval;

        CancellationToken token = _lifetime.Token;
        _ = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(every);
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                List<Connection> connections;
                lock (_stateLock)
                {
                    connections = _single is not null
                        ? new List<Connection> { _single }
                        : _members.Values.Select(member => member.Connection).ToList();
                }
                foreach (Connection connection in connections)
                {
                    if (connection.IsClosed || connection.Idle < every) continue;
                    try
                    {
                        // Any parseable reply proves liveness — N, or W
                        // from a non-owner — and resets the idle timer.
                        await connection.GetAsync(KeepaliveKey).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Keep-alive failures never surface; use redials lazily.
                    }
                }
            }
        }, token);
    }
}
