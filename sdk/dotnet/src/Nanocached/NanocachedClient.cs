using System.Collections.Concurrent;
using System.Net.Security;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Nanocached;

/// <summary>
/// The public client. An address list names either a single
/// nanocached-node or discovery server(s) fronting a cluster —
/// <see cref="ConnectAsync(Options)"/> finds out from the server's own handshake
/// response (the server type in the auth response), so calling code is identical either way.
///
/// Cluster mode implements client-side replication client-side replication: writes fan
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
        /// nanocached-node or a discovery replica (discovery HA) fronting a
        /// cluster; both the initial connect and every later node-list
        /// refresh walk this list until one yields a working target.</summary>
        public List<(string Host, int Port)> Addresses { get; } = new();

        /// <summary>Shared secret matching NANOCACHED_AUTH_SECRET on the
        /// server. An empty string is the same as none, matching the
        /// other SDKs: sent literally, an empty secret would reach the
        /// wire as an explicit zero-length secret, which the server
        /// rejects and closes the connection for without replying —
        /// turning what should be "no auth configured" into an opaque
        /// <see cref="ConnectionLostException"/>.</summary>
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
        /// on get/getBytes (value compression). Off by default. <b>Every
        /// client that reads or writes a given set of keys must agree on
        /// this setting</b> — it is a per-keyspace format decision, not a
        /// per-client preference; take care before
        /// enabling this against an existing keyspace another client may
        /// still touch with <see cref="Compress"/> off.</summary>
        public bool Compress { get; set; }

        /// <summary>Values shorter than this (in bytes) are never
        /// compressed — the per-value overhead of attempting it outweighs
        /// the savings. Only meaningful when <see cref="Compress"/> is
        /// true. Default 256. Negative is rejected by
        /// <see cref="ConnectAsync(Options)"/>.</summary>
        public int CompressionThreshold { get; set; } = 256;

        /// <summary>Let SetAsync/DeleteAsync return as soon as the primary
        /// owner acks, letting replica legs finish in the background
        /// instead of waiting for them too (fire-and-forget replica writes). Off by
        /// default. Unlike <see cref="Compress"/>, this is a pure
        /// latency/durability trade for this client's own writes — it
        /// carries no wire format and needs no agreement with other
        /// clients.</summary>
        public bool FireAndForgetReplicas { get; set; }

        /// <summary>On a clean miss (the key's first-reached owner reports
        /// it missing), probe the remaining owners before accepting that,
        /// and repair the primary in the background if one still has the
        /// value (read repair). Off by default. Costs extra reads
        /// only on the misses this actually applies to.</summary>
        public bool ReadRepair { get; set; }

        /// <summary>Send the same read to the next owner as well once the
        /// current wait has been silent for this long — first the primary,
        /// then (if still no answer) each further owner in turn, one more
        /// per interval, until every owner is in flight (Hedged reads). A
        /// slow-but-alive owner otherwise bounds every read that touches it
        /// at its full round trip, since the sequential read path only
        /// moves on to the next owner when the current one *fails*.
        /// <c>null</c> (the default) is off. Applies only when a ring is
        /// known and the key has at least 2 owners; with a single copy
        /// there is nobody to hedge to.
        ///
        /// <para>The first answer decides: a hit from any owner is final;
        /// a miss is final only from the primary — a replica's miss is
        /// provisional (it may simply lack the copy), so the primary is
        /// still waited for and hedging never turns a hit into a miss. A
        /// failure (connection-level, or any SDK exception other than
        /// <see cref="WrongNodeException"/>) hedges onward immediately,
        /// with no wait; a <see cref="WrongNodeException"/> from any owner
        /// propagates exactly as the non-hedged read path's does. The
        /// losing leg of a hedge is never cancelled — cancelling mid-write
        /// could desync a connection — but is left to finish detached and
        /// drained by <see cref="Close"/>, the same as a fire-and-forget
        /// replica write. Writes are unaffected. Rejected as invalid
        /// unless positive.</para></summary>
        public TimeSpan? ReadHedgeAfter { get; set; }

        /// <summary>How long, after a reconnect dial to an address fails,
        /// that address is treated as still down — a call routed to it
        /// during this window fails immediately with the original dial
        /// error instead of paying another full connect timeout redialing
        /// an address that just proved unreachable. Default 1 second. Keep
        /// well under <c>NodeListStaleAfter</c> (30s) so a node that
        /// genuinely recovers isn't shut out for long.
        ///
        /// <para><see cref="TimeSpan.Zero"/> means "use the default"
        /// (1 second), not "disable it" — this field already carries its
        /// own default value above, so zero is only ever seen here when a
        /// caller explicitly sets it back to zero, matching the Go SDK's
        /// zero-value <c>Config.ReconnectCooldown</c> and the Rust SDK's
        /// <c>Duration::ZERO</c>. To disable the cooldown entirely — every
        /// request that finds an address's connection dead pays its own
        /// full dial attempt instead of reusing a cached failure — set
        /// <see cref="DisableReconnectCooldown"/> instead (the Go SDK's
        /// equivalent is a negative <c>Config.ReconnectCooldown</c>; Rust's
        /// and Java's is their own <c>disableReconnectCooldown()</c>).
        /// Rejected as invalid if set negative; use
        /// <see cref="DisableReconnectCooldown"/> to disable, not a
        /// negative value.</para></summary>
        public TimeSpan ReconnectCooldown { get; set; } = TimeSpan.FromMilliseconds(1000);

        /// <summary>Disables the per-address reconnect cooldown entirely:
        /// every request that finds an address's connection dead pays its
        /// own full dial attempt instead of reusing a cached failure. See
        /// <see cref="ReconnectCooldown"/> for what the cooldown is. Off
        /// by default.</summary>
        public bool DisableReconnectCooldown { get; set; }
    }

    /// <summary>
    /// Point-in-time snapshot returned by <see cref="Stats"/>: counters
    /// for failures this client swallows by design instead of surfacing
    /// to the caller — a dead replica leg on a write (client-side replication,
    /// Fire-and-forget replica writes), a failed background repair of the primary
    /// after read-repair found a value on another owner (read repair),
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

    // TTL a read-repair write uses (read repair), in whole seconds —
    // the protocol's TTL unit throughout (see SetAsync's ttlSeconds). The
    // original TTL isn't recoverable from a GET response, and repairing
    // with TTL 0 (no expiry) would permanently resurrect data that was
    // legitimately expiring; 60s bounds the overshoot instead — an
    // immortal key just gets re-repaired on a later miss. Cross-SDK
    // policy decision, applied identically across all SDKs.
    private const long ReadRepairTtlSeconds = 60;

    private sealed class Member
    {
        internal Member(string address, Connection? connection)
        {
            Address = address;
            Connection = connection;
        }

        internal string Address { get; set; }

        /// <summary>The member's current connection — <c>null</c> for a
        /// member that discovery listed but that this client could not
        /// reach when it bootstrapped (issue #67): it stays routable (the
        /// ring already includes it — membership comes from discovery,
        /// unchanged), so a request for one of its keys fails over the
        /// same way it would after a mid-life node death, and the next
        /// request after the reconnect cooldown redials it (see
        /// <see cref="SlotConnectionAsync"/>).</summary>
        internal Connection? Connection { get; set; }
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
    // Resolved from Options.ReconnectCooldown/DisableReconnectCooldown:
    // null means disabled (Options.DisableReconnectCooldown was set); a
    // caller-specified TimeSpan.Zero resolves to DefaultReconnectCooldown
    // (see Options.ReconnectCooldown's doc comment) — never stored here as
    // literal zero, so DialWithCooldownAsync never has to special-case it.
    private readonly TimeSpan? _reconnectCooldown;
    private static readonly TimeSpan DefaultReconnectCooldown = TimeSpan.FromMilliseconds(1000);
    /// <summary>Per-address reconnect cooldown (see
    /// <see cref="Options.ReconnectCooldown"/>): the address of the most
    /// recently failed dial, and how long it stays "down" before another
    /// dial to it is attempted. Keyed by address, not slot — a member's
    /// slot (node name) can be reassigned to a different address by a
    /// refresh, but the address itself is what's actually unreachable.
    /// Mirrors TypeScript's reconnectCooldowns.</summary>
    private readonly ConcurrentDictionary<string, (DateTime Until, Exception Error)> _reconnectCooldowns = new();
    // Fire-and-forget replica writes: bounds in-flight background replica writes and
    // lets Close() drain them before tearing down connections.
    private readonly SemaphoreSlim _backgroundReplicaPermits;
    // The permit count _backgroundReplicaPermits was built with, captured
    // so Close() can acquire exactly all of them even if a test mutates
    // the static MaxInFlightBackgroundReplicaWrites afterwards.
    private readonly int _backgroundReplicaPermitCount;
    // TryReadRepairAsync's background primary-repair write
    // (read repair) draws from the same pool — one combined cap,
    // like every other SDK — so Close() drains it the same way.
    private readonly TimeSpan? _readHedgeAfter;
    // Hedge legs (Hedged reads) still running after a read has already
    // returned via a different leg (the losers): never cancelled —
    // cancelling mid-write could desync a connection (see Connection's own
    // doc comment on why no CancellationToken ever reaches the stream) —
    // left to finish detached, and drained by Close() exactly like
    // _backgroundReplicaPermits' pool is. Unlike that pool this one is
    // unbounded: a hedge leg count is already bounded by a key's own
    // replication factor, not by how many reads are concurrently in
    // flight, so there is no cap to enforce here.
    private readonly ConcurrentDictionary<Task, byte> _hedgedReads = new();
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
        // Empty is the same as none (see Options.AuthSecret's doc comment)
        // — matches the other SDKs.
        _authSecret = string.IsNullOrEmpty(options.AuthSecret) ? null : Encoding.UTF8.GetBytes(options.AuthSecret);
        _tls = BuildTlsOptions(options);
        _compress = options.Compress;
        _compressionThreshold = options.CompressionThreshold;
        _fireAndForgetReplicas = options.FireAndForgetReplicas;
        _backgroundReplicaPermitCount = MaxInFlightBackgroundReplicaWrites;
        _backgroundReplicaPermits = new SemaphoreSlim(_backgroundReplicaPermitCount, _backgroundReplicaPermitCount);
        _readRepair = options.ReadRepair;
        _readHedgeAfter = options.ReadHedgeAfter;
        _reconnectCooldown = options.DisableReconnectCooldown
            ? null
            : options.ReconnectCooldown == TimeSpan.Zero
                ? DefaultReconnectCooldown
                : options.ReconnectCooldown;
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
                nameof(options),
                "nanocached: ReconnectCooldown must not be negative; set DisableReconnectCooldown "
                + "instead of a negative value to disable it");
        }
        if (options.CompressionThreshold < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"nanocached: CompressionThreshold must not be negative, got {options.CompressionThreshold}");
        }
        if (options.ReadHedgeAfter is TimeSpan hedgeAfter && hedgeAfter <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "nanocached: ReadHedgeAfter must be a positive duration");
        }

        var client = new NanocachedClient(options);

        // Walk the addresses until one yields a working target; an
        // address that is unreachable, warming up (B, discovery HA), or knows
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

    /// <summary>Dials every node discovery listed, concurrently. A node
    /// that can't be reached (issue #67: typically one that just died and
    /// discovery hasn't evicted yet — its liveness window is seconds long,
    /// and every key is still served by another owner when R &gt; 1) is
    /// installed without a connection and with its reconnect cooldown
    /// armed (<see cref="ArmReconnectCooldown"/>), exactly the state a
    /// member is in after dying mid-life, so requests for its keys fail
    /// over per request instead of the whole <see cref="ConnectAsync"/>
    /// failing. Only a cluster with <em>no</em> reachable node fails,
    /// with the last such dial error. A listed address that answers but no
    /// longer identifies as a cache node — or fails for any other reason
    /// that isn't a connection-level failure — remains a hard error, as
    /// before this fix: every dial outcome is gathered first (so any
    /// connections opened for other, reachable nodes are still recorded in
    /// <see cref="_members"/> and get torn down by
    /// <see cref="ConnectAsync"/>'s catch-and-<see cref="Teardown"/>,
    /// rather than leaking), and only then is that non-connection-level
    /// error re-thrown.</summary>
    private async Task OpenClusterAsync(Identify.ClusterTarget cluster)
    {
        async Task<(DiscoveredNode Node, Connection? Connection, Exception? Error)> DialNodeAsync(
            DiscoveredNode node)
        {
            try
            {
                return (node, await OpenNodeConnectionAsync(node.Address).ConfigureAwait(false), null);
            }
            catch (Exception error)
            {
                return (node, null, error);
            }
        }

        var outcomes = await Task.WhenAll(cluster.Nodes.Select(DialNodeAsync)).ConfigureAwait(false);

        Exception? lastError = null;
        Exception? fatal = null;
        int reachable = 0;
        foreach (var (node, connection, error) in outcomes)
        {
            if (connection is not null)
            {
                _members[node.Name] = new Member(node.Address, connection);
                reachable++;
                continue;
            }

            if (error is ConnectionLostException)
            {
                _members[node.Name] = new Member(node.Address, null);
                ArmReconnectCooldown(node.Address, error);
                lastError = error;
            }
            else
            {
                fatal ??= error;
            }
        }

        if (fatal is not null)
        {
            ExceptionDispatchInfo.Capture(fatal).Throw();
        }

        if (reachable == 0)
        {
            ExceptionDispatchInfo.Capture(lastError!).Throw();
        }

        _ring = new HashRing(cluster.Nodes.Select(node => node.Name).ToList());
        _replication = cluster.Replication;
    }

    // ── 公開 API ──────────────────────────────────────────────────

    /// <summary>How many nodes hold each key (client-side replication) — 1 against a single node.</summary>
    public int Replication => _ring is not null ? _replication : 1;

    public bool IsClosed => _closed;

    /// <summary>
    /// A snapshot of counters for failures this client swallows by
    /// design (client-side replication / fire-and-forget replica writes's replica-leg writes, read repair's
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
    /// enabled (value compression). With <c>ReadRepair</c>, a clean miss
    /// probes the remaining owners before being accepted as final
    /// (read repair).</summary>
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

    /// <summary>read repair: probes the remaining owners of
    /// <paramref name="key"/> — every owner but the primary, which the
    /// normal read path already probed and got a clean miss from — in
    /// rank order, for a value. The first one that has it wins: its value
    /// is returned, and — detached, not awaited, but bounded and tracked
    /// via <see cref="_backgroundReplicaPermits"/> — the same pool that
    /// bounds replica writes, one combined cap —
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
        if (names.Count == 0) return null;
        foreach (string name in names.Skip(1))
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

            if (_backgroundReplicaPermits.Wait(0))
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
                // Bounded and tracked (fire-and-forget replica writes's mechanism,
                // reused here — the one shared pool, so replica legs and
                // repairs together never exceed
                // MaxInFlightBackgroundReplicaWrites): past the cap,
                // Wait(0) above just fails and this
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
                        _backgroundReplicaPermits.Release();
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
    /// (value compression).</summary>
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
        // Fire-and-forget replica writes: give background replica writes a chance to
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
        // Hedged reads: every losing leg still running is already tracked
        // in _hedgedReads (added when started, before any await) — drained
        // here the same way the background replica writes above are,
        // before Teardown() below tears out the connections those legs are
        // still reading from. Each task is removed from the set by this
        // loop itself, not left to its own completion callback, so a leg
        // that finishes concurrently with this drain can't be waited on
        // twice or looked up after it's already gone.
        while (!_hedgedReads.IsEmpty)
        {
            Task leg = _hedgedReads.Keys.First();
            _hedgedReads.TryRemove(leg, out _);
            try
            {
                leg.GetAwaiter().GetResult();
            }
            catch
            {
                // Already the caller's or lastError's concern (whichever
                // read started this leg observed or discarded its outcome
                // already); Close() only needs to wait it out, not surface
                // it again.
            }
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
            foreach (Member member in _members.Values) member.Connection?.Close();
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

        IReadOnlyList<string> names = OwnerNames(key);
        if (_readHedgeAfter is TimeSpan hedgeAfter && names.Count > 1)
        {
            return await ReadHedgedAsync(op, names, hedgeAfter).ConfigureAwait(false);
        }

        // Owners in rank order; fall through only on connection-level
        // failure — a replica hedges against a dead holder, not a miss.
        Exception? lastError = null;
        foreach (string name in names)
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

    /// <summary>
    /// Hedged reads (<see cref="Options.ReadHedgeAfter"/>): a slow — not
    /// dead — owner otherwise bounds every read that touches it at its
    /// full round trip, since <see cref="ReadAsync{T}"/>'s sequential path
    /// only moves on to the next owner when the current one *fails*. Here
    /// the read starts at the primary, and if no answer has arrived within
    /// <paramref name="hedgeAfter"/> the same read is also sent to the next
    /// owner (and so on, one more owner per interval, until every owner is
    /// in flight); the first answer decides:
    ///
    /// <list type="bullet">
    /// <item>a hit from any owner is final;</item>
    /// <item>a miss is final only from the primary — a replica's miss is
    /// provisional (it may simply lack the copy) and the primary is still
    /// waited for, so hedging never turns a hit into a miss; it is
    /// accepted only once every owner has answered or failed;</item>
    /// <item>a failure (connection-level, or any
    /// <see cref="NanocachedException"/> other than
    /// <see cref="WrongNodeException"/>) hedges onward immediately, with
    /// no wait;</item>
    /// <item><see cref="WrongNodeException"/> propagates as in
    /// <see cref="ReadAsync{T}"/>.</item>
    /// </list>
    ///
    /// The losing legs are never cancelled — cancelling mid-write could
    /// desync a connection (see <see cref="Connection"/>'s own doc
    /// comment) — but are left to finish detached in
    /// <see cref="_hedgedReads"/>, their outcome observed, and drained by
    /// <see cref="Close"/>.
    /// </summary>
    private async Task<T> ReadHedgedAsync<T>(
        Func<Connection, Task<T>> op, IReadOnlyList<string> names, TimeSpan hedgeAfter)
    {
        var legIndex = new Dictionary<Task<T>, int>();

        Task<T> StartLeg(int index)
        {
            Task<T> task = ApplyReconnectingAsync(names[index], op);
            legIndex[task] = index;
            _hedgedReads[task] = 0;
            task.ContinueWith(
                completed =>
                {
                    // Retrieves the outcome so a losing leg's exception —
                    // this method's own caller only ever inspects the leg
                    // that decided the read — never surfaces as an
                    // unobserved task exception; then leaves _hedgedReads
                    // exactly the way it found this leg once it's done,
                    // whether that happens while this method is still
                    // running (removed explicitly below instead) or long
                    // after it has already returned.
                    _ = completed.Exception;
                    _hedgedReads.TryRemove(task, out _);
                },
                TaskScheduler.Default);
            return task;
        }

        var pending = new HashSet<Task<T>> { StartLeg(0) };
        int nextIndex = 1;
        Exception? lastError = null;
        bool replicaMissed = false;

        while (pending.Count > 0)
        {
            Task<T> completed;
            if (nextIndex < names.Count)
            {
                using var cts = new CancellationTokenSource();
                Task timer = Task.Delay(hedgeAfter, cts.Token);
                var waitable = new List<Task>(pending) { timer };
                Task winner = await Task.WhenAny(waitable).ConfigureAwait(false);
                if (winner == timer)
                {
                    // Hedge interval elapsed with no answer: one more owner.
                    pending.Add(StartLeg(nextIndex));
                    nextIndex++;
                    continue;
                }
                cts.Cancel();
                completed = (Task<T>)winner;
            }
            else
            {
                completed = await Task.WhenAny(pending).ConfigureAwait(false);
            }
            pending.Remove(completed);

            int index = legIndex[completed];
            T value;
            try
            {
                value = await completed.ConfigureAwait(false);
            }
            catch (WrongNodeException)
            {
                // Remaining legs are already tracked in _hedgedReads (added
                // when started, above) — nothing more to do before this
                // propagates exactly as ReadAsync's own WrongNodeException
                // does.
                throw;
            }
            catch (Exception error) when (error is NanocachedException)
            {
                lastError = error;
                if (pending.Count == 0 && nextIndex < names.Count)
                {
                    // Everything in flight so far failed: the next owner
                    // gets its turn right away, not after waiting out the
                    // rest of the interval.
                    pending.Add(StartLeg(nextIndex));
                    nextIndex++;
                }
                continue;
            }

            if (value is not null || index == 0)
            {
                return value;
            }

            // A replica's miss is provisional — it may simply lack the
            // copy — so hedging never turns a hit into a miss: only the
            // primary's own miss is accepted as final.
            replicaMissed = true;
            if (pending.Count == 0 && nextIndex < names.Count)
            {
                pending.Add(StartLeg(nextIndex));
                nextIndex++;
            }
        }

        if (replicaMissed) return default!;
        throw lastError ?? new ConnectionLostException("nanocached: no owner is reachable for this key");
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
        // design (client-side replication) — a dead or disagreeing replica leaves the key
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
            // Fire-and-forget replica writes: with FireAndForgetReplicas, up to
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

        // Run the primary and capture its outcome instead of just
        // `await`-ing it — a `finally { await Task.WhenAll(replicaWrites); }`
        // here would let an uncaught exception from a replica leg (a
        // genuine programming bug; every *expected* failure is already
        // caught inside ReplicaWriteAsync above) REPLACE the try block's
        // outcome: a successful primary write would come back as an
        // exception, or the primary's own real error would be silently
        // discarded in favor of the replica bug. Mirrors the TypeScript
        // SDK's writeToOwners (client.ts, ~767-789).
        T result = default!;
        Exception? primaryError = null;
        try
        {
            result = await ApplyReconnectingAsync(names[0], op).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            primaryError = error;
        }

        // Always drain every replica leg — for Close()'s tracking (issue
        // #12/#14) and so a genuine replica-leg bug doesn't linger
        // unobserved — but never let one override an already-successful
        // primary write: the write happened, so throwing here despite
        // that would misreport a completed write as failed. A replica bug
        // is only ever surfaced by throwing when the primary itself also
        // failed; only the first such bug is surfaced this way, same as
        // TypeScript — any further ones are just logged, same as a
        // successful primary's replica bugs always are.
        Exception? replicaBug = null;
        foreach (Task replicaWrite in replicaWrites)
        {
            try
            {
                await replicaWrite.ConfigureAwait(false);
            }
            catch (Exception outcome)
            {
                if (primaryError is not null && replicaBug is null)
                {
                    replicaBug = outcome;
                }
                else
                {
                    Console.Error.WriteLine(
                        $"nanocached: a replica write raised an unexpected error: {outcome}");
                }
            }
        }

        if (primaryError is not null)
        {
            // ExceptionDispatchInfo preserves the original stack trace —
            // without it, re-throwing an exception caught earlier and held
            // across the awaits above would show this rethrow as the
            // exception's origin instead of where it actually happened.
            ExceptionDispatchInfo.Capture(replicaBug ?? primaryError).Throw();
        }
        return result;
    }

    // ── 遅延再接続 ────────────────────────────────────────────────

    private async Task<Connection> SlotConnectionAsync(string? slot)
    {
        (string slotKey, string address, Connection? current) = SnapshotSlot(slot);
        // current is null for a member that bootstrapped without a
        // connection (issue #67) — treated exactly like an already-closed
        // one below: it just goes straight to dialing.
        if (current is not null && !current.IsClosed) return current;

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
            if (current is not null && !current.IsClosed) return current;

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
            ArmReconnectCooldown(address, error);
            throw;
        }
    }

    /// <summary>Arms the per-address reconnect cooldown (see
    /// <see cref="_reconnectCooldowns"/>) after a dial to
    /// <paramref name="address"/> failed with <paramref name="error"/> — used
    /// both by a lazy redial's own failed dial (<see cref="DialWithCooldownAsync"/>)
    /// and by <see cref="OpenClusterAsync"/> installing an unreachable
    /// member at bootstrap (issue #67), so a request that reaches it right
    /// after connect fails immediately with this same cached error instead
    /// of paying its own dial. A no-op when
    /// <see cref="Options.DisableReconnectCooldown"/> was set
    /// (<see cref="_reconnectCooldown"/> is <c>null</c>) — every request
    /// then pays its own full dial attempt instead of reusing a cached
    /// failure.</summary>
    private void ArmReconnectCooldown(string address, Exception error)
    {
        if (_reconnectCooldown is TimeSpan resolvedCooldown)
        {
            _reconnectCooldowns[address] = (DateTime.UtcNow + resolvedCooldown, error);
        }
    }

    private (string SlotKey, string Address, Connection? Connection) SnapshotSlot(string? slot)
    {
        lock (_stateLock)
        {
            if (slot is null)
            {
                return ("", _singleAddress!, _single);
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
                _members[name].Connection?.Close();
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
                // consistent with client-side replication's eventual-consistency model.
                // Counted via Stats().RefreshFailures.
            }
        }

        lock (_stateLock)
        {
            _ring = new HashRing(_members.Keys.ToList());
            _replication = cluster.Replication;
        }
    }

    /// <summary>Walks every configured address (discovery HA); <c>null</c>
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
                // with client-side replication's eventual-consistency model. The next
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

    // Fire-and-forget replica writes: bounds how many replica writes a single client
    // may have running in the background at once when
    // FireAndForgetReplicas is enabled — once the cap is reached, further
    // replica legs fall back to running synchronously, the same as with
    // the option off. Internal and mutable only so tests can shrink it,
    // mirroring KeepAliveInterval. Read once per constructor call, so
    // tests must set it before ConnectAsync().
    internal static int MaxInFlightBackgroundReplicaWrites = 32;


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
                        // A member with no connection (issue #67: still
                        // unreachable since bootstrap, or between deaths)
                        // stays lazy — nothing here to ping, redialed on
                        // its next real use.
                        : _members.Values.Select(member => member.Connection).OfType<Connection>().ToList();
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
