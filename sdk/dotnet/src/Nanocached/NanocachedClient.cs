using System.Collections.Concurrent;
using System.Globalization;
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

        /// <summary>SDK proxy mode (issue #122): route through one
        /// <c>nanocached-proxy</c> — chosen at random from the fleet
        /// discovery currently knows about, via its <c>Q</c> roster —
        /// instead of connecting directly to every owner of a key. A
        /// proxy looks exactly like a single node that owns every key
        /// (full <c>G</c>/<c>S</c>/<c>D</c>, never <c>W</c>), so once
        /// connected this client is in its ordinary single-connection
        /// mode: no ring, no per-node connections, and no hedged reads —
        /// there is nobody else to hedge to, so a configured
        /// <see cref="ReadHedgeAfter"/> is accepted but inert under
        /// <see cref="ViaProxy"/>. Namespaces, clear/clear-all, tags,
        /// keep-alive, and compression all work unchanged over the one
        /// connection.
        ///
        /// <para>Only meaningful when every configured
        /// <see cref="Addresses"/> entry is a discovery server: if the
        /// first reachable one instead identifies as a cache node — the
        /// same identify handshake every other mode uses — <see cref="ConnectAsync(Options)"/>
        /// fails fast with a clear error rather than silently pinning to
        /// that node as non-proxy single mode would. An empty proxy
        /// roster, or every listed proxy being unreachable, is the SDK's
        /// ordinary connect error.</para>
        ///
        /// <para>On reconnect, the SDK first retries the same proxy (it
        /// may simply have restarted); only when that also fails does it
        /// re-fetch the roster from discovery and fail over to another
        /// proxy chosen at random — reusing the same lazy
        /// reconnect-on-use plumbing every other mode uses, not a second
        /// mechanism. Off by default.</para></summary>
        public bool ViaProxy { get; set; }
    }

    /// <summary>
    /// Point-in-time snapshot returned by <see cref="Stats"/>: counters
    /// for failures this client swallows by design instead of surfacing
    /// to the caller — a dead replica leg on a write (client-side replication,
    /// Fire-and-forget replica writes), a failed background repair of the primary
    /// after read-repair found a value on another owner (read repair),
    /// a failed node-list refresh attempt or per-node reconnect
    /// during one, and (issue #125) every retryable-error status <c>R</c>
    /// a connection received, whether it was transparently retried away or
    /// ultimately raised <see cref="RetryableException"/>. None of these
    /// ever fail an operation by themselves — an <c>R</c> retry only fails
    /// the call after 3 straight attempts (see
    /// <see cref="RetryableException"/>) — so this is purely observability:
    /// an operator who only watches for thrown exceptions can still notice
    /// replication silently degrading, a node-list refresh stuck failing,
    /// or an upstream (e.g. behind a proxy) degrading before it ever
    /// surfaces as a hard failure.
    /// </summary>
    public readonly record struct ClientStats(
        long ReplicaWriteFailures, long ReadRepairFailures, long RefreshFailures, long TransientRetries);

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

    // issue #151 — batched get/set: bounds how many keys GetManyAsync/
    // GetManyBytesAsync/SetManyAsync/SetManyBytesAsync pack into a single
    // `m`/`o` sub-frame per owner before splitting into more than one
    // (batch chunking) — a reply header must fit every key's/value's
    // decimal length field plus separators. Same value the Go/TypeScript/
    // Python/Java SDKs use.
    private const int MaxBatchKeys = 400;

    // issue #222: MaxBatchKeys alone bounds a sub-frame's key COUNT, not
    // its byte size — ValidateKey/ValidateKeyAndValue only check one pair
    // at a time, so 400 individually-valid pairs (e.g. 400 x 5 KiB values)
    // can still sum past MaxRequestBytes once packed into one `m`/`o`
    // frame. The server enforces MAX_REQUEST_SIZE on the whole frame
    // (request_is_too_large, src/server.rs) and just closes the
    // connection with no response, turning what should be a clear
    // validation error into a confusing ConnectionLost/WrongNode. This is
    // the per-entry allowance for the `m`/`o` header's decimal length
    // field(s) for one key (`MultiGetHeaderAllowance`) — " <key-len>" — or
    // one key and one value (`MultiSetHeaderAllowance`) — " <key-len>
    // <value-len>" — used below to track a running byte total per
    // sub-frame alongside the key count. 12 bytes per length field is
    // generous headroom (leading space + up to 10 decimal digits, more
    // than int.MaxValue ever needs, plus one spare byte) — deliberately
    // not tight, since undercounting here reproduces the exact bug this
    // fixes.
    private const int MultiGetHeaderAllowance = 12;
    private const int MultiSetHeaderAllowance = 24;

    private static readonly TimeSpan NodeListStaleAfter = TimeSpan.FromSeconds(30);
    // Reserved by the SDKs so a real application key can never collide
    // with it: a GET refreshes the pinged key's server-side LRU recency,
    // which is exactly why collision would matter — an app using key
    // {0x00} would previously have had its recency silently refreshed on
    // every keep-alive tick. The leading 0x00 also keeps this out of any
    // plausible printable-string keyspace.
    private static readonly byte[] KeepaliveKey =
        new byte[] { 0x00 }.Concat(Encoding.ASCII.GetBytes("nanocached-keepalive")).ToArray();

    // issue #105 — first-class namespaces: the default namespace every
    // GetAsync/SetAsync/DeleteAsync overload without an explicit namespace
    // routes through. Never reaches the wire as an explicit zero-length
    // namespace — Connection maps this back to the legacy G/S/D frames,
    // byte-for-byte (see Connection's GetAsync(byte[], byte[]) doc
    // comment) — so every pre-#105 call site keeps working unchanged.
    private static readonly byte[] EmptyNamespace = Array.Empty<byte>();

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
    // SDK proxy mode (issue #122): true routes _single through
    // DialProxyWithFailoverAsync instead of the plain DialWithCooldownAsync
    // every other single-connection client uses — see that method's doc
    // comment for the retry-then-refetch reconnect flow this gates.
    private readonly bool _viaProxy;
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
    // _backgroundReplicaPermits' pool is. Bounded at
    // MaxInFlightHedgeLoserLegs (issue #276): past that many concurrently
    // detached legs, ReadHedgedAsync awaits its own remaining losers
    // synchronously instead of leaving them detached here.
    private readonly ConcurrentDictionary<Task, byte> _hedgedReads = new();
    // Serializes a hedge leg's "check _closed, then register" (StartLeg)
    // against Close()'s "observe the set empty, then stop draining" (issue
    // #91). Without it a leg could be registered — and dialed against a
    // connection Teardown() is closing — after the drain had already found
    // the set empty. Held only briefly on both sides (never across a leg's
    // own await), so it doesn't serialize the reads themselves.
    private readonly object _hedgedReadsLock = new();
    private readonly CancellationTokenSource _lifetime = new();

    // Observability for failures this client swallows by design — see
    // Stats(). Interlocked because they're incremented from whichever
    // thread happens to hit the swallow site (foreground calls,
    // background replica writes, the keep-alive loop's redials).
    private long _replicaWriteFailures;
    private long _readRepairFailures;
    private long _refreshFailures;
    // issue #125 — retryable-error status R: every R any connection this
    // client owns has received, whether transparently retried away or
    // ultimately surfaced as RetryableException — see NewConnection's
    // onTransientRetry callback and ClientStats.TransientRetries.
    private long _transientRetries;

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
        _viaProxy = options.ViaProxy;
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
                    .ConnectAndIdentifyAsync(host, port, client._authSecret, client._tls, options.ViaProxy)
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
                    // SDK proxy mode (issue #122): a configured address
                    // that identifies as a plain cache node is a
                    // configuration error, not a target to pin to the way
                    // non-proxy mode's NodeTarget case below does — ViaProxy
                    // only ever makes sense against discovery addresses.
                    // Fatal (not "try the next address"): every other
                    // configured address is presumably the same kind of
                    // mistake.
                    case Identify.NodeTarget node when options.ViaProxy:
                        node.Stream.Dispose();
                        throw new NanocachedException(
                            $"nanocached: ViaProxy requires discovery addresses, but {host}:{port} "
                            + "identifies as a cache node");

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

                    // SDK proxy mode: an empty roster is exactly like an
                    // empty node list above — try the next discovery
                    // seed, another discovery replica may know of proxies
                    // this one hasn't heard announced yet.
                    case Identify.ProxyListTarget proxies when proxies.Proxies.Count == 0:
                        lastError = new NanocachedException(
                            $"nanocached: no proxies registered with discovery at {host}:{port}");
                        continue;

                    case Identify.ProxyListTarget proxies:
                        client._targetKey = key;
                        await client.OpenProxyAsync(proxies.Proxies).ConfigureAwait(false);
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
        return new Connection(
            stream, tagged, () => DecrementOpenTarget(key), () => Interlocked.Increment(ref _transientRetries));
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

    /// <summary>SDK proxy mode (issue #122): connects to exactly one of
    /// <paramref name="proxies"/>, picked at random (spreads a fleet of
    /// clients over the proxy fleet), failing over through the rest in
    /// random order on a dial failure. Leaves the client in its ordinary
    /// single-connection mode (<see cref="_ring"/> stays <c>null</c>) —
    /// a proxy owns every key, so there is no ring to build. Throws the
    /// last dial error when every proxy in the roster is unreachable,
    /// exactly like <see cref="OpenClusterAsync"/> does when every node
    /// is.</summary>
    private async Task OpenProxyAsync(IReadOnlyList<DiscoveredNode> proxies)
    {
        (Connection connection, string address) =
            await ConnectToAnyProxyAsync(proxies).ConfigureAwait(false);
        _single = connection;
        _singleAddress = address;
    }

    /// <summary>Shared by the initial SDK proxy mode connect
    /// (<see cref="OpenProxyAsync"/>) and reconnect failover
    /// (<see cref="DialProxyWithFailoverAsync"/>) — reusing one
    /// dial-and-pick routine rather than building it twice. Dials every
    /// proxy in <paramref name="proxies"/>, in a fresh random order each
    /// call (<see cref="ShuffleProxies"/>), and returns the first that
    /// connects — the same <see cref="OpenNodeConnectionAsync"/> a
    /// cluster node dial uses, since a proxy identifies exactly like one.
    /// Throws the last dial error when none connect.</summary>
    private async Task<(Connection Connection, string Address)> ConnectToAnyProxyAsync(
        IReadOnlyList<DiscoveredNode> proxies)
    {
        Exception? lastError = null;
        foreach (DiscoveredNode proxy in ShuffleProxies(proxies))
        {
            try
            {
                Connection connection = await OpenNodeConnectionAsync(proxy.Address).ConfigureAwait(false);
                return (connection, proxy.Address);
            }
            catch (Exception error) when (error is NanocachedException or IOException
                or System.Net.Sockets.SocketException)
            {
                lastError = error;
            }
        }
        throw lastError ?? new ConnectionLostException("nanocached: no proxy is reachable");
    }

    /// <summary>Fisher-Yates over a copy of <paramref name="proxies"/> —
    /// never mutates the roster the caller passed in.
    /// <see cref="Random.Shared"/> is thread-safe, so this needs no
    /// synchronization of its own even though several clients (or several
    /// concurrent redials on this one client) may shuffle at once.</summary>
    private static List<DiscoveredNode> ShuffleProxies(IReadOnlyList<DiscoveredNode> proxies)
    {
        var shuffled = proxies.ToList();
        for (int i = shuffled.Count - 1; i > 0; i--)
        {
            int j = Random.Shared.Next(i + 1);
            (shuffled[i], shuffled[j]) = (shuffled[j], shuffled[i]);
        }
        return shuffled;
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
        Interlocked.Read(ref _refreshFailures),
        Interlocked.Read(ref _transientRetries));

    /// <summary>
    /// issue #105 — first-class namespaces: a lightweight handle scoping
    /// every get/set/delete to <paramref name="namespaceBytes"/> — the same
    /// key name in two namespaces is two independent entries (namespaces).
    /// Cheap: shares this client's connections, and nothing is dialed or
    /// allocated beyond the small wrapper itself. Forwards every call to
    /// this client's own internal (namespace, key) methods rather than
    /// duplicating any networking, so routing (HRW over (ns, key)),
    /// replication fan-out, hedged reads, <c>W</c> refresh-and-retry,
    /// response tags, and compression all behave exactly as calling this
    /// client directly does.
    ///
    /// <para><see cref="Namespace(string)"/> called with <c>""</c> (the
    /// empty namespace) returns a handle equivalent to this client itself
    /// — it sends the legacy <c>G</c>/<c>S</c>/<c>D</c> frames,
    /// byte-for-byte, and hashes exactly as an un-namespaced key always
    /// has. The handle is invalid once this client is closed: every method
    /// then throws the same <see cref="AlreadyClosedException"/> this
    /// client's own methods raise.</para>
    /// </summary>
    public NanocachedNamespace Namespace(byte[] namespaceBytes) => new(this, namespaceBytes);

    /// <summary>As <see cref="Namespace(byte[])"/>, with
    /// <paramref name="namespaceName"/> UTF-8 encoded.</summary>
    public NanocachedNamespace Namespace(string namespaceName) => Namespace(Encoding.UTF8.GetBytes(namespaceName));

    // Strict — never silently replaces a malformed byte with U+FFFD; a
    // non-UTF-8 value raises DecoderFallbackException instead.
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public Task<string?> GetAsync(string key) => GetAsync(EmptyNamespace, key);

    /// <summary>Returns the value decoded as UTF-8, or <c>null</c> when
    /// the key is missing. A value that is not valid UTF-8 raises
    /// <see cref="System.Text.DecoderFallbackException"/> — use
    /// <see cref="GetBytesAsync(byte[])"/> for the raw bytes instead.</summary>
    public Task<string?> GetAsync(byte[] key) => GetAsync(EmptyNamespace, key);

    /// <summary>issue #105: as <see cref="GetAsync(string)"/>, scoped to
    /// <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to, rather than
    /// duplicating this client's networking.</summary>
    internal Task<string?> GetAsync(byte[] namespaceBytes, string key) =>
        GetAsync(namespaceBytes, Encoding.UTF8.GetBytes(key));

    /// <summary>issue #105: as <see cref="GetAsync(byte[])"/>, scoped to
    /// <paramref name="namespaceBytes"/>.</summary>
    internal async Task<string?> GetAsync(byte[] namespaceBytes, byte[] key)
    {
        byte[]? value = await GetBytesAsync(namespaceBytes, key).ConfigureAwait(false);
        return value is null ? null : StrictUtf8.GetString(value);
    }

    public Task<byte[]?> GetBytesAsync(string key) => GetBytesAsync(EmptyNamespace, key);

    /// <summary>Returns the raw value, or <c>null</c> when the key is
    /// missing. Transparently decompresses when <c>Compress</c> is
    /// enabled (value compression). With <c>ReadRepair</c>, a clean miss
    /// probes the remaining owners before being accepted as final
    /// (read repair).</summary>
    public Task<byte[]?> GetBytesAsync(byte[] key) => GetBytesAsync(EmptyNamespace, key);

    /// <summary>issue #105: as <see cref="GetBytesAsync(string)"/>, scoped
    /// to <paramref name="namespaceBytes"/>.</summary>
    internal Task<byte[]?> GetBytesAsync(byte[] namespaceBytes, string key) =>
        GetBytesAsync(namespaceBytes, Encoding.UTF8.GetBytes(key));

    /// <summary>issue #105: as <see cref="GetBytesAsync(byte[])"/>, scoped
    /// to <paramref name="namespaceBytes"/> — this is the internal method
    /// <see cref="NanocachedNamespace"/> forwards to, rather than
    /// duplicating this client's networking.</summary>
    internal async Task<byte[]?> GetBytesAsync(byte[] namespaceBytes, byte[] key)
    {
        byte[]? value = await GetRawBytesAsync(namespaceBytes, key).ConfigureAwait(false);
        return value is null || !_compress ? value : Compression.DecompressValue(value);
    }

    /// <summary>The shared read path behind <see cref="GetBytesAsync(byte[], byte[])"/>
    /// and <see cref="GetWithTokenAsync(byte[], byte[])"/> (issue #141): the
    /// value's EXACT WIRE BYTES, before this client's own decompression
    /// step — a compression-enabled client never decompresses server-side,
    /// so these are the same bytes a <c>k</c>/<c>x</c>'s condition is
    /// evaluated against, and hashing anything else would silently
    /// produce a digest the server can never match.</summary>
    private async Task<byte[]?> GetRawBytesAsync(byte[] namespaceBytes, byte[] key)
    {
        ValidateKey(namespaceBytes, key);
        await BeforeOperationAsync().ConfigureAwait(false);
        byte[]? value = await WithClusterRetryAsync(
            () => ReadAsync(namespaceBytes, key, connection => connection.GetAsync(namespaceBytes, key)))
            .ConfigureAwait(false);
        if (value is null && _readRepair && _ring is not null)
        {
            value = await TryReadRepairAsync(namespaceBytes, key).ConfigureAwait(false);
        }
        return value;
    }

    public Task<(string Value, string Token)?> GetWithTokenAsync(string key) =>
        GetWithTokenAsync(EmptyNamespace, key);

    /// <summary>issue #141 — compare-and-set: as <see cref="GetAsync(string)"/>,
    /// but also returns a content digest ("CAS token") for the value, for
    /// use with <see cref="ReplaceAsync(string, string, string, long)"/> or
    /// <see cref="DeleteIfMatchesAsync(string, string)"/> — the same
    /// not-found convention (<c>null</c>) as a plain <c>GetAsync</c> miss.
    /// The digest is computed from the exact wire bytes (see
    /// <see cref="ContentDigest"/>'s doc comment), so it is always the one
    /// the server itself would compute, even with <c>Compress</c>
    /// enabled.</summary>
    public Task<(string Value, string Token)?> GetWithTokenAsync(byte[] key) =>
        GetWithTokenAsync(EmptyNamespace, key);

    /// <summary>issue #141: as <see cref="GetWithTokenAsync(string)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<(string Value, string Token)?> GetWithTokenAsync(byte[] namespaceBytes, string key) =>
        GetWithTokenAsync(namespaceBytes, Encoding.UTF8.GetBytes(key));

    /// <summary>issue #141: as <see cref="GetWithTokenAsync(byte[])"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal async Task<(string Value, string Token)?> GetWithTokenAsync(byte[] namespaceBytes, byte[] key)
    {
        byte[]? raw = await GetRawBytesAsync(namespaceBytes, key).ConfigureAwait(false);
        if (raw is null) return null;
        string token = ContentDigest(raw);
        byte[] value = _compress ? Compression.DecompressValue(raw) : raw;
        return (StrictUtf8.GetString(value), token);
    }

    public Task<(byte[] Value, string Token)?> GetBytesWithTokenAsync(string key) =>
        GetBytesWithTokenAsync(EmptyNamespace, key);

    /// <summary>issue #141: as <see cref="GetWithTokenAsync(byte[])"/>, but
    /// returns the raw (decompressed) value instead of decoding it as
    /// UTF-8 — the <see cref="GetBytesAsync(byte[])"/> counterpart to
    /// <see cref="GetWithTokenAsync(byte[])"/>.</summary>
    public Task<(byte[] Value, string Token)?> GetBytesWithTokenAsync(byte[] key) =>
        GetBytesWithTokenAsync(EmptyNamespace, key);

    /// <summary>issue #141: as <see cref="GetBytesWithTokenAsync(string)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<(byte[] Value, string Token)?> GetBytesWithTokenAsync(byte[] namespaceBytes, string key) =>
        GetBytesWithTokenAsync(namespaceBytes, Encoding.UTF8.GetBytes(key));

    /// <summary>issue #141: as <see cref="GetBytesWithTokenAsync(byte[])"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal async Task<(byte[] Value, string Token)?> GetBytesWithTokenAsync(byte[] namespaceBytes, byte[] key)
    {
        byte[]? raw = await GetRawBytesAsync(namespaceBytes, key).ConfigureAwait(false);
        if (raw is null) return null;
        string token = ContentDigest(raw);
        byte[] value = _compress ? Compression.DecompressValue(raw) : raw;
        return (value, token);
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
    private async Task<byte[]?> TryReadRepairAsync(byte[] namespaceBytes, byte[] key)
    {
        IReadOnlyList<string> names = OwnerNames(namespaceBytes, key);
        if (names.Count == 0) return null;
        foreach (string name in names.Skip(1))
        {
            byte[]? value;
            try
            {
                value = await ApplyReconnectingAsync(name, connection => connection.GetAsync(namespaceBytes, key))
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
                            await connection.SetAsync(namespaceBytes, key, repairValue, ReadRepairTtlSeconds)
                                .ConfigureAwait(false);
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

    // ── batched get/set (issue #151) ─────────────────────────────────
    // m/o — see docs/protocol.html#multi. Every requested key's owner is
    // still resolved via HashRing/OwnerNames, exactly like a single
    // GetBytesAsync/SetAsync: GetManyBytesAsync groups keys by primary
    // owner and issues one `m` sub-frame per owner (batch chunking splits
    // an over-MaxBatchKeys or over-MaxRequestBytes group further, issue
    // #222); SetManyBytesAsync
    // groups by every owner across every rank, since one batch's keys
    // can place the same node as primary for one key and a replica for
    // another. A batch never fails as a whole (docs/protocol.html#multi):
    // GetManyBytesAsync returns every key that resolved, throwing
    // PartialWrongNodeException<T> (carrying that partial dictionary)
    // only if some keys are still wrong-node after one bounded
    // refresh-and-retry — the same policy GetBytesAsync's own
    // WithClusterRetryAsync applies, generalized to a per-key roster
    // instead of an all-or-nothing retry. SetManyBytesAsync has nothing
    // to return on success, so it just throws a plain WrongNodeException
    // on the same condition.

    /// <summary>As <see cref="GetManyBytesAsync(IReadOnlyList{string})"/>,
    /// decoding every hit as UTF-8.</summary>
    public Task<Dictionary<string, string>> GetManyAsync(IReadOnlyList<string> keys) =>
        GetManyAsync(EmptyNamespace, keys);

    /// <summary>issue #105: as <see cref="GetManyAsync(IReadOnlyList{string})"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal async Task<Dictionary<string, string>> GetManyAsync(byte[] namespaceBytes, IReadOnlyList<string> keys)
    {
        try
        {
            return DecodeMany(await GetManyBytesAsync(namespaceBytes, keys).ConfigureAwait(false));
        }
        catch (PartialWrongNodeException<Dictionary<string, byte[]>> partial)
        {
            throw new PartialWrongNodeException<Dictionary<string, string>>(DecodeMany(partial.PartialValues));
        }
        catch (PartialConnectionLostException<Dictionary<string, byte[]>> partial)
        {
            throw new PartialConnectionLostException<Dictionary<string, string>>(
                DecodeMany(partial.PartialValues), partial.InnerException!);
        }
    }

    private static Dictionary<string, string> DecodeMany(Dictionary<string, byte[]> raw)
    {
        var values = new Dictionary<string, string>(raw.Count);
        foreach ((string key, byte[] value) in raw)
        {
            values[key] = StrictUtf8.GetString(value);
        }
        return values;
    }

    /// <summary>Returns every requested key's raw value in one round trip
    /// per owner (batched get, docs/protocol.html#multi) — a missing key
    /// is simply absent from the returned dictionary, never an error, the
    /// same "a miss is not an error" contract <see cref="GetBytesAsync(byte[])"/>
    /// itself has. <paramref name="keys"/> must be non-empty.
    ///
    /// <para>A batch never fails as a whole: if some keys are still
    /// wrong-node after one bounded refresh-and-retry, throws
    /// <see cref="PartialWrongNodeException{T}"/> whose
    /// <c>PartialValues</c> holds every key that DID resolve, rather than
    /// discarding a mostly-successful batch over a handful of stale
    /// placements. In single-node/proxy mode a <c>W</c> propagates
    /// immediately, exactly as <see cref="GetBytesAsync(byte[])"/>'s own
    /// single-mode behavior does — there is no ring to refresh
    /// against.</para>
    ///
    /// <para>Larger batches are transparently split into more than one
    /// <c>m</c> sub-frame per owner (batch chunking, see
    /// <see cref="MaxBatchKeys"/> and, issue #222,
    /// <see cref="MaxRequestBytes"/>) — callers never need to think about
    /// this.</para></summary>
    public Task<Dictionary<string, byte[]>> GetManyBytesAsync(IReadOnlyList<string> keys) =>
        GetManyBytesAsync(EmptyNamespace, keys);

    /// <summary>issue #105: as <see cref="GetManyBytesAsync(IReadOnlyList{string})"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal async Task<Dictionary<string, byte[]>> GetManyBytesAsync(byte[] namespaceBytes, IReadOnlyList<string> keys)
    {
        if (keys.Count == 0)
        {
            throw new ArgumentException(
                "nanocached: GetManyAsync/GetManyBytesAsync requires at least one key", nameof(keys));
        }
        var keyBytes = new byte[keys.Count][];
        for (int i = 0; i < keys.Count; i++)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(keys[i]);
            ValidateKey(namespaceBytes, bytes);
            keyBytes[i] = bytes;
        }
        await BeforeOperationAsync().ConfigureAwait(false);

        var values = new Dictionary<string, byte[]>(keys.Count);
        // Cumulative decompressed bytes across this whole response — see
        // DecompressForBatch. A shared one-element holder so the bound
        // spans both cluster passes, not one pass.
        long[] budget = { 0 };

        if (_ring is null)
        {
            List<Connection.MultiEntry> entries;
            try
            {
                entries = await MultiGetChunkedAsync(null, namespaceBytes, keyBytes).ConfigureAwait(false);
            }
            catch (ChunkedBatchInterruptedException partial)
            {
                // issue #411: a later chunk's connection failure survived
                // the built-in reconnect — don't discard the earlier
                // chunks' already-decoded hits, carry them instead.
                var partialValues = new Dictionary<string, byte[]>(partial.CompletedEntries.Count);
                for (int i = 0; i < partial.CompletedEntries.Count; i++)
                {
                    Connection.MultiEntry entry = partial.CompletedEntries[i];
                    if (entry.Ok) partialValues[keys[i]] = DecompressForBatch(entry.Value!, budget);
                }
                throw new PartialConnectionLostException<Dictionary<string, byte[]>>(
                    partialValues, partial.InnerException!);
            }
            bool wrongNode = false;
            for (int i = 0; i < entries.Count; i++)
            {
                Connection.MultiEntry entry = entries[i];
                if (entry.Ok)
                {
                    values[keys[i]] = DecompressForBatch(entry.Value!, budget);
                }
                else if (entry.WrongNode)
                {
                    wrongNode = true;
                }
            }
            if (wrongNode) throw new PartialWrongNodeException<Dictionary<string, byte[]>>(values);
            return values;
        }

        List<int>? retry = await MultiGetPassAsync(namespaceBytes, keys, keyBytes, values, null, budget).ConfigureAwait(false);
        if (retry.Count == 0) return values;
        await MaybeRefreshAsync(force: true).ConfigureAwait(false);
        retry = await MultiGetPassAsync(namespaceBytes, keys, keyBytes, values, retry, budget).ConfigureAwait(false);
        if (retry.Count > 0) throw new PartialWrongNodeException<Dictionary<string, byte[]>>(values);
        return values;
    }

    /// <summary><c>Compress</c>'s decompression step (see
    /// <see cref="GetBytesAsync(byte[], byte[])"/>), generalized so
    /// <see cref="GetManyBytesAsync(byte[], IReadOnlyList{string})"/>'s
    /// per-entry splicing can share it: a no-op when <c>Compress</c> is
    /// off.</summary>
    private byte[] MaybeDecompress(byte[] value) => _compress ? Compression.DecompressValue(value) : value;

    /// <summary>Decompresses one hit value for a <c>GetMany</c> batch and
    /// charges its decompressed size against the response's cumulative
    /// budget (issue #386). <see cref="Compression.DecompressValue"/>
    /// already caps a single value; this bounds the whole response so a
    /// batch of highly compressible values can't amplify that per-value
    /// cap into gigabytes of allocation. Locked on the budget holder
    /// because <see cref="RunMultiGetLegAsync"/> runs one leg per owner
    /// concurrently: once the budget is crossed, remaining entries (in
    /// this and other legs) fail before decompressing rather than each
    /// allocating another value first — peak allocation is therefore the
    /// budget plus one value per concurrent leg.</summary>
    private byte[] DecompressForBatch(byte[] raw, long[] budget)
    {
        lock (budget)
        {
            if (budget[0] > Compression.MaxMultiGetDecompressedBytes)
            {
                throw new DecompressionException(
                    "nanocached: cumulative decompressed size of this GetMany response " +
                    "exceeds the maximum — possible decompression bomb across the batch");
            }
            byte[] value = MaybeDecompress(raw);
            budget[0] += value.Length;
            return value;
        }
    }

    /// <summary>
    /// issue #411 — internal signal only, never seen outside this file.
    /// Thrown by <see cref="MultiGetChunkedAsync"/>/
    /// <see cref="MultiSetChunkedAsync"/> when a chunk's connection failure
    /// (surviving <see cref="ApplyReconnectingAsync{T}"/>'s own built-in
    /// redial-and-retry) interrupts a chunked call after at least one
    /// earlier chunk of THAT call already completed. <see cref="CompletedEntries"/>
    /// holds those chunks' entries, in request order, corresponding 1:1 to
    /// the prefix of whatever key/value arrays the failing
    /// <see cref="MultiGetChunkedAsync"/>/<see cref="MultiSetChunkedAsync"/>
    /// call was given — <see cref="GetManyBytesAsync(byte[], IReadOnlyList{string})"/>/
    /// <see cref="SetManyBytesAsync(byte[], IReadOnlyDictionary{string, byte[]}, long)"/>'s
    /// single-mode branch and <see cref="RunMultiSetLegAsync"/> (cluster
    /// mode) each translate this into whatever "partial success" means for
    /// their own public contract (a <see cref="PartialConnectionLostException{T}"/>
    /// for the single-mode callers; per-key retry/replica-failure
    /// bookkeeping for the cluster leg runner) instead of losing it. Never
    /// thrown when the very first chunk is the one that fails — there is
    /// nothing to attach yet, so a bare connection-layer exception
    /// propagates unchanged in that case, exactly as before this fix.
    /// </summary>
    private sealed class ChunkedBatchInterruptedException : Exception
    {
        public IReadOnlyList<Connection.MultiEntry> CompletedEntries { get; }

        public ChunkedBatchInterruptedException(IReadOnlyList<Connection.MultiEntry> completedEntries, Exception inner)
            : base(inner.Message, inner)
        {
            CompletedEntries = completedEntries;
        }
    }

    /// <summary>Issues one or more <c>m</c> sub-frames against
    /// <paramref name="slot"/>'s connection (<c>null</c> for the
    /// single/proxy target) — already grouped to one owner by the caller
    /// — splitting so a sub-frame never exceeds <see cref="MaxBatchKeys"/>
    /// keys OR (issue #222) <see cref="MaxRequestBytes"/> of cumulative
    /// namespace+key wire bytes (batch chunking), whichever comes first.
    /// A single key always fits on its own — <see cref="ValidateKey"/>
    /// already guarantees that — so the byte bound only ever closes a
    /// chunk early, never drops an entry.
    ///
    /// <para>issue #411: when a chunk after the first one fails at the
    /// connection level (surviving <see cref="ApplyReconnectingAsync{T}"/>'s
    /// own redial-and-retry), throws <see cref="ChunkedBatchInterruptedException"/>
    /// carrying every earlier chunk's already-completed entries instead of
    /// discarding them — see that type's doc comment. A first-chunk
    /// failure has nothing to attach yet, so it propagates as the bare
    /// underlying exception, unchanged.</para></summary>
    private async Task<List<Connection.MultiEntry>> MultiGetChunkedAsync(
        string? slot, byte[] namespaceBytes, byte[][] keys)
    {
        var entries = new List<Connection.MultiEntry>(new Connection.MultiEntry[keys.Length]);
        int start = 0;
        while (start < keys.Length)
        {
            long total = namespaceBytes.Length + MultiGetHeaderAllowance + keys[start].Length;
            int end = start + 1;
            while (end < keys.Length && end - start < MaxBatchKeys)
            {
                long next = MultiGetHeaderAllowance + keys[end].Length;
                if (total + next > MaxRequestBytes) break;
                total += next;
                end++;
            }
            ArraySegment<byte[]> chunk = new(keys, start, end - start);
            List<Connection.MultiEntry> chunkEntries;
            try
            {
                chunkEntries = await ApplyReconnectingAsync(
                    slot, connection => connection.MultiGetAsync(namespaceBytes, chunk)).ConfigureAwait(false);
            }
            catch (Exception error) when (start > 0 && (error is NanocachedException or IOException
                or System.Net.Sockets.SocketException or ObjectDisposedException))
            {
                throw new ChunkedBatchInterruptedException(entries.GetRange(0, start), error);
            }
            for (int i = start; i < end; i++)
            {
                entries[i] = chunkEntries[i - start];
            }
            start = end;
        }
        return entries;
    }

    /// <summary>One pass of <see cref="GetManyBytesAsync(byte[], IReadOnlyList{string})"/>'s
    /// cluster routing: group the given indices (every key, when
    /// <paramref name="retryIndices"/> is <c>null</c> — the initial pass —
    /// or just the keys a previous pass left unresolved) by their current
    /// primary owner (matching plain <c>Get</c>'s own primary-first
    /// stance), dispatch one (possibly chunked) <c>m</c> exchange per
    /// owner concurrently, splice hits into <paramref name="values"/>,
    /// and return the indices still unresolved: a per-key <c>W</c>, or a
    /// whole owner group whose call failed outright. Called once for the
    /// initial pass and once more, if needed, after a single forced
    /// refresh.</summary>
    private async Task<List<int>> MultiGetPassAsync(
        byte[] namespaceBytes, IReadOnlyList<string> keys, byte[][] keyBytes,
        Dictionary<string, byte[]> values, List<int>? retryIndices, long[] budget)
    {
        List<int> indices = retryIndices ?? Enumerable.Range(0, keys.Count).ToList();

        var groups = new Dictionary<string, List<int>>();
        var retry = new List<int>();
        foreach (int idx in indices)
        {
            IReadOnlyList<string> owners = OwnerNames(namespaceBytes, keyBytes[idx]);
            if (owners.Count == 0)
            {
                retry.Add(idx);
                continue;
            }
            if (!groups.TryGetValue(owners[0], out List<int>? group))
            {
                group = new List<int>();
                groups[owners[0]] = group;
            }
            group.Add(idx);
        }

        var legs = new List<Task<List<int>>>(groups.Count);
        foreach ((string owner, List<int> groupIndices) in groups)
        {
            legs.Add(RunMultiGetLegAsync(namespaceBytes, owner, groupIndices, keys, keyBytes, values, budget));
        }
        foreach (Task<List<int>> leg in legs)
        {
            retry.AddRange(await leg.ConfigureAwait(false));
        }
        return retry;
    }

    /// <summary>One owner group's <c>m</c> exchange: a connection-level
    /// failure retries the whole group (indistinguishable from a
    /// possibly-idle-closed connection, same stance
    /// <see cref="ApplyReconnectingAsync{T}"/>'s own callers take
    /// elsewhere); a per-key <c>W</c> retries just that key; a hit is
    /// spliced into <paramref name="values"/> (a client-side <c>Compress</c>
    /// mismatch propagates, aborting the batch immediately — never fed
    /// into the retry pass, since it isn't a routing outcome).</summary>
    private async Task<List<int>> RunMultiGetLegAsync(
        byte[] namespaceBytes, string owner, List<int> groupIndices,
        IReadOnlyList<string> keys, byte[][] keyBytes, Dictionary<string, byte[]> values,
        long[] budget)
    {
        var groupKeys = new byte[groupIndices.Count][];
        for (int i = 0; i < groupIndices.Count; i++)
        {
            groupKeys[i] = keyBytes[groupIndices[i]];
        }

        List<Connection.MultiEntry> entries;
        try
        {
            entries = await MultiGetChunkedAsync(owner, namespaceBytes, groupKeys).ConfigureAwait(false);
        }
        catch (Exception error) when (error is NanocachedException or ChunkedBatchInterruptedException)
        {
            // issue #411: MultiGetChunkedAsync now throws
            // ChunkedBatchInterruptedException (not a NanocachedException)
            // instead of a bare connection exception when a chunk after
            // the first one fails mid-leg — caught here alongside it so it
            // can't escape uncaught. Reusing its partial entries for a
            // per-key retry/success split (like the multi-set leg runner
            // now does) isn't done here — out of this issue's confirmed
            // scope for the get side — so this whole leg is retried in
            // full, exactly as any other leg-level connection failure
            // already was before this fix; a retried key that already
            // resolved just resolves again.
            return new List<int>(groupIndices);
        }

        var retry = new List<int>();
        for (int i = 0; i < groupIndices.Count; i++)
        {
            int idx = groupIndices[i];
            Connection.MultiEntry entry = entries[i];
            if (entry.WrongNode)
            {
                retry.Add(idx);
            }
            else if (entry.Ok)
            {
                // Under lock: multiple owner legs run concurrently and
                // Dictionary isn't thread-safe for concurrent writers.
                lock (values)
                {
                    values[keys[idx]] = DecompressForBatch(entry.Value!, budget);
                }
            }
        }
        return retry;
    }

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no expiry.</summary>
    public Task SetAsync(string key, string value, long ttlSeconds = 0) =>
        SetAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no
    /// expiry. Transparently compresses values at or above
    /// <c>CompressionThreshold</c> when <c>Compress</c> is enabled
    /// (value compression).</summary>
    public Task SetAsync(byte[] key, byte[] value, long ttlSeconds = 0) =>
        SetAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary>issue #105: as <see cref="SetAsync(string, string, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task SetAsync(byte[] namespaceBytes, string key, string value, long ttlSeconds = 0) =>
        SetAsync(namespaceBytes, Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ttlSeconds);

    /// <summary>issue #105: as <see cref="SetAsync(byte[], byte[], long)"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal async Task SetAsync(byte[] namespaceBytes, byte[] key, byte[] value, long ttlSeconds = 0)
    {
        if (ttlSeconds < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ttlSeconds), $"nanocached: ttlSeconds must be non-negative, got {ttlSeconds}");
        }
        ValidateKeyAndValue(namespaceBytes, key, value);
        byte[] outgoing = _compress ? Compression.CompressValue(value, _compressionThreshold) : value;
        await BeforeOperationAsync().ConfigureAwait(false);
        await WithClusterRetryAsync<object?>(async () =>
        {
            await WriteAsync<object?>(namespaceBytes, key, async connection =>
            {
                await connection.SetAsync(namespaceBytes, key, outgoing, ttlSeconds).ConfigureAwait(false);
                return null;
            }).ConfigureAwait(false);
            return null;
        }).ConfigureAwait(false);
    }

    public Task<bool> DeleteAsync(string key) => DeleteAsync(EmptyNamespace, key);

    /// <summary>Returns whether the key existed before this call.</summary>
    public Task<bool> DeleteAsync(byte[] key) => DeleteAsync(EmptyNamespace, key);

    /// <summary>issue #105: as <see cref="DeleteAsync(string)"/>, scoped to
    /// <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<bool> DeleteAsync(byte[] namespaceBytes, string key) =>
        DeleteAsync(namespaceBytes, Encoding.UTF8.GetBytes(key));

    /// <summary>issue #105: as <see cref="DeleteAsync(byte[])"/>, scoped to
    /// <paramref name="namespaceBytes"/>. Returns whether the key existed
    /// before this call.</summary>
    internal async Task<bool> DeleteAsync(byte[] namespaceBytes, byte[] key)
    {
        ValidateKey(namespaceBytes, key);
        await BeforeOperationAsync().ConfigureAwait(false);
        return await WithClusterRetryAsync(
            () => WriteAsync(namespaceBytes, key, connection => connection.DeleteAsync(namespaceBytes, key)))
            .ConfigureAwait(false);
    }

    public Task SetManyAsync(IReadOnlyDictionary<string, string> values, long ttlSeconds = 0) =>
        SetManyAsync(EmptyNamespace, values, ttlSeconds);

    /// <summary>issue #105: as <see cref="SetManyAsync(IReadOnlyDictionary{string, string}, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task SetManyAsync(byte[] namespaceBytes, IReadOnlyDictionary<string, string> values, long ttlSeconds = 0)
    {
        var raw = new Dictionary<string, byte[]>(values.Count);
        foreach ((string key, string value) in values)
        {
            raw[key] = Encoding.UTF8.GetBytes(value);
        }
        return SetManyBytesAsync(namespaceBytes, raw, ttlSeconds);
    }

    /// <summary>Stores every raw value in <paramref name="values"/> in one
    /// round trip per involved node (batched set,
    /// docs/protocol.html#multi). <paramref name="ttlSeconds"/> of 0 (the
    /// default) means no expiry, shared by the whole batch — not per key,
    /// since every real caller of a batched set (Django's
    /// <c>set_many</c>, cache-manager's <c>mset</c>) already passes one
    /// TTL per call. <paramref name="values"/> must be non-empty.
    /// Transparently compresses values at or above
    /// <c>CompressionThreshold</c> when <c>Compress</c> is enabled,
    /// exactly like <see cref="SetAsync(byte[], byte[], long)"/>.
    ///
    /// <para>Within one batch, the same node can be a key's primary and
    /// another key's replica at once — it receives exactly one <c>o</c>
    /// sub-frame either way, and only its answer for the keys it is
    /// primary for decides that key's outcome; a replica-held key's
    /// failure or <c>W</c> is logged-and-swallowed into
    /// <c>Stats().ReplicaWriteFailures</c>, exactly like
    /// <see cref="SetAsync(byte[], byte[], long)"/>'s own replica legs
    /// (<see cref="WriteAsync{T}"/>). A batch never fails as a whole: if
    /// some keys' primaries are still wrong-node after one bounded
    /// refresh-and-retry, this throws <see cref="WrongNodeException"/> —
    /// every other key in the batch was still stored. In
    /// single-node/proxy mode a <c>W</c> propagates immediately, exactly
    /// as <see cref="SetAsync(byte[], byte[], long)"/>'s own single-mode
    /// behavior does.</para>
    ///
    /// <para>Larger batches are transparently split into more than one
    /// <c>o</c> sub-frame per node (batch chunking, see
    /// <see cref="MaxBatchKeys"/> and, issue #222,
    /// <see cref="MaxRequestBytes"/>).</para></summary>
    public Task SetManyBytesAsync(IReadOnlyDictionary<string, byte[]> values, long ttlSeconds = 0) =>
        SetManyBytesAsync(EmptyNamespace, values, ttlSeconds);

    /// <summary>issue #105: as <see cref="SetManyBytesAsync(IReadOnlyDictionary{string, byte[]}, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal async Task SetManyBytesAsync(
        byte[] namespaceBytes, IReadOnlyDictionary<string, byte[]> values, long ttlSeconds = 0)
    {
        if (values.Count == 0)
        {
            throw new ArgumentException(
                "nanocached: SetManyAsync/SetManyBytesAsync requires at least one key", nameof(values));
        }
        if (ttlSeconds < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ttlSeconds), $"nanocached: ttlSeconds must be non-negative, got {ttlSeconds}");
        }

        var keys = new List<string>(values.Count);
        var keyBytes = new byte[values.Count][];
        var valueBytes = new byte[values.Count][];
        int i = 0;
        foreach ((string key, byte[] original) in values)
        {
            byte[] kb = Encoding.UTF8.GetBytes(key);
            ValidateKeyAndValue(namespaceBytes, kb, original);
            keys.Add(key);
            keyBytes[i] = kb;
            valueBytes[i] = _compress ? Compression.CompressValue(original, _compressionThreshold) : original;
            i++;
        }
        await BeforeOperationAsync().ConfigureAwait(false);

        if (_ring is null)
        {
            List<Connection.MultiEntry> entries;
            try
            {
                entries = await MultiSetChunkedAsync(
                    null, namespaceBytes, keyBytes, valueBytes, ttlSeconds).ConfigureAwait(false);
            }
            catch (ChunkedBatchInterruptedException partial)
            {
                // issue #411: a later chunk's connection failure survived
                // the built-in reconnect — don't discard the keys the
                // earlier chunks already confirmed stored.
                var confirmed = new HashSet<string>();
                for (int idx = 0; idx < partial.CompletedEntries.Count; idx++)
                {
                    if (partial.CompletedEntries[idx].Ok) confirmed.Add(keys[idx]);
                }
                throw new PartialConnectionLostException<HashSet<string>>(confirmed, partial.InnerException!);
            }
            foreach (Connection.MultiEntry entry in entries)
            {
                if (entry.WrongNode) throw new WrongNodeException();
            }
            return;
        }

        List<int> retry =
            await MultiSetPassAsync(namespaceBytes, keys, keyBytes, valueBytes, ttlSeconds, null).ConfigureAwait(false);
        if (retry.Count == 0) return;
        await MaybeRefreshAsync(force: true).ConfigureAwait(false);
        retry = await MultiSetPassAsync(namespaceBytes, keys, keyBytes, valueBytes, ttlSeconds, retry)
            .ConfigureAwait(false);
        if (retry.Count > 0) throw new WrongNodeException();
    }

    /// <summary><see cref="MultiGetChunkedAsync"/>'s write-side twin: one
    /// or more <c>o</c> sub-frames against <paramref name="slot"/>'s
    /// connection, split so a sub-frame never exceeds
    /// <see cref="MaxBatchKeys"/> pairs OR (issue #222)
    /// <see cref="MaxRequestBytes"/> of cumulative namespace+key+value
    /// wire bytes, the same way — a single pair always fits on its own,
    /// since <see cref="ValidateKeyAndValue"/> already guarantees
    /// that.
    ///
    /// <para>issue #411: as <see cref="MultiGetChunkedAsync"/>, throws
    /// <see cref="ChunkedBatchInterruptedException"/> instead of the bare
    /// connection failure when a chunk after the first one fails at the
    /// connection level, carrying every earlier chunk's already-completed
    /// entries.</para></summary>
    private async Task<List<Connection.MultiEntry>> MultiSetChunkedAsync(
        string? slot, byte[] namespaceBytes, byte[][] keys, byte[][] values, long ttlSeconds)
    {
        var entries = new List<Connection.MultiEntry>(new Connection.MultiEntry[keys.Length]);
        int start = 0;
        while (start < keys.Length)
        {
            long total = namespaceBytes.Length + MultiSetHeaderAllowance + keys[start].Length + values[start].Length;
            int end = start + 1;
            while (end < keys.Length && end - start < MaxBatchKeys)
            {
                long next = MultiSetHeaderAllowance + keys[end].Length + values[end].Length;
                if (total + next > MaxRequestBytes) break;
                total += next;
                end++;
            }
            ArraySegment<byte[]> keyChunk = new(keys, start, end - start);
            ArraySegment<byte[]> valueChunk = new(values, start, end - start);
            List<Connection.MultiEntry> chunkEntries;
            try
            {
                chunkEntries = await ApplyReconnectingAsync(
                    slot, connection => connection.MultiSetAsync(namespaceBytes, keyChunk, valueChunk, ttlSeconds))
                    .ConfigureAwait(false);
            }
            catch (Exception error) when (start > 0 && (error is NanocachedException or IOException
                or System.Net.Sockets.SocketException or ObjectDisposedException))
            {
                throw new ChunkedBatchInterruptedException(entries.GetRange(0, start), error);
            }
            for (int idx = start; idx < end; idx++)
            {
                entries[idx] = chunkEntries[idx - start];
            }
            start = end;
        }
        return entries;
    }

    /// <summary>One owner's key/IsPrimary membership across one
    /// <see cref="MultiSetPassAsync"/> call — see that method's own doc
    /// comment for why a key can appear here with <c>IsPrimary</c> false:
    /// the same node can be primary for one key in the batch and a
    /// replica for another.</summary>
    private sealed class OwnerBatch
    {
        public readonly List<int> Indices = new();
        public readonly List<bool> IsPrimary = new();
    }

    /// <summary>One pass of <see cref="SetManyBytesAsync(byte[], IReadOnlyDictionary{string, byte[]}, long)"/>'s
    /// cluster routing: for every key still needing resolution (every
    /// key, when <paramref name="retryIndices"/> is <c>null</c>, or just
    /// what a previous pass left unresolved), build one sub-batch per
    /// <b>owner name across every rank</b> — not just primaries, unlike
    /// <see cref="MultiGetPassAsync"/> — because within one batch the
    /// same node can be primary for one key and a replica for another;
    /// each owner therefore gets exactly one <c>o</c> sub-frame covering
    /// every key it holds in any role. Only a leg's <em>primary</em> keys
    /// can end up in the returned retry list; a leg's replica-held keys
    /// are logged-and-swallowed into <see cref="_replicaWriteFailures"/>
    /// instead, mirroring <see cref="WriteAsync{T}"/>'s stance for
    /// single-key set. A leg that is a pure replica for every key it
    /// holds is eligible for <c>FireAndForgetReplicas</c>, exactly like a
    /// single-key replica write — see
    /// <see cref="RunMultiSetLegAsync"/>.</summary>
    private async Task<List<int>> MultiSetPassAsync(
        byte[] namespaceBytes, List<string> keys, byte[][] keyBytes, byte[][] valueBytes,
        long ttlSeconds, List<int>? retryIndices)
    {
        List<int> indices = retryIndices ?? Enumerable.Range(0, keys.Count).ToList();

        var owners = new Dictionary<string, OwnerBatch>();
        var retry = new List<int>();
        foreach (int idx in indices)
        {
            IReadOnlyList<string> names = OwnerNames(namespaceBytes, keyBytes[idx]);
            if (names.Count == 0)
            {
                retry.Add(idx);
                continue;
            }
            for (int rank = 0; rank < names.Count; rank++)
            {
                if (!owners.TryGetValue(names[rank], out OwnerBatch? batch))
                {
                    batch = new OwnerBatch();
                    owners[names[rank]] = batch;
                }
                batch.Indices.Add(idx);
                batch.IsPrimary.Add(rank == 0);
            }
        }

        var legs = new List<Task>();
        foreach ((string name, OwnerBatch batch) in owners)
        {
            Task RunLegAsync() => RunMultiSetLegAsync(namespaceBytes, name, batch, keyBytes, valueBytes, ttlSeconds, retry);

            // Fire-and-forget replica writes: with FireAndForgetReplicas,
            // up to MaxInFlightBackgroundReplicaWrites legs run in the
            // background instead of being waited for below — mirrors
            // WriteAsync's own fire-and-forget branch exactly, including
            // its Close()-race fallback.
            bool pureReplica = !batch.IsPrimary.Contains(true);
            if (_fireAndForgetReplicas && pureReplica && _backgroundReplicaPermits.Wait(0))
            {
                Task background = Task.Run(RunLegAsync);
                _ = background.ContinueWith(
                    completed =>
                    {
                        if (completed.Exception is not null)
                        {
                            Interlocked.Increment(ref _replicaWriteFailures);
                        }
                        _backgroundReplicaPermits.Release();
                    },
                    TaskScheduler.Default);
                continue;
            }

            legs.Add(RunLegAsync());
        }

        Exception? legBug = null;
        foreach (Task leg in legs)
        {
            try
            {
                await leg.ConfigureAwait(false);
            }
            catch (Exception error)
            {
                legBug = error;
            }
        }
        if (legBug is not null) ExceptionDispatchInfo.Capture(legBug).Throw();
        return retry;
    }

    /// <summary>Dispatches one owner's <c>o</c> sub-batch (via
    /// <see cref="MultiSetChunkedAsync"/>) and applies its result to
    /// <paramref name="retry"/>/<see cref="_replicaWriteFailures"/>: only
    /// primary-held keys can end up appended to <paramref name="retry"/>;
    /// every replica-held key's failure or <c>W</c> is counted in
    /// <see cref="_replicaWriteFailures"/> instead, mirroring
    /// <see cref="WriteAsync{T}"/>'s own stance for single-key set. A
    /// connection-level failure for the whole leg is treated the same
    /// way, key by key, since the SAME sub-frame can carry both primary-
    /// and replica-held keys and a transport failure doesn't distinguish
    /// between them. <paramref name="retry"/> is shared by every
    /// concurrently-running owner leg, so every access is under
    /// <c>lock (retry)</c>.</summary>
    private async Task RunMultiSetLegAsync(
        byte[] namespaceBytes, string name, OwnerBatch batch, byte[][] keyBytes, byte[][] valueBytes,
        long ttlSeconds, List<int> retry)
    {
        var groupKeys = new byte[batch.Indices.Count][];
        var groupValues = new byte[batch.Indices.Count][];
        for (int i = 0; i < batch.Indices.Count; i++)
        {
            int idx = batch.Indices[i];
            groupKeys[i] = keyBytes[idx];
            groupValues[i] = valueBytes[idx];
        }

        List<Connection.MultiEntry> entries;
        try
        {
            entries = await MultiSetChunkedAsync(name, namespaceBytes, groupKeys, groupValues, ttlSeconds)
                .ConfigureAwait(false);
        }
        catch (ChunkedBatchInterruptedException partial)
        {
            // issue #411: a later chunk's connection failure used to mark
            // this WHOLE leg — including keys an earlier chunk of the SAME
            // leg already confirmed stored — as failed for both the retry
            // list and ReplicaWriteFailures, overcounting failures for keys
            // that actually succeeded. Apply each already-completed key's
            // real result instead, and fall back to the old whole-failure
            // treatment only for the keys that were never attempted (the
            // chunk that failed, or a later one still queued behind it).
            int completed = partial.CompletedEntries.Count;
            lock (retry)
            {
                for (int i = 0; i < batch.Indices.Count; i++)
                {
                    if (i < completed)
                    {
                        if (!batch.IsPrimary[i])
                        {
                            if (partial.CompletedEntries[i].WrongNode) Interlocked.Increment(ref _replicaWriteFailures);
                            continue;
                        }
                        if (partial.CompletedEntries[i].WrongNode) retry.Add(batch.Indices[i]);
                        continue;
                    }
                    if (batch.IsPrimary[i]) retry.Add(batch.Indices[i]);
                    else Interlocked.Increment(ref _replicaWriteFailures);
                }
            }
            return;
        }
        catch (Exception error) when (error is NanocachedException or IOException
            or System.Net.Sockets.SocketException or ObjectDisposedException)
        {
            // Swallowed by design (client-side replication) for
            // replica-held keys — see the class doc comment above; only a
            // primary-held key's failure feeds the retry pass. Narrowed
            // to the connection layer's own failure types, so a genuine
            // programming bug propagates instead of being treated like a
            // dead owner.
            lock (retry)
            {
                for (int i = 0; i < batch.Indices.Count; i++)
                {
                    if (batch.IsPrimary[i]) retry.Add(batch.Indices[i]);
                    else Interlocked.Increment(ref _replicaWriteFailures);
                }
            }
            return;
        }

        lock (retry)
        {
            for (int i = 0; i < batch.Indices.Count; i++)
            {
                if (!batch.IsPrimary[i])
                {
                    if (entries[i].WrongNode) Interlocked.Increment(ref _replicaWriteFailures);
                    continue;
                }
                if (entries[i].WrongNode) retry.Add(batch.Indices[i]);
            }
        }
    }

    // ── incr / decr (issue #129) ─────────────────────────────────

    public Task<long?> IncrAsync(string key, long delta) => IncrAsync(EmptyNamespace, key, delta);

    /// <summary>issue #129: increments the counter stored at
    /// <paramref name="key"/> by <paramref name="delta"/> (a negative
    /// delta decrements — there is no separate decrement opcode; see
    /// <see cref="DecrAsync(byte[], long)"/>) and returns its new value, or
    /// <c>null</c> when the key is missing or expired — the same
    /// not-found convention <see cref="GetAsync(byte[])"/> uses. Throws
    /// <see cref="NotNumericException"/> when the stored value isn't an
    /// integer INCR can operate on, or applying the delta would overflow a
    /// signed 64-bit integer.
    ///
    /// <para><b>Exactly as volatile as <see cref="SetAsync(byte[], byte[], long)"/></b>:
    /// LRU eviction and TTL expiry reclaim an incremented value like any
    /// other entry. Good for rate limiting or approximate counters, not
    /// for durable counts (billing, inventory).</para>
    ///
    /// <para>Cluster replication: unlike <see cref="SetAsync(byte[], byte[], long)"/>/
    /// <see cref="DeleteAsync(byte[])"/>, which send the identical write to
    /// every owner, only the primary owner ever runs the increment — a
    /// successful result is forwarded to the replicas as an ordinary
    /// <c>Set</c> of the literal new value instead of replaying the delta
    /// on each of them, which would let a replica drift from the primary
    /// (e.g. after an earlier dropped replica write, or an independent
    /// eviction). See <see cref="IncrPrimaryThenReplicateAsync"/> for the
    /// full mechanics.</para>
    ///
    /// <para><b>At-least-once on a lost connection (issue #225)</b>: unlike
    /// <see cref="GetAsync(byte[])"/>/<see cref="SetAsync(byte[], byte[], long)"/>/
    /// <see cref="DeleteAsync(byte[])"/> — which are idempotent, so this
    /// SDK's built-in redial-and-retry-once always resends them — Incr is
    /// NOT idempotent, so the redial-and-retry only ever resends this
    /// request when the connection is known to have died before the
    /// request frame could be written at all (e.g. the server's idle
    /// timeout closed it first). If the request frame was fully written
    /// and only the reply never arrived, the primary may already have
    /// applied the increment; this method throws
    /// <see cref="ConnectionLostException"/> in that case instead of
    /// silently double-applying it. A caller that needs to know for certain
    /// whether an Incr landed after seeing this exception should read the
    /// counter back rather than assume either outcome.</para>
    ///
    /// <para><b>Incompatible with <c>Compress</c> (issue #321)</b>: throws
    /// <see cref="CompressionIncompatibleException"/> immediately, before
    /// any I/O, when this client was constructed with <c>Compress</c>
    /// enabled — an incremented value can't safely round-trip through
    /// compression. Disable <c>Compress</c> or use a separate client for
    /// counters.</para></summary>
    public Task<long?> IncrAsync(byte[] key, long delta) => IncrAsync(EmptyNamespace, key, delta);

    /// <summary>issue #129: as <see cref="IncrAsync(string, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<long?> IncrAsync(byte[] namespaceBytes, string key, long delta) =>
        IncrAsync(namespaceBytes, Encoding.UTF8.GetBytes(key), delta);

    /// <summary>issue #129: as <see cref="IncrAsync(byte[], long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — this is the internal
    /// method <see cref="NanocachedNamespace"/> forwards to, rather than
    /// duplicating this client's networking.</summary>
    internal async Task<long?> IncrAsync(byte[] namespaceBytes, byte[] key, long delta)
    {
        if (_compress)
        {
            throw new CompressionIncompatibleException();
        }
        ValidateKey(namespaceBytes, key);
        await BeforeOperationAsync().ConfigureAwait(false);
        try
        {
            return await WithClusterRetryAsync(() => IncrPrimaryThenReplicateAsync(namespaceBytes, key, delta))
                .ConfigureAwait(false);
        }
        catch (UnsafeToReplayException error)
        {
            // issue #225: the primary's request may already have been
            // applied — surface the connection loss, never replay it.
            throw error.Unwrap();
        }
    }

    /// <summary>issue #182: <see cref="IncrAsync(string, long)"/> with
    /// <paramref name="delta"/> negated — never a different wire op. Same
    /// at-least-once-on-a-lost-connection caveat as
    /// <see cref="IncrAsync(byte[], long)"/> (issue #225): not idempotent,
    /// so a connection lost after the request frame was already fully
    /// written throws <see cref="ConnectionLostException"/> rather than
    /// risk decrementing twice. Also throws
    /// <see cref="CompressionIncompatibleException"/> when <c>Compress</c>
    /// is enabled, for the same reason <see cref="IncrAsync(byte[], long)"/>
    /// does (issue #321).</summary>
    public Task<long?> DecrAsync(string key, long delta) => IncrAsync(key, NegateDecrDelta(delta));

    /// <summary>issue #129: <see cref="IncrAsync(byte[], long)"/> with
    /// <paramref name="delta"/> negated — never a different wire op;
    /// <c>i</c>'s own signed delta already covers decrementing. issue
    /// #182: delegates the negation to <see cref="NegateDecrDelta"/>,
    /// which rejects <see cref="long.MinValue"/> rather than silently
    /// wrapping it back to itself. See <see cref="IncrAsync(byte[], long)"/>'s
    /// doc comment for the at-least-once-on-a-lost-connection caveat
    /// (issue #225) this inherits.</summary>
    public Task<long?> DecrAsync(byte[] key, long delta) => IncrAsync(key, NegateDecrDelta(delta));

    internal Task<long?> DecrAsync(byte[] namespaceBytes, string key, long delta) =>
        IncrAsync(namespaceBytes, key, NegateDecrDelta(delta));

    internal Task<long?> DecrAsync(byte[] namespaceBytes, byte[] key, long delta) =>
        IncrAsync(namespaceBytes, key, NegateDecrDelta(delta));

    /// <summary>issue #182: shared negation guard for every
    /// <c>DecrAsync</c> overload above. <see cref="long.MinValue"/> has no
    /// corresponding positive <see cref="long"/> value in two's
    /// complement (<see cref="long.MaxValue"/> is one short of
    /// <c>|long.MinValue|</c>), so negating it wraps back to
    /// <see cref="long.MinValue"/> itself — silently turning a decrement
    /// into the largest possible increment. Rejecting it client-side,
    /// before any I/O, mirrors the Java and Rust SDKs' own rejection of
    /// this value.</summary>
    private static long NegateDecrDelta(long delta)
    {
        if (delta == long.MinValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(delta),
                "nanocached: decr delta must not be long.MinValue, which has no valid long negation");
        }
        return -delta;
    }

    /// <summary>issue #129 — the part that's easy to get subtly wrong: runs
    /// <c>i</c> against the key's PRIMARY owner only, awaits its reply,
    /// then — only on success — fans that literal result out to the
    /// remaining owners as an ordinary <c>Set</c> (the same wire op and
    /// framing <see cref="WriteAsync{T}"/>'s replica legs use), never by
    /// resending <c>i</c> to a replica. Forwarding the primary's absolute
    /// result keeps every replica byte-identical to it; replaying the
    /// increment on each replica independently could let a replica drift
    /// (e.g. if an earlier replica-leg write was dropped after a transient
    /// failure, or the replica separately evicted and reset the key) —
    /// the same reasoning the node's own migration/decommission-handoff
    /// logic uses server-side.
    ///
    /// <para>A miss (<c>N</c>) or not-numeric result (<c>T</c>) is
    /// returned/thrown directly, without touching any replica — nothing
    /// was written, so there is nothing to forward. A dead or
    /// disagreeing replica leg is swallowed exactly like
    /// <see cref="WriteAsync{T}"/>'s own replica legs — counted via
    /// <see cref="Stats"/>'s <c>ReplicaWriteFailures</c>, the same
    /// counter, not a new one — and, with <see cref="Options.FireAndForgetReplicas"/>,
    /// drawn from the same bounded background pool
    /// (<see cref="_backgroundReplicaPermits"/>) those legs already
    /// share with read-repair. The primary leg's own <c>W</c> or
    /// connection-level failure propagates up to
    /// <see cref="WithClusterRetryAsync{T}"/>'s caller, which refreshes
    /// the node list and retries this whole method once — since nothing
    /// is replicated until AFTER the primary succeeds, that retry only
    /// ever redoes the primary leg in practice, never a replay of an
    /// already-forwarded replica write.</para></summary>
    private async Task<long?> IncrPrimaryThenReplicateAsync(byte[] namespaceBytes, byte[] key, long delta)
    {
        if (_ring is null)
        {
            var single = await ApplyReconnectingNotIdempotentAsync(
                null, connection => connection.IncrAsync(namespaceBytes, key, delta)).ConfigureAwait(false);
            return single?.Value;
        }

        IReadOnlyList<string> names = OwnerNames(namespaceBytes, key);
        if (names.Count == 0)
        {
            throw new ConnectionLostException("nanocached: no owner is reachable for this key");
        }

        var primaryResult = await ApplyReconnectingNotIdempotentAsync(
            names[0], connection => connection.IncrAsync(namespaceBytes, key, delta)).ConfigureAwait(false);
        if (primaryResult is null) return null;

        (long value, long ttlSeconds) = primaryResult.Value;
        byte[] valueBytes = Encoding.ASCII.GetBytes(value.ToString(CultureInfo.InvariantCulture));

        // The primary already succeeded by this point, so — unlike
        // WriteAsync, which must reconcile a possibly-failed primary
        // against a replica-leg bug — there is nothing to reconcile here:
        // every failure ReplicateToOwnersAsync can produce is already
        // swallowed and counted inside it.
        await ReplicateToOwnersAsync(
            names.Skip(1), connection => connection.SetAsync(namespaceBytes, key, valueBytes, ttlSeconds))
            .ConfigureAwait(false);

        return value;
    }

    /// <summary>Shared replica fan-out for the "primary evaluates, then
    /// forwards the literal result" pattern used by <see cref="IncrPrimaryThenReplicateAsync"/>
    /// (issue #129) and <see cref="CasPrimaryThenReplicateAsync"/>/
    /// <see cref="RemoveIfMatchesPrimaryThenReplicateAsync"/> (issue #141):
    /// runs <paramref name="replicate"/> against each of <paramref
    /// name="names"/> (already excluding the primary, which the caller ran
    /// separately and successfully). Every expected failure (a dead
    /// replica, a lost socket) is swallowed and counted via
    /// <see cref="Stats"/>'s <c>ReplicaWriteFailures</c> — the same
    /// counter <see cref="WriteAsync{T}"/>'s own replica legs use, not a
    /// new one — since the primary already succeeded and a replica gap is
    /// healed by the next node-list refresh, not by failing an already-
    /// completed write. With <see cref="Options.FireAndForgetReplicas"/>,
    /// up to <see cref="MaxInFlightBackgroundReplicaWrites"/> legs run in
    /// the background instead of being awaited here, sharing the one pool
    /// <see cref="WriteAsync{T}"/>'s own replica legs and read-repair
    /// draw from.</summary>
    private async Task ReplicateToOwnersAsync(IEnumerable<string> names, Func<Connection, Task> replicate)
    {
        async Task RunOneAsync(string name)
        {
            try
            {
                await ApplyReconnectingAsync<object?>(name, async connection =>
                {
                    await replicate(connection).ConfigureAwait(false);
                    return null;
                }).ConfigureAwait(false);
            }
            catch (Exception error) when (error is NanocachedException or IOException
                or System.Net.Sockets.SocketException or ObjectDisposedException)
            {
                Interlocked.Increment(ref _replicaWriteFailures);
            }
        }

        var replicaWrites = new List<Task>();
        foreach (string name in names)
        {
            // Fire-and-forget replica writes: mirrors WriteAsync's own
            // background-pool logic exactly, sharing the same permit pool
            // — past the cap, or with the option off, a leg just runs on
            // the synchronous path below instead.
            if (_fireAndForgetReplicas && _backgroundReplicaPermits.Wait(0))
            {
                Task background = Task.Run(() => RunOneAsync(name));
                _ = background.ContinueWith(
                    completed =>
                    {
                        if (completed.Exception is not null)
                        {
                            Interlocked.Increment(ref _replicaWriteFailures);
                        }
                        _backgroundReplicaPermits.Release();
                    },
                    TaskScheduler.Default);
                continue;
            }

            replicaWrites.Add(RunOneAsync(name));
        }

        foreach (Task replicaWrite in replicaWrites)
        {
            await replicaWrite.ConfigureAwait(false);
        }
    }

    // ── compare-and-set (issue #141) ─────────────────────────────

    /// <summary>issue #141 — compare-and-set: SHA-256 of <paramref
    /// name="value"/>, truncated to the first 16 bytes (128 bits),
    /// lowercase-hex-encoded (32 characters) — the digest ("CAS token")
    /// the server computes over a key's exact stored bytes. Public and
    /// pure so a caller that already holds a value can compute its
    /// expected digest without a prior read — see
    /// <see cref="ReplaceAsync(byte[], string, byte[], long)"/>'s doc
    /// comment for why that shortcut is only safe when the caller's own
    /// serialization reproduces the stored bytes exactly, unlike the
    /// always-correct read-then-write-back path via
    /// <see cref="GetWithTokenAsync(byte[])"/>. Computed identically by
    /// the server and every SDK; pinned by a fixed cross-language test
    /// vector (see NanocachedClientTests).</summary>
    public static string ContentDigest(byte[] value)
    {
        byte[] hash = SHA256.HashData(value);
        return Convert.ToHexString(hash, 0, 16).ToLowerInvariant();
    }

    private const string CondAbsent = "A";
    private const string CondPresent = "P";

    public Task<bool> PutIfAbsentAsync(string key, string value, long ttlSeconds = 0) =>
        PutIfAbsentAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary>issue #141 — <c>add</c>/<c>putIfAbsent</c>: stores
    /// <paramref name="value"/> only if <paramref name="key"/> is
    /// currently absent (including lazily expired). Returns <c>true</c>
    /// when stored, <c>false</c> when the key already existed — a
    /// condition mismatch is a normal boolean outcome, never an
    /// exception, the same idiom <see cref="DeleteAsync(byte[])"/> uses.
    ///
    /// <para><b>Not a distributed lock</b>: LRU eviction can still reclaim
    /// the key exactly as it would after a plain <see cref="SetAsync(byte[], byte[], long)"/>
    /// — if a key used as a lock is evicted under memory pressure, a
    /// second caller's <c>PutIfAbsentAsync</c> succeeds while the first
    /// caller still believes it holds the lock. See
    /// docs/protocol.html#cas.</para>
    ///
    /// <para>Cluster replication: only the key's primary owner evaluates
    /// the condition; on success the SDK forwards the literal value to the
    /// remaining owners as an ordinary <see cref="SetAsync(byte[], byte[], long)"/>,
    /// never by replaying <c>k</c> on a replica — see
    /// <see cref="IncrAsync(byte[], long)"/>'s doc comment for why that
    /// matters.</para>
    ///
    /// <para><b>At-least-once on a lost connection (issue #225)</b>: like
    /// <see cref="IncrAsync(byte[], long)"/>, CAS is NOT idempotent — a
    /// mismatch response and an already-succeeded response mean different
    /// things, so blindly replaying it after a redial could report an
    /// already-successful CAS as a mismatch. The redial-and-retry this SDK
    /// builds in for <see cref="GetAsync(byte[])"/>/<see cref="SetAsync(byte[], byte[], long)"/>/
    /// <see cref="DeleteAsync(byte[])"/> only ever resends this request
    /// when the connection is known to have died before the request frame
    /// could be written at all; if the frame was fully written and only
    /// the reply was lost, this method throws
    /// <see cref="ConnectionLostException"/> instead of guessing. The same
    /// caveat applies to <see cref="ReplaceIfPresentAsync(byte[], byte[], long)"/>,
    /// <see cref="ReplaceAsync(byte[], string, byte[], long)"/>, and
    /// <see cref="DeleteIfMatchesAsync(byte[], string)"/>, which all share
    /// this same primary-then-replicate path.</para></summary>
    public Task<bool> PutIfAbsentAsync(byte[] key, byte[] value, long ttlSeconds = 0) =>
        PutIfAbsentAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary>issue #141: as <see cref="PutIfAbsentAsync(string, string, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<bool> PutIfAbsentAsync(byte[] namespaceBytes, string key, string value, long ttlSeconds = 0) =>
        PutIfAbsentAsync(namespaceBytes, Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ttlSeconds);

    /// <summary>issue #141: as <see cref="PutIfAbsentAsync(byte[], byte[], long)"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal Task<bool> PutIfAbsentAsync(byte[] namespaceBytes, byte[] key, byte[] value, long ttlSeconds = 0) =>
        CasAsync(namespaceBytes, key, value, CondAbsent, ttlSeconds);

    public Task<bool> ReplaceIfPresentAsync(string key, string value, long ttlSeconds = 0) =>
        ReplaceIfPresentAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary>issue #141 — the two-argument <c>replace(key, value)</c>:
    /// stores <paramref name="value"/> only if <paramref name="key"/>
    /// currently holds any (unexpired) value, whatever it is. Returns
    /// <c>true</c> when replaced, <c>false</c> when the key was absent —
    /// see <see cref="PutIfAbsentAsync(byte[], byte[], long)"/>'s doc
    /// comment for the shared not-a-lock caveat and replication
    /// rule.</summary>
    public Task<bool> ReplaceIfPresentAsync(byte[] key, byte[] value, long ttlSeconds = 0) =>
        ReplaceIfPresentAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary>issue #141: as <see cref="ReplaceIfPresentAsync(string, string, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<bool> ReplaceIfPresentAsync(byte[] namespaceBytes, string key, string value, long ttlSeconds = 0) =>
        ReplaceIfPresentAsync(namespaceBytes, Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ttlSeconds);

    /// <summary>issue #141: as <see cref="ReplaceIfPresentAsync(byte[], byte[], long)"/>,
    /// scoped to <paramref name="namespaceBytes"/>.</summary>
    internal Task<bool> ReplaceIfPresentAsync(byte[] namespaceBytes, byte[] key, byte[] value, long ttlSeconds = 0) =>
        CasAsync(namespaceBytes, key, value, CondPresent, ttlSeconds);

    public Task<bool> ReplaceAsync(string key, string token, string newValue, long ttlSeconds = 0) =>
        ReplaceAsync(EmptyNamespace, key, token, newValue, ttlSeconds);

    /// <summary>issue #141 — the three-argument <c>replace(key, old, new)</c>:
    /// stores <paramref name="newValue"/> only if <paramref name="key"/>
    /// holds an unexpired value whose content digest equals <paramref
    /// name="token"/> exactly (32-character lowercase hex — see
    /// <see cref="ContentDigest"/>). Returns <c>true</c> when replaced,
    /// <c>false</c> on a digest mismatch (including a since-deleted key).
    ///
    /// <para><paramref name="token"/> is ordinarily obtained from a real
    /// prior <see cref="GetWithTokenAsync(byte[])"/> — that path is always
    /// correct. Reconstructing a digest via <see cref="ContentDigest"/>
    /// from a value the caller already holds (skipping the read) is only
    /// correct if that reconstruction produces byte-identical bytes to
    /// what the server actually stores — true within one client sharing
    /// one serializer/compressor, not guaranteed across languages or
    /// mismatched <c>Compress</c> settings (the same caveat memcached's
    /// own value-based CAS has).</para>
    ///
    /// <para>See <see cref="PutIfAbsentAsync(byte[], byte[], long)"/>'s
    /// doc comment for the shared not-a-lock caveat, replication rule, and
    /// — since ReplaceAsync is CAS, not idempotent — the
    /// at-least-once-on-a-lost-connection caveat (issue #225).</para></summary>
    public Task<bool> ReplaceAsync(byte[] key, string token, byte[] newValue, long ttlSeconds = 0) =>
        ReplaceAsync(EmptyNamespace, key, token, newValue, ttlSeconds);

    /// <summary>issue #141: as <see cref="ReplaceAsync(string, string, string, long)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<bool> ReplaceAsync(byte[] namespaceBytes, string key, string token, string newValue, long ttlSeconds = 0) =>
        ReplaceAsync(namespaceBytes, Encoding.UTF8.GetBytes(key), token, Encoding.UTF8.GetBytes(newValue), ttlSeconds);

    /// <summary>issue #141: as <see cref="ReplaceAsync(byte[], string, byte[], long)"/>,
    /// scoped to <paramref name="namespaceBytes"/>. issue #223: validates
    /// <paramref name="token"/> before it ever reaches the wire — see
    /// <see cref="ValidateToken"/>.</summary>
    internal Task<bool> ReplaceAsync(byte[] namespaceBytes, byte[] key, string token, byte[] newValue, long ttlSeconds = 0)
    {
        ValidateToken(token);
        return CasAsync(namespaceBytes, key, newValue, token, ttlSeconds);
    }

    public Task<bool> DeleteIfMatchesAsync(string key, string token) => DeleteIfMatchesAsync(EmptyNamespace, key, token);

    /// <summary>issue #141 — the two-argument <c>remove(key, old)</c>:
    /// removes <paramref name="key"/> only if its current stored value's
    /// content digest equals <paramref name="token"/> exactly. Returns
    /// <c>true</c> when removed, <c>false</c> on a digest mismatch or a
    /// missing key — see <see cref="ReplaceAsync(byte[], string, byte[], long)"/>'s
    /// doc comment for how to obtain <paramref name="token"/>.
    ///
    /// <para>Cluster replication: only the primary owner evaluates the
    /// digest; on success the SDK forwards the removal to the remaining
    /// owners as an ordinary <see cref="DeleteAsync(byte[])"/>, never by
    /// replaying <c>x</c> on a replica.</para>
    ///
    /// <para><b>At-least-once on a lost connection (issue #225)</b>: unlike
    /// <see cref="DeleteAsync(byte[])"/>, this is a conditional remove, not
    /// idempotent — a mismatch and an already-succeeded removal mean
    /// different things. The built-in redial-and-retry only ever resends
    /// this request when the connection is known to have died before the
    /// request frame could be written at all; if the frame was fully
    /// written and only the reply was lost, this method throws
    /// <see cref="ConnectionLostException"/> instead of reporting a
    /// possibly-already-successful removal as a mismatch. See
    /// <see cref="PutIfAbsentAsync(byte[], byte[], long)"/>'s doc comment
    /// for the same caveat shared by every CAS method.</para></summary>
    public Task<bool> DeleteIfMatchesAsync(byte[] key, string token) => DeleteIfMatchesAsync(EmptyNamespace, key, token);

    /// <summary>issue #141: as <see cref="DeleteIfMatchesAsync(string, string)"/>,
    /// scoped to <paramref name="namespaceBytes"/> — the internal method
    /// <see cref="NanocachedNamespace"/> forwards to.</summary>
    internal Task<bool> DeleteIfMatchesAsync(byte[] namespaceBytes, string key, string token) =>
        DeleteIfMatchesAsync(namespaceBytes, Encoding.UTF8.GetBytes(key), token);

    /// <summary>issue #141: as <see cref="DeleteIfMatchesAsync(byte[], string)"/>,
    /// scoped to <paramref name="namespaceBytes"/>. issue #223: validates
    /// <paramref name="token"/> before it ever reaches the wire — see
    /// <see cref="ValidateToken"/>.</summary>
    internal async Task<bool> DeleteIfMatchesAsync(byte[] namespaceBytes, byte[] key, string token)
    {
        ValidateToken(token);
        ValidateKey(namespaceBytes, key);
        await BeforeOperationAsync().ConfigureAwait(false);
        try
        {
            return await WithClusterRetryAsync(
                () => RemoveIfMatchesPrimaryThenReplicateAsync(namespaceBytes, key, token))
                .ConfigureAwait(false);
        }
        catch (UnsafeToReplayException error)
        {
            // issue #225: same reasoning as IncrAsync — the primary's
            // removal may already have been applied.
            throw error.Unwrap();
        }
    }

    /// <summary>issue #141: as <see cref="IncrAsync(byte[], byte[], long)"/>
    /// but for <c>k</c> — validates, compresses (reusing
    /// <see cref="SetAsync(byte[], byte[], byte[], long)"/>'s exact
    /// encode step, so a subsequent plain <see cref="GetAsync(byte[])"/>
    /// from any client can always decompress it), and runs the
    /// primary-evaluates-then-replicates driver.</summary>
    private async Task<bool> CasAsync(byte[] namespaceBytes, byte[] key, byte[] value, string cond, long ttlSeconds)
    {
        if (ttlSeconds < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ttlSeconds), $"nanocached: ttlSeconds must be non-negative, got {ttlSeconds}");
        }
        ValidateKeyAndValue(namespaceBytes, key, value);
        byte[] outgoing = _compress ? Compression.CompressValue(value, _compressionThreshold) : value;
        await BeforeOperationAsync().ConfigureAwait(false);
        try
        {
            return await WithClusterRetryAsync(
                () => CasPrimaryThenReplicateAsync(namespaceBytes, key, outgoing, cond, ttlSeconds))
                .ConfigureAwait(false);
        }
        catch (UnsafeToReplayException error)
        {
            // issue #225: same reasoning as IncrAsync — the primary's CAS
            // may already have been applied (PutIfAbsentAsync/
            // ReplaceIfPresentAsync/ReplaceAsync all funnel through here).
            throw error.Unwrap();
        }
    }

    /// <summary>issue #141 — mirrors <see cref="IncrPrimaryThenReplicateAsync"/>
    /// exactly: runs <c>k</c> against the key's PRIMARY owner only —
    /// <paramref name="value"/> here is already the outgoing (possibly
    /// compressed) wire bytes — then, only on success, forwards that same
    /// literal value to the remaining owners as an ordinary <c>Set</c>,
    /// never by replaying <c>k</c> on a replica (which could evaluate
    /// <paramref name="cond"/> against a possibly-different copy and reach
    /// a different outcome).</summary>
    private async Task<bool> CasPrimaryThenReplicateAsync(
        byte[] namespaceBytes, byte[] key, byte[] value, string cond, long ttlSeconds)
    {
        if (_ring is null)
        {
            return await ApplyReconnectingNotIdempotentAsync(
                null, connection => connection.CasAsync(namespaceBytes, key, value, cond, ttlSeconds))
                .ConfigureAwait(false);
        }

        IReadOnlyList<string> names = OwnerNames(namespaceBytes, key);
        if (names.Count == 0)
        {
            throw new ConnectionLostException("nanocached: no owner is reachable for this key");
        }

        bool stored = await ApplyReconnectingNotIdempotentAsync(
            names[0], connection => connection.CasAsync(namespaceBytes, key, value, cond, ttlSeconds))
            .ConfigureAwait(false);
        if (!stored) return false;

        await ReplicateToOwnersAsync(
            names.Skip(1), connection => connection.SetAsync(namespaceBytes, key, value, ttlSeconds))
            .ConfigureAwait(false);
        return true;
    }

    /// <summary>issue #141 — mirrors <see cref="CasPrimaryThenReplicateAsync"/>
    /// for <c>x</c>: runs the digest-conditioned remove against the
    /// primary owner only, then — only on success — forwards the removal
    /// to the remaining owners as an ordinary <c>Delete</c>, never by
    /// replaying <c>x</c> on a replica.</summary>
    private async Task<bool> RemoveIfMatchesPrimaryThenReplicateAsync(byte[] namespaceBytes, byte[] key, string digest)
    {
        if (_ring is null)
        {
            return await ApplyReconnectingNotIdempotentAsync(
                null, connection => connection.RemoveIfMatchesAsync(namespaceBytes, key, digest))
                .ConfigureAwait(false);
        }

        IReadOnlyList<string> names = OwnerNames(namespaceBytes, key);
        if (names.Count == 0)
        {
            throw new ConnectionLostException("nanocached: no owner is reachable for this key");
        }

        bool removed = await ApplyReconnectingNotIdempotentAsync(
            names[0], connection => connection.RemoveIfMatchesAsync(namespaceBytes, key, digest))
            .ConfigureAwait(false);
        if (!removed) return false;

        await ReplicateToOwnersAsync(names.Skip(1), connection => connection.DeleteAsync(namespaceBytes, key))
            .ConfigureAwait(false);
        return true;
    }

    /// <summary>issue #106: drops every entry in <paramref name="namespaceBytes"/>
    /// (empty clears the default namespace — not rejected, see
    /// <see cref="NanocachedNamespace"/>'s doc comment on
    /// <c>Namespace("")</c>). The internal method
    /// <see cref="NanocachedNamespace.ClearAsync()"/> forwards to.</summary>
    internal async Task ClearAsync(byte[] namespaceBytes)
    {
        await BeforeOperationAsync().ConfigureAwait(false);
        await ClearFanOutWithRetryAsync(connection => connection.ClearAsync(namespaceBytes))
            .ConfigureAwait(false);
    }

    /// <summary>issue #106: drops every namespace, the default one
    /// included — the client-side equivalent of the server's <c>F</c>.
    /// Per the issue, deliberately not named "flush" to keep the public
    /// API's vocabulary to get/set/delete/clear.</summary>
    public async Task ClearAllAsync()
    {
        await BeforeOperationAsync().ConfigureAwait(false);
        await ClearFanOutWithRetryAsync(connection => connection.ClearAllAsync())
            .ConfigureAwait(false);
    }

    /// <summary>
    /// issue #106: a clear/flush is never key-addressed (docs/protocol.html's
    /// "c / F" section — no <c>W</c> ever, so <see cref="WithClusterRetryAsync{T}"/>'s
    /// single-key retry doesn't apply), so it fans out to <em>every</em>
    /// node in the client's current node list concurrently, the same way
    /// <see cref="WriteAsync{T}"/> fans a replicated write out to a key's
    /// owners. Success requires every node to have acked <c>C</c>: on any
    /// failure the node list is refreshed once (the same refresh path
    /// <see cref="WithClusterRetryAsync{T}"/> uses for a stale ring) and
    /// the clear is retried against every node of the <em>refreshed</em>
    /// list — not just the ones that failed, since a refresh can also
    /// reassign which node owns which share. A node still failing after
    /// that retry fails the whole call — a clear must never silently
    /// leave a namespace partially cleared — but the operation is
    /// idempotent, so a caller can simply retry it.
    ///
    /// <para>Single/standalone mode (<see cref="_ring"/> is <c>null</c>)
    /// has only the one connected node, so there is nothing to fan out to
    /// or refresh: <paramref name="op"/> just runs against it directly,
    /// through the same lazy-reconnect path every other operation
    /// uses.</para>
    /// </summary>
    private async Task ClearFanOutWithRetryAsync(Func<Connection, Task> op)
    {
        if (_ring is null)
        {
            await ApplyReconnectingAsync<object?>(null, async connection =>
            {
                await op(connection).ConfigureAwait(false);
                return null;
            }).ConfigureAwait(false);
            return;
        }

        List<string> names;
        lock (_stateLock) { names = _members.Keys.ToList(); }

        IReadOnlyList<(string Name, Exception Error)> failures =
            await ClearFanOutAsync(names, op).ConfigureAwait(false);
        if (failures.Count == 0) return;

        await MaybeRefreshAsync(force: true).ConfigureAwait(false);
        lock (_stateLock) { names = _members.Keys.ToList(); }

        failures = await ClearFanOutAsync(names, op).ConfigureAwait(false);
        if (failures.Count > 0)
        {
            string detail = string.Join(
                ", ", failures.Select(failure => $"{failure.Name} ({failure.Error.Message})"));
            throw new ConnectionLostException($"nanocached: clear failed on node(s): {detail}");
        }
    }

    /// <summary>Runs <paramref name="op"/> against every node in
    /// <paramref name="names"/> concurrently (fan-out — see
    /// <see cref="ClearFanOutWithRetryAsync"/>), through the same
    /// lazy-reconnect path a replica write uses. Returns the nodes that
    /// failed, paired with why, instead of throwing — the caller decides
    /// whether a first-pass failure warrants a refresh-and-retry or, on a
    /// second pass, is fatal.</summary>
    private async Task<IReadOnlyList<(string Name, Exception Error)>> ClearFanOutAsync(
        IReadOnlyList<string> names, Func<Connection, Task> op)
    {
        async Task<(string Name, Exception Error)?> RunAsync(string name)
        {
            try
            {
                await ApplyReconnectingAsync<object?>(name, async connection =>
                {
                    await op(connection).ConfigureAwait(false);
                    return null;
                }).ConfigureAwait(false);
                return null;
            }
            catch (Exception error) when (error is NanocachedException or IOException
                or System.Net.Sockets.SocketException or ObjectDisposedException)
            {
                // Narrowed the same way ReplicaWriteAsync's swallow site is
                // (client-side replication): a dead/unreachable node here
                // is exactly the case this fan-out's refresh-and-retry
                // exists for, not a programming bug — that still
                // propagates. OperationCanceledException (Close() racing
                // this) is deliberately not caught either, matching the
                // other swallow sites.
                return (name, error);
            }
        }

        (string Name, Exception Error)?[] outcomes =
            await Task.WhenAll(names.Select(RunAsync)).ConfigureAwait(false);
        return outcomes.Where(outcome => outcome is not null).Select(outcome => outcome!.Value).ToList();
    }

    /// <summary>Rejects an empty key, or a (namespace, key) pair so large
    /// that a bare <c>"g "</c>/<c>"G "</c>/<c>"d "</c>/<c>"D "</c> header
    /// plus the namespace and key alone would already risk tripping the
    /// server's MAX_REQUEST_SIZE (audit finding D2) — checked
    /// synchronously, before any connection is touched, mirroring
    /// <see cref="SetAsync(byte[], byte[], long)"/>'s ttlSeconds check.
    /// issue #105: the namespace counts toward the same budget as the key
    /// — the wire imposes no separate limit on it, so neither does this
    /// (the empty namespace contributes 0 bytes, so this is
    /// byte-identical to the pre-namespace check).</summary>
    private static void ValidateKey(byte[] namespaceBytes, byte[] key)
    {
        if (key.Length == 0)
        {
            throw new ArgumentException("nanocached: key must not be empty", nameof(key));
        }
        long total = (long)namespaceBytes.Length + key.Length;
        if (total > MaxRequestBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(key),
                namespaceBytes.Length == 0
                    ? $"nanocached: key is {key.Length} bytes, which exceeds the {MaxRequestBytes}-byte "
                      + "request limit (server MAX_REQUEST_SIZE, src/server.rs, is 1 MiB)"
                    : $"nanocached: namespace ({namespaceBytes.Length} bytes) + key ({key.Length} bytes) = "
                      + $"{total} bytes, which exceeds the {MaxRequestBytes}-byte request limit (server "
                      + "MAX_REQUEST_SIZE, src/server.rs, is 1 MiB)");
        }
    }

    /// <summary>As <see cref="ValidateKey(byte[], byte[])"/>, plus rejects a
    /// namespace+key+value combination too large for a single
    /// <c>s</c>/<c>S</c> request to have any chance of fitting under the
    /// server's MAX_REQUEST_SIZE (audit finding D2). Checked against the
    /// caller-supplied value, before compression — compression only ever
    /// shrinks what actually goes on the wire, so this is the conservative
    /// (never falsely permissive) side to check.</summary>
    private static void ValidateKeyAndValue(byte[] namespaceBytes, byte[] key, byte[] value)
    {
        ValidateKey(namespaceBytes, key);
        long total = (long)namespaceBytes.Length + key.Length + value.Length;
        if (total > MaxRequestBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                namespaceBytes.Length == 0
                    ? $"nanocached: key ({key.Length} bytes) + value ({value.Length} bytes) = {total} bytes, "
                      + $"which exceeds the {MaxRequestBytes}-byte request limit (server MAX_REQUEST_SIZE, "
                      + "src/server.rs, is 1 MiB)"
                    : $"nanocached: namespace ({namespaceBytes.Length} bytes) + key ({key.Length} bytes) + "
                      + $"value ({value.Length} bytes) = {total} bytes, which exceeds the "
                      + $"{MaxRequestBytes}-byte request limit (server MAX_REQUEST_SIZE, src/server.rs, is 1 MiB)");
        }
    }

    /// <summary>issue #223: rejects a caller-supplied CAS <paramref
    /// name="token"/> that isn't exactly 32 lowercase hex characters —
    /// mirrors Java's <c>validateToken</c>. <see cref="CasAsync(byte[], byte[], byte[], string, long)"/>'s
    /// wire encoding embeds <c>cond</c> (the <c>k</c> frame's condition
    /// field, and the <c>x</c> frame's digest) as a bare, non-length-
    /// prefixed field terminated by a newline — an unvalidated token
    /// (e.g. one forwarded from external input) could contain <c>\n</c>
    /// and smuggle an extra pipelined request onto the connection. Only
    /// called on the real digest path (<see cref="ReplaceAsync(byte[], byte[], string, byte[], long)"/>,
    /// <see cref="DeleteIfMatchesAsync(byte[], byte[], string)"/>) —
    /// never on the internal <see cref="CondAbsent"/>/<see cref="CondPresent"/>
    /// sentinels, which are fixed, safe constants, not caller
    /// input.</summary>
    private static void ValidateToken(string token)
    {
        bool valid = token is { Length: 32 };
        if (valid)
        {
            foreach (char c in token)
            {
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
                {
                    valid = false;
                    break;
                }
            }
        }
        if (!valid)
        {
            throw new ArgumentException(
                "nanocached: token must be a 32-character lowercase hex digest "
                    + "(from ContentDigest/GetWithTokenAsync), got: " + (token ?? "null"),
                nameof(token));
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
        // twice or looked up after it's already gone. A leg's own
        // completion callback may also remove it between the emptiness
        // check and the lookup, so the lookup itself must tolerate an
        // empty set (First() threw here — v0.3.0 .NET SDK).
        //
        // The emptiness check + removal is taken under _hedgedReadsLock
        // (issue #91): _closed was set above, and StartLeg checks _closed
        // under the same lock before registering, so once this observes the
        // set empty while holding the lock no further leg can be added — a
        // racing StartLeg would see _closed and throw. The blocking await
        // happens outside the lock so it never blocks a (doomed) StartLeg.
        while (true)
        {
            Task? leg;
            lock (_hedgedReadsLock)
            {
                leg = _hedgedReads.Keys.FirstOrDefault();
                if (leg is null)
                {
                    break;
                }
                _hedgedReads.TryRemove(leg, out _);
            }
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

    /// <summary>issue #105: (namespace, key) routes to owners via
    /// <see cref="HashRing.Owners(ReadOnlySpan{byte}, ReadOnlySpan{byte}, int)"/>
    /// — the empty namespace routes identically to the pre-namespace
    /// single-key form.</summary>
    private IReadOnlyList<string> OwnerNames(byte[] namespaceBytes, byte[] key)
    {
        lock (_stateLock)
        {
            return _ring!.Owners(namespaceBytes, key, _replication);
        }
    }

    /// <summary>
    /// Runs <paramref name="op"/> against the slot's connection, retrying
    /// once on a connection-level failure: a socket only learns of a peer
    /// FIN (e.g. the server's 60s idle timeout) on I/O, so lazy
    /// reconnect-on-use means the failed request poisons the connection,
    /// the redial replaces it, and the operation runs again. Safe because
    /// get/set/delete/clear are all idempotent — replaying one after a
    /// redial can never do anything worse than replaying it does anyway.
    ///
    /// <para>issue #225: NOT used for the primary leg of Incr/CAS/
    /// RemoveIfMatches — those are not idempotent (replaying a fully-sent
    /// Incr double-applies it; replaying a fully-sent CAS/conditional-delete
    /// can report an already-successful op as a mismatch). See
    /// <see cref="ApplyReconnectingNotIdempotentAsync{T}"/>.</para>
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

    /// <summary>
    /// issue #225 — internal marker exception: wraps a
    /// <see cref="ConnectionLostException"/> that must never be replayed,
    /// not even by <see cref="WithClusterRetryAsync{T}"/>'s own broader
    /// refresh-and-retry. Thrown only by
    /// <see cref="ApplyReconnectingNotIdempotentAsync{T}"/>, specifically so
    /// it passes UNCAUGHT through <see cref="WithClusterRetryAsync{T}"/>'s
    /// <c>catch (... or ConnectionLostException)</c> — that catch would
    /// otherwise retry the whole operation (refresh the ring, call the
    /// delegate again) exactly as blindly as the bug this issue fixes.
    /// Every public entry point whose primary leg can throw this
    /// (<see cref="IncrAsync(byte[], byte[], long)"/>, the private
    /// <c>CasAsync</c>, <see cref="DeleteIfMatchesAsync(byte[], byte[], string)"/>)
    /// unwraps it back to the inner <see cref="ConnectionLostException"/>
    /// before it can ever reach a caller — nothing outside this file ever
    /// sees this type.
    /// </summary>
    private sealed class UnsafeToReplayException : Exception
    {
        private readonly ConnectionLostException _inner;

        internal UnsafeToReplayException(ConnectionLostException inner) : base(inner.Message, inner)
        {
            _inner = inner;
        }

        internal ConnectionLostException Unwrap() => _inner;
    }

    /// <summary>
    /// issue #225 — the non-idempotent counterpart to
    /// <see cref="ApplyReconnectingAsync{T}"/>, used only for the primary
    /// leg of Incr/CAS/RemoveIfMatches (<see cref="IncrPrimaryThenReplicateAsync"/>,
    /// <see cref="CasPrimaryThenReplicateAsync"/>,
    /// <see cref="RemoveIfMatchesPrimaryThenReplicateAsync"/> — never the
    /// replica legs those forward as ordinary Set/Delete, which stay on the
    /// plain idempotent-safe method above). Blindly redialing and resending
    /// on ANY connection-level failure — what
    /// <see cref="ApplyReconnectingAsync{T}"/> does — is unsafe here: if the
    /// primary already applied the operation and only the reply was lost,
    /// replaying it double-applies an Incr, or turns an already-successful
    /// CAS/conditional-delete into a false mismatch.
    ///
    /// <para>Connection distinguishes the two failure shapes via
    /// <see cref="ConnectionLostException.RequestNotSent"/>: true means the
    /// request frame failed to write at all — the idle-FIN case, where the
    /// connection was already dead and the peer never received so much as a
    /// partial frame — so redialing and resending is exactly as safe as it
    /// is for Get/Set/Delete, and this method retries once, just like
    /// <see cref="ApplyReconnectingAsync{T}"/> does. False (the default on
    /// every other <see cref="ConnectionLostException"/> — a lost reply
    /// after a fully-written frame, a request timeout, a stream desync)
    /// means the request may already have reached the peer: this method
    /// does not retry, and wraps the exception in
    /// <see cref="UnsafeToReplayException"/> so
    /// <see cref="WithClusterRetryAsync{T}"/>'s own broader retry can't
    /// replay it either — the caller unwraps it back to the plain
    /// <see cref="ConnectionLostException"/> this SDK's exception contract
    /// promises.</para>
    /// </summary>
    private async Task<T> ApplyReconnectingNotIdempotentAsync<T>(string? slot, Func<Connection, Task<T>> op)
    {
        try
        {
            return await op(await SlotConnectionAsync(slot).ConfigureAwait(false)).ConfigureAwait(false);
        }
        catch (ConnectionLostException error) when (error.RequestNotSent)
        {
            try
            {
                return await op(await SlotConnectionAsync(slot).ConfigureAwait(false)).ConfigureAwait(false);
            }
            catch (ConnectionLostException retryError)
            {
                // The redial's own attempt failed too — whatever the
                // reason, this method never retries more than once (same
                // bound as ApplyReconnectingAsync), and this failure must
                // not be replayed further up either.
                throw new UnsafeToReplayException(retryError);
            }
        }
        catch (ConnectionLostException error)
        {
            throw new UnsafeToReplayException(error);
        }
    }

    private async Task<T> ReadAsync<T>(byte[] namespaceBytes, byte[] key, Func<Connection, Task<T>> op)
    {
        if (_ring is null)
        {
            return await ApplyReconnectingAsync(null, op).ConfigureAwait(false);
        }

        IReadOnlyList<string> names = OwnerNames(namespaceBytes, key);
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
            // Check _closed and register under _hedgedReadsLock so this
            // can't interleave with Close()'s drain observing the set empty
            // (issue #91): Close() sets _closed before its drain takes this
            // lock, so a leg that finds _closed here must not start — it
            // would dial against connections Teardown() is about to close
            // and never be awaited. A leg that passes the check is in
            // _hedgedReads before the lock is released, so the drain's next
            // locked snapshot sees it.
            lock (_hedgedReadsLock)
            {
                if (_closed) throw new AlreadyClosedException();
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
                // when started, above); issue #276: past
                // MaxInFlightHedgeLoserLegs concurrently detached legs,
                // leave the rest awaited synchronously here instead before
                // this propagates exactly as ReadAsync's own
                // WrongNodeException does.
                await ResolveHedgeLosersAsync(pending).ConfigureAwait(false);
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
                await ResolveHedgeLosersAsync(pending).ConfigureAwait(false);
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

    /// <summary>issue #276: <paramref name="tasks"/> is this read's own
    /// remaining legs once it has already decided its outcome via a
    /// different leg — normally left running detached in
    /// <see cref="_hedgedReads"/> for <see cref="Close"/> to eventually
    /// drain (they're already registered there, added by
    /// <c>StartLeg</c>, and self-remove via their own continuation). But
    /// past <see cref="MaxInFlightHedgeLoserLegs"/> concurrently detached
    /// legs — checked against <see cref="_hedgedReads"/>, which still
    /// counts <paramref name="tasks"/> themselves at this point — the
    /// rest are pulled out of the registry and awaited right here
    /// instead, the same "fall back to synchronous" shape
    /// <see cref="MaxInFlightBackgroundReplicaWrites"/> uses past its own
    /// cap, so a client issuing many concurrent hedged reads against a
    /// slow owner can't accumulate unbounded background legs. Outcomes
    /// are ignored either way: a loser's result was never going to be
    /// used.</summary>
    private async Task ResolveHedgeLosersAsync<T>(HashSet<Task<T>> tasks)
    {
        if (tasks.Count == 0) return;
        if (_hedgedReads.Count < MaxInFlightHedgeLoserLegs) return;
        foreach (Task<T> task in tasks)
        {
            _hedgedReads.TryRemove(task, out _);
        }
        await Task.WhenAll(tasks.Select(async task =>
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch
            {
                // Ignored — see doc comment above.
            }
        })).ConfigureAwait(false);
    }

    private async Task<T> WriteAsync<T>(byte[] namespaceBytes, byte[] key, Func<Connection, Task<T>> op)
    {
        if (_ring is null)
        {
            return await ApplyReconnectingAsync(null, op).ConfigureAwait(false);
        }

        IReadOnlyList<string> names = OwnerNames(namespaceBytes, key);
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

            // SDK proxy mode (issue #122): the single connection's redial
            // gets an extra fallback a cluster member's never needs — see
            // DialProxyWithFailoverAsync's doc comment.
            Connection fresh = slot is null && _viaProxy
                ? await DialProxyWithFailoverAsync(address).ConfigureAwait(false)
                : await DialWithCooldownAsync(address).ConfigureAwait(false);
            return InstallRedialedConnection(slot, fresh);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Installs <paramref name="fresh"/> — a connection this
    /// slot's redial just finished dialing — into <c>_single</c> or the
    /// named member, or discards it. Split out of
    /// <see cref="SlotConnectionAsync(string?)"/> so its guarded logic is
    /// independently testable (issue #330's regression test drives this
    /// directly, since forcing the underlying race deterministically from
    /// outside proved impractical — see that test's comment).</summary>
    private Connection InstallRedialedConnection(string? slot, Connection fresh)
    {
        lock (_stateLock)
        {
            if (_closed)
            {
                // Close() ran while we were reconnecting (issue #330):
                // mirrors RefreshNodeListAsync's/OpenNodeConnectionAsync's
                // same check — installing this connection now would leak
                // it and its read-loop task past Close() having already
                // returned.
                fresh.Close();
                throw new AlreadyClosedException();
            }
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

    /// <summary>SDK proxy mode (issue #122) reconnect: first retries
    /// <paramref name="address"/> — the currently connected proxy, which
    /// may simply have restarted — through the ordinary
    /// <see cref="DialWithCooldownAsync"/> (same per-address cooldown as
    /// every other redial). Only when that also fails does this re-fetch
    /// the proxy roster from discovery (<see cref="FetchProxyListAsync"/>)
    /// and fail over to another proxy chosen at random
    /// (<see cref="ConnectToAnyProxyAsync"/> — the same dial-and-pick
    /// <see cref="OpenProxyAsync"/>'s initial connect uses, not a second
    /// mechanism). On success, <see cref="_singleAddress"/> is updated to
    /// the winner so the *next* redial retries the new proxy first, same
    /// as this one did. An empty roster is reported with a message naming
    /// the situation, same wording as <see cref="ConnectAsync(Options)"/>'s
    /// own empty-roster error; every proxy in a non-empty roster being
    /// unreachable surfaces as the last dial error, same as any other
    /// connect failure.</summary>
    private async Task<Connection> DialProxyWithFailoverAsync(string address)
    {
        try
        {
            return await DialWithCooldownAsync(address).ConfigureAwait(false);
        }
        catch (NanocachedException sameProxyError)
        {
            IReadOnlyList<DiscoveredNode> proxies = await FetchProxyListAsync().ConfigureAwait(false);
            if (proxies.Count == 0)
            {
                throw new ConnectionLostException(
                    "nanocached: no proxies registered with discovery", sameProxyError);
            }

            (Connection connection, string newAddress) =
                await ConnectToAnyProxyAsync(proxies).ConfigureAwait(false);
            lock (_stateLock) { _singleAddress = newAddress; }

            // Issue #296: MaybeRefreshAsync's own cooldown prune
            // (RefreshNodeListAsync) never runs in proxy mode — it
            // early-returns while _ring stays null forever — so the
            // entry ArmReconnectCooldown left behind for `address` above
            // (the same-proxy retry that just failed) would otherwise sit
            // in _reconnectCooldowns forever: this client's own redial
            // path never dials it again once _singleAddress has moved on.
            // Unconditional, not gated on newAddress != address:
            // ConnectToAnyProxyAsync dials candidates directly, bypassing
            // _reconnectCooldowns entirely, so `address` could in
            // principle be the very entry that just won this failover —
            // in which case removing its now-stale cooldown record is
            // exactly as correct, since it was just proven reachable.
            _reconnectCooldowns.TryRemove(address, out _);
            return connection;
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
                Member departed = _members[name];
                departed.Connection?.Close();
                _members.Remove(name);
                // Node names are per-process UUIDs; a departed node's
                // redial gate would otherwise leak forever (issue #12).
                _redialGates.Remove(name);
                // Same leak, same reason, for the per-address cooldown map:
                // a departed node's address is never reused, so its cooldown
                // entry (if any) would otherwise linger forever (issue #96).
                _reconnectCooldowns.TryRemove(departed.Address, out _);
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

        // Dial every newly discovered node concurrently (issue #227):
        // this runs under _refreshGate, so a serial foreach here would
        // block refreshes/retries for N × connect-timeout on a scale-out
        // of N nodes. Every dial outcome is gathered first, then results
        // are installed under one lock — mirroring OpenClusterAsync.
        async Task<(DiscoveredNode Node, Connection? Connection, Exception? Error)> DialNodeAsync(
            DiscoveredNode node)
        {
            try
            {
                return (node, await OpenNodeConnectionAsync(node.Address).ConfigureAwait(false), null);
            }
            catch (NanocachedException error)
            {
                return (node, null, error);
            }
        }

        var outcomes = await Task.WhenAll(toOpen.Select(DialNodeAsync)).ConfigureAwait(false);

        lock (_stateLock)
        {
            if (_closed)
            {
                // Close() ran while we were dialing (issue #10):
                // installing these sockets now would leak them.
                foreach (var (_, connection, _) in outcomes)
                {
                    connection?.Close();
                }
                return;
            }

            foreach (var (node, connection, error) in outcomes)
            {
                if (connection is not null)
                {
                    _members[node.Name] = new Member(node.Address, connection);
                    continue;
                }

                Interlocked.Increment(ref _refreshFailures);
                // Install the just-discovered node with a null connection and
                // arm its cooldown, rather than leaving it out of the ring
                // (issue #67, matching OpenClusterAsync and the Go/Rust SDKs).
                // The HashRing ranks by the full candidate set, so dropping a
                // new node on a transient dial failure would make this
                // client's primary/replica choice for keys near it disagree
                // with every peer that DID reach it until the next refresh;
                // keeping it means its keys fail over per request instead. The
                // failure is still silent to the caller (refresh is best-effort)
                // and counted via Stats().RefreshFailures.
                _members[node.Name] = new Member(node.Address, null);
                ArmReconnectCooldown(node.Address, error!);
            }

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

    /// <summary>SDK proxy mode (issue #122): as <see cref="FetchNodeListAsync"/>,
    /// but walks <see cref="_addresses"/> for a discovery server willing
    /// to answer <c>Q</c> instead of <c>L</c>. Used by
    /// <see cref="DialProxyWithFailoverAsync"/>'s reconnect fallback; the
    /// initial connect fetches <c>Q</c> directly in
    /// <see cref="ConnectAsync(Options)"/>'s own loop instead, since that
    /// first fetch also needs the loop's own per-address bookkeeping
    /// (<c>_targetKey</c>, the forgotten-close warning). Unlike
    /// <see cref="FetchNodeListAsync"/>, an address that identifies as a
    /// plain cache node is not skipped as merely uninteresting: it means
    /// ViaProxy is pointed at the wrong kind of address, the same
    /// configuration error <see cref="ConnectAsync(Options)"/> fails fast
    /// on, so it is thrown here too rather than silently trying the next
    /// address.</summary>
    private async Task<IReadOnlyList<DiscoveredNode>> FetchProxyListAsync()
    {
        Exception? lastError = null;
        foreach (var (host, port) in _addresses)
        {
            Identify.Result identified;
            try
            {
                identified = await Identify
                    .ConnectAndIdentifyAsync(host, port, _authSecret, _tls, viaProxy: true)
                    .ConfigureAwait(false);
            }
            catch (Exception error) when (error is NanocachedException or IOException or System.Net.Sockets.SocketException)
            {
                Interlocked.Increment(ref _refreshFailures);
                lastError = error;
                continue;
            }

            switch (identified)
            {
                case Identify.NodeTarget node:
                    node.Stream.Dispose();
                    throw new NanocachedException(
                        $"nanocached: ViaProxy requires discovery addresses, but {host}:{port} "
                        + "identifies as a cache node");

                case Identify.ProxyListTarget proxies:
                    return proxies.Proxies;
            }
        }
        throw lastError ?? new NanocachedException(
            "nanocached: could not reach any discovery address for the proxy roster");
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

    // Hedged reads (Hedged reads), amended by issue #276: bounds how many
    // losing hedge legs may be left running detached in _hedgedReads at
    // once — past the cap, ReadHedgedAsync awaits the remaining losers
    // synchronously right there instead of leaving them detached, the
    // same "fall back to synchronous" shape MaxInFlightBackgroundReplicaWrites
    // uses past its own cap. Checked against _hedgedReads directly (not a
    // separate SemaphoreSlim, since legs must always be started for
    // correctness — only whether a decided read's losers stay detached is
    // gated) — a client issuing many concurrent hedged reads against a
    // slow owner can't accumulate unbounded background legs. Internal and
    // mutable only so tests can shrink it, mirroring
    // MaxInFlightBackgroundReplicaWrites.
    internal static int MaxInFlightHedgeLoserLegs = 32;


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
