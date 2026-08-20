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
    }

    private static readonly TimeSpan NodeListStaleAfter = TimeSpan.FromSeconds(30);
    // The server rejects empty keys, so the keep-alive G needs one byte.
    private static readonly byte[] KeepaliveKey = { 0 };

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
    // doc/adr/0014-*.md: bounds in-flight background replica writes and
    // lets Close() drain them before tearing down connections.
    private readonly SemaphoreSlim _backgroundReplicaPermits;
    private readonly ConcurrentDictionary<Task, byte> _backgroundReplicaWrites = new();
    private readonly CancellationTokenSource _lifetime = new();

    private volatile bool _closed;
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
        _backgroundReplicaPermits = new SemaphoreSlim(MaxInFlightBackgroundReplicaWrites, MaxInFlightBackgroundReplicaWrites);
        _readRepair = options.ReadRepair;
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
            RemoteCertificateValidationCallback = (_, certificate, _, _) =>
            {
                if (certificate is null) return false;
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
                        client._single = client.NewConnection(node.Stream);
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
    private Connection NewConnection(Stream stream)
    {
        string key = _targetKey!;
        IncrementOpenTarget(key);
        return new Connection(stream, () => DecrementOpenTarget(key));
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
    /// value is returned, and — detached, not awaited, no tracking —
    /// that same value repairs the true primary in the background, with
    /// TTL <see cref="ReadRepairTtlSeconds"/> (the original TTL can't be
    /// recovered from a GET, and TTL 0 would permanently resurrect
    /// already-expired data). Every failure along the way (connection
    /// lost, WrongNode, another miss) is swallowed; nothing here may turn
    /// an already-accepted miss into an error.</summary>
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

            if (names.Count > 0)
            {
                string primary = names[0];
                byte[] repairValue = value;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await ApplyReconnectingAsync<object?>(primary, async connection =>
                        {
                            await connection.SetAsync(key, repairValue, ReadRepairTtlSeconds).ConfigureAwait(false);
                            return null;
                        }).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Swallowed by design — see the doc comment.
                    }
                });
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
        await BeforeOperationAsync().ConfigureAwait(false);
        return await WithClusterRetryAsync(
            () => WriteAsync(key, connection => connection.DeleteAsync(key))).ConfigureAwait(false);
    }

    /// <summary>Idempotent; later operations throw <see cref="AlreadyClosedException"/>.
    /// A second call warns to stderr instead of erroring — usually a sign
    /// the caller lost track of this instance's lifecycle.</summary>
    public void Close()
    {
        if (_closed)
        {
            Console.Error.WriteLine("nanocached: close() called again on an already-closed client");
            return;
        }
        _closed = true;
        _lifetime.Cancel();
        // doc/adr/0014-*.md: give background replica writes a chance to
        // finish before their connections are torn out from under them.
        // Bounded by MaxInFlightBackgroundReplicaWrites, so this is a
        // short wait in practice.
        Task.WaitAll(_backgroundReplicaWrites.Keys.ToArray());
        Teardown();
    }

    public void Dispose() => Close();

    private void Teardown()
    {
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
            catch (AlreadyClosedException)
            {
                // A Close() racing this read should fail fast, not walk
                // the remaining owners (issue #12).
                throw;
            }
            catch (Exception error) when (error is NanocachedException)
            {
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
        // the write.
        async Task ReplicaWriteAsync(string name)
        {
            try
            {
                await ApplyReconnectingAsync(name, op).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Swallowed by design — see above.
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
                _backgroundReplicaWrites[background] = 0;
                _ = background.ContinueWith(
                    completed =>
                    {
                        _backgroundReplicaWrites.TryRemove(completed, out _);
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

            Connection fresh = await OpenNodeConnectionAsync(address).ConfigureAwait(false);
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
        return NewConnection(node.Stream);
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
                // Left out of the ring for now; the next refresh retries
                // it. Silent by design (§7 ②) — behavior is unaffected.
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
                // Silent by design (§7 ②) — the next refresh retries.
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
