using System.Net.Security;
using System.Text;

namespace Nanocached;

/// <summary>
/// The public client. A host/port (or a seeds list) may name either a
/// single nanocached-node or discovery server(s) fronting a cluster —
/// <see cref="ConnectAsync(Options)"/> finds out from the server's own handshake
/// response (doc/adr/0007-*.md), so calling code is identical either way.
///
/// Cluster mode implements ADR-0011 client-side replication: writes fan
/// out to each key's top-R owners (the primary's result decides; a dead
/// replica never fails a write), reads ask the primary and fall over to
/// the next owner only when the holder is unreachable. Dead connections
/// are redialed lazily on use (with one transparent retry — a socket only
/// learns of a peer FIN on I/O, and every operation is idempotent), and an
/// opt-in keep-alive can hold connections open across the server's 30s
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
        internal List<(string Host, int Port)> Seeds { get; } = new();
        internal byte[]? AuthSecretBytes { get; private set; }
        internal SslClientAuthenticationOptions? TlsOptions { get; private set; }
        internal TimeSpan? KeepAlive { get; private set; }

        /// <summary>Adds a target; call repeatedly to list discovery
        /// replicas (ADR-0010), tried in order for connect and every
        /// refresh.</summary>
        public Options Host(string host, int port)
        {
            Seeds.Add((host, port));
            return this;
        }

        /// <summary>Shared secret matching NANOCACHED_AUTH_SECRET on the server.</summary>
        public Options AuthSecret(string secret)
        {
            AuthSecretBytes = Encoding.UTF8.GetBytes(secret);
            return this;
        }

        /// <summary>Connect over TLS with these options (system trust by
        /// default; set a validation callback for a private CA).</summary>
        public Options Tls(SslClientAuthenticationOptions options)
        {
            TlsOptions = options;
            return this;
        }

        /// <summary>Opt-in keep-alive; pick something below the server's
        /// 30s idle timeout.</summary>
        public Options KeepAliveInterval(TimeSpan interval)
        {
            if (interval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(interval), "nanocached: KeepAliveInterval must be positive");
            }
            KeepAlive = interval;
            return this;
        }
    }

    private static readonly TimeSpan NodeListStaleAfter = TimeSpan.FromSeconds(30);
    // The server rejects empty keys, so the keep-alive G needs one byte.
    private static readonly byte[] KeepaliveKey = { 0 };

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

    private readonly object _stateLock = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly Dictionary<string, SemaphoreSlim> _redialGates = new();
    private readonly List<(string Host, int Port)> _seeds;
    private readonly byte[]? _authSecret;
    private readonly SslClientAuthenticationOptions? _tls;
    private readonly CancellationTokenSource _lifetime = new();

    private volatile bool _closed;
    private Connection? _single;
    private string? _singleAddress;
    private readonly Dictionary<string, Member> _members = new();
    private HashRing? _ring;
    private int _replication = 1;
    private DateTime _lastFetch = DateTime.UtcNow;

    private NanocachedClient(Options options)
    {
        _seeds = options.Seeds.ToList();
        _authSecret = options.AuthSecretBytes;
        _tls = options.TlsOptions;
    }

    public static Task<NanocachedClient> ConnectAsync(string host, int port) =>
        ConnectAsync(new Options().Host(host, port));

    public static async Task<NanocachedClient> ConnectAsync(Options options)
    {
        if (options.Seeds.Count == 0)
        {
            throw new ArgumentException(
                "nanocached: ConnectAsync() needs at least one host/port", nameof(options));
        }

        var client = new NanocachedClient(options);

        // Walk the seeds until one yields a working target; a seed that is
        // unreachable, warming up (B, ADR-0010), or knows no live nodes is
        // skipped — the next replica may do better.
        Exception? lastError = null;
        foreach (var (host, port) in client._seeds)
        {
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
                        if (client._seeds.Count > 1)
                        {
                            Console.Error.WriteLine(
                                $"nanocached: {host}:{port} is a cache node, so this client is pinned "
                                + "to that single server — the remaining seed(s) will not be used. "
                                + "Point seeds at discovery servers for cluster routing and failover.");
                        }
                        client._single = new Connection(node.Stream);
                        client._singleAddress = $"{host}:{port}";
                        client.StartKeepAlive(options.KeepAlive);
                        return client;

                    case Identify.ClusterTarget cluster when cluster.Nodes.Count == 0:
                        lastError = new NanocachedException(
                            $"nanocached: no live nodes registered with the discovery server at {host}:{port}");
                        continue;

                    case Identify.ClusterTarget cluster:
                        await client.OpenClusterAsync(cluster).ConfigureAwait(false);
                        client.StartKeepAlive(options.KeepAlive);
                        return client;
                }
            }
            catch
            {
                client.Teardown();
                throw;
            }
        }

        throw lastError ?? new NanocachedException("nanocached: could not connect to any seed");
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

    public Task<byte[]?> GetAsync(string key) => GetAsync(Encoding.UTF8.GetBytes(key));

    /// <summary>Returns the value, or <c>null</c> when the key is missing.</summary>
    public async Task<byte[]?> GetAsync(byte[] key)
    {
        await BeforeOperationAsync().ConfigureAwait(false);
        return await WithClusterRetryAsync(
            () => ReadAsync(key, connection => connection.GetAsync(key))).ConfigureAwait(false);
    }

    public Task SetAsync(string key, string value, long? ttlSeconds = null) =>
        SetAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ttlSeconds);

    public async Task SetAsync(byte[] key, byte[] value, long? ttlSeconds = null)
    {
        if (ttlSeconds is < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ttlSeconds), $"nanocached: ttlSeconds must be non-negative, got {ttlSeconds}");
        }
        await BeforeOperationAsync().ConfigureAwait(false);
        await WithClusterRetryAsync<object?>(async () =>
        {
            await WriteAsync<object?>(key, async connection =>
            {
                await connection.SetAsync(key, value, ttlSeconds).ConfigureAwait(false);
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

    /// <summary>Idempotent; later operations throw <see cref="AlreadyClosedException"/>.</summary>
    public void Close()
    {
        if (_closed) return;
        _closed = true;
        _lifetime.Cancel();
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
    /// FIN (e.g. the server's 30s idle timeout) on I/O, so lazy
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
        var replicaWrites = names.Skip(1).Select(async name =>
        {
            try
            {
                await ApplyReconnectingAsync(name, op).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Swallowed by design — see above.
            }
        }).ToList();

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
        return new Connection(node.Stream);
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
            catch (Exception error) when (error is NanocachedException)
            {
                Console.Error.WriteLine(
                    $"nanocached: could not connect to new node {node.Address}, will retry: {error.Message}");
            }
        }

        lock (_stateLock)
        {
            _ring = new HashRing(_members.Keys.ToList());
            _replication = cluster.Replication;
        }
    }

    /// <summary>Walks every seed (ADR-0010); <c>null</c> means keep the last-known list.</summary>
    private async Task<Identify.ClusterTarget?> FetchNodeListAsync()
    {
        foreach (var (host, port) in _seeds)
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
                        Console.Error.WriteLine(
                            $"nanocached: discovery at {host}:{port} returned no live nodes, skipping");
                        continue;
                    case Identify.NodeTarget node:
                        node.Stream.Dispose();
                        Console.Error.WriteLine(
                            $"nanocached: {host}:{port} no longer identifies as a discovery server");
                        continue;
                }
            }
            catch (Exception error) when (error is NanocachedException or IOException or System.Net.Sockets.SocketException)
            {
                Console.Error.WriteLine(
                    $"nanocached: could not refresh the node list from {host}:{port}: {error.Message}");
            }
        }
        Console.Error.WriteLine(
            "nanocached: no discovery seed could provide a node list, keeping the last-known list");
        return null;
    }

    // ── keep-alive ────────────────────────────────────────────────

    private void StartKeepAlive(TimeSpan? interval)
    {
        if (interval is not { } every) return;

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
