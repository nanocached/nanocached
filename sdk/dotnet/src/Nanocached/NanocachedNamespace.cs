using System.Text;

namespace Nanocached;

/// <summary>
/// issue #105 — first-class namespaces: a lightweight handle scoping every
/// operation to one namespace. A namespace is a flat, opaque byte string —
/// no delimiter, no escaping, no hierarchy (see docs/protocol.html's
/// "g / s / d — namespaced get, set, delete" section) — so the same key
/// name in two namespaces is two independent entries, routed to
/// (possibly) different owners: HRW hashing mixes the namespace into the
/// key-side hash (<see cref="HashRing.KeyHash"/>), which is what keeps a
/// common singleton key name (e.g. <c>config</c>) shared across many
/// namespaces from piling onto the same few nodes.
///
/// <para>Obtained from <see cref="NanocachedClient.Namespace(byte[])"/> /
/// <see cref="NanocachedClient.Namespace(string)"/>. Cheap — shares the
/// owning client's connections, and nothing is dialed or allocated beyond
/// this small wrapper — and forwards every call to the client's own
/// internal (namespace, key) methods, so it duplicates none of the
/// client's networking: routing, replication fan-out, hedged reads,
/// <c>W</c> refresh-and-retry, response tags, compression, and error types
/// are all identical to calling the client directly. Invalid once the
/// owning client is closed — every method then throws the same
/// <see cref="AlreadyClosedException"/> the client's own methods
/// raise.</para>
///
/// <para><c>client.Namespace("")</c> (the empty namespace) returns a
/// handle equivalent to the client itself: it sends the legacy
/// <c>G</c>/<c>S</c>/<c>D</c> frames, byte-for-byte, and hashes exactly as
/// an un-namespaced key always has — so a namespace-unaware deployment
/// reached through this handle is wire-indistinguishable from one reached
/// through the client directly.</para>
/// </summary>
public sealed class NanocachedNamespace
{
    private readonly NanocachedClient _client;
    private readonly byte[] _namespace;

    internal NanocachedNamespace(NanocachedClient client, byte[] namespaceBytes)
    {
        ArgumentNullException.ThrowIfNull(namespaceBytes);
        _client = client;
        // Issue #1182: copy, so a caller that reuses or overwrites the
        // buffer it passed in cannot change the HRW input and the
        // namespace of every later call through this handle.
        _namespace = (byte[])namespaceBytes.Clone();
    }

    /// <summary>This handle's namespace, decoded as UTF-8. Exposed for the
    /// framework adapters built on top of namespaces (issue #107/#108),
    /// which need to know which namespace a handle addresses. Only
    /// meaningful when the namespace was itself constructed from valid
    /// UTF-8 — a namespace built from arbitrary bytes (see
    /// <see cref="NanocachedClient.Namespace(byte[])"/>) may not decode
    /// cleanly; use <see cref="NamespaceBytes"/> for the raw form in that
    /// case.</summary>
    public string Namespace => Encoding.UTF8.GetString(_namespace);

    /// <summary>This handle's namespace as the raw bytes sent on the wire.
    /// A copy: changing the returned array does not change the handle.</summary>
    public byte[] NamespaceBytes => (byte[])_namespace.Clone();

    public Task<string?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<string?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetAsync(_namespace, key), cancellationToken);

    /// <summary>Returns the value decoded as UTF-8, or <c>null</c> when
    /// the key is missing. A value that is not valid UTF-8 raises
    /// <see cref="System.Text.DecoderFallbackException"/> — use
    /// <see cref="GetBytesAsync(byte[], CancellationToken)"/> for the raw bytes instead.</summary>
    public Task<string?> GetAsync(byte[] key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<string?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetAsync(_namespace, _client.OwnReadKey(key, cancellationToken)), cancellationToken);

    public Task<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<byte[]?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetBytesAsync(_namespace, key), cancellationToken);

    /// <summary>Returns the raw value, or <c>null</c> when the key is
    /// missing. Transparently decompresses when the owning client's
    /// <c>Compress</c> is enabled (value compression). With
    /// <c>ReadRepair</c>, a clean miss probes the remaining owners before
    /// being accepted as final (read repair).</summary>
    public Task<byte[]?> GetBytesAsync(byte[] key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<byte[]?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetBytesAsync(_namespace, _client.OwnReadKey(key, cancellationToken)), cancellationToken);

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no expiry.</summary>
    public Task SetAsync(string key, string value, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled(cancellationToken)
        : Cancellation.Abandonable(_client.SetAsync(_namespace, key, value, ttlSeconds), cancellationToken);

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no
    /// expiry. Transparently compresses values at or above the owning
    /// client's <c>CompressionThreshold</c> when <c>Compress</c> is
    /// enabled (value compression).</summary>
    public Task SetAsync(byte[] key, byte[] value, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled(cancellationToken)
        : Cancellation.Abandonable(_client.SetAsync(_namespace, Cancellation.Own(key, cancellationToken), Cancellation.Own(value, cancellationToken), ttlSeconds), cancellationToken);

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.DeleteAsync(_namespace, key), cancellationToken);

    /// <summary>Returns whether the key existed before this call.</summary>
    public Task<bool> DeleteAsync(byte[] key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.DeleteAsync(_namespace, Cancellation.Own(key, cancellationToken)), cancellationToken);

    /// <summary>issue #825: as <see cref="NanocachedClient.TouchAsync(string, long?, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<bool> TouchAsync(string key, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.TouchAsync(_namespace, key, ttlSeconds), cancellationToken);

    /// <summary>issue #825: as <see cref="TouchAsync(string, long?, CancellationToken)"/>.</summary>
    public Task<bool> TouchAsync(byte[] key, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.TouchAsync(_namespace, Cancellation.Own(key, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #825: as <see cref="NanocachedClient.GetAndTouchAsync(string, long?, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<string?> GetAndTouchAsync(string key, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<string?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetAndTouchAsync(_namespace, key, ttlSeconds), cancellationToken);

    /// <summary>issue #825: as <see cref="GetAndTouchAsync(string, long?, CancellationToken)"/>.</summary>
    public Task<string?> GetAndTouchAsync(byte[] key, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<string?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetAndTouchAsync(_namespace, Cancellation.Own(key, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #825: as <see cref="NanocachedClient.GetAndTouchBytesAsync(string, long?, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<byte[]?> GetAndTouchBytesAsync(string key, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<byte[]?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetAndTouchBytesAsync(_namespace, key, ttlSeconds), cancellationToken);

    /// <summary>issue #825: as <see cref="GetAndTouchBytesAsync(string, long?, CancellationToken)"/>.</summary>
    public Task<byte[]?> GetAndTouchBytesAsync(byte[] key, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<byte[]?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetAndTouchBytesAsync(_namespace, Cancellation.Own(key, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #871: as <see cref="NanocachedClient.TouchManyAsync(IReadOnlyList{string}, long?, CancellationToken)"/>,
    /// scoped to this namespace.</summary>
    public Task<Dictionary<string, bool>> TouchManyAsync(IReadOnlyList<string> keys, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<Dictionary<string, bool>>(cancellationToken)
        : Cancellation.Abandonable(_client.TouchManyAsync(_namespace, keys, ttlSeconds), cancellationToken);

    /// <summary>issue #871: as <see cref="NanocachedClient.GetManyAndTouchAsync(IReadOnlyList{string}, long?, CancellationToken)"/>,
    /// scoped to this namespace.</summary>
    public Task<Dictionary<string, string>> GetManyAndTouchAsync(IReadOnlyList<string> keys, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<Dictionary<string, string>>(cancellationToken)
        : Cancellation.Abandonable(_client.GetManyAndTouchAsync(_namespace, keys, ttlSeconds), cancellationToken);

    /// <summary>issue #871: as <see cref="NanocachedClient.GetManyAndTouchBytesAsync(IReadOnlyList{string}, long?, CancellationToken)"/>,
    /// scoped to this namespace.</summary>
    public Task<Dictionary<string, byte[]>> GetManyAndTouchBytesAsync(IReadOnlyList<string> keys, long? ttlSeconds, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<Dictionary<string, byte[]>>(cancellationToken)
        : Cancellation.Abandonable(_client.GetManyAndTouchBytesAsync(_namespace, keys, ttlSeconds), cancellationToken);

    /// <summary>issue #151: as <see cref="NanocachedClient.GetManyAsync(IReadOnlyList{string}, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<Dictionary<string, string>> GetManyAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<Dictionary<string, string>>(cancellationToken)
        : Cancellation.Abandonable(_client.GetManyAsync(_namespace, keys), cancellationToken);

    /// <summary>issue #151: as <see cref="NanocachedClient.GetManyBytesAsync(IReadOnlyList{string}, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<Dictionary<string, byte[]>> GetManyBytesAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<Dictionary<string, byte[]>>(cancellationToken)
        : Cancellation.Abandonable(_client.GetManyBytesAsync(_namespace, keys), cancellationToken);

    /// <summary>issue #151: as
    /// <see cref="NanocachedClient.SetManyAsync(IReadOnlyDictionary{string, string}, long, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task SetManyAsync(IReadOnlyDictionary<string, string> values, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled(cancellationToken)
        : Cancellation.Abandonable(_client.SetManyAsync(_namespace, values, ttlSeconds), cancellationToken);

    /// <summary>issue #151: as
    /// <see cref="NanocachedClient.SetManyBytesAsync(IReadOnlyDictionary{string, byte[]}, long, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task SetManyBytesAsync(IReadOnlyDictionary<string, byte[]> values, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled(cancellationToken)
        : Cancellation.Abandonable(_client.SetManyBytesAsync(_namespace, Cancellation.OwnValues(values, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #129: as <see cref="NanocachedClient.IncrAsync(byte[], long, CancellationToken)"/>,
    /// scoped to this handle's namespace. As volatile as
    /// <see cref="SetAsync(byte[], byte[], long, CancellationToken)"/> — not for durable
    /// counts — and, in a cluster, only the primary owner runs the
    /// increment; replicas receive the resulting value via <c>Set</c>. Throws
    /// <see cref="CompressionIncompatibleException"/> immediately when this
    /// client has <c>Compress</c> enabled (issue #321). See the client
    /// method's own doc comment for the full replication design.</summary>
    public Task<long?> IncrAsync(string key, long delta, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<long?>(cancellationToken)
        : Cancellation.Abandonable(_client.IncrAsync(_namespace, key, delta), cancellationToken);

    public Task<long?> IncrAsync(byte[] key, long delta, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<long?>(cancellationToken)
        : Cancellation.Abandonable(_client.IncrAsync(_namespace, Cancellation.Own(key, cancellationToken), delta), cancellationToken);

    /// <summary>issue #129: <see cref="IncrAsync(byte[], long, CancellationToken)"/> with
    /// <paramref name="delta"/> negated — never a different wire op.</summary>
    public Task<long?> DecrAsync(string key, long delta, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<long?>(cancellationToken)
        : Cancellation.Abandonable(_client.DecrAsync(_namespace, key, delta), cancellationToken);

    public Task<long?> DecrAsync(byte[] key, long delta, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<long?>(cancellationToken)
        : Cancellation.Abandonable(_client.DecrAsync(_namespace, Cancellation.Own(key, cancellationToken), delta), cancellationToken);

    /// <summary>issue #141 — compare-and-set: as
    /// <see cref="NanocachedClient.GetWithTokenAsync(string, CancellationToken)"/>, scoped to
    /// this handle's namespace.</summary>
    public Task<(string Value, string Token)?> GetWithTokenAsync(string key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<(string Value, string Token)?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetWithTokenAsync(_namespace, key), cancellationToken);

    public Task<(string Value, string Token)?> GetWithTokenAsync(byte[] key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<(string Value, string Token)?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetWithTokenAsync(_namespace, _client.OwnReadKey(key, cancellationToken)), cancellationToken);

    /// <summary>issue #141: as
    /// <see cref="NanocachedClient.GetBytesWithTokenAsync(string, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<(byte[] Value, string Token)?> GetBytesWithTokenAsync(string key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<(byte[] Value, string Token)?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetBytesWithTokenAsync(_namespace, key), cancellationToken);

    public Task<(byte[] Value, string Token)?> GetBytesWithTokenAsync(byte[] key, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<(byte[] Value, string Token)?>(cancellationToken)
        : Cancellation.Abandonable(_client.GetBytesWithTokenAsync(_namespace, _client.OwnReadKey(key, cancellationToken)), cancellationToken);

    /// <summary>issue #141 — <c>add</c>/<c>putIfAbsent</c>: as
    /// <see cref="NanocachedClient.PutIfAbsentAsync(string, string, long, CancellationToken)"/>,
    /// scoped to this handle's namespace. See the client method's own doc
    /// comment for the not-a-lock caveat and cluster replication rule.</summary>
    public Task<bool> PutIfAbsentAsync(string key, string value, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.PutIfAbsentAsync(_namespace, key, value, ttlSeconds), cancellationToken);

    public Task<bool> PutIfAbsentAsync(byte[] key, byte[] value, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.PutIfAbsentAsync(_namespace, Cancellation.Own(key, cancellationToken), Cancellation.Own(value, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #141 — the two-argument <c>replace(key, value)</c>:
    /// as <see cref="NanocachedClient.ReplaceIfPresentAsync(string, string, long, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<bool> ReplaceIfPresentAsync(string key, string value, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.ReplaceIfPresentAsync(_namespace, key, value, ttlSeconds), cancellationToken);

    public Task<bool> ReplaceIfPresentAsync(byte[] key, byte[] value, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.ReplaceIfPresentAsync(_namespace, Cancellation.Own(key, cancellationToken), Cancellation.Own(value, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #141 — the three-argument <c>replace(key, old, new)</c>:
    /// as <see cref="NanocachedClient.ReplaceAsync(string, string, string, long, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<bool> ReplaceAsync(string key, string token, string newValue, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.ReplaceAsync(_namespace, key, token, newValue, ttlSeconds), cancellationToken);

    public Task<bool> ReplaceAsync(byte[] key, string token, byte[] newValue, long ttlSeconds = 0, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.ReplaceAsync(_namespace, Cancellation.Own(key, cancellationToken), token, Cancellation.Own(newValue, cancellationToken), ttlSeconds), cancellationToken);

    /// <summary>issue #141 — the two-argument <c>remove(key, old)</c>: as
    /// <see cref="NanocachedClient.DeleteIfMatchesAsync(string, string, CancellationToken)"/>,
    /// scoped to this handle's namespace.</summary>
    public Task<bool> DeleteIfMatchesAsync(string key, string token, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.DeleteIfMatchesAsync(_namespace, key, token), cancellationToken);

    public Task<bool> DeleteIfMatchesAsync(byte[] key, string token, CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled<bool>(cancellationToken)
        : Cancellation.Abandonable(_client.DeleteIfMatchesAsync(_namespace, Cancellation.Own(key, cancellationToken), token), cancellationToken);

    /// <summary>issue #106: drops every entry in this handle's namespace —
    /// on the empty namespace (<c>client.Namespace("")</c>), clears the
    /// default namespace (<c>c 0\n</c>), not rejected. Fanned out to every
    /// node in the owning client's current node list, with a
    /// refresh-once-and-retry on any node failure and never a silent
    /// partial clear — see <see cref="NanocachedClient.ClearAllAsync"/>'s
    /// doc comment for the shared fan-out/retry logic this and it both
    /// use.</summary>
    public Task ClearAsync(CancellationToken cancellationToken = default) =>
        cancellationToken.IsCancellationRequested
        ? Task.FromCanceled(cancellationToken)
        : Cancellation.Abandonable(_client.ClearAsync(_namespace), cancellationToken);
}
