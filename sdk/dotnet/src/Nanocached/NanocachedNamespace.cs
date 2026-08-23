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
        _client = client;
        _namespace = namespaceBytes;
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

    /// <summary>This handle's namespace as the raw bytes sent on the wire.</summary>
    public byte[] NamespaceBytes => _namespace;

    public Task<string?> GetAsync(string key) => _client.GetAsync(_namespace, key);

    /// <summary>Returns the value decoded as UTF-8, or <c>null</c> when
    /// the key is missing. A value that is not valid UTF-8 raises
    /// <see cref="System.Text.DecoderFallbackException"/> — use
    /// <see cref="GetBytesAsync(byte[])"/> for the raw bytes instead.</summary>
    public Task<string?> GetAsync(byte[] key) => _client.GetAsync(_namespace, key);

    public Task<byte[]?> GetBytesAsync(string key) => _client.GetBytesAsync(_namespace, key);

    /// <summary>Returns the raw value, or <c>null</c> when the key is
    /// missing. Transparently decompresses when the owning client's
    /// <c>Compress</c> is enabled (value compression). With
    /// <c>ReadRepair</c>, a clean miss probes the remaining owners before
    /// being accepted as final (read repair).</summary>
    public Task<byte[]?> GetBytesAsync(byte[] key) => _client.GetBytesAsync(_namespace, key);

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no expiry.</summary>
    public Task SetAsync(string key, string value, long ttlSeconds = 0) =>
        _client.SetAsync(_namespace, key, value, ttlSeconds);

    /// <summary><paramref name="ttlSeconds"/> of 0 (the default) means no
    /// expiry. Transparently compresses values at or above the owning
    /// client's <c>CompressionThreshold</c> when <c>Compress</c> is
    /// enabled (value compression).</summary>
    public Task SetAsync(byte[] key, byte[] value, long ttlSeconds = 0) =>
        _client.SetAsync(_namespace, key, value, ttlSeconds);

    public Task<bool> DeleteAsync(string key) => _client.DeleteAsync(_namespace, key);

    /// <summary>Returns whether the key existed before this call.</summary>
    public Task<bool> DeleteAsync(byte[] key) => _client.DeleteAsync(_namespace, key);

    /// <summary>issue #106: drops every entry in this handle's namespace —
    /// on the empty namespace (<c>client.Namespace("")</c>), clears the
    /// default namespace (<c>c 0\n</c>), not rejected. Fanned out to every
    /// node in the owning client's current node list, with a
    /// refresh-once-and-retry on any node failure and never a silent
    /// partial clear — see <see cref="NanocachedClient.ClearAllAsync"/>'s
    /// doc comment for the shared fan-out/retry logic this and it both
    /// use.</summary>
    public Task ClearAsync() => _client.ClearAsync(_namespace);
}
