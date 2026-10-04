namespace Nanocached.Caching;

/// <summary>
/// Options for the <see cref="ServiceCollectionExtensions.AddNanocachedDistributedCache(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Action{NanocachedCacheOptions})"/>
/// overload that connects its own <see cref="NanocachedClient"/>.
/// Deliberately small — issue #108's shared spec — the connection basics
/// plus the options a secret-configured deployment cannot do without (TLS,
/// a private CA, value compression: the same set the Django adapter's
/// <c>OPTIONS</c> gained in issue #231). Everything else (hedged reads,
/// read repair, ...) is a client-level concern: an application that needs
/// it builds its own <see cref="NanocachedClient"/> via
/// <see cref="NanocachedClient.Options"/> and reuses it through the other
/// <c>AddNanocachedDistributedCache</c> overload instead.
/// </summary>
public sealed class NanocachedCacheOptions
{
    /// <summary>The namespace this adapter binds to when none is
    /// configured — one nanocached namespace shared by every application
    /// that doesn't ask for its own.</summary>
    public const string DefaultNamespace = "distributed-cache";

    /// <summary><c>"host:port"</c> targets, tried in order — see
    /// <see cref="NanocachedClient.Options.Addresses"/>. Required (and only
    /// consulted) by the overload that connects its own client.</summary>
    public List<string> Addresses { get; } = new();

    /// <summary>Shared secret matching <c>NANOCACHED_AUTH_SECRET</c> on the
    /// server. <c>null</c> or empty means no auth, matching
    /// <see cref="NanocachedClient.Options.AuthSecret"/>.</summary>
    public string? Secret { get; set; }

    /// <summary>Connect over TLS — see <see cref="NanocachedClient.Options.Tls"/>.
    /// Off by default (plaintext), matching the SDK. A shared secret sent
    /// without this is sent in the clear.</summary>
    public bool Tls { get; set; }

    /// <summary>Path to a PEM file of trusted root certificate(s) for a
    /// private CA — see <see cref="NanocachedClient.Options.Ca"/>. Only
    /// meaningful with <see cref="Tls"/>; setting it without
    /// <see cref="Tls"/> is rejected when the cache is first resolved,
    /// exactly as the SDK rejects it.</summary>
    public string? Ca { get; set; }

    /// <summary>Transparently compress values above
    /// <see cref="CompressionThreshold"/> — see
    /// <see cref="NanocachedClient.Options.Compress"/>. Off by default.
    /// <b>Every client reading or writing this cache's keys must agree on
    /// it</b>: it is a per-keyspace format decision, not a per-client
    /// preference.</summary>
    public bool Compress { get; set; }

    /// <summary>Values shorter than this (in bytes) are never compressed;
    /// only meaningful with <see cref="Compress"/>. Defaults to the SDK's
    /// own default (256) — see
    /// <see cref="NanocachedClient.Options.CompressionThreshold"/>.</summary>
    public int CompressionThreshold { get; set; } = 256;

    /// <summary>The nanocached namespace this cache instance binds to.
    /// Issue #108's shared spec: one adapter instance binds to exactly one
    /// namespace — two instances with different namespaces are fully
    /// isolated, even over the same keys, since namespaces enter cluster
    /// routing (they mix into the hash used to pick a key's owners).</summary>
    public string Namespace { get; set; } = DefaultNamespace;
}
