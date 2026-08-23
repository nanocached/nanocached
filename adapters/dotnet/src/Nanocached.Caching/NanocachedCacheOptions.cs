namespace Nanocached.Caching;

/// <summary>
/// Options for the <see cref="ServiceCollectionExtensions.AddNanocachedDistributedCache(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Action{NanocachedCacheOptions})"/>
/// overload that connects its own <see cref="NanocachedClient"/>.
/// Deliberately minimal — issue #108's shared spec — everything else
/// (compression, hedged reads, TLS, read repair, ...) is a client-level
/// concern: an application that needs any of it builds its own
/// <see cref="NanocachedClient"/> via <see cref="NanocachedClient.Options"/>
/// and reuses it through the other <c>AddNanocachedDistributedCache</c>
/// overload instead.
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

    /// <summary>The nanocached namespace this cache instance binds to.
    /// Issue #108's shared spec: one adapter instance binds to exactly one
    /// namespace — two instances with different namespaces are fully
    /// isolated, even over the same keys, since namespaces enter cluster
    /// routing (they mix into the hash used to pick a key's owners).</summary>
    public string Namespace { get; set; } = DefaultNamespace;
}
