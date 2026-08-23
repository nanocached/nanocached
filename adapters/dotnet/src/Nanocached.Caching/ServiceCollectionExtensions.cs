using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Nanocached.Caching;

/// <summary>DI wiring for <see cref="NanocachedDistributedCache"/> — issue
/// #108. Two overloads, matching the two ways an application already deals
/// with a <see cref="NanocachedClient"/>: connect one just for this cache,
/// or reuse one the application registered itself.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Registers a singleton <see cref="IDistributedCache"/> that
    /// connects — and owns — its own <see cref="NanocachedClient"/>, built
    /// from <paramref name="configureOptions"/>. The connection is made
    /// lazily, the first time something resolves <see cref="IDistributedCache"/>
    /// (a synchronous <c>NanocachedClient.ConnectAsync(...).GetAwaiter().GetResult()</c>
    /// inside the factory — DI singleton factories are synchronous, and
    /// resolving this service is the whole reason to connect, so whichever
    /// caller triggers it already pays a network round trip either
    /// way).
    ///
    /// <para>The client this registers is closed automatically when the
    /// container is disposed: the concrete singleton instance created for
    /// <see cref="IDistributedCache"/> implements <see cref="IDisposable"/>
    /// for exactly this purpose, and the built-in
    /// <c>Microsoft.Extensions.DependencyInjection</c> container disposes
    /// any <see cref="IDisposable"/>/<see cref="IAsyncDisposable"/>
    /// instance it creates for a singleton registration — regardless of
    /// the service type resolution asked for — when the container itself
    /// is disposed. (The bare <see cref="NanocachedClient"/> is
    /// deliberately never registered as its own container-tracked
    /// singleton here — doing so would give the container two independent
    /// reasons to close the same client; harmless, since
    /// <see cref="NanocachedClient.Close"/> is idempotent, but it would
    /// log a stray "closed again" warning.)</para></summary>
    public static IServiceCollection AddNanocachedDistributedCache(
        this IServiceCollection services, Action<NanocachedCacheOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);
        services.AddSingleton<IDistributedCache>(provider =>
        {
            NanocachedCacheOptions options = provider.GetRequiredService<IOptions<NanocachedCacheOptions>>().Value;
            NanocachedClient client = NanocachedClient.ConnectAsync(BuildClientOptions(options))
                .GetAwaiter().GetResult();
            return new OwnedNanocachedDistributedCache(client, options.Namespace);
        });
        return services;
    }

    /// <summary>Registers a singleton <see cref="IDistributedCache"/> bound
    /// to <paramref name="namespace"/> on a <see cref="NanocachedClient"/>
    /// the application already registered as a singleton itself. This
    /// overload dials nothing and owns nothing — the client's lifecycle,
    /// disposal included, stays whoever registered it's responsibility, so
    /// an app sharing one client across this cache and its own direct
    /// nanocached usage closes it exactly once, wherever it already
    /// does.</summary>
    public static IServiceCollection AddNanocachedDistributedCache(
        this IServiceCollection services, string @namespace = NanocachedCacheOptions.DefaultNamespace)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(@namespace);

        services.AddSingleton<IDistributedCache>(provider =>
            new NanocachedDistributedCache(provider.GetRequiredService<NanocachedClient>(), @namespace));
        return services;
    }

    private static NanocachedClient.Options BuildClientOptions(NanocachedCacheOptions options)
    {
        var clientOptions = new NanocachedClient.Options { AuthSecret = options.Secret };
        foreach (string address in options.Addresses)
        {
            clientOptions.Addresses.Add(ParseAddress(address));
        }
        return clientOptions;
    }

    private static (string Host, int Port) ParseAddress(string address)
    {
        int colon = address.LastIndexOf(':');
        if (colon <= 0 || colon == address.Length - 1 || !int.TryParse(address.AsSpan(colon + 1), out int port))
        {
            throw new ArgumentException(
                $"nanocached: invalid address \"{address}\" in NanocachedCacheOptions.Addresses, "
                + "expected \"host:port\"",
                nameof(address));
        }
        return (address[..colon], port);
    }

    /// <summary>The <see cref="IDistributedCache"/> instance actually
    /// created for the "owns its own client" overload above — see that
    /// overload's doc comment for why closing the client here, rather than
    /// via a separately-resolved <see cref="IDisposable"/> registration, is
    /// what makes disposal actually happen.</summary>
    private sealed class OwnedNanocachedDistributedCache : NanocachedDistributedCache, IDisposable
    {
        private readonly NanocachedClient _client;

        internal OwnedNanocachedDistributedCache(NanocachedClient client, string @namespace)
            : base(client, @namespace)
        {
            _client = client;
        }

        public void Dispose() => _client.Close();
    }
}
