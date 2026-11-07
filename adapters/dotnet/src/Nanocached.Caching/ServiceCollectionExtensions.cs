using System.Globalization;
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
            return new NanocachedDistributedCache(client, options.Namespace, ownsClient: true);
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
        var clientOptions = new NanocachedClient.Options
        {
            AuthSecret = options.Secret,
            Tls = options.Tls,
            Ca = options.Ca,
            Compress = options.Compress,
            CompressionThreshold = options.CompressionThreshold,
        };
        foreach (string address in options.Addresses)
        {
            clientOptions.Addresses.Add(ParseAddress(address));
        }
        return clientOptions;
    }

    /// <summary>One <c>"host:port"</c> entry of
    /// <see cref="NanocachedCacheOptions.Addresses"/>. An IPv6 literal is
    /// written in brackets to keep its colons apart from the port
    /// (<c>"[::1]:8357"</c>); the brackets are stripped here because the
    /// SDK takes the bare address.</summary>
    internal static (string Host, int Port) ParseAddress(string address)
    {
        int colon = address.LastIndexOf(':');
        if (colon <= 0 || colon == address.Length - 1)
        {
            throw new ArgumentException(
                $"nanocached: invalid address \"{address}\" in NanocachedCacheOptions.Addresses, "
                + "expected \"host:port\"",
                nameof(address));
        }
        // Issue #1228: digits only (no sign, no whitespace, no culture), and
        // a real TCP port, so a typo fails here naming the entry instead of
        // surfacing later as a socket error from the dial.
        if (!int.TryParse(address.AsSpan(colon + 1), NumberStyles.None, CultureInfo.InvariantCulture, out int port))
        {
            throw new ArgumentException(
                $"nanocached: invalid address \"{address}\" in NanocachedCacheOptions.Addresses, "
                + "the port is not a number",
                nameof(address));
        }
        if (port < 1 || port > 65535)
        {
            throw new ArgumentException(
                $"nanocached: invalid address \"{address}\" in NanocachedCacheOptions.Addresses, "
                + "the port must be between 1 and 65535",
                nameof(address));
        }
        string host = address[..colon];
        if (host.Length >= 2 && host[0] == '[' && host[^1] == ']')
        {
            host = host[1..^1];
        }
        return (host, port);
    }
}
