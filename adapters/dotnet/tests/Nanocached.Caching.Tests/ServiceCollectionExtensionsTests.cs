using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Nanocached.Caching.Tests;

/// <summary>Exercises the two <c>AddNanocachedDistributedCache</c>
/// overloads through real <see cref="IServiceCollection"/>/<see cref="ServiceProvider"/>
/// wiring — not just the returned <see cref="IDistributedCache"/> in
/// isolation — since the whole point of the DI extension is how it
/// interacts with the container's own lifetime management (issue
/// #108).</summary>
public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public async Task Owning_overload_connects_and_the_container_closes_the_client_on_dispose()
    {
        using var node = new MockNode();
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
        });
        ServiceProvider provider = services.BuildServiceProvider();

        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        await cache.SetStringAsync("k", "v");
        Assert.Equal("v", await cache.GetStringAsync("k"));

        await provider.DisposeAsync();

        // The client this registration owns was closed along with the
        // container — a further call through the very same cache instance
        // now sees a closed client, exactly like calling a closed
        // NanocachedClient directly would.
        await Assert.ThrowsAsync<AlreadyClosedException>(() => cache.GetStringAsync("k"));
    }

    [Fact]
    public async Task Reusing_overload_binds_to_an_already_registered_client_and_never_closes_it()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) } });
        try
        {
            var services = new ServiceCollection();
            services.AddSingleton(client);
            services.AddNanocachedDistributedCache("reused");
            ServiceProvider provider = services.BuildServiceProvider();

            IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
            await cache.SetStringAsync("shared-key", "shared-value");
            Assert.Equal("shared-value", await cache.GetStringAsync("shared-key"));

            await provider.DisposeAsync();

            // The reuse overload never owns the client, so disposing the
            // container must not have closed it — the app that registered
            // it is still responsible, and the very same cache instance
            // (still holding the now-container-outlived client) keeps
            // working afterward.
            Assert.False(client.IsClosed);
            Assert.Equal("shared-value", await cache.GetStringAsync("shared-key"));
        }
        finally
        {
            client.Close();
        }
    }

    [Fact]
    public async Task Reusing_overload_defaults_to_the_default_namespace()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) } });
        try
        {
            var services = new ServiceCollection();
            services.AddSingleton(client);
            services.AddNanocachedDistributedCache();
            await using ServiceProvider provider = services.BuildServiceProvider();

            IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
            await cache.SetStringAsync("dflt", "value");

            byte[]? viaNamespace =
                await client.Namespace(NanocachedCacheOptions.DefaultNamespace).GetBytesAsync("dflt");
            Assert.NotNull(viaNamespace);
        }
        finally
        {
            client.Close();
        }
    }
}
