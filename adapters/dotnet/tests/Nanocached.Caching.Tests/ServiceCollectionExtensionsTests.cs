using System.Security.Cryptography.X509Certificates;
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

    // The owning overload's TLS / CA / compress options (the SDK builder's
    // own flags, mirroring the Django adapter's OPTIONS from issue #231).
    // Before these existed a secret-configured cache could only connect in
    // plaintext.

    [Fact]
    public async Task Tls_with_a_private_ca_connects_over_tls()
    {
        using X509Certificate2 cert = TlsTestSupport.GenerateLoopbackCertificate();
        string caPath = TlsTestSupport.WritePemCertificate(cert);
        try
        {
            using var node = new MockNode(cert);
            var services = new ServiceCollection();
            services.AddNanocachedDistributedCache(options =>
            {
                options.Addresses.Add($"127.0.0.1:{node.Port}");
                options.Secret = "s3cret";
                options.Tls = true;
                options.Ca = caPath;
            });
            await using ServiceProvider provider = services.BuildServiceProvider();

            IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
            await cache.SetStringAsync("k", "over-tls");
            Assert.Equal("over-tls", await cache.GetStringAsync("k"));
        }
        finally
        {
            File.Delete(caPath);
        }
    }

    [Fact]
    public void Tls_without_the_private_ca_does_not_trust_the_node_certificate()
    {
        using X509Certificate2 cert = TlsTestSupport.GenerateLoopbackCertificate();
        using var node = new MockNode(cert);
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
            options.Tls = true;
        });
        using ServiceProvider provider = services.BuildServiceProvider();

        // The option really reaches the client: it dials TLS against the
        // system trust store, which does not know this self-signed root.
        Assert.ThrowsAny<Exception>(() => provider.GetRequiredService<IDistributedCache>());
    }

    [Fact]
    public void Tls_against_a_plaintext_node_fails_to_connect()
    {
        using var node = new MockNode();
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
            options.Tls = true;
        });
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.ThrowsAny<Exception>(() => provider.GetRequiredService<IDistributedCache>());
    }

    [Fact]
    public void Ca_without_Tls_is_rejected_like_the_sdk_rejects_it()
    {
        using var node = new MockNode();
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
            options.Ca = "/nonexistent/ca.pem";
        });
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<ArgumentException>(() => provider.GetRequiredService<IDistributedCache>());
    }

    [Fact]
    public async Task Compress_stores_large_values_compressed_and_reads_them_back()
    {
        string large = new('x', 20_000);
        byte[] key = "big"u8.ToArray();

        using var plainNode = new MockNode();
        var plainServices = new ServiceCollection();
        plainServices.AddNanocachedDistributedCache(o => o.Addresses.Add($"127.0.0.1:{plainNode.Port}"));
        await using (ServiceProvider provider = plainServices.BuildServiceProvider())
        {
            await provider.GetRequiredService<IDistributedCache>().SetStringAsync("big", large);
        }
        int plainLength = plainNode.EntryFor(NanocachedCacheOptions.DefaultNamespace, key)!.Value.Length;

        using var node = new MockNode();
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
            options.Compress = true;
            options.CompressionThreshold = 64;
        });
        await using ServiceProvider compressing = services.BuildServiceProvider();

        IDistributedCache cache = compressing.GetRequiredService<IDistributedCache>();
        await cache.SetStringAsync("big", large);
        Assert.Equal(large, await cache.GetStringAsync("big"));

        int compressedLength = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key)!.Value.Length;
        Assert.True(
            compressedLength * 10 < plainLength,
            $"expected the stored value to be compressed ({compressedLength} bytes vs {plainLength} uncompressed)");
    }

    [Fact]
    public async Task Defaults_are_unchanged_and_values_under_the_compression_threshold_stay_uncompressed()
    {
        var defaults = new NanocachedCacheOptions();
        Assert.False(defaults.Tls);
        Assert.Null(defaults.Ca);
        Assert.False(defaults.Compress);
        Assert.Equal(256, defaults.CompressionThreshold);

        using var node = new MockNode();
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
            options.Compress = true;
            options.CompressionThreshold = 4096;
        });
        await using ServiceProvider provider = services.BuildServiceProvider();
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        string value = new('y', 1000);
        await cache.SetStringAsync("small", value);
        Assert.Equal(value, await cache.GetStringAsync("small"));
        int stored = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, "small"u8.ToArray())!.Value.Length;
        Assert.True(stored >= value.Length, $"value under the threshold should be stored uncompressed ({stored})");
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
