using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Nanocached.Caching.Tests;

/// <summary>
/// Drives the adapter through the framework's own consumption API — DI
/// registration plus the standard <see cref="IDistributedCache"/>
/// extension methods (<c>SetStringAsync</c>/<c>GetStringAsync</c>) —
/// rather than only the adapter's SPI implementation directly (issue
/// #108's shared spec, lesson 1 from issue #107/PR #116). A few tests go
/// straight at <see cref="NanocachedDistributedCache"/>'s own
/// constructor/<c>Namespace</c> property, where DI would only add noise.
/// </summary>
public sealed class NanocachedDistributedCacheTests
{
    private static ServiceProvider BuildProvider(MockNode node, Action<NanocachedCacheOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddNanocachedDistributedCache(options =>
        {
            options.Addresses.Add($"127.0.0.1:{node.Port}");
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Round_trips_set_get_and_remove_through_DI()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        await cache.SetStringAsync("greeting", "hello");
        Assert.Equal("hello", await cache.GetStringAsync("greeting"));

        await cache.RemoveAsync("greeting");
        Assert.Null(await cache.GetStringAsync("greeting"));
    }

    [Fact]
    public async Task Missing_key_returns_null()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        Assert.Null(await cache.GetStringAsync("never-set"));
    }

    [Fact]
    public async Task Sync_Get_and_Set_work()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        cache.SetString("sync-key", "sync-value");
        Assert.Equal("sync-value", cache.GetString("sync-key"));

        cache.Refresh("sync-key");
        cache.Remove("sync-key");
        Assert.Null(cache.GetString("sync-key"));
    }

    [Fact]
    public async Task No_options_means_no_TTL_on_the_wire()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        await cache.SetAsync("eternal", new byte[] { 1, 2, 3 });

        MockNode.Entry? entry = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, "eternal"u8.ToArray());
        Assert.NotNull(entry);
        Assert.Equal(0, entry!.TtlSeconds); // 0 on the wire = no expiry
    }

    // ── Sliding expiration ──────────────────────────────────────────

    [Fact]
    public async Task Sliding_expiration_writes_the_configured_TTL_and_is_renewed_on_Get()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "session"u8.ToArray();

        await cache.SetAsync(
            "session", new byte[] { 9, 9 },
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(30) });

        MockNode.Entry? afterSet = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(afterSet);
        Assert.Equal(30, afterSet!.TtlSeconds);
        int setCountAfterSet = node.SetCount;

        byte[]? value = await cache.GetAsync("session");
        Assert.Equal(new byte[] { 9, 9 }, value);

        // A Get on a sliding entry re-sets it — a second `s` request must
        // actually have reached the wire, not just a re-read of the
        // existing entry.
        Assert.Equal(setCountAfterSet + 1, node.SetCount);
        MockNode.Entry? afterGet = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(afterGet);
        Assert.Equal(30, afterGet!.TtlSeconds);
    }

    [Fact]
    public async Task Refresh_renews_a_sliding_entrys_TTL()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "session2"u8.ToArray();

        await cache.SetAsync(
            "session2", new byte[] { 1 },
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(15) });
        int setCountAfterSet = node.SetCount;

        await cache.RefreshAsync("session2");

        Assert.Equal(setCountAfterSet + 1, node.SetCount); // a second `s` reached the wire
        MockNode.Entry? afterRefresh = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(afterRefresh);
        Assert.Equal(15, afterRefresh!.TtlSeconds);
    }

    [Fact]
    public async Task Refresh_of_a_missing_key_is_a_no_op()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        // Must not throw.
        await cache.RefreshAsync("was-never-set");
        Assert.Null(node.EntryFor(NanocachedCacheOptions.DefaultNamespace, "was-never-set"u8.ToArray()));
    }

    [Fact]
    public async Task Non_sliding_entries_are_not_rewritten_on_Get()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "fixed"u8.ToArray();

        await cache.SetAsync("fixed", new byte[] { 7 });
        int setCountAfterSet = node.SetCount;
        MockNode.Entry? beforeGet = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);

        await cache.GetAsync("fixed");

        Assert.Equal(setCountAfterSet, node.SetCount); // no second `s` was sent
        MockNode.Entry? afterGet = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.Same(beforeGet, afterGet);
    }

    [Fact]
    public async Task Sliding_plus_absolute_caps_the_TTL_at_the_absolute_remainder()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "capped"u8.ToArray();

        await cache.SetAsync(
            "capped", new byte[] { 1 },
            new DistributedCacheEntryOptions
            {
                SlidingExpiration = TimeSpan.FromSeconds(30),
                AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(5),
            });

        MockNode.Entry? entry = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(entry);
        // The absolute remainder (~5s) is tighter than the 30s sliding
        // window, so it wins — never a fixed 30.
        Assert.InRange(entry!.TtlSeconds, 1, 5);
    }

    // ── Absolute expiration ─────────────────────────────────────────

    [Fact]
    public async Task AbsoluteExpirationRelativeToNow_maps_to_the_exact_TTL()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "ttl60"u8.ToArray();

        await cache.SetAsync(
            "ttl60", new byte[] { 1 },
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(60) });

        MockNode.Entry? entry = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(entry);
        Assert.Equal(60, entry!.TtlSeconds);
    }

    [Fact]
    public async Task Sub_second_relative_expiration_rounds_up_to_one_second()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "subsecond"u8.ToArray();

        await cache.SetAsync(
            "subsecond", new byte[] { 1 },
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(200) });

        MockNode.Entry? entry = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(entry);
        Assert.Equal(1, entry!.TtlSeconds); // never rounds down to 0 (= eternal on the wire)
    }

    [Fact]
    public async Task Past_absolute_expiration_throws()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpiration = DateTimeOffset.UtcNow.AddSeconds(-5),
        };
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => cache.SetAsync("already-expired", new byte[] { 1 }, options));
    }

    // ── Namespaces ───────────────────────────────────────────────────

    [Fact]
    public async Task Default_namespace_is_distributed_cache()
    {
        using var node = new MockNode();
        var client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) } });
        try
        {
            var cache = new NanocachedDistributedCache(client);
            Assert.Equal(NanocachedCacheOptions.DefaultNamespace, cache.Namespace);
            Assert.Equal("distributed-cache", cache.Namespace);
        }
        finally
        {
            client.Close();
        }
    }

    [Fact]
    public async Task Two_namespaces_are_fully_isolated()
    {
        using var node = new MockNode();
        var client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) } });
        try
        {
            var users = new NanocachedDistributedCache(client, "users");
            var sessions = new NanocachedDistributedCache(client, "sessions");
            Assert.Equal("users", users.Namespace);
            Assert.Equal("sessions", sessions.Namespace);

            await users.SetAsync("42", new byte[] { 0xA1 }, new DistributedCacheEntryOptions());
            await sessions.SetAsync("42", new byte[] { 0xB2 }, new DistributedCacheEntryOptions());

            Assert.Equal(new byte[] { 0xA1 }, await users.GetAsync("42"));
            Assert.Equal(new byte[] { 0xB2 }, await sessions.GetAsync("42"));

            await users.RemoveAsync("42");
            Assert.Null(await users.GetAsync("42"));
            // Removing from "users" never touches "sessions"'s copy.
            Assert.Equal(new byte[] { 0xB2 }, await sessions.GetAsync("42"));
        }
        finally
        {
            client.Close();
        }
    }

    // ── Envelope ─────────────────────────────────────────────────────

    [Fact]
    public async Task Envelope_round_trips_arbitrary_binary_payloads()
    {
        using var node = new MockNode();
        var client = await NanocachedClient.ConnectAsync(
            new NanocachedClient.Options { Addresses = { ("127.0.0.1", node.Port) } });
        try
        {
            var cache = new NanocachedDistributedCache(client, "binary");
            byte[] payload = { 0x00, 0xFF, 0x01, 0xFE, 0x00, 0x00, 0xFF, 0xFF, 0x7F, 0x80 };

            await cache.SetAsync(
                "blob", payload,
                new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(20) });

            byte[]? roundTripped = await cache.GetAsync("blob");
            Assert.Equal(payload, roundTripped);
        }
        finally
        {
            client.Close();
        }
    }
}
