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

    // issue #391: the sliding renewal used to be read-then-SetAsync — a
    // non-atomic read-modify-write. A concurrent Set landing between the
    // read and the renewal write was silently clobbered by the stale
    // renewal (a lost update: e.g. a session read racing a session
    // update). The renewal must be token-conditional and LOSE that race.
    // MockNode.AfterGet interleaves the concurrent writer
    // deterministically, right after the adapter's read is served.
    [Fact]
    public async Task A_sliding_renewal_never_clobbers_a_concurrent_Set()
    {
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "session"u8.ToArray();

        await cache.SetAsync(
            "session", new byte[] { 1 },
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(30) });

        // The concurrent writer: its own client (and connection — the
        // hook runs on the read connection's serve loop, which a write
        // over the same connection would deadlock), firing between the
        // adapter's read and its renewal write — exactly the window the
        // old SetAsync renewal lost.
        await using ServiceProvider writerProvider = BuildProvider(node);
        IDistributedCache writer = writerProvider.GetRequiredService<IDistributedCache>();
        node.AfterGet = (_, _) =>
        {
            node.AfterGet = null; // the writer's own ops must not recurse
            writer.Set(
                "session", new byte[] { 2 },
                new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(60) });
        };

        byte[]? read = await cache.GetAsync("session");
        Assert.Equal(new byte[] { 1 }, read); // the read itself saw the old value

        // The concurrent writer's entry must have survived the renewal.
        byte[]? after = await cache.GetAsync("session");
        Assert.Equal(new byte[] { 2 }, after);
        MockNode.Entry? entry = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(entry);
        Assert.Equal(60, entry!.TtlSeconds);
    }

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
        int casCountAfterSet = node.CasCount;

        byte[]? value = await cache.GetAsync("session");
        Assert.Equal(new byte[] { 9, 9 }, value);

        // A Get on a sliding entry re-writes it — a `k` (token-conditional
        // replace, issue #391) must actually have reached the wire, not
        // just a re-read of the existing entry.
        Assert.Equal(casCountAfterSet + 1, node.CasCount);
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
        int casCountAfterSet = node.CasCount;

        await cache.RefreshAsync("session2");

        Assert.Equal(casCountAfterSet + 1, node.CasCount); // a `k` renewal reached the wire (issue #391)
        MockNode.Entry? afterRefresh = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(afterRefresh);
        Assert.Equal(15, afterRefresh!.TtlSeconds);
    }

    [Fact]
    public async Task Extreme_sliding_expiration_does_not_wrap_the_persisted_TTL_on_renewal()
    {
        // Regression for issue #304: Envelope.ToBytes() used to cast
        // SlidingSeconds (a `long`) down to `uint` with no range check,
        // wrapping any value past uint.MaxValue seconds (~136 years). 2^32
        // seconds is one past that — before the fix, this would wrap to
        // exactly 0 in the persisted envelope, so the *next* renewal would
        // write wire TTL 0 ("no expiry") instead of anything resembling the
        // caller's actual sliding window.
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "eon"u8.ToArray();
        var extremeSliding = TimeSpan.FromSeconds(4_294_967_296); // uint.MaxValue + 1

        await cache.SetAsync(
            "eon", new byte[] { 1 },
            new DistributedCacheEntryOptions { SlidingExpiration = extremeSliding });

        // The immediate write isn't limited by the envelope's 4-byte field —
        // it sends the caller's real, uncapped requested TTL as-is.
        MockNode.Entry? afterSet = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(afterSet);
        Assert.Equal(4_294_967_296L, afterSet!.TtlSeconds);

        // A Get() renews the sliding window from the *persisted* envelope,
        // which is now clamped to uint.MaxValue rather than wrapped.
        await cache.GetAsync("eon");

        MockNode.Entry? afterGet = node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key);
        Assert.NotNull(afterGet);
        Assert.Equal(uint.MaxValue, (uint)afterGet!.TtlSeconds);
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
        Assert.Equal(0, node.CasCount); // and no `k` renewal either (issue #391)
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

    [Fact]
    public async Task Get_treats_an_already_past_absolute_expiry_as_a_miss_and_deletes_it()
    {
        // Regression for issue #233: a sliding-window entry whose
        // absolute expiry has already passed used to be floored to a
        // fresh 1-second TTL and resurrected by Get's renew-on-read
        // instead of being treated as expired — CeilSeconds' floor
        // exists for a sub-second-but-still-future remainder, not for
        // "already past". The mock never enforces TTL expiry itself (see
        // its own doc comment), so the stale envelope is still sitting
        // there for this Get to actually observe as past-due, wall
        // clock included.
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "stale-absolute"u8.ToArray();

        await cache.SetAsync(
            "stale-absolute", new byte[] { 1 },
            new DistributedCacheEntryOptions
            {
                SlidingExpiration = TimeSpan.FromSeconds(30),
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(150),
            });
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        byte[]? value = await cache.GetAsync("stale-absolute");

        Assert.Null(value);
        Assert.Null(node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key));
    }

    [Fact]
    public async Task Refresh_treats_an_already_past_absolute_expiry_as_a_miss_and_deletes_it()
    {
        // Same regression as the Get test above, for Refresh's identical
        // renewal path.
        using var node = new MockNode();
        await using ServiceProvider provider = BuildProvider(node);
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();
        byte[] key = "stale-absolute-refresh"u8.ToArray();

        await cache.SetAsync(
            "stale-absolute-refresh", new byte[] { 1 },
            new DistributedCacheEntryOptions
            {
                SlidingExpiration = TimeSpan.FromSeconds(30),
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(150),
            });
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        await cache.RefreshAsync("stale-absolute-refresh");

        Assert.Null(node.EntryFor(NanocachedCacheOptions.DefaultNamespace, key));
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
