using System.Diagnostics;
using Xunit;

namespace Nanocached.Tests;

/// <summary>What a <see cref="CancellationToken"/> does on a call: a token that
/// is already cancelled sends nothing; cancelling while a request is in flight
/// abandons the wait (the request is already on the wire and answered in
/// order, so it cannot be taken back) and leaves the connection usable.</summary>
[Collection(DialSettings.CollectionName)]
public class CancellationTests
{
    private static NanocachedClient.Options Single(MockNode node) =>
        new() { Addresses = { ("127.0.0.1", node.Port) } };

    [Fact]
    public async Task AnAlreadyCancelledTokenSendsNothing()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.GetAsync("k", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.SetAsync("k", "v", 0, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.IncrAsync("c", 1, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ClearAllAsync(cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.Namespace("ns").GetAsync("k", cts.Token));

        Assert.Equal(0, node.GetCount);
        Assert.Equal(0, node.IncrRequestCount);
        Assert.Equal(0, node.ClearAllRequestCount);
        Assert.Equal(0, node.NamespacedRequestCount);
        Assert.False(node.Store.ContainsKey("k"));
    }

    [Fact]
    public async Task CancellingAbandonsTheWaitAndTheConnectionStaysUsable()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        await client.SetAsync("k", "v");

        node.DelayGets(600);
        using var cts = new CancellationTokenSource();
        var watch = Stopwatch.StartNew();
        Task<string?> abandoned = client.GetAsync("k", cts.Token);
        cts.CancelAfter(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);
        Assert.True(watch.ElapsedMilliseconds < 500, $"the wait was not abandoned: {watch.ElapsedMilliseconds} ms");

        // The abandoned request's reply arrives later and is discarded; the next
        // request must still be paired with its own reply, not that one's.
        node.DelayGets(0);
        await client.SetAsync("other", "second");
        Assert.Equal("second", await client.GetAsync("other"));
        Assert.Equal("v", await client.GetAsync("k"));
    }

    [Fact]
    public async Task ATokenThatIsNeverCancelledChangesNothing()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        using var cts = new CancellationTokenSource();

        await client.SetAsync("k", "v", 0, cts.Token);
        Assert.Equal("v", await client.GetAsync("k", cts.Token));
        Assert.True(await client.DeleteAsync("k", cts.Token));
        Assert.Null(await client.GetAsync("k", cts.Token));
    }

    [Fact]
    public async Task ConnectAsyncHonoursACancelledToken()
    {
        using var node = new MockNode();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => NanocachedClient.ConnectAsync(Single(node), cts.Token));

        node.DelayAuth(600);
        using var later = new CancellationTokenSource();
        var watch = Stopwatch.StartNew();
        Task<NanocachedClient> connecting = NanocachedClient.ConnectAsync(Single(node), later.Token);
        later.CancelAfter(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connecting);
        Assert.True(watch.ElapsedMilliseconds < 500, $"the connect was not abandoned: {watch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task DisposeAsyncClosesTheClient()
    {
        using var node = new MockNode();
        NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        await client.SetAsync("k", "v");

        await client.DisposeAsync();

        await Assert.ThrowsAsync<AlreadyClosedException>(() => client.GetAsync("k"));
    }
}
