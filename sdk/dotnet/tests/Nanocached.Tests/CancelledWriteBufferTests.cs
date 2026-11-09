using System.Text;
using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// A call cancelled while it waits for a redial is abandoned, not stopped:
/// the operation keeps going and builds its frame later. The byte[]
/// arguments of a call with a cancellable token are therefore copied at the
/// call, so a caller that reuses its buffers after the cancellation (an
/// ArrayPool, a request buffer) cannot make the abandoned operation store,
/// delete, increment or touch a different key or value.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class CancelledWriteBufferTests
{
    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static byte[]? Stored(MockNode node, string key) =>
        node.Store.TryGetValue(MockNode.KeyOf(Bytes(key)), out byte[]? value) ? value : null;

    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"timed out waiting for {what}");
            await Task.Delay(5);
        }
    }

    // The connection is cut and the redial takes 800 ms, so the call is
    // parked until after it has been cancelled at 50 ms and the caller has
    // overwritten its buffers.
    private static async Task AbandonDuringRedialAsync(
        MockNode node, NanocachedClient client, Func<CancellationToken, Task> call, Action overwrite)
    {
        await client.SetAsync("warm", "up");
        node.DelayAuth(800);
        node.DropConnections();
        await Task.Delay(150);

        using var cts = new CancellationTokenSource();
        Task pending = call(cts.Token);
        cts.CancelAfter(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        overwrite();
    }

    private static void Overwrite(byte[] buffer, string text) =>
        Encoding.UTF8.GetBytes(text).CopyTo(buffer, 0);

    private static NanocachedClient.Options Single(MockNode node) =>
        new() { Addresses = { ("127.0.0.1", node.Port) } };

    [Fact]
    public async Task AnAbandonedSetStoresTheKeyAndValueItWasCalledWith()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        byte[] key = Bytes("key-A");
        byte[] value = Bytes("value-A");

        await AbandonDuringRedialAsync(node, client, token => client.SetAsync(key, value, 0, token), () =>
        {
            Overwrite(key, "key-B");
            Overwrite(value, "value-B");
        });

        await WaitForAsync(() => Stored(node, "key-A") is not null || Stored(node, "key-B") is not null, "the set");
        Assert.Equal(Bytes("value-A"), Stored(node, "key-A"));
        Assert.Null(Stored(node, "key-B"));
    }

    [Fact]
    public async Task AnAbandonedNamespaceSetStoresTheKeyAndValueItWasCalledWith()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        NanocachedNamespace ns = client.Namespace("ns");
        byte[] key = Bytes("key-A");
        byte[] value = Bytes("value-A");

        await AbandonDuringRedialAsync(node, client, token => ns.SetAsync(key, value, 0, token), () =>
        {
            Overwrite(key, "key-B");
            Overwrite(value, "value-B");
        });

        byte[] nsBytes = Bytes("ns");
        await WaitForAsync(
            () => node.Store.ContainsKey(MockNode.KeyOf(nsBytes, Bytes("key-A")))
                || node.Store.ContainsKey(MockNode.KeyOf(nsBytes, Bytes("key-B"))),
            "the set");
        Assert.Equal(Bytes("value-A"), node.Store[MockNode.KeyOf(nsBytes, Bytes("key-A"))]);
        Assert.False(node.Store.ContainsKey(MockNode.KeyOf(nsBytes, Bytes("key-B"))));
    }

    [Fact]
    public async Task AnAbandonedDeleteDeletesTheKeyItWasCalledWith()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        await client.SetAsync("mine0001", "m");
        await client.SetAsync("victim01", "v");
        byte[] key = Bytes("mine0001");

        await AbandonDuringRedialAsync(node, client, token => client.DeleteAsync(key, token),
            () => Overwrite(key, "victim01"));

        await WaitForAsync(() => Stored(node, "mine0001") is null || Stored(node, "victim01") is null, "the delete");
        Assert.Null(Stored(node, "mine0001"));
        Assert.Equal(Bytes("v"), Stored(node, "victim01"));
    }

    [Fact]
    public async Task AnAbandonedIncrIncrementsTheKeyItWasCalledWith()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        await client.SetAsync("mine0001", "5");
        await client.SetAsync("victim01", "5");
        byte[] key = Bytes("mine0001");

        await AbandonDuringRedialAsync(node, client, token => client.IncrAsync(key, 1, token),
            () => Overwrite(key, "victim01"));

        await WaitForAsync(
            () => Stored(node, "mine0001")!.SequenceEqual(Bytes("6")) || Stored(node, "victim01")!.SequenceEqual(Bytes("6")),
            "the increment");
        Assert.Equal(Bytes("6"), Stored(node, "mine0001"));
        Assert.Equal(Bytes("5"), Stored(node, "victim01"));
    }

    [Fact]
    public async Task AnAbandonedPutIfAbsentStoresTheKeyAndValueItWasCalledWith()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        byte[] key = Bytes("key-A");
        byte[] value = Bytes("value-A");

        await AbandonDuringRedialAsync(node, client, token => client.PutIfAbsentAsync(key, value, 0, token), () =>
        {
            Overwrite(key, "key-B");
            Overwrite(value, "value-B");
        });

        await WaitForAsync(() => Stored(node, "key-A") is not null || Stored(node, "key-B") is not null, "the put");
        Assert.Equal(Bytes("value-A"), Stored(node, "key-A"));
        Assert.Null(Stored(node, "key-B"));
    }

    [Fact]
    public async Task AnAbandonedSetManyBytesStoresTheValuesItWasCalledWith()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(Single(node));
        byte[] value = Bytes("value-A");
        var values = new Dictionary<string, byte[]> { ["key-A"] = value };

        await AbandonDuringRedialAsync(node, client, token => client.SetManyBytesAsync(values, 0, token), () =>
        {
            Overwrite(value, "value-B");
            values.Clear();
            values["key-B"] = Bytes("value-B");
        });

        await WaitForAsync(() => Stored(node, "key-A") is not null || Stored(node, "key-B") is not null, "the batch");
        Assert.Equal(Bytes("value-A"), Stored(node, "key-A"));
        Assert.Null(Stored(node, "key-B"));
    }
}
