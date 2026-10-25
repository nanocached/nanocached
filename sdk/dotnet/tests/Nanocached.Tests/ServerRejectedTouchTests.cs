using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// issue #939 — only a node's <c>En</c> to a touch is "server too old".
/// Any other <c>E</c> (a bare <c>E</c>, or the tagged <c>E &lt;tag&gt;</c>
/// nanocached-proxy answers when an expiring touch's outcome is unknown)
/// is a connection lost after the frame was written: not resent for a
/// ttl=0 touch, and never reported as an old server.
/// </summary>
[Collection(DialSettings.CollectionName)]
public class ServerRejectedTouchTests
{
    private static NanocachedClient.Options SingleAddress(int port) =>
        new() { Addresses = { ("127.0.0.1", port) } };

    public static IEnumerable<object[]> Operations()
    {
        foreach (string reply in new[] { "E{tag}\n", "E\n" })
        {
            foreach (string op in new[] { "touch", "getAndTouch", "touchMany", "getManyAndTouch" })
            {
                yield return new object[] { reply, op };
            }
        }
    }

    private static Task RunAsync(NanocachedClient client, string op, long ttl) => op switch
    {
        "touch" => client.TouchAsync("k", ttl),
        "getAndTouch" => client.GetAndTouchAsync("k", ttl),
        "touchMany" => client.TouchManyAsync(new[] { "k" }, ttl),
        _ => client.GetManyAndTouchAsync(new[] { "k" }, ttl),
    };

    private static int FramesSeen(MockNode node, string op) =>
        op is "touch" or "getAndTouch" ? node.TouchFrames.Count : node.MultiTouchFrames.Count;

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task AnEOtherThanEnToAnExpiringTouchIsALostConnectionAndIsNotResent(string reply, string op)
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress(node.Port));
        await client.SetAsync("k", "v", 100);
        node.RejectTouchWith(reply);

        ConnectionLostException error =
            await Assert.ThrowsAnyAsync<ConnectionLostException>(() => RunAsync(client, op, 0));

        Assert.False(error.RequestNotSent);
        Assert.Equal(1, FramesSeen(node, op));
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task AnEOtherThanEnIsNeverServerTooOldForAnyTtl(string reply, string op)
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress(node.Port));
        await client.SetAsync("k", "v", 100);
        node.RejectTouchWith(reply);

        Exception error = await Assert.ThrowsAnyAsync<Exception>(() => RunAsync(client, op, 30));

        Assert.IsAssignableFrom<ConnectionLostException>(error);
        Assert.IsNotAssignableFrom<ServerTooOldException>(error);
    }

    [Theory]
    [InlineData("touch")]
    [InlineData("getAndTouch")]
    [InlineData("touchMany")]
    [InlineData("getManyAndTouch")]
    public async Task EnToAnExpiringTouchIsStillServerTooOld(string op)
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress(node.Port));
        await client.SetAsync("k", "v", 100);
        node.RejectTouchLikeAnOldServer();

        await Assert.ThrowsAsync<ServerTooOldException>(() => RunAsync(client, op, 0));

        Assert.Equal(1, FramesSeen(node, op));
    }
}
