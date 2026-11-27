using Xunit;

namespace Nanocached.Tests;

/// <summary>
/// issue #939 — only a node's <c>En</c> to a touch is "server too old".
/// Any other <c>E</c> (the bare <c>E</c> nanocached-proxy answers when an
/// expiring touch's outcome is unknown, untagged even on a tagged
/// connection, or an <c>E</c> that carries a tag)
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

    /// <summary>A node's <c>En</c> answers only the oldest request and the
    /// node closes the connection right after, so an incr pipelined behind
    /// the rejected touch was never run: it must be drained as not sent and
    /// replayed on the redial, not reported as possibly applied.</summary>
    [Fact]
    public async Task AnIncrPipelinedBehindATouchAnsweredEnIsReplayed()
    {
        using var node = new MockNode();
        using NanocachedClient client = await NanocachedClient.ConnectAsync(SingleAddress(node.Port));
        await client.SetAsync("k", "1", 100);
        node.DelayTouches(500);
        node.RejectTouchLikeAnOldServer();

        Task touch = client.TouchAsync("k", 30);
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (node.TouchFrames.Count == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "the touch frame never reached the node");
            await Task.Delay(5);
        }
        Task<long?> incr = client.IncrAsync("k", 1);

        await Assert.ThrowsAsync<ServerTooOldException>(() => touch);
        Assert.Equal(2, await incr.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(1, node.IncrRequestCount);
    }
}
