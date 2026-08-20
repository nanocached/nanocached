using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace Nanocached;

/// <summary>
/// One already-identified connection to a single nanocached-node, speaking
/// the cache protocol (<c>G</c>/<c>S</c>/<c>D</c> — the <c>A</c> identify
/// exchange happens in <see cref="Identify"/> before a Connection exists).
/// Requests are pipelined onto the socket and matched to responses in send
/// order (doc/adr/0016-*.md): a dedicated read loop, started in the
/// constructor, consumes responses and dispatches each to the oldest
/// still-pending request, since nanocached-node itself only ever answers in
/// the order it received requests. <see cref="Stream"/> supports one
/// concurrent reader and one concurrent writer safely, so the read loop
/// never contends with writers. Enqueuing the pending slot and writing the
/// frame happen under one semaphore, so concurrent callers' queue order
/// always matches the order their frames actually hit the wire.
///
/// <para>doc/adr/0019-*.md: when <see cref="_tagged"/> (negotiated during
/// identify), every request carries a per-connection tag as its header's
/// last field, and the server echoes it in the response. The tag is
/// claimed inside the same enqueue+write critical section as the pending
/// slot itself, so tag order can never skew from queue/wire order, and the
/// read loop verifies the echoed tag against the oldest pending slot's
/// expected tag <em>before</em> that slot is handed its response — closing
/// the desync window an off-by-one stream corruption (e.g. a swallowed
/// response) would otherwise leave open (the caller-side kind check in
/// <see cref="Mismatch"/> only ever notices that after the fact).</para>
/// </summary>
internal sealed class Connection
{
    // The server's own request cap is 1 MiB; this constant doubles that
    // as headroom, so a claimed length beyond it is definitely a corrupt
    // or malicious frame, never just a legitimately large value.
    private const int MaxValueLength = 2 * 1024 * 1024;

    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    /// <summary>ADR-0019: negotiated during identify — when true, every
    /// request carries a tag the server echoes, and the read loop verifies
    /// the echo against the oldest pending slot before resolving it.</summary>
    private readonly bool _tagged;
    // A u32 wrapping counter (ADR-0019), claimed only inside the
    // _writeGate critical section — never touched concurrently, so no
    // Interlocked ceremony is needed here the way _closedFlag needs one.
    private uint _nextTag;
    private readonly ConcurrentQueue<(TaskCompletionSource<(byte Marker, byte[]? Value)> Tcs, uint? Tag)> _pending = new();
    private readonly Stopwatch _sinceLastUse = Stopwatch.StartNew();
    private readonly Action? _onClosed;
    // 0 = open, 1 = closed. An int (not a bool) so Close() can gate on it
    // with Interlocked.Exchange: the previous plain "if (_closed) return;
    // _closed = true;" was a non-atomic check-then-set, so two concurrent
    // Close() calls could both pass the check and both run the body,
    // double-firing _onClosed and corrupting the open-target counter it
    // decrements. Exchange makes "am I the first caller to close this"
    // a single atomic step — exactly the guarantee Java's poison() gets
    // from `synchronized`.
    private int _closedFlag;

    /// <summary><paramref name="tagged"/> (ADR-0019): whether identify
    /// negotiated tagged mode for this connection. <paramref name="onClosed"/>,
    /// when given, fires exactly once — the first time this connection
    /// actually closes — no matter how many call sites call
    /// <see cref="Close"/> on it. Lets the client hook every place it
    /// closes or discards a connection (issue #12's forgotten-close
    /// tracking) without each call site worrying about double-counting.</summary>
    internal Connection(Stream stream, bool tagged = false, Action? onClosed = null)
    {
        _stream = stream;
        _tagged = tagged;
        _onClosed = onClosed;
        _ = ReadLoopAsync();
    }

    internal bool IsClosed => Volatile.Read(ref _closedFlag) != 0;

    internal TimeSpan Idle => _sinceLastUse.Elapsed;

    /// <summary>Idempotent — safe to call concurrently from more than one
    /// caller. Rejects every request still pending with a
    /// connection-closed error — the read loop's own exit path (a failed
    /// read) also routes through here, so this is the single place
    /// draining ever happens.</summary>
    internal void Close() => CloseWithReason(new ConnectionLostException("nanocached: connection closed"));

    /// <summary>Same idempotency as <see cref="Close"/>, but drains every
    /// still-pending request with <paramref name="reason"/> instead of the
    /// generic closed error — used by the ADR-0019 tag check below so that
    /// every request behind a desynced one is rejected with a message that
    /// actually says so, not a generic "connection closed".</summary>
    private void CloseWithReason(Exception reason)
    {
        if (Interlocked.Exchange(ref _closedFlag, 1) != 0) return;
        _stream.Dispose();
        _onClosed?.Invoke();
        while (_pending.TryDequeue(out var pending))
        {
            pending.Tcs.TrySetException(reason);
        }
    }

    internal async Task<byte[]?> GetAsync(byte[] key)
    {
        var (marker, value) = await RequestAsync(tag => Frame($"G {key.Length}{TagField(tag)}\n", key, null))
            .ConfigureAwait(false);
        return marker switch
        {
            (byte)'V' => value,
            (byte)'N' => null,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Mismatch(marker),
        };
    }

    /// <summary><paramref name="ttlSeconds"/> of 0 means no expiry, mapped
    /// on the wire exactly as the old absent-TTL header was.</summary>
    internal async Task SetAsync(byte[] key, byte[] value, long ttlSeconds)
    {
        var (marker, _) = await RequestAsync(tag =>
        {
            string header = ttlSeconds == 0
                ? $"S {key.Length} {value.Length}{TagField(tag)}\n"
                : $"S {key.Length} {value.Length} {ttlSeconds}{TagField(tag)}\n";
            return Frame(header, key, value);
        }).ConfigureAwait(false);
        if (marker == (byte)'W') throw new WrongNodeException();
        if (marker != (byte)'S') throw Mismatch(marker);
    }

    internal async Task<bool> DeleteAsync(byte[] key)
    {
        var (marker, _) = await RequestAsync(tag => Frame($"D {key.Length}{TagField(tag)}\n", key, null))
            .ConfigureAwait(false);
        return marker switch
        {
            (byte)'D' => true,
            (byte)'N' => false,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Mismatch(marker),
        };
    }

    private static byte[] Frame(string header, byte[] key, byte[]? value)
    {
        byte[] headerBytes = Encoding.ASCII.GetBytes(header);
        var frame = new byte[headerBytes.Length + key.Length + (value?.Length ?? 0)];
        headerBytes.CopyTo(frame, 0);
        key.CopyTo(frame, headerBytes.Length);
        value?.CopyTo(frame, headerBytes.Length + key.Length);
        return frame;
    }

    /// <summary>ADR-0019: on a tagged connection every request header's
    /// last field is the client's tag; an untagged connection's wire bytes
    /// are unchanged from before this field existed.</summary>
    private static string TagField(uint? tag) => tag is null ? "" : $" {tag}";

    /// <summary>
    /// A well-formed response of the wrong kind (a <c>S</c> answering a G)
    /// means the request/response streams are misaligned — every later
    /// response would answer the wrong request, silently returning other
    /// keys' data. Poison the connection, and classify as connection-lost
    /// so the client's retry layer redials and retries once. Requests
    /// still pending behind this one may already have been resolved with
    /// misaligned data by the time this runs — an inherent limitation of
    /// matching-by-order pipelining shared with the TypeScript SDK's
    /// Connection (doc/adr/0016-*.md), not something this SDK introduces.
    /// This is the second line of defense: on a tagged connection, the
    /// read loop's own tag check (ADR-0019) normally catches a desync like
    /// this before any response is ever handed to a caller.
    /// </summary>
    private ConnectionLostException Mismatch(byte marker)
    {
        Close();
        return new ConnectionLostException(
            $"nanocached: response '{(char)marker}' does not match the request (connection desynced)");
    }

    /// <summary>Enqueues a pending slot and writes the built frame under
    /// one semaphore — see the class doc comment — then awaits its own
    /// <see cref="TaskCompletionSource{TResult}"/>, not the stream.
    /// Nothing here needs to guard against the caller abandoning this
    /// await (e.g. racing it with a timeout): unlike some other SDKs'
    /// ports of this design, this method never receives a
    /// <see cref="CancellationToken"/> to pass into the underlying
    /// <see cref="Stream"/> calls, so the write, once started, always
    /// runs to completion regardless of what the caller does
    /// afterward — and completing an abandoned
    /// <see cref="TaskCompletionSource{TResult}"/> that nothing is
    /// awaiting anymore is harmless.</summary>
    /// <param name="buildFrame">Builds the wire frame from this request's
    /// claimed tag (<c>null</c> on an untagged connection). Called inside
    /// the write-gate critical section, after the tag is claimed but
    /// before anything is enqueued — an encoder that rejects its input
    /// must fail with nothing queued, or the next response would resolve
    /// an orphaned waiter and desync the stream (ADR-0019).</param>
    private async Task<(byte Marker, byte[]? Value)> RequestAsync(Func<uint?, byte[]> buildFrame)
    {
        if (IsClosed)
        {
            throw new ConnectionLostException("nanocached: connection is closed");
        }

        var tcs = new TaskCompletionSource<(byte Marker, byte[]? Value)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (IsClosed)
            {
                throw new ConnectionLostException("nanocached: connection is closed");
            }
            _sinceLastUse.Restart();
            // ADR-0019: the tag is claimed in the same critical section
            // that enqueues the waiter and writes the frame (ADR-0016's
            // enqueue+write atomicity), so tag order can never skew from
            // queue/wire order.
            uint? tag = _tagged ? ClaimTag() : null;
            byte[] frame = buildFrame(tag);
            _pending.Enqueue((tcs, tag));
            await _stream.WriteAsync(frame).ConfigureAwait(false);
            await _stream.FlushAsync().ConfigureAwait(false);
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException)
        {
            // The stream state after a failed write is unknown — poison
            // the connection so the client redials lazily.
            Close();
            throw new ConnectionLostException($"nanocached: connection failed: {error.Message}", error);
        }
        finally
        {
            _writeGate.Release();
        }

        return await tcs.Task.ConfigureAwait(false);
    }

    /// <summary>Only ever called from inside the <c>_writeGate</c>
    /// critical section, so no interlocking is needed despite concurrent
    /// callers of <see cref="RequestAsync"/>.</summary>
    private uint ClaimTag()
    {
        uint tag = _nextTag;
        unchecked { _nextTag++; } // wraps at u32, matching the wire's width
        return tag;
    }

    /// <summary>This connection's only reader, for its whole lifetime —
    /// nothing else may read from <see cref="_stream"/>. Consumes
    /// responses off the wire and dispatches each to the oldest pending
    /// request (FIFO — doc/adr/0016-*.md), until a read fails.</summary>
    private async Task ReadLoopAsync()
    {
        while (true)
        {
            (byte Marker, byte[]? Value, uint? Tag) response;
            try
            {
                response = await ReadResponseAsync().ConfigureAwait(false);
            }
            catch (Exception error) when (error is IOException or ObjectDisposedException or EndOfStreamException
                or ConnectionLostException)
            {
                // error belongs to whichever request has been waiting
                // longest — this loop only ever reads one response at a
                // time, in order, so a failure here is always about the
                // oldest pending request specifically, not the
                // connection in general; Close() drains everyone else
                // with a generic "connection closed" instead, since
                // their responses were never received at all.
                if (_pending.TryDequeue(out var failed))
                {
                    failed.Tcs.TrySetException(error);
                }
                Close();
                return;
            }

            bool wasEmpty = _pending.IsEmpty;
            _pending.TryDequeue(out var pending);

            // An unsolicited "busy" response means the server hit its
            // connection limit right after accept and is about to close
            // the connection; it isn't an answer to anything we sent
            // (mirrors the TypeScript SDK's Connection.onData). Busy is
            // always untagged (ADR-0019) — it is never a reply to a
            // specific request.
            if (response.Marker == (byte)'B' && wasEmpty)
            {
                Close();
                return;
            }

            if (pending.Tcs is null)
            {
                // Unsolicited and not the known busy case — desync.
                Close();
                return;
            }

            // ADR-0019: on a tagged connection, verify the echoed tag
            // against the request this response is about to answer —
            // *before* it can reach any caller. A mismatch means the
            // streams are misaligned; unlike the caller-side kind check
            // (Mismatch()), catching it here stops the misdelivery
            // instead of merely noticing it later.
            if (_tagged && response.Tag != pending.Tag)
            {
                var error = new ConnectionLostException(
                    $"nanocached: response tag {response.Tag} does not answer request tag {pending.Tag} "
                    + "(connection desynced)");
                // pending has already been dequeued, so CloseWithReason's
                // own drain won't reach it — poison the rest of the
                // queue with the same desynced reason, then reject this
                // one directly.
                CloseWithReason(error);
                pending.Tcs.TrySetException(error);
                return;
            }

            pending.Tcs.TrySetResult((response.Marker, response.Value));
        }
    }

    private async Task<(byte Marker, byte[]? Value, uint? Tag)> ReadResponseAsync()
    {
        byte marker = await ReadByteAsync().ConfigureAwait(false);
        switch (marker)
        {
            case (byte)'V':
            {
                // Untagged: `V <len>`. Tagged: `V <len> <tag>` (ADR-0019).
                string[] fields = (await ReadLineAsync().ConfigureAwait(false)).Split(' ');
                if (fields.Length != (_tagged ? 2 : 1))
                {
                    throw new ConnectionLostException("nanocached: invalid value header in response");
                }

                // A non-numeric, negative, or absurd length (the server
                // caps requests at 1 MiB) is protocol garbage: the
                // connection is desynced mid-frame and must be poisoned,
                // and the error must be connection-classified so the
                // redial/retry layer handles it (issue #8).
                if (!int.TryParse(fields[0], out int length) || length < 0 || length > MaxValueLength)
                {
                    throw new ConnectionLostException("nanocached: invalid value length in response");
                }
                uint? tag = _tagged ? ParseTag(fields[1]) : null;

                var value = new byte[length];
                await _stream.ReadExactlyAsync(value).ConfigureAwait(false);
                return (marker, value, tag);
            }
            case (byte)'S':
            case (byte)'D':
            case (byte)'N':
            case (byte)'W':
            {
                if (!_tagged)
                {
                    await ReadByteAsync().ConfigureAwait(false); // the trailing '\n'
                    return (marker, null, null);
                }

                // Tagged: `<marker> <tag>\n` — a byte other than the
                // space separator here means the server answered with the
                // untagged 2-byte form on a connection it agreed to tag,
                // i.e. the response is missing its tag entirely: the
                // streams are desynced exactly as much as an echoed wrong
                // tag would mean.
                byte next = await ReadByteAsync().ConfigureAwait(false);
                if (next != (byte)' ')
                {
                    throw new ConnectionLostException(
                        "nanocached: response is missing its tag (connection desynced)");
                }
                uint tag = ParseTag(await ReadLineAsync().ConfigureAwait(false));
                return (marker, null, tag);
            }
            case (byte)'B':
                // Busy is unsolicited and always bare (ADR-0019) — never
                // tagged, even on a tagged connection.
                await ReadByteAsync().ConfigureAwait(false); // the trailing '\n'
                return (marker, null, null);
            default:
                // A garbage marker means the stream is desynced; poison
                // and classify as connection-level (issue #8) so the
                // retry layer redials instead of failing the op outright.
                throw new ConnectionLostException(
                    $"nanocached: unexpected response from server: {(char)marker}");
        }
    }

    private static uint ParseTag(string field)
    {
        if (!uint.TryParse(field, out uint tag))
        {
            throw new ConnectionLostException("nanocached: invalid response tag");
        }
        return tag;
    }

    private async Task<byte> ReadByteAsync()
    {
        var single = new byte[1];
        await _stream.ReadExactlyAsync(single).ConfigureAwait(false);
        return single[0];
    }

    /// <summary>Reads up to (and consuming) the next '\n'.</summary>
    private async Task<string> ReadLineAsync()
    {
        var line = new StringBuilder();
        while (true)
        {
            byte b = await ReadByteAsync().ConfigureAwait(false);
            if (b == (byte)'\n') return line.ToString().Trim();
            line.Append((char)b);
        }
    }
}
