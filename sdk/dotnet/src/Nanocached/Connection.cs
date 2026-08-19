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
/// </summary>
internal sealed class Connection
{
    // The server never stores values above its 1 MiB request limit, so a
    // claimed length beyond this is a corrupt or malicious frame.
    private const int MaxValueLength = 2 * 1024 * 1024;

    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly ConcurrentQueue<TaskCompletionSource<(byte Marker, byte[]? Value)>> _pending = new();
    private readonly Stopwatch _sinceLastUse = Stopwatch.StartNew();
    private readonly Action? _onClosed;
    private volatile bool _closed;

    /// <summary><paramref name="onClosed"/>, when given, fires exactly
    /// once — the first time this connection actually closes — no matter
    /// how many call sites call <see cref="Close"/> on it. Lets the client
    /// hook every place it closes or discards a connection (issue #12's
    /// forgotten-close tracking) without each call site worrying about
    /// double-counting.</summary>
    internal Connection(Stream stream, Action? onClosed = null)
    {
        _stream = stream;
        _onClosed = onClosed;
        _ = ReadLoopAsync();
    }

    internal bool IsClosed => _closed;

    internal TimeSpan Idle => _sinceLastUse.Elapsed;

    /// <summary>Idempotent. Rejects every request still pending with a
    /// connection-closed error — the read loop's own exit path (a failed
    /// read) also routes through here, so this is the single place
    /// draining ever happens.</summary>
    internal void Close()
    {
        if (_closed) return;
        _closed = true;
        _stream.Dispose();
        _onClosed?.Invoke();
        while (_pending.TryDequeue(out var tcs))
        {
            tcs.TrySetException(new ConnectionLostException("nanocached: connection closed"));
        }
    }

    internal async Task<byte[]?> GetAsync(byte[] key)
    {
        var frame = Frame($"G {key.Length}\n", key, null);
        var (marker, value) = await RequestAsync(frame).ConfigureAwait(false);
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
        string header = ttlSeconds == 0
            ? $"S {key.Length} {value.Length}\n"
            : $"S {key.Length} {value.Length} {ttlSeconds}\n";
        var (marker, _) = await RequestAsync(Frame(header, key, value)).ConfigureAwait(false);
        if (marker == (byte)'W') throw new WrongNodeException();
        if (marker != (byte)'S') throw Mismatch(marker);
    }

    internal async Task<bool> DeleteAsync(byte[] key)
    {
        var (marker, _) = await RequestAsync(Frame($"D {key.Length}\n", key, null)).ConfigureAwait(false);
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
    /// </summary>
    private ConnectionLostException Mismatch(byte marker)
    {
        Close();
        return new ConnectionLostException(
            $"nanocached: response '{(char)marker}' does not match the request (connection desynced)");
    }

    /// <summary>Enqueues a pending slot and writes <paramref name="frame"/>
    /// under one semaphore — see the class doc comment — then awaits its
    /// own <see cref="TaskCompletionSource{TResult}"/>, not the stream.
    /// Nothing here needs to guard against the caller abandoning this
    /// await (e.g. racing it with a timeout): unlike some other SDKs'
    /// ports of this design, this method never receives a
    /// <see cref="CancellationToken"/> to pass into the underlying
    /// <see cref="Stream"/> calls, so the write, once started, always
    /// runs to completion regardless of what the caller does
    /// afterward — and completing an abandoned
    /// <see cref="TaskCompletionSource{TResult}"/> that nothing is
    /// awaiting anymore is harmless.</summary>
    private async Task<(byte Marker, byte[]? Value)> RequestAsync(byte[] frame)
    {
        if (_closed)
        {
            throw new ConnectionLostException("nanocached: connection is closed");
        }

        var tcs = new TaskCompletionSource<(byte Marker, byte[]? Value)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_closed)
            {
                throw new ConnectionLostException("nanocached: connection is closed");
            }
            _sinceLastUse.Restart();
            _pending.Enqueue(tcs);
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

    /// <summary>This connection's only reader, for its whole lifetime —
    /// nothing else may read from <see cref="_stream"/>. Consumes
    /// responses off the wire and dispatches each to the oldest pending
    /// request (FIFO — doc/adr/0016-*.md), until a read fails.</summary>
    private async Task ReadLoopAsync()
    {
        while (true)
        {
            (byte Marker, byte[]? Value) response;
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
                    failed.TrySetException(error);
                }
                Close();
                return;
            }

            bool wasEmpty = _pending.IsEmpty;
            _pending.TryDequeue(out var tcs);

            // An unsolicited "busy" response means the server hit its
            // connection limit right after accept and is about to close
            // the connection; it isn't an answer to anything we sent
            // (mirrors the TypeScript SDK's Connection.onData).
            if (response.Marker == (byte)'B' && wasEmpty)
            {
                Close();
                return;
            }
            if (tcs is null)
            {
                // Unsolicited and not the known busy case — desync.
                Close();
                return;
            }
            tcs.TrySetResult(response);
        }
    }

    private async Task<(byte Marker, byte[]? Value)> ReadResponseAsync()
    {
        byte marker = await ReadByteAsync().ConfigureAwait(false);
        switch (marker)
        {
            case (byte)'V':
            {
                // A non-numeric, negative, or absurd length (the server
                // caps requests at 1 MiB) is protocol garbage: the
                // connection is desynced mid-frame and must be poisoned,
                // and the error must be connection-classified so the
                // redial/retry layer handles it (issue #8).
                if (!int.TryParse(await ReadLineAsync().ConfigureAwait(false), out int length)
                    || length < 0
                    || length > MaxValueLength)
                {
                    throw new ConnectionLostException("nanocached: invalid value length in response");
                }
                var value = new byte[length];
                await _stream.ReadExactlyAsync(value).ConfigureAwait(false);
                return (marker, value);
            }
            case (byte)'S':
            case (byte)'D':
            case (byte)'N':
            case (byte)'W':
            case (byte)'B':
                await ReadByteAsync().ConfigureAwait(false); // the trailing '\n'
                return (marker, null);
            default:
                // A garbage marker means the stream is desynced; poison
                // and classify as connection-level (issue #8) so the
                // retry layer redials instead of failing the op outright.
                throw new ConnectionLostException(
                    $"nanocached: unexpected response from server: {(char)marker}");
        }
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
