using System.Diagnostics;
using System.Text;

namespace Nanocached;

/// <summary>
/// One already-identified connection to a single nanocached-node, speaking
/// the cache protocol (<c>G</c>/<c>S</c>/<c>D</c> — the <c>A</c> identify
/// exchange happens in <see cref="Identify"/> before a Connection exists).
/// Requests are serialized per connection (a semaphore around each round
/// trip) — a deliberate v1 simplification over the TypeScript SDK's
/// pipelining: nanocached-node answers in arrival order, so serializing is
/// always correct, just less concurrent. Concurrent callers queue.
/// </summary>
internal sealed class Connection
{
    private readonly Stream _stream;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Stopwatch _sinceLastUse = Stopwatch.StartNew();
    private volatile bool _closed;

    internal Connection(Stream stream)
    {
        _stream = stream;
    }

    internal bool IsClosed => _closed;

    internal TimeSpan Idle => _sinceLastUse.Elapsed;

    internal void Close()
    {
        _closed = true;
        _stream.Dispose();
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
            _ => throw Unexpected(marker),
        };
    }

    internal async Task SetAsync(byte[] key, byte[] value, long? ttlSeconds)
    {
        string header = ttlSeconds is null
            ? $"S {key.Length} {value.Length}\n"
            : $"S {key.Length} {value.Length} {ttlSeconds}\n";
        var (marker, _) = await RequestAsync(Frame(header, key, value)).ConfigureAwait(false);
        if (marker == (byte)'W') throw new WrongNodeException();
        if (marker != (byte)'S') throw Unexpected(marker);
    }

    internal async Task<bool> DeleteAsync(byte[] key)
    {
        var (marker, _) = await RequestAsync(Frame($"D {key.Length}\n", key, null)).ConfigureAwait(false);
        return marker switch
        {
            (byte)'D' => true,
            (byte)'N' => false,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Unexpected(marker),
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

    private static NanocachedException Unexpected(byte marker) =>
        new($"nanocached: unexpected response from server: {(char)marker}");

    private async Task<(byte Marker, byte[]? Value)> RequestAsync(byte[] frame)
    {
        if (_closed)
        {
            throw new ConnectionLostException("nanocached: connection is closed");
        }

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            _sinceLastUse.Restart();
            await _stream.WriteAsync(frame).ConfigureAwait(false);
            await _stream.FlushAsync().ConfigureAwait(false);
            return await ReadResponseAsync().ConfigureAwait(false);
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException or EndOfStreamException)
        {
            // The stream state after a failed round trip is unknown —
            // poison the connection so the client redials lazily.
            Close();
            throw new ConnectionLostException($"nanocached: connection failed: {error.Message}", error);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<(byte Marker, byte[]? Value)> ReadResponseAsync()
    {
        byte marker = await ReadByteAsync().ConfigureAwait(false);
        switch (marker)
        {
            case (byte)'V':
            {
                int length = int.Parse(await ReadLineAsync().ConfigureAwait(false));
                var value = new byte[length];
                await _stream.ReadExactlyAsync(value).ConfigureAwait(false);
                return (marker, value);
            }
            case (byte)'S':
            case (byte)'D':
            case (byte)'N':
            case (byte)'W':
                await ReadByteAsync().ConfigureAwait(false); // the trailing '\n'
                return (marker, null);
            case (byte)'B':
                // Unsolicited busy: connection-limit rejection, server closing.
                Close();
                throw new ConnectionLostException(
                    "nanocached: server rejected the connection (connection limit reached)");
            default:
                Close();
                throw Unexpected(marker);
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
