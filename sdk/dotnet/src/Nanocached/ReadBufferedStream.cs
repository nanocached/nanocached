namespace Nanocached;

/// <summary>
/// Buffers reads only; every write passes straight through to the inner
/// stream unbuffered. Wraps the stream at the single place a connection's
/// <see cref="System.IO.Stream"/> is created (<see cref="Identify"/>'s
/// OpenAsync), turning this SDK's per-byte header reads (<see
/// cref="Connection"/>'s ReadByteAsync/ReadLineAsync, and this class's own
/// identify-handshake reads) from one syscall each into one read that
/// fills the buffer, which then serves many subsequent 1-byte reads.
///
/// <para>Deliberately not the BCL's own <see cref="System.IO.BufferedStream"/>:
/// that type buffers both directions through one shared buffer/position,
/// which is unsafe here — <see cref="Connection"/> has a dedicated reader
/// loop reading continuously for the connection's whole lifetime while
/// requests are written concurrently under their own write gate (the class
/// doc comment's "one concurrent reader and one concurrent writer"), and
/// <see cref="System.IO.BufferedStream"/>'s shared internal state isn't safe under
/// that concurrent-read-while-write duplex pattern — confirmed by testing:
/// wrapping the connection stream in a plain <see cref="System.IO.BufferedStream"/>
/// produced spurious <see cref="ObjectDisposedException"/>
/// ("Cannot access a closed Stream") failures under the test suite's
/// concurrency. This type keeps read state (<see cref="_bufferStart"/>,
/// <see cref="_bufferLength"/>) entirely separate from writes, which never
/// touch it, so there is no such hazard: a write is simply forwarded to
/// the inner stream, which (like the <see cref="System.Net.Sockets.NetworkStream"/>
/// or <see cref="System.Net.Security.SslStream"/> this always wraps) is
/// itself safe for one concurrent reader plus one concurrent writer.</para>
///
/// <para>A read for at least the buffer's own size bypasses the buffer
/// (after first draining whatever is already buffered) and reads directly
/// from the inner stream — so a large read done in one call, like the
/// <c>V</c> response's value body, pays no extra copy.</para>
/// </summary>
internal sealed class ReadBufferedStream : Stream
{
    private const int DefaultBufferSize = 4096;

    private readonly Stream _inner;
    private readonly byte[] _buffer;
    private int _bufferStart;
    private int _bufferLength;

    internal ReadBufferedStream(Stream inner, int bufferSize = DefaultBufferSize)
    {
        _inner = inner;
        _buffer = new byte[bufferSize];
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => _inner.CanWrite;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_bufferLength == 0)
        {
            // Nothing buffered: a caller asking for at least a full
            // buffer's worth goes straight to the inner stream — copying
            // through the buffer first would only add a copy, not save a
            // syscall.
            if (buffer.Length >= _buffer.Length)
            {
                return await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            }
            _bufferLength = await _inner.ReadAsync(_buffer, cancellationToken).ConfigureAwait(false);
            _bufferStart = 0;
            if (_bufferLength == 0) return 0; // end of stream
        }

        int n = Math.Min(buffer.Length, _bufferLength);
        _buffer.AsSpan(_bufferStart, n).CopyTo(buffer.Span);
        _bufferStart += n;
        _bufferLength -= n;
        return n;
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    public override int Read(Span<byte> buffer)
    {
        // Only reached via the synchronous Stream.Read(Span<byte>)
        // surface, which this SDK never calls (every read here is async);
        // implemented for completeness/correctness as a full Stream.
        byte[] rented = new byte[buffer.Length];
        int n = Read(rented, 0, rented.Length);
        rented.AsSpan(0, n).CopyTo(buffer);
        return n;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        _inner.WriteAsync(buffer, cancellationToken);

    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

    public override void Flush() => _inner.Flush();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing) _inner.Dispose();
        base.Dispose(disposing);
    }
}
