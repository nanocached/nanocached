using System.IO.Compression;

namespace Nanocached;

/// <summary>
/// Value compression: transparent, opt-in value compression. Values are
/// prefixed with a one-byte marker once <c>Compress</c> is enabled on a
/// client — <c>0x00</c> for stored-as-is (below threshold, or
/// compression didn't shrink it), <c>0x01</c> for raw-DEFLATE-compressed
/// (RFC 1951, no zlib/gzip wrapper — <see cref="DeflateStream"/> is,
/// despite the name, already header-less raw DEFLATE). Never present when
/// <c>Compress</c> is off.
/// </summary>
internal static class Compression
{
    /// <summary>
    /// Turns on <c>System.IO.Compression</c>'s strict validation for the
    /// process the first time this class is used, before its own first
    /// <see cref="DeflateStream"/>. Without it, <see cref="DeflateStream"/>
    /// treats input that ends before the stream's final block as the end
    /// of the stream and returns the prefix it could inflate, so a
    /// truncated or corrupt value came back from a read as a shorter value
    /// with no error; with it, the read throws
    /// <see cref="InvalidDataException"/> ("truncated data"), which
    /// <see cref="DecompressValue(byte[])"/> reports as a
    /// <see cref="DecompressionException"/>, as the other SDKs do. The
    /// switch is read once, when <see cref="DeflateStream"/> is first used
    /// in the process, so this only takes effect if nothing used that class
    /// before; an application that does should set
    /// <c>System.IO.Compression.UseStrictValidation</c> in its own runtime
    /// configuration (README, value compression). An application that
    /// already set the switch, either way, is left alone.
    /// </summary>
    static Compression()
    {
        if (!AppContext.TryGetSwitch(StrictValidationSwitch, out _))
        {
            AppContext.SetSwitch(StrictValidationSwitch, true);
        }
    }

    internal const string StrictValidationSwitch = "System.IO.Compression.UseStrictValidation";

    private const byte MarkerRaw = 0x00;
    private const byte MarkerDeflate = 0x01;

    // Bounds a DEFLATE value's expanded output. The wire cap
    // (MaxValueLength) bounds only the compressed bytes received; without
    // this, a small, highly-repetitive value written by a compromised
    // node could expand to gigabytes and exhaust client memory on a plain
    // GetAsync (a decompression bomb). Far above any realistic cache
    // value. Same 64 MiB cap as the other five SDKs (issue #41).
    private const int MaxDecompressedLength = 64 * 1024 * 1024;

    // Bounds the CUMULATIVE decompressed size of a single
    // GetManyAsync/GetManyBytesAsync response (issue #386).
    // MaxDecompressedLength caps one value, but a batch (up to
    // MaxBatchKeys entries) could pair that per-value cap with the key
    // count to force ~MaxBatchKeys * 64 MiB of client allocation from one
    // small, highly-compressible wire response — the per-value bomb
    // defense amplified across the batch. 256 MiB leaves ample room for a
    // legitimate large batch (a 640 KiB average across 400 keys) while
    // bounding the worst case. A mutable field, not a const, only so a
    // test can lower it without allocating the real bound. Same 256 MiB
    // cap as the other five SDKs.
    internal static long MaxMultiGetDecompressedBytes = 256L * 1024 * 1024;

    // Bounds how many bytes of DEFLATE output one client may be producing
    // at any one time, across every concurrent Get/GetMany. The two caps
    // above are per call: N concurrent reads of a value that expands to
    // the per-value cap each decompressed in full at the same time, so 32
    // concurrent reads of one 61 KB value that inflates to 60 MiB failed
    // most of them with OutOfMemoryException in a 2 GiB process. Each
    // DEFLATE decompression reserves the per-value cap from this budget
    // (its output size is not known in advance) and releases it when it
    // returns, so at most four run at once by default; the rest wait.
    // What a caller holds after a read returns is the caller's. A mutable
    // field, not a const, only so a test can lower it; read once, when a
    // client is built.
    internal static long MaxInflightDecompressedBytes = 256L * 1024 * 1024;

    /// <summary><see cref="MaxInflightDecompressedBytes"/> as a counting
    /// semaphore of bytes, one per <c>NanocachedClient</c>. Decompression
    /// is synchronous CPU work on the calling thread, so a caller that
    /// must wait for budget blocks that thread, the same way the
    /// decompression itself does.</summary>
    internal sealed class InflateBudget
    {
        private readonly long _limit;
        private long _available;

        internal InflateBudget(long limit)
        {
            _limit = limit;
            _available = limit;
        }

        /// <summary>Reserves <paramref name="n"/> bytes (clamped to the
        /// whole budget, so a reservation can always be met), waiting
        /// until they are free.</summary>
        internal long Acquire(long n)
        {
            n = Math.Min(n, _limit);
            lock (this)
            {
                while (_available < n) Monitor.Wait(this);
                _available -= n;
            }
            return n;
        }

        internal void Release(long n)
        {
            lock (this)
            {
                _available += n;
                Monitor.PulseAll(this);
            }
        }
    }

    /// <summary>When set, called with <c>"start"</c> once a DEFLATE
    /// decompression holds its reservation and with <c>"end"</c> when it
    /// releases it — only so a test can observe how many decompressions
    /// run at once.</summary>
    internal static Action<string>? InflateTestHook;

    /// <summary>Below <paramref name="threshold"/>, or when compressing
    /// doesn't actually shrink the value (incompressible data), the
    /// marker byte alone is added and the value is stored unchanged.
    /// Always returns a value with the marker byte prefixed.</summary>
    internal static byte[] CompressValue(byte[] value, int threshold)
    {
        if (value.Length < threshold)
        {
            return Prefixed(MarkerRaw, value);
        }

        using var output = new MemoryStream();
        using (var deflate = new DeflateStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            deflate.Write(value, 0, value.Length);
        }
        byte[] compressed = output.ToArray();

        return compressed.Length < value.Length
            ? Prefixed(MarkerDeflate, compressed)
            : Prefixed(MarkerRaw, value);
    }

    /// <summary>The GetAsync/GetBytesAsync counterpart to
    /// <see cref="CompressValue"/>. Only ever called when <c>Compress</c>
    /// is enabled on this client — see value compression.</summary>
    internal static byte[] DecompressValue(byte[] value) => DecompressValue(value, null);

    /// <summary>As <see cref="DecompressValue(byte[])"/>, charging a
    /// DEFLATE value's expansion against <paramref name="budget"/>
    /// (<see cref="MaxInflightDecompressedBytes"/>) for as long as it
    /// runs; a null budget skips the accounting (tests of the codec
    /// itself).</summary>
    internal static byte[] DecompressValue(byte[] value, InflateBudget? budget)
    {
        if (value.Length == 0)
        {
            throw new DecompressionException(
                "nanocached: compress is enabled but this value has no marker byte (it's empty) — "
                + "was it written by a client with compress disabled?");
        }

        byte marker = value[0];

        if (marker == MarkerRaw)
        {
            return value[1..];
        }

        if (marker == MarkerDeflate)
        {
            long reserved = budget?.Acquire(MaxDecompressedLength + 1L) ?? 0;
            // Only a charged decompression is observable: the codec's own
            // tests (no budget) run in parallel with the client's.
            Action<string>? hook = budget is null ? null : InflateTestHook;
            hook?.Invoke("start");
            try
            {
                using var input = new MemoryStream(value, 1, value.Length - 1);
                using var deflate = new DeflateStream(input, CompressionMode.Decompress);
                using var output = new MemoryStream();
                // Copy in counted chunks instead of CopyTo so the total
                // is checked as it grows — an over-cap value stops here
                // rather than expanding fully into memory.
                byte[] chunk = new byte[64 * 1024];
                long total = 0;
                int read;
                while ((read = deflate.Read(chunk, 0, chunk.Length)) > 0)
                {
                    total += read;
                    if (total > MaxDecompressedLength)
                    {
                        throw new DecompressionException(
                            "nanocached: decompressed value exceeds the maximum size — "
                            + "possible decompression bomb");
                    }
                    output.Write(chunk, 0, read);
                }
                return output.ToArray();
            }
            catch (InvalidDataException error)
            {
                throw new DecompressionException(
                    "nanocached: failed to decompress a value marked as compressed — did every "
                    + $"client sharing this key enable compress (value compression)? ({error.Message})",
                    error);
            }
            finally
            {
                hook?.Invoke("end");
                budget?.Release(reserved);
            }
        }

        throw new DecompressionException(
            $"nanocached: unrecognized compression marker byte {marker} — was this value written "
            + "by a client with compress disabled (value compression)?");
    }

    private static byte[] Prefixed(byte marker, byte[] body)
    {
        byte[] result = new byte[body.Length + 1];
        result[0] = marker;
        Array.Copy(body, 0, result, 1, body.Length);
        return result;
    }
}
