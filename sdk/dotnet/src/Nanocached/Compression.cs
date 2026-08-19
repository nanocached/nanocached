using System.IO.Compression;

namespace Nanocached;

/// <summary>
/// doc/adr/0013-*.md: transparent, opt-in value compression. Values are
/// prefixed with a one-byte marker once <c>Compress</c> is enabled on a
/// client — <c>0x00</c> for stored-as-is (below threshold, or
/// compression didn't shrink it), <c>0x01</c> for raw-DEFLATE-compressed
/// (RFC 1951, no zlib/gzip wrapper — <see cref="DeflateStream"/> is,
/// despite the name, already header-less raw DEFLATE). Never present when
/// <c>Compress</c> is off.
/// </summary>
internal static class Compression
{
    private const byte MarkerRaw = 0x00;
    private const byte MarkerDeflate = 0x01;

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
    /// is enabled on this client — see doc/adr/0013-*.md.</summary>
    internal static byte[] DecompressValue(byte[] value)
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
            try
            {
                using var input = new MemoryStream(value, 1, value.Length - 1);
                using var deflate = new DeflateStream(input, CompressionMode.Decompress);
                using var output = new MemoryStream();
                deflate.CopyTo(output);
                return output.ToArray();
            }
            catch (InvalidDataException error)
            {
                throw new DecompressionException(
                    "nanocached: failed to decompress a value marked as compressed — did every "
                    + $"client sharing this key enable compress (doc/adr/0013-*.md)? ({error.Message})",
                    error);
            }
        }

        throw new DecompressionException(
            $"nanocached: unrecognized compression marker byte {marker} — was this value written "
            + "by a client with compress disabled (doc/adr/0013-*.md)?");
    }

    private static byte[] Prefixed(byte marker, byte[] body)
    {
        byte[] result = new byte[body.Length + 1];
        result[0] = marker;
        Array.Copy(body, 0, result, 1, body.Length);
        return result;
    }
}
