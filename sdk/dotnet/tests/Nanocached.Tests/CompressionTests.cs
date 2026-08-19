using System.Security.Cryptography;
using Xunit;

namespace Nanocached.Tests;

public class CompressionTests
{
    // doc/adr/0013-*.md: one canonical plaintext and its raw-DEFLATE
    // compressed bytes (produced once via Python's zlib, level 6,
    // wbits=-15), hardcoded identically into every SDK's test suite — the
    // same duplicated-pinned-constant pattern the hash-ring FNV-1a/score
    // vectors use. This asserts real cross-language interop: that this
    // SDK's decompressor accepts bytes another language's compressor
    // produced, not merely that it round-trips its own output.
    private const string CrossLanguagePlaintext =
        "{\"user\":\"alice\",\"role\":\"admin\",\"tags\":[\"a\",\"b\",\"c\"],\"note\":\"the quick brown "
        + "fox jumps over the lazy dog the quick brown fox jumps over the lazy dog\"}";

    private const string CrossLanguageDeflateHex =
        "958acb0d833010055b59bd3315d00ae2609bc531b1bdc41f9280d23b7609393ce98d662ed4cc0923947786312089e7"
        + "8e4b70b1615136639ca0dad76d06f38028a537e5c1f4aace3c492779475ae5435b0d7b26393851d75e9d5f5ac4d2"
        + "1f2d7e37";

    private static byte[] HexToBytes(string hex)
    {
        byte[] result = new byte[hex.Length / 2];
        for (int i = 0; i < result.Length; i++)
        {
            result[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        }
        return result;
    }

    [Fact]
    public void RoundTripsAValueAtOrAboveTheThreshold()
    {
        byte[] value = System.Text.Encoding.UTF8.GetBytes(new string('x', 1000));
        byte[] stored = Compression.CompressValue(value, 256);
        Assert.Equal(0x01, stored[0]);
        Assert.True(stored.Length < value.Length);
        Assert.Equal(value, Compression.DecompressValue(stored));
    }

    [Fact]
    public void LeavesAValueBelowTheThresholdUncompressed()
    {
        byte[] value = System.Text.Encoding.UTF8.GetBytes("short");
        byte[] stored = Compression.CompressValue(value, 256);
        Assert.Equal(0x00, stored[0]);
        Assert.Equal(value, stored[1..]);
        Assert.Equal(value, Compression.DecompressValue(stored));
    }

    [Fact]
    public void IncompressibleDataPassesThroughUnbloated()
    {
        byte[] value = new byte[512];
        RandomNumberGenerator.Fill(value);
        byte[] stored = Compression.CompressValue(value, 256);
        Assert.Equal(0x00, stored[0]);
        Assert.Equal(value, stored[1..]);
        Assert.Equal(value, Compression.DecompressValue(stored));
    }

    [Fact]
    public void RoundTripsAnEmptyValue()
    {
        byte[] stored = Compression.CompressValue(Array.Empty<byte>(), 256);
        Assert.Equal(Array.Empty<byte>(), Compression.DecompressValue(stored));
    }

    [Fact]
    public void DecompressesThePinnedCrossLanguageVector()
    {
        byte[] body = HexToBytes(CrossLanguageDeflateHex);
        byte[] compressed = new byte[body.Length + 1];
        compressed[0] = 0x01;
        Array.Copy(body, 0, compressed, 1, body.Length);
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(CrossLanguagePlaintext), Compression.DecompressValue(compressed));
    }

    [Fact]
    public void UnrecognizedMarkerByteThrows()
    {
        Assert.Throws<DecompressionException>(() => Compression.DecompressValue(new byte[] { 0x02, 1, 2, 3 }));
    }

    [Fact]
    public void EmptyValueWithCompressEnabledThrows()
    {
        Assert.Throws<DecompressionException>(() => Compression.DecompressValue(Array.Empty<byte>()));
    }

    [Fact]
    public void CorruptDeflateMarkedValueThrows()
    {
        Assert.Throws<DecompressionException>(
            () => Compression.DecompressValue(new byte[] { 0x01, 0xFF, 0xFF, 0xFF, 0xFF }));
    }
}
