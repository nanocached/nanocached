package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class CompressionTest {

    // doc/adr/0013-*.md: one canonical plaintext and its raw-DEFLATE
    // compressed bytes (produced once via Python's zlib, level 6,
    // wbits=-15), hardcoded identically into every SDK's test suite — the
    // same duplicated-pinned-constant pattern the hash-ring FNV-1a/score
    // vectors use. This asserts real cross-language interop: that this
    // SDK's decompressor accepts bytes another language's compressor
    // produced, not merely that it round-trips its own output.
    private static final byte[] CROSS_LANGUAGE_PLAINTEXT =
            ("{\"user\":\"alice\",\"role\":\"admin\",\"tags\":[\"a\",\"b\",\"c\"],\"note\":\"the quick "
                    + "brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog\"}")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final String CROSS_LANGUAGE_DEFLATE_HEX =
            "958acb0d833010055b59bd3315d00ae2609bc531b1bdc41f9280d23b7609393ce98d662ed4cc0923"
                    + "947786312089e78e4b70b1615136639ca0dad76d06f38028a537e5c1f4aace3c492779475ae5435b"
                    + "0d7b26393851d75e9d5f5ac4d21f2d7e37";

    private static byte[] hexToBytes(String hex) {
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    @Test
    void roundTripsAValueAtOrAboveTheThreshold() {
        byte[] value = "x".repeat(1000).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] stored = Compression.compressValue(value, 256);
        assertEquals(0x01, stored[0]);
        assertTrue(stored.length < value.length);
        assertArrayEquals(value, Compression.decompressValue(stored));
    }

    @Test
    void leavesAValueBelowTheThresholdUncompressed() {
        byte[] value = "short".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] stored = Compression.compressValue(value, 256);
        assertEquals(0x00, stored[0]);
        assertArrayEquals(value, Arrays.copyOfRange(stored, 1, stored.length));
        assertArrayEquals(value, Compression.decompressValue(stored));
    }

    @Test
    void incompressibleDataPassesThroughUnbloated() {
        byte[] value = new byte[512];
        new SecureRandom().nextBytes(value);
        byte[] stored = Compression.compressValue(value, 256);
        assertEquals(0x00, stored[0]);
        assertArrayEquals(value, Arrays.copyOfRange(stored, 1, stored.length));
        assertArrayEquals(value, Compression.decompressValue(stored));
    }

    @Test
    void roundTripsAnEmptyValue() {
        byte[] stored = Compression.compressValue(new byte[0], 256);
        assertArrayEquals(new byte[0], Compression.decompressValue(stored));
    }

    @Test
    void decompressesThePinnedCrossLanguageVector() {
        byte[] body = hexToBytes(CROSS_LANGUAGE_DEFLATE_HEX);
        byte[] compressed = new byte[body.length + 1];
        compressed[0] = 0x01;
        System.arraycopy(body, 0, compressed, 1, body.length);
        assertArrayEquals(CROSS_LANGUAGE_PLAINTEXT, Compression.decompressValue(compressed));
    }

    @Test
    void unrecognizedMarkerByteThrows() {
        assertThrows(NanocachedException.DecompressionFailed.class,
                () -> Compression.decompressValue(new byte[] {0x02, 1, 2, 3}));
    }

    @Test
    void emptyValueWithCompressEnabledThrows() {
        assertThrows(NanocachedException.DecompressionFailed.class,
                () -> Compression.decompressValue(new byte[0]));
    }

    @Test
    void anOverCapDecompressedValueThrowsInsteadOfExhaustingMemory() {
        // Regression (issue #41): a small compressed value expanding past
        // 64 MiB used to allocate without bound (a decompression bomb);
        // it must fail as a decompression error like the other SDKs.
        byte[] bomb = new byte[64 * 1024 * 1024 + 1];
        byte[] stored = Compression.compressValue(bomb, 0);
        assertEquals(0x01, stored[0]);
        assertTrue(stored.length < 1024 * 1024); // small on the wire
        NanocachedException.DecompressionFailed error =
                assertThrows(NanocachedException.DecompressionFailed.class,
                        () -> Compression.decompressValue(stored));
        assertTrue(error.getMessage().contains("decompression bomb"));
    }

    @Test
    void aValueOfExactlyTheDecompressionCapStillDecompresses() {
        byte[] atCap = new byte[64 * 1024 * 1024];
        byte[] stored = Compression.compressValue(atCap, 0);
        assertEquals(0x01, stored[0]);
        assertArrayEquals(atCap, Compression.decompressValue(stored));
    }

    @Test
    void corruptDeflateMarkedValueThrows() {
        assertThrows(NanocachedException.DecompressionFailed.class,
                () -> Compression.decompressValue(new byte[] {0x01, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
    }
}
