import os
import unittest

from nanocached import DecompressionError
from nanocached._compression import compress_value, decompress_value

# doc/adr/0013-*.md: one canonical plaintext and its raw-DEFLATE
# compressed bytes (produced once via Python's own zlib, level 6,
# wbits=-15), hardcoded identically into every SDK's test suite — the
# same duplicated-pinned-constant pattern the hash-ring FNV-1a/score
# vectors use. This asserts real cross-language interop: that this SDK's
# decompressor accepts bytes another language's compressor produced, not
# merely that it round-trips its own output.
CROSS_LANGUAGE_PLAINTEXT = (
    b'{"user":"alice","role":"admin","tags":["a","b","c"],"note":"the quick brown fox '
    b'jumps over the lazy dog the quick brown fox jumps over the lazy dog"}'
)
CROSS_LANGUAGE_DEFLATE_HEX = (
    "958acb0d833010055b59bd3315d00ae2609bc531b1bdc41f9280d23b7609393ce98d662ed4cc0923"
    "947786312089e78e4b70b1615136639ca0dad76d06f38028a537e5c1f4aace3c492779475ae5435b"
    "0d7b26393851d75e9d5f5ac4d21f2d7e37"
)


class CompressValueTests(unittest.TestCase):
    def test_round_trips_a_value_at_or_above_the_threshold(self):
        value = b"x" * 1000
        stored = compress_value(value, 256)
        self.assertEqual(stored[0], 0x01)
        self.assertLess(len(stored), len(value))
        self.assertEqual(decompress_value(stored), value)

    def test_leaves_a_value_below_the_threshold_uncompressed(self):
        value = b"short"
        stored = compress_value(value, 256)
        self.assertEqual(stored[0], 0x00)
        self.assertEqual(stored[1:], value)
        self.assertEqual(decompress_value(stored), value)

    def test_incompressible_data_passes_through_unbloated(self):
        value = os.urandom(512)
        stored = compress_value(value, 256)
        self.assertEqual(stored[0], 0x00)
        self.assertEqual(stored[1:], value)
        self.assertEqual(decompress_value(stored), value)

    def test_round_trips_an_empty_value(self):
        stored = compress_value(b"", 256)
        self.assertEqual(decompress_value(stored), b"")

    def test_decompresses_the_pinned_cross_language_vector(self):
        compressed = bytes([0x01]) + bytes.fromhex(CROSS_LANGUAGE_DEFLATE_HEX)
        self.assertEqual(decompress_value(compressed), CROSS_LANGUAGE_PLAINTEXT)

    def test_unrecognized_marker_byte_raises(self):
        with self.assertRaises(DecompressionError):
            decompress_value(bytes([0x02, 1, 2, 3]))

    def test_empty_value_with_compress_enabled_raises(self):
        with self.assertRaises(DecompressionError):
            decompress_value(b"")

    def test_corrupt_deflate_marked_value_raises(self):
        with self.assertRaises(DecompressionError):
            decompress_value(bytes([0x01, 0xFF, 0xFF, 0xFF, 0xFF]))


if __name__ == "__main__":
    unittest.main()
