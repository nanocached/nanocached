package nanocached

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// doc/adr/0013-*.md: one canonical plaintext and its raw-DEFLATE
// compressed bytes (produced once via Python's zlib, level 6,
// wbits=-15), hardcoded identically into every SDK's test suite — the
// same duplicated-pinned-constant pattern the hash-ring FNV-1a/score
// vectors use. This asserts real cross-language interop: that this SDK's
// decompressor accepts bytes another language's compressor produced, not
// merely that it round-trips its own output.
const crossLanguagePlaintext = `{"user":"alice","role":"admin","tags":["a","b","c"],"note":"the quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog"}`

const crossLanguageDeflateHex = "958acb0d833010055b59bd3315d00ae2609bc531b1bdc41f9280d23b7609393ce9" +
	"8d662ed4cc0923947786312089e78e4b70b1615136639ca0dad76d06f38028a537e5c1f4aace3c492779475ae5435b0d" +
	"7b26393851d75e9d5f5ac4d21f2d7e37"

func TestRoundTripsAValueAtOrAboveTheThreshold(t *testing.T) {
	value := bytes.Repeat([]byte("x"), 1000)
	stored := compressValue(value, 256)
	if stored[0] != compressionMarkerDeflate {
		t.Fatalf("marker = %d, want %d", stored[0], compressionMarkerDeflate)
	}
	if len(stored) >= len(value) {
		t.Fatalf("compressed length %d >= original length %d", len(stored), len(value))
	}
	got, err := decompressValue(stored)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("decompressValue = %v, %v", got, err)
	}
}

func TestLeavesAValueBelowTheThresholdUncompressed(t *testing.T) {
	value := []byte("short")
	stored := compressValue(value, 256)
	if stored[0] != compressionMarkerRaw {
		t.Fatalf("marker = %d, want %d", stored[0], compressionMarkerRaw)
	}
	if !bytes.Equal(stored[1:], value) {
		t.Fatalf("body = %v, want %v", stored[1:], value)
	}
	got, err := decompressValue(stored)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("decompressValue = %v, %v", got, err)
	}
}

func TestIncompressibleDataPassesThroughUnbloated(t *testing.T) {
	value := make([]byte, 512)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	stored := compressValue(value, 256)
	if stored[0] != compressionMarkerRaw {
		t.Fatalf("marker = %d, want %d", stored[0], compressionMarkerRaw)
	}
	if !bytes.Equal(stored[1:], value) {
		t.Fatalf("body = %v, want %v", stored[1:], value)
	}
	got, err := decompressValue(stored)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("decompressValue = %v, %v", got, err)
	}
}

func TestRoundTripsAnEmptyValue(t *testing.T) {
	stored := compressValue(nil, 256)
	got, err := decompressValue(stored)
	if err != nil || len(got) != 0 {
		t.Fatalf("decompressValue = %v, %v", got, err)
	}
}

func TestDecompressesThePinnedCrossLanguageVector(t *testing.T) {
	body, err := hex.DecodeString(crossLanguageDeflateHex)
	if err != nil {
		t.Fatal(err)
	}
	compressed := append([]byte{compressionMarkerDeflate}, body...)
	got, err := decompressValue(compressed)
	if err != nil || string(got) != crossLanguagePlaintext {
		t.Fatalf("decompressValue = %q, %v", got, err)
	}
}

func TestUnrecognizedMarkerByteErrors(t *testing.T) {
	if _, err := decompressValue([]byte{0x02, 1, 2, 3}); !errors.Is(err, ErrDecompression) {
		t.Fatalf("err = %v, want ErrDecompression", err)
	}
}

func TestEmptyValueWithCompressEnabledErrors(t *testing.T) {
	if _, err := decompressValue(nil); !errors.Is(err, ErrDecompression) {
		t.Fatalf("err = %v, want ErrDecompression", err)
	}
}

func TestCorruptDeflateMarkedValueErrors(t *testing.T) {
	if _, err := decompressValue([]byte{compressionMarkerDeflate, 0xFF, 0xFF, 0xFF, 0xFF}); !errors.Is(err, ErrDecompression) {
		t.Fatalf("err = %v, want ErrDecompression", err)
	}
	// Sanity check on the message itself, not just the sentinel.
	if _, err := decompressValue([]byte{compressionMarkerDeflate, 0xFF, 0xFF, 0xFF, 0xFF}); !strings.Contains(err.Error(), "failed to decompress") {
		t.Fatalf("err = %v, want a message mentioning the failure", err)
	}
}
