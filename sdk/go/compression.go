package nanocached

// doc/adr/0013-*.md: transparent, opt-in value compression. Values are
// prefixed with a one-byte marker once Config.Compress is enabled —
// compressionMarkerRaw (0x00) for stored-as-is (below threshold, or
// compression didn't shrink it), compressionMarkerDeflate (0x01) for
// raw-DEFLATE-compressed (RFC 1951, no zlib/gzip wrapper — compress/flate
// is already header-less raw DEFLATE). Never present when Compress is
// off.

import (
	"bytes"
	"compress/flate"
	"io"
)

const (
	compressionMarkerRaw     = 0x00
	compressionMarkerDeflate = 0x01
)

// maxDecompressedLength bounds a DEFLATE value's expanded output. The wire
// cap (maxValueLength) bounds only the compressed bytes received; without
// this, a small, highly-repetitive value written by a compromised node
// could expand to gigabytes and exhaust client memory on a plain Get (a
// decompression bomb). Far above any realistic cache value.
const maxDecompressedLength = 64 * 1024 * 1024

// compressValue: below threshold, or when compressing doesn't actually
// shrink the value (incompressible data), the marker byte alone is added
// and the value is stored unchanged. Always returns a value with the
// marker byte prefixed.
func compressValue(value []byte, threshold int) []byte {
	if len(value) < threshold {
		return prefixed(compressionMarkerRaw, value)
	}

	var buf bytes.Buffer
	// DefaultCompression is always a valid level and buf is an in-memory
	// bytes.Buffer, so neither the writer nor its writes can fail.
	writer, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	_, _ = writer.Write(value)
	_ = writer.Close()

	if buf.Len() < len(value) {
		return prefixed(compressionMarkerDeflate, buf.Bytes())
	}
	return prefixed(compressionMarkerRaw, value)
}

// decompressValue is the Get/GetBytes counterpart to compressValue. Only
// ever called when Config.Compress is enabled on this client — see
// doc/adr/0013-*.md.
func decompressValue(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, decompressionFailed(
			"compress is enabled but this value has no marker byte (it's empty) — was it " +
				"written by a client with compress disabled?")
	}

	marker, body := value[0], value[1:]
	switch marker {
	case compressionMarkerRaw:
		return append([]byte(nil), body...), nil
	case compressionMarkerDeflate:
		reader := flate.NewReader(bytes.NewReader(body))
		defer reader.Close()
		// +1 so an output of exactly the cap still reads, letting a value
		// that exceeds it be detected rather than silently truncated.
		out, err := io.ReadAll(io.LimitReader(reader, maxDecompressedLength+1))
		if err != nil {
			return nil, decompressionFailed(
				"failed to decompress a value marked as compressed — did every client sharing " +
					"this key enable compress (doc/adr/0013-*.md)?: " + err.Error())
		}
		if len(out) > maxDecompressedLength {
			return nil, decompressionFailed(
				"decompressed value exceeds the maximum size — possible decompression bomb")
		}
		return out, nil
	default:
		return nil, decompressionFailed(
			"unrecognized compression marker byte — was this value written by a client with " +
				"compress disabled (doc/adr/0013-*.md)?")
	}
}

func prefixed(marker byte, body []byte) []byte {
	result := make([]byte, len(body)+1)
	result[0] = marker
	copy(result[1:], body)
	return result
}
