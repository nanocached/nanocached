package nanocached

// Value compression: transparent, opt-in value compression. Values are
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
	"sync"
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

// maxMultiGetDecompressedBytes bounds the CUMULATIVE decompressed size of a
// single GetMany/GetManyBytes response. maxDecompressedLength caps one
// value, but a batch (up to maxBatchKeys entries) could pair that per-value
// cap with the key count to force ~maxBatchKeys * 64 MiB of client
// allocation from one small, highly-compressible wire response — the
// per-value bomb defense amplified across the batch. 256 MiB leaves ample
// room for a legitimate large batch (a 640 KiB average across 400 keys)
// while bounding the worst case. A var, not a const, only so a test can
// lower it without allocating the real bound.
var maxMultiGetDecompressedBytes int64 = 256 * 1024 * 1024

// maxInflightDecompressedBytes bounds how many bytes of DEFLATE output
// one client may be producing at any one time, across every concurrent
// Get/GetMany. The two caps above are per call: N concurrent reads of a
// value that expands to the per-value cap each decompressed in full at
// the same time, so 32 concurrent reads of one 61 KB value that inflates
// to 60 MiB took a 2 GiB process down. Each DEFLATE decompression
// reserves the per-value cap from this budget (its output size is not
// known in advance) and releases it when it returns, so at most four
// run at once by default; the rest wait. What a caller holds after a
// read returns is the caller's. A var, not a const, only so a test can
// lower it.
var maxInflightDecompressedBytes int64 = 256 * 1024 * 1024

// inflateBudget is maxInflightDecompressedBytes as a counting semaphore
// of bytes, one per Client (see Client.inflate).
type inflateBudget struct {
	mu        sync.Mutex
	cond      *sync.Cond
	available int64
	limit     int64
}

func newInflateBudget(limit int64) *inflateBudget {
	budget := &inflateBudget{available: limit, limit: limit}
	budget.cond = sync.NewCond(&budget.mu)
	return budget
}

// acquire reserves n bytes (clamped to the whole budget, so a reservation
// can always be met), waiting until they are free.
func (b *inflateBudget) acquire(n int64) int64 {
	if n > b.limit {
		n = b.limit
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.available < n {
		b.cond.Wait()
	}
	b.available -= n
	return n
}

func (b *inflateBudget) release(n int64) {
	b.mu.Lock()
	b.available += n
	b.mu.Unlock()
	b.cond.Broadcast()
}

// inflateTestHook, when set, is called with "start" once a decompression
// holds its reservation and with "end" when it releases it — only so a
// test can observe how many decompressions run at once.
var inflateTestHook func(phase string)

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
// Value compression. A DEFLATE value's expansion is charged against
// budget (maxInflightDecompressedBytes) for as long as it runs; a nil
// budget skips the accounting (tests of the codec itself).
func decompressValue(value []byte, budget *inflateBudget) ([]byte, error) {
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
		if budget != nil {
			reserved := budget.acquire(maxDecompressedLength + 1)
			defer budget.release(reserved)
		}
		if inflateTestHook != nil {
			inflateTestHook("start")
			defer inflateTestHook("end")
		}
		reader := flate.NewReader(bytes.NewReader(body))
		defer reader.Close()
		// +1 so an output of exactly the cap still reads, letting a value
		// that exceeds it be detected rather than silently truncated.
		out, err := io.ReadAll(io.LimitReader(reader, maxDecompressedLength+1))
		if err != nil {
			return nil, decompressionFailed(
				"failed to decompress a value marked as compressed — did every client sharing " +
					"this key enable compress (value compression)?: " + err.Error())
		}
		if len(out) > maxDecompressedLength {
			return nil, decompressionFailed(
				"decompressed value exceeds the maximum size — possible decompression bomb")
		}
		return out, nil
	default:
		return nil, decompressionFailed(
			"unrecognized compression marker byte — was this value written by a client with " +
				"compress disabled (value compression)?")
	}
}

func prefixed(marker byte, body []byte) []byte {
	result := make([]byte, len(body)+1)
	result[0] = marker
	copy(result[1:], body)
	return result
}
