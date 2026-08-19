//! doc/adr/0013-*.md: transparent, opt-in value compression. Values are
//! prefixed with a one-byte marker once `compress` is enabled on a
//! client — `0x00` for stored-as-is (below threshold, or compression
//! didn't shrink it), `0x01` for raw-DEFLATE-compressed (RFC 1951, no
//! zlib/gzip wrapper). Never present when `compress` is off.
//!
//! Gated behind the `compression` Cargo feature (default-on, same shape
//! as `tls` — see `Options::compress`): `resolve_compression` is the only
//! item this module exposes when the feature is disabled, so `compress(true)`
//! without it fails clearly at `connect()` time instead of silently
//! compiling to a no-op.

use crate::error::{Error, Result};

/// Resolves `Options::compress` against whether the `compression` feature
/// was compiled in — mirrors `identify::resolve_tls`'s shape exactly.
#[cfg(feature = "compression")]
pub(crate) fn resolve_compression(enabled: bool) -> Result<bool> {
    Ok(enabled)
}

#[cfg(not(feature = "compression"))]
pub(crate) fn resolve_compression(enabled: bool) -> Result<bool> {
    if enabled {
        return Err(Error::InvalidArgument(
            "nanocached: compress(true) requires the `compression` feature".to_string(),
        ));
    }
    Ok(false)
}

#[cfg(feature = "compression")]
mod deflate {
    use super::*;
    use flate2::read::{DeflateDecoder, DeflateEncoder};
    use flate2::Compression as Flate2Compression;
    use std::io::Read;

    const MARKER_RAW: u8 = 0x00;
    const MARKER_DEFLATE: u8 = 0x01;

    /// Bounds a DEFLATE value's expanded output. The wire cap
    /// (`MAX_VALUE_LENGTH`) bounds only the compressed bytes received;
    /// without this, a small, highly-repetitive value written by a
    /// compromised node could expand to gigabytes and exhaust client
    /// memory on a plain `get` (a decompression bomb). Far above any
    /// realistic cache value.
    const MAX_DECOMPRESSED_LENGTH: u64 = 64 * 1024 * 1024;

    /// Below `threshold`, or when compressing doesn't actually shrink the
    /// value (incompressible data), the marker byte alone is added and
    /// the value is stored unchanged. Always returns a value with the
    /// marker byte prefixed.
    pub(crate) fn compress_value(value: &[u8], threshold: usize) -> Vec<u8> {
        if value.len() < threshold {
            return prefixed(MARKER_RAW, value);
        }

        let mut encoder = DeflateEncoder::new(value, Flate2Compression::default());
        let mut compressed = Vec::new();
        // In-memory slice-backed encoder — an I/O error here would mean a
        // std::io bug, not a real failure; the `tls`/`compression` split
        // already keeps this crate's own errors distinct from that.
        encoder
            .read_to_end(&mut compressed)
            .expect("compressing an in-memory slice cannot fail");

        if compressed.len() < value.len() {
            prefixed(MARKER_DEFLATE, &compressed)
        } else {
            prefixed(MARKER_RAW, value)
        }
    }

    /// The `get`/`get_bytes` counterpart to `compress_value`. Only ever
    /// called when `compress` is enabled on this client — see
    /// doc/adr/0013-*.md.
    pub(crate) fn decompress_value(value: &[u8]) -> Result<Vec<u8>> {
        let Some((&marker, body)) = value.split_first() else {
            return Err(Error::Decompression(
                "nanocached: compress is enabled but this value has no marker byte (it's \
                 empty) — was it written by a client with compress disabled?"
                    .to_string(),
            ));
        };

        match marker {
            MARKER_RAW => Ok(body.to_vec()),
            MARKER_DEFLATE => {
                let decoder = DeflateDecoder::new(body);
                // +1 so an output of exactly the cap still reads, letting a
                // value that exceeds it be detected, not silently truncated.
                let mut limited = decoder.take(MAX_DECOMPRESSED_LENGTH + 1);
                let mut out = Vec::new();
                limited.read_to_end(&mut out).map_err(|error| {
                    Error::Decompression(format!(
                        "nanocached: failed to decompress a value marked as compressed — did \
                         every client sharing this key enable compress (doc/adr/0013-*.md)? \
                         ({error})"
                    ))
                })?;
                if out.len() as u64 > MAX_DECOMPRESSED_LENGTH {
                    return Err(Error::Decompression(
                        "nanocached: decompressed value exceeds the maximum size — possible \
                         decompression bomb"
                            .to_string(),
                    ));
                }
                Ok(out)
            }
            other => Err(Error::Decompression(format!(
                "nanocached: unrecognized compression marker byte {other} — was this value \
                 written by a client with compress disabled (doc/adr/0013-*.md)?"
            ))),
        }
    }

    fn prefixed(marker: u8, body: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(body.len() + 1);
        result.push(marker);
        result.extend_from_slice(body);
        result
    }
}

#[cfg(feature = "compression")]
pub(crate) use deflate::{compress_value, decompress_value};

// Unreachable at runtime without the `compression` feature:
// `resolve_compression` rejects `compress(true)` at `connect()` time, so
// `Inner::compress` can only be `true` — the guard every call site checks
// before calling these — when the feature is actually compiled in. These
// stubs exist purely so client.rs's call sites don't need their own cfg
// gating (mirrors identify::resolve_tls's shape).
#[cfg(not(feature = "compression"))]
pub(crate) fn compress_value(_value: &[u8], _threshold: usize) -> Vec<u8> {
    unreachable!("compress_value called without the `compression` feature")
}

#[cfg(not(feature = "compression"))]
pub(crate) fn decompress_value(_value: &[u8]) -> Result<Vec<u8>> {
    unreachable!("decompress_value called without the `compression` feature")
}

#[cfg(test)]
#[cfg(feature = "compression")]
mod tests {
    use super::*;

    // doc/adr/0013-*.md: one canonical plaintext and its raw-DEFLATE
    // compressed bytes (produced once via Python's zlib, level 6,
    // wbits=-15), hardcoded identically into every SDK's test suite — the
    // same duplicated-pinned-constant pattern the hash-ring FNV-1a/score
    // vectors use (see hash_ring.rs). This asserts real cross-language
    // interop: that this SDK's decompressor accepts bytes another
    // language's compressor produced, not merely that it round-trips its
    // own output.
    const CROSS_LANGUAGE_PLAINTEXT: &[u8] = b"{\"user\":\"alice\",\"role\":\"admin\",\"tags\":[\"a\",\"b\",\"c\"],\"note\":\"the quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog\"}";
    const CROSS_LANGUAGE_DEFLATE_HEX: &str = "958acb0d833010055b59bd3315d00ae2609bc531b1bdc41f9280d23b7609393ce98d662ed4cc0923947786312089e78e4b70b1615136639ca0dad76d06f38028a537e5c1f4aace3c492779475ae5435b0d7b26393851d75e9d5f5ac4d21f2d7e37";

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn round_trips_a_value_at_or_above_the_threshold() {
        let value = vec![b'x'; 1000];
        let stored = compress_value(&value, 256);
        assert_eq!(stored[0], 0x01);
        assert!(stored.len() < value.len());
        assert_eq!(decompress_value(&stored).unwrap(), value);
    }

    #[test]
    fn leaves_a_value_below_the_threshold_uncompressed() {
        let value = b"short".to_vec();
        let stored = compress_value(&value, 256);
        assert_eq!(stored[0], 0x00);
        assert_eq!(&stored[1..], &value[..]);
        assert_eq!(decompress_value(&stored).unwrap(), value);
    }

    #[test]
    fn incompressible_data_passes_through_unbloated() {
        // A small dependency-free xorshift64* PRNG (no extra crate
        // needed) — high enough entropy that DEFLATE cannot shrink it
        // below its own overhead, unlike a simple arithmetic sequence.
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut next = || {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545F4914F6CDD1D)
        };
        let value: Vec<u8> = (0..512).map(|_| next() as u8).collect();

        let stored = compress_value(&value, 256);
        assert_eq!(stored[0], 0x00);
        assert_eq!(&stored[1..], &value[..]);
        assert_eq!(decompress_value(&stored).unwrap(), value);
    }

    #[test]
    fn round_trips_an_empty_value() {
        let stored = compress_value(&[], 256);
        assert_eq!(decompress_value(&stored).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn decompresses_the_pinned_cross_language_vector() {
        let mut compressed = vec![0x01u8];
        compressed.extend(hex_to_bytes(CROSS_LANGUAGE_DEFLATE_HEX));
        assert_eq!(
            decompress_value(&compressed).unwrap(),
            CROSS_LANGUAGE_PLAINTEXT
        );
    }

    #[test]
    fn unrecognized_marker_byte_errors() {
        assert!(decompress_value(&[0x02, 1, 2, 3]).is_err());
    }

    #[test]
    fn empty_value_with_compress_enabled_errors() {
        assert!(decompress_value(&[]).is_err());
    }

    #[test]
    fn corrupt_deflate_marked_value_errors() {
        assert!(decompress_value(&[0x01, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }
}
