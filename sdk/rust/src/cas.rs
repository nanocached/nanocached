//! Compare-and-set (CAS, issue #141): `k`/`x`'s content digest and the
//! opaque token wrapping it.
//!
//! `k`'s `<cond>` field (and `x`'s, which is always a digest) names the
//! key's *expected current value* not by shipping that value back to the
//! server (memcached's own CAS sends an opaque server-assigned generation
//! number instead, for the same reason) but by its digest: SHA-256 of the
//! value's exact stored bytes, truncated to the first 16 bytes. The server
//! and every SDK compute this identically — see docs/protocol.html#cas's
//! pinned cross-language test vector, reproduced in this module's own
//! tests.
//!
//! **Correctness note for callers of this crate with `compress` enabled**
//! (value compression): the digest is always computed over the exact
//! bytes the wire carries — the compression marker byte included — never
//! over the decompressed value `get`/`get_bytes` returns, because the
//! server itself never decompresses and so could never reproduce a digest
//! computed the other way. [`NanocachedClient::get_with_token`] handles
//! this correctly on its own; it only matters here for a caller
//! reconstructing a digest via [`content_digest`] directly from a value it
//! already holds — see [`NanocachedClient::replace`]'s doc comment for
//! that path's own caveat.

use std::fmt;

use crate::error::{Error, Result};

/// SHA-256 of `value`, truncated to the first 16 bytes (128 bits) — the
/// digest `k`/`x`'s `<cond>` field carries as 32 lowercase hex characters.
/// Computed identically by the server and every SDK.
///
/// Exposed on its own (not only via [`CasToken`]) so a caller that already
/// holds a value in hand — one it never read back from a `get` — can
/// compute its expected digest without a round trip first. That
/// reconstruction path is only correct if it reproduces the exact bytes
/// the server actually stores byte-for-byte (see [`crate::NanocachedClient::replace`]'s
/// doc comment); reading the digest back via
/// [`crate::NanocachedClient::get_with_token`] instead is always correct,
/// since it hashes the same wire bytes the server itself would compare
/// against.
///
/// ```
/// let digest = nanocached::content_digest(b"nanocached-cas-vector");
/// assert_eq!(
///     nanocached::CasToken::from(digest).to_string(),
///     "36287141940ca57acbd7695ccdde9d43"
/// );
/// ```
pub fn content_digest(value: &[u8]) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(value);
    let mut digest = [0u8; 16];
    digest.copy_from_slice(&hash[..16]);
    digest
}

/// An opaque compare-and-set token: [`content_digest`]'s 16-byte output,
/// wrapped so it's passed around by its own type rather than a bare
/// `[u8; 16]` that could be confused with some other 16-byte value.
///
/// Returned by [`crate::NanocachedClient::get_with_token`] (and
/// [`crate::Namespace::get_with_token`]), and accepted by
/// [`crate::NanocachedClient::replace`]/[`crate::NanocachedClient::delete_if_matches`]
/// (and their `Namespace` counterparts) via `impl Into<CasToken>`, which a
/// bare `[u8; 16]` digest from [`content_digest`] also satisfies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CasToken(pub(crate) [u8; 16]);

impl CasToken {
    /// The raw 16-byte digest this token wraps.
    pub fn digest(&self) -> [u8; 16] {
        self.0
    }

    /// Parses a 32-character lowercase hex digest — the same shape
    /// [`fmt::Display`] produces and the wire itself sends — into a
    /// token. Rejects anything else (wrong length, uppercase, non-hex
    /// characters) as [`Error::InvalidArgument`], since it could never be
    /// a digest this crate or the server would ever produce.
    pub fn from_hex(hex: &str) -> Result<Self> {
        let invalid = || {
            Error::InvalidArgument(format!(
                "nanocached: not a valid CAS digest (want 32 lowercase hex characters): {hex}"
            ))
        };
        if hex.len() != 32 {
            return Err(invalid());
        }
        let mut digest = [0u8; 16];
        let bytes = hex.as_bytes();
        for (byte, chunk) in digest.iter_mut().zip(bytes.chunks_exact(2)) {
            let hi = hex_nibble(chunk[0]).ok_or_else(invalid)?;
            let lo = hex_nibble(chunk[1]).ok_or_else(invalid)?;
            *byte = (hi << 4) | lo;
        }
        Ok(Self(digest))
    }
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

impl From<[u8; 16]> for CasToken {
    fn from(digest: [u8; 16]) -> Self {
        Self(digest)
    }
}

/// The 32-character lowercase hex encoding — exactly what this crate
/// sends on the wire as `k`/`x`'s `<cond>` field.
impl fmt::Display for CasToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pinned cross-language test vector (docs/protocol.html#cas): the
    /// same fixed input hashed identically by the server and every SDK,
    /// duplicated verbatim across every one of those suites — a mismatch
    /// here means CAS silently breaks between languages sharing a
    /// keyspace.
    #[test]
    fn content_digest_matches_the_pinned_cross_language_vector() {
        let digest = content_digest(b"nanocached-cas-vector");
        assert_eq!(
            CasToken::from(digest).to_string(),
            "36287141940ca57acbd7695ccdde9d43"
        );
    }

    #[test]
    fn content_digest_is_deterministic_and_sensitive_to_every_byte() {
        assert_eq!(content_digest(b"Alice"), content_digest(b"Alice"));
        assert_ne!(content_digest(b"Alice"), content_digest(b"alice"));
    }

    #[test]
    fn content_digest_of_empty_input_is_stable() {
        // No special-casing anywhere for an empty value — CAS on an empty
        // stored value is a perfectly ordinary case.
        assert_eq!(content_digest(b""), content_digest(b""));
    }

    #[test]
    fn cas_token_hex_round_trips() {
        let token = CasToken::from(content_digest(b"Alice"));
        assert_eq!(CasToken::from_hex(&token.to_string()).unwrap(), token);
    }

    #[test]
    fn from_hex_rejects_uppercase() {
        assert!(CasToken::from_hex(&"A".repeat(32)).is_err());
    }

    #[test]
    fn from_hex_rejects_the_wrong_length() {
        assert!(CasToken::from_hex("abc").is_err());
        assert!(CasToken::from_hex(&"a".repeat(33)).is_err());
    }

    #[test]
    fn from_hex_rejects_non_hex_characters() {
        assert!(CasToken::from_hex(&"g".repeat(32)).is_err());
    }
}
