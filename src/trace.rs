//! Opt-in key tracing, built only with `--features trace`.
//!
//! A key that is missing or stale after a membership change has to be followed
//! through the cache and the forward paths to tell which write or which sweep
//! decided it, and the failure that shows it (a chaos run, once in some
//! twenty) does not come back on request. With the feature on, every
//! `trace!` site whose key name starts with one of the comma-separated
//! prefixes in `NANOCACHED_TRACE_PREFIX` writes one `TRACE ...` line to
//! stderr; with the variable unset or empty nothing is written. Without the feature
//! the macro expands to nothing, so the release binaries carry no trace code
//! and evaluate none of its arguments.

#[cfg(feature = "trace")]
pub(crate) fn traced(name: &[u8]) -> bool {
    static PREFIXES: std::sync::OnceLock<Option<Vec<u8>>> = std::sync::OnceLock::new();
    match PREFIXES.get_or_init(|| {
        std::env::var("NANOCACHED_TRACE_PREFIX")
            .ok()
            .map(String::into_bytes)
    }) {
        Some(prefixes) => matches_prefix(prefixes, name),
        None => false,
    }
}

/// Whether `name` starts with one of the comma-separated `prefixes`. An
/// empty prefix (an empty variable, or a stray comma) is ignored: every
/// key starts with the empty string, so honouring it would trace them all.
#[cfg(feature = "trace")]
fn matches_prefix(prefixes: &[u8], name: &[u8]) -> bool {
    prefixes
        .split(|b| *b == b',')
        .any(|prefix| !prefix.is_empty() && name.starts_with(prefix))
}

#[cfg(feature = "trace")]
pub(crate) fn name(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}

/// The first eight characters of a peer token, for the handoff trace lines.
/// Cut at a character boundary: a roster token is any UTF-8 a registrant
/// chose, and a byte-index slice would panic inside a multi-byte character.
#[cfg(feature = "trace")]
pub(crate) fn short_token(token: &str) -> String {
    token.chars().take(8).collect()
}

#[cfg(feature = "trace")]
macro_rules! trace {
    ($key:expr, $($arg:tt)+) => {
        if $crate::trace::traced($key) {
            eprintln!("TRACE {}", format_args!($($arg)+));
        }
    };
}

#[cfg(not(feature = "trace"))]
macro_rules! trace {
    ($key:expr, $($arg:tt)+) => {};
}

#[cfg(all(test, feature = "trace"))]
mod tests {
    use super::{matches_prefix, short_token};

    #[test]
    fn a_token_is_cut_at_a_character_boundary() {
        assert_eq!(short_token("0123456789abcdef"), "01234567");
        assert_eq!(short_token("abc"), "abc");
        assert_eq!(short_token(""), "");
        // Byte 8 falls inside the fifth two-byte character.
        assert_eq!(short_token("ééééééééé"), "éééééééé");
        assert_eq!(short_token("日本語日本語日本語"), "日本語日本語日本");
    }

    #[test]
    fn an_empty_variable_traces_nothing() {
        assert!(!matches_prefix(b"", b"chaos-1"));
        assert!(!matches_prefix(b"", b""));
    }

    #[test]
    fn a_stray_comma_does_not_turn_every_key_into_a_match() {
        assert!(!matches_prefix(b",", b"chaos-1"));
        assert!(!matches_prefix(b"touch-,", b"chaos-1"));
        assert!(!matches_prefix(b",touch-", b"chaos-1"));
    }

    #[test]
    fn a_key_matching_any_listed_prefix_is_traced() {
        assert!(matches_prefix(b"touch-,chaos-", b"chaos-1"));
        assert!(matches_prefix(b"touch-,chaos-", b"touch-9"));
        assert!(!matches_prefix(b"touch-,chaos-", b"other"));
    }
}
