//! Opt-in key tracing, built only with `--features trace`.
//!
//! A key that is missing or stale after a membership change has to be followed
//! through the cache and the forward paths to tell which write or which sweep
//! decided it, and the failure that shows it (a chaos run, once in some
//! twenty) does not come back on request. With the feature on, every
//! `trace!` site whose key name starts with one of the comma-separated
//! prefixes in `NANOCACHED_TRACE_PREFIX` writes one `TRACE ...` line to
//! stderr; with the variable unset nothing is written. Without the feature
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
        Some(prefixes) => prefixes
            .split(|b| *b == b',')
            .any(|prefix| name.starts_with(prefix)),
        None => false,
    }
}

#[cfg(feature = "trace")]
pub(crate) fn name(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
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
