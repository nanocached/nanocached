//! Process-global tracking of open sockets per connect() target — a
//! programming-error guard that warns when `connect()` is called again
//! for a target that still has open sockets from a previous, unclosed
//! client (mirrors the TypeScript SDK's `openTargets`,
//! sdk/typescript/src/client.ts). Purely diagnostic; never affects
//! behavior.
//!
//! Keyed by the client's winning connect address ("host:port" of
//! whichever configured address answered `connect()`), not by the
//! address of each individual socket — a cluster client's member
//! connections all count against the discovery address that produced
//! them, exactly as the TypeScript implementation counts every socket
//! against `this.url`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static OPEN_TARGETS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();

fn targets() -> &'static Mutex<HashMap<String, usize>> {
    OPEN_TARGETS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Called once per socket the SDK successfully opens for a client.
pub(crate) fn increment(key: &str) {
    let mut targets = targets()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    *targets.entry(key.to_string()).or_insert(0) += 1;
}

/// Called once per socket the SDK closes or discards. A no-op if `key`
/// has no recorded opens (never double-decrements: see the call sites in
/// `Connection::close`, which only decrement on the closed=false→true
/// transition).
pub(crate) fn decrement(key: &str) {
    let mut targets = targets()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if let Some(count) = targets.get_mut(key) {
        if *count <= 1 {
            targets.remove(key);
        } else {
            *count -= 1;
        }
    }
}

/// Whether `key` currently has any open sockets.
pub(crate) fn has_open(key: &str) -> bool {
    let targets = targets()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    targets.contains_key(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A key unique to this test avoids collisions with whatever other
    // tests (in this crate or doctests) touch the process-global map
    // concurrently.
    #[test]
    fn tracks_opens_and_closes_per_key() {
        let key = "open-targets-test.invalid:65535";
        assert!(!has_open(key));

        increment(key);
        assert!(has_open(key));

        increment(key);
        decrement(key);
        assert!(has_open(key), "one socket is still open");

        decrement(key);
        assert!(!has_open(key));

        // Decrementing past zero must not panic or go negative.
        decrement(key);
        assert!(!has_open(key));
    }
}
