//! nanocached — async client SDK for the nanocached distributed cache.
//!
//! See <https://github.com/nanocached/nanocached> for the server and
//! protocol, and this crate's README for usage.

mod cas;
mod client;
mod compression;
mod connection;
mod error;
mod hash_ring;
mod identify;
mod open_targets;

pub use cas::{content_digest, CasToken};
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use client::HEDGE_SUSPECT_LATE_LEGS;
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use client::KEEPALIVE_INTERVAL_MS;
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use client::MAX_CONCURRENT_DIALS;
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use client::MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES;
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use client::MAX_INFLIGHT_HEDGE_LOSER_LEGS;
pub use client::{Namespace, NanocachedClient, Options, Stats};
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use client::{PANIC_HEDGE_LEG_ARMED, PANIC_HEDGE_LEG_FOR};
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use connection::MAX_MULTI_GET_RESPONSE_BYTES;
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use connection::REQUEST_TIMEOUT_MS;
pub use error::{Error, Result};
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use hash_ring::HashRing;
#[cfg(feature = "__test_hooks")]
#[doc(hidden)]
pub use identify::DiscoveredNode;
