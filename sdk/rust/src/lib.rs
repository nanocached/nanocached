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
#[doc(hidden)]
pub use client::KEEPALIVE_INTERVAL_MS;
#[doc(hidden)]
pub use client::MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES;
#[doc(hidden)]
pub use client::MAX_INFLIGHT_HEDGE_LOSER_LEGS;
#[doc(hidden)]
pub use client::NODE_LIST_STALE_AFTER_MS;
pub use client::{Namespace, NanocachedClient, Options, Stats};
#[doc(hidden)]
pub use connection::MAX_MULTI_GET_RESPONSE_BYTES;
#[doc(hidden)]
pub use connection::REQUEST_TIMEOUT_MS;
pub use error::{Error, Result};
pub use hash_ring::HashRing;
pub use identify::DiscoveredNode;
