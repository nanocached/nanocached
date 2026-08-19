//! nanocached — async client SDK for the nanocached distributed cache.
//!
//! See <https://github.com/nanocached/nanocached> for the server and
//! protocol, and this crate's README for usage.

mod client;
mod compression;
mod connection;
mod error;
mod hash_ring;
mod identify;
mod open_targets;

#[doc(hidden)]
pub use client::KEEPALIVE_INTERVAL_MS;
pub use client::{NanocachedClient, Options};
pub use error::{Error, Result};
pub use hash_ring::HashRing;
pub use identify::DiscoveredNode;
