//! nanocached — async client SDK for the nanocached distributed cache.
//!
//! See <https://github.com/nanocached/nanocached> for the server and
//! protocol, and this crate's README for usage.

mod client;
mod connection;
mod error;
mod hash_ring;
mod identify;

pub use client::{NanocachedClient, Options};
pub use error::{Error, Result};
pub use hash_ring::HashRing;
pub use identify::DiscoveredNode;
#[cfg(feature = "tls")]
pub use identify::TlsConfig;
