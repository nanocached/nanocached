//! A cache key as the server addresses it: a flat namespace plus the
//! key bytes within it (issue #105). Namespaces are opaque byte strings
//! with no hierarchy — every framework cache SPI the SDKs target (Spring
//! Cache, JCache, Django, cache-manager) is a flat set of named caches,
//! and a hierarchy would re-import the delimiter-interpretation problem
//! that an explicit, length-prefixed wire field was chosen to avoid.
//!
//! The un-namespaced commands (`G`/`S`/`D`) address the *default*
//! namespace — the empty byte string — so every pre-namespace client
//! keeps working unchanged, and internally everything is uniform: one
//! sub-map per namespace, with legacy traffic in the `""` one.

use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    /// Empty for the default namespace.
    pub namespace: Bytes,
    pub name: Bytes,
}

impl Key {
    pub fn new(namespace: Bytes, name: Bytes) -> Self {
        Self { namespace, name }
    }

    /// A key in the default (empty) namespace — what `G`/`S`/`D` address.
    pub fn unnamespaced(name: Bytes) -> Self {
        Self {
            namespace: Bytes::new(),
            name,
        }
    }

    pub fn is_namespaced(&self) -> bool {
        !self.namespace.is_empty()
    }
}

impl From<Bytes> for Key {
    fn from(name: Bytes) -> Self {
        Self::unnamespaced(name)
    }
}
