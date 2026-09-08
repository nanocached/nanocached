//! Internal support crate backing every `nanocached` binary
//! (`nanocached-node`, `nanocached-proxy`, `nanocached-discovery`, and the
//! `ncd` dev tool). Not published — this exists purely so the binaries can
//! share connection- and process-level glue that has nothing to do with
//! cluster or wire-protocol behavior: TLS setup, the metrics HTTP
//! responder, shutdown-signal handling, the accept-loop backoff, and
//! per-source-IP connection limiting. See `infra`'s own module docs for
//! the exact contents.
//!
//! Deliberately excluded, even though more than one binary implements it:
//! the HRW ring (`fnv1a`/`fmix64` scoring) and every piece of
//! wire-protocol framing/parsing. Those stay independently implemented per
//! binary by repo policy (see `nanocached-proxy`'s and
//! `nanocached-discovery`'s own module docs) — each is pinned by the same
//! cross-implementation test vectors every SDK is also pinned by, so an
//! independent copy in each binary is a deliberate check against a bug
//! that a single shared implementation could hide from every caller at
//! once. `verify-staged-join` exists specifically to exercise the wire
//! protocol from its own from-scratch client and depends on nothing here,
//! including the infra pieces below — pulling it in would blunt exactly
//! the property it exists for.

pub mod infra;
