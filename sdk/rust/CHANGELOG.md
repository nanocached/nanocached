# Changelog

All notable changes to the Rust SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/rust/vX.Y.Z` tags.

## [Unreleased]

### Changed

- **Breaking: added `Error::Authentication`, a new variant of the `Error`
  enum.** The server rejecting the `A` handshake's secret (no secret
  configured when the server requires one, or a wrong one) used to fold
  into `Error::Protocol`; it's now its own `Error::Authentication`
  variant, matching the Go/TypeScript/Python SDKs (`ErrAuthenticationFailed`
  / `AuthenticationError`) and letting callers distinguish a non-transient
  credentials problem — retrying with the same configuration can never
  succeed — from a genuine wire-protocol violation. Any code that
  exhaustively matches on `Error` (no wildcard `_`/catch-all arm) will
  fail to compile until it adds an arm for `Error::Authentication`.
- **Breaking: `Options::reconnect_cooldown(Duration::ZERO)` now means "use
  the default" (1s) instead of disabling the cooldown.** This aligns the
  Rust SDK with the Go SDK, whose zero-value `Config.ReconnectCooldown`
  can't distinguish "not specified" from "explicitly zero" and so has
  always treated zero as "default". To disable the cooldown, call the new
  `Options::disable_reconnect_cooldown()` instead (the Go SDK's
  equivalent is a negative `Config.ReconnectCooldown`). Anyone who was
  passing `Duration::ZERO` to disable the cooldown must switch to
  `disable_reconnect_cooldown()`.
- Read repair (`Options::read_repair(true)`) no longer re-probes the
  primary on a clean miss — it already missed there on the normal read
  path. It now probes only the remaining owners, matching its
  documentation and saving one redundant `G` per repaired miss.
- **MSRV raised from 1.75 to 1.85.** The declared `rust-version = "1.75"`
  was never a real floor: `zeroize` 1.9 (pulled in through `rustls` by the
  default `tls` feature) requires edition 2024, i.e. Cargo 1.85 or newer.
  The manifest now states 1.85 and CI checks it, so the value can no
  longer drift silently (PR #52).
- The crate declares an empty `[workspace]` so Cargo no longer walks up
  to the repository-root server manifest when building from a checkout
  (PR #52). No effect on consumers from crates.io.
- **Breaking: `HashRing::route` now returns `Result<&str>` instead of
  `&str`.** Calling it on an empty ring used to panic (index out of
  bounds); it now returns `Err(Error::InvalidArgument)` instead, matching
  how the rest of this crate's public API reports a caller error. Callers
  using `route`'s return value directly as a `&str` need to unwrap or
  otherwise handle the new `Result`.

### Fixed

- `Options::auth_secret("")` now normalizes to no secret (`None`),
  matching the other SDKs. Previously it sent an explicit zero-length
  secret on the wire, which the server rejects as `EmptySecret` and
  closes without replying — surfacing as an opaque `ConnectionLost`
  instead of connecting as if no `auth_secret` had been given at all.

### Performance

- `HashRing::owners` (used on every routing decision, under the client's
  routing lock) now selects just the top `replicas` nodes instead of
  fully sorting the whole node list — `O(n)` average instead of
  `O(n log n)` in the cluster size. Output is unchanged.

## [0.1.1]

- First tag-driven release of the aligned SDK line (see the repository
  release history for earlier changes).

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/rust/v0.1.1...HEAD
[0.1.1]: https://github.com/nanocached/nanocached/releases/tag/sdk/rust/v0.1.1
