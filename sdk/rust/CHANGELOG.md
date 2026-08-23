# Changelog

All notable changes to the Rust SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/rust/vX.Y.Z` tags.

## [Unreleased]

### Added

- Namespaces (issue #105): `client.namespace(ns)` returns a lightweight
  `Namespace` handle scoped to `ns` — a flat, opaque byte string. The
  same key name under two different namespaces (or under no namespace at
  all) is a wholly independent entry. The handle exposes the same
  `get`/`get_bytes`/`set`/`delete` operations as `NanocachedClient`, with
  identical semantics (routing, replication fan-out, hedged reads, `W`
  refresh-and-retry, response tags, compression) keyed off `(namespace,
  key)` together; it shares the client's connections and routing rather
  than duplicating any networking, and is invalid once the client is
  closed (`Error::AlreadyClosed`, same as the client's own methods).
  `namespace("")` returns a handle equivalent to the client itself. The
  namespace-less API is unchanged and remains the default.

  On the wire, a non-empty namespace switches `get`/`set`/`delete` from
  the `G`/`S`/`D` frames to their lowercase `g`/`s`/`d` counterparts,
  which carry the namespace's length and bytes ahead of the key; the
  default (empty) namespace always sends the legacy `G`/`S`/`D` bytes
  byte-for-byte, so existing code and connections to a pre-namespace
  server are unaffected. Routing folds the namespace into the rendezvous
  hash's key input (`fnv1a(be32(len(ns)) || ns || key)` for a non-empty
  namespace; the default namespace hashes exactly like the pre-namespace
  form, so an un-namespaced key's placement never moves) — pinned against
  the same cross-language test vectors the server and every other SDK
  assert. Namespaced frames need a namespace-aware server.

  **Breaking: `HashRing::owners` and `HashRing::route` gained a leading
  `namespace: &[u8]` parameter** (`owners(namespace, key, replicas)` /
  `route(namespace, key)`); pass `b""` to route an un-namespaced key
  exactly as before.

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. Server-side, this
release fixes the cluster data-loss and availability bugs found in the
v0.2.0 end-to-end run (issues #61–#63, #66) and changes the discovery
heartbeat acknowledgment so nodes learn of evictions: upgrade nodes
before discovery servers.

### Changed

- `Error::DiscoveryBusy`'s message no longer claims the replica is
  "warming up after a restart" — `B` is also what a replica whose
  replication factor disagrees with the cluster's answers (issue #68).

### Added

- Hedged reads: `Options::read_hedge_after(Duration)` sends a read to the
  next owner as well once the primary has gone silent for that long
  (and, if needed, the owner after it once another interval passes),
  taking the first answer — a hit from any owner, or a miss once every
  owner has answered or failed. A replica's miss is only ever provisional
  (the primary's answer still decides), and a `WrongNode` answer
  propagates exactly as the existing read path's does. Off by default;
  needs `R >= 2`. The losing leg of a hedge is never cancelled — it runs
  to completion detached and is drained by `close()`, the same way a
  fire-and-forget replica write is (issue #64).

### Fixed

- `connect()` no longer fails outright just because one of the nodes
  discovery lists can't be reached yet — typically one that just died
  and hasn't been evicted from discovery's liveness window (a few
  seconds). Every listed node is now dialed concurrently; one that can't
  be reached is installed as a member without a live connection and with
  its reconnect cooldown armed, exactly the state a member is in after
  dying mid-life, so requests for its keys fail over per request instead
  of failing `connect()`. Only a cluster with no reachable node at all
  still fails `connect()`, with the last dial error (issue #67).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version.
0.1.x were pipeline-validation releases; as a pre-1.0 line, breaking
changes ship without a deprecation cycle and are listed below.

### Changed

- **Breaking: `Client::close` is now `pub async fn`.** It drains in-flight
  fire-and-forget replica writes before returning, so a process that
  closes and exits no longer drops replica writes that were still being
  sent. Callers must `.await` it (PR #48).
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

[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/rust/v0.1.1...sdk/rust/v0.2.0
[0.1.1]: https://github.com/nanocached/nanocached/releases/tag/sdk/rust/v0.1.1
