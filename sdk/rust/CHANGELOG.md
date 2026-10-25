# Changelog

All notable changes to the Rust SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/rust/vX.Y.Z` tags.

## [Unreleased]

### Fixed

- A `Some(0)` touch (`touch`, `get_and_touch`, `touch_many`,
  `get_many_and_touch`) is no longer sent again after a reply that cannot be
  matched to it: a reply of the wrong kind, a roster of the wrong length, or
  a reply that is not a reply at all. The frame was written, so the node had
  already expired the keys, and the second one reported them as `false` or as
  misses, and a `get_and_touch` lost the value. They now fail with the error
  the bad reply raised. The same reply of the wrong kind used to make `incr`,
  `decr` and the compare-and-set family redial and send the frame again, so
  an `incr` could apply its delta twice; they now fail with
  `ConnectionLost` instead.
- The crate compiles with the `compression` feature off
  (`--no-default-features`, or `tls` alone). The cumulative size check for
  `get_many` named `Error::Decompression`, which exists only with
  `compression`, outside its cfg, so a consumer who turned the feature off
  got a compile error. CI now builds and tests every feature combination.

### Added

- `touch(key, ttl_seconds)` replaces an entry's TTL in place and returns
  whether the key existed; `get_and_touch(key, ttl_seconds)` and
  `get_and_touch_bytes` read the value and replace the TTL in one step.
  All three are on `Namespace` too. `ttl_seconds` is a required
  `Option<u64>`: `None` removes the TTL and `Some(0)` expires the entry at
  once (the opposite of `set`, where `0` means no expiry). They are
  idempotent, so unlike `incr` and the compare-and-set family a lost reply
  is retried; they go to every owner (`get_and_touch` asks only the
  primary for the value). Against a node that predates the command they
  return the new `Error::ServerTooOld`. Needs a server release that
  includes the `t` command.
- `touch_many(keys, ttl_seconds)` and `get_many_and_touch(keys,
  ttl_seconds)` (with `get_many_and_touch_bytes`, and all three on
  `Namespace`) are `touch`/`get_and_touch` for a list of keys, in one round
  trip per involved node. `touch_many` returns a map of key to whether it
  existed; `get_many_and_touch` returns the hits, as `get_many` does. One
  TTL for the whole batch, with the meaning and no default of `touch`. Every
  owner is sent the batch; only a key's primary is asked for the value, so a
  node that is primary for some keys and a replica for others receives two
  frames. `get_many_and_touch` fails like `get_many` (partial results
  attached); `touch_many` returns a plain `Error::WrongNode` or the
  connection error. Needs a server release that includes the `y` command;
  against a node that has `t` but not `y` they return `Error::ServerTooOld`.
- `Error::ServerTooOld` now carries the wire command the node did not know
  (`Error::ServerTooOld { command: "t" }` or `"y"`), so one variant covers
  both. It is still unreleased, so nothing that matched the unit variant
  has shipped.

## [0.4.8] - 2026-10-21

### Fixed

- A node at its connection limit, which answers `B` and closes, is
  reported as busy (`Error::ConnectionLost`, naming the limit) after one
  dial. The two-byte reply used to end the three-byte identity read in
  `UnexpectedEof`, which was classified as a legacy server's door-slam,
  so the dial was retried with the two lesser `A` forms and came out as
  a closed connection.

- `get_many`/`get_many_bytes` fetch a repeated key once. A list of many
  copies of one key used to be sent as one `m` frame per 400 copies with
  every frame's reply retained until the call returned, so about 60 KB of
  request could hold hundreds of MB of replies (#698).
- `set_many`/`set_many_bytes` in cluster mode retry only the legs that
  failed: a key whose primary leg failed used to be re-sent to every
  owner after the refresh, including a replica that had already stored
  it (#720). And a key whose primary could not be reached at all (no `W`
  was ever received) now returns `Error::PartialConnectionLostKeys` with
  the keys whose primary acknowledged, instead of `Error::WrongNode`
  (#722).
- The `Display` text of `Error::PartialConnectionLost`,
  `Error::PartialConnectionLostText` and `Error::PartialConnectionLostKeys`
  no longer describes a chunked batch interrupted partway, which a cluster
  batch whose keys had no reachable owner never was (#671).
- Forced node-list refreshes are single-flight: callers that hit a dead
  node at the same time used to run one full refresh each, one after
  another behind the refresh gate; a forced caller now reuses a refresh
  that finished while it waited (#721).
- `close()` releases a caller blocked writing a request to a peer that
  has stopped reading, and every caller queued behind it. They used to
  stay pending until the request deadline, with the socket open (#723).
- A request that timed out is no longer redialed and resent once: the
  peer was reachable and simply did not answer, so the resend waited out
  a second full deadline — a cluster `set` whose replica had stopped
  reading took 60 s instead of 30 s (#725). `Error` gains the internal
  `ConnectionLostTimedOut` variant, folded into `ConnectionLost` before
  any public method returns it, like `ConnectionLostAfterSend`.

## [0.4.7] - 2026-10-19

### Fixed

- `get_many`/`get_many_bytes` in cluster mode now read a key from its
  next owner when its primary cannot be reached at the connection level,
  the same walk a single `get` already did (#623, the Rust part of #616).
  Before, the batch sent every key to its primary only, retried the same
  primary once after a refresh, and then returned
  `Error::PartialWrongNode` for keys that nothing had answered `W` for —
  seen in the field as a client that could not verify one node's rotated
  TLS certificate failing a bulk read although the other owner held every
  key. Keys whose every owner is unreachable now return
  `Error::PartialConnectionLost` (or the bare connection error when
  nothing resolved) instead of a wrong-node error.

## [0.4.6] - 2026-10-17

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.5] - 2026-10-15

### Fixed

- Large requests and large responses on one pipelined connection could
  deadlock. A request blocked mid-write (the server was not reading) held
  the lock the read task needs to dispatch each response, so a server
  blocked writing 1 MB values to a client that had stopped reading never got
  to read the next request: nothing moved until the request timeout closed
  the connection, failing everything pending on it, non-idempotent calls
  included, as possibly sent. The pending queue now has its own lock, which
  the read task takes without waiting for a write. A mixed batch of 96
  concurrent 1 MB gets and sets, which failed after the 5 s test timeout,
  now finishes in about 30 ms.
- Keep-alive pings no longer run one after another. A half-open node, whose
  ping blocks until the request timeout, used to keep every node after it
  from being pinged until those had sat idle for about 60 s, the server's
  idle limit. Each ping now runs as its own task (as in the Go SDK), and a
  connection whose last ping is still outstanding is not pinged again.
- `connect()` no longer dials a whole roster at once. A discovery server can
  list up to 65536 nodes, which opened that many sockets together; at most
  64 dials are now in flight at a time. Outcomes are unchanged.

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project: from 0.4.4 on,
every release ships the server, the six SDKs and the seven framework
adapters at one version, whether or not a component changed.

## [0.4.2] - 2026-09-07

### Fixed

- **Breaking:** `connect()` now rejects `read_hedge_after` combined with `via_proxy(true)`
  and `ca` combined with `tls(false)`, both previously silently inert
  (issue #488).
- The per-request timeout is now progress-based, like the Go SDK: a
  request queued behind others under deep pipelining is no longer timed
  out while the server is still answering; a half-open server is still
  reclaimed within one window (issue #488).

## [0.4.1] - 2026-09-05

### Fixed

- Documentation only (issue #478): the crate-level doc no longer claims every
  operation is idempotent (`incr`/`decr`/`cas`/`delete_if_matches` are not,
  see issue #225), the read-repair doc comment and README no longer describe
  the write-back as untracked/undrained (it shares the fire-and-forget
  replica budget and `close()` drains it), and the `close()` example in the
  project docs now awaits it. No code change.

## [0.4.0] - 2026-09-05

### Added

- `incr`/`decr` (issue #129): `client.incr(key, delta)` atomically adds a
  signed `i64` delta to an integer counter stored at `key` and returns its
  new value (`None` on a missing/expired key, matching `get`'s own miss
  convention; `Err(Error::NotNumeric)` when the stored value isn't a plain
  signed-decimal integer or the delta would overflow `i64`). `decr` is a
  thin wrapper that negates the delta — there is no separate wire opcode.
  Both are also available on a `Namespace` handle. On the wire this is the
  new `i` command (always namespaced, unlike `G`/`S`/`D`'s
  uppercase/lowercase pair — there is no legacy uppercase form), answered
  `I <value-length> [<ttl-seconds>]` on success, `N` on a missing key, or
  the new `T` status for not-numeric/overflow. In cluster mode, `incr`
  sends `i` to the key's primary owner only; once it succeeds, the literal
  new value (and its TTL, if any) is forwarded to the remaining owners as
  an ordinary `set`, never replayed as another `i` there, which is what
  keeps replicas byte-identical to the primary instead of letting them
  drift. `incr` is exactly as volatile as `set` — LRU eviction and TTL
  expiry reclaim an incremented value like any other entry — so it suits
  rate limiting or approximate counters, not a durable count.

- Retryable-error status `R` (issue #125): the connect/identify handshake
  now probes with `A <len> T R` first — one more capability token in
  front of the existing `T` negotiation — declaring that this client
  understands a transient-failure reply on any data command
  (`G`/`S`/`D`/`g`/`s`/`d`/`c`/`F`). Falls back transparently to `A <len>
  T` and then plain `A <len>` on the existing legacy-server signal (a
  connection closed before any reply), exactly like the pre-existing `T`
  fallback, just with one more stage in front — every connection this SDK
  dials (per-node, proxy, discovery, hedge, reconnect) goes through the
  same probe. Only `nanocached-proxy` sends `R` today (an upstream node
  was briefly unreachable and survived its own one refresh-and-retry),
  but any connection handles it regardless. When a request is answered
  `R`, it is transparently retried on the very same connection, up to
  twice more (three attempts total, 50ms then 100ms apart); if the third
  attempt still answers `R`, the operation fails with the new
  `Error::Retryable` — the connection is never closed or redialed, either
  while retrying or after giving up, and stays usable for whatever the
  caller does next. Every `R` received, including the final exhausting
  one, is counted in the new `stats().transient_retries` counter, which
  never resets. Hedged reads need no special-casing: a hedge leg answered
  `R` retries on its own connection exactly like a non-hedged read, and
  an eventual `Error::Retryable` is treated like any other leg failure.

- SDK proxy mode (issue #122): `Options::via_proxy(true)` connects
  through a `nanocached-proxy` tier instead of joining the cluster
  directly. `addresses` must still name discovery server(s); `connect()`
  fetches the proxy roster (the new `Q` discovery command — `N <count>\n`
  then, per proxy, the same `<name-len> <addr-len>\n<name><addr>\n` shape
  `L` uses, minus the replication field) rather than the node roster, and
  lands on one proxy chosen at random, spreading a fleet of clients
  across the proxy tier; on a failed dial it fails over through the rest
  of the roster in random order. A proxy identifies itself on the wire
  exactly like a single node that owns every key, so from there the
  client runs in its existing single-connection mode: no ring, no
  per-node connections, and `Options::read_hedge_after` is simply inert
  if also set (there is nothing to hedge a read to). If the proxy
  connection is lost, the same proxy is redialed first; only if that also
  fails is the roster re-fetched and another, randomly chosen, reachable
  proxy dialed — reusing this crate's existing reconnect plumbing and
  `stats().refresh_failures` counter rather than a second one. Pointing
  `via_proxy` at an address that identifies as a cache node fails
  `connect()` fast with `Error::InvalidArgument`; an empty or wholly
  unreachable proxy roster is a normal connect error. Off by default;
  `close()` is unchanged.

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

- Namespace clear / flush-everything (issue #106): `Namespace::clear()`
  drops every entry in that one namespace (`namespace("").clear()`
  clears the default namespace and is not rejected); the new
  `NanocachedClient::clear_all()` flushes every namespace, the default
  one included. Neither is key-addressed (a namespace's keys are spread
  over every node by rendezvous hashing), so both fan out to every node
  the client currently knows about and succeed only once every node has
  acked `C` — never a partial clear. If any node fails, the node list is
  refreshed once (the same path a `W` reply already triggers) and the
  whole fan-out is retried against the refreshed list; a node still
  failing after that fails the call with `Error::ConnectionLost` naming
  it. Both operations are idempotent, so a caller can simply retry.
  Raises `Error::AlreadyClosed` after `close()`, like every other
  operation. On the wire these are the new `c <namespace-length>[
  <tag>]\n<namespace>` / `F[ <tag>]\n` commands, acked `C[ <tag>]\n`;
  talking to a pre-#106 server, both fail with `Error::Protocol` (an
  unrecognized command answered `E`).

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
