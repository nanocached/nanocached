# Changelog

All notable changes to the Go SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/go/vX.Y.Z` tags.

From 0.4.4 on, every release ships the server, the six SDKs and the seven
framework adapters at one version, whether or not a component changed.
There is no `sdk/go/v0.4.3` tag because 0.4.3 was a server-only release.

## [Unreleased]

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project: from 0.4.4 on,
every release ships the server, the six SDKs and the seven framework
adapters at one version, whether or not a component changed.

## [0.4.2] - 2026-09-07

### Changed

- **Breaking: `Connect` now rejects `Config.ReadHedgeAfter > 0` combined
  with `Config.ViaProxy`, and `Config.CA` combined with `Config.TLS ==
  false`.** Both combinations used to be accepted and silently had no
  effect (a proxy connection has no replicas to hedge to; a CA file is
  only read over TLS, so `CA` without `TLS` connected in plaintext).
  Callers that toggle `ViaProxy` per environment must now unset
  `ReadHedgeAfter` alongside it (issue #488).

### Fixed

- In proxy mode the per-address reconnect-cooldown map is now bounded:
  arming a cooldown also drops every entry whose window has passed, and a
  proxy failover keeps only the addresses of the freshly fetched roster,
  so a proxy the client failed away from no longer leaves an entry behind
  for the life of the client (issue #486).
- A proxy (`Q`) roster result can no longer be misread as a node roster
  with a replication factor of zero; asking it for one is now an explicit
  error (issue #486).
- Documentation: the README now states the rule for retrying
  non-idempotent operations (`Incr`/`Decr`, the compare-and-set family,
  `DeleteIfMatches`) after a connection failure: they are replayed only
  when the request provably never left this process. No behavior change;
  Go already worked this way (issues #484, #487).

## [0.4.1] - 2026-09-05

### Fixed

- Documentation only (issue #478): the README no longer says the read-repair
  write-back is uncapped and not drained by `Close()`. It shares the
  fire-and-forget replica budget and `Close()` waits for it, as it already
  did since 0.2.0. No code change in the public API.

## [0.4.0] - 2026-09-05

### Added

- Namespaces (issue #105): `client.Namespace(ns)` returns a lightweight
  `*Namespace` handle that scopes every operation to `ns`. The same key
  under two different namespaces (or under no namespace) is a wholly
  independent entry. The handle has the same `Get`/`GetBytes`/`Set`/
  `SetBytes`/`Delete`, `GetMany*`/`SetMany*`, `Incr`/`Decr` and
  compare-and-set methods as `*Client`, shares the client's connections,
  routing, replication, hedging and compression, and returns `ErrClosed`
  once the client is closed. `Namespace("")` is equivalent to the client
  itself and is never rejected; `Name()` returns the namespace string.
  The namespace-less API is unchanged. Namespaced frames need a
  namespace-aware server.
- `HashRing.OwnersNS(namespace, key, replicas)` and
  `HashRing.RouteNS(namespace, key)` route a namespaced key, pinned against
  the same cross-language test vectors as the server and the other SDKs.
  `Owners` and `Route` keep their signatures and place an un-namespaced
  key exactly where it was placed before.
- Namespace clear (issue #106): `Namespace.Clear()` drops every entry in
  that namespace and `Client.ClearAll()` flushes every namespace, the
  default one included. Both fan out to every node the client knows
  about and succeed only once every node has acknowledged; if a node
  fails, the node list is refreshed once and the whole fan-out is retried,
  and a node that still fails fails the call. Both are idempotent, so
  callers can simply retry.
- `Incr`/`Decr` (issue #129): atomically add a signed `int64` delta to an
  integer counter and return the new value. `ok` is false on a missing or
  expired key, matching `Get`'s miss convention. They return
  `ErrNotNumeric` when the stored value is not a signed decimal integer or
  the result would overflow `int64`. In a cluster only the primary owner
  runs the increment; replicas receive the literal result as an ordinary
  set. Counters are exactly as volatile as `Set` (LRU eviction and TTL
  expiry reclaim them), so they suit rate limiting and approximate counts,
  not durable ones. `Decr(key, math.MinInt64)` returns `ErrInvalidArgument`
  (no valid negation, issue #182). On a client built with
  `Config.Compress`, `Incr`/`Decr` return `ErrCompressIncompatible` before
  any I/O (issue #321).
- Compare-and-set (issue #141): `GetWithToken` returns a value plus a
  `CasToken`; `PutIfAbsent`, `ReplaceIfPresent`, `Replace(key, token, ...)`
  and `DeleteIfMatches(key, token)` condition a write on that exact content
  and report whether it was applied. `CasToken` carries a 128-bit content
  digest (`Digest()`, `Hex()`); `ContentDigest` and `TokenFromDigest` build
  one from a value the caller already holds. Only the primary owner
  evaluates the condition and replicas receive the result. This is content
  based CAS, not a distributed lock: LRU eviction can reclaim a key used as
  a lock.
- Multi-get and multi-set (issues #128, #150, #151): `GetMany`/
  `GetManyBytes` and `SetMany`/`SetManyBytes` batch many keys into one
  round trip per owner, transparently chunked into several frames when a
  batch is large. A missing key is absent from the returned map, not an
  error. `SetMany*` takes one TTL for the whole batch. Partial failure is
  returned as a tuple: `GetMany*` returns the keys that resolved together
  with `ErrWrongNode` for the rest, and `SetMany*` returns `(map[string]bool,
  error)` where the map holds exactly the keys confirmed stored (issues
  #222, #411). In single-node and proxy mode a `W` answer propagates
  immediately.
- SDK proxy mode (issue #122): `Config.ViaProxy` connects through a
  `nanocached-proxy` tier. `Addresses` must still name discovery
  server(s); `Connect` fetches the proxy roster and lands on one proxy
  chosen at random, failing over through the rest in random order. From
  there the client runs in single-connection mode (no ring, no per-node
  connections). If the connection is lost, the same proxy is redialed
  first and only then is the roster re-fetched. Pointing `ViaProxy` at an
  address that identifies as a cache node fails `Connect` fast. Off by
  default.
- Retryable-error status `R` (issue #125): the handshake now negotiates
  support for a transient-failure reply, falling back transparently
  against older servers. A request answered `R` (today only
  `nanocached-proxy` sends it) is retried on the same connection up to
  twice more, 50ms then 100ms apart; if every attempt answers `R` the
  call fails with the new `ErrRetryable`. The connection is never closed
  or redialed for this. Every `R` received is counted in the new
  `Stats().TransientRetries` counter.

### Fixed

- `Incr`/`Decr`, the compare-and-set family and `DeleteIfMatches` are no
  longer replayed after a connection failure unless the request provably
  never reached the wire. Replaying could double-apply an increment or
  report an already-applied CAS as a mismatch. These operations are
  at-least-once: after a failure past that point they return
  `ErrConnectionLost` and the caller decides whether to retry (issue #225).
- A multi-get reply is now bounded to 64 MiB of cumulative value bytes,
  and the cumulative decompressed size of one `GetMany*` response is
  bounded to 256 MiB (charged before the check, and applied only when
  `Config.Compress` is enabled). A hostile or buggy node could previously
  force very large allocations from one response (issues #207, #410).
- `GetMany*`/`SetMany*` sub-frames are now also split by cumulative wire
  size, not just key count, so a batch of many mid-sized values no longer
  produces a frame over the server's request limit that the server can only
  reject by closing the connection (issue #222).
- The namespace length now counts toward the maximum request size, so a
  very long namespace is rejected with `ErrInvalidArgument` instead of
  producing an oversized frame that the server answers by closing the
  connection.
- A connection failure partway through a chunked multi-get or multi-set no
  longer discards the results of the chunks that already succeeded; they
  are returned alongside the error (issue #411).
- `SetMany*` in a cluster no longer blames a whole owner leg for a failure
  that only affected the chunks that got no response, which over-counted
  `Stats().ReplicaWriteFailures` and re-sent already stored keys.
- An unsolicited untagged `B` ("connection limit reached") arriving while
  requests are pending is now recognized and poisons the connection with
  the busy error. It used to be delivered to the oldest pending request
  and surface as a generic desync error (issue #334).
- Every integer parsed off the wire (value and multi-get lengths, TTLs,
  tags, discovery counts, replication factor, ports) must now be ASCII
  digits only; anything else (a leading `+`, whitespace, `_`, an exponent)
  poisons the connection. The `Incr` result keeps its single optional
  leading `-` (issue #462).
- The number of hedged-read losing legs left running detached is now
  bounded at 32; past that, a hedged read waits for the losing legs before
  returning, so sustained load against one slow owner can no longer grow
  them without limit (issue #276).
- `NewHashRing` now drops repeated node names, keeping the first
  occurrence. A duplicated name used to be scored once per slot and
  inflated that node's share of the owner set (issue #360).
- A duplicated node name in a discovery roster no longer leaks the live
  connection already moved into the member map, which used to leave a TCP
  socket and its read goroutine running past `Close()` (issue #389).
- TLS connections now get `TCP_NODELAY` like plaintext ones. Nagle's
  algorithm used to be enabled on every TLS connection, adding latency to
  small frames (issue #301).
- Reconnect-cooldown entries for addresses that have left the cluster are
  now purged when the node list is refreshed. A fresh IP:port per restart,
  as in container deployments, used to leak one entry per departed address
  for the life of the process (issue #96).
- `Close()` now waits for the keep-alive goroutine to exit, and keep-alive
  pings go to idle connections in parallel, so one slow node no longer
  delays the ping reaching every other member (issue #192).

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. Server-side, this
release fixes the cluster data-loss and availability bugs found in the
v0.2.0 end-to-end run (issues #61-#63, #66) and changes the discovery
heartbeat acknowledgment so nodes learn of evictions: upgrade nodes
before discovery servers.

### Added

- Hedged reads: `Config.ReadHedgeAfter` sends a `Get`/`GetBytes` to the
  next owner as well once the primary has been silent for that long (and
  to the owner after it once another interval passes), taking the first
  answer: a hit from any owner, or a miss only when it comes from the
  primary. A replica's miss is provisional, so hedging can never turn a hit
  into a miss. Zero (the default) disables hedging; a negative value is
  rejected by `Connect`. It has no effect for a single-node client or when
  `Replication()` is below 2. The losing leg is never cancelled: it runs to
  completion detached and `Close()` waits for it, like a fire-and-forget
  replica write (issue #64).

### Changed

- `ErrDiscoveryBusy`'s message no longer claims the discovery server is
  "warming up after a restart". `B` is also what a discovery server whose
  replication factor disagrees with the cluster's answers (issue #68).

### Fixed

- `Connect` no longer fails outright just because one of the nodes
  discovery lists cannot be reached yet, typically one that just died and
  has not been evicted from discovery's liveness window. Every listed node
  is now dialed concurrently, and an unreachable one is installed as a
  member without a live connection and with its reconnect cooldown armed,
  so requests for its keys fail over per request. Only a cluster with no
  reachable node still fails `Connect`, with the last dial error
  (issue #67).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version.
0.1.x were pipeline-validation releases; as a pre-1.0 line, breaking
changes ship without a deprecation cycle and are listed below.

### Added

- `ErrAuthenticationFailed`: the server rejecting the `A` handshake's
  secret (no `AuthSecret` configured for a server that requires one, or a
  wrong one) is now matchable with `errors.Is`. It is never transient:
  retrying with the same configuration cannot succeed (issue #47).
- `ErrProtocol`: a malformed or unexpected response frame (garbage header
  or length, unparsable tag, unknown marker, over-long header line) is now
  reported as `ErrProtocol` instead of being rewrapped as
  `ErrConnectionLost`. The connection is still poisoned either way.
- `ErrInvalidArgument`: client-side validation that runs before any I/O
  (empty or oversized key, key plus value over the request cap, a negative
  TTL) now returns it.
- `Client.Stats()` returns monotonic counters for failures the client
  swallows by design: `ReplicaWriteFailures`, `ReadRepairFailures` and
  `RefreshFailures`.
- `Config.ReconnectCooldown` and `DefaultReconnectCooldown` (1s): after a
  failed dial, further dials to that address are skipped for the cooldown.
  Zero means the default; a negative value disables the cooldown.
- `Config.String()` and `GoString()` redact `AuthSecret`, so a logged
  `Config` no longer leaks it.
- Response tags: the handshake now asks the server to echo a per-request
  tag on every response, and the read loop verifies the pairing before
  dispatching a response, so a desynced pipelined stream poisons the
  connection instead of handing a caller a plausible wrong value. It falls
  back transparently to the untagged protocol against older servers.

### Changed

- **Breaking: `HashRing.Route` now returns `(string, error)` instead of
  `string`.** Calling it on an empty ring used to panic; it now returns an
  error wrapping `ErrInvalidArgument`.
- **Breaking: `DiscoveredNode` is no longer exported.** No public API
  returned or accepted it.
- `Connect` gives each connection attempt a single 5s budget covering dial,
  TLS handshake and the identify exchange, instead of a 10s dial plus a
  separate 5s identify deadline.
- `Connect` returns an error for a negative `Config.CompressionThreshold`.
- Established connections now enforce a 30s request timeout, so a
  half-open server can no longer hang requests forever or block `Close()`.
  The timeout is progress-based: it is armed when the first request becomes
  pending and re-armed each time a response arrives while others are still
  waiting, so steady traffic against a silent server no longer pushes it
  out indefinitely.
- Read repair now writes the repaired value with a 60s TTL instead of no
  TTL (a repaired key can no longer resurrect already-expired data
  permanently), and probes only the remaining owners rather than re-probing
  the primary that just missed. The write-back is bounded (it shares the
  fire-and-forget replica budget) and `Close()` waits for it.
- The keep-alive ping key is now the reserved `\x00nanocached-keepalive`
  instead of the one-byte key `\x00`, which is a valid application key
  whose recency every ping refreshed.

### Fixed

- `Get`, `GetBytes` and `Delete` now validate the key client-side, like
  `Set`. An oversized key used to poison the connection and every pipelined
  request on it.
- Discovery node-list responses are capped at 65536 nodes and an aggregate
  16 MiB, closing an out-of-memory path from a malicious discovery server.
- Response header lines are capped at 4 KiB.
- A final read that returns data together with EOF is now treated as
  success instead of a connection failure.
- `HashRing.Owners` now selects just the top `replicas` nodes instead of
  fully sorting the node list: `O(n * replicas)` instead of `O(n log n)`.
  Output is unchanged.

## [0.1.1] - 2026-08-20

Pipeline-validation release: the first tag-driven release of the aligned
SDK line, with no code change relative to 0.1.0.

## [0.1.0] - 2026-08-20

Pipeline-validation release of the first Go SDK.

[0.4.4]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.4.2...sdk/go/v0.4.4
[0.4.2]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.4.1...sdk/go/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.4.0...sdk/go/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.3.0...sdk/go/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.2.0...sdk/go/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.1.1...sdk/go/v0.2.0
[0.1.1]: https://github.com/nanocached/nanocached/compare/sdk/go/v0.1.0...sdk/go/v0.1.1
[0.1.0]: https://github.com/nanocached/nanocached/releases/tag/sdk/go/v0.1.0
