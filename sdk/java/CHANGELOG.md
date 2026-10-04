# Changelog

All notable changes to the Java SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/java/vX.Y.Z` tags. The Maven artifact is
`org.nanocached:nanocached`.

From 0.4.4 on, every release ships the server, the six SDKs and the seven
framework adapters at one version, whether or not a component changed.
There is no `sdk/java/v0.4.3` tag: 0.4.3 was a server-only release.

## [Unreleased]

### Fixed

- `NanocachedClient`: the cluster ring and replication count are now
  `volatile`. They are written under the client's state lock by a roster
  refresh but read lock-free on the request path, so a request thread could
  keep routing by a stale ring.
- Hedged reads: a hedge leg is now submitted to the leg pool after the
  client's hedge-registration lock is released. With the pool saturated the
  submitting thread runs the leg itself, and that whole read round trip used
  to run holding the lock, blocking every other hedged read and `close()`.
- A node-list refresh now dials newly listed nodes concurrently, in waves of
  at most 16, instead of one at a time under the refresh lock, so k
  unreachable new nodes no longer stall every caller for k connect timeouts.
  A new node that cannot be reached is kept as a member without a connection
  (reconnect cooldown armed, redialed on first use), as at connect time and
  in the other SDKs, so the ring matches its peers' instead of dropping it.
- Keep-alive pings run concurrently on a small pool (16 threads) instead of
  one after another on a single thread, with at most one ping in flight per
  connection. One half-open node used to hold its ping for the 30 s request
  timeout and delay the pings to every other node until about 60 s idle, the
  server's idle limit (issue #192).
- Host name resolution is now bounded by the 5 s connect deadline. A stalled
  DNS server used to hang a dial outside that deadline, because
  `new InetSocketAddress(host, port)` resolved on the calling thread; a lookup
  that times out now fails the dial like an unreachable address. IP literals
  are not resolved.

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project: from 0.4.4 on,
every release ships the server, the six SDKs and the seven framework
adapters at one version, whether or not a component changed.

## [0.4.2] - 2026-09-07

### Changed

- **Breaking:** `connect()` now rejects `readHedgeAfter` combined with
  `viaProxy(true)` and `ca` combined with `tls(false)` with an
  `IllegalArgumentException`. Both combinations were previously accepted
  and silently inert (a proxy connection has no replica to hedge to; a CA
  file is only read over TLS, so the client connected in plaintext). A
  caller that toggles `viaProxy` per environment must now also leave
  `readHedgeAfter` unset there (issue #488).

### Fixed

- `incr`/`decr`, the compare-and-set operations and `deleteIfMatches` are
  now retried after a redial when a write or flush failure proves the frame
  never reached the server, as the Go SDK already did. Previously any write
  failure was treated as ambiguous and never retried. A failure after the
  frame was handed over in full is still never replayed, so none of these
  can be applied twice (issue #484).
- The replica-writer pool is now bounded (threads and queue). Previously a
  burst of synchronous-fallback or batched owner legs could queue without
  limit; on overflow the submitting thread now runs the task itself, and a
  shut-down pool still rejects work so the `close()` race handling is
  unchanged (issue #486).
- Reconnect cooldown entries are now dropped once their window has passed
  whenever a new cooldown is armed. In proxy mode nothing else pruned that
  map (issue #486).
- Fire-and-forget replica writes and read repair now run on their own pool,
  sized to the permit count, instead of sharing the replica-writer pool. A
  burst of synchronous or batched legs filling that shared queue could make
  a `set()` with `fireAndForgetReplicas(true)` run its replica leg inline
  and block the caller until the replica acked (measured 308 ms against a
  replica that delayed its ack by 300 ms) (issue #500).

## [0.4.1] - 2026-09-05

### Fixed

- Documentation only (issue #478): the read-repair doc comments and README
  no longer describe the write-back as uncapped and undrained (it shares the
  fire-and-forget replica budget and `close()` drains it), and stale
  comments were corrected. No behavior change.

## [0.4.0] - 2026-09-05

### Added

- Namespaces (issue #105): `client.namespace(String)` and
  `client.namespace(byte[])` return a lightweight `Namespace` handle scoped
  to a flat, opaque byte-string namespace. The same key under two
  namespaces, or under none, is a wholly independent entry. The handle
  exposes the same `get`/`getBytes`/`set`/`delete` surface as the client
  (and, from the features below, the bulk, counter, compare-and-set and
  `clear` operations) with identical routing, replication, hedging,
  `WrongNode` retry and compression behavior. It shares the client's
  connections and raises `AlreadyClosed` once the client is closed. An empty
  namespace is equivalent to the client itself. A non-empty namespace uses
  new wire frames and so needs a namespace-aware server; the default
  namespace keeps sending the legacy frames and its key placement does not
  move. `HashRing.owners(byte[] namespace, byte[] key, int replicas)` is a
  new overload; the existing two-argument form is unchanged.
- Namespace clear and flush (issue #106): `Namespace.clear()` drops every
  entry in that namespace and `NanocachedClient.clearAll()` flushes every
  namespace, the default one included. Both fan out to every known node and
  succeed only once all of them have acknowledged. If a node fails, the node
  list is refreshed once and the whole fan-out is retried; a node that
  still fails raises an exception naming it, never a silent partial clear.
  Both are idempotent, so a caller can simply retry.
- SDK proxy mode (issue #122): `Options.viaProxy(true)` connects through a
  `nanocached-proxy` tier instead of joining the cluster. `addresses` still
  names discovery servers; `connect()` fetches the proxy roster and lands on
  one proxy chosen at random, failing over through the rest of the roster.
  There is no ring and no per-node connection. If the connection is lost,
  the same proxy is redialed first and only then is the roster re-fetched.
  Pointing it at an address that identifies as a cache node fails
  `connect()` fast. Off by default.
- Retryable status `R` (issue #125): the handshake now declares that the
  client understands a transient-failure reply, falling back transparently
  to the older handshakes against older servers. A request answered `R` is
  retried on the same connection up to twice more (50 ms, then 100 ms
  apart); if the third attempt still answers `R` it fails with the new
  `NanocachedException.RetryableError`. The connection is never closed or
  redialed for this, and stays usable. Every `R` received is counted in the
  new `stats().transientRetries()` component of `ClientStats`.
- `incr`/`decr` (issue #129): atomically adds a signed `long` delta to an
  integer counter and returns the new value as an `OptionalLong`, empty on a
  missing or expired key. A stored value that is not a plain decimal
  integer, or a delta that would overflow `long`, raises the new
  `NanocachedException.NotNumeric`. In cluster mode the primary owner
  applies the delta and its literal result is forwarded to replicas as an
  ordinary set, so replicas stay byte-identical to the primary. A counter is
  as volatile as any entry: eviction and TTL expiry reclaim it, so it suits
  rate limiting and approximate counts, not durable ones. A client built
  with `compress(true)` rejects `incr`/`decr` before any I/O with the new
  `NanocachedException.CompressionIncompatible` (issue #321), since the
  protocol cannot tell a compressed value from a counter.
- Compare-and-set (issue #141): `putIfAbsent`, `replaceIfPresent`,
  `replace(key, token, newValue)` and `deleteIfMatches(key, token)`, each
  returning a `boolean` (a mismatch is `false`, not an exception), plus
  `getWithToken` returning a `CasEntry(value, token)`. The token is a digest
  of the key's exact stored bytes taken from the read, not a copy of the
  value, so it stays correct with compression enabled.
  `NanocachedClient.contentDigest(byte[])` computes one directly. As with
  `incr`, only the primary evaluates the condition and replicas receive the
  resulting state. This is not a distributed lock: eviction can still
  reclaim a key and let two callers both believe they acquired it.
- Multi-get and multi-set (issue #151): `getMany`/`getManyBytes` and
  `setMany`/`setManyBytes`, with `String`/`Map` forms and, from issue #160,
  positional `byte[][]` forms (`getManyBytes(byte[][])` and
  `setManyBytes(byte[][], byte[][], long)`). Keys are grouped by owner into
  one frame per node, large batches are split transparently (at 400 keys,
  and also by cumulative request bytes so no frame exceeds the server's
  request limit), and each key's outcome is independent. Hedged reads and
  read repair do not apply to batches.
- Partial-failure exceptions for batches. A key still routed to the wrong
  node after the one refresh-and-retry raises `PartialWrongNode` (byte
  values, `getManyBytes`), `PartialWrongNodeStrings` (`getMany`) or
  `PartialWrongNodeRaw` (positional `getManyBytes`, with
  `unresolvedIndices`); all three extend `WrongNode`, so existing
  `catch (WrongNode)` code keeps working. They are separate concrete classes
  because a Java `Throwable` cannot be generic. `setMany` and `setManyBytes`
  throw a plain `WrongNode`.
- A connection failure on a later chunk of a multi-chunk batch, after the
  client's own retry, no longer discards the earlier chunks' results (issue
  #411). It raises `PartialConnectionFailed`, `PartialConnectionFailedStrings`
  or `PartialConnectionFailedRaw` for gets (carrying `partialValues`, plus
  `unresolvedIndices` for the positional form) and
  `PartialConnectionFailedSet` or `PartialConnectionFailedSetRaw` for sets
  (carrying `succeededKeys` or `succeededIndices`). All extend
  `ConnectionFailed`, which is no longer `final`. A failure on the first
  chunk still raises a plain `ConnectionFailed`.
- `ConnectionFailed.notSent()` reports whether the failed request provably
  never reached the server (issues #225, #484).

### Changed

- Non-idempotent operations (`incr`/`decr`, compare-and-set,
  `deleteIfMatches`) are never blindly replayed after a connection failure.
  If the primary applied the operation and the reply was lost, the call
  throws `ConnectionFailed` instead of resending and double-applying an
  increment or reporting a succeeded compare-and-set as a mismatch. A
  redial and retry happens only when the request provably never reached the
  wire (issue #225).
- Every integer parsed off the wire (value and multi-op lengths and counts,
  TTLs, tags, discovery counts, lengths and ports, proxy-roster counts) now
  accepts ASCII digits only. A leading `+`, whitespace or any other
  character poisons the connection. The counter body of `incr`/`decr`
  keeps its single optional leading `-` (issue #462).

### Fixed

- Duplicate node names in a discovery roster are now deduplicated (first
  occurrence wins) in `HashRing` and in the initial connect and refresh
  paths. A repeated name was previously dialed twice, leaking the first
  connection, and scored once per slot in the ring (issue #461).
- Hedged reads: the number of losing hedge legs left detached is now capped
  (32). Sustained hedging against one persistently slow owner could
  previously accumulate one detached leg per hedged call; past the cap the
  losing legs are joined synchronously (issue #276).
- A hedge leg can no longer be registered after a concurrent `close()` has
  drained the hedge set, which surfaced as a spurious mid-request exception
  from a leg `close()` never awaited (issue #91).
- `close()` now waits for an in-flight synchronous replica leg, bounded by
  the request timeout, instead of tearing down the connection it was reading
  after a fixed 5 seconds (issue #97).
- A background (`fireAndForgetReplicas`) replica leg no longer reads the
  caller's own `byte[]` when compression is off: a caller that reused or
  mutated its array after `set()` returned could change what a replica
  stored. Background legs now write from a defensive copy (issue #326).
- A `getManyBytes` racing `close()` could leak a raw
  `RejectedExecutionException` out of the public API. It now falls back to
  running the leg inline (issue #277).
- A multi-get now drains every owner leg instead of stopping at the first
  failure, so a second leg's genuine bug is no longer hidden. The same
  first-failure-wins draining now applies to multi-set and to
  `clearAll()`, which previously kept only the last failing leg's bug
  (issues #230, #233).
- A `DecompressionFailed` from a concurrent multi-get leg is no longer
  miscounted in `stats().backgroundWriteBugs()` (issue #413).
- Bulk reply and decompression bounds, against a malicious or compromised
  node. A multi-get reply is poisoned if its entries total more than
  64 MiB (issue #179). Cumulative decompressed output across one multi-get
  is capped at 256 MiB, charged before the check so the crossing entry is
  caught, and applied only when compression is enabled (issues #386, #410).
- Bootstrap no longer sizes its dialer pool to the discovery roster
  (up to 65,536 entries). It is capped at 16 threads and dials a large
  cluster in waves, instead of dying with `OutOfMemoryError: unable to
  create native thread` (issue #178).
- Proxy mode: a failover to a different proxy now drops the abandoned
  proxy's reconnect-cooldown entry, and prunes the map against the fresh
  roster, so a churning proxy fleet no longer grows it without bound
  (issue #296).

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. Server-side, this release
fixes the cluster data-loss and availability bugs found in the v0.2.0
end-to-end run (issues #61-#63, #66) and changes the discovery heartbeat
acknowledgment so nodes learn of evictions: upgrade nodes before discovery
servers.

### Added

- Hedged reads: `Options.readHedgeAfter(Duration)` sends a read to the next
  owner as well once the primary has been silent for that long (and to one
  more owner per further interval), taking the first answer. A hit from any
  owner is final; a miss is final only from the primary, so hedging never
  turns a hit into a miss. A `WrongNode` answer propagates as on the
  normal read path. Off by default (`null`), a non-positive duration is
  rejected, and it needs a replication factor of at least 2. The losing leg
  is never cancelled; it finishes on a background executor and is drained by
  `close()` (issue #64).

### Changed

- `DiscoveryBusy`'s message no longer claims the replica is "warming up
  after a restart": `B` is also what a replica whose replication factor
  disagrees with the cluster's answers (issue #68).

### Fixed

- `connect()` no longer fails outright because one node discovery lists is
  unreachable, typically one that just died and has not yet left discovery's
  liveness window. Every listed node is dialed, and one that cannot be
  reached is installed as a member without a live connection and with its
  reconnect cooldown armed, so its keys fail over per request. Only a
  cluster with no reachable node at all still fails `connect()` (issue #67).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version. 0.1.x
were pipeline-validation releases; as a pre-1.0 line, breaking changes ship
without a deprecation cycle and are listed below.

### Added

- Response tags: a client that authenticates with `A <len> T` has each
  request's tag echoed on every response, and the read loop verifies the
  pairing before dispatching, so a desynced stream poisons the connection
  instead of resolving queued requests with wrong values. Against a server
  that predates this, the client transparently redials with the untagged
  protocol once (issue #35).
- `NanocachedException.AuthenticationFailed`: a rejected secret (none
  configured against a server that requires one, or a wrong one) is now
  matchable instead of requiring a string match on the message (issue #47).
- `Options.reconnectCooldown(Duration)`: after a redial to an address fails,
  the address is treated as down for this long (default 1 s) and requests
  routed to it fail immediately with the original dial error instead of each
  paying another connect timeout. `Duration.ZERO` means the default and a
  negative or null duration is rejected;
  `Options.disableReconnectCooldown()` turns the cooldown off.
- `client.stats()`, returning a `ClientStats` record of monotonic counters
  for failures the client swallows by design: `replicaWriteFailures`,
  `readRepairFailures`, `refreshFailures` and `backgroundWriteBugs` (a
  programming bug in background-write handling, which must never increase
  and is also logged to stderr).
- A progress-based in-flight request timeout (30 s), armed when the pending
  queue goes from empty to non-empty and re-armed on each response. A
  half-open server that stays silent after authentication no longer hangs
  `get`/`set`/`delete` and everything pipelined behind them; on expiry the
  connection is poisoned and the redial/retry layer takes over. An idle
  connection is never closed by it (issue #42).

### Changed

- **Breaking:** TLS connections now verify that the server certificate was
  issued to the host being dialed (HTTPS-style endpoint identification).
  Previously only the certificate chain was checked, so a certificate from a
  trusted CA for any other name was accepted. A server certificate whose
  names do not cover the dialed host or IP will now fail to connect.
- Empty keys, and keys or key-plus-value pairs too large for one request,
  are now rejected with an `IllegalArgumentException` before any I/O. The
  server silently closed the connection on such requests, which poisoned
  every in-flight request sharing it.
- `Options.authSecret(null)` throws `IllegalArgumentException` instead of a
  `NullPointerException`, and `authSecret("")` is treated as no secret
  instead of being sent as a zero-length secret that the server rejects
  without replying (surfacing as an opaque `ConnectionFailed`).
- A negative `compressionThreshold` is rejected at `connect()`.
- Read repair no longer re-probes the primary on a clean miss, and its
  write-back now carries a 60 s TTL instead of none, so a repaired key can
  no longer resurrect expired data permanently.
- `HashRing.route` on an empty ring throws `IllegalStateException` instead
  of an index-out-of-bounds exception.
- The keep-alive ping key is now the reserved 21-byte `0x00` plus
  `nanocached-keepalive` instead of the single byte `0x00`, which is a valid
  application key whose LRU recency every tick silently refreshed.
- Failures swallowed by design (dead replica legs, read repair, refresh) are
  narrowed to the SDK's own exceptions and I/O errors, so programming bugs
  propagate instead of vanishing. An unexpected exception from a replica leg
  no longer replaces a successful primary result with a raw
  `CompletionException`: the primary's success wins, and the bug is counted
  and logged.

### Fixed

- Discovery replies are bounded: node-list responses are capped at 65,536
  nodes and 16 MiB in aggregate, header lines at 4 KiB (the discovery
  identify path previously had no cap and grew a buffer without bound), and
  a port outside the valid range is rejected as a `NanocachedException`.
  Malformed numbers in a discovery reply are wrapped in
  `NanocachedException` instead of leaking `NumberFormatException`.
- Decompressed output is capped at 64 MiB, so a tiny hostile or corrupt
  value with `compress(true)` can no longer expand into an arbitrarily large
  allocation. It surfaces as `DecompressionFailed` (issue #41).
- The identify exchange now has a read timeout. A peer that accepts the TCP
  connection but never answers hung `connect()`, redial and refresh forever
  (issue #40).
- A node-list refresh no longer dials newly listed nodes while holding the
  routing lock, where one unresponsive new node stalled all traffic for the
  whole dial.
- `close()` no longer races background replica writes: it acquires every
  background permit instead of snapshotting a task set, so a write that
  passed its closed check cannot register a new leg after the drain. Two
  concurrent `close()` calls no longer both run the teardown. The replica
  writer pool is bounded and awaited on close, and stale reconnect-cooldown
  entries are pruned.
- A failed `Connection` construction no longer leaks its socket or its slot
  in the open-target counter, and connection fields are `volatile` for
  cross-thread visibility.
- Untagged fixed-length responses now have their trailing LF verified.

## [0.1.1] - 2026-08-20

- First tag-driven release of the aligned SDK line (see the repository
  release history for earlier changes).

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.4.4...HEAD
[0.4.4]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.4.2...sdk/java/v0.4.4
[0.4.2]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.4.1...sdk/java/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.4.0...sdk/java/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.3.0...sdk/java/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.2.0...sdk/java/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/java/v0.1.1...sdk/java/v0.2.0
[0.1.1]: https://github.com/nanocached/nanocached/releases/tag/sdk/java/v0.1.1
