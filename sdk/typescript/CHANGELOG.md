# Changelog

All notable changes to the TypeScript SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/typescript/vX.Y.Z` tags. From 0.4.4 on, every release ships
the server, the six SDKs and the seven framework adapters at one version,
whether or not a component changed. There is no `sdk/typescript/v0.4.3`
tag: 0.4.3 was a server-only release.

## [Unreleased]

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project: from 0.4.4 on,
every release ships the server, the six SDKs and the seven framework
adapters at one version, whether or not a component changed.

## [0.4.2] - 2026-09-07

### Fixed

- **Breaking:** `connect()` now rejects `readHedgeAfterMs` combined with `viaProxy: true`
  and `ca` combined with `tls` unset or `false`, both previously accepted
  and silently inert. The error is a `NanocachedError` (issue #488).
- `incr`/`decr` no longer poison the connection when the key's remaining
  TTL in the reply is larger than `Number.MAX_SAFE_INTEGER`. A key written
  by another SDK with such a TTL made the reply parser throw `invalid ttl
  in response`, which tore down the shared connection and every request
  pipelined on it. The reply's TTL is now accepted over the wire's full
  unsigned 64-bit range (imprecise past 2^53) and clamped to
  `Number.MAX_SAFE_INTEGER` when forwarded to replicas (issue #501).

## [0.4.1] - 2026-09-05

### Fixed

- `close()` now resolves only after the sockets' `close` events have
  fired. Before, `await client.close()` followed by `connect()` to the same
  address could log a false "was close() forgotten?" warning, because the
  per-target open-connection guard had not been released yet. The warning
  still fires when `close()` is genuinely not awaited (issue #478).
- README only: the read-repair write-back is no longer described as
  uncapped and undrained (it shares the fire-and-forget replica budget and
  `close()` drains it).

## [0.4.0] - 2026-09-05

### Added

- Namespaces (issue #105): `client.namespace(ns)` returns a
  `NanocachedNamespace` handle scoped to `ns` (a `string` or `Uint8Array`,
  opaque bytes). The same key name under two namespaces, or under none, is
  an independent entry. The handle has the same operations as the client
  and shares its connections and routing. `namespace("")` is equivalent to
  the client itself, and the namespace-less API is unchanged. Namespaced
  requests use new wire frames, so every node in a cluster must be
  upgraded before clients use namespaces; the default namespace still sends
  the legacy frames, so existing placement and old servers are unaffected.
- Namespace clear and flush (issue #106): `handle.clear()` drops one
  namespace and `client.clearAll()` drops every namespace, the default one
  included. Both fan out to every node and resolve only once every node has
  acknowledged. If any node fails, the node list is refreshed once and the
  whole fan-out is retried; a node that still fails raises a
  `NanocachedError` naming it. Both are idempotent, so retrying is safe.
- SDK proxy mode (issue #122): `viaProxy: true` connects through a
  `nanocached-proxy` tier. `addresses` must still name discovery server(s),
  which now also serve the proxy roster. The client picks one proxy at
  random (spreading a fleet across the tier), fails over through the rest,
  and then runs in single-connection mode: no ring and no per-node
  connections. After a connection loss it redials the same proxy first and
  only then re-fetches the roster. Pointing it at a cache node address
  fails `connect()` fast. Off by default.
- Retryable-error status `R` (issue #125): every connection now declares
  that it understands a transient-failure reply, falling back transparently
  to the older handshakes against servers that do not. A request answered
  `R` (today only `nanocached-proxy` sends it) is retried on the same
  connection up to twice more, 50 ms then 100 ms apart. If the third
  attempt is still answered `R`, the operation rejects with the new
  `RetryableError`; the connection is never closed over it. Every `R`
  received is counted in the new `stats().transientRetries`.
- `incr(key, delta = 1)` and `decr(key, delta = 1)` (issue #129), also on
  namespace handles. They atomically add to an integer counter and resolve
  the new value, or `null` for a missing or expired key. A non-integer
  stored value, or a `delta` that would overflow, rejects with the new
  `NotNumericError`. In a cluster only the primary owner runs the
  increment; replicas receive its exact result as an ordinary `set`. A
  counter result past `Number.MAX_SAFE_INTEGER` rejects with the new
  `CounterOutOfRangeError` (the increment itself still happened), and an
  unsafe `delta` rejects with `RangeError` before any I/O. A client built
  with `compress: true` rejects `incr`/`decr` up front with the new
  `CompressionIncompatibleError` (issue #321). A counter is as volatile as
  any `set` value: eviction and expiry reclaim it.
- Compare-and-set (issue #141): `getWithToken(key)` resolves `{ value,
  token }` (or `null`), where `token` is a digest of the value's exact
  stored bytes. `putIfAbsent`, `replaceIfPresent`, `replace(key, token,
  newValue, ttlSeconds)` and `deleteIfMatches(key, token)` resolve `true`
  when applied and `false` on a condition mismatch, never throwing for the
  mismatch. `contentDigest` is exported. Only the primary owner evaluates
  the condition, and replicas receive the resulting state. This is not a
  distributed lock, because LRU eviction can reclaim a key.
- Batched get and set (issue #151): `getMany`/`getManyBytes` resolve a
  `Map` of the hits (misses are absent), and `setMany`/`setManyBytes` take a
  record and one shared TTL. Keys are `string`s, and the methods are also
  on namespace handles. One round trip per owner, with transparent
  chunking above `MAX_BATCH_KEYS` (now exported, 400) and by cumulative
  frame bytes so a chunk never exceeds the server's request cap. Hedged
  reads and read repair do not apply to batches. Each key's outcome is
  independent: if keys are still routed to the wrong node after one
  refresh-and-retry, `getMany`/`getManyBytes` throw `PartialWrongNodeError`
  (a `WrongNodeError` subclass whose `partialValues` holds every key that
  resolved), and `setMany`/`setManyBytes` throw a plain `WrongNodeError`.
  If the connection dies on a later chunk after an earlier one succeeded,
  the new `PartialConnectionLostError` (a `ConnectionLostError` subclass)
  carries `partialValues`: a `Map` for gets, or the `string[]` of keys
  already stored for sets. A reply is bounded to 64 MiB in total and, on
  a compress-enabled client, to 256 MiB decompressed across the batch.
- `ConnectionLostError` gained a `requestWasSent` property (default
  `true`), `false` only when the SDK can prove the request never reached
  the wire.

### Changed

- `incr`/`decr`, the compare-and-set operations and `deleteIfMatches` are
  no longer replayed after a lost reply (issue #225). They are retried only
  after a `WrongNodeError` (the node declined the request) or when the
  request provably never reached the primary. If the primary may have
  applied it and only the reply was lost, the call rejects with
  `ConnectionLostError`
  (`requestWasSent: true`), because replaying would double-apply an
  increment or misreport a successful compare-and-set as a mismatch.
  `get`/`set`/`delete`, `clear` and the batch operations are still retried
  as before.
- A node-list refresh that finds a newly listed node unreachable now keeps
  it in the ring without a connection, with its reconnect cooldown armed,
  like `connect()` does, instead of dropping it. Before, this client's
  primary and replica choice for keys near that node disagreed with every
  peer that did reach it until the next refresh. The failure stays silent
  and is counted in `stats().refreshFailures`.
- Newly listed nodes in a refresh are now dialed concurrently, so a
  scale-out of N slow nodes no longer stalls waiting requests for N
  connect timeouts.

### Fixed

- `ttlSeconds` above `Number.MAX_SAFE_INTEGER` (for example `1e21`) is
  now rejected synchronously with `RangeError` by `set`, the
  compare-and-set writes and `setMany`/`setManyBytes`, before any I/O.
  Before, it serialized as `1e+21`, which the server cannot parse. It
  answers with no reply and closes the connection, taking every pipelined
  request down with it.
- Wire integers (lengths, counts, TTLs, tags, discovery node counts and
  replication factors, ports) are now parsed digits-only. A leading `+`,
  an exponent or a digit string past the safe-integer range is treated as
  a protocol violation instead of being accepted through `Number()`
  (issues #462 and #233).
- Duplicate node names in a discovery roster are now deduplicated (first
  occurrence wins) during connect and refresh. A repeated name used to be
  dialed twice and could weigh double in rendezvous scoring (issue #461).
- Hedged reads (issue #276): the losing legs left running in the
  background are now capped at 32 per client. Past the cap a read awaits its
  own losers instead of detaching them, so a persistently slow owner can no
  longer make that set grow without bound.
- A `close()` racing a hedged read can no longer leave a hedge leg dialing
  a connection that teardown is already closing, which surfaced as a
  spurious mid-request error (issue #91).
- Reconnect-cooldown entries for addresses that left the cluster are now
  purged on refresh (issue #96), and in proxy mode when the client swaps
  to another proxy (issue #296). A long-lived client behind churning
  addresses used to leak one entry per departed address.
- Every connected socket now sets `TCP_NODELAY` (issue #301), as the other
  SDKs, the server and the proxy already did. Before, small request and
  response frames could wait on Nagle's algorithm.
- A genuine bug (a non-network error) in a fire-and-forget replica leg is
  no longer swallowed when the primary write fails: the real error now
  surfaces, as documented (issues #188 and #233).
- A response that fails to parse, or a frame that grows past the size cap
  without completing, now marks the connection closed immediately. Before,
  another request could be written into the already-dead connection
  before the socket's `close` event arrived (issue #187).
- The discovery and handshake read path no longer re-copies its buffer on
  every chunk (quadratic in the response size), and multi-get/set header
  length caps can no longer be bypassed by a complete header arriving in
  one chunk (issue #423).

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. Server-side, this
release fixes the cluster data-loss and availability bugs found in the
v0.2.0 end-to-end run (issues #61–#63, #66) and changes the discovery
heartbeat acknowledgment so nodes learn of evictions: upgrade nodes
before discovery servers.

### Added

- Hedged reads: `readHedgeAfterMs` sends a read to the next owner as well
  once the primary has been silent for that many milliseconds (and the
  owner after it once another interval passes), taking the first answer:
  a hit from any owner, or a miss once every owner has answered or
  failed. A replica's miss is only provisional (the primary's answer still
  decides), and a `WrongNodeError` propagates as in the plain read path.
  Off by default (`undefined`); needs `replication >= 2`; a non-positive
  value is rejected at `connect()`. The losing leg is not cancelled: it
  runs to completion detached and `close()` drains it (issue #64).

### Changed

- `DiscoveryBusyError`'s message no longer claims the replica is "warming
  up after a restart": `B` is also what a replica whose replication factor
  disagrees with the cluster's answers (issue #68).

### Fixed

- `connect()` no longer fails just because one of the nodes discovery
  lists cannot be reached yet, typically one that just died and has not
  yet been evicted from discovery's liveness window. Every listed node is
  now dialed concurrently, and an unreachable one is installed as a member
  without a connection and with its reconnect cooldown armed, so requests
  for its keys fail over per request. Only a cluster with no reachable
  node at all still fails `connect()`, with the last dial error
  (issue #67).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version.
0.1.x were pipeline-validation releases; as a pre-1.0 line, behavior
changes ship without a deprecation cycle and are listed below.

### Added

- `NanocachedError` is now the common base class of every error the SDK
  raises (issue #44): `AlreadyClosedError`, `WrongNodeError`,
  `ConnectionLostError`, `DecompressionError` and `DiscoveryBusyError`
  extend it, and authentication, protocol and discovery-response failures
  that used to be plain `Error`s now throw it. Node socket errors such as
  `ECONNREFUSED` still surface unchanged.
- `AuthenticationError` (a `NanocachedError`) is thrown when the server
  rejects the handshake secret, whether none was supplied for a secured
  server or the secret was wrong. It lets callers tell a non-transient
  credentials problem from a protocol violation without matching on the
  message.
- `client.stats()` returns `{ replicaWriteFailures, readRepairFailures,
  refreshFailures }`, monotonic counters for the failures the SDK swallows
  by design. Only a failed repair write-back counts in
  `readRepairFailures`; a failed owner probe is silent.
- `reconnectCooldownMs` option (default 1000): an address whose redial
  just failed is treated as still down for that long, and requests routed
  to it fail immediately with the original dial error instead of each
  paying another connect timeout.
- Echoed response tags (issue #35): the handshake negotiates a per-request
  tag that the server echoes on every response, so a desynced stream is
  detected and the connection closed before a misaligned frame is
  delivered to a caller. Against a pre-tag server the client falls back
  transparently to the untagged protocol after one redial.
- A 30-second progress-based in-flight request timeout (issue #42): the
  deadline is armed when the pending queue goes non-empty and re-armed on
  each response, so a half-open server (TCP alive, silent after the
  handshake) can no longer hang requests, and everything pipelined behind
  them, indefinitely. An idle connection is never closed by it.

### Changed

- `close()` now returns `Promise<void>` and resolves only after every
  in-flight fire-and-forget replica write and read-repair write-back has
  finished and the connections are torn down (issue #47). Callers that
  ignore the result still compile, but must `await` it to get the drain
  guarantee, as a process that closes and exits no longer drops replica
  writes that were still being sent.
- Read repair now writes the repaired value with a 60-second TTL instead
  of none, so a repaired key can no longer resurrect already-expired data
  permanently. It also no longer re-probes the primary on a clean miss.
  Its write-backs are bounded and drained by `close()`.
- Empty keys and requests over the server's request size cap are now
  rejected client-side with `RangeError` before any I/O. The server
  silently closes the connection on both, which took every request
  pipelined on it down. A `set` is also checked against the size cap
  before compression, as in the other SDKs, rather than after.
- The keep-alive ping key is now the reserved `\0nanocached-keepalive`
  instead of the single byte `\0`, which is a valid application key whose
  LRU recency every ping silently refreshed.

### Fixed

- Failures that are bugs rather than network conditions (`TypeError`,
  `RangeError` and similar thrown from a replica leg, a read-repair
  write-back or a refresh) now propagate instead of being swallowed like a
  dead replica. This also fixes an unhandled-rejection crash on replica
  legs, and a replica leg error no longer replaces an already-successful
  primary result.
- A socket write error now closes the whole connection and rejects every
  pending request, instead of leaving a broken socket routable. An
  unsolicited `B` (connection-limit) frame closes the connection
  immediately, and `B` during a node handshake is a `ConnectionLostError`
  so the retry layer handles it.
- Receive buffers are bounded against a malicious server: header
  searches and total frame accumulation are capped (the cap includes the
  echoed tag, so a legal near-maximum value frame is not mistaken for a
  desync), a discovery node list is capped at 65536 nodes and 16 MiB
  total, and chunked accumulation replaces per-chunk `Buffer.concat`.
- Discovery-supplied ports are validated as 0-65535 and a bad one is a
  swallowable `NanocachedError`, restoring the "a refresh never throws to
  the caller" contract (it used to escape as a `RangeError`).
- Discovery now shares one deadline per seed address instead of two
  independent timers, and an empty node list counts as a refresh failure.
- Unterminated untagged fixed-size responses are checked for their trailing
  newline and close the connection if it is missing.

## [0.1.2] - 2026-08-20

0.1.x were pipeline-validation releases: 0.1.1 failed to publish, and
0.1.2 is the first release published by CI.

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.4...HEAD
[0.4.4]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.2...sdk/typescript/v0.4.4
[0.4.2]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.1...sdk/typescript/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.0...sdk/typescript/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.3.0...sdk/typescript/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.2.0...sdk/typescript/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.1.2...sdk/typescript/v0.2.0
[0.1.2]: https://github.com/nanocached/nanocached/releases/tag/sdk/typescript/v0.1.2
