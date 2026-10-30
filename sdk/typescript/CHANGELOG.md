# Changelog

All notable changes to the TypeScript SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/typescript/vX.Y.Z` tags. From 0.4.4 on, every release ships
the server, the six SDKs and the seven framework adapters at one version,
whether or not a component changed. There is no `sdk/typescript/v0.4.3`
tag: 0.4.3 was a server-only release.

## [Unreleased]

### Fixed

- A string with a lone surrogate (`"x\ud800"`) as a key, value or namespace
  was encoded by `Buffer.from` as U+FFFD, so two different keys could be one
  entry and a value or namespace came back rewritten. The call now rejects it
  with a `RangeError` before any I/O, as the Python, Java and .NET SDKs do.
  A `Uint8Array` is unaffected.

## [0.5.1] - 2026-10-27

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.5.0] - 2026-10-26

### Fixed

- A `ttlSeconds` `0` touch (`touch`, `getAndTouch`, `touchMany`,
  `getManyAndTouch`) no longer sends the other owners the plain touch before
  the primary has answered. A primary that could not be reached, or that
  answered `W` from a stale ring, is rerouted to the surviving replica, which
  had already expired its copy and answered `N`: `getAndTouch` and
  `getManyAndTouch` returned a miss for a key that existed (the value was
  consumed and never returned) and `touch` and `touchMany` reported `false`.
  Now the primary legs go first (both passes of a batch), and the replicas
  get one plain touch afterwards for every key whose primary answered, `N`
  included; a key whose primary failed or answered `W` gets no replica leg.
  These replica legs run after the primary's answer, outside the pipelined-
  order guarantee, like the existing retry paths. A live or removed TTL
  keeps the concurrent fan-out.
- `touch`, `getAndTouch`, `touchMany` and `getManyAndTouch` throw
  `ServerTooOldError` only for the node's `En`. The proxy answers a bare
  `E` (or `E <tag>`) when a touch's outcome is unknown, for example a
  backend failure after the frame was written with `ttlSeconds` `0`; that is
  now a `ConnectionLostError` with `requestWasSent` `true`, like a fatal `E`
  to any other command, and an expiring touch is not sent again.
- In a cluster, `getManyAndTouch` no longer discards the hits other
  primaries returned when one primary answers `En` to `y`. It throws
  `PartialServerTooOldError`, a `ServerTooOldError` subclass, with them in
  `.partialValues`; with `ttlSeconds` `0` they were consumed on the nodes
  and could not be read again. `touchMany` still throws the plain error.

### Added

- `touch(key, ttlSeconds)` replaces an entry's TTL in place and resolves
  whether the key existed; `getAndTouch(key, ttlSeconds)` and
  `getAndTouchBytes` read the value and replace the TTL in one step. All
  three are on `NanocachedNamespace` too. `ttlSeconds` has no default:
  `null` removes the TTL and `0` expires the entry at once (the opposite of
  `set`, where `0` means no expiry); omitting it throws a `RangeError`. They
  are idempotent, so unlike `incr` and the compare-and-set family a lost
  reply is retried; they go to every owner (`getAndTouch` asks only the
  primary for the value). Against a node that predates the command they
  throw the new `ServerTooOldError`. Needs a server release that includes
  the `t` command.
- `touchMany(keys, ttlSeconds)` and `getManyAndTouch(keys, ttlSeconds)`
  (with `getManyAndTouchBytes`, and all three on `NanocachedNamespace`) are
  `touch`/`getAndTouch` for a list of keys, in one round trip per involved
  node. `touchMany` resolves a map of key to whether it existed;
  `getManyAndTouch` resolves the hits, as `getMany` does. One TTL for the
  whole batch, with the meaning and no default of `touch`. Every owner is
  sent the batch; only a key's primary is asked for the value, so a node that
  is primary for some keys and a replica for others receives two frames.
  Failures follow `setMany`/`getMany`: `WrongNodeError` from `touchMany` and
  `PartialWrongNodeError`/`PartialConnectionLostError` from
  `getManyAndTouch`. Needs a server release that includes the `y` command;
  against a node that has `t` but not `y` they throw `ServerTooOldError`,
  which now has a `command` property (`"t"` or `"y"`).

### Changed

- The minimum supported Node.js is 22 (`engines.node` is `>=22`, was
  `>=20`). Node 20 reached end of life on 2026-04-30 and the 20 line has no
  release carrying the root-certificate update made after the eMudhra
  intermediate CA key compromise. The test suite now runs on Node 22, 24
  and the current LTS.
- `set` and the compare-and-set writes no longer copy a `Uint8Array` or
  `Buffer` value when they are called: a write that waits behind a slow or
  stalled peer holds a reference to the caller's buffer instead of a second
  copy of it (256 pending sets of one 512 KiB buffer held 130 MiB; they now
  hold about 3 MiB, and the container's peak against a node that never
  reads fell from 155 to 27 MiB). The bytes are read when the request is
  written, which can be after the call returns, so leave a buffer alone until
  the promise settles (the Go, Java, .NET and Rust SDKs do not copy it
  either). A string is still encoded when the call is made, and
  `setMany`/`setManyBytes` still take their snapshot up front. (#853)

## [0.4.8] - 2026-10-21

### Fixed

- `setMany`/`setManyBytes` report a key as stored when the owner that
  acknowledged it as a replica became its primary between the two
  passes (the old primary answered `W` and left the roster): the key
  sits on its primary, but the earlier acknowledgement was recorded as
  a replica's and the key was missing from
  `PartialConnectionLostError`'s stored keys.
- A connection reset or broken pipe while the server answers the
  handshake throws `ConnectionLostError` (`requestWasSent` `false`, the
  system error as `cause`), as a failed dial does. It escaped as the
  raw `ECONNRESET`/`EPIPE` system error once the legacy-server
  fallbacks were spent, the one connect failure
  `error instanceof NanocachedError` did not catch.
- A dial that fails — `ECONNREFUSED`, `ENOTFOUND`, a reset during the
  handshake — throws `ConnectionLostError` (`requestWasSent` `false`,
  the Node system error as `cause`) instead of the raw system error, so
  `error instanceof NanocachedError` catches it, from `connect()` and
  from the redial every operation makes after its node died (`incr`
  already wrapped its own). A node at its connection limit, which
  answers `B` and closes, is reported as busy instead of as a connection
  closed before the expected response.

- `setMany` stores an entry whose key is `"__proto__"` instead of
  silently dropping it: the string-to-bytes conversion collected the
  batch on a plain object, where assigning that key sets the prototype.
  `set` and `setManyBytes` were unaffected. Callers building a batch
  themselves need a prototype-less object (`Object.create(null)`) for
  that key, since an object literal drops it before the SDK sees it
  (#730).

- `setMany`/`setManyBytes` in cluster mode throw
  `PartialConnectionLostError` (with the keys whose primary did store
  them in `partialValues`) when a key's primary could not be reached at
  all on both passes, instead of `WrongNodeError`, which is now reserved
  for a primary that actually answered `W`. A caller that reacted to
  `WrongNodeError` by refreshing and retrying did so for a dead node, and
  the message claimed the node had answered (#722).
- A namespace is hashed once per batch rather than once per key:
  `getMany`/`setMany` under a 900 KB namespace took 8.8 s for 400 keys
  and blocked the event loop for the whole call.
- The pending-request queue is drained from a head index instead of
  `Array.shift()`, which is O(n): 200,000 concurrent requests on one
  connection spent 52 s in `shift` alone.
- A reply is received into one buffer sized from its header, filled in
  place, instead of one `Buffer` object per `data` event held until the
  frame completes: a peer trickling a 2 MiB reply one byte at a time held
  the client at about 450 MiB (a 64 MiB batch reply at 2.4 GB). The
  buffer is released once a large frame has been parsed.
- Socket writes respect backpressure: a request waits for the socket's
  `drain` before its frame is written (in call order, so wire order is
  unchanged), so a burst of requests to a stalled node no longer sits in
  the stream's buffer in full (600 queued 900 KiB sets held 1.1 GB).
- An idle client no longer keeps the Node process alive: a connection's
  socket is `unref`'d while no request is pending and `ref`'d while one
  is, so a script that never calls `close()` exits once its last request
  has been answered, as with the other SDKs.
- `getMany`/`getManyBytes` read a repeated key once, so a list of many
  copies of one key costs one key's worth of request and reply instead
  of one `m` frame per 400 copies and one retained reply per frame.
- `setMany` in cluster mode retries only the legs that failed. A key
  whose primary leg failed used to be re-sent to every owner after the
  refresh, including the replica that had already acknowledged it.

## [0.4.7] - 2026-10-19

### Fixed

- `getMany`/`getManyBytes` in cluster mode now read a key from its next
  owner when its primary cannot be reached at the connection level, the
  same walk a single `get` already did (#623, the TypeScript part of
  #616). Before, the batch sent every key to its primary only, retried the
  same primary once after a refresh, and then threw `PartialWrongNodeError`
  for keys that nothing had answered `W` for — seen in the field as a
  client that could not verify one node's rotated TLS certificate failing a
  bulk read although the other owner held every key. Keys whose every
  owner is unreachable now throw `PartialConnectionLostError` (or the bare
  connection error when nothing resolved) instead of a wrong-node error.

## [0.4.6] - 2026-10-17

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.5] - 2026-10-15

### Fixed

- `close()` no longer hangs when a node reset its connection between the
  identify handshake and the end of the dial round (#574). A `Connection`
  is built only once the round's slowest dial is done, so a socket reset
  in between had already emitted its `close` event; the connection then
  waited for that event forever and `close()` with it. Such a connection
  is now closed on construction, and the per-target open-connection guard
  no longer counts it, so a later single-address `connect()` to the same
  address is not told a `close()` was forgotten. The same guard was what
  made the client test suite flaky: its tests did not await `close()`, and
  an ephemeral port reused by the next test's mock node tripped it.
- A peer reset on a node socket that had finished identify but was still
  waiting on slower sibling dials in the same bootstrap or refresh round
  was an uncaught `ECONNRESET` that crashed the process; identified
  sockets now always carry an `error` listener.
- A malformed roster address (no port, port above 65535) now counts as one
  unreachable node during `connect()` and node-list refresh, instead of
  aborting the round and leaking the sockets its sibling dials had opened.
- A large reply no longer costs quadratic CPU while it arrives. Every
  fragment used to re-concatenate (and re-parse) everything received so
  far, so a 64 MiB multi-get reply in 64 KiB fragments held the event loop
  for about 3 s, and a 65536-node roster about 0.9 s. The connection now
  waits until the frame's declared length is buffered and concatenates once,
  and the discovery reader parses each roster entry once and drops it.
  Wire behavior is unchanged.
- Routing a key no longer scores every node with BigInt arithmetic and then
  sorts them all: `HashRing.owners` keeps only the best `replicas`
  candidates, like the Go, Rust, Java and .NET SDKs, and does the 64-bit
  scoring on 32-bit halves. A lookup takes about 3 us at 100 nodes (was
  22 us) and 31 us at 1000 nodes (was 278 us). The owner order, ties
  included, is unchanged.
- `connect()` and node-list refresh no longer dial a whole roster at once.
  A discovery server can list up to 65536 nodes, which opened that many
  sockets in a single tick; at most 64 dials are now in flight at a time.
  Outcomes (which nodes are installed, which count as unreachable) are
  unchanged.

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

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.5...HEAD
[0.5.1]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.5.0...sdk/typescript/v0.5.1
[0.5.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.8...sdk/typescript/v0.5.0
[0.4.8]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.7...sdk/typescript/v0.4.8
[0.4.7]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.6...sdk/typescript/v0.4.7
[0.4.6]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.5...sdk/typescript/v0.4.6
[0.4.5]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.4...sdk/typescript/v0.4.5
[0.4.4]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.2...sdk/typescript/v0.4.4
[0.4.2]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.1...sdk/typescript/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.4.0...sdk/typescript/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.3.0...sdk/typescript/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.2.0...sdk/typescript/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.1.2...sdk/typescript/v0.2.0
[0.1.2]: https://github.com/nanocached/nanocached/releases/tag/sdk/typescript/v0.1.2
