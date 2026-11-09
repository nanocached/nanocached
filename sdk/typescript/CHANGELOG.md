# Changelog

All notable changes to the TypeScript SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/typescript/vX.Y.Z` tags. From 0.4.4 on, every release ships
the server, the six SDKs and the seven framework adapters at one version,
whether or not a component changed. There is no `sdk/typescript/v0.4.3`
tag: 0.4.3 was a server-only release.

## [Unreleased]

### Changed

- **Breaking:** `MAX_BATCH_KEYS` is no longer exported (it was exported only so tests could assert on it; the cache-manager adapter now holds its own copy of the number). `client.url` is no longer reachable (it was an undocumented public field) and `client.nodeUrls` is read-only.
- **Breaking:** the option mistakes `connect()` catches before dialing (an empty `addresses`, a bad `readHedgeAfterMs` or `reconnectCooldownMs`, `readHedgeAfterMs` with `viaProxy`, `ca` without `tls`) now throw a builtin `RangeError` instead of a `NanocachedError`, as the README already said caller mistakes do. Python raises `ValueError` for the same mistakes, Java `IllegalArgumentException`.
- `readHedgeAfterMs` above 24 hours is now rejected. Above 2^31-1 the timer overflowed and fired after about a millisecond, so hedging sent a second request at once.
- **Breaking:** `reconnectCooldownMs: 0` no longer switches the reconnect cooldown off; like omitting it, it now means the default (1000 ms), which is how the Go, Rust, Java and .NET SDKs read `0`. A negative or non-finite value is now rejected. To switch the cooldown off, use the new `disableReconnectCooldown: true` (combining it with a non-zero `reconnectCooldownMs` is rejected).

### Fixed

- Hedged reads (`readHedgeAfterMs`) stalled once an owner had stopped answering: after 32 hedge legs were left running against it, the next read waited for the request timeout (30 s) for one of them instead of returning the replica's answer, and against an owner that was only slow the limit made hedging ineffective at moderate read rates (about 400 reads per second and up against an owner answering in a few hundred milliseconds). An owner that has 32 hedge legs unanswered after the hedge interval is now tried last, so reads go to its replica first; a miss still waits for the primary. The limit on hedge legs in flight, which waits for a read's remaining legs, is raised from 32 to 256 and now only applies when every owner of a key is slow. (#1390)
- The limit of 256 on the hedge legs a client leaves running was compared against every hedge leg in flight, including the legs of reads that had not yet been answered, so it measured read concurrency rather than accumulation: 400 concurrent `get()` calls against a slow primary all waited for it, and any workload with more than 256 reads in flight made every read with a slow loser wait for it. The limit now counts only the legs left running after their read returned. (#1449)
- The last owner a hedged read started was never counted as late, because the hedge timer was armed only while another owner remained to start. With replication 2 a node was therefore demoted only for the keys it is primary for. The timer is now waited out once more for the final leg. (#1450)
- `getMany` and `getManyAndTouch` replaced the wrong-node or connection-loss error with a `TypeError` when one of the values already resolved was not valid UTF-8: the partial values were decoded strictly all at once, so the first binary value threw out of the `catch` that was re-throwing `PartialWrongNodeError`, `PartialConnectionLostError` or `PartialServerTooOldError`, and the caller lost both the error and the other values. The partial values are now decoded one by one: a value that is not valid UTF-8 is left out of `partialValues` (`getManyBytes` still returns its bytes) and the original error is re-thrown with the rest, as the Python SDK does. (#1484)
- A redial that finished after a node-list refresh had removed the member failed the operation with a plain `NanocachedError` ("... left the cluster while reconnecting"), which the refresh-and-retry layer does not recognise, so even a never-written `incr`/`decr` or compare-and-set failed instead of refreshing and rerouting to the new primary. The error is now a `ConnectionLostError` marked as never sent, like the other failures to obtain a member's connection. (#1569)
- The reconnect-cooldown map was pruned only for the members removed in the same refresh, so a redial that was still in flight when a refresh removed its member (a black-holed address takes up to the connect timeout to fail) wrote its cooldown afterwards and nothing deleted it, one small entry per such address. Every refresh now drops the cooldowns of addresses that are no longer listed, as the Python and Go SDKs do. (#1547)
- With `compress: true`, a key and value exactly at the request size limit passed the check on the original value and were then refused with a `RangeError` for the one byte the compression marker adds, which the Go SDK accepts; and `setMany`/`setManyBytes` with such a value in a later sub-frame stored the earlier sub-frames before throwing, although the size check is meant to run before any I/O. The limit is now applied to the value before compression, as in Go, and the encoders allow the marker byte on top of it. (#1546)
- The hash ring broke a tie between two equal scores by comparing node names as UTF-16 code units, while the server and the other SDKs compare them as UTF-8 bytes; the two orders differ for a name with a supplementary-plane character against one in U+E000..U+FFFF, so on a score tie (two equal 64-bit scores, practically unreachable) the client could pick a different primary than the node. Names are now compared by their UTF-8 bytes. (#1544)
- A forced node-list refresh (after an operation failed against a dead or wrong node) opened a new discovery connection for every failed operation: while discovery still listed a dead node, a caller looping over keys routed to it opened one connection per failure, measured at about 765 per second until loopback ephemeral ports ran out (`stats().refreshFailures` in the thousands). A forced refresh now waits at least 200 ms after the previous refresh finished: a caller that fails inside that window reuses the refresh that just finished, and callers waiting on an older refresh share one follow-up that starts when the window has passed. (#1530)
- A request that was already running when `close()` destroyed its connection (and then needed a redial, for instance as the retry after the connection failure) dialed the node anyway: the redial path checked whether the client was closed only after the dial had finished, so the request spent up to a connect deadline (5 s) opening a socket it then closed before failing with `AlreadyClosedError`. A single-node, cluster-member or proxy redial now fails with `AlreadyClosedError` before opening any connection once the client is closed. (#1613)
- `Connection.close()` only destroyed the socket, and the connection counted as closed when the socket's `'close'` event arrived on a later tick. A request issued in between (an operation that still held the connection of a member a node-list refresh had just closed) found it open, wrote into the destroyed socket and failed with a `ConnectionLostError` marked as sent although the node had received nothing, so an `incr`, compare-and-set or expiring touch was not retried. `close()` now marks the connection closed at once, through the same path as every other fatal error, so such a request is rejected as never sent and rerouted; requests already written when `close()` runs are still rejected as sent. (#1614)
- `touchMany` and `getManyAndTouch` with a TTL of `0` in a cluster gave up on every chunk from the failed one onward when a connection was lost after a chunk's frame was written: the chunks that were never sent were counted as lost as well, so they were neither expired nor retried and the call threw for all of them (1,200 keys in three chunks, connection dropped on the second: two frames sent, 800 keys reported lost). Only the chunk whose frame was written is now lost, because only it may have been applied; the chunks after it go to the retry pass and are sent, as the Rust, Java and .NET SDKs do. (#1601)
- A listed address that answered as a discovery server or proxy instead of a node (an address another kind of server now answers on) failed `set`, `incr` and the other operations on the keys it owns with a plain `NanocachedError`, whether it was met when a node-list refresh dialed a newly listed node (the error is cached for the reconnect cooldown) or when a lost connection was redialed. The refresh-and-retry layer does not recognise that error, so nothing refreshed or rerouted until the 30-second staleness refresh, and a never-written `incr` or compare-and-set failed too; reads fell back to the next owner. Both cases are now a `ConnectionLostError` marked as never sent, like the other failures to reach a node, so the operation refreshes the node list and reroutes. (#1596)

## [0.6.2] - 2026-11-03

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.1] - 2026-11-02

### Fixed

- With `fireAndForgetReplicas`, a write whose primary failed (a refused
  connection, say) still waited for every background replica write to settle
  before reporting the failure, so one slow or hung replica delayed the error
  and the refresh-and-retry behind it by up to its request timeout (35 s
  measured against a stopped node). Only the synchronous legs are waited for
  now; a programming error that a background leg has already raised is still
  reported (#1240).
- A forced node-list refresh (made after a connection failure or a wrong-node
  answer) joined a refresh that was already in flight, even when that one had
  fetched its list from discovery before the failure. If a node died in
  between, the stale list still contained it, the retried operation failed
  against it again and the error reached the caller. A forced refresh now
  waits such a refresh out and is satisfied only by one that started after
  the failure; callers that arrived during the old refresh share a single
  follow-up refresh (#1227).
- A `close()` that ran in the few microtasks between a redial finishing and
  its connection being installed (single-node, cluster member or proxy
  target) closed only the connections installed at that moment, so the new
  connection stayed open with nothing left to close it, until the server's
  60-second idle timeout. The redial's result is now checked against the
  closed flag before it is installed: it is closed, and the interrupted call
  rejects with `AlreadyClosedError` (#1216).
- Any `Error` carrying a string `code` was treated as a lost connection, so
  `ERR_INVALID_ARG_TYPE` (what `Buffer.concat` throws when a JavaScript
  caller passes a non-Buffer value, e.g. `set(k, 123 as any)`) made the
  client force a discovery refresh and run the operation a second time; in
  proxy mode the refresh also replaced a healthy connection without closing
  the old one. Only `ConnectionLostError` and the errno codes of a dead or
  unreachable peer (`ECONNRESET`, `EPIPE`, `ECONNREFUSED`, `ETIMEDOUT`,
  `ENOTFOUND`, `EHOSTUNREACH`, `ENETUNREACH`, `ECONNABORTED`) count now
  (#1188).
- A response that arrived with no request pending (a server or proxy
  replying to something nobody asked) was parsed and silently discarded, and
  the connection kept being used. The Python and Go SDKs treat it as a
  desynced stream; this SDK now does too: the connection is poisoned with a
  `ConnectionLostError` at once and later requests redial (#1177).
- `connect()` with a seed address whose port is not a valid TCP port (`NaN`
  from an unset environment variable, a fraction, a number above 65535)
  rejected at once, as it should, but the dial timer had already been armed
  and stayed armed: five seconds later it fired, touched the socket binding
  that was never initialised and threw a `ReferenceError` out of a timer
  callback — an uncaught exception that ended the process after the caller
  had handled the rejection. The socket is now created before the timer is
  armed, so a synchronous dial failure leaves nothing behind (#1163).
- With `fireAndForgetReplicas`, a replica write that was still waiting for its
  connection when `set`, `delete`, `touch`, `getAndTouch`, `incr` or the
  compare-and-set calls resolved read the caller's `Uint8Array` key and value
  at that later point, so a caller that reused its buffers after the `await`
  stored the new bytes under another key on the replica (a `set` of `A..A`
  followed by overwriting the key buffer with `Z` wrote `Z` on the replica).
  The read-repair write-back to the primary had the same problem with the key
  and with the value the read returned. These background legs now work from
  copies made when they are started; the legs a call waits for still use the
  caller's buffers without copying (#1277).

## [0.6.0] - 2026-11-01

### Fixed

- A string with a lone surrogate (`"x\ud800"`) as a key, value or namespace
  was encoded by `Buffer.from` as U+FFFD, so two different keys could be one
  entry and a value or namespace came back rewritten. The call now rejects it
  with a `RangeError` before any I/O, as the Python, Java and .NET SDKs do.
  A `Uint8Array` is unaffected.
- The lone-surrogate check that every string key, value and namespace goes
  through used a lookaround regular expression, which took about 5 ms on a
  1 MiB string of emoji and 1.5 ms on CJK text, on the event loop for every
  `set` of such a value. It now uses the engine's `String.prototype.isWellFormed`
  (Node 22, the package's minimum, has it), about forty times faster, with the
  same result.

### Changed

- The SDK is built and type-checked with TypeScript 7 (`typescript` is a
  development dependency only). The emitted `.d.ts` files are byte-for-byte
  identical to the TypeScript 5.9 output, so nothing changes for consumers.
  `tsconfig.json` now names `node` in `types`, which TypeScript 7 no longer
  includes implicitly.

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

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.6.2...HEAD
[0.6.2]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.6.1...sdk/typescript/v0.6.2
[0.6.1]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.6.0...sdk/typescript/v0.6.1
[0.6.0]: https://github.com/nanocached/nanocached/compare/sdk/typescript/v0.5.1...sdk/typescript/v0.6.0
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
