# Changelog

All notable changes to the Python SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/python/vX.Y.Z` tags. From 0.4.4 on, every release ships the
server, the six SDKs and the seven framework adapters at one version,
whether or not a component changed (there is no `sdk/python/v0.4.3` tag:
0.4.3 was a server-only release).

## [Unreleased]

### Fixed

- `touch`, `get_and_touch`, `touch_many` and `get_many_and_touch` report
  `ServerTooOldError` only for the node's `En` answer. Any other `E`
  (nanocached-proxy answers a bare `E` when a `ttl=0` touch's outcome is
  unknown, or when a request failed transiently) raised "server too old,
  upgrade every node"; it is now the `ConnectionLostError` a sent request
  fails with, so a `ttl=0` touch is not sent again.
- A reply whose echoed tag does not answer the request it follows is now a
  `ConnectionLostError` for that request, like every other failure after
  the frame was written. It was a plain `ConnectionError`, so a `ttl=0`
  touch, `incr` and the compare-and-set family were sent again: the second
  `touch` reported a key that existed as missing and `incr` was applied
  twice.
- A `ttl=0` touch (`touch`, `get_and_touch`, `touch_many`,
  `get_many_and_touch`) is no longer sent again after a reply that cannot be
  matched to it, a reply of the wrong kind or a roster of the wrong length.
  The frame was written, so the node had already expired the keys, and the
  second one reported them as missing and a `get_and_touch` lost the value.
  They now raise `ConnectionLostError`, as the docstring of that class
  already said a misaligned reply does.
- In a cluster, `get_many_and_touch` no longer discards the hits other
  primaries returned when one primary answers `En` to `y`. It raises
  `PartialServerTooOldError`, a `ServerTooOldError` subclass, with them in
  `partial_values`; with `ttl_seconds=0` they were consumed on the nodes and
  could not be read again. `touch_many` still raises the plain error.

### Added

- `touch(key, ttl_seconds)` replaces an entry's TTL in place and returns
  whether the key existed; `get_and_touch(key, ttl_seconds)` and
  `get_and_touch_bytes` read the value and replace the TTL in one step.
  Both are on `Namespace` too. `ttl_seconds` has no default: `None` removes
  the TTL and `0` expires the entry at once (the opposite of `set()`, where
  `0` means no expiry). They are idempotent, so unlike `incr` and the
  compare-and-set family a lost reply is retried; they go to every owner
  (`get_and_touch` asks only the primary for the value). Against a node that
  predates the command they raise the new `ServerTooOldError`. Needs a
  server release that includes the `t` command.
- `touch_many(keys, ttl_seconds)` and `get_many_and_touch(keys, ttl_seconds)`
  (with `get_many_and_touch_bytes`, and all three on `Namespace`) are
  `touch`/`get_and_touch` for a list of keys, in one round trip per involved
  node. `touch_many` returns a dict of key to whether it existed;
  `get_many_and_touch` returns the hits, as `get_many` does. One TTL for the
  whole batch, with the meaning and no default of `touch`. Every owner is
  sent the batch; only a key's primary is asked for the value, so a node
  that is primary for some keys and a replica for others receives two
  frames. Failures follow `set_many`/`get_many`: `WrongNodeError` from
  `touch_many` and `PartialWrongNodeError`/`PartialConnectionLostError` from
  `get_many_and_touch`. Needs a server release that includes the `y`
  command; against a node that has `t` but not `y` they raise
  `ServerTooOldError`, which now has a `command` attribute (`"t"` or `"y"`).

## [0.4.8] - 2026-10-21

### Fixed

- `set_many`/`set_many_bytes` report a key as stored when the owner that
  acknowledged it as a replica became its primary between the two
  passes (the old primary answered `W` and left the roster): the key
  sits on its primary, but the earlier acknowledgement was recorded as
  a replica's and the key was missing from
  `PartialSetConnectionLostError`'s stored keys.
- A dial that fails raises `ConnectFailedError` (a `NanocachedError` and
  a `ConnectionError`, with the OS error as `__cause__`) instead of the
  raw `ConnectionRefusedError`, `socket.gaierror` or
  `ConnectionResetError` — or, for the connect deadline, a builtin
  `ConnectionError` — so `except NanocachedError` catches a refused or
  unresolvable address the way it does in the Go, Java and Rust SDKs.
  Applies to `connect()` and to the redial any operation makes after
  its node died. A node at its connection limit, which answers `B` and
  closes, is now reported as busy by that same error instead of as
  "connection closed during the auth handshake".

- `set_many` in cluster mode raises `PartialSetConnectionLostError`
  (with the keys whose primary did store them in `partial_keys`, and the
  connection failure as `__cause__`) when a key's primary could not be
  reached at all on both passes, instead of `WrongNodeError`, which is
  now reserved for a primary that actually answered `W`. A caller that
  reacted to `WrongNodeError` by refreshing and retrying did so for a
  dead node, and the message claimed the node had answered (#722).
- A write whose bytes the node has not taken (a node that stopped
  reading) no longer outlives the 30 s request deadline, and `close()`
  no longer leaves such callers pending with their socket open: the
  connection's transport is aborted when it is torn down with unsent
  bytes, so callers blocked in the write fail at once. `close()` with
  `fire_and_forget_replicas=True` therefore returns within the request
  deadline when a replica has stopped reading, instead of never.
- `set_many` in cluster mode retries only the legs that failed. A key
  whose primary leg failed used to be re-sent to every owner after the
  refresh, including the replica that had already acknowledged it.
- `get_many`/`get_many_bytes` collapse a repeated key (the same key
  twice, or `"a"` and `b"a"`, which encode identically) and read it
  once, keyed by its first occurrence, as the other SDKs do, instead of
  raising `ValueError`. A list of many copies of one key no longer
  sends one `m` per 400 copies and retains one reply per chunk.
  `set_many` still rejects two values for one encoded key.
- A namespace is hashed once per batch rather than once per key:
  `get_many`/`set_many` under a 900 KB namespace took 27 s for 400 keys
  and blocked the event loop for the whole call.

## [0.4.7] - 2026-10-19

### Fixed

- `get_many`/`get_many_bytes` in cluster mode now read a key from its next
  owner when its primary cannot be reached at the connection level, the
  same walk a single `get` already did. Before, the batch sent every key
  to its primary only, retried the same primary once after a refresh, and
  then raised `PartialWrongNodeError` for keys that nothing had answered
  `W` for — seen in the field as a client that could not verify one
  node's rotated TLS certificate failing a bulk read although the other
  owner held every key. Keys whose every owner is unreachable now raise
  `PartialConnectionLostError` (or the bare connection error when nothing
  resolved) instead of a wrong-node error.

## [0.4.6] - 2026-10-17

### Fixed

- A connection the node resets mid-session (a node killed, or its listener
  torn down under a scale-in) now waits for its socket to finish closing
  once the read loop has failed it, which retrieves the
  `ConnectionResetError` asyncio had recorded on the protocol's close
  waiter. Before, `_poison()` only closed the writer and the client
  replaced the connection on its next redial without ever waiting on it,
  so the error sat unretrieved and asyncio logged `Future exception was
  never retrieved` (an ERROR through the `asyncio` logger) whenever the
  garbage collector happened to finalize the waiter before the protocol
  — the established-connection twin of the dial-time case fixed in 0.4.5
  (#588). Only a connection the peer already took down is waited for; a
  `close()` of a healthy connection never waits on a TLS shutdown
  handshake. No socket was leaked; the error the caller sees is
  unchanged (#599).
- A dial that fails during the `A` handshake — typically a node exiting
  mid-handshake and resetting the connection — now waits for its socket
  to finish closing, which retrieves the connection error asyncio had
  recorded on the protocol's close waiter. Before, that error sat
  unretrieved and asyncio logged `Future exception was never retrieved`
  (an ERROR through the `asyncio` logger) whenever the garbage collector
  happened to finalize the waiter before the protocol — seen as a
  spurious ERROR at interpreter exit after a node scale-in. No socket was
  leaked; the error the caller sees is unchanged.

## [0.4.5] - 2026-10-15

### Fixed

- A malformed roster address (no `:port`, port above 65535) now counts as
  one unreachable node during `connect()`, as it already did during
  node-list refresh, instead of failing the whole bootstrap. A dial round
  that is abandoned for any other reason (an unexpected exception, a
  cancellation) now closes the sockets its sibling dials had opened rather
  than leaving them to the garbage collector.
- `connect()` and node-list refresh no longer dial a whole roster at once.
  A discovery server can list up to 65536 nodes, which opened that many
  sockets together; at most 64 dials are now in flight at a time. Outcomes
  (which nodes are installed, which count as unreachable) are unchanged,
  and refresh still dials new nodes eagerly.
- Keep-alive pings no longer run one after another. A half-open node, whose
  ping blocks until the 30 s request timeout, used to keep every node after
  it from being pinged until those had sat idle for about 60 s, the
  server's idle limit. Each connection now has its own ping task (as in the
  Go SDK), and a connection whose last ping is still outstanding is not
  pinged again.
- Routing a key no longer sorts every node: `HashRing.owners()` keeps only
  the best `replicas` candidates, like the Go, Rust, Java and .NET SDKs,
  and skips building a tuple for any node scoring below the worst one
  kept. A lookup takes about 39 us at 100 nodes (was 61 us) and 367 us at
  1000 nodes (was 686 us); the rest is the 64-bit scoring itself, which
  stays pure Python. The owner order, ties included, is unchanged.

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project: from 0.4.4 on,
every release ships the server, the six SDKs and the seven framework
adapters at one version, whether or not a component changed.

## [0.4.2] - 2026-09-07

### Changed

- **Breaking:** `NanocachedClient.connect()` now raises `ValueError` for two option
  combinations that were previously accepted and silently ignored:
  `read_hedge_after` together with `via_proxy=True` (a proxy connection has
  no replicas to hedge to) and `ca` without `tls=True` (the CA file was
  never read, so the client connected in plaintext) (issue #488).
- `get_many()`: the `partial_values` carried by `PartialWrongNodeError` and
  `PartialConnectionLostError` is now decoded strictly, like a successful
  `get_many()`. A value that is not valid UTF-8 is left out of
  `partial_values` instead of being returned with replacement characters;
  `get_many_bytes()` still carries the exact bytes (issue #488).

### Fixed

- In cluster mode, `get_many()` no longer resends an owner's whole group
  when a later chunk fails at the connection level. The chunks that already
  answered are kept and only the unresolved tail goes to the retry pass,
  as in the TypeScript and Rust SDKs (issue #485).

## [0.4.1] - 2026-09-05

### Fixed

- Documentation only (issue #478): the README no longer says the read-repair
  write-back is uncapped and not drained on `close()`. It shares the
  in-flight budget with fire-and-forget replica writes and `close()` drains
  it. No behavior change.

## [0.4.0] - 2026-09-05

### Added

- Namespaces (issue #105): `client.namespace(ns)` returns a lightweight
  `Namespace` handle (exported from `nanocached`) scoped to `ns`, which
  accepts `str` or `bytes`. The same key under two namespaces, or under no
  namespace, is an independent entry. The handle has the same operations as
  the client (`get`, `get_bytes`, `get_with_token`, `set`, `delete`,
  `get_many`, `get_many_bytes`, `set_many`, `incr`, `decr`, `put_if_absent`,
  `replace_if_present`, `replace`, `delete_if_matches`) with the same
  routing, replication, hedged-read, `W` refresh-and-retry and compression
  behavior, and shares the client's connections. `namespace("")` is
  equivalent to the client itself. Once the client is closed, the handle's
  operations raise `AlreadyClosedError`. A non-empty namespace is part of
  the rendezvous-hash input (a length-prefixed form); the default namespace
  hashes exactly as before, so existing keys do not move. `HashRing.owners()`
  and `HashRing.route()` gained a keyword-only `namespace=b""` argument, so
  existing calls are unchanged. Namespaced frames need a namespace-aware
  server.
- Clearing (issue #106): `Namespace.clear()` drops every entry in that
  namespace (`namespace("").clear()` clears the default namespace), and
  `NanocachedClient.clear_all()` flushes every namespace. Both fan out to
  every known node and succeed only once all nodes acknowledge. If a node
  fails, the node list is refreshed once and the whole fan-out is retried;
  a node still failing then raises `NanocachedError` naming it (in
  single-node mode the error propagates immediately). Both are idempotent,
  so retrying is safe. They raise `AlreadyClosedError` after `close()`.
- Proxy mode (issue #122): `connect(..., via_proxy=True)` connects through
  a `nanocached-proxy` tier. `addresses` must still name discovery servers;
  the client fetches the proxy roster (the new `Q` command) and connects to
  one proxy chosen at random, so a fleet of clients spreads across the
  tier. If the connection is lost, the same proxy is redialed first, then
  the roster is re-fetched and another random proxy is tried. Pointing
  `via_proxy` at an address that identifies as a cache node raises
  `NanocachedError`. Reconnect cooldowns for proxies the client has left
  are purged (issue #363). Off by default.
- Retryable-error status `R` (issue #125): the handshake now also
  advertises that this client understands a transient-failure reply, and a
  request answered `R` is retried on the same connection up to twice more
  (3 attempts in total, 50 ms then 100 ms apart). If the third attempt is
  still answered `R`, the call raises the new `RetryableError`; the
  connection is never closed or redialed and stays usable. Every `R`
  received is counted in the new `ClientStats.transient_retries` counter.
  Falls back automatically against servers that do not know the capability.
  Only `nanocached-proxy` sends `R` today.
- `incr(key, delta=1)` / `decr(key, delta=1)` (issue #129): atomically add a
  signed 64-bit `delta` to an integer counter and return the new value, or
  `None` for a missing or expired key. Raises the new `NotNumericError` when
  the stored value is not an integer or the result would overflow. In
  cluster mode only the primary runs the increment; the resulting value is
  then forwarded to the replicas as an ordinary `set`, so replicas never
  drift. A counter is as volatile as any other entry (eviction and TTL
  expiry apply), so it suits rate limiting and approximate counts, not
  durable ones. `incr`/`decr` on a client built with `compress=True` raise
  the new `CompressionIncompatibleError` before any I/O, because an
  increment's result carries no compression marker (issue #321).
- Compare-and-set (issue #141): `put_if_absent(key, value)`,
  `replace_if_present(key, value)`, `replace(key, token, new_value)` and
  `delete_if_matches(key, token)`, each returning a `bool`; the three
  writers take `ttl_seconds`. `get_with_token(key)` returns
  `(value_bytes, token)` or `None`, and `content_digest(value)` computes a
  token from known bytes. A token is a digest of the stored bytes, not the
  value itself, so reconstructing one without a prior read is only correct
  if it reproduces the stored wire bytes exactly. Only the primary applies
  the operation; its result is forwarded to the replicas. Eviction can
  break the mutual exclusion these give.
- Batched operations (issues #128, #150, #151): `get_many(keys)`,
  `get_many_bytes(keys)` and `set_many(values, ttl_seconds=0)` send one
  round trip per involved node instead of one per key. The result is a
  `dict` keyed by the objects the caller passed; a miss is simply absent.
  `set_many` applies one shared TTL to the whole batch. Large batches are
  transparently split into several sub-frames, bounded by key count (400)
  and by bytes (issue #222). Two keys that encode to the same bytes raise
  `ValueError` up front. Hedged reads and read repair do not apply to batches.
  If some keys are still wrong-node after one bounded refresh-and-retry,
  `get_many` raises the new `PartialWrongNodeError` (a `WrongNodeError`
  subclass, so existing `except WrongNodeError` keeps working) whose
  `partial_values` holds the keys that did resolve; `set_many` raises a
  plain `WrongNodeError` (every other key was still stored). In
  single-node and proxy mode, a connection failure on a later chunk after
  an earlier one succeeded raises `PartialConnectionLostError`
  (`partial_values`) or `PartialSetConnectionLostError` (`partial_keys`),
  with the original error as `__cause__` (issue #411).
- `ConnectionLostError` (a `NanocachedError` and `ConnectionError`):
  raised when a request's frame had already been written to the socket
  before the connection failed, so the server may have applied it.

### Changed

- Requests that are not idempotent (`incr`/`decr`, the compare-and-set
  operations) are never replayed after a lost reply: if the frame may have
  reached the server, `ConnectionLostError` is raised instead of resending,
  which would double the increment or misreport a succeeded
  compare-and-set. A connection that was already dead before the frame was
  written still redials and retries transparently. `get`, `set`, `delete`,
  `clear` and the batch operations keep retrying as before. The request is
  treated as sent as soon as `write()` hands it to the socket buffer, before
  `drain()` completes, so a timeout during backpressure is not mistaken for
  "not sent" (issues #225, #412).
- `ClientStats` has a fourth field, `transient_retries`.

### Fixed

- `set()`, `set_many()` and the compare-and-set writers reject a
  `ttl_seconds` above 2**64 - 1 with `ValueError` before any I/O. Such a
  value made the server drop the frame without replying, which closed the
  shared connection and failed every other in-flight request on it.
- A malformed `I` (increment) reply body is now reported as
  `NanocachedError` and poisons the connection, instead of escaping as a
  bare `ValueError`. Every integer field read off the wire (lengths,
  counts, TTLs, tags, discovery counts, ports) is now parsed digits-only;
  a leading `+`, whitespace or an underscore poisons the connection
  (issue #462).
- Batch replies whose entry count differs from the request raise a desync
  `ConnectionError` instead of silently shifting later values onto the
  wrong keys (issue #181). The cumulative size of one multi-get reply is
  bounded at 64 MiB (issue #207), and, with `compress=True`, the cumulative
  decompressed size of one `get_many()` response at 256 MiB. The decompression
  budget is charged before it is checked, so the entry that crosses the cap
  is the one rejected, and it is not applied to clients without compression
  (issue #410).
- Hedged reads: a replica hit now beats the primary's miss when both land in
  the same event-loop tick, and a `WrongNodeError` leg no longer shadows a
  simultaneous hit, so hedging cannot turn a hit into a miss (issue #387).
  Losing legs that finish in the same tick as the winner are no longer
  leaked (issue #229); legs are no longer leaked when the caller cancels the
  read, for example with `asyncio.wait_for` (issue #324), including when
  the cancellation lands while joining an over-cap batch of losing legs
  (issue #364); a leg registered
  while `close()` is running is no longer missed by its drain (issue #91);
  and the number of detached losing legs is capped at 32, with further legs
  awaited inline (issue #192).
- `close()`: also awaits the keep-alive task and the connections' read
  tasks, so closing right before the event loop shuts down no longer logs
  "Task was destroyed but it is pending!". Replica legs left running by a
  cancelled `set()` are now handed to the drained background pool instead
  of being orphaned (issues #189, #412).
- Newly discovered nodes are dialed concurrently during a node-list refresh,
  so several nodes joining at once, one of them slow or unreachable, no
  longer stalls every call for up to N dial timeouts (issue #190).
- Reconnect-cooldown entries for addresses that have left the cluster are
  purged on refresh, and re-raising a stored cooldown error no longer grows
  its traceback on every hit (issue #96).
- `HashRing` keeps only the first occurrence of a duplicated node name, and
  the initial connect no longer dials or installs a duplicated name twice,
  which had inflated that node's share of the ring (issues #360, #461).
- `connect()`: a discovery roster entry that is not a cache node no longer
  raises an `AttributeError` that masked the intended error and leaked
  sockets.

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. Server-side, this release
fixes the cluster data-loss and availability bugs found in the v0.2.0
end-to-end run (issues #61-#63, #66) and changes the discovery heartbeat
acknowledgment so nodes learn of evictions: upgrade nodes before discovery
servers.

### Added

- Hedged reads: `connect(..., read_hedge_after=seconds)` sends a read to the
  next owner as well once the primary has been silent for that long (and,
  if needed, to the owner after it once another interval passes), taking
  the first answer. A hit from any owner is final; a replica's miss is
  only provisional and the primary's answer still decides, so hedging never
  turns a hit into a miss. A `WrongNodeError` propagates as before. Off by
  default; needs a replication factor of at least 2; a value that is not a
  positive number raises `ValueError`. The losing leg is never cancelled: it
  finishes detached and is drained by `close()`, like a fire-and-forget
  replica write (issue #64).

### Changed

- The message of `DiscoveryBusyError` no longer claims the replica is only
  "warming up after a restart": `B` is also what a replica answers when its
  replication factor disagrees with the cluster's (issue #68).

### Fixed

- `connect()` no longer fails just because one of the nodes discovery lists
  cannot be reached yet, typically one that died and has not yet left
  discovery's liveness window. All listed nodes are dialed concurrently; an
  unreachable one is installed without a connection and with its reconnect
  cooldown armed, so requests for its keys fail over per request. Only a
  cluster with no reachable node still fails `connect()`, with the last dial
  error. A discovery entry that is not a cache node is still a hard error,
  and every socket a bootstrap round opened is now closed before it fails
  (issue #67).
- `close()` no longer spins forever, freezing the whole event loop, when
  every background replica write had already finished but its removal
  callback had not yet run. This was easy to hit with
  `fire_and_forget_replicas=True` (issue #65).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version. 0.1.x
were pipeline-validation releases; as a pre-1.0 line, breaking changes ship
without a deprecation cycle and are listed below.

### Added

- `ClientStats` (exported) and `client.stats()`: monotonic counters for
  failures the client swallows by design, `replica_write_failures`,
  `read_repair_failures` and `refresh_failures`, so a silently degrading
  replication or a stuck node-list refresh is observable.
- `connect(..., reconnect_cooldown=1.0)`: how many seconds a node whose dial
  failed stays "down" before the next redial attempt, so a dead node is not
  redialed on every request. Applies per address.
- `AuthenticationError` (a `NanocachedError`): raised when the server
  rejects the handshake secret, either because none was configured or
  because it is wrong. It is never transient, so it can be told apart from a
  genuine protocol error. Handshake failures were previously a plain
  `NanocachedError`.
- Echoed response tags: the handshake negotiates a tag that every request
  carries and the server echoes, and the client verifies the echo before
  handing a response out, so a misaligned pipeline can no longer deliver
  one request's response to another caller. The connection is closed on a
  mismatch. Falls back transparently to the untagged form against an older
  server.
- Progress-based request timeout: a connection that makes no progress for
  30 seconds while requests are outstanding is closed and redialed, so a
  half-open server can no longer hang `get`/`set`/`delete` forever. A
  request queued behind others is not timed out while the server is still
  answering (issue #42).

### Changed

- **Breaking: `NanocachedClient.close()` is now a coroutine and must be
  awaited.** It returns only after every in-flight background replica write
  has finished, so a process that closes and exits no longer drops replica
  writes still being sent. It also waits for an in-flight node-list refresh
  or redial, which the other SDKs' `close()` does not (issue #47).
- **Breaking: `HashRing.route()` on an empty ring raises `ValueError`**
  instead of a bare `IndexError`.
- Read repair no longer re-probes the primary on a clean miss; it probes
  only the remaining owners. The repair write-back now carries a fixed
  60-second TTL instead of none, so it cannot resurrect already-expired data
  permanently, and it shares the in-flight budget with fire-and-forget
  replica writes. Past that cap, the repair for that miss is skipped.
  Only a failed write-back is counted in `read_repair_failures` (issue #43).
- Errors swallowed by design (replica writes, read repair, refresh) are now
  limited to `NanocachedError`, `ConnectionError` and `OSError`; a
  programming error inside a replica leg is no longer hidden. If the primary
  write itself fails, a replica leg's unexpected error is raised in its
  place, and further ones are warned.
- Discovery responses are validated strictly: non-numeric or truncated
  headers and entries, undecodable entries and an out-of-range port raise
  `NanocachedError` instead of a raw `ValueError`, `EOFError` or
  `UnicodeDecodeError`, and a node-list response larger than 16 MiB is
  rejected.

### Fixed

- `get`, `set`, `delete` and the other keyed operations reject an empty key,
  or a request over the server's size limit, with `ValueError` before
  anything is written. The server drops such a frame without replying, which
  closed the shared connection and failed every other request pipelined on
  it.
- A reply line longer than the stream's buffer limit no longer kills the
  read loop silently and leaves every pending request hanging; the
  connection is poisoned and the request fails (issue #8). A response with
  an unexpected byte after its marker poisons the connection instead of
  desyncing it.
- A redial that is still running when its awaiting caller is cancelled is no
  longer started a second time by the next caller.
- `nanocached.__version__` now matches the package version; the 0.1.1
  release reported `"1.0.0"`.
- The keep-alive ping uses a dedicated key (`"\x00nanocached-keepalive"`)
  instead of a single NUL, so it no longer touches the LRU recency of an
  application key that happens to be `"\x00"`.

## [0.1.1] - 2026-08-20

- First tag-driven release of the aligned SDK line (see the repository
  release history for earlier changes).

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.5...HEAD
[0.4.8]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.7...sdk/python/v0.4.8
[0.4.7]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.6...sdk/python/v0.4.7
[0.4.6]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.5...sdk/python/v0.4.6
[0.4.5]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.4...sdk/python/v0.4.5
[0.4.4]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.2...sdk/python/v0.4.4
[0.4.2]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.1...sdk/python/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.4.0...sdk/python/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.3.0...sdk/python/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.2.0...sdk/python/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/python/v0.1.1...sdk/python/v0.2.0
[0.1.1]: https://github.com/nanocached/nanocached/releases/tag/sdk/python/v0.1.1
