# Changelog

All notable changes to the .NET SDK (NuGet package `Nanocached`) are
documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
the `sdk/dotnet/vX.Y.Z` tags. From 0.4.4 on, every release ships the
server, the six SDKs and the seven framework adapters at one version,
whether or not a component changed. There is no `sdk/dotnet/v0.4.3` tag:
0.4.3 was a server-only release.

## [Unreleased]

## [0.5.1] - 2026-10-27

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.5.0] - 2026-10-26

### Fixed

- A touch with a TTL of 0 (`TouchAsync`, `GetAndTouchAsync`,
  `TouchManyAsync`, `GetManyAndTouchAsync`) no longer sends the plain touch
  to a key's replicas alongside the primary's. When the primary could not be
  reached or answered `W`, the retry over the refreshed roster went to the
  surviving replica, which had already expired its copy: it answered a miss
  for a key that existed, and the value was lost. The replicas are now
  touched only after the primary answered (for a batch: one fan-out after
  both primary-only passes, for the keys a primary answered), so a rerouted
  call finds the entry. A positive or removed TTL keeps the concurrent
  fan-out. The replica legs of a TTL-0 touch now run after the primary
  answered, outside the pipelined-order guarantee, as the retry paths do.
- Only a node's `En` to a touch is reported as `ServerTooOldException`.
  Any other `E` answer to `TouchAsync`, `GetAndTouchAsync`, `TouchManyAsync`
  or `GetManyAndTouchAsync` (a bare `E`, or the tagged `E <tag>`
  nanocached-proxy answers when an expiring touch's outcome is unknown) is now
  a `ConnectionLostException`, as for every other command: a TTL-0 touch is
  not resent after it, and it no longer tells the caller to upgrade every node.
- The documentation of `TouchManyAsync`, `GetManyAndTouchAsync` and
  `PartialConnectionLostException<T>` now says that with a TTL of 0 the keys
  a connection failure leaves unresolved include ones whose frame was sent
  and whose outcome is unknown: the node may already have expired them, so
  they must not be sent again (a second touch finds them gone). No
  behaviour change.
- A failed redial of a dead node is now marked request-not-sent, as the
  first connect already did. `TouchAsync(key, 0)`, `GetAndTouchAsync(key, 0)`,
  `TouchManyAsync(keys, 0)`, `GetManyAndTouchAsync(keys, 0)`, `IncrAsync`
  and the compare-and-set family used to throw `ConnectionLostException`
  ("could not connect") when the key's primary was dead and already dropped
  from the roster, instead of refreshing the roster and rerouting to the
  surviving owner as a TTL of 30 or a removed TTL already did.
- In a cluster, `GetManyAndTouchAsync` no longer discards the hits other
  primaries returned when one primary answers `En` to `y`. It throws
  `PartialServerTooOldException<T>`, a `ServerTooOldException` subclass, with
  them in `PartialValues`; with a TTL of 0 they were consumed on the nodes
  and could not be read again. `ServerTooOldException` is no longer
  `sealed`. `TouchManyAsync` still throws the plain exception.

### Added

- `TouchAsync(key, ttlSeconds)` replaces an entry's TTL in place and
  returns whether the key existed; `GetAndTouchAsync(key, ttlSeconds)` and
  `GetAndTouchBytesAsync` read the value and replace the TTL in one step.
  All three are on `NanocachedNamespace` too. `ttlSeconds` is a required
  `long?`: `null` removes the TTL and `0` expires the entry at once (the
  opposite of `SetAsync`, where `0` means no expiry). They are idempotent,
  so unlike `IncrAsync` and the compare-and-set family a lost reply is
  retried; they go to every owner (`GetAndTouchAsync` asks only the primary
  for the value). Against a node that predates the command they throw the
  new `ServerTooOldException`. Needs a server release that includes the
  `t` command.
- `TouchManyAsync(keys, ttlSeconds)` and `GetManyAndTouchAsync(keys,
  ttlSeconds)` (with `GetManyAndTouchBytesAsync`, and all three on
  `NanocachedNamespace`) are `TouchAsync`/`GetAndTouchAsync` for a list of
  keys, in one round trip per involved node. `TouchManyAsync` returns a
  dictionary of key to whether it existed; `GetManyAndTouchAsync` returns
  the hits, as `GetManyAsync` does. One TTL for the whole batch, with the
  meaning and no default of `TouchAsync`. Every owner is sent the batch;
  only a key's primary is asked for the value, so a node that is primary for
  some keys and a replica for others receives two frames.
  `GetManyAndTouchAsync` fails like `GetManyAsync` (partial results
  attached); `TouchManyAsync` throws a plain `WrongNodeException` or the
  connection failure. Needs a server release that includes the `y` command;
  against a node that has `t` but not `y` they throw
  `ServerTooOldException`, whose new `Command` is `"y"`.

## [0.4.8] - 2026-10-21

### Fixed

- The request-timeout callback decides under the deadline gate whether
  the deadline is still armed before closing the connection. Read
  outside it, the callback could see a deadline a response being
  dispatched at that moment was about to clear, and close a connection
  that had just answered.
- `ConnectAsync` throws `ConnectionLostException` (the
  `SocketException`/`IOException` as `InnerException`) when no address
  could be reached, instead of the raw exception of the last address
  tried, so `catch (NanocachedException)` covers it as it already covered
  a failed redial; a rejected TLS certificate stays an
  `AuthenticationException`. A node at its
  connection limit, which answers `B` and closes, is reported as busy
  instead of as a connection closed during the handshake.

- The error for a namespace, key or value over the request limit names
  the limit ("the server accepts requests of up to 1 MiB") instead of a
  server source constant and file (#734).
- A `string` key, namespace or value that is not well-formed UTF-16 (a
  lone surrogate) now throws `ArgumentException`. Before,
  `Encoding.UTF8.GetBytes` silently encoded the lone surrogate as U+FFFD,
  so `"x\uD800"` and `"x\uFFFD"` were the same cache entry (#731).
- `GetManyAsync`/`GetManyBytesAsync` fetch a repeated key once. A list of
  many copies of one key used to be sent as one `m` frame per 400 copies
  with every frame's reply retained until the call returned, so about
  60 KB of request could hold hundreds of MB of replies (#698).
- `SetManyAsync`/`SetManyBytesAsync` in cluster mode retry only the legs
  that failed: a key whose primary leg failed used to be re-sent to every
  owner after the refresh, including a replica that had already stored
  it (#720). And a key whose primary could not be reached at all (no `W`
  was ever received) now throws
  `PartialConnectionLostException<HashSet<string>>`, with the keys whose
  primary acknowledged in `PartialValues`, instead of `WrongNodeException`
  (#722).
- A cluster `GetManyAsync`/`GetManyBytesAsync` whose unresolved keys had
  no reachable owner says "could not reach any owner of N of M keys"
  instead of describing a chunked batch interrupted partway, which was
  not what happened (#671).
- Forced node-list refreshes are single-flight: callers that hit a dead
  node at the same time used to run one full refresh each, one after
  another behind the refresh gate; a forced caller now reuses a refresh
  that finished while it waited (#721).
- Concurrent decompressions are bounded by a client-wide budget (256 MiB
  of in-flight DEFLATE output; each decompression reserves the 64 MiB
  per-value cap while it runs), so N concurrent reads of a value that
  inflates to the cap no longer hold N × 64 MiB at once (#712).
- A request that timed out is no longer redialed and resent once: the
  peer was reachable and simply did not answer, so the resend waited out
  a second full deadline — a cluster `SetAsync` whose replica had stopped
  reading took 60 s instead of 30 s (#725).
  `ConnectionLostException.TimedOut` tells such a failure apart.

## [0.4.7] - 2026-10-19

### Fixed

- A request that fails inside the connection's read loop (a desynced or
  over-large reply, a reset mid-response) now sees the connection marked
  closed before its own failure is delivered. The failure used to be
  delivered first, so the client's redial step could run before `Close()`
  and reuse the dying connection, whose drain then rejected the retry with
  a generic "connection closed" instead of the read's own reason (#633).
- `GetManyAsync`/`GetManyBytesAsync` in cluster mode now read a key from
  its next owner when its primary cannot be reached at the connection
  level, the same walk a single `GetAsync` already did (#623, the .NET
  part of #616). Before, the batch sent every key to its primary only,
  retried the same primary once after a refresh, and then threw
  `PartialWrongNodeException<T>` for keys that nothing had answered `W`
  for — seen in the field as a client that could not verify one node's
  rotated TLS certificate failing a bulk read although the other owner
  held every key. Keys whose every owner is unreachable now throw
  `PartialConnectionLostException<T>` (or the bare connection error when
  nothing resolved) instead of a wrong-node error.

## [0.4.6] - 2026-10-17

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.5] - 2026-10-15

### Fixed

- A TLS certificate or handshake failure (`AuthenticationException`) on a
  node dial is now treated like any other unreachable node, as in the other
  SDKs. Before, it escaped the dial paths, so one new node with a bad
  certificate faulted a node-list refresh (breaking its "never throws"
  contract and orphaning the connections its sibling dials had opened) and
  aborted `ConnectAsync` for the whole cluster; and a bad-certificate first
  address ended `ConnectAsync` instead of trying the next one. A bad-certificate
  node is now installed without a connection and fails over per request, and a
  connect to a single address still reports the `AuthenticationException`.
- Keep-alive pings now run concurrently, one per idle connection, instead of
  one after another. A single half-open node held its ping for the 30 s request
  timeout and delayed the pings to every other node until about 60 s idle,
  which is the server's idle limit (issue #192).
- Dialing a discovered roster is now bounded to 64 nodes at a time, both at
  connect and on a node-list refresh. A roster of up to 65536 nodes used to be
  dialed all at once.

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project: from 0.4.4 on,
every release ships the server, the six SDKs and the seven framework
adapters at one version, whether or not a component changed.

## [0.4.2] - 2026-09-07

### Fixed

- **Breaking:** `ConnectAsync` now throws `ArgumentException` for `ReadHedgeAfter`
  combined with `ViaProxy = true`, and for `Ca` combined with
  `Tls = false`. Both combinations were previously accepted and silently
  ignored (a proxy connection has no replicas to hedge to; a CA file is
  only read over TLS, so `Ca` without `Tls` connected in plaintext).
  Callers that set both must now unset one (issue #488).
- `IncrAsync`/`DecrAsync`, the compare-and-set methods and
  `DeleteIfMatchesAsync` are replayed after a redial only when the SDK can
  prove no complete request frame reached the server. Previously any
  failure from the write or flush was treated as "not sent", so a
  read-loop FIN, the request-timeout watchdog or an explicit `Close()`
  disposing the stream under an in-flight write could replay a request
  the server had already applied (a double increment, or a successful CAS
  reported as a mismatch). A `FlushAsync` failure after the write
  completed, or any failure once the connection is already closed, now
  surfaces as `ConnectionLostException` (issue #484).
- Cluster-mode `GetManyAsync`/`GetManyBytesAsync` keep the chunks an owner
  already answered when a later chunk of the same owner's group fails at
  the connection level, and retry only the unresolved tail. Previously the
  completed chunks were discarded and the whole group was resent, and a
  persisting failure's `PartialWrongNodeException<T>` carried none of
  them (issue #485).
- Arming a reconnect cooldown now also drops every entry whose window has
  passed, so in proxy mode (where no node-list refresh ever prunes the
  table) the cooldown map stays bounded (issue #486).

## [0.4.1] - 2026-09-05

### Fixed

- Documentation only (issue #478): the README and the class-level doc
  comments no longer claim every operation is idempotent
  (`IncrAsync`/`DecrAsync`/`ReplaceAsync`/`DeleteIfMatchesAsync` are not,
  see issue #225), and the read-repair description no longer calls the
  write-back untracked/undrained: it shares the fire-and-forget replica
  budget and `Close()` drains it. No behaviour change.

## [0.4.0] - 2026-09-05

### Added

- Namespaces (issue #105): `client.Namespace(string)` /
  `client.Namespace(byte[])` return a lightweight `NanocachedNamespace`
  handle. The same key under two namespaces (or under no namespace) is a
  wholly independent entry. The handle exposes the same operations as
  `NanocachedClient` with identical semantics (routing, replication,
  hedged reads, `W` refresh-and-retry, compression), shares the client's
  connections, and throws `AlreadyClosedException` once the client is
  closed. `Namespace("")` is equivalent to the client itself and sends
  the legacy frames unchanged, so existing code and connections to a
  pre-namespace server are unaffected. Non-empty namespaces need a
  namespace-aware server. `HashRing` gained `Owners(namespaceBytes, key,
  replicas)` and `Route(namespaceBytes, key)` overloads; the existing
  overloads route un-namespaced keys exactly as before.
- Namespace clear / flush-everything (issue #106):
  `NanocachedNamespace.ClearAsync()` drops every entry in one namespace
  (`Namespace("").ClearAsync()` clears the default namespace);
  `NanocachedClient.ClearAllAsync()` flushes every namespace. Both fan out
  to every node the client knows about and succeed only once every node
  has acked, never a partial clear. On any failure the node list is
  refreshed once and the whole fan-out is retried; a node still failing
  fails the call with `ConnectionLostException` naming it. Both are
  idempotent, so callers can simply retry.
- SDK proxy mode (issue #122): `Options.ViaProxy = true` connects through a
  `nanocached-proxy` tier instead of joining the cluster. `Addresses`
  must name discovery server(s); `ConnectAsync` fetches the proxy roster
  and connects to one proxy chosen at random, failing over through the
  rest on a failed dial. The client then runs in its single-connection
  mode (no ring, no per-node connections). If the connection is lost the
  same proxy is redialed first, and only if that fails is the roster
  re-fetched and another proxy chosen. Pointing `ViaProxy` at an address
  that identifies as a cache node fails `ConnectAsync` fast. Off by
  default.
- Retryable-error status `R` (issue #125): the connect handshake now
  declares that this client understands a transient-failure reply, with a
  transparent fallback for older servers. When a request is answered `R`
  (today only `nanocached-proxy` sends it) it is retried on the same
  connection up to twice more (three attempts, 50 ms then 100 ms apart);
  if the third still answers `R` the call throws the new
  `RetryableException`. The connection is never closed or redialed. Every
  `R` received is counted in the new `Stats().TransientRetries` counter.
- `IncrAsync`/`DecrAsync` (issue #129): atomically add a signed `long`
  delta to an integer counter and return the new value, or `null` for a
  missing or expired key (the same miss convention as `GetAsync`). A
  stored value that is not a plain signed decimal integer, or a delta that
  would overflow `long`, throws the new `NotNumericException`. Also on
  `NanocachedNamespace`. In cluster mode only the key's primary runs the
  increment and the resulting value is forwarded to the replicas as an
  ordinary set, so replicas cannot drift. A counter is exactly as
  volatile as any other entry (LRU eviction, TTL expiry), so it suits rate
  limiting and approximate counts, not durable ones. `DecrAsync` rejects
  `long.MinValue` with `ArgumentOutOfRangeException` instead of silently
  wrapping it into an increment (issue #182). Both throw the new
  `CompressionIncompatibleException` before any I/O on a client created
  with `Compress = true`, because an incremented value cannot round-trip
  through compression (issue #321).
- Compare-and-set (issue #141): `GetWithTokenAsync`/`GetBytesWithTokenAsync`
  return the value together with a content-digest token;
  `PutIfAbsentAsync`, `ReplaceIfPresentAsync`, `ReplaceAsync(key, token,
  newValue)` and `DeleteIfMatchesAsync(key, token)` return `true` when the
  condition held and `false` otherwise (a mismatch is not an exception).
  `NanocachedClient.ContentDigest(byte[])` computes a token locally. The
  digest is taken over the exact stored wire bytes, so it stays correct
  with `Compress` enabled. Only the primary evaluates the condition; on
  success the result is forwarded to the replicas as an ordinary set or
  delete. A malformed token (anything other than 32 lowercase hex
  characters) is rejected with `ArgumentException` before any frame is
  built, so a token taken from external input cannot smuggle a second
  request onto the connection (issue #223). These are not a distributed
  lock: LRU eviction can reclaim the key and let a second caller win.
- Multi-get / multi-set (issue #151): `GetManyAsync`/`GetManyBytesAsync`
  (returning a dictionary of the keys that hit) and
  `SetManyAsync`/`SetManyBytesAsync`, also on `NanocachedNamespace`. Keys
  are grouped per owner and sent as one frame per owner, split
  transparently into sub-frames of at most 400 keys and at most the
  server's request-size limit. Hedged reads and read repair do not apply
  to batches. A batch never fails as a whole: when some keys are still
  wrong-node after one refresh-and-retry, `GetManyAsync` throws
  `PartialWrongNodeException<T>` (a `WrongNodeException` subclass) whose
  `PartialValues` holds every key that resolved; `SetManyAsync` throws a
  plain `WrongNodeException`.
- `PartialConnectionLostException<T>` (a `ConnectionLostException`
  subclass): when a connection failure interrupts a chunked batch after an
  earlier sub-frame already succeeded and the built-in reconnect-and-retry
  also fails, `PartialValues` carries what was confirmed (the resolved
  entries for a multi-get, the `HashSet<string>` of stored keys for a
  multi-set) instead of implying nothing happened. A failure on the very
  first sub-frame still throws a bare `ConnectionLostException`.

### Changed

- **Breaking: `NanocachedClient.ClientStats` gained a fourth positional
  member, `TransientRetries`.** Code that constructs or deconstructs the
  record positionally must be updated; reading the existing properties is
  unaffected.
- `WrongNodeException` and `ConnectionLostException` are no longer
  `sealed`, so the partial-result exceptions above can subclass them.
  Existing `catch` blocks keep working unchanged.

### Fixed

- Every integer field read off the wire (value and counter lengths,
  counts, TTLs, tags, discovery node counts, ports) is parsed digits-only
  with the invariant culture. A leading `+`, surrounding whitespace or
  culture-dependent forms such as `V +5` or `V  5` are no longer accepted
  and poison the connection; the counter body keeps its one documented
  exception, a single leading `-` (issue #462).
- A duplicated node name in a discovery roster no longer corrupts the
  client. `HashRing` now deduplicates (first occurrence wins) so a repeated
  name cannot inflate its share of the ring; `ConnectAsync` no longer
  dials the duplicate twice and leaks the first connection; and the
  node-list refresh no longer throws `ArgumentException` from every
  get/set/delete that triggers it (issue #461).
- A multi-get reply is bounded: its cumulative size is capped at 64 MiB,
  checked before each entry's body is read (issue #207), and cumulative
  decompression across one reply is capped at 256 MiB, counting the entry
  that crosses the cap and applied only when `Compress` is on (issues
  #386, #410). Previously a hostile or compromised node could force up to
  batch size times the per-value limit of allocation from one reply.
- A full 400-key multi-get reply, whose `M` header line can reach about
  3200 bytes, no longer trips the 1024-byte header limit and poisons the
  connection; the limit is now 4096 bytes (issue #273).
- Multi-set and multi-get sub-frames are now bounded by cumulative bytes,
  not only by key count. A batch of individually valid pairs (for example
  400 keys with 5 KiB values) could previously build a frame over the
  server's 1 MiB request limit, which the server answers by silently
  closing the connection (issue #222).
- A connection lost after a request frame was fully written no longer
  replays `IncrAsync`/`DecrAsync`, compare-and-set or `DeleteIfMatchesAsync`
  (a double increment, or a successful CAS reported as a mismatch); it
  throws `ConnectionLostException` instead. The redial-and-retry applies
  to these only when the request was provably not sent (issue #225).
- A node whose dial fails on a node-list refresh stays in the ring as a
  member without a live connection, with its reconnect cooldown armed,
  matching initial connect. Previously the node was dropped, so this
  client's primary/replica choice for nearby keys disagreed with every
  peer that did reach it until the next refresh. Newly discovered nodes
  are also dialed concurrently, not one after another under the refresh
  gate.
- Hedged reads: losing hedge legs are bounded at 32 in flight. Beyond
  that the remaining losers are awaited synchronously, so a persistently
  slow owner can no longer accumulate one detached leg per read (issue
  #276). Hedge-leg registration is rechecked against a concurrent
  `Close()`, which could otherwise register a leg after the drain had
  finished and dial a connection teardown was closing (issue #91).
- Reconnect-cooldown entries for departed addresses are purged on
  node-list refresh, and in proxy mode when the client fails over to a
  different proxy, so address churn (containers, pods) no longer leaks one
  entry per departed address for the life of the process (issues #96,
  #296).
- A redial that completes after `Close()` no longer installs its fresh
  connection and read loop into a closed client. It is discarded and the
  call throws `AlreadyClosedException` (issue #330).

## [0.3.1] - 2026-08-22

.NET-only patch release; the server and the other SDKs stay at 0.3.0.

### Fixed

- `Close()`/`Dispose()` no longer throws `InvalidOperationException` when a
  hedged-read leg finishes while the hedge set is being drained (issue
  #89). It showed up on three of nine CI runs of the v0.3.0 tag.

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. Server-side, this
release fixes the cluster data-loss and availability bugs found in the
v0.2.0 end-to-end run (issues #61-#63, #66) and changes the discovery
heartbeat acknowledgment so nodes learn of evictions: upgrade nodes
before discovery servers.

### Added

- Hedged reads: `Options.ReadHedgeAfter` (`TimeSpan?`, off by default)
  sends a read to the next owner as well once the primary has been silent
  for that long, and to each further owner after another interval, taking
  the first answer. A hit from any owner is final; a miss is final only
  from the primary, since a replica's miss is provisional, so hedging never
  turns a hit into a miss. A connection-level failure hedges onward
  immediately, and `WrongNodeException` propagates exactly as on the
  non-hedged path. Needs replication of 2 or more; a non-positive value is
  rejected with `ArgumentOutOfRangeException`. Losing legs are never
  cancelled: they finish detached and are drained by `Close()`, like
  fire-and-forget replica writes. Writes are unaffected (issue #64).

### Changed

- `DiscoveryBusyException`'s message no longer claims the replica is
  "warming up after a restart"; `B` is also what a replica whose
  replication factor disagrees with the cluster's answers (issue #68).

### Fixed

- `ConnectAsync` no longer fails outright just because one of the nodes
  discovery lists can't be reached yet, typically one that just died and
  hasn't been evicted from discovery's liveness window (a few seconds).
  Every listed node is now dialed concurrently; one that can't be reached
  is installed as a member without a live connection and with its
  reconnect cooldown armed, exactly the state a member is in after dying
  mid-life, so requests for its keys fail over per request. Only a cluster
  with no reachable node at all still fails, with the last dial error. A
  node that answers but no longer identifies as a cache node remains a
  hard error (issue #67).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version.
0.1.x were pipeline-validation releases; as a pre-1.0 line, breaking
changes ship without a deprecation cycle and are listed below.

### Added

- `AuthenticationFailedException` (a `NanocachedException` subclass),
  raised when the server rejects the handshake secret, whether none was
  configured for a server that requires one or the secret is wrong. It lets
  callers tell a non-transient credentials problem apart from other
  failures without matching the message text.
- `Options.ReconnectCooldown` (default 1 s) and
  `Options.DisableReconnectCooldown`. After a reconnect dial to an address
  fails, calls routed to it fail immediately with the original error for
  the cooldown window instead of each paying a full connect timeout.
  `TimeSpan.Zero` means "use the default", not "disable"; use
  `DisableReconnectCooldown = true` to disable. A negative value is
  rejected with `ArgumentOutOfRangeException`.
- `NanocachedClient.Stats()` returning `ClientStats` with
  `ReplicaWriteFailures`, `ReadRepairFailures` and `RefreshFailures`:
  monotonic counters for the failures the client swallows by design, so a
  silently degrading replica or a stuck node-list refresh is detectable
  without watching for exceptions.
- Echoed response tags: the connect handshake negotiates a per-request tag
  that the server echoes, and the read loop verifies it before handing a
  response to its caller. A desynced stream is now poisoned before any
  misaligned response can resolve a queued request with a plausible wrong
  value. A pre-tag server is detected from the connection closing before
  any reply, and the client redials once in the old untagged form.
- A progress-based in-flight request timeout (30 s), armed when requests
  are outstanding and re-armed on every response. A half-open server that
  accepts the TCP connection but never answers no longer hangs
  `GetAsync`/`SetAsync`/`DeleteAsync` and everything pipelined behind them
  forever; the connection is poisoned with `ConnectionLostException`, and
  the existing redial/retry layer takes over. An idle connection is never
  closed by it (issue #42).

### Changed

- `Options.AuthSecret = ""` now means no secret, matching the other SDKs.
  Previously an explicit zero-length secret reached the wire, which the
  server rejects and closes without replying, surfacing as an opaque
  `ConnectionLostException`.
- Empty keys and requests over the server's 1 MiB request limit are
  rejected client-side with `ArgumentException`/`ArgumentOutOfRangeException`
  before any I/O. Previously the server silently closed the connection,
  which also failed every other request pipelined on it.
- A negative `CompressionThreshold` is rejected by `ConnectAsync`.
- Read repair no longer re-probes the primary on a clean miss (it already
  missed there on the normal read path) and writes the repaired value back
  with a 60-second TTL instead of none, so a repaired key can no longer
  resurrect expired data permanently.
- `HashRing.Owners` selects the top `replicas` nodes with a bounded
  insertion instead of sorting every node, which matters because it runs
  per key under the routing lock. Output is unchanged.
- `HashRing.Route` on an empty ring now throws `InvalidOperationException`
  instead of an index-out-of-range error.
- A `Close()` that races a read no longer aborts the owner walk early; the
  walk continues as in the other SDKs, and the caller still sees
  `AlreadyClosedException` (issue #47).
- The keep-alive ping key is now the reserved 21-byte `0x00` +
  `"nanocached-keepalive"` instead of the single byte `{0x00}`, which was a
  valid application key whose LRU recency every tick silently refreshed.

### Fixed

- A custom `Ca` no longer causes TLS hostname mismatches to be ignored; the
  certificate must now match the host being dialed.
- A compressed value can no longer expand without bound: decompressed
  output is capped at 64 MiB per value and surfaces as
  `DecompressionException` (issue #41).
- `Close()` no longer loses in-flight background replica writes to a race:
  a write that passed its closed check could register after the drain's
  snapshot and have its connection torn down under it. `Close()` now waits
  for every replica-write permit. Two concurrent `Close()` calls no longer
  both run the teardown.
- When the request-timeout watchdog fired, the stalled caller could see a
  raw `ObjectDisposedException` and escape the retry layer. It now always
  sees `ConnectionLostException`.
- A server FIN racing an in-flight request now surfaces as
  `ConnectionLostException` rather than a raw `EndOfStreamException`.
- The read loop could strand a request forever when its emptiness check and
  dequeue raced; it now dequeues atomically. A busy marker arriving with a
  request pending, or a trailing byte other than `\n` on an untagged fixed
  response, now poisons the connection instead of silently desyncing it.
- A failure on a replica leg of a write can no longer replace the outcome
  of the primary leg.
- Discovery responses are bounded: header and handshake lines are capped,
  node-list responses at 65536 nodes and an aggregate 16 MiB, and an
  out-of-range port is rejected as `NanocachedException` rather than
  surfacing later as a raw `ArgumentOutOfRangeException` from the socket
  layer.
- Replica writes and read-repair write-backs share one bounded pool of 32
  background permits, and read-repair write-backs are now drained by
  `Close()`. Failed TLS handshakes dispose their streams, and exceptions
  from fire-and-forget tasks are observed and counted instead of vanishing.
- The by-design swallow sites for dead replicas and failed repairs now catch
  only the SDK's own exceptions and I/O errors, so a genuine programming
  error propagates instead of being counted as a dead replica.

### Performance

- Response header lines are read through a read-only buffering stream
  instead of one 1-byte `ReadExactlyAsync` per byte.

## [0.1.1] - 2026-08-20

- First tag-driven release of the aligned SDK line. 0.1.x were
  pipeline-validation releases (see the repository release history for
  earlier changes).

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.5...HEAD
[0.5.1]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.5.0...sdk/dotnet/v0.5.1
[0.5.0]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.8...sdk/dotnet/v0.5.0
[0.4.8]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.7...sdk/dotnet/v0.4.8
[0.4.7]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.6...sdk/dotnet/v0.4.7
[0.4.6]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.5...sdk/dotnet/v0.4.6
[0.4.5]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.4...sdk/dotnet/v0.4.5
[0.4.4]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.2...sdk/dotnet/v0.4.4
[0.4.2]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.1...sdk/dotnet/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.4.0...sdk/dotnet/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.3.1...sdk/dotnet/v0.4.0
[0.3.1]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.3.0...sdk/dotnet/v0.3.1
[0.3.0]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.2.0...sdk/dotnet/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/compare/sdk/dotnet/v0.1.1...sdk/dotnet/v0.2.0
[0.1.1]: https://github.com/nanocached/nanocached/releases/tag/sdk/dotnet/v0.1.1
