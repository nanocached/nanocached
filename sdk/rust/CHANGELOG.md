# Changelog

All notable changes to the Rust SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/rust/vX.Y.Z` tags.

## [Unreleased]

### Changed

- Five doc comments on public methods (`read_hedge_after`, `touch_many`, `get_many_bytes`, `set_many_bytes`) linked to private items and rendered as broken links on docs.rs; they now state the values (24 hours, 400 keys per sub-frame) in plain text. The crate's CI builds the docs with `-D warnings` so this cannot recur. (#1455)

- `Error::ConnectionLostAfterSend` and `Error::ConnectionLostTimedOut` are `#[doc(hidden)]` and documented as not part of the API: they are how the client passes "the frame was written" and "the deadline expired" between its own layers, and no public method returns them (every path folds them into `ConnectionLost`; a test now covers `set`, `incr` and `delete`). They may be removed or renamed in any release. `Error::Decompression` no longer depends on the `compression` feature, so the set of variants is the same whichever features the build enables.
- **Breaking:** `HashRing` and `DiscoveredNode` are no longer public, and neither are `Options::keep_alive_interval` and `Options::request_timeout` (the keep-alive interval and the 30 s request timeout stay as they were, just not settable) or the `#[doc(hidden)]` statics and `NanocachedClient` methods that existed for this crate's own tests. No public method took or returned `HashRing` or `DiscoveredNode`. The tests reach them through a new `__test_hooks` Cargo feature (enabled by the crate's own dev-dependency); it is not an API and nothing else should enable it.
- **Breaking:** `Error` and `Stats` are now `#[non_exhaustive]`. A later release can add a variant or a counter without a new major version, so a `match` on `Error` needs a wildcard arm and a `Stats` is read by field, not built with a struct literal.

### Fixed

- A node's `En` to one request failed every request pipelined behind it on the same connection with the same non-retryable error (a `touch` got `ServerTooOld`, a `get`/`set` sharing the connection `Error::Protocol`), and the proxy's `E` (outcome unknown, #939) to an `incr` was an `Error::Protocol` for it and for every request behind it. As in the Go SDK, an `En` now goes only to the request it answers and the others get `ConnectionLost`, so they are redialed and retried; any other `E` is a `ConnectionLost` for every request on the connection, which a non-idempotent operation (`incr`, CAS, an expiring touch) does not resend. (#1873)
- In proxy mode (`via_proxy`), one operation with no proxy left to fail over to (an empty proxy roster) ran the proxy failover three times: three `Q` requests to discovery, and `stats().refresh_failures` grew by 3. Connection acquisition, which fails over since #1845, is now the only place that does; the operation is retried only when no failover came up empty during its first attempt, so it is one `Q` and one refresh failure. (#1864)
- In proxy mode (`via_proxy`), `get_many`, `set_many`, `touch_many` and `get_many_and_touch` never failed over to another proxy: they only redialed the dead proxy's address, so a bulk-only workload stayed on `ConnectionLost` while a healthy proxy existed (only the next single-key operation recovered). The failover now happens when the connection is acquired, as in the Java, .NET and Go SDKs, before anything is written, so it applies to every operation and is safe for non-idempotent ones. (#1845)
- In proxy mode (`via_proxy`), losing the proxy with many requests in flight made every caller re-fetch the proxy roster, dial a proxy and replace the connection a sibling had just installed, so retries failed with `ConnectionLost("connection closed")` although a healthy proxy existed (a probe of 40 concurrent `get`s after the proxy stopped failed 25-37 of them, with 41 roster requests and 40 connections). The failover is now single-flight: callers queue on the redial gate and one that finds a live connection installed by a sibling leaves it alone. (#1846)
- An untagged no-body reply marker (`S`, `D`, `N`, `W`, `C`, `R`, `T`, `B`) is now followed by a checked `\n`; a reply such as `Sx` was accepted as `S`, so a desynchronised stream went undetected. It is now an `Error::Protocol` that closes the connection, as in the Java and .NET SDKs. (#1848)
- In proxy mode (`via_proxy`), the reconnect after losing the proxy counted a `stats().refresh_failures` whenever no proxy could be dialed, including when discovery returned a usable proxy roster and only the dials failed. It now counts only a refresh that yields no usable roster, as the Python, TypeScript and Java SDKs do. (#1822)
- At the initial connect, a listed node that answered the handshake with something that is no reply to `A` (an HTTP server on its reused address) was stored in the reconnect cooldown with the dial's `Error::Protocol`, so for the first cooldown (1 s by default) an operation routed to it failed with `Protocol` instead of refreshing and rerouting. The cooldown now carries `Error::ConnectionLost`, as a redial gives it; `connect()` still returns `Error::Protocol` when no listed node is reachable. (#1821)
- A multi-key operation still running when `close()` was called (`close()` does not wait for it) ran its forced node-list refresh after a failed leg: a discovery round trip (with slow discovery, seconds) and dials of new nodes on a closed client. A closed client no longer refreshes, as in the Python, TypeScript and Java SDKs. (#1814)
- At the initial connect, a listed address that answers as a discovery server or a proxy (its IP reused by another process) failed the whole `connect()` with `Error::Protocol`, while running clients carried on and an address that failed to dial was already tolerated. It is now installed as a dead member with a reconnect cooldown, carrying an `Error::ConnectionLost` ("does not identify as a cache node") like the one a redial gives it; `connect()` fails only when no listed node is reachable, and then with the same `Error::Protocol` as before for a non-node address. (#1801)
- A forced node-list refresh or redial turned every `Error::Protocol` from the dial into `ConnectionLost`, including ones unrelated to the handshake (a failed TLS setup task, a malformed node list). Only the unexpected reply to `A` is converted now. (#1732)
- A `B` (busy) reply while requests were waiting on the connection was reported as `Error::Protocol` on an untagged connection and as a tag-mismatch `ConnectionLost` on a tagged one. It is now one `ConnectionLost` (busy) either way, as in the Go SDK. (#1731)
- `set_many` and `touch_many` (and the `Namespace` forms) on a namespace longer than 167,772 bytes could send an `o` or `y` frame whose namespace length times key count was over the node's 64 MiB limit; the node answered `En` and closed the connection, failing the other requests in flight on it. A chunk now holds at most `min(400, 64 MiB / namespace length)` keys. (#1725)
- Hedged reads (`read_hedge_after`) stalled once an owner had stopped answering: after 32 hedge legs were left running against it, the next read waited for the request timeout (30 s) for one of them instead of returning the replica's answer, and against an owner that was only slow the limit made hedging ineffective at moderate read rates (about 400 reads per second and up against an owner answering in a few hundred milliseconds). An owner that has 32 hedge legs unanswered after the hedge interval is now tried last, so reads go to its replica first; a miss still waits for the primary. The limit on hedge legs in flight, which waits for a read's remaining legs, is raised from 32 to 256 and now only applies when every owner of a key is slow. (#1390)
- The limit of 256 on the hedge legs a client leaves running was compared against every hedge leg in flight, including the legs of reads that had not yet been answered, so it measured read concurrency rather than accumulation: 400 concurrent `get` calls against a slow primary all waited for it, and any workload with more than 256 reads in flight made every read with a slow loser wait for it. The limit now counts only the legs left running after their read returned. (#1449)
- The last owner a hedged read started was never counted as late, because the hedge timer was armed only while another owner remained to start. With replication 2 a node was therefore demoted only for the keys it is primary for. The timer is now waited out once more for the final leg. (#1450)
- `set`, `put_if_absent`, `replace_if_present` and `replace` (and their `Namespace` counterparts) checked the 1 MiB request budget against the value after compression, with a comment saying the other SDKs did the same. `set_many` and the Java, .NET and Python SDKs check the raw value before compressing, so a highly compressible value a little over the budget was accepted by `set` and rejected by `set_many` on the same client. `set` and the compare-and-set writes now check the raw size before compression too, and return `Error::InvalidArgument` for such a value. (#1494)
- A forced node-list refresh (after an operation failed against a dead or wrong node) opened a new discovery connection for every failed operation: while discovery still listed a dead node (its liveness window), each operation routed to it forced a refresh that installed the same list, so a caller looping over keys routed to it opened one connection per failure. A forced refresh now starts at least 200 ms after the previous refresh finished; an operation that fails inside that window reuses the refresh that just finished and still retries. (#1580)
- A forced node-list refresh reused a refresh that was already in flight when the operation failed, because it compared the time the last fetch completed instead of when the refresh began. A refresh that had read discovery's list before the node died (say the 30 s staleness refresh) installed the list that still named the dead node, and the operation's one retry went back to that node. Such a refresh is now waited out, and the caller runs, or joins, a refresh that began after its failure (after the 200 ms interval). (#1580)
- `get_many` and `get_many_and_touch` returned `Error::InvalidUtf8` instead of `PartialWrongNodeText`, `PartialServerTooOldText` or `PartialConnectionLostText` when one of the values already resolved was not valid UTF-8 (stored with `set`/`set_many_bytes`): the partial map was decoded strictly with `?`, so the first binary value replaced the wrong-node, server-too-old or connection-loss error, and the caller lost both the error class and the other values. The partial map is now decoded value by value: a value that is not valid UTF-8 is left out of it (`get_many_bytes` still returns its bytes) and the original error is returned with the rest, as the TypeScript and Python SDKs do. (#1526)
- When the future of a call that had started a forced node-list refresh was dropped before the refresh finished (say a `tokio::time::timeout` around `set`), the refresh's start time stayed recorded. A forced caller that had seen its own failure earlier and was queued behind it then took that start as a refresh that ran after its failure, skipped its own, and retried on the node list that still named the dead node. A refresh that does not finish now puts the previous start time back, so the queued caller refreshes. (#1610)
- Three tests lowered the process-wide `NODE_LIST_STALE_AFTER_MS` while the rest of the suite ran in parallel, so every cluster client in every concurrently running test refreshed its node list on each call and the tests that count discovery fetches failed (6 of 6 runs of the narrowed command in the issue; the full suite passed). The staleness window (30 s) is now a per-client value, set through a hidden test hook on the client, and the static is gone. No change for callers. (#1609)
- An operation whose owner's address had started answering as a discovery server or a proxy (the node was replaced by another process on the same IP:port) failed with `Error::Protocol` until the next 30 s staleness refresh: the redial's "no longer identifies as a cache node" was not a connection error, so the cluster retry did not refresh the node list and reroute, and the reconnect cooldown repeated the same error. The redial now fails with `Error::ConnectionLost`, as any other dial that could not be made, so the operation refreshes the node list and retries once. (#1598)
- An operation whose owner's address answered the `A` handshake with something that is no handshake reply at all (an HTTP server's `HTTP/1.1 400 Bad Request`, after the node was replaced by another process on the same IP:port) failed with `Error::Protocol` ("unexpected response to A") until the next 30 s staleness refresh, for the same reason as above: the cluster retry does not act on `Protocol`, and the reconnect cooldown repeated the error. A redial whose handshake gets such a reply, raised before any frame of the request exists, now fails with `Error::ConnectionLost` (so even a never-sent `incr` or compare-and-set refreshes and reroutes, and an expiring `touch_many` counts the chunk as not sent). The first `connect()` still returns `Error::Protocol`. (#1668)

## [0.6.2] - 2026-11-03

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.1] - 2026-11-02

### Fixed

- A request in flight when `close()` ran could still dial and authenticate
  against the node (up to the 5 s connect deadline) before failing with
  `AlreadyClosed`: `slot_connection` and `open_node_stream` checked whether
  the client was closed only after the dial. `slot_connection` now checks
  before dialing, after taking the per-slot redial lock, so the node no
  longer sees a connection from a client that is already closed. (#1231)

- `keep_alive_interval(Duration::ZERO)` turned the keep-alive task into a
  `sleep(0)` loop that pinged continuously, `request_timeout(Duration::ZERO)`
  made every request time out and close its connection, and
  `reconnect_cooldown(Duration::MAX)` panicked on the first failed dial
  (`Instant::now() + cooldown` overflows). `connect()` now returns
  `Error::InvalidArgument` for a zero `keep_alive_interval` or
  `request_timeout`, and for any of those two, `reconnect_cooldown` and
  `read_hedge_after` above 24 hours (`reconnect_cooldown(ZERO)` still means
  "use the default"). (#1186)

- With `read_hedge_after` set, a `get` whose hedge leg panicked could wait
  forever. Each leg reported its outcome only at the end of its task, and
  `read_hedged` held the original sender itself, so once every owner was in
  flight a leg that ended without reporting left `rx.recv()` pending with no
  timeout (`resolve_hedge_losers` waited the same way). Every leg now reports
  through a drop guard that sends an error outcome if the task is dropped
  unsent, and a panic inside a leg is caught and returned to the caller as an
  `Error::Protocol("... hedged read leg panicked: <message>")` instead of
  being hedged around. No panic path in a leg is known today; this removes
  the hang if one is ever introduced. (#1164)

## [0.6.0] - 2026-11-01

### Changed

- The minimum supported Rust version moves from 1.85 to 1.88 (`rust-version`
  in `Cargo.toml`, checked in CI). The `sha2` dependency moves from 0.10 to
  0.11, whose current release requires rustc 1.88; the server already uses
  sha2 0.11, so the CAS digest code on both sides now builds on the same
  `digest` generation.

### Fixed

- A large request over TLS could stall until `request_timeout` and fail with
  `ConnectionLostTimedOut` (a `set` is not retried) when the server read
  slowly. A request was written with `write_all` and never flushed;
  tokio-rustls's `write` returns once rustls has accepted the plaintext, so up
  to ~64 KiB of the frame's tail could stay queued inside rustls, and nothing
  else on the connection (the read half never writes) pushed it to the
  socket: the server never saw the end of the frame and never replied. The
  request now flushes after the write, inside the same shutdown-cancellable
  step, and the identify handshake (`A`, `L`, `Q`) flushes its writes too.
  Plain TCP is unaffected (flush is a no-op there). (#1104)
- A compressed value whose DEFLATE body was cut short is now an
  `Error::Decompression` instead of a silent prefix of the original. The
  decoder was driven through flate2's `Read` adapters (`DeflateDecoder` +
  `take` + `read_to_end`), which on some flate2 versions return `Ok` at end of
  input without checking that the stream reached its final block, so a
  40,000-byte value whose body was cut to 1 byte, half, or all but the last
  bytes decoded without error to 0, about 12,700, or up to all 40,000 bytes.
  Decompression now drives
  `flate2::Decompress` directly and succeeds only on `Status::StreamEnd`; a
  marker-only value (empty body) is an error too, and the 64 MiB decompressed
  size cap is unchanged. (#1106)

## [0.5.1] - 2026-10-27

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.5.0] - 2026-10-26

### Fixed

- A `Some(0)` touch (`touch`, `get_and_touch`, `touch_many`,
  `get_many_and_touch`) no longer loses the value when its primary is
  rerouted. The plain `t ... 0` to the other owners used to go out at the
  same time as the primary's frame, so a primary that could not be reached
  or answered `W` from a stale ring left a replica that had already expired
  its copy; the reroute then reached that replica as the new primary and got
  a miss (`touch` returned `false`, `get_and_touch` / `get_many_and_touch`
  returned nothing for a key that existed, its value consumed and never
  returned). With `Some(0)` the primary is now asked alone, and the other
  owners of the ring in force at that moment are sent the plain touch only
  after it answered `S`, `V` or `N`; a failed or `W` primary is rerouted with
  no replica touched. In a batch both passes ask the primaries only, then one
  plain fan-out goes to the replicas for the keys a primary answered (a miss
  on the primary included: a replica may still hold a copy). A positive TTL
  and `None` keep the concurrent fan-out. The replica legs of an expiring
  touch therefore run after the primary answered, outside the order a
  pipelined connection otherwise gives a key's operations, like the existing
  retry paths.
- With `Some(0)`, a chunk of `touch_many` / `get_many_and_touch` that fails
  after its frame was written no longer takes the chunks after it down with
  it. A batch of more than 400 keys on one primary is sent as several `y`
  frames; only the keys of the chunk that was written are reported as lost
  (that frame is not sent again), and the chunks never sent now go out in the
  refresh-and-retry pass, so `touch_many`'s "every other key was touched"
  holds and `get_many_and_touch` returns their hits. An address that stopped
  identifying as a node when it was redialed (`Error::Protocol` from the
  dial, before anything was written) is no longer counted as a lost frame
  either: its keys are retried on the second pass instead of being given up
  on.
- In a cluster, `get_many_and_touch` no longer discards the hits other
  primaries returned when one primary answers `En` to `y`. It returns the new
  `Error::PartialServerTooOldText` (`get_many_and_touch_bytes`:
  `PartialServerTooOld`) with them attached; with `Some(0)` they were
  consumed on the nodes and could not be read again. Code that matches
  `Error::ServerTooOld` to detect an old node must match these as well, and
  an exhaustive `match` on `Error` needs the two new arms. `touch_many` still
  returns the plain error.
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
- Only a node's `En` is reported as `Error::ServerTooOld` by the touch
  methods. Any other `E` to a `t` or `y` frame (the proxy answers a bare `E`,
  tagged `E <tag>`, when it could not tell whether an expiring touch was
  applied) was reported as "server too old, upgrade every node"; it is now
  the connection lost after the frame was written, which an expiring touch is
  not sent again on, and which `touch` / `touch_many` with a TTL still send
  once more on a fresh connection like any lost reply. The caller gets
  `Error::ConnectionLost` either way.

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
