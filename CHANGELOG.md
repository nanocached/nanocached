# Changelog

All notable changes to the nanocached server (the `nanocached-node`,
`nanocached-proxy` and `nanocached-discovery` binaries and their container
images) are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
the `server/vX.Y.Z` tags. SDK and adapter changes are in their own
changelogs.

From 0.4.4 on, every release ships the server, the six SDKs and the seven
framework adapters at one version, whether or not a component changed.

## [Unreleased]

### Fixed

- `nanocached-node`: a join no longer ends the forwarding window of the join
  before it. A node holds one handoff slot, and a new `M` used to replace
  it and revoke its forwards even though the earlier joiner was confirmed
  and its forwarding grace (100+ seconds) was still open. A client whose
  node list had not caught up kept writing to the old owners, which from
  then on forwarded only to the newest joiner, so an earlier joiner that
  owned the key stopped receiving the writes and stayed behind (later a
  stale read once the other copies left). The superseded handoff now moves
  to a `lingering` list on the new slot, keeps its connection and its
  forwards, and receives a write for every key it owns under the current
  ring (and every `CLEAR`) until its grace ends. It is dropped and its
  forwards are revoked, as before, when its join was abandoned, when the
  same name joins again at a new generation, and when discovery evicts or
  removes it (issues #267 and #474: its address may belong to another node
  by then). Observed in a join-then-decommission-then-kill chaos run: the
  restarted node that had joined just before the next join was behind its
  peers on 4–285 keys right before the last kill (median 139) and is now
  behind on 0–47 (median 6.5); the end-to-end count of stale reads in that
  run did not change measurably because other causes dominate it (#563,
  #565). Issue #564.
- `nanocached-node`: with an auth secret configured, a connection that has
  not authenticated yet is now held to what an unauthenticated peer can
  legitimately do. The node used to parse and buffer a whole request (up to
  1 MiB, and for an `M` a 32-byte span per roster entry) before checking
  `authenticated`, so each of up to 1024 unauthenticated connections could
  pin that much. Now only an `A` frame is accepted first: anything else is
  answered `En` and closed on its first byte without being buffered, an `A`
  frame (header plus secret) is capped at 4096 bytes while incomplete
  (matching discovery's pre-identification cap; a longer secret field no
  longer makes the node keep reading). Commands pipelined behind a complete
  `A` frame are unaffected, and a node with no secret behaves exactly as
  before. Because an `A` frame must now fit in 4096 bytes, the node refuses
  to start with a `NANOCACHED_AUTH_SECRET` longer than about 4080 bytes
  (discovery's identical cap already ruled such a secret out in a cluster).
- `nanocached-node`: a multi-key `m`/`o` frame no longer costs (keys x
  namespace length) on the node's single thread. A namespace can be about
  1 MiB, and the ownership checks (FNV over the whole namespace) and the
  cache lookups (SipHash over it) ran once per key, so one frame of 1-byte keys
  under a ~500 KB namespace hashed tens of gigabytes. The namespace's
  share of the key hash and its cache entry are now resolved once per
  frame, and the migration, decommission and re-replication loops hash
  each distinct namespace once. Placement is unchanged (the hash is
  byte-identical) and there is no new limit on namespace length.
- `nanocached-node`: a namespace's name is now charged to `--max-memory`
  (its length plus a fixed 256-byte overhead, released when the namespace
  empties or is cleared; the default namespace is not charged). Before, a
  stream of `s`/`o` writes naming fresh large namespaces with 1-byte keys
  and values was accounted about 100 bytes apiece while the process held
  every name, so the memory bound never evicted. Per-namespace
  `--namespace-budget` accounting and the `/metrics` per-namespace rows are
  unchanged; the namespace rows now sum to `used_bytes` minus these name
  charges.
- `nanocached-node`: client writes to keys being handed to a joining node (or
  to the entrants of a decommission) are no longer dropped when the target
  is slow. Each write was its own task behind a bounded channel, so while
  the target was busy (for example a replacement node receiving its
  handoff) the backlog passed about 4350 and the rest were dropped with a
  warning: under chaos load, 10,000 to 35,000 per node, leaving the joiner
  with older values than the cluster had acknowledged. Writes now wait in a
  per-target queue and a later write to a key replaces one still waiting
  for it, so the queue is bounded by the number of distinct keys, not by
  the number of writes, and nothing is dropped for want of room. A clear
  and a put-if-absent relay keep their place in the order. One drainer per
  target sends in order, so a large `o` costs one queue insert per key.
- `nanocached-node`: a `U` or `u` carrying the wrong membership token on a
  connection that negotiated tagged mode no longer panics the connection
  task. The rejection is now answered `R <tag>` (the tagged form of the
  retryable-error status) before the connection is closed; handoff peers
  connect untagged and still get the bare `R`.
- `nanocached-node`: `--drain-timeout` is now limited to 604800 seconds
  (7 days) and rejected above that at startup. Any larger value (the flag
  took an unbounded `u64`) overflowed the `Instant` the drain deadline is
  computed from, so the first SIGTERM panicked instead of shutting down.
- `nanocached-node`: forwarding a client write to a joining node (or a
  decommission entrant) no longer `Debug`-formats the whole key on every
  forward. A key can be about 1 MiB, so this was a per-write allocation
  and format on the node's single thread.
- `nanocached-node`: shutdown no longer waits out an unresponsive peer
  during an in-flight re-replication. The dial/auth and send legs are now
  interrupted by the shutdown signal (before, only the gaps between
  attempts noticed it, so a stalled leg held the exit for up to its 10 s
  bound), and `run` waits at most 5 s for the heartbeat task (which carries
  re-replication) before aborting it.
- `nanocached-node` and `nanocached-proxy`: an `m`/`o` frame whose body arrives
  a few bytes at a time no longer has its (O(keys)) header re-parsed on every
  read. This ran before authentication, so a large header followed by a
  trickled body could keep the node's single thread busy.
- `nanocached-node`: reporting a finished migration to discovery (`C`) is now
  bounded by one 10 s budget for the whole call. Before, the auth leg and the
  `C`/`A` exchange each had their own 10 s, so a slow discovery could hold the
  report for up to 20 s.
- `nanocached-node` and `nanocached-proxy`: an `M` (multi-get) reply is now
  bounded to 16 MiB of values. A request that named one key many times could
  make the node allocate a reply of (key count x value size), over a gigabyte
  from a ~1 MiB request; hits past the budget are now answered as misses. The
  proxy used to reject any backend `M` reply over 1 MiB of values as a
  protocol error, dropping the shared backend connection and failing every
  other client's in-flight requests on it; it now accepts up to the node's
  16 MiB and holds its own reassembled reply to the same figure.
- `nanocached-proxy` and `nanocached-discovery`: a panic while serving a
  metrics scrape is now logged (`WARN metrics connection task failed`)
  instead of going unobserved, like the node's metrics endpoint.
- `nanocached-proxy`: routing the keys of a large `m`/`o` frame no longer
  scores, sorts and copies the whole roster once per key (O(nodes log nodes)
  per key, hundreds of thousands of times for a ~1 MiB frame, on a tokio
  worker). Each key now keeps only its top owners in one pass, and keys are
  grouped by node without cloning an address per key. Key placement is
  unchanged.
- `nanocached-proxy`: a burst of `W` replies (a node mid-handoff answers `W`
  for every key it no longer owns) no longer drives one roster fetch and
  `Y` announce to every discovery replica per reply, back to back. Forced
  refreshes are now spaced at least 1 s apart and the nudges in between are
  coalesced into one fetch, and a fetched roster identical to the current
  one is no longer republished to the proxy's connections.
- `nanocached-proxy`: after a client was answered the fatal `E` (or a write to
  it failed or timed out), its connection now closes at once. Before, the
  reader kept waiting for up to the 60 s idle timeout, holding the socket,
  a connection permit and a per-IP slot; and a frame the client sent in that
  window was still run against the backends before its reply failed to
  send, so a `S` or `i` could be applied after the client had been told the
  connection was closed.
- `nanocached-discovery`: a connection's idle clock for its next command now
  starts when the previous command's handler finishes, not when it was
  parsed. A joining node that waited in the queue for more than 60 s was
  promoted and answered `R`, then closed as idle before its first heartbeat
  could arrive (it recovered by redialing and re-announcing, at the cost of a
  reconnect and a `WARN`); the same applied after a slow `C` or `V` handler.
- `nanocached-discovery`: the periodic sweep (liveness eviction, proxy
  reaping, the migration-timeout reaper) no longer pauses while an abandoned
  join's `X` fan-out or the next join's `M` fan-out is running. Those used
  to be awaited inline, so a batch of unresponsive nodes froze the sweep for
  up to ~40 s (`X`) or ~120 s (`M`, three attempts) per batch of 64, exactly
  when nodes were failing. They now run in the background, one at a time
  (as serialized with each other as before), and an eviction noticed while
  one is running is acted on as soon as it finishes.
- `nanocached-node`: once the cache is at `--max-memory`, each write no longer
  scans every namespace to find the one holding the oldest entry. A lazily
  kept min-heap of each namespace's oldest entry replaces the scan, so the
  cost per eviction no longer grows with the number of namespaces (with
  200,000 namespaces, an evicting write went from about 7.6 ms to about
  14 us). Which entry is evicted is unchanged.

### Security

- Updated `rustls` to 0.23.45 in the node, proxy and discovery binaries
  (RUSTSEC-2026-0285: TLS 1.3 handshake messages were accepted across
  encryption-level boundaries).
- `nanocached-proxy`: with an auth secret configured, a connection that has
  not yet authenticated is now held to the same bounds discovery applies. It
  may buffer at most 4096 bytes (only an `A` frame is acceptable before the
  secret is checked; a larger declared frame is refused from its header) and
  must authenticate within a fixed 60 s of being accepted. Before, it could
  declare an `A` of up to 1 MiB and trickle it in a byte per 59 s, holding
  1-2 MiB indefinitely per connection (about 1 GiB across the default 1024
  connections) because the 60 s idle timeout restarts on every read. A
  proxy with no secret is unchanged.
- `nanocached-node`, `nanocached-proxy` and `nanocached-discovery`: the
  per-source-IP connection cap now counts an IPv6 peer by its /64 prefix
  instead of its full address, and an IPv4-mapped IPv6 peer
  (`::ffff:a.b.c.d`) as the IPv4 address. Before, one /64 offered 2^64
  distinct sources, each with a full cap of its own, so a single host could
  hold every connection slot. IPv4 behavior is unchanged. Clients that share
  a /64 (hosts on one subnet, or pods on one node of an IPv6 Kubernetes
  cluster) now share one cap, so raise `--max-connections-per-ip` for them
  as for any other shared source.
- `nanocached-discovery`: the cap on concurrent `Waiting`/`Joining`
  registrations per source (`MAX_WAITING_PER_SOURCE_IP`) now counts an IPv6
  source by its /64 prefix, like the per-source-IP connection caps. Before,
  it compared the registered address text, so every address of one /64 held
  its own allowance and one host could fill the global waiting queue.

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project (the first release
that also publishes the seven framework adapters).

## [0.4.3] - 2026-09-08

Server-only release.

### Added

- `nanocached-proxy --max-connections-per-ip <n>` (default 256, the same
  default the node has always had) caps client connections per source IP.
  Previously only the global `--max-connections` applied, so one client IP
  holding idle connections could exhaust the whole budget and make every
  other client see `B` while the cluster was healthy. Over the cap the proxy
  answers `B` and closes, like the node. The value must not exceed
  `--max-connections`; an unset cap follows a lowered `--max-connections`
  down (issue #520).

## [0.4.2] - 2026-09-07

### Fixed

- `nanocached-proxy` keeps its shared backend connections alive: after 30 s
  without writes it sends the SDKs' reserved keep-alive `G` probe, so the
  node's 60 s idle timeout no longer closes the connection underneath an idle
  proxy. Before, the first request after a quiet minute was written to a dead
  socket, and an `INCR`/`CAS` among them was answered with an error because a
  written frame cannot be retried. A connection lost with only the probe on it
  is now logged at INFO instead of WARN (issue #514).

## [0.4.1] - 2026-09-06

### Fixed

- `nanocached-proxy` no longer replays pipelined `INCR`/`CAS`/`CAD` requests
  that sat behind a failed reply on a shared backend connection. Those frames
  had already been written and may have been applied, but were classified as
  "not sent" and so retried after the redial, which could apply a counter
  increment twice. They now fail as "may have been applied" (issue #497).
- `nanocached-node` shutdown no longer waits up to 60 s (twice back to back)
  for a re-replication wait to time out: the waits now stop as soon as
  shutdown is requested, so a SIGTERM during a join migration or a superseded
  re-replication stays within the orchestrator's stop grace (issue #498).
- `nanocached-node` shutdown drain now also runs forwards still queued behind
  the last connection task. Before, a write already acknowledged to a client
  could be dropped unlogged without ever reaching the joining or leaving node
  (issue #502).

## [0.4.0] - 2026-09-05

First server release since 0.3.0: new `nanocached-proxy` binary and image,
namespaces, new commands, autoscaling support (metrics, health endpoints,
graceful scale-in), and the fixes from audit passes 3 to 11. The
cluster-internal frames changed (see the **Breaking** entries below):
upgrade every node and every discovery replica in the same rollout, and
upgrade nodes before clients start using namespaces.

### Added

- `nanocached-proxy` (issues #109, #110): a new binary and a third container
  image (`nanocached-proxy`) that terminates client connections and routes on
  their behalf. To a client it looks like one node that owns every key, so an
  unmodified SDK in single-address mode works against it. Reads go to the
  primary with replica fallback, writes and deletes fan out to all R owners
  with the primary's answer returned, a stale-view `W` is refreshed and
  retried, and `c`/`F` fan out to every member. All client traffic for a node
  shares one pipelined backend connection, so the node-side connection count
  is the proxy count rather than the client count. Flags: `--host`, `--port`,
  `--discovery`, `--tls-cert`/`--tls-key`/`--tls-ca`, `--max-connections`,
  `--max-connections-per-ip`, `--drain-timeout`, `--metrics-port`. The
  shared secret comes from `NANOCACHED_AUTH_SECRET` (`NANOCACHED_SECRET` is
  accepted as a deprecated fallback with a startup warning; the new name wins
  when both are set).
- Discovery serves the proxy roster (issue #122): `Y` (proxy announce), `Z`
  (proxy deregister) and `Q` (proxy list, answered `B` during the startup
  grace). Proxy entries live apart from node membership and expire after
  `--liveness-timeout` without a re-announce.
- Namespaces (issue #105): lowercase `g`/`s`/`d` commands carry a
  length-prefixed namespace ahead of the key; the uppercase `G`/`S`/`D`
  address the default namespace unchanged. Routing hashes
  `fnv1a(be32(len(ns)) || ns || key)` for a non-empty namespace and the
  legacy `fnv1a(key)` for the default one, so existing key placement does not
  move. The cache keeps one LRU sub-map per namespace. A pre-namespace node
  answers `E` and closes on a namespaced frame.
- `c <namespace-length>` clears one namespace and `F` flushes every namespace,
  both answered `C` (issue #106). On a node this is an O(1) sub-map drop.
  During a join handoff, clears are queued and replayed on the transfer stream
  so they cannot be overtaken by stale keys.
- `i` atomic counter (issue #129): answered `I <value-length> [<ttl>]`, `N`
  for a missing key, or the new `T` status when the stored value is not a
  plain decimal `i64` or the delta would overflow. In a cluster only the
  primary evaluates it and its literal result is forwarded to replicas as an
  ordinary set.
- `k`/`x` compare-and-set and compare-and-delete (issue #141): the condition
  is `A` (absent), `P` (present) or a 32-hex-digit digest, the first 16 bytes
  of the SHA-256 of the stored bytes. Primary-only evaluation with
  result-forwarding replication, like `i`. LRU eviction can reclaim a
  CAS-protected key, so this is not a distributed lock.
- `m`/`M` batched get and `o`/`O` batched set (issue #150), with one shared
  TTL per `o` batch. The proxy groups keys by owner and does one bounded
  refresh-and-retry pass for stale views, with per-key `W` for keys that still
  fail.
- Retryable-error status `R` (issue #125): `A` accepts an optional trailing
  `R` capability token (`A <len> [T] [R]`). On a connection that declared it,
  `nanocached-proxy` answers a transient upstream failure with `R` and keeps
  the connection open instead of `E` and close; legacy connections still get
  `E`. Node and discovery accept the token.
- Operations endpoints (issue #124): `--metrics-port <port>` on node, proxy
  and discovery serves `/metrics` (Prometheus text format), `/healthz` and
  `/readyz` on a separate listener, off by default and unauthenticated, so
  keep it internal. Node `/readyz` is 503 until a clustered node has adopted a
  membership view; proxy `/readyz` is 503 without a roster and while
  draining; discovery `/readyz` is 503 during the startup grace. The listeners
  accept at most 16 concurrent connections.
- Graceful scale-in (issue #124). The node, on SIGTERM, stops heartbeating,
  hands every key it owns to the node the ring promotes for it (new `U`
  handoff-store command), leaves membership with the new `V` discovery
  command, keeps forwarding writes for a short window, then exits. A second
  signal overrides the drain. The proxy, on SIGTERM, deregisters via `Z`,
  stops accepting, finishes in-flight requests and exits. `--drain-timeout
  <secs>` (default 25) bounds both and must fit the orchestrator's stop grace.
- Node flags `--max-connections <n>` (default 1024) and
  `--max-connections-per-ip <n>` (default 256) replace the compile-time
  limits (issue #126). Both must be at least 1, an explicit per-IP cap may not
  exceed the total, and an unset per-IP cap follows a lowered total down.
  The new gauge `nanocached_node_connections_max` reports the bound.
- Node flag `--namespace-budget <ns>=<bytes>` (repeatable; an empty name is
  the default namespace) caps one namespace's share of memory (issue #127):
  over its cap, a namespace evicts its own oldest entries. It is a cap, not
  a reservation, and `--max-memory` stays authoritative. Values below 1000
  bytes and duplicate namespaces are rejected (issue #355). Reported by the
  gauge `nanocached_node_namespace_budget_bytes`.
- A surviving node re-replicates keys after an eviction promotes a new owner
  (idempotent put-if-absent via `U ... A`), so a cluster no longer runs
  under-replicated until a second failure loses data (issues #266, #267).
- Discovery `T` command (node roster including membership tokens), usable only
  by a registered, `Joined` node that identifies itself with its own token.
  Decommission and eviction-triggered re-replication use it instead of `L`.

### Changed

- **Breaking: `U`/`u` handoff frames now carry the receiving node's
  membership token, and `M` carries the joining node's token** (issue #295).
  Before, any holder of the shared secret could forge a handoff frame and
  write or delete a key regardless of ownership. Frames without the token are
  rejected, so a node and a discovery server from different versions cannot
  complete a staged join or a handoff. Upgrade all nodes and all discovery
  replicas together.
- **Breaking: a clustered node now hands off its entries and leaves
  membership on SIGTERM**, within `--drain-timeout` (default 25 s). Before,
  it exited immediately. Make sure the orchestrator's stop grace (ECS
  `stopTimeout`, Kubernetes `terminationGracePeriodSeconds`) exceeds the
  budget, or pass `--drain-timeout 0` for the old behavior.
- `M` and `C` carry a join generation as an optional trailing field, so
  discovery rejects a handoff-complete report from a superseded join of the
  same node name (issue #421). A legacy peer's frames still parse, but the
  protection is complete only once every node and discovery replica has
  upgraded.
- Sets applied by handoff (`U`) no longer count toward the client-visible
  `sets_total` and `cas_sets_total` metrics; scale events do not inflate them
  (issue #394). Staged-join transfers still arrive as ordinary `S` frames and
  are counted (the metric's help text says so).

### Fixed

- Discovery serves every node parked during the startup grace once it ends,
  not just the first (issue #113).
- A node reverts its ring view when a completed join is later abandoned,
  including implicit abandonment by the next `M` (issues #93, #218), so it no
  longer answers `W` for keys of a joiner that never took them over.
- Writes during a handoff or decommission are no longer lost or reordered:
  batched `o` writes are forwarded to the joining or leaving node (issue #176;
  a later fix covers a join starting while the write was in progress), a write dropped when the forward channel was full is queued instead
  (bounded at 4096 waiters), forwards to one key are applied in order, and
  forwards queued behind a permanently failed target make one attempt until it
  answers.
- Forwards are dropped once their handoff is revoked (eviction of the joiner,
  abandoned join, superseded `M`), so a backlog no longer lands on a process
  that was handed the dead node's recycled IP (issue #474). The forwarding
  window also closes as soon as discovery evicts the joiner.
- Node: `M` retries for a crash-restarted joiner under the same name are told
  apart by join generation instead of being answered as duplicates, which had
  stalled the join until its timeout.
- Decommission now authenticates to discovery with discovery's own ack (`Od`);
  with `NANOCACHED_AUTH_SECRET` set, scale-in handoffs used to be abandoned
  and the leaver's entries lost (found in the ECS verification).
- Decommission, re-replication and abandon waits stop at shutdown, and a
  second SIGTERM during a drain exits promptly and still sends `V`.
  The migration wait is capped at min(drain budget / 4, 5 s), and transfer
  retries back off 100 ms times the attempt number.
- Re-replication after consecutive evictions no longer stalls: every
  surviving old owner may send, not only the top-ranked survivor (issue #405).
  A re-replication put-if-absent that arrives mid-handoff is relayed to the
  joining owner (issue #266).
- Node heartbeat-ack read is bounded, so a hostile discovery cannot exhaust
  memory (issue #92). `fetch_roster` and discovery line reads are bounded by
  one overall deadline rather than per byte (issues #216, #217).
- Discovery: handoff-complete reports from a superseded join generation are
  rejected (issue #421), and generations are seeded from the clock so a
  restarted discovery cannot reissue old ones.
- Discovery: an explicit `V` from a joining or waiting node now abandons the
  in-flight join and wakes a parked promotion wait, instead of stalling later
  joins and leaking a connection slot until the migration timeout (issues #297,
  #323).
- Discovery performance and bounds: the `L`, `H` and `T` responses are cached
  per roster change (issues #95, #298, #356), the registry fan-out is bounded
  (issue #299), the roster is no longer deep-cloned per ready node (issue
  #409), and duplicate `J` reconnects are not rejected by the request size
  check (issue #421).
- Proxy correctness: `INCR`, `CAS` and `CAD` are replayed only when the
  request provably never reached the wire (issues #272, #293, #322).
  Multi-get retry falls back to replicas, not only the fresh primary (issue
  #221). The proxy no longer exits on a transient accept error (issue #271),
  prunes backends for addresses that left the roster (issue #220, which
  otherwise grew without bound under autoscaling), and sets `TCP_NODELAY` on
  client and backend connections (about 40 ms stalls on pipelined bursts over
  a real network).
- Proxy DoS bounds: backend and client writes, the client TLS handshake and
  the busy reply have timeouts, so a client that stops reading can no longer
  hold a connection slot forever; discovery replicas are queried concurrently.
- Node and proxy: the header-terminating newline scan is cached per
  connection, so a header trickled in byte by byte no longer costs O(n^2)
  CPU, reachable before authentication.
- Accept loops on the node, discovery and proxy metrics listeners back off on
  fd exhaustion (issue #184), and all three bound concurrent metrics
  connections (issues #327, #358).
- Constant-time comparison for CAS digests (issue #336) and proxy tokens
  (issue #183).
- `HashRing` deduplicates repeated node names, so `owners` and `is_owner` can
  no longer disagree during a migration (issue #328).
- A new namespace's map key is copied, so it can no longer pin a whole
  pipelined receive buffer without being charged to memory (issue #406).
- Migration marks are accounted to the owning namespace, so the per-namespace
  `/metrics` rows sum to the global figure and `--namespace-budget` accounts
  them (found in the seventh-pass audit).

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. It fixes the cluster
data-loss and availability bugs found in the 0.2.0 end-to-end run (issues
#61 to #63, #66).

### Changed

- **Breaking: the discovery heartbeat acknowledgment now carries the current
  member roster** (`A <count> <r>` followed by `L`-shaped entries; a bare `A`
  during the startup grace or a replication-factor dispute) instead of a bare
  `A`. Nodes adopt their primary replica's roster whenever it differs from
  their current view. **Upgrade nodes before discovery servers:** a new node
  accepts either form, an old node redials on every heartbeat when handed the
  new one (issue #61).
- Failed auth handshakes with discovery log their cause, and a peer closing
  without TLS `close_notify` logs at INFO instead of one WARN per
  disconnect (issue #68).

### Fixed

- After an eviction, surviving nodes kept routing the dead node's keys to it:
  `W` for every such key at R=1, and writes one copy short at R>1. Each
  node now refreshes its ring view from the heartbeat ack (issue #61).
- Back-to-back or concurrent joins no longer lose keys. A source that
  finished its handoff accepts the next `M` while forwarding (the `M`'s
  `joined` roster says whether the previous join completed or was abandoned),
  and dead copies are kept until discovery confirms the join (issue #62).
- Discovery holds joins until its startup grace has ended; a `J` accepted
  during the grace parks in `Waiting` and starts once, so a restarted
  discovery no longer hands off from only the members that had re-announced
  (issue #63).
- Reads during a join: a displaced key is served locally only while the
  handoff is still forwarding and unconfirmed; once discovery confirms the
  join the old owner answers `W`, so a stale client lands on the joiner
  instead of missing (issue #66).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version. 0.1.x
were pipeline-validation releases; as a pre-1.0 line, breaking changes ship
without a deprecation cycle and are listed here.

### Added

- Baseline feature set: `G`/`S`/`D` with optional TTL, shared-secret
  authentication (`A`), optional TLS on every connection, pipelining, echoed
  response tags (tagged mode) so a pipeline cannot desynchronize, idle
  timeouts, and request-size and connection limits (256 connections per
  source IP).
- Memory-bounded cache with LRU eviction, `--max-memory <bytes>` (default
  256 MiB, minimum 1 MiB).
- Cluster mode through `nanocached-discovery`: nodes register with
  `--discovery`, keys are placed by rendezvous hashing and replicated to the
  top R nodes (`--replication-factor`, default 2), joins migrate keys in a
  staged handoff, and discovery runs as soft-state replicas (HA). Membership
  commands between nodes and discovery are authenticated with a per-node
  token.
- Container images for `nanocached-node` and `nanocached-discovery`
  (separate Dockerfile targets).

[Unreleased]: https://github.com/nanocached/nanocached/compare/server/v0.4.4...HEAD
[0.4.4]: https://github.com/nanocached/nanocached/compare/server/v0.4.3...server/v0.4.4
[0.4.3]: https://github.com/nanocached/nanocached/compare/server/v0.4.2...server/v0.4.3
[0.4.2]: https://github.com/nanocached/nanocached/compare/server/v0.4.1...server/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/server/v0.4.0...server/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/server/v0.3.0...server/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/server/v0.2.0...server/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/releases/tag/server/v0.2.0
