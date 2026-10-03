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

- `nanocached-node`: reporting a finished migration to discovery (`C`) is now
  bounded by one 10 s budget for the whole call. Before, the auth leg and the
  `C`/`A` exchange each had their own 10 s, so a slow discovery could hold the
  report for up to 20 s.
- `nanocached-proxy` and `nanocached-discovery`: a panic while serving a
  metrics scrape is now logged (`WARN metrics connection task failed`)
  instead of going unobserved, like the node's metrics endpoint.

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
