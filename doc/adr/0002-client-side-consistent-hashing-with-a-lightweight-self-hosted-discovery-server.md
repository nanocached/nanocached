# 2. Client-side consistent hashing with a lightweight self-hosted discovery server

Date: 2026-08-15

## Status

Accepted

## Context

kvelo is a look-aside cache in front of a system-of-record database, not a
store of record itself. It has no persistence and no durability requirement:
a lost or unreachable cache entry just falls back to the database. Single-node
benchmarking (see project history) showed kvelo's unpipelined throughput and
latency already tie Redis's, and both are dominated by kernel syscall time
rather than application or runtime code, so raw single-node throughput is
unlikely to be the limiting factor for realistic cache workloads. The next
scaling axis is therefore horizontal: run multiple independent, single-threaded
kvelo nodes rather than making a single node multi-threaded.

Two architectures were considered and rejected for distributing keys across
nodes:

- A masterless, gossip-replicated design in the style of Cassandra/ScyllaDB.
  That machinery (gossip membership, replication, quorum) exists to make a
  system of record durable and consistent across node failure. kvelo does not
  have that requirement, since a cache miss is a normal, cheap outcome, not a
  data-loss incident. Adopting it would import complexity kvelo doesn't need.
- Redis Cluster-style single-key hashed sharding. Its known failure mode
  (e.g. a large aggregate structure such as a leaderboard sorted set, which is
  pinned entirely to whichever single node owns that key's hash slot) is a
  structural ceiling worth avoiding, but it isn't directly relevant to kvelo
  either, since kvelo's values are independent per-key entries, not large
  shared aggregate structures.

For learning which nodes currently exist, a third-party coordination service
(etcd/ZooKeeper/Consul) was also considered and rejected: depending on an
external project's roadmap and licensing means unplanned rework whenever that
project's direction changes. DNS-based discovery was considered and rejected
as impractical for local/single-machine development.

## Decision

Scale out via **client-side consistent hashing**, with a minimal, purpose-built
**discovery server** for cluster membership instead of a third-party
coordination service or gossip protocol.

- Cache nodes remain simple, independent, single-threaded processes with no
  peer-to-peer protocol between them.
- A dedicated discovery server process tracks which nodes are currently live.
  It is the only new component; it has no dependency on any third-party
  coordination product.
- Client SDKs are configured with only a discovery-server endpoint URL. With
  no arguments, an SDK defaults to a discovery server on `localhost`, on a
  second, dedicated port distinct from a cache node's port — a minimal local
  setup needs no external DNS or coordination service, just two local
  processes.
- Cache nodes know the discovery server's URL at startup and register
  themselves by sending it periodic heartbeats. Heartbeats are pushed from
  node to discovery server (not polled by the discovery server), which is
  simpler and also tolerates nodes that are only reachable outbound (e.g.
  behind NAT).
  - Each heartbeat is an idempotent upsert: register the node if it isn't
    already known, otherwise refresh its liveness. There is no separate
    one-time "join" message. This means the discovery server's registry is
    fully rebuildable from node heartbeats: if the discovery server restarts
    and loses its in-memory state, it self-heals automatically within one
    heartbeat interval, with no reconciliation logic needed on the node side.
  - A node that stops sending heartbeats is dropped from the registry after a
    liveness timeout. This covers both graceful shutdown and ungraceful
    crashes; no explicit "leave" message is required.
- Clients periodically poll the discovery server for the current node list and
  rebuild their local hash ring from it. Real-time propagation of membership
  changes is not required: a client acting on a stale node list only produces
  an extra cache miss (falls back to the database), never an incorrect
  result.
- Client SDKs apply a sanity check to registry updates: if the node count
  drops sharply between successive polls (e.g. by more than roughly half),
  the update is treated as a probable false signal (discovery-server hiccup,
  transient network partition) rather than genuine mass node failure, and is
  not applied that cycle. This avoids clients concentrating traffic onto the
  remaining nodes in response to a false signal, mirroring Netflix Eureka's
  "self-preservation mode".
- The discovery server itself is an accepted single point of failure. In
  production it runs as its own isolated process/container. Because its state
  is intentionally nothing more than what node heartbeats rebuild, recovering
  from a crash is just restarting it and waiting one heartbeat interval, not
  an incident. While it is unavailable, clients keep operating from their
  last-known node list, so a discovery-server outage degrades only topology
  updates, not already-established cache traffic.

## Consequences

Easier:

- Cache nodes stay simple and independent; there is no peer-to-peer protocol,
  no replication/consensus subsystem, and no persistence engine to build or
  operate.
- No third-party discovery/coordination dependency to track for breaking
  changes or licensing shifts.
- Local development needs only one extra lightweight process (the discovery
  server), not an external service such as DNS or etcd.
- Nodes can be added or removed at any time without reconfiguring clients by
  hand; the cluster resizes itself, satisfying the "add or remove nodes
  freely" goal without adopting Cassandra/Scylla-style replicated storage.

Harder / risks to mitigate:

- The discovery server is a new component to build, deploy, and monitor, and
  is a single point of failure for propagating topology changes (though not
  for already-established cache traffic, which keeps working from cached
  routing state).
- Clients can briefly disagree about cluster topology after a membership
  change (bounded by the poll interval), so two clients may route the same
  key to different nodes for a short window. This is acceptable for a cache
  but would not be acceptable if kvelo were ever used as a system of record.
- Consistent-hash ring changes redistribute key ownership, so a scaling event
  causes a burst of cache misses on the affected key range, which the backing
  database must absorb. This is expected and should be accounted for in
  database capacity planning, not treated as a kvelo bug.
- The self-preservation-style sanity check trades safety for a degraded
  window: during a genuine mass-failure event, clients keep routing some
  requests to now-dead nodes until the discovery server's view is trusted
  again, which shows up as elevated latency/timeouts, not incorrect results,
  until the situation resolves.
- Concrete parameters (heartbeat interval, liveness timeout, poll interval,
  and the sanity-check drop threshold) are not yet decided and are follow-up
  work, likely informed by measurement rather than chosen up front.
