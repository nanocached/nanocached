# 8. Staged node join with discovery-orchestrated data handoff

Date: 2026-08-16

## Status

Proposed — not yet implemented or verified. See the explicitly deferred
items below; several mechanics are follow-up work, not yet designed.

Builds on [2. Client-side consistent hashing with a lightweight self-hosted discovery server](0002-client-side-consistent-hashing-with-a-lightweight-self-hosted-discovery-server.md)

## Context

[[0002]]'s consequences already named the expected cost of a scaling event:
"a burst of cache misses on the affected key range, which the backing
database must absorb." Building the TypeScript client's cluster mode
against that design surfaced that the actual cost is worse than that
framing suggests, in ways that matter for whether the cluster design does
its job at all:

- Adding one node to an existing cluster reroutes roughly `1/(N+1)` of the
  keyspace to the new node — measured at 44–56% for a single node joining
  a 1-node cluster with realistic `host:port` node names, i.e. a bare
  majority of the keyspace, not a small "affected range." The new node
  starts empty, so every one of those keys is a guaranteed miss until the
  application re-populates it from the database.
- The data left behind on the node that lost that range is never deleted
  by this rerouting — only nanocached-node's ordinary LRU eviction
  (`src/cache.rs`) can reclaim it, and that eviction is purely reactive: it
  only runs inside `insert()`, triggered by a *new* write pushing memory
  over budget. A workload with few new keys (common for a hot, mostly-read
  cache) may never trigger it, in which case the abandoned data occupies
  memory indefinitely. This means adding a node does not reliably relieve
  the memory pressure that was presumably the reason for adding it.
- Worse, when eviction does run, it has no way to distinguish "abandoned by
  a routing change" from "just not accessed recently" — both are plain LRU
  recency. Simulating a rerouting against a realistic access-order history
  showed that among the entries LRU would evict first, the fraction that
  were actually *still* correctly owned by the node (i.e. wrongly evicted)
  matched the overall population's stay/move ratio almost exactly (13.6%
  wrongly evicted vs. 12.6% population share) — eviction order carries no
  information about which entries are safe to lose, so it evicts live and
  dead entries in whatever ratio the population happens to contain, not
  preferentially targeting the dead ones.

A design keeping a bounded number of past `HashRing` generations on the
client, as a best-effort fallback lookup on a miss, was considered and
rejected: a key untouched since before *any* topology change can be
arbitrarily far back in the cluster's history, so no fixed number of
generations is ever complete, and unbounded generation history isn't
practical (unreachable nodes, unbounded connection/memory growth).
Framing the residual gap as acceptable because "an extra cache miss is
cheap, not data loss" — [[0002]]'s own stated premise — was also
considered and rejected as a misuse of that premise: it addresses
correctness, not throughput, and a cache whose hit rate degrades this much
on every scaling event is failing at the one job a cache has. A design
that only works when topology changes are rare and a temporary throughput
hit is tolerable is a design an operator could just as well replace with
Redis; it would not justify this project's own cluster machinery.

[[0002]] rejected Cassandra-style gossip/replication for durability
kvelo doesn't need (a cache miss is safe to lose, unlike a system of
record). That rejection does not extend to Cassandra's separate idea of a
new node not becoming routable until it has received the data it will
own — a one-time, non-durable, non-consensus data copy, not continuous
replication. This ADR adopts that second idea only; replication for fault
tolerance (holding each key on 2+ nodes) is explicitly out of scope here
and left for a future ADR.

## Decision

Node join becomes staged and orchestrated by `nanocached-discovery`,
instead of a node becoming visible to clients as soon as its first
heartbeat is registered. A node moves through three states, not two:

- **Waiting**: a node that has started and is heartbeating to discovery as
  usual, but has not asked to join. This is the default state on startup —
  there is no separate "start up already joining" mode. A node sits here
  for as long as it likes before requesting to join, and discovery may
  hold it here regardless (see below).
- **Joining**: a node that has sent an explicit join request (a new
  command) and is actively receiving its handoff data from every ready
  node.
- **Joined**: a node whose handoff is complete and is now included in the
  list `L` returns to clients.

Only a **joining** node is excluded from `L`; a **waiting** node is also
excluded (it isn't yet safe to route to and hasn't asked to be). Clients
therefore only ever see a node set that changes atomically from
fully-populated set to fully-populated set — never a newly-registered,
still-empty node, and never a node mid-handoff.

Discovery only progresses one node through waiting → joining at a time:
if a join request arrives while another node is already joining, the
requester is left in (or returned to) waiting until the in-progress join
finishes, rather than running two handoffs concurrently. A cluster scaled
from 1 to 3 nodes in quick succession therefore passes through a state
where one of the two new nodes is joining and the other is still waiting.

Once a node's join request is accepted and it enters joining:

- Discovery tells every already-ready node about the join.
- Each ready node computes, using the same consistent-hash algorithm
  clients use ([[0002]]; `HashRing` in the TypeScript client, the matching
  Rust implementation in `src/bin/bench.rs`), which of its own locally-held
  keys the new node now owns (comparing the ring without vs. with the
  joining node), and hands those key/value pairs to the new node.
- That handoff reuses the existing client-facing `G`/`S`/`D` protocol — a
  ready node acts as an ordinary client and issues `SET` to the joining
  node. No new wire protocol is introduced for the transfer itself. (If
  this turns out not to hold up once implemented, it will be revisited —
  noted here as the working assumption, not a settled guarantee.)
- Both nodes hold the handed-off data for the duration of the transfer;
  since the joining node isn't published yet, clients are unaffected and
  keep working exactly as before the join started.
- Each ready node reports completion to discovery. Once every ready node
  has reported done, discovery marks the joining node ready and publishes
  it — the next `L` response includes it.
- Handed-off keys must eventually be reclaimed from the node that no
  longer owns them, and ordinary reactive LRU eviction cannot be trusted
  to target them specifically (see Context). Handed-off keys are instead
  marked, and a separate background task actively sweeps and deletes
  marked entries — the same active-deletion facility previously proposed
  for proactive TTL expiry (today, `Cache::get_at` in `src/cache.rs` only
  expires a TTL'd entry lazily, on access) and never built; this ADR ties
  the two together as one facility rather than building two. This
  background task must be paused for the duration of any in-progress join,
  so it never deletes data still needed as the authoritative source
  mid-handoff. A machine with 2+ vCPUs is assumed so this background work
  doesn't compete for the same core serving client traffic.

## Consequences

Easier:

- A client never routes traffic to a node before that node actually has
  the data for the keys it's now responsible for — the miss burst and
  database load spike described in Context should not occur for a
  properly completed join.
- Memory pressure that motivated adding a node is now actually addressed:
  handed-off keys are positively identified and actively removed from the
  node that no longer owns them, instead of waiting on reactive LRU that
  Context showed cannot be trusted to reclaim them (or even to avoid
  evicting the wrong entries first).
- The data-transfer mechanism reuses the existing client protocol; no new
  wire format needs designing or implementing for it.

Harder / risks to mitigate — explicitly deferred, to keep in view for
follow-up work:

- **Concurrent writes during an in-progress handoff** are unhandled: a key
  copied to the joining node early in the transfer, then updated on the
  source node before the join completes, can leave the joining node with a
  stale value once published. Cassandra has machinery for this (hinted
  handoff, read repair, vector clocks); this ADR deliberately does not
  adopt it, to keep the join mechanism itself tractable first.
- **An existing ready node that fails or never responds** during a
  handoff has no defined timeout/retry policy yet — today this could stall
  a join indefinitely on one unresponsive node. The likely shape of a fix:
  if completion reports don't arrive from every ready node within some
  timeout, discovery rejects the join (the joining node returns to
  waiting, not joined) and tells every ready node to clear whatever
  handed-off-key marks they set for this join, so the abandoned attempt
  doesn't leave stray marked entries for the background deletion task to
  act on. Not designed in detail; recorded here so it isn't lost.
- **The joining node itself failing or disconnecting mid-handoff** has no
  defined recovery — whether the join is retried, abandoned, or requires
  operator intervention is undecided.
- **`nanocached-discovery` is a single point of failure**, already true
  for topology visibility under [[0002]], but this ADR adds active
  orchestration responsibility on top of it, raising the cost of a
  discovery outage during a join specifically. This ADR assumes discovery
  does not go down; redundancy (e.g. a client accepting more than one
  discovery URL) is explicitly out of scope here and left for later.
- **Fault tolerance / replication** (holding each key on 2+ nodes) is
  explicitly out of scope for this ADR, per Context, and would need its
  own design.
- The exact wire-level mechanics of the joining/ready node distinction —
  how discovery signals existing nodes, how completion is reported — are
  not finalized by this ADR and remain implementation work. Working out
  those mechanics surfaced a dependency this ADR didn't anticipate: a
  ready node's diff computation needs every node's hashing identity to
  agree with what clients use, which a plain network address can't
  guarantee in general (see [[0009]]).
