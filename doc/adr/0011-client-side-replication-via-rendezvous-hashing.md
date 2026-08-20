# 11. Client-side replication via rendezvous hashing

Date: 2026-08-17

## Status

Proposed — being implemented and verified on `feat/replication`.

Builds on [2. Client-side consistent hashing with a lightweight self-hosted discovery server](0002-client-side-consistent-hashing-with-a-lightweight-self-hosted-discovery-server.md),
[8. Staged node join with discovery-orchestrated data handoff](0008-staged-node-join-with-discovery-orchestrated-data-handoff.md),
[9. Decouple node identity from network address](0009-decouple-node-identity-from-network-address.md),
and [10. Discovery HA via soft-state replicas and member announce](0010-discovery-ha-via-soft-state-replicas-and-member-announce.md)

`bench` (`src/bin/bench.rs`) was removed from the repository in #29 (a load-test client living in `src/bin/` read as product surface; see commit 97c83fe) — every mention of it below is historical context from when this decision was made, not a reference to current code.

## Context

Losing a cache node today loses every key it held: each key lives on
exactly one node, so a node death converts that node's whole share of the
keyspace into misses at once — a thundering herd against whatever sits
behind the cache. The goal is availability (surviving a node death without
a miss storm), not durability; the source of truth lives elsewhere.

Separately, measuring the [[0002]] hash ring exposed that it is badly
skewed even with production (UUID) node names: FNV-1a's weak high-bit
avalanche clusters each node's 128 virtual points into narrow bands, and
measured deviation from a fair share reaches 42% at 2 nodes and 95% at 3
(one node holding ~2× its share, another ~⅓). That is a poor foundation to
stack replication on, and any fix to either is a wire-compatible break for
every ring participant — so both change together, once.

## Decision

### 1. Rendezvous hashing (HRW) replaces the ring

For each (node, key) pair, `score = fmix64(fnv1a(name) ⊕ fnv1a(key))`
(fmix64 is MurmurHash3's 64-bit finalizer). A key's owners are the R
highest-scoring nodes, in score order (ties, effectively impossible at 64
bits, break toward the lexicographically smaller name); its **primary** is
the top one. Measured deviation is under 2% at every cluster size, both
for primaries and for each replica rank. Virtual nodes, ring construction,
and binary search all disappear; a lookup is O(cluster size), irrelevant at
nanocached's scale. Adding a node never reorders existing nodes relative
to each other — it only inserts — which is what keeps both data movement
and the replica-cleanup rule (below) minimal and local. As with [[0002]],
every participant (SDK, node, bench) independently implements the same
computation and must agree on it exactly.

### 2. Client-side replication, `R` owned by discovery

`nanocached-discovery --replication-factor <n>` (default 2, min 1) is the
single source of truth for R. It travels in the two membership messages
that already exist: the `L` response header becomes `N <count> <r>\n`, and
the `M` migrate header becomes `M <name-len> <addr-len> <count> <r>\n`.
Clients learn R from `L`; nodes learn it from `M`. Nothing else needs
configuring, and R cannot skew between participants that are up to date
with the same discovery. R=1 reproduces today's behavior exactly.

The SDK, per operation on a cluster target:

- `set`/`delete` fan out to all R owners in parallel. The **primary's
  result is the operation's result**; replica failures are swallowed (a
  dead replica must not fail writes — the key is merely under-replicated
  until the next node-list refresh drops the dead node from the ranking).
- `get` asks the primary; only on a connection-level failure does it fall
  through to the next owner in rank order. A `W` answer keeps its existing
  meaning (topology stale → refresh and retry once). A plain miss is a
  miss — replicas are a hedge against a *dead* holder, not extra lookups
  on every miss.

Nodes stay independent: no node-to-node replication traffic exists. The
node-side `W` check becomes "am I in this key's top-R" instead of "am I
its owner".

### 3. Joins and replica cleanup ([[0008]] generalized)

When node N joins, a key is affected only if N enters its top-R. For each
such key, among the pre-join owners:

- the **old primary** sends the copy to N (one designated sender — no
  duplicate transfers, and the primary is the copy client writes always
  reached);
- the node **displaced from rank R to R+1** — the only node HRW can expel,
  and there is exactly one per affected key — marks its now-dead copy and
  sweeps it once its handoff duty completes, using the existing
  mark/unmark/sweep machinery. Marking is thus decoupled from sending: a
  displaced non-primary marks without sending; a primary that stays in
  top-R sends without marking. At R=1 the two coincide, which is exactly
  today's behavior.

Every node detects both sets by scanning its own store against the
old/new rosters carried by `M` — replica copies are found by the same scan
as primary copies, because the store does not distinguish them. Concurrent
writes racing a handoff forward to N whenever N is in the key's new top-R
(the existing [[0008]] forwarding, membership test widened).

On node **leave**, ranks only move up, so no surviving copy ever becomes
dead — there is nothing to sweep. Keys the departed node held become
under-replicated; there is deliberately no re-replication (anti-entropy):
new writes are fully replicated, reads are served by the surviving owners,
and TTL/LRU age the imbalance out. That is the cache-shaped trade.

## Consequences

- A node death no longer causes a miss storm: reads fail over to the
  key's next owner, which holds a copy of everything written since it
  entered that key's top-R.
- **Effective capacity is total memory ÷ R.** R is an
  availability-versus-capacity dial; the default R=2 halves capacity and
  survives any single node death with zero misses for replicated keys.
- Write traffic from clients multiplies by R (fan-out is client-side).
- Replicas are eventually consistent with their primary: a replica write
  can fail while the primary write succeeds (and vice versa never matters
  — primary defines the result). A failed-over read can therefore return
  a slightly stale value; TTLs bound the staleness, and last-write-wins
  is already this cache's model.
- The hash change plus the widened `L`/`M` headers are a **coordinated
  breaking change**: nodes, discovery, and SDK must be upgraded together,
  and on switchover the keyspace re-places itself — one cold-start-shaped
  miss bump, then it re-warms. No dual-ring migration mode is provided;
  the complexity is not worth it for a cache.
- Sweep gaps (`M` never delivered to a node) leave dead copies exactly as
  in [[0008]] today; LRU still guarantees they are the first casualties
  under memory pressure, and a restart clears everything. Under-replicated
  keys after a leave heal only via traffic and TTL — accepted, recorded
  here.
