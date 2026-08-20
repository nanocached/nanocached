# 10. Discovery HA via soft-state replicas and member announce

Date: 2026-08-17

## Status

Accepted — implemented. Amended 2026-08-20 to match the implementation
(issue #46): the client-side option is named `addresses` (not `seeds`)
and exists in all six SDKs; the startup grace is pinned to the liveness
timeout rather than carrying its own `--startup-grace` flag; and the
bootstrap-vs-refresh asymmetry for an address that answers as a cache
node is now written down.

Builds on [2. Client-side consistent hashing with a lightweight self-hosted discovery server](0002-client-side-consistent-hashing-with-a-lightweight-self-hosted-discovery-server.md),
[8. Staged node join with discovery-orchestrated data handoff](0008-staged-node-join-with-discovery-orchestrated-data-handoff.md),
and [9. Decouple node identity from network address](0009-decouple-node-identity-from-network-address.md)

## Context

`nanocached-discovery` is a single point of failure. The blast radius is
already deliberately small — [[0002]] makes clients keep serving from their
last-known node list when a refresh fails, and nodes keep serving cache
traffic when their heartbeats fail — but two things still break outright
while discovery is down:

1. **Bootstrap.** A new client has nowhere to fetch `L` from.
2. **Topology changes.** No node can join or be observed leaving.

There is also a recovery wrinkle. Discovery's registry is pure soft state
(in-memory, rebuilt from what nodes send it), which makes restarting it
cheap in principle — but the only way a node can re-register today is `J`,
the [[0008]] *join* command, which orchestrates a data handoff. After a
discovery restart every live node re-`J`s into an amnesiac registry one at
a time (joins are serialized), causing:

- a window where `L` returns a partial node list, so a client that
  bootstraps during recovery builds the wrong ring; and
- a cascade of pointless [[0008]] migrations — each rejoining node is
  treated as new, and the ring only converges back to where it already was
  after every node has "joined" again, shuffling data that was already in
  the right place.

The fix must fit [[0002]]'s constraints: lightweight, self-hosted, no new
runtime dependencies. Raft-style consensus or an external registry
(etcd/Consul) would solve this but are out of proportion for nanocached.

## Decision

Three cooperating changes, all resting on the observation that discovery
state is soft and node-sourced — replicas don't need to talk to each other,
because every replica converges on the same registry by independently
listening to the same nodes.

### 1. A new `P` (announce) command

`P <name-length> <addr-length>\n<name><addr>` — sent by a node that is
already a cluster member (it has been promoted via `J` → `R` at least once
in its process lifetime) to declare "I am a Joined member at this address".
Discovery upserts the node straight to `Joined` — no handoff, no
serialization through the [[0008]] join queue — replies `R\n`, and the
connection becomes an ordinary heartbeat connection, exactly like a `J`
connection after promotion. An announce for a name currently mid-join
(`Waiting`/`Joining`) is rejected; it would corrupt the join bookkeeping,
and no correct node sends it (a node announces only after promotion).

Node-side rule: the *first* registration in a process's lifetime is `J`
(its data really may need to be handed to it); every subsequent
re-registration — after a heartbeat connection breaks, including a
discovery restart — is `P`. This alone fixes the restart migration
cascade: an amnesiac discovery re-learns all live members within one
heartbeat interval, with zero data movement.

### 2. Discovery replicas, node-side fan-out, client-side seeds

`nanocached-node --discovery` now accepts a comma-separated list of
discovery addresses. The node registers with and heartbeats to *all* of
them, but asks to **join only the first** (the primary):

- Primary (first address): `J` → wait `R` → heartbeat; on any later
  reconnection, `P` → `R` → heartbeat. [[0008]] `C` completion reports
  also go to the primary only.
- Standbys (the rest): wait until the primary has promoted this node,
  then `P` → `R` → heartbeat, from the start.

Join orchestration ([[0008]] `M`/`X`) is therefore only ever driven by the
primary — replicas stay symmetric (no primary/standby *configuration* on
the discovery side; "primary" is purely the node-side list order), and the
one piece of state that genuinely needs a single writer keeps exactly one.
**Every node must list the discovery addresses in the same order**; this is
an operational requirement, not something the system verifies.

Every SDK's `connect()` takes an `addresses` option (a list of
discovery addresses tried in order, for both bootstrap and node-list
refresh), so losing any one discovery replica costs clients nothing.
*(As implemented: the option is named `addresses` — per each language's
casing — in all six SDKs, and the identifier `seeds` this ADR
originally proposed appears nowhere in the source.)*

When a node-list refresh brings in a node the client hasn't seen
before, *when* that node is dialed is a blessed per-SDK trade-off
(issue #47 item 2), not drift: Go and Rust dial lazily on first use (a
placeholder dead connection until then, favoring refresh latency),
while Python/TypeScript/Java/.NET dial eagerly during the refresh
(favoring first-request latency to the new node). Both are correct —
neither is observable as anything but a latency difference — so each
SDK keeps its shape.

One asymmetry, uniform across all six SDKs, is deliberate: when a
configured address answers `A` as a **cache node** rather than a
discovery server, bootstrap **stops** there — the client pins itself to
that single node (single-node mode, with a warning when more addresses
were configured), since a lone node cannot provide cluster routing but
is a perfectly good deliberate dev/single-node target. During a
node-list **refresh** the same answer is **skipped** and the walk
continues: an established cluster client must never silently collapse
to single-node mode because one address in its list turned out to be a
node.

### 3. A startup grace period for `L`

Until the grace elapses after process start, `L` is answered with the
existing busy byte `B\n` instead of a node list. *(As implemented: the
grace is pinned to `--liveness-timeout` and is not separately
configurable — the proposed `--startup-grace` flag was dropped. The
grace exists so every live member has had time to re-announce before
`L` is served, and that time is the liveness window by definition; a
separate knob would only invite setting it shorter than the value that
makes it correct.)*
Within one heartbeat interval of a restart every live member has
re-announced, so once the grace passes the registry is complete; answering
`B` in the meantime keeps a bootstrapping client from ever building a ring
out of a half-recovered registry. The SDK treats `B` from a discovery
server as "warming up" and moves to the next seed (an established client's
refresh already keeps its last-known list on any failure, including this
one). `P`/`J`/`H`/`C` are unaffected by the grace — recovery itself must
run during it.

## Consequences

- Discovery loses its two remaining failure modes for the data path:
  clients bootstrap and refresh through any live replica, and a discovery
  restart no longer migrates data or serves partial rings.
- **Joins still require the primary discovery to be up.** That is the
  accepted residual SPOF, and it only blocks topology *changes*, never
  cache traffic or client bootstrap. Failing join orchestration over to a
  standby is exactly the leader-election problem this ADR deliberately
  avoids; if it ever matters, that is a future ADR.
- Replicas converge but are not transactionally identical: two replicas
  may disagree about membership for up to roughly one liveness timeout
  (e.g. one has swept a dead node the other hasn't). [[0002]]'s existing
  staleness tolerance — clients poll and self-correct, nodes answer `W`
  for keys they don't own — already absorbs exactly this class of skew.
- Nodes configured with discovery lists in different orders can make two
  replicas orchestrate joins concurrently, which [[0008]] does not
  support. Documented, not enforced.
- A brand-new discovery (first boot, empty cluster) also serves its grace
  period, delaying the very first `L` by up to the liveness timeout — it
  cannot distinguish first boot from an amnesiac restart. Operators who
  care can shorten `--liveness-timeout`, which shortens the grace with
  it (they are the same window by definition; see Decision 3).
