# 9. Decouple node identity from network address

Date: 2026-08-16

## Status

Proposed — not yet implemented or verified. [[0008]] depends on this: its
node-to-node handoff cannot compute a correct ring diff without it.

Builds on [2. Client-side consistent hashing with a lightweight self-hosted discovery server](0002-client-side-consistent-hashing-with-a-lightweight-self-hosted-discovery-server.md),
[7. Encode server type in the auth response, for a unified connect()](0007-encode-server-type-in-the-auth-response-for-a-unified-connect.md),
and [8. Staged node join with discovery-orchestrated data handoff](0008-staged-node-join-with-discovery-orchestrated-data-handoff.md)

## Context

[[0002]] and everything built on it so far (the TypeScript client's
`HashRing`, `bench.rs`'s `HashRing`, and now [[0008]]'s node-to-node handoff)
uses a node's advertised network address (`advertise_addr`, `host:port`) as
both: (a) how to open a connection to it, and (b) its identity for consistent
hashing — the string fed into `HashRing::new`.

Folding out [[0008]]'s node-to-node handoff surfaced that (b) breaks once a
node's address isn't the same string everywhere it's seen. On a single local
machine this never comes up — one process, one `host:port`, seen identically
by every client and every other node. On AWS (or any deployment where a
node's address can be seen differently depending on who's looking — a
private IP within a VPC vs. a load-balanced or NAT'd endpoint from outside
it) it does: if a client's hash ring and a node's hash ring are built from
different address strings for what is actually the same node, the two rings
disagree about which node owns a given key. A client would then believe a
node holds a key that the node itself never received (or vice versa) —
silent data unavailability, not just an extra cache miss, since the client
is confidently asking the wrong node and getting a miss it doesn't expect.

This is strictly worse for [[0008]]'s handoff than for ordinary routing:
ordinary client routing only needs the *same* client to be internally
consistent between polls, which [[0002]]'s existing staleness handling
already covers. Handoff requires a *ready node* and *every client* to agree
on ring membership using literally the same identifier for the same node,
or the handoff computes the wrong diff.

A per-node explicit `--name` flag was considered and rejected: it pushes a
uniqueness obligation onto whoever deploys nanocached-node (pick a name,
guarantee no collision with any other node, ever) for no benefit over
generating one automatically — kvelo already has no persistence
requirement ([[0002]]'s Context), so there is nothing a stable,
operator-chosen name would preserve across a restart that isn't already
lost on restart anyway (a restarted node holds no data, identical in effect
to a brand new node joining under a brand new identity).

## Decision

A node's consistent-hashing identity and its network address are two
different things from now on:

- **Name**: a random, process-lifetime identifier — a v4 UUID, generated
  once at startup via the new `uuid` dependency, never persisted. This is
  what `HashRing::new` is keyed on, everywhere: the TypeScript client, both
  Rust reference implementations (`bench.rs`, `src/hash_ring.rs`, per
  [[0006]]'s independent-copy convention), and a ready node computing
  [[0008]]'s handoff diff. Because it isn't derived from network
  configuration, every party that learns it from discovery is guaranteed to
  agree on it, regardless of how a node's address happens to be seen
  differently by different observers.
- **Address**: unchanged from [[0002]] — `advertise_addr`, used only to
  open a connection (a client's `G`/`S`/`D`, or, per [[0008]], a ready
  node's `SET`-based handoff to the joining node). Carries no identity
  meaning anymore.

A node generating a new name on every restart is intentional, not a gap:
[[0002]]'s registry is already fully rebuilt from scratch on every restart
(node or discovery), and a restarted node has no data to reclaim its old
identity for.

This changes every wire message that currently carries just an address:

- `H`/`J`/`C` (discovery protocol, [[0008]]) become two length-prefixed
  fields instead of one: `<cmd> <name-length> <addr-length>\n<name><addr>`.
- `L`'s response carries `<name-length> <addr-length>\n<name><addr>` per
  entry instead of a bare `<addr>\n` line (the `N <count>\n` header is
  unchanged).
- `M` ([[0008]], not yet implemented) carries a snapshot of the relevant
  nodes as `(name, address, state)` tuples, using the same two-field
  framing per entry.

`nanocached-discovery`'s registry is keyed by name, with address stored as
a field alongside a node's state. Client SDKs build their hash ring from
the list of names in `L`'s response, and separately keep a name → address
map to actually dial a node once the ring has picked it.

## Consequences

Easier:

- A client's and a node's hash ring computations agree by construction,
  independent of how divergently a node's address is observed from
  different vantage points — the identity fed into `HashRing::new` no
  longer depends on network topology at all.
- No new operational burden: names require no operator input and no
  uniqueness discipline to get right (a UUID collision is not a practical
  concern).

Harder / risks to mitigate:

- Every reader of `H`/`J`/`L`/`C` changes again, on top of [[0007]]'s
  already-once-repeated lesson that this class of change touches
  `nanocached-discovery`, `nanocached-node`'s heartbeat client, the
  TypeScript client, `bench.rs`, and their test suites, all in lockstep.
- New dependency: `uuid` (with a CSPRNG backend for `Uuid::new_v4`), the
  first addition to `Cargo.toml` made specifically for this cluster
  machinery rather than the cache server itself.
- A node's identity changing on every restart means discovery/clients must
  treat a restarted node as an unrelated new node needing its own
  [[0008]] join — restarting a `Joined` node is not a lighter-weight
  operation than adding a brand new one, and doesn't try to be.
- Whether a node's `advertise_addr` is always reachable both by clients
  and by other nodes (the assumption [[0008]]'s handoff currently makes,
  reusing the client protocol) is still open. This ADR only removes
  *identity* from being address-dependent; it does not introduce a second,
  node-facing address. If local-then-AWS verification (per [[0008]]) shows
  client-reachable and node-reachable addresses must differ, that is
  follow-up work, not addressed here.
