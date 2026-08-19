# 17. Size-derived migration timeout and forwarding grace

Date: 2026-08-19

## Status

Accepted

## Context

`nanocached-discovery`'s `--migration-timeout` (default 60s, ADR-0008
pattern 3) abandons a join if a ready node goes quiet — alive, but never
reports `C` for the handoff it's mid-`Migrate` for. It's a flat bound on
total elapsed time since the join started, regardless of how much data
that handoff actually moves. An earlier option-trim review wanted this
flag gone entirely, but removing it outright without changing the
mechanism is dangerous the other way: a large node's legitimate join
would get aborted repeatedly, forever unable to finish (issue #24).

Two designs were considered to replace it:

- **Progress-based abandonment** — track the last time any ready node
  reported partial progress, resetting the abandonment clock on each
  signal, instead of measuring from a single start time. This needs a new
  wire message reporting incremental progress mid-transfer. It also has a
  blind spot the flat timeout didn't: with only one ready node in the
  `expected` set (`R=1`, the common case for a small cluster), that one
  node's handoff is the *entire* join — there's no second node's
  completion to signal progress against, so this design degrades to "wait
  for the one node to finish" exactly like the timeout it was meant to
  improve on, just with extra wire complexity.
- **Size-derived timeout** — a ready node reports how many entries it's
  about to transfer once, before starting; discovery sizes the bound as
  `base + entries × per-entry-budget` instead of a flat constant. No
  blind spot for `R=1` (the bound already accounts for that node's own
  data volume), and the wire change is a single count on an acknowledgment
  that already exists (`M`'s ack), not a new message type.

This ADR adopts the size-derived design.

`src/server.rs`'s `FORWARDING_GRACE` (also 60s) is a second, related
constant: after a ready node finishes its own share of a handoff, it
keeps forwarding concurrent client writes to the joining node for this
long, covering the window before every *other* ready node has also
finished and discovery has promoted the joiner (issue #3). It was
deliberately set to mirror `--migration-timeout`'s default 1:1 — a
carry-over from #26 flagged that whatever replaces the timeout must
redesign this pairing too, since a fixed 60s forwarding window would
silently stop covering a handoff that the new size-derived timeout is
happy to let run past 60s.

A second #26 carry-over constrains the redesign: the SDKs' node-list
staleness bound (30s, all six SDKs, ADR-0009-adjacent) must stay strictly
below whatever bounds the post-handoff forwarding window — that ordering
is what guarantees a stale client's write to the old owner still reaches
the joiner before forwarding stops.

## Decision

**`M`'s acknowledgment carries an entry count.** `Response::MigrationAccepted`
changes from a unit variant to `MigrationAccepted(usize)`, encoded as
`A <entries>\n` instead of a bare `A\n` (`MigrationCancelled`'s `X` ack is
unaffected — still a bare `A\n`, a different variant that happens to
share that encoding). Before writing this ack, the node counts how many
of its own entries it's actually the designated sender for (the same
sender/displaced predicate `run_migration` itself computes, applied as a
one-off count rather than a transfer) — this happens after `list_entries`
but before any key is actually sent, so it costs one extra cache-task
round trip per `M`, not an extra network hop. A concurrent write racing
this snapshot can shift the true count slightly; `run_migration`'s own
transfer loop re-checks every key live regardless (already true before
this change), so the count is only ever a timeout-sizing estimate, never
a transfer plan.

**Discovery derives the join's timeout from the largest count reported.**
`PendingJoin` gains `max_entries: usize` (starts at 0), updated to the
max of itself and each ready node's reported count as `M`'s acks arrive
in `try_begin_next_join`'s parallel sends. Since every ready node
transfers concurrently, the join's total duration tracks whichever one
has the most to send, not the sum. `sweep_expired` computes
`MIGRATION_TIMEOUT_BASE (60s) + MIGRATION_TIMEOUT_PER_ENTRY (5ms) ×
max_entries` per tick instead of comparing against a flat injected
`Duration` — `--migration-timeout` is removed from the CLI entirely,
matching the option-trim review's actual goal (removing the knob, not
just renaming it). Both constants are hardcoded, not configurable: a
per-entry network-speed budget is inherently a rough, defensible-by-
convention number, not something a per-cluster flag would make more
correct — 5ms/entry gives an empty-ish join the same ~60s bound as
before and lets a 100k-entry join run for roughly 8 minutes before
being reaped, without needing a human to tune it per deployment.

**`FORWARDING_GRACE` becomes size-derived using the same shape, computed
locally.** `ActiveMigration` gains `forwarding_grace: Duration`, set
together with `completed_at` (`MigrationGuard::completed`) from
`FORWARDING_GRACE_BASE (60s) + FORWARDING_GRACE_PER_ENTRY (5ms) ×
entries_sent` — the same two constants' values as discovery's, kept in
separate declarations (see below), using the node's *own* transfer count
(`run_migration`'s `sent_count`), not anything reported by other ready
nodes. This is a deliberate, narrower fix than #26's carry-over comment
ideally wanted (an active "join fully complete, stop forwarding" signal
from discovery to every ready node) — that would need a new broadcast
message and touches every ready node's connection lifecycle, a
meaningfully bigger change than pairing this constant with the timeout
it was always meant to mirror. Left as explicit future work, not
attempted here (see Consequences).

Both binaries declare their own copies of the base/per-entry constants
(`nanocached-node` and `nanocached-discovery` share no modules by
design — see `nanocached-discovery.rs`'s own module doc comment), cross-
referencing each other and this ADR in their doc comments so they're
kept in sync by convention, the same way the flat 60s constants they
replace already were.

The `MIGRATION_TIMEOUT_BASE` staying at 60s keeps it comfortably above
the SDKs' 30s node-list staleness bound regardless of `max_entries` (the
per-entry term only ever grows the bound), preserving the ordering the
second #26 carry-over requires — this ADR doesn't change that bound or
need to.

## Consequences

Easier:

- A large node's legitimate join no longer gets aborted purely for being
  large — the timeout scales with what it actually has to move.
- `--migration-timeout` is gone, per the original option-trim goal, with
  nothing configurable added back in its place.
- An `R=1` cluster (the size-derived design's specific advantage over the
  progress-based alternative) gets a correctly-sized bound with no
  special-casing.

Harder / risks accepted:

- The per-entry budget (5ms) is a hardcoded, network-speed-dependent
  guess, not a measured or configurable value — a deployment on
  unusually slow links could still see a large, legitimate join time out.
  This is the same class of trade-off the old flat 60s constant already
  made (also a guess), just a better-shaped one.
- The reported entry count is a snapshot taken once, before transfer
  starts; it doesn't shrink if concurrent deletes remove keys mid-handoff,
  so the derived timeout can end up somewhat more generous than the
  handoff strictly needs. Never less generous, which is the direction
  that matters for this ADR's goal.
- `FORWARDING_GRACE`'s redesign only fixes the pairing with the timeout;
  it's still a timer, not a signal tied to the join's actual cluster-wide
  completion, same limitation issue #3 always had. A discovery-driven
  "stop forwarding" broadcast remains a distinct, larger future change.
