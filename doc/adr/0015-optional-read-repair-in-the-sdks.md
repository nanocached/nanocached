# 15. Optional read repair in the SDKs

Date: 2026-08-19

## Status

Accepted. Amended 2026-08-20 to match the implementations (issues #46
and #43): the repair write carries a 60-second TTL rather than "no
TTL", and the `readRepairFailures` stats counter is defined to count
failed repair write-backs only.

## Context

[[0011]] deliberately ships without read repair: `get`/`get_bytes` asks
only the first owner it can successfully reach, in rank order, and
accepts whatever that owner says — including a clean "not found." When a
primary restarts empty (a fresh node identity per [[0009]], or the
ranking shifting under it), a key a replica still holds reads as missing
until the next write or TTL re-converges it. This is a real, if narrow,
window: it only opens right after a primary-affecting membership change,
and only for keys nobody has written since.

An opt-in mode that probes the remaining owners on a clean primary miss,
and repairs the gap by writing the found value back to the primary in
the background, closes that window at the cost of extra reads on
misses — exactly the trade the issue asks for. Two things distinguish
this from [[0014]]'s fire-and-forget replica writes, both of which keep
this feature simpler than that one:

- **Frequency.** A fire-and-forget replica write happens on every single
  write when enabled. A read-repair write only happens when a read
  misses on its first-reached owner *and* a later owner has the value —
  the narrow post-restart window above. There is no realistic scenario
  where this fires on every request, so the backpressure concern
  [[0014]] needed (bounding in-flight background writes, degrading to
  synchronous past a cap) doesn't apply here.
- **Stakes.** A fire-and-forget replica write is real replication work —
  losing one on `close()` under-replicates a key exactly as a dead
  replica already can (an accepted [[0011]] risk, but a real one).
  Losing a read-repair write on `close()` costs nothing beyond staying in
  the same window this feature narrows for one more read — the read
  itself already returned the correct value from the replica that had
  it. This is why read repair needs no [[0014]]-style `close()` draining:
  a truly best-effort, fire-and-forget background write with no tracking
  is enough.

Two things this can't get right, from the wire protocol itself:

- **TTL.** `G`'s response (`docs/protocol.html`) is `V <length>\n<value>`
  or `N\n` — it never carries remaining TTL. A repair write therefore
  cannot preserve the original expiry. *(As implemented, in all six
  SDKs: the repair writes with a fixed **60-second TTL**, not "no TTL"
  as this ADR originally said. TTL 0 — no expiry — would permanently
  immortalize a key that was legitimately expiring; 60s bounds the
  overshoot instead, and a key that outlives its repair TTL simply gets
  re-repaired on a later miss. This is the shared cross-SDK policy.)*
  Either way the original expiry is unrecoverable — a real, permanent
  limitation of the current wire protocol, not an implementation gap.
- **Which owner actually missed.** By the time `get`/`get_bytes` sees a
  clean miss, the normal read path (unrelated to this feature) has
  already walked past any owner that was merely unreachable and landed
  on the first one it could talk to — which one that was isn't
  surfaced. Rather than plumb that detail out of the hot path for every
  read, read repair re-walks the full owner list from the top when it
  activates; the owner that already answered "not found" gets asked
  again once, redundantly. This only happens on an already-rare path
  (a clean miss with the feature enabled), so the extra round trip isn't
  worth avoiding at the cost of complicating every read.

## Decision

Each SDK gets one new opt-in option, off by default, named per that
language's own convention: `readRepair`/`read_repair`/`ReadRepair`
(bool).

**Read path.** `get`/`get_bytes` is unchanged when the normal owner walk
returns a hit, an error, or `read_repair` is off. When it returns a
clean miss (no error, key not found) *and* `read_repair` is on:

1. Walk every owner of the key, in the same rank order [[0011]] already
   ranks them in, asking each in turn. Any failure — connection lost,
   `WrongNode`, another miss — is swallowed and the walk moves to the
   next owner; nothing here is allowed to turn a miss into an error, or
   to slow down or fail the read repair itself already fell back to
   being a plain miss.
2. The first owner that returns a value wins: that value is returned to
   the caller (the read is now a hit), and — detached, not awaited, no
   in-flight tracking, no `close()` draining (Context) — that same value
   is written back to `names[0]` (the true primary, regardless of which
   owner in the walk actually had it) with a 60-second TTL (see the TTL
   note in Context). Errors from this write are swallowed the same way
   every other best-effort write in this codebase is ([[0011]]'s replica
   writes, [[0014]]'s background replica writes).

Observability (issue #43): each SDK's `stats()` exposes a
`readRepairFailures` counter, and it counts **failed repair write-backs
only** — a swallowed failure in step 2's background write. Failed owner
probes during step 1's walk are swallowed silently and are *not*
counted; they are the normal texture of walking a cluster with a dead
member, not a failed repair. This definition is uniform across all six
SDKs.
3. If no owner has it, the result is what it already was: a clean miss.

This operates purely on wire bytes, below [[0013]]'s compression layer —
repair copies whatever bytes the source owner returned, verbatim, to the
primary; it neither decompresses nor recompresses them. A `compress`-on
client's repair writes are exactly as valid as its normal writes for the
same reason `get`/`set` already are.

Single-node mode is unaffected — there are no other owners to probe.

## Consequences

Easier:

- Closes the [[0011]] post-restart miss window described in Context, at
  the cost of extra reads only on the misses that actually hit it — the
  common case (primary has the key) pays nothing extra.
- No wire or server change — this is a client-side read pattern, exactly
  like [[0011]]'s replication and [[0013]]'s compression before it.
- Meaningfully simpler than [[0014]]: no bounding, no permit tracking,
  no `close()` hook — justified in Context by how rarely this actually
  fires and how little is lost if a repair write is abandoned.

Harder / risks to mitigate:

- **A repaired key loses its original TTL** (Context) — a real,
  permanent consequence of the wire protocol not exposing remaining TTL
  on `G`, not something a future SDK change can fix without a protocol
  change. The repaired copy lives for the fixed 60-second repair TTL
  instead, so callers relying on TTL-bounded staleness for keys that
  might hit this path should treat that guarantee as best-effort (off
  by up to 60s), not absolute, once `read_repair` is enabled.
- A clean miss with `read_repair` on costs up to R sequential round
  trips instead of one, worst case (every owner unreachable or missing).
  Still bounded and small (R is typically 2-3), but not free — this is
  the exact trade the issue asks for, not a hidden cost.
- Unlike [[0013]]'s `compress`, this needs no cross-client agreement —
  it only changes what a client with it enabled does about its *own*
  misses, and every write it issues (the repair write included) is a
  perfectly ordinary write any other client, with or without
  `read_repair`, can read normally.
