# 14. Optional fire-and-forget replica writes in the SDKs

Date: 2026-08-19

## Status

Accepted

## Context

[[0011]] fans a write out to a key's top-R owners concurrently, but every
SDK's `write()` still waits for every replica leg to finish before
returning — the primary's outcome decides the operation's result, replica
failures are already swallowed (a dead replica just leaves the key
under-replicated until the next node-list refresh), so waiting for them
buys nothing but write latency. Write latency is therefore
`max(primary, slowest replica)` instead of just the primary's, purely
because of how each SDK currently sequences the wait, not because of
anything the protocol requires.

An opt-in mode that resolves the write as soon as the primary acks, and
lets replica legs keep running in the background, removes that tax with
no change to what already-accepted risk the design carries (ADR-0011's
consequences already accept a replica lagging or dying without failing
the write). Two things need care that the current fully-synchronous
design gets for free:

- **Unbounded background work.** Without a cap, a client issuing writes
  faster than replicas can keep up accumulates an ever-growing number of
  in-flight background writes — unlike the synchronous path, which
  self-throttles the caller by construction.
- **`close()`/`Dispose()` dropping in-flight work.** A caller that writes
  and immediately closes must not silently lose replication commitments
  that were never even given a chance to leave the process.

The six SDKs (TypeScript, Python, Java, Rust, .NET, Go) split into two
groups on how their existing `close()` already behaves, discovered while
designing the draining behavior below:

- **Go, Java, .NET** run on native OS threads with no async-runtime
  re-entrancy hazard, and their `close()`/`Close()`/`Dispose()` already
  synchronously tears down every connection before returning.
- **Rust, Python, TypeScript** run on a single-threaded or cooperative
  async runtime, and — independently of this issue — already avoid
  blocking inside `close()`: Rust's `close()` uses `try_lock` and falls
  back to a spawned task rather than block if the connection state is
  briefly contended (`client.rs`'s `close_all_connections` caller);
  Python's `close()` calls the underlying `StreamWriter.close()`, itself
  non-blocking; TypeScript's `close(): void` returns `undefined`, not a
  `Promise`, so it structurally cannot await anything. A synchronous
  method in these three cannot block-wait on outstanding async work
  without either changing its public signature or re-entering the event
  loop (both worse than the problem being solved).

## Decision

Each SDK gets one new opt-in option, off by default, named per that
language's own convention: `fireAndForgetReplicas`/`fire_and_forget_replicas`/
`FireAndForgetReplicas` (bool). Unlike [[0013]]'s `compress`, this is a
pure latency/durability trade a client makes for its own writes — it
carries no wire format and no cross-client agreement requirement; two
clients with different settings interoperate exactly as they do today.

**Write path.** In the existing per-owner fan-out (each SDK's
`write`/`writeToOwners`/`_write`), when `fireAndForgetReplicas` is
enabled, each replica leg — before being awaited inline like today —
first tries to claim one of a fixed 32 per-client
"in-flight background replica write" slots:

- **Slot claimed:** the replica leg runs detached, outside the write
  call's own await chain; the caller returns as soon as the primary
  acks, without waiting for this leg. Its result is swallowed exactly
  like every replica result already is (ADR-0011) — success or failure
  makes no difference — and it releases its slot on completion.
- **No slot free (32 already in flight):** the replica leg runs exactly
  as it does today — awaited inline, before the write call returns. This
  is a deliberate graceful degradation, not a queue or a drop: under
  sustained backpressure the client degrades toward today's fully
  synchronous behavior instead of accumulating unbounded background work
  or losing a replica write outright. 32 is a fixed constant, not
  user-configurable — small enough to bound worst-case resource use,
  large enough not to matter for any workload this project has actually
  seen; a follow-up issue can make it tunable if that ever changes.

This applies to both `set` and `delete`, since both go through the same
owner fan-out. Reads and the primary leg of a write are unaffected.

**Close draining**, split along the grouping in Context:

- **Go, Java, .NET:** `Close()`/`close()`/`Dispose()` blocks — same as it
  does today — until every currently in-flight background replica write
  finishes, *then* tears down connections. Bounded by the 32-slot cap
  above, so the added wait is bounded and, in practice, short (`Go`:
  `sync.WaitGroup`; `Java`: a tracked `Future`/latch join; `.NET`: an
  awaited `Task` set).
- **Rust, Python, TypeScript:** `close()` keeps returning immediately —
  unchanged public behavior, consistent with how each already avoids
  blocking there (Context). Instead of tearing connections down
  immediately, the actual teardown is deferred until the outstanding
  background writes using them settle (Rust: extends the existing
  spawned-task fallback to also await outstanding background write
  handles first; Python: schedules a coroutine via `ensure_future` that
  awaits the pending task set before tearing down, the same pattern
  `close()` already uses for other async cleanup; TypeScript: connections
  are closed inside a `.finally()`/`Promise.allSettled()` continuation of
  the outstanding background writes rather than synchronously inline).
  A background write's connection may still be forcibly severed if the
  underlying socket errors independently mid-flight; that failure is
  swallowed the same as any other replica failure.

## Consequences

Easier:

- Write latency drops to the primary's alone for clients that opt in,
  with no change to what ADR-0011 already accepts about replica
  durability.
- No wire or server change — this is purely how each SDK sequences work
  it was already doing.

Harder / risks to mitigate:

- A client under sustained write pressure with `fireAndForgetReplicas`
  on will see some fraction of replica legs fall back to synchronous
  once the 32-slot cap is saturated — expected, not a bug; it's the
  backpressure valve, not a queue.
- `close()`'s draining behavior is not textually identical across the
  six SDKs (three block, three defer) — a deliberate, documented split
  driven by each runtime's own pre-existing `close()` shape (Context),
  not an oversight. The externally observable guarantee is the same in
  both groups: a background replica write that had already started is
  given the chance to finish before its connection is torn down.
- Every SDK's shared test matrix gains cases for: default-off behavior
  unchanged, a write returning before a slow replica leg completes when
  enabled, the 32-slot degrade-to-synchronous path, and close() not
  losing an in-flight background write.
