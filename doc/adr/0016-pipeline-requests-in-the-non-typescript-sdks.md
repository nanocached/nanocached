# 16. Pipeline requests in the non-TypeScript SDKs

Date: 2026-08-19

## Status

Accepted — implemented in all five SDKs. Clarified 2026-08-20 (issue
#46): the Context below describes the pre-decision state; today **all
six** SDKs pipeline identically — TypeScript's `connection.ts` `send()`
claims a tag, pushes a waiter onto the FIFO and writes the frame
immediately, and the other five match it per the Decision — so no SDK
serializes requests per connection anymore. Amended by [[0019]] (echoed
response tags close the desync window noted in Consequences).

## Context

Five of the six SDKs (Python, Java, Rust, .NET, Go) serialize requests
per connection: each `get`/`set`/`delete` holds a lock for the full round
trip (write the frame, then block reading the matched response), so a
second concurrent caller on the same connection queues behind the first
one's full network latency instead of behind just its write. This was a
documented v1 simplification — always correct, since nanocached-node
only ever answers in the order it received requests, just not as
concurrent as it could be.

The TypeScript SDK already does better: `Connection.send` pushes a
waiter onto a FIFO queue and writes the frame, without waiting for the
response; a single `data` handler parses whatever bytes arrive and
resolves the oldest queued waiter for each complete frame, trusting
`nanocached-node`'s own in-order guarantee to keep queue order and wire
order aligned. Multiple requests can be in flight on one connection at
once; each pays only its own network latency, not everyone else's ahead
of it in line.

Porting this needs a queue and something to drain it in each SDK's own
concurrency model — a dedicated reader (task, goroutine, or thread)
that owns the socket's read side for the connection's whole lifetime,
paired with a FIFO of pending response slots. The one invariant every
port must preserve, because the read side has no request IDs to check
against: **the order responses get matched to callers must exactly
match the order their frames were actually written to the socket** — not
just the order callers *started* their request. Every port therefore
serializes "claim the next queue slot" and "write the frame" into one
atomic step per connection (a single lock spanning both, mirroring how
TypeScript's single-threaded event loop makes `send`'s enqueue-then-write
atomic for free); the read side never shares that lock across an actual
blocking I/O read, so a slow writer can't stall the reader dispatching
already-arrived responses, and vice versa.

TypeScript's design also has a known, accepted limitation this port
keeps rather than fixes: if a response turns out to be the wrong *kind*
for its request (a desync — `mismatch()`), the connection is poisoned,
but the read side doesn't pause to let the caller notice before
dispatching whatever's already parsed for requests queued behind it —
those may already have been resolved with misaligned data by the time
the mismatch is caught. This is inherent to matching-by-order without
request IDs, not something any of these ports introduces or could fix
without a wire change; ADR-0013's mismatch-caveat precedent (raw
DEFLATE's own best-effort error detection) is the same shape of
trade-off.

## Decision

Each SDK's connection type gets an internal FIFO of pending response
slots and a dedicated reader that owns the socket's read side for the
connection's lifetime, replacing the per-request round-trip lock:

- **Go**: `connection.request` pushes a buffered `chan roundTripResult`
  onto `pending` and writes the frame under one `sync.Mutex`; a goroutine
  started in `newConnection` (`readLoop`) parses responses off the wire
  and sends each to the oldest pending channel. `poison` (renamed from
  the old `mismatch`-only close path) closes the socket and drains every
  still-pending channel with the same error, called from either side —
  a write failure, a read failure, or a caller-detected mismatch.
- **Python** (asyncio) and **Rust** (tokio): the same shape, with each
  runtime's native single-producer queue primitive (an `asyncio.Future`/
  `oneshot` channel per request) and a background task as the dedicated
  reader. Both runtimes support a caller abandoning a request mid-flight
  (`asyncio.wait_for`, `tokio::time::timeout`) by actually dropping the
  awaiting future, which this design turns into a real improvement over
  the old serialized behavior rather than just preserving it: abandoning
  a request *after* its write has gone out no longer needs to poison the
  connection at all — the slot is simply left in the queue, and the read
  task dispatches its eventual response to no one in particular once it
  notices no one is listening (a dropped `Future`/dropped `oneshot::Receiver`)
  and moves on; every request queued behind it is unaffected. Only
  cancellation *mid-write* (the frame possibly only partially on the
  wire) still poisons the whole connection — Rust needs an explicit RAII
  guard around the write to detect this since a dropped `async fn`
  future gives no other hook; Python's `except asyncio.CancelledError`
  around the same span does the equivalent.
- **Java** (threads): a dedicated reader thread per connection; callers
  block on a `CompletableFuture` (or equivalent) pushed onto the FIFO
  under the same lock that guards the write. Java exposes no
  request-cancellation API on this blocking path, so this scenario
  doesn't arise.
- **.NET** (Task): the same shape, with a `TaskCompletionSource` per
  request queued in a `ConcurrentQueue` and a background `Task` as the
  dedicated reader. Simpler still than Python/Rust: since none of this
  SDK's `Stream` calls are given a `CancellationToken`, a write, once
  started, always runs to completion regardless of what the caller does
  afterward (a `Task.WaitAsync(timeout)`-style wrapper only stops
  *awaiting* the underlying task, it doesn't stop it) — so there is no
  mid-write cancellation case to guard against at all, and completing an
  abandoned `TaskCompletionSource` that nothing is awaiting anymore is
  already harmless by construction.

No wire or protocol change — this is purely how each SDK sequences work
it was already doing, exactly like [[0014]] and [[0015]] before it. The
public `Connection`/`connection` API (`get`/`set`/`delete`,
`isClosed`/`close`) is unchanged in every SDK; `NanocachedClient` itself
needs no changes, since it already only calls those methods.

Every SDK's test suite gains a version of TypeScript's own pipelining
test: N concurrent requests on one connection, each independently
verified to round-trip its own value — a matching-order bug would show
up as swapped or wrong values here, not a crash. Existing
desync/mismatch tests (a well-formed response of the wrong kind poisons
the connection and the client's retry layer redials) must keep passing
unchanged, proving the port didn't alter observable behavior, only
concurrency.

## Consequences

Easier:

- Per-connection throughput parity with the TypeScript SDK: concurrent
  callers on one connection now pay their own network latency, not the
  cumulative latency of everyone queued ahead of them.
- No API change for callers of any of these five SDKs — this is purely
  an internal connection-layer change.

Harder / risks to mitigate:

- Every port takes on TypeScript's own desync limitation (Context): a
  caller-detected mismatch can't retroactively un-resolve responses the
  reader already dispatched to requests queued behind it. Existing
  mismatch tests only exercise the single-request-in-flight case, where
  this never shows up — a latent gap in coverage this ADR doesn't
  attempt to close, consistent with matching TypeScript's behavior
  exactly rather than exceeding it. *Since closed by [[0019]]: echoed
  response tags let the read loop verify alignment before dispatch on
  negotiated connections; only connections to pre-0019 servers retain
  this window.*
- The write side is still a single critical section per connection in
  every port (Context) — pipelining removes the *read* wait from the
  critical path, not the write. A connection whose writes themselves
  are slow (a saturated socket buffer) still serializes callers on that,
  same as TypeScript's single-threaded writes would.
- Each SDK's own concurrency primitives (goroutines/channels,
  asyncio tasks/futures, tokio tasks/channels, .NET Tasks, Java
  threads/`CompletableFuture`) are different enough that the ports are
  not line-for-line identical — the invariant in Context (queue order
  matches wire order) is what's actually shared, not the code shape.
