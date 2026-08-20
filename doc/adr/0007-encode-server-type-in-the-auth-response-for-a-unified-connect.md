# 7. Encode server type in the auth response, for a unified connect()

Date: 2026-08-16

## Status

Accepted

Builds on [2. Client-side consistent hashing with a lightweight self-hosted discovery server](0002-client-side-consistent-hashing-with-a-lightweight-self-hosted-discovery-server.md)

`bench` (`src/bin/bench.rs`) was removed from the repository in #29 (a load-test client living in `src/bin/` read as product surface; see commit 97c83fe) — every mention of it below is historical context from when this decision was made, not a reference to current code.

## Context

The TypeScript client (`sdk/typescript`) was rebuilt from scratch after
a review found its previous design split into two unrelated classes —
`NanocachedClient` for a single node, `NanocachedClusterClient` for a
discovery-fronted cluster — with different `connect()` option shapes
(`{host, port}` vs `{discoveryHost, discoveryPort}`). Nothing in that shape
difference is actually meaningful to a caller: `get`/`set`/`delete`/`close`
are identical either way, and an application choosing between "run against
one local node" and "run against a discovery-fronted cluster" per
environment had to branch its own connection code to pick a class, with no
shared type to hold the result.

The requirement set for the rebuild: one class, one `connect()` shape,
usable against a node address or a discovery address without the caller
saying — or even being able to infer from the options shape or a port-number
convention — which kind of server it's dialing.

This requires the client to determine, from the connection itself, what
kind of server answered. Three designs were considered:

- **Probe by trial**: connect, send a discovery-only command (`L`), and see
  whether the connection is torn down (a node closes on an unrecognized
  command, per `src/server.rs`'s `handle_connection`) or answered (a
  discovery server responds). Verified experimentally to work, but a wrong
  guess (the common case — a node) always burns the connection: the
  identify attempt itself has to be redone.
- **Server-sent greeting banner** on connection, before any client command.
  Rejected: every existing consumer of this wire protocol
  (`src/bin/bench.rs`, `nanocached-node`'s own heartbeat connection to
  discovery in `server.rs`, and a dozen-plus unit tests) assumes the
  client always speaks first and reads a response of known length; an
  unsolicited banner would silently corrupt those reads.
- **Encode the type into the existing `A` (auth) response.** Both
  `nanocached-node` and `nanocached-discovery` already require every
  connection to send `A <len>\n<secret>` before anything else (a no-op that
  always succeeds if no `NANOCACHED_AUTH_SECRET` is configured — see
  [[0005]]), so this identification rides on a message the client sends
  anyway, on the very first round trip, without ever risking the
  connection.

The auth response, previously the same `O\n`/`E\n` on both node and
discovery, was widened to 3 bytes so the second byte carries the type:
`On\n`/`En\n` (node) and `Od\n`/`Ed\n` (discovery). This is a wire-format
change with no external compatibility burden (no released clients depend
on the old 2-byte form) but touches every reader of that response —
`nanocached-node`'s heartbeat client (which authenticates to discovery),
`bench.rs`'s `authenticate()` (talks to both node and discovery, and
correctly only checks the leading `O`/`E`, not the type byte, since it
already knows which it's dialing from its own CLI args), and the test
suites for both binaries.

## Decision

`Response::AuthOk`/`Unauthorized` (`src/response.rs`, used by
`nanocached-node`) encode as `On\n`/`En\n`. `nanocached-discovery`'s
inline auth responses (`src/bin/nanocached-discovery.rs`) encode as
`Od\n`/`Ed\n`. Both accept and process `A` identically otherwise — this is
purely an additional byte of information on an existing exchange, not a
new command or a new required step.

The TypeScript client (`sdk/typescript/src/identify.ts`) always sends
`A` on `connect()`, using the caller's `authSecret` if given, or a 1-byte
placeholder otherwise (a server with no secret configured accepts any
non-empty secret without inspecting it, so the placeholder authenticates
successfully there and is correctly rejected — same as any wrong secret —
against a server that does require one). The second response byte decides
what happens next: a node's socket is handed back live and ready for
`G`/`S`/`D`; a discovery server's connection is used once for `L` and then
discarded, exactly as before. `NanocachedClient.connect()` exposes none of
this — callers get one class, one options shape
(`{host, port, authSecret?, tls?}`), regardless of which they dialed.

## Consequences

Easier:

- `NanocachedClient` is a single public type for both a standalone node and
  a discovery-fronted cluster; switching an application between them (e.g.
  local dev vs. production) needs no branch in the caller's own connection
  code.
- Identification never costs a wasted connection or a redone handshake —
  it's free-riding on the `A` round trip every connection already makes.
- The mechanism is purely additive to the wire protocol: existing `G`/`S`/`D`
  and `L`/`H` command handling is untouched.

Harder / risks to mitigate:

- Every place that reads an auth response (not just clients) had to change
  in lockstep with this — missed in [[0005]] once already for the
  heartbeat connection's own auth check, and hit again here. There is no
  compile-time guard against a future new connection type reading the old
  2-byte form; it would just misparse.
- `bench.rs` and any other future Rust consumer of this protocol must
  remember to check only the leading `O`/`E` when they don't care which
  kind of server they're talking to (they usually already know from their
  own configuration), not the full 3-byte response, or they'll need to
  special-case both node and discovery variants for no reason.
