# 19. Echoed response tags close the pipeline desync window

Date: 2026-08-20

## Status

Accepted — implemented on `feat/response-tags` (issue #35).

Builds on [7. Encode server type in the auth response for a unified connect](0007-encode-server-type-in-the-auth-response-for-a-unified-connect.md)
and [16. Pipeline requests in the non-TypeScript SDKs](0016-pipeline-requests-in-the-non-typescript-sdks.md)

## Context

[[0016]] pipelined every SDK's connection by trusting nanocached-node's
in-order guarantee: the read loop mechanically resolves each parsed
response against the oldest pending slot, because the wire carries no
request IDs to check against. Its Consequences named the residual gap
this leaves open: a desync (a well-formed response of the wrong *kind*)
is only detectable at the **caller**, after the read loop has already
dispatched whatever else was parsed — so requests queued behind the
misaligned one may have been resolved with plausible, wrong,
exception-free values by the time the connection is poisoned. Silent
wrong data, not an error.

The comment audit that produced issue #35 confirmed this trade-off was
explicitly deferred by [[0016]] ("not something any of these ports
introduces or could fix without a wire change") and judged the risk
under-weighted: every other audit finding was fixed, leaving this as the
one deliberate gap. Closing it requires exactly the wire change [[0016]]
declined: something in each response the read loop itself can match
against the request it's about to answer.

## Decision

Add an opt-in, per-connection wire extension: the client generates a
sequence number (tag) per request, and the server echoes it in the
response, so the read loop can verify request/response alignment
**before** handing the response to any caller.

### Negotiation, on the existing `A` exchange

- A tag-aware client authenticates with `A <len> T\n<secret>` — the
  existing auth frame plus a second header field, the literal `T`.
- A server that parses the extended form echoes the capability in its
  identity reply ([[0007]]) by inserting `T` before the LF: `OnT\n` /
  `EnT\n` from a node, `OdT\n` / `EdT\n` from discovery — four bytes
  instead of three, sent only to clients that asked. An accepted `A`
  with `T` puts that node connection in **tagged mode** for its
  lifetime.
- An older server rejects the extra field as a parse error and closes
  the connection without replying. A tag-aware client that hits
  close-before-reply on the extended `A` redials once with the plain
  form and runs the connection untagged — transparent fallback, keeping
  [[0016]]'s exact behavior (residual window included) against old
  servers only.
- An older client never sends the flag, so a new server answers `On\n`
  as always: old-SDK-to-new-server needs no changes anywhere.
- Discovery never tags anything (its post-auth traffic is the one-shot
  `L`), but must still accept and echo the flag, because a client
  doesn't know which kind of server it dialed until `A`'s reply.

### Tagged frames, on tagged-mode connections only

The tag is a per-connection wrapping u32 counter, encoded as the
decimal text field the protocol already uses for every number. Requests
append it as the last header field — `G <klen> <seq>\n`,
`D <klen> <seq>\n`, `S <klen> <vlen> <seq>\n`,
`S <klen> <vlen> <ttl> <seq>\n` — and responses echo it the same way:
`V <len> <seq>\n`, `S <seq>\n`, `D <seq>\n`, `N <seq>\n`, `W <seq>\n`.
Tagged mode is connection state on both sides, so `S`'s three-field
form is never ambiguous (tagged three-field = no TTL, untagged
three-field = TTL). `B` (busy) stays untagged: it's unsolicited,
written before auth ever happens.

Each SDK's connection assigns the tag inside the same critical section
that already serializes "claim the next queue slot" and "write the
frame" ([[0016]]), stores it on the pending slot, and the read loop
compares each response's echoed tag against the slot it's about to
resolve. A missing or mismatched tag poisons the connection **before**
dispatch — every pending caller gets the poison error, none gets
another request's data. The caller-side kind checks stay as a second
line of defense.

### What deliberately stays untagged

Tagging is opt-in per connection, so every internal client that speaks
the old form keeps working unchanged: the node's own migration/forward
`S`/`D` sends and its `A`/`H`/`J`/`P`/`C` traffic to discovery (all
fixed-length ACK reads in `src/server.rs`), `verify-staged-join`'s
hand-rolled parser, and discovery's `M`/`X` sends. None of them
pipelines concurrent requests on one connection, so none needs the
protection.

## Consequences

Easier:

- The [[0016]] residual gap is closed end-to-end on new-client ↔
  new-server connections: a desync can no longer resolve *any* request
  with another request's data — the read loop refuses the frame before
  dispatch. Wrong-kind detection at the caller becomes a redundant
  backstop instead of the only line of defense.
- The wire cost is a few bytes per frame (a decimal field), zero for
  old clients, and the negotiation reuses the `A` exchange every
  connection already performs — no new round trip.

Harder / risks to mitigate:

- Wire-protocol change: node parse/encode, discovery's auth reply, all
  six SDKs' identify + encode + parse + read loop, and both protocol
  documents (README.md and docs/protocol.html) move together in one
  change.
- The transparent fallback reopens the old window against old servers
  — by design (user decision on issue #35): compatibility over
  strictness there, and old-server deployments are exactly as exposed
  as they were before this ADR, no worse.
- The fallback triggers on close-before-reply, which a transient
  network failure can mimic; the cost is one redial and an untagged
  connection where a tagged one was possible, never a wrong answer.
- A wrapped u32 tag can theoretically realign after exactly 2^32
  in-flight-misaligned frames; in practice a desync is caught on the
  first mismatched frame, so the width is cosmetic.
