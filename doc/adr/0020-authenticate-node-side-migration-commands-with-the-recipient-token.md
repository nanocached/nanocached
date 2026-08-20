# 20. Authenticate node-side migration commands with the recipient's token

Date: 2026-08-20

## Status

Accepted — implemented on `fix/python-version-drift` (issue #34, node side).

Builds on [5. Shared-secret authentication via environment variable](0005-shared-secret-authentication-via-environment-variable.md),
[8. Staged node join with discovery-orchestrated data handoff](0008-staged-node-join-with-discovery-orchestrated-data-handoff.md),
and [18. Per-node ephemeral token authentication for membership commands](0018-per-node-ephemeral-token-authentication-for-membership-commands.md)

## Context

[[0018]] closed the *discovery-side* half of the shared-secret gap: since
[[0005]]'s secret proves only "member of the cluster", not "the node
behind this name", it added a per-node membership token to every
node→discovery command that names the sending node (`J`/`P`/`H`/`C`).

The symmetric gap on the *node* side stayed open. A `Joined` node accepts
`M` (migrate) and `X` (cancel-migration) from discovery, gated by nothing
more than [[0005]]'s `authenticated` flag — the same flag an ordinary
`G`/`S`/`D` client clears. So any client holding the shared secret could
send, straight to a node:

    M <joining-name-len> <attacker-addr-len> <joined-count> <replication>\n<name><addr>...

and the node's `run_migration` would compute which of its live keys the
attacker's fake "joining node" owns and stream them — as ordinary `SET`s —
to `attacker-addr`. With `joined-count` 0 and a large `replication`, that
is essentially the whole cache. `X` likewise let any such client abort a
legitimate in-flight handoff. The security audit behind PR #33 flagged the
discovery side (fixed in [[0018]]); this is the same class of bug on the
node side, and the shared secret alone cannot close it for the same reason
[[0018]] gives: it does not distinguish *which* member is talking.

[[0018]] deliberately left `M` tokenless — its load-bearing invariant is
that a token "never sent back out" can't be impersonated, and it spelled
out the hazard as "every client (or every node receiving an `M`) can
impersonate every node" the moment a token appears in a response. That
invariant is about not leaking *some other* node's token: an `M` sent to
ready node B, about joining node A, must not carry A's token, or B learns
it and can speak for A.

## Decision

Echo the **recipient's own** token on `M` and `X`. Discovery already
stores every node's token (`NodeInfo::token`, established at `J`), so when
it sends `M`/`X` to ready node B it includes B's token, and B verifies it
(constant-time) against its own before acting:

    M <joining-name-len> <joining-addr-len> <joined-count> <replication> <token-len>\n<token><joining-name><joining-addr><entries>
    X <joining-name-len> <token-len>\n<token><joining-name>

The token leads the body so it can be checked before any migration work.
A mismatch is rejected loudly (`M` also gets a `MigrationRejected` ack) and
the connection is closed, exactly like [[0018]]'s wrong-token rejections.

This does **not** violate [[0018]]'s invariant. The token in an `M`/`X` is
the *recipient's* own token — which it already holds, and which no client
knows (a client never registered, so it never learned any node's token;
tokens are still never listed in `L`). Only a discovery server the node
registered with can produce it, which is precisely the "is this really my
discovery server" proof the shared secret couldn't give. [[0018]]'s prose
("every node receiving an `M`") was scoped to leaking *other* nodes'
tokens; echoing the recipient's own token back to it leaks nothing.

Like [[0009]]/[[0012]]/[[0018]], this is a node↔discovery wire change
shipped in both binaries at once with no compatibility shim. `L` is
untouched, so **no SDK changes** and no client-visible protocol
difference.

## Consequences

- A holder of the shared secret can no longer make a node exfiltrate its
  cache via `M`, or abort a handoff via `X`: both now require the target
  node's per-process token, which only its discovery server holds. The
  [[0005]] secret still gates who may talk to a node at all; the token now
  distinguishes discovery from an ordinary client.
- Default-unauthenticated deployments gain the same protection against a
  forged `M`/`X` (the attacker must now also guess a per-process UUID), but
  everything [[0005]] says still applies: without the secret and TLS, an
  on-path attacker who can observe a real `M` sees the token in cleartext,
  exactly as with the shared secret and [[0018]]'s tokens. The token
  travels in cleartext unless [[0006]] TLS is configured.
- The `M` size estimate `start_join` checks against `NODE_MAX_REQUEST_SIZE`
  now includes the token length; all recipient tokens are per-process
  UUIDs of equal length, so the joining node's own token stands in for an
  accurate estimate without a per-recipient lookup.
- The same lockstep-change cost [[0018]] paid: every raw-frame producer and
  consumer of `M`/`X` changed (discovery's `send_migrate`/`send_cancel` and
  `build_migrate_message`, the node's `command.rs` parser and
  `handle_connection`, both test suites). SDKs are untouched because
  neither `M` nor `X` is an SDK-facing frame.
