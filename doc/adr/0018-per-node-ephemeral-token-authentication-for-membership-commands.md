# 18. Per-node ephemeral token authentication for membership commands

Date: 2026-08-20

## Status

Accepted — implemented on `fix/announce-node-takeover` (issue #34).

Builds on [5. Shared-secret authentication via environment variable](0005-shared-secret-authentication-via-environment-variable.md),
[9. Decouple node identity from network address](0009-decouple-node-identity-from-network-address.md),
[10. Discovery HA via soft-state replicas and member announce](0010-discovery-ha-via-soft-state-replicas-and-member-announce.md),
and [12. Derive node addresses from the registration connection](0012-derive-node-addresses-from-the-registration-connection.md)

## Context

[[0010]]'s `P` (announce) upserts a name straight to `Joined` — including
overwriting the registered address of an *existing* `Joined` node — with
no check that the sender is that node. Node names are public (`L` lists
them), and [[0012]] derives the registered address from the announcing
connection's source IP, so anyone who can reach discovery can list a
victim's name, announce it from their own IP, and have every `G`/`S`/`D`
for that node's key range routed to a listener they control (cache
poisoning, data theft, MITM). The security audit behind PR #33 found
this; it was split into issue #34 because the fix is an authentication
design change, not a patch.

Two facts scope the problem:

- **Default-unauthenticated operation is by design** ([[0005]]: network
  isolation is the primary control, like memcached/Redis). This ADR does
  not change that posture.
- But even with `NANOCACHED_AUTH_SECRET` set, the secret is one shared
  value ([[0005]] chose that deliberately): it proves "member of the
  cluster", not "the node behind this name". One compromised node — or
  anything else holding the secret — could still take over any other
  node's name. That gap is inside the authentication boundary, so
  [[0005]] alone can't close it.

A simple patch can't either. The registered address comes from the
connection's source IP ([[0012]]), so a takeover *is* "a new IP claiming
an existing name" — which is byte-for-byte identical to a legitimate
re-registration after a container reschedule or instance replacement.
Any heuristic that rejects address changes breaks the legitimate case.
What's missing is a credential that distinguishes the node itself from
everyone else who merely knows its (public) name.

Three designs were considered:

1. **A per-node ephemeral token** — the node generates a random token
   alongside its [[0009]] name and presents it on every command naming
   itself; discovery binds token to name at registration and requires a
   match thereafter.
2. **Binding the name to the connection (or identity) that joined it** —
   rejected: the heartbeat connection legitimately breaks and is redialed
   (from a new IP after a reschedule), so a literal connection binding
   breaks the normal case, and fixing that means binding to some
   identity established at join — which *is* design 1.
3. **mTLS with per-node certificates** (CN/SAN matched against the node
   name) — rejected: names are random per-process UUIDs ([[0009]]), so
   certificates would have to be issued online at node startup, meaning a
   CA in the deployment loop. That contradicts [[0009]]'s
   zero-operator-input identity and [[0002]]/[[0010]]'s
   lightweight/self-hosted constraints, and TLS support ([[0006]]) is
   deliberately optional.

## Decision

Design 1. Each node generates a **second random v4 UUID at startup — its
membership token** — held only in process memory, exactly like its name:
never persisted, never configured, regenerated on restart. Because
[[0009]] already made identity per-process-lifetime, the token needs no
issuance, storage, rotation, or revocation machinery — a restarted node
is a new node with a new name and a new token, which the existing join
flow already handles.

Every node→discovery command that *names the sending node* now carries
the token as an additional length-prefixed field:

    J <name-len> <port> <token-len>\n<name><token>
    P <name-len> <port> <token-len>\n<name><token>
    H <name-len> <r> <token-len>\n<name><token>
    C <name-len> <joining-len> <token-len>\n<name><joining><token>

`C` is included because it is the same class of claim ("I, node X, ...");
a forged completion report would promote a joining node before it
actually holds the reporter's share of the keyspace.

Discovery stores the token in the registry entry (`NodeInfo::token`) and
verifies with the existing constant-time comparison:

- **Established at first registration per replica** — by `J`, or by `P`
  for a name the replica doesn't know (a standby, or an amnesiac
  restart). This is trust-on-first-use, and it is the only option
  available: [[0010]] replicas deliberately never talk to each other, so
  there is nowhere else a replica could learn the token from.
- **Required to match on everything after**: a `P` naming a registered
  node with the wrong token is rejected (loudly logged — it is either an
  attack or a grave misconfiguration) and the existing entry is left
  untouched; a wrong-token `H` refreshes nothing; a wrong-token `C` is
  ignored like issue #5's stale reports; a wrong-token duplicate `J` is
  rejected rather than allowed to park on the real entry's promotion
  `Notify`.
- **Never sent back out.** `L` and `M` carry no tokens — this is the
  design's load-bearing invariant. The moment a token appears in a
  response, every client (or every node receiving an `M`) can
  impersonate every node, and the design collapses back to the
  shared-secret status quo.

Announce ordering fix, folded in because the same audit path exposed it:
the `P` handler used to mark the connection as owning the announced name
*before* validating the announce. A rejected announce's teardown then ran
`on_node_connection_ended` against the real node's entry — letting
anyone abort an in-progress join (or evict a Waiting node) just by
announcing its name and disconnecting. The connection now claims the
name only after the announce is accepted, closing that unauthenticated
join-abort DoS.

Like [[0012]], this is purely a node↔discovery wire change shipped in
both binaries at once, with no compatibility shim: `L` is untouched, so
**no SDK changes** and no client-visible protocol difference.

## Consequences

- Knowing a node's name no longer lets anyone — including a holder of
  the shared secret, i.e. another (compromised) node — re-point its
  address, spoof its liveness, or forge its handoff reports. The
  [[0005]] shared secret keeps gating who may talk to discovery at all;
  the token now distinguishes *which node* is talking.
- **Residual TOFU window**: on a replica that doesn't know the name yet
  (fresh standby, amnesiac restart), the first announcer wins. The
  window is one heartbeat interval wide (live members re-announce within
  it, [[0010]]), an attacker must also hold the shared secret where one
  is configured, and losing the race is loud: the real node's own
  announces are then rejected with WARN logs on the replica. Accepted —
  strictly better than the status quo, where the takeover window was
  *always* open — and closing it entirely is the per-node-credential
  provisioning problem that designs 2/3 were rejected for.
- A node that loses the race (or otherwise hits a token conflict) can't
  re-register under that name until the entry expires via
  `sweep_expired`; its heartbeat task keeps retrying through the normal
  reconnect path, so it recovers without operator action once the stale
  entry is swept.
- Default-unauthenticated deployments gain the same protection against
  *name takeover* (the attacker must now guess a UUID), but everything
  [[0005]] says still applies: without the shared secret and TLS, an
  on-path attacker or anyone who can reach the port still has the rest
  of the attack surface. The token travels in cleartext unless [[0006]]
  TLS is configured, exactly like the shared secret.
- Every raw-frame consumer of `J`/`P`/`H`/`C` changed again (discovery
  server, node heartbeat/report paths, both test suites) — the same
  lockstep-change cost [[0009]] and [[0012]] already paid; SDKs are
  untouched because `L` is.
