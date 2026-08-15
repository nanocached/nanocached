# 5. Shared-secret authentication via environment variable

Date: 2026-08-15

## Status

Accepted

## Context

`nanocached-node` and `nanocached-discovery` had no authentication: any
process able to reach the listening port could issue any command. The
default bind address (`127.0.0.1`) is the primary defense, matching Redis's
own documented model, where network isolation — not authentication — is the
primary control and `requirepass`/`AUTH` is explicitly a secondary "layer of
redundancy". Redis's own docs also state that `AUTH` is sent in cleartext
and provides no protection against network eavesdropping; nanocached's
protocol has no transport encryption either, so the same limitation applies
here and authentication cannot be sold as more than it is.

A related question was whether cached values themselves need app-level
encryption. Checking real-world precedent (Spring Data Redis has no
built-in encrypting `RedisSerializer`; the default serializer does not
encrypt) confirmed that per-value encryption is not standard practice for
cache layers. The standard practice is network isolation plus, where
warranted, TLS and infra-level disk encryption — and, for data that would
be catastrophic if leaked, not caching it at all rather than encrypting a
cached copy. That conclusion sets the bar for this decision: authentication
here is a secondary access-control layer, not a substitute for keeping
sensitive data out of the cache or for network-level protection.

Given that framing, two designs were considered for the credential itself:

- A single shared secret (Redis's legacy `requirepass` model): one value,
  known to every legitimate client, checked as a single yes/no gate.
- A paired identifier + secret (closer to Redis 6+ ACL users, or typical
  API key/secret pairs): distinguishes which client connected, permits
  per-identity revocation without rotating everyone else's credential, and
  could support per-identity permissions later.

The paired form buys identity and finer-grained revocation, at the cost of
a credential store (even a minimal one) and provisioning/rotation flow for
issuing and retiring individual identities. nanocached has no concept of
users or permissions elsewhere in the system, and rotating a single shared
secret is an acceptable operational cost for the deployments this project
currently targets. The paired form's benefits don't pay for its complexity
here.

## Decision

Add a single shared-secret authentication gate to both `nanocached-node`
and `nanocached-discovery`, read from the `NANOCACHED_AUTH_SECRET`
environment variable rather than a CLI flag — CLI arguments are visible to
any other process on the host via `ps`, which would leak the secret to
exactly the kind of same-host, different-user attacker this feature is
meant to defend against. An unset or empty value disables authentication
entirely, matching Redis's `requirepass`-unset default.

Protocol: a new `A <secret-length>\n<secret>` command, mirroring the
existing single-length-prefixed-body commands (`G`, `D`). The server
responds `O\n` on success or `E\n` followed by closing the connection on
failure. If no secret is configured server-side, `A` is a harmless no-op
that always returns `O\n`. If a secret is configured, every other command
is rejected with `E\n` (and the connection closed) until a matching `A` has
been sent on that connection; `A` itself is always accepted before
authentication so a client can attempt to authenticate at all.

Secret comparison uses a constant-time equality check (early-return only on
length mismatch, since length isn't secret; otherwise XOR-accumulate every
byte) implemented by hand in both binaries rather than pulling in a crate,
consistent with the project's preference for minimal dependencies for a
primitive this small.

`nanocached-node`'s background heartbeat task authenticates to the
discovery server (using the node's own configured secret) immediately after
connecting and before sending any heartbeat, since the discovery server
applies the same gate to a node's heartbeat connection as to any other
client.

`src/bin/bench.rs` gets a `--auth-secret <secret>` **CLI flag** instead of
an environment variable, since it is an interactive development/load-test
tool rather than a production service — the `ps`-visibility concern doesn't
apply the same way, and a flag is more convenient for one-off runs
(mirroring `redis-cli -a`).

Both `server.rs` and `nanocached-discovery.rs` implement this
independently (their own `ConnectionConfig`, `read_auth_secret`,
`constant_time_eq`), matching this project's established convention that
`src/bin/*.rs` binaries share no code via a `lib.rs`.

## Consequences

Easier:

- Deployments that can't fully rely on network isolation (shared hosts,
  broader network ranges) get a real, if secondary, access-control layer
  with a one-line environment variable.
- The `A`/`O`/`E` protocol addition is minimal and consistent with the
  existing one-byte command/status convention.
- No new dependency: the constant-time comparison and secret handling are
  a few dozen lines, not a crate.

Harder / risks to mitigate:

- The credential is a single shared value: revoking one compromised client
  means rotating the secret for every client, node, and the discovery
  server at once. Acceptable for now per the Context above, but would need
  revisiting if nanocached ever needs per-client identity or revocation.
- Authentication does not protect against network eavesdropping — the
  secret and all cached data still travel in cleartext. TLS remains a
  separate, not-yet-started follow-up for deployments that need protection
  against an on-path attacker, not just an unauthenticated same-network
  client.
- Every binary that speaks the protocol (`nanocached-node`,
  `nanocached-discovery`, `bench`, and any future client SDK) must
  implement the `A` handshake correctly, including authenticating
  long-lived background connections like the node's heartbeat — missing
  this on any one connection type reintroduces a silent gap, as happened
  once already with the heartbeat connection during implementation.
