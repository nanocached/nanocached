# 6. TLS support via rustls, required once configured

Date: 2026-08-15

## Status

Accepted

Builds on [5. Shared-secret authentication via environment variable](0005-shared-secret-authentication-via-environment-variable.md)

## Context

[[0005]] added shared-secret authentication but explicitly framed it as a
secondary defense layer: Redis's own docs state `AUTH` is sent in
cleartext and gives no protection against network eavesdropping, and
nanocached's protocol had the same limitation — the auth secret and every
cached key/value traveled unencrypted. [[0005]] named TLS as the
follow-up needed for deployments where that matters (an on-path attacker,
not just an unauthenticated same-network client). This ADR covers that
follow-up.

Two designs were considered for how strict TLS should be once configured:

- **Required once configured**: if a cert/key is set, that port accepts
  only TLS connections — matching how [[0005]]'s auth secret gates every
  command once set, with no partial/opt-in behavior per connection.
- **Optional per-connection**: a single port sniffs the first bytes of a
  connection to decide whether to speak TLS or plaintext, so already-set-up
  plaintext clients keep working during a migration to TLS.

The optional/sniffing design buys a smoother migration path at the cost of
a second, subtler code path in every connection handler (detect-and-branch
logic, and a real risk of a client silently falling back to plaintext
against an operator's intent). Given [[0005]]'s "either fully required or
fully off" precedent for the auth gate, and that this project has no
existing deployments to migrate, required-once-configured is simpler and
avoids a class of "TLS was configured but a client silently downgraded"
production risk.

For the TLS library, `rustls` (via `tokio-rustls`) was chosen over
`native-tls`/OpenSSL bindings: it's pure Rust apart from its crypto
backend, avoiding a system OpenSSL dependency that would complicate the
project's cross-compiled `linux/arm64` Docker builds (see ADR for the
multi-arch publish workflow). Within rustls's crypto backend choice, the
`ring` feature was picked over the default `aws-lc-rs`: `aws-lc-rs`
requires a C/CMake toolchain to build (`aws-lc-sys`), which adds real cost
under the project's QEMU-emulated `linux/arm64` builds; `ring` is the more
common choice for cross-compiled Rust projects and needs only a C compiler
for a small assembly component, not a full CMake/C++ toolchain.

Mutual TLS (client certificates) was considered and rejected for this
pass: it would require a certificate issuance/revocation flow this project
has no infrastructure for, and [[0005]]'s shared-secret auth already
covers client identity/authorization at a complexity level matching the
project's actual deployment needs. Server-only TLS (encrypting the
channel; auth for who's allowed in) is the right layering for now.

## Decision

Add `--tls-cert`/`--tls-key` (PEM files) to `nanocached-node` and
`nanocached-discovery`: when both are set, every accepted connection must
complete a TLS handshake before speaking the wire protocol, with a 10
second handshake timeout; there is no plaintext fallback. `--tls-cert` and
`--tls-key` must be given together or not at all.

`nanocached-node` also gets `--tls-ca` (PEM CA file(s)) for its own
outbound role: the background heartbeat connection to a discovery server
uses this to verify the discovery server's certificate and upgrades to TLS
if set, with no plaintext fallback on that connection either. Only the
CA(s) in this file are trusted — not the system trust store — since this
is meant for a private cluster's own (likely self-signed) certificates,
not publicly-issued ones. `nanocached-discovery` has no outbound role, so
it only needs `--tls-cert`/`--tls-key`.

`src/bin/bench.rs` gets a matching `--tls-ca` flag (no `--tls-cert`, since
it never accepts connections) and connects to every node and the
discovery server over TLS when set.

Certificate/key paths are CLI flags, not environment variables, unlike the
auth secret in [[0005]]: a file *path* isn't itself sensitive the way a
secret value is (the private key's contents are protected by ordinary file
permissions instead), so the `ps`-visibility concern that justified an
environment variable for the auth secret doesn't apply here.

Implementation: an accepted or outbound connection is represented as a
small `MaybeTls` wrapper (`Plain(TcpStream)` or
`Tls(Box<tokio_rustls::…::TlsStream<TcpStream>>)`) implementing
`AsyncRead`/`AsyncWrite` by delegating to whichever variant is active, so
the rest of each connection handler is unchanged by whether TLS is in
play. `server.rs` needs both an accept-side and a connect-side version (it
terminates client connections *and* opens the outbound heartbeat
connection), so it defines one generic `MaybeTls<P, T>` reused for both via
a type alias per role; `nanocached-discovery.rs` (accept-only) and
`bench.rs` (connect-only) each define their own single-role, non-generic
copy, consistent with this project's established rule that `src/bin/*.rs`
binaries share no code via a `lib.rs`.

## Consequences

Easier:

- Deployments that need protection against network eavesdropping — not
  just an unauthenticated same-network client — now have a real option,
  closing the gap [[0005]] explicitly left open.
- "Required once configured" means there's no silent downgrade path to
  reason about: a connection either completes a TLS handshake or is
  refused, full stop.
- Pure-Rust `rustls` with the `ring` backend keeps the existing
  cross-compiled multi-arch Docker build working without adding a system
  OpenSSL or CMake/C++ toolchain dependency.

Harder / risks to mitigate:

- Operators must generate and manage certificates themselves; nanocached
  provides no certificate issuance, rotation, or revocation tooling. For
  self-signed certificates (the expected common case for a private
  cluster), the certificate must have `CA:FALSE` in its basic
  constraints — a self-signed cert generated with common defaults is often
  marked as its own CA, which rustls correctly refuses to accept as a
  server's leaf certificate. This is documented in the README with a
  working `openssl` invocation, but it's a real one-time surprise (it was
  hit during this feature's own manual end-to-end verification).
- No mutual TLS: a server's identity is verified by clients, but the
  server doesn't verify client identity via certificates — that's still
  [[0005]]'s shared secret's job. If per-client identity or certificate
  revocation is ever needed, this would need revisiting.
- Every binary that opens a connection to another nanocached process
  (`nanocached-node`'s heartbeat client, `bench`) must be updated in
  lockstep whenever a new outbound connection type is added, or it will
  silently lack TLS support on that path — the same class of gap
  [[0005]] hit once already with the heartbeat connection's auth.
