# 3. Rename to nanocached and dispatch node/discovery through an ncd CLI

Date: 2026-08-15

## Status

Accepted

## Context

The project was named `kvelo`, and its binaries (`kvelo`, `bench`,
`discovery`) took that name or plain generic words directly. Two problems
surfaced once a discovery server was added (see [[0002]]):

- `kvelo` did not communicate that the project is a cache server, which was
  a stated goal for the rename.
- Bare command names like `discovery` are prone to colliding with unrelated
  tools once installed on a system `PATH` or shipped in a container image.

`nanocached` was chosen as the new project name: `-cached` places it in the
same naming lineage as `memcached`, communicating "cache server" on sight,
while `nano-` signals a smaller feature set than Memcached or Redis, which
matches the project's actual positioning. It was confirmed unclaimed on
crates.io at decision time.

For the command-line tools, three binaries now need names: the cache
server, the discovery server, and (separately) the `bench` load-test
client. `bench` is a development tool, not a product component, and is
deliberately excluded from whatever the primary tools are, echoing its
existing independence rule (see `src/bin/bench.rs`).

## Decision

- Rename the crate and project to `nanocached`.
- The cache server and discovery server binaries take descriptive,
  collision-resistant names: `nanocached-node` and `nanocached-discovery`.
  `bench` keeps its existing name and stays outside this scheme.
- Add a thin dispatcher binary, `ncd`, as the primary way to run a built
  binary or container: `ncd node start [options]` and
  `ncd discovery start [options]`. `ncd` does not contain node or discovery
  logic itself; it looks up the sibling binary next to its own executable
  path and runs it with the remaining arguments, the same pattern Cargo
  uses to dispatch `cargo <subcommand>` to `cargo-<subcommand>` binaries.
  This keeps `nanocached-node` and `nanocached-discovery` as fully separate
  compilation units — consistent with the existing rule that `bench.rs`
  must not be merged into a shared `lib.rs` — while still presenting one
  short, memorable command to operators.
- The Docker image ships `ncd`, `nanocached-node`, and
  `nanocached-discovery` (not `bench`, which is not a product component).
  `ENTRYPOINT` is `ncd`; `CMD` defaults to `node start --host 0.0.0.0`, and
  the discovery role is selected by overriding the command
  (`discovery start --host 0.0.0.0`) in a separate container.

## Consequences

Easier:

- The project name now signals what it is (a cache server) without
  explanation.
- `nanocached-node`/`nanocached-discovery` are unlikely to collide with
  other tools on a shared `PATH`, while `ncd` gives operators one short
  command to remember instead of two long ones.
- `nanocached-node` and `nanocached-discovery` remain independently
  buildable and independently discardable; a change to one cannot break
  compilation of the other, and `bench` remains fully isolated from both,
  same as before this change.
- One Docker image serves both roles, selected at `docker run` time, so
  there is only one image to build and publish.

Harder / risks to mitigate:

- `ncd` only works once its sibling binaries have actually been built next
  to it. `cargo run --bin ncd` alone will not build
  `nanocached-node`/`nanocached-discovery`, which is a real papercut for
  local development; the README documents running `nanocached-node`
  directly (or `nanocached-discovery` directly) for that case instead.
- The GitHub repository and its container registry path
  (`ghcr.io/t0k0sh1/kvelo`) were intentionally left unchanged by this
  decision, since renaming a GitHub repository is an external, shared-state
  change outside the scope of this ADR. The crate/binary names and the
  repository name are now inconsistent until that is addressed separately.
