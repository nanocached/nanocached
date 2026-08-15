# 4. Split the Docker image into separate node and discovery images

Date: 2026-08-15

## Status

Accepted

Amends [3. Rename to nanocached and dispatch node/discovery through an ncd CLI](0003-rename-to-nanocached-and-dispatch-node-discovery-through-an-ncd-cli.md)

## Context

[[0003]] shipped one Docker image containing `ncd`, `nanocached-node`, and
`nanocached-discovery`, with the role picked at `docker run` time via `ncd`'s
subcommand (`node start` by default, `discovery start` as an override).

A container only ever plays one role for its entire lifetime — that choice
is made once, at deploy time, not per-request like `ncd`'s original
bare-metal CLI use case. Shipping both binaries (plus the dispatcher) in
every container means every node container carries discovery-server code
it will never run, and vice versa: unnecessary image content, and a larger
attack/patch surface than the role actually needs.

## Decision

Split the single image into two, built as separate stages of the same
`Dockerfile` (`node` and `discovery`), each `FROM alpine:3.21` and
containing only its own binary as `ENTRYPOINT` — no `ncd`, no the other
role's binary:

```sh
docker build --target node --tag nanocached-node .
docker build --target discovery --tag nanocached-discovery .
```

The builder stage (compiling both binaries) stays shared, so there is still
only one `Dockerfile` and one Rust compilation to maintain; only the final,
shipped layer differs per target. CI now publishes both as separate images
(`ghcr.io/<repo>-node` and `ghcr.io/<repo>-discovery`) via a build matrix.

`ncd` itself is unchanged and still ships for bare-metal use (see [[0003]]);
it is simply no longer part of either container image.

## Consequences

Easier:

- Each image contains exactly the binary its role needs, nothing else —
  smaller images and a smaller surface to patch/scan per role.
- The two roles can be deployed, versioned, and scaled independently as
  container images, matching how they already run as independent processes.
- The shared builder stage means adding this split cost no duplication of
  the Rust build step.

Harder / risks to mitigate:

- `docker build .` with no `--target` has no default final stage and fails;
  this is intentional (no accidental "pick whichever stage happens to be
  last"), but it is a one-time surprise for anyone used to a single-image
  `Dockerfile` and must stay documented in the README.
- Two images now need to be built, pushed, and tracked in CI instead of
  one, which is somewhat more moving parts in `publish.yaml`.
