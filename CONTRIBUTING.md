# Contributing to nanocached

This file covers building the server from source and running its tests and
checks. For what nanocached is and how to use it, see the
[README](README.md).

## Requirements

- Rust 1.96.0. The repository includes `rust-toolchain.toml`, so Rustup
  selects the required toolchain and installs the Clippy and rustfmt
  components automatically.
- [`cargo-nextest`](https://nexte.st), which `make test` runs.
- [`cargo-mutants`](https://mutants.rs), for mutation testing.

## Building

```sh
cargo build
```

This produces `nanocached-node`, `nanocached-discovery`, `nanocached-proxy`
and `ncd` (see the [README](README.md#programs)), plus `verify-staged-join`,
a development tool that drives a staged join against a running cluster. It is
not part of a deployment.

## Checks and tests

Run formatting and static checks (`cargo fmt --check` and Clippy with
warnings denied):

```sh
make check
```

Run the test suite with cargo-nextest:

```sh
make test
```

## Mutation tests

Run mutation tests for the entire codebase or for a specific source file:

```sh
make mutants
make mutants FILE=src/cache.rs
```

Run mutation tests only against the changes relative to `HEAD`:

```sh
make mutants-diff
```

Use `MUTANTS_BASE` to select a different comparison revision and
`MUTANTS_JOBS` to change the mutation-test concurrency (default 4):

```sh
make mutants-diff MUTANTS_BASE=origin/main MUTANTS_JOBS=2
```

## End-to-end scenarios

The Docker-based scenarios under [`tests/e2e`](tests/e2e/README.md) exercise
cluster-level behaviour (joins, evictions, slow nodes, hostile peers). Most
are not run in CI; the README there lists the prerequisites and each
scenario.

## SDKs and adapters

Each SDK under [`sdk/`](sdk) and each adapter under [`adapters/`](adapters)
has its own README and changelog; look there for that component's
toolchain and, where the README has a "Build", "Building" or "Testing"
section, its build and test steps.

## Releases

A release ships the server, the six SDKs and the seven framework adapters at
one version. [`tools/release-all.sh`](tools/release-all.sh) documents the
procedure.

## License

nanocached is released under the [MIT License](LICENSE).
