# nanocached

nanocached is a compact in-memory key-value cache server written in Rust. It
uses a small, binary-safe TCP protocol and supports optional time-to-live
(TTL) values.

> [!NOTE]
> nanocached is currently experimental and is not ready for production use.

## Features

- In-memory storage for binary keys and values
- `G`, `S`, and `D` commands
- Optional TTL for stored values
- Multiple requests per TCP connection
- Pipelined requests
- Idle connection timeout
- Request-size and connection-count limits
- Memory-bounded cache with least-recently-used (LRU) eviction

## Requirements

- Rust 1.96.0
- `cargo-nextest` for `make test`
- `cargo-mutants` for mutation testing

The repository includes `rust-toolchain.toml`, so Rustup selects the required
toolchain and installs the Clippy and rustfmt components automatically.

## Command-line tools

Building the project produces four binaries:

- `ncd` — a thin dispatcher: `ncd node start [options]` runs the cache node,
  `ncd discovery start [options]` runs the discovery server. Convenient for
  running a bare-metal build; Docker images run their binary directly
  instead (see [Docker](#docker) below) so each image only contains what
  its role needs.
- `nanocached-node` — the cache server itself.
- `nanocached-discovery` — the standalone cluster-membership registry (see
  [Discovery server](#discovery-server) below).
- `bench` — a load-test client; a development tool, not part of the
  product, so it is intentionally not reachable through `ncd` (see
  [Benchmarking](#benchmarking) below).

`ncd` looks up `nanocached-node`/`nanocached-discovery` next to its own
executable, so it only works once both have been built into the same
`target/` directory (`cargo build` covers this; `cargo run --bin ncd` alone
does not, since Cargo only builds the binary you asked to run).

## Running the server

### Cargo, for local development

```sh
cargo run --bin nanocached-node -- --host 0.0.0.0 --port 8356
```

nanocached listens on `127.0.0.1:8356` by default. Override the bind address
and port with `--host` and `--port`.

To register with a [discovery server](#discovery-server) so client SDKs can
find this node, pass `--discovery`:

```sh
cargo run --bin nanocached-node -- --port 8356 --discovery 127.0.0.1:8357
```

The node sends a heartbeat every `--heartbeat-interval` seconds (default 5)
declaring `--advertise-addr` (default: `--host:--port`); omit `--discovery`
to run standalone.

### A built binary, via ncd

```sh
cargo build --release
./target/release/ncd node start --host 0.0.0.0 --port 8356
```

### Docker

The included `Dockerfile` builds two separate, minimal Alpine-based images
via multi-stage targets — each contains only its own binary, not `ncd` and
not the other role's binary:

```sh
docker build --target node --tag nanocached-node .
docker run --rm --publish 8356:8356 nanocached-node

docker build --target discovery --tag nanocached-discovery .
docker run --rm --publish 8357:8357 nanocached-discovery
```

`--target` is required; the Dockerfile has no default final stage.

The latest images built from the `main` branch are also available from
GitHub Container Registry:

```sh
docker pull ghcr.io/t0k0sh1/nanocached-node:latest
docker run --rm --publish 8356:8356 ghcr.io/t0k0sh1/nanocached-node:latest

docker pull ghcr.io/t0k0sh1/nanocached-discovery:latest
docker run --rm --publish 8357:8357 ghcr.io/t0k0sh1/nanocached-discovery:latest
```

For example, store and retrieve the value `Alice` under the key `name`:

```sh
printf 'S 4 5\nnameAliceG 4\nname' | nc 127.0.0.1 8356
```

The server responds with:

```text
S
V 5
Alice
```

## Protocol

Each request starts with an ASCII header terminated by `\n`. The key and
value bodies immediately follow the header and are read according to their byte
lengths. Bodies are not terminated by a delimiter and may contain arbitrary
bytes. One-byte command and status identifiers minimize protocol overhead while
keeping frames readable during development.

A `<key-length>` of `0` is rejected for every command. There is no dedicated
maximum key or value length beyond the overall request-size limit below.

### G (get)

```text
G <key-length>\n<key>
```

Responses:

```text
V <value-length>\n<value>
```

or:

```text
N\n
```

### S (set)

Without a TTL:

```text
S <key-length> <value-length>\n<key><value>
```

With a TTL in seconds:

```text
S <key-length> <value-length> <ttl-seconds>\n<key><value>
```

Response:

```text
S\n
```

### D (delete)

```text
D <key-length>\n<key>
```

Responses:

```text
D\n
```

or:

```text
N\n
```

When the connection limit has been reached, the server responds with:

```text
B\n
```

## Development

Run formatting and static checks:

```sh
make check
```

Run the test suite with cargo-nextest:

```sh
make test
```

Run mutation tests for the entire codebase or a specific source file:

```sh
make mutants
make mutants FILE=src/cache.rs
```

Run mutation tests only against changes relative to `HEAD`:

```sh
make mutants-diff
```

Use `MUTANTS_BASE` to select a different comparison revision and
`MUTANTS_JOBS` to change the mutation-test concurrency.

```sh
make mutants-diff MUTANTS_BASE=origin/main MUTANTS_JOBS=2
```

### Benchmarking

`src/bin/bench.rs` is an async, multi-threaded load client for nanocached's
protocol:

```sh
cargo run --release --bin bench -- --help
cargo run --release --bin bench -- -c 64 --workload mixed
```

### Discovery server

`src/bin/nanocached-discovery.rs` is a standalone cluster-membership
registry that cache nodes and client SDKs use to find each other for
horizontal scaling (see `doc/adr/0002-*.md` for the design rationale). It
has no dependency on nanocached's own protocol modules.

```sh
cargo run --bin nanocached-discovery -- --help
cargo run --bin nanocached-discovery -- --port 8357
```

## Current limits

### nanocached-node

- Maximum request size: 1 MiB
- Maximum concurrent connections: 1,024
- Maximum cache memory usage: 256 MiB (approximate: sum of stored key and
  value bytes), least-recently-used entries evicted first
- Idle connection timeout: 30 seconds

### nanocached-discovery

- Maximum request size: 4 KiB
- Maximum concurrent connections: 1,024
- Idle connection timeout: 30 seconds
