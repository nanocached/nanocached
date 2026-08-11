# Kvelo

Kvelo is a compact in-memory key-value cache server written in Rust. It uses a
small, binary-safe TCP protocol and supports optional time-to-live (TTL) values.

> [!NOTE]
> Kvelo is currently experimental and is not ready for production use.

## Features

- In-memory storage for binary keys and values
- `GET`, `SET`, and `DEL` commands
- Optional TTL for stored values
- Multiple requests per TCP connection
- Pipelined requests
- Idle connection timeout
- Request-size and connection-count limits

## Requirements

- Rust 1.96.0
- `cargo-nextest` for `make test`
- `cargo-mutants` for mutation testing

The repository includes `rust-toolchain.toml`, so Rustup selects the required
toolchain and installs the Clippy and rustfmt components automatically.

## Running the server

### Cargo

```sh
cargo run
```

Kvelo listens on `0.0.0.0:8356`.

### Docker

Use the included `Dockerfile` to build and run the Alpine-based container
image:

```sh
docker build --tag kvelo .
docker run --rm --publish 8356:8356 kvelo
```

The latest image built from the `main` branch is also available from GitHub
Container Registry:

```sh
docker pull ghcr.io/t0k0sh1/kvelo:latest
docker run --rm --publish 8356:8356 ghcr.io/t0k0sh1/kvelo:latest
```

For example, store and retrieve the value `Alice` under the key `name`:

```sh
printf 'SET 4 5\r\nnameAliceGET 4\r\nname' | nc 127.0.0.1 8356
```

The server responds with:

```text
STORED
VALUE 5
Alice
```

## Protocol

Each request starts with an ASCII header terminated by `\r\n`. The key and
value bodies immediately follow the header and are read according to their byte
lengths. Bodies are not terminated by a delimiter and may contain arbitrary
bytes.

### GET

```text
GET <key-length>\r\n<key>
```

Responses:

```text
VALUE <value-length>\r\n<value>
```

or:

```text
NOT_FOUND\r\n
```

### SET

Without a TTL:

```text
SET <key-length> <value-length>\r\n<key><value>
```

With a TTL in seconds:

```text
SET <key-length> <value-length> <ttl-seconds>\r\n<key><value>
```

Response:

```text
STORED\r\n
```

### DEL

```text
DEL <key-length>\r\n<key>
```

Responses:

```text
DELETED\r\n
```

or:

```text
NOT_FOUND\r\n
```

When the connection limit has been reached, the server responds with:

```text
BUSY\r\n
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

## Current limits

- Maximum request size: 1 MiB
- Maximum concurrent connections: 1,024
- Idle connection timeout: 30 seconds
