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
- Optional shared-secret authentication (`A` command)
- Optional TLS (required for every connection once configured, no
  plaintext fallback)
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

`--discovery` accepts a comma-separated list of discovery replicas
(`doc/adr/0010-*.md`): the node registers with and heartbeats to all of
them, but only the first — the primary — ever orchestrates its join. Every
node in a cluster must list the same addresses in the same order:

```sh
cargo run --bin nanocached-node -- --port 8356 \
  --discovery 127.0.0.1:8357,127.0.0.1:8358
```

### Authentication

Set `NANOCACHED_AUTH_SECRET` to require clients to authenticate with a shared
secret before issuing any other command (see [A (auth)](#a-auth) below). It's
an environment variable rather than a CLI flag so the secret doesn't show up
in `ps` output. Both `nanocached-node` and `nanocached-discovery` read it
independently, so set it on both to protect a cluster:

```sh
NANOCACHED_AUTH_SECRET=change-me cargo run --bin nanocached-node -- --port 8356
NANOCACHED_AUTH_SECRET=change-me cargo run --bin nanocached-discovery -- --port 8357
```

If a node registers with a discovery server that requires auth, it uses the
same `NANOCACHED_AUTH_SECRET` value to authenticate its own heartbeats.

Leaving `NANOCACHED_AUTH_SECRET` unset (or empty) disables authentication —
matching Redis's own `requirepass`-unset default — and `A` becomes a no-op
that always succeeds. This is a secondary layer of defense, not a substitute
for network isolation: without [TLS](#tls), the protocol has no transport
encryption, so an attacker who can already observe the connection can read
the secret and every key/value in cleartext. Bind to `127.0.0.1` or a
private network interface (the default) and treat authentication as
protection against other processes/users reachable on that network, not
against network eavesdropping, unless TLS is also enabled.

### TLS

Pass `--tls-cert`/`--tls-key` (PEM files) to require TLS on every
connection a node or discovery server accepts — there is no plaintext
fallback once set, matching how [authentication](#authentication) is
either fully required or fully off:

```sh
cargo run --bin nanocached-node -- --port 8356 --tls-cert cert.pem --tls-key key.pem
cargo run --bin nanocached-discovery -- --port 8357 --tls-cert cert.pem --tls-key key.pem
```

A node that registers with a TLS-secured discovery server also needs
`--tls-ca` (a PEM file of CA certificate(s) to trust) so its heartbeat
connection can verify the discovery server's certificate — only those CAs
are trusted, not the system trust store, since this is meant for a
private cluster's own certificates rather than publicly-issued ones:

```sh
cargo run --bin nanocached-node -- --port 8356 --tls-cert cert.pem --tls-key key.pem \
  --tls-ca ca.pem --discovery 127.0.0.1:8357
```

For local development, generate a self-signed certificate with OpenSSL.
The certificate must have `CA:FALSE` in its basic constraints — a
self-signed cert generated with defaults is often marked as its own CA,
which rustls correctly refuses to accept as a server's leaf certificate —
and a `subjectAltName` matching whatever host/IP clients will actually
connect to:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 1 \
  -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth"
```

Since this is self-signed, the same `cert.pem` doubles as the `--tls-ca`
trust anchor for any client (a node's heartbeat, or `bench --tls-ca`)
connecting to a server using it.

Unlike authentication, TLS has no environment-variable option: certificate
and key paths aren't secrets themselves (the private key file's contents
are, but the OS already protects that via file permissions), so there's no
`ps`-visibility concern with passing paths as CLI flags.

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

### A (auth)

```text
A <secret-length>\n<secret>
```

Responses:

```text
O\n
```

or, if the secret doesn't match:

```text
E\n
```

immediately followed by the server closing the connection. If the server
has no auth secret configured, `A` always responds `O\n` regardless of what
secret is sent. If it does, every other command on that connection is
rejected with `E\n` (and the connection closed) until a matching `A` has
been sent — `A` itself is always accepted before authentication.

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

## SDKs

- [`sdk/typescript`](sdk/typescript/README.md) — a TypeScript/
  Node.js client (`get`/`set`/`delete`, authentication, TLS, and
  discovery-based cluster routing with replication).
- [`sdk/python`](sdk/python/README.md) — an asyncio Python client with
  the same feature set (Python 3.11+, no runtime dependencies).
- [`sdk/java`](sdk/java/README.md) — a thread-safe Java client with the
  same feature set (Java 17+, no runtime dependencies,
  `org.nanocached:nanocached`).

## Capacity planning

Memory, effective capacity, replication (R), TTL, and hit rate trade off
against each other — but not all at once: the hit rate is bounded by two
independent ceilings (one set by capacity, one by TTL), and only the
binding one responds to tuning. [`tools/capacity-planner.html`](tools/capacity-planner.html)
is a self-contained, offline estimator (Japanese UI) that makes those
trade-offs visible. Open it in any browser — no server, no build step:

```sh
open tools/capacity-planner.html
```

Enter your workload on the left — average key/value sizes, distinct key
count, request rate, access skew (Zipf exponent), TTL, and the cluster
shape (nodes × memory ÷ R) — and it estimates:

- the predicted hit rate, and **which constraint is the bottleneck**
  (adding memory only helps when capacity-bound; extending the TTL only
  helps when TTL-bound — spend accordingly);
- hit-rate curves against node memory and against TTL, with your current
  configuration marked;
- the minimum TTL and node memory needed to reach a target hit rate, or
  a warning when the target is unreachable for the workload.

The model is a TTL-extended Che approximation over a Zipf/Poisson
workload (assumptions are listed at the bottom of the page). Treat the
numbers as ±a-few-percent estimates for sizing, not guarantees; if they
diverge from production, re-derive the skew and key count from real
access logs first.

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

Pass `--discovery <addr>` instead of `--host`/`--port` to fetch the node
list from a discovery server and route keys across those nodes by
consistent hashing:

```sh
cargo run --release --bin bench -- --discovery 127.0.0.1:8357 -c 64 --workload mixed
```

Pass `--auth-secret <secret>` if the target node(s) or discovery server
require authentication (see [Authentication](#authentication)); it's a CLI
flag rather than an environment variable because `bench` is an interactive
dev/test tool, not a production service (mirroring `redis-cli -a`).

Pass `--tls-ca <path>` if the target node(s) or discovery server require
TLS (see [TLS](#tls)); `bench` then connects to every node and the
discovery server over TLS instead of plaintext, trusting only the CA(s) in
that file.

Note: running bench and the node(s) it's driving on the same machine means
they compete for the same CPU cores, which can make bench itself the
bottleneck once enough nodes are involved. For a trustworthy capacity
measurement of more than a couple of nodes, run bench on separate hardware
from the nodes.

### Discovery server

`src/bin/nanocached-discovery.rs` is a standalone cluster-membership
registry that cache nodes and client SDKs use to find each other for
horizontal scaling (see `doc/adr/0002-*.md` for the design rationale). It
has no dependency on nanocached's own protocol modules.

```sh
cargo run --bin nanocached-discovery -- --help
cargo run --bin nanocached-discovery -- --port 8357
```

It supports the same `NANOCACHED_AUTH_SECRET`-based authentication and
`--tls-cert`/`--tls-key`-based TLS as `nanocached-node` (see
[Authentication](#authentication) and [TLS](#tls)); nodes and `bench` speak
the same `A` handshake and TLS handshake to it as they do to a cache node.

`--replication-factor <n>` (default 2, min 1) sets how many nodes hold
each key (`doc/adr/0011-*.md`): keys are ranked by rendezvous hashing and
live on their top-R nodes, so any single node death costs no cached data —
reads fail over to the next owner. Discovery is R's single source of
truth: clients learn it from the `L` response, nodes from `M`. Effective
cluster capacity is total memory ÷ R; `--replication-factor 1` restores
single-copy behavior.

Discovery's registry is soft state, rebuilt from node announces, so it can
run as several independent replicas with no coordination between them
(`doc/adr/0010-*.md`): point every node's `--discovery` (and every SDK
client's `seeds`) at the same list of replicas, and losing any one replica
— including the primary — costs neither cache traffic nor client
bootstrap. Only *joins* need the primary up. After a (re)start, a replica
answers `L` with `B` (busy) for `--startup-grace` seconds (default: the
liveness timeout) while live members re-announce themselves, so a
bootstrapping client never sees a half-recovered node list.

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
