<p align="center">
  <img src="docs/assets/logo.jpg" alt="nanocached" width="100%">
</p>

<p align="center">
  <a href="https://github.com/nanocached/nanocached/actions/workflows/ci.yaml"><img src="https://github.com/nanocached/nanocached/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://github.com/nanocached/nanocached/actions/workflows/chaos-nightly.yaml"><img src="https://github.com/nanocached/nanocached/actions/workflows/chaos-nightly.yaml/badge.svg?branch=main" alt="Chaos nightly"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/nanocached/nanocached" alt="License: MIT"></a>
  <a href="https://crates.io/crates/nanocached"><img src="https://img.shields.io/crates/v/nanocached?label=crates.io" alt="crates.io"></a>
  <a href="https://www.npmjs.com/package/nanocached"><img src="https://img.shields.io/npm/v/nanocached?label=npm" alt="npm"></a>
  <a href="https://pypi.org/project/nanocached/"><img src="https://img.shields.io/pypi/v/nanocached?label=PyPI" alt="PyPI"></a>
  <a href="https://www.nuget.org/packages/Nanocached"><img src="https://img.shields.io/nuget/v/Nanocached?label=NuGet" alt="NuGet"></a>
  <a href="https://central.sonatype.com/artifact/org.nanocached/nanocached"><img src="https://img.shields.io/maven-central/v/org.nanocached/nanocached?label=Maven%20Central" alt="Maven Central"></a>
</p>

nanocached is a compact in-memory key-value cache server written in Rust. It
uses a small, binary-safe TCP protocol and supports optional time-to-live
(TTL) values. One node runs on its own; several nodes form a replicated
cluster in which the clients do the routing.

> [!NOTE]
> nanocached is currently experimental and is not ready for production use.

Full documentation is at <https://nanocached.org>.

## Features

- In-memory storage for binary keys and values, with optional TTL
- Get, set and delete, their namespaced forms, and clearing one namespace or
  all of them
- Atomic counters, in-place TTL refresh, compare-and-set and
  compare-and-delete, and batched get, set and touch
- Memory-bounded cache with least-recently-used (LRU) eviction
- Replicated clusters with client-side routing, a discovery server for
  membership, and an optional stateless proxy for clients that cannot route
- Optional shared-secret authentication and optional TLS
- Multiple and pipelined requests per TCP connection
- Request-size, connection-count and idle-timeout limits
- Client SDKs for six languages and seven framework adapters

## Quick start

### Docker

Images of the latest release are published to GitHub Container Registry as
`:latest` and `:<version>`. Every `main` commit that passes CI is also
published as `:main` and `:sha-<commit>`, for trying unreleased changes.
[Compatibility](https://nanocached.org/compatibility.html) says
what each tag promises; pin `:<version>`, not `:latest`, in production.

```sh
docker pull ghcr.io/nanocached/nanocached-node:latest
docker run --rm --publish 8356:8356 ghcr.io/nanocached/nanocached-node:latest
```

### From source

Rustup selects the toolchain pinned in `rust-toolchain.toml` automatically.

```sh
git clone https://github.com/nanocached/nanocached.git
cd nanocached
cargo run --bin nanocached-node -- --host 0.0.0.0 --port 8356
```

A node listens on `127.0.0.1:8356` by default; override that with `--host`
and `--port`. Run `cargo run --bin nanocached-node -- --help` for every
option.

### Try it

Store the value `Alice` under the key `name`, then read it back:

```sh
printf 'S 4 5\nnameAliceG 4\nname' | nc 127.0.0.1 8356
```

The server responds with:

```text
S
V 5
Alice
```

Each request is an ASCII header ending in `\n`, followed by the key and value
bytes, whose lengths the header gives. `S 4 5` sets a 4-byte key to a 5-byte
value, and `G 4` gets a 4-byte key. The [protocol reference](https://nanocached.org/protocol.html)
lists every command.

## Programs

Building the repository (`cargo build`) produces these binaries:

| Binary | Role | Default port |
|---|---|---|
| `nanocached-node` | The cache server itself | 8356 |
| `nanocached-discovery` | Cluster-membership registry: nodes announce themselves to it and clients fetch the node list from it | 8357 |
| `nanocached-proxy` | Optional stateless proxy that routes for clients that cannot hold the node list themselves (see the [proxy guide](https://nanocached.org/deployment-proxy.html)) | 8358 |
| `ncd` | Thin dispatcher: `ncd node start [options]` and `ncd discovery start [options]` | — |

`ncd` looks up `nanocached-node` and `nanocached-discovery` next to its own
executable, so it works once `cargo build` has built them into the same
`target/` directory; `cargo run --bin ncd` alone does not build them.

## Running a cluster

A cluster is several nodes plus a discovery server. Start the discovery
server, then point each node at it with `--discovery`:

```sh
cargo run --bin nanocached-discovery -- --port 8357
cargo run --bin nanocached-node -- --port 8356 --discovery 127.0.0.1:8357
cargo run --bin nanocached-node -- --port 8358 --discovery 127.0.0.1:8357
```

Every key lives on `--replication-factor` nodes (default 2, set on the
discovery server), so one node dying loses no cached data; effective capacity
is the total memory divided by that factor. Nodes join with a staged data
handoff, and on SIGTERM a node hands its keys to their next owners before it
exits. Discovery can run as several independent replicas: list the same
replicas in the same order on every node and client with
`--discovery host1:8357,host2:8357`.

See [getting started](https://nanocached.org/getting-started.html) for
replication and discovery replicas in detail, and the
[deployment guides](https://nanocached.org/deployment.html) for verified
setups on EC2, ECS Fargate and EKS.

## Authentication

Set `NANOCACHED_AUTH_SECRET` to require clients to authenticate with a shared
secret before issuing any other command. It is an environment variable rather
than a flag so the secret does not show up in `ps` output. Set it on the
node and the discovery server alike; a node uses the same value to
authenticate its own heartbeats.

```sh
NANOCACHED_AUTH_SECRET=change-me cargo run --bin nanocached-node -- --port 8356
NANOCACHED_AUTH_SECRET=change-me cargo run --bin nanocached-discovery -- --port 8357
```

Leaving it unset or empty disables authentication, and a server that then
listens on a non-loopback address logs a warning at startup. Authentication
is a second layer of defense, not a substitute for network isolation:
without TLS the protocol has no transport encryption, so keep servers on
`127.0.0.1` or a private network.

## TLS

Pass `--tls-cert` and `--tls-key` (PEM files) to require TLS on every
connection a node or discovery server accepts; there is no plaintext
fallback once they are set.

```sh
cargo run --bin nanocached-node -- --port 8356 --tls-cert cert.pem --tls-key key.pem
```

A node that registers with a TLS-secured discovery server also needs
`--tls-ca` (a PEM file of the CA certificate(s) to trust). In a cluster the
certificates must carry each node's IP address in `subjectAltName`. The
[getting started](https://nanocached.org/getting-started.html#tls) page has
an OpenSSL command that generates a certificate for local development, and
the [TLS guide](https://nanocached.org/deployment-tls.html) covers clusters.

## Docker images

The `Dockerfile` builds a separate minimal image per role. `--target` is
required; there is no default final stage.

```sh
docker build --target node --tag nanocached-node .
docker run --rm --publish 8356:8356 nanocached-node

docker build --target discovery --tag nanocached-discovery .
docker run --rm --publish 8357:8357 nanocached-discovery
```

The images set `--host 0.0.0.0` as their default command. Arguments you pass
to `docker run` replace that default, so pass `--host 0.0.0.0` again
whenever you pass any other argument, as the proxy needs to:

```sh
docker build --target proxy --tag nanocached-proxy .
docker run --rm --publish 8358:8358 nanocached-proxy \
  --host 0.0.0.0 --discovery disc:8357
```

(`disc` has to resolve from inside the container; put the containers on a
shared Docker network.) The prebuilt `nanocached-discovery` and
`nanocached-proxy` images are at `ghcr.io/nanocached/` next to
`nanocached-node`.

## Client SDKs

Each SDK talks to a single node or to a discovery-fronted cluster, detects
which from the server's handshake, and handles authentication, TLS, and
routing with replication. See the [SDK overview](https://nanocached.org/sdks.html).

| Language | Package | Requires | README |
|---|---|---|---|
| TypeScript / Node.js | npm `nanocached` | Node.js 22+ | [`sdk/typescript`](sdk/typescript/README.md) |
| Python | PyPI `nanocached` | Python 3.11+, no runtime dependencies | [`sdk/python`](sdk/python/README.md) |
| Java | Maven `org.nanocached:nanocached` | Java 17+, no runtime dependencies | [`sdk/java`](sdk/java/README.md) |
| Rust | crates.io `nanocached` | Rust 1.88+; `tls` and `compression` are default features | [`sdk/rust`](sdk/rust/README.md) |
| .NET | NuGet `Nanocached` | .NET 10+, no dependencies | [`sdk/dotnet`](sdk/dotnet/README.md) |
| Go | `github.com/nanocached/nanocached/sdk/go` | Go 1.26+, standard library only | [`sdk/go`](sdk/go/README.md) |

For example, with the Python SDK (`pip install nanocached`), against the node
started above:

```python
import asyncio
from nanocached import NanocachedClient

async def main():
    async with await NanocachedClient.connect([("127.0.0.1", 8356)]) as client:
        await client.set("greeting", "hello", ttl_seconds=60)
        print(await client.get("greeting"))   # hello
        print(await client.delete("greeting"))  # True

asyncio.run(main())
```

Framework adapters (Django, Spring, Spring Boot, JCache, Keyv, cache-manager
and .NET caching) live under [`adapters/`](adapters); they are described on the
SDK overview page.

## Limits

Defaults. Where a flag exists it is named; discovery's limits are fixed:

| | `nanocached-node` | `nanocached-discovery` |
|---|---|---|
| Maximum request size | 1 MiB | 4 KiB |
| Concurrent connections | 1,024 (`--max-connections`), at most 256 per source IP (`--max-connections-per-ip`) | 1,024, at most 256 per source IP |
| Cache memory | 256 MiB (`--max-memory`, at least 1 MiB), LRU eviction | — |
| In-flight bytes outside the cache | 256 MiB (`--max-in-flight-bytes`) | — |
| Idle connection timeout | 60 seconds | 60 seconds |

`--max-memory` bounds the cache only. Size the process at `--max-memory` plus
`--max-in-flight-bytes` plus headroom. For how the in-flight budget is
charged, see [Sizing a node process](https://nanocached.org/deployment-operations.html#memory-budget);
the proxy's own limits are in the [proxy guide](https://nanocached.org/deployment-proxy.html).

## More documentation

- [Architecture](https://nanocached.org/architecture.html): routing,
  replication, joins, and the [accepted behaviour](https://nanocached.org/architecture.html#accepted-behaviour)
  list. Read the list before relying on a cluster during a membership change.
- [Protocol](https://nanocached.org/protocol.html): every command and status.
- [Deployment](https://nanocached.org/deployment.html): EC2, ECS and EKS,
  [operations](https://nanocached.org/deployment-operations.html) and
  [metrics](https://nanocached.org/deployment-metrics.html) (`--metrics-port`
  serves Prometheus `/metrics`, `/healthz` and `/readyz`).
- [Capacity planner](https://nanocached.org/capacity-planner.html): an
  offline estimator of hit rate against memory, replication and TTL.
- [Compatibility](https://nanocached.org/compatibility.html): the project is
  pre-1.0, so any release may break compatibility; this page describes what
  1.0.0 will freeze.

## Contributing

Building from source, running the tests and the project's checks are
described in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

nanocached, its six SDKs and its seven framework adapters are released under
the [MIT License](LICENSE).
