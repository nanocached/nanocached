# nanocached (Rust)

Async (tokio) client SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake, so the calling code is identical either way.

## Quick start

```rust
use nanocached::{NanocachedClient, Options};

// Point at a single node, or at a discovery server fronting a
// cluster — same call either way.
let client = NanocachedClient::connect(Options::new().host("127.0.0.1", 8357)).await?;

client.set("greeting", "hello", Some(60)).await?;      // TTL in seconds
let value: Option<Vec<u8>> = client.get("greeting").await?;
let existed: bool = client.delete("greeting").await?;

client.close();
```

Keys and values are anything `AsRef<[u8]>`; values come back as
`Option<Vec<u8>>`. The client is a cheaply cloneable handle — clones
share one set of connections. Requests are serialized per connection
(concurrent callers queue).

## Discovery replicas

When the cluster runs more than one discovery server, list them all;
both the initial connect and every node-list refresh try them in order.
A seed that is warming up after a restart (answers `B`) is skipped like
an unreachable one; if every seed is warming up, `connect()` returns
`Error::DiscoveryBusy` — retry shortly.

```rust
let client = NanocachedClient::connect(
    Options::new().host("10.0.0.1", 8357).host("10.0.0.2", 8357),
).await?;
```

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `set`/`delete` fan out to all R owners
of a key (the primary's result decides; a dead replica never fails a
write), and `get` asks the primary, falling over to the next owner only
when the holder is unreachable. `client.replication()` exposes the
factor in use. A write whose primary just died recovers automatically
once discovery drops the node (bounded by its liveness timeout): the
failed attempt forces a node-list refresh and one retry.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 30 seconds; a request
that finds its connection dead redials and retries once transparently
(all operations are idempotent). If that extra round trip matters, opt
in to keep-alive:

```rust
let client = NanocachedClient::connect(
    Options::new()
        .host("127.0.0.1", 8357)
        .keep_alive_interval(std::time::Duration::from_secs(15)),
).await?;
```

Every dial (connect, redial, refresh) is bounded by a 10-second
connect deadline covering the TCP connect and handshake, so a node
whose address has become a blackhole (a dead cloud instance) fails
over instead of hanging.

## Authentication and TLS

```rust
// NANOCACHED_AUTH_SECRET on the server:
let options = Options::new().host("cache.internal", 8357).auth_secret("change-me");
```

TLS is behind the `tls` feature and takes a rustls `ClientConfig` the
caller builds (system roots, a private CA — your choice):

```toml
nanocached = { version = "0.1", features = ["tls"] }
```

```rust
let options = options.tls(std::sync::Arc::new(rustls_client_config));
```

## Notes

- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server.
- It shares no code with the server (the repository's independence
  rule); the hash pipeline is pinned to cross-language test vectors that
  the server, TypeScript, Python, and Java implementations also assert.

## License

MIT
