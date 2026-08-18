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
let client = NanocachedClient::connect(Options::new().addresses([("127.0.0.1", 8357)])).await?;

client.set("greeting", "hello", 60).await?;             // TTL in seconds, 0 = no expiry
let value: Option<String> = client.get("greeting").await?;
let bytes: Option<Vec<u8>> = client.get_bytes("greeting").await?; // raw bytes, no UTF-8 check
let existed: bool = client.delete("greeting").await?;

client.close();
```

Keys and values are anything `AsRef<[u8]>`. `get` decodes the stored
value as UTF-8 with a strict decoder — a value that isn't valid UTF-8
returns `Error::InvalidUtf8` rather than lossily replacing the invalid
bytes; use `get_bytes` when a value might not be text. The client is a
cheaply cloneable handle — clones share one set of connections. Requests
are serialized per connection (concurrent callers queue).

## Addresses and discovery replicas

`Options::addresses` takes every connect target as one list, tried in
order for both the initial connect and every node-list refresh. A
single-node deployment is a one-element list:

```rust
let client = NanocachedClient::connect(Options::new().addresses([("127.0.0.1", 8357)])).await?;
```

When the cluster runs more than one discovery server, list them all —
this is what makes discovery itself redundant:

```rust
let client = NanocachedClient::connect(
    Options::new().addresses([("10.0.0.1", 8357), ("10.0.0.2", 8357)]),
).await?;
```

An address that is warming up after a restart (answers `B`) is skipped
like an unreachable one; if every address is warming up, `connect()`
returns `Error::DiscoveryBusy` — retry shortly. An empty addresses list
is rejected eagerly with `Error::InvalidArgument`.

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `set`/`delete` fan out to all R owners
of a key (the primary's result decides; a dead replica never fails a
write), and `get`/`get_bytes` ask the primary, falling over to the next
owner only when the holder is unreachable. `client.replication()`
exposes the factor in use. A write whose primary just died recovers
automatically once discovery drops the node (bounded by its liveness
timeout): the failed attempt forces a node-list refresh and one retry.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 30 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 15 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent). There is nothing to configure.

Every dial (connect, redial, refresh) is bounded by a 10-second
connect deadline covering the TCP connect and handshake, so a node
whose address has become a blackhole (a dead cloud instance) fails
over instead of hanging.

## Authentication and TLS

```rust
// NANOCACHED_AUTH_SECRET on the server:
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .auth_secret("change-me");
```

TLS is behind the `tls` feature, enabled by default:

```toml
nanocached = "0.1"                                     # tls included
nanocached = { version = "0.1", default-features = false } # plaintext only, smaller build
```

`tls` is a plain bool; `ca` names a PEM file of trusted root
certificate(s) and is only meaningful when `tls(true)` (silently ignored
otherwise). Without `ca`, `tls(true)` verifies against the platform's
trust store; with it, `ca`'s certificate(s) replace that store entirely.
An unreadable or unparseable CA file is a `connect()`-time error, as is
`tls(true)` when the crate was built with `default-features = false`.

```rust
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .tls(true);                              // platform trust store

let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .tls(true)
    .ca("/etc/nanocached/ca.pem");            // a private CA instead
```

## Notes

- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server.
- It shares no code with the server (the repository's independence
  rule); the hash pipeline is pinned to cross-language test vectors that
  the server, TypeScript, Python, and Java implementations also assert.

## License

MIT
