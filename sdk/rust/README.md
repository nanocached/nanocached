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

client.close().await;
```

Keys and values are anything `AsRef<[u8]>`. `get` decodes the stored
value as UTF-8 with a strict decoder — a value that isn't valid UTF-8
returns `Error::InvalidUtf8` rather than lossily replacing the invalid
bytes; use `get_bytes` when a value might not be text. The client is a
cheaply cloneable handle — clones share one set of connections. Requests
are pipelined per connection (doc/adr/0016-*.md) — concurrent callers on
the same connection each pay only their own network latency, not
everyone else's ahead of them.

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

## Fire-and-forget replica writes

Off by default. `set`/`delete` normally wait for every replica leg to
finish, same as the primary. Enabling `fire_and_forget_replicas` returns
as soon as the primary acks, letting replica legs finish in the
background (doc/adr/0014-*.md):

```rust
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .fire_and_forget_replicas(true);
```

Unlike `compress`, this is a pure latency/durability trade for this
client's own writes — it carries no wire format, and different clients
may use different settings freely. At most 32 replica writes across the
whole client run in the background at once; past that cap, further
replica legs run synchronously exactly as with the option off (a
graceful degrade, not a queue or a drop). `close()` is async and
returns only after any still-in-flight background replica writes have
finished and the connections are torn down.

## Read repair

Off by default. A clean miss (the key's first-reached owner reports it
missing) is normally accepted as-is. Enabling `read_repair` probes the
remaining owners before accepting that, and repairs the primary in the
background if one still has the value (doc/adr/0015-*.md):

```rust
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .read_repair(true);
```

Closes the narrow window after a primary restart where a replica still
holds a key its (fresh) primary doesn't, at the cost of extra reads only
on the misses that hit that window. The repair write carries no TTL —
the wire protocol's `G` response never returns one to preserve — and,
unlike fire-and-forget replica writes, is uncapped and not drained on
`close()`: this only fires on an already-rare clean miss, and losing one
costs nothing beyond staying in the window for one more read.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
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

## Value compression

Off by default. When enabled, values at or above `compression_threshold`
bytes are transparently DEFLATE-compressed on `set` and decompressed on
`get`/`get_bytes` (doc/adr/0013-\*.md). Behind the `compression` feature,
enabled by default alongside `tls`:

```rust
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .compress(true)
    .compression_threshold(256); // default; bytes, below which values are stored as-is
```

Without the `compression` feature, `compress(true)` is a `connect()`-time
error instead of a compile error — same shape as `tls(true)` without the
`tls` feature.

**Every client that reads or writes a given set of keys must agree on
`compress`.** This is a per-keyspace format decision, not a per-client
preference — enabling it prefixes every value this client writes with a
one-byte marker, so a client with `compress(false)` reading one of those
values gets the marker byte back as if it were part of the value (wrong,
silently), and a client with `compress(true)` reading a value written
before compression was enabled anywhere risks misreading that value's
first byte as the marker (an `Error::Decompression`, or — if that byte
happens to be the "uncompressed" marker by chance, or the decoder doesn't
reject the garbage that follows it — a silently wrong read; raw DEFLATE
has no checksum). There is no dual-mode migration path: only turn this on
for a fresh keyspace, or only after every client touching an existing one
has upgraded and enabled it together. Incompressible data
(already-compressed media, random bytes) is passed through unchanged
rather than bloated.

## Notes

- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server.
- It shares no code with the server (the repository's independence
  rule); the hash pipeline is pinned to cross-language test vectors that
  the server, TypeScript, Python, and Java implementations also assert.

## License

MIT
