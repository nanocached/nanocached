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
are pipelined per connection (request pipelining) — concurrent callers on
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

## Namespaces

A namespace is a flat, opaque byte string that scopes a key: the same
key name in two namespaces (or with no namespace at all) is a wholly
independent entry. `client.namespace(ns)` returns a lightweight handle
with the same `get`/`get_bytes`/`set`/`delete`/`clear` methods as the
client itself, just scoped to `ns`:

```rust
let users = client.namespace("users");
users.set("alice", "online", 0).await?;
client.set("alice", "offline", 0).await?; // a different entry — no namespace

assert_eq!(users.get("alice").await?, Some("online".to_string()));
assert_eq!(client.get("alice").await?, Some("offline".to_string()));

users.clear().await?; // drops every entry in "users" — "alice" (offline) survives
assert_eq!(users.get("alice").await?, None);
assert_eq!(client.get("alice").await?, Some("offline".to_string()));
```

The handle is cheap — it shares the client's connections and routing,
and opens no sockets of its own — and forwards to the same internal
methods `get`/`set`/`delete` themselves use, so it gets identical
semantics: routing, replication fan-out, hedged reads, `W`
refresh-and-retry, response tags, and compression all key off `(ns,
key)` together. `client.namespace("")` returns a handle equivalent to
the client itself (the empty namespace is the default one every
namespace-less call already uses); `ns.name()` returns the namespace
back. A handle outlives neither the client's connections nor its own
lifetime — using one after `close()` raises the same `AlreadyClosed`
error the client's own methods do.

On the wire, a non-empty namespace switches `get`/`set`/`delete` from
the `G`/`S`/`D` frames to their lowercase `g`/`s`/`d` counterparts,
which carry the namespace's length and bytes alongside the key's; the
default (empty) namespace always sends the exact legacy `G`/`S`/`D`
bytes, so code that never touches namespaces is unaffected and keeps
working against an older server. A namespace has no length limit of its
own beyond the request-size bound this crate already applies to
key+value, and — like a key — may contain any bytes; there is no
delimiter, no escaping, no hierarchy. Namespaced frames need a
namespace-aware server; talking to an older one, don't use them.

### Clearing a namespace, or everything

`namespace.clear()` drops every entry in that one namespace —
`namespace("").clear()` clears the default namespace, and is not
rejected. `client.clear_all()` flushes the whole store instead: every
namespace, the default one included:

```rust
client.namespace("users").clear().await?; // just "users"
client.clear_all().await?;                // everything, every namespace
```

Neither is key-addressed (a namespace's keys are spread over every node
by rendezvous hashing), so in a cluster both fan out to every node the
client currently knows about and succeed only once every one of them
has acked — never a partial clear. If any node fails, the node list is
refreshed once (the same path a `W` reply already triggers) and the
whole fan-out is retried against the refreshed list; a node still
failing after that fails the call with an error naming it. Both are
idempotent, so a caller can simply retry on error. On the wire these are
the `c`/`F` commands, a single O(1) sub-map drop on each node rather
than a scan-and-delete, so neither stalls other requests to that node.

## Fire-and-forget replica writes

Off by default. `set`/`delete` normally wait for every replica leg to
finish, same as the primary. Enabling `fire_and_forget_replicas` returns
as soon as the primary acks, letting replica legs finish in the
background (fire-and-forget replica writes):

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
background if one still has the value (read repair):

```rust
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .read_repair(true);
```

Closes the narrow window after a primary restart where a replica still
holds a key its (fresh) primary doesn't, at the cost of extra reads only
on the misses that hit that window. The repair write carries a fixed 60-second TTL — the wire protocol's `G` response never returns the original one to preserve, and no TTL at all would immortalize already-expired keys — and,
unlike fire-and-forget replica writes, is uncapped and not drained on
`close()`: this only fires on an already-rare clean miss, and losing one
costs nothing beyond staying in the window for one more read.

## Hedged reads

Off by default. A read goes to the key's primary owner and moves on to
the next owner only when the primary *fails* — so one slow-but-alive
node (a saturated host, a bad link) makes every read that touches it
wait out its full round trip, and with `R` copies on `N` nodes that is
roughly `R/N` of all reads. Setting `read_hedge_after` (a `Duration`)
sends the same read to the next owner as well once the primary has been
silent for that long, and takes the first answer:

```rust
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .read_hedge_after(Duration::from_millis(10)); // hedge after 10 ms
```

A hit from any owner is final. A miss is only final from the primary: a
replica's miss is provisional (it may simply lack the copy), so the
primary's answer is still waited for and hedging never turns a hit into
a miss — a genuine miss on a slow primary still pays its round trip. Pick
a value a few times the healthy p99 so a fast cluster hedges rarely: each
hedge costs one extra read on another owner. Needs `R >= 2`; with a
single copy there is nobody to hedge to. Writes are unaffected — every
copy must be written, so a slow owner bounds writes to it regardless
(`fire_and_forget_replicas` moves only the replica legs off the caller's
path). The losing leg of a hedge is never cancelled — dropping a
request mid-write could leave the connection desynced — so it is left
to finish and drained by `close()`, the same way a fire-and-forget
replica write is.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent). There is nothing to configure.

Every dial (connect, redial, refresh) is bounded by a 5-second
connect deadline covering the TCP connect and handshake, so a node
whose address has become a blackhole (a dead cloud instance) fails
over instead of hanging.

An address whose redial just failed is treated as still down for
`Options::reconnect_cooldown` (default 1 second): requests routed to it
during that window fail immediately with the original dial error instead
of each paying another full 5-second connect timeout.

```rust
let options = Options::new()
    .addresses([("127.0.0.1", 8357)])
    .reconnect_cooldown(std::time::Duration::from_millis(500));
```

Keep it well under the 30-second node-list refresh interval so a node
that genuinely recovers isn't shut out for long. `Duration::ZERO` means
"use the default" (matching the Go SDK's zero-value `Config`, which
can't tell "not specified" apart from "explicitly zero"); call
`.disable_reconnect_cooldown()` to disable it entirely instead (every
request that finds a dead connection pays its own full dial attempt) —
the Go SDK's equivalent is a negative `Config.ReconnectCooldown`.

`connect()` itself tolerates a node that discovery still lists but that
can't be reached — typically one that just died and hasn't been evicted
yet (a window of seconds): the node is kept in the ring without a
connection, requests for its keys fail over per request exactly as they
would after a mid-life death, and it is redialed after the cooldown.
Only a cluster with no reachable node at all fails `connect()`.

Every `get`/`set`/`delete` also carries its own 30-second wall-clock
timeout, measured from when that request is issued, so a half-open
server (one that accepts the connection but stops answering) can't hang
a caller forever. This is a deliberate difference from the Go SDK, whose
timeout is connection-level and progress-based — it resets whenever any
response arrives on the connection, not per request. Under deep
pipelining against a slow-but-healthy server the two SDKs can therefore
time out differently for the same workload; that's intentional, not a
bug, in both SDKs.

## Authentication and TLS

```rust
// NANOCACHED_AUTH_SECRET on the server:
let options = Options::new()
    .addresses([("cache.internal", 8357)])
    .auth_secret("change-me");
```

TLS is behind the `tls` feature, enabled by default:

```toml
nanocached = "0.2"                                     # tls included
nanocached = { version = "0.2", default-features = false } # plaintext only, smaller build
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
`get`/`get_bytes` (value compression). Behind the `compression` feature,
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

- Requires Rust 1.85 or newer (`rust-version` in `Cargo.toml`; checked in
  CI). See [CHANGELOG.md](CHANGELOG.md) for release notes.
- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server.
- It shares no code with the server (the repository's independence
  rule); the hash pipeline is pinned to cross-language test vectors that
  the server, TypeScript, Python, and Java implementations also assert.
- Every failure is a variant of the one `Error` enum. Caller mistakes
  are `Error::InvalidArgument` — this SDK's `Result`-based idiom for
  what the other SDKs raise as host-language builtins (issue #47);
  authentication failure surfaces as `Error::Protocol`.

## License

MIT
