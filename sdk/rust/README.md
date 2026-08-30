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

## SDK proxy mode

`Options::via_proxy(true)` connects through a `nanocached-proxy` tier
instead of joining the cluster directly — useful once a fleet of client
processes is large enough that one connection per node per process would
overrun a node's own connection limit. `addresses` must still name
discovery server(s); `connect()` fetches the *proxy* roster from
discovery (not the node roster) and lands on one proxy, chosen at
random so a fleet of clients spreads across the proxy tier:

```rust
let client = NanocachedClient::connect(
    Options::new()
        .addresses([("10.0.0.1", 8357)])
        .via_proxy(true),
).await?;
```

A proxy looks, on the wire, exactly like a single node that owns every
key, so from here on the client runs in its existing single-connection
mode: no ring view, no per-node connections, and **`read_hedge_after` is
inert if also set** — there are no replicas on this one connection to
hedge a read to. Namespaces, `clear`/`clear_all`, compression, and
keep-alive all work unchanged. If the proxy connection is lost, the same
proxy is redialed first (it may simply have restarted); only if that
also fails does the client re-fetch the roster from discovery and swap
onto another, randomly chosen, reachable proxy. Pointing `via_proxy` at
an address that turns out to be a cache node, not a discovery server,
fails `connect()` fast with `Error::InvalidArgument`; an empty (or
wholly unreachable) proxy roster is a normal connect error.

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `set`/`delete` fan out to all R owners
of a key (the primary's result decides; a dead replica never fails a
write), and `get`/`get_bytes` ask the primary, falling over to the next
owner only when the holder is unreachable. `client.replication()`
exposes the factor in use. A write whose primary just died recovers
automatically once discovery drops the node (bounded by its liveness
timeout): the failed attempt forces a node-list refresh and one retry.

## Counters (`incr`/`decr`)

`incr` atomically adds a signed delta to an integer counter stored at a
key, and returns its new value:

```rust
client.set("hits", "10", 0).await?;
assert_eq!(client.incr("hits", 1).await?, Some(11));
assert_eq!(client.decr("hits", 5).await?, Some(6));
```

`decr` is `incr` with the delta negated — there is no separate wire
opcode, so `client.decr(key, 5).await` is exactly `client.incr(key,
-5).await`. Both return `None` when the key is missing or expired,
matching `get`/`get_bytes`'s own miss convention, and
`Err(Error::NotNumeric)` when the key exists but its stored value isn't a
plain signed-decimal integer (or the delta would overflow `i64`). Both
are also available on a `Namespace` handle, scoped exactly like its
`get`/`set`/`delete`.

**`incr` is exactly as volatile as `set`**: LRU eviction and TTL expiry
reclaim an incremented value like any other entry. It's a good fit for
rate limiting or approximate counters, not a substitute for a durable
counter (billing, inventory) — nothing here makes a counter survive
eviction that a plain `set` value wouldn't.

In a cluster, only the key's primary owner ever runs the increment;
replicas instead receive its literal new value as an ordinary `set`, so a
replica that missed an earlier write (or evicted the key on its own)
converges to the primary's exact value instead of drifting from replaying
the increment independently.

**At-least-once, not exactly-once, under connection loss.** `incr`/`decr`
are not idempotent — replaying one would double-apply `delta` — so unlike
`get`/`set`/`delete`, this SDK never silently retries an increment whose
request had already been fully written to the socket when the connection
was lost: the server may have already applied it before the reply went
missing, and that surfaces as a plain `Err(Error::ConnectionLost)` rather
than a redial-and-retry. Only a connection already dead *before* the call
even reached it (the idle-FIN case, e.g. the server's 60s idle timeout) is
retried, since nothing could have been applied yet. On
`Err(Error::ConnectionLost)`, whether the counter actually changed is
unknown — check with a subsequent `get` if that matters.

## Batched get and set

`get_many`/`get_many_bytes` and `set_many`/`set_many_bytes` (the `m`/`o`
frames) fetch or store several keys in one round trip per owner instead
of one round trip per key:

```rust
use std::collections::HashMap;

client.set_many(&HashMap::from([("a".to_string(), "1".to_string())]), 0).await?; // shared ttl_seconds for the whole batch
let values = client.get_many(&["a", "b", "missing"]).await?;
// {"a": "1"} — "missing" is simply absent
```

A missing key is simply absent from the returned `HashMap`, the same "a
miss is not an error" shape `get`/`get_bytes` use. Both are also
namespace-scoped: `client.namespace(ns).get_many(...)`/`.set_many(...)`,
same as `get`/`set`. Batch keys are always `String`-shaped (`&[impl
AsRef<str>]` for reads, `&HashMap<String, _>` for writes) — unlike
single-key `get`/`set`, which accept any `impl AsRef<[u8]>`.

**A batch never fails as a whole.** Each key's outcome is independent:
if some keys are still routed to the wrong node after one bounded
refresh-and-retry (the same policy `get`/`set` apply per key, not per
call), `get_many`/`get_many_bytes` return
`Err(Error::PartialWrongNode(map))`/`Err(Error::PartialWrongNodeText(map))`
— the `map` holds every key that DID resolve, so a caller that wants the
partial results can match on it directly:

```rust
match client.get_many(&keys).await {
    Ok(values) => values,
    Err(Error::PartialWrongNodeText(partial)) => partial,
    Err(error) => return Err(error),
}
```

`set_many`/`set_many_bytes` have nothing to attach on a persisting
wrong-node, so they just return a plain `Err(Error::WrongNode)` — every
other key in the batch was still stored. In single-node/proxy mode a `W`
propagates immediately, exactly like `get`/`set`'s own single-mode
behavior — there is no ring to refresh against.

Within one `set_many`/`set_many_bytes` batch, the same node can be one
key's primary and another key's replica at once; it receives exactly
one `o` sub-frame either way, and only its answer for the keys it is
primary for decides those keys' outcome — a replica-held key's failure
is logged-and-swallowed into `stats().replica_write_failures`, exactly
like a plain `set`'s own replica legs.

Very large batches are transparently split into more than one `m`/`o`
sub-frame per owner — callers never need to think about this.

## Compare-and-set

`put_if_absent`, `replace_if_present`, `replace`, and `delete_if_matches`
condition a write or delete on the key's current state instead of just
overwriting it unconditionally:

```rust
assert!(client.put_if_absent("name", "Alice", 0).await?); // stored — was absent
assert!(!client.put_if_absent("name", "Bob", 0).await?);  // mismatch — already exists

let (value, token) = client.get_with_token("name").await?.unwrap();
assert!(client.replace("name", token, "Bob", 0).await?); // stored — digest matched
assert!(client.delete_if_matches("name", nanocached::content_digest(b"Bob")).await?);
```

`replace`/`delete_if_matches`'s expected value is a **digest**, not the
literal value: `get_with_token` returns one alongside the value it read
(a `CasToken`, wrapping a 16-byte SHA-256-derived digest of the value's
exact stored bytes), and a bare `[u8; 16]` from `nanocached::content_digest`
works too via `impl Into<CasToken>`. The `get_with_token`-then-`replace`
path is always correct; reconstructing a digest from a value you already
hold (rather than one just read back) is only correct if your
re-serialization is byte-identical to what the server actually stores —
the same caveat memcached's own value-based CAS has. All four are also
available on a `Namespace` handle, scoped exactly like its
`get`/`set`/`delete`. Every mismatch is a plain `false`, never an error —
the same idiom `delete` already uses for "nothing here to act on".

**This is not a distributed lock.** LRU eviction reclaims a key exactly
as it would after a plain `set`, CAS or not — a key used as a lock
(`put_if_absent` to acquire, a TTL to eventually release) that gets
evicted under memory pressure lets a second caller's `put_if_absent`
succeed while the first still believes it holds the lock, and CAS alone
cannot detect that silent double-acquisition.

In a cluster, only the key's primary owner ever evaluates the condition;
on success, replicas receive the literal result as an ordinary `set`/
`delete`, mirroring `incr`'s own replication rule above. See
`docs/protocol.html#cas` for the full wire spec.

**At-least-once, not exactly-once, under connection loss.** CAS is not
idempotent — replaying a request that already succeeded could report a
now-stale condition as a mismatch — so, exactly like `incr`/`decr` above,
this SDK never silently retries a `put_if_absent`/`replace_if_present`/
`replace`/`delete_if_matches` request whose bytes had already been fully
written to the socket when the connection was lost. That surfaces as a
plain `Err(Error::ConnectionLost)` instead of a redial-and-retry, and
whether the write actually happened is unknown — check with a subsequent
`get`/`get_with_token` if that matters. Only a connection already dead
before the call reached it is retried, since nothing could have been
applied yet.

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

## Transient retries and `Error::Retryable`

`nanocached-proxy` can answer an individual request with a transient
"try again shortly" status instead of failing it outright — its upstream
node was briefly unreachable and its own one refresh-and-retry didn't
land in time, but the proxy connection itself is perfectly healthy.
Nodes and discovery servers never send this on their own, but every
connection this SDK opens (per-node, proxy, discovery, hedge leg,
reconnect) understands it regardless, since a node or proxy address can
change roles over a cluster's lifetime.

When that happens, this SDK retries the same request on the exact same
connection — no redial, nothing torn down — up to twice more (three
attempts total, 50ms before the first retry and 100ms before the
second). If the third attempt still comes back transient, `get`/`set`/
`delete`/`clear`/`clear_all` return `Error::Retryable` instead of
succeeding — the connection stays open and usable for whatever the
caller does next; only that one operation failed. This is entirely
transparent to hedged reads: a hedge leg that lands on a proxy answering
transiently just retries on its own connection like any other read, and
its eventual `Error::Retryable` (if the retry budget runs out) is
treated like any other leg failure — hedging onward to the next owner
immediately, or propagating if none are left.

`client.stats().transient_retries` counts every one of these transient
replies this client has ever received, across every connection it has
opened over its lifetime — including the ones that were later replaced
by a redial, and including the last, exhausting reply that produced an
`Error::Retryable`. It never resets, and complements the other
`stats()` counters (`replica_write_failures`, `read_repair_failures`,
`refresh_failures`) as an observability signal: a client whose
`transient_retries` is climbing is talking to a proxy tier under strain,
even though most of those requests still ultimately succeed.

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
  authentication failure surfaces as `Error::Protocol`. `Error::Retryable`
  is the one variant that's never a sign of a real problem with the
  connection — see "Transient retries and `Error::Retryable`" above.

## License

MIT
