# nanocached (Go)

Go client SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake, so the calling code is identical either way.

Requires Go 1.22+. No dependencies outside the standard library.

## Install

```sh
go get github.com/nanocached/nanocached/sdk/go
```

## Quick start

```go
import nanocached "github.com/nanocached/nanocached/sdk/go"

// Point at a single node, or at a discovery server fronting a
// cluster — same call either way.
client, err := nanocached.Connect(nanocached.Config{
    Addresses: []nanocached.Address{{Host: "127.0.0.1", Port: 8357}},
})
if err != nil { ... }
defer client.Close()

err = client.Set("greeting", "hello", 60) // ttlSeconds; 0 = no expiry
value, ok, err := client.Get("greeting")  // ok=false when missing
existed, err := client.Delete("greeting")
```

Keys are `string`. Values are `string` via `Get`/`Set`, or raw `[]byte`
via `GetBytes`/`SetBytes` for binary data — `Get` decodes with a plain
`string(bytes)` conversion, which in Go is always lossless, so unlike
some other nanocached SDKs there is no decode-failure case to handle.
The client is safe for concurrent use; requests are pipelined per
connection (request pipelining) — concurrent callers on the same
connection each pay only their own network latency, not everyone
else's ahead of them.

## Discovery replicas

When the cluster runs more than one discovery server, list them all in
`Addresses`; both the initial connect and every node-list refresh try
them in order. An address that is warming up after a restart (answers
`B`) is skipped like an unreachable one; if every address is warming up,
`Connect` returns `ErrDiscoveryBusy` (match with `errors.Is`) — retry
shortly.

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `Set`/`SetBytes`/`Delete` fan out to all
R owners of a key (the primary's result decides; a dead replica never
fails a write), and `Get`/`GetBytes` ask the primary, falling over to
the next owner only when the holder is unreachable. `client.Replication()`
exposes the factor in use. A write whose primary just died recovers
automatically once discovery drops the node (bounded by its liveness
timeout): the failed attempt forces a node-list refresh and one retry.

## Fire-and-forget replica writes

Off by default. `Set`/`SetBytes`/`Delete` normally wait for every
replica leg to finish, same as the primary. Enabling
`FireAndForgetReplicas` returns as soon as the primary acks, letting
replica legs finish in the background (fire-and-forget replica writes):

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses:             []nanocached.Address{{Host: "cache.internal", Port: 8357}},
    FireAndForgetReplicas: true,
})
```

Unlike `Compress`, this is a pure latency/durability trade for this
client's own writes — it carries no wire format, and different clients
may use different settings freely. At most 32 replica writes across the
whole client run in the background at once; past that cap, further
replica legs run synchronously exactly as with the option off (a
graceful degrade, not a queue or a drop). `Close()` waits for any
still-in-flight background replica writes before tearing down
connections.

## Read repair

Off by default. A clean miss (the key's first-reached owner reports it
missing) is normally accepted as-is. Enabling `ReadRepair` probes the
remaining owners before accepting that, and repairs the primary in the
background if one still has the value (read repair):

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses:  []nanocached.Address{{Host: "cache.internal", Port: 8357}},
    ReadRepair: true,
})
```

Closes the narrow window after a primary restart where a replica still
holds a key its (fresh) primary doesn't, at the cost of extra reads only
on the misses that hit that window. The repair write carries a fixed 60-second TTL — the wire protocol's `G` response never returns the original one to preserve, and no TTL at all would immortalize already-expired keys — and, unlike
fire-and-forget replica writes, is uncapped and not drained on `Close()`:
this only fires on an already-rare clean miss, and losing one costs
nothing beyond staying in the window for one more read.

## Hedged reads

Off by default. A read goes to the key's primary owner and moves on to
the next owner only when the primary *fails* — so one slow-but-alive
node (a saturated host, a bad link) makes every read that touches it
wait out its full round trip, and with `R` copies on `N` nodes that is
roughly `R/N` of all reads. Setting `ReadHedgeAfter` sends the same read
to the next owner as well once the primary has been silent for that
long, and takes the first answer:

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses:      []nanocached.Address{{Host: "cache.internal", Port: 8357}},
    ReadHedgeAfter: 10 * time.Millisecond,
})
```

A hit from any owner is final. A miss is only final from the primary: a
replica's miss is provisional (it may simply lack the copy), so the
primary's answer is still waited for and hedging never turns a hit into
a miss — a genuine miss on a slow primary still pays its round trip. Pick
a value a few times the healthy p99 so a fast cluster hedges rarely: each
hedge costs one extra read on another owner. Needs `R >= 2`; with a
single copy there is nobody to hedge to. Writes are unaffected — every
copy must be written, so a slow owner bounds writes to it regardless
(`FireAndForgetReplicas` moves only the replica legs off the caller's
path). The losing leg of a hedge is left to finish and is drained by
`Close()`. Zero (the default) disables hedging; a negative value is
rejected by `Connect`.

## Namespaces

`Client.Namespace(ns)` returns a lightweight handle scoping
`Get`/`GetBytes`/`Set`/`SetBytes`/`Delete`/`Clear` to `ns`: the same key
name in two different namespaces — or in a namespace versus the default,
unnamespaced keyspace — names two independent cache entries.

```go
users := client.Namespace("users")
err = users.Set("42", "alice", 0)
value, ok, err := users.Get("42")       // "alice", scoped to "users"
_, ok, err = client.Get("42")           // unrelated: the default namespace
```

A `Namespace` does no networking of its own: it shares the client's
connections and every method simply forwards to the client's own
internal `(namespace, key)` methods, so routing (rendezvous hashing over
`(namespace, key)` — see below), replication fan-out, hedged reads, `W`
refresh-and-retry, response tags, and value compression all apply exactly
as they do to the client's own namespace-less methods. It's cheap to
create, safe for concurrent use, and becomes invalid — every method
returns `ErrClosed` — once the client is closed; it has no `Close` of its
own. `client.Namespace("")` returns a handle equivalent to the client
itself: legacy, byte-for-byte `G`/`S`/`D` frames and the same key
placement as before namespaces existed, so it's never rejected. A
namespace is a flat, opaque byte string — no delimiter, no escaping, no
hierarchy, and any bytes are allowed — and `Namespace.Name()` returns it
back.

Namespaces enter routing too: a key's owners are computed from
`(namespace, key)` rather than `key` alone, so the same key name in
different namespaces can land on different nodes. The default namespace
hashes exactly as it did before namespaces existed, so an unnamespaced
key's placement never moves across a rolling upgrade. Namespaced frames
need a server that understands them (an old server answers `E` and closes
the connection), so upgrade every node before using namespaces.

### Clearing a namespace, or everything

`Namespace.Clear()` drops every entry in that namespace; `Client.ClearAll()`
drops every namespace, the default one included:

```go
err = users.Clear()      // only the "users" namespace
err = client.ClearAll()  // everything, every namespace
```

Neither is key-addressed, so unlike `Get`/`Set`/`Delete` there's no single
owner to route to: the request fans out to every node in the cluster
concurrently, and each node drops its own share of the namespace. Success
requires every node to acknowledge; if any node fails, the client
refreshes its node list once and retries the whole fan-out against the
refreshed list, raising an error naming the still-failing node(s) only if
that retry also fails. Both operations are idempotent, so a caller can
simply retry on error. `client.Namespace("").Clear()` clears the default
namespace and is never rejected.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent).

An address whose redial just failed is treated as still down for
`Config.ReconnectCooldown` (default `DefaultReconnectCooldown`, 1 second):
requests routed to it during that window fail immediately with the
original dial error instead of each paying another full 5-second connect
timeout. Keep it short — a node that genuinely recovers is shut out for
at most this long. A zero `Config.ReconnectCooldown` means the default;
set it negative to disable the cooldown entirely (every request that
finds a dead connection pays its own full dial attempt). This mirrors
the Rust SDK, where `Options::reconnect_cooldown(Duration::ZERO)` also
means "use the default" and `Options::disable_reconnect_cooldown()` is
the equivalent of a negative value here.

`Connect` itself tolerates a node that discovery still lists but that
can't be reached — typically one that just died and hasn't been evicted
yet (a window of seconds): the node is kept in the ring without a
connection, requests for its keys fail over per request exactly as they
would after a mid-life death, and it is redialed after the cooldown.
Only a cluster with no reachable node at all fails `Connect`.

## Authentication and TLS

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses:  []nanocached.Address{{Host: "cache.internal", Port: 8357}},
    AuthSecret: "change-me", // NANOCACHED_AUTH_SECRET on the server
    TLS:        true,        // system/platform trust store by default
    CA:         "",          // path to a PEM file of trusted root cert(s);
                              // only meaningful when TLS is true, replacing
                              // the default trust store
})
```

`CA` is silently ignored when `TLS` is `false`. An unreadable or
unparseable CA file when `TLS` is `true` fails `Connect`.

## Values and TTL

```go
err = client.Set("k", "hello", 0)              // string value, no expiry
err = client.SetBytes("k", []byte{0xff}, 300)  // raw bytes, 300s TTL
value, ok, err := client.Get("k")              // string
raw, ok, err := client.GetBytes("k")           // []byte
```

`ttlSeconds` is a whole number of seconds; `0` means no expiry. A
negative `ttlSeconds` is rejected before any network call.

## Value compression

Off by default. When enabled, values at or above `CompressionThreshold`
bytes are transparently DEFLATE-compressed on `Set`/`SetBytes` and
decompressed on `Get`/`GetBytes` (value compression):

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses:            []nanocached.Address{{Host: "cache.internal", Port: 8357}},
    Compress:             true,
    CompressionThreshold: 256, // default; bytes, below which values are stored as-is
})
```

**Every client that reads or writes a given set of keys must agree on
`Compress`.** This is a per-keyspace format decision, not a per-client
preference — enabling it prefixes every value this client writes with a
one-byte marker, so a client with `Compress: false` reading one of those
values gets the marker byte back as if it were part of the value
(wrong, silently), and a client with `Compress: true` reading a value
written before compression was enabled anywhere risks misreading that
value's first byte as the marker (an `ErrDecompression`, or — if that
byte happens to be the "uncompressed" marker by chance, or the decoder
doesn't reject the garbage that follows it — a silently wrong read; raw
DEFLATE has no checksum). There is no dual-mode migration path: only
turn this on for a fresh keyspace, or only after every client touching
an existing one has upgraded and enabled it together. Incompressible
data (already-compressed media, random bytes) is passed through
unchanged rather than bloated.

## Notes

- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server. The hash
  pipeline is pinned to cross-language test vectors that the server and
  the TypeScript/Python/Java/Rust/.NET SDKs also assert.
- Errors: `ErrClosed`, `ErrWrongNode`, `ErrDiscoveryBusy`,
  `ErrConnectionLost`, `ErrAuthenticationFailed`, and `ErrDecompression`
  are sentinels for `errors.Is`. Caller mistakes (an invalid TTL, an
  empty address list) surface as ordinary errors outside the sentinel
  set — they indicate a bug in the calling code, not a nanocached
  failure; this convention is shared across the SDKs (issue #47).
- `Close()` is idempotent; calling it a second time warns to stderr
  instead of erroring. `Connect()` also warns to stderr if it's called
  for a single address that a previous, still-open connection from this
  process already points at — a common sign that `Close()` was
  forgotten.

## License

MIT
