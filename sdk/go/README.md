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

## Proxy mode

Off by default. `ViaProxy` connects through a `nanocached-proxy` fronting
the cluster instead of joining the ring directly:

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses: []nanocached.Address{{Host: "discovery.internal", Port: 8356}},
    ViaProxy:  true,
})
```

`Addresses` must name discovery server(s) — `Connect` fetches the
registered proxy roster instead of the node roster, and connects to one
proxy chosen at random, spreading a fleet of clients across the proxy
fleet rather than piling onto whichever proxy happens to be listed first;
a proxy that can't be reached fails over to another, still at random.
Pointing `ViaProxy` at a plain node address (not discovery) fails
`Connect` with a clear error.

A proxy answers the identify handshake exactly like a single node that
owns every key, so from there the client runs in its ordinary
single-connection mode: no ring, no per-node connections, and — since a
single connection has no replicas to hedge to — **a configured
`ReadHedgeAfter` is inert in proxy mode** (every read simply goes to the
one connection; there is nothing to hedge onto). Every other option —
`Compress`, `FireAndForgetReplicas`, `ReadRepair`, namespaces,
`Clear`/`ClearAll`, keep-alive — works unchanged over the one connection.

Losing the proxy connection first retries the same proxy (it may simply
have restarted); only if that also fails does the client re-fetch the
roster from discovery and fail over to another one at random, reusing the
same lazy-reconnect-on-use path as the node/cluster modes.

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

## Incr and Decr

`Incr`/`Decr` atomically add a signed delta to a key's stored counter
value and return the result:

```go
err = client.Set("hits", "0", 0)
value, ok, err := client.Incr("hits", 1)   // 1, true, nil
value, ok, err = client.Decr("hits", 1)    // 0, true, nil — negates delta, same wire op
value, ok, err = client.Incr("missing", 1) // 0, false, nil — no such key
```

`ok` is `false` on a missing or expired key, the same shape `Get`/`GetBytes`
use. A stored value that isn't a signed decimal integer — or a delta that
would overflow one — returns `ErrNotNumeric` (`errors.Is`-matchable). `Decr`
is a thin wrapper that negates `delta` and calls `Incr`; there is no
separate decrement opcode on the wire. Both are also namespace-scoped:
`client.Namespace(ns).Incr(...)`/`.Decr(...)`, same as `Get`/`Set`/`Delete`.

**`Incr`/`Decr` are exactly as volatile as `Set`**: LRU eviction and TTL
expiry reclaim an incremented value like any other cache entry. Good for
rate limiting or approximate counters, wrong for anything that needs a
durable count — billing, inventory — since the counter can simply vanish
under memory pressure or its own TTL.

In a cluster, only the key's primary owner actually runs the increment;
the primary's literal resulting value (and TTL) is then fanned out to the
remaining owners as an ordinary `Set`, exactly like a normal write's
replica legs, rather than each replica replaying the increment itself —
replaying could let a replica drift from the primary (e.g. after a
dropped earlier replica write), while forwarding the literal result keeps
every replica byte-identical to it.

## Compare-and-set

`PutIfAbsent`/`ReplaceIfPresent`/`Replace` (the `k` frame) and
`DeleteIfMatches` (the `x` frame) condition a write on the key's current
content instead of writing unconditionally:

```go
// add: store only if the key doesn't already exist.
stored, err := client.PutIfAbsent("lock:job-1", []byte("worker-a"), 30)

// two-argument replace: store only if the key currently holds any value.
stored, err = client.ReplaceIfPresent("session:42", []byte("refreshed"), 300)

// three-argument replace: store only if the key's content matches a
// prior read exactly.
value, token, ok, err := client.GetWithToken("counter-config")
stored, err = client.Replace("counter-config", token, []byte("new-config"), 0)

// two-argument remove: delete only if the key's content matches.
deleted, err := client.DeleteIfMatches("counter-config", token)
```

`Replace`/`DeleteIfMatches` are conditioned on a `CasToken`, not a literal
value — a small wrapper around a content digest (the first 16 bytes of
the key's SHA-256, lowercase hex on the wire), obtained from
`GetWithToken` (or `Namespace.GetWithToken`), the `CasToken`-returning
sibling of `GetBytes`. Every one of these four calls returns a plain
`(bool, error)`: a condition mismatch — the key already existed, didn't
exist, or its content digest didn't match — is `false, nil`, never an
error, the same idiom `Delete`'s `existed` return already uses; a non-nil
error means a genuine failure (connection lost, wrong-node retries
exhausted, invalid arguments), not a mismatch.

`ContentDigest(value []byte) [16]byte` and `TokenFromDigest` let a caller
that already holds a value (not from a real `GetWithToken`) build a token
by hand — but that reconstruction is only correct if it reproduces the
exact bytes the server stores (compression's marker byte included, if
`Compress` is enabled), the same caveat memcached's own value-based CAS
has. The read-then-write-back path (`GetWithToken` -> `Replace`) has no
such caveat: the digest always came from the server's own bytes. All four
methods are also namespace-scoped: `client.Namespace(ns).PutIfAbsent(...)`
etc., same as `Get`/`Set`/`Delete`/`Incr`.

**This is not a distributed lock.** LRU eviction reclaims a key exactly as
it would after a plain `Set`, CAS or not: a key used as a lock
(`PutIfAbsent` to acquire, a TTL to eventually release) that gets evicted
under memory pressure leaves a second caller's `PutIfAbsent` free to
succeed while the first caller still believes it holds the lock — a
silent double-acquisition CAS cannot detect.

In a cluster, only the key's primary owner evaluates the condition; on
success, the primary's literal result is fanned out to the remaining
owners as an ordinary `Set`/`Delete`, exactly like `Incr`/`Decr` — never
by replaying `k`/`x` on a replica, which could otherwise evaluate the
same condition against its own possibly-different copy and reach a
different outcome.

See [`docs/protocol.html#cas`](../../docs/protocol.html#cas) for the wire
protocol.

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

## Transient retries

Some servers (today, only `nanocached-proxy`) can answer a single
request `R` instead of a value: the request itself failed transiently
(e.g. the proxy's upstream node was briefly unreachable and survived its
own one refresh-and-retry), but the connection is fine and the same
request is worth trying again right away. The SDK handles this
automatically and transparently — callers never see `R` as such:

- Every connection this SDK opens probes for the capability during
  connect, so this needs no configuration.
- An `R` reply is retried on the *same* connection, up to 2 retries (3
  attempts total), sleeping 50ms before the first retry and 100ms before
  the second. If the third attempt still answers `R`, `Get`/`Set`/
  `Delete`/etc. return `ErrRetryable` (an `errors.Is`-matchable
  sentinel, like the SDK's other error kinds) — the connection itself is
  untouched and stays open and usable for the caller's next operation.
- `Stats().TransientRetries` counts every `R` this client has received,
  whether or not the retry that followed it went on to succeed —
  observability for a proxy backend that's flaking, even when every
  individual call still came back with a value.

```go
value, ok, err := client.Get("k")
if errors.Is(err, nanocached.ErrRetryable) {
    // The request itself kept failing transiently after 3 attempts;
    // the connection is still fine, try again (or give up) as fits
    // the caller.
}
```

An older server that predates this capability, or `R` entirely,
never sends it — this SDK talks to those exactly as before.

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
  `ErrConnectionLost`, `ErrAuthenticationFailed`, `ErrDecompression`,
  `ErrNotNumeric` (see "Incr and Decr" above), and `ErrRetryable` (see
  "Transient retries" above) are sentinels for `errors.Is`. Caller
  mistakes (an invalid TTL, an empty address list)
  surface as ordinary errors outside the sentinel set — they indicate a
  bug in the calling code, not a nanocached failure; this convention is
  shared across the SDKs (issue #47).
- `Close()` is idempotent; calling it a second time warns to stderr
  instead of erroring. `Connect()` also warns to stderr if it's called
  for a single address that a previous, still-open connection from this
  process already points at — a common sign that `Close()` was
  forgotten.

## License

MIT
