# Nanocached (.NET)

Async .NET client SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake, so the calling code is identical either way.

Targets .NET 8+. No dependencies. NuGet package ID: `Nanocached`.

## Quick start

```csharp
using Nanocached;

// Point at a single node, or at a discovery server fronting a
// cluster — same call either way.
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options { Addresses = { ("127.0.0.1", 8357) } });

await client.SetAsync("greeting", "hello", ttlSeconds: 60);
string? value = await client.GetAsync("greeting");   // null when missing, strict UTF-8 decode
bool existed = await client.DeleteAsync("greeting");
```

`GetAsync` decodes the value as UTF-8 with a strict decoder — a value
that isn't valid UTF-8 throws `DecoderFallbackException` instead of
silently replacing bad bytes. Use `GetBytesAsync` for the raw bytes:

```csharp
byte[]? raw = await client.GetBytesAsync("greeting");
```

Keys and values accept `string` (UTF-8 encoded) everywhere, with `byte[]`
overloads of `GetAsync`/`GetBytesAsync`/`SetAsync`/`DeleteAsync` for raw
bytes. The client is thread-safe; requests are pipelined per connection
(request pipelining) — concurrent callers on the same connection each pay
only their own network latency, not everyone else's ahead of them.

## Addresses and discovery replicas

`Options.Addresses` is a list of `(string Host, int Port)` targets, tried
in order — a single-element list is the common case. When the cluster
runs more than one discovery server, list them all; both the initial
connect and every node-list refresh try them in order. An address that
is warming up after a restart (answers `B`) is skipped like an
unreachable one; if every address is warming up, `ConnectAsync()` throws
`DiscoveryBusyException` — retry shortly.

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("10.0.0.1", 8357), ("10.0.0.2", 8357) },
    });
```

## SDK proxy mode

Off by default. Set `ViaProxy` to route through one `nanocached-proxy`
from the fleet instead of connecting directly to the cluster:

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("discovery.internal", 8357) }, // discovery, not a proxy directly
        ViaProxy = true,
    });
```

`Addresses` must name discovery server(s) — `ConnectAsync` fetches the
current proxy roster from discovery and connects to one proxy, chosen at
random (spreading a fleet of clients across the proxy fleet); pointing
`ViaProxy` at a plain node address fails fast with a clear error instead
of silently pinning to it. A proxy looks exactly like a single node that
owns every key (full `Get`/`Set`/`Delete`, never routing-stale), so once
connected the client is in its ordinary single-connection mode: **no
ring, no per-node connections, and no hedged reads** — there is nobody
else to hedge to, so a configured `ReadHedgeAfter` is accepted but inert
under `ViaProxy`. Namespaces, `ClearAsync`/`ClearAllAsync`, response
tags, keep-alive, and compression all work unchanged over the one
connection.

On reconnect, the client first retries the same proxy (it may simply
have restarted); only if that also fails does it re-fetch the roster
from discovery and fail over to another proxy chosen at random — reusing
the same lazy reconnect-on-use plumbing every other mode uses, not a
second mechanism.

## Namespaces

A namespace is a flat, opaque byte string that scopes a key: the same key
name in two namespaces is two independent entries — useful for sharing one
cluster across tenants or subsystems without key-prefixing conventions.
`client.Namespace(...)` returns a lightweight `NanocachedNamespace` handle
exposing the same operations as the client:

```csharp
NanocachedNamespace users = client.Namespace("users");

await users.SetAsync("42", "alice", ttlSeconds: 60);
string? name = await users.GetAsync("42");   // "alice"
bool existed = await users.DeleteAsync("42");

// A different namespace, or the client itself, never sees "42" above —
// same key name, independent entries.
await client.SetAsync("42", "not alice");
```

The handle is cheap (it shares the client's connections and dials
nothing of its own) and forwards every call to the client, so it gets
identical routing, replication fan-out, hedged reads, `W`
refresh-and-retry, response tags, and compression — nothing about
namespaces duplicates the client's networking. It becomes invalid once
the owning client is closed, raising the same `AlreadyClosedException`
the client's own methods do.

`client.Namespace("")` (the empty namespace) is equivalent to the client
itself: it sends the legacy `G`/`S`/`D` frames, byte-for-byte, and hashes
exactly as an un-namespaced key always has — so existing code that never
touches namespaces is completely unaffected, and an unnamespaced
deployment reached through `Namespace("")` is wire-indistinguishable
from calling the client directly. `Namespace(byte[])` accepts a raw,
binary-safe namespace too — there's no delimiter, no escaping, and no
hierarchy, so a namespace may contain any bytes.

Namespaces also enter cluster routing: a key's owners are computed from
`(namespace, key)` together, not the key alone, so the same key name in
different namespaces can land on different owners — this is what keeps a
common key (e.g. a per-tenant `config`) from piling every namespace's
copy onto the same few nodes. Namespaced frames need a server that
understands them; talking `g`/`s`/`d` to a pre-namespace node gets `E\n`
and a closed connection, so upgrade every node before using namespaces.

`ClearAsync()` drops every entry in a namespace in one call — an O(1)
sub-map drop on each node, not a scan-and-delete — including on the
empty (default) namespace's own handle. `ClearAllAsync()`, on the client
itself, drops every namespace, the default one included:

```csharp
await users.ClearAsync();     // only "users" is gone
await client.ClearAllAsync(); // every namespace, including the default one
```

Neither is key-addressed, so in a cluster both fan out to every node in
the client's current node list concurrently (a namespace's keys are
spread across all of them). Success requires every node to ack; on any
failure the node list is refreshed once and the clear retried against
the refreshed list, exactly like a stale-routing retry elsewhere in this
SDK — a node still failing after that raises a `ConnectionLostException`
naming it, never a silent partial clear. Both raise
`AlreadyClosedException` after `Close()`, like every other operation.

## Incr / Decr

`IncrAsync`/`DecrAsync` atomically increment or decrement a counter stored
as a decimal integer:

```csharp
await client.SetAsync("hits", "0");
long? hits = await client.IncrAsync("hits", 1);   // 1
hits = await client.DecrAsync("hits", 1);          // 0, back to 0
```

`DecrAsync` is `IncrAsync` with the delta negated — there is no separate
decrement opcode on the wire. Both return `null` when the key is missing
or expired (the same convention `GetAsync` uses for a miss — INCR never
creates a counter from nothing) and throw `NotNumericException` when the
stored value isn't an integer, or applying the delta would overflow a
64-bit signed integer.

**As volatile as `Set`** — LRU eviction and TTL expiry reclaim an
incremented value like any other entry. Good for rate limiting or
approximate counters, not for durable counts (billing, inventory).

In a cluster, only the primary owner ever runs the increment; replicas
receive the resulting value via an ordinary `Set` instead of replaying
the delta, so a replica can never drift from the primary (e.g. after an
earlier dropped replica write). `NanocachedNamespace` exposes the same
`IncrAsync`/`DecrAsync` pair, scoped to its namespace.

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `SetAsync`/`DeleteAsync` fan out to all
R owners of a key (the primary's result decides; a dead replica never
fails a write), and `GetAsync`/`GetBytesAsync` ask the primary, falling
over to the next owner only when the holder is unreachable.
`client.Replication` exposes the factor in use. A write whose primary
just died recovers automatically once discovery drops the node (bounded
by its liveness timeout): the failed attempt forces a node-list refresh
and one retry.

## Fire-and-forget replica writes

Off by default. `SetAsync`/`DeleteAsync` normally wait for every replica
leg to finish, same as the primary. Enabling `FireAndForgetReplicas`
returns as soon as the primary acks, letting replica legs finish in the
background (fire-and-forget replica writes):

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        FireAndForgetReplicas = true,
    });
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

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        ReadRepair = true,
    });
```

Closes the narrow window after a primary restart where a replica still
holds a key its (fresh) primary doesn't, at the cost of extra reads only
on the misses that hit that window. The repair write carries a fixed 60-second TTL — the wire protocol's `G` response never returns the original one to preserve, and no TTL at all would immortalize already-expired keys — and,
unlike fire-and-forget replica writes, is uncapped and not drained on
`Close()`: this only fires on an already-rare clean miss, and losing one
costs nothing beyond staying in the window for one more read.

## Hedged reads

Off by default. A read goes to the key's primary owner and moves on to
the next owner only when the primary *fails* — so one slow-but-alive
node (a saturated host, a bad link) makes every read that touches it
wait out its full round trip, and with `R` copies on `N` nodes that is
roughly `R/N` of all reads. Setting `ReadHedgeAfter` sends the same read
to the next owner as well once the primary has been silent for that
long, and takes the first answer:

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        ReadHedgeAfter = TimeSpan.FromMilliseconds(10), // hedge after 10 ms
    });
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
`Close()`. Inert under [SDK proxy mode](#sdk-proxy-mode) (`ViaProxy`):
a proxy connection has no ring and nobody else to hedge to.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent).

`ConnectAsync` itself tolerates a node that discovery still lists but
that can't be reached — typically one that just died and hasn't been
evicted yet (a window of seconds): the node is kept in the ring without a
connection, requests for its keys fail over per request exactly as they
would after a mid-life death, and it is redialed after the cooldown. Only
a cluster with no reachable node at all fails `ConnectAsync`.

An address whose redial just failed is treated as still down for
`ReconnectCooldown` (default 1 second): requests routed to it during that
window fail immediately with the original dial error instead of each
paying another full connect timeout. Keep it short — a node that
genuinely recovers is shut out for at most this long. `TimeSpan.Zero`
means "use the default", not "disable it" — set `DisableReconnectCooldown`
instead to make every request pay its own full dial attempt.

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        ReconnectCooldown = TimeSpan.FromMilliseconds(1000), // default
        // DisableReconnectCooldown = true, // opt out of the cooldown entirely
    });
```

## Authentication and TLS

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        AuthSecret = "change-me",   // NANOCACHED_AUTH_SECRET on the server
        Tls = true,                 // system trust store
    });
```

An empty `AuthSecret` is the same as leaving it `null` — matching the
other SDKs — not a literal zero-length secret.

For a private CA, point `Ca` at a PEM file of trusted root certificate(s)
— it replaces the default trust store and is only consulted when `Tls`
is true (a set `Ca` is silently ignored when `Tls` is false; an
unreadable or unparseable CA file when `Tls` is true is a connect-time
error):

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        Tls = true,
        Ca = "/etc/nanocached/ca.pem",
    });
```

## Value compression

Off by default. When enabled, values at or above `CompressionThreshold`
bytes are transparently DEFLATE-compressed on `Set`/`SetAsync` and
decompressed on `Get`/`GetAsync`/`GetBytesAsync` (value compression):

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options
    {
        Addresses = { ("cache.internal", 8357) },
        Compress = true,
        CompressionThreshold = 256, // default; bytes, below which values are stored as-is
    });
```

**Every client that reads or writes a given set of keys must agree on
`Compress`.** This is a per-keyspace format decision, not a per-client
preference — enabling it prefixes every value this client writes with a
one-byte marker, so a client with `Compress = false` reading one of
those values gets the marker byte back as if it were part of the value
(wrong, silently), and a client with `Compress = true` reading a value
written before compression was enabled anywhere risks misreading that
value's first byte as the marker (a `DecompressionException`, or — if
that byte happens to be the "uncompressed" marker by chance, or the
decoder doesn't reject the garbage that follows it — a silently wrong
read; raw DEFLATE has no checksum). There is no dual-mode migration
path: only turn this on for a fresh keyspace, or only after every
client touching an existing one has upgraded and enabled it together.
Incompressible data (already-compressed media, random bytes) is passed
through unchanged rather than bloated.

## close()

`client.Close()` (or `Dispose()`/`using`) is idempotent; calling it a
second time is harmless but writes a warning to stderr, since it usually
means the caller lost track of the client's lifecycle. Likewise,
`ConnectAsync()` warns to stderr if it's called again for the same
single configured address while a previous connection to it is still
open — a sign `close()` was forgotten. Neither warning fires for
multi-address configs, since legitimate concurrent clients would make
that a false positive.

## Errors

Every exception this SDK throws extends `NanocachedException` —
including `AuthenticationFailedException` for a rejected secret and
`NotNumericException` for an `IncrAsync`/`DecrAsync` whose stored value
isn't an integer — so one catch clause covers "an expected nanocached
failure". Caller mistakes
(an invalid TTL, an empty address list) throw `ArgumentException`
instead: they indicate a bug in the calling code, not a nanocached
failure, a convention shared across the SDKs (issue #47).

### Retryable errors and `transient_retries`

`nanocached-proxy` may answer a request `R` instead of its usual reply
when the request specifically failed transiently (e.g. its upstream node
was briefly unreachable) — the connection itself is fine. This SDK
handles `R` transparently: it retries the same request on the same
connection up to twice more (three attempts total, 50ms then 100ms
apart) before the caller ever sees anything. Only if the third attempt
still answers `R` does the call fail, with `RetryableException` — the
connection is never closed or redialed for this (`R` is not a connection
error) and stays usable for the next operation.

Every `R` a connection receives — whether retried away transparently or
not — is counted in `client.Stats().TransientRetries`, alongside the
other swallowed-failure counters (`ReplicaWriteFailures`,
`ReadRepairFailures`, `RefreshFailures`). Nodes and discovery servers
accept the capability but never send `R` today; only
`nanocached-proxy` does, so this mainly matters in [SDK proxy
mode](#sdk-proxy-mode) — but the SDK handles it on every connection
regardless.

## Build

```sh
dotnet test tests/Nanocached.Tests    # unit tests against in-process mock servers
dotnet pack src/Nanocached
```

This SDK speaks the current wire protocol (rendezvous hashing,
replication-aware `L`/`W`); it requires an up-to-date server. The hash
pipeline is pinned to cross-language test vectors that the server and
the TypeScript/Python/Java/Rust SDKs also assert.

## License

MIT
