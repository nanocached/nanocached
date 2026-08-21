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

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent).

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
including `AuthenticationFailedException` for a rejected secret — so one
catch clause covers "an expected nanocached failure". Caller mistakes
(an invalid TTL, an empty address list) throw `ArgumentException`
instead: they indicate a bug in the calling code, not a nanocached
failure, a convention shared across the SDKs (issue #47).

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
