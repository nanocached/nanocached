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
using NanocachedClient client = await NanocachedClient.ConnectAsync("127.0.0.1", 8357);

await client.SetAsync("greeting", "hello", ttlSeconds: 60);
byte[]? value = await client.GetAsync("greeting");   // null when missing
bool existed = await client.DeleteAsync("greeting");
```

Keys and values are `byte[]`, with `string` convenience overloads
(encoded as UTF-8). The client is thread-safe; requests are serialized
per connection (concurrent callers queue).

## Discovery replicas

When the cluster runs more than one discovery server, list them all;
both the initial connect and every node-list refresh try them in order.
A seed that is warming up after a restart (answers `B`) is skipped like
an unreachable one; if every seed is warming up, `ConnectAsync()` throws
`DiscoveryBusyException` — retry shortly.

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options()
        .Host("10.0.0.1", 8357)
        .Host("10.0.0.2", 8357));
```

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `SetAsync`/`DeleteAsync` fan out to all
R owners of a key (the primary's result decides; a dead replica never
fails a write), and `GetAsync` asks the primary, falling over to the
next owner only when the holder is unreachable. `client.Replication`
exposes the factor in use. A write whose primary just died recovers
automatically once discovery drops the node (bounded by its liveness
timeout): the failed attempt forces a node-list refresh and one retry.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 30 seconds; a request
that finds its connection dead redials and retries once transparently
(all operations are idempotent). If that extra round trip matters, opt
in to keep-alive:

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options()
        .Host("127.0.0.1", 8357)
        .KeepAliveInterval(TimeSpan.FromSeconds(15)));
```

## Authentication and TLS

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options()
        .Host("cache.internal", 8357)
        .AuthSecret("change-me")                       // NANOCACHED_AUTH_SECRET on the server
        .Tls(new SslClientAuthenticationOptions()));   // system trust; customize for a private CA
```

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
