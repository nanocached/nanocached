# Nanocached.Caching

`IDistributedCache` adapter for the
[nanocached](https://github.com/nanocached/nanocached) .NET SDK:
`Microsoft.Extensions.Caching.Distributed.IDistributedCache` implemented on
`Nanocached.NanocachedClient`, so ASP.NET Core session state, response
caching, and any application code already written against the standard SPI
run against a nanocached cluster.

- **One `IDistributedCache` ⇄ one namespace.** Each registration binds to
  one nanocached namespace; the same key in two registrations never
  collides. The SPI has no `Clear`, so none is exposed here (unlike the
  Spring adapter, which maps `Cache.clear()` onto the namespace `CLEAR`).
- **Get/Set/Refresh/Remove** map onto the SDK's namespaced
  get/set/delete, with all of its routing, replication, hedged reads and
  retries.
- **Sliding expiration**, which the wire protocol has no server-side
  concept of, is emulated client-side: see "Sliding expiration" below.
- Values are opaque `byte[]` — the SPI's own type — with no serializer
  involved.

## Setup

Two ways to wire this up, matching the two ways an application already
deals with a `NanocachedClient`.

**The adapter connects its own client** — the common case:

```csharp
using Nanocached.Caching;

services.AddNanocachedDistributedCache(options =>
{
    options.Addresses.Add("10.0.0.1:8357");
    // options.Secret = "change-me";     // NANOCACHED_AUTH_SECRET, if configured
    // options.Namespace = "my-app-cache"; // default: "distributed-cache"
});
```

This registers a singleton `IDistributedCache`. The connection happens
lazily, the first time something resolves it (typically the host building
its root service provider); the client it connects is closed automatically
when the container is disposed — no separate cleanup needed. In
ASP.NET Core, source the values from configuration the ordinary way
(property binding is the host's, nothing adapter-specific):

```csharp
builder.Services.AddNanocachedDistributedCache(options =>
{
    builder.Configuration.GetSection("Nanocached").Bind(options);
});
```

**The application already has its own `NanocachedClient` singleton** —
reuse it instead of connecting a second one:

```csharp
services.AddSingleton(await NanocachedClient.ConnectAsync(new NanocachedClient.Options
{
    Addresses = { ("10.0.0.1", 8357) },
    Compress = true, // any client-level option the app needs
}));
services.AddNanocachedDistributedCache("my-app-cache"); // namespace; defaults to "distributed-cache"
```

This overload dials nothing and owns nothing — the client's lifecycle,
disposal included, stays whoever registered it's responsibility. Use this
form when the application also talks to nanocached directly (or needs a
client-level option — compression, hedged reads, TLS, read repair — none of
which `NanocachedCacheOptions` exposes; those are configured on
`NanocachedClient.Options` itself).

Neither overload does anything by itself beyond registering the service —
as with any `IDistributedCache` implementation, application code that wants
caching still calls it (directly, or through something like ASP.NET Core
session state, which is itself just a consumer of `IDistributedCache`).

## Usage

Standard `IDistributedCache` — the built-in extension methods work
unchanged:

```csharp
IDistributedCache cache = app.Services.GetRequiredService<IDistributedCache>();

await cache.SetStringAsync("greeting", "hello",
    new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10) });
string? value = await cache.GetStringAsync("greeting");
await cache.RemoveAsync("greeting");
```

`Get`/`Set`/`Refresh`/`Remove` (the synchronous members of the interface)
block on their async counterparts (`.GetAwaiter().GetResult()`) — the SDK
itself is async-only.

`NanocachedDistributedCache` can also be constructed directly, without DI:

```csharp
using NanocachedClient client = await NanocachedClient.ConnectAsync(
    new NanocachedClient.Options { Addresses = { ("10.0.0.1", 8357) } });
IDistributedCache cache = new NanocachedDistributedCache(client, "my-app-cache");
```

## Sliding expiration

nanocached's TTL is a one-shot countdown — a key's remaining time never
changes just because it was read. `DistributedCacheEntryOptions.SlidingExpiration`
is emulated on top of that: every value is wrapped in a small envelope
(one version byte, the configured sliding window, the configured absolute
expiry, then the payload) so a later `Get`/`Refresh` knows what to
recompute. `Get` on an entry with a sliding window re-sets it — envelope
and all — with a freshly computed TTL before returning, awaited (never
fire-and-forget); `Refresh` does the same without returning the value, and
is a no-op on a missing key, per the SPI's contract. An entry with no
sliding window is never rewritten by `Get` — its TTL, if any, is fixed
regardless of access, so the extra round trip would buy nothing.

Whole-second TTLs only reach the wire: a positive sub-second remainder
always rounds **up** to 1 second, never down to 0 (which would mean "no
expiry"). `AbsoluteExpiration`/`AbsoluteExpirationRelativeToNow` and
`SlidingExpiration` may be combined — the tighter of the two always wins,
exactly like a real sliding-plus-absolute cache entry. A past (or
exactly-now) absolute expiration throws `ArgumentOutOfRangeException`,
mirroring `Microsoft.Extensions.Caching.Memory.MemoryDistributedCache`.
No options at all means no TTL — the entry lives until evicted or
explicitly removed.

## Namespaces

Two `IDistributedCache` instances bound to different namespaces are fully
isolated, even over identical keys — the same guarantee
`NanocachedNamespace` gives the SDK directly. `NanocachedDistributedCache.Namespace`
exposes which one an instance is bound to, since the SPI itself has no
notion of it — useful for an application that also reads/writes the same
namespace directly through `client.Namespace(...)`.

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for `IDistributedCache`; other ecosystems get
their own idiomatic adapters (Spring `CacheManager`, Django cache backend,
cache-manager store, [JCache](../jcache)) rather than mirrors of this one.

[#25]: https://github.com/nanocached/nanocached/issues/25

## Requirements

.NET 8+, nanocached server ≥ the release that ships namespaces (issue
#105) — namespaced frames need a server that understands them.

## Building

```sh
cd adapters/dotnet
dotnet test tests/Nanocached.Caching.Tests
dotnet pack src/Nanocached.Caching
```

The build references the sibling `sdk/dotnet` sources directly (project
reference), so a checkout needs no locally-installed SDK package.

## License

MIT
