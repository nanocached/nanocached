# nanocached-jcache

JSR-107 (`javax.cache`) provider for the
[nanocached](https://github.com/nanocached/nanocached) Java SDK:
`CachingProvider` / `CacheManager` / `Cache<K,V>` implemented on
`org.nanocached:nanocached`, discoverable through the standard
`Caching.getCachingProvider()` `ServiceLoader` mechanism.

This is an **honest subset** of JSR-107, not a full TCK-compliant
implementation (not a goal — see "Honest subset" below). Every gap is a
consequence of the wire protocol, documented rather than silently papered
over.

- **Named cache ⇄ namespace.** Each JCache `Cache` is one nanocached
  namespace named after it. Unlike `nanocached-spring`'s "any name works
  on first use", JSR-107 requires explicit `CacheManager.createCache(...)`
  — `getCache` on an uncreated name answers `null`, per spec.
- **`putIfAbsent`/`replace(k,old,new)`/`remove(k,old)`** map directly to
  the SDK's compare-and-set primitives (issue #141) — genuinely atomic,
  not a racy get-then-write.
- **`getAndPut`/`getAndReplace`/`getAndRemove`** are bounded
  compare-and-set retry loops built on `getWithToken` + the CAS
  primitives — see "Atomicity" below.
- **`ExpiryPolicy`** is evaluated on every write (and, for
  `AccessedExpiryPolicy`/`TouchedExpiryPolicy`, on every read too) and
  translated to the wire's TTL convention.
- **Local-only `CacheEntryListener`s**: this instance's own
  create/update/remove, never a remote client/JVM's changes and never
  `EXPIRED`.

## Setup

Get a `CacheManager` through the standard JSR-107 API. Connection settings
come from `Properties` passed to `getCacheManager` — the `nanocached.*`
keys mirror the Spring Boot starter's:

```java
Properties properties = new Properties();
properties.setProperty("nanocached.addresses", "10.0.0.1:8357,10.0.0.2:8357");
// optional: nanocached.secret, nanocached.tls, nanocached.ca,
// nanocached.compress, nanocached.compression-threshold

CachingProvider provider = Caching.getCachingProvider(); // ServiceLoader-discovered
CacheManager manager = provider.getCacheManager(URI.create("nanocached:my-app"), null, properties);

MutableConfiguration<String, User> config = new MutableConfiguration<>();
config.setTypes(String.class, User.class);
config.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 10)));
Cache<String, User> users = manager.createCache("users", config);
```

The URI is only an identity key for `CacheManager` caching (per the
JSR-107 spec's own identity contract) — it is never parsed as a
config-file pointer. `getCachingProvider().getCacheManager()` (the true
no-arg convenience call) always fails fast: `getDefaultProperties()` is
empty, so there is no `nanocached.addresses` to connect with unless you
call the 3-arg overload with your own `Properties`.

Each `CacheManager` **owns** its `NanocachedClient` — created on
`getCacheManager` and closed together with the manager (or the whole
provider) — unlike `nanocached-spring`, where the application supplies
and owns the client.

## Usage

```java
users.put("alice", new User("alice", 30));
User alice = users.get("alice");

users.putIfAbsent("bob", new User("bob", 25));       // atomic
users.replace("alice", oldUser, newUser);            // atomic, CAS
users.remove("alice", oldUser);                      // atomic, CAS

User previous = users.getAndPut("alice", newUser);   // atomic retry loop
users.removeAll();                                    // maps to CLEAR
```

## Honest subset — what this cannot do

- **`iterator()`** always throws `UnsupportedOperationException`: the wire
  protocol has no key enumeration, so a cache cannot be listed.
- **`invoke`/`invokeAll`** (entry processors) always throw
  `UnsupportedOperationException` — out of scope for this adapter.
- **Read-through (`CacheLoader`) and write-through (`CacheWriter`)** are
  rejected at `createCache` (`UnsupportedOperationException`), as is
  `storeByValue(false)` — every value crosses the wire as bytes, so
  store-by-reference is impossible.
- **`getExpiryForUpdate()` returning `null`** is supposed to mean "leave
  the current TTL unchanged", but the wire has no way to read a key's
  remaining TTL. A `null` update policy is treated as "no expiry"
  (eternal) instead — not faithful to the spec, and the one gap in this
  module that cannot be closed without a protocol change.
- **`CacheEntryListener`s see only this cache instance's own mutations.**
  Never another client/JVM's changes to the same namespace, and never
  `EXPIRED` (server-side TTL expiry isn't observable). The 2-argument
  `replace(K,V)` fires `Updated` with the old value unavailable (a single
  CAS round trip, not a read-then-write); `removeAll()` fires no
  per-entry events at all and behaves exactly like `clear()` —
  enumerating what a bulk `CLEAR` removed would need a client-side key
  registry this adapter does not keep.
- **`containsKey`** fetches the full value to check presence (no
  lightweight existence check on the wire) — same constraint the Java SDK
  itself documents.
- **Management (`CacheMXBean`)** is not implemented; `enableManagement`
  is a no-op and `isManagementEnabled()` always reports `false`.
- **`javax.cache.annotation`** (the CDI-based `@CacheResult` etc.) is out
  of scope — it needs a CDI container, which this module does not provide
  or depend on.

## Atomicity

`putIfAbsent`, `replace(k,old,new)`, and `remove(k,old)` map straight onto
the SDK's `putIfAbsent`/`replace`/`deleteIfMatches` (issue #141) — a
single conditional wire round trip, genuinely atomic.

`getAndPut`, `getAndReplace`, and `getAndRemove` have no single atomic
wire primitive, so each is a bounded compare-and-set retry loop: read a
token (`getWithToken`), attempt the conditioned write, and retry on a
concurrent change, up to 10 attempts. Under pathological sustained
contention on one key, the loop gives up CAS and falls back to a single
unconditional write/delete so the call still makes progress — the
"previous value" it returns in that case may be stale by the time the
overwrite lands.

## Statistics

`CacheManager.enableStatistics("name", true)` registers a bare-bones
`CacheStatisticsMXBean` (hit/miss/put/removal counts) under
`javax.cache:type=CacheStatistics,CacheManager=...,Cache=...` on the
platform MBean server. Timings and evictions are not tracked — the wire
protocol doesn't report server-side LRU evictions to the client.

## Consistency notes

`getExpiryForAccess()` (used by `AccessedExpiryPolicy`/
`TouchedExpiryPolicy`) turns every `get()` into a read followed by an
unconditional TTL-refresh write — a concurrent writer landing between the
two could, rarely, be clobbered back to the value this `get()` observed.

## Requirements

Java 17+, `javax.cache:cache-api:1.1.1`, nanocached server ≥ the release
that ships namespaces/CLEAR (issues #105/#106) and compare-and-set
(issue #141).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for JSR-107; other ecosystems get their own
idiomatic adapters (Spring `CacheManager`, `IDistributedCache`, Django
cache backend, [cache-manager](../cache-manager) v5,
[Keyv](../keyv) for cache-manager v6+/NestJS 11) rather than mirrors of
this one.

[#25]: https://github.com/nanocached/nanocached/issues/25

## Building

```
cd adapters/jcache
gradle test
```

The build includes the sibling `sdk/java` sources (Gradle composite
build), so a checkout needs no locally-installed SDK artifact.

MIT license.
