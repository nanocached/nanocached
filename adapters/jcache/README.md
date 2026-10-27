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
  the SDK's compare-and-set primitives — genuinely atomic,
  not a racy get-then-write.
- **`getAndPut`/`getAndReplace`/`getAndRemove`** are bounded
  compare-and-set retry loops built on `getWithToken` + the CAS
  primitives — see "Atomicity" below.
- **`ExpiryPolicy`** is evaluated on every write (and, for
  `AccessedExpiryPolicy`/`TouchedExpiryPolicy`, on every read too: `get`
  uses the wire's touch, `getAll` its batched form — see "Consistency
  notes") and translated to the wire's TTL convention.
- **Local-only `CacheEntryListener`s**: this instance's own
  create/update/remove, never a remote client/JVM's changes and never
  `EXPIRED`.

## Install

```groovy
implementation 'org.nanocached:nanocached-jcache:0.5.1'
```

```xml
<dependency>
  <groupId>org.nanocached</groupId>
  <artifactId>nanocached-jcache</artifactId>
  <version>0.5.1</version>
</dependency>
```

Brings in the `org.nanocached:nanocached` SDK of the same version and
`javax.cache:cache-api`.

## Setup

Get a `CacheManager` through the standard JSR-107 API. Connection settings
come from `Properties` passed to `getCacheManager` — the `nanocached.*`
keys mirror the Spring Boot starter's:

```java
Properties properties = new Properties();
properties.setProperty("nanocached.addresses", "10.0.0.1:8357,10.0.0.2:8357");
// optional, all as plain strings:
//   nanocached.secret, nanocached.tls (true/false), nanocached.ca,
//   nanocached.compress (true/false), nanocached.compression-threshold (bytes),
//   nanocached.fire-and-forget-replicas (true/false), nanocached.read-repair (true/false),
//   nanocached.reconnect-cooldown-millis (0 disables the cooldown),
//   nanocached.read-hedge-after-millis

CachingProvider provider = Caching.getCachingProvider(); // ServiceLoader-discovered
CacheManager manager = provider.getCacheManager(URI.create("nanocached:my-app"), null, properties);

MutableConfiguration<String, User> config = new MutableConfiguration<>();
config.setTypes(String.class, User.class);
config.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 10)));
Cache<String, User> users = manager.createCache("users", config);
```

A property that cannot be parsed, or that the client rejects, fails
`getCacheManager` with a `CacheException` naming the property; booleans
accept only `true` and `false`. Durations are whole milliseconds, where
the Boot starter takes ISO-8601 durations. A positive expiry on a cache
rounds up to whole seconds (the wire's unit): 1.2 s becomes 2 s, so an
entry never expires earlier than its policy asks.

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

`putIfAbsent` maps straight onto the SDK's `putIfAbsent` — a
single conditional wire round trip, genuinely atomic.

`getAndPut`, `getAndReplace`, `getAndRemove`, `replace(k,old,new)`, and
`remove(k,old)` have no single atomic wire primitive that compares by
`equals()`, so each is a bounded compare-and-set retry loop: read the
current value and the digest it was read with (`getWithToken`), compare
(for the two CAS-condition ops, against the caller's `oldValue` via
`equals()` — not a digest of `oldValue`'s own re-serialized form, which
would fail for a value that's `equals()`-equal but not canonically
serialized, e.g. a `HashMap` built in a different insertion order), attempt
the conditioned write with the digest that read observed, and retry on a
concurrent change, pausing a little longer each time. A loop that is
still losing the race after 5 seconds fails with a `CacheException`
rather than guessing: there is no fallback to an unconditional write or
delete, so a previous value is returned to exactly one caller and
`getAndRemove` never hands out a value it did not remove. (A fixed
budget of ten attempts used to end in such a fallback, and four writers
on one key were enough to exhaust it.)

## Statistics

`CacheManager.enableStatistics("name", true)` registers a bare-bones
`CacheStatisticsMXBean` (hit/miss/put/removal counts) under
`javax.cache:type=CacheStatistics,CacheManager=...,Cache=...` on the
platform MBean server. Timings and evictions are not tracked — the wire
protocol doesn't report server-side LRU evictions to the client.

## Consistency notes

`getAll` is one batched read per owning node. The values in one batch
reply total at most 16 MiB, and a hit past that budget is answered as a
miss, so `getAll` over many large keys can omit a key that exists —
`get` it on its own rather than taking it for evicted.

`getExpiryForAccess()` (used by `AccessedExpiryPolicy`/
`TouchedExpiryPolicy`) makes `get()` read and refresh the TTL in one wire
step (`getAndTouch`), and `getAll` do the same for the whole batch
(`getManyAndTouch`, one round trip per owning node): the values do not
travel back to the node, and a write landing at the same time is never
overwritten. A key that is missing is a miss and is not created. A policy
whose access duration is zero expires the entries on read, in the same
step. A server older than the touch commands makes `get()` (without the
batched form, `t`) or `getAll` (`y`) fail with the SDK's `ServerTooOld`
(wrapped in a `CacheException`) under such a policy; the default policies
never touch. Upgrade every server before the adapters that use such a
policy: `getAll` sends the batch to all owning nodes before it reports the
old ones, so in a mixed cluster the nodes that were upgraded have already
refreshed their hits when the exception is raised — and with a zero access
duration, already expired them — while the caller gets the exception and none
of the values. There is no fallback to a plain read plus a touch, because it
could not give those expired values back.

## Trust boundary / deserialization

Values cross the wire as bytes and come back through
`ObjectInputStream.readObject()` (plain JDK serialization, like
`nanocached-spring`'s default). That means **anyone who can write to this
cache's namespace can execute code in the process that reads it back** —
any holder of the cluster's auth secret, or, when no secret is
configured, any peer that can reach the server at all. This is inherent
to Java object deserialization of untrusted bytes, not specific to
nanocached.

Mitigate with all of:

- **TLS + an auth secret** on the connection (`nanocached.tls` /
  `nanocached.secret` in the `Properties` passed to `getCacheManager`),
  so only trusted processes can write to the namespace at all.
- **Never share a namespace (or its secret) with an untrusted writer** —
  give each trust domain its own namespace/`CacheManager`.
- **A JVM-wide serial filter**: `-Djdk.serialFilter=...`, or
  `ObjectInputFilter.Config.setSerialFilter(...)` set once before this
  module deserializes anything. Deserialization here applies that filter
  explicitly on every `ObjectInputStream`, so it is honored even on JVMs
  where `ObjectInputStream` would not otherwise pick it up by default.

There is currently no pluggable `CacheValueSerializer` equivalent in this
module (see `nanocached-spring`'s, which this one could grow if a
non-Java-serialization format is needed here too) — for now, a JVM-wide
serial filter is the only mitigation available without forking this
module.

## When the node is unreachable

Every operation fails fast with a `CacheException` whose cause is the
SDK's `NanocachedException.ConnectionFailed`; nothing hangs. The same
`CacheException` wrapping applies to every backend failure, as the
`javax.cache.Cache` contract asks: a value that cannot be read back
(a class missing from the manager's class loader, a serial-filter
rejection) and a value the server will not accept.

## Value size limit

A serialized value must fit the server's request limit of 1 MiB (key
and value together, after serialization and optional compression); a
larger one is rejected before anything is sent, as a `CacheException`
whose cause is the SDK's `IllegalArgumentException`.

## Requirements

Java 17+, `javax.cache:cache-api:1.1.1`, nanocached server 0.4.0 or
later (the first release with namespaces, `CLEAR`, compare-and-set and
the bulk frames `getAll`/`putAll` use).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy: parity applies to the SDK core
only. This module exists for JSR-107; other ecosystems get their own
idiomatic adapters (Spring `CacheManager`, `IDistributedCache`, Django
cache backend, [cache-manager](../cache-manager) v5,
[Keyv](../keyv) for cache-manager v6+/NestJS 11) rather than mirrors of
this one.

## Building

```
cd adapters/jcache
gradle test
```

The build includes the sibling `sdk/java` sources (Gradle composite
build), so a checkout needs no locally-installed SDK artifact.

MIT license.
