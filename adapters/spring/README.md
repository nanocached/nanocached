# nanocached-spring

Spring Cache adapter for the [nanocached](https://github.com/nanocached/nanocached)
Java SDK: `org.springframework.cache.CacheManager` / `Cache` implemented on
`org.nanocached:nanocached`, so `@Cacheable`, `@CachePut`, `@CacheEvict` and
friends run against a nanocached cluster.

- **Named cache ⇄ namespace.** Each Spring cache is one nanocached
  namespace named after it — same key in two caches never collides, and
  `Cache.clear()` is the namespace's `CLEAR` (an O(1) sub-map drop on every
  node, no key scan).
- **`get`/`put`/`evict`** map to the SDK's namespaced get/set/delete, with
  all of its routing, replication, hedged reads and retries.
- **`get(key, valueLoader)`** (read-through) computes on a miss under a
  per-JVM striped lock: one computation per process per key; processes
  sharing the cluster may compute concurrently (last write wins) — the
  usual stampede trade-off, same as Spring's other non-distributed
  adapters.
- **Per-cache TTL**, a default TTL, and `Duration.ZERO` = no expiry.
- **Null caching** on by default (Spring's `NullValue` marker), like
  Spring's own cache implementations; disable with
  `allowNullValues(false)`.

## Usage

Declare the manager, then cache with the standard annotations —
`@Cacheable`, `@CachePut`, `@CacheEvict` work as on any other Spring
`CacheManager`:

```java
@EnableCaching
@Configuration
class CacheConfig { /* the cacheManager() bean below */ }

@Service
class UserService {
    @Cacheable("users")
    User findUser(String name) { /* hits the DB only on a cache miss */ }

    @CachePut(cacheNames = "users", key = "#user.name")
    User saveUser(User user) { /* refreshes the cached entry */ }

    @CacheEvict("users")
    void deleteUser(String name) {}

    @CacheEvict(cacheNames = "users", allEntries = true)
    void deleteEveryUser() {}   // one CLEAR on every node, not N deletes
}
```

`sync = true` routes through the adapter's `get(key, valueLoader)`
(per-JVM herding, see below). SpEL keys that name method parameters
(`key = "#user.name"`) need the `-parameters` compiler flag, as usual
with Spring.

```java
NanocachedClient client = NanocachedClient.connect(new NanocachedClient.Options()
        .addresses(List.of(new NanocachedClient.Address("10.0.0.1", 8357))));

@Bean
public CacheManager cacheManager() {
    return NanocachedCacheManager.builder(client)
            .defaultTtl(Duration.ofMinutes(10))
            .ttl("sessions", Duration.ofSeconds(30))
            // .cacheNames(List.of("users", "sessions"))  // optional: closed set
            .build();
}
```

By default any cache name works — namespaces need no server-side setup, so
`getCache("anything")` creates the cache on first use. `cacheNames(...)`
switches to a closed, eagerly-created set (`getCache` answers `null` for
unknown names, letting a `CompositeCacheManager` fall through).

The manager borrows the client; closing the client (e.g. a
`@Bean(destroyMethod = "close")`) is the application's job.

## Serialization

Values: pluggable `CacheValueSerializer`; the default is JDK serialization
(`JdkCacheValueSerializer`), which round-trips every `Serializable` value
including Spring's `NullValue`. Plug in JSON/Kryo when values are shared
with non-JVM readers — such a serializer must handle `NullValue` if null
caching stays enabled.

Keys: pluggable `CacheKeyConverter`. The default maps `byte[]` through
untouched; `String`, boxed numbers, `Boolean`, `Character` and `UUID` to a
type-prefixed canonical string (so `"42"` and `42L` stay distinct); and
everything else (Spring's `SimpleKey` included) through JDK serialization.
Key bytes must be canonical across every JVM sharing the cache — prefer
simple keys, or plug in a converter that knows your key type.

## Consistency notes

The wire has single-key get/set/delete and no compare-and-set, so
`putIfAbsent` is get-then-put (two racing writers can both see "absent";
the later put wins), and cross-JVM `get(key, valueLoader)` stampedes are
possible as described above.

## Requirements

Java 17+, Spring Framework 6.x, nanocached server ≥ the release that ships
namespaces and CLEAR (issues #105/#106). A Spring Boot starter
(autoconfiguration) is a planned follow-up.

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for Spring; other ecosystems get their own
idiomatic adapters (`IDistributedCache`, Django cache backend,
cache-manager store, JCache) rather than mirrors of this one.

[#25]: https://github.com/nanocached/nanocached/issues/25

## Building

```
cd adapters/spring
gradle test
```

The build includes the sibling `sdk/java` sources (Gradle composite
build), so a checkout needs no locally-installed SDK artifact.

MIT license.
