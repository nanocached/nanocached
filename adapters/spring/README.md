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

## Setup

Two beans set the adapter up — the SDK client and the manager — plus
`@EnableCaching`. There is no Spring Boot autoconfiguration (yet, see
below), so nothing happens by merely adding the dependency or editing
`application.yaml`: these beans are the setup.

```java
@Configuration
@EnableCaching
class CacheConfig {

    // destroyMethod: the client owns real connections; Spring closes it
    // with the context. The manager borrows it and needs no closing.
    @Bean(destroyMethod = "close")
    NanocachedClient nanocachedClient() {
        return NanocachedClient.connect(new NanocachedClient.Options()
                .addresses(List.of(new NanocachedClient.Address("10.0.0.1", 8357))));
    }

    @Bean
    CacheManager cacheManager(NanocachedClient client) {
        return NanocachedCacheManager.builder(client)
                .defaultTtl(Duration.ofMinutes(10))
                .ttl("sessions", Duration.ofSeconds(30))
                // .cacheNames(List.of("users", "sessions"))  // optional: closed set
                .build();
    }
}
```

In a Spring Boot app the same two beans apply, and you can source the
values from `application.yaml` yourself the ordinary way — property
binding is Boot's, nothing adapter-specific:

```yaml
nanocached:
  addresses: "10.0.0.1:8357,10.0.0.2:8357"
  default-ttl: 10m
```

```java
@Bean(destroyMethod = "close")
NanocachedClient nanocachedClient(
        @Value("${nanocached.addresses}") List<String> addresses) {
    return NanocachedClient.connect(new NanocachedClient.Options()
            .addresses(addresses.stream()
                    .map(a -> a.split(":", 2))
                    .map(a -> new NanocachedClient.Address(a[0], Integer.parseInt(a[1])))
                    .toList()));
}

@Bean
CacheManager cacheManager(
        NanocachedClient client,
        @Value("${nanocached.default-ttl:0s}") Duration defaultTtl) {
    return NanocachedCacheManager.builder(client).defaultTtl(defaultTtl).build();
}
```

Boot's `spring.cache.*` keys (e.g. `spring.cache.type`) do **not** know
this adapter; they configure Boot's own autoconfigured managers, and your
explicit `CacheManager` bean takes precedence over those — Boot's
`CacheAutoConfiguration` backs off whenever a `CacheManager` bean exists,
so with the two beans above nanocached wins even with the Redis starter on
the classpath and even with a stray `spring.cache.type=redis` left in the
yaml (`BootAutoConfigurationInteractionTest` pins all of this against real
Boot autoconfiguration). Boot users who would rather skip the two `@Bean`
methods above can instead add
[`nanocached-spring-boot-starter`](../spring-boot-starter), which
autoconfigures both beans from `nanocached.*` properties — same adapter,
zero Java config.

## Usage

With the setup in place, cache with the standard annotations —
`@Cacheable`, `@CachePut`, `@CacheEvict` work as on any other Spring
`CacheManager`:

```java
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

By default any cache name works — namespaces need no server-side setup, so
`getCache("anything")` creates the cache on first use. `cacheNames(...)`
switches to a closed, eagerly-created set (`getCache` answers `null` for
unknown names, letting a `CompositeCacheManager` fall through).

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
namespaces and CLEAR (issues #105/#106).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for Spring; other ecosystems get their own
idiomatic adapters (`IDistributedCache`, Django cache backend,
cache-manager store, [JCache](../jcache), [Keyv](../keyv)) rather than
mirrors of this one.

[#25]: https://github.com/nanocached/nanocached/issues/25

## Building

```
cd adapters/spring
gradle test
```

The build includes the sibling `sdk/java` sources (Gradle composite
build), so a checkout needs no locally-installed SDK artifact.

MIT license.
