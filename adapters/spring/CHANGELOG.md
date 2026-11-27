# Changelog

All notable changes to nanocached-spring are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/spring/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Fixed

- **Key format change:** two `equals()`-equal `Serializable` keys could get different wire keys when one held the same `String`, boxed primitive, `UUID`, `BigInteger` or `BigDecimal` instance twice and the other two equal copies: JDK serialization writes a repeated instance as a back-reference. A key object `Pair(s, s)` (alone or as a `SimpleKey` argument) was stored under one key and looked up or evicted under another when the equal key was built from separately parsed values, so the lookup missed, `evictIfPresent` returned `false` and `@CacheEvict` left the stale value in place. A value of those types equal to one already written is now always written as a back-reference, so such keys encode the same. An entry cached under a key that held two separate equal values of those types becomes a miss once. (#2097)
- A TTL of `ChronoUnit.FOREVER.getDuration()` (set through the builder's `defaultTtl` / `ttl` or the Boot starter's `nanocached.cache.default-ttl` / `ttl.<name>`) overflowed while being rounded up to whole seconds and was sent as a 1-second TTL. The rounding now saturates at `Long.MAX_VALUE` seconds. (#1886)

- The README said the value size limit is measured after serialization and optional compression. The SDK checks the serialized value before compressing it, so a value over the limit is rejected even if it would compress to less; the README now says so. (#1603)

- **Key format change:** equal multi-argument keys could map to different wire keys, so a `@CacheEvict` (or a lookup) missed the entry a `@Cacheable` wrote. The default `DefaultCacheKeyConverter` JDK-serialized Spring's `SimpleKey` whole, and JDK serialization writes an instance it has already written as a back-reference: `prices(from, from)` with one `Long from = 1000L` and `prices(1000L, Long.valueOf("1000"))` are equal `SimpleKey`s with different bytes, and so are two `String` arguments read from separate requests. A `SimpleKey` is now a `0x01` marker followed by each argument length-prefixed and encoded on its own (`String:sku-1`, `Long:1000`, JDK serialization for other `Serializable` types; an `Object[]` argument, as a varargs method gets, element by element), so its bytes depend only on the arguments' values. Every entry cached under a `SimpleKey` before this release (a method with no arguments, several, or a single `null` or array argument) becomes a miss and is recomputed; keys of methods with one other argument, and explicit `key` expressions that don't build a `SimpleKey`, are unchanged. A `SimpleKey` argument that is not `Serializable` is now rejected with `IllegalArgumentException` instead of `IllegalStateException`, and one holding a `String` with a lone surrogate is now rejected, as a single `String` key already was. (#1952)

## [0.6.2] - 2026-11-03

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.1] - 2026-11-02

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.0] - 2026-11-01

### Changed

- The baseline moves to Spring Framework 7.0 (`spring-context` 7.0.10, as an
  `api` dependency) and the Spring Boot version the tests run against to 4.1.2;
  Spring Framework 6.2 is no longer supported. Open-source support for Spring
  Framework 6.2 and Spring Boot 3.5 ended on 2026-06-30, so 0.5.1 is the last
  release for them. The adapter's own API is unchanged.

### Security

- The `api` dependency on Spring Framework moves from 6.2.8 to 6.2.19, the
  latest 6.2 patch release. 6.2.8 predates several published security fixes in
  later 6.2 releases, and as an `api` dependency it is the version an
  application inherits unless it overrides it. The Spring Boot version the
  tests run against moves from 3.5.3 to 3.5.16, which manages the same Spring
  Framework version.

## [0.5.1] - 2026-10-27

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.5.0] - 2026-10-26

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.8] - 2026-10-21

### Fixed

- `get(key, valueLoader)` releases the callers waiting on a load whose
  loader failed with an `Error` (an assertion, a stack overflow, a class
  that failed to initialise). Only a `RuntimeException` used to settle
  the in-flight outcome, so those callers parked forever; they now see
  the failed run and load on their own, as they do after an exception.
- A `Character` key holding a lone surrogate is rejected with
  `IllegalArgumentException`, as a `String` key holding one is. It was
  encoded through UTF-8, which turns every lone surrogate into `?`, so
  such keys all addressed one entry.
- `get(key, valueLoader)` (`@Cacheable(sync = true)`) no longer holds a
  lock across the loader. It used 64 lock stripes shared by every key, so
  a sync loader that read another sync-cached key could deadlock against
  a second thread doing the reverse with no logical cycle between the
  keys, and the monitor held across the network round trip pinned
  virtual threads. Each key now has its own in-flight loader run that
  concurrent callers wait for; a loader that re-enters its own key on
  the same thread is reported with `IllegalStateException` (#673).
- Cached values are deserialized with the application's class loader —
  the builder's new `classLoader(...)` (set by the Boot starter) or the
  reading thread's context class loader — instead of the adapter's own,
  which cannot see application classes in a child loader (a devtools
  restart, a shared server library directory): every cache hit failed
  with `ClassNotFoundException` (#674).
- With `@Cacheable(sync = true)`, a store that fails after a successful
  load (a value over the size limit, a node lost between the miss and
  the put) is thrown as-is so the application's `CacheErrorHandler` sees
  it, instead of as a `ValueRetrievalException`, which Spring rethrows
  without consulting the handler (#727).
- A `String` key that is not well-formed UTF-16 (a lone surrogate) is
  rejected with `IllegalArgumentException` instead of being encoded as
  `?`, which made `"x\uD800"`, `"x\uD801"` and `"x?"` one cache entry
  (#679).

## [0.4.7] - 2026-10-19

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.6] - 2026-10-17

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.5] - 2026-10-15

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.4] - 2026-09-09

First release: a Spring Cache `CacheManager` / `Cache` on the Java SDK.
