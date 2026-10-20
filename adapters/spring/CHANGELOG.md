# Changelog

All notable changes to nanocached-spring are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/spring/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Fixed

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
