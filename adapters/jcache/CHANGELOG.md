# Changelog

All notable changes to nanocached-jcache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/jcache/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Fixed

- Backend failures are reported as `CacheException` (with the SDK's
  exception as the cause), as every `javax.cache.Cache` operation
  declares: a stopped node used to surface as
  `NanocachedException.ConnectionFailed`, a value over the size limit as
  `IllegalArgumentException`, and a value that could not be read back as
  `IllegalStateException` (#676).
- `getAndPut`, `getAndReplace`, `getAndRemove`, `replace(k, old, new)`
  and `remove(k, old)` retry their compare-and-set until it succeeds or
  5 seconds have passed, and then fail with `CacheException`. The fixed
  budget of ten attempts they had, followed by an unconditional write or
  delete, was exhausted by four writers on one key in ordinary use, so
  the same previous value was returned to more than one caller and
  `getAndRemove` handed out a value it had not removed (#677, #726).
- Values are deserialized with the class loader the `CacheManager` was
  created with (`getCacheManager(uri, classLoader, props)`), or the
  reading thread's context class loader when none was given, instead of
  the adapter's own loader, which cannot see application classes in a
  child loader: every cache hit failed with `ClassNotFoundException`
  (#675).
- `getCacheManager` reports a property it cannot parse, or that the
  client rejects, as a `CacheException` naming the property instead of a
  raw `NumberFormatException`/`IllegalArgumentException`; booleans accept
  only `true`/`false`, so `nanocached.tls=yes` no longer silently connects
  in plaintext; `nanocached.reconnect-cooldown-millis=0` now disables the
  cooldown, as the Boot starter's zero does (#728).
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

### Fixed

- A `nanocached.addresses` entry that is not a valid `host:port` (no port,
  empty host, non-numeric or out-of-range port) now fails with a
  `CacheException` naming the entry, instead of escaping as an
  `ArrayIndexOutOfBoundsException`. The last colon splits the entry, so a
  bracketed IPv6 host works (PR #531).
- The static thread pool behind `removeAll(Set)` no longer keeps up to 256
  idle threads alive for the life of the JVM after one large call: its core
  threads now exit after 2 seconds idle, so the pool drains back to zero
  threads (and stops pinning the adapter's classloader across redeploys).

## [0.4.4] - 2026-09-09

First release: a JSR-107 (`javax.cache`) provider on the Java SDK.
