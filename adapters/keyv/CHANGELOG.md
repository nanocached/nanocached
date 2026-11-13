# Changelog

All notable changes to nanocached-keyv are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/keyv/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Added

- `disableReconnectCooldown` is passed through to the SDK. Since the SDK
  started reading `reconnectCooldownMs: 0` as the default, this switch is the
  only way to turn the reconnect cooldown off, and the store did not forward
  it.

### Fixed

- `set()` with a `ttl` of `Infinity`, or any value past the SDK's range, threw a `RangeError` from the SDK. It now writes with no expiry, as the Django backend does. (#1851)
- The README said the value size limit is measured after compression when `compress` is on. The SDK checks the value before compressing it, so a value over the limit is rejected with `RangeError` even if it would compress to less; the README now says so. (#1603)

## [0.6.2] - 2026-11-03

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.1] - 2026-11-02

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.0] - 2026-11-01

### Added

- keyv 6 support: the adapter type-checks and works against keyv 6.1 as well
  as keyv 5 (`peerDependencies.keyv` is `^5.0.0 || ^6.0.0`). It stays a
  relative-`ttl` adapter, which keyv 6 wraps in its own bridge that converts
  its absolute expiry back to the remaining milliseconds, so TTL rounding is
  the same on both versions and behaviour under keyv 5 does not change. On
  keyv 6, a `Keyv` with a `namespace` prefixes keys itself and its `clear()`
  reports an error and deletes nothing (the wire cannot enumerate keys); a
  `Keyv` without one clears the nanocached namespace as before. See the
  README. keyv 6 itself needs Node.js 22.19 or later.

### Changed

- `NanocachedKeyvStore` no longer declares `implements KeyvStoreAdapter`
  (that interface is keyv 6's `KeyvStorageAdapter`, which makes
  `has`/`getMany`/`setMany`/`deleteMany` mandatory, and keyv 5's is a
  different shape). Runtime behaviour is unchanged; the type is still accepted
  by `new Keyv(store)` under both versions.
- Built and type-checked with TypeScript 7 (a development dependency only;
  no change to the published package). `tsconfig.json` now names `node` in
  `types`, which TypeScript 7 no longer includes implicitly.

## [0.5.1] - 2026-10-27

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.5.0] - 2026-10-26

### Changed

- The minimum supported Node.js is 22 (`engines.node` is `>=22`, was
  `>=20`), as for the TypeScript SDK this adapter is built on: Node 20 reached
  end of life on 2026-04-30. The test suite now runs on Node 22, 24 and the
  current LTS.

## [0.4.8] - 2026-10-21

### Fixed

- README: `clear()` wipes the whole nanocached namespace, so `Keyv`
  instances sharing one store (or one store `namespace`) lose their keys
  together regardless of Keyv's own `namespace` prefix — the opposite of
  Keyv's README example, which the README used to call "always exactly
  right". The wire cannot delete by prefix; isolate instances with one
  store per nanocached namespace (#678).

### Fixed

- The README now describes what happens when the node is unreachable
  (Keyv's default swallows the SDK's `ConnectionLostError`: writes
  resolve `false` and emit `error`, reads resolve `undefined` without
  an event; `throwOnErrors` turns both back into rejections), what a value over the request size limit
  does (`RangeError`, swallowed the same way), and names the minimum
  server version, with tests pinning both behaviours (#672, #736).

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

First release: a Keyv `KeyvStoreAdapter` on the TypeScript SDK, for `cache-manager` v6+ and NestJS 11.
