# Changelog

All notable changes to nanocached-keyv are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/keyv/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

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
