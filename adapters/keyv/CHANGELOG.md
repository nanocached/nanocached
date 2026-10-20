# Changelog

All notable changes to nanocached-keyv are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/keyv/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

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
