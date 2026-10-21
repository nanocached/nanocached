# Changelog

All notable changes to nanocached-cache-manager are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/cache-manager/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

## [0.4.8] - 2026-10-21

### Changed

- Storing `undefined` throws `NoCacheableError` instead of being a
  silent no-op, and the config's `isCacheable` predicate is honoured by
  `set` and `mset` (the whole batch is checked before any entry is
  written). This is what cache-manager's own memory store and
  `cache-manager-redis-yet` do; the README's claim that the old no-op
  matched the Redis store was wrong. Through `wrap()` the difference is
  an `error` event instead of silence (#679).

### Fixed

- `mset` stores an entry whose key is `"__proto__"` instead of dropping
  it: the batch was collected on a plain object, where assigning that
  key sets the prototype. `set` on the same key always worked, so the
  two calls disagreed about one key (#730).
- The README no longer claims hedged reads for `mget`/`mset` (the SDK
  applies them to single-key reads only), names the minimum server
  version, and describes what happens when the node is unreachable and
  when a value is over the request size limit (#672, #736).

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

First release: a `cache-manager` v5 `Store` (`nanocachedStore`) for the TypeScript SDK.
