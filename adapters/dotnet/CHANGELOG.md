# Changelog

All notable changes to Nanocached.Caching are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/dotnet/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

## [0.4.8] - 2026-10-21

### Fixed

- An entry with an absolute expiry and no sliding window is no longer
  served up to one second past its deadline: the wire TTL rounds up to
  whole seconds, so a 1.2 s `AbsoluteExpirationRelativeToNow` lived 2 s
  on the node and a `Get` at 1.8 s hit. `Get` now checks the envelope's
  exact deadline for every entry, as `MemoryDistributedCache` misses at
  the exact time (#679).

### Fixed

- A key that is not well-formed UTF-16 (a lone surrogate) throws
  `ArgumentException` from every member instead of being encoded as
  U+FFFD, under which `"x\uD800"` and `"x\uD801"` shared one entry and
  `Get` under one returned the other's value (#679).
- `Addresses` accepts an IPv6 literal in brackets (`"[::1]:8357"`): the
  brackets are stripped before the address reaches the SDK (#729).
- README: the namespace is not interchangeable with direct SDK access
  (every value is wrapped in the adapter's envelope, so a direct write
  makes `Get` throw and a direct read sees envelope bytes), plus new
  sections on an unreachable node and the request size limit, and the
  minimum server version is named (#672, #736).

## [0.4.7] - 2026-10-19

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.6] - 2026-10-17

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.5] - 2026-10-15

### Added

- `NanocachedCacheOptions` gains `Tls`, `Ca`, `Compress` and
  `CompressionThreshold`, forwarded to the client the "owns its own client"
  overload connects. Before, a cache configured with a `Secret` could only
  connect in plaintext. Defaults are unchanged (plaintext, no compression,
  threshold 256); the names and semantics mirror the Django adapter's `TLS`,
  `CA`, `COMPRESS` and `COMPRESSION_THRESHOLD` options (#231).

## [0.4.4] - 2026-09-09

First release: an `IDistributedCache` implementation on the .NET SDK.
