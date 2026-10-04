# Changelog

All notable changes to Nanocached.Caching are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/dotnet/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Added

- `NanocachedCacheOptions` gains `Tls`, `Ca`, `Compress` and
  `CompressionThreshold`, forwarded to the client the "owns its own client"
  overload connects. Before, a cache configured with a `Secret` could only
  connect in plaintext. Defaults are unchanged (plaintext, no compression,
  threshold 256); the names and semantics mirror the Django adapter's `TLS`,
  `CA`, `COMPRESS` and `COMPRESSION_THRESHOLD` options (#231).

## [0.4.4] - 2026-09-09

First release: an `IDistributedCache` implementation on the .NET SDK.
