# Changelog

All notable changes to nanocached-jcache are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/jcache/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Fixed

- A `nanocached.addresses` entry that is not a valid `host:port` (no port,
  empty host, non-numeric or out-of-range port) now fails with a
  `CacheException` naming the entry, instead of escaping as an
  `ArrayIndexOutOfBoundsException`. The last colon splits the entry, so a
  bracketed IPv6 host works (PR #531).

## [0.4.4] - 2026-09-09

First release: a JSR-107 (`javax.cache`) provider on the Java SDK.
