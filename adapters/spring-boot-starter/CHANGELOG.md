# Changelog

All notable changes to nanocached-spring-boot-starter are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/spring-boot-starter/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

## [0.4.8] - 2026-10-21

### Fixed

- The auto-configured cache manager deserializes cached values with the
  application context's class loader, so a devtools restart (whose
  application classes live in a child loader) no longer fails every
  cache hit with `ClassNotFoundException` (#674).

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
  empty host, non-numeric or out-of-range port) now fails startup with an
  `IllegalArgumentException` naming the entry, instead of an
  `ArrayIndexOutOfBoundsException`. The last colon splits the entry, so a
  bracketed IPv6 host works (PR #531).

## [0.4.4] - 2026-09-09

First release: Spring Boot autoconfiguration for `nanocached-spring`, driven by `nanocached.*` properties.
