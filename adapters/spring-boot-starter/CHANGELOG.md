# Changelog

All notable changes to nanocached-spring-boot-starter are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/spring-boot-starter/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

### Fixed

- **Breaking:** a `nanocached.cache.ttl.<name>` property that can never bind now stops startup with a message naming it and the bracketed form, where it used to be ignored. Spring Boot's binder reads an unbracketed dotted key as a nested path, so `nanocached.cache.ttl.users.by.id=40s` bound nothing and `@Cacheable("users.by.id")` silently used the default TTL, which is no expiry unless `cache.default-ttl` is set; `nanocached.cache.ttl.[users.by.id]=40s` binds. An environment variable such as `NANOCACHED_CACHE_TTL_USER_SESSIONS` (read as `nanocached.cache.ttl.user.sessions`) is rejected for the same reason. The README now documents the bracket form next to the warning about bare numbers. (#1680)

## [0.6.2] - 2026-11-03

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.1] - 2026-11-02

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.6.0] - 2026-11-01

### Changed

- The baseline moves to Spring Boot 4.1 (dependencies platform 4.1.2, managing
  Spring Framework 7.0) together with `nanocached-spring`; Spring Boot 3.5 is
  no longer supported. Open-source support for Spring Boot 3.5 and Spring
  Framework 6.2 ended on 2026-06-30, so 0.5.1 is the last release for them.
  The auto-configuration is ordered before Boot's cache auto-configuration by
  name, because Boot 4 moved it to
  `org.springframework.boot.cache.autoconfigure.CacheAutoConfiguration` in the
  `spring-boot-cache` module, which an application without
  `spring-boot-starter-cache` does not have. The `nanocached.*` properties are
  unchanged.

### Security

- The Spring Boot dependencies platform moves from 3.5.3 to 3.5.16, the latest
  3.5 patch release (it manages Spring Framework 6.2.19). 3.5.3 predates
  several published security fixes in later 3.5 releases, and as an `api`
  platform it is the version an application inherits unless it overrides it.

## [0.5.1] - 2026-10-27

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.5.0] - 2026-10-26

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

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
