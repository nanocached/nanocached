# nanocached-spring-boot-starter

Spring Boot autoconfiguration for [`nanocached-spring`](../spring): the same
`CacheManager`/`Cache` adapter, wired from `nanocached.*` properties instead
of the two manual `@Bean` methods that module's README describes. Adding
this dependency and setting `nanocached.addresses` is the whole setup.

## Setup

```yaml
nanocached:
  addresses: "10.0.0.1:8357,10.0.0.2:8357"
  cache:
    default-ttl: 10m
    ttl:
      sessions: 30s
```

```java
@SpringBootApplication
@EnableCaching
class Application { }
```

No `@Bean` methods. The autoconfiguration registers a `NanocachedClient`
(closed with the context) and a `CacheManager` on top of it, both
`@ConditionalOnMissingBean` — define either yourself and the starter backs
off just that one, leaving the other autoconfigured.

**Inert without `nanocached.addresses`.** The autoconfiguration is gated on
that property (`@ConditionalOnProperty`), so adding the dependency alone
changes nothing — same principle as Boot's own autoconfigurations backing
off until configured. It also runs `@AutoConfigureBefore` Boot's own
`CacheAutoConfiguration`, so it wins over Boot's default manager (e.g. the
Redis starter's) with zero `spring.cache.*` configuration, and a stray
`spring.cache.type=...` left over from a migration does not override it —
those keys only steer Boot's own autoconfiguration, which has already
backed off once an explicit `CacheManager` bean exists.

## Properties

All under the `nanocached` prefix, passed straight to
`NanocachedClient.Options` / `NanocachedCacheManager.Builder` (see
[nanocached-spring](../spring) and the Java SDK for what each one means):

| Property | Maps to |
|---|---|
| `addresses` | `Options.addresses` — required, `host:port` list |
| `secret` | `Options.authSecret` |
| `tls` | `Options.tls` |
| `ca` | `Options.ca` |
| `compress` | `Options.compress` |
| `compression-threshold` | `Options.compressionThreshold` |
| `fire-and-forget-replicas` | `Options.fireAndForgetReplicas` |
| `read-repair` | `Options.readRepair` |
| `reconnect-cooldown` | `Options.reconnectCooldown` |
| `read-hedge-after` | `Options.readHedgeAfter` |
| `cache.default-ttl` | `NanocachedCacheManager.Builder.defaultTtl` |
| `cache.ttl.<name>` | `NanocachedCacheManager.Builder.ttl(name, ...)` |
| `cache.allow-null-values` | `NanocachedCacheManager.Builder.allowNullValues` |
| `cache.cache-names` | `NanocachedCacheManager.Builder.cacheNames` |

`cache.cache-names` is aligned with Boot's own `spring.cache.cache-names`:
restricts the manager to a closed, eagerly-created set instead of creating
a cache on first use.

## Usage

Once autoconfigured, the standard annotations work exactly as documented in
[nanocached-spring](../spring)'s README — `@Cacheable`, `@CachePut`,
`@CacheEvict`, `sync = true` read-through, and everything else that
module's `NanocachedCache` implements. This module contributes nothing
beyond the two beans; it does not change adapter behavior.

## Requirements

Java 17+, Spring Boot 3.x, nanocached server ≥ the release that ships
namespaces and CLEAR (issues #105/#106).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only.

[#25]: https://github.com/nanocached/nanocached/issues/25

## Building

```
cd adapters/spring-boot-starter
gradle test
```

The build includes the sibling `sdk/java` and `adapters/spring` sources
(Gradle composite build), so a checkout needs no locally-installed
artifacts.

MIT license.
