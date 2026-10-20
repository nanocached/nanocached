# nanocached-cache-manager

A [`cache-manager`](https://github.com/jaredwray/cacheable/tree/main/packages/cache-manager)
v5 `Store` for the [nanocached](https://github.com/nanocached/nanocached)
TypeScript/Node.js SDK — `nanocachedStore` plugs into `caching(...)` the
same way `cache-manager`'s own Redis store does, so `get`/`set`/`del`,
multi-key ops, and `wrap()` (its read-through idiom) all run against a
nanocached cluster.

- **One store ⇄ one namespace.** Every `NanocachedStore` is scoped to
  exactly one nanocached namespace, named by its `namespace`
  config (default `"cache-manager"`) — two stores on different namespaces
  never collide, even against the same cluster, and `reset()` is that
  namespace's `CLEAR`: one sub-map removal on every node, with the
  entries freed off its cache thread, never the whole-cluster flush.
- **`get`/`set`/`del`/`mget`/`mset`/`mdel`** map to the SDK's namespaced
  operations, with all of its routing, replication and retries (hedged
  reads and read repair apply to single-key `get` only, not to batches).
  `mget`/`mset` use the SDK's batched `getMany`/`setMany`: one wire
  round trip per involved node. `mdel` remains a `Promise.all` loop —
  the wire has no multi-key delete.
- **Values are JSON-serialized** (`JSON.stringify`/`JSON.parse`), the
  convention `cache-manager`'s own stores use (its Redis store does the
  same).
- **`keys()` is unsupported** — the wire has no key-enumeration operation,
  so it always throws a documented `NotSupportedError` rather than
  returning a misleading empty answer. **`ttl()` degrades gracefully
  instead**:
  the wire has no TTL-readback operation either, but `ttl()` is a
  *required* `Store` member that `cache-manager`'s own `wrap()` calls
  unconditionally whenever `refreshThreshold` is configured — throwing
  there would turn every cache hit into an uncaught error. So `ttl()`
  always returns `-1`, which `wrap()` treats as "unknown" and never
  refreshes ahead for. See "Refresh-ahead" below.

## Install

```sh
npm install nanocached-cache-manager cache-manager@5
```

`cache-manager` v5 is a peer dependency; the `nanocached` SDK comes with
the package (each adapter release depends on the SDK release of the same
version).

## Setup

`nanocachedStore` is a `cache-manager` v5 store *factory*: pass it as
`caching()`'s first argument, with connection and namespace settings as
the second:

```ts
import { caching } from "cache-manager";
import { nanocachedStore } from "nanocached-cache-manager";

const cache = await caching(nanocachedStore, {
  addresses: [{ host: "10.0.0.1", port: 8357 }],
  namespace: "cache-manager", // default; one nanocached namespace per store
  ttl: 60_000,                // default TTL in MILLISECONDS (cache-manager's unit)
  secret: "...",              // optional, matches NANOCACHED_AUTH_SECRET
});

await cache.set("user:42", { name: "Ada" });
const user = await cache.get("user:42"); // { name: "Ada" } | undefined

// The headline cache-manager idiom: computes once, later calls (until
// expiry) are served from the cache.
const user2 = await cache.wrap("user:42", () => loadUserFromDb(42));

await cache.store.disconnect(); // see "Lifecycle" below
```

`addresses` is required — one or more `nanocached-node`/`nanocached-discovery`
addresses, tried in order, exactly like
`NanocachedClientOptions.addresses` in the SDK. Every other SDK connect
option is passed through under the same name (`tls`, `ca`, `compress`,
`compressionThreshold`, `fireAndForgetReplicas`, `readRepair`,
`readHedgeAfterMs`, `reconnectCooldownMs`), except the shared secret,
renamed `secret` here (`authSecret` in the SDK) to match the shorter name
`cache-manager`'s own stores use for the equivalent setting.

`nanocachedStore` connects a fresh `NanocachedClient` every call — it
never reuses a client the caller already has (matching how `cache-manager`'s
own store factories, e.g. its Redis store, always dial their own client).
Nothing about *using* this package changes behavior on its own — as with
any `cache-manager` store, the `caching(...)` call above is the setup;
merely adding the dependency does nothing.

### Lifecycle

`cache-manager` never closes a store itself — there's no `close`/`disconnect`
in its own `Store` type. `store.disconnect()` (this adapter's own
addition, matching the Redis store's convention) closes the underlying
`NanocachedClient`; call it on shutdown. It's idempotent, like
`NanocachedClient.close`. `store.client` reaches the underlying SDK
client directly, for anything this store doesn't itself surface —
`client.stats()`, `client.replication`, a second namespace via
`client.namespace(...)`, etc.

## Semantics

- **TTL.** `cache-manager` passes milliseconds throughout (`Config.ttl`,
  `Store.set`'s `ttl` parameter); the wire's unit is whole seconds. A
  positive value under one second **rounds up** to 1s — it never rounds
  down to "no expiry". A call's `ttl` wins whenever it is given — an
  explicit `0` means "no expiry" and is *not* replaced by the default,
  exactly as in `cache-manager-redis-yet` and the built-in memory store;
  only an omitted `ttl` falls back to the store's configured default
  (`ttl` in the config above), and no default configured either means
  no expiry (TTL 0 on the wire). This is a real, documented precision
  loss:
  `cache.set(key, value, 1)` (1ms) and `cache.set(key, value, 999)`
  (999ms) both land on the wire as a 1-second TTL — nanocached has no
  finer granularity.
- **`undefined`** is "missing": `get` resolves `undefined` on a miss, and
  storing it (`cache.set(key, undefined)`, or an `mset` entry holding it)
  throws `NoCacheableError` without writing anything — the same rule
  `cache-manager`'s own memory store and `cache-manager-redis-yet` apply.
  The config's `isCacheable` predicate replaces that default: `set` and
  `mset` throw `NoCacheableError` for any value it rejects, and `mset`
  checks the whole batch before writing any of it. `wrap()` catches the
  rejection itself and emits it on the cache's `error` event, so a
  wrapped function that returns `undefined` still returns `undefined`
  to its caller; nothing is cached and the event fires. **`null`** is a
  real, distinct value and round-trips normally (`JSON.stringify(null)`
  is `"null"`, not `undefined`).
- **`reset()`** clears this store's namespace only (`CLEAR`)
  — never `NanocachedClient.clearAll()`'s whole-cluster flush. Two stores
  on different namespaces are fully isolated from each other's `reset()`.
- **`keys()`** always throws `NotSupportedError`: a nanocached node only
  ever answers about one key at a time (`get`/`set`/`delete`/`clear`), so
  there is no request this store could send to enumerate a namespace's
  keys.
- **Refresh-ahead (`refreshThreshold`) silently degrades to plain
  caching.** `ttl()` always resolves `-1` — never a real remaining TTL,
  since the wire has no TTL-readback operation either (same limitation as
  `keys()`), and unlike `keys()`, `ttl()` can't just throw: it's a
  required `Store` member, and `cache-manager`'s `wrap()` calls
  `store.ttl(key)` unconditionally and uncaught whenever a caller passes
  `refreshThreshold` (`cache.wrap(key, fn, ttl, refreshThreshold)` or the
  `refreshThreshold` config option), on *every* cache hit. `-1` is exactly
  the sentinel `wrap()`'s own logic already treats as "unknown TTL, skip
  the refresh-ahead check" (`remainingTtl !== -1 && remainingTtl <
  refreshThresholdConfig`), so passing `refreshThreshold` here never
  throws and never triggers a background reload — it behaves exactly like
  `wrap()` without `refreshThreshold` at all. If your application relies
  on refresh-ahead actually firing before expiry, this store cannot
  provide that; use `wrap()` for its cache-aside behavior only.
- **`mget`/`mset`** ride the wire's batched multi-get/multi-set — one round
  trip per involved node; **`mdel`** is a concurrent
  `Promise.all` loop over `del`. `mget` preserves key order in its result
  (including `undefined` holes for misses — and a hole for a key the
  node could not fit: the values in one batch reply total at most
  16 MiB, and a hit past that budget is answered as a miss, so an `mget`
  of many large keys can come back with fewer values than exist; `get`
  such a key on its own rather than taking it for evicted); `mset`
  applies one `ttl` to
  every entry, same as `cache-manager`'s own multi-store fallback for a
  store lacking a native `mset`.

## When the node is unreachable

Every operation fails fast rather than hanging: the SDK bounds each
connect and each request by its timeouts, and a node that is down makes
`get`/`set`/`del`/`mget`/`mset`/`mdel`/`reset` reject with the SDK's
`ConnectionLostError` (a connection error from the operating system when
no connection could be opened at all). Called directly, that rejection
is the caller's to handle. `wrap()` fails open: `cache-manager` catches
the store's `get` and `set` rejections, emits them on the cache's
`error` event, and runs the wrapped function as if the key were a miss —
so an outage costs the computation, never the request, but a listener
on `error` is the only place the outage is reported.

## Value size limit

A namespace, key and value together must fit in one request: 1 MiB
minus a 256-byte header, 1,048,320 bytes. A `set` or `mset` entry over
that size rejects with a `RangeError` before anything is sent, and the
entry is not written. Values
are measured after JSON serialization, and after compression when
`compress` is on.

## Requirements

Node.js 20+, `cache-manager` ^5 (the `get`/`set`/`del`/`reset`/`mget`/`mset`
shape this targets — its `Keyv`-based v6+/NestJS 11 successor is
[`nanocached-keyv`](../keyv), a separate package), nanocached server
0.4.0 or later — the release that added namespaces, `CLEAR` and the
batched multi-get/multi-set frames `mget`/`mset` use.

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy: parity applies to the SDK core
only. This module exists for `cache-manager` v5; other ecosystems get
their own idiomatic adapters (Spring `CacheManager`, `IDistributedCache`,
Django cache backend, [JCache](../jcache), [Keyv](../keyv) for
`cache-manager` v6+/NestJS 11) rather than mirrors of this one.

## Building

```
cd adapters/cache-manager
pnpm install && pnpm typecheck && pnpm test
```

This package depends on the sibling `sdk/typescript` via
`"nanocached": "link:../../sdk/typescript"` — pnpm's `link:` protocol,
which always creates a real symlink to that directory (unlike plain
`file:`, which — verified while building this package — packs a
*snapshot* of the target directory at `pnpm install` time, respecting its
`package.json`'s `"files"` allowlist; since the SDK's build output
(`dist/`) is gitignored and produced by a separate build step, an install
that ran before that step packed an incomplete dependency missing
`dist/` entirely, permanently, until the next `pnpm install`). With the
symlink, `pretypecheck`/`pretest` scripts here run `sdk/typescript`'s own
`build` first (`pnpm --dir ../../sdk/typescript run build`) so
`dist/` exists before `tsc`/`node --test` resolve `nanocached` through it
— no manual build step, no checked-in SDK artifact needed for a fresh
checkout, and `pnpm install && pnpm typecheck && pnpm test` works
unmodified.

MIT license.
