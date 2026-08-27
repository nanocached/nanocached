# nanocached-cache-manager

A [`cache-manager`](https://github.com/jaredwray/cacheable/tree/main/packages/cache-manager)
v5 `Store` for the [nanocached](https://github.com/nanocached/nanocached)
TypeScript/Node.js SDK — `nanocachedStore` plugs into `caching(...)` the
same way `cache-manager`'s own Redis store does, so `get`/`set`/`del`,
multi-key ops, and `wrap()` (its read-through idiom) all run against a
nanocached cluster.

- **One store ⇄ one namespace.** Every `NanocachedStore` is scoped to
  exactly one nanocached namespace (issue #105), named by its `namespace`
  config (default `"cache-manager"`) — two stores on different namespaces
  never collide, even against the same cluster, and `reset()` is that
  namespace's `CLEAR` (issue #106): an O(1) sub-map drop on every node,
  never the whole-cluster flush.
- **`get`/`set`/`del`/`mget`/`mset`/`mdel`** map to the SDK's namespaced
  get/set/delete, with all of its routing, replication, hedged reads and
  retries. `mget`/`mset`/`mdel` are a client-side `Promise.all` loop over
  the single-key ops — bulk wire operations are a separate, later
  decision, not part of this adapter.
- **Values are JSON-serialized** (`JSON.stringify`/`JSON.parse`), the
  convention `cache-manager`'s own stores use (its Redis store does the
  same).
- **`keys()`/`ttl()` are unsupported** — the wire has no key-enumeration
  or TTL-readback operation, so both always throw a documented
  `NotSupportedError` rather than returning a misleading empty/zero
  answer.

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
  down to "no expiry". A call's `ttl` (when positive) wins; `ttl` 0 or
  omitted falls back to the store's configured default (`ttl` in the
  config above); no default configured either means no expiry (TTL 0 on
  the wire). This is a real, documented precision loss:
  `cache.set(key, value, 1)` (1ms) and `cache.set(key, value, 999)`
  (999ms) both land on the wire as a 1-second TTL — nanocached has no
  finer granularity.
- **`undefined`** is "missing": `get` resolves `undefined` on a miss;
  storing `undefined` (`cache.store.set(key, undefined)`) is a no-op,
  matching the Redis store's own convention, since `undefined` already
  means exactly that. **`null`** is a real, distinct value and round-trips
  normally (`JSON.stringify(null)` is `"null"`, not `undefined`).
- **`reset()`** clears this store's namespace only (`CLEAR`, issue #106)
  — never `NanocachedClient.clearAll()`'s whole-cluster flush. Two stores
  on different namespaces are fully isolated from each other's `reset()`.
- **`keys()`/`ttl()`** always throw `NotSupportedError`: a nanocached node
  only ever answers about one key at a time (`get`/`set`/`delete`/`clear`),
  so there is no request this store could send to enumerate a namespace's
  keys or read back a stored TTL.
- **`mget`/`mset`/`mdel`** are `Promise.all` loops over `get`/`set`/`del`
  — concurrent, not a single wire round trip. `mget` preserves key order
  in its result (including `undefined` holes for misses); `mset` applies
  one `ttl` to every entry, same as `cache-manager`'s own multi-store
  fallback for a store lacking a native `mset`.

## Requirements

Node.js 20+, `cache-manager` ^5 (the `get`/`set`/`del`/`reset`/`mget`/`mset`
shape this targets — its `Keyv`-based v6+/NestJS 11 successor is a
follow-up, not this package), nanocached server ≥ the release that ships
namespaces and `CLEAR` (issues #105/#106).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for `cache-manager`; other ecosystems get their
own idiomatic adapters (Spring `CacheManager`, `IDistributedCache`, Django
cache backend, [JCache](../jcache)) rather than mirrors of this one.

[#25]: https://github.com/nanocached/nanocached/issues/25

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
