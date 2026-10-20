# nanocached-keyv

A [Keyv](https://keyv.org/) `KeyvStoreAdapter` for the
[nanocached](https://github.com/nanocached/nanocached) TypeScript/Node.js SDK
— for `cache-manager` v6+ (its `createCache()`, not the older `caching()`)
and NestJS 11's `CacheModule`, both of which moved onto Keyv storage
adapters. If you're still on `cache-manager` v5, see the sibling
[`nanocached-cache-manager`](../cache-manager) instead.

- **One store ⇄ one namespace.** Every `NanocachedKeyvStore` is scoped to
  exactly one nanocached namespace, named by its own
  `namespace` config (default `"keyv"`) — two stores on different
  namespaces never collide, even against the same cluster, and `clear()`
  is that namespace's `CLEAR`.
- **`get`/`set`/`delete`/`clear`** map to the SDK's namespaced
  get/set/delete, with all of its routing, replication, hedged reads and
  retries.
- **Values pass through opaque.** Keyv itself JSON-encodes `{value,
  expires}` before ever calling `set()`, and decodes whatever `get()`
  returns — this store never touches that envelope, it just persists and
  returns the raw string.
- **`has`/`hasMany`, `getMany`/`setMany`/`deleteMany`, and `iterator` are
  all deliberately omitted** — see "Honest subset" below for why each one
  is the *more correct* choice, not just less code.

## Install

```sh
npm install nanocached-keyv keyv
```

`keyv` v5 is a peer dependency; the `nanocached` SDK comes with the
package (each adapter release depends on the SDK release of the same
version).

## Setup

```ts
import Keyv from "keyv";
import { nanocachedKeyvStore } from "nanocached-keyv";

const store = await nanocachedKeyvStore({
  addresses: [{ host: "10.0.0.1", port: 8357 }],
  namespace: "keyv", // default; one nanocached namespace per store
  secret: "...",     // optional, matches NANOCACHED_AUTH_SECRET
});

// Always wrap it yourself, and always with useKeyPrefix: false — see
// "Namespace vs. Keyv's own prefixing" below for why.
const keyv = new Keyv({ store, useKeyPrefix: false });

await keyv.set("user:42", { name: "Ada" }, 60_000); // ttl in ms
const user = await keyv.get("user:42"); // { name: "Ada" } | undefined

await store.disconnect(); // see "Lifecycle" below
```

`addresses` is required — one or more `nanocached-node`/`nanocached-discovery`
addresses, tried in order, exactly like `NanocachedClientOptions.addresses`
in the SDK. Every other SDK connect option is passed through under the same
name (`tls`, `ca`, `compress`, `compressionThreshold`,
`fireAndForgetReplicas`, `readRepair`, `readHedgeAfterMs`,
`reconnectCooldownMs`), except the shared secret, renamed `secret` here
(`authSecret` in the SDK), matching the sibling `cache-manager` v5 store's
convention.

`nanocachedKeyvStore` connects a fresh `NanocachedClient` every call — it
never reuses a client the caller already has.

### With `cache-manager` v6+

```ts
import { createCache } from "cache-manager";
import Keyv from "keyv";
import { nanocachedKeyvStore } from "nanocached-keyv";

const store = await nanocachedKeyvStore({ addresses: [{ host: "10.0.0.1", port: 8357 }] });
const cache = createCache({ stores: [new Keyv({ store, useKeyPrefix: false })] });

await cache.set("user:42", { name: "Ada" });
const user = await cache.get("user:42");

// createCache() never disconnects its stores on its own (verified against
// cache-manager 7.2.9) — call this yourself on shutdown, e.g. in a signal
// handler. See "Lifecycle" below.
process.on("SIGTERM", () => cache.disconnect());
```

### With NestJS 11's `CacheModule`

```ts
import { Module } from "@nestjs/common";
import { CacheModule } from "@nestjs/cache-manager";
import Keyv from "keyv";
import { nanocachedKeyvStore } from "nanocached-keyv";

const store = await nanocachedKeyvStore({ addresses: [{ host: "10.0.0.1", port: 8357 }] });

@Module({
  imports: [CacheModule.register({ stores: [new Keyv({ store, useKeyPrefix: false })] })],
})
export class AppModule {}
```

**Build the `Keyv` instance yourself, always** — including under NestJS.
`@nestjs/cache-manager`'s `CacheModule.register` *will* accept a bare
`KeyvStoreAdapter` in `stores` and wrap it in a `Keyv` for you, but that
internal wrapping path (`cache.providers.js`, verified against
`@nestjs/cache-manager` 3.x) never forwards `useKeyPrefix` — there is no
way to disable Keyv's own key prefixing through it. Wrapping the store
yourself, as shown above, is the only way to control it under Nest, and
`CacheModule.register` accepts an already-built `Keyv` instance
unmodified.

### Lifecycle

Keyv never closes a store itself. `store.disconnect()` (this adapter's own
addition, matching the sibling `cache-manager` store's convention) closes
the underlying `NanocachedClient`; call it on shutdown. It's idempotent,
like `NanocachedClient.close`.

Plain `cache-manager` v6+ `createCache()` does **not** auto-disconnect its
stores — verified against installed `cache-manager@7.2.9`: `createCache()`
only exposes its own `cache.disconnect()` (which fans out to each store's
`disconnect()`), it never calls it for you. Skipping this in your own
shutdown path leaks the socket and its keep-alive timer, as in the
`cache-manager` v6+ example above.

NestJS's `CacheModule` is the one case that *does* wire this up for you:
`@nestjs/cache-manager` (verified against 3.1.3) sets
`cacheManager.onModuleDestroy` to call each store's `disconnect()`, which
Nest invokes when the module is destroyed — on `app.close()`, or on a
shutdown signal if `app.enableShutdownHooks()` was called. Don't call
`disconnect()` a second time yourself in that case.

`store.client` reaches the underlying SDK client directly, for anything
this store doesn't itself surface (`client.stats()`, `client.replication`,
a second namespace via `client.namespace(...)`, etc.).

## Namespace vs. Keyv's own prefixing

This adapter's `namespace` config (→ one dedicated nanocached namespace) and
Keyv's own `namespace`/`useKeyPrefix` options (an in-band string prefix Keyv
adds to every key before ever calling this store, independent of any
storage backend) are **two unrelated concepts that happen to share a
name** — the classic trap here is conflating them, not any actual
double-write bug: this store's namespace isolation is already complete on
its own (one instance = one nanocached namespace = `clear()` is always
exactly right, no key-prefix bookkeeping needed), so Keyv's own prefixing
is pure redundancy on top of it. Leaving it on (the default) is harmless —
just an extra string baked into the wire key bytes, and keys that are less
legible if you inspect the nanocached server directly — which is why the
examples above pass `useKeyPrefix: false`.

## Honest subset — what's omitted, and why each is the more correct choice

Probed directly against `keyv@5.6.0` (the version real `cache-manager` v6+
and `@nestjs/cache-manager` 3.x currently depend on) while building this
adapter:

- **`has`/`hasMany`**: a naive presence check on this wire would misreport
  a key as present *after* its precise millisecond `expires` deadline
  (embedded in Keyv's own JSON envelope) has passed but *before* the
  wire's coarser whole-second TTL has actually swept it server-side.
  Confirmed live: a hand-rolled `has()` that only checked raw wire
  presence returned `true` for an already-expired key; Keyv's own built-in
  fallback (calling `get()`, which decodes the envelope and checks
  `expires` itself) correctly returned `false`. Omitting `has` lets that
  correct fallback run, instead of shadowing it with a less-correct one.
- **`getMany`/`setMany`/`deleteMany`**: Keyv's built-in fallback (used
  automatically when an adapter omits these) is already a concurrent
  per-key loop — confirmed by timing against an artificially-slowed fake
  store. A custom implementation would cost exactly the same number of
  wire round trips as the fallback, so it's pure surface with no benefit.
- **`iterator`**: even *defining* the property — including one that only
  throws — is actively dangerous with this Keyv version. Keyv's
  constructor gates iterator wiring behind
  `'iterator' in store && store.opts && this._checkIterableAdapter()`, and
  `_checkIterableAdapter()` unconditionally does
  `store.opts.url.includes(...)` — which **throws a `TypeError` inside
  `new Keyv(...)`** for any adapter whose `opts` lacks a `url` string.
  Reproduced live. Omitting the `iterator` property entirely avoids that
  code path; `keyv.iterator` simply stays `undefined`, which is one of the
  two outcomes the wire's actual limitation (no key enumeration) leaves
  available anyway.

## When the node is unreachable

Every operation fails fast rather than hanging: the SDK bounds each
connect and each request by its timeouts, and a node that is down makes
the store's `get`/`set`/`delete`/`clear` reject with the SDK's
`ConnectionLostError`, or with the operating system's own connect error
(an `Error` with `code: "ECONNREFUSED"`, for example) when no connection
could be opened at all. What your code sees is then Keyv's own
convention, not this adapter's: by default (`throwOnErrors: false`) Keyv
swallows the rejection. `set`/`delete`/`clear` resolve `false` and emit
the error on Keyv's `error` event; `get` resolves `undefined` and, in
`keyv` 5.6, emits nothing at all — so an outage looks like a cold cache
on reads, and only writes report it, and only to an `error` listener.
With `new Keyv({ store, throwOnErrors: true })` every rejection
propagates to the caller instead. `cache-manager` v6+ `createCache()` and NestJS's
`CacheModule` build on Keyv's default, so their `get`/`wrap` fail open
in the same way.

## Value size limit

A namespace, key and value together must fit in one request: 1 MiB
minus a 256-byte header, 1,048,320 bytes. A `set` over that size rejects
with a `RangeError` before anything is sent, and nothing is written. The value is measured as Keyv hands it
over — its JSON envelope (`{value, expires}`) included — and after
compression when `compress` is on. Under Keyv's default error policy
that `RangeError` is the same `error` event plus `set` resolving `false`
described above, so an oversize value is silently not cached unless
`throwOnErrors` is on or an `error` listener is attached.

## Requirements

Node.js 20+, `keyv` ^5 (the relative-millisecond-`ttl` adapter contract
this targets — `keyv@6`'s absolute-`expires` contract is a different shape
and, as of writing, still only at a release candidate, not what published
`cache-manager`/`@nestjs/cache-manager` depend on), nanocached server
0.4.0 or later — the release that added namespaces and `CLEAR`.

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy: parity applies to the SDK core
only. This module exists for Keyv/`cache-manager` v6+/NestJS 11; other
ecosystems get their own idiomatic adapters (Spring `CacheManager`,
`IDistributedCache`, Django cache backend, [JCache](../jcache),
[`cache-manager` v5](../cache-manager)) rather than mirrors of this one.

## Building

```
cd adapters/keyv
pnpm install && pnpm typecheck && pnpm test
```

This package depends on the sibling `sdk/typescript` via
`"nanocached": "link:../../sdk/typescript"` — see the sibling
`cache-manager` adapter's README for why `link:` (not `file:`) and the
`build:sdk`/`pretypecheck`/`pretest` scripts exist.

MIT license.
