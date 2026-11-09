# nanocached-keyv

A [Keyv](https://keyv.org/) storage adapter for the
[nanocached](https://github.com/nanocached/nanocached) TypeScript/Node.js SDK,
for **keyv 5 and keyv 6** — and so for `cache-manager` v6+ (its
`createCache()`, not the older `caching()`) and NestJS 11's `CacheModule`,
both of which moved onto Keyv storage adapters (and currently run on
keyv 5). If you're still on `cache-manager` v5, see the sibling
[`nanocached-cache-manager`](../cache-manager) instead.

- **One store ⇄ one namespace.** Every `NanocachedKeyvStore` is scoped to
  exactly one nanocached namespace, named by its own
  `namespace` config (default `"keyv"`) — two stores on different
  namespaces never collide, even against the same cluster, and `clear()`
  is that namespace's `CLEAR`: it wipes the whole nanocached namespace,
  so every `Keyv` instance sharing one store (or one store `namespace`)
  loses its keys together, whatever Keyv's own `namespace` prefix says.
  See "Namespace vs. Keyv's own prefixing".
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

`keyv` (5 or 6) is a peer dependency; the `nanocached` SDK comes with the
package (each adapter release depends on the SDK release of the same
version). keyv 6 itself requires Node.js 22.19 or later; this package's own
floor is Node.js 22, which is enough with keyv 5.

## Setup

```ts
import Keyv from "keyv";
import { nanocachedKeyvStore } from "nanocached-keyv";

const store = await nanocachedKeyvStore({
  addresses: [{ host: "10.0.0.1", port: 8357 }],
  namespace: "keyv", // default; one nanocached namespace per store
  secret: "...",     // optional, matches NANOCACHED_AUTH_SECRET
});

// Always wrap it yourself. With keyv 5, always with useKeyPrefix: false —
// see "Namespace vs. Keyv's own prefixing" below for why.
const keyv = new Keyv({ store, useKeyPrefix: false }); // keyv 5
// With keyv 6 (which has no useKeyPrefix option) it is just:
// const keyv = new Keyv(store);

await keyv.set("user:42", { name: "Ada" }, 60_000); // ttl in ms
const user = await keyv.get("user:42"); // { name: "Ada" } | undefined

await store.disconnect(); // see "Lifecycle" below
```

`addresses` is required — one or more `nanocached-node`/`nanocached-discovery`
addresses, tried in order, exactly like `NanocachedClientOptions.addresses`
in the SDK. Every other SDK connect option is passed through under the same
name (`tls`, `ca`, `compress`, `compressionThreshold`,
`fireAndForgetReplicas`, `readRepair`, `readHedgeAfterMs`,
`reconnectCooldownMs`, `disableReconnectCooldown`), except `viaProxy`, which this store does not
expose, and the shared secret, renamed `secret` here
(`authSecret` in the SDK), matching the sibling `cache-manager` v5 store's
convention.

`nanocachedKeyvStore` connects a fresh `NanocachedClient` every call — it
never reuses a client the caller already has. To share a client you already
hold, build the store yourself: `new NanocachedKeyvStore(client, namespace)`.
`store.disconnect()` then closes that client for every user of it, so close
the client yourself when you are done with all of them. The members `opts`,
`namespace` and `on()` exist only because Keyv's adapter protocol asks for
them; do not use them.

### With `cache-manager` v6+

`cache-manager` 7 and `@nestjs/cache-manager` build on keyv 5, so the
examples below use the keyv 5 form (`useKeyPrefix: false`). Build the `Keyv`
instance from the same keyv major version the framework itself uses.

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
name**. This store's isolation is the nanocached namespace: one store
instance is one namespace, and `clear()` is that namespace's `CLEAR`.
Keyv's own prefix is a string baked into each wire key; the wire has no
way to enumerate or delete keys by prefix, so this store cannot honour
it for `clear()`. Keyv's README shows two instances on one store with
different `namespace` options, where `users.clear()` leaves
`cache.get("foo")` intact — **that does not hold here**: with
`new Keyv({ store, namespace: "users" })` and
`new Keyv({ store, namespace: "cache" })` on the same store (or on two
stores with the same `namespace` config), `users.clear()` empties the
nanocached namespace and `cache.get("foo")` is `undefined` afterwards.
To isolate `Keyv` instances, give each its own store with its own
`namespace` config; then their `clear()`s are independent, and Keyv's
prefix buys nothing. Leaving Keyv's prefixing on costs only an extra
string in every wire key, and keys that are less legible if you inspect
the server directly — which is why the examples above pass
`useKeyPrefix: false`.

**keyv 6 differs in two ways.** There is no `useKeyPrefix` option: a
`Keyv` with no `namespace` of its own sends the raw key to the wire (the same
thing `useKeyPrefix: false` gives you on keyv 5), and a `Keyv` with a
`namespace` has keyv 6 prefix every key with `<namespace>::<key>` itself. And
`clear()` on a `Keyv` that has a namespace does **not** wipe the nanocached
namespace: keyv 6 clears a namespace by enumerating the store's keys, which
the wire cannot do, so it emits an `error` event (it throws when nothing
listens, see "When the node is unreachable") and deletes nothing. That is the
safe outcome rather than a silent wipe, but it means that with keyv 6 you
should not set a Keyv `namespace` on a store you call `clear()` on; leave it
out and give each `Keyv` its own store with its own `namespace` config, as
above. Without a Keyv namespace, `clear()` is the nanocached namespace's
`CLEAR`, exactly as on keyv 5.

## Honest subset — what's omitted, and why each is the more correct choice

Probed directly against `keyv@5.6.0` (the version real `cache-manager` v6+
and `@nestjs/cache-manager` currently depend on) while building this
adapter, and re-checked against `keyv@6.1.0`:

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
  available anyway. (keyv 6 has no such crash, and its `keyv.iterator` yields
  nothing for this adapter.)

## How keyv 6 drives this adapter

keyv 6 changed the storage-adapter contract: `set(key, value, expires)`
receives an absolute Unix-millisecond expiry, for an adapter that advertises
`capabilities.expires`. This adapter does not advertise it, so keyv 6 treats
it as a legacy relative-`ttl` adapter and wraps it in its own
`KeyvBridgeAdapter`, which turns the absolute expiry back into the remaining
milliseconds before calling `set(key, value, ttl)` — the same call keyv 5
makes. One code path therefore serves both versions, and a keyv 5 and a
keyv 6 `Keyv` can even share one store without either misreading the other's
`set()`. (If the adapter did advertise `expires`, it could not tell which
convention a given call uses, and a keyv 5 relative `ttl` of 60000 would read
as an expiry in 1970.)

TTL rounding is unchanged: a positive sub-second value rounds up to 1 s,
other values round up to the next whole second, and no expiry sends 0. Under
keyv 6 the value being rounded is the time left when the bridge reaches the
adapter, a few milliseconds less than the `ttl` you passed, so a `ttl` that
is not a whole number of seconds (2001 ms, say) can land on the lower second
(2 s rather than 3 s); whole-second values (1000, 60000) are exact. A
deadline that has already passed is never written: the bridge deletes the
key instead, as keyv 5 does for a negative `ttl`. Both versions also check
the deadline on read, so an entry is hidden at its precise millisecond
whatever the wire's second-granularity ttl says.

Other visible differences between the versions:

- A miss is `undefined` on both, and a stored `null` still round-trips as
  `null`.
- `has`, `getMany` and `deleteMany` come from the bridge on keyv 6 (loops over
  `get`/`delete`), from keyv 5's own fallbacks on keyv 5; same results.

## When the node is unreachable

Every operation fails fast rather than hanging: the SDK bounds each
connect and each request by its timeouts, and a node that is down makes
the store's `get`/`set`/`delete`/`clear` reject with the SDK's
`ConnectionLostError`, or with the operating system's own connect error
(an `Error` with `code: "ECONNREFUSED"`, for example) when no connection
could be opened at all. What your code sees is then Keyv's own
convention, not this adapter's. With **keyv 5**, by default
(`throwOnErrors: false`) Keyv swallows the rejection: `set`/`delete`/`clear`
resolve `false` and emit the error on Keyv's `error` event; `get` resolves
`undefined` and, in `keyv` 5.6, emits nothing at all — so an outage looks
like a cold cache on reads, and only writes report it, and only to an
`error` listener. With `new Keyv({ store, throwOnErrors: true })` every
rejection propagates to the caller instead. With **keyv 6** there is no
`throwOnErrors`; it follows the `error` listener instead: with a listener
attached, the failed `set`/`delete` resolve `false`, `get` resolves
`undefined`, and every failure (reads included) is passed to the listener;
with no listener on the `Keyv`, the call rejects with the error. `cache-manager` v6+ `createCache()` and NestJS's
`CacheModule` build on Keyv's default, so their `get`/`wrap` fail open
in the same way.

## Value size limit

A namespace, key and value together must fit in one request: 1 MiB
minus a 256-byte header, 1,048,320 bytes. A `set` over that size rejects
with a `RangeError` before anything is sent, and nothing is written. The value is measured as Keyv hands it
over — its JSON envelope (`{value, expires}`) included — and before
compression, so with `compress` on a value over the limit is still
rejected, even if it would compress to less. Under Keyv's default error policy
that `RangeError` is the same `error` event plus `set` resolving `false`
described above, so an oversize value is silently not cached unless
`throwOnErrors` is on or an `error` listener is attached.

## Stored format

Values are stored as whatever string Keyv hands the store: Keyv's own serialization, not this package's. That format is **not covered** by the compatibility promise: a release may change it, and says so in its changelog. Plan a rolling deploy accordingly: while old and new workers share a namespace, one side may fail to read what the other wrote (see the [compatibility page](https://nanocached.org/compatibility.html)).

## Requirements

Node.js 22+ (keyv 6 itself needs 22.19+), `keyv` ^5 or ^6 (tested against
5.6.0 and 6.1.0), nanocached server 0.4.0 or later — the release that added
namespaces and `CLEAR`.

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

The tests run the adapter through keyv 6 directly (`keyv`, the dev
dependency) and through keyv 5 (`keyv5`, an npm alias of `keyv@^5`, which is
also what `cache-manager` and NestJS use).

This package depends on the sibling `sdk/typescript` via
`"nanocached": "link:../../sdk/typescript"` — see the sibling
`cache-manager` adapter's README for why `link:` (not `file:`) and the
`build:sdk`/`pretypecheck`/`pretest` scripts exist.

MIT license.
