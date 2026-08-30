import { NanocachedClient, type NanocachedAddress, type NanocachedNamespace } from "nanocached";
import type { KeyvStoreAdapter } from "keyv";

// cache-manager v5's own store convention (see the sibling
// `nanocached-cache-manager` package) binds one store to one namespace by
// default; this adapter follows the same convention for the same reason —
// two adapter instances on different namespaces never collide, even
// against the same cluster. "keyv" (not "cache-manager") is the default
// here so the two packages never silently share keyspace against the same
// cluster if an app happens to use both during a migration.
const DEFAULT_NAMESPACE = "keyv";

/**
 * Config accepted by `nanocachedKeyvStore`. Field names mirror
 * `NanocachedClientOptions` directly, same as the sibling `cache-manager`
 * v5 store's `NanocachedStoreConfig` (`secret` renamed from that type's
 * `authSecret`, everything else passed straight through).
 *
 * **`namespace` here is unrelated to Keyv's own `namespace`/`useKeyPrefix`
 * options** — see the module README's "Namespace vs. Keyv's own prefixing"
 * section. Conflating the two is the classic mistake this note exists to
 * head off.
 */
export interface NanocachedKeyvStoreConfig {
  /** Connect targets — same shape and semantics as
   * `NanocachedClientOptions.addresses`: one or more node/discovery
   * addresses, tried in order. Required; there is no default. */
  addresses: NanocachedAddress[];
  /** The nanocached namespace this store reads and writes — **not** the
   * same thing as Keyv's own `namespace` option (an in-band key-prefix
   * string Keyv applies before ever calling this store). Default
   * `"keyv"`. */
  namespace?: string;
  /** Shared secret to authenticate with (`NanocachedClientOptions.authSecret`).
   * Omit if the server has no secret configured. */
  secret?: string;
  /** See `NanocachedClientOptions.tls`. */
  tls?: boolean;
  /** See `NanocachedClientOptions.ca`. */
  ca?: string;
  /** See `NanocachedClientOptions.compress`. */
  compress?: boolean;
  /** See `NanocachedClientOptions.compressionThreshold`. */
  compressionThreshold?: number;
  /** See `NanocachedClientOptions.fireAndForgetReplicas`. */
  fireAndForgetReplicas?: boolean;
  /** See `NanocachedClientOptions.readRepair`. */
  readRepair?: boolean;
  /** See `NanocachedClientOptions.readHedgeAfterMs`. */
  readHedgeAfterMs?: number;
  /** See `NanocachedClientOptions.reconnectCooldownMs`. */
  reconnectCooldownMs?: number;
}

// Keyv's own unit is milliseconds throughout (`Keyv`'s `ttl` option and
// `.set()`'s `ttl` parameter); the wire's is whole seconds. A positive
// sub-second value rounds UP, never down to 0 ("no expiry") — same policy
// as the cache-manager v5 store's `resolveTtlSeconds`. `ttl` arrives here
// as `undefined` when the caller passed none at all, or `null` when Keyv
// itself resolved "no ttl configured anywhere" (observed directly against
// keyv 5.6.0 — its internal `set()` passes `null`, not `undefined`, in
// that case), so both are treated identically.
//
// A *negative* ttl is different from 0/null/undefined: it means "this
// entry is already expired" (issue #300) — verified against keyv 5.6.0,
// which normalizes only `ttl === 0` to `undefined` and passes negative
// ttls through untouched, relying on its own client-side
// `expires = Date.now() + ttl` (already in the past) to hide the entry.
// Mapping a negative ttl to wire TTL 0 here — as this function used to —
// would write an *immortal* entry to nanocached that Keyv's expiry check
// then hides forever: an unreclaimable server-side storage leak. So an
// explicit negative ttl short-circuits to `DO_NOT_CACHE` before the
// 0/null/undefined fallback, same policy as the cache-manager v5 store's
// `resolveTtlSeconds` and the Django adapter's `_DO_NOT_CACHE`.
const DO_NOT_CACHE = Symbol("nanocached-keyv: do not write, delete instead");

function wireTtlSeconds(ttlMs: number | null | undefined): number | typeof DO_NOT_CACHE {
  if (ttlMs !== null && ttlMs !== undefined && ttlMs < 0) return DO_NOT_CACHE;
  if (ttlMs === null || ttlMs === undefined || ttlMs <= 0) return 0;
  return Math.ceil(ttlMs / 1000);
}

/**
 * A Keyv `KeyvStoreAdapter` (keyv ^5, the version real `cache-manager` v6+
 * and `@nestjs/cache-manager` 3.x currently depend on) backed by one
 * nanocached namespace — issue #120's follow-up to `nanocached-cache-manager`
 * (issue #108/#118's sibling), which targets cache-manager v5's older
 * `Store` API instead.
 *
 * **Values pass through opaque.** Keyv itself JSON-encodes `{value,
 * expires}` before ever calling `set()`, and decodes whatever `get()`
 * returns — confirmed directly against keyv 5.6.0. This store never
 * touches that envelope; it just persists and returns the raw string
 * byte-for-byte.
 *
 * **Deliberately minimal surface — `has`/`hasMany`, `getMany`/`setMany`/
 * `deleteMany`, and `iterator` are all omitted**, not implemented and left
 * to Keyv's own built-in fallbacks. See the module README's "Honest
 * subset" section for why each omission is not just "less code" but
 * actually the *more correct* choice here — in particular, a naive `has()`
 * built on this wire is subtly wrong (it would misreport a key as present
 * past Keyv's own precise millisecond expiry, before the wire's coarser
 * whole-second TTL sweeps it), and defining `iterator` at all — even one
 * that only throws — trips a crash in Keyv's own constructor for any
 * adapter whose `opts` lacks a `url` string.
 */
export class NanocachedKeyvStore implements KeyvStoreAdapter {
  /** Required by the `KeyvStoreAdapter` type; Keyv never reads anything
   * out of it for a custom adapter beyond checking it's present (its
   * `_checkIterableAdapter` gate, which inspects `opts.url`/`opts.dialect`,
   * only runs when the adapter also defines `iterator` — this one
   * doesn't). Left empty. */
  readonly opts: Record<string, never> = {};

  /** Set by Keyv itself after construction (`new Keyv({ store })` assigns
   * `store.namespace = <keyv's own namespace option>`) — this adapter
   * never reads it, since key prefixing is entirely Keyv's own concern.
   * Present only to satisfy the `KeyvStoreAdapter` type. */
  namespace?: string;

  private readonly ns: NanocachedNamespace;

  constructor(
    /** The underlying SDK client — exposed so a caller can reach anything
     * this store doesn't itself surface (`stats()`, `replication`, a
     * second namespace via `client.namespace(...)`, etc.), same as the
     * cache-manager v5 store's `store.client`. */
    readonly client: NanocachedClient,
    namespace: string,
  ) {
    this.ns = client.namespace(namespace);
  }

  async get<Value>(key: string): Promise<Value | undefined> {
    const raw = await this.ns.get(key);
    return raw === null ? undefined : (raw as unknown as Value);
  }

  async set(key: string, value: unknown, ttl?: number | null): Promise<void> {
    const wireTtl = wireTtlSeconds(ttl);
    // Issue #300: a negative ttl means "already expired" — don't write an
    // immortal entry Keyv's own client-side expiry check will just hide
    // forever; delete whatever's there instead, so a stale value can't be
    // served.
    if (wireTtl === DO_NOT_CACHE) {
      await this.ns.delete(key);
      return;
    }
    await this.ns.set(key, value as string, wireTtl);
  }

  async delete(key: string): Promise<boolean> {
    return this.ns.delete(key);
  }

  /** Namespace `CLEAR` (issue #106) — this store's namespace only, never
   * the whole cluster, so two stores on different namespaces stay
   * isolated from each other's `clear()`. */
  async clear(): Promise<void> {
    await this.ns.clear();
  }

  /** Part of `KeyvStoreAdapter`'s `IEventEmitter` requirement — Keyv
   * conditionally calls `store.on("error", ...)` if the method exists, to
   * re-emit a store's out-of-band errors through its own `error` event.
   * This store has none: every operation already surfaces its errors
   * through the returned `Promise`'s rejection, so there is nothing to
   * emit out of band. A no-op, kept only to satisfy the type. */
  on(): this {
    return this;
  }

  /** Closes the underlying SDK client. Keyv never closes a store itself
   * (`disconnect` is this adapter's own addition, same convention as the
   * cache-manager v5 store), so a caller that owns this store's lifecycle
   * calls this itself, typically on shutdown. Idempotent, like
   * `NanocachedClient.close`. */
  async disconnect(): Promise<void> {
    await this.client.close();
  }
}

/**
 * Connects a fresh `NanocachedClient` and returns a `NanocachedKeyvStore`
 * ready to hand to `new Keyv({ store, useKeyPrefix: false })` — see the
 * module README for why `useKeyPrefix: false` is recommended (not
 * required) here.
 */
export async function nanocachedKeyvStore(config: NanocachedKeyvStoreConfig): Promise<NanocachedKeyvStore> {
  if (config.addresses === undefined) {
    throw new Error("nanocached-keyv: nanocachedKeyvStore() config requires `addresses`");
  }

  const client = await NanocachedClient.connect({
    addresses: config.addresses,
    authSecret: config.secret,
    tls: config.tls,
    ca: config.ca,
    compress: config.compress,
    compressionThreshold: config.compressionThreshold,
    fireAndForgetReplicas: config.fireAndForgetReplicas,
    readRepair: config.readRepair,
    readHedgeAfterMs: config.readHedgeAfterMs,
    reconnectCooldownMs: config.reconnectCooldownMs,
  });
  return new NanocachedKeyvStore(client, config.namespace ?? DEFAULT_NAMESPACE);
}
