import {
  NanocachedClient,
  PartialWrongNodeError,
  MAX_BATCH_KEYS,
  type NanocachedAddress,
  type NanocachedNamespace,
} from "nanocached";
import type { Config, Milliseconds, Store } from "cache-manager";

// cache-manager convention (matching its own Redis store): a store binds
// to one implicit "keyspace". nanocached's equivalent is a namespace
// (issue #105) — every NanocachedStore is scoped to exactly one, so two
// stores pointed at different namespaces never collide even against the
// same cluster, and reset() below is that one namespace's CLEAR rather
// than the whole-cluster flush.
const DEFAULT_NAMESPACE = "cache-manager";

/**
 * Config accepted by `nanocachedStore` (passed as `caching`'s second
 * argument) — cache-manager's own `Config` (just `ttl`, in milliseconds,
 * plus its refresh/isCacheable knobs) extended with what's needed to
 * reach a nanocached cluster. Field names mostly mirror
 * `NanocachedClientOptions` directly; `secret` is renamed from that
 * type's `authSecret` to match the shorter name cache-manager's own
 * stores (e.g. the Redis one) use for the equivalent setting.
 */
export interface NanocachedStoreConfig extends Config {
  /** Connect targets — same shape and semantics as
   * `NanocachedClientOptions.addresses`: one or more node/discovery
   * addresses, tried in order. Required; there is no default. */
  addresses: NanocachedAddress[];
  /** The nanocached namespace this store reads and writes. Default
   * `"cache-manager"` — not the SDK's own default (empty) namespace, so a
   * cache-manager app never silently shares keyspace with a caller using
   * the SDK directly against the same cluster. */
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

// The wire's TTL unit is whole seconds (every nanocached SDK/server
// agrees on this — see the shared adapter spec); cache-manager's is
// milliseconds throughout (Config.ttl, Store.set's ttl parameter). A
// positive sub-second value must round UP, never down to 0 ("no expiry")
// — losing a caller's intended expiry silently would be far worse than
// rounding it up to 1s. `effectiveMs` folds in the "0/absent means fall
// back to the store's configured default" rule cache-manager stores
// generally follow (ttl 0 is indistinguishable from "not passed" here,
// matching the Redis store's own handling of Milliseconds).
//
// A *negative* per-call ttl is different from 0/absent: it means "this
// entry is already expired" (issue #300), same semantic the Django
// adapter's `_DO_NOT_CACHE` gives `timeout <= 0`. Mapping it to wire TTL
// 0 — as this function used to, since `ttlMs > 0` is false for negatives
// too — would silently write an *immortal* entry instead: an
// unreclaimable server-side storage leak. So an explicit negative call
// ttl is checked first and short-circuits straight to `DO_NOT_CACHE`,
// bypassing the configured default entirely (mirroring how Django's
// `get_backend_timeout` only consults `default_timeout` when the caller
// passed nothing at all, not when it passed an explicit negative).
//
// Issue #333: the same leak existed one level up — a *negative*
// `defaultTtlMs` (the constructor's configured default, `Config.ttl`)
// used to fall straight through to `effectiveMs <= 0` and return `0`,
// i.e. "no expiry", the exact silent-immortal-entry bug #300 fixed for
// the per-call case, just missed for the configured-default case. There
// is no principled reason a negative default should mean something
// different from a negative per-call value — both are "already expired"
// — so `effectiveMs` (whichever of the two won) is now checked for
// negativity the same way, after the per-call short-circuit above.
const DO_NOT_CACHE = Symbol("nanocached-cache-manager: do not write, delete instead");

function resolveTtlSeconds(
  ttlMs: Milliseconds | undefined,
  defaultTtlMs: Milliseconds | undefined,
): number | typeof DO_NOT_CACHE {
  if (ttlMs !== undefined && ttlMs < 0) return DO_NOT_CACHE;
  const effectiveMs = ttlMs !== undefined && ttlMs > 0 ? ttlMs : defaultTtlMs;
  if (effectiveMs === undefined) return 0;
  if (effectiveMs < 0) return DO_NOT_CACHE;
  if (effectiveMs === 0) return 0;
  return Math.ceil(effectiveMs / 1000);
}

/** Thrown by `keys()` — see `NanocachedStore` for why. */
export class NotSupportedError extends Error {
  constructor(operation: string) {
    super(
      `nanocached-cache-manager: ${operation}() is not supported — the nanocached wire protocol has no key-enumeration ` +
        "or TTL-readback operation (a node only ever answers get/set/delete/clear for a specific key), so there is no " +
        "request this store could send to implement it.",
    );
    this.name = "NotSupportedError";
  }
}

/**
 * A cache-manager v5 `Store` backed by a nanocached namespace. Returned by
 * `nanocachedStore` (the factory `caching()` expects); see that function
 * and the module README for how the two fit together.
 *
 * Values are JSON-serialized (`JSON.stringify`/`JSON.parse`) — the
 * convention cache-manager's own stores use (its Redis store does the
 * same) so any JSON-safe value round-trips, `null` included. Storing
 * `undefined` is a no-op, matching the Redis store's behavior, since
 * `undefined` is exactly what a miss already means here — there is
 * nothing meaningful to write.
 */
export class NanocachedStore implements Store {
  private readonly ns: NanocachedNamespace;

  constructor(
    /** The underlying SDK client — exposed so a caller can reach anything
     * this store doesn't itself surface (`stats()`, `replication`, a
     * second namespace via `client.namespace(...)`, etc.), same as the
     * Redis store's `store.client`. */
    readonly client: NanocachedClient,
    namespace: string,
    private readonly defaultTtlMs: Milliseconds | undefined,
  ) {
    this.ns = client.namespace(namespace);
  }

  async get<T>(key: string): Promise<T | undefined> {
    const raw = await this.ns.get(key);
    return raw === null ? undefined : (JSON.parse(raw) as T);
  }

  async set<T>(key: string, data: T, ttl?: Milliseconds): Promise<void> {
    // A no-op, not a delete: cache-manager's Store.set is never asked to
    // remove a key (that's del()), and undefined is already what a miss
    // means for this store — writing it would be indistinguishable from
    // writing nothing (matching the Redis store's own convention).
    if (data === undefined) return;
    const wireTtl = resolveTtlSeconds(ttl, this.defaultTtlMs);
    // Issue #300: a negative ttl means "already expired" — don't write an
    // immortal entry the framework will just keep hiding forever; delete
    // whatever's there instead, so a stale value can't be served.
    if (wireTtl === DO_NOT_CACHE) {
      await this.ns.delete(key);
      return;
    }
    // Issue #333: `JSON.stringify` doesn't throw for a function, a
    // `Symbol`, or another top-level non-serializable value — it just
    // returns the *value* `undefined` (not the string "undefined"). Left
    // unchecked, that `undefined` would reach `ns.set` and produce a
    // confusing raw error deep inside the SDK instead of a clear one at
    // this store's own boundary, right where the actual mistake (the
    // caller's value shape) is.
    const serialized = JSON.stringify(data);
    if (serialized === undefined) {
      throw new TypeError(
        `nanocached-cache-manager: set(${JSON.stringify(key)}) value could not be JSON-serialized — ` +
          "JSON.stringify() returned undefined, which usually means the value is a function, a Symbol, " +
          "or another type JSON has no representation for.",
      );
    }
    await this.ns.set(key, serialized, wireTtl);
  }

  async del(key: string): Promise<void> {
    await this.ns.delete(key);
  }

  /** Namespace CLEAR (issue #106) — this store's namespace only, never
   * the whole cluster (`NanocachedClient.clearAll`), so two stores on
   * different namespaces stay isolated from each other's reset(). */
  async reset(): Promise<void> {
    await this.ns.clear();
  }

  /** One wire round trip per involved node (issue #152), via the SDK's
   * `getMany` — vs. a `get` per key. Missing keys come back as
   * `undefined` holes, exactly like individual `get` misses; array order
   * matches the input `keys` order regardless of the returned Map's
   * iteration order. */
  async mget(...keys: string[]): Promise<unknown[]> {
    if (keys.length === 0) return [];
    const raw = await this.mgetResolved(keys);
    return keys.map((key) => {
      const value = raw.get(key);
      return value === undefined ? undefined : (JSON.parse(value) as unknown);
    });
  }

  /** `getMany`, but resolving a ring reconfiguration mid-batch itself
   * (issue #416) instead of discarding an otherwise-successful batch.
   *
   * `ns.getMany` already does one bounded refresh-and-retry internally
   * per key before giving up on it (the SDK's own `multiGetPass`); a
   * batch that's STILL got some keys routed to the wrong node after that
   * throws `PartialWrongNodeError` instead of returning, with
   * `.partialValues` holding every key that resolved (hit) before the
   * unresolved ones were hit. Without this, that error would propagate
   * straight out of `mget` and throw away every value the batch DID
   * manage to fetch.
   *
   * `.partialValues` doesn't distinguish "genuine miss" from "still
   * wrong node" — both are simply absent from the map — so the keys
   * retried here are a superset of the ones that actually need
   * re-routing; retrying a genuine miss again is harmless, just an extra
   * round trip for it. If the retry itself still can't place every key
   * (another concurrent reconfiguration), this gives up after that one
   * retry and merges whatever it got rather than looping or throwing —
   * any keys still unresolved come back as ordinary misses (`undefined`
   * from the caller's perspective), same as if they'd never been in the
   * cache. */
  private async mgetResolved(keys: string[]): Promise<Map<string, string>> {
    try {
      return await this.ns.getMany(keys);
    } catch (error) {
      if (!(error instanceof PartialWrongNodeError)) throw error;
      const succeeded = error.partialValues as Map<string, string>;
      const stillNeeded = keys.filter((key) => !succeeded.has(key));
      if (stillNeeded.length === 0) return succeeded;
      try {
        const retried = await this.ns.getMany(stillNeeded);
        return new Map([...succeeded, ...retried]);
      } catch (retryError) {
        if (!(retryError instanceof PartialWrongNodeError)) throw retryError;
        return new Map([...succeeded, ...(retryError.partialValues as Map<string, string>)]);
      }
    }
  }

  /** One wire round trip per involved node (issue #152), via the SDK's
   * `setMany` — vs. a `set` per key. `ttl` (if given) applies to every
   * entry, matching `setMany`'s own single-TTL-per-call signature.
   * `undefined` values are skipped, same no-op convention as `set`. */
  async mset(entries: Array<[string, unknown]>, ttl?: Milliseconds): Promise<void> {
    const wireTtl = resolveTtlSeconds(ttl, this.defaultTtlMs);
    // Issue #300: same "already expired" handling as set() — one ttl
    // applies to the whole batch (mset's own signature), so a negative
    // ttl deletes every entry the call would otherwise have written.
    if (wireTtl === DO_NOT_CACHE) {
      const keys = entries.filter(([, value]) => value !== undefined).map(([key]) => key);
      if (keys.length === 0) return;
      await this.mdel(...keys);
      return;
    }
    const values: Record<string, string> = {};
    for (const [key, value] of entries) {
      if (value === undefined) continue;
      // Issue #333: same JSON.stringify-can-return-undefined guard as set().
      const serialized = JSON.stringify(value);
      if (serialized === undefined) {
        throw new TypeError(
          `nanocached-cache-manager: mset() value for key ${JSON.stringify(key)} could not be JSON-serialized — ` +
            "JSON.stringify() returned undefined, which usually means the value is a function, a Symbol, " +
            "or another type JSON has no representation for.",
        );
      }
      values[key] = serialized;
    }
    if (Object.keys(values).length === 0) return;
    await this.ns.setMany(values, wireTtl);
  }

  /** Client-side loop over `del`, concurrently — chunked at
   * `MAX_BATCH_KEYS` (issue #416), the same bound `mget`/`mset` get for
   * free from the SDK's own `getMany`/`setMany` chunking. There's no
   * bulk-delete wire op (unlike `getMany`/`setMany`'s `m`/`o`), so a
   * single `Promise.all` here would fan out one concurrent `d` request
   * per key with no bound at all for an arbitrarily large batch; this
   * instead runs at most `MAX_BATCH_KEYS` deletes concurrently at a
   * time, chunk by chunk. */
  async mdel(...keys: string[]): Promise<void> {
    for (let start = 0; start < keys.length; start += MAX_BATCH_KEYS) {
      const chunk = keys.slice(start, start + MAX_BATCH_KEYS);
      await Promise.all(chunk.map((key) => this.del(key)));
    }
  }

  /** The wire has no way to enumerate a namespace's keys — a node only
   * ever answers about one key at a time. Always throws; documented here
   * and in the README rather than silently returning `[]`, which would
   * read as "this namespace is empty" instead of "this store can't
   * answer that". */
  async keys(_pattern?: string): Promise<string[]> {
    throw new NotSupportedError("keys");
  }

  /** The wire has no TTL-readback operation (a `V` response carries the
   * value, never its remaining expiry) — same reasoning as `keys()`.
   * Unlike `keys()`, though, this one is a required `Store` member that
   * `cache-manager`'s own `wrap()` calls *unconditionally and uncaught*
   * whenever a `refreshThreshold` is configured (issue #274) — throwing
   * here would turn every cache hit into a thrown error the moment a
   * caller opts into refresh-ahead, which is worse than just not
   * supporting refresh-ahead.
   *
   * So this always answers `-1` — the exact sentinel `wrap()` already
   * treats as "unknown/no TTL" (`remainingTtl !== -1 && remainingTtl <
   * refreshThresholdConfig`, cache-manager's own `caching.js`): with `-1`
   * that condition is always false, so the background-refresh branch
   * never runs, for any `refreshThreshold` value. Net effect: refresh-ahead
   * silently degrades to plain read-through caching instead of crashing —
   * documented in the README under "Semantics". */
  async ttl(_key: string): Promise<number> {
    return -1;
  }

  /** Closes the underlying SDK client. cache-manager never closes a
   * store itself (there's no `Store.close`/`Store.disconnect` in its own
   * type), so — matching the Redis store's convention — callers that own
   * this store's lifecycle call this themselves, typically on shutdown.
   * Idempotent: see `NanocachedClient.close`. */
  async disconnect(): Promise<void> {
    await this.client.close();
  }
}

/**
 * The `cache-manager` v5 store factory for nanocached — pass this
 * directly as `caching()`'s first argument:
 *
 * ```ts
 * import { caching } from "cache-manager";
 * import { nanocachedStore } from "nanocached-cache-manager";
 *
 * const cache = await caching(nanocachedStore, {
 *   addresses: [{ host: "10.0.0.1", port: 8357 }],
 *   namespace: "cache-manager", // default
 *   ttl: 60_000,                // default TTL, milliseconds
 * });
 * ```
 *
 * Connects a fresh `NanocachedClient` every call — matching how
 * cache-manager's own store factories work (e.g. its Redis store dials
 * its own client rather than reusing one the caller already has), so
 * `store.disconnect()`/`client.close()` is this call's own to close.
 */
export async function nanocachedStore(config?: NanocachedStoreConfig): Promise<NanocachedStore> {
  // `config` is optional only so this function's type matches
  // cache-manager's `FactoryStore<S, T>` (`(config?: FactoryConfig<T>) =>
  // ...` — a factory called with zero arguments is a real case cache-
  // manager's own type supports, e.g. `caching(nanocachedStore)`), not
  // because `addresses` is actually optional — there is no sensible
  // default target to connect to, so its absence is a caller error,
  // checked here instead of surfacing as a less obvious failure from
  // deep inside NanocachedClient.connect (which defaults a missing/empty
  // list to its own generic "needs a non-empty addresses list" message).
  if (config?.addresses === undefined) {
    throw new Error("nanocached-cache-manager: nanocachedStore() config requires `addresses`");
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
  return new NanocachedStore(client, config.namespace ?? DEFAULT_NAMESPACE, config.ttl);
}
