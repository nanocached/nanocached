import { readFileSync } from "node:fs";
import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { Connection, ConnectionLostError, CounterOutOfRangeError, isConnectionError, WrongNodeError } from "./connection.js";
import { connectAndIdentify, connectAndListProxies, type DiscoveredNode } from "./identify.js";
import { HashRing } from "./hashRing.js";
import { compressValue, decompressValue } from "./compression.js";
import { NanocachedError } from "./errors.js";
import {
  checkKey,
  checkKeyAndValue,
  contentDigest,
  EMPTY_NAMESPACE,
  MAX_REQUEST_BYTES,
  multiGetEntryCost,
  multiSetEntryCost,
  MULTI_FRAME_HEADER_SLACK,
  type CasCondition,
  type MultiAckEntry,
  type MultiEntry,
} from "./protocol.js";

export { ConnectionLostError, CounterOutOfRangeError, NotNumericError, RetryableError, WrongNodeError } from "./connection.js";
export { NanocachedError } from "./errors.js";
export { DecompressionError } from "./compression.js";
export { contentDigest } from "./protocol.js";

// A value decoded by get() must be exactly what set() would have encoded —
// no silent U+FFFD replacement for bytes that aren't valid UTF-8. A single
// shared instance is fine: decode() carries no state across calls unless
// `stream: true` is passed, which this SDK never does.
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

/** Thrown by get/set/delete when called after close(). Not thrown by
 * close() itself, which is idempotent (see NanocachedClient.close). */
export class AlreadyClosedError extends NanocachedError {
  constructor() {
    super("nanocached: this client is closed");
    this.name = "AlreadyClosedError";
  }
}

/** Thrown by getMany/getManyBytes (issues #128/#150/#151) when some
 * keys are still wrong-node after the one bounded refresh-and-retry
 * every batch gets (the per-key analogue of get/set's own `W`
 * refresh-and-retry) — a subclass of WrongNodeError, so existing
 * `catch (WrongNodeError)` handling keeps working unchanged.
 * `partialValues` holds every key that DID resolve (a `Map<string,
 * Buffer>` from getManyBytes, `Map<string, string>` from getMany) —
 * a batch never fails as a whole (docs/protocol.html#multi), so a
 * handful of stale placements shouldn't force discarding an otherwise
 * successful batch. setMany/setManyBytes have nothing to return on
 * success, so they just throw a plain WrongNodeError on the same
 * condition — there's no partial payload worth attaching. */
export class PartialWrongNodeError<T = unknown> extends WrongNodeError {
  constructor(public readonly partialValues: T) {
    super();
    this.name = "PartialWrongNodeError";
  }
}

// Constructors that only ever show up at a by-design swallow site because
// of an actual bug in this SDK's own code (or in a caller's arguments,
// e.g. an invalid ttlSeconds surfacing from encodeSet deep inside a
// replica leg) — never something nanocached deliberately throws on
// purpose, which is always NanocachedError or one of its subclasses
// (WrongNodeError, ConnectionLostError, DiscoveryBusyError,
// AlreadyClosedError). isSwallowable below uses this as the discriminator
// instead of an allowlist of "expected" error classes, since the set of
// plain Errors a flaky discovery/node can legitimately produce during a
// refresh (a stale auth secret, a malformed response, …) is open-ended
// and all of it must stay swallowed, exactly as it is today.
const PROGRAMMING_ERROR_CONSTRUCTORS = [TypeError, RangeError, ReferenceError, SyntaxError, EvalError, URIError];

/** Whether `error` is safe for a by-design swallow site (replica-leg
 * writes, read repair, node-list refresh — see stats()/ClientStats) to
 * absorb. Everything except PROGRAMMING_ERROR_CONSTRUCTORS is: those
 * indicate an actual bug, which must propagate instead of vanishing
 * identically to a dead replica or a stuck refresh. */
function isSwallowable(error: unknown): boolean {
  return !PROGRAMMING_ERROR_CONSTRUCTORS.some((ctor) => error instanceof ctor);
}

/** Snapshot of counters for failures this client swallows by design
 * (client-side replication / fire-and-forget replica writes / read repair) instead of raising them to a caller — observability
 * for silently degrading replication or a stuck node-list refresh, which
 * would otherwise be invisible. See NanocachedClient.stats(). */
export interface ClientStats {
  /** Replica-leg write failures swallowed during a cluster write
   * (writeToOwners), whether the leg ran synchronously or as a
   * fireAndForgetReplicas background write (client-side replication,
   * Fire-and-forget replica writes). */
  replicaWriteFailures: number;
  /** Failures swallowed while probing owners or writing back the repaired
   * value during read repair (read repair). */
  readRepairFailures: number;
  /** Node-list refresh attempts that failed, and per-node connect
   * failures swallowed while reconciling a refresh's member list
   * (refreshNodeList/fetchNodeList) — discovery outages degrade only
   * topology updates, never already-established cache traffic. */
  refreshFailures: number;
  /** Retryable-error status (issue #125): every `R` this client has
   * received on any data command (`G`/`S`/`D`/`g`/`s`/`d`/`c`/`F`),
   * whether the transparent, bounded retry that followed it ultimately
   * succeeded or exhausted into a `RetryableError`. Today only
   * `nanocached-proxy` emits `R` — for a request whose upstream node was
   * briefly unreachable and survived its own refresh-and-retry — but this
   * counts it on any connection, since the SDK handles `R` uniformly
   * regardless of what's on the other end. */
  transientRetries: number;
}

export interface NanocachedAddress {
  host: string;
  port: number;
}

export interface NanocachedClientOptions {
  /** Connect targets: one or more nanocached-node or nanocached-discovery
   * addresses (discovery HA), tried in order. A one-element list is the
   * single-target case — there is no separate host/port shorthand. Both
   * the initial connect and every later node-list refresh walk this list
   * until one provides a node list, so losing any one discovery replica
   * costs nothing. An address that answers `B` (still inside its startup
   * grace after a restart) is skipped the same way as an unreachable
   * one. */
  addresses: NanocachedAddress[];
  /** SDK proxy mode (issue #122): connect through one `nanocached-proxy`
   * instead of routing directly to the cluster. Only meaningful when
   * every configured address is a discovery server — if the first one
   * reached identifies as a cache node instead, `connect()` fails fast
   * with a clear error, since proxy mode has no direct-node fallback.
   * Once connected, this client is in the same single-connection mode a
   * lone node address puts it in: no ring view, no per-node connections,
   * and no hedged reads — `readHedgeAfterMs` is inert here, since a proxy
   * connection has no replicas to hedge to. Namespaces, clear/clearAll,
   * tags, keep-alive, and compression all work unchanged over that one
   * connection. The proxy is chosen at random from discovery's roster
   * (spreading a fleet of clients across proxies), with random failover
   * through the rest if the chosen one is unreachable; on a later
   * connection loss, the same proxy is retried first (it may have simply
   * restarted) before the roster is re-fetched and another is picked.
   * Off by default. */
  viaProxy?: boolean;
  /** Shared secret to authenticate with, matching NANOCACHED_AUTH_SECRET
   * on the server. Omit if the server has no secret configured. */
  authSecret?: string;
  /** Connect over TLS instead of plaintext — required if the server was
   * started with --tls-cert/--tls-key. `boolean`, not just the literal
   * `true`, so a single config value (e.g. an env var) can toggle this
   * across environments without an `x ? true : undefined` workaround. */
  tls?: boolean;
  /** Path to a PEM file of trusted root certificate(s) to use instead of
   * Node's default, publicly-trusted CA store — for a server running a
   * self-signed certificate with no CA-issued alternative available.
   * Read once (synchronously) inside `connect()` and reused for every
   * dial this client ever makes, including reconnects and node-list
   * refreshes. Only meaningful when `tls` is true; silently ignored
   * otherwise. An unreadable or unparseable file is a connect-time
   * error. */
  ca?: string;
  /** Transparently compress values above `compressionThreshold` on `set`
   * and decompress them on `get`/`getBytes` (value compression). Off by
   * default. **Every client that reads or writes a given set of keys must
   * agree on this setting** — it is a per-keyspace format decision, not a
   * per-client preference; take care before enabling
   * this against an existing keyspace another client may still touch
   * with `compress` off. */
  compress?: boolean;
  /** Values shorter than this (in bytes) are never compressed — the
   * per-value overhead of attempting it outweighs the savings. Only
   * meaningful when `compress` is true. Default 256. */
  compressionThreshold?: number;
  /** Let `set`/`delete` return as soon as the primary owner acks,
   * letting replica legs finish in the background instead of waiting
   * for them too (fire-and-forget replica writes). Off by default. Unlike `compress`,
   * this is a pure latency/durability trade for this client's own
   * writes — it carries no wire format and needs no agreement with other
   * clients. */
  fireAndForgetReplicas?: boolean;
  /** On a clean miss (the key's first-reached owner reports it missing),
   * probe the remaining owners before accepting that, and repair the
   * primary in the background if one still has the value
   * (read repair). Off by default. Costs extra reads only on the
   * misses this actually applies to. */
  readRepair?: boolean;
  /** Hedged reads (issue #64): if the primary owner hasn't answered a read
   * within this many milliseconds, the same read is also sent to the next
   * owner — and so on, one more owner per interval, until every owner is
   * in flight. The first answer decides: a hit from any owner is final; a
   * miss is final only from the primary (a replica's miss is provisional,
   * since it may simply lack the copy); a failure hedges onward
   * immediately. Undefined (the default) turns this off — reads then use
   * the plain sequential path in `readFromOwners`, which only moves past
   * an owner that has *failed*, so one slow-but-alive owner bounds every
   * read that touches it at its full round trip. Only applies when the
   * key has at least 2 owners (`replication >= 2`); with a single copy
   * there is nobody to hedge to. Must be a positive number when set — a
   * non-positive value is rejected at `connect()`. Inert under `viaProxy`
   * (issue #122): a proxy connection is single-connection mode, so there
   * is never a second owner to hedge to either. */
  readHedgeAfterMs?: number;
  /** How long, after a reconnect dial to an address fails, that address is
   * treated as still down — a request routed to it during this window
   * fails immediately with the original dial error instead of paying
   * another full `CONNECT_DEADLINE_MS` (5s, socket.ts) redialing an
   * address that just proved unreachable. Default `DEFAULT_RECONNECT_COOLDOWN_MS`
   * (1s). Keep well under `NODE_LIST_STALE_AFTER_MS` so a node that
   * genuinely recovers isn't shut out for long. */
  reconnectCooldownMs?: number;
}

// See `NanocachedClientOptions.reconnectCooldownMs`.
const DEFAULT_RECONNECT_COOLDOWN_MS = 1_000;

const DEFAULT_COMPRESSION_THRESHOLD = 256;

// TTL a read-repair write uses (read repair), in whole seconds —
// the protocol's TTL unit throughout (see encodeSet in protocol.ts). The
// original TTL isn't recoverable from a GET response, and repairing with
// TTL 0 (no expiry) would permanently resurrect data that was legitimately
// expiring; 60s bounds the overshoot instead — an immortal key just gets
// re-repaired on a later miss. Cross-SDK policy decision, applied
// identically across all SDKs.
const READ_REPAIR_TTL_SECONDS = 60;

/** Bounds how many replica writes a single client may have running in
 * the background at once when `fireAndForgetReplicas` is enabled
 * (fire-and-forget replica writes) — once the cap is reached, further replica legs
 * fall back to running synchronously, the same as with the option off.
 * A mutable object only so tests can shrink it, mirroring
 * KEEPALIVE_TUNING. */
export const FIRE_AND_FORGET_TUNING = { maxInFlight: 32 };

/** Keep-alive is always on and internal (issue #27): every interval, a
 * lightweight request goes out on each connection real traffic has left
 * idle for at least that long, so the server's 60s idle timeout never
 * severs a healthy client. Half the idle timeout by design; exported as
 * a mutable object only so tests can shorten it. */
export const KEEPALIVE_TUNING = { intervalMs: 30_000 };

function splitHostPort(address: string): { host: string; port: number } {
  const separator = address.lastIndexOf(":");
  if (separator === -1) {
    throw new NanocachedError(`nanocached: invalid node address from discovery server: ${address}`);
  }

  const host = address.slice(0, separator);
  const port = Number(address.slice(separator + 1));
  // Must be a valid TCP port (0-65535, matching the Python SDK's
  // split_host_port) — a bogus value like "999999" or "-1" would
  // otherwise reach net.connect/tls.connect and throw a synchronous
  // RangeError there instead, which — being a programming-error
  // constructor, see PROGRAMMING_ERROR_CONSTRUCTORS/isSwallowable — is
  // not swallowable and would escape refreshNodeList() into the
  // caller's get/set/delete, breaking "refresh never throws to the
  // caller". Throwing NanocachedError here instead keeps this a
  // by-design swallow, exactly like every other malformed-address case
  // above.
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new NanocachedError(`nanocached: invalid node address from discovery server: ${address}`);
  }

  return { host, port };
}

interface ClusterMember {
  /** Last-known address for this node name — kept so a connection the
   * server closed (e.g. its 60s idle timeout) can be reopened lazily on
   * the next request that routes here, without waiting for a node-list
   * refresh. */
  address: string;
  /** `null` for a member that was listed by discovery but unreachable
   * when this client bootstrapped (issue #67): it stays routable — a
   * request for one of its keys fails over the same way it would after a
   * mid-life node death — and the next request after the reconnect
   * cooldown redials it (ensureConnected/memberConnection). */
  connection: Connection | null;
}

type Target =
  | { kind: "single"; connection: Connection }
  // SDK proxy mode (issue #122, `viaProxy`): a single connection to one
  // `nanocached-proxy`, exactly like `"single"` except `address` is
  // mutable — a `refreshProxyTarget` can swap it to a different proxy
  // after a reconnect-time `Q` re-fetch, unlike a plain node target's
  // fixed `NanocachedClient.url`.
  | { kind: "proxy"; connection: Connection; address: string }
  // `members` is keyed by node *name* (node identity decoupled from address), matching what
  // `ring.owners()` returns — not by address, which carries no identity
  // meaning and is only used to open connections. `replication` is
  // discovery's R (client-side replication), learned from the same `L` response as the
  // member list.
  | { kind: "cluster"; ring: HashRing; members: Map<string, ClusterMember>; replication: number };

function targetKey(options: { host: string; port: number }): string {
  return `${options.host}:${options.port}`;
}

/** Fisher-Yates over a copy of `items` — never mutates its argument.
 * Backs SDK proxy mode's random proxy selection (issue #122): spreading a
 * fleet of fresh clients across a discovery server's proxy roster, and
 * failing over through the rest in random order (rather than roster
 * order) when the first pick turns out to be down. */
function shuffled<T>(items: readonly T[]): T[] {
  const result = [...items];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

// How long a cluster client's node list may go without being re-fetched
// from discovery before get/set/delete refreshes it first. Checked lazily
// on use rather than on a timer — see NanocachedClient.maybeRefreshNodeList.
const NODE_LIST_STALE_AFTER_MS = 30_000;

// Batch chunking (issues #128/#150/#151): NanocachedClient never sends
// more than this many keys in one `m`/`o` sub-frame — larger batches
// transparently become more than one sub-frame per owner, invisible to
// callers. Sized against protocol.ts's MAX_MULTI_HEADER_LENGTH (see its
// own derivation comment) — matches the Go SDK's identically-derived
// maxBatchKeys (sdk/go/client.go). Exported only so tests can assert
// against it directly, mirroring FIRE_AND_FORGET_TUNING/KEEPALIVE_TUNING.
//
// A key count bound alone isn't enough, though (issue #222): MAX_BATCH_KEYS
// individually-valid pairs can still sum past the server's own per-request
// cap (a 400-key batch of 5 KiB values is nowhere near 400 keys but is
// well over 1 MiB). nextChunkEnd below adds the missing cumulative-bytes
// bound on top of this one.
export const MAX_BATCH_KEYS = 400;

// Batch chunking's byte bound (issue #222): finds the largest run starting
// at `start` — at most MAX_BATCH_KEYS entries — whose namespace, plus
// protocol.ts's own MULTI_FRAME_HEADER_SLACK, plus per-entry wire cost
// (`entryBytes` — protocol.ts's multiGetEntryCost for `m`, multiSetEntryCost
// for `o`, both already honest about the header field(s) each entry adds,
// not just its key/value bytes) still fits protocol.ts's MAX_REQUEST_BYTES.
// This mirrors encodeMultiGet/encodeMultiSet's own "total" bound exactly,
// entry cost and header slack alike, so a chunk built this way can never
// trip the encoder's RangeError. A single entry always fits by itself —
// checkKey/checkKeyAndValue already validated every entry eagerly, before
// chunking ever starts (see getManyBytesInNamespace/setManyInNamespace) —
// so a run is never empty and this always makes progress.
function nextChunkEnd(namespace: Uint8Array, count: number, start: number, entryBytes: (index: number) => number): number {
  let end = start;
  let total = namespace.length + MULTI_FRAME_HEADER_SLACK;
  while (end < count && end - start < MAX_BATCH_KEYS) {
    const next = total + entryBytes(end);
    if (end > start && next > MAX_REQUEST_BYTES) break;
    total = next;
    end++;
  }
  return end;
}

// Tracks, per connect() target (not per instance — there's no `close()` yet
// to hook into), how many live sockets are still open for it. Purely a
// programming-error guard: catches "connect() called again for the same
// target before the previous one was ever released" without affecting
// behavior — connecting again still works, this only warns. Cleared via
// each socket's own native "close" event, so it needs no cooperation from
// a public close() method; whatever eventually destroys these sockets
// (including a future close()) already fires that event.
const openTargets = new Map<string, number>();

function trackOpenTarget(key: string, sockets: Array<Socket | TLSSocket>): void {
  openTargets.set(key, (openTargets.get(key) ?? 0) + sockets.length);

  for (const socket of sockets) {
    socket.once("close", () => {
      const remaining = (openTargets.get(key) ?? 1) - 1;
      if (remaining <= 0) {
        openTargets.delete(key);
      } else {
        openTargets.set(key, remaining);
      }
    });
  }
}

/** Outcome of dialing one node discovery listed, during cluster bootstrap
 * (issue #67). `"ok"` and `"unreachable"` are both tolerated — see
 * `NanocachedClient.connect`'s cluster branch; `"hard"` (a listed address
 * that identifies as something other than a node, or an actual
 * programming bug surfacing from the dial) aborts connect() outright,
 * exactly as it always has. */
type ClusterDialOutcome =
  | { node: DiscoveredNode; kind: "ok"; socket: Socket | TLSSocket; tagged: boolean }
  | { node: DiscoveredNode; kind: "unreachable"; error: Error }
  | { node: DiscoveredNode; kind: "hard"; error: Error };

/** Dials one node from discovery's list. Never rejects — every outcome,
 * including a hard failure, comes back as a `ClusterDialOutcome` so
 * `connect()` can run every dial concurrently with a plain `Promise.all`
 * and still tell "no one home yet" (tolerated) apart from "something is
 * actually wrong" (isSwallowable — a real programming bug, or a listed
 * address that isn't a node at all) without losing track of sockets
 * already opened by other concurrent dials. */
async function dialClusterNode(
  node: DiscoveredNode,
  authSecret: string | undefined,
  tls: boolean | undefined,
  ca: Buffer | undefined,
): Promise<ClusterDialOutcome> {
  const { host, port } = splitHostPort(node.address);

  let identified;
  try {
    identified = await connectAndIdentify({ host, port, authSecret, tls, ca });
  } catch (error) {
    if (!isSwallowable(error)) return { node, kind: "hard", error: error as Error };
    // Connection-level failure (issue #67): typically a node that just
    // died and discovery hasn't evicted yet — its liveness window is
    // seconds long, and every key is still served by another owner when
    // replication >= 2. Tolerated here; installed as a connectionless
    // member by the caller.
    return { node, kind: "unreachable", error: error as Error };
  }

  if (identified.kind !== "node") {
    return {
      node,
      kind: "hard",
      error: new NanocachedError(`nanocached: discovery server returned a non-node address: ${node.address}`),
    };
  }

  return { node, kind: "ok", socket: identified.socket, tagged: identified.tagged };
}

/** The forwarding operations `NanocachedNamespace` delegates to — bound
 * closures over one `NanocachedClient` instance and one namespace, built by
 * `NanocachedClient.namespace()`. Kept as a plain object of closures
 * (rather than handing the handle the client instance itself) so the
 * handle can only ever reach the client through this narrow, namespace-
 * scoped surface — never anything else `NanocachedClient` exposes. */
interface NamespaceOps {
  get(key: string | Uint8Array): Promise<string | null>;
  getBytes(key: string | Uint8Array): Promise<Buffer | null>;
  getWithToken(key: string | Uint8Array): Promise<{ value: Buffer; token: string } | null>;
  set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds: number): Promise<void>;
  delete(key: string | Uint8Array): Promise<boolean>;
  getMany(keys: readonly string[]): Promise<Map<string, string>>;
  getManyBytes(keys: readonly string[]): Promise<Map<string, Buffer>>;
  setMany(values: Record<string, string>, ttlSeconds: number): Promise<void>;
  setManyBytes(values: Record<string, Uint8Array>, ttlSeconds: number): Promise<void>;
  clear(): Promise<void>;
  incr(key: string | Uint8Array, delta: number): Promise<number | null>;
  decr(key: string | Uint8Array, delta: number): Promise<number | null>;
  putIfAbsent(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds: number): Promise<boolean>;
  replaceIfPresent(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds: number): Promise<boolean>;
  replace(key: string | Uint8Array, token: string, newValue: string | Uint8Array, ttlSeconds: number): Promise<boolean>;
  deleteIfMatches(key: string | Uint8Array, token: string): Promise<boolean>;
}

/**
 * A lightweight handle scoped to one namespace (first-class namespaces,
 * issue #105), returned by `NanocachedClient.namespace(ns)`. Exposes the
 * same data operations the client itself does, with identical semantics —
 * routing (HRW over `(namespace, key)`), replication fan-out, hedged
 * reads, `W` refresh-and-retry, response tags, compression, error types —
 * because every method here just forwards to the client's own internal
 * (namespace, key)-taking machinery instead of duplicating any of its
 * networking. Cheap to create, shares the client's connections, and — like
 * every client method — throws `AlreadyClosedError` once the client is
 * closed.
 */
export class NanocachedNamespace {
  /** The raw namespace bytes this handle addresses (a UTF-8-encoded
   * string is stored this way too) — useful for the framework adapters
   * built on top of this (#107/#108), which need to name a namespace back
   * to its caller. */
  readonly namespace: Buffer;

  constructor(namespace: Buffer, private readonly ops: NamespaceOps) {
    this.namespace = namespace;
  }

  /** See `NanocachedClient.get`. */
  get(key: string | Uint8Array): Promise<string | null> {
    return this.ops.get(key);
  }

  /** See `NanocachedClient.getBytes`. */
  getBytes(key: string | Uint8Array): Promise<Buffer | null> {
    return this.ops.getBytes(key);
  }

  /** See `NanocachedClient.set`. */
  set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<void> {
    return this.ops.set(key, value, ttlSeconds);
  }

  /** See `NanocachedClient.delete`. */
  delete(key: string | Uint8Array): Promise<boolean> {
    return this.ops.delete(key);
  }

  /** See `NanocachedClient.getMany`. */
  getMany(keys: readonly string[]): Promise<Map<string, string>> {
    return this.ops.getMany(keys);
  }

  /** See `NanocachedClient.getManyBytes`. */
  getManyBytes(keys: readonly string[]): Promise<Map<string, Buffer>> {
    return this.ops.getManyBytes(keys);
  }

  /** See `NanocachedClient.setMany`. */
  setMany(values: Record<string, string>, ttlSeconds = 0): Promise<void> {
    return this.ops.setMany(values, ttlSeconds);
  }

  /** See `NanocachedClient.setManyBytes`. */
  setManyBytes(values: Record<string, Uint8Array>, ttlSeconds = 0): Promise<void> {
    return this.ops.setManyBytes(values, ttlSeconds);
  }

  /** Clears this namespace (issue #106) — every entry in it, on every
   * node. See `NanocachedClient.clearAll` to flush every namespace
   * instead, and `NanocachedClient`'s `fanoutClear` for the underlying
   * fan-out/retry mechanics this forwards to. */
  clear(): Promise<void> {
    return this.ops.clear();
  }

  /** See `NanocachedClient.incr`. */
  incr(key: string | Uint8Array, delta = 1): Promise<number | null> {
    return this.ops.incr(key, delta);
  }

  /** See `NanocachedClient.decr`. */
  decr(key: string | Uint8Array, delta = 1): Promise<number | null> {
    return this.ops.decr(key, delta);
  }

  /** See `NanocachedClient.getWithToken`. */
  getWithToken(key: string | Uint8Array): Promise<{ value: Buffer; token: string } | null> {
    return this.ops.getWithToken(key);
  }

  /** See `NanocachedClient.putIfAbsent`. */
  putIfAbsent(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<boolean> {
    return this.ops.putIfAbsent(key, value, ttlSeconds);
  }

  /** See `NanocachedClient.replaceIfPresent`. */
  replaceIfPresent(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<boolean> {
    return this.ops.replaceIfPresent(key, value, ttlSeconds);
  }

  /** See `NanocachedClient.replace`. */
  replace(key: string | Uint8Array, token: string, newValue: string | Uint8Array, ttlSeconds = 0): Promise<boolean> {
    return this.ops.replace(key, token, newValue, ttlSeconds);
  }

  /** See `NanocachedClient.deleteIfMatches`. */
  deleteIfMatches(key: string | Uint8Array, token: string): Promise<boolean> {
    return this.ops.deleteIfMatches(key, token);
  }
}

/**
 * A client for nanocached: each configured address may name either a
 * single nanocached-node or a nanocached-discovery server fronting a
 * cluster — `connect()` doesn't take a separate option or shape for either
 * case, it finds out from the server's own response to the connection
 * handshake (see the server type in the auth response). Callers never need to know or care
 * which they're talking to.
 *
 * This establishes the connection(s) and routing table, and exposes
 * `get`/`set`/`delete`/`close`.
 */
export class NanocachedClient {
  private closed = false;
  private lastNodeListFetch = Date.now();
  private nodeListRefresh: Promise<void> | null = null;
  /** One in-flight lazy reconnect per connection slot (the node name in
   * cluster mode, "" in single mode) — concurrent requests that all find
   * the same dead connection share one reconnect instead of dialing N
   * times. See routedConnection. */
  private readonly reconnects = new Map<string, Promise<Connection>>();
  /** Per-address reconnect cooldown (see `NanocachedClientOptions.reconnectCooldownMs`):
   * the address of the most recently failed dial, and how long it stays
   * "down" before another dial to it is attempted. Keyed by address, not
   * slot — `memberConnection`'s slot (node name) can be reassigned to a
   * different address by a refresh, but the address itself is what's
   * actually unreachable. */
  private readonly reconnectCooldowns = new Map<string, { until: number; error: Error }>();
  private keepAliveTimer: NodeJS.Timeout | null = null;

  // Backing counters for stats()/ClientStats — see its doc comment.
  private replicaWriteFailures = 0;
  private readRepairFailures = 0;
  private refreshFailures = 0;
  /** Retryable-error status (issue #125) — see ClientStats.transientRetries.
   * Wired into every `Connection` this client ever opens: the handful
   * created before this instance exists (the initial `connect()`/
   * `connectViaProxy()` dial) via `Connection.setOnTransientRetry` right
   * after construction, every later one (reconnects, refreshes) by
   * passing the callback straight to `new Connection(...)`. */
  private transientRetries = 0;

  /** The node(s) actually being talked to, by address (for display/
   * introspection — routing itself uses each node's name, not its
   * address, see node identity decoupled from address): `[url]` in single mode, or the set of
   * nodes this instance currently holds a connection to in cluster mode —
   * kept current by maybeRefreshNodeList(), which reconciles `target`'s
   * ring/connections to match (see refreshNodeList). */
  nodeUrls: readonly string[];

  private constructor(
    private target: Target,
    /** The address that answered connect() — a node's own address in
     * single mode (which is also what a lazy reconnect redials), the
     * winning discovery server's address in cluster mode. */
    readonly url: string,
    nodeUrls: readonly string[],
    /** Every configured address (discovery HA) — what fetchNodeList walks on a
     * refresh, not just the address that happened to win the initial
     * connect. */
    private readonly addresses: readonly NanocachedAddress[],
    private readonly authSecret: string | undefined,
    private readonly tls: boolean | undefined,
    /** PEM contents read once from `NanocachedClientOptions.ca` (if any)
     * by connect(); reused for every dial this instance makes. */
    private readonly ca: Buffer | undefined,
    private readonly compress: boolean,
    private readonly compressionThreshold: number,
    private readonly fireAndForgetReplicas: boolean,
    private readonly readRepair: boolean,
    /** See `NanocachedClientOptions.reconnectCooldownMs`. */
    private readonly reconnectCooldownMs: number,
    /** See `NanocachedClientOptions.readHedgeAfterMs`. */
    private readonly readHedgeAfterMs: number | undefined,
  ) {
    this.nodeUrls = nodeUrls;
    this.startKeepAlive(KEEPALIVE_TUNING.intervalMs);
  }

  /** fire-and-forget replica writes: replica writes currently running in the
   * background (fireAndForgetReplicas) — close() drains these before
   * tearing down connections instead of abandoning them. */
  private readonly backgroundReplicaWrites = new Set<Promise<void>>();

  /** Hedged reads (issue #64): legs still in flight after a read has
   * already returned (the losers) — never cancelled, just left to finish
   * detached, their outcome retrieved so nothing surfaces as an unhandled
   * rejection, and drained by close() exactly like
   * `backgroundReplicaWrites`. See `readHedged`. */
  private readonly hedgedReads = new Set<Promise<unknown>>();

  static async connect(options: NanocachedClientOptions): Promise<NanocachedClient> {
    const addresses = options.addresses ?? [];
    if (addresses.length === 0) {
      throw new NanocachedError("nanocached: connect() needs a non-empty addresses list");
    }
    if (options.readHedgeAfterMs !== undefined && !(options.readHedgeAfterMs > 0)) {
      throw new NanocachedError("nanocached: readHedgeAfterMs must be a positive number of milliseconds");
    }

    // ca is meaningful only paired with tls: true; a set ca with tls not
    // enabled is silently ignored rather than an error. Read once here
    // (not per-dial) and reused for every connection this instance ever
    // opens, including reconnects and node-list refreshes.
    const ca = options.tls === true && options.ca !== undefined ? readFileSync(options.ca) : undefined;
    const compress = options.compress === true;
    const compressionThreshold = options.compressionThreshold ?? DEFAULT_COMPRESSION_THRESHOLD;
    const reconnectCooldownMs = options.reconnectCooldownMs ?? DEFAULT_RECONNECT_COOLDOWN_MS;

    if (options.viaProxy === true) {
      return NanocachedClient.connectViaProxy(options, addresses, ca, compress, compressionThreshold, reconnectCooldownMs);
    }

    // Walk the addresses in order until one yields a working target. An
    // address is skipped when it's unreachable, answers `B` (discovery HA
    // startup grace), or knows no live nodes — the next replica may do
    // better.
    let lastError: Error | null = null;

    for (const address of addresses) {
      const key = targetKey(address);
      // Only meaningful for a single explicit target: with an addresses
      // list, another client instance legitimately holding connections to
      // the same address makes this heuristic false-positive (issue #12).
      if (addresses.length === 1 && openTargets.has(key)) {
        console.warn(
          `nanocached: connect() called for ${key} while a previous connection to it is still open — was close() forgotten?`,
        );
      }

      let identified;
      try {
        identified = await connectAndIdentify({ host: address.host, port: address.port, authSecret: options.authSecret, tls: options.tls, ca });
      } catch (error) {
        lastError = error as Error;
        continue;
      }

      if (identified.kind === "node") {
        if (addresses.length > 1) {
          // Multiple addresses imply the caller expected redundancy, but a
          // node target pins the client to exactly this one server: the
          // remaining addresses don't form a cluster, and a later death of
          // this node is redialed, never failed over. Direct node targets
          // are for development or single-node deployments — clusters
          // should point addresses at discovery servers.
          console.warn(
            `nanocached: ${key} is a cache node, so this client is pinned to that single server — ` +
              `the ${addresses.length - 1} remaining address(es) will not be used. ` +
              `Point addresses at discovery servers for cluster routing and failover.`,
          );
        }

        trackOpenTarget(key, [identified.socket]);
        // Retryable-error status (issue #125): this connection is opened
        // before the client instance exists, so `transientRetries` can't
        // be closed over yet — wire it up right after construction
        // instead (see the `transientRetries` field's doc comment). Safe
        // because no `R` can arrive before then: identify traffic never
        // produces one.
        const connection = new Connection(identified.socket, identified.tagged);
        const client = new NanocachedClient(
          { kind: "single", connection },
          key,
          [key],
          addresses,
          options.authSecret,
          options.tls,
          ca,
          compress,
          compressionThreshold,
          options.fireAndForgetReplicas === true,
          options.readRepair === true,
          reconnectCooldownMs,
          options.readHedgeAfterMs,
        );
        connection.setOnTransientRetry(() => client.transientRetries++);
        return client;
      }

      if (identified.nodes.length === 0) {
        lastError = new NanocachedError(`nanocached: no live nodes registered with the discovery server at ${key}`);
        continue;
      }

      // Dials every node discovery listed, concurrently (issue #67). A
      // node that can't be reached — typically one that just died and
      // discovery hasn't evicted yet — is tolerated: it's installed as a
      // member with no connection and its reconnect cooldown armed
      // (exactly the state a member is in after dying mid-life), so it
      // stays in the ring and requests for its keys fail over per
      // request instead of the whole connect() failing. A listed address
      // that identifies as something other than a node (or an actual dial
      // bug) is still a hard error — another address would hand back the
      // same node list, so don't try one; another address would hand back
      // the same node list either way. Only a cluster with *no* reachable
      // node at all fails connect(), with the last dial error.
      const outcomes = await Promise.all(
        identified.nodes.map((node) => dialClusterNode(node, options.authSecret, options.tls, ca)),
      );

      const hard = outcomes.find((outcome) => outcome.kind === "hard");
      if (hard) {
        for (const outcome of outcomes) {
          if (outcome.kind === "ok") outcome.socket.destroy();
        }
        throw hard.error;
      }

      const members = new Map<string, ClusterMember>();
      const sockets: Array<Socket | TLSSocket> = [];
      const cooldowns: Array<[string, { until: number; error: Error }]> = [];
      let reachable = 0;
      let lastNodeError: Error | null = null;

      for (const outcome of outcomes) {
        if (outcome.kind === "ok") {
          members.set(outcome.node.name, {
            address: outcome.node.address,
            connection: new Connection(outcome.socket, outcome.tagged),
          });
          sockets.push(outcome.socket);
          reachable++;
        } else {
          members.set(outcome.node.name, { address: outcome.node.address, connection: null });
          cooldowns.push([outcome.node.address, { until: Date.now() + reconnectCooldownMs, error: outcome.error }]);
          lastNodeError = outcome.error;
        }
      }

      if (reachable === 0) {
        throw lastNodeError as Error;
      }

      trackOpenTarget(key, sockets);

      const client = new NanocachedClient(
        {
          kind: "cluster",
          ring: new HashRing(identified.nodes.map((node) => node.name)),
          members,
          replication: identified.replication,
        },
        key,
        identified.nodes.map((node) => node.address),
        addresses,
        options.authSecret,
        options.tls,
        ca,
        compress,
        compressionThreshold,
        options.fireAndForgetReplicas === true,
        options.readRepair === true,
        reconnectCooldownMs,
        options.readHedgeAfterMs,
      );
      // Armed after construction (reconnectCooldowns is initialized by the
      // constructor's field declarations before its body runs) — the same
      // cooldown state a mid-life redial failure would have left behind
      // (ensureConnected), so the first request for one of these nodes'
      // keys fails over immediately instead of paying a dial timeout, and
      // only redials once the cooldown has elapsed.
      for (const [address, cooldown] of cooldowns) {
        client.reconnectCooldowns.set(address, cooldown);
      }
      // Retryable-error status (issue #125): same deferred wiring as the
      // single-target branch above, one member connection at a time.
      for (const member of members.values()) {
        member.connection?.setOnTransientRetry(() => client.transientRetries++);
      }
      return client;
    }

    throw lastError ?? new NanocachedError("nanocached: could not connect to any address");
  }

  /** SDK proxy mode (issue #122, `viaProxy`): walks `addresses` — every
   * one of which must be a discovery server, never a bare node — for a
   * proxy roster via `Q` (mirroring the plain flow's walk for `L`), then
   * dials one proxy chosen at random from it, failing over through the
   * rest in random order if the pick is unreachable. A proxy looks
   * exactly like a single node that owns every key (full `G`/`S`/`D`,
   * never `W`), so once connected this client runs the same
   * single-connection path a lone node target does — `Target.kind ===
   * "proxy"` just additionally knows how to re-fetch `Q` and pick a new
   * proxy on reconnect (`refreshProxyTarget`), the way a cluster target
   * re-fetches `L` (`refreshNodeList`). See via-proxy-spec.md. */
  private static async connectViaProxy(
    options: NanocachedClientOptions,
    addresses: NanocachedAddress[],
    ca: Buffer | undefined,
    compress: boolean,
    compressionThreshold: number,
    reconnectCooldownMs: number,
  ): Promise<NanocachedClient> {
    let lastError: Error | null = null;

    for (const address of addresses) {
      let result;
      try {
        result = await connectAndListProxies({ host: address.host, port: address.port, authSecret: options.authSecret, tls: options.tls, ca });
      } catch (error) {
        // Unreachable, still warming up (DiscoveryBusyError), or an
        // actual programming bug — either way, try the next address the
        // same way the plain flow does for a discovery seed.
        lastError = error as Error;
        continue;
      }

      if (result.kind === "node") {
        // A hard config error, not a transient one (issue #122): every
        // other configured address would identify the same way (either
        // they're all discovery servers, or the caller pointed viaProxy
        // at the wrong thing), so there is no point trying the rest —
        // fail fast with a clear message instead of quietly falling back
        // to a node the caller never asked this mode to talk to directly.
        throw new NanocachedError(
          `nanocached: viaProxy is set, but ${address.host}:${address.port} identifies as a cache node, not a discovery server — proxy mode needs discovery addresses`,
        );
      }

      if (result.proxies.length === 0) {
        lastError = new NanocachedError(`nanocached: no proxies registered with the discovery server at ${address.host}:${address.port}`);
        continue;
      }

      // Random spread (issue #122): a fleet of fresh clients spreads
      // evenly across proxies, and a down first pick fails over through
      // the rest in that same random order rather than roster order.
      for (const proxy of shuffled(result.proxies)) {
        let identified;
        try {
          identified = await connectAndIdentify({ ...splitHostPort(proxy.address), authSecret: options.authSecret, tls: options.tls, ca });
        } catch (error) {
          lastError = error as Error;
          continue;
        }

        if (identified.kind !== "node") {
          // A listed proxy that no longer identifies as one — treat like
          // any other bad candidate and keep failing over.
          lastError = new NanocachedError(`nanocached: ${proxy.address} no longer identifies as a proxy`);
          continue;
        }

        const key = targetKey(address);
        trackOpenTarget(key, [identified.socket]);
        // Retryable-error status (issue #125): same deferred wiring as
        // the plain-node branch of connect() above.
        const connection = new Connection(identified.socket, identified.tagged);
        const client = new NanocachedClient(
          { kind: "proxy", connection, address: proxy.address },
          key,
          [proxy.address],
          addresses,
          options.authSecret,
          options.tls,
          ca,
          compress,
          compressionThreshold,
          options.fireAndForgetReplicas === true,
          options.readRepair === true,
          reconnectCooldownMs,
          options.readHedgeAfterMs,
        );
        connection.setOnTransientRetry(() => client.transientRetries++);
        return client;
      }
      // Every listed proxy was unreachable: unlike cluster bootstrap
      // (issue #67), a proxy target needs exactly one live connection to
      // start with — there is no connectionless-member fallback to install
      // — so fall through to the next discovery address instead.
    }

    throw lastError ?? new NanocachedError("nanocached: could not connect to any address");
  }

  /** Whether close() has already been called on this instance. */
  isClosed(): boolean {
    return this.closed;
  }

  /** Resolves only after every in-flight background replica write has
   * finished and the connections are torn down (fire-and-forget replica writes as
   * amended by issue #47 item 3 — the drain contract every SDK now
   * shares). Callers that don't await keep the old fire-and-forget
   * behavior: `closed` flips synchronously, and teardown still happens
   * once the drain settles. */
  async close(): Promise<void> {
    // Still idempotent (not an error, matching how socket.destroy() itself
    // behaves on an already-destroyed socket) — but a second close() is
    // usually a sign the caller lost track of this instance's lifecycle,
    // so flag it the same way connect() flags a forgotten close().
    if (this.closed) {
      console.warn("nanocached: close() called again on an already-closed client");
      return;
    }
    this.closed = true;

    if (this.keepAliveTimer !== null) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }

    // A loop, not a single snapshot-and-await: registerBackgroundReplicaWrite
    // rechecks `this.closed` before adding to the set, so the window for a
    // leg to be registered after this line has mostly closed already — but
    // a write() that read `this.closed === false` a moment before this
    // method flipped it, and is now between that read and actually adding
    // its leg, can still land one after the first await settles. Looping
    // until the set is genuinely empty catches that leg too instead of
    // abandoning it mid-flight when teardownConnections runs (issue #47
    // item 3 / audit finding, mirroring the Go and Rust SDKs' drain loops).
    while (this.backgroundReplicaWrites.size > 0) {
      await Promise.allSettled([...this.backgroundReplicaWrites]);
    }
    // Hedged reads (issue #64): same drain shape as backgroundReplicaWrites
    // above, for the same reason — a losing leg is left running detached
    // (readHedged), not cancelled, so close() must still wait for it
    // instead of abandoning it mid-flight.
    while (this.hedgedReads.size > 0) {
      await Promise.allSettled([...this.hedgedReads]);
    }
    this.teardownConnections();
  }

  private teardownConnections(): void {
    // Both "single" and "proxy" (issue #122) targets are one bare
    // `connection` — only "cluster" has a member map to fan out over.
    if (this.target.kind !== "cluster") {
      this.target.connection.close();
      return;
    }

    for (const member of this.target.members.values()) member.connection?.close();
  }

  /** How many nodes hold each key (client-side replication) — discovery's replication
   * factor in cluster mode, 1 against a single node. */
  get replication(): number {
    return this.target.kind === "cluster" ? this.target.replication : 1;
  }

  /** Observability for failures this client swallows by design
   * (client-side replication / fire-and-forget replica writes / read repair) — lets operators detect silently degrading
   * replication or a stuck node-list refresh. A snapshot, not a live
   * view; each count is monotonic for the lifetime of this client. */
  stats(): ClientStats {
    return {
      replicaWriteFailures: this.replicaWriteFailures,
      readRepairFailures: this.readRepairFailures,
      refreshFailures: this.refreshFailures,
      transientRetries: this.transientRetries,
    };
  }

  /** Resolves the value strictly decoded as UTF-8 — a value that isn't
   * valid UTF-8 rejects (native `TypeError` from `TextDecoder`'s fatal
   * mode), it is never silently replaced. Use `getBytes` for raw bytes,
   * e.g. for values this client didn't itself write as a UTF-8 string. */
  async get(key: string | Uint8Array): Promise<string | null> {
    return this.getInNamespace(EMPTY_NAMESPACE, key);
  }

  /** The raw-bytes companion to `get`: same routing/retry/cluster
   * behavior, no decoding. Transparently decompresses when `compress` is
   * enabled (value compression). With `readRepair`, a clean miss probes
   * the remaining owners before being accepted as final
   * (read repair). */
  async getBytes(key: string | Uint8Array): Promise<Buffer | null> {
    return this.getBytesInNamespace(EMPTY_NAMESPACE, key);
  }

  /** Compare-and-set (issue #141): reads `key` like `getBytes`, but also
   * returns a `token` — a content digest of the value's exact stored
   * bytes — that `replace`/`deleteIfMatches` accept as their expected
   * value, so a caller can act on a value only if nothing else changed it
   * since this read. `null` on a miss, matching `get`'s own convention.
   * See README.md's "Compare-and-set" section and docs/protocol.html#cas. */
  async getWithToken(key: string | Uint8Array): Promise<{ value: Buffer; token: string } | null> {
    return this.getWithTokenInNamespace(EMPTY_NAMESPACE, key);
  }

  private async getInNamespace(namespace: Uint8Array, key: string | Uint8Array): Promise<string | null> {
    const value = await this.getBytesInNamespace(namespace, key);
    return value === null ? null : UTF8_DECODER.decode(value);
  }

  private async getBytesInNamespace(namespace: Uint8Array, key: string | Uint8Array): Promise<Buffer | null> {
    const value = await this.getRawInNamespace(namespace, key);
    if (value === null || !this.compress) return value;
    return decompressValue(value);
  }

  /** The raw-wire-bytes fetch `getBytes`/`getWithToken` both build on:
   * routing, `W` refresh-and-retry, and read repair, but no decompression
   * — for a compression-enabled client this is exactly the marker-
   * prefixed bytes the server itself stores, never the decompressed
   * value. Factored out so `getWithToken` (issue #141) can compute
   * `contentDigest` over these same bytes: the digest MUST match what the
   * server would compute, and the server never decompresses, so hashing
   * anything else would silently break every CAS call once compression is
   * on. */
  private async getRawInNamespace(namespace: Uint8Array, key: string | Uint8Array): Promise<Buffer | null> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    let value = await this.withWrongNodeRetry(() =>
      this.target.kind === "cluster"
        ? this.readFromOwners(key, namespace, (connection) => connection.get(key, namespace))
        : this.connectionForSingleTarget().then((connection) => connection.get(key, namespace)),
    );
    if (value === null && this.readRepair && this.target.kind === "cluster") {
      value = await this.tryReadRepair(key, namespace);
    }
    return value;
  }

  /** Compare-and-set (issue #141): the low-level "get with a CAS token"
   * primitive — `null` on a miss, matching `get`'s own convention.
   * `token` is `contentDigest` of the exact raw wire bytes (marker byte
   * included, for a compression-enabled client), computed *before*
   * decompression — the same bytes the server itself hashes — so it can
   * be handed straight to `replace`/`deleteIfMatches` to condition on
   * exactly the value just read, regardless of `compress`. `value` is the
   * ordinary, decompressed result `getBytes` would return. */
  private async getWithTokenInNamespace(namespace: Uint8Array, key: string | Uint8Array): Promise<{ value: Buffer; token: string } | null> {
    const raw = await this.getRawInNamespace(namespace, key);
    if (raw === null) return null;
    const token = contentDigest(raw);
    const value = this.compress ? decompressValue(raw) : raw;
    return { value, token };
  }

  /** read repair: probes every owner of `key`, in rank order, for a
   * value the normal read path already reported missing. The first
   * owner that has it wins: its value is returned, and — detached, not
   * awaited by this method's own caller — that same value repairs
   * `names[0]` (the true primary) in the background, with TTL
   * READ_REPAIR_TTL_SECONDS (the original TTL can't be recovered from a
   * GET, and TTL 0 would permanently resurrect already-expired data).
   * That write-back is bounded and tracked the same way a
   * fireAndForgetReplicas replica write is (FIRE_AND_FORGET_TUNING.maxInFlight,
   * backgroundReplicaWrites — fire-and-forget replica writes), so close() drains it too
   * and an unlucky run of misses can't spawn unbounded background writes;
   * past the cap, the repair for that miss is simply skipped — read
   * repair is opportunistic, so a later miss on the same key repairs it.
   * Every failure along the way (connection lost, WrongNode, another
   * miss) is swallowed; only a failed repair *write-back* is counted in
   * stats().readRepairFailures — a failed owner probe is silent, matching
   * the counter's write-back semantics in the other five SDKs (issue
   * #43). Nothing here may turn an already-accepted miss into an error —
   * except an actual programming bug (isSwallowable), which still
   * propagates. */
  private async tryReadRepair(key: string | Uint8Array, namespace: Uint8Array): Promise<Buffer | null> {
    const names = this.ownerNames(key, namespace);
    // Every owner but the primary, which the normal read path already
    // probed and got a clean miss from (same as the Rust, Go, Java and
    // .NET SDKs).
    for (const name of names.slice(1)) {
      let value: Buffer | null;
      try {
        const connection = await this.memberConnection(name);
        value = await connection.get(key, namespace);
      } catch (error) {
        if (!isSwallowable(error)) throw error;
        continue;
      }
      if (value === null) continue;

      const primaryName = names[0];
      const repairValue = value;
      // Bounded and tracked the same way a fireAndForgetReplicas replica
      // write is (fire-and-forget replica writes) — reusing backgroundReplicaWrites and
      // FIRE_AND_FORGET_TUNING.maxInFlight — so close() drains this
      // write-back too instead of abandoning it, and this can't grow
      // unbounded the way an untracked spawn-per-miss would. Past the
      // cap, skip the repair for this miss; it's opportunistic, so a
      // later miss on the same key repairs it.
      // `!this.closed` is rechecked here, synchronously and immediately
      // before this leg is ever registered in backgroundReplicaWrites —
      // no await happens between this check and the `.add()` below, so
      // close()'s own drain can't race a leg into existence after it
      // already took its snapshot (issue #47 item 3 / audit finding). When
      // closed, the repair is simply skipped, same as when the cap is
      // already reached — read repair is opportunistic, so a later miss
      // repairs it.
      if (primaryName !== undefined && !this.closed && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
        const repaired = this.memberConnection(primaryName)
          .then((connection) => connection.set(key, repairValue, READ_REPAIR_TTL_SECONDS, namespace))
          .catch((error) => {
            // Swallowed by design — see the doc comment.
            if (!isSwallowable(error)) throw error;
            this.readRepairFailures++;
          });
        // This write is detached: tryReadRepair has already returned to
        // its caller by the time it settles, so an actual programming bug
        // rethrown above has nowhere left to propagate to. Attach a no-op
        // catch anyway, synchronously — otherwise Node would flag the
        // rethrow above as an unhandled rejection (it isn't counted in
        // readRepairFailures either way, so it stays distinguishable from
        // a routine swallow to anything inspecting stats()) — before
        // tracking it in backgroundReplicaWrites, mirroring writeToOwners.
        const settled = repaired.catch(() => {});
        this.backgroundReplicaWrites.add(repaired);
        settled.finally(() => this.backgroundReplicaWrites.delete(repaired));
      }
      return value;
    }
    return null;
  }

  /** `ttlSeconds` (whole seconds, default 0) is when the key expires; 0
   * means no expiry. Must be a non-negative integer. Transparently
   * compresses values at or above `compressionThreshold` when `compress`
   * is enabled (value compression). */
  async set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<void> {
    return this.setInNamespace(EMPTY_NAMESPACE, key, value, ttlSeconds);
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    return this.deleteInNamespace(EMPTY_NAMESPACE, key);
  }

  /** Atomically adds `delta` (signed, default 1) to `key`'s stored
   * counter and returns the new value — `null` if the key is missing or
   * expired, matching `get`'s own miss convention. Throws
   * `NotNumericError` if the stored value isn't an integer INCR can
   * operate on, or if applying `delta` would overflow; throws
   * `CounterOutOfRangeError` (issue #224) if the new counter falls
   * outside `±Number.MAX_SAFE_INTEGER` — the wire protocol's counter is a
   * full signed 64-bit integer, wider than a JS `number` can represent
   * exactly, so this call refuses to silently hand back a rounded value.
   * The increment itself still happened (and, in a cluster, was still
   * replicated byte-exact) even when this throws — only the value handed
   * back to *this* call is affected.
   *
   * **As volatile as `set`**: LRU eviction and TTL expiry reclaim an
   * incremented value exactly like any other entry, so this is for rate
   * limiting / approximate counters, never for a durable count (billing,
   * inventory). See README.md's "incr / decr" section for the cluster
   * replication caveat: only the primary owner ever runs the increment,
   * replicas just receive its literal result. */
  async incr(key: string | Uint8Array, delta = 1): Promise<number | null> {
    return this.incrInNamespace(EMPTY_NAMESPACE, key, delta);
  }

  /** `incr` with a negated delta — there is no separate wire operation for
   * decrement, the server (and the wire protocol) only ever sees `i`. */
  async decr(key: string | Uint8Array, delta = 1): Promise<number | null> {
    return this.incrInNamespace(EMPTY_NAMESPACE, key, -delta);
  }

  /** Compare-and-set (issue #141): `add`/`k` with an `absent` condition —
   * stores `value` only if `key` doesn't currently hold an unexpired
   * value. Resolves `true` if stored, `false` if the key already existed
   * — a condition mismatch is a normal boolean outcome, never an
   * exception, the same idiom `delete()` already uses for "nothing to
   * act on". See README.md's "Compare-and-set" section for the
   * not-a-distributed-lock caveat: LRU eviction can still reclaim `key`
   * exactly as it would after a plain `set`. */
  async putIfAbsent(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<boolean> {
    return this.casInNamespace(EMPTY_NAMESPACE, key, value, { kind: "absent" }, ttlSeconds);
  }

  /** Compare-and-set (issue #141): the two-argument `replace(key, value)`
   * — `k` with a `present` condition — stores `value` only if `key`
   * currently holds any (unexpired) value, whatever it is. Resolves
   * `true` if replaced, `false` if the key was absent — a mismatch is a
   * normal boolean outcome, never an exception. */
  async replaceIfPresent(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<boolean> {
    return this.casInNamespace(EMPTY_NAMESPACE, key, value, { kind: "present" }, ttlSeconds);
  }

  /** Compare-and-set (issue #141): the three-argument
   * `replace(key, old, new)` — `k` with a digest condition — stores
   * `newValue` only if `key`'s current stored bytes hash to exactly
   * `token` (see `contentDigest`/`getWithToken`). Resolves `true` if
   * replaced, `false` on a mismatch (the key changed, or is missing) —
   * never an exception for a mismatch.
   *
   * `token` accepts the hex-digest string `contentDigest`/`getWithToken`
   * produce directly. A token taken from a real prior read (via
   * `getWithToken`) is always correct. A token *reconstructed* by
   * re-serializing/re-compressing a value the caller already holds,
   * rather than one taken from an actual read, is only correct if that
   * reconstruction produces byte-identical output to what the server
   * actually stores — exactly as sensitive to encoding as memcached's own
   * value-based CAS, and not guaranteed across languages or compression
   * settings the way the read-then-write-back path always is. */
  async replace(key: string | Uint8Array, token: string, newValue: string | Uint8Array, ttlSeconds = 0): Promise<boolean> {
    return this.casInNamespace(EMPTY_NAMESPACE, key, newValue, { kind: "digest", digest: token }, ttlSeconds);
  }

  /** Compare-and-set (issue #141): the two-argument `remove(key, old)` —
   * `x` — deletes `key` only if its current stored bytes hash to exactly
   * `token` (see `contentDigest`/`getWithToken`). Resolves `true` if
   * deleted, `false` on a mismatch or a missing key — never an exception
   * for either. */
  async deleteIfMatches(key: string | Uint8Array, token: string): Promise<boolean> {
    return this.casDeleteInNamespace(EMPTY_NAMESPACE, key, token);
  }

  private async setInNamespace(namespace: Uint8Array, key: string | Uint8Array, value: string | Uint8Array, ttlSeconds: number): Promise<void> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    const valueBytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
    // Validate the *original* value's size before compressing, matching
    // Python's Set (issue #47 audit item 3) — an oversized value must be
    // rejected outright, not silently forwarded just because DEFLATE
    // happens to shrink it under the cap. Re-checked after compression
    // too, purely as a defense-in-depth backstop should compressValue
    // ever grow a value instead of shrinking it (encodeSet, reached
    // through connection.set below, does that second check).
    checkKeyAndValue(keyBytes, valueBytes, namespace);
    const outgoing = this.compress ? compressValue(valueBytes, this.compressionThreshold) : valueBytes;
    return this.withWrongNodeRetry(() =>
      this.target.kind === "cluster"
        ? this.writeToOwners(key, namespace, (connection) => connection.set(key, outgoing, ttlSeconds, namespace))
        : this.connectionForSingleTarget().then((connection) => connection.set(key, outgoing, ttlSeconds, namespace)),
    );
  }

  private async deleteInNamespace(namespace: Uint8Array, key: string | Uint8Array): Promise<boolean> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    return this.withWrongNodeRetry(() =>
      this.target.kind === "cluster"
        ? this.writeToOwners(key, namespace, (connection) => connection.delete(key, namespace))
        : this.connectionForSingleTarget().then((connection) => connection.delete(key, namespace)),
    );
  }

  // ── batched get/set (issues #128/#150/#151) ────────────────────────

  /** Returns every requested key's value as a string, in one round trip
   * per owner (batched get) instead of one round trip per key — see
   * `getManyBytes`, which this decodes, for the full contract (missing
   * keys, the partial-result error, chunking). */
  async getMany(keys: readonly string[]): Promise<Map<string, string>> {
    return this.getManyInNamespace(EMPTY_NAMESPACE, keys);
  }

  private async getManyInNamespace(namespace: Uint8Array, keys: readonly string[]): Promise<Map<string, string>> {
    try {
      return this.decodeMany(await this.getManyBytesInNamespace(namespace, keys));
    } catch (error) {
      if (error instanceof PartialWrongNodeError) {
        throw new PartialWrongNodeError(this.decodeMany(error.partialValues as Map<string, Buffer>));
      }
      throw error;
    }
  }

  private decodeMany(raw: Map<string, Buffer>): Map<string, string> {
    const values = new Map<string, string>();
    for (const [key, value] of raw) values.set(key, UTF8_DECODER.decode(value));
    return values;
  }

  /** Returns every requested key's raw value in one round trip per
   * owner (batched get, docs/protocol.html#multi) — a missing key is
   * simply absent from the returned map, never an error, the same "a
   * miss is not an error" contract `getBytes` itself has. `keys` must
   * be non-empty.
   *
   * A batch never fails as a whole: if some keys are still wrong-node
   * after one bounded refresh-and-retry (the same policy `getBytes`
   * itself applies, generalized to a per-key roster instead of an
   * all-or-nothing retry — see `multiGetPass`), this throws
   * `PartialWrongNodeError` whose `.partialValues` holds every key that
   * DID resolve, rather than discarding a mostly-successful batch over
   * a handful of stale placements. In single-node/proxy mode a `W`
   * propagates the same way, immediately — there is no ring to refresh
   * against.
   *
   * Larger batches are transparently split into more than one `m`
   * sub-frame per owner (batch chunking, see MAX_BATCH_KEYS and, for
   * the cumulative-bytes bound alongside it, MAX_REQUEST_BYTES /
   * `nextChunkEnd`) — callers never need to think about this. */
  async getManyBytes(keys: readonly string[]): Promise<Map<string, Buffer>> {
    return this.getManyBytesInNamespace(EMPTY_NAMESPACE, keys);
  }

  private async getManyBytesInNamespace(namespace: Uint8Array, keys: readonly string[]): Promise<Map<string, Buffer>> {
    if (this.closed) throw new AlreadyClosedError();
    if (keys.length === 0) {
      throw new RangeError("nanocached: getMany/getManyBytes requires at least one key");
    }
    // Eager, up-front, before any network I/O — the same fail-fast
    // contract every other public method already gets from its own
    // encoder; a bad key deep in a 900-key batch must not leave earlier
    // sub-frames already sent before it's discovered.
    const keyBytes = keys.map((key) => {
      const bytes = Buffer.from(key, "utf8");
      checkKey(bytes, namespace);
      return bytes;
    });
    await this.maybeRefreshNodeList();

    const values = new Map<string, Buffer>();

    if (this.target.kind !== "cluster") {
      // Single/proxy mode: no retry layer at all, matching
      // getRawInNamespace's own non-cluster branch (withWrongNodeRetry
      // excludes "single", and a proxy connection has no ring to
      // refresh against either way).
      const entries = await this.multiGetChunked(() => this.connectionForSingleTarget(), namespace, keyBytes);
      let wrongNode = false;
      for (let i = 0; i < entries.length; i++) {
        const entry = entries[i];
        if (entry.kind === "hit") values.set(keys[i], entry.value);
        else if (entry.kind === "wrongNode") wrongNode = true;
      }
      if (wrongNode) throw new PartialWrongNodeError(values);
      return values;
    }

    let retryIndices = await this.multiGetPass(namespace, keys, keyBytes, values, undefined);
    if (retryIndices.length === 0) return values;

    await this.maybeRefreshNodeList({ force: true });
    retryIndices = await this.multiGetPass(namespace, keys, keyBytes, values, retryIndices);
    if (retryIndices.length > 0) throw new PartialWrongNodeError(values);
    return values;
  }

  /** One pass of getManyBytes' cluster routing: group the given indices
   * (every key, when retryIndices is undefined — the initial pass — or
   * just what a previous pass left unresolved) by their current
   * primary owner (matching readFromOwners' own primary-first stance),
   * dispatch one (possibly chunked) `m` exchange per owner
   * concurrently, splice hits into values, and return the indices
   * still unresolved: a per-key `W`, or a whole owner group whose call
   * failed outright — indistinguishable from a possibly-idle-closed
   * connection, the same stance memberConnection's own callers take
   * elsewhere. Called once for the initial pass and once more, if
   * needed, after a single forced refresh — see
   * getManyBytesInNamespace. This deliberately does not reuse
   * withWrongNodeRetry (whole-operation, exception-driven); it's the
   * same two-pass-with-forced-refresh idiom fanoutClear/
   * clearFanoutAttempt already use, just keyed per-key instead of
   * per-node. */
  private async multiGetPass(
    namespace: Uint8Array,
    keys: readonly string[],
    keyBytes: readonly Buffer[],
    values: Map<string, Buffer>,
    retryIndices: readonly number[] | undefined,
  ): Promise<number[]> {
    const indices = retryIndices ?? keyBytes.map((_, index) => index);

    const groups = new Map<string, number[]>();
    const retry: number[] = [];
    for (const index of indices) {
      const owners = this.ownerNames(keyBytes[index], namespace);
      if (owners.length === 0) {
        retry.push(index);
        continue;
      }
      const primary = owners[0];
      const group = groups.get(primary);
      if (group) group.push(index);
      else groups.set(primary, [index]);
    }

    await Promise.all(
      [...groups.entries()].map(async ([owner, groupIndices]) => {
        const groupKeyBytes = groupIndices.map((index) => keyBytes[index]);
        let entries: MultiEntry[];
        try {
          entries = await this.multiGetChunked(() => this.memberConnection(owner), namespace, groupKeyBytes);
        } catch {
          retry.push(...groupIndices);
          return;
        }
        for (let i = 0; i < groupIndices.length; i++) {
          const index = groupIndices[i];
          const entry = entries[i];
          if (entry.kind === "wrongNode") retry.push(index);
          else if (entry.kind === "hit") values.set(keys[index], entry.value);
        }
      }),
    );

    return retry;
  }

  /** Issues one or more `m` sub-frames against whatever `connectionFor`
   * resolves to — already grouped to one owner (or the single/proxy
   * target) by the caller — splitting into chunks bounded by both
   * MAX_BATCH_KEYS and MAX_REQUEST_BYTES (batch chunking, issue #222,
   * see `nextChunkEnd`) so no reply header risks exceeding protocol.ts's
   * MAX_MULTI_HEADER_LENGTH, and no request frame risks exceeding the
   * server's own per-request cap. `connectionFor` is called fresh for
   * every chunk (not resolved once up front), so a mid-batch reconnect
   * is handled exactly the way a single-key get/set handles one today. */
  private async multiGetChunked(
    connectionFor: () => Promise<Connection>,
    namespace: Uint8Array,
    keyBytes: readonly Buffer[],
  ): Promise<MultiEntry[]> {
    const entries: MultiEntry[] = new Array(keyBytes.length);
    for (let start = 0; start < keyBytes.length; ) {
      const end = nextChunkEnd(namespace, keyBytes.length, start, (i) => multiGetEntryCost(keyBytes[i]));
      const connection = await connectionFor();
      const chunkEntries = await connection.multiGet(keyBytes.slice(start, end), namespace);
      for (let i = start; i < end; i++) entries[i] = chunkEntries[i - start];
      start = end;
    }
    return entries;
  }

  /** Stores every value in `values` in one round trip per involved node
   * (batched set) instead of one round trip per key — see
   * `setManyBytes` for the raw-bytes form this wraps, including its
   * wrong-node and replication contract. `ttlSeconds` is shared by the
   * whole batch, not per key (one real caller of a batched set —
   * Django's `set_many`, cache-manager's `mset` — already passes one
   * TTL per call). */
  async setMany(values: Record<string, string>, ttlSeconds = 0): Promise<void> {
    return this.setManyStringInNamespace(EMPTY_NAMESPACE, values, ttlSeconds);
  }

  private async setManyStringInNamespace(namespace: Uint8Array, values: Record<string, string>, ttlSeconds: number): Promise<void> {
    const raw: Record<string, Uint8Array> = {};
    for (const [key, value] of Object.entries(values)) raw[key] = Buffer.from(value, "utf8");
    return this.setManyInNamespace(namespace, raw, ttlSeconds);
  }

  /** Stores every raw value in `values` in one round trip per involved
   * node (batched set, docs/protocol.html#multi). `ttlSeconds` is a
   * whole number of seconds shared by the whole batch; 0 means no
   * expiry, negative or non-integer is rejected. `values` must be
   * non-empty. Transparently compresses values at or above
   * `compressionThreshold` when `compress` is enabled, exactly like
   * `setBytes`.
   *
   * Within one batch, the same node can be a key's primary and another
   * key's replica at once — it receives exactly one `o` sub-frame
   * either way, and only its answer for the keys it is primary for
   * decides those keys' outcome; a replica-held key's failure is
   * logged-and-swallowed into `stats().replicaWriteFailures`, exactly
   * like `setBytes`' own replica legs (`writeToOwners`). A batch never
   * fails as a whole: if some keys' primaries are still wrong-node
   * after one bounded refresh-and-retry, this throws `WrongNodeError`
   * — every other key in the batch was still stored. In
   * single-node/proxy mode a `W` propagates immediately, exactly as
   * `setBytes`' own single-mode behavior does.
   *
   * Larger batches are transparently split into more than one `o`
   * sub-frame per node (batch chunking, see MAX_BATCH_KEYS and, for
   * the cumulative-bytes bound alongside it, MAX_REQUEST_BYTES /
   * `nextChunkEnd`). */
  async setManyBytes(values: Record<string, Uint8Array>, ttlSeconds = 0): Promise<void> {
    return this.setManyInNamespace(EMPTY_NAMESPACE, values, ttlSeconds);
  }

  private async setManyInNamespace(namespace: Uint8Array, values: Record<string, Uint8Array>, ttlSeconds: number): Promise<void> {
    if (this.closed) throw new AlreadyClosedError();
    const keys = Object.keys(values);
    if (keys.length === 0) {
      throw new RangeError("nanocached: setMany/setManyBytes requires at least one key");
    }
    if (!Number.isInteger(ttlSeconds) || ttlSeconds < 0) {
      throw new RangeError(`nanocached: ttlSeconds must be a non-negative integer, got ${ttlSeconds}`);
    }
    // Eager, up-front, before any network I/O — see
    // getManyBytesInNamespace's own doc comment for why.
    const keyBytes = new Array<Buffer>(keys.length);
    const valueBytes = new Array<Buffer>(keys.length);
    for (let i = 0; i < keys.length; i++) {
      const key = Buffer.from(keys[i], "utf8");
      const original = Buffer.from(values[keys[i]]);
      checkKeyAndValue(key, original, namespace);
      keyBytes[i] = key;
      valueBytes[i] = this.compress ? compressValue(original, this.compressionThreshold) : original;
    }
    await this.maybeRefreshNodeList();

    if (this.target.kind !== "cluster") {
      const entries = await this.multiSetChunked(() => this.connectionForSingleTarget(), namespace, keyBytes, valueBytes, ttlSeconds);
      if (entries.some((entry) => entry.kind === "wrongNode")) throw new WrongNodeError();
      return;
    }

    let retryIndices = await this.multiSetPass(namespace, keyBytes, valueBytes, ttlSeconds, undefined);
    if (retryIndices.length === 0) return;

    await this.maybeRefreshNodeList({ force: true });
    retryIndices = await this.multiSetPass(namespace, keyBytes, valueBytes, ttlSeconds, retryIndices);
    if (retryIndices.length > 0) throw new WrongNodeError();
  }

  /** multiGetChunked's write-side twin: one or more `o` sub-frames
   * against whatever `connectionFor` resolves to, chunked by both
   * MAX_BATCH_KEYS and MAX_REQUEST_BYTES the same way (issue #222,
   * `nextChunkEnd`) — a chunk's namespace + every key's and every
   * value's bytes together must fit MAX_REQUEST_BYTES, mirroring
   * encodeMultiSet's own bound. */
  private async multiSetChunked(
    connectionFor: () => Promise<Connection>,
    namespace: Uint8Array,
    keyBytes: readonly Buffer[],
    valueBytes: readonly Buffer[],
    ttlSeconds: number,
  ): Promise<MultiAckEntry[]> {
    const entries: MultiAckEntry[] = new Array(keyBytes.length);
    for (let start = 0; start < keyBytes.length; ) {
      const end = nextChunkEnd(namespace, keyBytes.length, start, (i) => multiSetEntryCost(keyBytes[i], valueBytes[i]));
      const connection = await connectionFor();
      const chunkEntries = await connection.multiSet(keyBytes.slice(start, end), valueBytes.slice(start, end), ttlSeconds, namespace);
      for (let i = start; i < end; i++) entries[i] = chunkEntries[i - start];
      start = end;
    }
    return entries;
  }

  /** One pass of setManyBytes' cluster routing: for every key still
   * needing resolution (every key, when retryIndices is undefined, or
   * just what a previous pass left unresolved), build one sub-batch
   * per **owner name across every rank** — not just primaries, unlike
   * multiGetPass — because within one batch the same node can be
   * primary for one key and a replica for another (see setManyBytes'
   * own doc comment); each owner therefore gets exactly one `o`
   * sub-frame covering every key it holds in any role. Only a leg's
   * *primary* keys can end up in the returned retry list; a leg's
   * replica-held keys are logged-and-swallowed into
   * stats().replicaWriteFailures instead, mirroring writeToOwners'
   * stance for single-key set. A leg that is a pure replica for every
   * key it holds is eligible for fireAndForgetReplicas, exactly like a
   * single-key replica write. */
  private async multiSetPass(
    namespace: Uint8Array,
    keyBytes: readonly Buffer[],
    valueBytes: readonly Buffer[],
    ttlSeconds: number,
    retryIndices: readonly number[] | undefined,
  ): Promise<number[]> {
    const indices = retryIndices ?? keyBytes.map((_, index) => index);

    const groups = new Map<string, { indices: number[]; isPrimary: boolean[] }>();
    const retry: number[] = [];
    for (const index of indices) {
      const names = this.ownerNames(keyBytes[index], namespace);
      if (names.length === 0) {
        retry.push(index);
        continue;
      }
      names.forEach((name, rank) => {
        let group = groups.get(name);
        if (!group) {
          group = { indices: [], isPrimary: [] };
          groups.set(name, group);
        }
        group.indices.push(index);
        group.isPrimary.push(rank === 0);
      });
    }

    const legs: Promise<void>[] = [];
    // The real fire-and-forget leg promises (issue #188), kept so a
    // genuine programming bug on one of them can still surface — see the
    // fire-and-forget branch below and the drain after Promise.all(legs).
    const backgroundLegs: Promise<void>[] = [];
    for (const [name, group] of groups) {
      const runLeg = async (): Promise<void> => {
        let entries: MultiAckEntry[];
        try {
          const groupKeyBytes = group.indices.map((index) => keyBytes[index]);
          const groupValueBytes = group.indices.map((index) => valueBytes[index]);
          entries = await this.multiSetChunked(() => this.memberConnection(name), namespace, groupKeyBytes, groupValueBytes, ttlSeconds);
        } catch (error) {
          // Swallowed by design, mirroring writeToOwners' replicaWrite
          // closure — an actual programming bug (isSwallowable) still
          // propagates instead of vanishing into a retry/stat.
          if (!isSwallowable(error)) throw error;
          for (let i = 0; i < group.indices.length; i++) {
            if (group.isPrimary[i]) retry.push(group.indices[i]);
            else this.replicaWriteFailures++;
          }
          return;
        }
        for (let i = 0; i < group.indices.length; i++) {
          if (!group.isPrimary[i]) {
            if (entries[i].kind === "wrongNode") this.replicaWriteFailures++;
            continue;
          }
          if (entries[i].kind === "wrongNode") retry.push(group.indices[i]);
        }
      };

      const pureReplica = group.isPrimary.every((primary) => !primary);
      if (this.fireAndForgetReplicas && pureReplica && !this.closed && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
        // Detached: a pure-replica leg holds no primary key, so it has
        // nothing to contribute to `retry` — see runLeg's own bounds.
        // `!this.closed` is rechecked synchronously, immediately before
        // `.add()`, with no `await` between (issue #47 audit
        // invariant), mirroring writeToOwners exactly. Not awaited here
        // (that would delay this whole pass by however long the slowest
        // pure-replica leg takes, defeating fireAndForgetReplicas'
        // point) — `backgroundLegs` keeps the real promise around so a
        // genuine bug on it can still surface below if a primary-holding
        // leg fails anyway (issue #188).
        const background = runLeg();
        const settled = background.catch(() => {});
        this.backgroundReplicaWrites.add(background);
        settled.finally(() => this.backgroundReplicaWrites.delete(background));
        backgroundLegs.push(background);
        continue;
      }

      const leg = runLeg();
      leg.catch(() => {});
      legs.push(leg);
    }

    try {
      await Promise.all(legs);
    } catch (error) {
      // A primary-holding leg hit a genuine bug (isSwallowable) — this
      // pass is failing regardless, so draining the fire-and-forget
      // pure-replica legs here costs nothing, and surfacing a bug found
      // among them takes priority: it's evidence of the same class of
      // problem, and losing it silently is exactly what issue #188 was
      // filed against.
      const backgroundResults = await Promise.allSettled(backgroundLegs);
      const backgroundBug = backgroundResults.find((result): result is PromiseRejectedResult => result.status === "rejected");
      throw backgroundBug ? backgroundBug.reason : error;
    }
    return retry;
  }

  /** INCR/DECR (issue #129). Unlike `writeToOwners` (which sends the same
   * write to every owner), a cluster incr only ever sends `i` to the
   * primary — see `incrOnOwners` for why and how the result reaches
   * replicas instead. In single/proxy mode there is nothing to replicate,
   * so this is just the one connection's own `incr`.
   *
   * Either branch can come back with a `value` that lost precision being
   * parsed into a `number` (issue #224: the wire's counter is a full
   * signed 64-bit integer, `Number.MAX_SAFE_INTEGER` is 2^53 - 1) — that
   * check is applied once, here, after any replica fan-out already
   * happened using the exact wire bytes (`incrOnOwners`'s `raw`), so a
   * counter too large for this call to report back still leaves every
   * replica byte-identical to the primary; only the value handed back to
   * *this* caller is refused. */
  private async incrInNamespace(namespace: Uint8Array, key: string | Uint8Array, delta: number): Promise<number | null> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    const result = await this.withWrongNodeRetry(() =>
      this.target.kind === "cluster"
        ? this.incrOnOwners(key, namespace, delta)
        : this.connectionForSingleTarget().then((connection) => connection.incr(key, delta, namespace)),
    );
    if (result === null) return null;
    if (!Number.isSafeInteger(result.value)) throw new CounterOutOfRangeError(result.raw.toString("ascii"));
    return result.value;
  }

  /** Compare-and-set (issue #141). Encodes/validates/compresses `value`
   * exactly like `setInNamespace` — a new value written by CAS must go
   * through the same compression pipeline `set` uses, or a later plain
   * `get` from any compress-enabled client would fail to decompress it —
   * then dispatches to the primary only (`casOnOwners`), mirroring
   * `incrInNamespace`'s own split between single/proxy mode (one
   * connection's own `cas`) and cluster mode. */
  private async casInNamespace(
    namespace: Uint8Array,
    key: string | Uint8Array,
    value: string | Uint8Array,
    cond: CasCondition,
    ttlSeconds: number,
  ): Promise<boolean> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    const valueBytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
    // Same two-layer size check setInNamespace uses: the *original* value
    // must fit MAX_REQUEST_BYTES, not just its compressed form (issue #47
    // audit item 3).
    checkKeyAndValue(keyBytes, valueBytes, namespace);
    const outgoing = this.compress ? compressValue(valueBytes, this.compressionThreshold) : valueBytes;
    return this.withWrongNodeRetry(() =>
      this.target.kind === "cluster"
        ? this.casOnOwners(key, namespace, outgoing, cond, ttlSeconds)
        : this.connectionForSingleTarget().then((connection) => connection.cas(key, outgoing, cond, ttlSeconds, namespace)),
    );
  }

  /** Compare-and-delete (issue #141): the `x` op's client-side driver,
   * mirroring `casInNamespace` above for the delete case — dispatches to
   * the primary only (`casDeleteOnOwners`). */
  private async casDeleteInNamespace(namespace: Uint8Array, key: string | Uint8Array, token: string): Promise<boolean> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    return this.withWrongNodeRetry(() =>
      this.target.kind === "cluster"
        ? this.casDeleteOnOwners(key, namespace, token)
        : this.connectionForSingleTarget().then((connection) => connection.casDelete(key, token, namespace)),
    );
  }

  /** Clears one namespace (issue #106): every entry in it, on every node.
   * `namespace` defaults to the default (empty) namespace — see `clearAll`
   * for the client's own `clear_all`-style flush of every namespace at
   * once. See `fanoutClear` for the shared fan-out/retry mechanics. */
  private clearInNamespace(namespace: Uint8Array): Promise<void> {
    return this.fanoutClear((connection) => connection.clear(namespace));
  }

  /** Flushes every namespace, the default one included (issue #106) —
   * the client-level counterpart to a namespace handle's `clear()`. See
   * `fanoutClear`. */
  async clearAll(): Promise<void> {
    return this.fanoutClear((connection) => connection.clearAll());
  }

  /** Returns a lightweight handle scoped to `ns` (first-class namespaces,
   * issue #105) — see `NanocachedNamespace`. `ns` accepts the same
   * key-ish types this client accepts for keys: a `string` is UTF-8
   * encoded, a `Uint8Array` is used as-is (namespaces are opaque bytes —
   * no delimiter, no escaping, no hierarchy, matching `src/key.rs` on the
   * server). The empty namespace (`""`/an empty `Uint8Array`) is not
   * rejected: it returns a handle that behaves exactly like this client,
   * since it addresses the very same default namespace `get`/`set`/
   * `delete` already do — see `getInNamespace`/`encodeGet` and friends,
   * which all send the legacy frame for it. The handle shares this
   * client's connections and forwards every call to this client's own
   * (namespace, key)-taking methods — it opens nothing of its own and
   * duplicates none of this client's networking. */
  namespace(ns: string | Uint8Array): NanocachedNamespace {
    const namespaceBytes = typeof ns === "string" ? Buffer.from(ns, "utf8") : Buffer.from(ns);
    return new NanocachedNamespace(namespaceBytes, {
      get: (key) => this.getInNamespace(namespaceBytes, key),
      getBytes: (key) => this.getBytesInNamespace(namespaceBytes, key),
      getWithToken: (key) => this.getWithTokenInNamespace(namespaceBytes, key),
      set: (key, value, ttlSeconds) => this.setInNamespace(namespaceBytes, key, value, ttlSeconds),
      delete: (key) => this.deleteInNamespace(namespaceBytes, key),
      getMany: (keys) => this.getManyInNamespace(namespaceBytes, keys),
      getManyBytes: (keys) => this.getManyBytesInNamespace(namespaceBytes, keys),
      setMany: (values, ttlSeconds) => this.setManyStringInNamespace(namespaceBytes, values, ttlSeconds),
      setManyBytes: (values, ttlSeconds) => this.setManyInNamespace(namespaceBytes, values, ttlSeconds),
      clear: () => this.clearInNamespace(namespaceBytes),
      incr: (key, delta) => this.incrInNamespace(namespaceBytes, key, delta),
      decr: (key, delta) => this.incrInNamespace(namespaceBytes, key, -delta),
      putIfAbsent: (key, value, ttlSeconds) => this.casInNamespace(namespaceBytes, key, value, { kind: "absent" }, ttlSeconds),
      replaceIfPresent: (key, value, ttlSeconds) => this.casInNamespace(namespaceBytes, key, value, { kind: "present" }, ttlSeconds),
      replace: (key, token, newValue, ttlSeconds) => this.casInNamespace(namespaceBytes, key, newValue, { kind: "digest", digest: token }, ttlSeconds),
      deleteIfMatches: (key, token) => this.casDeleteInNamespace(namespaceBytes, key, token),
    });
  }

  /** The names of `key`'s top-R owners in `namespace`, primary first
   * (client-side replication). Only meaningful in cluster mode. */
  private ownerNames(key: string | Uint8Array, namespace: Uint8Array): string[] {
    if (this.target.kind !== "cluster") return [];
    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    return this.target.ring.owners(keyBytes, this.target.replication, namespace);
  }

  /** Cluster read (client-side replication): ask the key's owners in rank order,
   * falling through to the next one only on a connection-level failure —
   * a replica is a hedge against a *dead* holder, not an extra lookup on
   * every miss (a `notFound` from a live owner is the answer). A `W`
   * propagates untouched: it means this client's routing table is stale,
   * which withWrongNodeRetry fixes with a refresh and one retry. */
  private async readFromOwners<T>(
    key: string | Uint8Array,
    namespace: Uint8Array,
    op: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    const names = this.ownerNames(key, namespace);

    // Hedged reads (issue #64): only meaningful with at least 2 owners —
    // with a single copy there is nobody to hedge to, so the plain
    // sequential path below runs unchanged.
    if (this.readHedgeAfterMs !== undefined && names.length > 1) {
      return this.readHedged(op, names);
    }

    let lastError: Error | null = null;

    for (const name of names) {
      let connection: Connection;
      try {
        connection = await this.memberConnection(name);
      } catch (error) {
        lastError = error as Error;
        continue;
      }

      try {
        return await op(connection);
      } catch (error) {
        if (error instanceof WrongNodeError) throw error;
        lastError = error as Error;
      }
    }

    throw lastError ?? new ConnectionLostError("nanocached: no owner is reachable for this key");
  }

  /** Hedged reads (issue #64): one slow — not dead — owner otherwise
   * bounds every read that touches it at its full round trip, since
   * `readFromOwners` only moves on to the next owner when the current one
   * *fails*. Here the read starts at the primary, and if no answer has
   * arrived within `readHedgeAfterMs` the same read is also sent to the
   * next owner (and so on, one more owner per interval); the first answer
   * decides:
   *
   * - a hit from any owner is final;
   * - a miss is final only from the primary — a replica's miss is
   *   provisional (it may simply lack a copy) and the primary is still
   *   waited for, so hedging never turns a hit into a miss; a miss is
   *   accepted only once every owner has answered or failed;
   * - a failure (anything but `WrongNodeError`) hedges onward
   *   immediately — the same fall-through condition `readFromOwners`
   *   uses;
   * - `WrongNodeError` propagates exactly as in `readFromOwners`.
   *
   * The losing legs are never cancelled — there is nothing to cancel a
   * pending Promise with, and doing so would risk poisoning a connection
   * mid-request anyway — they are left to finish detached in
   * `hedgedReads`, their outcome retrieved (so nothing surfaces as an
   * unhandled rejection), and drained by close(). */
  private async readHedged<T>(
    op: (connection: Connection) => Promise<T>,
    names: string[],
  ): Promise<T> {
    const hedgeAfterMs = this.readHedgeAfterMs;
    if (hedgeAfterMs === undefined) {
      throw new NanocachedError("nanocached: internal error — readHedged called without readHedgeAfterMs");
    }

    type Outcome = { index: number; ok: true; value: T } | { index: number; ok: false; error: unknown };

    // Wraps a leg so it never rejects (failures become { ok: false }
    // outcomes instead) — Promise.race below is then safe to use freely,
    // and a leg that outlives the read (a loser) can't trip Node's
    // unhandled-rejection detector, since its rejection is handled right
    // here, synchronously, in the same expression that creates it.
    const start = (index: number): Promise<Outcome> => {
      // Recheck this.closed immediately before registering, exactly as the
      // fire-and-forget replica path does (see registerBackgroundReplicaWrite),
      // so a read racing close() can't add a leg the drain has already passed
      // (issue #91): close() sets this.closed before draining hedgedReads, and
      // this check-and-add runs synchronously (Node is single-threaded), so
      // once this.closed is set no further leg is ever added and the drain
      // sees a set that only shrinks. A read that lost this race fails the
      // same way its own closed-check would have.
      if (this.closed) throw new AlreadyClosedError();
      const outcome: Promise<Outcome> = this.memberConnection(names[index])
        .then((connection) => op(connection))
        .then(
          (value): Outcome => ({ index, ok: true, value }),
          (error): Outcome => ({ index, ok: false, error }),
        );
      this.hedgedReads.add(outcome as Promise<unknown>);
      // Self-removing: once this leg settles — whether it's the winner or
      // a loser left running past the read's return — it no longer needs
      // close() to wait on it specifically (a loser still in flight stays
      // in the set via this same entry until then).
      outcome.finally(() => this.hedgedReads.delete(outcome as Promise<unknown>));
      return outcome;
    };

    let pending: Array<{ promise: Promise<Outcome>; index: number }> = [{ promise: start(0), index: 0 }];
    let nextIndex = 1;
    let lastError: unknown = null;
    let replicaMissed = false;

    while (pending.length > 0) {
      let timer: NodeJS.Timeout | null = null;
      const racers: Array<Promise<Outcome | "timeout">> = pending.map((leg) => leg.promise);
      if (nextIndex < names.length) {
        racers.push(
          new Promise<"timeout">((resolve) => {
            timer = setTimeout(() => resolve("timeout"), hedgeAfterMs);
          }),
        );
      }

      const winner = await Promise.race(racers);
      // Cleared unconditionally, whether the timer itself won the race or
      // a leg beat it — an uncleared timer would otherwise keep the event
      // loop alive until it eventually fires, long after this read (and
      // possibly this whole client) is done with it.
      if (timer !== null) clearTimeout(timer);

      if (winner === "timeout") {
        // Hedge interval elapsed with no answer: one more owner, no
        // change to `pending` for the legs already in flight.
        pending.push({ promise: start(nextIndex), index: nextIndex });
        nextIndex++;
        continue;
      }

      pending = pending.filter((leg) => leg.index !== winner.index);

      if (!winner.ok) {
        if (winner.error instanceof WrongNodeError) throw winner.error;
        lastError = winner.error;
      } else if (winner.value !== null || winner.index === 0) {
        return winner.value;
      } else {
        // A replica miss is provisional — it may simply lack the copy.
        replicaMissed = true;
      }

      if (pending.length === 0 && nextIndex < names.length) {
        // Everything so far failed or missed provisionally: the next
        // owner gets its turn right away, no waiting for the interval.
        pending.push({ promise: start(nextIndex), index: nextIndex });
        nextIndex++;
      }
    }

    if (replicaMissed) return null as T;
    throw lastError ?? new ConnectionLostError("nanocached: no owner is reachable for this key");
  }

  /** Cluster write (client-side replication): fan the operation out to every owner in
   * parallel. The primary's outcome is the operation's outcome — a
   * successful primary write is always what's returned, even if a
   * replica leg goes on to hit a genuine bug — replica failures are
   * swallowed — a dead replica must not fail writes, it just leaves the
   * key under-replicated until the next node-list refresh drops the dead
   * node out of the ranking. (A replica may also answer `W` when its own
   * membership view disagrees; equally ignorable — the refresh converges
   * everyone.) */
  private async writeToOwners<T>(
    key: string | Uint8Array,
    namespace: Uint8Array,
    op: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    const [primaryName, ...replicaNames] = this.ownerNames(key, namespace);
    if (primaryName === undefined) {
      throw new ConnectionLostError("nanocached: no owner is reachable for this key");
    }

    const replicaWrite = async (name: string): Promise<void> => {
      try {
        const connection = await this.memberConnection(name);
        await op(connection);
      } catch (error) {
        // Swallowed by design — see the doc comment. Counted in
        // stats().replicaWriteFailures, whether this leg ran
        // synchronously or as a fireAndForgetReplicas background write —
        // both paths share this function. An actual programming bug
        // (isSwallowable) still propagates.
        if (!isSwallowable(error)) throw error;
        this.replicaWriteFailures++;
      }
    };

    // Fire-and-forget replica writes: with fireAndForgetReplicas, up to
    // FIRE_AND_FORGET_TUNING.maxInFlight replica legs run in the
    // background instead of being waited for below — past that cap,
    // further legs fall back to the synchronous path exactly as with the
    // option off. `synchronousReplicaWrites` holds only the legs this
    // call always waits for; `backgroundLegs` holds the real fire-and-
    // forget leg promises (issue #188) — replicaWrite already maps a
    // swallowable failure to a resolve and only rejects on a genuine
    // programming bug, so no separate mapping is needed here. These are
    // deliberately kept OUT of synchronousReplicaWrites: awaiting them
    // unconditionally would delay every fireAndForgetReplicas write by
    // however long its slowest replica takes, defeating the option's
    // entire point (see "returns as soon as the primary acks" below).
    // They're only drained if the primary itself fails, at which point
    // the call is already failing and there's no success path left to
    // protect from delay.
    const synchronousReplicaWrites: Promise<void>[] = [];
    const backgroundLegs: Promise<void>[] = [];
    for (const name of replicaNames) {
      // `!this.closed` is rechecked here, synchronously and immediately
      // before this leg is ever registered in backgroundReplicaWrites —
      // no await happens between this check and the `.add()` below, so
      // close()'s own drain can't race a leg into existence after it
      // already took its snapshot (issue #47 item 3 / audit finding). When
      // closed, this falls through to the synchronous branch below instead,
      // exactly as if fireAndForgetReplicas were off — that leg is awaited
      // by this very call via Promise.allSettled below, so it can't outlive
      // close()'s teardown either.
      if (this.fireAndForgetReplicas && !this.closed && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
        const background = replicaWrite(name);
        // Now that replicaWrite can legitimately reject (a programming bug
        // — see isSwallowable), attach a rejection handler synchronously,
        // in the same tick this promise is created: without it, Node
        // would flag `background` as an unhandled rejection before
        // anything else gets a chance to observe it, since nothing else
        // awaits it until the `.finally` below, close()'s drain, or (on a
        // failed primary) the Promise.allSettled below runs, possibly
        // ticks later.
        const settled = background.catch(() => {});
        this.backgroundReplicaWrites.add(background);
        settled.finally(() => this.backgroundReplicaWrites.delete(background));
        backgroundLegs.push(background);
        continue;
      }
      const write = replicaWrite(name);
      // Same reasoning as above: attach a no-op catch synchronously so a
      // genuine programming bug surfacing here doesn't trip Node's
      // unhandled-rejection detector before the `Promise.allSettled`
      // below gets a chance to await this exact promise and propagate the
      // real error to the caller of set()/delete() — unless the primary
      // already succeeded, in which case that success wins instead (see
      // below).
      write.catch(() => {});
      synchronousReplicaWrites.push(write);
    }

    let primary: { ok: true; value: T } | { ok: false; error: unknown };
    try {
      const connection = await this.memberConnection(primaryName);
      primary = { ok: true, value: await op(connection) };
    } catch (error) {
      primary = { ok: false, error };
    }

    // Always drain the synchronous replica legs — for close()'s tracking,
    // and so a genuine replica-leg bug (isSwallowable) doesn't linger as
    // an unhandled rejection — but never let one override an
    // already-successful primary write: the write happened, so
    // set()/delete() throwing despite that would misreport a completed
    // write as failed (this used to be a plain `finally { await
    // Promise.all(...) }`, whose rejection silently replaced a
    // successful `return` from the try). A genuine replica bug is only
    // ever surfaced when the primary itself failed, same as before.
    const replicaResults = await Promise.allSettled(synchronousReplicaWrites);

    if (primary.ok) return primary.value;

    // The primary failed anyway, so there's nothing left to protect from
    // delay — drain the fire-and-forget legs here too (issue #188) so a
    // genuine bug on one of them surfaces instead of silently vanishing
    // just because it happened to run in the background.
    const backgroundResults = await Promise.allSettled(backgroundLegs);
    const replicaBug = [...replicaResults, ...backgroundResults].find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    throw replicaBug ? replicaBug.reason : primary.error;
  }

  /** Cluster INCR/DECR (issue #129) — deliberately **not**
   * `writeToOwners`'s same-op-to-every-owner pattern. `i` is sent to the
   * primary owner only; if it succeeds, the *literal result* (the new
   * value, and its TTL if any) is forwarded to the remaining owners as an
   * ordinary `set` — never replayed as `i` there. Replaying the increment
   * on a replica would let it drift from the primary (e.g. if an earlier
   * replica-leg write was dropped, or the replica separately evicted and
   * reset the key); forwarding the absolute result instead keeps every
   * replica byte-identical to the primary, the same reasoning the node's
   * own migration/decommission-handoff logic uses server-side.
   *
   * A miss or `NotNumericError` from the primary is returned/thrown
   * directly, before any replica is touched — nothing was written, so
   * there is nothing to fan out. A dead/wrong-node primary throws out to
   * `withWrongNodeRetry`, which retries this whole call once against a
   * freshly refreshed ranking — safe to retry in full, since a failure at
   * this point (by construction) always precedes ever reaching a replica
   * write, so no attempt can double-apply the increment. Replica-leg
   * failures are swallowed exactly like `writeToOwners`'s own
   * (stats().replicaWriteFailures, fireAndForgetReplicas-aware) — but
   * because the primary's increment already succeeded and was (at least
   * best-effort) forwarded, its result is always what's returned,
   * regardless of what happens on replicas.
   *
   * The replica `set` carries `result.raw` — the exact ASCII digit bytes
   * the primary answered with — never `String(result.value)`. `value` is
   * a `number`, which loses precision past `Number.MAX_SAFE_INTEGER`
   * (issue #224); re-encoding *that* for the replica would silently
   * diverge it from the primary's actual stored bytes once a counter grew
   * past 2^53. Returning `raw` alongside `value` (rather than deciding
   * here whether `value` is safe to hand back) lets `incrInNamespace`
   * apply that check once, after this fan-out, for both the cluster and
   * single/proxy paths. */
  private async incrOnOwners(
    key: string | Uint8Array,
    namespace: Uint8Array,
    delta: number,
  ): Promise<{ value: number; raw: Buffer } | null> {
    const [primaryName, ...replicaNames] = this.ownerNames(key, namespace);
    if (primaryName === undefined) {
      throw new ConnectionLostError("nanocached: no owner is reachable for this key");
    }

    const primaryConnection = await this.memberConnection(primaryName);
    const result = await primaryConnection.incr(key, delta, namespace);
    if (result === null || replicaNames.length === 0) {
      return result === null ? null : { value: result.value, raw: result.raw };
    }

    const valueBytes = result.raw;
    const ttlSeconds = result.ttlSeconds ?? 0;

    const replicaWrite = async (name: string): Promise<void> => {
      try {
        const connection = await this.memberConnection(name);
        await connection.set(key, valueBytes, ttlSeconds, namespace);
      } catch (error) {
        // Swallowed by design — see the doc comment above (and
        // writeToOwners', which this mirrors). An actual programming bug
        // (isSwallowable) still propagates.
        if (!isSwallowable(error)) throw error;
        this.replicaWriteFailures++;
      }
    };

    // Fire-and-forget replica writes (same cap/fallback as writeToOwners):
    // with fireAndForgetReplicas, up to FIRE_AND_FORGET_TUNING.maxInFlight
    // replica legs run in the background instead of being waited for
    // below — past that cap, further legs fall back to the synchronous
    // path exactly as with the option off.
    const replicaWrites = replicaNames.map((name) => {
      if (this.fireAndForgetReplicas && !this.closed && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
        const background = replicaWrite(name);
        const settled = background.catch(() => {});
        this.backgroundReplicaWrites.add(background);
        settled.finally(() => this.backgroundReplicaWrites.delete(background));
        return Promise.resolve();
      }
      const write = replicaWrite(name);
      // No-op catch attached synchronously, same reasoning as
      // writeToOwners: a genuine programming bug surfacing here must not
      // trip Node's unhandled-rejection detector before Promise.allSettled
      // below gets a chance to observe it.
      write.catch(() => {});
      return write;
    });

    // Drained for close()'s tracking and so a genuine replica-leg bug
    // doesn't linger as an unhandled rejection — but the primary's
    // increment already happened, so its result is always what's
    // returned; nothing here can turn it into a failure.
    await Promise.allSettled(replicaWrites);

    return { value: result.value, raw: result.raw };
  }

  /** Cluster compare-and-set (issue #141) — deliberately **not**
   * `writeToOwners`'s same-op-to-every-owner pattern, mirroring
   * `incrOnOwners` exactly: `k` is sent to the primary owner only. On a
   * condition mismatch, nothing was written, so nothing is replicated —
   * `false` is returned directly, before any replica is touched. On
   * success, the *literal new value* (and its TTL) is forwarded to the
   * remaining owners as an ordinary `set` — never replayed as `k` there.
   * A replica evaluating the same condition against its own possibly-
   * different copy could reach a different outcome than the primary just
   * did, so every replica always ends up with the exact bytes the primary
   * just stored, regardless of what its own prior copy held.
   *
   * A dead/wrong-node primary throws out to `withWrongNodeRetry`, which
   * retries this whole call once against a freshly refreshed ranking —
   * safe to retry in full, since a failure at this point (by construction)
   * always precedes ever reaching a replica write. Replica-leg failures
   * are swallowed exactly like `incrOnOwners`'s own
   * (stats().replicaWriteFailures, fireAndForgetReplicas-aware) — the
   * primary's write already succeeded, so its result is always what's
   * returned. */
  private async casOnOwners(key: string | Uint8Array, namespace: Uint8Array, value: Buffer, cond: CasCondition, ttlSeconds: number): Promise<boolean> {
    const [primaryName, ...replicaNames] = this.ownerNames(key, namespace);
    if (primaryName === undefined) {
      throw new ConnectionLostError("nanocached: no owner is reachable for this key");
    }

    const primaryConnection = await this.memberConnection(primaryName);
    const stored = await primaryConnection.cas(key, value, cond, ttlSeconds, namespace);
    if (!stored || replicaNames.length === 0) {
      return stored;
    }

    const replicaWrite = async (name: string): Promise<void> => {
      try {
        const connection = await this.memberConnection(name);
        await connection.set(key, value, ttlSeconds, namespace);
      } catch (error) {
        // Swallowed by design — see the doc comment above (and
        // incrOnOwners'/writeToOwners', which this mirrors). An actual
        // programming bug (isSwallowable) still propagates.
        if (!isSwallowable(error)) throw error;
        this.replicaWriteFailures++;
      }
    };

    // Fire-and-forget replica writes (same cap/fallback as
    // writeToOwners/incrOnOwners): with fireAndForgetReplicas, up to
    // FIRE_AND_FORGET_TUNING.maxInFlight replica legs run in the
    // background instead of being waited for below — past that cap,
    // further legs fall back to the synchronous path exactly as with the
    // option off.
    const replicaWrites = replicaNames.map((name) => {
      if (this.fireAndForgetReplicas && !this.closed && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
        const background = replicaWrite(name);
        const settled = background.catch(() => {});
        this.backgroundReplicaWrites.add(background);
        settled.finally(() => this.backgroundReplicaWrites.delete(background));
        return Promise.resolve();
      }
      const write = replicaWrite(name);
      // No-op catch attached synchronously, same reasoning as
      // writeToOwners/incrOnOwners: a genuine programming bug surfacing
      // here must not trip Node's unhandled-rejection detector before
      // Promise.allSettled below gets a chance to observe it.
      write.catch(() => {});
      return write;
    });

    // Drained for close()'s tracking and so a genuine replica-leg bug
    // doesn't linger as an unhandled rejection — but the primary's write
    // already happened, so its result is always what's returned; nothing
    // here can turn it into a failure.
    await Promise.allSettled(replicaWrites);

    return true;
  }

  /** Cluster compare-and-delete (issue #141) — the `x` counterpart to
   * `casOnOwners`, mirroring it exactly: only the primary evaluates the
   * digest; on success the deletion (not a replayed `x`) is forwarded to
   * the remaining owners as an ordinary `delete`, since a replica
   * evaluating the same digest against its own possibly-different copy
   * could reach a different outcome. */
  private async casDeleteOnOwners(key: string | Uint8Array, namespace: Uint8Array, token: string): Promise<boolean> {
    const [primaryName, ...replicaNames] = this.ownerNames(key, namespace);
    if (primaryName === undefined) {
      throw new ConnectionLostError("nanocached: no owner is reachable for this key");
    }

    const primaryConnection = await this.memberConnection(primaryName);
    const deleted = await primaryConnection.casDelete(key, token, namespace);
    if (!deleted || replicaNames.length === 0) {
      return deleted;
    }

    const replicaWrite = async (name: string): Promise<void> => {
      try {
        const connection = await this.memberConnection(name);
        await connection.delete(key, namespace);
      } catch (error) {
        if (!isSwallowable(error)) throw error;
        this.replicaWriteFailures++;
      }
    };

    const replicaWrites = replicaNames.map((name) => {
      if (this.fireAndForgetReplicas && !this.closed && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
        const background = replicaWrite(name);
        const settled = background.catch(() => {});
        this.backgroundReplicaWrites.add(background);
        settled.finally(() => this.backgroundReplicaWrites.delete(background));
        return Promise.resolve();
      }
      const write = replicaWrite(name);
      write.catch(() => {});
      return write;
    });

    await Promise.allSettled(replicaWrites);

    return true;
  }

  /** Shared fan-out for `clearAll()`/a namespace handle's `clear()`
   * (issue #106). Unlike get/set/delete, a clear isn't key-addressed —
   * a namespace's keys are spread across every node by HRW, so there's
   * no single owner (or owner set) to route to the way `ownerNames`
   * finds for a key. `send` is issued against every node in single
   * mode, just the one connection.
   *
   * In cluster mode: `send` goes out to every current member
   * concurrently (clearFanoutAttempt). Success requires every member to
   * have acked; if any failed — a dead connection, a timeout, anything
   * — the node list is refreshed once, the same refresh path a `W`/dead
   * primary retry uses (withWrongNodeRetry), and the clear is retried
   * against every member of the *refreshed* list (clearFanoutAttempt
   * reads `this.target.members` fresh each call, so this falls out
   * naturally once the refresh has swapped `target` in). A member that
   * still fails after that raises the SDK's normal error type, naming
   * it — never a silent partial clear, since a caller has no way to
   * tell which entries actually survived. The operation is idempotent,
   * so a caller that gets this error can simply retry it. refreshNodeList's
   * own failures are already counted in stats().refreshFailures, so
   * nothing new needs tracking here (per issue #106: reuse an existing
   * counter that fits rather than adding one). */
  private async fanoutClear(send: (connection: Connection) => Promise<void>): Promise<void> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();

    // Single connection either way (plain node, or one proxy — issue
    // #122): no fan-out, no retry-through-refresh — same as get/set/
    // delete's own non-cluster branch above.
    if (this.target.kind !== "cluster") {
      const connection = await this.connectionForSingleTarget();
      await send(connection);
      return;
    }

    let failedNodes = await this.clearFanoutAttempt(send);
    if (failedNodes.length === 0) return;

    await this.maybeRefreshNodeList({ force: true });
    failedNodes = await this.clearFanoutAttempt(send);
    if (failedNodes.length === 0) return;

    throw new NanocachedError(`nanocached: clear failed on node(s): ${failedNodes.join(", ")}`);
  }

  /** One pass of the clear fan-out: `send` against every node currently
   * in the cluster's member list, concurrently — returning the names of
   * whichever ones failed (see fanoutClear). Always reads
   * `this.target.members` fresh, so a second call after a node-list
   * refresh naturally targets the refreshed list, not the one this
   * fan-out started with. */
  private async clearFanoutAttempt(send: (connection: Connection) => Promise<void>): Promise<string[]> {
    if (this.target.kind !== "cluster") return [];
    const names = [...this.target.members.keys()];
    const results = await Promise.allSettled(names.map((name) => this.memberConnection(name).then(send)));
    return names.filter((_, index) => results[index].status === "rejected");
  }

  /** Runs `operation`; if a routed-to node answers `W` (staged node join: it
   * doesn't hold this key per its own view of cluster membership — this
   * client's routing table is stale), forces a node-list refresh and
   * retries the whole operation once against the fresh ranking. A second
   * `W` after a *fresh* refresh is unusual enough (this client, the
   * routed-to node, and discovery all disagreeing right after resyncing)
   * that retrying further would likely just mask a real problem, so that
   * error propagates. In single mode there's no discovery to refresh
   * from, so `W` propagates immediately — see `WrongNodeError`. Proxy
   * mode (issue #122) *does* have discovery to fall back on — a
   * connection-level failure there forces a `Q` re-fetch and a retry
   * against whichever proxy that lands on, the same shape as the cluster
   * case (`WrongNodeError` itself can't happen against a proxy, which
   * never answers `W`, but the shared connection-error branch still
   * applies). */
  private async withWrongNodeRetry<T>(operation: () => Promise<T>): Promise<T> {
    try {
      return await operation();
    } catch (error) {
      // Connection-level failures retry the same way `W` does: the usual
      // cause is a node death that discovery has since noticed, so a
      // forced refresh re-ranks the key onto survivors. The retry window
      // for a dead primary is therefore bounded by discovery's liveness
      // timeout. A second failure after a fresh refresh propagates.
      const retryable = error instanceof WrongNodeError || isConnectionError(error);
      if (!retryable || this.target.kind === "single") throw error;
      await this.maybeRefreshNodeList({ force: true });
      return await operation();
    }
  }

  /** No-op in single mode. In cluster mode, re-fetches the node list from
   * discovery if it's older than NODE_LIST_STALE_AFTER_MS, or unconditionally
   * when `force` is set (see withWrongNodeRetry). In proxy mode (issue
   * #122) there's no periodic staleness check — a single proxy connection
   * doesn't drift the way cluster membership does — so this is a no-op
   * there too unless `force` is set, which only ever happens after the
   * connected proxy has already died (`withWrongNodeRetry`). Concurrent
   * callers that both need a refresh share one in-flight refresh
   * (nodeListRefresh is set synchronously, before the first await, so a
   * second caller arriving before the first refresh resolves sees it
   * already set) rather than each starting their own — including a
   * `force` call arriving while an ordinary staleness-triggered refresh is
   * already in flight, which is still enough to satisfy it (either way,
   * the node/proxy list ends up current). */
  private async maybeRefreshNodeList(options?: { force?: boolean }): Promise<void> {
    if (this.target.kind === "single") return;
    if (this.target.kind === "proxy") {
      if (!options?.force) return;
    } else if (!options?.force && Date.now() - this.lastNodeListFetch < NODE_LIST_STALE_AFTER_MS) {
      return;
    }

    if (this.nodeListRefresh) {
      await this.nodeListRefresh;
      return;
    }

    this.nodeListRefresh = this.target.kind === "proxy" ? this.refreshProxyTarget() : this.refreshNodeList();
    try {
      await this.nodeListRefresh;
    } finally {
      this.nodeListRefresh = null;
    }
  }

  /** Re-fetches the node list and reconciles `target`'s ring/connections
   * to match: closes connections to nodes no longer listed, opens
   * connections to newly listed ones, and leaves unchanged nodes' existing
   * connections (and any in-flight requests on them) alone.
   *
   * By design, a discovery outage should degrade only topology updates,
   * not already-established cache traffic — so failure here (discovery
   * unreachable, or a specific new node failing to connect) never throws
   * out to the get/set/delete call that triggered it. It fails silently
   * (each such failure counted in stats().refreshFailures — see
   * ClientStats), keeps the current target as-is (skipping just the node
   * that failed to connect, if only one did), and tries again on the next
   * stale check. */
  private async refreshNodeList(): Promise<void> {
    if (this.target.kind !== "cluster") return;
    const currentMembers = this.target.members;

    const identified = await this.fetchNodeList();
    if (identified === null) {
      this.lastNodeListFetch = Date.now();
      return;
    }

    // Reconciled by name (node identity decoupled from address), not address — see `Target`.
    const nodeByName = new Map<string, DiscoveredNode>(identified.nodes.map((node) => [node.name, node]));
    const members = new Map<string, ClusterMember>(currentMembers);

    for (const [name, member] of currentMembers) {
      if (!nodeByName.has(name)) {
        // `null` here just means this member never had a live connection
        // (issue #67) — nothing to close.
        member.connection?.close();
        members.delete(name);
        // A departed node's address is never reused (names/addresses are
        // per-process), so its per-address cooldown entry would otherwise
        // linger forever in a churny deployment (issue #96).
        this.reconnectCooldowns.delete(member.address);
      }
    }

    for (const node of identified.nodes) {
      const existing = members.get(node.name);
      if (existing) {
        // Same name means the same node process (names are per-process
        // UUIDs), but keep the address current for lazy reconnects anyway.
        existing.address = node.address;
        continue;
      }

      try {
        const nodeIdentified = await connectAndIdentify({ ...splitHostPort(node.address), authSecret: this.authSecret, tls: this.tls, ca: this.ca });

        if (nodeIdentified.kind !== "node") {
          // Discovery returned an address that no longer identifies as a
          // cache node — skip it silently, same as any other failure to
          // connect here (see the doc comment above), and count it in
          // stats().refreshFailures.
          this.refreshFailures++;
          continue;
        }

        if (this.closed) {
          // close() ran while we were dialing (issue #10): installing this
          // socket now would leak it — nothing will ever close it again.
          nodeIdentified.socket.destroy();
          return;
        }

        trackOpenTarget(this.url, [nodeIdentified.socket]);
        members.set(node.name, { address: node.address, connection: new Connection(nodeIdentified.socket, nodeIdentified.tagged, () => this.transientRetries++) });
      } catch (error) {
        // Connecting to this new node failed — skip it silently and retry
        // on the next refresh (see the doc comment above), counted in
        // stats().refreshFailures. An actual programming bug
        // (isSwallowable) still propagates.
        if (!isSwallowable(error)) throw error;
        this.refreshFailures++;
      }
    }

    if (this.closed) {
      // Same race, caught at commit time: close() already tore down the
      // members it knew about; anything newly opened here must die too.
      for (const member of members.values()) member.connection?.close();
      return;
    }

    this.target = {
      kind: "cluster",
      ring: new HashRing([...members.keys()]),
      members,
      replication: identified.replication,
    };
    this.nodeUrls = identified.nodes.filter((node) => members.has(node.name)).map((node) => node.address);
    this.lastNodeListFetch = Date.now();
  }

  /** Walks every configured address (discovery HA) in order for a fresh node
   * list. Returns `null` — keep the last-known list — when none can
   * provide one: unreachable, still inside its startup grace (`B`), no
   * longer a discovery server, or knowing no live nodes. Fails silently;
   * see the doc comment on refreshNodeList. Each unreachable/erroring
   * address is counted in stats().refreshFailures. */
  private async fetchNodeList(): Promise<{ nodes: DiscoveredNode[]; replication: number } | null> {
    for (const address of this.addresses) {
      let identified;
      try {
        identified = await connectAndIdentify({ host: address.host, port: address.port, authSecret: this.authSecret, tls: this.tls, ca: this.ca });
      } catch (error) {
        // An actual programming bug (isSwallowable) still propagates.
        if (!isSwallowable(error)) throw error;
        this.refreshFailures++;
        continue;
      }

      if (identified.kind !== "cluster") {
        identified.socket.destroy();
        continue;
      }

      if (identified.nodes.length === 0) {
        // A discovery server that's up but knows no live nodes is just as
        // unusable for this refresh as one that's unreachable — count it
        // the same way, matching the initial connect() path's treatment
        // of an empty node list as a real failure (issue #47 audit item
        // 7), not a silent skip.
        this.refreshFailures++;
        continue;
      }

      return identified;
    }

    return null;
  }

  /** SDK proxy mode's counterpart to `fetchNodeList` (issue #122): walks
   * every configured address for a fresh proxy roster via `Q` instead of
   * `L`. Same "keep the last-known list, never throw" contract —
   * unreachable, still warming up (`B`), no longer a discovery server, or
   * registering zero proxies are all just reasons to try the next
   * address, counted in stats().refreshFailures; an actual programming
   * bug (isSwallowable) still propagates. */
  private async fetchProxyList(): Promise<DiscoveredNode[] | null> {
    for (const address of this.addresses) {
      let result;
      try {
        result = await connectAndListProxies({ host: address.host, port: address.port, authSecret: this.authSecret, tls: this.tls, ca: this.ca });
      } catch (error) {
        if (!isSwallowable(error)) throw error;
        this.refreshFailures++;
        continue;
      }

      if (result.kind !== "cluster") {
        // A configured address stopped identifying as a discovery server
        // — connect() is what hard-errors on this at bootstrap (issue
        // #122); here it's just another reason this address can't serve
        // the refresh, same as an unreachable one.
        this.refreshFailures++;
        continue;
      }

      if (result.proxies.length === 0) {
        this.refreshFailures++;
        continue;
      }

      return result.proxies;
    }

    return null;
  }

  /** Proxy mode's reconnect-on-loss (issue #122): only ever reached via a
   * *forced* `maybeRefreshNodeList`, itself only triggered once
   * `proxyConnection`'s own same-address redial has already failed (see
   * `withWrongNodeRetry`) — so this always starts from "the connected
   * proxy is confirmed unreachable, right now." Re-fetches the roster
   * (`fetchProxyList`) and dials candidates in random order — the same
   * random-spread/failover shape `connectViaProxy`'s bootstrap uses —
   * until one connects, swapping it into `target`.
   *
   * Mirrors `refreshNodeList`'s failure contract: never throws to the
   * caller (an actual programming bug still propagates), and when nothing
   * reachable turns up, simply leaves `target` holding its already-dead
   * connection — the retried operation's own `proxyConnection` call then
   * redials that same (still dead) address once more and surfaces a real
   * connection error, rather than this method hanging or manufacturing a
   * misleading success. */
  private async refreshProxyTarget(): Promise<void> {
    if (this.target.kind !== "proxy") return;

    const proxies = await this.fetchProxyList();
    if (proxies === null) {
      this.lastNodeListFetch = Date.now();
      return;
    }

    for (const proxy of shuffled(proxies)) {
      if (this.closed) return; // close() ran while we were dialing (issue #10-style race)

      let identified;
      try {
        identified = await connectAndIdentify({ ...splitHostPort(proxy.address), authSecret: this.authSecret, tls: this.tls, ca: this.ca });
      } catch (error) {
        if (!isSwallowable(error)) throw error;
        this.refreshFailures++;
        continue;
      }

      if (identified.kind !== "node") {
        // A listed proxy that no longer identifies as one — skip it like
        // any other bad candidate, same as fetchProxyList's own handling
        // of a stale address.
        this.refreshFailures++;
        continue;
      }

      if (this.closed) {
        identified.socket.destroy();
        return;
      }

      trackOpenTarget(this.url, [identified.socket]);
      this.target = { kind: "proxy", connection: new Connection(identified.socket, identified.tagged, () => this.transientRetries++), address: proxy.address };
      this.nodeUrls = [proxy.address];
      this.lastNodeListFetch = Date.now();
      return;
    }

    // Every candidate was unreachable — see the doc comment above.
    this.lastNodeListFetch = Date.now();
  }

  /** The "ensure connected" path (issue #1) for a single-node target: if
   * the one connection has died since it was opened — most commonly the
   * server's 60s idle timeout — reconnect to the same node first.
   * Reconnecting is lazy (nothing watches for closes in the background)
   * and shared (concurrent requests finding the same dead connection
   * await one dial, see `reconnects`). */
  private async singleConnection(): Promise<Connection> {
    if (this.target.kind !== "single") {
      throw new NanocachedError("nanocached: internal error — singleConnection on a cluster target");
    }
    if (!this.target.connection.isClosed()) return this.target.connection;

    const connection = await this.ensureConnected("", this.url);
    if (this.target.kind === "single" && this.target.connection.isClosed()) {
      this.target.connection = connection;
    }
    return connection;
  }

  /** The proxy-mode "ensure connected" path (issue #122): the same shape
   * as `singleConnection` — redial first before doing anything more
   * drastic, since the proxy may have simply restarted — except the
   * address it redials is `target.address` rather than the fixed
   * `this.url`: a `refreshProxyTarget` can have swapped it to a different
   * proxy since the connection this call finds dead was opened. */
  private async proxyConnection(): Promise<Connection> {
    if (this.target.kind !== "proxy") {
      throw new NanocachedError("nanocached: internal error — proxyConnection on a non-proxy target");
    }
    if (!this.target.connection.isClosed()) return this.target.connection;

    const connection = await this.ensureConnected("", this.target.address);
    if (this.target.kind === "proxy" && this.target.connection.isClosed()) {
      this.target.connection = connection;
    }
    return connection;
  }

  /** Dispatches to whichever single-connection path applies —
   * `singleConnection` for a plain node target, `proxyConnection` for a
   * proxy one (issue #122) — so get/set/delete/clear's non-cluster branch
   * doesn't need its own three-way `target.kind` check at every call
   * site. Never called in cluster mode. */
  private connectionForSingleTarget(): Promise<Connection> {
    return this.target.kind === "proxy" ? this.proxyConnection() : this.singleConnection();
  }

  /** The cluster-mode "ensure connected" path: the named member's
   * connection, redialing its last-known address first if the connection
   * has died (same laziness and sharing as `singleConnection`). Every
   * read and every write leg funnels through here, per owner. */
  private async memberConnection(name: string): Promise<Connection> {
    if (this.target.kind !== "cluster") {
      throw new NanocachedError("nanocached: internal error — memberConnection on a single target");
    }

    const member = this.target.members.get(name);
    if (!member) {
      // Connection-classified (issue #8): the usual cause is a refresh
      // racing this operation, which the refresh-and-retry layer heals.
      throw new ConnectionLostError(`nanocached: ${name} has no open connection`);
    }
    // `null` (issue #67: this member was listed by discovery but
    // unreachable at bootstrap, or has since died mid-life) is handled
    // exactly like a closed connection below — ensureConnected redials,
    // respecting the reconnect cooldown armed for its address.
    if (member.connection !== null && !member.connection.isClosed()) return member.connection;

    const connection = await this.ensureConnected(name, member.address);

    // A node-list refresh may have swapped `target` while we dialed; adopt
    // the new connection only into a member still holding the dead (or
    // absent) one, and defer to the refresh's own connection otherwise so
    // no socket is left open but untracked.
    const current = this.target.kind === "cluster" ? this.target.members.get(name) : null;
    if (!current) {
      connection.close();
      throw new NanocachedError(`nanocached: ${name} left the cluster while reconnecting`);
    }
    const currentConnection = current.connection;
    if (currentConnection === null || currentConnection.isClosed()) {
      current.connection = connection;
      return connection;
    }
    if (currentConnection !== connection) connection.close();
    return currentConnection;
  }

  private async ensureConnected(slot: string, address: string): Promise<Connection> {
    const inFlight = this.reconnects.get(slot);
    if (inFlight) return inFlight;

    // Per-address reconnect cooldown (see reconnectCooldowns' own doc
    // comment): an address whose dial just failed stays "down" for
    // reconnectCooldownMs, so a burst of requests routed to it — or one
    // request every keep-alive tick — fails immediately with the same
    // error the dial itself produced, instead of each paying another full
    // CONNECT_DEADLINE_MS in turn.
    const cooldown = this.reconnectCooldowns.get(address);
    if (cooldown && Date.now() < cooldown.until) {
      throw cooldown.error;
    }

    const attempt = this.openNodeConnection(address).then(
      (connection) => {
        this.reconnectCooldowns.delete(address);
        return connection;
      },
      (error: Error) => {
        this.reconnectCooldowns.set(address, { until: Date.now() + this.reconnectCooldownMs, error });
        throw error;
      },
    );
    this.reconnects.set(slot, attempt);
    try {
      return await attempt;
    } finally {
      this.reconnects.delete(slot);
    }
  }

  private async openNodeConnection(address: string): Promise<Connection> {
    const identified = await connectAndIdentify({ ...splitHostPort(address), authSecret: this.authSecret, tls: this.tls, ca: this.ca });

    if (identified.kind !== "node") {
      throw new NanocachedError(`nanocached: ${address} no longer identifies as a cache node`);
    }
    if (this.closed) {
      identified.socket.destroy();
      throw new AlreadyClosedError();
    }

    trackOpenTarget(this.url, [identified.socket]);
    return new Connection(identified.socket, identified.tagged, () => this.transientRetries++);
  }

  /** See KEEPALIVE_TUNING. Each tick pings only
   * connections that are open (dead ones stay lazy, reconnected on use)
   * and that real traffic has left idle for at least a full interval. Any
   * parseable reply proves liveness and resets the server's idle timer —
   * `N` from a node without the key, or `W` from a clustered node that
   * doesn't own it (there is no dedicated ping in the wire protocol, so
   * the ping is a real `G` against KEEPALIVE_KEY, a byte sequence reserved
   * by the SDKs so it can never collide with a real application key — a
   * `G` does refresh the server's LRU recency of whatever key it names,
   * so a collision would have silently reset a real key's recency every
   * tick) — hence errors are swallowed rather than routed through the
   * wrong-node retry. */
  private startKeepAlive(intervalMs: number): void {
    const timer = setInterval(() => {
      const connections =
        // "single" and "proxy" (issue #122) are both one bare connection.
        this.target.kind !== "cluster"
          ? [this.target.connection]
          : [...this.target.members.values()]
              .map((member) => member.connection)
              // `null` (issue #67): no connection to ping — stays lazy,
              // dialed on the next request that routes there.
              .filter((connection): connection is Connection => connection !== null);

      for (const connection of connections) {
        if (connection.isClosed()) continue;
        if (connection.idleMs() < intervalMs) continue;
        connection.get(KEEPALIVE_KEY).catch(() => {});
      }
    }, intervalMs);

    // A keep-alive timer must never be what keeps the process running.
    timer.unref();
    this.keepAliveTimer = timer;
  }
}

// Reserved for keep-alive pings: 0x00 followed by the ASCII bytes of
// "nanocached-keepalive" (21 bytes total) — not just a single NUL, which
// is itself a valid application key. A keep-alive `G` refreshes the
// server's LRU recency of whatever key it names (see startKeepAlive), so
// an app that happened to use key "\x00" would have had its recency
// silently reset every tick; this longer, namespaced sequence is reserved
// by the SDKs so a real application key can never collide with it.
const KEEPALIVE_KEY = Uint8Array.from([0, ...Buffer.from("nanocached-keepalive", "ascii")]);
