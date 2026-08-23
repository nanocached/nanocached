import { readFileSync } from "node:fs";
import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { Connection, ConnectionLostError, isConnectionError, WrongNodeError } from "./connection.js";
import { connectAndIdentify, type DiscoveredNode } from "./identify.js";
import { HashRing } from "./hashRing.js";
import { compressValue, decompressValue } from "./compression.js";
import { NanocachedError } from "./errors.js";
import { checkKeyAndValue } from "./protocol.js";

export { ConnectionLostError, WrongNodeError } from "./connection.js";
export { NanocachedError } from "./errors.js";
export { DecompressionError } from "./compression.js";

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
   * non-positive value is rejected at `connect()`. */
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
  // `members` is keyed by node *name* (node identity decoupled from address), matching what
  // `ring.owners()` returns — not by address, which carries no identity
  // meaning and is only used to open connections. `replication` is
  // discovery's R (client-side replication), learned from the same `L` response as the
  // member list.
  | { kind: "cluster"; ring: HashRing; members: Map<string, ClusterMember>; replication: number };

function targetKey(options: { host: string; port: number }): string {
  return `${options.host}:${options.port}`;
}

// How long a cluster client's node list may go without being re-fetched
// from discovery before get/set/delete refreshes it first. Checked lazily
// on use rather than on a timer — see NanocachedClient.maybeRefreshNodeList.
const NODE_LIST_STALE_AFTER_MS = 30_000;

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
        return new NanocachedClient(
          { kind: "single", connection: new Connection(identified.socket, identified.tagged) },
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
      return client;
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
    if (this.target.kind === "single") {
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
    };
  }

  /** Resolves the value strictly decoded as UTF-8 — a value that isn't
   * valid UTF-8 rejects (native `TypeError` from `TextDecoder`'s fatal
   * mode), it is never silently replaced. Use `getBytes` for raw bytes,
   * e.g. for values this client didn't itself write as a UTF-8 string. */
  async get(key: string | Uint8Array): Promise<string | null> {
    const value = await this.getBytes(key);
    return value === null ? null : UTF8_DECODER.decode(value);
  }

  /** The raw-bytes companion to `get`: same routing/retry/cluster
   * behavior, no decoding. Transparently decompresses when `compress` is
   * enabled (value compression). With `readRepair`, a clean miss probes
   * the remaining owners before being accepted as final
   * (read repair). */
  async getBytes(key: string | Uint8Array): Promise<Buffer | null> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    let value = await this.withWrongNodeRetry(() =>
      this.target.kind === "single"
        ? this.singleConnection().then((connection) => connection.get(key))
        : this.readFromOwners(key, (connection) => connection.get(key)),
    );
    if (value === null && this.readRepair && this.target.kind === "cluster") {
      value = await this.tryReadRepair(key);
    }
    if (value === null || !this.compress) return value;
    return decompressValue(value);
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
  private async tryReadRepair(key: string | Uint8Array): Promise<Buffer | null> {
    const names = this.ownerNames(key);
    // Every owner but the primary, which the normal read path already
    // probed and got a clean miss from (same as the Rust, Go, Java and
    // .NET SDKs).
    for (const name of names.slice(1)) {
      let value: Buffer | null;
      try {
        const connection = await this.memberConnection(name);
        value = await connection.get(key);
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
          .then((connection) => connection.set(key, repairValue, READ_REPAIR_TTL_SECONDS))
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
    checkKeyAndValue(keyBytes, valueBytes);
    const outgoing = this.compress ? compressValue(valueBytes, this.compressionThreshold) : valueBytes;
    return this.withWrongNodeRetry(() =>
      this.target.kind === "single"
        ? this.singleConnection().then((connection) => connection.set(key, outgoing, ttlSeconds))
        : this.writeToOwners(key, (connection) => connection.set(key, outgoing, ttlSeconds)),
    );
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    return this.withWrongNodeRetry(() =>
      this.target.kind === "single"
        ? this.singleConnection().then((connection) => connection.delete(key))
        : this.writeToOwners(key, (connection) => connection.delete(key)),
    );
  }

  /** The names of `key`'s top-R owners, primary first (client-side replication). Only
   * meaningful in cluster mode. */
  private ownerNames(key: string | Uint8Array): string[] {
    if (this.target.kind !== "cluster") return [];
    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    return this.target.ring.owners(keyBytes, this.target.replication);
  }

  /** Cluster read (client-side replication): ask the key's owners in rank order,
   * falling through to the next one only on a connection-level failure —
   * a replica is a hedge against a *dead* holder, not an extra lookup on
   * every miss (a `notFound` from a live owner is the answer). A `W`
   * propagates untouched: it means this client's routing table is stale,
   * which withWrongNodeRetry fixes with a refresh and one retry. */
  private async readFromOwners<T>(
    key: string | Uint8Array,
    op: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    const names = this.ownerNames(key);

    // Hedged reads (issue #64): only meaningful with at least 2 owners —
    // with a single copy there is nobody to hedge to, so the plain
    // sequential path below runs unchanged.
    if (this.readHedgeAfterMs !== undefined && names.length > 1) {
      return this.readHedged(key, op, names);
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
    key: string | Uint8Array,
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
    op: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    const [primaryName, ...replicaNames] = this.ownerNames(key);
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
    // option off.
    const synchronousReplicaWrites = replicaNames.map((name) => {
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
        // awaits it until the `.finally` below (or close()'s drain) runs,
        // possibly ticks later. There is no caller left to propagate a
        // background write's failure to by the time it settles anyway
        // (set() already returned), so this is a no-op rather than a real
        // handler.
        const settled = background.catch(() => {});
        this.backgroundReplicaWrites.add(background);
        settled.finally(() => this.backgroundReplicaWrites.delete(background));
        return Promise.resolve();
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
      return write;
    });

    let primary: { ok: true; value: T } | { ok: false; error: unknown };
    try {
      const connection = await this.memberConnection(primaryName);
      primary = { ok: true, value: await op(connection) };
    } catch (error) {
      primary = { ok: false, error };
    }

    // Always drain the replica legs — for close()'s tracking, and so a
    // genuine replica-leg bug (isSwallowable) doesn't linger as an
    // unhandled rejection — but never let one override an
    // already-successful primary write: the write happened, so
    // set()/delete() throwing despite that would misreport a completed
    // write as failed (this used to be a plain `finally { await
    // Promise.all(...) }`, whose rejection silently replaced a
    // successful `return` from the try). A genuine replica bug is only
    // ever surfaced when the primary itself failed, same as before.
    const replicaResults = await Promise.allSettled(synchronousReplicaWrites);

    if (primary.ok) return primary.value;

    const replicaBug = replicaResults.find((result): result is PromiseRejectedResult => result.status === "rejected");
    throw replicaBug ? replicaBug.reason : primary.error;
  }

  /** Runs `operation`; if a routed-to node answers `W` (staged node join: it
   * doesn't hold this key per its own view of cluster membership — this
   * client's routing table is stale), forces a node-list refresh and
   * retries the whole operation once against the fresh ranking. A second
   * `W` after a *fresh* refresh is unusual enough (this client, the
   * routed-to node, and discovery all disagreeing right after resyncing)
   * that retrying further would likely just mask a real problem, so that
   * error propagates. In single mode there's no discovery to refresh
   * from, so `W` propagates immediately — see `WrongNodeError`. */
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
      if (!retryable || this.target.kind !== "cluster") throw error;
      await this.maybeRefreshNodeList({ force: true });
      return await operation();
    }
  }

  /** No-op in single mode. In cluster mode, re-fetches the node list from
   * discovery if it's older than NODE_LIST_STALE_AFTER_MS, or unconditionally
   * when `force` is set (see withWrongNodeRetry). Concurrent callers that
   * both need a refresh share one in-flight refresh (nodeListRefresh is set
   * synchronously, before the first await, so a second caller arriving
   * before the first refresh resolves sees it already set) rather than each
   * starting their own — including a `force` call arriving while an
   * ordinary staleness-triggered refresh is already in flight, which is
   * still enough to satisfy it (either way, the node list ends up current). */
  private async maybeRefreshNodeList(options?: { force?: boolean }): Promise<void> {
    if (this.target.kind !== "cluster") return;
    if (!options?.force && Date.now() - this.lastNodeListFetch < NODE_LIST_STALE_AFTER_MS) return;

    if (this.nodeListRefresh) {
      await this.nodeListRefresh;
      return;
    }

    this.nodeListRefresh = this.refreshNodeList();
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
        members.set(node.name, { address: node.address, connection: new Connection(nodeIdentified.socket, nodeIdentified.tagged) });
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
    return new Connection(identified.socket, identified.tagged);
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
        this.target.kind === "single"
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
