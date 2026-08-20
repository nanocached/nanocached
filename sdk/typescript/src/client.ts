import { readFileSync } from "node:fs";
import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { Connection, ConnectionLostError, isConnectionError, WrongNodeError } from "./connection.js";
import { connectAndIdentify, type DiscoveredNode } from "./identify.js";
import { HashRing } from "./hashRing.js";
import { compressValue, decompressValue } from "./compression.js";
import { NanocachedError } from "./errors.js";

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
 * (ADR-0011/0014/0015) instead of raising them to a caller — observability
 * for silently degrading replication or a stuck node-list refresh, which
 * would otherwise be invisible. See NanocachedClient.stats(). */
export interface ClientStats {
  /** Replica-leg write failures swallowed during a cluster write
   * (writeToOwners), whether the leg ran synchronously or as a
   * fireAndForgetReplicas background write (doc/adr/0011-*.md,
   * doc/adr/0014-*.md). */
  replicaWriteFailures: number;
  /** Failures swallowed while probing owners or writing back the repaired
   * value during read repair (doc/adr/0015-*.md). */
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
   * addresses (ADR-0010), tried in order. A one-element list is the
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
   * and decompress them on `get`/`getBytes` (doc/adr/0013-*.md). Off by
   * default. **Every client that reads or writes a given set of keys must
   * agree on this setting** — it is a per-keyspace format decision, not a
   * per-client preference; see the ADR's Consequences before enabling
   * this against an existing keyspace another client may still touch
   * with `compress` off. */
  compress?: boolean;
  /** Values shorter than this (in bytes) are never compressed — the
   * per-value overhead of attempting it outweighs the savings. Only
   * meaningful when `compress` is true. Default 256. */
  compressionThreshold?: number;
  /** Let `set`/`delete` return as soon as the primary owner acks,
   * letting replica legs finish in the background instead of waiting
   * for them too (doc/adr/0014-*.md). Off by default. Unlike `compress`,
   * this is a pure latency/durability trade for this client's own
   * writes — it carries no wire format and needs no agreement with other
   * clients. */
  fireAndForgetReplicas?: boolean;
  /** On a clean miss (the key's first-reached owner reports it missing),
   * probe the remaining owners before accepting that, and repair the
   * primary in the background if one still has the value
   * (doc/adr/0015-*.md). Off by default. Costs extra reads only on the
   * misses this actually applies to. */
  readRepair?: boolean;
}

const DEFAULT_COMPRESSION_THRESHOLD = 256;

// TTL a read-repair write uses (doc/adr/0015-*.md), in whole seconds —
// the protocol's TTL unit throughout (see encodeSet in protocol.ts). The
// original TTL isn't recoverable from a GET response, and repairing with
// TTL 0 (no expiry) would permanently resurrect data that was legitimately
// expiring; 60s bounds the overshoot instead — an immortal key just gets
// re-repaired on a later miss. Cross-SDK policy decision, applied
// identically across all SDKs.
const READ_REPAIR_TTL_SECONDS = 60;

/** Bounds how many replica writes a single client may have running in
 * the background at once when `fireAndForgetReplicas` is enabled
 * (doc/adr/0014-*.md) — once the cap is reached, further replica legs
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
  if (!Number.isInteger(port)) {
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
  connection: Connection;
}

type Target =
  | { kind: "single"; connection: Connection }
  // `members` is keyed by node *name* (doc/adr/0009-*.md), matching what
  // `ring.owners()` returns — not by address, which carries no identity
  // meaning and is only used to open connections. `replication` is
  // discovery's R (ADR-0011), learned from the same `L` response as the
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

/**
 * A client for nanocached: each configured address may name either a
 * single nanocached-node or a nanocached-discovery server fronting a
 * cluster — `connect()` doesn't take a separate option or shape for either
 * case, it finds out from the server's own response to the connection
 * handshake (see doc/adr/0007-*.md). Callers never need to know or care
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
  private keepAliveTimer: NodeJS.Timeout | null = null;

  // Backing counters for stats()/ClientStats — see its doc comment.
  private replicaWriteFailures = 0;
  private readRepairFailures = 0;
  private refreshFailures = 0;

  /** The node(s) actually being talked to, by address (for display/
   * introspection — routing itself uses each node's name, not its
   * address, see doc/adr/0009-*.md): `[url]` in single mode, or the set of
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
    /** Every configured address (ADR-0010) — what fetchNodeList walks on a
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
  ) {
    this.nodeUrls = nodeUrls;
    this.startKeepAlive(KEEPALIVE_TUNING.intervalMs);
  }

  /** doc/adr/0014-*.md: replica writes currently running in the
   * background (fireAndForgetReplicas) — close() drains these before
   * tearing down connections instead of abandoning them. */
  private readonly backgroundReplicaWrites = new Set<Promise<void>>();

  static async connect(options: NanocachedClientOptions): Promise<NanocachedClient> {
    const addresses = options.addresses ?? [];
    if (addresses.length === 0) {
      throw new NanocachedError("nanocached: connect() needs a non-empty addresses list");
    }

    // ca is meaningful only paired with tls: true; a set ca with tls not
    // enabled is silently ignored rather than an error. Read once here
    // (not per-dial) and reused for every connection this instance ever
    // opens, including reconnects and node-list refreshes.
    const ca = options.tls === true && options.ca !== undefined ? readFileSync(options.ca) : undefined;
    const compress = options.compress === true;
    const compressionThreshold = options.compressionThreshold ?? DEFAULT_COMPRESSION_THRESHOLD;

    // Walk the addresses in order until one yields a working target. An
    // address is skipped when it's unreachable, answers `B` (ADR-0010
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
        );
      }

      if (identified.nodes.length === 0) {
        lastError = new NanocachedError(`nanocached: no live nodes registered with the discovery server at ${key}`);
        continue;
      }

      // Keyed by name (doc/adr/0009-*.md), not address — see `Target`.
      const sockets = new Map<string, { socket: Socket | TLSSocket; tagged: boolean }>();

      try {
        for (const node of identified.nodes) {
          const { host, port } = splitHostPort(node.address);
          const nodeIdentified = await connectAndIdentify({ host, port, authSecret: options.authSecret, tls: options.tls, ca });

          if (nodeIdentified.kind !== "node") {
            throw new NanocachedError(`nanocached: discovery server returned a non-node address: ${node.address}`);
          }

          sockets.set(node.name, { socket: nodeIdentified.socket, tagged: nodeIdentified.tagged });
        }
      } catch (error) {
        // A node (not the discovery address) is the problem here; another
        // address would hand back the same node list, so don't try one.
        for (const { socket } of sockets.values()) socket.destroy();
        throw error;
      }

      trackOpenTarget(key, [...sockets.values()].map(({ socket }) => socket));

      const members = new Map<string, ClusterMember>();
      for (const node of identified.nodes) {
        const { socket, tagged } = sockets.get(node.name)!;
        members.set(node.name, { address: node.address, connection: new Connection(socket, tagged) });
      }

      return new NanocachedClient(
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
      );
    }

    throw lastError ?? new NanocachedError("nanocached: could not connect to any address");
  }

  /** Whether close() has already been called on this instance. */
  isClosed(): boolean {
    return this.closed;
  }

  close(): void {
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

    // doc/adr/0014-*.md: give background replica writes a chance to
    // finish before their connections are torn out from under them —
    // close() stays synchronous (it already can't block on this
    // runtime), so the teardown itself is just deferred, not awaited.
    if (this.backgroundReplicaWrites.size > 0) {
      void Promise.allSettled([...this.backgroundReplicaWrites]).then(() => this.teardownConnections());
      return;
    }
    this.teardownConnections();
  }

  private teardownConnections(): void {
    if (this.target.kind === "single") {
      this.target.connection.close();
      return;
    }

    for (const member of this.target.members.values()) member.connection.close();
  }

  /** How many nodes hold each key (ADR-0011) — discovery's replication
   * factor in cluster mode, 1 against a single node. */
  get replication(): number {
    return this.target.kind === "cluster" ? this.target.replication : 1;
  }

  /** Observability for failures this client swallows by design
   * (ADR-0011/0014/0015) — lets operators detect silently degrading
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
   * enabled (doc/adr/0013-*.md). With `readRepair`, a clean miss probes
   * the remaining owners before being accepted as final
   * (doc/adr/0015-*.md). */
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

  /** doc/adr/0015-*.md: probes every owner of `key`, in rank order, for a
   * value the normal read path already reported missing. The first
   * owner that has it wins: its value is returned, and — detached, not
   * awaited, no tracking — that same value repairs `names[0]` (the true
   * primary) in the background, with TTL READ_REPAIR_TTL_SECONDS (the
   * original TTL can't be recovered from a GET, and TTL 0 would
   * permanently resurrect already-expired data). Every failure along the
   * way (connection lost, WrongNode, another miss) is swallowed; only a
   * failed repair *write-back* is counted in stats().readRepairFailures —
   * a failed owner probe is silent, matching the counter's write-back
   * semantics in the other five SDKs (issue #43). Nothing here may turn
   * an already-accepted miss into an error — except an actual programming
   * bug (isSwallowable), which still propagates. */
  private async tryReadRepair(key: string | Uint8Array): Promise<Buffer | null> {
    const names = this.ownerNames(key);
    for (const name of names) {
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
      if (primaryName !== undefined) {
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
        // a routine swallow to anything inspecting stats()).
        repaired.catch(() => {});
      }
      return value;
    }
    return null;
  }

  /** `ttlSeconds` (whole seconds, default 0) is when the key expires; 0
   * means no expiry. Must be a non-negative integer. Transparently
   * compresses values at or above `compressionThreshold` when `compress`
   * is enabled (doc/adr/0013-*.md). */
  async set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<void> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    const outgoing = this.compress
      ? compressValue(typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value), this.compressionThreshold)
      : value;
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

  /** The names of `key`'s top-R owners, primary first (ADR-0011). Only
   * meaningful in cluster mode. */
  private ownerNames(key: string | Uint8Array): string[] {
    if (this.target.kind !== "cluster") return [];
    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    return this.target.ring.owners(keyBytes, this.target.replication);
  }

  /** Cluster read (ADR-0011): ask the key's owners in rank order,
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

  /** Cluster write (ADR-0011): fan the operation out to every owner in
   * parallel. The primary's outcome is the operation's outcome; replica
   * failures are swallowed — a dead replica must not fail writes, it just
   * leaves the key under-replicated until the next node-list refresh
   * drops the dead node out of the ranking. (A replica may also answer
   * `W` when its own membership view disagrees; equally ignorable — the
   * refresh converges everyone.) */
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

    // doc/adr/0014-*.md: with fireAndForgetReplicas, up to
    // FIRE_AND_FORGET_TUNING.maxInFlight replica legs run in the
    // background instead of being waited for below — past that cap,
    // further legs fall back to the synchronous path exactly as with the
    // option off.
    const synchronousReplicaWrites = replicaNames.map((name) => {
      if (this.fireAndForgetReplicas && this.backgroundReplicaWrites.size < FIRE_AND_FORGET_TUNING.maxInFlight) {
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
      // unhandled-rejection detector before the `finally` below gets a
      // chance to await this exact promise and propagate the real error
      // to the caller of set()/delete().
      write.catch(() => {});
      return write;
    });

    try {
      const connection = await this.memberConnection(primaryName);
      return await op(connection);
    } finally {
      await Promise.all(synchronousReplicaWrites);
    }
  }

  /** Runs `operation`; if a routed-to node answers `W` (ADR-0008: it
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
   * Per ADR-2, a discovery outage should degrade only topology updates,
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

    // Reconciled by name (doc/adr/0009-*.md), not address — see `Target`.
    const nodeByName = new Map<string, DiscoveredNode>(identified.nodes.map((node) => [node.name, node]));
    const members = new Map<string, ClusterMember>(currentMembers);

    for (const [name, member] of currentMembers) {
      if (!nodeByName.has(name)) {
        member.connection.close();
        members.delete(name);
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
      for (const member of members.values()) member.connection.close();
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

  /** Walks every configured address (ADR-0010) in order for a fresh node
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
    if (!member.connection.isClosed()) return member.connection;

    const connection = await this.ensureConnected(name, member.address);

    // A node-list refresh may have swapped `target` while we dialed; adopt
    // the new connection only into a member still holding the dead one,
    // and defer to the refresh's own connection otherwise so no socket is
    // left open but untracked.
    const current = this.target.kind === "cluster" ? this.target.members.get(name) : null;
    if (!current) {
      connection.close();
      throw new NanocachedError(`nanocached: ${name} left the cluster while reconnecting`);
    }
    if (current.connection.isClosed()) {
      current.connection = connection;
      return connection;
    }
    if (current.connection !== connection) connection.close();
    return current.connection;
  }

  private async ensureConnected(slot: string, address: string): Promise<Connection> {
    const inFlight = this.reconnects.get(slot);
    if (inFlight) return inFlight;

    const attempt = this.openNodeConnection(address);
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
          : [...this.target.members.values()].map((member) => member.connection);

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
