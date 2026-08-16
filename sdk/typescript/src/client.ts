import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { Connection, WrongNodeError } from "./connection.js";
import { connectAndIdentify, type DiscoveredNode } from "./identify.js";
import { HashRing } from "./hashRing.js";
import type { NanocachedTlsOptions } from "./socket.js";

export type { NanocachedTlsOptions } from "./socket.js";
export { WrongNodeError } from "./connection.js";

/** Thrown by get/set/delete when called after close(). Not thrown by
 * close() itself, which is idempotent (see NanocachedClient.close). */
export class AlreadyClosedError extends Error {
  constructor() {
    super("nanocached: this client is closed");
    this.name = "AlreadyClosedError";
  }
}

export interface NanocachedSeed {
  host: string;
  port: number;
}

export interface NanocachedClientOptions {
  host?: string;
  port?: number;
  /** Discovery replicas (ADR-0010), tried in order — an alternative to
   * `host`/`port` for clusters running more than one discovery server.
   * Both the initial connect and every later node-list refresh walk this
   * list until a seed provides a node list, so losing any one replica
   * costs nothing. A seed that answers `B` (still inside its startup
   * grace after a restart) is skipped the same way as an unreachable
   * one. */
  seeds?: NanocachedSeed[];
  /** Shared secret to authenticate with, matching NANOCACHED_AUTH_SECRET
   * on the server. Omit if the server has no secret configured. */
  authSecret?: string | Uint8Array;
  /** Connect over TLS instead of plaintext — required if the server was
   * started with --tls-cert/--tls-key. `boolean`, not just the literal
   * `true`, so a single config value (e.g. an env var) can toggle this
   * across environments without an `x ? true : undefined` workaround: pass
   * `true`/`false` to verify (or not) against Node's default, publicly-
   * trusted CA store — the normal case either way — or `{ ca }` if the
   * server is running a self-signed certificate with no CA-issued
   * alternative available. */
  tls?: boolean | NanocachedTlsOptions;
  /** Opt-in keep-alive: every `keepAliveIntervalMs`, send a lightweight
   * request on each connection that real traffic has left idle for at
   * least that long. nanocached-node closes connections idle for 30s
   * (hardcoded), so pick something comfortably below that — e.g. 10—15s.
   * Without this, an idle connection is simply closed by the server and
   * transparently reopened on the next request (one extra round trip);
   * keep-alive is purely a latency optimization, at the cost of putting
   * background load on every node from every long-lived client. */
  keepAliveIntervalMs?: number;
}

function splitHostPort(address: string): { host: string; port: number } {
  const separator = address.lastIndexOf(":");
  if (separator === -1) {
    throw new Error(`nanocached: invalid node address from discovery server: ${address}`);
  }

  const host = address.slice(0, separator);
  const port = Number(address.slice(separator + 1));
  if (!Number.isInteger(port)) {
    throw new Error(`nanocached: invalid node address from discovery server: ${address}`);
  }

  return { host, port };
}

interface ClusterMember {
  /** Last-known address for this node name — kept so a connection the
   * server closed (e.g. its 30s idle timeout) can be reopened lazily on
   * the next request that routes here, without waiting for a node-list
   * refresh. */
  address: string;
  connection: Connection;
}

type Target =
  | { kind: "single"; connection: Connection }
  // `members` is keyed by node *name* (doc/adr/0009-*.md), matching
  // what `ring.route()` returns — not by address, which carries no
  // identity meaning and is only used to open connections.
  | { kind: "cluster"; ring: HashRing; members: Map<string, ClusterMember> };

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
 * A client for nanocached: `host`/`port` may name either a single
 * nanocached-node or a nanocached-discovery server fronting a cluster —
 * `connect()` doesn't take a separate option or shape for either case, it
 * finds out from the server's own response to the connection handshake
 * (see doc/adr/0007-*.md). Callers never need to know or care which
 * they're talking to.
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

  /** The node(s) actually being talked to, by address (for display/
   * introspection — routing itself uses each node's name, not its
   * address, see doc/adr/0009-*.md): `[url]` in single mode, or the set of
   * nodes this instance currently holds a connection to in cluster mode —
   * kept current by maybeRefreshNodeList(), which reconciles `target`'s
   * ring/connections to match (see refreshNodeList). */
  nodeUrls: readonly string[];

  private constructor(
    private target: Target,
    /** The seed that answered connect() — a node's own address in single
     * mode (which is also what a lazy reconnect redials), the winning
     * discovery server's address in cluster mode. */
    readonly url: string,
    nodeUrls: readonly string[],
    /** Every configured discovery seed (ADR-0010) — what fetchNodeList
     * walks on a refresh, not just the seed that happened to win the
     * initial connect. */
    private readonly seeds: readonly NanocachedSeed[],
    private readonly authSecret: string | Uint8Array | undefined,
    private readonly tls: boolean | NanocachedTlsOptions | undefined,
    keepAliveIntervalMs: number | undefined,
  ) {
    this.nodeUrls = nodeUrls;
    if (keepAliveIntervalMs !== undefined) this.startKeepAlive(keepAliveIntervalMs);
  }

  static async connect(options: NanocachedClientOptions): Promise<NanocachedClient> {
    if (
      options.keepAliveIntervalMs !== undefined &&
      (!Number.isInteger(options.keepAliveIntervalMs) || options.keepAliveIntervalMs <= 0)
    ) {
      // Same reasoning as encodeSet's TTL check: fail synchronously on a
      // value that could never be meant, before opening any connection.
      throw new RangeError(
        `nanocached: keepAliveIntervalMs must be a positive integer, got ${options.keepAliveIntervalMs}`,
      );
    }

    const seeds: NanocachedSeed[] =
      options.seeds ??
      (options.host !== undefined && options.port !== undefined
        ? [{ host: options.host, port: options.port }]
        : []);
    if (seeds.length === 0) {
      throw new Error("nanocached: connect() needs either host/port or a non-empty seeds list");
    }

    // Walk the seeds in order until one yields a working target. A seed
    // is skipped when it's unreachable, answers `B` (ADR-0010 startup
    // grace), or knows no live nodes — the next replica may do better.
    let lastError: Error | null = null;

    for (const seed of seeds) {
      const key = targetKey(seed);
      if (openTargets.has(key)) {
        console.warn(
          `nanocached: connect() called for ${key} while a previous connection to it is still open — was close() forgotten?`,
        );
      }

      let identified;
      try {
        identified = await connectAndIdentify({ host: seed.host, port: seed.port, authSecret: options.authSecret, tls: options.tls });
      } catch (error) {
        lastError = error as Error;
        continue;
      }

      if (identified.kind === "node") {
        if (seeds.length > 1) {
          // Multiple seeds imply the caller expected redundancy, but a
          // node target pins the client to exactly this one server: the
          // remaining seeds don't form a cluster, and a later death of
          // this node is redialed, never failed over. Direct node targets
          // are for development or single-node deployments — clusters
          // should seed discovery servers.
          console.warn(
            `nanocached: ${key} is a cache node, so this client is pinned to that single server — ` +
              `the ${seeds.length - 1} remaining seed(s) will not be used. ` +
              `Point seeds at discovery servers for cluster routing and failover.`,
          );
        }

        trackOpenTarget(key, [identified.socket]);
        return new NanocachedClient(
          { kind: "single", connection: new Connection(identified.socket) },
          key,
          [key],
          seeds,
          options.authSecret,
          options.tls,
          options.keepAliveIntervalMs,
        );
      }

      if (identified.nodes.length === 0) {
        lastError = new Error(`nanocached: no live nodes registered with the discovery server at ${key}`);
        continue;
      }

      // Keyed by name (doc/adr/0009-*.md), not address — see `Target`.
      const sockets = new Map<string, Socket | TLSSocket>();

      try {
        for (const node of identified.nodes) {
          const { host, port } = splitHostPort(node.address);
          const nodeIdentified = await connectAndIdentify({ host, port, authSecret: options.authSecret, tls: options.tls });

          if (nodeIdentified.kind !== "node") {
            throw new Error(`nanocached: discovery server returned a non-node address: ${node.address}`);
          }

          sockets.set(node.name, nodeIdentified.socket);
        }
      } catch (error) {
        // A node (not the discovery seed) is the problem here; another
        // seed would hand back the same node list, so don't try one.
        for (const socket of sockets.values()) socket.destroy();
        throw error;
      }

      trackOpenTarget(key, [...sockets.values()]);

      const members = new Map<string, ClusterMember>();
      for (const node of identified.nodes) {
        members.set(node.name, { address: node.address, connection: new Connection(sockets.get(node.name)!) });
      }

      return new NanocachedClient(
        {
          kind: "cluster",
          ring: new HashRing(identified.nodes.map((node) => node.name)),
          members,
        },
        key,
        identified.nodes.map((node) => node.address),
        seeds,
        options.authSecret,
        options.tls,
        options.keepAliveIntervalMs,
      );
    }

    throw lastError ?? new Error("nanocached: could not connect to any seed");
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

    if (this.target.kind === "single") {
      this.target.connection.close();
      return;
    }

    for (const member of this.target.members.values()) member.connection.close();
  }

  async get(key: string | Uint8Array): Promise<Buffer | null> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    return this.withWrongNodeRetry(key, (connection) => connection.get(key));
  }

  async set(key: string | Uint8Array, value: string | Uint8Array, options?: { ttlSeconds?: number }): Promise<void> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    return this.withWrongNodeRetry(key, (connection) => connection.set(key, value, options));
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    if (this.closed) throw new AlreadyClosedError();
    await this.maybeRefreshNodeList();
    return this.withWrongNodeRetry(key, (connection) => connection.delete(key));
  }

  /** Runs `op` against `key`'s currently-routed connection. If the node
   * answers `W` (ADR-0008: it no longer owns this key per its own view of
   * cluster membership — this client's routing table is stale), forces a
   * node-list refresh and retries once against the freshly-routed
   * connection. A second `W` after a *fresh* refresh is unusual enough
   * (this client, the routed-to node, and discovery all disagreeing right
   * after resyncing) that retrying further would likely just mask a real
   * problem, so that error propagates. In single mode there's no
   * discovery to refresh from, so `W` propagates immediately — see
   * `WrongNodeError`. */
  private async withWrongNodeRetry<T>(
    key: string | Uint8Array,
    op: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    try {
      return await op(await this.routedConnection(key));
    } catch (error) {
      if (!(error instanceof WrongNodeError) || this.target.kind !== "cluster") throw error;
      await this.maybeRefreshNodeList({ force: true });
      return await op(await this.routedConnection(key));
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
   * out to the get/set/delete call that triggered it. It logs a warning,
   * keeps the current target as-is (skipping just the node that failed to
   * connect, if only one did), and tries again on the next stale check. */
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
        const nodeIdentified = await connectAndIdentify({ ...splitHostPort(node.address), authSecret: this.authSecret, tls: this.tls });

        if (nodeIdentified.kind !== "node") {
          console.warn(`nanocached: discovery server returned a non-node address: ${node.address}, skipping`);
          continue;
        }

        trackOpenTarget(this.url, [nodeIdentified.socket]);
        members.set(node.name, { address: node.address, connection: new Connection(nodeIdentified.socket) });
      } catch (error) {
        console.warn(`nanocached: could not connect to new node ${node.address}, will retry on the next refresh: ${(error as Error).message}`);
      }
    }

    this.target = { kind: "cluster", ring: new HashRing([...members.keys()]), members };
    this.nodeUrls = identified.nodes.filter((node) => members.has(node.name)).map((node) => node.address);
    this.lastNodeListFetch = Date.now();
  }

  /** Walks every discovery seed (ADR-0010) in order for a fresh node
   * list. Returns `null` — keep the last-known list — when no seed can
   * provide one: unreachable, still inside its startup grace (`B`), no
   * longer a discovery server, or knowing no live nodes. */
  private async fetchNodeList(): Promise<{ nodes: DiscoveredNode[] } | null> {
    for (const seed of this.seeds) {
      const key = targetKey(seed);

      let identified;
      try {
        identified = await connectAndIdentify({ host: seed.host, port: seed.port, authSecret: this.authSecret, tls: this.tls });
      } catch (error) {
        console.warn(`nanocached: could not refresh the node list from ${key}: ${(error as Error).message}`);
        continue;
      }

      if (identified.kind !== "cluster") {
        identified.socket.destroy();
        console.warn(`nanocached: ${key} no longer identifies as a discovery server, skipping`);
        continue;
      }

      if (identified.nodes.length === 0) {
        console.warn(`nanocached: discovery server at ${key} returned no live nodes, skipping`);
        continue;
      }

      return identified;
    }

    console.warn("nanocached: no discovery seed could provide a node list, keeping the last-known list");
    return null;
  }

  /** The single "ensure connected" path (issue #1) every request funnels
   * through: routes `key` to its connection and, if that connection has
   * died since it was opened — most commonly the server's 30s idle
   * timeout — reconnects to the same node first. Reconnecting is lazy
   * (nothing watches for closes in the background) and shared (concurrent
   * requests finding the same dead connection await one dial, see
   * `reconnects`). */
  private async routedConnection(key: string | Uint8Array): Promise<Connection> {
    if (this.target.kind === "single") {
      if (!this.target.connection.isClosed()) return this.target.connection;

      const connection = await this.ensureConnected("", this.url);
      if (this.target.kind === "single" && this.target.connection.isClosed()) {
        this.target.connection = connection;
      }
      return connection;
    }

    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    const name = this.target.ring.route(keyBytes);

    const member = this.target.members.get(name);
    if (!member) {
      throw new Error(`nanocached: ring routed to ${name}, which has no open connection`);
    }
    if (!member.connection.isClosed()) return member.connection;

    const connection = await this.ensureConnected(name, member.address);

    // A node-list refresh may have swapped `target` while we dialed; adopt
    // the new connection only into a member still holding the dead one, and
    // re-route (or defer to the refresh's own connection) otherwise so no
    // socket is left open but untracked.
    const current = this.target.kind === "cluster" ? this.target.members.get(name) : null;
    if (!current) {
      connection.close();
      return this.routedConnection(key);
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
    const identified = await connectAndIdentify({ ...splitHostPort(address), authSecret: this.authSecret, tls: this.tls });

    if (identified.kind !== "node") {
      throw new Error(`nanocached: ${address} no longer identifies as a cache node`);
    }
    if (this.closed) {
      identified.socket.destroy();
      throw new AlreadyClosedError();
    }

    trackOpenTarget(this.url, [identified.socket]);
    return new Connection(identified.socket);
  }

  /** See NanocachedClientOptions.keepAliveIntervalMs. Each tick pings only
   * connections that are open (dead ones stay lazy, reconnected on use)
   * and that real traffic has left idle for at least a full interval. Any
   * parseable reply proves liveness and resets the server's idle timer —
   * `N` from a node without the key, or `W` from a clustered node that
   * doesn't own it (there is no dedicated ping in the wire protocol, so
   * the ping is a real, harmless `G`) — hence errors are swallowed rather
   * than routed through the wrong-node retry. */
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

// The server rejects empty keys, so the keep-alive `G` needs at least one
// byte; a single NUL keeps it out of the way of any real key space.
const KEEPALIVE_KEY = Uint8Array.from([0]);
