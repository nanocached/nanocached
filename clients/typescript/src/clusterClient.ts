import { NanocachedClient } from "./client.js";
import { fetchNodes } from "./discovery.js";
import { HashRing } from "./hashRing.js";
import type { NanocachedTlsOptions } from "./socket.js";

export interface NanocachedClusterOptions {
  /** Address of the discovery server (nanocached-discovery) to fetch the
   * current node list from. */
  discoveryHost: string;
  discoveryPort: number;
  /** Shared secret to authenticate with — used for both the discovery
   * server and every cache node, matching NANOCACHED_AUTH_SECRET. A
   * cluster's discovery server and nodes are expected to share one secret,
   * the same way nanocached-node's own heartbeat connection does. */
  authSecret?: string | Uint8Array;
  /** TLS options, applied uniformly to the discovery server and every
   * cache node — a nanocached cluster is either fully on TLS or fully off,
   * there's no per-connection mixing. */
  tls?: boolean | NanocachedTlsOptions;
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

/**
 * A client for a nanocached cluster fronted by a discovery server: fetches
 * the current node list once at connect time, opens one connection to each
 * node, and routes each key to a node with the same consistent-hash ring
 * `bench.rs` uses on the Rust side (see hashRing.ts). Like bench.rs, this
 * doesn't react to nodes joining or leaving after `connect()` — the node
 * list and ring are both fixed for the lifetime of the instance.
 */
export class NanocachedClusterClient {
  private constructor(
    private readonly ring: HashRing,
    private readonly connections: ReadonlyMap<string, NanocachedClient>,
  ) {}

  static async connect(options: NanocachedClusterOptions): Promise<NanocachedClusterClient> {
    const nodes = await fetchNodes({
      host: options.discoveryHost,
      port: options.discoveryPort,
      authSecret: options.authSecret,
      tls: options.tls,
    });

    if (nodes.length === 0) {
      throw new Error(
        `nanocached: no live nodes registered with discovery server at ${options.discoveryHost}:${options.discoveryPort}`,
      );
    }

    const connections = new Map<string, NanocachedClient>();

    try {
      for (const node of nodes) {
        const { host, port } = splitHostPort(node);
        const client = await NanocachedClient.connect({
          host,
          port,
          authSecret: options.authSecret,
          tls: options.tls,
        });
        connections.set(node, client);
      }
    } catch (error) {
      for (const client of connections.values()) client.close();
      throw error;
    }

    return new NanocachedClusterClient(new HashRing(nodes), connections);
  }

  async get(key: string | Uint8Array): Promise<Buffer | null> {
    return this.route(key).get(key);
  }

  async set(
    key: string | Uint8Array,
    value: string | Uint8Array,
    options?: { ttlSeconds?: number },
  ): Promise<void> {
    return this.route(key).set(key, value, options);
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    return this.route(key).delete(key);
  }

  close(): void {
    for (const client of this.connections.values()) client.close();
  }

  private route(key: string | Uint8Array): NanocachedClient {
    const keyBytes = typeof key === "string" ? Buffer.from(key, "utf8") : Buffer.from(key);
    const node = this.ring.route(keyBytes);

    const client = this.connections.get(node);
    if (!client) {
      throw new Error(`nanocached: ring routed to ${node}, which has no open connection`);
    }

    return client;
  }
}
