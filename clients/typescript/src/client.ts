import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { connectAndIdentify } from "./identify.js";
import { HashRing } from "./hashRing.js";
import type { NanocachedTlsOptions } from "./socket.js";

export type { NanocachedTlsOptions } from "./socket.js";

export interface NanocachedClientOptions {
  host: string;
  port: number;
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

type Target =
  | { kind: "single"; socket: Socket | TLSSocket }
  | { kind: "cluster"; ring: HashRing; sockets: ReadonlyMap<string, Socket | TLSSocket> };

function targetKey(options: { host: string; port: number }): string {
  return `${options.host}:${options.port}`;
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

/**
 * A client for nanocached: `host`/`port` may name either a single
 * nanocached-node or a nanocached-discovery server fronting a cluster —
 * `connect()` doesn't take a separate option or shape for either case, it
 * finds out from the server's own response to the connection handshake
 * (see doc/adr/0007-*.md). Callers never need to know or care which
 * they're talking to.
 *
 * This only establishes the connection(s) and routing table; it
 * deliberately doesn't yet expose `get`/`set`/`delete`/`close` — those are
 * a separate, not-yet-authorized piece of work.
 */
export class NanocachedClient {
  private constructor(private readonly target: Target) {}

  static async connect(options: NanocachedClientOptions): Promise<NanocachedClient> {
    const key = targetKey(options);
    if (openTargets.has(key)) {
      console.warn(
        `nanocached: connect() called for ${key} while a previous connection to it is still open — was close() forgotten?`,
      );
    }

    const identified = await connectAndIdentify(options);

    if (identified.kind === "node") {
      trackOpenTarget(key, [identified.socket]);
      return new NanocachedClient({ kind: "single", socket: identified.socket });
    }

    if (identified.nodes.length === 0) {
      throw new Error(`nanocached: no live nodes registered with the discovery server at ${options.host}:${options.port}`);
    }

    const sockets = new Map<string, Socket | TLSSocket>();

    try {
      for (const nodeAddress of identified.nodes) {
        const { host, port } = splitHostPort(nodeAddress);
        const nodeIdentified = await connectAndIdentify({ host, port, authSecret: options.authSecret, tls: options.tls });

        if (nodeIdentified.kind !== "node") {
          throw new Error(`nanocached: discovery server returned a non-node address: ${nodeAddress}`);
        }

        sockets.set(nodeAddress, nodeIdentified.socket);
      }
    } catch (error) {
      for (const socket of sockets.values()) socket.destroy();
      throw error;
    }

    trackOpenTarget(key, [...sockets.values()]);

    return new NanocachedClient({
      kind: "cluster",
      ring: new HashRing(identified.nodes),
      sockets,
    });
  }
}
