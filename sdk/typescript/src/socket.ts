import { connect as netConnect, type Socket } from "node:net";
import { connect as tlsConnect, type TLSSocket } from "node:tls";
import { ConnectionLostError } from "./connection.js";

/** Default bound on dial (and TLS handshake), matching the Go and Java
 * SDKs. Without it, a node whose IP has been reclaimed (a stopped
 * container, a dead cloud instance) blackholes the TCP connect and a
 * caller hangs for the kernel's own timeout — minutes — instead of
 * failing over. */
export const CONNECT_DEADLINE_MS = 5_000;

export interface ConnectSocketOptions {
  host: string;
  port: number;
  /** Connect over TLS instead of plaintext. Plain `boolean` so a single
   * config value (e.g. an env var) can toggle this across environments. */
  tls?: boolean;
  /** PEM-encoded trusted root certificate(s), already read from disk by
   * the caller (see `NanocachedClientOptions.ca`) — this *replaces* Node's
   * default (publicly-trusted) CA store rather than adding to it, matching
   * how Node's own `tls.connect` treats an explicit `ca`. Only meaningful
   * when `tls` is true; ignored otherwise. Leave unset to verify against
   * Node's default, publicly-trusted CA store — the normal case whenever
   * the server's certificate is issued by a trusted CA. */
  ca?: Buffer;
  /** Bound on the dial (and TLS handshake); defaults to
   * `CONNECT_DEADLINE_MS`. Exposed for tests. */
  connectDeadlineMs?: number;
}

/** Opens a plain TCP or TLS connection and resolves once it's usable
 * (connected, or connected *and* the TLS handshake has completed). */
export async function connectSocket(options: ConnectSocketOptions): Promise<Socket | TLSSocket> {
  return new Promise((resolve, reject) => {
    const deadline = options.connectDeadlineMs ?? CONNECT_DEADLINE_MS;
    const timer = setTimeout(() => {
      socket.destroy();
      reject(
        new ConnectionLostError(
          `nanocached: connecting to ${options.host}:${options.port} timed out after ${deadline}ms`,
        ),
      );
    }, deadline);
    const onError = (error: Error) => {
      clearTimeout(timer);
      reject(error);
    };

    const socket = options.tls
      ? tlsConnect({
          host: options.host,
          port: options.port,
          ...(options.ca !== undefined ? { ca: options.ca } : {}),
        })
      : netConnect({ host: options.host, port: options.port });

    socket.once("error", onError);
    socket.once(options.tls ? "secureConnect" : "connect", () => {
      clearTimeout(timer);
      socket.removeListener("error", onError);
      resolve(socket);
    });
  });
}
