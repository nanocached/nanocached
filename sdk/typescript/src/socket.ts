import { connect as netConnect, type Socket } from "node:net";
import { connect as tlsConnect, type TLSSocket } from "node:tls";
import { ConnectionLostError } from "./connection.js";

/** Default bound on dial (and TLS handshake), matching the Go and Java
 * SDKs. Without it, a node whose IP has been reclaimed (a stopped
 * container, a dead cloud instance) blackholes the TCP connect and a
 * caller hangs for the kernel's own timeout — minutes — instead of
 * failing over. */
export const CONNECT_DEADLINE_MS = 10_000;

export interface NanocachedTlsOptions {
  /** PEM-encoded certificate(s) to trust when the server has no
   * CA-issued certificate available (e.g. local development, or a private
   * cluster with no PKI of its own) and runs with a self-signed
   * certificate instead. This *replaces* Node's default (publicly-trusted)
   * CA store rather than adding to it — that's how Node's own
   * `tls.connect` treats an explicit `ca`. Matches nanocached-node's own
   * --tls-ca option. Leave unset/false (use `tls: true`) whenever the
   * server's certificate is issued by a trusted CA. */
  ca: string | Buffer | Array<string | Buffer>;
}

export interface ConnectSocketOptions {
  host: string;
  port: number;
  /** `boolean`, not just the literal `true`, so a single config value
   * (e.g. an env var) can toggle this across environments without an
   * `x ? true : undefined` workaround. `true`/`false` verify (or not)
   * against Node's default, publicly-trusted CA store; `{ ca }` trusts
   * only a private/self-signed certificate instead. */
  tls?: boolean | NanocachedTlsOptions;
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
          ...(options.tls === true ? {} : { ca: options.tls.ca }),
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
