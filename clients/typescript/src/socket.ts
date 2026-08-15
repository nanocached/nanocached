import { connect as netConnect, type Socket } from "node:net";
import { connect as tlsConnect, type TLSSocket } from "node:tls";

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
}

/** Opens a plain TCP or TLS connection and resolves once it's usable
 * (connected, or connected *and* the TLS handshake has completed). Shared
 * by NanocachedClient (the cache protocol) and fetchNodes (the discovery
 * protocol) — both processes speak the same auth handshake over whichever
 * transport this returns. */
export async function connectSocket(options: ConnectSocketOptions): Promise<Socket | TLSSocket> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => reject(error);

    const socket = options.tls
      ? tlsConnect({
          host: options.host,
          port: options.port,
          ...(options.tls === true ? {} : { ca: options.tls.ca }),
        })
      : netConnect({ host: options.host, port: options.port });

    socket.once("error", onError);
    socket.once(options.tls ? "secureConnect" : "connect", () => {
      socket.removeListener("error", onError);
      resolve(socket);
    });
  });
}
