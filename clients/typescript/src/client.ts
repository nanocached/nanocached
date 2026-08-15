import { connect as netConnect, type Socket } from "node:net";
import { connect as tlsConnect, type TLSSocket } from "node:tls";
import {
  encodeAuth,
  encodeDelete,
  encodeGet,
  encodeSet,
  tryParseResponse,
  type ParsedResponse,
} from "./protocol.js";

export interface NanocachedTlsOptions {
  /** PEM-encoded certificate(s) to trust when the server has no
   * CA-issued certificate available (e.g. local development, or a private
   * cluster with no PKI of its own) and runs with a self-signed
   * certificate instead. This *replaces* Node's default (publicly-trusted)
   * CA store rather than adding to it — that's how Node's own
   * `tls.connect` treats an explicit `ca`. Matches nanocached-node's own
   * --tls-ca option. Leave unset (use `tls: true`) whenever the server's
   * certificate is issued by a trusted CA. */
  ca: string | Buffer | Array<string | Buffer>;
}

export interface NanocachedClientOptions {
  host: string;
  port: number;
  /** Shared secret to authenticate with, matching NANOCACHED_AUTH_SECRET
   * on the server. Omit if the server has no secret configured. */
  authSecret?: string | Uint8Array;
  /** Connect over TLS instead of plaintext — required if the server was
   * started with --tls-cert/--tls-key. Pass `true` to verify the server's
   * certificate against Node's default, publicly-trusted CA store — the
   * normal case; pass `{ ca }` instead only if the server is running a
   * self-signed certificate with no CA-issued alternative available. */
  tls?: true | NanocachedTlsOptions;
}

interface Waiter {
  resolve: (response: ParsedResponse) => void;
  reject: (error: Error) => void;
}

function toBytes(value: string | Uint8Array): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
}

function unexpectedResponse(response: ParsedResponse): Error {
  return new Error(`nanocached: unexpected response from server: ${response.kind}`);
}

/**
 * A single connection to a nanocached-node. Requests are pipelined onto one
 * TCP (or TLS) connection and matched to responses in send order, since the
 * protocol has no request IDs — nanocached-node itself only ever answers in
 * the order it received requests, so this is safe as long as nothing else
 * writes to the same socket concurrently.
 */
export class NanocachedClient {
  private readonly socket: Socket | TLSSocket;
  private buffer: Buffer = Buffer.alloc(0);
  private readonly pending: Waiter[] = [];
  private closed = false;
  private lastError: Error | null = null;

  private constructor(socket: Socket | TLSSocket) {
    this.socket = socket;
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (error: Error) => this.onError(error));
    this.socket.on("close", () => this.onClose());
  }

  static async connect(options: NanocachedClientOptions): Promise<NanocachedClient> {
    const socket = await new Promise<Socket | TLSSocket>((resolve, reject) => {
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

    const client = new NanocachedClient(socket);

    if (options.authSecret !== undefined) {
      const response = await client.send(encodeAuth(toBytes(options.authSecret)));
      if (response.kind !== "authOk") {
        client.close();
        throw new Error("nanocached: authentication failed");
      }
    }

    return client;
  }

  async get(key: string | Uint8Array): Promise<Buffer | null> {
    const response = await this.send(encodeGet(toBytes(key)));
    if (response.kind === "value") return response.value;
    if (response.kind === "notFound") return null;
    throw unexpectedResponse(response);
  }

  async set(
    key: string | Uint8Array,
    value: string | Uint8Array,
    options?: { ttlSeconds?: number },
  ): Promise<void> {
    const response = await this.send(encodeSet(toBytes(key), toBytes(value), options?.ttlSeconds));
    if (response.kind !== "stored") throw unexpectedResponse(response);
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    const response = await this.send(encodeDelete(toBytes(key)));
    if (response.kind === "deleted") return true;
    if (response.kind === "notFound") return false;
    throw unexpectedResponse(response);
  }

  close(): void {
    this.socket.destroy();
  }

  private send(frame: Buffer): Promise<ParsedResponse> {
    if (this.closed) {
      return Promise.reject(this.lastError ?? new Error("nanocached: connection is closed"));
    }

    return new Promise((resolve, reject) => {
      const waiter: Waiter = { resolve, reject };
      this.pending.push(waiter);

      this.socket.write(frame, (error) => {
        if (error) {
          const index = this.pending.indexOf(waiter);
          if (index !== -1) this.pending.splice(index, 1);
          reject(error);
        }
      });
    });
  }

  private onData(chunk: Buffer): void {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);

    for (;;) {
      let parsed;
      try {
        parsed = tryParseResponse(this.buffer);
      } catch (error) {
        this.lastError = error as Error;
        this.socket.destroy();
        return;
      }

      if (parsed === null) return;

      this.buffer = this.buffer.subarray(parsed.consumed);

      // An unsolicited "busy" response means the server hit its connection
      // limit right after accept and is about to close the connection; it
      // isn't an answer to anything we sent.
      if (parsed.response.kind === "busy" && this.pending.length === 0) {
        this.lastError = new Error("nanocached: server rejected the connection (connection limit reached)");
        continue;
      }

      const waiter = this.pending.shift();
      waiter?.resolve(parsed.response);
    }
  }

  private onError(error: Error): void {
    this.lastError = error;
  }

  private onClose(): void {
    this.closed = true;
    const error = this.lastError ?? new Error("nanocached: connection closed");
    const waiters = this.pending.splice(0);
    for (const waiter of waiters) waiter.reject(error);
  }
}
