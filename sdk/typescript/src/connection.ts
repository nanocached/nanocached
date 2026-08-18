import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { encodeDelete, encodeGet, encodeSet, tryParseResponse, type ParsedResponse } from "./protocol.js";

interface Waiter {
  resolve: (response: ParsedResponse) => void;
  reject: (error: Error) => void;
}

function toBytes(value: string | Uint8Array): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
}


/** Thrown by get/set/delete when the node answers `W` (ADR-0008): per its
 * own current view of cluster membership, this node no longer (or not yet)
 * owns the key — the caller's routing table is stale. Carries no
 * forwarding address; `NanocachedClient` catches this to re-fetch the node
 * list and retry once (see its own doc comment), not something callers of
 * `NanocachedClient.get`/`set`/`delete` normally need to handle themselves
 * unless they're bypassing that retry (e.g. by calling a single `Connection`
 * directly). */
export class WrongNodeError extends Error {
  constructor() {
    super("nanocached: this node no longer owns the requested key");
    this.name = "WrongNodeError";
  }
}

/** A connection-level failure: the socket died (or was already dead) out
 * from under a request. In cluster mode the client treats this like `W` —
 * refresh the node list and retry once — since the usual cause is a node
 * death that discovery has since noticed. */
export class ConnectionLostError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConnectionLostError";
  }
}

/** Whether an error is connection-shaped: our own ConnectionLostError, or
 * a Node system error (ECONNREFUSED, ECONNRESET, EPIPE, ...). */
export function isConnectionError(error: unknown): boolean {
  if (error instanceof ConnectionLostError) return true;
  return error instanceof Error && typeof (error as NodeJS.ErrnoException).code === "string";
}

/**
 * One already-identified (see `identify.ts`) connection to a single
 * nanocached-node. Requests are pipelined onto one TCP (or TLS) connection
 * and matched to responses in send order, since the protocol has no
 * request IDs — nanocached-node itself only ever answers in the order it
 * received requests, so this is safe as long as nothing else writes to the
 * same socket concurrently.
 */
export class Connection {
  private readonly socket: Socket | TLSSocket;
  private buffer: Buffer = Buffer.alloc(0);
  private readonly pending: Waiter[] = [];
  private closed = false;
  private lastError: Error | null = null;
  private lastUsed = Date.now();

  constructor(socket: Socket | TLSSocket) {
    this.socket = socket;
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (error: Error) => this.onError(error));
    this.socket.on("close", () => this.onClose());
  }

  async get(key: string | Uint8Array): Promise<Buffer | null> {
    const response = await this.send(encodeGet(toBytes(key)));
    if (response.kind === "value") return response.value ?? Buffer.alloc(0);
    if (response.kind === "notFound") return null;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  async set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<void> {
    const response = await this.send(encodeSet(toBytes(key), toBytes(value), ttlSeconds));
    if (response.kind === "wrongNode") throw new WrongNodeError();
    if (response.kind !== "stored") throw this.mismatch(response);
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    const response = await this.send(encodeDelete(toBytes(key)));
    if (response.kind === "deleted") return true;
    if (response.kind === "notFound") return false;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  /** A well-formed response of the wrong kind (a `stored` answering a G)
   * means the request/response streams are misaligned — every later
   * response would answer the wrong request, silently returning other
   * keys' data. Poison the connection, and classify as a connection error
   * so the client's retry layer redials and retries once. */
  private mismatch(response: ParsedResponse): ConnectionLostError {
    const error = new ConnectionLostError(
      `nanocached: response "${response.kind}" does not match the request (connection desynced)`,
    );
    // Mark closed synchronously — destroy()'s 'close' event only lands on
    // a later tick, and the client's retry layer re-checks isClosed()
    // before then to decide whether to redial.
    this.closed = true;
    this.lastError = error;
    this.socket.destroy();
    return error;
  }

  close(): void {
    this.socket.destroy();
  }

  /** Whether the underlying socket has closed — locally via close(), or
   * remotely (e.g. the server's 30s idle timeout sent a FIN). Once true, a
   * caller holding this connection must open a new one; see
   * `NanocachedClient.routedConnection`. */
  isClosed(): boolean {
    return this.closed;
  }

  /** Milliseconds since the last request was sent on this connection —
   * what the keep-alive timer checks against the ping interval, so pings
   * only go out on connections real traffic isn't already keeping alive. */
  idleMs(): number {
    return Date.now() - this.lastUsed;
  }

  private send(frame: Buffer): Promise<ParsedResponse> {
    if (this.closed) {
      return Promise.reject(this.lastError ?? new ConnectionLostError("nanocached: connection is closed"));
    }
    this.lastUsed = Date.now();

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
    const error = this.lastError ?? new ConnectionLostError("nanocached: connection closed");
    const waiters = this.pending.splice(0);
    for (const waiter of waiters) waiter.reject(error);
  }
}
