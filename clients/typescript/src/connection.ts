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

function unexpectedResponse(response: ParsedResponse): Error {
  return new Error(`nanocached: unexpected response from server: ${response.kind}`);
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
    throw unexpectedResponse(response);
  }

  async set(key: string | Uint8Array, value: string | Uint8Array, options?: { ttlSeconds?: number }): Promise<void> {
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
