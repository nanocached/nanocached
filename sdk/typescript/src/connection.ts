import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import {
  encodeDelete,
  encodeGet,
  encodeSet,
  MAX_RESPONSE_FRAME_LENGTH,
  tryParseResponse,
  type ParsedResponse,
} from "./protocol.js";

interface Waiter {
  resolve: (response: ParsedResponse) => void;
  reject: (error: Error) => void;
  /** ADR-0019: the tag this request was sent with, which its response
   * must echo — `undefined` on untagged connections. */
  tag?: number;
}

function toBytes(value: string | Uint8Array): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
}

/** Bounds how long the connection may go without progress while requests
 * are outstanding (issue #42) — each response must arrive within this
 * window of the previous one (or of its own send, when the queue was
 * empty): without it, a half-open server that accepts the TCP connection
 * but never writes back — or stops mid-stream — would hang get/set/delete
 * forever. Generous versus the server's own 10s outbound timeouts, and
 * the same 30s the Go and Rust SDKs use. Exported as a mutable object
 * only so tests can shorten it, mirroring KEEPALIVE_TUNING. */
export const REQUEST_TIMEOUT_TUNING = { timeoutMs: 30_000 };


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
  /** ADR-0019: negotiated during identify — when true, every request
   * carries a tag the server echoes, and `onData` verifies the echo
   * against the oldest waiter before resolving it. */
  private readonly tagged: boolean;
  private nextTag = 0;
  // Chunks are accumulated in an array and only concatenated when a parse
  // is attempted, instead of concatenating on every onData call — avoids
  // an O(n^2) cost re-copying the whole buffer for each fragment of a
  // large value.
  private chunks: Buffer[] = [];
  private chunksLength = 0;
  private readonly pending: Waiter[] = [];
  private closed = false;
  private lastError: Error | null = null;
  private lastUsed = Date.now();
  /** The progress-based request deadline (issue #42): armed when the
   * pending queue goes from empty to non-empty, re-armed by `onData` each
   * time a response is dispatched with more still outstanding, cleared
   * once nothing is. Never fires on an idle connection. */
  private requestTimer: NodeJS.Timeout | null = null;

  constructor(socket: Socket | TLSSocket, tagged = false) {
    this.socket = socket;
    this.tagged = tagged;
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (error: Error) => this.onError(error));
    this.socket.on("close", () => this.onClose());
  }

  async get(key: string | Uint8Array): Promise<Buffer | null> {
    const response = await this.send((tag) => encodeGet(toBytes(key), tag));
    if (response.kind === "value") return response.value ?? Buffer.alloc(0);
    if (response.kind === "notFound") return null;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  async set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0): Promise<void> {
    const response = await this.send((tag) => encodeSet(toBytes(key), toBytes(value), ttlSeconds, tag));
    if (response.kind === "wrongNode") throw new WrongNodeError();
    if (response.kind !== "stored") throw this.mismatch(response);
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array): Promise<boolean> {
    const response = await this.send((tag) => encodeDelete(toBytes(key), tag));
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
   * remotely (e.g. the server's 60s idle timeout sent a FIN). Once true, a
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

  private send(build: (tag: number | undefined) => Buffer): Promise<ParsedResponse> {
    if (this.closed) {
      return Promise.reject(this.lastError ?? new ConnectionLostError("nanocached: connection is closed"));
    }
    this.lastUsed = Date.now();

    return new Promise((resolve, reject) => {
      // The tag is claimed in the same synchronous span that enqueues the
      // waiter and writes the frame (ADR-0016's enqueue+write atomicity),
      // so tag order can never skew from queue/wire order (ADR-0019).
      const tag = this.tagged ? this.claimTag() : undefined;
      // Build before enqueueing: an encoder that rejects its input (e.g.
      // encodeSet's TTL check) must fail with nothing queued, or the next
      // response would resolve an orphaned waiter and desync the stream.
      const frame = build(tag);
      const waiter: Waiter = { resolve, reject, tag };
      this.pending.push(waiter);
      // Armed only on the empty→non-empty transition: arming on *every*
      // request would let a continuous stream of new requests push the
      // deadline forever ahead of a server that has stopped answering —
      // exactly the half-open hang the timeout exists to catch.
      if (this.pending.length === 1) this.armRequestTimer();

      this.socket.write(frame, (error) => {
        if (error) {
          const index = this.pending.indexOf(waiter);
          if (index !== -1) this.pending.splice(index, 1);
          if (this.pending.length === 0) this.clearRequestTimer();
          reject(error);
        }
      });
    });
  }

  private claimTag(): number {
    const tag = this.nextTag;
    this.nextTag = (this.nextTag + 1) >>> 0; // wrap at u32, matching the wire's width
    return tag;
  }

  private onData(chunk: Buffer): void {
    this.chunks.push(chunk);
    this.chunksLength += chunk.length;

    for (;;) {
      const buffer = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks, this.chunksLength);

      let parsed;
      try {
        parsed = tryParseResponse(buffer, this.tagged);
      } catch (error) {
        this.lastError = error as Error;
        this.socket.destroy();
        return;
      }

      if (parsed === null) {
        // A frame this large without ever completing means the server
        // will never send a valid terminator — tryParseResponse already
        // bounds the `V` header search on its own, but this is a backstop
        // covering the value body (and any other response kind), so a
        // malicious server can't wedge this open by trickling bytes that
        // never assemble into a parseable frame (issue #12 follow-up).
        if (buffer.length > MAX_RESPONSE_FRAME_LENGTH) {
          this.lastError = new Error("nanocached: response frame exceeds maximum size (connection desynced)");
          this.socket.destroy();
          return;
        }
        // Collapse back to a single stored chunk so later onData calls
        // don't re-concat bytes already merged here.
        this.chunks = [buffer];
        this.chunksLength = buffer.length;
        return;
      }

      const remainder = buffer.subarray(parsed.consumed);
      this.chunks = remainder.length > 0 ? [remainder] : [];
      this.chunksLength = remainder.length;

      // An unsolicited "busy" response means the server hit its connection
      // limit right after accept and is about to close the connection; it
      // isn't an answer to anything we sent.
      if (parsed.response.kind === "busy" && this.pending.length === 0) {
        this.lastError = new Error("nanocached: server rejected the connection (connection limit reached)");
        continue;
      }

      const waiter = this.pending.shift();
      if (waiter === undefined) continue;

      // Progress-based deadline (see send()): a dispatched response is
      // progress, so the next-oldest request gets a fresh window; with
      // nothing left waiting, clear it so an otherwise-idle connection is
      // never closed by it.
      if (this.pending.length === 0) this.clearRequestTimer();
      else this.armRequestTimer();

      // ADR-0019: on a tagged connection, verify the echoed tag against
      // the request this response is about to answer — *before* it can
      // reach any caller. A mismatch means the streams are misaligned;
      // unlike the caller-side kind check (`mismatch()`), catching it
      // here stops the misdelivery instead of merely noticing it later.
      if (this.tagged && parsed.response.tag !== waiter.tag) {
        const error = new ConnectionLostError(
          `nanocached: response tag ${parsed.response.tag} does not answer request tag ${waiter.tag} (connection desynced)`,
        );
        this.closed = true;
        this.lastError = error;
        this.socket.destroy();
        // The shifted waiter is no longer in `pending`, so onClose won't
        // reach it — reject it here; the rest drain on the close event.
        waiter.reject(error);
        return;
      }

      waiter.resolve(parsed.response);
    }
  }

  private armRequestTimer(): void {
    this.clearRequestTimer();
    this.requestTimer = setTimeout(() => {
      // Poison, exactly like a read error: mark closed synchronously and
      // destroy the socket — the 'close' event then rejects every pending
      // waiter (the stalled request and everything pipelined behind it)
      // with this error, and the client's retry layer redials.
      this.lastError = new ConnectionLostError(
        `nanocached: no response from server within ${REQUEST_TIMEOUT_TUNING.timeoutMs}ms (request timed out)`,
      );
      this.closed = true;
      this.requestTimer = null;
      this.socket.destroy();
    }, REQUEST_TIMEOUT_TUNING.timeoutMs);
  }

  private clearRequestTimer(): void {
    if (this.requestTimer !== null) {
      clearTimeout(this.requestTimer);
      this.requestTimer = null;
    }
  }

  private onError(error: Error): void {
    this.lastError = error;
  }

  private onClose(): void {
    this.closed = true;
    this.clearRequestTimer();
    const error = this.lastError ?? new ConnectionLostError("nanocached: connection closed");
    const waiters = this.pending.splice(0);
    for (const waiter of waiters) waiter.reject(error);
  }
}
