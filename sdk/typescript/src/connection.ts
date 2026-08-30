import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { NanocachedError } from "./errors.js";
import {
  EMPTY_NAMESPACE,
  encodeCas,
  encodeCasDelete,
  encodeClear,
  encodeClearAll,
  encodeDelete,
  encodeGet,
  encodeIncr,
  encodeMultiGet,
  encodeMultiSet,
  encodeSet,
  MAX_RESPONSE_FRAME_LENGTH,
  peekMultiFrameLength,
  tryParseResponse,
  type CasCondition,
  type MultiAckEntry,
  type MultiEntry,
  type ParsedResponse,
} from "./protocol.js";

interface Waiter {
  resolve: (response: ParsedResponse) => void;
  reject: (error: Error) => void;
  /** echoed response tags: the tag this request was sent with, which its response
   * must echo — `undefined` on untagged connections. */
  tag?: number;
}

// Issue: `Buffer.from(uint8array)` copies its input, and every caller here
// immediately hands the result to `Buffer.concat` (in encodeGet/encodeSet/
// encodeDelete), which copies again to build the final frame — a Uint8Array
// key/value paid for two copies where one suffices. `Buffer.concat` accepts
// plain `Uint8Array` entries directly, so a `Uint8Array` input is returned
// unchanged here; only the string case needs a fresh Buffer, since encoding
// text to bytes has no existing buffer to reuse.
function toBytes(value: string | Uint8Array): Uint8Array {
  return typeof value === "string" ? Buffer.from(value, "utf8") : value;
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


/** Thrown by get/set/delete when the node answers `W` (staged node join): per its
 * own current view of cluster membership, this node no longer (or not yet)
 * owns the key — the caller's routing table is stale. Carries no
 * forwarding address; `NanocachedClient` catches this to re-fetch the node
 * list and retry once (see its own doc comment), not something callers of
 * `NanocachedClient.get`/`set`/`delete` normally need to handle themselves
 * unless they're bypassing that retry (e.g. by calling a single `Connection`
 * directly). */
export class WrongNodeError extends NanocachedError {
  constructor() {
    super("nanocached: this node no longer owns the requested key");
    this.name = "WrongNodeError";
  }
}

/** A connection-level failure: the socket died (or was already dead) out
 * from under a request. In cluster mode the client treats this like `W` —
 * refresh the node list and retry once — since the usual cause is a node
 * death that discovery has since noticed. That blanket retry is only safe
 * for idempotent requests (get/set/delete/clear/multiGet/multiSet):
 * replaying incr/decr, CAS (replace/putIfAbsent) or deleteIfMatches can
 * double-apply the write, or turn a CAS that actually succeeded into a
 * reported mismatch, if the request had already reached the server and
 * only the reply was lost (issue #225). `requestWasSent` distinguishes
 * the two cases so `NanocachedClient`'s non-idempotent call sites can
 * retry only when it's provably `false`. */
export class ConnectionLostError extends NanocachedError {
  /** Whether the request this error is rejecting had already begun being
   * written to the socket. `false` only for the one case where `Connection`
   * rejects before ever calling `socket.write()` — the connection was
   * already closed when the call was made, so the frame for *this*
   * specific request definitely never reached the wire, and replaying it
   * is always safe. Defaults to `true` (the conservative "may have been
   * applied" assumption) for every other path: a `socket.write()` failure
   * partway through the frame, and an ordinary close/timeout/mismatch/tag
   * desync discovered after the frame was already handed to the socket —
   * none of those can prove the server never received (and acted on) the
   * bytes, so this stays `true` there even though the exact boundary
   * between "definitely sent" and "ambiguous" isn't tracked separately. */
  readonly requestWasSent: boolean;

  constructor(message: string, options?: { requestWasSent?: boolean }) {
    super(message);
    this.name = "ConnectionLostError";
    this.requestWasSent = options?.requestWasSent ?? true;
  }
}

/** Whether an error is connection-shaped: our own ConnectionLostError, or
 * a Node system error (ECONNREFUSED, ECONNRESET, EPIPE, ...). */
export function isConnectionError(error: unknown): boolean {
  if (error instanceof ConnectionLostError) return true;
  return error instanceof Error && typeof (error as NodeJS.ErrnoException).code === "string";
}

/** Thrown by get/set/delete/clear/clearAll when a request is answered the
 * retryable-error status `R` (issue #125) on every one of
 * `RETRYABLE_RETRY_DELAYS_MS.length + 1` attempts: the request itself
 * kept failing transiently — e.g. a `nanocached-proxy`'s upstream node was
 * briefly unreachable and stayed that way through the proxy's own
 * refresh-and-retry — but unlike every other error this SDK raises, the
 * connection itself is fine. It is never poisoned by `R`: a caller
 * catching (or ignoring) this error can immediately reuse the same
 * `NanocachedClient`/`Connection` for another operation. */
export class RetryableError extends NanocachedError {
  constructor() {
    super("nanocached: request failed transiently and was not accepted after retrying (connection is still usable)");
    this.name = "RetryableError";
  }
}

/** Thrown by incr/decr (issue #129) when the key exists but its stored
 * value isn't INCR's counter grammar, or applying `<delta>` would overflow
 * the representable range — the server answers `T` for either case, since
 * the client has no way to tell which apart without the raw stored value
 * itself. */
export class NotNumericError extends NanocachedError {
  constructor() {
    super("nanocached: the stored value is not an integer INCR can operate on");
    this.name = "NotNumericError";
  }
}

/** Thrown by incr/decr (issue #224) when the counter's new value, after
 * applying `delta`, falls outside `±Number.MAX_SAFE_INTEGER` — the wire
 * protocol's counter is a full signed 64-bit integer, but a JS `number`
 * can only represent integers exactly up to 2^53 - 1. Returning a rounded
 * `number` past that point would silently misreport the counter (and,
 * before this fix, corrupt replicas that re-encoded the rounded value —
 * see `NanocachedClient`'s `incrOnOwners`, which now forwards the exact
 * digit bytes received from the primary instead). The write itself always
 * still succeeds and is still replicated byte-exact; only the value
 * handed back from this call is unrepresentable as a `number`. */
export class CounterOutOfRangeError extends NanocachedError {
  constructor(raw: string) {
    super(
      `nanocached: counter value ${raw} exceeds the safe integer range (±${Number.MAX_SAFE_INTEGER}) and cannot be returned as a number — the write itself succeeded and was replicated exactly`,
    );
    this.name = "CounterOutOfRangeError";
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Retryable-error status (issue #125): up to 2 retries (3 attempts total)
// on the same connection before giving up with a RetryableError. The
// delay before each retry — 50ms before the first, 100ms before the
// second — mirrors the other five SDKs' identical bounded-retry policy.
const RETRYABLE_RETRY_DELAYS_MS = [50, 100];

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
  /** echoed response tags: negotiated during identify — when true, every request
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
  /** Retryable-error status (issue #125): invoked once per `R` this
   * connection receives (whether the retry that followed ultimately
   * succeeded or not) — backs `NanocachedClient`'s `stats().transientRetries`
   * counter. `undefined` until wired up (see `setOnTransientRetry`), which
   * matters only for the handful of connections `NanocachedClient.connect`
   * opens before the client instance (and so this callback) exists; no `R`
   * can arrive before then, since identify traffic never sees one. */
  private onTransientRetry: (() => void) | undefined;

  constructor(socket: Socket | TLSSocket, tagged = false, onTransientRetry?: () => void) {
    this.socket = socket;
    this.tagged = tagged;
    this.onTransientRetry = onTransientRetry;
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    this.socket.on("error", (error: Error) => this.onError(error));
    this.socket.on("close", () => this.onClose());
  }

  /** Deferred wiring for the `onTransientRetry` callback — see its own
   * doc comment on why this exists alongside the constructor parameter. */
  setOnTransientRetry(callback: () => void): void {
    this.onTransientRetry = callback;
  }

  /** `namespace` (first-class namespaces, issue #105) defaults to the
   * default (empty) namespace, which sends the exact legacy `G` frame —
   * see `encodeGet`. */
  async get(key: string | Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): Promise<Buffer | null> {
    const response = await this.send((tag) => encodeGet(toBytes(key), tag, namespace));
    if (response.kind === "value") return response.value ?? Buffer.alloc(0);
    if (response.kind === "notFound") return null;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  async set(key: string | Uint8Array, value: string | Uint8Array, ttlSeconds = 0, namespace: Uint8Array = EMPTY_NAMESPACE): Promise<void> {
    const response = await this.send((tag) => encodeSet(toBytes(key), toBytes(value), ttlSeconds, tag, namespace));
    if (response.kind === "wrongNode") throw new WrongNodeError();
    if (response.kind !== "stored") throw this.mismatch(response);
  }

  /** Returns whether the key existed before this call. */
  async delete(key: string | Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): Promise<boolean> {
    const response = await this.send((tag) => encodeDelete(toBytes(key), tag, namespace));
    if (response.kind === "deleted") return true;
    if (response.kind === "notFound") return false;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  /** INCR/DECR (issue #129): applies `delta` (signed) to `key`'s stored
   * counter, always namespaced on the wire (unlike get/set/delete, `i` has
   * no separate uppercase legacy form — namespace-length 0 addresses the
   * default namespace, matching `namespace`'s own default here). Returns
   * `null` on a miss, matching `get`'s own miss convention; throws
   * `NotNumericError` when the stored value isn't INCR's counter grammar
   * (or applying `delta` would overflow). `value` is the new counter,
   * decimal-parsed as a `number` — imprecisely once it passes
   * `Number.MAX_SAFE_INTEGER` (issue #224), which is why `raw` is also
   * returned alongside it: the exact ASCII digit bytes this node answered
   * with, unrounded, for a caller that needs to forward them byte-exact
   * rather than trust the parsed `number`. `ttlSeconds` is the entry's
   * remaining TTL, present only when it has one. This is the single-node
   * primitive only — `NanocachedClient` is what turns a successful
   * primary increment into a cluster-wide, drift-free write by forwarding
   * `raw` to replicas as an ordinary `set` (never `String(value)`, which
   * would re-round it), never replaying `i` there (see its own doc
   * comment); it's also what decides whether `value` is safe to hand back
   * to its own caller, throwing `CounterOutOfRangeError` when it isn't. */
  async incr(
    key: string | Uint8Array,
    delta: number,
    namespace: Uint8Array = EMPTY_NAMESPACE,
  ): Promise<{ value: number; raw: Buffer; ttlSeconds?: number } | null> {
    const response = await this.send((tag) => encodeIncr(toBytes(key), delta, tag, namespace));
    if (response.kind === "incremented") {
      const raw = response.value ?? Buffer.alloc(0);
      return { value: Number(raw.toString("ascii")), raw, ttlSeconds: response.ttlSeconds };
    }
    if (response.kind === "notFound") return null;
    if (response.kind === "notNumeric") throw new NotNumericError();
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  /** `k` — compare-and-set (issue #141): stores `value` for `key` only if
   * `cond` holds against the key's current stored bytes — see
   * `CasCondition`. Returns `true` on success (the wire's `S`), `false` on
   * a condition mismatch (the wire's `N`, the same status a miss already
   * uses — a mismatch is a normal outcome here, never an exception).
   * Always namespaced on the wire, matching `incr`'s own default here.
   * This is the single-node primitive only — `NanocachedClient` is what
   * turns a successful primary CAS into a cluster-wide, drift-free write
   * by forwarding the literal new value to replicas as an ordinary `set`,
   * never replaying `k` there (see its own doc comment, mirroring
   * `incr`'s). */
  async cas(key: string | Uint8Array, value: Uint8Array, cond: CasCondition, ttlSeconds = 0, namespace: Uint8Array = EMPTY_NAMESPACE): Promise<boolean> {
    const response = await this.send((tag) => encodeCas(toBytes(key), value, cond, ttlSeconds, tag, namespace));
    if (response.kind === "stored") return true;
    if (response.kind === "notFound") return false;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  /** `x` — compare-and-delete (issue #141): removes `key` only if its
   * current stored bytes hash to exactly `digest` (see `contentDigest`).
   * Returns `true` on success (the wire's `D`), `false` on a mismatch or a
   * missing key (the wire's `N`) — never an exception for either. Always
   * namespaced on the wire. Like `cas` above, this is the single-node
   * primitive; `NanocachedClient` forwards a successful primary delete to
   * replicas as an ordinary `delete`, never replaying `x` there. */
  async casDelete(key: string | Uint8Array, digest: string, namespace: Uint8Array = EMPTY_NAMESPACE): Promise<boolean> {
    const response = await this.send((tag) => encodeCasDelete(toBytes(key), digest, tag, namespace));
    if (response.kind === "deleted") return true;
    if (response.kind === "notFound") return false;
    if (response.kind === "wrongNode") throw new WrongNodeError();
    throw this.mismatch(response);
  }

  /** Clears one namespace on this node (issue #106) — `namespace`
   * defaults to the default (empty) namespace, matching `get`/`set`/
   * `delete` above. Unlike those, a clear is never key-addressed, so the
   * node never answers `W` for it (a `W` here would just be an
   * unexpected kind, handled below like any other mismatch); it's
   * `NanocachedClient` that turns this per-connection primitive into a
   * cluster-wide operation by fanning it out to every node — see its
   * `fanoutClear`. */
  async clear(namespace: Uint8Array = EMPTY_NAMESPACE): Promise<void> {
    const response = await this.send((tag) => encodeClear(namespace, tag));
    if (response.kind !== "cleared") throw this.mismatch(response);
  }

  /** Flushes every namespace on this node, default included (issue
   * #106) — the `F` command's per-connection primitive; see `clear` and
   * `NanocachedClient.clearAll` for the cluster-wide fan-out. */
  async clearAll(): Promise<void> {
    const response = await this.send((tag) => encodeClearAll(tag));
    if (response.kind !== "cleared") throw this.mismatch(response);
  }

  /** Batched get (issues #128/#150/#151): `n` keys under one round trip
   * through the cache, instead of `n` independent `get()` calls. Always
   * namespaced on the wire (namespace defaults to the default
   * namespace, matching `incr`'s own default) — `m` has no legacy
   * uppercase form. Returns one entry per key, in request order: a
   * batch never fails as a whole (docs/protocol.html#multi), so a
   * per-key `W` lives inside the returned array, never as a thrown
   * `WrongNodeError` for the whole call. This is the single-node
   * primitive only — `NanocachedClient` is what groups keys by owner
   * and drives the per-key refresh-and-retry pass. */
  async multiGet(keys: readonly Uint8Array[], namespace: Uint8Array = EMPTY_NAMESPACE): Promise<MultiEntry[]> {
    const response = await this.send((tag) => encodeMultiGet(keys, tag, namespace));
    if (response.kind !== "multi" || response.entries === undefined) throw this.mismatch(response);
    if (response.entries.length !== keys.length) {
      throw this.desynced(
        `multi-get response roster length ${response.entries.length} does not match request key count ${keys.length}`,
      );
    }
    return response.entries;
  }

  /** Batched set (issues #150/#151): `n` keys stored under one round
   * trip, one shared `ttlSeconds` for the whole batch rather than per
   * key. See `multiGet` for the same per-key independence contract and
   * always-namespaced shape. */
  async multiSet(
    keys: readonly Uint8Array[],
    values: readonly Uint8Array[],
    ttlSeconds = 0,
    namespace: Uint8Array = EMPTY_NAMESPACE,
  ): Promise<MultiAckEntry[]> {
    const response = await this.send((tag) => encodeMultiSet(keys, values, ttlSeconds, tag, namespace));
    if (response.kind !== "multiAck" || response.ackEntries === undefined) throw this.mismatch(response);
    if (response.ackEntries.length !== keys.length) {
      throw this.desynced(
        `multi-set response roster length ${response.ackEntries.length} does not match request key count ${keys.length}`,
      );
    }
    return response.ackEntries;
  }

  /** A well-formed response of the wrong kind (a `stored` answering a G)
   * means the request/response streams are misaligned — every later
   * response would answer the wrong request, silently returning other
   * keys' data. Poison the connection, and classify as a connection error
   * so the client's retry layer redials and retries once. */
  private mismatch(response: ParsedResponse): ConnectionLostError {
    return this.desynced(`response "${response.kind}" does not match the request`);
  }

  /** Same fatal classification as `mismatch` above, but for a
   * well-formed response of the *right* kind whose roster length still
   * disagrees with the request (issues #128/#150/#151, `multiGet`/
   * `multiSet`) — the streams are just as desynced as a kind mismatch
   * would indicate. */
  private desynced(message: string): ConnectionLostError {
    const error = new ConnectionLostError(`nanocached: ${message} (connection desynced)`);
    this.poison(error);
    return error;
  }

  /** Marks the connection closed, clears the request timer, and destroys
   * the socket — every fatal path (a kind mismatch, a tag mismatch, an
   * unsolicited busy response, the request timeout, and a failed write)
   * converges here, mirroring the Python SDK's `_poison()`
   * (_connection.py), which every one of its own fatal paths already
   * goes through. Marking closed synchronously matters: destroy()'s
   * 'close' event only lands on a later tick, and the client's retry
   * layer re-checks isClosed() before then to decide whether to redial.
   * Rejecting every still-pending waiter (including whichever one
   * triggered this) is left to the 'close' handler this destroy() call
   * guarantees will fire. Safe to call more than once — only the first
   * call has any effect. */
  private poison(error: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.lastError = error;
    this.clearRequestTimer();
    this.socket.destroy();
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

  /** Retryable-error status (issue #125): wraps `sendOnce`, transparently
   * retrying an `R`-answered request — possible on any data command — up
   * to `RETRYABLE_RETRY_DELAYS_MS.length` times, sleeping the configured
   * delay before each retry. `R` is not a connection error (no `mismatch`,
   * no `poison`): the connection stays open and reusable throughout,
   * whether this ultimately resolves or throws `RetryableError`. Every
   * retry re-sends the request fresh (a new tag in tagged mode), same as
   * calling `sendOnce` again — the protocol has no concept of resuming a
   * specific prior attempt. */
  private async send(build: (tag: number | undefined) => Buffer): Promise<ParsedResponse> {
    let response = await this.sendOnce(build);
    for (let attempt = 0; response.kind === "retryable"; attempt++) {
      this.onTransientRetry?.();
      if (attempt >= RETRYABLE_RETRY_DELAYS_MS.length) {
        throw new RetryableError();
      }
      await sleep(RETRYABLE_RETRY_DELAYS_MS[attempt]);
      response = await this.sendOnce(build);
    }
    return response;
  }

  private sendOnce(build: (tag: number | undefined) => Buffer): Promise<ParsedResponse> {
    if (this.closed) {
      // Issue #225: nothing was ever written for *this* call — this
      // branch returns before ever building a frame or touching the
      // socket — so it's always safe to replay, regardless of what
      // killed the connection (a previous request's failure, an
      // idle-FIN the server sent before this call was even made, ...).
      // Wrapped fresh with requestWasSent: false rather than reusing
      // `this.lastError` as-is, so this classification never depends on
      // which error the connection happened to die with.
      const cause = this.lastError;
      return Promise.reject(
        new ConnectionLostError(
          cause ? `nanocached: connection is closed: ${cause.message}` : "nanocached: connection is closed",
          { requestWasSent: false },
        ),
      );
    }
    this.lastUsed = Date.now();

    return new Promise((resolve, reject) => {
      // The tag is claimed in the same synchronous span that enqueues the
      // waiter and writes the frame (request pipelining's enqueue+write atomicity),
      // so tag order can never skew from queue/wire order (echoed response tags).
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
          // A write can fail (EPIPE, ECONNRESET, ...) after the frame
          // went only partially onto the wire — desyncing every request
          // pipelined behind this one, not just this one. Poison the
          // whole connection like the other fatal paths instead of
          // splicing out and rejecting just this waiter: that used to
          // leave everything else pending until the 30s request timeout
          // eventually noticed the socket was dead. Mirrors Python's
          // `_poison()` on any write OSError (_connection.py:233-247);
          // `waiter` (and every other still-pending waiter) is rejected
          // by the 'close' handler this triggers.
          this.poison(new ConnectionLostError(`nanocached: connection failed: ${error.message}`));
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
        // Route through poison() (issue #187) rather than destroying the
        // socket directly: poison() flips `closed` synchronously, before
        // destroy()'s 'close' event lands next tick. A direct destroy()
        // left that window open for another request to pick this
        // connection and write to an already-dead socket.
        this.poison(error as Error);
        return;
      }

      if (parsed === null) {
        // A frame this large without ever completing means the server
        // will never send a valid terminator — tryParseResponse already
        // bounds the `V` header search on its own, but this is a backstop
        // covering the value body (and any other response kind), so a
        // malicious server can't wedge this open by trickling bytes that
        // never assemble into a parseable frame (issue #12 follow-up).
        // A batched `M`/`O` reply's total size depends on its roster
        // (issues #128/#150/#151) rather than one fixed value, so
        // MAX_RESPONSE_FRAME_LENGTH alone — sized for exactly one
        // maximally-sized `V` — would kill a large, perfectly legitimate
        // batch reply mid-flight. peekMultiFrameLength reads the
        // already-received header (if any) to compute the real total
        // once it's known, widening the backstop to match; every other
        // response kind's bound is completely unaffected (peek returns
        // undefined for them).
        const expectedMultiLength = peekMultiFrameLength(buffer, this.tagged);
        const limit =
          expectedMultiLength === undefined ? MAX_RESPONSE_FRAME_LENGTH : Math.max(MAX_RESPONSE_FRAME_LENGTH, expectedMultiLength);
        if (buffer.length > limit) {
          // poison() (issue #187), not a direct destroy(): see the parse-
          // failure branch above for why isClosed() must flip synchronously.
          this.poison(new NanocachedError("nanocached: response frame exceeds maximum size (connection desynced)"));
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
      // isn't an answer to anything we sent. Poison immediately (issue
      // #45), like the other five SDKs and the tag-mismatch path below —
      // waiting for the server's follow-up FIN would let the client keep
      // writing requests into a connection the server has already
      // declared it is abandoning.
      if (parsed.response.kind === "busy" && this.pending.length === 0) {
        this.poison(new NanocachedError("nanocached: server rejected the connection (connection limit reached)"));
        return;
      }

      const waiter = this.pending.shift();
      if (waiter === undefined) continue;

      // Progress-based deadline (see send()): a dispatched response is
      // progress, so the next-oldest request gets a fresh window; with
      // nothing left waiting, clear it so an otherwise-idle connection is
      // never closed by it.
      if (this.pending.length === 0) this.clearRequestTimer();
      else this.armRequestTimer();

      // Echoed response tags: on a tagged connection, verify the echoed tag against
      // the request this response is about to answer — *before* it can
      // reach any caller. A mismatch means the streams are misaligned;
      // unlike the caller-side kind check (`mismatch()`), catching it
      // here stops the misdelivery instead of merely noticing it later.
      if (this.tagged && parsed.response.tag !== waiter.tag) {
        const error = new ConnectionLostError(
          `nanocached: response tag ${parsed.response.tag} does not answer request tag ${waiter.tag} (connection desynced)`,
        );
        this.poison(error);
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
      // Poison, exactly like a read error: the 'close' event then
      // rejects every pending waiter (the stalled request and everything
      // pipelined behind it) with this error, and the client's retry
      // layer redials.
      this.requestTimer = null;
      this.poison(
        new ConnectionLostError(
          `nanocached: no response from server within ${REQUEST_TIMEOUT_TUNING.timeoutMs}ms (request timed out)`,
        ),
      );
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
    const error = this.closeError();
    const waiters = this.pending.splice(0);
    for (const waiter of waiters) waiter.reject(error);
  }

  /** The error used to reject every waiter still pending when the socket
   * closes. Every one of these waiters already had its frame handed to
   * `socket.write()` before this fires — a request that never got that
   * far is rejected synchronously by `sendOnce`'s own closed-at-entry
   * check (`requestWasSent: false`) and never reaches `pending` at all —
   * so none of these can be proven not to have reached the server;
   * always a `ConnectionLostError` at its default `requestWasSent: true`
   * (issue #225). `lastError` may already be a `ConnectionLostError` from
   * `poison()` (a mismatch, a request timeout, a write failure, ...) —
   * used as-is — or a raw socket error Node's own 'error' event recorded
   * via `onError` without going through `poison()` first (e.g. a bare
   * ECONNRESET on read) — wrapped here so every caller checking
   * `requestWasSent` sees a `ConnectionLostError`, never a raw
   * `NodeJS.ErrnoException` that would otherwise skip that check
   * entirely and risk being replayed. */
  private closeError(): Error {
    const cause = this.lastError;
    if (cause === null) return new ConnectionLostError("nanocached: connection closed");
    if (cause instanceof ConnectionLostError) return cause;
    return new ConnectionLostError(`nanocached: connection closed: ${cause.message}`);
  }
}
