/**
 * Wire encoding for nanocached-node's cache protocol (see `src/response.rs`
 * and `src/command.rs` on the Rust side). The `A` (auth/identify) exchange
 * is handled separately in `identify.ts` — by the time a `Connection` uses
 * these encoders/parser, identification is already done and the socket
 * only ever carries `G`/`S`/`D` requests and their responses.
 */

function toAscii(text: string): Buffer {
  return Buffer.from(text, "ascii");
}

export function encodeGet(key: Uint8Array): Buffer {
  return Buffer.concat([toAscii(`G ${key.length}\n`), key]);
}

export function encodeSet(key: Uint8Array, value: Uint8Array, ttlSeconds?: number): Buffer {
  if (ttlSeconds !== undefined && (!Number.isInteger(ttlSeconds) || ttlSeconds < 0)) {
    // A non-integer/negative TTL (3.5, -1, NaN, Infinity) would serialize to a
    // frame the server rejects with no reply, closing the shared, pipelined
    // connection — taking every other in-flight request on it down too. Reject
    // it here, synchronously, before anything is written.
    throw new RangeError(`nanocached: ttlSeconds must be a non-negative integer, got ${ttlSeconds}`);
  }

  const header =
    ttlSeconds === undefined
      ? `S ${key.length} ${value.length}\n`
      : `S ${key.length} ${value.length} ${ttlSeconds}\n`;
  return Buffer.concat([toAscii(header), key, value]);
}

export function encodeDelete(key: Uint8Array): Buffer {
  return Buffer.concat([toAscii(`D ${key.length}\n`), key]);
}

export interface ParsedResponse {
  kind: "value" | "stored" | "deleted" | "notFound" | "busy" | "wrongNode";
  value?: Buffer;
}

// The server never stores values above its 1 MiB request limit, so a
// claimed length beyond this is a corrupt or malicious frame.
const MAX_VALUE_LENGTH = 2 * 1024 * 1024;

const MARKER_STORED = 0x53; // 'S'
const MARKER_DELETED = 0x44; // 'D'
const MARKER_NOT_FOUND = 0x4e; // 'N'
const MARKER_BUSY = 0x42; // 'B'
const MARKER_VALUE = 0x56; // 'V'
const MARKER_WRONG_NODE = 0x57; // 'W'
const LF = 0x0a;

/** Parses one response frame from the front of `buf`, returning `null`
 * while more bytes are still needed. Matches `Response::encode` on the
 * Rust side exactly. */
export function tryParseResponse(buf: Buffer): { response: ParsedResponse; consumed: number } | null {
  if (buf.length === 0) return null;

  switch (buf[0]) {
    case MARKER_STORED:
      return buf.length < 2 ? null : { response: { kind: "stored" }, consumed: 2 };
    case MARKER_DELETED:
      return buf.length < 2 ? null : { response: { kind: "deleted" }, consumed: 2 };
    case MARKER_NOT_FOUND:
      return buf.length < 2 ? null : { response: { kind: "notFound" }, consumed: 2 };
    case MARKER_BUSY:
      return buf.length < 2 ? null : { response: { kind: "busy" }, consumed: 2 };
    case MARKER_WRONG_NODE:
      return buf.length < 2 ? null : { response: { kind: "wrongNode" }, consumed: 2 };

    case MARKER_VALUE: {
      const headerEnd = buf.indexOf(LF);
      if (headerEnd === -1) return null;

      const length = Number(buf.subarray(2, headerEnd).toString("ascii"));
      // Lengths beyond the server's own 1 MiB request cap are protocol
      // garbage — reject before buffering toward them (issue #12).
      if (!Number.isInteger(length) || length < 0 || length > MAX_VALUE_LENGTH) {
        throw new Error("nanocached: invalid value length in response");
      }

      const valueStart = headerEnd + 1;
      const valueEnd = valueStart + length;
      if (buf.length < valueEnd) return null;

      return {
        response: { kind: "value", value: Buffer.from(buf.subarray(valueStart, valueEnd)) },
        consumed: valueEnd,
      };
    }

    default:
      throw new Error(`nanocached: unexpected response from server: ${String.fromCharCode(buf[0])}`);
  }
}
