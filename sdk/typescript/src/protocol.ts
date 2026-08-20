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

// ADR-0019: on a tagged-mode connection every request header carries the
// client's tag as its last field, and the server echoes it in the
// response — `tag === undefined` is the untagged (pre-0019) form.
function tagField(tag: number | undefined): string {
  return tag === undefined ? "" : ` ${tag}`;
}

export function encodeGet(key: Uint8Array, tag?: number): Buffer {
  return Buffer.concat([toAscii(`G ${key.length}${tagField(tag)}\n`), key]);
}

// 0 means no expiry (the default) and is sent on the wire by omitting the
// TTL field entirely — exactly what an absent/undefined TTL meant before
// this field existed; the server has no separate "explicit no-op TTL"
// concept, so any other encoding would be a distinct thing.
export function encodeSet(key: Uint8Array, value: Uint8Array, ttlSeconds = 0, tag?: number): Buffer {
  if (!Number.isInteger(ttlSeconds) || ttlSeconds < 0) {
    // A non-integer/negative TTL (3.5, -1, NaN, Infinity) would serialize to a
    // frame the server rejects with no reply, closing the shared, pipelined
    // connection — taking every other in-flight request on it down too. Reject
    // it here, synchronously, before anything is written.
    throw new RangeError(`nanocached: ttlSeconds must be a non-negative integer, got ${ttlSeconds}`);
  }

  const header =
    ttlSeconds === 0
      ? `S ${key.length} ${value.length}${tagField(tag)}\n`
      : `S ${key.length} ${value.length} ${ttlSeconds}${tagField(tag)}\n`;
  return Buffer.concat([toAscii(header), key, value]);
}

export function encodeDelete(key: Uint8Array, tag?: number): Buffer {
  return Buffer.concat([toAscii(`D ${key.length}${tagField(tag)}\n`), key]);
}

export interface ParsedResponse {
  kind: "value" | "stored" | "deleted" | "notFound" | "busy" | "wrongNode";
  value?: Buffer;
  /** ADR-0019: the echoed request tag, present on every response parsed
   * in tagged mode except the unsolicited `busy`. */
  tag?: number;
}

// The server's own request cap is 1 MiB; this constant doubles that as
// headroom, so a claimed length beyond it is definitely a corrupt or
// malicious frame, never just a legitimately large value.
const MAX_VALUE_LENGTH = 2 * 1024 * 1024;

// The longest a legal `V <len>\n` header can ever be: marker + space +
// the decimal digits of MAX_VALUE_LENGTH + LF. A buffer that has grown
// past this without an LF can never complete into a legal header, so the
// header search below must not keep waiting for one — a malicious server
// could otherwise withhold the LF forever while the caller buffers
// unboundedly (issue #12 follow-up).
const MAX_VALUE_HEADER_LENGTH = 2 + String(MAX_VALUE_LENGTH).length + 1;

// The longest a complete `V` frame can ever be: its header plus the
// value body. Exported so Connection can bound total per-frame
// accumulation as a backstop covering every response kind, not just the
// header search above.
export const MAX_RESPONSE_FRAME_LENGTH = MAX_VALUE_HEADER_LENGTH + MAX_VALUE_LENGTH;

const MARKER_STORED = 0x53; // 'S'
const MARKER_DELETED = 0x44; // 'D'
const MARKER_NOT_FOUND = 0x4e; // 'N'
const MARKER_BUSY = 0x42; // 'B'
const MARKER_VALUE = 0x56; // 'V'
const MARKER_WRONG_NODE = 0x57; // 'W'
const LF = 0x0a;

// A tag is a u32 in decimal (ADR-0019) — the longest a tagged
// fixed-response frame (`S <tag>\n`) can ever be. Bounds the header
// search the same way MAX_VALUE_HEADER_LENGTH does for `V`.
const MAX_TAG = 0xffffffff;
const MAX_TAGGED_FIXED_FRAME_LENGTH = 2 + String(MAX_TAG).length + 1;

function parseTag(field: string): number {
  const tag = Number(field);
  if (!Number.isInteger(tag) || tag < 0 || tag > MAX_TAG) {
    throw new Error("nanocached: invalid response tag");
  }
  return tag;
}

/** Parses one response frame from the front of `buf`, returning `null`
 * while more bytes are still needed. Matches `Response::encode` (and,
 * with `tagged`, `Response::encode_with_tag` — ADR-0019) on the Rust
 * side exactly. In tagged mode every response carries a trailing echoed
 * tag, except `busy`, which is unsolicited and always bare. */
export function tryParseResponse(buf: Buffer, tagged = false): { response: ParsedResponse; consumed: number } | null {
  if (buf.length === 0) return null;

  switch (buf[0]) {
    case MARKER_STORED:
    case MARKER_DELETED:
    case MARKER_NOT_FOUND:
    case MARKER_WRONG_NODE: {
      const kind =
        buf[0] === MARKER_STORED ? "stored" : buf[0] === MARKER_DELETED ? "deleted" : buf[0] === MARKER_NOT_FOUND ? "notFound" : "wrongNode";

      if (!tagged) {
        return buf.length < 2 ? null : { response: { kind }, consumed: 2 };
      }

      const headerEnd = buf.indexOf(LF);
      if (headerEnd === -1) {
        if (buf.length > MAX_TAGGED_FIXED_FRAME_LENGTH) {
          throw new Error("nanocached: invalid tagged response (missing terminator)");
        }
        return null;
      }
      if (buf[1] !== 0x20 /* ' ' */) {
        throw new Error("nanocached: response is missing its tag (connection desynced)");
      }

      const tag = parseTag(buf.subarray(2, headerEnd).toString("ascii"));
      return { response: { kind, tag }, consumed: headerEnd + 1 };
    }

    case MARKER_BUSY:
      return buf.length < 2 ? null : { response: { kind: "busy" }, consumed: 2 };

    case MARKER_VALUE: {
      const headerEnd = buf.indexOf(LF);
      if (headerEnd === -1) {
        // Keep waiting only while the header could still turn out legal;
        // beyond MAX_VALUE_HEADER_LENGTH with no LF, it never will.
        if (buf.length > MAX_VALUE_HEADER_LENGTH + (tagged ? 1 + String(MAX_TAG).length : 0)) {
          throw new Error("nanocached: invalid value length in response (missing header terminator)");
        }
        return null;
      }

      // Untagged: `V <len>`. Tagged: `V <len> <tag>` (ADR-0019).
      const fields = buf.subarray(2, headerEnd).toString("ascii").split(" ");
      if (fields.length !== (tagged ? 2 : 1)) {
        throw new Error("nanocached: invalid value header in response");
      }

      const length = Number(fields[0]);
      // Lengths beyond the server's own 1 MiB request cap are protocol
      // garbage — reject before buffering toward them (issue #12).
      if (!Number.isInteger(length) || length < 0 || length > MAX_VALUE_LENGTH) {
        throw new Error("nanocached: invalid value length in response");
      }

      const tag = tagged ? parseTag(fields[1]) : undefined;

      const valueStart = headerEnd + 1;
      const valueEnd = valueStart + length;
      if (buf.length < valueEnd) return null;

      return {
        response: { kind: "value", value: Buffer.from(buf.subarray(valueStart, valueEnd)), tag },
        consumed: valueEnd,
      };
    }

    default:
      throw new Error(`nanocached: unexpected response from server: ${String.fromCharCode(buf[0])}`);
  }
}
