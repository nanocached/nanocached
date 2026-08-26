/**
 * Wire encoding for nanocached-node's cache protocol (see `src/response.rs`
 * and `src/command.rs` on the Rust side). The `A` (auth/identify) exchange
 * is handled separately in `identify.ts` — by the time a `Connection` uses
 * these encoders/parser, identification is already done and the socket
 * only ever carries `G`/`S`/`D` (and their namespaced `g`/`s`/`d`
 * counterparts), `c`/`F` (clear a namespace / flush everything, issue
 * #106), `i` (INCR/DECR, issue #129) requests, and their responses.
 */

import { NanocachedError } from "./errors.js";

/** The default namespace — what the un-namespaced `G`/`S`/`D` commands
 * always address (first-class namespaces, issue #105). Every encoder below
 * takes an optional trailing `namespace`, defaulting to this: an empty
 * namespace emits the exact legacy `G`/`S`/`D` frame, byte-for-byte, so a
 * caller that never touches namespaces (or a client that predates this
 * feature) keeps working unchanged — see `src/key.rs`'s doc comment on the
 * Rust side for the same design. Only a non-empty namespace switches to the
 * lowercase `g`/`s`/`d` frame with its extra leading
 * `<namespace-length>` header field. */
export const EMPTY_NAMESPACE: Uint8Array = new Uint8Array(0);

function toAscii(text: string): Buffer {
  return Buffer.from(text, "ascii");
}

// Echoed response tags: on a tagged-mode connection every request header carries the
// client's tag as its last field, and the server echoes it in the
// response — `tag === undefined` is the untagged (pre-0019) form.
function tagField(tag: number | undefined): string {
  return tag === undefined ? "" : ` ${tag}`;
}

// The longest a request header can ever legally be: marker + space + up to
// 10-digit key length + space + up to 10-digit value length + space + up to
// 10-digit TTL + up to 10-digit tag + LF. Rounded well up so MAX_REQUEST_BYTES
// below can be computed without pushing key+value right up against the
// server's own limit and letting the header alone tip a request over it.
// 256 bytes, standardized across every SDK (Go/Rust's original value;
// Java's and .NET's headroom constants match — issue: cross-SDK audit
// finding, headroom constants had drifted to 64/1024 in different SDKs).
const MAX_REQUEST_HEADER_LENGTH = 256;

// Mirrors nanocached-node's own `MAX_REQUEST_SIZE` (src/server.rs, 1 MiB) —
// the cap on a whole request frame (header + key + value) — minus headroom
// for the header itself. A `G`/`D` key, or an `S` key+value, beyond this
// would serialize into a frame the server's `request_is_too_large` check
// rejects outright with no reply at all, exactly like the bad-TTL case
// below: that closes the shared, pipelined connection and takes every
// other in-flight request on it down too.
export const MAX_REQUEST_BYTES = 1024 * 1024 - MAX_REQUEST_HEADER_LENGTH;

// An empty key (`key_length == 0`) hits `ParseError::EmptyKey` in
// src/command.rs, which — like a frame that's too large, and like the
// bad-TTL case in encodeSet below — the server rejects with no reply,
// closing the shared, pipelined connection and taking every other
// in-flight request on it down with it. Reject it here, synchronously,
// before anything is written, for every command that carries a key.
// Namespaces (issue #105) share the key+value size budget rather than
// getting one of their own: the server's own MAX_REQUEST_SIZE check
// (src/server.rs) is over the whole frame — header, namespace, key, and
// value together — so a namespace that pushed the total past
// MAX_REQUEST_BYTES would hit exactly the same no-reply, poisoned-
// connection rejection as an oversized key or value. There is no separate
// per-namespace limit beyond that shared budget (ns-spec.md's SDK-port
// spec, "no limit on ns beyond the request size rules the SDK already
// applies to key+value").
function checkKey(key: Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): void {
  if (key.length === 0) {
    throw new RangeError("nanocached: key must not be empty");
  }
  if (namespace.length + key.length > MAX_REQUEST_BYTES) {
    throw new RangeError(`nanocached: namespace and key together exceed MAX_REQUEST_BYTES (${MAX_REQUEST_BYTES} bytes), got ${namespace.length + key.length} bytes`);
  }
}

// Exported so callers that need to size-check a value *before* it reaches
// encodeSet can share this exact rule — see NanocachedClient.set, which
// checks the pre-compression value this way (issue #47 audit item 3:
// checking only the post-compression frame here let an oversized value
// that compresses well slip past the cap; Python's client.py has the
// same two-layer check for the same reason).
export function checkKeyAndValue(key: Uint8Array, value: Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): void {
  checkKey(key, namespace);
  if (namespace.length + key.length + value.length > MAX_REQUEST_BYTES) {
    // See MAX_REQUEST_BYTES/checkKey above: same server-side rejection, same
    // poisoned-connection consequence, just measured across namespace+key+
    // value together instead of namespace+key alone.
    throw new RangeError(
      `nanocached: namespace, key and value together exceed MAX_REQUEST_BYTES (${MAX_REQUEST_BYTES} bytes), got ${namespace.length + key.length + value.length} bytes`,
    );
  }
}

export function encodeGet(key: Uint8Array, tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  checkKey(key, namespace);
  if (namespace.length === 0) {
    return Buffer.concat([toAscii(`G ${key.length}${tagField(tag)}\n`), key]);
  }
  return Buffer.concat([toAscii(`g ${namespace.length} ${key.length}${tagField(tag)}\n`), namespace, key]);
}

// 0 means no expiry (the default) and is sent on the wire by omitting the
// TTL field entirely — exactly what an absent/undefined TTL meant before
// this field existed; the server has no separate "explicit no-op TTL"
// concept, so any other encoding would be a distinct thing.
export function encodeSet(key: Uint8Array, value: Uint8Array, ttlSeconds = 0, tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  if (!Number.isInteger(ttlSeconds) || ttlSeconds < 0) {
    // A non-integer/negative TTL (3.5, -1, NaN, Infinity) would serialize to a
    // frame the server rejects with no reply, closing the shared, pipelined
    // connection — taking every other in-flight request on it down too. Reject
    // it here, synchronously, before anything is written.
    throw new RangeError(`nanocached: ttlSeconds must be a non-negative integer, got ${ttlSeconds}`);
  }
  checkKeyAndValue(key, value, namespace);

  if (namespace.length === 0) {
    const header =
      ttlSeconds === 0
        ? `S ${key.length} ${value.length}${tagField(tag)}\n`
        : `S ${key.length} ${value.length} ${ttlSeconds}${tagField(tag)}\n`;
    return Buffer.concat([toAscii(header), key, value]);
  }

  const header =
    ttlSeconds === 0
      ? `s ${namespace.length} ${key.length} ${value.length}${tagField(tag)}\n`
      : `s ${namespace.length} ${key.length} ${value.length} ${ttlSeconds}${tagField(tag)}\n`;
  return Buffer.concat([toAscii(header), namespace, key, value]);
}

export function encodeDelete(key: Uint8Array, tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  checkKey(key, namespace);
  if (namespace.length === 0) {
    return Buffer.concat([toAscii(`D ${key.length}${tagField(tag)}\n`), key]);
  }
  return Buffer.concat([toAscii(`d ${namespace.length} ${key.length}${tagField(tag)}\n`), namespace, key]);
}

// Unlike G/S/D, INCR has no uppercase legacy form — it always carries a
// namespace-length header field, with 0 addressing the default namespace
// (issue #129). `delta` is a *signed* wire integer, unlike every other
// integer field this module encodes; `String(delta)` already produces the
// canonical form the wire wants (an optional leading `-`, no leading
// zeros, no `+`).
export function encodeIncr(key: Uint8Array, delta: number, tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  checkKey(key, namespace);
  // JS `number` can't exactly represent the wire protocol's full i64
  // range (it loses precision past 2^53) — `Number.isSafeInteger` is the
  // practical range this SDK can validate and round-trip exactly. A delta
  // outside it (or a non-integer, like ttlSeconds' own check in encodeSet
  // above) would serialize into a frame carrying a value the caller didn't
  // actually mean, so reject it here, synchronously, before anything is
  // written.
  if (!Number.isSafeInteger(delta)) {
    throw new RangeError(`nanocached: delta must be a safe integer (got ${delta}) — see README's incr/decr section for the precision caveat`);
  }
  return Buffer.concat([toAscii(`i ${namespace.length} ${key.length} ${delta}${tagField(tag)}\n`), namespace, key]);
}

// Clear a namespace / flush everything (issue #106). Neither is
// key-addressed — a namespace's keys are spread over every node by HRW —
// so, unlike G/S/D, there's no separate uppercase/lowercase pair keyed on
// whether the namespace is empty: `c` (lowercase) always encodes the
// clear, with namespace-length 0 addressing the default namespace, and
// there's no dedicated uppercase clear command at all (the obvious letter,
// `C`, is already the response marker below). NanocachedClient is what
// turns a namespace-scoped `c`/an `F` into a cluster-wide operation, by
// fanning either out to every node — see its `fanoutClear`.
export function encodeClear(namespace: Uint8Array = EMPTY_NAMESPACE, tag?: number): Buffer {
  // No key here, but a namespace alone can still push a frame past the
  // server's per-request cap — same no-reply, poisoned-connection
  // rejection as an oversized key/value (see checkKey above), so this
  // guards for it too before anything is written.
  if (namespace.length > MAX_REQUEST_BYTES) {
    throw new RangeError(`nanocached: namespace exceeds MAX_REQUEST_BYTES (${MAX_REQUEST_BYTES} bytes), got ${namespace.length} bytes`);
  }
  return Buffer.concat([toAscii(`c ${namespace.length}${tagField(tag)}\n`), namespace]);
}

export function encodeClearAll(tag?: number): Buffer {
  return toAscii(`F${tagField(tag)}\n`);
}

export interface ParsedResponse {
  kind: "value" | "stored" | "deleted" | "notFound" | "busy" | "wrongNode" | "cleared" | "retryable" | "incremented" | "notNumeric";
  value?: Buffer;
  /** echoed response tags: the echoed request tag, present on every response parsed
   * in tagged mode except the unsolicited `busy`. */
  tag?: number;
  /** INCR's optional remaining TTL (issue #129), present only on an
   * `incremented` response when the entry has a TTL — the same
   * optional-trailing-field idiom `S`'s own request-side TTL uses, just
   * mirrored on the response: on an untagged connection 0 trailing header
   * fields after `<value-length>` means no TTL, 1 means TTL present; on a
   * tagged connection 1 trailing field means "just the tag", 2 means
   * "ttl then tag" — disambiguated purely by whether the connection is
   * tagged, never guessed frame by frame. */
  ttlSeconds?: number;
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

// A tag is a u32 in decimal (echoed response tags) — the longest a tagged
// fixed-response frame (`S <tag>\n`) can ever be. Bounds the header
// search the same way MAX_VALUE_HEADER_LENGTH does for `V`.
const MAX_TAG = 0xffffffff;
const MAX_TAGGED_FIXED_FRAME_LENGTH = 2 + String(MAX_TAG).length + 1;

// The longest a complete `V` frame can ever be: its header plus the
// value body — including the echoed tag a tagged-mode header carries
// (`V <len> <tag>\n`, echoed response tags), so a legal near-max frame arriving in
// chunks is never mistaken for a desynced one. Exported so Connection
// can bound total per-frame accumulation as a backstop covering every
// response kind, not just the header search above.
export const MAX_RESPONSE_FRAME_LENGTH =
  MAX_VALUE_HEADER_LENGTH + 1 + String(MAX_TAG).length + MAX_VALUE_LENGTH;

const MARKER_STORED = 0x53; // 'S'
const MARKER_DELETED = 0x44; // 'D'
const MARKER_NOT_FOUND = 0x4e; // 'N'
const MARKER_BUSY = 0x42; // 'B'
const MARKER_VALUE = 0x56; // 'V'
const MARKER_WRONG_NODE = 0x57; // 'W'
const MARKER_CLEARED = 0x43; // 'C' — answers both `c` and `F` (issue #106)
// Retryable-error status (issue #125): this request failed transiently
// (e.g. a proxy's upstream node was briefly unreachable) — the
// connection is fine and stays open, unlike every other status here.
// Possible on any data command (G/S/D/g/s/d/c/F). Only nanocached-proxy
// emits it today, but the SDK must handle it on any connection
// regardless — see Connection.send's bounded retry.
const MARKER_RETRYABLE = 0x52; // 'R'
// INCR (issue #129): a successful increment/decrement — like `V`, carries
// a length-prefixed body (the new counter value, decimal ASCII), plus an
// optional trailing TTL field the request-side marker letters above never
// need. See ParsedResponse.ttlSeconds.
const MARKER_INCREMENTED = 0x49; // 'I'
// INCR (issue #129): the key exists but its stored value isn't INCR's
// counter grammar, or applying `<delta>` would overflow the representable
// range — a new marker, not used by any other op.
const MARKER_NOT_NUMERIC = 0x54; // 'T'
const LF = 0x0a;

// Strict decimal-digits-only, matching Rust/Go/Python's integer parsing —
// bare `Number(field)` also accepts scientific notation ("1e2"), leading
// whitespace (" 5"), and a leading sign ("+5"), any of which would parse a
// desynced/corrupt tag field as if it were a legitimate one.
const TAG_PATTERN = /^\d+$/;

function parseTag(field: string): number {
  if (!TAG_PATTERN.test(field)) {
    throw new NanocachedError("nanocached: invalid response tag");
  }
  const tag = Number(field);
  if (tag > MAX_TAG) {
    throw new NanocachedError("nanocached: invalid response tag");
  }
  return tag;
}

// INCR's optional response-side TTL field (issue #129) — remaining
// whole seconds, same strict decimal-digits-only grammar as a tag (see
// TAG_PATTERN's own doc comment); a desynced/corrupt field must not be
// silently accepted the way bare `Number(field)` would.
function parseTtlSeconds(field: string): number {
  if (!TAG_PATTERN.test(field)) {
    throw new NanocachedError("nanocached: invalid ttl in response");
  }
  return Number(field);
}

/** Parses one response frame from the front of `buf`, returning `null`
 * while more bytes are still needed. Matches `Response::encode` (and,
 * with `tagged`, `Response::encode_with_tag` — echoed response tags) on the Rust
 * side exactly. In tagged mode every response carries a trailing echoed
 * tag, except `busy`, which is unsolicited and always bare. */
export function tryParseResponse(buf: Buffer, tagged = false): { response: ParsedResponse; consumed: number } | null {
  if (buf.length === 0) return null;

  switch (buf[0]) {
    case MARKER_STORED:
    case MARKER_DELETED:
    case MARKER_NOT_FOUND:
    case MARKER_WRONG_NODE:
    case MARKER_CLEARED:
    case MARKER_RETRYABLE:
    case MARKER_NOT_NUMERIC: {
      const kind =
        buf[0] === MARKER_STORED
          ? "stored"
          : buf[0] === MARKER_DELETED
            ? "deleted"
            : buf[0] === MARKER_NOT_FOUND
              ? "notFound"
              : buf[0] === MARKER_WRONG_NODE
                ? "wrongNode"
                : buf[0] === MARKER_CLEARED
                  ? "cleared"
                  : buf[0] === MARKER_RETRYABLE
                    ? "retryable"
                    : "notNumeric";

      if (!tagged) {
        if (buf.length < 2) return null;
        // The untagged form is always exactly `<marker>\n` — a second
        // byte other than LF means the server tagged a response on an
        // untagged connection (or some other desync), and every later
        // response would be misaligned too (issue: audit finding,
        // unverified trailing byte on the untagged fast path).
        if (buf[1] !== LF) {
          throw new NanocachedError("nanocached: unexpected byte after response marker (connection desynced)");
        }
        return { response: { kind }, consumed: 2 };
      }

      const headerEnd = buf.indexOf(LF);
      if (headerEnd === -1) {
        if (buf.length > MAX_TAGGED_FIXED_FRAME_LENGTH) {
          throw new NanocachedError("nanocached: invalid tagged response (missing terminator)");
        }
        return null;
      }
      if (buf[1] !== 0x20 /* ' ' */) {
        throw new NanocachedError("nanocached: response is missing its tag (connection desynced)");
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
          throw new NanocachedError("nanocached: invalid value length in response (missing header terminator)");
        }
        return null;
      }

      // Untagged: `V <len>`. Tagged: `V <len> <tag>` (echoed response tags).
      const fields = buf.subarray(2, headerEnd).toString("ascii").split(" ");
      if (fields.length !== (tagged ? 2 : 1)) {
        throw new NanocachedError("nanocached: invalid value header in response");
      }

      const length = Number(fields[0]);
      // Lengths beyond the server's own 1 MiB request cap are protocol
      // garbage — reject before buffering toward them (issue #12).
      if (!Number.isInteger(length) || length < 0 || length > MAX_VALUE_LENGTH) {
        throw new NanocachedError("nanocached: invalid value length in response");
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

    case MARKER_INCREMENTED: {
      const headerEnd = buf.indexOf(LF);
      if (headerEnd === -1) {
        // Same backstop as MARKER_VALUE above, generously widened for the
        // optional TTL field this response can also carry.
        if (buf.length > MAX_VALUE_HEADER_LENGTH + 1 + String(MAX_TAG).length + (tagged ? 1 + String(MAX_TAG).length : 0)) {
          throw new NanocachedError("nanocached: invalid value length in response (missing header terminator)");
        }
        return null;
      }

      // Untagged: `I <len>` (no TTL) or `I <len> <ttl>`. Tagged: `I <len>
      // <tag>` (no TTL) or `I <len> <ttl> <tag>` — disambiguated purely by
      // field count for the connection's mode (see ParsedResponse.ttlSeconds).
      const fields = buf.subarray(2, headerEnd).toString("ascii").split(" ");
      const minFields = tagged ? 2 : 1;
      const maxFields = tagged ? 3 : 2;
      if (fields.length < minFields || fields.length > maxFields) {
        throw new NanocachedError("nanocached: invalid incremented-value header in response");
      }

      const length = Number(fields[0]);
      if (!Number.isInteger(length) || length < 0 || length > MAX_VALUE_LENGTH) {
        throw new NanocachedError("nanocached: invalid value length in response");
      }

      // A TTL, when present, always sits right after the length; the tag
      // (tagged mode) is always the last field regardless.
      const hasTtl = fields.length === maxFields;
      const ttlSeconds = hasTtl ? parseTtlSeconds(fields[1]) : undefined;
      const tag = tagged ? parseTag(fields[fields.length - 1]) : undefined;

      const valueStart = headerEnd + 1;
      const valueEnd = valueStart + length;
      if (buf.length < valueEnd) return null;

      return {
        response: {
          kind: "incremented",
          value: Buffer.from(buf.subarray(valueStart, valueEnd)),
          ttlSeconds,
          tag,
        },
        consumed: valueEnd,
      };
    }

    default:
      throw new NanocachedError(`nanocached: unexpected response from server: ${String.fromCharCode(buf[0])}`);
  }
}
