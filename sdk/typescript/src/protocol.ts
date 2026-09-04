/**
 * Wire encoding for nanocached-node's cache protocol (see `src/response.rs`
 * and `src/command.rs` on the Rust side). The `A` (auth/identify) exchange
 * is handled separately in `identify.ts` — by the time a `Connection` uses
 * these encoders/parser, identification is already done and the socket
 * only ever carries `G`/`S`/`D` (and their namespaced `g`/`s`/`d`
 * counterparts), `c`/`F` (clear a namespace / flush everything, issue
 * #106), `i` (INCR/DECR, issue #129), `k`/`x` (compare-and-set/-delete,
 * issue #141) requests, and their responses.
 */

import { createHash } from "node:crypto";
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
//
// Exported (in addition to checkKeyAndValue below) so NanocachedClient's
// getMany/getManyBytes (issues #128/#150/#151) can validate every key
// eagerly, before any network I/O — the same fail-fast-up-front
// contract every other public method already gets by calling this
// indirectly through its own encoder.
export function checkKey(key: Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): void {
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
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 0) {
    // A non-integer/negative/too-large TTL (3.5, -1, NaN, Infinity, 1e21)
    // would serialize to a frame the server rejects with no reply, closing
    // the shared, pipelined connection — taking every other in-flight
    // request on it down too. `Number.isInteger(1e21)` is true but
    // `${1e21}` serializes as "1e+21", a non-decimal TTL field the
    // server's parser can't read — reject it here, synchronously, before
    // anything is written, same bound as encodeIncr's delta.
    throw new RangeError(`nanocached: ttlSeconds must be a non-negative safe integer, got ${ttlSeconds}`);
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

/** The CAS content digest (issue #141): SHA-256 of `value`'s exact bytes,
 * truncated to the first 16 bytes (128 bits), lowercase hex-encoded (32
 * characters) — computed identically by the server and every SDK (see
 * docs/protocol.html#cas). For a compression-enabled client this must be
 * computed over the raw wire bytes exactly as received (the marker byte
 * included, since the server never decompresses), never the decompressed
 * value the public API returns — see `NanocachedClient.getWithToken`,
 * which taps the raw bytes at the same point `getBytes` does, before this
 * SDK's own decompression step. Exported standalone so a caller that
 * already holds a value in memory can reconstruct its expected digest
 * without a prior GET — see `NanocachedClient.replace`'s own doc comment
 * for when that reconstruction is (and isn't) safe.
 *
 * Pinned cross-language: SHA-256 of the UTF-8 bytes `nanocached-cas-vector`
 * truncates to exactly `36287141940ca57acbd7695ccdde9d43` — every SDK and
 * the server itself are pinned to this same vector (see
 * protocol.test.ts). */
export function contentDigest(value: Uint8Array): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 32);
}

// A digest is 16 bytes hex-encoded, always lowercase (contentDigest never
// produces anything else) — the shape a `<cond>`/`x`'s own `<cond>` digest
// token must match.
const DIGEST_PATTERN = /^[0-9a-f]{32}$/;

// The digest is a bare wire token, not length-prefixed like everything
// else this module encodes (see docs/protocol.html#cas) — its own fixed
// shape is what lets the server tell it apart from `A`/`P` without a
// length. A caller-supplied string that doesn't match this shape exactly
// (wrong length, uppercase, non-hex, or — critically — anything containing
// a space or newline) would corrupt the frame itself rather than just fail
// the condition, so this is checked eagerly, synchronously, before
// anything is written — same rationale as checkKey/the ttlSeconds checks
// above.
function validateDigest(digest: string): void {
  if (!DIGEST_PATTERN.test(digest)) {
    throw new RangeError(`nanocached: token must be a 32-character lowercase hex digest, got ${JSON.stringify(digest)}`);
  }
}

/** Compare-and-set conditions (issue #141): `k`'s bare, non-length-prefixed
 * `<cond>` token identifies its own shape — `absent` succeeds only if the
 * key doesn't currently hold an unexpired value (`add`/`putIfAbsent`),
 * `present` succeeds if it holds any (unexpired) value at all
 * (two-argument `replace(key, value)`), and `digest` succeeds only if the
 * key's current value hashes (see `contentDigest`) to exactly this
 * 32-character lowercase hex string (three-argument
 * `replace(key, old, new)`). `x`'s own `<cond>` (two-argument
 * `remove(key, old)`) only ever uses `digest` — see `encodeCasDelete`. */
export type CasCondition = { readonly kind: "absent" } | { readonly kind: "present" } | { readonly kind: "digest"; readonly digest: string };

function condToken(cond: CasCondition): string {
  switch (cond.kind) {
    case "absent":
      return "A";
    case "present":
      return "P";
    case "digest":
      validateDigest(cond.digest);
      return cond.digest;
  }
}

// `k` — compare-and-set (issue #141): stores `value` only if `cond` holds
// against the key's current stored bytes (see `CasCondition`). On success:
// `S\n`, the same acknowledgement a plain `S` gives. On a condition
// mismatch: `N\n`, reusing the same "nothing here to act on" status
// `G`/`D` already use for a miss — `k` introduces no new response marker,
// so tryParseResponse below needs no changes for it (nor for `x`).
// Always namespaced, like `i` — no pre-namespace legacy form, so
// `<namespace-length>` is unconditionally present. `ttlSeconds` means
// exactly what it means for `S` (omitted/0 = no expiry); unlike `i`, the
// new value is supplied whole by the caller, so there's no old TTL to
// preserve.
export function encodeCas(key: Uint8Array, value: Uint8Array, cond: CasCondition, ttlSeconds = 0, tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 0) {
    // Same rationale as encodeSet's own check: a bad/too-large TTL would
    // serialize into a frame the server rejects with no reply, closing the
    // shared, pipelined connection and taking every other in-flight
    // request on it down too.
    throw new RangeError(`nanocached: ttlSeconds must be a non-negative safe integer, got ${ttlSeconds}`);
  }
  checkKeyAndValue(key, value, namespace);
  const token = condToken(cond);

  const header =
    ttlSeconds === 0
      ? `k ${namespace.length} ${key.length} ${value.length} ${token}${tagField(tag)}\n`
      : `k ${namespace.length} ${key.length} ${value.length} ${token} ${ttlSeconds}${tagField(tag)}\n`;
  return Buffer.concat([toAscii(header), namespace, key, value]);
}

// `x` — compare-and-delete (issue #141): removes the key only if `cond`
// holds — the two-argument `remove(key, old)`. Unlike `k`, `<cond>` here
// is always a digest: an absent/present-only conditioned delete is
// already the plain, unconditional `D`. On success: `D\n`, the same
// acknowledgement a plain `D` gives for a key that existed. On a mismatch
// or a missing key: `N\n`, the same status `D` already gives when there
// was nothing to delete. Always namespaced, same as `k`/`i`.
export function encodeCasDelete(key: Uint8Array, digest: string, tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  checkKey(key, namespace);
  validateDigest(digest);
  return Buffer.concat([toAscii(`x ${namespace.length} ${key.length} ${digest}${tagField(tag)}\n`), namespace, key]);
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

// m/o's header grows with the number of entries — every extra key (`m`)
// or key+value pair (`o`) adds its own decimal length field(s) plus
// separating space(s) to the header line, unlike every fixed-shape
// single-key command above. Bounding a batch's *total* wire size by
// namespace+key(+value) bytes alone (as the running totals below used
// to) silently ignores that growth: MAX_BATCH_KEYS (client.ts) worth of
// entries can add several KiB of header the client never budgeted for,
// on top of a payload already sitting right at MAX_REQUEST_BYTES — the
// same no-reply, poisoned-connection rejection checkKey/checkKeyAndValue
// exist to prevent in the first place (issue #222).
//
// multiGetEntryCost/multiSetEntryCost below size one entry's *honest*
// contribution — its length field(s), the space(s) separating them, and
// the key/value bytes themselves — matching the `lengths`/`lengthFields`
// construction each encoder does a few lines down exactly. Exported so
// NanocachedClient's batch chunking (client.ts's `nextChunkEnd`) can
// budget a sub-frame the same way the encoder itself does, instead of by
// key/value bytes alone.
function decimalDigits(n: number): number {
  return String(n).length;
}

// The `m`/`o` header bytes no entry's own cost accounts for: the marker
// and its space, the namespace-length and entry-count decimal fields
// (each capped at MAX_BATCH_KEYS' own digit count — client.ts never
// batches more than 400 entries), an optional TTL field (`o` only, up to
// 11 bytes: a space plus a couple of headroom digits over ttlSeconds'
// realistic range), an optional tag field (a space plus up to 10 digits
// for a u32), and the trailing LF. 64 bytes leaves comfortable headroom
// over that worst case — mirrors the Go SDK's identically-derived
// multiFrameHeaderSlack (sdk/go/client.go).
export const MULTI_FRAME_HEADER_SLACK = 64;

// One more key's honest wire cost in an `m` frame (issue #222): a
// separating space, that key's decimal length field, and the key bytes
// themselves — exactly what `lengths`/the header construction below adds
// per key.
export function multiGetEntryCost(key: Uint8Array): number {
  return 1 + decimalDigits(key.length) + key.length;
}

// multiGetEntryCost's write-side twin (issue #222), matching
// `lengthFields`/the header construction below: two separating spaces,
// the key's and value's decimal length fields, and the key and value
// bytes themselves.
export function multiSetEntryCost(key: Uint8Array, value: Uint8Array): number {
  return 2 + decimalDigits(key.length) + decimalDigits(value.length) + key.length + value.length;
}

// m/o — batched get/set (issues #128/#150/#151, docs/protocol.html#multi):
// n keys under one round trip through the cache instead of n independent
// get/set calls. Always namespaced, same class as i/k/x above — there is
// no legacy uppercase form to preserve. o shares one ttlSeconds across
// the whole batch, not per key: every real caller of a batched set
// (Django's set_many, cache-manager's mset) already passes one TTL per
// call, so a per-key TTL field would complicate the frame for no
// consumer that exists.
export function encodeMultiGet(keys: readonly Uint8Array[], tag?: number, namespace: Uint8Array = EMPTY_NAMESPACE): Buffer {
  if (keys.length === 0) {
    throw new RangeError("nanocached: encodeMultiGet requires at least one key");
  }
  // Per-key checkKey catches an empty key or one key alone too large;
  // the running total below catches what per-key checking alone would
  // miss — many small keys whose sum still can't fit the server's own
  // per-request cap. Uses multiGetEntryCost (issue #222), not raw key
  // bytes, so this bound is honest about the header each key also adds —
  // see multiGetEntryCost/MULTI_FRAME_HEADER_SLACK above. The FIRST key
  // is exempt from the total check (issue #390), exactly like client.ts's
  // nextChunkEnd and the Go SDK's multiGetChunked: a checkKey-valid key
  // whose entry cost lands within the last ~72 bytes below
  // MAX_REQUEST_BYTES would otherwise pass validation and then trip this
  // bound — while the frame it builds is still safely under the server's
  // real 1 MiB cap, because MAX_REQUEST_BYTES already reserves
  // MAX_REQUEST_HEADER_LENGTH (256 bytes) of headroom that comfortably
  // absorbs MULTI_FRAME_HEADER_SLACK plus one length field.
  let total = namespace.length + MULTI_FRAME_HEADER_SLACK;
  for (let i = 0; i < keys.length; i++) {
    checkKey(keys[i], namespace);
    total += multiGetEntryCost(keys[i]);
    if (i > 0 && total > MAX_REQUEST_BYTES) {
      throw new RangeError(
        `nanocached: namespace and keys together (including their header overhead) exceed MAX_REQUEST_BYTES (${MAX_REQUEST_BYTES} bytes), got ${total} bytes`,
      );
    }
  }

  const lengths = keys.map((key) => ` ${key.length}`).join("");
  const header = `m ${namespace.length} ${keys.length}${lengths}${tagField(tag)}\n`;
  return Buffer.concat([toAscii(header), namespace, ...keys]);
}

export function encodeMultiSet(
  keys: readonly Uint8Array[],
  values: readonly Uint8Array[],
  ttlSeconds = 0,
  tag?: number,
  namespace: Uint8Array = EMPTY_NAMESPACE,
): Buffer {
  if (keys.length === 0) {
    throw new RangeError("nanocached: encodeMultiSet requires at least one key");
  }
  if (keys.length !== values.length) {
    throw new RangeError(`nanocached: encodeMultiSet keys/values length mismatch (${keys.length} vs ${values.length})`);
  }
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 0) {
    throw new RangeError(`nanocached: ttlSeconds must be a non-negative safe integer, got ${ttlSeconds}`);
  }

  // Uses multiSetEntryCost (issue #222), not raw key+value bytes, so
  // this bound is honest about the two header fields each pair also
  // adds — see multiSetEntryCost/MULTI_FRAME_HEADER_SLACK above. The
  // FIRST pair is exempt from the total check (issue #390) for the same
  // reason encodeMultiGet's is: a checkKeyAndValue-valid pair near the
  // bound must not trip an encoder a chunker-built frame can't actually
  // violate — MAX_REQUEST_BYTES' 256-byte header headroom absorbs the
  // slack plus one pair's length fields.
  let total = namespace.length + MULTI_FRAME_HEADER_SLACK;
  const lengthFields: string[] = new Array(keys.length);
  for (let i = 0; i < keys.length; i++) {
    checkKeyAndValue(keys[i], values[i], namespace);
    total += multiSetEntryCost(keys[i], values[i]);
    if (i > 0 && total > MAX_REQUEST_BYTES) {
      throw new RangeError(
        `nanocached: namespace, keys and values together (including their header overhead) exceed MAX_REQUEST_BYTES (${MAX_REQUEST_BYTES} bytes), got ${total} bytes`,
      );
    }
    lengthFields[i] = ` ${keys[i].length} ${values[i].length}`;
  }

  const ttlField = ttlSeconds === 0 ? "" : ` ${ttlSeconds}`;
  const header = `o ${namespace.length} ${keys.length}${lengthFields.join("")}${ttlField}${tagField(tag)}\n`;
  const pieces: Uint8Array[] = [toAscii(header), namespace];
  for (let i = 0; i < keys.length; i++) {
    pieces.push(keys[i], values[i]);
  }
  return Buffer.concat(pieces);
}

/** One key's outcome inside an `M` (multi-get) or `O` (multi-set)
 * response roster (issues #128/#150/#151, docs/protocol.html#multi) — a
 * batch never fails as a whole, so each key's result is independent of
 * every other key's. `"hit"`/`"miss"` are `M`-only (a `O` reply has
 * nothing to echo back); `"stored"` is `O`-only; `"wrongNode"` is
 * shared by both. */
export type MultiEntry = { readonly kind: "hit"; readonly value: Buffer } | { readonly kind: "miss" } | { readonly kind: "wrongNode" };
export type MultiAckEntry = { readonly kind: "stored" } | { readonly kind: "wrongNode" };

export interface ParsedResponse {
  kind:
    | "value"
    | "stored"
    | "deleted"
    | "notFound"
    | "busy"
    | "wrongNode"
    | "cleared"
    | "retryable"
    | "incremented"
    | "notNumeric"
    | "multi"
    | "multiAck";
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
  /** Batched get's per-key roster (issues #128/#150/#151), present only
   * on a `"multi"` response, one entry per requested key in request
   * order (docs/protocol.html#multi). */
  entries?: MultiEntry[];
  /** Batched set's per-key roster, present only on a `"multiAck"`
   * response — see `entries`' own doc comment; the same per-key
   * independence, just without hit bytes to carry. */
  ackEntries?: MultiAckEntry[];
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
// Batched get/set (issues #128/#150/#151): `M` answers `m`, `O` answers
// `o`. Never confused with the `On`/`OnT` identify reply — identify.ts
// handles that before a Connection exists, and no other request's reply
// ever begins with 'O'.
const MARKER_MULTI = 0x4d; // 'M'
const MARKER_MULTI_ACK = 0x4f; // 'O'
const LF = 0x0a;

// The longest a legal `M`/`O` response header can ever be before its
// terminating LF. Unlike every other response kind, a batch's header
// grows with the number of keys, so there's no small fixed bound the way
// MAX_VALUE_HEADER_LENGTH is for `V` — sized instead against
// NanocachedClient's own MAX_BATCH_KEYS (client.ts), which never sends
// more than that many keys in one `m`/`o`: a roster token costs at most
// `len(String(MAX_VALUE_LENGTH)) + 1 = 8` bytes (a decimal hit length
// plus its separating space), so 400 keys comfortably fit under 4 KiB
// with a tag field to spare — the same derivation the Go SDK's
// maxHeaderLineLength/maxBatchKeys pair uses (sdk/go/connection.go,
// sdk/go/client.go). A peer whose claimed roster would need a longer
// header than this can never complete anyway (see parseMultiHeader).
const MAX_MULTI_HEADER_LENGTH = 4096;

// Bounds the SUM of every hit's declared length across one `M` reply
// (issue #207, follow-up to the Java fix's issue #179/PR #201). Each
// individual hit length is already capped at MAX_VALUE_LENGTH above, but
// that alone doesn't bound the reply as a whole: a node answering a
// 400-key multi-get with 400 x MAX_VALUE_LENGTH (2 MiB) hits would force
// ~800 MB of allocation from a single reply. Reuses the 64 MiB figure
// MAX_DECOMPRESSED_LENGTH (compression.ts) already established as "far
// above any realistic cache value/reply". Exported as a mutable object
// only so tests can shrink it, mirroring REQUEST_TIMEOUT_TUNING.
export const MULTI_GET_TUNING = { maxResponseBytes: 64 * 1024 * 1024 };

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
// silently accepted the way bare `Number(field)` would. Issue #233: also
// bounded by magnitude, same as parseTag — a long-enough digit string
// parses to `Infinity` (or a value that's silently lost precision) via
// bare `Number()` instead of failing, unlike Rust's `str::parse::<u64>`
// (this field's wire type), which errors on overflow.
function parseTtlSeconds(field: string): number {
  if (!TAG_PATTERN.test(field)) {
    throw new NanocachedError("nanocached: invalid ttl in response");
  }
  const ttlSeconds = Number(field);
  if (!Number.isSafeInteger(ttlSeconds)) {
    throw new NanocachedError("nanocached: invalid ttl in response");
  }
  return ttlSeconds;
}

// Same strict-decimal-digits-only, magnitude-checked parsing as
// parseTtlSeconds above (issue #233's TAG_PATTERN/Number.isSafeInteger
// discipline), shared by the multi-header `count` field and each
// per-key `length` token in an `M` reply — those used bare `Number()` +
// `Number.isInteger`, which (unlike this) accepts scientific notation,
// leading `+`/whitespace, and precision-losing long digit strings.
// Returns `undefined` rather than throwing so each call site can attach
// its own error message (and, for peekMultiFrameLength, keep its
// never-throws contract).
export function parseStrictInteger(field: string): number | undefined {
  if (!TAG_PATTERN.test(field)) return undefined;
  const value = Number(field);
  return Number.isSafeInteger(value) ? value : undefined;
}

// INCR/DECR's counter-body grammar (issue #462) — decimal ASCII digits
// with an optional single leading `-` (never `+`), 1-19 digits (an int64
// never needs more), matching the Python SDK's own `_INCR_VALUE_RE`
// (_connection.py) and .NET's `TryParseWireCounter` (Connection.cs),
// which likewise rejects a leading `+` before parsing. Deliberately
// doesn't reject on magnitude the way parseStrictInteger does: unlike a
// length or a tag, a counter legitimately exceeds
// `Number.MAX_SAFE_INTEGER` (Connection.incr's own doc comment) — that's
// `CounterOutOfRangeError`'s job (client.ts), a distinct failure from
// this wire-grammar check. Returns `undefined` rather than throwing so
// the caller can poison the connection with its own message, same as
// parseStrictInteger's call sites.
const COUNTER_PATTERN = /^-?[0-9]{1,19}$/;

export function parseCounterValue(field: string): number | undefined {
  return COUNTER_PATTERN.test(field) ? Number(field) : undefined;
}

interface MultiHeader {
  count: number;
  tokens: string[];
  tag?: number;
  headerEnd: number;
}

// Shared by tryParseResponse's MARKER_MULTI/MARKER_MULTI_ACK cases and
// peekMultiFrameLength below, so the two can never disagree about what a
// legal `M`/`O` header looks like. Returns null while the header line
// itself is still incomplete (bounded by MAX_MULTI_HEADER_LENGTH, like
// every other header search in this module); throws on anything
// malformed once the LF is seen — a lying `count` can never cause an
// out-of-bounds read, since `fields` is already fully materialized from
// the one (already length-bounded) header line before count is even
// read.
function parseMultiHeader(buf: Buffer, tagged: boolean): MultiHeader | null {
  const headerEnd = buf.indexOf(LF);
  if (headerEnd === -1) {
    if (buf.length > MAX_MULTI_HEADER_LENGTH) {
      throw new NanocachedError("nanocached: invalid multi-get/multi-set response (missing header terminator)");
    }
    return null;
  }
  // Applied unconditionally, not just on the incomplete-header branch
  // above: a peer that delivers a complete header + LF within a single
  // chunk (realistic — Node can hand `onData` far more than 4KB at once)
  // must not skip this cap, since `count` below is otherwise unbounded
  // and drives `new Array(header.count)` further down.
  if (headerEnd > MAX_MULTI_HEADER_LENGTH) {
    throw new NanocachedError("nanocached: invalid multi-get/multi-set response (header too long)");
  }

  const fields = buf.subarray(2, headerEnd).toString("ascii").split(" ");
  const count = parseStrictInteger(fields[0]);
  if (count === undefined) {
    throw new NanocachedError("nanocached: invalid multi-get/multi-set count in response");
  }
  const expectedFields = 1 + count + (tagged ? 1 : 0);
  if (fields.length !== expectedFields) {
    throw new NanocachedError("nanocached: invalid multi-get/multi-set header in response");
  }

  const tokens = fields.slice(1, 1 + count);
  const tag = tagged ? parseTag(fields[1 + count]) : undefined;
  return { count, tokens, tag, headerEnd };
}

/** Peeks the total byte length an in-progress `M`/`O` response frame
 * will need once complete, without waiting for the body — unlike every
 * other response kind, a batch's total size depends on its roster (the
 * sum of however many hit lengths `M` declares), so Connection's own
 * incomplete-frame backstop (MAX_RESPONSE_FRAME_LENGTH, sized for one
 * value) can't bound it without this. Returns undefined while that
 * isn't computable yet (header incomplete, or the buffer isn't a multi
 * response at all) — never throws; a malformed header is left for
 * tryParseResponse itself to reject the next time it's actually
 * invoked. */
export function peekMultiFrameLength(buf: Buffer, tagged: boolean): number | undefined {
  if (buf.length === 0 || (buf[0] !== MARKER_MULTI && buf[0] !== MARKER_MULTI_ACK)) return undefined;

  let header: MultiHeader | null;
  try {
    header = parseMultiHeader(buf, tagged);
  } catch {
    return undefined;
  }
  if (header === null) return undefined;

  if (buf[0] === MARKER_MULTI_ACK) return header.headerEnd + 1; // no body

  let total = header.headerEnd + 1;
  for (const token of header.tokens) {
    if (token === "-" || token === "W") continue;
    const length = parseStrictInteger(token);
    if (length === undefined) return undefined;
    total += length;
  }
  return total;
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

      // Strict decimal-digits-only (issue #462), same as parseStrictInteger's
      // other call sites — bare `Number(fields[0])` would accept "+5", " 5",
      // and "1e2" as if they were legitimate lengths.
      const length = parseStrictInteger(fields[0]);
      // Lengths beyond the server's own 1 MiB request cap are protocol
      // garbage — reject before buffering toward them (issue #12).
      if (length === undefined || length > MAX_VALUE_LENGTH) {
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

      // Strict decimal-digits-only (issue #462) — see MARKER_VALUE above.
      const length = parseStrictInteger(fields[0]);
      if (length === undefined || length > MAX_VALUE_LENGTH) {
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

    // `M` — batched get's reply (issues #128/#150/#151): `M <n>
    // <result-1>...<result-n>[ <tag>]\n<hit values, concatenated in
    // request order>`. Each token is a decimal byte length (a hit —
    // that many body bytes belong to this key, in order), "-" (a clean
    // miss), or "W" (this node doesn't own this particular key). The
    // whole header is parsed before any body byte is read, so the
    // total body length is known up front — see peekMultiFrameLength,
    // which Connection uses to bound its own incomplete-frame backstop
    // for exactly this reason, and MULTI_GET_TUNING below, which rejects
    // a roster whose declared total already exceeds a sane bound before
    // ever waiting for (or allocating) that body (issue #207).
    case MARKER_MULTI: {
      const header = parseMultiHeader(buf, tagged);
      if (header === null) return null;

      const hitLengths: number[] = new Array(header.count);
      let bodyLength = 0;
      for (let i = 0; i < header.count; i++) {
        const token = header.tokens[i];
        if (token === "-" || token === "W") {
          hitLengths[i] = -1;
          continue;
        }
        const length = parseStrictInteger(token);
        if (length === undefined || length > MAX_VALUE_LENGTH) {
          throw new NanocachedError("nanocached: invalid multi-get result length in response");
        }
        hitLengths[i] = length;
        bodyLength += length;
        // Checked as soon as the running total crosses the bound — while
        // the header is still being read, before ever waiting for (let
        // alone allocating) the oversized body itself (issue #207).
        if (bodyLength > MULTI_GET_TUNING.maxResponseBytes) {
          throw new NanocachedError(
            `nanocached: multi-get response exceeds ${MULTI_GET_TUNING.maxResponseBytes} bytes (connection desynced)`,
          );
        }
      }

      const bodyStart = header.headerEnd + 1;
      const bodyEnd = bodyStart + bodyLength;
      if (buf.length < bodyEnd) return null;

      const entries: MultiEntry[] = new Array(header.count);
      let offset = bodyStart;
      for (let i = 0; i < header.count; i++) {
        const token = header.tokens[i];
        if (token === "-") {
          entries[i] = { kind: "miss" };
        } else if (token === "W") {
          entries[i] = { kind: "wrongNode" };
        } else {
          const length = hitLengths[i];
          entries[i] = { kind: "hit", value: Buffer.from(buf.subarray(offset, offset + length)) };
          offset += length;
        }
      }

      return { response: { kind: "multi", entries, tag: header.tag }, consumed: bodyEnd };
    }

    // `O` — batched set's reply (issues #150/#151): `O <n>
    // <result-1>...<result-n>[ <tag>]\n` — no body, unlike `M`'s hit
    // values (a set has nothing to echo back). Each token is "S"
    // (stored) or "W" (wrong node). No MULTI_GET_TUNING-style cumulative
    // bound needed here (issue #207, #179): every token is a fixed-width
    // single character with no length-prefixed body, so this map is
    // already O(count) and count is already bounded by parseMultiHeader
    // rejecting an overlong header line (MAX_MULTI_HEADER_LENGTH).
    case MARKER_MULTI_ACK: {
      const header = parseMultiHeader(buf, tagged);
      if (header === null) return null;

      const ackEntries: MultiAckEntry[] = header.tokens.map((token) => {
        if (token === "S") return { kind: "stored" };
        if (token === "W") return { kind: "wrongNode" };
        throw new NanocachedError("nanocached: invalid multi-set result token in response");
      });

      return { response: { kind: "multiAck", ackEntries, tag: header.tag }, consumed: header.headerEnd + 1 };
    }

    default:
      throw new NanocachedError(`nanocached: unexpected response from server: ${String.fromCharCode(buf[0])}`);
  }
}
