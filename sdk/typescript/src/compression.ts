import { deflateRawSync, inflateRawSync } from "node:zlib";
import { NanocachedError } from "./errors.js";

// doc/adr/0013-*.md: the one-byte marker every value carries once
// `compress` is enabled on this client — 0x00 for "stored as-is" (below
// threshold, or compressing didn't actually shrink it), 0x01 for
// "raw-DEFLATE-compressed" (RFC 1951, no zlib/gzip wrapper). Never
// present when `compress` is off.
const MARKER_RAW = 0x00;
const MARKER_DEFLATE = 0x01;

// Bounds a DEFLATE value's expanded output. The wire cap (MAX_VALUE_LENGTH)
// bounds only the compressed bytes received; without this, a small,
// highly-repetitive value written by a compromised node could expand to
// gigabytes and exhaust client memory on a plain get (a decompression
// bomb). Far above any realistic cache value.
const MAX_DECOMPRESSED_LENGTH = 64 * 1024 * 1024;

/** Thrown by get/getBytes when a value with `compress` enabled can't be
 * interpreted — almost always a `compress` mismatch between clients
 * sharing this key (see doc/adr/0013-*.md's compatibility caveat: every
 * client touching a given keyspace must agree on `compress`), not a
 * transient failure. */
export class DecompressionError extends NanocachedError {
  constructor(message: string) {
    super(message);
    this.name = "DecompressionError";
  }
}

/** doc/adr/0013-*.md: below `threshold`, or when compressing doesn't
 * actually shrink the value (incompressible data — already-compressed
 * media, random bytes), the marker byte alone is added and the value is
 * stored unchanged. Always returns a value with the marker byte prefixed,
 * since a mix of marked and unmarked values under the same `compress`
 * setting would make `decompressValue` ambiguous. */
export function compressValue(value: Buffer, threshold: number): Buffer {
  if (value.length < threshold) {
    return Buffer.concat([Buffer.from([MARKER_RAW]), value]);
  }

  const compressed = deflateRawSync(value);
  if (compressed.length < value.length) {
    return Buffer.concat([Buffer.from([MARKER_DEFLATE]), compressed]);
  }

  return Buffer.concat([Buffer.from([MARKER_RAW]), value]);
}

/** The `get`/`getBytes` counterpart to `compressValue`. Only ever called
 * when `compress` is enabled on this client — see doc/adr/0013-*.md. */
export function decompressValue(value: Buffer): Buffer {
  if (value.length === 0) {
    throw new DecompressionError(
      "nanocached: compress is enabled but this value has no marker byte (it's empty) — " +
        "was it written by a client with compress disabled?",
    );
  }

  const marker = value[0];
  const body = value.subarray(1);

  if (marker === MARKER_RAW) return Buffer.from(body);

  if (marker === MARKER_DEFLATE) {
    try {
      // maxOutputLength makes zlib throw rather than allocate past the cap.
      return inflateRawSync(body, { maxOutputLength: MAX_DECOMPRESSED_LENGTH });
    } catch (error) {
      throw new DecompressionError(
        `nanocached: failed to decompress a value marked as compressed — did every client ` +
          `sharing this key enable compress (doc/adr/0013-*.md)? (${(error as Error).message})`,
      );
    }
  }

  throw new DecompressionError(
    `nanocached: unrecognized compression marker byte ${marker} — was this value written by a ` +
      "client with compress disabled (doc/adr/0013-*.md)?",
  );
}
