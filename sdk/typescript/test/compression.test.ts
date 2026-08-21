import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { compressValue, decompressValue, DecompressionError } from "../src/compression.js";

// Value compression: one canonical plaintext and its raw-DEFLATE
// compressed bytes (produced once via Python's zlib, level 6, wbits=-15),
// hardcoded identically into every SDK's test suite — the same
// duplicated-pinned-constant pattern the hash-ring FNV-1a/score vectors
// use. This asserts real cross-language interop: that this SDK's
// decompressor accepts bytes another language's compressor produced, not
// merely that this SDK round-trips its own output.
const CROSS_LANGUAGE_PLAINTEXT = Buffer.from(
  '{"user":"alice","role":"admin","tags":["a","b","c"],"note":"the quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog"}',
  "utf8",
);
const CROSS_LANGUAGE_DEFLATE_HEX =
  "958acb0d833010055b59bd3315d00ae2609bc531b1bdc41f9280d23b7609393ce98d662ed4cc09239" +
  "47786312089e78e4b70b1615136639ca0dad76d06f38028a537e5c1f4aace3c492779475ae5435b0" +
  "d7b26393851d75e9d5f5ac4d21f2d7e37";

describe("compressValue / decompressValue", () => {
  it("round-trips a value at or above the threshold", () => {
    const value = Buffer.from("x".repeat(1000), "utf8");
    const stored = compressValue(value, 256);
    assert.equal(stored[0], 0x01, "expected the DEFLATE marker byte");
    assert.ok(stored.length < value.length, "a highly repetitive value must actually shrink");
    assert.deepEqual(decompressValue(stored), value);
  });

  it("leaves a value below the threshold uncompressed, marker byte only", () => {
    const value = Buffer.from("short", "utf8");
    const stored = compressValue(value, 256);
    assert.equal(stored[0], 0x00);
    assert.deepEqual(stored.subarray(1), value);
    assert.deepEqual(decompressValue(stored), value);
  });

  it("falls back to raw passthrough for incompressible data above the threshold", () => {
    // Random bytes: DEFLATE cannot shrink this, so the marker must stay 0x00.
    const value = randomBytes(512);
    const stored = compressValue(value, 256);
    assert.equal(stored[0], 0x00);
    assert.deepEqual(stored.subarray(1), value);
    assert.deepEqual(decompressValue(stored), value);
  });

  it("round-trips an empty value", () => {
    const stored = compressValue(Buffer.alloc(0), 256);
    assert.deepEqual(decompressValue(stored), Buffer.alloc(0));
  });

  it("decompresses the pinned cross-language vector", () => {
    const compressed = Buffer.concat([Buffer.from([0x01]), Buffer.from(CROSS_LANGUAGE_DEFLATE_HEX, "hex")]);
    assert.deepEqual(decompressValue(compressed), CROSS_LANGUAGE_PLAINTEXT);
  });

  it("throws DecompressionError for an unrecognized marker byte", () => {
    const value = Buffer.from([0x02, 1, 2, 3]);
    assert.throws(() => decompressValue(value), DecompressionError);
  });

  it("throws DecompressionError for a value with no marker byte at all (empty)", () => {
    assert.throws(() => decompressValue(Buffer.alloc(0)), DecompressionError);
  });

  it("throws DecompressionError for a corrupt DEFLATE-marked value", () => {
    const value = Buffer.from([0x01, 0xff, 0xff, 0xff, 0xff]);
    assert.throws(() => decompressValue(value), DecompressionError);
  });
});
