import { afterEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  contentDigest,
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
  MAX_REQUEST_BYTES,
  MULTI_GET_TUNING,
  peekMultiFrameLength,
  tryParseResponse,
} from "../src/protocol.js";

const NS = Buffer.from("users");

describe("encodeGet", () => {
  it("frames the key with its byte length", () => {
    assert.deepEqual(encodeGet(Buffer.from("key")), Buffer.from("G 3\nkey"));
  });

  it("uses the byte length, not the character count", () => {
    const key = Buffer.from("日本", "utf8"); // 6 bytes, 2 characters
    assert.deepEqual(encodeGet(key), Buffer.concat([Buffer.from("G 6\n"), key]));
  });

  it("rejects an empty key synchronously", () => {
    assert.throws(() => encodeGet(Buffer.alloc(0)), RangeError);
  });

  it("rejects a key beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeGet(Buffer.alloc(MAX_REQUEST_BYTES + 1)), RangeError);
  });

  it("accepts a key of exactly MAX_REQUEST_BYTES", () => {
    const key = Buffer.alloc(MAX_REQUEST_BYTES, "k");
    assert.doesNotThrow(() => encodeGet(key));
  });
});

describe("encodeSet", () => {
  it("frames key and value without a TTL by default", () => {
    assert.deepEqual(encodeSet(Buffer.from("key"), Buffer.from("value")), Buffer.from("S 3 5\nkeyvalue"));
  });

  it("appends the TTL when given", () => {
    assert.deepEqual(encodeSet(Buffer.from("k"), Buffer.from("v"), 60), Buffer.from("S 1 1 60\nkv"));
  });

  it("treats a TTL of zero as no expiry, omitting it from the wire like the default", () => {
    assert.deepEqual(encodeSet(Buffer.from("k"), Buffer.from("v"), 0), Buffer.from("S 1 1\nkv"));
  });

  it("rejects non-integer and negative TTLs synchronously", () => {
    for (const ttl of [3.5, -1, NaN, Infinity]) {
      assert.throws(() => encodeSet(Buffer.from("k"), Buffer.from("v"), ttl), RangeError);
    }
  });

  it("rejects an empty key synchronously", () => {
    assert.throws(() => encodeSet(Buffer.alloc(0), Buffer.from("v")), RangeError);
  });

  it("rejects a key+value combination beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(
      () => encodeSet(Buffer.alloc(MAX_REQUEST_BYTES), Buffer.from("x")),
      RangeError,
    );
  });

  it("accepts a key+value combination of exactly MAX_REQUEST_BYTES", () => {
    const key = Buffer.alloc(MAX_REQUEST_BYTES - 1, "k");
    const value = Buffer.from("v");
    assert.doesNotThrow(() => encodeSet(key, value));
  });
});

describe("encodeDelete", () => {
  it("frames the key with its byte length", () => {
    assert.deepEqual(encodeDelete(Buffer.from("key")), Buffer.from("D 3\nkey"));
  });

  it("rejects an empty key synchronously", () => {
    assert.throws(() => encodeDelete(Buffer.alloc(0)), RangeError);
  });

  it("rejects a key beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeDelete(Buffer.alloc(MAX_REQUEST_BYTES + 1)), RangeError);
  });
});

describe("encodeIncr (issue #129)", () => {
  it("always frames the namespace length, even for the default namespace", () => {
    assert.deepEqual(encodeIncr(Buffer.from("key"), 1), Buffer.from("i 0 3 1\nkey"));
  });

  it("frames a signed positive delta in canonical decimal form", () => {
    assert.deepEqual(encodeIncr(Buffer.from("key"), 5), Buffer.from("i 0 3 5\nkey"));
  });

  it("frames a signed negative delta with a leading minus, no other punctuation", () => {
    assert.deepEqual(encodeIncr(Buffer.from("key"), -5), Buffer.from("i 0 3 -5\nkey"));
  });

  it("frames a named namespace ahead of the key, matching g/s/d's field order", () => {
    assert.deepEqual(encodeIncr(Buffer.from("alpha"), 3, undefined, NS), Buffer.concat([Buffer.from("i 5 5 3\n"), NS, Buffer.from("alpha")]));
  });

  it("appends the tag as the last header field", () => {
    assert.deepEqual(encodeIncr(Buffer.from("key"), 1, 7), Buffer.from("i 0 3 1 7\nkey"));
    assert.deepEqual(encodeIncr(Buffer.from("key"), -1, 7, NS), Buffer.concat([Buffer.from("i 5 3 -1 7\n"), NS, Buffer.from("key")]));
  });

  it("rejects an empty key synchronously", () => {
    assert.throws(() => encodeIncr(Buffer.alloc(0), 1), RangeError);
  });

  it("rejects a key beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeIncr(Buffer.alloc(MAX_REQUEST_BYTES + 1), 1), RangeError);
  });

  it("rejects a non-safe-integer delta synchronously", () => {
    for (const delta of [3.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, -(Number.MAX_SAFE_INTEGER + 1)]) {
      assert.throws(() => encodeIncr(Buffer.from("key"), delta), RangeError);
    }
  });

  it("accepts the boundary safe-integer deltas", () => {
    assert.doesNotThrow(() => encodeIncr(Buffer.from("key"), Number.MAX_SAFE_INTEGER));
    assert.doesNotThrow(() => encodeIncr(Buffer.from("key"), -Number.MAX_SAFE_INTEGER));
    assert.doesNotThrow(() => encodeIncr(Buffer.from("key"), 0));
  });
});

describe("encodeMultiGet (issues #128/#150/#151)", () => {
  it("always frames the namespace length, even for the default namespace", () => {
    assert.deepEqual(encodeMultiGet([Buffer.from("a"), Buffer.from("bb")]), Buffer.from("m 0 2 1 2\nabb"));
  });

  it("frames a named namespace ahead of the keys, matching i's field order", () => {
    assert.deepEqual(
      encodeMultiGet([Buffer.from("a")], undefined, NS),
      Buffer.concat([Buffer.from("m 5 1 1\n"), NS, Buffer.from("a")]),
    );
  });

  it("appends the tag as the last header field", () => {
    assert.deepEqual(encodeMultiGet([Buffer.from("a")], 9), Buffer.from("m 0 1 1 9\na"));
  });

  it("accepts a binary namespace and binary keys, no delimiter or escaping", () => {
    const ns = Buffer.from([0xff, 0x00]);
    const key = Buffer.from([0x01, 0x02, 0x03]);
    assert.deepEqual(encodeMultiGet([key], undefined, ns), Buffer.concat([Buffer.from("m 2 1 3\n"), ns, key]));
  });

  it("rejects an empty keys array synchronously", () => {
    assert.throws(() => encodeMultiGet([]), RangeError);
  });

  it("rejects an empty key inside the batch synchronously", () => {
    assert.throws(() => encodeMultiGet([Buffer.from("a"), Buffer.alloc(0)]), RangeError);
  });

  it("rejects a single key beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeMultiGet([Buffer.alloc(MAX_REQUEST_BYTES + 1)]), RangeError);
  });

  it("rejects many keys whose combined size exceeds MAX_REQUEST_BYTES even though no single key does", () => {
    const keys = Array.from({ length: 10 }, () => Buffer.alloc(Math.ceil(MAX_REQUEST_BYTES / 9)));
    assert.throws(() => encodeMultiGet(keys), RangeError);
  });

  // issue #390: a checkKey-valid key whose entry cost (space + decimal
  // length field + slack) lands within the last ~72 bytes below
  // MAX_REQUEST_BYTES used to pass validation and then trip the
  // encoder's own total bound — a bound the frame doesn't actually
  // violate server-side, since MAX_REQUEST_BYTES already reserves 256
  // bytes of header headroom. The first entry is exempt from the total
  // check, exactly like nextChunkEnd's and Go's chunkers.
  it("accepts a lone checkKey-valid key even when its header overhead overshoots the total bound", () => {
    const key = Buffer.alloc(MAX_REQUEST_BYTES); // checkKey's exact limit
    const frame = encodeMultiGet([key]);
    assert.ok(frame.length > MAX_REQUEST_BYTES);
    assert.ok(frame.length < 1024 * 1024); // still under the server's real cap
  });
});

describe("encodeMultiSet (issues #150/#151)", () => {
  const keys = [Buffer.from("a"), Buffer.from("bb")];
  const values = [Buffer.from("x"), Buffer.from("yy")];

  it("omits the TTL field when zero (the default), same as encodeSet", () => {
    assert.deepEqual(encodeMultiSet(keys, values), Buffer.from("o 0 2 1 1 2 2\naxbbyy"));
  });

  it("appends the TTL after the length fields when given", () => {
    assert.deepEqual(encodeMultiSet(keys, values, 60), Buffer.from("o 0 2 1 1 2 2 60\naxbbyy"));
  });

  it("frames a named namespace ahead of the keys", () => {
    assert.deepEqual(
      encodeMultiSet(keys, values, 0, undefined, NS),
      Buffer.concat([Buffer.from("o 5 2 1 1 2 2\n"), NS, Buffer.from("axbbyy")]),
    );
  });

  it("appends the tag as the last header field, after the TTL when both are present", () => {
    assert.deepEqual(encodeMultiSet(keys, values, 0, 9), Buffer.from("o 0 2 1 1 2 2 9\naxbbyy"));
    assert.deepEqual(encodeMultiSet(keys, values, 60, 9), Buffer.from("o 0 2 1 1 2 2 60 9\naxbbyy"));
  });

  it("rejects an empty keys array synchronously", () => {
    assert.throws(() => encodeMultiSet([], []), RangeError);
  });

  it("rejects a keys/values length mismatch synchronously", () => {
    assert.throws(() => encodeMultiSet([Buffer.from("a")], []), RangeError);
  });

  it("rejects an empty key inside the batch synchronously", () => {
    assert.throws(() => encodeMultiSet([Buffer.alloc(0)], [Buffer.from("v")]), RangeError);
  });

  it("rejects non-integer and negative TTLs synchronously", () => {
    for (const ttl of [3.5, -1, NaN, Infinity]) {
      assert.throws(() => encodeMultiSet(keys, values, ttl), RangeError);
    }
  });

  it("rejects many pairs whose combined size exceeds MAX_REQUEST_BYTES even though no single pair does", () => {
    const bigKeys = Array.from({ length: 10 }, () => Buffer.alloc(1));
    const bigValues = Array.from({ length: 10 }, () => Buffer.alloc(Math.ceil(MAX_REQUEST_BYTES / 9)));
    assert.throws(() => encodeMultiSet(bigKeys, bigValues), RangeError);
  });
});

describe("contentDigest (issue #141)", () => {
  it("matches the pinned cross-language test vector", () => {
    // SHA-256 of the UTF-8 bytes "nanocached-cas-vector", truncated to the
    // first 16 bytes, lowercase hex — pinned identically into the Rust
    // server and every SDK (docs/protocol.html#cas). A mismatch here means
    // CAS silently breaks across languages.
    assert.equal(contentDigest(Buffer.from("nanocached-cas-vector", "utf8")), "36287141940ca57acbd7695ccdde9d43");
  });

  it("produces a 32-character lowercase hex string", () => {
    const digest = contentDigest(Buffer.from("some arbitrary value", "utf8"));
    assert.match(digest, /^[0-9a-f]{32}$/);
  });

  it("is a pure function of the exact bytes — differs for different content", () => {
    assert.notEqual(contentDigest(Buffer.from("a")), contentDigest(Buffer.from("b")));
  });

  it("is deterministic", () => {
    const value = Buffer.from("deterministic", "utf8");
    assert.equal(contentDigest(value), contentDigest(value));
  });
});

describe("encodeCas (issue #141)", () => {
  it("frames an absent condition with the bare `A` token", () => {
    assert.deepEqual(encodeCas(Buffer.from("key"), Buffer.from("value"), { kind: "absent" }), Buffer.from("k 0 3 5 A\nkeyvalue"));
  });

  it("frames a present condition with the bare `P` token", () => {
    assert.deepEqual(encodeCas(Buffer.from("key"), Buffer.from("value"), { kind: "present" }), Buffer.from("k 0 3 5 P\nkeyvalue"));
  });

  it("frames a digest condition with the raw digest string, not length-prefixed", () => {
    const digest = "0123456789abcdef0123456789abcdef";
    assert.deepEqual(
      encodeCas(Buffer.from("key"), Buffer.from("value"), { kind: "digest", digest: digest.slice(0, 32) }),
      Buffer.from(`k 0 3 5 ${digest.slice(0, 32)}\nkeyvalue`),
    );
  });

  it("always frames the namespace length, even for the default namespace", () => {
    assert.deepEqual(encodeCas(Buffer.from("key"), Buffer.from("v"), { kind: "absent" }), Buffer.from("k 0 3 1 A\nkeyv"));
  });

  it("frames a named namespace ahead of the key, matching i's field order", () => {
    assert.deepEqual(
      encodeCas(Buffer.from("alpha"), Buffer.from("v"), { kind: "present" }, undefined, undefined, NS),
      Buffer.concat([Buffer.from("k 5 5 1 P\n"), NS, Buffer.from("alpha"), Buffer.from("v")]),
    );
  });

  it("omits the TTL field when zero (the default), same as encodeSet", () => {
    assert.deepEqual(encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "absent" }, 0), Buffer.from("k 0 1 1 A\nkv"));
  });

  it("appends the TTL after the cond token when given", () => {
    assert.deepEqual(encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "absent" }, 60), Buffer.from("k 0 1 1 A 60\nkv"));
  });

  it("appends the tag as the last header field, after the TTL when both are present", () => {
    assert.deepEqual(encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "present" }, 0, 7), Buffer.from("k 0 1 1 P 7\nkv"));
    assert.deepEqual(encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "present" }, 60, 7), Buffer.from("k 0 1 1 P 60 7\nkv"));
  });

  it("rejects an empty key synchronously", () => {
    assert.throws(() => encodeCas(Buffer.alloc(0), Buffer.from("v"), { kind: "absent" }), RangeError);
  });

  it("rejects a key+value combination beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeCas(Buffer.alloc(MAX_REQUEST_BYTES), Buffer.from("x"), { kind: "absent" }), RangeError);
  });

  it("rejects non-integer and negative TTLs synchronously", () => {
    for (const ttl of [3.5, -1, NaN, Infinity]) {
      assert.throws(() => encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "absent" }, ttl), RangeError);
    }
  });

  it("rejects a digest condition that isn't a 32-character lowercase hex string", () => {
    for (const bad of ["", "A", "P", "not-hex-at-all-not-hex-at-all!!", "0".repeat(31), "0".repeat(33), "0123456789abcdef0123456789abcdef".toUpperCase(), "abc def0123456789abcdef012345 "]) {
      assert.throws(() => encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "digest", digest: bad }), RangeError);
    }
  });

  it("rejects a digest condition containing a space or newline (frame-corruption guard)", () => {
    assert.throws(
      () => encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "digest", digest: "0123456789abcdef 123456789abcdef" }),
      RangeError,
    );
    assert.throws(
      () => encodeCas(Buffer.from("k"), Buffer.from("v"), { kind: "digest", digest: "0123456789abcdef\n123456789abcde" }),
      RangeError,
    );
  });
});

describe("encodeCasDelete (issue #141)", () => {
  const digest = "36287141940ca57acbd7695ccdde9d43";

  it("frames the digest as a bare token, not length-prefixed", () => {
    assert.deepEqual(encodeCasDelete(Buffer.from("key"), digest), Buffer.from(`x 0 3 ${digest}\nkey`));
  });

  it("always frames the namespace length, even for the default namespace", () => {
    assert.deepEqual(encodeCasDelete(Buffer.from("key"), digest), Buffer.from(`x 0 3 ${digest}\nkey`));
  });

  it("frames a named namespace ahead of the key", () => {
    assert.deepEqual(
      encodeCasDelete(Buffer.from("alpha"), digest, undefined, NS),
      Buffer.concat([Buffer.from(`x 5 5 ${digest}\n`), NS, Buffer.from("alpha")]),
    );
  });

  it("appends the tag as the last header field", () => {
    assert.deepEqual(encodeCasDelete(Buffer.from("key"), digest, 7), Buffer.from(`x 0 3 ${digest} 7\nkey`));
  });

  it("rejects an empty key synchronously", () => {
    assert.throws(() => encodeCasDelete(Buffer.alloc(0), digest), RangeError);
  });

  it("rejects a key beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeCasDelete(Buffer.alloc(MAX_REQUEST_BYTES + 1), digest), RangeError);
  });

  it("rejects a digest that isn't a 32-character lowercase hex string", () => {
    for (const bad of ["", "A", "P", digest.slice(0, 31), digest + "0", digest.toUpperCase()]) {
      assert.throws(() => encodeCasDelete(Buffer.from("key"), bad), RangeError);
    }
  });
});

describe("encodeClear / encodeClearAll (issue #106)", () => {
  it("frames the default namespace as namespace-length 0, with no body", () => {
    assert.deepEqual(encodeClear(), Buffer.from("c 0\n"));
    assert.deepEqual(encodeClear(Buffer.alloc(0)), Buffer.from("c 0\n"));
  });

  it("frames a named namespace's length ahead of its bytes", () => {
    assert.deepEqual(encodeClear(NS), Buffer.concat([Buffer.from("c 5\n"), NS]));
  });

  it("encodeClearAll is just the bare marker, no body at all", () => {
    assert.deepEqual(encodeClearAll(), Buffer.from("F\n"));
  });

  it("appends the tag as the last header field on both", () => {
    assert.deepEqual(encodeClear(NS, 7), Buffer.concat([Buffer.from("c 5 7\n"), NS]));
    assert.deepEqual(encodeClear(Buffer.alloc(0), 7), Buffer.from("c 0 7\n"));
    assert.deepEqual(encodeClearAll(9), Buffer.from("F 9\n"));
  });

  it("rejects a namespace beyond MAX_REQUEST_BYTES synchronously", () => {
    assert.throws(() => encodeClear(Buffer.alloc(MAX_REQUEST_BYTES + 1)), RangeError);
  });

  it("accepts a namespace of exactly MAX_REQUEST_BYTES", () => {
    assert.doesNotThrow(() => encodeClear(Buffer.alloc(MAX_REQUEST_BYTES, "n")));
  });
});

describe("tryParseResponse", () => {
  it("returns null on an empty buffer", () => {
    assert.equal(tryParseResponse(Buffer.alloc(0)), null);
  });

  it("parses each two-byte status response", () => {
    const cases = [
      ["S\n", "stored"],
      ["D\n", "deleted"],
      ["N\n", "notFound"],
      ["B\n", "busy"],
      ["W\n", "wrongNode"],
      ["C\n", "cleared"],
      // Retryable-error status (issue #125): possible on any data command.
      ["R\n", "retryable"],
      // INCR (issue #129): the stored value isn't INCR's counter grammar,
      // or applying delta would overflow.
      ["T\n", "notNumeric"],
    ] as const;

    for (const [wire, kind] of cases) {
      const parsed = tryParseResponse(Buffer.from(wire));
      assert.deepEqual(parsed, { response: { kind }, consumed: 2 });
    }
  });

  it("waits for the newline of a status response", () => {
    assert.equal(tryParseResponse(Buffer.from("S")), null);
  });

  it("throws when an untagged S/D/N/W response's second byte isn't a newline", () => {
    // Issue: audit finding — the untagged form is always exactly
    // `<marker>\n`; a second byte other than LF means the streams are
    // desynced (this used to be accepted silently, with consumed: 2).
    for (const wire of ["SX", "DX", "NX", "WX", "CX", "RX", "TX"]) {
      assert.throws(() => tryParseResponse(Buffer.from(wire)), /connection desynced/);
    }
  });

  it("parses a value response", () => {
    const parsed = tryParseResponse(Buffer.from("V 5\nhello"));
    assert.equal(parsed?.consumed, 9);
    assert.equal(parsed?.response.kind, "value");
    assert.deepEqual(parsed?.response.value, Buffer.from("hello"));
  });

  it("parses an empty value", () => {
    const parsed = tryParseResponse(Buffer.from("V 0\n"));
    assert.equal(parsed?.response.kind, "value");
    assert.deepEqual(parsed?.response.value, Buffer.alloc(0));
  });

  it("returns null while a value's header or body is incomplete", () => {
    assert.equal(tryParseResponse(Buffer.from("V 5")), null);
    assert.equal(tryParseResponse(Buffer.from("V 5\nhel")), null);
  });

  it("consumes only the first frame when several are buffered", () => {
    const buf = Buffer.from("V 2\nhiS\n");
    const first = tryParseResponse(buf);
    assert.deepEqual(first?.response.value, Buffer.from("hi"));
    assert.equal(first?.consumed, 6);

    const second = tryParseResponse(buf.subarray(first!.consumed));
    assert.equal(second?.response.kind, "stored");
  });

  it("copies the value out of the shared receive buffer", () => {
    const buf = Buffer.from("V 2\nhi");
    const parsed = tryParseResponse(buf);
    buf.fill(0);
    assert.deepEqual(parsed?.response.value, Buffer.from("hi"));
  });

  it("throws on an invalid value length", () => {
    assert.throws(() => tryParseResponse(Buffer.from("V x\n")), /invalid value length/);
    assert.throws(() => tryParseResponse(Buffer.from("V -1\n")), /invalid value length/);
  });

  it("keeps waiting for a `V` header's newline while it could still be legal", () => {
    // The longest legal header is "V " + digits of MAX_VALUE_LENGTH (7
    // digits) + "\n" = 10 bytes; one byte short of that, still unterminated,
    // must still mean "need more data", not "garbage".
    assert.equal(tryParseResponse(Buffer.from(`V ${"9".repeat(6)}`)), null);
  });

  it("throws instead of buffering forever when a `V` header's newline never arrives", () => {
    // Regression for the unbounded-buffer-growth issue (issue #12
    // follow-up): a malicious/corrupted server could otherwise withhold
    // the LF forever, growing the caller's buffer without limit. Once
    // the header has grown past what any legal header could be, this
    // must throw immediately rather than return null.
    assert.throws(
      () => tryParseResponse(Buffer.concat([Buffer.from("V "), Buffer.alloc(4096, 0x39 /* '9' */)])),
      /invalid value length|missing header terminator/,
    );
  });

  it("throws on an unknown marker", () => {
    assert.throws(() => tryParseResponse(Buffer.from("Z\n")), /unexpected response/);
  });
});

describe("tryParseResponse — INCR's `I` response (issue #129)", () => {
  it("parses a successful increment with no TTL", () => {
    const parsed = tryParseResponse(Buffer.from("I 2\n42"));
    assert.equal(parsed?.consumed, 6);
    assert.deepEqual(parsed?.response, { kind: "incremented", value: Buffer.from("42"), ttlSeconds: undefined, tag: undefined });
  });

  it("parses a successful increment with a TTL", () => {
    const parsed = tryParseResponse(Buffer.from("I 2 60\n42"));
    assert.equal(parsed?.consumed, 9);
    assert.deepEqual(parsed?.response, { kind: "incremented", value: Buffer.from("42"), ttlSeconds: 60, tag: undefined });
  });

  it("parses a negative counter value", () => {
    const parsed = tryParseResponse(Buffer.from("I 3\n-42"));
    assert.deepEqual(parsed?.response.value, Buffer.from("-42"));
  });

  it("returns null while the header or body is incomplete", () => {
    assert.equal(tryParseResponse(Buffer.from("I 2")), null);
    assert.equal(tryParseResponse(Buffer.from("I 2\n4")), null);
  });

  it("parses a tagged increment with no TTL — just the tag", () => {
    const parsed = tryParseResponse(Buffer.from("I 2 9\n42"), true);
    assert.equal(parsed?.consumed, 8);
    assert.deepEqual(parsed?.response, { kind: "incremented", value: Buffer.from("42"), ttlSeconds: undefined, tag: 9 });
  });

  it("parses a tagged increment with a TTL — ttl then tag", () => {
    const parsed = tryParseResponse(Buffer.from("I 2 60 9\n42"), true);
    assert.equal(parsed?.consumed, 11);
    assert.deepEqual(parsed?.response, { kind: "incremented", value: Buffer.from("42"), ttlSeconds: 60, tag: 9 });
  });

  it("throws on a tagged increment missing its tag entirely", () => {
    assert.throws(() => tryParseResponse(Buffer.from("I 2\n42"), true), /invalid incremented-value header/);
  });

  it("throws on an untagged increment with too many header fields", () => {
    assert.throws(() => tryParseResponse(Buffer.from("I 2 60 9\n42")), /invalid incremented-value header/);
  });

  it("throws on an invalid value length", () => {
    assert.throws(() => tryParseResponse(Buffer.from("I x\n")), /invalid value length/);
  });

  it("throws on a non-decimal ttl field", () => {
    assert.throws(() => tryParseResponse(Buffer.from("I 2 abc\n42")), /invalid ttl/);
  });

  it("throws on a ttl field with no magnitude bound (issue #233)", () => {
    // Regression: unlike parseTag's MAX_TAG check, parseTtlSeconds used
    // to accept any all-digit string, so a desynced/corrupt field long
    // enough to overflow `Number.isSafeInteger` (or even `Number` itself,
    // returning `Infinity`) was silently accepted instead of raising a
    // protocol error.
    const hugeButFinite = "9".repeat(30); // parses to a finite double, but well past 2^53-1
    assert.throws(() => tryParseResponse(Buffer.from(`I 2 ${hugeButFinite}\n42`)), /invalid ttl/);
    const overflowsToInfinity = "9".repeat(400);
    assert.throws(() => tryParseResponse(Buffer.from(`I 2 ${overflowsToInfinity}\n42`)), /invalid ttl/);
  });
});

describe("tryParseResponse — batched get/set M/O (issues #128/#150/#151)", () => {
  it("parses a roster of hits, a miss, and a wrong-node, with hit bytes concatenated in order", () => {
    const parsed = tryParseResponse(Buffer.from("M 3 1 - 2\nabc"));
    assert.equal(parsed?.consumed, 13);
    assert.deepEqual(parsed?.response, {
      kind: "multi",
      entries: [{ kind: "hit", value: Buffer.from("a") }, { kind: "miss" }, { kind: "hit", value: Buffer.from("bc") }],
      tag: undefined,
    });
  });

  it("parses an all-wrong-node roster with no body at all", () => {
    const parsed = tryParseResponse(Buffer.from("M 2 W W\n"));
    assert.equal(parsed?.consumed, 8);
    assert.deepEqual(parsed?.response.entries, [{ kind: "wrongNode" }, { kind: "wrongNode" }]);
  });

  it("returns null while the header or a hit's body is incomplete", () => {
    assert.equal(tryParseResponse(Buffer.from("M 2 1")), null);
    assert.equal(tryParseResponse(Buffer.from("M 2 1 1\na")), null); // second hit's byte hasn't arrived yet
  });

  it("parses a tagged multi-get response", () => {
    const parsed = tryParseResponse(Buffer.from("M 1 - 9\n"), true);
    assert.equal(parsed?.consumed, 8);
    assert.deepEqual(parsed?.response, { kind: "multi", entries: [{ kind: "miss" }], tag: 9 });
  });

  it("throws on a malformed roster (count disagrees with the field count actually present)", () => {
    assert.throws(() => tryParseResponse(Buffer.from("M 3 1 -\n")), /invalid multi-get\/multi-set header/);
  });

  it("throws on an invalid hit length", () => {
    assert.throws(() => tryParseResponse(Buffer.from("M 1 x\n")), /invalid multi-get result length/);
  });

  describe("cumulative response size bound (issue #207)", () => {
    const originalBound = MULTI_GET_TUNING.maxResponseBytes;
    afterEach(() => {
      MULTI_GET_TUNING.maxResponseBytes = originalBound;
    });

    it("throws once the running total of hit lengths crosses the bound, even before the body arrives", () => {
      MULTI_GET_TUNING.maxResponseBytes = 3;
      // Header alone declares 2 + 2 = 4 bytes, already over the shrunk
      // 3-byte bound — must throw off the header, never waiting for (or
      // allocating) either hit's body.
      assert.throws(() => tryParseResponse(Buffer.from("M 2 2 2\n")), /multi-get response exceeds 3 bytes/);
    });

    it("does not throw for a roster whose total sits right at the bound", () => {
      MULTI_GET_TUNING.maxResponseBytes = 3;
      const parsed = tryParseResponse(Buffer.from("M 1 3\nabc"));
      assert.deepEqual(parsed?.response.entries, [{ kind: "hit", value: Buffer.from("abc") }]);
    });
  });

  it("stores results are S/W, with no body — the reply completes the instant its header is read", () => {
    const parsed = tryParseResponse(Buffer.from("O 2 S W\nEXTRA"));
    assert.equal(parsed?.consumed, 8);
    assert.deepEqual(parsed?.response, { kind: "multiAck", ackEntries: [{ kind: "stored" }, { kind: "wrongNode" }], tag: undefined });
  });

  it("parses a tagged multi-set response", () => {
    const parsed = tryParseResponse(Buffer.from("O 1 S 9\n"), true);
    assert.deepEqual(parsed?.response, { kind: "multiAck", ackEntries: [{ kind: "stored" }], tag: 9 });
  });

  it("returns null while an O header is incomplete", () => {
    assert.equal(tryParseResponse(Buffer.from("O 2 S")), null);
  });

  it("throws on an invalid multi-set result token", () => {
    assert.throws(() => tryParseResponse(Buffer.from("O 1 X\n")), /invalid multi-set result token/);
  });

  it("consumes only the first frame when several M/O frames are buffered back to back", () => {
    const parsed = tryParseResponse(Buffer.from("M 1 -\nO 1 S\n"));
    assert.equal(parsed?.consumed, 6);
    assert.equal(parsed?.response.kind, "multi");
  });

  it("copies hit bytes out of the shared receive buffer, not a view into it", () => {
    const buf = Buffer.from("M 1 2\nab");
    const parsed = tryParseResponse(buf);
    const entries = parsed?.response.entries;
    buf.fill(0);
    assert.deepEqual(entries?.[0], { kind: "hit", value: Buffer.from("ab") });
  });
});

describe("peekMultiFrameLength (issues #128/#150/#151)", () => {
  it("computes the total M frame length from the header alone, before the body arrives", () => {
    // Header declares two 5-byte hits; only 3 of those 10 body bytes
    // have actually arrived yet.
    const buf = Buffer.from("M 2 5 5\nabc");
    assert.equal(peekMultiFrameLength(buf, false), "M 2 5 5\n".length + 10);
  });

  it("returns the header length alone for O, which has no body", () => {
    const buf = Buffer.from("O 2 S W");
    assert.equal(peekMultiFrameLength(buf, false), undefined); // header not yet terminated
    assert.equal(peekMultiFrameLength(Buffer.from("O 2 S W\n"), false), "O 2 S W\n".length);
  });

  it("returns undefined for a non-multi marker", () => {
    assert.equal(peekMultiFrameLength(Buffer.from("V 5\nAlice"), false), undefined);
  });

  it("returns undefined while the header line itself hasn't arrived yet", () => {
    assert.equal(peekMultiFrameLength(Buffer.from("M 2 5"), false), undefined);
  });

  it("returns undefined on an empty buffer", () => {
    assert.equal(peekMultiFrameLength(Buffer.alloc(0), false), undefined);
  });

  it("accounts for the tag field in tagged mode", () => {
    const buf = Buffer.from("M 1 3 9\nabc");
    assert.equal(peekMultiFrameLength(buf, true), "M 1 3 9\n".length + 3);
  });
});

describe("tagged frames (echoed response tags)", () => {
  it("appends the tag as the last request header field", () => {
    assert.deepEqual(encodeGet(Buffer.from("key"), 7), Buffer.from("G 3 7\nkey"));
    assert.deepEqual(encodeDelete(Buffer.from("key"), 8), Buffer.from("D 3 8\nkey"));
    assert.deepEqual(encodeSet(Buffer.from("k"), Buffer.from("v"), 0, 9), Buffer.from("S 1 1 9\nkv"));
    assert.deepEqual(encodeSet(Buffer.from("k"), Buffer.from("v"), 60, 9), Buffer.from("S 1 1 60 9\nkv"));
  });

  it("parses tagged status responses", () => {
    assert.deepEqual(tryParseResponse(Buffer.from("S 7\n"), true), {
      response: { kind: "stored", tag: 7 },
      consumed: 4,
    });
    assert.deepEqual(tryParseResponse(Buffer.from("N 4294967295\n"), true), {
      response: { kind: "notFound", tag: 4294967295 },
      consumed: 13,
    });
    assert.deepEqual(tryParseResponse(Buffer.from("C 7\n"), true), {
      response: { kind: "cleared", tag: 7 },
      consumed: 4,
    });
    // Retryable-error status (issue #125): tagged like every other
    // request-answering status above.
    assert.deepEqual(tryParseResponse(Buffer.from("R 7\n"), true), {
      response: { kind: "retryable", tag: 7 },
      consumed: 4,
    });
    // INCR (issue #129): tagged like every other status marker above.
    assert.deepEqual(tryParseResponse(Buffer.from("T 7\n"), true), {
      response: { kind: "notNumeric", tag: 7 },
      consumed: 4,
    });
  });

  it("parses a tagged value response", () => {
    assert.deepEqual(tryParseResponse(Buffer.from("V 5 9\nAlice"), true), {
      response: { kind: "value", value: Buffer.from("Alice"), tag: 9 },
      consumed: 11,
    });
  });

  it("waits for the rest of a tagged status frame", () => {
    assert.equal(tryParseResponse(Buffer.from("S 12"), true), null);
  });

  it("throws when a tagged response is missing its tag", () => {
    assert.throws(() => tryParseResponse(Buffer.from("S\n"), true), /missing its tag/);
    assert.throws(() => tryParseResponse(Buffer.from("V 5\nAlice"), true), /invalid value header/);
  });

  it("rejects a tag field that isn't strictly decimal digits", () => {
    // Regression (issue #47 audit item 4): bare `Number(field)` also
    // accepts scientific notation, leading whitespace, and a leading
    // sign — any of which would parse a desynced/corrupt tag field as if
    // it were a legitimate one.
    assert.throws(() => tryParseResponse(Buffer.from("S 1e2\n"), true), /invalid response tag/);
    assert.throws(() => tryParseResponse(Buffer.from("S  5\n"), true), /invalid response tag/);
    assert.throws(() => tryParseResponse(Buffer.from("S +5\n"), true), /invalid response tag/);
  });

  it("still parses an ordinary all-digit tag", () => {
    assert.deepEqual(tryParseResponse(Buffer.from("S 100\n"), true), {
      response: { kind: "stored", tag: 100 },
      consumed: 6,
    });
  });

  it("keeps the unsolicited busy response bare in tagged mode", () => {
    assert.deepEqual(tryParseResponse(Buffer.from("B\n"), true), {
      response: { kind: "busy" },
      consumed: 2,
    });
  });
});

describe("namespaced encoders (first-class namespaces, issue #105)", () => {
  it("an empty (default) namespace emits the exact legacy G/S/D frame, byte-for-byte", () => {
    // The SDK rule from ns-spec.md's SDK-port spec: an unchanged client
    // talking to an old server must keep working, so the default
    // namespace never touches the lowercase g/s/d path.
    assert.deepEqual(encodeGet(Buffer.from("key"), undefined, Buffer.alloc(0)), encodeGet(Buffer.from("key")));
    assert.deepEqual(
      encodeSet(Buffer.from("k"), Buffer.from("v"), 60, undefined, Buffer.alloc(0)),
      encodeSet(Buffer.from("k"), Buffer.from("v"), 60),
    );
    assert.deepEqual(encodeDelete(Buffer.from("key"), undefined, Buffer.alloc(0)), encodeDelete(Buffer.from("key")));
  });

  it("encodeGet frames the namespace length ahead of the key length", () => {
    assert.deepEqual(encodeGet(Buffer.from("alpha"), undefined, NS), Buffer.concat([Buffer.from("g 5 5\n"), NS, Buffer.from("alpha")]));
  });

  it("encodeDelete frames the namespace length ahead of the key length", () => {
    assert.deepEqual(encodeDelete(Buffer.from("alpha"), undefined, NS), Buffer.concat([Buffer.from("d 5 5\n"), NS, Buffer.from("alpha")]));
  });

  it("encodeSet frames the namespace length first, with no TTL by default", () => {
    assert.deepEqual(
      encodeSet(Buffer.from("k"), Buffer.from("v"), 0, undefined, NS),
      Buffer.concat([Buffer.from("s 5 1 1\n"), NS, Buffer.from("kv")]),
    );
  });

  it("encodeSet appends the TTL after the three lengths when given", () => {
    assert.deepEqual(
      encodeSet(Buffer.from("k"), Buffer.from("v"), 60, undefined, NS),
      Buffer.concat([Buffer.from("s 5 1 1 60\n"), NS, Buffer.from("kv")]),
    );
  });

  it("keeps the tag as the last header field, with and without a TTL", () => {
    assert.deepEqual(encodeGet(Buffer.from("alpha"), 7, NS), Buffer.concat([Buffer.from("g 5 5 7\n"), NS, Buffer.from("alpha")]));
    assert.deepEqual(encodeDelete(Buffer.from("alpha"), 8, NS), Buffer.concat([Buffer.from("d 5 5 8\n"), NS, Buffer.from("alpha")]));
    assert.deepEqual(
      encodeSet(Buffer.from("k"), Buffer.from("v"), 0, 9, NS),
      Buffer.concat([Buffer.from("s 5 1 1 9\n"), NS, Buffer.from("kv")]),
    );
    assert.deepEqual(
      encodeSet(Buffer.from("k"), Buffer.from("v"), 60, 9, NS),
      Buffer.concat([Buffer.from("s 5 1 1 60 9\n"), NS, Buffer.from("kv")]),
    );
  });

  it("a namespace may contain arbitrary bytes — no delimiter, no escaping", () => {
    const binaryNs = Buffer.from([0xff, 0x00]);
    assert.deepEqual(
      encodeGet(Buffer.from("beta"), undefined, binaryNs),
      Buffer.concat([Buffer.from("g 2 4\n"), binaryNs, Buffer.from("beta")]),
    );
  });

  it("a namespace-length of 0 (explicitly passed as an empty buffer) is the same request as the legacy form", () => {
    assert.deepEqual(encodeGet(Buffer.from("name"), undefined, Buffer.alloc(0)), Buffer.from("G 4\nname"));
  });

  it("still rejects an empty key when namespaced", () => {
    assert.throws(() => encodeGet(Buffer.alloc(0), undefined, NS), RangeError);
    assert.throws(() => encodeSet(Buffer.alloc(0), Buffer.from("v"), 0, undefined, NS), RangeError);
    assert.throws(() => encodeDelete(Buffer.alloc(0), undefined, NS), RangeError);
  });

  it("counts the namespace toward MAX_REQUEST_BYTES alongside the key", () => {
    // ns-spec.md: "no limit on ns beyond the request size rules the SDK
    // already applies to key+value" — so namespace+key (encodeGet/
    // encodeDelete) and namespace+key+value (encodeSet) share the same
    // budget the unnamespaced forms already enforce.
    const namespace = Buffer.alloc(MAX_REQUEST_BYTES - 2, "n");
    assert.doesNotThrow(() => encodeGet(Buffer.from("ab"), undefined, namespace));
    assert.throws(() => encodeGet(Buffer.from("abc"), undefined, namespace), RangeError);

    const smallNamespace = Buffer.alloc(MAX_REQUEST_BYTES - 2, "n");
    assert.doesNotThrow(() => encodeSet(Buffer.from("a"), Buffer.from("b"), 0, undefined, smallNamespace));
    assert.throws(() => encodeSet(Buffer.from("a"), Buffer.from("bc"), 0, undefined, smallNamespace), RangeError);
  });
});
