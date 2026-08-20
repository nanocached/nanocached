import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { encodeDelete, encodeGet, encodeSet, MAX_REQUEST_BYTES, tryParseResponse } from "../src/protocol.js";

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
    for (const wire of ["SX", "DX", "NX", "WX"]) {
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

describe("tagged frames (doc/adr/0019-*.md)", () => {
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

  it("keeps the unsolicited busy response bare in tagged mode", () => {
    assert.deepEqual(tryParseResponse(Buffer.from("B\n"), true), {
      response: { kind: "busy" },
      consumed: 2,
    });
  });
});
