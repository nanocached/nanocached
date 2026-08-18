import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { encodeDelete, encodeGet, encodeSet, tryParseResponse } from "../src/protocol.js";

describe("encodeGet", () => {
  it("frames the key with its byte length", () => {
    assert.deepEqual(encodeGet(Buffer.from("key")), Buffer.from("G 3\nkey"));
  });

  it("uses the byte length, not the character count", () => {
    const key = Buffer.from("日本", "utf8"); // 6 bytes, 2 characters
    assert.deepEqual(encodeGet(key), Buffer.concat([Buffer.from("G 6\n"), key]));
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
});

describe("encodeDelete", () => {
  it("frames the key with its byte length", () => {
    assert.deepEqual(encodeDelete(Buffer.from("key")), Buffer.from("D 3\nkey"));
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

  it("throws on an unknown marker", () => {
    assert.throws(() => tryParseResponse(Buffer.from("Z\n")), /unexpected response/);
  });
});
