import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { encodeClear, encodeClearAll, encodeDelete, encodeGet, encodeSet, MAX_REQUEST_BYTES, tryParseResponse } from "../src/protocol.js";

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
    for (const wire of ["SX", "DX", "NX", "WX", "CX", "RX"]) {
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
