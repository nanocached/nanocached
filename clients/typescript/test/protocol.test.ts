import assert from "node:assert/strict";
import { test } from "node:test";
import {
  encodeAuth,
  encodeDelete,
  encodeGet,
  encodeSet,
  tryParseResponse,
} from "../src/protocol.js";

test("encodeGet writes the key length before the key", () => {
  assert.deepEqual(encodeGet(Buffer.from("name")), Buffer.from("G 4\nname"));
});

test("encodeSet without a TTL omits the ttl field", () => {
  assert.deepEqual(
    encodeSet(Buffer.from("name"), Buffer.from("Alice")),
    Buffer.from("S 4 5\nnameAlice"),
  );
});

test("encodeSet with a TTL includes it after the value length", () => {
  assert.deepEqual(
    encodeSet(Buffer.from("name"), Buffer.from("Alice"), 60),
    Buffer.from("S 4 5 60\nnameAlice"),
  );
});

test("encodeDelete writes the key length before the key", () => {
  assert.deepEqual(encodeDelete(Buffer.from("name")), Buffer.from("D 4\nname"));
});

test("encodeAuth writes the secret length before the secret", () => {
  assert.deepEqual(encodeAuth(Buffer.from("s3cret")), Buffer.from("A 6\ns3cret"));
});

test("tryParseResponse returns null when the header is incomplete", () => {
  assert.equal(tryParseResponse(Buffer.from("V 5")), null);
});

test("tryParseResponse returns null while the value body is still arriving", () => {
  assert.equal(tryParseResponse(Buffer.from("V 5\nAli")), null);
});

test("tryParseResponse reads a value response and reports bytes consumed", () => {
  const result = tryParseResponse(Buffer.from("V 5\nAliceG 4\nname"));
  assert.deepEqual(result?.response, { kind: "value", value: Buffer.from("Alice") });
  assert.equal(result?.consumed, "V 5\nAlice".length);
});

test("tryParseResponse reads a not-found response", () => {
  const result = tryParseResponse(Buffer.from("N\n"));
  assert.deepEqual(result?.response, { kind: "notFound" });
  assert.equal(result?.consumed, 2);
});

test("tryParseResponse reads stored, deleted, busy, authOk, and unauthorized responses", () => {
  assert.deepEqual(tryParseResponse(Buffer.from("S\n"))?.response, { kind: "stored" });
  assert.deepEqual(tryParseResponse(Buffer.from("D\n"))?.response, { kind: "deleted" });
  assert.deepEqual(tryParseResponse(Buffer.from("B\n"))?.response, { kind: "busy" });
  assert.deepEqual(tryParseResponse(Buffer.from("O\n"))?.response, { kind: "authOk" });
  assert.deepEqual(tryParseResponse(Buffer.from("E\n"))?.response, { kind: "unauthorized" });
});

test("tryParseResponse rejects an unknown response byte", () => {
  assert.throws(() => tryParseResponse(Buffer.from("Z\n")), /unknown response byte/);
});

test("tryParseResponse rejects a non-numeric value length", () => {
  assert.throws(() => tryParseResponse(Buffer.from("V x\n")), /invalid value length/);
});
