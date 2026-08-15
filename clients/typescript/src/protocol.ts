/**
 * Wire encoding/decoding for nanocached's binary-safe TCP protocol (see the
 * "Protocol" section of the repository README). This module only knows how
 * to turn requests into bytes and bytes into responses; it has no I/O of
 * its own, so it can be unit-tested without a socket.
 */

const LF = 0x0a;

function header(...parts: Array<string | number>): Buffer {
  return Buffer.from(parts.join(" ") + "\n", "ascii");
}

export function encodeAuth(secret: Uint8Array): Buffer {
  return Buffer.concat([header("A", secret.length), secret]);
}

export function encodeGet(key: Uint8Array): Buffer {
  return Buffer.concat([header("G", key.length), key]);
}

export function encodeSet(
  key: Uint8Array,
  value: Uint8Array,
  ttlSeconds?: number,
): Buffer {
  const head =
    ttlSeconds === undefined
      ? header("S", key.length, value.length)
      : header("S", key.length, value.length, ttlSeconds);
  return Buffer.concat([head, key, value]);
}

export function encodeDelete(key: Uint8Array): Buffer {
  return Buffer.concat([header("D", key.length), key]);
}

export type ParsedResponse =
  | { kind: "value"; value: Buffer }
  | { kind: "stored" }
  | { kind: "deleted" }
  | { kind: "notFound" }
  | { kind: "busy" }
  | { kind: "authOk" }
  | { kind: "unauthorized" };

/**
 * Reads one response frame from the front of `buf`, if a complete one is
 * present. Returns `null` (not an error) when more bytes are needed, so
 * callers can accumulate across multiple socket reads the same way the
 * server accumulates requests.
 */
export function tryParseResponse(
  buf: Buffer,
): { response: ParsedResponse; consumed: number } | null {
  const lineEnd = buf.indexOf(LF);
  if (lineEnd === -1) {
    return null;
  }

  const kind = buf[0];

  switch (kind) {
    case 0x56: {
      // 'V'
      const lengthText = buf.subarray(2, lineEnd).toString("ascii");
      const length = Number(lengthText);
      if (!Number.isInteger(length) || length < 0) {
        throw new Error(`invalid value length in response: ${lengthText}`);
      }

      const total = lineEnd + 1 + length;
      if (buf.length < total) {
        return null;
      }

      return {
        response: { kind: "value", value: Buffer.from(buf.subarray(lineEnd + 1, total)) },
        consumed: total,
      };
    }
    case 0x53: // 'S'
      return { response: { kind: "stored" }, consumed: lineEnd + 1 };
    case 0x44: // 'D'
      return { response: { kind: "deleted" }, consumed: lineEnd + 1 };
    case 0x4e: // 'N'
      return { response: { kind: "notFound" }, consumed: lineEnd + 1 };
    case 0x42: // 'B'
      return { response: { kind: "busy" }, consumed: lineEnd + 1 };
    case 0x4f: // 'O'
      return { response: { kind: "authOk" }, consumed: lineEnd + 1 };
    case 0x45: // 'E'
      return { response: { kind: "unauthorized" }, consumed: lineEnd + 1 };
    default:
      throw new Error(`unknown response byte: 0x${kind.toString(16)}`);
  }
}
