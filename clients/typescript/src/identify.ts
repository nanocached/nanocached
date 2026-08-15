import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { connectSocket, type NanocachedTlsOptions } from "./socket.js";

export interface IdentifyOptions {
  host: string;
  port: number;
  authSecret?: string | Uint8Array;
  tls?: boolean | NanocachedTlsOptions;
}

/**
 * A single connection can turn out to be either a cache node (`kind:
 * "node"` — the socket is handed back live, ready for `G`/`S`/`D`) or a
 * discovery server (`kind: "cluster"` — the socket has already been used
 * for `L` and discarded; `nodes` is the address list it returned). Callers
 * never choose which of these to expect: `A`'s response says so (see
 * doc/adr/0007-*.md), which is what lets `NanocachedClient.connect()` take
 * the exact same options for either.
 */
export type IdentifyResult = { kind: "node"; socket: Socket | TLSSocket } | { kind: "cluster"; nodes: string[] };

function toBytes(value: string | Uint8Array): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
}

// Sent as the `A` secret when the caller didn't configure an authSecret.
// A server with no secret configured accepts any non-empty secret without
// even looking at it, so this placeholder authenticates successfully
// there; a server that does require a real secret correctly rejects it,
// same as it would reject any other wrong secret.
const NO_SECRET_PLACEHOLDER = Buffer.from([0]);

/** Reads from `socket` until `tryParse` returns non-null, resolving with
 * that value. One-shot: meant for a single request/response, not a
 * long-lived connection matching multiple in-flight requests. */
function readFrame<T>(socket: Socket | TLSSocket, tryParse: (buf: Buffer) => T | null): Promise<T> {
  return new Promise((resolve, reject) => {
    let buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);

    const cleanup = () => {
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("close", onClose);
    };
    const onData = (chunk: Buffer) => {
      buffer = buffer.length === 0 ? chunk : Buffer.concat([buffer, chunk]);

      let parsed: T | null;
      try {
        parsed = tryParse(buffer);
      } catch (error) {
        cleanup();
        reject(error as Error);
        return;
      }

      if (parsed !== null) {
        cleanup();
        resolve(parsed);
      }
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onClose = () => {
      cleanup();
      reject(new Error("nanocached: connection closed before the expected response arrived"));
    };

    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

interface AuthIdentity {
  accepted: boolean;
  kind: "node" | "cluster";
}

/** Parses the fixed 3-byte reply to `A`: `On\n`/`En\n` from a cache node,
 * `Od\n`/`Ed\n` from a discovery server (see doc/adr/0007-*.md). The
 * second byte is what identifies which kind of server this is — it isn't
 * an accident of the auth outcome, it's present whether accepted or
 * rejected. */
function tryParseIdentity(buf: Buffer): AuthIdentity | null {
  if (buf.length < 3) return null;

  const status = buf[0];
  const type = buf[1];
  if (buf[2] !== 0x0a /* '\n' */) {
    throw new Error("nanocached: unexpected response to A");
  }

  const accepted = status === 0x4f /* 'O' */;
  if (!accepted && status !== 0x45 /* 'E' */) {
    throw new Error("nanocached: unexpected response to A");
  }

  if (type === 0x6e /* 'n' */) return { accepted, kind: "node" };
  if (type === 0x64 /* 'd' */) return { accepted, kind: "cluster" };
  throw new Error("nanocached: unexpected response to A");
}

/** Parses an `N <count>\n` header followed by `count` `<addr>\n` lines,
 * returning `null` while more bytes are still needed. */
function tryParseNodeList(buf: Buffer): string[] | null {
  const headerEnd = buf.indexOf(0x0a);
  if (headerEnd === -1) return null;

  if (buf[0] !== 0x4e /* 'N' */) {
    throw new Error(`nanocached: unexpected response from discovery server: ${buf.subarray(0, headerEnd).toString("ascii")}`);
  }

  const count = Number(buf.subarray(2, headerEnd).toString("ascii"));
  if (!Number.isInteger(count) || count < 0) {
    throw new Error("nanocached: invalid node count in discovery response");
  }

  const nodes: string[] = [];
  let offset = headerEnd + 1;

  for (let i = 0; i < count; i++) {
    const lineEnd = buf.indexOf(0x0a, offset);
    if (lineEnd === -1) return null;
    nodes.push(buf.subarray(offset, lineEnd).toString("utf8"));
    offset = lineEnd + 1;
  }

  return nodes;
}

/**
 * Connects to `options.host:options.port` and figures out, from the
 * server's own response, whether it's a cache node or a discovery server
 * — the caller never says which it expects. Every connection authenticates
 * first (with a placeholder secret if the caller didn't configure one; see
 * `NO_SECRET_PLACEHOLDER`), since identification rides on `A`'s response.
 *
 * A node's socket is handed back live and ready for `G`/`S`/`D`. A
 * discovery server's connection is used once for `L` and then discarded
 * (matching its one-shot fetch-then-close role elsewhere in this SDK).
 */
export async function connectAndIdentify(options: IdentifyOptions): Promise<IdentifyResult> {
  const socket = await connectSocket(options);

  const secret = options.authSecret !== undefined ? toBytes(options.authSecret) : NO_SECRET_PLACEHOLDER;
  const authFrame = Buffer.concat([Buffer.from(`A ${secret.length}\n`, "ascii"), secret]);

  let identity: AuthIdentity;
  try {
    socket.write(authFrame);
    identity = await readFrame(socket, tryParseIdentity);
  } catch (error) {
    socket.destroy();
    throw error;
  }

  if (!identity.accepted) {
    socket.destroy();
    if (options.authSecret === undefined) {
      throw new Error(
        `nanocached: ${options.host}:${options.port} requires authentication, but no authSecret was provided`,
      );
    }
    throw new Error("nanocached: authentication failed");
  }

  if (identity.kind === "node") {
    return { kind: "node", socket };
  }

  try {
    socket.write(Buffer.from("L\n"));
    const nodes = await readFrame(socket, tryParseNodeList);
    return { kind: "cluster", nodes };
  } finally {
    socket.destroy();
  }
}
