import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { encodeAuth } from "./protocol.js";
import { connectSocket, type NanocachedTlsOptions } from "./socket.js";

const LF = 0x0a;
const AUTH_OK = Buffer.from("O\n");

export interface FetchNodesOptions {
  host: string;
  port: number;
  /** Shared secret to authenticate to the discovery server with, matching
   * NANOCACHED_AUTH_SECRET on nanocached-discovery. */
  authSecret?: string | Uint8Array;
  tls?: boolean | NanocachedTlsOptions;
}

function toBytes(value: string | Uint8Array): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
}

/** Reads from `socket` until `tryParse` returns non-null, resolving with
 * that value. One-shot: meant for a single request/response, not a
 * long-lived connection matching multiple in-flight requests. */
function readFrame<T>(socket: Socket | TLSSocket, tryParse: (buf: Buffer) => T | null): Promise<T> {
  return new Promise((resolve, reject) => {
    let buffer: Buffer = Buffer.alloc(0);

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
      reject(new Error("nanocached: discovery server closed the connection"));
    };

    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

/** Parses an `N <count>\n` header followed by `count` `<addr>\n` lines,
 * returning `null` while more bytes are still needed. */
function tryParseNodeList(buf: Buffer): string[] | null {
  const headerEnd = buf.indexOf(LF);
  if (headerEnd === -1) return null;

  if (buf[0] !== 0x4e) {
    // 'N'
    throw new Error(`nanocached: unexpected response from discovery server: ${buf.subarray(0, headerEnd).toString("ascii")}`);
  }

  const count = Number(buf.subarray(2, headerEnd).toString("ascii"));
  if (!Number.isInteger(count) || count < 0) {
    throw new Error("nanocached: invalid node count in discovery response");
  }

  const nodes: string[] = [];
  let offset = headerEnd + 1;

  for (let i = 0; i < count; i++) {
    const lineEnd = buf.indexOf(LF, offset);
    if (lineEnd === -1) return null;
    nodes.push(buf.subarray(offset, lineEnd).toString("utf8"));
    offset = lineEnd + 1;
  }

  return nodes;
}

/**
 * Fetches the current live node list from a nanocached-discovery server
 * (`L` -> `N <count>\n` followed by `count` `<addr>\n` lines). One-shot:
 * opens a connection, reads the list, and closes it — this doesn't track
 * membership changes after the initial fetch, matching bench.rs's own
 * fetch-once-at-startup behavior on the Rust side.
 */
export async function fetchNodes(options: FetchNodesOptions): Promise<string[]> {
  const socket = await connectSocket(options);

  try {
    if (options.authSecret !== undefined) {
      socket.write(encodeAuth(toBytes(options.authSecret)));
      const ack = await readFrame(socket, (buf) => (buf.length >= 2 ? buf.subarray(0, 2) : null));
      if (!ack.equals(AUTH_OK)) {
        throw new Error("nanocached: discovery authentication failed");
      }
    }

    socket.write(Buffer.from("L\n"));
    return await readFrame(socket, tryParseNodeList);
  } finally {
    socket.destroy();
  }
}
