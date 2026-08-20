import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { ConnectionLostError } from "./connection.js";
import { NanocachedError } from "./errors.js";
import { CONNECT_DEADLINE_MS, connectSocket } from "./socket.js";

export interface IdentifyOptions {
  host: string;
  port: number;
  authSecret?: string;
  tls?: boolean;
  /** PEM-encoded trusted root certificate(s), already read from disk once
   * by the caller. See `ConnectSocketOptions.ca`. */
  ca?: Buffer;
  /** Bound on each phase of connecting (dial, handshake read); defaults
   * to `CONNECT_DEADLINE_MS`. Exposed for tests. */
  connectDeadlineMs?: number;
}

/** A node's consistent-hashing identity (a random per-process UUID) and
 * its network address (`host:port`) — two different things since
 * doc/adr/0009-*.md. `name` is what a hash ring must be built from, so
 * every party (this client, another client, or a node computing a
 * handoff) agrees on cluster membership; `address` is only for opening a
 * connection, and carries no identity meaning of its own. */
export interface DiscoveredNode {
  name: string;
  address: string;
}

/**
 * A single connection can turn out to be either a cache node (`kind:
 * "node"` — the socket is handed back live, ready for `G`/`S`/`D`) or a
 * discovery server (`kind: "cluster"` — the socket has already been used
 * for `L` and discarded; `nodes` is the name/address list it returned).
 * Callers never choose which of these to expect: `A`'s response says so
 * (see doc/adr/0007-*.md), which is what lets `NanocachedClient.connect()`
 * take the exact same options for either.
 */
export type IdentifyResult =
  // `tagged` (ADR-0019): the node accepted the extended `A ... T`, so
  // this socket's `G`/`S`/`D` traffic must carry tags and its responses
  // echo them; false means an older node answered the plain-`A` fallback.
  | { kind: "node"; socket: Socket | TLSSocket; tagged: boolean }
  // `replication` (ADR-0011) is discovery's replication factor R — how
  // many nodes hold each key. It rides the `L` response so clients can't
  // skew from the cluster's setting.
  | { kind: "cluster"; nodes: DiscoveredNode[]; replication: number };

// Sent as the `A` secret when the caller didn't configure an authSecret.
// A server with no secret configured accepts any non-empty secret without
// even looking at it, so this placeholder authenticates successfully
// there; a server that does require a real secret correctly rejects it,
// same as it would reject any other wrong secret.
const NO_SECRET_PLACEHOLDER = Buffer.from([0]);

// Bound a discovery `N` response, mirroring MAX_VALUE_LENGTH on the `V`
// path: a malicious or MITM'd discovery server must not be able to make
// the client buffer arbitrary memory from an unverified length prefix.
const MAX_NODE_COUNT = 1 << 16;
const MAX_NODE_FIELD_LENGTH = 64 * 1024;

// Aggregate cap on a whole `N ...` node-list response, independent of the
// per-field caps above: bounds a malicious discovery server's memory
// pressure while still fitting a full 65536-node registry. This same
// constant is being added to all six SDKs.
const MAX_NODE_LIST_RESPONSE_LENGTH = 16 * 1024 * 1024;

/** Reads from `socket` until `tryParse` returns non-null, resolving with
 * that value. One-shot: meant for a single request/response, not a
 * long-lived connection matching multiple in-flight requests. `maxBufferLength`,
 * when given, poisons the read if the accumulated buffer grows past it
 * without ever yielding a parseable frame — a backstop against a
 * malicious/misbehaving server that never sends a valid terminator
 * (issue #12 follow-up). */
function readFrame<T>(
  socket: Socket | TLSSocket,
  tryParse: (buf: Buffer) => T | null,
  deadlineMs: number,
  maxBufferLength?: number,
): Promise<T> {
  return new Promise((resolve, reject) => {
    let buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);

    // A server that accepts the connection but never answers (a
    // blackholed address behaves the same way) must not hang the caller.
    const timer = setTimeout(() => {
      cleanup();
      reject(new ConnectionLostError(`nanocached: no response from server within ${deadlineMs}ms`));
    }, deadlineMs);

    const cleanup = () => {
      clearTimeout(timer);
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
        return;
      }

      if (maxBufferLength !== undefined && buffer.length > maxBufferLength) {
        cleanup();
        reject(new NanocachedError("nanocached: discovery response exceeds maximum size (connection desynced)"));
      }
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onClose = () => {
      cleanup();
      reject(new NanocachedError("nanocached: connection closed before the expected response arrived"));
    };

    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

interface AuthIdentity {
  accepted: boolean;
  kind: "node" | "cluster";
  /** ADR-0019: the server echoed the tag capability (`OnT\n`/`OdT\n`). */
  tagged: boolean;
}

/** Parses the reply to `A`: `On\n`/`En\n` from a cache node, `Od\n`/`Ed\n`
 * from a discovery server (see doc/adr/0007-*.md), each stretched to four
 * bytes by a `T` before the LF when the server is echoing the tag
 * capability our extended `A` asked for (doc/adr/0019-*.md). The second
 * byte is what identifies which kind of server this is — it isn't an
 * accident of the auth outcome, it's present whether accepted or
 * rejected. */
function tryParseIdentity(buf: Buffer): AuthIdentity | null {
  if (buf.length < 3) return null;

  const status = buf[0];
  const type = buf[1];

  const accepted = status === 0x4f /* 'O' */;
  if (!accepted && status !== 0x45 /* 'E' */) {
    throw new NanocachedError("nanocached: unexpected response to A");
  }

  let tagged: boolean;
  if (buf[2] === 0x0a /* '\n' */) {
    tagged = false;
  } else if (buf[2] === 0x54 /* 'T' */) {
    if (buf.length < 4) return null;
    if (buf[3] !== 0x0a) {
      throw new NanocachedError("nanocached: unexpected response to A");
    }
    tagged = true;
  } else {
    throw new NanocachedError("nanocached: unexpected response to A");
  }

  if (type === 0x6e /* 'n' */) return { accepted, kind: "node", tagged };
  if (type === 0x64 /* 'd' */) return { accepted, kind: "cluster", tagged };
  throw new NanocachedError("nanocached: unexpected response to A");
}

/** Thrown when the server rejects the `A` handshake's secret — either no
 * `authSecret` was configured for a server that requires one, or the
 * configured secret is wrong. Never transient: retrying with the same
 * configuration cannot succeed. */
export class AuthenticationError extends NanocachedError {
  constructor(message: string) {
    super(message);
    this.name = "AuthenticationError";
  }
}

/** Thrown when a discovery server answers `L` with `B` — it is inside its
 * startup grace (ADR-0010), re-learning cluster membership after a
 * restart, and refuses to serve a possibly-partial node list. The caller
 * should try another address, or retry shortly. */
export class DiscoveryBusyError extends NanocachedError {
  constructor() {
    super("nanocached: the discovery server is warming up after a restart");
    this.name = "DiscoveryBusyError";
  }
}

/** Parses an `N <count>\n` header followed by `count` entries, each
 * `<name-length> <addr-length>\n<name><addr>\n` (doc/adr/0009-*.md) —
 * name and address are simply concatenated, split by their declared
 * lengths, not by a delimiter. Returns `null` while more bytes are still
 * needed. */
// Longest legal `N <count> <r>\n` header: marker + space + digits of
// MAX_NODE_COUNT + space + a generous digit allowance for the
// replication factor (uncapped on the wire) + LF.
const MAX_NODE_LIST_HEADER_LENGTH = 2 + String(MAX_NODE_COUNT).length + 1 + 20 + 1;

// Longest legal `<name-length> <addr-length>\n` entry header: two
// MAX_NODE_FIELD_LENGTH-digit fields, a space, and the LF.
const MAX_NODE_ENTRY_HEADER_LENGTH = 2 * String(MAX_NODE_FIELD_LENGTH).length + 1 + 1;

function tryParseNodeList(buf: Buffer): { nodes: DiscoveredNode[]; replication: number } | null {
  const headerEnd = buf.indexOf(0x0a);
  if (headerEnd === -1) {
    // Keep waiting only while the header could still turn out legal; a
    // malicious server withholding the LF forever must not be able to
    // buffer this unboundedly (issue #12 follow-up).
    if (buf.length > MAX_NODE_LIST_HEADER_LENGTH) {
      throw new NanocachedError("nanocached: invalid node-list header in discovery response (missing header terminator)");
    }
    return null;
  }

  if (buf[0] === 0x42 /* 'B' */) {
    throw new DiscoveryBusyError();
  }

  if (buf[0] !== 0x4e /* 'N' */) {
    throw new NanocachedError(`nanocached: unexpected response from discovery server: ${buf.subarray(0, headerEnd).toString("ascii")}`);
  }

  // `N <count> <r>\n` (ADR-0011) — the replication factor rides along.
  const header = buf.subarray(2, headerEnd).toString("ascii").split(" ");
  if (header.length !== 2) {
    throw new NanocachedError("nanocached: invalid node-list header in discovery response");
  }

  const count = Number(header[0]);
  if (!Number.isInteger(count) || count < 0 || count > MAX_NODE_COUNT) {
    throw new NanocachedError("nanocached: invalid node count in discovery response");
  }

  const replication = Number(header[1]);
  if (!Number.isInteger(replication) || replication < 1) {
    throw new NanocachedError("nanocached: invalid replication factor in discovery response");
  }

  const nodes: DiscoveredNode[] = [];
  let offset = headerEnd + 1;

  for (let i = 0; i < count; i++) {
    const entryHeaderEnd = buf.indexOf(0x0a, offset);
    if (entryHeaderEnd === -1) {
      if (buf.length - offset > MAX_NODE_ENTRY_HEADER_LENGTH) {
        throw new NanocachedError("nanocached: invalid node entry header in discovery response (missing header terminator)");
      }
      return null;
    }

    const lengths = buf.subarray(offset, entryHeaderEnd).toString("ascii").split(" ");
    if (lengths.length !== 2) {
      throw new NanocachedError("nanocached: invalid node entry header in discovery response");
    }

    const nameLength = Number(lengths[0]);
    const addrLength = Number(lengths[1]);
    if (
      !Number.isInteger(nameLength) ||
      nameLength < 0 ||
      nameLength > MAX_NODE_FIELD_LENGTH ||
      !Number.isInteger(addrLength) ||
      addrLength < 0 ||
      addrLength > MAX_NODE_FIELD_LENGTH
    ) {
      throw new NanocachedError("nanocached: invalid node entry lengths in discovery response");
    }

    const nameStart = entryHeaderEnd + 1;
    const addrStart = nameStart + nameLength;
    const addrEnd = addrStart + addrLength;
    const entryEnd = addrEnd + 1; // the trailing '\n' after the address

    if (buf.length < entryEnd) return null;
    if (buf[addrEnd] !== 0x0a) {
      throw new NanocachedError("nanocached: malformed node entry in discovery response");
    }

    nodes.push({
      name: buf.subarray(nameStart, addrStart).toString("utf8"),
      address: buf.subarray(addrStart, addrEnd).toString("utf8"),
    });
    offset = entryEnd;
  }

  return { nodes, replication };
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
  try {
    return await identifyOnce(options, true);
  } catch (error) {
    if (!isLegacyServerSignal(error)) throw error;
    // ADR-0019 transparent fallback: a pre-0019 server rejects the
    // extended `A ... T` as a parse error and closes without replying —
    // redial once with the plain form and run the connection untagged
    // (the pre-0019 behavior, desync window included).
    return identifyOnce(options, false);
  }
}

/** Whether an identify failure looks like a pre-0019 server slamming the
 * door on the extended `A` (close/reset before any reply) — the only
 * failures worth retrying with the plain form. A timeout is not one: the
 * server kept the connection open, it just didn't answer. */
function isLegacyServerSignal(error: unknown): boolean {
  if (error instanceof Error && error.message === "nanocached: connection closed before the expected response arrived") {
    return true;
  }
  const code = (error as NodeJS.ErrnoException)?.code;
  return code === "ECONNRESET" || code === "EPIPE";
}

async function identifyOnce(options: IdentifyOptions, requestTags: boolean): Promise<IdentifyResult> {
  const deadlineMs = options.connectDeadlineMs ?? CONNECT_DEADLINE_MS;
  const socket = await connectSocket(options);

  const secret = options.authSecret !== undefined ? Buffer.from(options.authSecret, "utf8") : NO_SECRET_PLACEHOLDER;
  const authFrame = Buffer.concat([Buffer.from(`A ${secret.length}${requestTags ? " T" : ""}\n`, "ascii"), secret]);

  let identity: AuthIdentity;
  try {
    socket.write(authFrame);
    identity = await readFrame(socket, tryParseIdentity, deadlineMs);
  } catch (error) {
    socket.destroy();
    throw error;
  }

  if (!identity.accepted) {
    socket.destroy();
    if (options.authSecret === undefined) {
      throw new AuthenticationError(
        `nanocached: ${options.host}:${options.port} requires authentication, but no authSecret was provided`,
      );
    }
    throw new AuthenticationError("nanocached: authentication failed");
  }

  if (identity.kind === "node") {
    return { kind: "node", socket, tagged: identity.tagged };
  }

  try {
    socket.write(Buffer.from("L\n"));
    const { nodes, replication } = await readFrame(
      socket,
      tryParseNodeList,
      deadlineMs,
      MAX_NODE_LIST_RESPONSE_LENGTH,
    );
    return { kind: "cluster", nodes, replication };
  } finally {
    socket.destroy();
  }
}
