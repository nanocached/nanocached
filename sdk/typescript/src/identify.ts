import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { ConnectionLostError } from "./connection.js";
import { NanocachedError } from "./errors.js";
import { parseStrictInteger } from "./protocol.js";
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

/** A node's hash-ring identity (a random per-process UUID) and its
 * network address (`host:port`) — two different things since
 * Node identity decoupled from address. `name` is what a hash ring must be built from, so
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
 * (see the server type in the auth response), which is what lets `NanocachedClient.connect()`
 * take the exact same options for either.
 */
export type IdentifyResult =
  // `tagged` (echoed response tags): the node accepted the extended `A ... T`, so
  // this socket's `G`/`S`/`D` traffic must carry tags and its responses
  // echo them; false means an older node answered the plain-`A` fallback.
  | { kind: "node"; socket: Socket | TLSSocket; tagged: boolean }
  // `replication` (client-side replication) is discovery's replication factor R — how
  // many nodes hold each key. It rides the `L` response so clients can't
  // skew from the cluster's setting.
  | { kind: "cluster"; nodes: DiscoveredNode[]; replication: number };

/**
 * Result of `connectAndListProxies` (SDK proxy mode, issue #122's
 * `viaProxy`): `Q`'s answer, fetched instead of `L`'s once a discovery
 * server is identified. `kind: "node"` means the configured address
 * turned out to be a cache node rather than a discovery server — proxy
 * mode needs discovery addresses, so `NanocachedClient`'s proxy-mode
 * connect/reconnect flow (`connectViaProxy`/`fetchProxyList` in
 * client.ts) treats this as a hard error (at bootstrap) or an unusable
 * candidate (on a later refresh) rather than something to open `G`/`S`/`D`
 * traffic on. No `replication` field — a proxy client needs no R (see the
 * module doc comment on `connectAndListProxies`).
 */
export type ProxyListResult = { kind: "node" } | { kind: "cluster"; proxies: DiscoveredNode[] };

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

/** A resumable parser for `readFrame`. `parse` is handed the bytes not yet
 * consumed and returns the result once the frame is complete, `null`
 * while it needs more. On a `null` return it reports how far it got:
 * `consumed` bytes at the front of `buf` are finished with (they are
 * dropped, never handed to it again, and the next `parse` call's buffer
 * starts right after them), and `needed` is the total length `buf` must
 * reach before another attempt can make progress (0 = unknown, retry on
 * any new byte). Both are relative to the `buf` just passed. */
interface FrameParser<T> {
  parse(buf: Buffer): T | null;
  consumed: number;
  needed: number;
}

/** Wraps a stateless `tryParse` (one that always re-reads from the front
 * of the whole buffer, as the `A` reply's does) as a `FrameParser`. */
function restartingParser<T>(tryParse: (buf: Buffer) => T | null): FrameParser<T> {
  return { parse: tryParse, consumed: 0, needed: 0 };
}

/** Reads from `socket` until `parser` returns non-null, resolving with
 * that value. One-shot: meant for a single request/response, not a
 * long-lived connection matching multiple in-flight requests. `maxBufferLength`,
 * when given, poisons the read if the total bytes received grow past it
 * without ever yielding a parseable frame — a backstop against a
 * malicious/misbehaving server that never sends a valid terminator
 * (issue #12 follow-up). */
function readFrame<T>(
  socket: Socket | TLSSocket,
  parser: FrameParser<T>,
  deadlineMs: number,
  maxBufferLength?: number,
): Promise<T> {
  return new Promise((resolve, reject) => {
    // Chunks are accumulated in an array and only concatenated when the
    // parser can make progress, instead of concatenating on every onData
    // call; and what the parser has already consumed (a long roster's
    // earlier entries) is dropped rather than carried — and re-copied,
    // re-parsed — by every later fragment. Together that keeps a large
    // discovery response O(n) in copies where concatenating the whole
    // accumulation per chunk was O(n^2). Mirrors connection.ts's fix for
    // value bodies.
    let chunks: Buffer[] = [];
    let chunksLength = 0;
    let needed = 0;
    // Bytes already dropped as consumed, so the size backstop still
    // counts the whole response.
    let discarded = 0;

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
      chunks.push(chunk);
      chunksLength += chunk.length;
      // Frame known to need more bytes than are buffered: nothing to
      // parse yet, and so nothing to copy. The size backstop below still
      // has to run, though.
      if (chunksLength < needed) {
        if (maxBufferLength !== undefined && discarded + chunksLength > maxBufferLength) {
          cleanup();
          reject(new NanocachedError("nanocached: discovery response exceeds maximum size (connection desynced)"));
        }
        return;
      }
      const buffer = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, chunksLength);

      let parsed: T | null;
      try {
        parsed = parser.parse(buffer);
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

      if (maxBufferLength !== undefined && discarded + buffer.length > maxBufferLength) {
        cleanup();
        reject(new NanocachedError("nanocached: discovery response exceeds maximum size (connection desynced)"));
        return;
      }

      // Collapse back to a single stored chunk (minus whatever the parser
      // has finished with) so later onData calls don't re-concat bytes
      // already merged here.
      const rest = parser.consumed > 0 ? buffer.subarray(parser.consumed) : buffer;
      discarded += parser.consumed;
      chunks = rest.length > 0 ? [rest] : [];
      chunksLength = rest.length;
      needed = Math.max(0, parser.needed - parser.consumed);
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
  /** echoed response tags: the server echoed the tag capability (`OnT\n`/`OdT\n`). */
  tagged: boolean;
}

/** Parses the reply to `A`: `On\n`/`En\n` from a cache node, `Od\n`/`Ed\n`
 * from a discovery server (see the server type in the auth response), each stretched to four
 * bytes by a `T` before the LF when the server is echoing the tag
 * capability our extended `A` asked for (echoed response tags). The second
 * byte is what identifies which kind of server this is — it isn't an
 * accident of the auth outcome, it's present whether accepted or
 * rejected. */
function tryParseIdentity(buf: Buffer): AuthIdentity | null {
  if (buf.length < 3) return null;

  const status = buf[0];
  const type = buf[1];

  const accepted = status === 0x4f /* 'O' */;
  if (status === 0x42 /* 'B' */) {
    // The node is at its connection limit and closes right after this
    // (see nanocached-node's reject_over_limit). Connection-classified
    // so the client's retry/cooldown layer treats it like any other
    // failed dial instead of surfacing a generic protocol error.
    throw new ConnectionLostError("nanocached: the server is at its connection limit (busy)");
  }
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
 * startup grace (discovery HA), re-learning cluster membership after a
 * restart, and refuses to serve a possibly-partial node list. The caller
 * should try another address, or retry shortly. */
export class DiscoveryBusyError extends NanocachedError {
  constructor() {
    super("nanocached: the discovery server is busy: warming up after a restart, or its replication factor disagrees with the cluster's");
    this.name = "DiscoveryBusyError";
  }
}

/** Parses an `N <count>\n` header followed by `count` entries, each
 * `<name-length> <addr-length>\n<name><addr>\n` (node identity decoupled from address) —
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

/** Parses one `<name-length> <addr-length>\n<name><addr>\n` entry at
 * `offset` (node identity decoupled from address) — the entry shape
 * shared, byte-for-byte, by `L`'s node list and `Q`'s proxy roster
 * (issue #122); the two responses differ only in their header
 * (`readNodeListHeader` vs `readProxyListHeader`), never in how an
 * individual entry is laid out. Returns the node and the offset just past
 * it, or `{ needed }` while more bytes are still needed — `needed` being
 * the total buffer length the entry will take (0 while its header line is
 * itself incomplete, so unknown). */
function parseEntry(buf: Buffer, offset: number): { node: DiscoveredNode; end: number } | { needed: number } {
  const entryHeaderEnd = buf.indexOf(0x0a, offset);
  if (entryHeaderEnd === -1) {
    if (buf.length - offset > MAX_NODE_ENTRY_HEADER_LENGTH) {
      throw new NanocachedError("nanocached: invalid entry header in discovery response (missing header terminator)");
    }
    return { needed: 0 };
  }

  const lengths = buf.subarray(offset, entryHeaderEnd).toString("ascii").split(" ");
  if (lengths.length !== 2) {
    throw new NanocachedError("nanocached: invalid entry header in discovery response");
  }

  // `parseStrictInteger` (digits only,
  // safe-integer range) rather than bare `Number()` + `isInteger`, which
  // accepted `+1`, ` 1`, `1e2` and precision-losing digit strings —
  // the same strictness protocol.ts already applies to every node frame.
  const nameLength = parseStrictInteger(lengths[0]);
  const addrLength = parseStrictInteger(lengths[1]);
  if (
    nameLength === undefined ||
    nameLength > MAX_NODE_FIELD_LENGTH ||
    addrLength === undefined ||
    addrLength > MAX_NODE_FIELD_LENGTH
  ) {
    throw new NanocachedError("nanocached: invalid entry lengths in discovery response");
  }

  const nameStart = entryHeaderEnd + 1;
  const addrStart = nameStart + nameLength;
  const addrEnd = addrStart + addrLength;
  const entryEnd = addrEnd + 1; // the trailing '\n' after the address

  if (buf.length < entryEnd) return { needed: entryEnd };
  if (buf[addrEnd] !== 0x0a) {
    throw new NanocachedError("nanocached: malformed entry in discovery response");
  }

  return {
    node: {
      name: buf.subarray(nameStart, addrStart).toString("utf8"),
      address: buf.subarray(addrStart, addrEnd).toString("utf8"),
    },
    end: entryEnd,
  };
}

/** What an `N` response's header line yields: how many entries follow,
 * where the header ends, and how to turn the parsed entries into the
 * caller's result. */
interface ListHeader<T> {
  count: number;
  headerEnd: number;
  finish: (nodes: DiscoveredNode[]) => T;
}

/** A resumable `N <header>\n` + `count` entries parser (`L`'s node list or
 * `Q`'s proxy roster, depending on `readHeader`). It parses each entry
 * exactly once and reports it consumed, so `readFrame` neither
 * re-concatenates nor re-parses the earlier part of a long roster on
 * every new fragment (a 65536-node roster arrives in hundreds of them). */
function entryListParser<T>(readHeader: (buf: Buffer) => ListHeader<T> | null): FrameParser<T> {
  let header: ListHeader<T> | undefined;
  const entries: DiscoveredNode[] = [];
  const parser: FrameParser<T> = {
    consumed: 0,
    needed: 0,
    parse(buf) {
      let offset = 0;
      if (header === undefined) {
        const read = readHeader(buf);
        if (read === null) {
          parser.consumed = 0;
          parser.needed = 0;
          return null;
        }
        header = read;
        offset = read.headerEnd + 1;
      }
      while (entries.length < header.count) {
        const entry = parseEntry(buf, offset);
        if ("needed" in entry) {
          parser.consumed = offset;
          parser.needed = entry.needed;
          return null;
        }
        entries.push(entry.node);
        offset = entry.end;
      }
      return header.finish(entries);
    },
  };
  return parser;
}

function readNodeListHeader(buf: Buffer): ListHeader<{ nodes: DiscoveredNode[]; replication: number }> | null {
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

  // `N <count> <r>\n` (client-side replication) — the replication factor rides along.
  const header = buf.subarray(2, headerEnd).toString("ascii").split(" ");
  if (header.length !== 2) {
    throw new NanocachedError("nanocached: invalid node-list header in discovery response");
  }

  const count = parseStrictInteger(header[0]);
  if (count === undefined || count > MAX_NODE_COUNT) {
    throw new NanocachedError("nanocached: invalid node count in discovery response");
  }

  const replication = parseStrictInteger(header[1]);
  if (replication === undefined || replication < 1) {
    throw new NanocachedError("nanocached: invalid replication factor in discovery response");
  }

  return { count, headerEnd, finish: (nodes) => ({ nodes, replication }) };
}

/** Parses `Q`'s `N <count>\n` response (issue #122) — `L`'s node-list
 * header minus the trailing replication field (a proxy client needs no
 * R), followed by the exact same per-entry shape `parseEntry` already
 * knows. Entry/count caps mirror `readNodeListHeader`'s (MAX_NODE_COUNT,
 * MAX_NODE_FIELD_LENGTH) rather than inventing separate ones, per the
 * SDK-port spec — `Q`'s roster is bounded by the same discovery-server
 * trust model `L`'s is. */
function readProxyListHeader(buf: Buffer): ListHeader<DiscoveredNode[]> | null {
  const headerEnd = buf.indexOf(0x0a);
  if (headerEnd === -1) {
    // `N <count>\n` can only ever be shorter than `N <count> <r>\n`, so
    // MAX_NODE_LIST_HEADER_LENGTH is a safe (if slightly generous) bound
    // here too — see the same reasoning on readNodeListHeader above.
    if (buf.length > MAX_NODE_LIST_HEADER_LENGTH) {
      throw new NanocachedError("nanocached: invalid proxy-list header in discovery response (missing header terminator)");
    }
    return null;
  }

  if (buf[0] === 0x42 /* 'B' */) {
    // Same startup-grace refusal as `L` — see DiscoveryBusyError.
    throw new DiscoveryBusyError();
  }

  if (buf[0] !== 0x4e /* 'N' */) {
    throw new NanocachedError(`nanocached: unexpected response from discovery server: ${buf.subarray(0, headerEnd).toString("ascii")}`);
  }

  const count = parseStrictInteger(buf.subarray(2, headerEnd).toString("ascii"));
  if (count === undefined || count > MAX_NODE_COUNT) {
    throw new NanocachedError("nanocached: invalid proxy count in discovery response");
  }

  return { count, headerEnd, finish: (proxies) => proxies };
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
    // Retryable-error status (issue #125): probe with the fullest
    // extended form first — `A <len> T R`. This adds one stage in front
    // of the existing tag fallback below; it doesn't build a new
    // mechanism.
    return await identifyOnce(options, true, true);
  } catch (error) {
    if (!isLegacyServerSignal(error)) throw error;
    try {
      // A server that understands `T` but predates `R` treats the
      // extended `A ... T R` as a parse error and closes without
      // replying — redial once with just `T`.
      return await identifyOnce(options, true, false);
    } catch (innerError) {
      if (!isLegacyServerSignal(innerError)) throw innerError;
      // Echoed response tags transparent fallback: a pre-0019 server rejects the
      // extended `A ... T` as a parse error and closes without replying —
      // redial once with the plain form and run the connection untagged
      // (the pre-0019 behavior, desync window included — see
      // docs/protocol.html#untagged-desync).
      return identifyOnce(options, false, false);
    }
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

// One shared per-attempt budget across the dial and every handshake read
// phase that follows it — matching Rust (a single `tokio::time::timeout`
// around the whole attempt, identify.rs) and Python (a single
// `asyncio.wait_for` around the whole attempt, _identify.py) — rather
// than each phase getting its own fresh `deadlineMs`. `connectSocket`
// already spends up to `deadlineMs` on the dial itself (its own internal
// timer, left as-is: the dial is phase one of the shared budget), so
// every read phase after it must draw from what's left of that same
// budget (issue #47 audit item 3: two independent full-length timers
// could add up to ~2x the intended per-attempt deadline).
export function remainingDeadline(budgetMs: number, startedAt: number): number {
  return Math.max(0, budgetMs - (Date.now() - startedAt));
}

interface Authenticated {
  socket: Socket | TLSSocket;
  tagged: boolean;
  kind: "node" | "cluster";
}

/** The shared `A` handshake — dial, authenticate, read back which kind of
 * server this is — behind both `identifyOnce` (which follows a
 * discovery-kind result with `L`) and `listProxiesOnce` (`Q`, issue
 * #122): identical up through authentication, they only diverge in which
 * command a discovery-kind result sends next. The socket is handed back
 * live either way and closing it is left to the caller, exactly as
 * `identifyOnce` always did before this was factored out of it. */
async function authenticate(
  options: IdentifyOptions,
  requestTags: boolean,
  requestRetryable: boolean,
  deadlineMs: number,
  startedAt: number,
): Promise<Authenticated> {
  const socket = await connectSocket(options);

  const secret = options.authSecret !== undefined ? Buffer.from(options.authSecret, "utf8") : NO_SECRET_PLACEHOLDER;
  // Retryable-error status (issue #125): the capability token order on
  // the wire is fixed as `[T] [R]` — `R` never rides without `T`, since
  // the probe only ever asks for it alongside tags (see
  // connectAndIdentify's three-stage fallback below).
  const authFrame = Buffer.concat([
    Buffer.from(`A ${secret.length}${requestTags ? " T" : ""}${requestRetryable ? " R" : ""}\n`, "ascii"),
    secret,
  ]);

  let identity: AuthIdentity;
  try {
    socket.write(authFrame);
    identity = await readFrame(socket, restartingParser(tryParseIdentity), remainingDeadline(deadlineMs, startedAt));
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

  return { socket, tagged: identity.tagged, kind: identity.kind };
}

async function identifyOnce(options: IdentifyOptions, requestTags: boolean, requestRetryable: boolean): Promise<IdentifyResult> {
  const deadlineMs = options.connectDeadlineMs ?? CONNECT_DEADLINE_MS;
  const startedAt = Date.now();
  const identified = await authenticate(options, requestTags, requestRetryable, deadlineMs, startedAt);

  if (identified.kind === "node") {
    return { kind: "node", socket: identified.socket, tagged: identified.tagged };
  }

  try {
    identified.socket.write(Buffer.from("L\n"));
    const { nodes, replication } = await readFrame(
      identified.socket,
      entryListParser(readNodeListHeader),
      remainingDeadline(deadlineMs, startedAt),
      MAX_NODE_LIST_RESPONSE_LENGTH,
    );
    return { kind: "cluster", nodes, replication };
  } finally {
    identified.socket.destroy();
  }
}

/**
 * SDK proxy mode (issue #122, `viaProxy`): the `Q` counterpart to
 * `connectAndIdentify`'s `L` fetch — authenticates exactly the same way,
 * then asks a discovery server for its registered *proxy* roster instead
 * of its node list. A proxy looks exactly like a single node that owns
 * every key (full `G`/`S`/`D`, never `W`), so once one is chosen from
 * this roster, `NanocachedClient` opens it with the ordinary
 * `connectAndIdentify` — this function's job is only ever to learn which
 * addresses to try, mirroring `L`'s role for the plain cluster path.
 * `kind: "node"` (the configured address is a cache node, not a discovery
 * server) closes the socket before returning — there is never anything
 * for the caller to reuse: proxy mode needs discovery addresses.
 * Used only by `NanocachedClient`'s proxy-mode connect/reconnect flow
 * (`connectViaProxy`/`fetchProxyList` in client.ts).
 */
export async function connectAndListProxies(options: IdentifyOptions): Promise<ProxyListResult> {
  try {
    // Retryable-error status (issue #125) — see connectAndIdentify's own
    // doc comment on this same three-stage probe.
    return await listProxiesOnce(options, true, true);
  } catch (error) {
    if (!isLegacyServerSignal(error)) throw error;
    try {
      return await listProxiesOnce(options, true, false);
    } catch (innerError) {
      if (!isLegacyServerSignal(innerError)) throw innerError;
      // Echoed response tags transparent fallback — see connectAndIdentify's
      // own doc comment on this same retry.
      return listProxiesOnce(options, false, false);
    }
  }
}

async function listProxiesOnce(options: IdentifyOptions, requestTags: boolean, requestRetryable: boolean): Promise<ProxyListResult> {
  const deadlineMs = options.connectDeadlineMs ?? CONNECT_DEADLINE_MS;
  const startedAt = Date.now();
  const identified = await authenticate(options, requestTags, requestRetryable, deadlineMs, startedAt);

  if (identified.kind === "node") {
    identified.socket.destroy();
    return { kind: "node" };
  }

  try {
    identified.socket.write(Buffer.from("Q\n"));
    const proxies = await readFrame(
      identified.socket,
      entryListParser(readProxyListHeader),
      remainingDeadline(deadlineMs, startedAt),
      // Same aggregate cap as `L` (issue #122) — see readProxyListHeader's
      // doc comment on why the count/field caps are shared too.
      MAX_NODE_LIST_RESPONSE_LENGTH,
    );
    return { kind: "cluster", proxies };
  } finally {
    identified.socket.destroy();
  }
}
