/**
 * In-process stand-ins for nanocached-node and nanocached-discovery,
 * speaking just enough of the wire protocol (`A`, `G`/`S`/`D` and their
 * namespaced `g`/`s`/`d` counterparts (issue #105), `L`, `i` (INCR/DECR,
 * issue #129), `k`/`x` (compare-and-set/-delete, issue #141)) for the
 * client tests to exercise NanocachedClient end-to-end over real TCP
 * sockets without the Rust binaries.
 */

import { createServer, type Server, type Socket } from "node:net";
import { createHash } from "node:crypto";

// Compare-and-set (issue #141): the same digest this mock uses to evaluate
// a `k`/`x` `<cond>` — SHA-256 of the value's exact bytes, truncated to the
// first 16 bytes, lowercase hex — independently implemented here (not
// imported from src/protocol.ts's own `contentDigest`) so a bug in that
// implementation can't also hide identically in the test double that's
// supposed to catch it.
function digestOf(value: Buffer): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 32);
}

interface MockServerBase {
  port: number;
  address: string;
  close(): Promise<void>;
}

export interface MockNode extends MockServerBase {
  store: Map<string, Buffer>;
  /** The default-namespace store, keyed by namespace (issue #105) —
   * `store` above is exactly `namespacedStore("")`, exposed separately
   * only because it predates namespaces and plenty of tests already
   * address it directly. A `namespace` never seen on the wire gets a
   * fresh, empty map the first time it's asked for (matching the real
   * server: an unknown namespace simply has no entries yet). */
  namespacedStore(namespace: string | Uint8Array): Map<string, Buffer>;
  /** Queue a one-off `W` reply for the next G/S/D request. */
  answerWrongNodeOnce(): void;
  /** Queue a one-off `W` reply for the next S specifically (not G/D) —
   * for tests that need a node to keep answering GET normally while a
   * later SET against it (e.g. a read-repair write-back) fails. Mirrors
   * the .NET mock's hook of the same name. */
  answerWrongNodeOnSetOnce(): void;
  /** Queue a one-off reply for the next G request on a tagged connection
   * that echoes the WRONG tag (the request's tag + 1) — the desync a
   * pre-tag stream misalignment would produce. */
  answerWrongTagOnce(): void;
  /** Swallow the next G request entirely (no reply) — the off-by-one
   * stream desync where every later response answers the previous
   * request. */
  swallowGetOnce(): void;
  /** Queue a one-off garbage `V` header for the next G request. */
  answerMalformedValueOnce(): void;
  /** Queue a one-off `V` reply for the next G request whose header is
   * never terminated by an LF — streams chunks of non-newline bytes
   * (until the socket is destroyed, or a large safety cap is hit)
   * instead, simulating a malicious/corrupted server withholding the
   * terminator forever. */
  answerUnterminatedValueOnce(): void;
  /** Total bytes written so far by the unterminated-value stream queued
   * with answerUnterminatedValueOnce — lets a test assert the client
   * detected and closed the connection without waiting for much data. */
  unterminatedValueBytesSent(): number;
  /** Queue a one-off `S` reply for the next G request — a well-formed
   * frame of the wrong kind, as a desynced (off-by-one) stream would
   * produce. */
  answerStoredToGetOnce(): void;
  /** How many connections this server has ever accepted. */
  connectionCount(): number;
  /** How many `G` requests this server has ever received. */
  getCount(): number;
  /** How many `i` (INCR, issue #129) requests this server has ever
   * received — the critical assertion for cluster-replication tests: a
   * replica must receive a `set`/`s` carrying the primary's literal
   * result, and must NEVER receive an `i` frame at all. */
  incrCount(): number;
  /** How many `k` (compare-and-set, issue #141) requests this server has
   * ever received — the critical assertion for cluster-replication tests:
   * a replica must receive a `set`/`s` carrying the primary's literal
   * result, and must NEVER receive a `k` frame at all. */
  casCount(): number;
  /** How many `x` (compare-and-delete, issue #141) requests this server
   * has ever received — same critical-assertion role as `casCount` above,
   * for the delete side: a replica must receive a plain `delete`/`d`, and
   * must NEVER receive an `x` frame. */
  casDeleteCount(): number;
  /** How many `c`/`F` (clear/flush, issue #106) requests this server has
   * ever received — lets a test assert a clear fanned out to every node,
   * even one holding no keys in the cleared namespace. */
  clearCount(): number;
  /** How many `m` (batched get, issues #128/#150/#151) requests this
   * server has ever received. */
  multiGetCount(): number;
  /** How many `o` (batched set, issues #150/#151) requests this server
   * has ever received. */
  multiSetCount(): number;
  /** Each `m` frame's wire body size received so far — namespace length
   * plus the sum of every key length in that one frame — in receipt
   * order. Lets a test assert the SDK's byte-bound batch chunking
   * (issue #222) actually kept every sub-frame under the cap, not just
   * that it split into more than one. */
  multiGetFrameBytes(): number[];
  /** `multiGetFrameBytes`'s write-side twin: each `o` frame's wire body
   * size (namespace length plus every key's and every value's length in
   * that frame) received so far, in receipt order. */
  multiSetFrameBytes(): number[];
  /** Makes every `m`/`o` roster containing `key` answer just that key
   * `W`, for the next `times` such requests (consumed one per match,
   * across as many separate `m`/`o` calls as it takes) — the batched
   * analogue of `answerWrongNodeOnce`, which answers a whole G/S `W`
   * instead of naming a single key inside a roster. */
  answerMultiWrongNodeTimes(key: string, times: number): void;
  /** Retryable-error status (issue #125): queue `n` one-off `R` replies
   * (tagged correctly on a tagged connection) for the next `n` data
   * requests — any of `G`/`S`/`D`/`g`/`s`/`d`/`c`/`F`, in whatever order
   * they arrive. */
  answerRetryableTimes(n: number): void;
  /** The raw `A ...` header line this server most recently received (no
   * trailing LF) — lets a test assert the exact probe form a connect
   * sent, e.g. `"A 1 T R"`. */
  lastAuthHeader(): string;
  /** Queue a one-off failure for the next `c`/`F` request: instead of
   * acking with `C`, the connection is destroyed — the "some node
   * failed" half of the clear fan-out's refresh-once-and-retry path
   * (issue #106), the same connection-level failure shape
   * `answerWrongNodeOnce` et al. simulate for get/set/delete via a
   * different mechanism (an explicit `W`) rather than a real drop, since
   * a clear is never key-addressed and so never gets a `W` at all. */
  failClearOnce(): void;
  /** The raw command letter (`"G"`/`"S"`/`"D"`/`"g"`/`"s"`/`"d"`) of the
   * most recent cache-op request this server received — lets a test
   * assert the SDK rule that the default namespace sends the legacy
   * uppercase frame, byte-for-byte, never the lowercase `g`/`s`/`d` form
   * (first-class namespaces, issue #105). */
  lastCommand(): string;
  /** The TTL (whole seconds; 0 if omitted on the wire) from the most
   * recent `S` request this server received. */
  lastSetTtl(): number;
  /** Server-side close of every currently open connection (a FIN, like
   * nanocached-node's own idle timeout), leaving the server listening. */
  dropConnections(): void;
  /** Makes every future `S` reply wait `ms` first — for tests proving a
   * caller isn't blocked on a slow replica leg (fire-and-forget replica writes). */
  delaySets(ms: number): void;
  /** Makes every future `G` reply wait `ms` first — a slow-but-alive node,
   * for hedged-read tests (issue #64). */
  delayGets(ms: number): void;
  /** Makes this node a half-open server from this point on: it still
   * accepts and completes the `A` handshake, and still reads every
   * request frame off the wire (so the TCP stream stays well-formed),
   * but never writes a reply — regression coverage for the request
   * timeout (issue #42), mirroring the Go suite's hook of the same
   * name. */
  goSilentAfterHandshake(): void;
}

export interface MockDiscovery extends MockServerBase {
  setNodes(nodes: Array<{ name: string; address: string }>): void;
  /** While true, `L` and `Q` both answer `B\n` and close — the discovery
   * HA startup grace of a freshly restarted discovery server (shared by
   * both commands on the real server — see nanocached-discovery.rs's
   * `ListProxies` handler). */
  setWarmingUp(warming: boolean): void;
  /** Queue a one-off `N` reply for the next `L` request whose header is
   * never terminated by an LF — streams chunks of non-newline bytes
   * (until the socket is destroyed, or a large safety cap is hit)
   * instead, simulating a malicious/corrupted discovery server. */
  answerUnterminatedListOnce(): void;
  /** Total bytes written so far by the unterminated-list stream queued
   * with answerUnterminatedListOnce. */
  unterminatedListBytesSent(): number;
  /** SDK proxy mode (issue #122): sets the roster `Q` serves — entirely
   * separate from `setNodes`' node list, mirroring the real server's
   * separate `proxies`/`nodes` registries. Callable mid-test to simulate
   * a roster change (e.g. a proxy dying and being swept). */
  setProxies(proxies: Array<{ name: string; address: string }>): void;
  /** How many `L` requests this server has received — lets a test assert
   * proxy mode never touches the node list at all (issue #122). */
  listCount(): number;
  /** How many `Q` requests this server has received (issue #122). */
  listProxiesCount(): number;
}

/** A port with nothing listening on it — bound once to reserve a real
 * ephemeral port, then released. */
export async function unusedPort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return port;
}

function listen(server: Server, port = 0): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("mock server bound to a non-TCP address"));
        return;
      }
      resolve(address.port);
    });
  });
}

/** Encodes the `<name-length> <addr-length>\n<name><addr>\n` entries
 * shared, byte-for-byte, by `L`'s node list and `Q`'s proxy roster (issue
 * #122) — the two responses differ only in their header. */
function encodeEntries(entries: Array<{ name: string; address: string }>): Buffer[] {
  return entries.map(({ name, address }) => {
    const nameBytes = Buffer.from(name, "utf8");
    const addrBytes = Buffer.from(address, "utf8");
    return Buffer.concat([Buffer.from(`${nameBytes.length} ${addrBytes.length}\n`), nameBytes, addrBytes, Buffer.from("\n")]);
  });
}

function trackAndClose(server: Server): { sockets: Set<Socket>; close: () => Promise<void> } {
  const sockets = new Set<Socket>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    // An abrupt client-side destroy (e.g. close() racing an in-flight
    // keep-alive ping) surfaces as ECONNRESET here; without a listener
    // the 'error' event would crash the test process as an uncaught
    // exception — a pure test-infra flake.
    socket.on("error", () => {});
    socket.on("close", () => sockets.delete(socket));
  });

  return {
    sockets,
    close: () =>
      new Promise<void>((resolve, reject) => {
        for (const socket of sockets) socket.destroy();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

export async function startMockNode(
  options: {
    requiredSecret?: string;
    /** Speak echoed response tags: acknowledge `A ... T` with `OnT\n` and echo tags
     * on that connection's replies. Off by default so the bulk of the
     * suite keeps exercising the legacy untagged path. */
    supportTags?: boolean;
    /** Behave like a legacy pre-tag server: an extended `A ... T` is a
     * parse error — close the connection without replying. */
    closeOnExtendedAuth?: boolean;
    /** Retryable-error status (issue #125): behave like a server that
     * understands `T` but predates `R` — an `A ... T R` is a parse error,
     * closing the connection without replying, while a plain `A ... T`
     * (or bare `A`) still works. Exercises the middle stage of the
     * SDK's three-stage connect probe (`A <len> T R` → `A <len> T` →
     * `A <len>`). */
    closeOnRetryableAuth?: boolean;
    /** Pin the listener to this port instead of an ephemeral one — for a
     * node that comes back on the address discovery already advertised
     * (issue #67 tests). */
    port?: number;
  } = {},
): Promise<MockNode> {
  const store = new Map<string, Buffer>();
  // Namespaces (issue #105): one sub-map per non-empty namespace, keyed
  // by its raw bytes (base64, so an arbitrary/binary namespace is a safe
  // Map key) — mirroring src/key.rs's doc comment on the server side
  // ("one sub-map per namespace, with legacy traffic in the \"\" one").
  // The default namespace's entries live in `store` itself, not a
  // namespaceStores entry, so `store` keeps meaning exactly what it
  // always did.
  const namespaceStores = new Map<string, Map<string, Buffer>>();
  function storeFor(namespace: Buffer): Map<string, Buffer> {
    if (namespace.length === 0) return store;
    const key = namespace.toString("base64");
    let existing = namespaceStores.get(key);
    if (existing === undefined) {
      existing = new Map();
      namespaceStores.set(key, existing);
    }
    return existing;
  }
  // INCR (issue #129): the TTL a key was last `S`/`s`'d (or successfully
  // `i`'d) with, so a later `i` response can carry a real "remaining TTL"
  // field — needed for the SDK's client-side replication driver, which
  // forwards INCR's literal result to replicas as a `set` carrying this
  // same TTL (never replays `i` there). Mirrors `store`/`namespaceStores`'
  // own default-vs-named-namespace split; absence (or 0) means no TTL.
  const ttls = new Map<string, number>();
  const namespaceTtls = new Map<string, Map<string, number>>();
  function ttlsFor(namespace: Buffer): Map<string, number> {
    if (namespace.length === 0) return ttls;
    const key = namespace.toString("base64");
    let existing = namespaceTtls.get(key);
    if (existing === undefined) {
      existing = new Map();
      namespaceTtls.set(key, existing);
    }
    return existing;
  }
  let wrongNodeReplies = 0;
  let wrongNodeOnSetReplies = 0;
  let wrongTagReplies = 0;
  let swallowedGets = 0;
  let malformedValueReplies = 0;
  let unterminatedValueReplies = 0;
  let unterminatedBytesSent = 0;
  let storedToGetReplies = 0;
  let connections = 0;
  let gets = 0;
  let incrs = 0;
  let casRequests = 0;
  let casDeleteRequests = 0;
  let clears = 0;
  let failClearReplies = 0;
  let setDelayMs = 0;
  let getDelayMs = 0;
  let lastSetTtl = 0;
  let lastCommand = "";
  let silent = false;
  // Retryable-error status (issue #125).
  let retryableReplies = 0;
  let lastAuthHeader = "";
  // Batched get/set (issues #128/#150/#151).
  let multiGets = 0;
  let multiSets = 0;
  // Per-frame wire body sizes (issue #222), in receipt order — see
  // multiGetFrameBytes/multiSetFrameBytes above.
  const multiGetFrameBytes: number[] = [];
  const multiSetFrameBytes: number[] = [];
  // multiWrongNodeKey/multiWrongNodeLeft: when multiWrongNodeKey is set,
  // every `m`/`o` roster containing that exact key answers just that key
  // `W`, for as long as multiWrongNodeLeft has budget left (consumed one
  // per match) — the batched analogue of wrongNodeReplies, which answers
  // a whole G/S `W` instead of naming a single key inside a batch.
  let multiWrongNodeKey: string | undefined;
  let multiWrongNodeLeft = 0;

  const server = createServer((socket) => {
    connections++;
    let buffer = Buffer.alloc(0);
    // Echoed response tags: set when this connection's `A ... T` was acknowledged —
    // its requests then carry a trailing tag the replies must echo.
    let tagged = false;

    socket.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);

      for (;;) {
        const lf = buffer.indexOf(0x0a);
        if (lf === -1) return;

        const parts = buffer.subarray(0, lf).toString("ascii").split(" ");
        const bodyStart = lf + 1;
        // On a tagged connection every request's last header field is its
        // tag, echoed back as each reply's own last field.
        const tag = tagged ? ` ${parts[parts.length - 1]}` : "";

        switch (parts[0]) {
          case "A": {
            lastAuthHeader = buffer.subarray(0, lf).toString("ascii");

            if (parts.length > 2 && options.closeOnExtendedAuth) {
              socket.destroy();
              return;
            }
            // Retryable-error status (issue #125): a server that
            // understands `T` but predates `R` slams the door on the
            // fuller `A <len> T R` form — parts beyond `A <len> T`.
            if (parts.length > 3 && options.closeOnRetryableAuth) {
              socket.destroy();
              return;
            }

            const secretLength = Number(parts[1]);
            if (buffer.length < bodyStart + secretLength) return;
            const secret = buffer.subarray(bodyStart, bodyStart + secretLength);
            buffer = buffer.subarray(bodyStart + secretLength);

            const accepted =
              options.requiredSecret === undefined
                ? secret.length > 0
                : secret.equals(Buffer.from(options.requiredSecret, "utf8"));
            tagged = accepted && options.supportTags === true && parts[2] === "T";
            socket.write(accepted ? (tagged ? "OnT\n" : "On\n") : "En\n");
            if (!accepted) socket.end();
            break;
          }

          case "G":
          case "g": {
            lastCommand = parts[0];
            // Namespaced variants (issue #105): `g` carries one extra
            // leading `<namespace-length>` header field, and the
            // namespace bytes lead the body — see encodeGet.
            const namespaced = parts[0] === "g";
            const namespaceLength = namespaced ? Number(parts[1]) : 0;
            const keyLength = Number(parts[namespaced ? 2 : 1]);
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);
            gets++;

            if (silent) break;

            const targetStore = storeFor(namespace);

            // Factored out so delayGets (hedged reads, issue #64) can hold
            // the whole reply — including every special-cased one-off
            // reply below, same as the Python mock's delay_gets — instead
            // of only the plain store lookup.
            const sendGetReply = () => {
              // Retryable-error status (issue #125): checked first, ahead
              // of every other one-off reply below.
              if (retryableReplies > 0) {
                retryableReplies--;
                socket.write(`R${tag}\n`);
                return;
              }

              if (swallowedGets > 0) {
                swallowedGets--;
                return;
              }

              if (wrongTagReplies > 0 && tagged) {
                wrongTagReplies--;
                // The tag is always the header's last field on a tagged
                // connection, namespaced or not.
                socket.write(`N ${Number(parts[parts.length - 1]) + 1}\n`);
                return;
              }

              if (malformedValueReplies > 0) {
                malformedValueReplies--;
                socket.write("V x\n");
                return;
              }

              if (unterminatedValueReplies > 0) {
                unterminatedValueReplies--;
                socket.write("V");
                // Stream non-newline bytes so the header never terminates,
                // simulating a malicious/corrupted server withholding the
                // LF forever. A well-behaved client must detect and close
                // the connection long before this safety cap (a few
                // hundred KB) is reached; the interval also stops itself
                // once the socket is gone.
                const interval = setInterval(() => {
                  if (socket.destroyed || unterminatedBytesSent > 512 * 1024) {
                    clearInterval(interval);
                    return;
                  }
                  const filler = Buffer.alloc(1024, 0x39 /* '9' */);
                  unterminatedBytesSent += filler.length;
                  socket.write(filler);
                }, 1);
                return;
              }

              if (storedToGetReplies > 0) {
                storedToGetReplies--;
                socket.write(`S${tag}\n`);
                return;
              }

              if (wrongNodeReplies > 0) {
                wrongNodeReplies--;
                socket.write(`W${tag}\n`);
                return;
              }

              const value = targetStore.get(key);
              if (value === undefined) {
                socket.write(`N${tag}\n`);
              } else {
                socket.write(Buffer.concat([Buffer.from(`V ${value.length}${tag}\n`), value]));
              }
            };

            if (getDelayMs > 0) {
              setTimeout(sendGetReply, getDelayMs);
            } else {
              sendGetReply();
            }
            break;
          }

          case "S":
          case "s": {
            lastCommand = parts[0];
            // Namespaced variant (issue #105): `s` carries one extra
            // leading `<namespace-length>` header field ahead of the key
            // and value lengths, and the namespace bytes lead the body —
            // see encodeSet. `offset` shifts every following field index
            // by one when present.
            const namespaced = parts[0] === "s";
            const offset = namespaced ? 1 : 0;
            const namespaceLength = namespaced ? Number(parts[1]) : 0;
            const keyLength = Number(parts[1 + offset]);
            const valueLength = Number(parts[2 + offset]);
            if (buffer.length < bodyStart + namespaceLength + keyLength + valueLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            const value = Buffer.from(
              buffer.subarray(bodyStart + namespaceLength + keyLength, bodyStart + namespaceLength + keyLength + valueLength),
            );
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength + valueLength);
            // The TTL, when present, is the field after the key/value
            // lengths (omitted on the wire means "no expiry", i.e. 0 —
            // see encodeSet's doc comment); on a tagged connection the
            // tag sits after it as the last field.
            const ttlFieldCount = parts.length - (tagged ? 4 + offset : 3 + offset);
            lastSetTtl = ttlFieldCount > 0 ? Number(parts[3 + offset]) : 0;

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (wrongNodeOnSetReplies > 0) {
              wrongNodeOnSetReplies--;
              socket.write(`W${tag}\n`);
              break;
            }

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write(`W${tag}\n`);
              break;
            }

            storeFor(namespace).set(key, value);
            // INCR (issue #129): remember this key's TTL so a later `i`
            // response can report a real "remaining TTL" — see ttlsFor.
            if (lastSetTtl > 0) ttlsFor(namespace).set(key, lastSetTtl);
            else ttlsFor(namespace).delete(key);
            if (setDelayMs > 0) {
              setTimeout(() => socket.write(`S${tag}\n`), setDelayMs);
            } else {
              socket.write(`S${tag}\n`);
            }
            break;
          }

          // Batched get (issues #128/#150/#151): `m <ns-len> <n>
          // <key-len-1>...<key-len-n>[ <tag>]\n<ns><key-1>...<key-n>` —
          // always namespaced, no legacy uppercase form (see
          // encodeMultiGet). Answers `M <n> <result-1>...<result-n>[
          // <tag>]\n<hit values, concatenated in request order>`
          // (docs/protocol.html#multi): a decimal byte length for a
          // hit, "-" for a clean miss, "W" for a per-key wrong-node
          // (multiWrongNodeKey/multiWrongNodeLeft above).
          case "m": {
            lastCommand = parts[0];
            const namespaceLength = Number(parts[1]);
            const count = Number(parts[2]);
            const keyLengths = parts.slice(3, 3 + count).map(Number);
            const totalKeyLength = keyLengths.reduce((sum, length) => sum + length, 0);
            if (buffer.length < bodyStart + namespaceLength + totalKeyLength) return;

            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const keys: string[] = new Array(count);
            let cursor = bodyStart + namespaceLength;
            for (let i = 0; i < count; i++) {
              keys[i] = buffer.subarray(cursor, cursor + keyLengths[i]).toString("utf8");
              cursor += keyLengths[i];
            }
            buffer = buffer.subarray(cursor);
            multiGets++;
            multiGetFrameBytes.push(namespaceLength + totalKeyLength);

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            const targetStore = storeFor(namespace);
            let header = `M ${count}`;
            const hits: Buffer[] = [];
            for (const key of keys) {
              if (multiWrongNodeKey === key && multiWrongNodeLeft > 0) {
                multiWrongNodeLeft--;
                header += " W";
                continue;
              }
              const value = targetStore.get(key);
              if (value === undefined) {
                header += " -";
              } else {
                header += ` ${value.length}`;
                hits.push(value);
              }
            }
            socket.write(Buffer.concat([Buffer.from(`${header}${tag}\n`), ...hits]));
            break;
          }

          // Batched set (issues #150/#151): `o <ns-len> <n> <key-len-1>
          // <value-len-1>...<key-len-n> <value-len-n> [<ttl>][ <tag>]\n
          // <ns><key-1><value-1>...<key-n><value-n>` — one shared TTL
          // for the whole batch, not per key (see encodeMultiSet).
          // Answers `O <n> <result-1>...<result-n>[ <tag>]\n` (no
          // body): "S" (stored) or "W" (per-key wrong-node, same
          // multiWrongNodeKey knob `m` uses).
          case "o": {
            lastCommand = parts[0];
            const namespaceLength = Number(parts[1]);
            const count = Number(parts[2]);
            const lengthFields: number[] = [];
            for (let i = 0; i < count; i++) {
              lengthFields.push(Number(parts[3 + 2 * i]), Number(parts[3 + 2 * i + 1]));
            }
            const totalBodyLength = lengthFields.reduce((sum, length) => sum + length, 0);
            if (buffer.length < bodyStart + namespaceLength + totalBodyLength) return;

            // The ttl field, when present, always sits immediately after
            // the last length field, regardless of whether the
            // connection is tagged — same convention `s`'s own ttlField
            // above relies on.
            const lengthFieldCount = 3 + 2 * count;
            const ttlFieldCount = parts.length - (tagged ? lengthFieldCount + 1 : lengthFieldCount);
            const ttl = ttlFieldCount > 0 ? Number(parts[lengthFieldCount]) : 0;

            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const keys: string[] = new Array(count);
            const values: Buffer[] = new Array(count);
            let cursor = bodyStart + namespaceLength;
            for (let i = 0; i < count; i++) {
              const keyLength = lengthFields[2 * i];
              const valueLength = lengthFields[2 * i + 1];
              keys[i] = buffer.subarray(cursor, cursor + keyLength).toString("utf8");
              cursor += keyLength;
              values[i] = Buffer.from(buffer.subarray(cursor, cursor + valueLength));
              cursor += valueLength;
            }
            buffer = buffer.subarray(cursor);
            multiSets++;
            multiSetFrameBytes.push(namespaceLength + totalBodyLength);

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            const targetStore = storeFor(namespace);
            const targetTtls = ttlsFor(namespace);
            lastSetTtl = ttl;
            let header = `O ${count}`;
            for (let i = 0; i < count; i++) {
              const key = keys[i];
              if (multiWrongNodeKey === key && multiWrongNodeLeft > 0) {
                multiWrongNodeLeft--;
                header += " W";
                continue;
              }
              targetStore.set(key, values[i]);
              if (ttl > 0) targetTtls.set(key, ttl);
              else targetTtls.delete(key);
              header += " S";
            }
            socket.write(`${header}${tag}\n`);
            break;
          }

          case "D":
          case "d": {
            lastCommand = parts[0];
            const namespaced = parts[0] === "d";
            const namespaceLength = namespaced ? Number(parts[1]) : 0;
            const keyLength = Number(parts[namespaced ? 2 : 1]);
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write(`W${tag}\n`);
              break;
            }

            const deleted = storeFor(namespace).delete(key);
            ttlsFor(namespace).delete(key);
            socket.write(deleted ? `D${tag}\n` : `N${tag}\n`);
            break;
          }

          case "i": {
            // INCR/DECR (issue #129): `i <namespace-length> <key-length>
            // <delta> [tag]\n<namespace><key>` — always namespaced, unlike
            // G/S/D, so there's no offset/namespaced branch here (see
            // encodeIncr).
            lastCommand = parts[0];
            const namespaceLength = Number(parts[1]);
            const keyLength = Number(parts[2]);
            const delta = Number(parts[3]);
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);
            incrs++;

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write(`W${tag}\n`);
              break;
            }

            const targetStore = storeFor(namespace);
            const existing = targetStore.get(key);
            if (existing === undefined) {
              socket.write(`N${tag}\n`);
              break;
            }

            const existingText = existing.toString("ascii");
            if (!/^-?\d+$/.test(existingText)) {
              socket.write(`T${tag}\n`);
              break;
            }

            // BigInt, not `Number(existingText) + delta`: a test seeding
            // `existingText` past 2^53 (issue #224) needs this mock to
            // compute the exact i64 sum the real server would, not one
            // already rounded by the mock's own arithmetic before it ever
            // reaches the client under test.
            const newValueBytes = Buffer.from(String(BigInt(existingText) + BigInt(delta)), "ascii");
            targetStore.set(key, newValueBytes);

            const ttlSeconds = ttlsFor(namespace).get(key) ?? 0;
            const ttlField = ttlSeconds > 0 ? ` ${ttlSeconds}` : "";
            socket.write(Buffer.concat([Buffer.from(`I ${newValueBytes.length}${ttlField}${tag}\n`), newValueBytes]));
            break;
          }

          case "k": {
            // Compare-and-set (issue #141): `k <namespace-length>
            // <key-length> <value-length> <cond> [<ttl-seconds>]
            // [<tag>]\n<namespace><key><value>` — always namespaced, like
            // `i`. `<cond>` is a bare token: `A` (absent), `P` (present),
            // or a 32-character lowercase hex digest — never
            // length-prefixed, so it's identified purely by its own shape
            // (see encodeCas).
            lastCommand = parts[0];
            const namespaceLength = Number(parts[1]);
            const keyLength = Number(parts[2]);
            const valueLength = Number(parts[3]);
            const cond = parts[4];
            if (buffer.length < bodyStart + namespaceLength + keyLength + valueLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer
              .subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength)
              .toString("utf8");
            const value = Buffer.from(
              buffer.subarray(
                bodyStart + namespaceLength + keyLength,
                bodyStart + namespaceLength + keyLength + valueLength,
              ),
            );
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength + valueLength);
            // The TTL, when present, is the field after `<cond>`; on a
            // tagged connection the tag sits after it as the last field —
            // same field-counting idiom `S`'s own TTL parsing above uses.
            const ttlFieldCount = parts.length - (tagged ? 6 : 5);
            const ttlSeconds = ttlFieldCount > 0 ? Number(parts[5]) : 0;
            casRequests++;

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write(`W${tag}\n`);
              break;
            }

            const targetStore = storeFor(namespace);
            const existing = targetStore.get(key);
            const conditionHolds =
              cond === "A" ? existing === undefined : cond === "P" ? existing !== undefined : existing !== undefined && digestOf(existing) === cond;

            if (!conditionHolds) {
              socket.write(`N${tag}\n`);
              break;
            }

            targetStore.set(key, value);
            if (ttlSeconds > 0) ttlsFor(namespace).set(key, ttlSeconds);
            else ttlsFor(namespace).delete(key);
            lastSetTtl = ttlSeconds;
            socket.write(`S${tag}\n`);
            break;
          }

          case "x": {
            // Compare-and-delete (issue #141): `x <namespace-length>
            // <key-length> <cond> [<tag>]\n<namespace><key>` — `<cond>` is
            // always a digest here (an absent/present-only conditioned
            // delete is already the plain `D`).
            lastCommand = parts[0];
            const namespaceLength = Number(parts[1]);
            const keyLength = Number(parts[2]);
            const cond = parts[3];
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer
              .subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength)
              .toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);
            casDeleteRequests++;

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write(`W${tag}\n`);
              break;
            }

            const targetStore = storeFor(namespace);
            const existing = targetStore.get(key);
            const matches = existing !== undefined && digestOf(existing) === cond;

            if (!matches) {
              socket.write(`N${tag}\n`);
              break;
            }

            targetStore.delete(key);
            ttlsFor(namespace).delete(key);
            socket.write(`D${tag}\n`);
            break;
          }

          case "c": {
            // Clear one namespace (issue #106): `c <namespace-length>
            // [tag]\n<namespace>` — namespace-length 0 clears the default
            // namespace (`store` itself, same sub-map `storeFor("")`
            // resolves to).
            lastCommand = parts[0];
            const namespaceLength = Number(parts[1]);
            if (buffer.length < bodyStart + namespaceLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            buffer = buffer.subarray(bodyStart + namespaceLength);
            clears++;

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (failClearReplies > 0) {
              failClearReplies--;
              socket.destroy();
              return;
            }

            storeFor(namespace).clear();
            ttlsFor(namespace).clear();
            socket.write(`C${tag}\n`);
            break;
          }

          case "F": {
            // Flush everything (issue #106): `F [tag]\n`, no body — drops
            // the default namespace and every named one.
            lastCommand = parts[0];
            buffer = buffer.subarray(bodyStart);
            clears++;

            if (silent) break;

            if (retryableReplies > 0) {
              retryableReplies--;
              socket.write(`R${tag}\n`);
              break;
            }

            if (failClearReplies > 0) {
              failClearReplies--;
              socket.destroy();
              return;
            }

            store.clear();
            namespaceStores.clear();
            ttls.clear();
            namespaceTtls.clear();
            socket.write(`C${tag}\n`);
            break;
          }

          default:
            socket.destroy();
            return;
        }
      }
    });
  });

  const { sockets, close } = trackAndClose(server);
  const port = await listen(server, options.port ?? 0);

  return {
    port,
    address: `127.0.0.1:${port}`,
    store,
    namespacedStore: (namespace) => storeFor(typeof namespace === "string" ? Buffer.from(namespace, "utf8") : Buffer.from(namespace)),
    answerWrongNodeOnce: () => {
      wrongNodeReplies++;
    },
    answerRetryableTimes: (n) => {
      retryableReplies += n;
    },
    lastAuthHeader: () => lastAuthHeader,
    answerWrongNodeOnSetOnce: () => {
      wrongNodeOnSetReplies++;
    },
    answerWrongTagOnce: () => {
      wrongTagReplies++;
    },
    swallowGetOnce: () => {
      swallowedGets++;
    },
    answerMalformedValueOnce: () => {
      malformedValueReplies++;
    },
    answerUnterminatedValueOnce: () => {
      unterminatedValueReplies++;
    },
    unterminatedValueBytesSent: () => unterminatedBytesSent,
    answerStoredToGetOnce: () => {
      storedToGetReplies++;
    },
    connectionCount: () => connections,
    getCount: () => gets,
    incrCount: () => incrs,
    casCount: () => casRequests,
    casDeleteCount: () => casDeleteRequests,
    clearCount: () => clears,
    multiGetCount: () => multiGets,
    multiSetCount: () => multiSets,
    multiGetFrameBytes: () => [...multiGetFrameBytes],
    multiSetFrameBytes: () => [...multiSetFrameBytes],
    answerMultiWrongNodeTimes: (key, times) => {
      multiWrongNodeKey = key;
      multiWrongNodeLeft += times;
    },
    failClearOnce: () => {
      failClearReplies++;
    },
    lastCommand: () => lastCommand,
    lastSetTtl: () => lastSetTtl,
    dropConnections: () => {
      for (const socket of sockets) socket.end();
    },
    delaySets: (ms) => {
      setDelayMs = ms;
    },
    delayGets: (ms) => {
      getDelayMs = ms;
    },
    goSilentAfterHandshake: () => {
      silent = true;
    },
    close,
  };
}

export async function startMockDiscovery(
  initialNodes: Array<{ name: string; address: string }>,
  options: { replication?: number } = {},
): Promise<MockDiscovery> {
  let nodes = initialNodes;
  // SDK proxy mode (issue #122): entirely separate from `nodes`, matching
  // the real server's separate registries — starts empty, since most
  // existing (non-proxy) test suites never call setProxies.
  let proxies: Array<{ name: string; address: string }> = [];
  let warmingUp = false;
  let unterminatedListReplies = 0;
  let unterminatedListBytesSent = 0;
  let listCalls = 0;
  let listProxiesCalls = 0;
  // Default 1 (no replication) so single-placement assertions in tests
  // stay exact; replication tests opt in explicitly. The real server
  // defaults to 2.
  const replication = options.replication ?? 1;

  const server = createServer((socket) => {
    let buffer = Buffer.alloc(0);

    socket.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);

      for (;;) {
        const lf = buffer.indexOf(0x0a);
        if (lf === -1) return;

        const parts = buffer.subarray(0, lf).toString("ascii").split(" ");
        const bodyStart = lf + 1;

        switch (parts[0]) {
          case "A": {
            const secretLength = Number(parts[1]);
            if (buffer.length < bodyStart + secretLength) return;
            buffer = buffer.subarray(bodyStart + secretLength);
            // Echoed response tags: echo the tag capability — clients send the
            // extended A before knowing which kind of server answered.
            socket.write(parts[2] === "T" ? "OdT\n" : "Od\n");
            break;
          }

          case "L": {
            buffer = buffer.subarray(bodyStart);
            listCalls++;

            if (warmingUp) {
              socket.write("B\n");
              socket.end();
              return;
            }

            if (unterminatedListReplies > 0) {
              unterminatedListReplies--;
              socket.write("N");
              // Stream non-newline bytes so the header never terminates —
              // see MockNode's answerUnterminatedValueOnce for the same
              // idea on the cache-node path.
              const interval = setInterval(() => {
                if (socket.destroyed || unterminatedListBytesSent > 512 * 1024) {
                  clearInterval(interval);
                  return;
                }
                const filler = Buffer.alloc(1024, 0x39 /* '9' */);
                unterminatedListBytesSent += filler.length;
                socket.write(filler);
              }, 1);
              return;
            }

            socket.write(Buffer.concat([Buffer.from(`N ${nodes.length} ${replication}\n`), ...encodeEntries(nodes)]));
            break;
          }

          case "Q": {
            // SDK proxy mode (issue #122): same shape as `L` above, minus
            // the trailing replication field a proxy client needs no R
            // for, and served from the separate `proxies` roster.
            buffer = buffer.subarray(bodyStart);
            listProxiesCalls++;

            if (warmingUp) {
              socket.write("B\n");
              socket.end();
              return;
            }

            socket.write(Buffer.concat([Buffer.from(`N ${proxies.length}\n`), ...encodeEntries(proxies)]));
            break;
          }

          default:
            socket.destroy();
            return;
        }
      }
    });
  });

  const { close } = trackAndClose(server);
  const port = await listen(server);

  return {
    port,
    address: `127.0.0.1:${port}`,
    setNodes: (next) => {
      nodes = next;
    },
    setWarmingUp: (warming) => {
      warmingUp = warming;
    },
    answerUnterminatedListOnce: () => {
      unterminatedListReplies++;
    },
    unterminatedListBytesSent: () => unterminatedListBytesSent,
    setProxies: (next) => {
      proxies = next;
    },
    listCount: () => listCalls,
    listProxiesCount: () => listProxiesCalls,
    close,
  };
}
