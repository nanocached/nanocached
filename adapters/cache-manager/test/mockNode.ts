/**
 * A trimmed in-process stand-in for nanocached-node, speaking just enough
 * of the wire protocol to drive this adapter's tests over a real TCP
 * socket: `A` (handshake, always accepting and always answering untagged
 * — this store never needs echoed response tags), namespaced `g`/`s`/`d`/`c`
 * (issue #105/#106 — the only frames a namespace-scoped client ever
 * sends) plus legacy `G` (only ever seen here as the SDK's internal
 * keep-alive probe against the default namespace, never sent by this
 * store's own operations) and `F` (never sent by this adapter, since
 * `reset()` is always a namespace `c`; kept for protocol completeness/a
 * possible future test), and namespaced `m`/`o` (issue #152 — `mget`/
 * `mset`'s bulk wire ops).
 *
 * This is a fresh re-implementation for this module, not a copy of the
 * SDK's own (private) test double — see the shared adapter spec, item 3.
 */

import { createServer, type Server, type Socket } from "node:net";

export interface MockNode {
  port: number;
  address: string;
  /** Raw per-namespace stores, keyed by the namespace's raw bytes
   * (base64, so an arbitrary namespace is a safe Map key) — `""` is the
   * default namespace. Exposed mainly so a test can assert isolation
   * between two stores/namespaces directly, without going back through
   * the wire. */
  store(namespace: string): Map<string, Buffer>;
  /** How many `c`/`F` (clear) requests this server has received —
   * lets a test assert reset() sent exactly one wire frame. */
  clearCount(): number;
  /** The TTL (whole seconds; 0 if the field was omitted on the wire) from
   * the most recent `s`/`S` request this server received — lets a test
   * assert the millisecond-to-second rounding the store did before
   * writing. */
  lastSetTtl(): number;
  /** The raw command letter of the most recent request received
   * (`"g"`/`"s"`/`"d"`/`"c"`/`"G"`/`"F"`) — mostly useful for asserting
   * the keep-alive probe's shape if a test ever needs to. */
  lastCommand(): string;
  close(): Promise<void>;
}

function listen(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("mock node bound to a non-TCP address"));
        return;
      }
      resolve(address.port);
    });
  });
}

export async function startMockNode(): Promise<MockNode> {
  const namespaceStores = new Map<string, Map<string, Buffer>>();
  function storeFor(namespace: Buffer): Map<string, Buffer> {
    const key = namespace.toString("base64");
    let existing = namespaceStores.get(key);
    if (existing === undefined) {
      existing = new Map();
      namespaceStores.set(key, existing);
    }
    return existing;
  }

  let clears = 0;
  let lastSetTtl = 0;
  let lastCommand = "";
  const sockets = new Set<Socket>();

  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.on("close", () => sockets.delete(socket));

    let buffer = Buffer.alloc(0);

    socket.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);

      for (;;) {
        const lf = buffer.indexOf(0x0a);
        if (lf === -1) return;

        const parts = buffer.subarray(0, lf).toString("ascii").split(" ");
        const bodyStart = lf + 1;
        const command = parts[0];

        switch (command) {
          case "A": {
            const secretLength = Number(parts[1]);
            if (buffer.length < bodyStart + secretLength) return;
            buffer = buffer.subarray(bodyStart + secretLength);
            // Always accepts, always untagged — this adapter's client
            // never needs echoed response tags (single-node, no cluster
            // routing retries to disambiguate).
            socket.write("On\n");
            break;
          }

          case "G":
          case "g": {
            lastCommand = command;
            const namespaced = command === "g";
            const namespaceLength = namespaced ? Number(parts[1]) : 0;
            const keyLength = Number(parts[namespaced ? 2 : 1]);
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);

            const value = storeFor(namespace).get(key);
            if (value === undefined) {
              socket.write("N\n");
            } else {
              socket.write(Buffer.concat([Buffer.from(`V ${value.length}\n`), value]));
            }
            break;
          }

          case "S":
          case "s": {
            lastCommand = command;
            const namespaced = command === "s";
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

            // TTL, when present, is the field right after key/value
            // lengths; its absence means "no expiry" (0) — see
            // encodeSet's doc comment in the SDK.
            const ttlFieldCount = parts.length - (3 + offset);
            lastSetTtl = ttlFieldCount > 0 ? Number(parts[3 + offset]) : 0;

            storeFor(namespace).set(key, value);
            socket.write("S\n");
            break;
          }

          case "D":
          case "d": {
            lastCommand = command;
            const namespaced = command === "d";
            const namespaceLength = namespaced ? Number(parts[1]) : 0;
            const keyLength = Number(parts[namespaced ? 2 : 1]);
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);

            socket.write(storeFor(namespace).delete(key) ? "D\n" : "N\n");
            break;
          }

          case "c": {
            // Clear one namespace (issue #106): `c <namespace-length>\n<namespace>`.
            lastCommand = command;
            const namespaceLength = Number(parts[1]);
            if (buffer.length < bodyStart + namespaceLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            buffer = buffer.subarray(bodyStart + namespaceLength);
            clears++;

            storeFor(namespace).clear();
            socket.write("C\n");
            break;
          }

          case "F": {
            lastCommand = command;
            buffer = buffer.subarray(bodyStart);
            clears++;
            namespaceStores.clear();
            socket.write("C\n");
            break;
          }

          case "m": {
            // Batched get (issue #152, docs/protocol.html "m / o"):
            // `m <ns-len> <n> <key-len-1> ... <key-len-n>\n<namespace><key-1>...<key-n>`.
            lastCommand = command;
            const namespaceLength = Number(parts[1]);
            const n = Number(parts[2]);
            const keyLengths: number[] = [];
            for (let i = 0; i < n; i++) keyLengths.push(Number(parts[3 + i]));
            const totalKeyLength = keyLengths.reduce((sum, len) => sum + len, 0);
            if (buffer.length < bodyStart + namespaceLength + totalKeyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            let offset = bodyStart + namespaceLength;
            const keys: string[] = [];
            for (const len of keyLengths) {
              keys.push(buffer.subarray(offset, offset + len).toString("utf8"));
              offset += len;
            }
            buffer = buffer.subarray(offset);

            const store = storeFor(namespace);
            const values = keys.map((key) => store.get(key));
            const results = values.map((value) => (value === undefined ? "-" : String(value.length)));
            const hits = values.filter((value): value is Buffer => value !== undefined);
            socket.write(Buffer.concat([Buffer.from(`M ${n} ${results.join(" ")}\n`), ...hits]));
            break;
          }

          case "o": {
            // Batched set (issue #152, docs/protocol.html "m / o"): one
            // shared TTL for the whole batch, omitted from the wire when 0.
            // `o <ns-len> <n> <key-len-1> <val-len-1> ... <key-len-n> <val-len-n> [ttl]\n<namespace><key-1><value-1>...<key-n><value-n>`.
            lastCommand = command;
            const namespaceLength = Number(parts[1]);
            const n = Number(parts[2]);
            const keyLengths: number[] = [];
            const valueLengths: number[] = [];
            for (let i = 0; i < n; i++) {
              keyLengths.push(Number(parts[3 + 2 * i]));
              valueLengths.push(Number(parts[4 + 2 * i]));
            }
            const trailingFieldCount = parts.length - (3 + 2 * n);
            const ttl = trailingFieldCount > 0 ? Number(parts[3 + 2 * n]) : 0;
            const totalKeyValueLength =
              keyLengths.reduce((sum, len) => sum + len, 0) + valueLengths.reduce((sum, len) => sum + len, 0);
            if (buffer.length < bodyStart + namespaceLength + totalKeyValueLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            let offset = bodyStart + namespaceLength;
            const store = storeFor(namespace);
            for (let i = 0; i < n; i++) {
              const key = buffer.subarray(offset, offset + keyLengths[i]).toString("utf8");
              offset += keyLengths[i];
              const value = Buffer.from(buffer.subarray(offset, offset + valueLengths[i]));
              offset += valueLengths[i];
              store.set(key, value);
            }
            buffer = buffer.subarray(offset);
            lastSetTtl = ttl;

            socket.write(`O ${n} ${new Array(n).fill("S").join(" ")}\n`);
            break;
          }

          default:
            socket.destroy();
            return;
        }
      }
    });
  });

  const port = await listen(server);

  return {
    port,
    address: `127.0.0.1:${port}`,
    store: (namespace) => storeFor(Buffer.from(namespace, "utf8")),
    clearCount: () => clears,
    lastSetTtl: () => lastSetTtl,
    lastCommand: () => lastCommand,
    close: () =>
      new Promise<void>((resolve, reject) => {
        for (const socket of sockets) socket.destroy();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}
