/**
 * A trimmed in-process stand-in for nanocached-node, speaking just enough
 * of the wire protocol to drive this adapter's tests over a real TCP
 * socket: `A` (handshake, always accepting and always untagged) and
 * namespaced `g`/`s`/`d`/`c` (issues #105/#106 — the only frames a
 * namespace-scoped client ever sends).
 *
 * A fresh re-implementation for this module, not a copy of the SDK's own
 * (private) test double or the sibling `nanocached-cache-manager`
 * adapter's — see the shared adapter spec, item 3.
 */

import { createServer, type Server, type Socket } from "node:net";

export interface MockNode {
  port: number;
  address: string;
  /** Raw per-namespace stores, keyed by the namespace's raw bytes
   * (base64, so an arbitrary namespace is a safe Map key) — lets a test
   * assert isolation between two stores/namespaces directly, without
   * going back through the wire. */
  store(namespace: string): Map<string, Buffer>;
  /** How many `c` (clear) requests this server has received — lets a
   * test assert `clear()` sent exactly one wire frame. */
  clearCount(): number;
  /** The TTL (whole seconds; 0 if the field was omitted on the wire) from
   * the most recent `s` request this server received — lets a test
   * assert the millisecond-to-second rounding the store did before
   * writing. */
  lastSetTtl(): number;
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
            socket.write("On\n");
            break;
          }

          case "g": {
            const namespaceLength = Number(parts[1]);
            const keyLength = Number(parts[2]);
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

          case "s": {
            const namespaceLength = Number(parts[1]);
            const keyLength = Number(parts[2]);
            const valueLength = Number(parts[3]);
            if (buffer.length < bodyStart + namespaceLength + keyLength + valueLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            const value = Buffer.from(
              buffer.subarray(bodyStart + namespaceLength + keyLength, bodyStart + namespaceLength + keyLength + valueLength),
            );
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength + valueLength);

            // TTL, when present, is the field right after key/value
            // lengths; its absence means "no expiry" (0).
            const ttlFieldCount = parts.length - 4;
            lastSetTtl = ttlFieldCount > 0 ? Number(parts[4]) : 0;

            storeFor(namespace).set(key, value);
            socket.write("S\n");
            break;
          }

          case "d": {
            const namespaceLength = Number(parts[1]);
            const keyLength = Number(parts[2]);
            if (buffer.length < bodyStart + namespaceLength + keyLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            const key = buffer.subarray(bodyStart + namespaceLength, bodyStart + namespaceLength + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + namespaceLength + keyLength);

            socket.write(storeFor(namespace).delete(key) ? "D\n" : "N\n");
            break;
          }

          case "c": {
            const namespaceLength = Number(parts[1]);
            if (buffer.length < bodyStart + namespaceLength) return;
            const namespace = Buffer.from(buffer.subarray(bodyStart, bodyStart + namespaceLength));
            buffer = buffer.subarray(bodyStart + namespaceLength);
            clears++;

            storeFor(namespace).clear();
            socket.write("C\n");
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
    close: () =>
      new Promise<void>((resolve, reject) => {
        for (const socket of sockets) socket.destroy();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}
