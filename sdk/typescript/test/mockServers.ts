/**
 * In-process stand-ins for nanocached-node and nanocached-discovery,
 * speaking just enough of the wire protocol (`A`, `G`/`S`/`D`, `L`) for
 * the client tests to exercise NanocachedClient end-to-end over real TCP
 * sockets without the Rust binaries.
 */

import { createServer, type Server, type Socket } from "node:net";

interface MockServerBase {
  port: number;
  address: string;
  close(): Promise<void>;
}

export interface MockNode extends MockServerBase {
  store: Map<string, Buffer>;
  /** Queue a one-off `W` reply for the next G/S/D request. */
  answerWrongNodeOnce(): void;
  /** Queue a one-off garbage `V` header for the next G request. */
  answerMalformedValueOnce(): void;
  /** Queue a one-off `S` reply for the next G request — a well-formed
   * frame of the wrong kind, as a desynced (off-by-one) stream would
   * produce. */
  answerStoredToGetOnce(): void;
  /** How many connections this server has ever accepted. */
  connectionCount(): number;
  /** How many `G` requests this server has ever received. */
  getCount(): number;
  /** Server-side close of every currently open connection (a FIN, like
   * nanocached-node's own idle timeout), leaving the server listening. */
  dropConnections(): void;
  /** Makes every future `S` reply wait `ms` first — for tests proving a
   * caller isn't blocked on a slow replica leg (doc/adr/0014-*.md). */
  delaySets(ms: number): void;
}

export interface MockDiscovery extends MockServerBase {
  setNodes(nodes: Array<{ name: string; address: string }>): void;
  /** While true, `L` answers `B\n` and closes — the ADR-0010 startup
   * grace of a freshly restarted discovery server. */
  setWarmingUp(warming: boolean): void;
}

/** A port with nothing listening on it — bound once to reserve a real
 * ephemeral port, then released. */
export async function unusedPort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return port;
}

function listen(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("mock server bound to a non-TCP address"));
        return;
      }
      resolve(address.port);
    });
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

export async function startMockNode(options: { requiredSecret?: string } = {}): Promise<MockNode> {
  const store = new Map<string, Buffer>();
  let wrongNodeReplies = 0;
  let malformedValueReplies = 0;
  let storedToGetReplies = 0;
  let connections = 0;
  let gets = 0;
  let setDelayMs = 0;

  const server = createServer((socket) => {
    connections++;
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
            const secret = buffer.subarray(bodyStart, bodyStart + secretLength);
            buffer = buffer.subarray(bodyStart + secretLength);

            const accepted =
              options.requiredSecret === undefined
                ? secret.length > 0
                : secret.equals(Buffer.from(options.requiredSecret, "utf8"));
            socket.write(accepted ? "On\n" : "En\n");
            if (!accepted) socket.end();
            break;
          }

          case "G": {
            const keyLength = Number(parts[1]);
            if (buffer.length < bodyStart + keyLength) return;
            const key = buffer.subarray(bodyStart, bodyStart + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + keyLength);
            gets++;

            if (malformedValueReplies > 0) {
              malformedValueReplies--;
              socket.write("V x\n");
              break;
            }

            if (storedToGetReplies > 0) {
              storedToGetReplies--;
              socket.write("S\n");
              break;
            }

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write("W\n");
              break;
            }

            const value = store.get(key);
            if (value === undefined) {
              socket.write("N\n");
            } else {
              socket.write(Buffer.concat([Buffer.from(`V ${value.length}\n`), value]));
            }
            break;
          }

          case "S": {
            const keyLength = Number(parts[1]);
            const valueLength = Number(parts[2]);
            if (buffer.length < bodyStart + keyLength + valueLength) return;
            const key = buffer.subarray(bodyStart, bodyStart + keyLength).toString("utf8");
            const value = Buffer.from(buffer.subarray(bodyStart + keyLength, bodyStart + keyLength + valueLength));
            buffer = buffer.subarray(bodyStart + keyLength + valueLength);

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write("W\n");
              break;
            }

            store.set(key, value);
            if (setDelayMs > 0) {
              setTimeout(() => socket.write("S\n"), setDelayMs);
            } else {
              socket.write("S\n");
            }
            break;
          }

          case "D": {
            const keyLength = Number(parts[1]);
            if (buffer.length < bodyStart + keyLength) return;
            const key = buffer.subarray(bodyStart, bodyStart + keyLength).toString("utf8");
            buffer = buffer.subarray(bodyStart + keyLength);

            if (wrongNodeReplies > 0) {
              wrongNodeReplies--;
              socket.write("W\n");
              break;
            }

            socket.write(store.delete(key) ? "D\n" : "N\n");
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
  const port = await listen(server);

  return {
    port,
    address: `127.0.0.1:${port}`,
    store,
    answerWrongNodeOnce: () => {
      wrongNodeReplies++;
    },
    answerMalformedValueOnce: () => {
      malformedValueReplies++;
    },
    answerStoredToGetOnce: () => {
      storedToGetReplies++;
    },
    connectionCount: () => connections,
    getCount: () => gets,
    dropConnections: () => {
      for (const socket of sockets) socket.end();
    },
    delaySets: (ms) => {
      setDelayMs = ms;
    },
    close,
  };
}

export async function startMockDiscovery(
  initialNodes: Array<{ name: string; address: string }>,
  options: { replication?: number } = {},
): Promise<MockDiscovery> {
  let nodes = initialNodes;
  let warmingUp = false;
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
            socket.write("Od\n");
            break;
          }

          case "L": {
            buffer = buffer.subarray(bodyStart);

            if (warmingUp) {
              socket.write("B\n");
              socket.end();
              return;
            }

            const entries = nodes.map(({ name, address }) => {
              const nameBytes = Buffer.from(name, "utf8");
              const addrBytes = Buffer.from(address, "utf8");
              return Buffer.concat([
                Buffer.from(`${nameBytes.length} ${addrBytes.length}\n`),
                nameBytes,
                addrBytes,
                Buffer.from("\n"),
              ]);
            });
            socket.write(Buffer.concat([Buffer.from(`N ${nodes.length} ${replication}\n`), ...entries]));
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
    close,
  };
}
