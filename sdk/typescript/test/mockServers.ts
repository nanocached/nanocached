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
}

export interface MockDiscovery extends MockServerBase {
  setNodes(nodes: Array<{ name: string; address: string }>): void;
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
            socket.write("S\n");
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

  const { close } = trackAndClose(server);
  const port = await listen(server);

  return {
    port,
    address: `127.0.0.1:${port}`,
    store,
    answerWrongNodeOnce: () => {
      wrongNodeReplies++;
    },
    close,
  };
}

export async function startMockDiscovery(
  initialNodes: Array<{ name: string; address: string }>,
): Promise<MockDiscovery> {
  let nodes = initialNodes;

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
            socket.write(Buffer.concat([Buffer.from(`N ${nodes.length}\n`), ...entries]));
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
    close,
  };
}
