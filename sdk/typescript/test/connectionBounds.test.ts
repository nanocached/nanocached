import { afterEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { connect, createServer, type AddressInfo, type Server, type Socket } from "node:net";
import { Connection, HeadQueue } from "../src/connection.js";
import { startMockNode } from "./mockServers.js";

// Bounds on what one connection holds: the pending queue's drain cost,
// the receive buffer while a reply trickles in, the socket's write
// buffer under a stalled peer, and the process itself while idle.

const servers: Server[] = [];
const accepted: Socket[] = [];

afterEach(() => {
  // Accepted sockets are destroyed too: a paused one never sees the
  // client's FIN and would keep the process alive past the test.
  for (const socket of accepted.splice(0)) socket.destroy();
  for (const s of servers.splice(0)) s.close();
});

async function listen(onConnection: (socket: Socket) => void): Promise<number> {
  const server = createServer((socket) => {
    accepted.push(socket);
    onConnection(socket);
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

async function dial(port: number): Promise<Socket> {
  const socket = connect(port, "127.0.0.1");
  await new Promise<void>((resolve) => socket.once("connect", resolve));
  return socket;
}

/** Writes `payload` one byte per `write()`, each only after the previous
 * one was flushed, so the kernel cannot coalesce them. */
async function trickle(socket: Socket, payload: Buffer): Promise<void> {
  for (let offset = 0; offset < payload.length && !socket.destroyed; offset++) {
    await new Promise<void>((resolve) => socket.write(payload.subarray(offset, offset + 1), () => resolve()));
  }
}

describe("HeadQueue", () => {
  it("is a FIFO that drains in linear time", () => {
    const queue = new HeadQueue<number>();
    assert.equal(queue.shift(), undefined);
    for (let i = 0; i < 5; i++) queue.push(i);
    assert.equal(queue.length, 5);
    assert.equal(queue.shift(), 0);
    assert.equal(queue.shift(), 1);
    queue.push(5);
    assert.deepEqual(queue.drain(), [2, 3, 4, 5]);
    assert.equal(queue.length, 0);
    assert.equal(queue.shift(), undefined);

    // 200,000 entries pushed then shifted: `Array.shift()` on an array
    // this size is O(n) per call, 52 s in all; a head index is O(1).
    const started = process.hrtime.bigint();
    for (let i = 0; i < 200_000; i++) queue.push(i);
    let sum = 0;
    for (let item = queue.shift(); item !== undefined; item = queue.shift()) sum += item;
    const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
    assert.equal(sum, (199_999 * 200_000) / 2);
    assert.ok(elapsedMs < 2000, `draining 200,000 entries took ${elapsedMs.toFixed(0)} ms`);
  });
});

describe("Connection receive buffer", () => {
  it("holds a trickled reply in one buffer sized from its header, released once parsed", async () => {
    // The reply arrives one byte per `data` event. Each event used to be
    // kept as its own Buffer object until the frame completed (about
    // 390 bytes each, 450 MiB for a 2 MiB reply); now the header sizes
    // one buffer and the body is copied into it in place.
    const value = Buffer.alloc(100_000, 0x62);
    const header = Buffer.from(`V ${value.length}\n`);
    // The peer trickles the first 3,000 bytes of the body and then holds
    // the rest until the test has looked at the mid-frame state.
    let release: () => void = () => {};
    const released = new Promise<void>((resolve) => (release = resolve));
    const port = await listen((socket) => {
      socket.once("data", () => {
        socket.write(header);
        void trickle(socket, value.subarray(0, 3000))
          .then(() => released)
          .then(() => socket.write(value.subarray(3000)));
      });
    });
    const socket = await dial(port);
    const connection = new Connection(socket);
    const internals = connection as unknown as { receive: Buffer; receiveLength: number; neededLength: number };

    const got = connection.get("big");
    // Mid-frame: one buffer of exactly the frame's size, filled in place,
    // whatever the number of events so far.
    while (internals.receiveLength < header.length + 3000) await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(internals.neededLength, header.length + value.length);
    assert.equal(internals.receive.length, header.length + value.length, "the buffer must not grow per event");

    release();
    assert.deepEqual(await got, value);
    // Parsed and past the release threshold: the buffer is let go.
    assert.equal(internals.receiveLength, 0);
    assert.equal(internals.receive.length, 0);
    connection.close();
  });
});

describe("Connection write backpressure", () => {
  it("waits for drain before writing the next frame to a stalled peer", async () => {
    // The peer never reads. Fifty 900 KiB sets used to be written into
    // the socket's buffer in full (45 MB, 1.1 GB for 600); now only the
    // frame being written is in the stream's buffer and the rest wait
    // their turn, so the buffer stays within a frame of the mark.
    const port = await listen((socket) => socket.pause());
    const socket = await dial(port);
    const connection = new Connection(socket);
    const value = Buffer.alloc(900 * 1024, 0x78);
    const sets = Array.from({ length: 50 }, (_, i) => connection.set(`k${i}`, value).catch((error: Error) => error));
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.ok(
      socket.writableLength <= 2 * value.length,
      `${socket.writableLength} bytes buffered in the socket for 50 queued sets`,
    );
    connection.close();
    const outcomes = await Promise.all(sets);
    assert.ok(outcomes.every((outcome) => outcome instanceof Error));
  });
});

describe("Connection and the process lifetime", () => {
  it("an idle client does not keep the process alive", async () => {
    // A script that connects, writes once and never calls close() exits
    // on its own (the other five SDKs let the process exit too); it used
    // to hang until killed, since an open socket keeps the event loop
    // alive. The socket is ref'd only while a request is pending.
    const node = await startMockNode();
    try {
      const index = new URL("../src/index.js", import.meta.url).href;
      const script = `
        const { NanocachedClient } = await import(${JSON.stringify(index)});
        const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: ${node.port} }] });
        await client.set("k", "v");
        const value = await client.get("k");
        process.stdout.write("got " + value);
      `;
      const child = spawn(process.execPath, ["--input-type=module", "-e", script], { stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (chunk: Buffer) => (stdout += chunk.toString()));
      child.stderr.on("data", (chunk: Buffer) => (stderr += chunk.toString()));
      const exit = await new Promise<number | null | "timeout">((resolve) => {
        const timer = setTimeout(() => {
          child.kill("SIGKILL");
          resolve("timeout");
        }, 8000);
        child.once("exit", (code) => {
          clearTimeout(timer);
          resolve(code);
        });
      });
      assert.equal(exit, 0, `the script must exit on its own (stdout: ${stdout}, stderr: ${stderr})`);
      assert.equal(stdout, "got v");
    } finally {
      await node.close();
    }
  });
});
