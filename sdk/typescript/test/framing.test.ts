import { afterEach, describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { connect, createServer, type AddressInfo, type Server, type Socket } from "node:net";
import { Connection } from "../src/connection.js";
import { connectAndIdentify, connectAndListProxies } from "../src/identify.js";

// Regression tests for the O(n^2) re-copying of a large response while it
// trickles in (audit finding): every new fragment used to concatenate the
// whole accumulation again (connection.ts onData, identify.ts readFrame),
// and readFrame also re-parsed a roster from its first entry each time.
// Asserted on bytes copied through Buffer.concat, never wall-clock.

const servers: Server[] = [];

afterEach(() => {
  mock.restoreAll();
  for (const s of servers.splice(0)) s.close();
});

async function listen(onConnection: (socket: Socket) => void): Promise<number> {
  const server = createServer(onConnection);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

/** Writes `payload` to `socket` in `chunkSize` pieces, each only after the
 * previous one was flushed, so the client sees many separate fragments. */
async function writeInChunks(socket: Socket, payload: Buffer, chunkSize: number): Promise<void> {
  for (let offset = 0; offset < payload.length && !socket.destroyed; offset += chunkSize) {
    const piece = payload.subarray(offset, Math.min(offset + chunkSize, payload.length));
    await new Promise<void>((resolve) => socket.write(piece, () => resolve()));
  }
}

/** Counts every byte handed to `Buffer.concat` from now on. */
function countConcatBytes(): { bytes: () => number } {
  let total = 0;
  const original = Buffer.concat;
  mock.method(Buffer, "concat", (list: readonly Uint8Array[], totalLength?: number) => {
    total += totalLength ?? list.reduce((sum, b) => sum + b.length, 0);
    return original.call(Buffer, list, totalLength);
  });
  return { bytes: () => total };
}

function rosterPayload(nodeCount: number): { wire: Buffer; nodes: { name: string; address: string }[] } {
  const nodes = Array.from({ length: nodeCount }, (_, i) => ({
    name: `node-${String(i).padStart(8, "0")}-0000-4000-8000-000000000000`,
    address: `10.${(i >> 16) & 255}.${(i >> 8) & 255}.${i & 255}:11211`,
  }));
  const parts = nodes.map((n) => `${n.name.length} ${n.address.length}\n${n.name}${n.address}\n`);
  return { wire: Buffer.from(`N ${nodeCount} 2\n${parts.join("")}`), nodes };
}

describe("Connection copies a large response once, not once per fragment", () => {
  it("an 8 MiB multi-get reply in 16 KiB fragments", async () => {
    const keyCount = 8;
    const valueLength = 1024 * 1024;
    const body = Buffer.alloc(keyCount * valueLength, 0x61);
    body.writeUInt32BE(0xdeadbeef, body.length - 4);
    const header = `M ${keyCount} ${Array(keyCount).fill(valueLength).join(" ")}\n`;
    const payload = Buffer.concat([Buffer.from(header), body]);

    const port = await listen((socket) => {
      socket.once("data", () => void writeInChunks(socket, payload, 16 * 1024));
    });
    const socket = connect(port, "127.0.0.1");
    await new Promise<void>((resolve) => socket.once("connect", resolve));
    const connection = new Connection(socket);

    const counter = countConcatBytes();
    const entries = await connection.multiGet(Array.from({ length: keyCount }, (_, i) => Buffer.from(`k${i}`)));
    connection.close();

    assert.equal(entries.length, keyCount);
    for (const entry of entries) {
      assert.equal(entry.kind, "hit");
      assert.equal(entry.kind === "hit" ? entry.value.length : -1, valueLength);
    }
    const last = entries[keyCount - 1];
    assert.equal(last.kind === "hit" ? last.value.readUInt32BE(valueLength - 4) : -1, 0xdeadbeef);
    // One concat to assemble the frame (plus the request frame's own small
    // one): quadratic copying would be hundreds of times the payload.
    assert.ok(counter.bytes() <= 2 * payload.length, `${counter.bytes()} bytes concatenated for a ${payload.length}-byte reply`);
  });

  it("a 1 MiB value trickling in 1 KiB fragments, followed by a pipelined reply in the same stream", async () => {
    const value = Buffer.alloc(1024 * 1024, 0x62);
    const payload = Buffer.concat([Buffer.from(`V ${value.length}\n`), value, Buffer.from("N\n")]);

    const port = await listen((socket) => {
      socket.once("data", () => void writeInChunks(socket, payload, 1024));
    });
    const socket = connect(port, "127.0.0.1");
    await new Promise<void>((resolve) => socket.once("connect", resolve));
    const connection = new Connection(socket);

    const counter = countConcatBytes();
    const first = connection.get("big");
    const second = connection.get("missing");
    const [got, miss] = await Promise.all([first, second]);
    connection.close();

    assert.deepEqual(got, value);
    assert.equal(miss, null);
    assert.ok(counter.bytes() <= 2 * payload.length, `${counter.bytes()} bytes concatenated for a ${payload.length}-byte stream`);
  });
});

describe("identify reads a large roster in one pass", () => {
  function rosterServer(wire: Buffer, chunkSize: number): Promise<number> {
    return listen((socket) => {
      socket.on("error", () => {});
      let requests = 0;
      socket.on("data", () => {
        requests++;
        if (requests === 1) socket.write("Od\n"); // answers `A`: a discovery server
        else void writeInChunks(socket, wire, chunkSize); // answers `L`/`Q`
      });
    });
  }

  it("L: a 20000-node roster in 4 KiB fragments is parsed intact without re-copying it per fragment", async () => {
    const { wire, nodes } = rosterPayload(20_000);
    const port = await rosterServer(wire, 4096);

    const counter = countConcatBytes();
    const result = await connectAndIdentify({ host: "127.0.0.1", port });

    assert.equal(result.kind, "cluster");
    if (result.kind !== "cluster") return;
    assert.equal(result.replication, 2);
    assert.deepEqual(result.nodes, nodes);
    assert.ok(counter.bytes() <= 2 * wire.length, `${counter.bytes()} bytes concatenated for a ${wire.length}-byte roster`);
  });

  it("Q: the proxy roster takes the same path", async () => {
    const { wire, nodes } = rosterPayload(5_000);
    // `Q`'s header has no replication field.
    const proxyWire = Buffer.concat([Buffer.from(`N ${nodes.length}\n`), wire.subarray(wire.indexOf(0x0a) + 1)]);
    const port = await rosterServer(proxyWire, 2048);

    const counter = countConcatBytes();
    const result = await connectAndListProxies({ host: "127.0.0.1", port });

    assert.equal(result.kind, "cluster");
    if (result.kind !== "cluster") return;
    assert.deepEqual(result.proxies, nodes);
    assert.ok(counter.bytes() <= 2 * proxyWire.length, `${counter.bytes()} bytes concatenated for a ${proxyWire.length}-byte roster`);
  });

  for (const chunkSize of [1, 3, 17, 100]) {
    it(`parses a small roster identically when it arrives ${chunkSize} byte(s) at a time`, async () => {
      const { wire, nodes } = rosterPayload(40);
      const port = await rosterServer(wire, chunkSize);
      const result = await connectAndIdentify({ host: "127.0.0.1", port });
      assert.equal(result.kind, "cluster");
      if (result.kind !== "cluster") return;
      assert.deepEqual(result.nodes, nodes);
    });
  }

  it("still rejects a malformed entry that arrives after valid ones were already consumed", async () => {
    const good = "4 9\nname127.0.0.1:1\n";
    const wire = Buffer.from(`N 3 2\n${good}${good}4 9\nname127.0.0.1:1X`);
    const port = await rosterServer(wire, 5);
    await assert.rejects(connectAndIdentify({ host: "127.0.0.1", port }), /malformed entry/);
  });
});
