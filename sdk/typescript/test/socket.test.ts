import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { Socket } from "node:net";
import { connectSocket } from "../src/socket.js";
import { startMockNode } from "./mockServers.js";

describe("connectSocket (issue #301)", () => {
  it("disables Nagle's algorithm (setNoDelay(true)) on every connected socket", async () => {
    // Every other SDK, the server, the proxy, and discovery all disable
    // Nagle's algorithm explicitly — small request/response frames (the
    // common case for this protocol) would otherwise sit buffered for up
    // to Nagle's own delay before going out. `TLSSocket` inherits
    // `setNoDelay` from `net.Socket` rather than overriding it, so
    // spying on the shared prototype method catches both the plain and
    // TLS paths through connectSocket.
    const node = await startMockNode();
    const calls: unknown[] = [];
    const spy = mock.method(Socket.prototype, "setNoDelay", function (this: Socket, ...args: unknown[]) {
      calls.push(args[0]);
      return this;
    });
    try {
      const socket = await connectSocket({ host: "127.0.0.1", port: node.port });
      try {
        assert.deepEqual(calls, [true], "expected exactly one setNoDelay(true) call while connecting");
      } finally {
        socket.destroy();
      }
    } finally {
      spy.mock.restore();
      await node.close();
    }
  });
});

describe("connectSocket with an address Node rejects synchronously (issue #1163)", () => {
  // `net.connect` throws synchronously for a port that is NaN, a
  // fraction or above 65535 (a misconfigured seed address — a discovery
  // address never gets here). The promise rejected correctly, but the
  // dial timer had already been armed and stayed armed: on firing it
  // touched the never-initialised `socket` binding and threw a
  // ReferenceError out of a timer callback — an uncaught exception that
  // ended the process five seconds after the caller had handled the
  // rejection.
  for (const port of [70000, Number.NaN, 1.5]) {
    it(`port ${port}: rejects at once and leaves no timer behind`, async () => {
      const uncaught: unknown[] = [];
      const capture = (error: unknown) => {
        uncaught.push(error);
      };
      process.on("uncaughtException", capture);
      try {
        await assert.rejects(
          connectSocket({ host: "127.0.0.1", port, connectDeadlineMs: 20 }),
          (error: unknown) => error instanceof RangeError || error instanceof TypeError,
        );
        // Past the deadline by a wide margin: a leftover timer fires here.
        await new Promise((resolve) => setTimeout(resolve, 100));
        assert.deepEqual(
          uncaught.map((error) => String(error)),
          [],
          "the connect timer fired after the synchronous dial failure",
        );
      } finally {
        process.removeListener("uncaughtException", capture);
      }
    });
  }
});
