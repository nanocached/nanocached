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
