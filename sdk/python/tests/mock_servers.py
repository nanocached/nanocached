"""In-process stand-ins for nanocached-node and nanocached-discovery,
speaking just enough of the wire protocol (``A``, ``G``/``S``/``D``, ``L``)
for the client tests to exercise NanocachedClient end-to-end over real TCP
sockets without the Rust binaries. Mirrors the TypeScript SDK's mocks."""

from __future__ import annotations

import asyncio


class MockNode:
    def __init__(self, required_secret: bytes | None = None) -> None:
        self.store: dict[bytes, bytes] = {}
        self.required_secret = required_secret
        self.connection_count = 0
        self.get_count = 0
        self._wrong_node_replies = 0
        self._server: asyncio.Server | None = None
        self._sockets: set[asyncio.StreamWriter] = set()
        self.port = 0

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def answer_wrong_node_once(self) -> None:
        self._wrong_node_replies += 1

    async def start(self) -> "MockNode":
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    def drop_connections(self) -> None:
        """Server-side FIN on every open connection, like the idle timeout."""
        for writer in list(self._sockets):
            writer.close()

    async def close(self) -> None:
        self.drop_connections()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connection_count += 1
        self._sockets.add(writer)
        try:
            while True:
                try:
                    header = await reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, ConnectionError):
                    return
                parts = header[:-1].split(b" ")

                if parts[0] == b"A":
                    secret = await reader.readexactly(int(parts[1]))
                    accepted = (
                        len(secret) > 0
                        if self.required_secret is None
                        else secret == self.required_secret
                    )
                    writer.write(b"On\n" if accepted else b"En\n")
                    await writer.drain()
                    if not accepted:
                        return

                elif parts[0] == b"G":
                    key = await reader.readexactly(int(parts[1]))
                    self.get_count += 1
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W\n")
                    elif key in self.store:
                        value = self.store[key]
                        writer.write(b"V %d\n%b" % (len(value), value))
                    else:
                        writer.write(b"N\n")
                    await writer.drain()

                elif parts[0] == b"S":
                    key = await reader.readexactly(int(parts[1]))
                    value = await reader.readexactly(int(parts[2]))
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W\n")
                    else:
                        self.store[key] = value
                        writer.write(b"S\n")
                    await writer.drain()

                elif parts[0] == b"D":
                    key = await reader.readexactly(int(parts[1]))
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W\n")
                    else:
                        writer.write(b"D\n" if self.store.pop(key, None) is not None else b"N\n")
                    await writer.drain()

                else:
                    return
        finally:
            self._sockets.discard(writer)
            writer.close()


class MockDiscovery:
    def __init__(
        self,
        nodes: list[tuple[str, str]],
        replication: int = 1,
    ) -> None:
        self.nodes = nodes
        self.replication = replication
        self.warming_up = False
        self._server: asyncio.Server | None = None
        self.port = 0

    async def start(self) -> "MockDiscovery":
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                try:
                    header = await reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, ConnectionError):
                    return
                parts = header[:-1].split(b" ")

                if parts[0] == b"A":
                    await reader.readexactly(int(parts[1]))
                    writer.write(b"Od\n")
                    await writer.drain()
                elif parts[0] == b"L":
                    if self.warming_up:
                        writer.write(b"B\n")
                        await writer.drain()
                        return
                    frame = b"N %d %d\n" % (len(self.nodes), self.replication)
                    for name, address in self.nodes:
                        name_b, addr_b = name.encode(), address.encode()
                        frame += b"%d %d\n%b%b\n" % (len(name_b), len(addr_b), name_b, addr_b)
                    writer.write(frame)
                    await writer.drain()
                else:
                    return
        finally:
            writer.close()


async def unused_port() -> int:
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return port
