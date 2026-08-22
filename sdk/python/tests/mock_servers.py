"""In-process stand-ins for nanocached-node and nanocached-discovery,
speaking just enough of the wire protocol (``A``, ``G``/``S``/``D``, ``L``)
for the client tests to exercise NanocachedClient end-to-end over real TCP
sockets without the Rust binaries. Mirrors the TypeScript SDK's mocks."""

from __future__ import annotations

import asyncio


class MockNode:
    def __init__(
        self,
        required_secret: bytes | None = None,
        support_tags: bool = False,
        close_on_extended_auth: bool = False,
    ) -> None:
        self.store: dict[bytes, bytes] = {}
        self.required_secret = required_secret
        # Echoed response tags: acknowledge `A ... T` with `OnT\n` and echo tags on
        # that connection's replies. Off by default so the bulk of the
        # suite keeps exercising the legacy untagged path.
        self.support_tags = support_tags
        # Behave like a legacy pre-tag server: an extended `A ... T` is a
        # parse error — close the connection without replying.
        self.close_on_extended_auth = close_on_extended_auth
        self.connection_count = 0
        self.get_count = 0
        self._wrong_node_replies = 0
        self._wrong_node_on_set_replies = 0
        self._wrong_tag_replies = 0
        self._swallowed_gets = 0
        self._malformed_value_replies = 0
        self._unterminated_value_replies = 0
        self.unterminated_value_bytes_sent = 0
        self._stored_to_get_replies = 0
        self._malformed_status_replies = 0
        self._missing_tag_replies = 0
        self._invalid_tag_value_replies = 0
        self._get_delay = 0.0
        self._gets_delay = 0.0
        self._set_delay = 0.0
        self._silent = False
        self.last_set_ttl = 0
        self._server: asyncio.Server | None = None
        self._sockets: set[asyncio.StreamWriter] = set()
        self.port = 0

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def answer_wrong_node_once(self) -> None:
        self._wrong_node_replies += 1

    def answer_wrong_node_on_set_once(self) -> None:
        """Reply ``W`` to the next S specifically (not G/D) — for tests
        that need a node to keep answering GET normally while a later
        SET against it (e.g. a read-repair write-back) fails. Mirrors
        the .NET mock's hook of the same name."""
        self._wrong_node_on_set_replies += 1

    def answer_wrong_tag_once(self) -> None:
        """Queue a one-off reply for the next G request on a tagged
        connection that echoes the WRONG tag (the request's tag + 1) —
        the desync a pre-tag stream misalignment would produce."""
        self._wrong_tag_replies += 1

    def swallow_get_once(self) -> None:
        """Swallow the next G request entirely (no reply) — the
        off-by-one stream desync where every later response answers the
        previous request."""
        self._swallowed_gets += 1

    def answer_malformed_value_once(self) -> None:
        self._malformed_value_replies += 1

    def answer_unterminated_value_once(self) -> None:
        """Reply `V` to the next G, then stream chunks of non-newline
        bytes (until the socket is closed, or a large safety cap is hit)
        instead of ever completing the header — simulating a malicious or
        corrupted server withholding the terminating LF forever."""
        self._unterminated_value_replies += 1

    def answer_stored_to_get_once(self) -> None:
        """Reply `S` to the next G — a well-formed frame of the wrong kind,
        as a desynced (off-by-one) stream would produce."""
        self._stored_to_get_replies += 1

    def answer_malformed_status_once(self) -> None:
        """Reply with a tagged-shaped fixed response (e.g. `S1\\n`) to the
        next G on an untagged connection — the byte after the marker is
        never LF, as a server that tagged a response on an untagged
        connection (or some other desync) would produce."""
        self._malformed_status_replies += 1

    def answer_missing_tag_once(self) -> None:
        """Reply to the next G on a *tagged* connection with the untagged
        fixed form (`N\\n`, no trailing tag field) — a server that forgot
        to tag its reply, or some other desync, would produce this."""
        self._missing_tag_replies += 1

    def answer_invalid_tag_value_once(self) -> None:
        """Reply to the next G on a *tagged* connection with a `V` whose
        tag field is non-numeric (`V 1 abc\\n1`) — exercises
        Connection._parse_tag's invalid-value path (issue #47 audit
        item 5), as opposed to answer_missing_tag_once's missing-field
        desync above."""
        self._invalid_tag_value_replies += 1

    def delay_next_get(self, seconds: float) -> None:
        """Hold the next G's response, so a test can abandon the request
        mid-flight (asyncio.wait_for) and probe cancellation safety."""
        self._get_delay = seconds

    def delay_gets(self, seconds: float) -> None:
        """Hold every future G's response — a slow-but-alive node, for
        hedged-read tests (issue #64)."""
        self._gets_delay = seconds

    def delay_sets(self, seconds: float) -> None:
        """Hold every future S's response — for tests proving a caller
        isn't blocked on a slow replica leg (fire-and-forget replica writes)."""
        self._set_delay = seconds

    def go_silent_after_handshake(self) -> None:
        """Makes this node a half-open server from this point on: it
        still accepts and completes the ``A`` handshake, and still reads
        every request frame off the wire (so the TCP stream stays
        well-formed), but never writes a reply — regression coverage for
        the request timeout (issue #42), mirroring the Go suite's hook
        of the same name."""
        self._silent = True

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
        # Echoed response tags: set when this connection's `A ... T` was acknowledged
        # — its requests then carry a trailing tag the replies must echo.
        tagged = False
        try:
            while True:
                try:
                    header = await reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, ConnectionError):
                    return
                parts = header[:-1].split(b" ")
                # On a tagged connection every request's last header field
                # is its tag, echoed back as each reply's own last field.
                tag_suffix = b" " + parts[-1] if tagged else b""

                if parts[0] == b"A":
                    if len(parts) > 2 and self.close_on_extended_auth:
                        writer.close()
                        return

                    secret = await reader.readexactly(int(parts[1]))
                    accepted = (
                        len(secret) > 0
                        if self.required_secret is None
                        else secret == self.required_secret
                    )
                    tagged = accepted and self.support_tags and len(parts) > 2 and parts[2] == b"T"
                    writer.write(b"OnT\n" if tagged else (b"On\n" if accepted else b"En\n"))
                    await writer.drain()
                    if not accepted:
                        return

                elif parts[0] == b"G":
                    key = await reader.readexactly(int(parts[1]))
                    self.get_count += 1
                    if self._silent:
                        continue
                    if self._get_delay > 0:
                        delay, self._get_delay = self._get_delay, 0.0
                        await asyncio.sleep(delay)
                    if self._gets_delay > 0:
                        await asyncio.sleep(self._gets_delay)
                    if self._swallowed_gets > 0:
                        self._swallowed_gets -= 1
                        continue
                    if self._wrong_tag_replies > 0 and tagged:
                        self._wrong_tag_replies -= 1
                        writer.write(b"N %d\n" % (int(parts[-1]) + 1))
                        await writer.drain()
                        continue
                    if self._stored_to_get_replies > 0:
                        self._stored_to_get_replies -= 1
                        writer.write(b"S" + tag_suffix + b"\n")
                        await writer.drain()
                        continue
                    if self._malformed_status_replies > 0:
                        self._malformed_status_replies -= 1
                        writer.write(b"S1\n")
                        await writer.drain()
                        continue
                    if self._malformed_value_replies > 0:
                        self._malformed_value_replies -= 1
                        writer.write(b"V x\n")
                        await writer.drain()
                        continue
                    if self._unterminated_value_replies > 0:
                        self._unterminated_value_replies -= 1
                        writer.write(b"V")
                        try:
                            while self.unterminated_value_bytes_sent <= 512 * 1024:
                                filler = b"9" * 1024
                                writer.write(filler)
                                await writer.drain()
                                self.unterminated_value_bytes_sent += len(filler)
                        except (ConnectionError, OSError):
                            pass
                        return
                    if self._missing_tag_replies > 0 and tagged:
                        self._missing_tag_replies -= 1
                        writer.write(b"N\n")
                        await writer.drain()
                        continue
                    if self._invalid_tag_value_replies > 0 and tagged:
                        self._invalid_tag_value_replies -= 1
                        writer.write(b"V 1 abc\n1")
                        await writer.drain()
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    elif key in self.store:
                        value = self.store[key]
                        writer.write(b"V %d%b\n%b" % (len(value), tag_suffix, value))
                    else:
                        writer.write(b"N" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] == b"S":
                    key = await reader.readexactly(int(parts[1]))
                    value = await reader.readexactly(int(parts[2]))
                    # parts[3], when present (and not the tag itself), is
                    # the TTL (omitted on the wire means "no expiry", i.e.
                    # 0 — see _encode_set's doc comment in
                    # _connection.py). On a tagged connection the tag sits
                    # after it as the last field.
                    base_field_count = 4 if tagged else 3
                    self.last_set_ttl = int(parts[3]) if len(parts) > base_field_count else 0
                    if self._silent:
                        continue
                    if self._set_delay > 0:
                        await asyncio.sleep(self._set_delay)
                    if self._wrong_node_on_set_replies > 0:
                        self._wrong_node_on_set_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    elif self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    else:
                        self.store[key] = value
                        writer.write(b"S" + tag_suffix + b"\n")
                    await writer.drain()

                elif parts[0] == b"D":
                    key = await reader.readexactly(int(parts[1]))
                    if self._silent:
                        continue
                    if self._wrong_node_replies > 0:
                        self._wrong_node_replies -= 1
                        writer.write(b"W" + tag_suffix + b"\n")
                    else:
                        deleted = self.store.pop(key, None) is not None
                        writer.write((b"D" if deleted else b"N") + tag_suffix + b"\n")
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
        self._unterminated_list_replies = 0
        self.unterminated_list_bytes_sent = 0
        self._server: asyncio.Server | None = None
        self.port = 0

    def answer_unterminated_list_once(self) -> None:
        """Reply `N` to the next L, then stream chunks of non-newline
        bytes (until the socket is closed, or a large safety cap is hit)
        instead of ever completing the header — mirrors MockNode's
        answer_unterminated_value_once on the cache-node path."""
        self._unterminated_list_replies += 1

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
                    # Echoed response tags: echo the tag capability — clients send the
                    # extended A before knowing which kind of server
                    # answered. Discovery's `L` exchange never uses tags
                    # itself, so nothing else here depends on this.
                    writer.write(b"OdT\n" if len(parts) > 2 and parts[2] == b"T" else b"Od\n")
                    await writer.drain()
                elif parts[0] == b"L":
                    if self.warming_up:
                        writer.write(b"B\n")
                        await writer.drain()
                        return
                    if self._unterminated_list_replies > 0:
                        self._unterminated_list_replies -= 1
                        writer.write(b"N")
                        try:
                            while self.unterminated_list_bytes_sent <= 512 * 1024:
                                filler = b"9" * 1024
                                writer.write(filler)
                                await writer.drain()
                                self.unterminated_list_bytes_sent += len(filler)
                        except (ConnectionError, OSError):
                            pass
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
