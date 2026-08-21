"""Connect-and-identify: dials ``host:port``, authenticates, and figures
out from the server's own ``A`` response whether it reached a cache node
(``On``) or a discovery server (``Od``) — the caller never says which it
expects (doc/adr/0007-*.md). A node's streams are handed back live, ready
for ``G``/``S``/``D``; a discovery connection is used once for ``L`` and
closed, returning the name/address list and the cluster's replication
factor R (doc/adr/0009, 0010, 0011)."""

from __future__ import annotations

import asyncio
import ssl as ssl_module
from dataclasses import dataclass

from ._errors import AuthenticationError, DiscoveryBusyError, NanocachedError

# Sent as the `A` secret when the caller didn't configure one: a server
# with no secret accepts any non-empty secret, and one that requires a
# real secret correctly rejects this placeholder.
_NO_SECRET_PLACEHOLDER = b"\x00"

# Bound on dial + handshake, matching the Go and Java SDKs. Without it, a
# node whose IP has been reclaimed (a stopped container, a dead cloud
# instance) blackholes the TCP connect and a caller hangs for the kernel's
# own timeout — minutes — instead of failing over.
CONNECT_DEADLINE = 5.0

# Bound a discovery `N` response before allocation, mirroring
# `_MAX_VALUE_LENGTH` on the `V` path: a malicious or MITM'd discovery
# server must not be able to make the client buffer arbitrary memory, and
# a negative length must fail cleanly rather than silently mis-slice the
# body via Python's negative-index semantics.
_MAX_NODE_COUNT = 1 << 16
_MAX_NODE_FIELD_LENGTH = 64 * 1024

# Aggregate cap on a whole `N ...` node-list response, independent of the
# per-field caps above: bounds a malicious discovery server's memory
# pressure while still fitting a full 65536-node registry. This same
# constant is being added to all six SDKs.
_MAX_NODE_LIST_RESPONSE_LENGTH = 16 * 1024 * 1024


@dataclass(frozen=True)
class DiscoveredNode:
    """A node's hash-ring identity (a random per-process UUID) and its
    network address — two different things since doc/adr/0009-*.md."""

    name: str
    address: str


@dataclass
class NodeTarget:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    # ADR-0019: whether the node accepted the extended `A ... T` — when
    # true, this connection's `G`/`S`/`D` traffic must carry tags and its
    # responses echo them; false means an older node answered the plain-`A`
    # fallback.
    tagged: bool = False


@dataclass
class ClusterTarget:
    nodes: list[DiscoveredNode]
    replication: int


class _LegacyServerSignal(Exception):
    """Internal-only: raised when the extended `A ... T` auth attempt hit a
    connection-closed-shaped failure while reading the identify response —
    the signal a pre-ADR-0019 server gives by treating the extended form as
    a parse error and closing without replying. Caught by
    connect_and_identify to trigger the transparent untagged fallback
    (doc/adr/0019-*.md); never escapes this module."""


def split_host_port(address: str) -> tuple[str, int]:
    host, separator, port = address.rpartition(":")
    # isascii() matters: str.isdigit() also accepts characters like "²"
    # that int() rejects with a raw ValueError.
    if not separator or not port.isascii() or not port.isdigit():
        raise NanocachedError(f"nanocached: invalid node address from discovery server: {address}")
    port_number = int(port)
    if port_number > 65535:
        raise NanocachedError(f"nanocached: invalid node address from discovery server: {address}")
    return host, port_number


async def connect_and_identify(
    host: str,
    port: int,
    auth_secret: bytes | None,
    ssl_context: ssl_module.SSLContext | None,
) -> NodeTarget | ClusterTarget:
    try:
        return await _connect_and_identify_with_deadline(host, port, auth_secret, ssl_context, request_tags=True)
    except _LegacyServerSignal:
        # ADR-0019 transparent fallback: a pre-0019 server treats the
        # extended `A ... T` as a parse error and closes without replying
        # — redial once with the plain form and run the connection
        # untagged (the pre-0019 behavior, desync window included).
        return await _connect_and_identify_with_deadline(host, port, auth_secret, ssl_context, request_tags=False)


async def _connect_and_identify_with_deadline(
    host: str,
    port: int,
    auth_secret: bytes | None,
    ssl_context: ssl_module.SSLContext | None,
    request_tags: bool,
) -> NodeTarget | ClusterTarget:
    try:
        return await asyncio.wait_for(
            _connect_and_identify(host, port, auth_secret, ssl_context, request_tags), CONNECT_DEADLINE
        )
    except TimeoutError as error:
        raise ConnectionError(
            f"nanocached: connecting to {host}:{port} timed out after {CONNECT_DEADLINE}s"
        ) from error


async def _connect_and_identify(
    host: str,
    port: int,
    auth_secret: bytes | None,
    ssl_context: ssl_module.SSLContext | None,
    request_tags: bool,
) -> NodeTarget | ClusterTarget:
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)

    try:
        secret = auth_secret if auth_secret is not None else _NO_SECRET_PLACEHOLDER
        tag_field = b" T" if request_tags else b""

        # ADR-0019: read 3 bytes first. `byte[2] == '\n'` is the
        # traditional fixed-width ack (`On\n`/`En\n`/`Od\n`/`Ed\n`),
        # tagging disabled. `byte[2] == 'T'` means one more byte follows,
        # which must be `\n` (`OnT\n`/`EnT\n`/`OdT\n`/`EdT\n`), tagging
        # enabled. Anything else is a protocol error, same as before this
        # field existed.
        #
        # The write is inside this guard too, not just the ack read: a
        # pre-0019 server can slam the door (RST/broken pipe) as soon as it
        # sees the extended `A ... T` frame, before ever reading far enough
        # to reply — the door-slam is a reaction to what we sent, not just
        # to us waiting for an answer. Rust/Go/TS all classify a write-time
        # RST/EOF the same way as a read-time one for exactly this reason
        # (TS's identify.ts wraps its `socket.write(authFrame)` and the
        # ack read in the same try/catch before classifying). Scoping the
        # guard to only the read, as before, meant a write-time door-slam
        # propagated as a raw connection error instead of triggering the
        # untagged retry.
        try:
            writer.write(b"A %d%b\n%b" % (len(secret), tag_field, secret))
            await writer.drain()
            ack = await reader.readexactly(3)
            if ack[0:1] not in (b"O", b"E") or ack[1:2] not in (b"n", b"d"):
                raise NanocachedError("nanocached: unexpected response to A")

            if ack[2:3] == b"\n":
                tagged = False
            elif ack[2:3] == b"T":
                fourth = await reader.readexactly(1)
                if fourth != b"\n":
                    raise NanocachedError("nanocached: unexpected response to A")
                tagged = True
            else:
                raise NanocachedError("nanocached: unexpected response to A")
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError) as error:
            # Only a request_tags=True attempt can fall back further — the
            # transparent fallback itself must fail like any other
            # connection error. A timeout is not this: the server kept the
            # connection open, it just didn't answer (handled by the
            # asyncio.wait_for wrapper instead), and a malformed-but-present
            # reply is not this either — both are real protocol errors, not
            # a legacy server's silent door-slam.
            if request_tags:
                raise _LegacyServerSignal() from error
            if isinstance(error, asyncio.IncompleteReadError):
                # An EOFError, not an OSError — wrap it so it stays inside
                # client.py's _SWALLOWABLE_ERRORS contract.
                raise NanocachedError(
                    "nanocached: connection closed during the auth handshake"
                ) from error
            raise

        if ack[0:1] == b"E":
            if auth_secret is None:
                raise AuthenticationError(
                    f"nanocached: {host}:{port} requires authentication, but no auth_secret was given"
                )
            raise AuthenticationError("nanocached: authentication failed")

        if ack[1:2] == b"n":
            return NodeTarget(reader=reader, writer=writer, tagged=tagged)

        # A discovery server: one-shot `L`, then this connection is done.
        writer.write(b"L\n")
        await writer.drain()
        cluster = await _read_node_list(reader)
        writer.close()
        return cluster
    except BaseException:
        writer.close()
        raise


async def _read_node_list(reader: asyncio.StreamReader) -> ClusterTarget:
    try:
        header = await reader.readuntil(b"\n")
    except asyncio.LimitOverrunError as error:
        # No `\n` within the stream's internal buffer limit (64 KiB) — the
        # connection is desynced mid-frame. LimitOverrunError is a
        # ValueError subclass, neither NanocachedError nor OSError, so
        # left unwrapped it would break client.py's `except
        # (NanocachedError, OSError)` contracts ("try next address
        # silently", "refresh swallows failures") and escape raw to
        # callers instead (mirrors the TimeoutError wrapping above).
        raise NanocachedError(
            "nanocached: invalid node-list header in discovery response (missing header terminator)"
        ) from error
    except asyncio.IncompleteReadError as error:
        # EOF before the terminator — an EOFError, not an OSError, so the
        # same escape hatch applies.
        raise NanocachedError(
            "nanocached: truncated node-list header in discovery response"
        ) from error

    if header.startswith(b"B"):
        raise DiscoveryBusyError()
    if not header.startswith(b"N "):
        raise NanocachedError(
            f"nanocached: unexpected response from discovery server: {header[:-1]!r}"
        )

    # `N <count> <r>\n` (ADR-0011) — the replication factor rides along.
    fields = header[2:-1].split(b" ")
    if len(fields) != 2:
        raise NanocachedError("nanocached: invalid node-list header in discovery response")
    try:
        count, replication = int(fields[0]), int(fields[1])
    except ValueError as error:
        # Non-numeric fields must surface as NanocachedError, not a raw
        # ValueError — client.py's _SWALLOWABLE_ERRORS contract ("try next
        # address silently", "refresh swallows failures") only covers
        # NanocachedError/ConnectionError/OSError.
        raise NanocachedError("nanocached: invalid node-list header in discovery response") from error
    if replication < 1:
        raise NanocachedError("nanocached: invalid replication factor in discovery response")
    if count < 0 or count > _MAX_NODE_COUNT:
        raise NanocachedError("nanocached: invalid node count in discovery response")

    nodes: list[DiscoveredNode] = []
    total_bytes = len(header)
    for _ in range(count):
        try:
            entry_header = await reader.readuntil(b"\n")
        except asyncio.LimitOverrunError as error:
            raise NanocachedError(
                "nanocached: invalid node entry header in discovery response (missing header terminator)"
            ) from error
        except asyncio.IncompleteReadError as error:
            raise NanocachedError(
                "nanocached: truncated node entry header in discovery response"
            ) from error
        total_bytes += len(entry_header)
        lengths = entry_header[:-1].split(b" ")
        if len(lengths) != 2:
            raise NanocachedError("nanocached: invalid node entry header in discovery response")
        try:
            name_length, addr_length = int(lengths[0]), int(lengths[1])
        except ValueError as error:
            raise NanocachedError(
                "nanocached: invalid node entry header in discovery response"
            ) from error
        if (
            name_length < 0
            or addr_length < 0
            or name_length > _MAX_NODE_FIELD_LENGTH
            or addr_length > _MAX_NODE_FIELD_LENGTH
        ):
            raise NanocachedError("nanocached: invalid node entry lengths in discovery response")

        total_bytes += name_length + addr_length + 1
        if total_bytes > _MAX_NODE_LIST_RESPONSE_LENGTH:
            raise NanocachedError("nanocached: discovery response exceeds maximum size")

        try:
            body = await reader.readexactly(name_length + addr_length + 1)  # +1: trailing '\n'
        except asyncio.IncompleteReadError as error:
            # EOF mid-entry (discovery restarted, connection dropped).
            # IncompleteReadError is an EOFError, not an OSError, so left
            # unwrapped it would escape _SWALLOWABLE_ERRORS.
            raise NanocachedError(
                "nanocached: truncated node entry in discovery response"
            ) from error
        if body[-1:] != b"\n":
            raise NanocachedError("nanocached: malformed node entry in discovery response")
        try:
            nodes.append(
                DiscoveredNode(
                    name=body[:name_length].decode("utf-8"),
                    address=body[name_length:-1].decode("utf-8"),
                )
            )
        except UnicodeDecodeError as error:
            raise NanocachedError(
                "nanocached: malformed node entry in discovery response"
            ) from error

    return ClusterTarget(nodes=nodes, replication=replication)
