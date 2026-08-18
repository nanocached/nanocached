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

from ._errors import DiscoveryBusyError, NanocachedError

# Sent as the `A` secret when the caller didn't configure one: a server
# with no secret accepts any non-empty secret, and one that requires a
# real secret correctly rejects this placeholder.
_NO_SECRET_PLACEHOLDER = b"\x00"

# Bound on dial + handshake, matching the Go and Java SDKs. Without it, a
# node whose IP has been reclaimed (a stopped container, a dead cloud
# instance) blackholes the TCP connect and a caller hangs for the kernel's
# own timeout — minutes — instead of failing over.
CONNECT_DEADLINE = 5.0


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


@dataclass
class ClusterTarget:
    nodes: list[DiscoveredNode]
    replication: int


def split_host_port(address: str) -> tuple[str, int]:
    host, separator, port = address.rpartition(":")
    if not separator or not port.isdigit():
        raise NanocachedError(f"nanocached: invalid node address from discovery server: {address}")
    return host, int(port)


async def connect_and_identify(
    host: str,
    port: int,
    auth_secret: bytes | None,
    ssl_context: ssl_module.SSLContext | None,
) -> NodeTarget | ClusterTarget:
    try:
        return await asyncio.wait_for(
            _connect_and_identify(host, port, auth_secret, ssl_context), CONNECT_DEADLINE
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
) -> NodeTarget | ClusterTarget:
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)

    try:
        secret = auth_secret if auth_secret is not None else _NO_SECRET_PLACEHOLDER
        writer.write(b"A %d\n%b" % (len(secret), secret))
        await writer.drain()

        ack = await reader.readexactly(3)
        if ack[2:3] != b"\n" or ack[0:1] not in (b"O", b"E") or ack[1:2] not in (b"n", b"d"):
            raise NanocachedError("nanocached: unexpected response to A")

        if ack[0:1] == b"E":
            if auth_secret is None:
                raise NanocachedError(
                    f"nanocached: {host}:{port} requires authentication, but no auth_secret was given"
                )
            raise NanocachedError("nanocached: authentication failed")

        if ack[1:2] == b"n":
            return NodeTarget(reader=reader, writer=writer)

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
    header = await reader.readuntil(b"\n")

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
    count, replication = int(fields[0]), int(fields[1])
    if replication < 1:
        raise NanocachedError("nanocached: invalid replication factor in discovery response")

    nodes: list[DiscoveredNode] = []
    for _ in range(count):
        entry_header = await reader.readuntil(b"\n")
        lengths = entry_header[:-1].split(b" ")
        if len(lengths) != 2:
            raise NanocachedError("nanocached: invalid node entry header in discovery response")
        name_length, addr_length = int(lengths[0]), int(lengths[1])

        body = await reader.readexactly(name_length + addr_length + 1)  # +1: trailing '\n'
        if body[-1:] != b"\n":
            raise NanocachedError("nanocached: malformed node entry in discovery response")
        nodes.append(
            DiscoveredNode(
                name=body[:name_length].decode("utf-8"),
                address=body[name_length:-1].decode("utf-8"),
            )
        )

    return ClusterTarget(nodes=nodes, replication=replication)
