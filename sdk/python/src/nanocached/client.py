"""The public client. ``host``/``port`` (or a ``seeds`` list) may name
either a single nanocached-node or discovery server(s) fronting a cluster —
``connect()`` finds out from the server's own handshake response
(doc/adr/0007-*.md), so calling code is identical either way.

Cluster mode implements ADR-0011 client-side replication: writes fan out to
each key's top-R owners (the primary's result decides; a dead replica never
fails a write), reads ask the primary and fall over to the next owner only
when the holder is unreachable. Dead connections are redialed lazily on use
(issue #1), and an opt-in keep-alive can hold connections open across the
server's 30s idle timeout.
"""

from __future__ import annotations

import asyncio
import ssl as ssl_module
import sys
import time

from ._connection import Connection
from ._errors import AlreadyClosedError, NanocachedError, WrongNodeError
from ._hashring import HashRing
from ._identify import (
    ClusterTarget,
    NodeTarget,
    connect_and_identify,
    split_host_port,
)

# How long the node list may go without a re-fetch from discovery before
# get/set/delete refreshes it first (checked lazily on use).
_NODE_LIST_STALE_AFTER = 30.0

# The keep-alive ping: the server rejects empty keys, so it needs at least
# one byte; a single NUL stays out of any real key space.
_KEEPALIVE_KEY = b"\x00"
# Half the server's 30s idle timeout; internal (issue #27), mutable only
# so tests can shorten it.
_KEEPALIVE_INTERVAL = 15.0


def _warn(message: str) -> None:
    print(message, file=sys.stderr)


def _to_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


class _Member:
    """One cluster member: its last-known address (for lazy redials) and
    its current connection."""

    def __init__(self, address: str, connection: Connection) -> None:
        self.address = address
        self.connection = connection


class NanocachedClient:
    def __init__(self) -> None:  # use `await NanocachedClient.connect(...)`
        self._closed = False
        self._single: Connection | None = None
        self._single_address: str | None = None
        self._members: dict[str, _Member] = {}
        self._ring: HashRing | None = None
        self._replication = 1
        self._seeds: list[tuple[str, int]] = []
        self._auth_secret: bytes | None = None
        self._tls: bool | ssl_module.SSLContext = False
        self._last_fetch = time.monotonic()
        self._refresh_task: asyncio.Task[None] | None = None
        self._redials: dict[str, asyncio.Task[Connection]] = {}
        self._keepalive_task: asyncio.Task[None] | None = None

    # ── 接続 ──────────────────────────────────────────────────────

    @classmethod
    async def connect(
        cls,
        host: str | None = None,
        port: int | None = None,
        *,
        seeds: list[tuple[str, int]] | None = None,
        auth_secret: str | bytes | None = None,
        tls: bool | ssl_module.SSLContext = False,
    ) -> "NanocachedClient":
        resolved_seeds = seeds if seeds is not None else (
            [(host, port)] if host is not None and port is not None else []
        )
        if not resolved_seeds:
            raise ValueError("nanocached: connect() needs either host/port or a non-empty seeds list")

        client = cls()
        client._seeds = list(resolved_seeds)
        client._auth_secret = _to_bytes(auth_secret) if auth_secret is not None else None
        client._tls = tls

        # Walk the seeds until one yields a working target; a seed that is
        # unreachable, warming up (`B`, ADR-0010), or knows no live nodes
        # is skipped — the next replica may do better.
        last_error: Exception | None = None
        for seed_host, seed_port in client._seeds:
            try:
                identified = await connect_and_identify(
                    seed_host, seed_port, client._auth_secret, client._tls
                )
            except (NanocachedError, OSError) as error:
                last_error = error
                continue

            if isinstance(identified, NodeTarget):
                if len(client._seeds) > 1:
                    _warn(
                        f"nanocached: {seed_host}:{seed_port} is a cache node, so this client is "
                        f"pinned to that single server — the remaining seed(s) will not be used. "
                        f"Point seeds at discovery servers for cluster routing and failover."
                    )
                client._single = Connection(identified.reader, identified.writer)
                client._single_address = f"{seed_host}:{seed_port}"
                client._start_keepalive()
                return client

            if not identified.nodes:
                last_error = NanocachedError(
                    f"nanocached: no live nodes registered with the discovery server at "
                    f"{seed_host}:{seed_port}"
                )
                continue

            try:
                await client._open_cluster(identified)
            except BaseException:
                client._teardown()
                raise
            client._start_keepalive()
            return client

        raise last_error if last_error is not None else NanocachedError(
            "nanocached: could not connect to any seed"
        )

    async def _open_cluster(self, identified: ClusterTarget) -> None:
        for node in identified.nodes:
            node_host, node_port = split_host_port(node.address)
            target = await connect_and_identify(node_host, node_port, self._auth_secret, self._tls)
            if not isinstance(target, NodeTarget):
                raise NanocachedError(
                    f"nanocached: discovery server returned a non-node address: {node.address}"
                )
            self._members[node.name] = _Member(node.address, Connection(target.reader, target.writer))

        self._ring = HashRing([node.name for node in identified.nodes])
        self._replication = identified.replication

    # ── 公開 API ───────────────────────────────────────────────────

    @property
    def replication(self) -> int:
        """How many nodes hold each key (ADR-0011) — 1 against a single node."""
        return self._replication if self._ring is not None else 1

    @property
    def closed(self) -> bool:
        return self._closed

    async def get(self, key: str | bytes) -> bytes | None:
        key_bytes = _to_bytes(key)
        await self._before_operation()
        return await self._with_wrong_node_retry(
            lambda: self._read(key_bytes, lambda connection: connection.get(key_bytes))
        )

    async def set(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int | None = None
    ) -> None:
        if ttl_seconds is not None and (not isinstance(ttl_seconds, int) or ttl_seconds < 0):
            raise ValueError(f"nanocached: ttl_seconds must be a non-negative integer, got {ttl_seconds}")
        key_bytes, value_bytes = _to_bytes(key), _to_bytes(value)
        await self._before_operation()
        await self._with_wrong_node_retry(
            lambda: self._write(
                key_bytes, lambda connection: connection.set(key_bytes, value_bytes, ttl_seconds)
            )
        )

    async def delete(self, key: str | bytes) -> bool:
        """Returns whether the key existed before this call."""
        key_bytes = _to_bytes(key)
        await self._before_operation()
        return await self._with_wrong_node_retry(
            lambda: self._write(key_bytes, lambda connection: connection.delete(key_bytes))
        )

    def close(self) -> None:
        """Idempotent; later get/set/delete raise AlreadyClosedError."""
        if self._closed:
            return
        self._closed = True
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
        self._teardown()

    def _teardown(self) -> None:
        if self._single is not None:
            self._single.close()
        for member in self._members.values():
            member.connection.close()

    # ── ルーティングと複製 ─────────────────────────────────────────

    async def _before_operation(self) -> None:
        if self._closed:
            raise AlreadyClosedError()
        await self._maybe_refresh()

    async def _with_wrong_node_retry(self, operation):
        try:
            return await operation()
        except (WrongNodeError, ConnectionError, OSError):
            # Connection-level failures retry the same way `W` does: the
            # usual cause is a node death that discovery has since noticed,
            # so a forced refresh re-ranks the key onto survivors. The
            # retry window for a dead primary is therefore bounded by
            # discovery's liveness timeout. A second failure after a fresh
            # refresh propagates.
            if self._ring is None:
                raise
            await self._maybe_refresh(force=True)
            return await operation()

    def _owner_names(self, key: bytes) -> list[str]:
        assert self._ring is not None
        return self._ring.owners(key, self._replication)

    async def _read(self, key: bytes, op):
        if self._ring is None:
            return await op(await self._single_connection())

        # Owners in rank order; fall through only on connection-level
        # failure — a replica hedges against a dead holder, not a miss.
        last_error: Exception | None = None
        for name in self._owner_names(key):
            try:
                connection = await self._member_connection(name)
            except (ConnectionError, OSError, NanocachedError) as error:
                if isinstance(error, WrongNodeError):
                    raise
                last_error = error
                continue
            try:
                return await op(connection)
            except WrongNodeError:
                raise
            except (NanocachedError, ConnectionError, OSError) as error:
                # Fall through to the next owner on anything but a
                # WrongNode answer (issue #8) — matching the TypeScript
                # SDK's semantics.
                last_error = error
        raise last_error if last_error is not None else ConnectionError(
            "nanocached: no owner is reachable for this key"
        )

    async def _write(self, key: bytes, op):
        if self._ring is None:
            return await op(await self._single_connection())

        names = self._owner_names(key)
        if not names:
            raise ConnectionError("nanocached: no owner is reachable for this key")
        primary, replicas = names[0], names[1:]

        async def replica_write(name: str) -> None:
            try:
                await op(await self._member_connection(name))
            except Exception:
                # Swallowed by design (ADR-0011): a dead or disagreeing
                # replica leaves the key under-replicated until the next
                # node-list refresh, never fails the write.
                pass

        replica_tasks = [asyncio.ensure_future(replica_write(name)) for name in replicas]
        try:
            return await op(await self._member_connection(primary))
        finally:
            if replica_tasks:
                await asyncio.gather(*replica_tasks, return_exceptions=True)

    # ── 遅延再接続(issue #1)────────────────────────────────────────

    async def _single_connection(self) -> Connection:
        assert self._single is not None and self._single_address is not None
        if not self._single.closed:
            return self._single
        return await self._redial("", self._single_address)

    async def _member_connection(self, name: str) -> Connection:
        member = self._members.get(name)
        if member is None:
            # Connection-classified (issue #8): the usual cause is a
            # refresh racing this operation, which the retry layer heals.
            raise ConnectionError(f"nanocached: {name} has no open connection")
        if not member.connection.closed:
            return member.connection
        return await self._redial(name, member.address)

    async def _redial(self, slot: str, address: str) -> Connection:
        """Concurrent requests finding the same dead connection share one
        dial instead of each opening a socket. The shared task itself
        adopts the fresh connection into client state (issue #10): with
        adoption in the awaiting caller, a cancelled caller (e.g.
        asyncio.wait_for around a client call) would abandon the
        shield-protected task's connection unreferenced — a leaked
        socket."""
        in_flight = self._redials.get(slot)
        if in_flight is not None:
            return await asyncio.shield(in_flight)

        task = asyncio.ensure_future(self._open_and_adopt(slot, address))
        self._redials[slot] = task
        try:
            return await asyncio.shield(task)
        finally:
            if self._redials.get(slot) is task:
                del self._redials[slot]

    async def _open_and_adopt(self, slot: str, address: str) -> Connection:
        connection = await self._open_node_connection(address)

        if slot == "":
            if self._single is not None and self._single.closed:
                self._single = connection
                return connection
            if self._single is connection or (self._single is not None and not self._single.closed):
                if self._single is not connection:
                    connection.close()
                    return self._single
            return connection

        current = self._members.get(slot)
        if current is None:
            connection.close()
            raise ConnectionError(f"nanocached: {slot} left the cluster while reconnecting")
        if current.connection.closed:
            current.connection = connection
            return connection
        if current.connection is not connection:
            connection.close()
        return current.connection

    async def _open_node_connection(self, address: str) -> Connection:
        node_host, node_port = split_host_port(address)
        identified = await connect_and_identify(node_host, node_port, self._auth_secret, self._tls)
        if not isinstance(identified, NodeTarget):
            raise NanocachedError(f"nanocached: {address} no longer identifies as a cache node")
        if self._closed:
            identified.writer.close()
            raise AlreadyClosedError()
        return Connection(identified.reader, identified.writer)

    # ── ノードリスト更新 ────────────────────────────────────────────

    async def _maybe_refresh(self, force: bool = False) -> None:
        if self._ring is None:
            return
        if not force and time.monotonic() - self._last_fetch < _NODE_LIST_STALE_AFTER:
            return

        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.ensure_future(self._refresh_node_list())
        await asyncio.shield(self._refresh_task)

    async def _refresh_node_list(self) -> None:
        cluster = await self._fetch_node_list()
        self._last_fetch = time.monotonic()
        if cluster is None:
            return

        by_name = {node.name: node for node in cluster.nodes}

        for name in list(self._members):
            if name not in by_name:
                self._members[name].connection.close()
                del self._members[name]

        for node in cluster.nodes:
            existing = self._members.get(node.name)
            if existing is not None:
                existing.address = node.address
                continue
            try:
                node_host, node_port = split_host_port(node.address)
                target = await connect_and_identify(node_host, node_port, self._auth_secret, self._tls)
                if not isinstance(target, NodeTarget):
                    _warn(f"nanocached: discovery returned a non-node address: {node.address}, skipping")
                    continue
                if self._closed:
                    # close() ran while we were dialing (issue #10):
                    # installing this socket now would leak it.
                    target.writer.close()
                    return
                self._members[node.name] = _Member(node.address, Connection(target.reader, target.writer))
            except (NanocachedError, OSError) as error:
                _warn(f"nanocached: could not connect to new node {node.address}, will retry: {error}")

        if self._closed:
            self._teardown()
            return

        self._ring = HashRing(list(self._members))
        self._replication = cluster.replication

    async def _fetch_node_list(self) -> ClusterTarget | None:
        """Walks every seed (ADR-0010); None means keep the last-known list."""
        for seed_host, seed_port in self._seeds:
            try:
                identified = await connect_and_identify(
                    seed_host, seed_port, self._auth_secret, self._tls
                )
            except (NanocachedError, OSError) as error:
                _warn(f"nanocached: could not refresh the node list from {seed_host}:{seed_port}: {error}")
                continue
            if isinstance(identified, NodeTarget):
                identified.writer.close()
                _warn(f"nanocached: {seed_host}:{seed_port} no longer identifies as a discovery server")
                continue
            if not identified.nodes:
                _warn(f"nanocached: discovery at {seed_host}:{seed_port} returned no live nodes, skipping")
                continue
            return identified
        _warn("nanocached: no discovery seed could provide a node list, keeping the last-known list")
        return None

    # ── keep-alive ─────────────────────────────────────────────────

    def _start_keepalive(self) -> None:
        # Always on, with an internal interval (issue #27): half the
        # server's 30s idle timeout, so it never severs a healthy client.
        # Module-level only so tests can shorten it.
        interval = _KEEPALIVE_INTERVAL

        async def ping_loop() -> None:
            while not self._closed:
                await asyncio.sleep(interval)
                connections = (
                    [self._single] if self._single is not None
                    else [member.connection for member in self._members.values()]
                )
                for connection in connections:
                    if connection is None or connection.closed:
                        continue  # dead connections stay lazy, redialed on use
                    if connection.idle_seconds() < interval:
                        continue  # real traffic already reset the server's timer
                    try:
                        # Any parseable reply proves liveness — `N`, or `W`
                        # from a non-owner — and resets the idle timer.
                        await connection.get(_KEEPALIVE_KEY)
                    except Exception:
                        pass

        self._keepalive_task = asyncio.ensure_future(ping_loop())
