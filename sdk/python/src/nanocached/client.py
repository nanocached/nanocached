"""The public client. An ``addresses`` list may name either a single
nanocached-node or discovery server(s) fronting a cluster — ``connect()``
finds out from the server's own handshake response (doc/adr/0007-*.md), so
calling code is identical either way.

Cluster mode implements ADR-0011 client-side replication: writes fan out to
each key's top-R owners (the primary's result decides; a dead replica never
fails a write), reads ask the primary and fall over to the next owner only
when the holder is unreachable. Dead connections are redialed lazily on use
(issue #1), and an opt-in keep-alive can hold connections open across the
server's 60s idle timeout.
"""

from __future__ import annotations

import asyncio
import os
import ssl as ssl_module
import sys
import threading
import time
from collections.abc import Sequence

from ._compression import compress_value, decompress_value
from ._connection import Connection
from ._errors import AlreadyClosedError, NanocachedError, WrongNodeError
from ._hashring import HashRing
from ._identify import (
    ClusterTarget,
    NodeTarget,
    connect_and_identify,
    split_host_port,
)

# doc/adr/0013-*.md: values shorter than this (bytes) are never
# compressed — the per-value overhead of attempting it outweighs the
# savings. Only meaningful when compress=True.
_DEFAULT_COMPRESSION_THRESHOLD = 256

# How long the node list may go without a re-fetch from discovery before
# get/set/delete refreshes it first (checked lazily on use).
_NODE_LIST_STALE_AFTER = 30.0

# The keep-alive ping: the server rejects empty keys, so it needs at least
# one byte; a single NUL stays out of any real key space.
_KEEPALIVE_KEY = b"\x00"
# Half the server's 60s idle timeout; internal (issue #27), mutable only
# so tests can shorten it.
_KEEPALIVE_INTERVAL = 30.0

# doc/adr/0014-*.md: bounds how many replica writes a single client may
# have running in the background at once when fire_and_forget_replicas is
# enabled — once the cap is reached, further replica legs fall back to
# running synchronously, the same as with the option off. Mutable only so
# tests can shrink it, mirroring _KEEPALIVE_INTERVAL.
_MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES = 32


def _warn(message: str) -> None:
    print(message, file=sys.stderr)


def _to_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


def _build_ssl_context(tls: bool, ca: str | os.PathLike | None) -> ssl_module.SSLContext | None:
    """``ca`` is meaningful only when ``tls`` is true — silently ignored
    otherwise. An unreadable/unparseable CA file is a connect-time error
    (raised synchronously, before any socket is opened)."""
    if not tls:
        return None
    if ca is not None:
        return ssl_module.create_default_context(cafile=ca)
    return ssl_module.create_default_context()


# Tracks, per connect() target ("host:port", not per client instance — see
# NanocachedClient._target_key), how many live sockets are still open for
# it. Purely a programming-error guard: catches "connect() called again for
# the same target before the previous one was ever released" without
# affecting behavior — connecting again still works, this only warns.
# Mirrors the TypeScript SDK's module-level `openTargets` map
# (sdk/typescript/src/client.ts); guarded by a lock since, unlike a single
# JS event loop, nothing prevents this module from being used from more
# than one thread.
_open_targets: dict[str, int] = {}
_open_targets_lock = threading.Lock()


def _increment_open_target(key: str) -> None:
    with _open_targets_lock:
        _open_targets[key] = _open_targets.get(key, 0) + 1


def _decrement_open_target(key: str) -> None:
    with _open_targets_lock:
        remaining = _open_targets.get(key, 1) - 1
        if remaining <= 0:
            _open_targets.pop(key, None)
        else:
            _open_targets[key] = remaining


def _has_open_target(key: str) -> bool:
    with _open_targets_lock:
        return _open_targets.get(key, 0) > 0


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
        self._addresses: list[tuple[str, int]] = []
        self._auth_secret: bytes | None = None
        self._ssl_context: ssl_module.SSLContext | None = None
        self._compress: bool = False
        self._compression_threshold: int = _DEFAULT_COMPRESSION_THRESHOLD
        self._fire_and_forget_replicas: bool = False
        self._background_replica_writes: set[asyncio.Task[None]] = set()
        self._target_key: str | None = None
        self._last_fetch = time.monotonic()
        self._refresh_task: asyncio.Task[None] | None = None
        self._redials: dict[str, asyncio.Task[Connection]] = {}
        self._keepalive_task: asyncio.Task[None] | None = None

    # ── 接続 ──────────────────────────────────────────────────────

    @classmethod
    async def connect(
        cls,
        addresses: Sequence[tuple[str, int]],
        *,
        auth_secret: str | None = None,
        tls: bool = False,
        ca: str | os.PathLike | None = None,
        compress: bool = False,
        compression_threshold: int = _DEFAULT_COMPRESSION_THRESHOLD,
        fire_and_forget_replicas: bool = False,
    ) -> "NanocachedClient":
        if not addresses:
            raise ValueError("nanocached: connect() needs a non-empty addresses list")

        client = cls()
        client._addresses = list(addresses)
        client._auth_secret = auth_secret.encode("utf-8") if auth_secret is not None else None
        client._ssl_context = _build_ssl_context(tls, ca)
        client._compress = compress
        client._compression_threshold = compression_threshold
        client._fire_and_forget_replicas = fire_and_forget_replicas

        # Walk the addresses until one yields a working target; an address
        # that is unreachable, warming up (`B`, ADR-0010), or knows no live
        # nodes is skipped — the next replica may do better.
        last_error: Exception | None = None
        for address_host, address_port in client._addresses:
            key = f"{address_host}:{address_port}"
            # Only meaningful for a single explicit target: with a
            # multi-address config, another client instance legitimately
            # holding connections to the same address makes this heuristic
            # false-positive (issue #12).
            if len(client._addresses) == 1 and _has_open_target(key):
                _warn(
                    f"nanocached: connect() called for {key} while a previous connection to it "
                    f"is still open — was close() forgotten?"
                )

            try:
                identified = await connect_and_identify(
                    address_host, address_port, client._auth_secret, client._ssl_context
                )
            except (NanocachedError, OSError) as error:
                last_error = error
                continue

            if isinstance(identified, NodeTarget):
                if len(client._addresses) > 1:
                    _warn(
                        f"nanocached: {key} is a cache node, so this client is pinned to that "
                        f"single server — the {len(client._addresses) - 1} remaining address(es) "
                        f"will not be used. Point addresses at discovery servers for cluster "
                        f"routing and failover."
                    )
                client._target_key = key
                client._single = client._new_connection(identified.reader, identified.writer)
                client._single_address = key
                client._start_keepalive()
                return client

            if not identified.nodes:
                last_error = NanocachedError(
                    f"nanocached: no live nodes registered with the discovery server at {key}"
                )
                continue

            client._target_key = key
            try:
                await client._open_cluster(identified)
            except BaseException:
                client._teardown()
                raise
            client._start_keepalive()
            return client

        raise last_error if last_error is not None else NanocachedError(
            "nanocached: could not connect to any address"
        )

    def _new_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Connection:
        """Wraps a freshly identified node socket, tracking it against this
        client's target key (§7 ③) until it closes — whether via close(),
        redial replacement, or refresh reconciliation."""
        assert self._target_key is not None
        key = self._target_key
        _increment_open_target(key)
        return Connection(reader, writer, on_close=lambda: _decrement_open_target(key))

    async def _open_cluster(self, identified: ClusterTarget) -> None:
        for node in identified.nodes:
            node_host, node_port = split_host_port(node.address)
            target = await connect_and_identify(node_host, node_port, self._auth_secret, self._ssl_context)
            if not isinstance(target, NodeTarget):
                raise NanocachedError(
                    f"nanocached: discovery server returned a non-node address: {node.address}"
                )
            self._members[node.name] = _Member(node.address, self._new_connection(target.reader, target.writer))

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

    async def get_bytes(self, key: str | bytes) -> bytes | None:
        """The raw companion to get(): no UTF-8 decoding, so it never
        raises on a value that isn't valid UTF-8. Transparently
        decompresses when ``compress`` is enabled (doc/adr/0013-*.md)."""
        key_bytes = _to_bytes(key)
        await self._before_operation()
        value = await self._with_wrong_node_retry(
            lambda: self._read(key_bytes, lambda connection: connection.get(key_bytes))
        )
        if value is None or not self._compress:
            return value
        return decompress_value(value)

    async def get(self, key: str | bytes) -> str | None:
        """Strict UTF-8 decode of the stored value (bytes.decode()) — a
        value that is not valid UTF-8 raises UnicodeDecodeError rather than
        silently replacing it. Use get_bytes() for the raw bytes."""
        value = await self.get_bytes(key)
        return value.decode() if value is not None else None

    async def set(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> None:
        """``ttl_seconds`` is whole seconds; 0 (the default) means no
        expiry. Negative values are rejected eagerly, before any I/O.
        Transparently compresses values at or above
        ``compression_threshold`` when ``compress`` is enabled
        (doc/adr/0013-*.md)."""
        if not isinstance(ttl_seconds, int) or ttl_seconds < 0:
            raise ValueError(f"nanocached: ttl_seconds must be a non-negative integer, got {ttl_seconds}")
        key_bytes, value_bytes = _to_bytes(key), _to_bytes(value)
        if self._compress:
            value_bytes = compress_value(value_bytes, self._compression_threshold)
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
        """Idempotent; later get/set/delete raise AlreadyClosedError. A
        second close() is usually a sign the caller lost track of this
        instance's lifecycle, so — unlike the first — it warns."""
        if self._closed:
            _warn("nanocached: close() called again on an already-closed client")
            return
        self._closed = True
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()

        # doc/adr/0014-*.md: give background replica writes a chance to
        # finish before their connections are torn out from under them —
        # close() stays synchronous, so the teardown itself is deferred
        # via a scheduled coroutine rather than awaited here.
        if self._background_replica_writes:
            pending = list(self._background_replica_writes)

            async def _drain_then_teardown() -> None:
                await asyncio.gather(*pending, return_exceptions=True)
                self._teardown()

            asyncio.ensure_future(_drain_then_teardown())
            return

        self._teardown()

    async def __aenter__(self) -> "NanocachedClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.close()

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

        replica_tasks = []
        for name in replicas:
            # doc/adr/0014-*.md: with fire_and_forget_replicas, up to
            # _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES legs run in the
            # background instead of being waited for below — past that
            # cap, further legs fall back to the synchronous path exactly
            # as with the option off.
            if (
                self._fire_and_forget_replicas
                and len(self._background_replica_writes) < _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES
            ):
                task = asyncio.ensure_future(replica_write(name))
                self._background_replica_writes.add(task)
                task.add_done_callback(self._background_replica_writes.discard)
                continue
            replica_tasks.append(asyncio.ensure_future(replica_write(name)))

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
        identified = await connect_and_identify(node_host, node_port, self._auth_secret, self._ssl_context)
        if not isinstance(identified, NodeTarget):
            raise NanocachedError(f"nanocached: {address} no longer identifies as a cache node")
        if self._closed:
            identified.writer.close()
            raise AlreadyClosedError()
        return self._new_connection(identified.reader, identified.writer)

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
                target = await connect_and_identify(node_host, node_port, self._auth_secret, self._ssl_context)
                if not isinstance(target, NodeTarget):
                    # Refresh warnings are silent by design (§7 ②) —
                    # behavior is unchanged, this node is just skipped.
                    continue
                if self._closed:
                    # close() ran while we were dialing (issue #10):
                    # installing this socket now would leak it.
                    target.writer.close()
                    return
                self._members[node.name] = _Member(node.address, self._new_connection(target.reader, target.writer))
            except (NanocachedError, OSError):
                pass

        if self._closed:
            self._teardown()
            return

        self._ring = HashRing(list(self._members))
        self._replication = cluster.replication

    async def _fetch_node_list(self) -> ClusterTarget | None:
        """Walks every address (ADR-0010); None means keep the last-known
        list. Failures here are silent by design (§7 ②) — behavior is
        unchanged either way, it just happens without a warning."""
        for address_host, address_port in self._addresses:
            try:
                identified = await connect_and_identify(
                    address_host, address_port, self._auth_secret, self._ssl_context
                )
            except (NanocachedError, OSError):
                continue
            if isinstance(identified, NodeTarget):
                identified.writer.close()
                continue
            if not identified.nodes:
                continue
            return identified
        return None

    # ── keep-alive ─────────────────────────────────────────────────

    def _start_keepalive(self) -> None:
        # Always on, with an internal interval (issue #27): half the
        # server's 60s idle timeout, so it never severs a healthy client.
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
