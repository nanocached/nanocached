"""The public client. An ``addresses`` list may name either a single
nanocached-node or discovery server(s) fronting a cluster — ``connect()``
finds out from the server's own handshake response (the server type in the auth response), so
calling code is identical either way.

Cluster mode implements client-side replication client-side replication: writes fan out to
each key's top-R owners (the primary's result decides; a dead replica never
fails a write), reads ask the primary and fall over to the next owner only
when the holder is unreachable. Dead connections are redialed lazily on use
(issue #1), and an opt-in keep-alive can hold connections open across the
server's 60s idle timeout.

Namespaces (issue #105): every get/get_bytes/set/delete below implicitly
addresses the default (empty) namespace, which is byte-for-byte the
pre-namespace protocol — namespace() returns a Namespace handle for
addressing any other one, sharing this client's connections and routing
without duplicating its networking (see namespace()'s docstring).

Clear / flush (issue #106): a namespace's keys are spread over every node
by HRW, not owned by one primary, so Namespace.clear() and this client's
own clear_all() fan out to every currently-known node instead of routing
by key — see _fan_out_clear()'s docstring for the refresh-once-and-retry
contract that gives it the same all-or-nothing guarantee a keyed write's
`W` retry has.
"""

from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Any
import ssl as ssl_module
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from ._compression import compress_value, decompress_value
from ._connection import CAS_ABSENT, CAS_PRESENT, WRONG_NODE, Connection
from ._digest import content_digest
from ._errors import AlreadyClosedError, NanocachedError, PartialWrongNodeError, WrongNodeError
from ._hashring import HashRing
from ._identify import (
    ClusterTarget,
    DiscoveredNode,
    NodeTarget,
    connect_and_identify,
    connect_and_identify_proxy,
    split_host_port,
)

# Errors safe for a by-design swallow site (client-side replication / fire-and-forget replica writes / read repair, node-list
# refresh) to absorb: this SDK's own error hierarchy, plus OS-level I/O
# errors (ConnectionError is itself an OSError subclass, kept in the tuple
# for readability at each call site). Programming errors — TypeError,
# AttributeError, and the like — are deliberately not in this tuple, so
# they propagate instead of vanishing identically to a dead replica or a
# stuck refresh; see _connection.py and _identify.py for what this SDK's
# own code actually raises.
_SWALLOWABLE_ERRORS = (NanocachedError, ConnectionError, OSError)


@dataclass(frozen=True)
class ClientStats:
    """Snapshot of counters for failures this client swallows by design
    (client-side replication / fire-and-forget replica writes / read repair) instead of raising them to a caller — observability
    for silently degrading replication or a stuck node-list refresh, which
    would otherwise be invisible. See ``NanocachedClient.stats()``.

    ``replica_write_failures``: replica-leg write failures swallowed
    during a cluster write, whether the leg ran synchronously or as a
    ``fire_and_forget_replicas`` background write (client-side replication,
    Fire-and-forget replica writes).

    ``read_repair_failures``: failures swallowed while probing owners or
    writing back the repaired value during read repair (read repair).

    ``refresh_failures``: node-list refresh attempts that failed, and
    per-node connect failures swallowed while reconciling a refresh's
    member list — discovery outages degrade only topology updates, never
    already-established cache traffic.

    ``transient_retries``: every retryable-error status (``R``, issue
    #125) received on any connection this client owns — counted whether
    the retry that followed it eventually succeeded or, after 3 attempts
    running, raised ``RetryableError``. ``R`` means the request itself
    failed transiently (e.g. nanocached-proxy's upstream node was briefly
    unreachable); the connection stays open and is retried transparently,
    so this counter is the only visibility into how often that's
    happening.
    """

    replica_write_failures: int
    read_repair_failures: int
    refresh_failures: int
    transient_retries: int


# Value compression: values shorter than this (bytes) are never
# compressed — the per-value overhead of attempting it outweighs the
# savings. Only meaningful when compress=True.
_DEFAULT_COMPRESSION_THRESHOLD = 256

# How long the node list may go without a re-fetch from discovery before
# get/set/delete refreshes it first (checked lazily on use).
_NODE_LIST_STALE_AFTER = 30.0

# Default for NanocachedClient.connect's reconnect_cooldown: how long,
# after a reconnect dial to an address fails, that address is treated as
# still down — a call routed to it during this window fails immediately
# with the original dial error instead of paying another full
# CONNECT_DEADLINE (5s, _identify.py) redialing an address that just
# proved unreachable. Keep well under _NODE_LIST_STALE_AFTER so a node
# that genuinely recovers isn't shut out for long.
_DEFAULT_RECONNECT_COOLDOWN = 1.0

# TTL (whole seconds — the protocol's TTL unit throughout, see
# _encode_set in _connection.py) a read-repair write uses
# (read repair). The original TTL isn't recoverable from a GET
# response, and repairing with TTL 0 (no expiry) would permanently
# resurrect data that was legitimately expiring; 60s bounds the overshoot
# instead — an immortal key just gets re-repaired on a later miss.
# Cross-SDK policy decision, applied identically across all SDKs.
_READ_REPAIR_TTL = 60

# Reserved for keep-alive pings: 0x00 followed by the ASCII bytes of
# "nanocached-keepalive" (21 bytes total) — not just a single NUL, which
# is itself a valid application key. A keep-alive G refreshes the server's
# LRU recency of whatever key it names (see _start_keepalive), so an app
# that happened to use key "\x00" would have had its recency silently
# reset every tick; this longer, namespaced sequence is reserved by the
# SDKs so a real application key can never collide with it.
_KEEPALIVE_KEY = b"\x00nanocached-keepalive"
# Half the server's 60s idle timeout; internal (issue #27), mutable only
# so tests can shorten it.
_KEEPALIVE_INTERVAL = 30.0

# Fire-and-forget replica writes, amended by read repair (issue #47 audit item
# 5): bounds how many replica writes a single client may have running in
# the background at once, combined across fire_and_forget_replicas writes
# (_write()) *and* read-repair write-backs (_try_read_repair()) — one
# shared pool, not one cap per category — mirroring Go's
# backgroundReplicaSem and Rust's background_replica_permits. Once the
# cap is reached, further fire_and_forget_replicas legs fall back to
# running synchronously, the same as with the option off; a read-repair
# write-back past the cap is simply skipped, since it has no synchronous
# fallback. Mutable only so tests can shrink it, mirroring
# _KEEPALIVE_INTERVAL.
_MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES = 32

# The longest a request header can ever legally be: marker + space + up to
# 10-digit key length + space + up to 10-digit value length + space + up
# to 10-digit TTL + up to 10-digit tag + LF (see _encode_get/_encode_set/
# _encode_delete in _connection.py). Rounded well up so _MAX_REQUEST_BYTES
# below can be computed without pushing key+value right up against the
# server's own limit and letting the header alone tip a request over it.
# 256, matching Rust's MAX_REQUEST_BYTES (client.rs) and Go's
# maxRequestBytes (client.go) headroom — standardised across the SDKs
# (issue #47 audit) so the same key+value fits or fails identically
# regardless of which client sends it.
_MAX_REQUEST_HEADER_BYTES = 256

# Mirrors nanocached-node's own MAX_REQUEST_SIZE (src/server.rs, 1 MiB) —
# the cap on a whole request frame (header + key + value) — minus headroom
# for the header itself. A G/D key, or an S key+value, beyond this would
# serialize into a frame the server's request_is_too_large check rejects
# outright with no reply at all — like an invalid ttl_seconds below, that
# closes the shared, pipelined connection and takes every other in-flight
# request on it down too.
_MAX_REQUEST_BYTES = 1024 * 1024 - _MAX_REQUEST_HEADER_BYTES

# Batched get/set (issues #128/#150/#151): the SDK transparently splits a
# larger batch into more than one m/o sub-frame per owner so no reply
# header risks outgrowing this connection's read buffer. Python's own
# ceiling here is generous — asyncio.StreamReader's default internal
# limit is 64 KiB, 16x Go's 4 KiB maxHeaderLineLength that its own 400
# derives from — so 400 is chosen to match every other SDK's chunking
# behavior (identical test expectations, identical operational
# characteristics), not because Python needs a smaller number of its
# own.
_MAX_BATCH_KEYS = 400


def _warn(message: str) -> None:
    print(message, file=sys.stderr)


def _to_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


def _check_key(key_bytes: bytes, namespace: bytes = b"") -> None:
    """An empty key (``key_length == 0``) hits ``ParseError::EmptyKey`` in
    src/command.rs, which — like a frame that's too large — the server
    rejects with no reply, closing the shared, pipelined connection and
    taking every other in-flight request on it down with it. Raised here,
    synchronously, before anything is written, for every command that
    carries a key.

    A namespace has no length limit of its own (issue #105 spec) — it is
    only ever bounded by the same MAX_REQUEST_BYTES the whole frame
    already has to fit under, so its bytes are folded into this check
    too rather than validated separately."""
    if len(key_bytes) == 0:
        raise ValueError("nanocached: key must not be empty")
    total = len(namespace) + len(key_bytes)
    if total > _MAX_REQUEST_BYTES:
        raise ValueError(
            f"nanocached: namespace and key together exceed MAX_REQUEST_BYTES "
            f"({_MAX_REQUEST_BYTES} bytes), got {total} bytes"
        )


def _check_key_and_value(key_bytes: bytes, value_bytes: bytes, namespace: bytes = b"") -> None:
    """Same server-side rejection and poisoned-connection consequence as
    _check_key, just measured across namespace+key+value together instead
    of namespace+key alone — see _MAX_REQUEST_BYTES."""
    _check_key(key_bytes, namespace)
    total = len(namespace) + len(key_bytes) + len(value_bytes)
    if total > _MAX_REQUEST_BYTES:
        raise ValueError(
            f"nanocached: namespace, key and value together exceed MAX_REQUEST_BYTES "
            f"({_MAX_REQUEST_BYTES} bytes), got {total} bytes"
        )


def _check_namespace(namespace: bytes) -> None:
    """A clear frame (issue #106) carries no key, just the namespace
    itself — same server-side rejection and poisoned-connection
    consequence as _check_key if it alone pushed the frame over
    MAX_REQUEST_BYTES, so it gets the same eager, synchronous check."""
    if len(namespace) > _MAX_REQUEST_BYTES:
        raise ValueError(
            f"nanocached: namespace exceeds MAX_REQUEST_BYTES "
            f"({_MAX_REQUEST_BYTES} bytes), got {len(namespace)} bytes"
        )


# Counters (issue #129): the server treats <delta> as an i64 — validated
# here, synchronously and before any I/O, the same way ttl_seconds and
# an empty key already are; a delta outside this range would otherwise
# either be rejected server-side with no reply (poisoning the shared,
# pipelined connection, same consequence as an empty key or an oversized
# frame) or silently truncated on the wire.
_MIN_I64 = -(2**63)
_MAX_I64 = 2**63 - 1


def _check_delta(delta: int) -> None:
    if not isinstance(delta, int) or isinstance(delta, bool):
        raise ValueError(f"nanocached: delta must be an integer, got {delta!r}")
    if not (_MIN_I64 <= delta <= _MAX_I64):
        raise ValueError(
            f"nanocached: delta must fit in a signed 64-bit integer "
            f"({_MIN_I64}..{_MAX_I64}), got {delta}"
        )


# Compare-and-set (issue #141): a token must be exactly the 32-character
# lowercase hex shape content_digest() produces and k/x's digest <cond>
# form requires — anything else would be sent as raw bytes the server
# can't parse as a digest, triggering the same no-reply,
# poisoned-connection rejection an empty key or an oversized frame does
# (_check_key). Checked eagerly, before any I/O, mirroring _check_delta.
_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")


def _check_token(token: str) -> bytes:
    if not isinstance(token, str) or not _TOKEN_RE.fullmatch(token):
        raise ValueError(
            f"nanocached: token must be a 32-character lowercase hex digest, got {token!r}"
        )
    return token.encode("ascii")


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
    its current connection — ``None`` for a member that was listed by
    discovery but unreachable when this client bootstrapped (issue #67):
    it stays routable, so a request for one of its keys fails over the
    same way it would after a mid-life node death, and the next request
    after the reconnect cooldown redials it."""

    def __init__(self, address: str, connection: Connection | None) -> None:
        self.address = address
        self.connection = connection


async def _drain_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    """close()'s drain of a set of detached tasks — see the comment at its
    call site for why each task is removed here, by this loop, before it
    is awaited (issue #65)."""
    while tasks:
        task = next(iter(tasks))
        tasks.discard(task)
        try:
            await task
        except asyncio.CancelledError:
            # The task's own cancellation is its outcome, retrieved here
            # like any other; close() itself being cancelled must still
            # propagate.
            if not task.cancelled():
                raise
        except Exception:
            pass


def _retrieve_outcome(task: asyncio.Task[Any]) -> None:
    """Done-callback for a detached task nobody will await: retrieves its
    exception so it never surfaces as an "exception was never retrieved"
    warning."""
    if not task.cancelled():
        task.exception()


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
        # Shared by both fire_and_forget_replicas writes (_write()) and
        # read-repair write-backs (_try_read_repair()): both are gated on
        # len(self._background_replica_writes) < _MAX_INFLIGHT_BACKGROUND_
        # REPLICA_WRITES before a task is ever created, so the two
        # categories draw from ONE combined pool of at most
        # _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES in flight — mirroring
        # Go's backgroundReplicaSem and Rust's background_replica_permits,
        # which are likewise a single semaphore shared across both paths
        # (issue #47 audit item 5). A previous, separate
        # asyncio.Semaphore(32) just for read-repair let the two
        # categories admit up to twice the intended cap combined.
        self._background_replica_writes: set[asyncio.Task[None]] = set()
        self._read_repair: bool = False
        # Hedged reads (issue #64): None = off. See _read_hedged().
        self._read_hedge_after: float | None = None
        # Hedge legs still in flight after a read has already returned
        # (the losers); finished detached, drained by close() exactly like
        # _background_replica_writes.
        self._hedged_reads: set[asyncio.Task[object]] = set()
        # SDK proxy mode (issue #122): set once, at connect(), when
        # via_proxy is true — never flips back. Gates the reconnect
        # fallback in _open_node_connection (retry the same proxy, then
        # re-fetch Q and pick another) and is otherwise inert: a proxy
        # connection is this client's ordinary single-connection mode
        # (self._single/self._single_address), so every other code path
        # (self._ring is None) already treats it exactly like a lone
        # node target.
        self._via_proxy: bool = False
        self._target_key: str | None = None
        self._last_fetch = time.monotonic()
        self._refresh_task: asyncio.Task[None] | None = None
        self._redials: dict[str, asyncio.Task[Connection]] = {}
        self._reconnect_cooldown: float = _DEFAULT_RECONNECT_COOLDOWN
        # Per-address reconnect cooldown (see _redial) — keyed by address
        # (not slot, which cluster mode can reassign across a refresh),
        # each entry is (monotonic deadline, the error that dial raised).
        self._redial_cooldowns: dict[str, tuple[float, BaseException]] = {}
        self._keepalive_task: asyncio.Task[None] | None = None
        # Backing counters for stats()/ClientStats — see its docstring.
        self._replica_write_failures = 0
        self._read_repair_failures = 0
        self._refresh_failures = 0
        self._transient_retries = 0

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
        read_repair: bool = False,
        reconnect_cooldown: float = _DEFAULT_RECONNECT_COOLDOWN,
        read_hedge_after: float | None = None,
        via_proxy: bool = False,
    ) -> "NanocachedClient":
        if not addresses:
            raise ValueError("nanocached: connect() needs a non-empty addresses list")
        if read_hedge_after is not None and not read_hedge_after > 0:
            raise ValueError("nanocached: read_hedge_after must be a positive number of seconds")

        client = cls()
        client._addresses = list(addresses)
        client._auth_secret = auth_secret.encode("utf-8") if auth_secret is not None else None
        client._ssl_context = _build_ssl_context(tls, ca)
        client._compress = compress
        client._compression_threshold = compression_threshold
        client._fire_and_forget_replicas = fire_and_forget_replicas
        client._read_repair = read_repair
        client._reconnect_cooldown = reconnect_cooldown
        client._read_hedge_after = read_hedge_after

        # SDK proxy mode (issue #122): a separate connect path — fetch the
        # proxy roster (`Q`) from a discovery seed instead of the node
        # roster (`L`), and land on a single proxy connection instead of a
        # ring. Kept as its own loop (mirroring the ring-mode one just
        # below) rather than interleaved with it, since the two share only
        # the outermost "walk the addresses" shape and diverge in what
        # they fetch, how a `NodeTarget` answer is treated (a config error
        # here, a legitimate single-node pin there), and what a successful
        # answer installs.
        if via_proxy:
            return await cls._connect_via_proxy(client)

        # Walk the addresses until one yields a working target; an address
        # that is unreachable, warming up (`B`, discovery HA), or knows no live
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
                client._single = client._new_connection(identified.reader, identified.writer, identified.tagged)
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

    @classmethod
    async def _connect_via_proxy(cls, client: "NanocachedClient") -> "NanocachedClient":
        """SDK proxy mode (issue #122): ``client`` already has every
        connect()-time option applied — this only walks the discovery
        seeds for a proxy roster (``Q``) instead of a node roster
        (``L``), and lands on one proxy instead of a ring. Reuses
        connect()'s own seed-iteration and `B` handling (an address
        that's unreachable, warming up, or has an empty roster is
        skipped, the next seed may do better); once a seed answers with a
        *non-empty* roster this commits to it exactly the way
        connect()'s ring-mode path commits to the first discovery
        server that lists any nodes — if every proxy in that roster then
        turns out unreachable, connect() fails outright rather than
        trying yet another discovery seed for a fresh roster."""
        last_error: Exception | None = None
        for address_host, address_port in client._addresses:
            key = f"{address_host}:{address_port}"
            if len(client._addresses) == 1 and _has_open_target(key):
                _warn(
                    f"nanocached: connect() called for {key} while a previous connection to it "
                    f"is still open — was close() forgotten?"
                )

            try:
                identified = await connect_and_identify_proxy(
                    address_host, address_port, client._auth_secret, client._ssl_context
                )
            except (NanocachedError, OSError) as error:
                last_error = error
                continue

            if isinstance(identified, NodeTarget):
                # via_proxy is only meaningful against discovery
                # addresses (issue #122 spec) — the identify handshake
                # already tells node and discovery apart, so a node
                # answer here is a configuration error, not a liveness
                # one, and fails fast instead of silently falling back
                # to the next address the way an unreachable one does.
                identified.writer.close()
                raise NanocachedError(
                    f"nanocached: via_proxy requires a discovery server address, but {key} "
                    f"identifies as a cache node"
                )

            if not identified:
                last_error = NanocachedError(
                    f"nanocached: no proxies registered with discovery server at {key}"
                )
                continue

            client._target_key = key
            try:
                await client._connect_to_proxy(identified)
            except BaseException:
                client._teardown()
                raise
            client._start_keepalive()
            return client

        raise last_error if last_error is not None else NanocachedError(
            "nanocached: could not connect to any address"
        )

    def _new_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tagged: bool
    ) -> Connection:
        """Wraps a freshly identified node socket, incrementing this
        client's target key in _open_targets (the forgotten-close warning
        above) and decrementing it again via on_close whenever the
        connection eventually closes — whether via close(), redial
        replacement, or refresh reconciliation. ``tagged`` is the identify
        exchange's own echoed response tags negotiation result, threaded straight
        through to Connection."""
        assert self._target_key is not None
        key = self._target_key
        _increment_open_target(key)
        return Connection(
            reader,
            writer,
            on_close=lambda: _decrement_open_target(key),
            tagged=tagged,
            on_transient_retry=self._record_transient_retry,
        )

    async def _open_cluster(self, identified: ClusterTarget) -> None:
        """Dials every node discovery listed, concurrently. A node that
        can't be reached (issue #67: typically one that just died and
        discovery hasn't evicted yet — its liveness window is seconds
        long, and every key is still served by another owner when R > 1)
        is installed without a connection and with its reconnect cooldown
        armed, exactly the state a member is in after dying mid-life, so
        requests for its keys fail over per request instead of the whole
        connect() failing. Only a cluster with *no* reachable node fails
        connect(), with the last dial error."""

        async def dial(node):
            node_host, node_port = split_host_port(node.address)
            try:
                target = await connect_and_identify(
                    node_host, node_port, self._auth_secret, self._ssl_context
                )
            except (NanocachedError, OSError) as error:
                return node, error
            return node, target

        outcomes = await asyncio.gather(*(dial(node) for node in identified.nodes))

        # A non-node answer is a configuration error, not a liveness one:
        # checked across every outcome first so the sockets this same
        # round already opened are closed rather than leaked.
        for node, outcome in outcomes:
            if not isinstance(outcome, (Exception, NodeTarget)):
                for _, other in outcomes:
                    if not isinstance(other, Exception):
                        other.writer.close()
                raise NanocachedError(
                    f"nanocached: discovery server returned a non-node address: {node.address}"
                )

        reachable = 0
        last_error: Exception | None = None
        for node, outcome in outcomes:
            if isinstance(outcome, Exception):
                self._members[node.name] = _Member(node.address, None)
                self._redial_cooldowns[node.address] = (
                    time.monotonic() + self._reconnect_cooldown,
                    outcome,
                )
                last_error = outcome
                continue
            self._members[node.name] = _Member(
                node.address, self._new_connection(outcome.reader, outcome.writer, outcome.tagged)
            )
            reachable += 1

        if reachable == 0:
            assert last_error is not None
            raise last_error

        self._ring = HashRing([node.name for node in identified.nodes])
        self._replication = identified.replication

    async def _connect_to_proxy(self, proxies: list[DiscoveredNode]) -> None:
        """SDK proxy mode (issue #122) initial connect: installs the
        winner of _dial_one_proxy_randomly as this client's single
        connection — from here on it behaves exactly like a lone
        explicit node target (the classmethod connect()'s own NodeTarget
        branch), never a ring: a proxy looks exactly like one node that
        owns every key (issue #122 spec)."""
        self._via_proxy = True
        self._single = await self._dial_one_proxy_randomly(proxies)

    async def _dial_one_proxy_randomly(self, proxies: list[DiscoveredNode]) -> Connection:
        """Shuffles ``proxies`` and dials each in turn until one answers
        as a live cache node: "connect to ONE proxy chosen AT RANDOM —
        spreads a fleet over proxies — ...on connect failure, fail over
        through the remaining proxies in random order" (issue #122
        spec). Shared by the initial proxy-mode connect
        (_connect_to_proxy) and the reconnect-time roster refetch
        (_reconnect_via_new_proxy) so there is exactly one place this
        logic lives, per the spec's "reuse the existing reconnect/
        refresh plumbing... do not build a second reconnect machine".
        Always updates self._single_address to whichever proxy actually
        answered — including on a reconnect that lands on a *different*
        proxy than the one that just died — since that is what the next
        redial (_single_connection) will target."""
        order = list(proxies)
        random.shuffle(order)
        last_error: Exception | None = None
        for proxy in order:
            proxy_host, proxy_port = split_host_port(proxy.address)
            try:
                target = await connect_and_identify(
                    proxy_host, proxy_port, self._auth_secret, self._ssl_context
                )
            except (NanocachedError, OSError) as error:
                last_error = error
                continue
            if not isinstance(target, NodeTarget):
                # A proxy always identifies as a node (issue #122 spec);
                # anything else means it stopped being a proxy between Q
                # and this dial (retired, address reused) — treated as
                # just another unreachable candidate rather than aborting
                # the whole roster the way _open_cluster's stricter
                # "discovery returned a non-node address" check does,
                # since via_proxy's whole point is resilience across a
                # fleet of proxies.
                last_error = NanocachedError(
                    f"nanocached: proxy {proxy.address} no longer identifies as a cache node"
                )
                continue
            if self._closed:
                target.writer.close()
                raise AlreadyClosedError()
            self._single_address = proxy.address
            return self._new_connection(target.reader, target.writer, target.tagged)
        raise last_error if last_error is not None else NanocachedError(
            "nanocached: no proxies registered with discovery"
        )

    async def _reconnect_via_new_proxy(self) -> Connection:
        """SDK proxy mode (issue #122) reconnect fallback, invoked by
        _open_node_connection when a redial to the currently pinned
        proxy itself fails ("first retry the same proxy... if that
        fails, re-fetch Q from discovery and pick another", issue #122
        spec — the "retry the same proxy" half is just the ordinary
        _redial attempt against self._single_address that got us here).
        Re-fetches the roster and dials one at random via
        _dial_one_proxy_randomly, reusing exactly the same fan-out and
        random-order failover as the initial connect."""
        proxies = await self._fetch_proxy_list()
        if not proxies:
            raise NanocachedError(
                "nanocached: no proxies registered with discovery (proxy mode reconnect)"
            )
        return await self._dial_one_proxy_randomly(proxies)

    async def _fetch_proxy_list(self) -> list[DiscoveredNode]:
        """Walks every configured discovery address (discovery HA),
        exactly like _fetch_node_list, returning the first that answers
        with a (possibly empty) proxy roster. A node-identifying address
        is skipped (closed) rather than erroring, and every failure is
        swallowed here: like _fetch_node_list, this reconnect-time fetch
        must never itself raise something other than "no proxies were
        found" — the caller (_reconnect_via_new_proxy) turns an empty
        result into the same clear error an empty initial roster would
        raise at connect(). Each failed address is counted in
        stats().refresh_failures — the same counter _fetch_node_list
        itself uses, since both are "the node/proxy list this client
        routes with just went stale" (per the spec: reuse the SDK's
        existing counters rather than adding a proxy-only one)."""
        for address_host, address_port in self._addresses:
            try:
                identified = await connect_and_identify_proxy(
                    address_host, address_port, self._auth_secret, self._ssl_context
                )
            except _SWALLOWABLE_ERRORS:
                self._refresh_failures += 1
                continue
            if isinstance(identified, NodeTarget):
                identified.writer.close()
                continue
            if not identified:
                continue
            return identified
        return []

    # ── 公開 API ───────────────────────────────────────────────────

    @property
    def replication(self) -> int:
        """How many nodes hold each key (client-side replication) — 1 against a single node."""
        return self._replication if self._ring is not None else 1

    def stats(self) -> ClientStats:
        """Observability for failures this client swallows by design
        (client-side replication / fire-and-forget replica writes / read repair) — lets operators detect silently degrading
        replication or a stuck node-list refresh. A snapshot, not a live
        view; each count is monotonic for the lifetime of this client."""
        return ClientStats(
            replica_write_failures=self._replica_write_failures,
            read_repair_failures=self._read_repair_failures,
            refresh_failures=self._refresh_failures,
            transient_retries=self._transient_retries,
        )

    def _record_transient_retry(self) -> None:
        """Connection's on_transient_retry hook (issue #125): every
        connection this client creates (_new_connection) reports its own
        `R` responses here, regardless of which routing mode created it
        (single node, cluster member, proxy, hedge leg) — one shared
        counter, mirroring how replica_write_failures/read_repair_failures
        are already shared across their own call sites."""
        self._transient_retries += 1

    @property
    def closed(self) -> bool:
        return self._closed

    async def get_bytes(self, key: str | bytes) -> bytes | None:
        """The raw companion to get(): no UTF-8 decoding, so it never
        raises on a value that isn't valid UTF-8. Transparently
        decompresses when ``compress`` is enabled (value compression).
        With ``read_repair``, a clean miss probes the remaining owners
        before being accepted as final (read repair)."""
        return await self._get_bytes(b"", key)

    async def _get_bytes(self, namespace: bytes, key: str | bytes) -> bytes | None:
        """The namespace-carrying implementation behind get_bytes() and
        Namespace.get_bytes() (issue #105) — see namespace()'s docstring:
        the handle never duplicates this client's networking, it just
        calls in here with its own namespace instead of b""."""
        key_bytes = _to_bytes(key)
        _check_key(key_bytes, namespace)
        await self._before_operation()
        value = await self._with_wrong_node_retry(
            lambda: self._read(
                namespace, key_bytes, lambda connection: connection.get(key_bytes, namespace)
            )
        )
        if value is None and self._read_repair and self._ring is not None:
            value = await self._try_read_repair(namespace, key_bytes)
        if value is None or not self._compress:
            return value
        return decompress_value(value)

    async def get_with_token(self, key: str | bytes) -> tuple[bytes, str] | None:
        """Like get_bytes(), but also returns a CAS token (issue #141):
        content_digest() of the *exact wire bytes* the server holds for
        this key — for a compress-enabled client, that's the
        marker-byte-prefixed compressed bytes (_compression.py), NOT the
        decompressed value this method's own return also carries,
        because the server never decompresses and so can never match a
        digest computed over bytes it never saw. Pass the token straight
        to replace()/delete_if_matches() to condition on this exact read.
        Returns ``None`` on a miss, matching get_bytes()'s own
        convention. See README.md's "Compare-and-set" section and
        docs/protocol.html "k / x"."""
        return await self._get_with_token(b"", key)

    async def _get_with_token(self, namespace: bytes, key: str | bytes) -> tuple[bytes, str] | None:
        """The namespace-carrying implementation behind get_with_token()
        and Namespace.get_with_token() (issue #141) — see _get_bytes(),
        whose raw-read-then-decompress shape this mirrors exactly (read
        repair included), except the digest is taken from the raw value
        *before* decompress_value() ever runs, per get_with_token()'s own
        docstring."""
        key_bytes = _to_bytes(key)
        _check_key(key_bytes, namespace)
        await self._before_operation()
        raw = await self._with_wrong_node_retry(
            lambda: self._read(
                namespace, key_bytes, lambda connection: connection.get(key_bytes, namespace)
            )
        )
        if raw is None and self._read_repair and self._ring is not None:
            raw = await self._try_read_repair(namespace, key_bytes)
        if raw is None:
            return None
        token = content_digest(raw)
        return (decompress_value(raw) if self._compress else raw), token

    async def _try_read_repair(self, namespace: bytes, key: bytes) -> bytes | None:
        """read repair: probes the remaining owners of ``key`` —
        every owner but the primary, which the normal read path already
        probed and got a clean miss from (same as the Rust, Go, Java and
        .NET SDKs) — in rank order, for a value the normal read path
        already reported missing. The first owner that has it wins: its value is
        returned, and — detached from the caller, which already has its
        value — that same value repairs the true primary in the
        background, with TTL _READ_REPAIR_TTL (the original TTL can't be
        recovered from a GET, and TTL 0 would permanently resurrect
        already-expired data). The write-back shares its admission check
        and its tracking set (_background_replica_writes) with
        fire_and_forget_replicas writes in _write() — one combined pool of
        at most _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES in flight, exactly
        as Go's backgroundReplicaSem and Rust's background_replica_permits
        are shared across both paths (issue #47 audit item 5) — so
        close() drains it too instead of leaving it dangling. Past the cap,
        or once close() has started (self._closed), the repair for this
        miss is simply skipped: it's opportunistic (a later miss repairs
        the key instead), and unlike a replica write there's no
        synchronous fallback available here. Every failure along the way
        is swallowed; only a failed repair *write-back* is counted in
        stats().read_repair_failures — a failed owner probe is silent,
        matching the counter's write-back semantics in the other five SDKs
        (issue #43). Nothing here may turn an already-accepted miss into
        an error — except a genuine programming error (anything outside
        _SWALLOWABLE_ERRORS), which still propagates."""
        names = self._owner_names(namespace, key)
        for name in names[1:]:
            try:
                connection = await self._member_connection(name)
                value = await connection.get(key, namespace)
            except _SWALLOWABLE_ERRORS:
                continue
            if value is None:
                continue

            if names:
                primary = names[0]

                async def repair(primary: str = primary, value: bytes = value) -> None:
                    try:
                        connection = await self._member_connection(primary)
                        await connection.set(key, value, _READ_REPAIR_TTL, namespace)
                    except _SWALLOWABLE_ERRORS:
                        # Swallowed by design — see the docstring.
                        self._read_repair_failures += 1

                # Recheck self._closed here, immediately before
                # registering the task: close() sets it before draining
                # _background_replica_writes, so a get_bytes() call still
                # in flight when close() runs must not add a new entry
                # that drain has already (or is about to have) taken its
                # snapshot of — see close()'s own comment.
                if (
                    not self._closed
                    and len(self._background_replica_writes) < _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES
                ):
                    task = asyncio.ensure_future(repair())
                    self._background_replica_writes.add(task)
                    task.add_done_callback(self._background_replica_writes.discard)
            return value
        return None

    async def get(self, key: str | bytes) -> str | None:
        """Strict UTF-8 decode of the stored value (bytes.decode()) — a
        value that is not valid UTF-8 raises UnicodeDecodeError rather than
        silently replacing it. Use get_bytes() for the raw bytes."""
        return await self._get(b"", key)

    async def _get(self, namespace: bytes, key: str | bytes) -> str | None:
        """The namespace-carrying implementation behind get() and
        Namespace.get() (issue #105) — see _get_bytes()."""
        value = await self._get_bytes(namespace, key)
        return value.decode() if value is not None else None

    async def set(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> None:
        """``ttl_seconds`` is whole seconds; 0 (the default) means no
        expiry. Negative values are rejected eagerly, before any I/O.
        Transparently compresses values at or above
        ``compression_threshold`` when ``compress`` is enabled
        (value compression)."""
        await self._set(b"", key, value, ttl_seconds=ttl_seconds)

    async def _set(
        self, namespace: bytes, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> None:
        """The namespace-carrying implementation behind set() and
        Namespace.set() (issue #105) — see _get_bytes()."""
        if not isinstance(ttl_seconds, int) or ttl_seconds < 0:
            raise ValueError(f"nanocached: ttl_seconds must be a non-negative integer, got {ttl_seconds}")
        key_bytes, value_bytes = _to_bytes(key), _to_bytes(value)
        # Validate the *original* value's size before compressing, matching
        # Rust's and Go's Set — an oversized value must be rejected outright,
        # not silently forwarded just because DEFLATE happens to shrink it
        # under the cap (issue #47 audit; Rust/Go's own bound is on the
        # pre-compression value for the same reason). Re-checked after
        # compression too, purely as a defense-in-depth backstop should
        # compress_value ever grow a value instead of shrinking it.
        _check_key_and_value(key_bytes, value_bytes, namespace)
        if self._compress:
            value_bytes = compress_value(value_bytes, self._compression_threshold)
            _check_key_and_value(key_bytes, value_bytes, namespace)
        await self._before_operation()
        await self._with_wrong_node_retry(
            lambda: self._write(
                namespace,
                key_bytes,
                lambda connection: connection.set(key_bytes, value_bytes, ttl_seconds, namespace),
            )
        )

    async def delete(self, key: str | bytes) -> bool:
        """Returns whether the key existed before this call."""
        return await self._delete(b"", key)

    async def _delete(self, namespace: bytes, key: str | bytes) -> bool:
        """The namespace-carrying implementation behind delete() and
        Namespace.delete() (issue #105) — see _get_bytes()."""
        key_bytes = _to_bytes(key)
        _check_key(key_bytes, namespace)
        await self._before_operation()
        return await self._with_wrong_node_retry(
            lambda: self._write(
                namespace, key_bytes, lambda connection: connection.delete(key_bytes, namespace)
            )
        )

    # ── Batched get and set (issues #128/#150/#151) ────────────────────
    #
    # get_many/get_many_bytes/set_many exist because a caller with N keys
    # otherwise pays N round trips — one m/o sub-frame per involved node
    # replaces that with one, transparently chunked into more than one
    # sub-frame when a batch exceeds _MAX_BATCH_KEYS. A batch never fails
    # as a whole: every key's outcome (hit/miss/stored/wrong-node) is
    # independent (docs/protocol.html "m / o — batched get and set").
    # Both str and bytes keys are accepted, matching every other method
    # on this client — the returned dict is keyed by whichever original
    # object the caller passed, indexed by position rather than by
    # encoded bytes, so a batch may freely mix the two; because "a" and
    # b"a" encode identically, a batch containing both is rejected
    # up-front with ValueError rather than silently colliding.

    def _dedupe_batch_keys(
        self, keys: Sequence[str | bytes], namespace: bytes
    ) -> tuple[list[str | bytes], list[bytes]]:
        originals: list[str | bytes] = []
        key_bytes_list: list[bytes] = []
        seen: dict[bytes, str | bytes] = {}
        for original in keys:
            key_bytes = _to_bytes(original)
            _check_key(key_bytes, namespace)
            if key_bytes in seen:
                raise ValueError(
                    f"nanocached: duplicate key after encoding: {original!r} and "
                    f"{seen[key_bytes]!r} both encode to {key_bytes!r}"
                )
            seen[key_bytes] = original
            originals.append(original)
            key_bytes_list.append(key_bytes)
        return originals, key_bytes_list

    def _maybe_decompress(self, value: bytes) -> bytes:
        return decompress_value(value) if self._compress else value

    async def get_many_bytes(self, keys: Sequence[str | bytes]) -> dict[str | bytes, bytes]:
        """The raw companion to get_many(): no UTF-8 decoding. See
        get_many()'s docstring for the batch contract."""
        return await self._get_many_bytes(b"", keys)

    async def get_many(self, keys: Sequence[str | bytes]) -> dict[str | bytes, str]:
        """Batched get (issues #128/#150/#151): one round trip per
        involved node instead of one per key. ``keys`` must be
        non-empty. A miss is simply absent from the returned dict, the
        same "a miss is not an error" contract get() itself has.

        A batch never fails as a whole: if some keys are still
        wrong-node after one bounded refresh-and-retry (the same policy
        get()'s own _with_wrong_node_retry applies, generalized to a
        per-key roster instead of an all-or-nothing retry — see
        _multi_get_pass()), this raises PartialWrongNodeError — a
        WrongNodeError subclass — whose ``partial_values`` holds every
        key that DID resolve, rather than discarding a
        mostly-successful batch over a handful of stale placements. In
        single-node/proxy mode a ``W`` propagates immediately as a plain
        WrongNodeError, exactly as get()'s own single-mode behavior
        does — there is no ring to refresh against.

        Larger batches are transparently split into more than one ``m``
        sub-frame per owner (_MAX_BATCH_KEYS) — callers never need to
        think about this. Hedged reads and read repair do not apply to
        batches: hedging's miss sentinel and read repair both assume a
        single value, not a partial-hit roster (Go's and TypeScript's
        ports made the same call)."""
        return await self._get_many(b"", keys)

    async def _get_many_bytes(
        self, namespace: bytes, keys: Sequence[str | bytes]
    ) -> dict[str | bytes, bytes]:
        """The namespace-carrying implementation behind get_many_bytes()
        and Namespace.get_many_bytes() — see _get_bytes()."""
        if not keys:
            raise ValueError("nanocached: get_many/get_many_bytes requires at least one key")
        originals, key_bytes_list = self._dedupe_batch_keys(keys, namespace)
        await self._before_operation()

        if self._ring is None:
            entries, error = await self._multi_get_chunked(
                self._single_connection, namespace, key_bytes_list
            )
            values: dict[str | bytes, bytes] = {}
            for original, entry in zip(originals, entries):
                if isinstance(entry, (bytes, bytearray)):
                    values[original] = self._maybe_decompress(entry)
            if error is not None:
                raise error
            if any(entry is WRONG_NODE for entry in entries):
                raise WrongNodeError()
            return values

        values = {}
        retry = await self._multi_get_pass(namespace, originals, key_bytes_list, values, None)
        if not retry:
            return values
        await self._maybe_refresh(force=True)
        retry = await self._multi_get_pass(namespace, originals, key_bytes_list, values, retry)
        if retry:
            raise PartialWrongNodeError(values)
        return values

    async def _get_many(self, namespace: bytes, keys: Sequence[str | bytes]) -> dict[str | bytes, str]:
        """The namespace-carrying implementation behind get_many() and
        Namespace.get_many() — see _get()."""
        try:
            raw = await self._get_many_bytes(namespace, keys)
        except PartialWrongNodeError as error:
            error.partial_values = {key: value.decode() for key, value in error.partial_values.items()}
            raise
        return {key: value.decode() for key, value in raw.items()}

    async def _multi_get_chunked(
        self, connection_getter, namespace: bytes, keys: list[bytes]
    ) -> tuple[list[bytes | None | object], BaseException | None]:
        """Issues one or more ``m`` sub-frames for ``keys`` — already
        grouped to one owner (or the single/proxy target) by the caller
        — splitting into _MAX_BATCH_KEYS-sized chunks so no reply header
        risks growing unbounded. ``connection_getter`` is a zero-arg
        async callable returning the Connection to use, called fresh per
        chunk, so a mid-batch reconnect is handled the same way a
        single-key get/set is. Always returns len(keys) entries: a chunk
        that fails outright (a connection-level failure, not a per-key
        ``W``) leaves the remaining keys' entries at the zero value
        (None — a clean miss) and that chunk's error is returned
        alongside — the caller treats that gap as "still needs a
        retry", exactly like a per-key WrongNode."""
        entries: list[bytes | None | object] = [None] * len(keys)
        for start in range(0, len(keys), _MAX_BATCH_KEYS):
            chunk = keys[start : start + _MAX_BATCH_KEYS]
            try:
                connection = await connection_getter()
                chunk_entries = await connection.multi_get(chunk, namespace)
            except _SWALLOWABLE_ERRORS as error:
                return entries, error
            entries[start : start + len(chunk)] = chunk_entries
        return entries, None

    async def _multi_get_pass(
        self,
        namespace: bytes,
        originals: list[str | bytes],
        key_bytes_list: list[bytes],
        values: dict[str | bytes, bytes],
        retry_indices: list[int] | None,
    ) -> list[int]:
        """Runs one pass of get_many_bytes' cluster routing: group the
        given indices (every key, when retry_indices is None — the
        initial pass — or just the keys a previous pass left unresolved)
        by their current primary owner (matching plain get()'s own
        primary-first stance), dispatch one (possibly chunked) ``m``
        exchange per owner concurrently, splice hits into ``values``,
        and return the indices still unresolved: a per-key ``W``, or a
        whole owner group whose call failed outright (indistinguishable
        from a possibly-idle-closed connection, same stance _read()
        itself takes). Called once for the initial pass and once more,
        if needed, after a single forced refresh — see
        _get_many_bytes()."""
        indices = retry_indices if retry_indices is not None else list(range(len(originals)))

        groups: dict[str, list[int]] = {}
        retry: list[int] = []
        for idx in indices:
            owners = self._owner_names(namespace, key_bytes_list[idx])
            if not owners:
                retry.append(idx)
                continue
            groups.setdefault(owners[0], []).append(idx)

        async def run_group(owner: str, group_indices: list[int]) -> tuple[list[int], dict[str | bytes, bytes]]:
            group_keys = [key_bytes_list[idx] for idx in group_indices]
            entries, error = await self._multi_get_chunked(
                lambda: self._member_connection(owner), namespace, group_keys
            )
            if error is not None:
                return list(group_indices), {}
            local_retry: list[int] = []
            local_values: dict[str | bytes, bytes] = {}
            for idx, entry in zip(group_indices, entries):
                if entry is WRONG_NODE:
                    local_retry.append(idx)
                elif entry is not None:
                    local_values[originals[idx]] = self._maybe_decompress(entry)
            return local_retry, local_values

        results = await asyncio.gather(*(run_group(owner, idxs) for owner, idxs in groups.items()))
        for local_retry, local_values in results:
            retry.extend(local_retry)
            values.update(local_values)
        return retry

    async def set_many(
        self, values: Mapping[str | bytes, str | bytes], *, ttl_seconds: int = 0
    ) -> None:
        """Batched set (issues #128/#150/#151): one round trip per
        involved node instead of one per key. ``ttl_seconds`` is shared
        by the whole batch, not per key (one real caller of a batched
        set — Django's set_many, cache-manager's mset — already passes
        one TTL per call); 0 (the default) means no expiry. ``values``
        must be non-empty. Transparently compresses values at or above
        ``compression_threshold`` when ``compress`` is enabled, exactly
        like set().

        Within one batch, the same node can be a key's primary and
        another key's replica at once — it receives exactly one ``o``
        sub-frame either way, and only its answer for the keys it is
        primary for decides that key's outcome; a replica-held key's
        failure or ``W`` is logged-and-swallowed into
        stats().replica_write_failures, exactly like set()'s own replica
        legs. A batch never fails as a whole: if some keys' primaries
        are still wrong-node after one bounded refresh-and-retry, this
        raises a plain WrongNodeError — every other key in the batch was
        still stored (there is nothing partial worth attaching, unlike
        get_many/get_many_bytes, since a write has no value to return).
        In single-node/proxy mode a ``W`` propagates immediately, exactly
        as set()'s own single-mode behavior does.

        Larger batches are transparently split into more than one ``o``
        sub-frame per node (_MAX_BATCH_KEYS)."""
        await self._set_many(b"", values, ttl_seconds=ttl_seconds)

    async def _set_many(
        self, namespace: bytes, values: Mapping[str | bytes, str | bytes], *, ttl_seconds: int = 0
    ) -> None:
        """The namespace-carrying implementation behind set_many() and
        Namespace.set_many() — see _set()."""
        if not values:
            raise ValueError("nanocached: set_many requires at least one key")
        if not isinstance(ttl_seconds, int) or ttl_seconds < 0:
            raise ValueError(f"nanocached: ttl_seconds must be a non-negative integer, got {ttl_seconds}")

        key_bytes_list: list[bytes] = []
        value_bytes_list: list[bytes] = []
        seen: dict[bytes, str | bytes] = {}
        for original, value in values.items():
            key_bytes, value_bytes = _to_bytes(original), _to_bytes(value)
            # See _set()'s own comment: validated before compressing, and
            # re-checked after, purely as a defense-in-depth backstop.
            _check_key_and_value(key_bytes, value_bytes, namespace)
            if self._compress:
                value_bytes = compress_value(value_bytes, self._compression_threshold)
                _check_key_and_value(key_bytes, value_bytes, namespace)
            if key_bytes in seen:
                raise ValueError(
                    f"nanocached: duplicate key after encoding: {original!r} and "
                    f"{seen[key_bytes]!r} both encode to {key_bytes!r}"
                )
            seen[key_bytes] = original
            key_bytes_list.append(key_bytes)
            value_bytes_list.append(value_bytes)
        await self._before_operation()

        if self._ring is None:
            entries, error = await self._multi_set_chunked(
                self._single_connection, namespace, key_bytes_list, value_bytes_list, ttl_seconds
            )
            if error is not None:
                raise error
            if any(entry is WRONG_NODE for entry in entries):
                raise WrongNodeError()
            return

        retry = await self._multi_set_pass(
            namespace, key_bytes_list, value_bytes_list, ttl_seconds, None
        )
        if not retry:
            return
        await self._maybe_refresh(force=True)
        retry = await self._multi_set_pass(
            namespace, key_bytes_list, value_bytes_list, ttl_seconds, retry
        )
        if retry:
            raise WrongNodeError()

    async def _multi_set_chunked(
        self,
        connection_getter,
        namespace: bytes,
        keys: list[bytes],
        values: list[bytes],
        ttl_seconds: int,
    ) -> tuple[list[bool | object], BaseException | None]:
        """_multi_get_chunked's write-side twin: one or more ``o``
        sub-frames against ``connection_getter`` for keys/values (already
        grouped to one owner, or the single/proxy target), split into
        _MAX_BATCH_KEYS-sized chunks. Always returns len(keys) entries; a
        chunk that fails outright leaves the remaining keys' entries at
        the zero value (False) and that chunk's error is returned
        alongside."""
        entries: list[bool | object] = [False] * len(keys)
        for start in range(0, len(keys), _MAX_BATCH_KEYS):
            end = start + _MAX_BATCH_KEYS
            try:
                connection = await connection_getter()
                chunk_entries = await connection.multi_set(
                    keys[start:end], values[start:end], ttl_seconds, namespace
                )
            except _SWALLOWABLE_ERRORS as error:
                return entries, error
            entries[start:end] = chunk_entries
        return entries, None

    async def _multi_set_pass(
        self,
        namespace: bytes,
        key_bytes_list: list[bytes],
        value_bytes_list: list[bytes],
        ttl_seconds: int,
        retry_indices: list[int] | None,
    ) -> list[int]:
        """Runs one pass of set_many's cluster routing: for every key
        still needing resolution (every key, when retry_indices is None,
        or just what a previous pass left unresolved), build one
        sub-batch per **owner name across every rank** — not just
        primaries, unlike _multi_get_pass — because within one batch the
        same node can be primary for one key and a replica for another
        (see set_many()'s own docstring); each owner therefore gets
        exactly one ``o`` sub-frame covering every key it holds in any
        role. Only a leg's *primary* keys can end up in the returned
        retry list; a leg's replica-held keys are logged-and-swallowed
        into stats().replica_write_failures instead, mirroring
        _dispatch_replica_writes' stance for a single-key set(). A leg
        that is a pure replica for every key it holds is eligible for
        fire_and_forget_replicas, exactly like a single-key replica
        write."""
        indices = retry_indices if retry_indices is not None else list(range(len(key_bytes_list)))

        owners: dict[str, tuple[list[int], list[bool]]] = {}
        retry: list[int] = []
        for idx in indices:
            names = self._owner_names(namespace, key_bytes_list[idx])
            if not names:
                retry.append(idx)
                continue
            for rank, name in enumerate(names):
                owner_indices, owner_is_primary = owners.setdefault(name, ([], []))
                owner_indices.append(idx)
                owner_is_primary.append(rank == 0)

        async def run_leg(name: str, leg_indices: list[int], is_primary: list[bool]) -> list[int]:
            group_keys = [key_bytes_list[idx] for idx in leg_indices]
            group_values = [value_bytes_list[idx] for idx in leg_indices]
            entries, error = await self._multi_set_chunked(
                lambda: self._member_connection(name), namespace, group_keys, group_values, ttl_seconds
            )
            leg_retry: list[int] = []
            if error is not None:
                for idx, primary in zip(leg_indices, is_primary):
                    if primary:
                        leg_retry.append(idx)
                    else:
                        self._replica_write_failures += 1
                return leg_retry
            for idx, primary, entry in zip(leg_indices, is_primary, entries):
                if not primary:
                    if entry is WRONG_NODE:
                        self._replica_write_failures += 1
                    continue
                if entry is WRONG_NODE:
                    leg_retry.append(idx)
            return leg_retry

        synchronous_legs = []
        for name, (leg_indices, is_primary) in owners.items():
            pure_replica = not any(is_primary)
            if (
                pure_replica
                and self._fire_and_forget_replicas
                and not self._closed
                and len(self._background_replica_writes) < _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES
            ):
                task = asyncio.ensure_future(run_leg(name, leg_indices, is_primary))
                self._background_replica_writes.add(task)
                task.add_done_callback(self._background_replica_writes.discard)
                continue
            synchronous_legs.append(run_leg(name, leg_indices, is_primary))

        for leg_retry in await asyncio.gather(*synchronous_legs):
            retry.extend(leg_retry)
        return retry

    async def incr(self, key: str | bytes, delta: int = 1) -> int | None:
        """Atomically adds ``delta`` (may be negative) to the integer
        counter stored at ``key`` and returns the new value — or
        ``None`` if the key does not exist, matching get()'s own miss
        convention (issue #129). Raises ``NotNumericError`` if the
        stored value isn't an integer INCR can operate on, or applying
        ``delta`` would overflow a signed 64-bit integer.

        INCR is exactly as volatile as set(): LRU eviction and TTL
        expiry reclaim an incremented value like any other entry, so
        this suits rate limiting and approximate counters, never durable
        counts (billing, inventory). See decr() for subtraction, and
        README.md's "Counters" section for the cluster-replication
        caveat: only the primary owner ever runs the increment, so a
        replica can never drift onto a different counter value than the
        one the primary actually holds."""
        return await self._incr(b"", key, delta)

    async def decr(self, key: str | bytes, delta: int = 1) -> int | None:
        """``incr(key, -delta)`` (issue #129) — decr never sends a
        separate wire op; see incr()."""
        return await self.incr(key, -delta)

    async def _incr(self, namespace: bytes, key: str | bytes, delta: int) -> int | None:
        """The namespace-carrying implementation behind incr()/decr() and
        Namespace.incr()/Namespace.decr() (issue #129) — see
        _get_bytes()."""
        key_bytes = _to_bytes(key)
        _check_key(key_bytes, namespace)
        _check_delta(delta)
        await self._before_operation()
        return await self._with_wrong_node_retry(
            lambda: self._incr_write(namespace, key_bytes, delta)
        )

    async def _incr_write(self, namespace: bytes, key: bytes, delta: int) -> int | None:
        """Sends ``i`` to the primary owner only, then — only once that
        succeeds with a real value — forwards its literal result to the
        remaining owners as an ordinary set() (issue #129): replaying the
        increment on a replica instead would let it drift from the
        primary (e.g. an earlier replica-leg write dropped after a
        transient failure, or the replica separately evicting and
        resetting the key), exactly the reasoning the node's own
        migration/decommission-handoff logic uses server-side. A miss or
        NotNumericError from the primary is returned/raised directly
        without ever touching a replica — nothing was written. Wrapped by
        _incr() in the same _with_wrong_node_retry every other write
        uses, so an unreachable or `W`-answering primary gets one retry
        against freshly refreshed owners — but that retry re-enters this
        method from scratch, so it only ever redoes the primary leg
        (nothing has been sent to any replica yet at that point)."""
        if self._ring is None:
            connection = await self._single_connection()
            outcome = await connection.incr(key, delta, namespace)
            return None if outcome is None else int(outcome[0])

        names = self._owner_names(namespace, key)
        if not names:
            raise ConnectionError("nanocached: no owner is reachable for this key")
        primary, replicas = names[0], names[1:]

        connection = await self._member_connection(primary)
        outcome = await connection.incr(key, delta, namespace)
        if outcome is None:
            return None
        value_bytes, ttl_seconds = outcome
        if replicas:
            await self._replicate_incr_result(
                replicas, namespace, key, value_bytes, ttl_seconds or 0
            )
        return int(value_bytes)

    async def _replicate_incr_result(
        self, names: list[str], namespace: bytes, key: bytes, value: bytes, ttl_seconds: int
    ) -> None:
        """Fans the primary incr's literal new value out to ``names``
        (the remaining owners) as an ordinary set() — see
        _incr_write()'s docstring for why the increment itself is never
        replayed on a replica. Reuses _dispatch_replica_writes, the exact
        same machinery _write()'s own replica legs use: honors
        fire_and_forget_replicas, the shared
        _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES cap and
        _background_replica_writes pool, and counts a swallowed leg in
        stats().replica_write_failures. Unlike _write(), there is no
        primary leg here that could still fail alongside a replica bug —
        the increment already succeeded — so a genuine programming error
        escaping a synchronous leg is only ever warned about, never
        raised; the caller's already-successful incr() must not be
        reported as failed because of it."""
        tasks = self._dispatch_replica_writes(
            names, lambda connection: connection.set(key, value, ttl_seconds, namespace)
        )
        if not tasks:
            return
        for outcome in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(outcome, BaseException):
                _warn(f"nanocached: a replica write raised an unexpected error: {outcome!r}")

    # ── Compare-and-set (issue #141) ──────────────────────────────────
    #
    # k/x are conditioned writes: only the key's primary owner ever
    # evaluates <cond> (docs/protocol.html "k / x"), exactly incr()'s own
    # replication rule above — on success, the SDK forwards the primary's
    # literal result to the remaining owners as an ordinary set()/delete(),
    # never by replaying k/x itself (a replica evaluating the same
    # condition against its own possibly-different copy could reach a
    # different outcome). A condition mismatch is a plain ``False``
    # return, never an exception — the same idiom delete() already uses
    # for "nothing here to act on". Not a distributed lock: LRU eviction
    # reclaims a key exactly as it would after a plain set(), CAS or not
    # — see README.md's "Compare-and-set" section.

    async def put_if_absent(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> bool:
        """Stores ``value`` only if ``key`` is currently absent (including
        lazily expired) — the ``A``-conditioned ``k``, aka
        ``add``/``putIfAbsent``. Returns ``True`` if stored, ``False`` if
        the key already existed. ``ttl_seconds`` means exactly what it
        means for set() (0 = no expiry)."""
        return await self._cas_set(b"", key, value, CAS_ABSENT, ttl_seconds)

    async def replace_if_present(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> bool:
        """Stores ``value`` only if ``key`` currently holds any (unexpired)
        value, whatever it is — the ``P``-conditioned ``k``, the
        two-argument ``replace(key, value)``. Returns ``True`` if
        replaced, ``False`` if the key was absent."""
        return await self._cas_set(b"", key, value, CAS_PRESENT, ttl_seconds)

    async def replace(
        self, key: str | bytes, token: str, new_value: str | bytes, *, ttl_seconds: int = 0
    ) -> bool:
        """Stores ``new_value`` only if ``key``'s current stored bytes
        match ``token`` exactly — the digest-conditioned ``k``, the
        three-argument ``replace(key, old, new)``. ``token`` is the
        32-character hex digest content_digest() produces, normally
        obtained from a prior get_with_token() call on this same key —
        that read-then-write-back path is always correct. Reconstructing
        a token from a value the caller already holds instead (skipping
        the read) is only correct if that reconstruction reproduces
        byte-identical wire content to what the server actually stores —
        exactly as sensitive to encoding as memcached's own value-based
        CAS, and not guaranteed at all once client-side compression
        differs between the writer and whoever built the token. Returns
        ``True`` if replaced, ``False`` on any mismatch (key absent, or
        its current value doesn't match ``token``)."""
        return await self._cas_set(b"", key, new_value, _check_token(token), ttl_seconds)

    async def delete_if_matches(self, key: str | bytes, token: str) -> bool:
        """Removes ``key`` only if its current stored bytes match
        ``token`` exactly — the digest-conditioned ``x``, the
        two-argument ``remove(key, old)``. Returns ``True`` if deleted,
        ``False`` on any mismatch (key absent, or its current value
        doesn't match ``token``) — see replace()'s docstring for where
        ``token`` comes from."""
        return await self._cas_delete(b"", key, token)

    async def _cas_set(
        self, namespace: bytes, key: str | bytes, value: str | bytes, cond: bytes, ttl_seconds: int
    ) -> bool:
        """The namespace-carrying implementation behind
        put_if_absent()/replace_if_present()/replace() and their
        Namespace counterparts (issue #141) — see _get_bytes(). Validates
        and compresses exactly like _set() (the new value written by a
        successful ``k`` must go through this client's own compression
        pipeline, or a later plain get() would fail to decompress it)."""
        if not isinstance(ttl_seconds, int) or ttl_seconds < 0:
            raise ValueError(f"nanocached: ttl_seconds must be a non-negative integer, got {ttl_seconds}")
        key_bytes, value_bytes = _to_bytes(key), _to_bytes(value)
        _check_key_and_value(key_bytes, value_bytes, namespace)
        if self._compress:
            value_bytes = compress_value(value_bytes, self._compression_threshold)
            _check_key_and_value(key_bytes, value_bytes, namespace)
        await self._before_operation()
        return await self._with_wrong_node_retry(
            lambda: self._cas_set_write(namespace, key_bytes, value_bytes, cond, ttl_seconds)
        )

    async def _cas_set_write(
        self, namespace: bytes, key: bytes, value: bytes, cond: bytes, ttl_seconds: int
    ) -> bool:
        """Sends ``k`` to the primary owner only, then — only on success —
        forwards its literal new value to the remaining owners as an
        ordinary set() (issue #141): mirrors _incr_write()'s docstring
        for why the write is never replayed on a replica. A mismatch
        (``False``) from the primary is returned directly without ever
        touching a replica — nothing was written. Wrapped by _cas_set()
        in the same _with_wrong_node_retry every other write uses."""
        if self._ring is None:
            connection = await self._single_connection()
            return await connection.cas_set(key, value, cond, ttl_seconds, namespace)

        names = self._owner_names(namespace, key)
        if not names:
            raise ConnectionError("nanocached: no owner is reachable for this key")
        primary, replicas = names[0], names[1:]

        connection = await self._member_connection(primary)
        stored = await connection.cas_set(key, value, cond, ttl_seconds, namespace)
        if not stored:
            return False
        if replicas:
            await self._replicate_cas_set_result(replicas, namespace, key, value, ttl_seconds)
        return True

    async def _replicate_cas_set_result(
        self, names: list[str], namespace: bytes, key: bytes, value: bytes, ttl_seconds: int
    ) -> None:
        """See _replicate_incr_result() — identical fan-out machinery,
        just forwarding ``k``'s literal result instead of ``i``'s."""
        tasks = self._dispatch_replica_writes(
            names, lambda connection: connection.set(key, value, ttl_seconds, namespace)
        )
        if not tasks:
            return
        for outcome in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(outcome, BaseException):
                _warn(f"nanocached: a replica write raised an unexpected error: {outcome!r}")

    async def _cas_delete(self, namespace: bytes, key: str | bytes, token: str) -> bool:
        """The namespace-carrying implementation behind
        delete_if_matches() and Namespace.delete_if_matches() (issue
        #141) — see _get_bytes()."""
        key_bytes = _to_bytes(key)
        _check_key(key_bytes, namespace)
        cond = _check_token(token)
        await self._before_operation()
        return await self._with_wrong_node_retry(
            lambda: self._cas_delete_write(namespace, key_bytes, cond)
        )

    async def _cas_delete_write(self, namespace: bytes, key: bytes, cond: bytes) -> bool:
        """Sends ``x`` to the primary owner only, then — only on success —
        forwards a plain delete() to the remaining owners (issue #141) —
        see _cas_set_write()."""
        if self._ring is None:
            connection = await self._single_connection()
            return await connection.cas_delete(key, cond, namespace)

        names = self._owner_names(namespace, key)
        if not names:
            raise ConnectionError("nanocached: no owner is reachable for this key")
        primary, replicas = names[0], names[1:]

        connection = await self._member_connection(primary)
        deleted = await connection.cas_delete(key, cond, namespace)
        if not deleted:
            return False
        if replicas:
            await self._replicate_cas_delete_result(replicas, namespace, key)
        return True

    async def _replicate_cas_delete_result(
        self, names: list[str], namespace: bytes, key: bytes
    ) -> None:
        """See _replicate_incr_result() — identical fan-out machinery,
        forwarding an ordinary delete() instead of a set()."""
        tasks = self._dispatch_replica_writes(
            names, lambda connection: connection.delete(key, namespace)
        )
        if not tasks:
            return
        for outcome in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(outcome, BaseException):
                _warn(f"nanocached: a replica write raised an unexpected error: {outcome!r}")

    async def clear_all(self) -> None:
        """Drops every namespace on every node, the default namespace
        included (issue #106) — the whole-store flush counterpart to
        Namespace.clear(), which drops just one. Raises
        AlreadyClosedError after close(), like get/set/delete. See
        _fan_out_clear() for the fan-out and failure semantics."""
        await self._before_operation()
        await self._fan_out_clear(lambda connection: connection.clear_all())

    async def _clear(self, namespace: bytes) -> None:
        """The namespace-carrying implementation behind Namespace.clear()
        (issue #106) — namespace() never duplicates this client's
        networking, it just calls in here with its own namespace (see
        namespace()'s docstring). An empty namespace clears the default
        namespace (`c 0\\n`) rather than being rejected, per the issue
        #106 spec."""
        _check_namespace(namespace)
        await self._before_operation()
        await self._fan_out_clear(lambda connection: connection.clear(namespace))

    async def _fan_out_clear(self, op) -> None:
        """Shared by clear_all() and _clear() (issue #106): a clear isn't
        key-addressed like get/set/delete — a namespace's keys are spread
        over every node by HRW, so ``op`` (already bound to either
        Connection.clear(namespace) or Connection.clear_all()) is sent to
        every currently-known node concurrently, mirroring _write()'s
        replica fan-out. It only succeeds once every node has acked `C`;
        a node that fails (connection error, timeout, or any other
        non-`C` answer — Connection.clear()/clear_all() already turn
        those into a raised error) triggers exactly one node-list refresh
        (the same forced-refresh path a `W`/dead primary uses,
        _maybe_refresh(force=True)) and one retry against every node of
        the *refreshed* list — mirroring _with_wrong_node_retry's
        "refresh once, retry once" contract, just fanned out across every
        node instead of a single owner. A node still failing after that
        retry is named in the raised error; the whole operation is
        idempotent (dropping an already-empty namespace is a no-op on the
        server), so a caller can simply retry it. In standalone (single
        node, no discovery) mode there is nothing to refresh from, so a
        failure there — like _with_wrong_node_retry's own single-mode
        case — just propagates immediately instead of retrying."""
        failures = await self._clear_round(op)
        if not failures:
            return

        if self._ring is None:
            _, error = failures[0]
            raise error

        await self._maybe_refresh(force=True)
        failures = await self._clear_round(op)
        if not failures:
            return

        names = ", ".join(name for name, _ in failures)
        raise NanocachedError(f"nanocached: clear failed on: {names}")

    async def _clear_round(self, op) -> list[tuple[str, BaseException]]:
        """One fan-out pass for _fan_out_clear(): every node gets ``op``
        concurrently, with each failure captured here instead of raised
        immediately — a connection-level exception on one node must not
        cancel the others still in flight, the same reasoning
        asyncio.gather(..., return_exceptions=True) applies to a write's
        replica legs. Standalone (self._ring is None) mode degenerates to
        the one connection there is."""
        if self._ring is None:
            try:
                await op(await self._single_connection())
            except _SWALLOWABLE_ERRORS as error:
                return [(self._single_address or "?", error)]
            return []

        async def attempt(name: str) -> tuple[str, BaseException | None]:
            try:
                await op(await self._member_connection(name))
            except _SWALLOWABLE_ERRORS as error:
                return name, error
            return name, None

        outcomes = await asyncio.gather(*(attempt(name) for name in self._members))
        return [(name, error) for name, error in outcomes if error is not None]

    def namespace(self, ns: str | bytes) -> "Namespace":
        """A lightweight handle scoped to ``ns`` (issue #105): its
        get/get_bytes/set/delete are this client's own, just with every
        key implicitly scoped to ``ns`` — same routing (HRW over
        (namespace, key)), replication fan-out, hedged reads, ``W``
        refresh-and-retry, response tags, and compression as calling this
        client directly. Its clear() (issue #106) fans out across every
        node instead — see _fan_out_clear(). ``ns`` accepts the same
        key-ish types as a key (a ``str`` is UTF-8 encoded); no length
        limit beyond the request-size rules already applied to key+value
        (_check_key).

        ``namespace("")`` returns a handle equivalent to this client
        itself — it hashes and encodes identically to the un-namespaced
        form (the SDK-wide rule that the default namespace always speaks
        the legacy G/S/D frames, never g/s/d), so it is not rejected.

        The handle is cheap (it holds no state of its own beyond the
        namespace bytes and a reference to this client) and shares this
        client's connections; it does not become invalid until this
        client does, at which point its operations raise the same
        AlreadyClosedError get/set/delete already would."""
        return Namespace(self, _to_bytes(ns))

    async def close(self) -> None:
        """Idempotent; later get/set/delete raise AlreadyClosedError. A
        second close() is usually a sign the caller lost track of this
        instance's lifecycle, so — unlike the first — it warns.

        Returns only after every in-flight background replica write has
        finished and the connections are torn down (fire-and-forget replica writes as
        amended by issue #47 item 3 — the drain contract every SDK now
        shares); a coroutine since then, the same shape aiohttp's
        ``ClientSession.close`` has. This SDK also waits for any in-flight
        background node-list refresh or redial (issue #1) to finish before
        returning — a guarantee the other SDKs' close() does not make."""
        if self._closed:
            _warn("nanocached: close() called again on an already-closed client")
            return
        self._closed = True
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()

        # A node-list refresh or a redial (issue #1) can still be running
        # in the background — shielded from whichever caller originally
        # triggered it, per asyncio.shield's own doc comment on _redial —
        # after that caller has already moved on (e.g. a timed-out
        # asyncio.wait_for). Drained here the same way
        # _background_replica_writes already is: awaited (not cancelled),
        # so a redial that's about to succeed still gets to adopt its
        # connection — which _teardown() below then actually closes,
        # rather than leaking it past close() — and so its exception, if
        # any, is retrieved here instead of surfacing later as an
        # "exception was never retrieved" warning with nothing left to
        # report it to.
        #
        # The keepalive task is gathered here too (issue #189): unlike
        # the redial/refresh tasks it IS cancelled just above, but that
        # cancel() only schedules the cancellation — nothing before this
        # ever awaited the task itself, so without this it stayed
        # pending past close() and the loop teardown that usually follows
        # could log "Task was destroyed but it is pending". gather()'s
        # return_exceptions=True absorbs the resulting CancelledError the
        # same way it absorbs a redial/refresh failure above.
        pending = list(self._redials.values())
        if self._refresh_task is not None:
            pending.append(self._refresh_task)
        if self._keepalive_task is not None:
            pending.append(self._keepalive_task)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        # Looped, not a single gather: every registration site rechecks
        # self._closed immediately before adding to this set (issue #47
        # audit item 4), so nothing should slip in after it's set above —
        # but draining in a loop, like Go's Close() re-checking its
        # WaitGroup, means anything that did would still be awaited here
        # instead of leaking past close() and surfacing later as an
        # "exception was never retrieved" warning with nothing left to
        # report it to.
        #
        # Each task is removed from the set here, by this loop, before it
        # is awaited — never left to the add_done_callback(discard) hook
        # (issue #65). That hook runs via call_soon, i.e. only when the
        # event loop gets control back; but awaiting an already-finished
        # task (or, on Python 3.12+, a gather() of only finished tasks)
        # completes synchronously without ever yielding to the loop. So
        # a "while set: await gather(set)" whose every member had
        # finished, with their discard hooks still queued, re-checked a
        # never-shrinking set forever — a synchronous spin that froze the
        # whole event loop (every other coroutine in the process, the
        # request-timeout timers, the read loops), not just close().
        await _drain_tasks(self._background_replica_writes)
        await _drain_tasks(self._hedged_reads)

        self._teardown()

    async def __aenter__(self) -> "NanocachedClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _teardown(self) -> None:
        if self._single is not None:
            self._single.close()
        for member in self._members.values():
            if member.connection is not None:
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

    def _owner_names(self, namespace: bytes, key: bytes) -> list[str]:
        """HRW routing over (namespace, key) (issue #105) — an empty
        namespace hashes byte-identically to the pre-namespace form (see
        _hashring.key_hash), so an un-namespaced call site's routing is
        unchanged."""
        assert self._ring is not None
        return self._ring.owners(key, self._replication, namespace=namespace)

    async def _read(self, namespace: bytes, key: bytes, op):
        if self._ring is None:
            return await op(await self._single_connection())

        names = self._owner_names(namespace, key)
        if self._read_hedge_after is not None and len(names) > 1:
            return await self._read_hedged(key, op, names)

        # Owners in rank order; fall through only on connection-level
        # failure — a replica hedges against a dead holder, not a miss.
        last_error: Exception | None = None
        for name in names:
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

    async def _read_hedged(self, key: bytes, op, names: list[str]):
        """Hedged reads (issue #64): one slow — not dead — owner otherwise
        bounds every read that touches it at its full RTT, since the
        sequential path in _read() only moves on to the next owner when
        the current one *fails*. Here the read starts at the primary, and
        if no answer has arrived within ``read_hedge_after`` the same read
        is also sent to the next owner (and so on, one more owner per
        interval); the first answer decides:

        - a hit from any owner is final;
        - a miss is final only from the primary — a replica's miss is
          provisional (it may simply lack a copy) and the primary is still
          waited for, so hedging never turns a hit into a miss; it is
          accepted only once every owner has answered or failed;
        - a failure (connection-level or a NanocachedError other than
          WrongNodeError) just hedges onward immediately;
        - WrongNodeError propagates as in _read().

        The losing legs are never cancelled — cancelling mid-write poisons
        a connection (see Connection._request) — but left to finish
        detached in ``_hedged_reads``, their outcome retrieved, and
        drained by close()."""
        hedge_after = self._read_hedge_after
        assert hedge_after is not None
        legs: dict[asyncio.Task[object], int] = {}

        def start(index: int) -> asyncio.Task[object]:
            # Recheck self._closed immediately before registering, exactly
            # as the fire_and_forget_replicas path does (issue #47 item 4),
            # so a read racing close() can't add a leg that close()'s drain
            # has already passed (issue #91): close() sets self._closed
            # before draining _hedged_reads, and this check-and-add is one
            # synchronous critical section (asyncio is single-threaded), so
            # once self._closed is set no further leg is ever added and the
            # drain sees a set that only shrinks. A read that lost this race
            # fails the same way _before_operation would have.
            if self._closed:
                raise AlreadyClosedError()

            async def leg() -> object:
                connection = await self._member_connection(names[index])
                return await op(connection)

            task = asyncio.ensure_future(leg())
            task.add_done_callback(_retrieve_outcome)
            legs[task] = index
            self._hedged_reads.add(task)
            return task

        def detach(tasks: set[asyncio.Task[object]]) -> None:
            # Left in _hedged_reads for close() to drain; removed from it
            # on completion so the set doesn't grow with finished legs.
            for task in tasks:
                task.add_done_callback(self._hedged_reads.discard)

        pending: set[asyncio.Task[object]] = {start(0)}
        next_index = 1
        last_error: Exception | None = None
        replica_missed = False

        while pending:
            timeout = hedge_after if next_index < len(names) else None
            done, pending = await asyncio.wait(
                pending, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                # Hedge interval elapsed with no answer: one more owner.
                pending.add(start(next_index))
                next_index += 1
                continue
            for task in done:
                self._hedged_reads.discard(task)
                index = legs[task]
                try:
                    value = task.result()
                except WrongNodeError:
                    detach(pending)
                    raise
                except (NanocachedError, ConnectionError, OSError) as error:
                    last_error = error
                    continue
                if value is not None or index == 0:
                    detach(pending)
                    return value
                replica_missed = True
            if not pending and next_index < len(names):
                # Everything so far failed or missed provisionally: the
                # next owner gets its turn right away.
                pending.add(start(next_index))
                next_index += 1

        if replica_missed:
            return None
        raise last_error if last_error is not None else ConnectionError(
            "nanocached: no owner is reachable for this key"
        )

    def _dispatch_replica_writes(self, names: list[str], op) -> list[asyncio.Task[None]]:
        """Admits each name in ``names`` as a replica leg running ``op`` —
        the fan-out machinery shared by _write()'s own replica legs and
        incr()'s result-forwarding replicate step (NanocachedClient.incr,
        issue #129): a leg that fits under fire_and_forget_replicas' own
        _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES cap (and close() hasn't
        started) runs detached in self._background_replica_writes; every
        other leg's task is returned here for the caller to await
        synchronously. Every leg swallows _SWALLOWABLE_ERRORS internally
        and counts it in stats().replica_write_failures (client-side
        replication) — only a genuine programming error can still surface
        in a returned task's result."""

        async def replica_write(name: str) -> None:
            try:
                await op(await self._member_connection(name))
            except _SWALLOWABLE_ERRORS:
                # Swallowed by design (client-side replication): a dead or disagreeing
                # replica leaves the key under-replicated until the next
                # node-list refresh, never fails the write. Counted in
                # stats().replica_write_failures, whether this leg ran
                # synchronously or as a fire_and_forget_replicas
                # background write — both paths share this function. A
                # genuine programming error still escapes this except
                # clause (see the callers' own handling of a returned
                # task's outcome).
                self._replica_write_failures += 1

        replica_tasks: list[asyncio.Task[None]] = []
        for name in names:
            # Fire-and-forget replica writes: with fire_and_forget_replicas, up to
            # _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES legs — shared with
            # read-repair write-backs, see _background_replica_writes'
            # own comment in __init__ (issue #47 audit item 5) — run in
            # the background instead of being returned for the caller to
            # await. Past that cap, or once close() has started
            # (self._closed, rechecked here immediately before
            # registering the task — close() sets it before draining, so
            # a write still in flight when close() runs must not add an
            # entry that drain has already, or is about to have, taken
            # its snapshot of), further legs fall back to the synchronous
            # path exactly as with the option off.
            if (
                self._fire_and_forget_replicas
                and not self._closed
                and len(self._background_replica_writes) < _MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES
            ):
                task = asyncio.ensure_future(replica_write(name))
                self._background_replica_writes.add(task)
                task.add_done_callback(self._background_replica_writes.discard)
                continue
            replica_tasks.append(asyncio.ensure_future(replica_write(name)))
        return replica_tasks

    async def _write(self, namespace: bytes, key: bytes, op):
        if self._ring is None:
            return await op(await self._single_connection())

        names = self._owner_names(namespace, key)
        if not names:
            raise ConnectionError("nanocached: no owner is reachable for this key")
        primary, replicas = names[0], names[1:]

        replica_tasks = self._dispatch_replica_writes(replicas, op)

        primary_error: BaseException | None = None
        try:
            result = await op(await self._member_connection(primary))
        except asyncio.CancelledError:
            # The caller gave up on this call (asyncio.wait_for, task
            # cancellation): propagate right away instead of first
            # waiting on every synchronous replica leg below — those
            # finish on their own, and swallow their own failures. But
            # leaving replica_tasks as pure orphans here would break
            # close()'s drain contract (issue #189): they are already-
            # running asyncio Tasks that nothing else references, so a
            # leg that would have succeeded can instead get its
            # connection yanked out from under it by _teardown() once
            # close() runs. Hand them to the same background-replica-
            # write pool close() already drains via
            # _drain_tasks(self._background_replica_writes) — exactly as
            # if fire-and-forget dispatch had picked them up in the
            # first place (_dispatch_replica_writes above) — so close()
            # still waits for them instead of poisoning a write that was
            # already on its way to succeeding.
            for replica_task in replica_tasks:
                self._background_replica_writes.add(replica_task)
                replica_task.add_done_callback(self._background_replica_writes.discard)
            raise
        except BaseException as error:  # noqa: BLE001 - decided below, not swallowed
            result = None
            primary_error = error

        replica_bug: BaseException | None = None
        if replica_tasks:
            # replica_write() already fully handles every expected
            # failure internally (caught by _SWALLOWABLE_ERRORS, counted
            # in stats().replica_write_failures, never re-raised), so the
            # only thing that can still escape a task here is a genuine
            # programming error. It must not be silently dropped — but
            # must never override an already-successful primary write
            # (the write happened, so raising here would misreport a
            # completed write as failed): return_exceptions=True keeps
            # that from happening; the bug is only ever surfaced this way
            # — instead of just via a warning — when the primary itself
            # also failed, same as before. Mirrors the TypeScript SDK's
            # writeToOwners: `replicaBug ? replicaBug.reason :
            # primary.error` (see its own comment for the full
            # reasoning). Only the first such bug is surfaced this way,
            # same as TypeScript; any further ones are still warned.
            for outcome in await asyncio.gather(*replica_tasks, return_exceptions=True):
                if not isinstance(outcome, BaseException):
                    continue
                if primary_error is not None and replica_bug is None:
                    replica_bug = outcome
                else:
                    _warn(f"nanocached: a replica write raised an unexpected error: {outcome!r}")

        if primary_error is not None:
            raise replica_bug if replica_bug is not None else primary_error
        return result

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
        if member.connection is not None and not member.connection.closed:
            return member.connection
        return await self._redial(name, member.address)

    async def _redial(self, slot: str, address: str) -> Connection:
        """Concurrent requests finding the same dead connection share one
        dial instead of each opening a socket. The shared task itself
        adopts the fresh connection into client state (issue #10): with
        adoption in the awaiting caller, a cancelled caller (e.g.
        asyncio.wait_for around a client call) would abandon the
        shield-protected task's connection unreferenced — a leaked
        socket. For the same reason, the in-flight entry is cleared by
        the dial task's own done-callback rather than by whichever
        awaiter happens to unwind first: if the originating awaiter is
        cancelled while asyncio.shield keeps the dial running, the entry
        must stay put until that dial actually finishes, or a later
        caller would start a redundant second dial to the same address."""
        in_flight = self._redials.get(slot)
        if in_flight is not None:
            return await asyncio.shield(in_flight)

        # Per-address reconnect cooldown: an address whose dial just
        # failed stays "down" for self._reconnect_cooldown seconds, so a
        # burst of calls routed to it — or one call every keep-alive tick
        # — fails immediately with the same error the dial itself
        # produced, instead of each paying another full CONNECT_DEADLINE
        # (5s, _identify.py) in turn.
        cooldown = self._redial_cooldowns.get(address)
        if cooldown is not None:
            until, error = cooldown
            if time.monotonic() < until:
                # Reset the traceback before re-raising: re-raising the
                # *stored* exception instance splices a fresh traceback
                # segment onto it on every cooldown hit, so a burst of
                # calls to a down address (or one per keep-alive tick)
                # would grow it without bound for the life of the
                # cooldown (#96). with_traceback(None) returns the same
                # instance, so the caller still sees its original type
                # and message.
                raise error.with_traceback(None)

        task = asyncio.ensure_future(self._open_and_adopt(slot, address))
        self._redials[slot] = task

        def _on_redial_done(t: "asyncio.Task[Connection]") -> None:
            if self._redials.get(slot) is t:
                self._redials.pop(slot, None)
            if t.cancelled():
                return
            error = t.exception()
            if error is not None:
                self._redial_cooldowns[address] = (time.monotonic() + self._reconnect_cooldown, error)
            else:
                self._redial_cooldowns.pop(address, None)

        task.add_done_callback(_on_redial_done)
        return await asyncio.shield(task)

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
        if current.connection is None or current.connection.closed:
            current.connection = connection
            return connection
        if current.connection is not connection:
            connection.close()
        return current.connection

    async def _open_node_connection(self, address: str) -> Connection:
        node_host, node_port = split_host_port(address)
        try:
            identified = await connect_and_identify(node_host, node_port, self._auth_secret, self._ssl_context)
        except (NanocachedError, OSError):
            # SDK proxy mode (issue #122) reconnect: this is only ever
            # reached via _redial("", self._single_address) — "first
            # retry the same proxy (it may have restarted)" — so a dial
            # failure here specifically means *that* retry just failed;
            # fall back to re-fetching the roster and picking another,
            # instead of endlessly retrying a proxy that may be gone for
            # good. The `address == self._single_address` guard is belt
            # and braces: in via_proxy mode this method is never called
            # any other way (there are no cluster members to redial), but
            # matching only the pinned address keeps that assumption from
            # silently mattering here.
            if self._via_proxy and address == self._single_address:
                return await self._reconnect_via_new_proxy()
            raise
        if not isinstance(identified, NodeTarget):
            raise NanocachedError(f"nanocached: {address} no longer identifies as a cache node")
        if self._closed:
            identified.writer.close()
            raise AlreadyClosedError()
        return self._new_connection(identified.reader, identified.writer, identified.tagged)

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
                stale = self._members[name].connection
                if stale is not None:
                    stale.close()
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
                    # Refresh is opportunistic/best-effort and must never
                    # fail the caller's operation (client-side replication's
                    # eventual-consistency model) — this node is just
                    # skipped, counted in stats().refresh_failures.
                    self._refresh_failures += 1
                    continue
                if self._closed:
                    # close() ran while we were dialing (issue #10):
                    # installing this socket now would leak it.
                    target.writer.close()
                    return
                self._members[node.name] = _Member(
                    node.address, self._new_connection(target.reader, target.writer, target.tagged)
                )
            except _SWALLOWABLE_ERRORS:
                # Same rationale as above: never fail the caller over a
                # refresh-time connect failure. A genuine programming
                # error (anything outside _SWALLOWABLE_ERRORS) still
                # propagates.
                self._refresh_failures += 1

        if self._closed:
            self._teardown()
            return

        # Drop reconnect-cooldown entries for addresses whose owning node
        # has left the cluster: in a churny deployment (a fresh IP:port on
        # every restart) they would otherwise accumulate one dead entry
        # per departed address forever (#96).
        live_addresses = {node.address for node in cluster.nodes}
        for cooled_address in list(self._redial_cooldowns):
            if cooled_address not in live_addresses:
                del self._redial_cooldowns[cooled_address]

        self._ring = HashRing(list(self._members))
        self._replication = cluster.replication

    async def _fetch_node_list(self) -> ClusterTarget | None:
        """Walks every address (discovery HA); None means keep the last-known
        list. Failures here are silent by design — refresh is
        opportunistic/best-effort and must never fail the caller's
        operation (client-side replication's eventual-consistency model), so behavior is
        unchanged either way, it just happens without a warning. Each
        failed address is counted in stats().refresh_failures."""
        for address_host, address_port in self._addresses:
            try:
                identified = await connect_and_identify(
                    address_host, address_port, self._auth_secret, self._ssl_context
                )
            except _SWALLOWABLE_ERRORS:
                # A genuine programming error still propagates.
                self._refresh_failures += 1
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
                    else [
                        member.connection
                        for member in self._members.values()
                        if member.connection is not None
                    ]
                )
                for connection in connections:
                    if connection is None or connection.closed:
                        continue  # dead connections stay lazy, redialed on use
                    if connection.idle_seconds() < interval:
                        continue  # real traffic already reset the server's timer
                    try:
                        # Any parseable reply proves liveness — `N`, or `W`
                        # from a non-owner — and resets the idle timer. G
                        # refreshes the server's LRU recency of whatever
                        # key it names, which is exactly why
                        # _KEEPALIVE_KEY has to be a sequence reserved by
                        # the SDKs: a real application key would have had
                        # its recency silently reset every tick.
                        await connection.get(_KEEPALIVE_KEY)
                    except Exception:
                        pass

        self._keepalive_task = asyncio.ensure_future(ping_loop())


class Namespace:
    """A lightweight, namespace-scoped view of a NanocachedClient (issue
    #105, SDK parity issue #25) — returned by NanocachedClient.namespace(),
    see its docstring for the full contract. Holds nothing but the owning
    client and the namespace's already-encoded bytes; every operation just
    forwards to that client's own (namespace, key)-taking internals
    (_get_bytes/_get/_set/_delete), so this never duplicates the client's
    networking, routing, replication, or retry logic — it only ever
    supplies a namespace those already know how to carry."""

    def __init__(self, client: NanocachedClient, namespace: bytes) -> None:
        self._client = client
        self._namespace = namespace

    @property
    def namespace(self) -> bytes:
        """The raw bytes this handle scopes every key to — the same bytes
        that go on the wire and into HRW routing. Useful for the
        framework adapters built on top of this handle (#107/#108)."""
        return self._namespace

    async def get_bytes(self, key: str | bytes) -> bytes | None:
        """Same as NanocachedClient.get_bytes(), scoped to this namespace."""
        return await self._client._get_bytes(self._namespace, key)

    async def get_with_token(self, key: str | bytes) -> tuple[bytes, str] | None:
        """Same as NanocachedClient.get_with_token(), scoped to this
        namespace (issue #141)."""
        return await self._client._get_with_token(self._namespace, key)

    async def get(self, key: str | bytes) -> str | None:
        """Same as NanocachedClient.get(), scoped to this namespace."""
        return await self._client._get(self._namespace, key)

    async def set(self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0) -> None:
        """Same as NanocachedClient.set(), scoped to this namespace."""
        await self._client._set(self._namespace, key, value, ttl_seconds=ttl_seconds)

    async def delete(self, key: str | bytes) -> bool:
        """Same as NanocachedClient.delete(), scoped to this namespace."""
        return await self._client._delete(self._namespace, key)

    async def get_many_bytes(self, keys: Sequence[str | bytes]) -> dict[str | bytes, bytes]:
        """Same as NanocachedClient.get_many_bytes(), scoped to this
        namespace (issues #128/#150/#151)."""
        return await self._client._get_many_bytes(self._namespace, keys)

    async def get_many(self, keys: Sequence[str | bytes]) -> dict[str | bytes, str]:
        """Same as NanocachedClient.get_many(), scoped to this namespace
        (issues #128/#150/#151)."""
        return await self._client._get_many(self._namespace, keys)

    async def set_many(
        self, values: Mapping[str | bytes, str | bytes], *, ttl_seconds: int = 0
    ) -> None:
        """Same as NanocachedClient.set_many(), scoped to this namespace
        (issues #128/#150/#151)."""
        await self._client._set_many(self._namespace, values, ttl_seconds=ttl_seconds)

    async def incr(self, key: str | bytes, delta: int = 1) -> int | None:
        """Same as NanocachedClient.incr(), scoped to this namespace
        (issue #129)."""
        return await self._client._incr(self._namespace, key, delta)

    async def decr(self, key: str | bytes, delta: int = 1) -> int | None:
        """Same as NanocachedClient.decr(), scoped to this namespace
        (issue #129)."""
        return await self._client._incr(self._namespace, key, -delta)

    async def put_if_absent(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> bool:
        """Same as NanocachedClient.put_if_absent(), scoped to this
        namespace (issue #141)."""
        return await self._client._cas_set(self._namespace, key, value, CAS_ABSENT, ttl_seconds)

    async def replace_if_present(
        self, key: str | bytes, value: str | bytes, *, ttl_seconds: int = 0
    ) -> bool:
        """Same as NanocachedClient.replace_if_present(), scoped to this
        namespace (issue #141)."""
        return await self._client._cas_set(self._namespace, key, value, CAS_PRESENT, ttl_seconds)

    async def replace(
        self, key: str | bytes, token: str, new_value: str | bytes, *, ttl_seconds: int = 0
    ) -> bool:
        """Same as NanocachedClient.replace(), scoped to this namespace
        (issue #141)."""
        return await self._client._cas_set(
            self._namespace, key, new_value, _check_token(token), ttl_seconds
        )

    async def delete_if_matches(self, key: str | bytes, token: str) -> bool:
        """Same as NanocachedClient.delete_if_matches(), scoped to this
        namespace (issue #141)."""
        return await self._client._cas_delete(self._namespace, key, token)

    async def clear(self) -> None:
        """Drops every entry in this namespace, on every node (issue
        #106) — namespace(""), the handle equivalent to the client
        itself, clears the default namespace rather than being rejected.
        See NanocachedClient.clear_all() for the whole-store flush
        counterpart, and _fan_out_clear() for the fan-out and failure
        semantics both share."""
        await self._client._clear(self._namespace)
