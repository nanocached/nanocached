"""``django.core.cache.backends.base.BaseCache`` on top of the nanocached
Python SDK (issue #108, following the Spring adapter's #107 pattern) —
one ``NanocachedCache`` instance is one namespace (``OPTIONS.NAMESPACE``,
default ``"django"``), so ``clear()`` is that namespace's ``CLEAR`` and
never the whole store, and two aliases with different namespaces never
collide even when they share a node (issue #105/#106; see
``nanocached.Namespace``).

The sync/async bridge: Django's cache SPI is sync, the SDK is
asyncio-only. Each backend instance owns a dedicated daemon-thread event
loop, started lazily on first use and driven from any calling thread via
``asyncio.run_coroutine_threadsafe`` — never ``asyncio.run()`` per call,
which would reconnect the SDK client (and redo its handshake) on every
single cache operation instead of reusing one persistent connection.
Django's own ``aget``/``aset``/... come for free from ``BaseCache``'s
``sync_to_async(..., thread_sensitive=True)`` wrappers around the sync
methods below, so this module implements no native async path itself —
doing so would just be a second bridge fighting the first.

Lifecycle note: Django connects ``close_caches()`` to the
``request_finished`` signal unconditionally (``django/core/cache/
__init__.py``), which calls ``.close()`` on every cache alias a thread has
already touched — after every single request. Honoring that literally
would tear down and re-open this backend's loop thread and SDK connection
(TCP + auth, plus a discovery refresh in cluster mode) once per request,
defeating the persistent connections the whole client design is built
around. So, like django-redis, ``close()`` is a no-op by default; opt in
to per-request teardown with ``OPTIONS: {"CLOSE_ON_REQUEST": True}`` if
short-lived processes matter more than connection reuse, and use
``shutdown()`` for the explicit, unconditional teardown (process exit,
tests).
"""

from __future__ import annotations

import asyncio
import math
import os
import pickle
import re
import threading
import weakref

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.exceptions import ImproperlyConfigured

from nanocached import (
    CompressionIncompatibleError,
    NanocachedClient,
    PartialConnectionLostError,
    PartialSetConnectionLostError,
    PartialWrongNodeError,
    WrongNodeError,
)

# OPTIONS.NAMESPACE default — every key this backend touches lives under
# this namespace unless the CACHES entry overrides it (issue #105).
_DEFAULT_NAMESPACE = "django"

# issue #231: every other adapter exposes at least tls/ca/compress, but
# this one only ever read NAMESPACE/SECRET/CLOSE_ON_REQUEST, leaving a
# Django deployment stuck on plaintext with no way to opt into TLS,
# compression, fire-and-forget replication, read repair, hedged reads or
# a non-default reconnect cooldown. Maps each upper-case OPTIONS key onto
# the NanocachedClient.connect() keyword it forwards to — see that
# method's signature (sdk/python/src/nanocached/client.py) for what each
# one does and its type/default. Deliberately does *not* include
# ``via_proxy``: that flag changes which roster connect() fetches
# (proxies vs. nodes) rather than tuning an established connection, and
# LOCATION already tells this backend which addresses it was given, so
# there's no Django-level concept it would attach to.
_CONNECT_OPTION_KWARGS: dict[str, str] = {
    "TLS": "tls",
    "CA": "ca",
    "COMPRESS": "compress",
    "COMPRESSION_THRESHOLD": "compression_threshold",
    "FIRE_AND_FORGET_REPLICAS": "fire_and_forget_replicas",
    "READ_REPAIR": "read_repair",
    "READ_HEDGE_AFTER": "read_hedge_after",
    "RECONNECT_COOLDOWN": "reconnect_cooldown",
}

# issue #185: how many times _run() re-tries _ensure_started() when a
# concurrent shutdown()/close() raced it out from under a snapshot of
# (loop, namespace handle) — see _run()'s own docstring. One retry covers
# every realistic interleaving (shutdown() only runs on explicit request);
# a small bound just avoids ever looping forever if something pathological
# keeps shutting the backend down between every attempt.
_RUN_RECONNECT_ATTEMPTS = 3

# get_backend_timeout()'s sentinel for "don't cache this": Django's
# timeout=0 (or a negative timeout) means "expire immediately", but
# nanocached's wire TTL uses the *opposite* polarity — 0 there means "no
# expiry" (see NanocachedClient.set's docstring) — so there is no wire TTL
# that means "expire immediately" and this case can't be forwarded as a
# TTL at all. Distinguished from a real ttl_seconds int (which is always
# >= 1 once this sentinel doesn't apply) with `is`, the same way Django's
# own DEFAULT_TIMEOUT sentinel is compared.
_DO_NOT_CACHE = object()

_ADDRESS_SPLIT_RE = re.compile(r"[;,]")

# Issue #129: INCR needs a real counter on the wire, and a pickled int
# isn't one — the server can't add to an opaque pickle stream. So a
# plain ``int`` value (``bool`` excluded: Django, like Python, treats
# booleans as their own type, not a number a counter makes sense for) is
# stored as INCR's own canonical decimal-ASCII grammar instead of
# pickled; everything else keeps the existing pickle round trip.
#
# The two encodings never collide: pickle protocol 2+ (Python 3's
# ``HIGHEST_PROTOCOL`` always is) begins every stream with the PROTO
# opcode, ``0x80`` — a byte that can never appear in an ASCII decimal
# integer — so ``get()`` can tell which decoder to use just by looking
# at the stored bytes, with no extra marker byte needed. This also means
# a value written by an un-upgraded peer, or from before this change,
# still decodes correctly: its pickled ints just keep unpickling.
_DECIMAL_INT_RE = re.compile(rb"^-?(0|[1-9]\d*)$")


def _is_counter_value(value: object) -> bool:
    # Exactly ``int`` — not a subclass (issue #392): ``IntEnum``/
    # ``IntFlag`` (including Django's own ``models.IntegerChoices``)
    # would round-trip through the decimal fast path as a plain ``int``,
    # silently losing enum identity (``.label``, ``isinstance`` checks).
    # Such values take the pickle path instead, which preserves their
    # type; ``incr()``/``decr()`` keep requiring plain-int counters,
    # matching django.core.cache's own semantics. ``bool`` is excluded
    # for free — it's an ``int`` subclass too.
    return type(value) is int


def _encode_value(value: object) -> bytes:
    if _is_counter_value(value):
        return str(value).encode("ascii")
    return pickle.dumps(value, pickle.HIGHEST_PROTOCOL)


def _decode_value(raw: bytes) -> object:
    if _DECIMAL_INT_RE.match(raw):
        return int(raw)
    return pickle.loads(raw)


async def _delete_all(handle, cache_keys: list) -> None:
    """``delete_many``'s fan-out (issue #233): every key's ``delete()``
    dispatched concurrently on ``handle``'s own event loop, in the one
    coroutine ``_run`` awaits — a plain generator expression passed
    straight to ``asyncio.gather`` isn't itself a coroutine (``_run``'s
    ``make_coro`` must return one, for ``run_coroutine_threadsafe``), so
    this exists to be that coroutine.

    Issue #332: ``return_exceptions=True`` so one leg raising doesn't stop
    ``gather`` from waiting on the rest — plain ``gather`` re-raises the
    first exception as soon as it's seen, before the other already-running
    deletes are ever awaited, so their outcomes (including further
    exceptions) are never observed here. That leaves this coroutine
    returning while sibling deletes may still be in flight against the
    node — an ambiguous state for a fan-out that's supposed to mean "all of
    these are gone". Waiting for every leg first, then re-raising, matches
    ``set_many``'s convention on this same class (see its comment): a
    partial failure isn't collected per-key (``BaseCache.delete_many`` has
    no per-key return value to collect it into, unlike ``set_many``), it
    raises — just only after every leg has actually finished."""
    results = await asyncio.gather(*(handle.delete(cache_key) for cache_key in cache_keys), return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result


async def _get_many_bytes_resolved(handle, cache_keys: list) -> dict:
    """``get_many_bytes``, but retrying a mid-batch partial failure's
    still-unresolved remainder once instead of discarding an otherwise-
    successful batch (issue #439, mirroring cache-manager's
    ``mgetResolved`` and jcache's ``getManyBytesResolvingWrongNode``
    — see ``adapters/cache-manager/src/store.ts`` and
    ``adapters/jcache/.../NanocachedCache.java``).

    ``PartialWrongNodeError`` (a ring reconfiguration left some keys
    still wrong-node after the SDK's own one bounded refresh-and-retry)
    and ``PartialConnectionLostError`` (a later chunk's connection
    failure after an earlier one already resolved, single-node/proxy
    mode only) both carry ``partial_values`` for what DID resolve —
    without this, either would propagate straight out of ``get_many``
    and throw away every value the batch DID manage to fetch. This
    retries exactly the keys still missing from ``partial_values`` once
    and merges the result; a second failure of either kind propagates
    unchanged rather than being silently swallowed into a false miss.

    A *plain* ``WrongNodeError`` (single-node/proxy mode's immediate
    ``W``, with nothing yet resolved) is deliberately not caught here:
    there is no ring to retry against in that mode, so a retry couldn't
    behave any differently — it propagates exactly as before."""
    try:
        return await handle.get_many_bytes(cache_keys)
    except (PartialWrongNodeError, PartialConnectionLostError) as error:
        resolved = error.partial_values
        remaining = [key for key in cache_keys if key not in resolved]
        if not remaining:
            return resolved
        retried = await handle.get_many_bytes(remaining)  # a second failure propagates as-is
        merged = dict(resolved)
        merged.update(retried)
        return merged


async def _set_many_resolved(handle, payload: dict, ttl_seconds: int) -> None:
    """``set_many``, but retrying a mid-batch partial failure's
    remainder once instead of failing an otherwise-successful batch
    outright (issue #439, mirroring cache-manager's ``msetResolved`` and
    jcache's ``setManyBytesResolvingWrongNode``).

    A plain ``WrongNodeError`` (a ring reconfiguration left some keys'
    primaries still wrong-node after the SDK's own one bounded
    refresh-and-retry) carries no partial payload worth attaching —
    ``set_many`` has no value to report back beyond which keys landed —
    so this resends the WHOLE batch once; safe, since every per-key
    write is idempotent, so re-storing an already-landed key is just a
    harmless duplicate of the same value/TTL. ``PartialSetConnectionLostError``
    (a later chunk's connection failure after an earlier one already
    stored its keys, single-node/proxy mode only) DOES carry
    ``partial_keys`` for what's already confirmed stored, so this
    resends only the remainder. Either way, a second failure propagates
    unchanged rather than being silently swallowed."""
    try:
        await handle.set_many(payload, ttl_seconds=ttl_seconds)
    except PartialSetConnectionLostError as error:
        stored = error.partial_keys
        remaining = {key: value for key, value in payload.items() if key not in stored}
        if not remaining:
            return
        await handle.set_many(remaining, ttl_seconds=ttl_seconds)  # a second failure propagates as-is
    except WrongNodeError:
        await handle.set_many(payload, ttl_seconds=ttl_seconds)  # no partial payload — resend the whole batch


def _split_host_port(address: str) -> tuple[str, int]:
    """Parses one ``"host:port"`` LOCATION entry. Deliberately not reusing
    the SDK's own internal splitter (``nanocached._identify.
    split_host_port``, used for discovery-returned addresses) — that
    module is private to the SDK; this is config parsing, not wire
    framing, so a small local copy is simpler than depending on it."""
    host, separator, port = address.rpartition(":")
    if not separator or not port.isascii() or not port.isdigit():
        raise ImproperlyConfigured(
            f"nanocached_django: invalid LOCATION address {address!r}, expected \"host:port\""
        )
    return host, int(port)


def _parse_addresses(location) -> list[tuple[str, int]]:
    """LOCATION accepts what the issue spec's example shows: a list of
    ``"host:port"`` strings (cluster / discovery addresses), or a single
    ``"host:port"`` string — optionally ``;``/``,``-separated for more
    than one address in that single-string form, mirroring Django's own
    ``RedisCache`` LOCATION convention."""
    if isinstance(location, (list, tuple)):
        entries = list(location)
    elif location:
        entries = [entry for entry in _ADDRESS_SPLIT_RE.split(location) if entry]
    else:
        entries = []
    if not entries:
        raise ImproperlyConfigured(
            "nanocached_django: CACHES LOCATION must name at least one \"host:port\" address"
        )
    return [_split_host_port(entry.strip()) for entry in entries]


# issue #414: os.register_at_fork's after_in_child hook resets every
# live instance's _lifecycle_lock in the forked child. A plain
# threading.Lock is *not* automatically reinitialized by CPython after
# fork the way the interpreter's own import lock is — if some other
# thread happened to hold this lock at fork time (a threaded warm-up
# cache touch racing gunicorn ``preload_app``'s fork, or uWSGI without
# ``lazy-apps``), the child inherits it already locked with no thread
# that could ever release it, so every subsequent ``with
# self._lifecycle_lock`` in _ensure_started()/_run()/shutdown() blocks
# forever in that child — before it even reaches the issue #393
# fork-PID check above that would otherwise rebuild the loop/thread/
# client. Swapping in a fresh, unlocked Lock() per instance clears
# that regardless of whether the inherited lock happened to be held.
#
# Registered once per process (not once per instance — register_at_fork
# has no matching unregister, so a repeated call would pile up one more
# no-op hook per instance ever constructed) the first time any
# NanocachedCache is built. A WeakSet, not a strong list, so tracking an
# instance here never keeps it alive past its own last real reference.
_live_instances: "weakref.WeakSet[NanocachedCache]" = weakref.WeakSet()
_fork_hook_registration_lock = threading.Lock()
_fork_hook_registered = False


def _reset_lifecycle_locks_in_child() -> None:
    for instance in list(_live_instances):
        instance._lifecycle_lock = threading.Lock()


def _ensure_fork_hook_registered() -> None:
    global _fork_hook_registered
    if _fork_hook_registered:
        return
    with _fork_hook_registration_lock:
        if _fork_hook_registered:
            return
        # os.register_at_fork only exists on POSIX (no fork() on
        # Windows) — guarded rather than imported unconditionally so
        # this module still loads there.
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=_reset_lifecycle_locks_in_child)
        _fork_hook_registered = True


class NanocachedCache(BaseCache):
    """Usage: ``"BACKEND": "nanocached_django.NanocachedCache"`` with
    ``LOCATION`` and ``OPTIONS`` as described in the module README's Setup
    section. ``KEY_PREFIX``/``VERSION``/``TIMEOUT`` are handled by
    ``BaseCache`` itself (``make_key``/``get_backend_timeout``'s base
    default) exactly as for any other Django cache backend."""

    def __init__(self, server, params) -> None:
        super().__init__(params)
        self._addresses = _parse_addresses(server)
        options = params.get("OPTIONS", {})
        self._namespace = options.get("NAMESPACE", _DEFAULT_NAMESPACE)
        self._secret = options.get("SECRET")
        # See the module docs' lifecycle note: Django close()es every
        # touched alias after every request, so real teardown there is
        # opt-in, django-redis-style.
        self._close_on_request = bool(options.get("CLOSE_ON_REQUEST", False))
        # issue #231: only the OPTIONS keys actually present are forwarded
        # to connect() — a key this CACHES entry never mentions must leave
        # the SDK's own default in force, not silently pass e.g.
        # ``compress=False`` where connect() already defaults to that.
        self._connect_kwargs = {
            kwarg: options[option_key]
            for option_key, kwarg in _CONNECT_OPTION_KWARGS.items()
            if option_key in options
        }

        # The sync/async bridge's state — all None until _ensure_started()
        # first runs (lazily, on the first cache operation, not here:
        # __init__ runs synchronously wherever Django constructs this
        # backend, e.g. while importing settings, and must not block on
        # network I/O or spin up a thread nothing has asked for yet).
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._client: NanocachedClient | None = None
        self._namespace_handle = None
        # The PID that started (and whose thread drives) self._loop —
        # see _ensure_started's fork check (issue #393).
        self._loop_pid: int | None = None
        # Guards start/close against concurrent callers on different
        # Django worker threads racing to lazily start (or to close) the
        # same backend instance — the loop and client themselves are
        # otherwise only ever touched from _run()'s single background
        # thread.
        self._lifecycle_lock = threading.Lock()
        # issue #414: track this instance so a fork() elsewhere in the
        # process can reset its lock in the child — see
        # _reset_lifecycle_locks_in_child() above.
        _live_instances.add(self)
        _ensure_fork_hook_registered()

    # ── the sync/async bridge ───────────────────────────────────────

    def _ensure_started(self) -> None:
        if self._loop is not None and self._loop_pid == os.getpid():
            return
        with self._lifecycle_lock:
            if self._loop is not None:
                if self._loop_pid == os.getpid():
                    return
                # issue #393: this process is a fork() child (preforking
                # WSGI servers with preload — Gunicorn preload_app,
                # uWSGI without lazy-apps — where a warm-up cache touch
                # in the master started the loop before workers forked).
                # Only the forking thread survives fork(), so the thread
                # driving this loop does not exist here: every
                # run_coroutine_threadsafe(...).result() against it would
                # block forever. Drop the inherited bridge state and
                # start a fresh loop/thread/client for this process; the
                # parent's objects (and its now-shared sockets) are the
                # parent's to close — touching them from the child could
                # corrupt the parent's live connections mid-frame.
                self._loop = None
                self._loop_thread = None
                self._client = None
                self._namespace_handle = None
            loop = asyncio.new_event_loop()
            started = threading.Event()

            def run_loop() -> None:
                asyncio.set_event_loop(loop)
                started.set()
                loop.run_forever()

            thread = threading.Thread(
                target=run_loop, name="nanocached-django-loop", daemon=True
            )
            thread.start()
            started.wait()
            # Assigned before the connect below (not after) so a second
            # caller blocked on _lifecycle_lock still sees a loop/thread
            # to submit its own coroutine to once this connect completes;
            # self._client/_namespace_handle are what actually gate a
            # request going out before the connection exists (they stay
            # None until _connect() finishes).
            self._loop = loop
            self._loop_thread = thread
            self._loop_pid = os.getpid()
            try:
                asyncio.run_coroutine_threadsafe(self._connect(), loop).result()
            except BaseException:
                # A failed connect must not leave a half-started loop
                # thread behind for the next call to trip over.
                loop.call_soon_threadsafe(loop.stop)
                thread.join()
                loop.close()
                self._loop = None
                self._loop_thread = None
                raise

    async def _connect(self) -> None:
        self._client = await NanocachedClient.connect(
            self._addresses,
            auth_secret=self._secret,
            **self._connect_kwargs,
        )
        self._namespace_handle = self._client.namespace(self._namespace)

    def _run(self, make_coro):
        """Runs ``make_coro(handle)`` — a one-argument callable that
        builds the coroutine to await from the namespace handle, e.g.
        ``lambda handle: handle.get_bytes(key)`` — on this instance's
        loop thread and blocks the calling thread for its result.
        Taking a *callable* rather than an already-built coroutine
        matters: building the coroutine touches the namespace handle,
        which is only set once ``_ensure_started()`` below has
        connected, so that has to happen first — a coroutine built by
        the caller before calling this method would run against a
        still-``None`` handle. ``run_coroutine_threadsafe`` is the
        primitive for driving an asyncio object from a different thread
        than the one running its loop, which is what every sync SPI
        method here needs.

        issue #185: ``self._loop`` and the namespace handle are
        snapshotted *together*, under ``_lifecycle_lock``, instead of
        being read as two separate unguarded attribute accesses (one of
        them buried inside the caller's ``make_coro``). A concurrent
        ``shutdown()`` (or ``close()`` with ``CLOSE_ON_REQUEST``) sets
        both to ``None`` under that same lock, so without this the two
        reads could straddle a shutdown and hand a coroutine either a
        ``None`` loop or a ``None`` handle — surfacing as a raw
        ``AttributeError`` instead of a clean outcome. Snapshotting
        under the lock also means a caller that arrives while
        ``_ensure_started()``'s own connect is still in flight (held
        under the same lock) waits for it to finish rather than racing
        a still-``None`` handle.

        If the snapshot lands mid-shutdown (``loop``/``handle`` still
        ``None`` right after ``_ensure_started()`` returns), retrying
        ``_ensure_started()`` reconnects exactly as it would on first
        use — bounded by ``_RUN_RECONNECT_ATTEMPTS`` so a pathological
        caller that keeps shutting the backend down between every
        attempt fails loudly instead of spinning forever."""
        for _ in range(_RUN_RECONNECT_ATTEMPTS):
            self._ensure_started()
            with self._lifecycle_lock:
                loop, handle = self._loop, self._namespace_handle
            if loop is not None and handle is not None:
                return asyncio.run_coroutine_threadsafe(make_coro(handle), loop).result()
        raise RuntimeError(
            "nanocached_django: backend was shut down concurrently with "
            "this operation and could not reconnect; retry the operation"
        )

    def close(self, **kwargs) -> None:
        """Called by Django after every request (``request_finished`` →
        ``close_caches``): a no-op unless ``CLOSE_ON_REQUEST`` was set —
        see the module docs' lifecycle note. Use :meth:`shutdown` to
        always tear down."""
        if self._close_on_request:
            self.shutdown()

    def shutdown(self) -> None:
        """Unconditionally stops the loop thread and closes the client
        (safe to call twice, or before any use); the next cache operation
        lazily reconnects.

        issue #185: the loop-stop/thread-join/loop-close teardown runs in
        a ``finally`` so it always happens even if ``client.close()``
        raises (e.g. the connection was already dead) — before this fix,
        such a raise skipped the teardown entirely and leaked the loop
        thread (and whatever socket it still held open)."""
        with self._lifecycle_lock:
            loop, thread, client = self._loop, self._loop_thread, self._client
            self._loop = None
            self._loop_thread = None
            self._client = None
            self._namespace_handle = None
        if loop is None:
            return
        try:
            if client is not None:
                asyncio.run_coroutine_threadsafe(client.close(), loop).result()
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join()
            loop.close()

    # ── timeout translation ─────────────────────────────────────────

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        """Django's timeout conventions, translated to nanocached's wire
        TTL (whole seconds, 0 = no expiry — see ``_DO_NOT_CACHE`` above
        for why 0/negative can't just be forwarded as a TTL):

        - ``DEFAULT_TIMEOUT`` (the sentinel meaning "no timeout was
          passed") uses this backend's own ``default_timeout``.
        - ``None`` means never expires, in both Django's convention and
          nanocached's wire TTL 0 — the one case where the two polarities
          happen to agree.
        - 0 or negative means "don't cache" (``_DO_NOT_CACHE``): the
          caller's set() turns into a delete, matching the "delete/no-op"
          the issue spec calls for instead of resurrecting the entry with
          the wrong TTL.
        - Any other positive value rounds UP to whole seconds
          (``math.ceil``) — the wire has no sub-second resolution, and
          rounding down could turn a short-but-nonzero timeout into
          wire TTL 0, which means eternal instead of "expires very soon".
        """
        if timeout is DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        if timeout is None:
            return 0
        if timeout <= 0:
            return _DO_NOT_CACHE
        return max(1, math.ceil(timeout))

    # ── BaseCache SPI ────────────────────────────────────────────────

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """Atomic add via the SDK's ``put_if_absent`` (issue #141's k/A
        compare-and-set, wired up here in #414): the key's primary owner
        alone evaluates "is this absent" and performs the write, so two
        racing ``add()`` calls can never both observe absent and both
        write — the loser's ``put_if_absent`` simply comes back ``False``,
        matching ``BaseCache.add()``'s "return False, don't overwrite"
        contract exactly. (This docstring used to claim "the wire has no
        compare-and-set" — true before #141/PR #146 added ``k``/``x``,
        stale by the time of #414.)"""
        cache_key = self.make_and_validate_key(key, version=version)
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            # Nothing to store either way (see get_backend_timeout), so
            # there's no write for put_if_absent to condition — just
            # report whether the key really was absent, the same
            # "logically added, immediately expired" contract other
            # backends give a zero/negative timeout (e.g. LocMemCache).
            existing = self._run(lambda handle: handle.get_bytes(cache_key))
            return existing is None
        encoded = _encode_value(value)
        return self._run(
            lambda handle: handle.put_if_absent(cache_key, encoded, ttl_seconds=wire_ttl)
        )

    def get(self, key, default=None, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        raw = self._run(lambda handle: handle.get_bytes(cache_key))
        if raw is None:
            return default
        return _decode_value(raw)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            self._run(lambda handle: handle.delete(cache_key))
            return
        encoded = _encode_value(value)
        self._run(lambda handle: handle.set(cache_key, encoded, ttl_seconds=wire_ttl))

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        """get_bytes + re-set with the new timeout — also not atomic (a
        concurrent write between the two can be overwritten with the
        pre-touch value); returns False without writing anything if the
        key is already missing."""
        cache_key = self.make_and_validate_key(key, version=version)
        raw = self._run(lambda handle: handle.get_bytes(cache_key))
        if raw is None:
            return False
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            self._run(lambda handle: handle.delete(cache_key))
            return True
        # Re-sent as the raw bytes already read back — already encoded
        # (pickled, or a counter's decimal-ASCII form), so there's
        # nothing to gain (and a value round-trip to lose) by decoding
        # and re-encoding it just to change the TTL.
        self._run(lambda handle: handle.set(cache_key, raw, ttl_seconds=wire_ttl))
        return True

    def delete(self, key, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        return self._run(lambda handle: handle.delete(cache_key))

    def has_key(self, key, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        return self._run(lambda handle: handle.get_bytes(cache_key)) is not None

    def get_many(self, keys, version=None):
        # One wire round trip per involved node (issue #152), via the
        # SDK's get_many_bytes — vs. a get() per key. cache_keys maps
        # the namespaced wire key back to the caller's original key,
        # since get_many_bytes echoes back whatever keys it was given.
        # Issue #439: a mid-batch partial failure (PartialWrongNodeError/
        # PartialConnectionLostError) is retried once for its remainder
        # by _get_many_bytes_resolved instead of propagating raw and
        # discarding an otherwise-successful batch — see that helper's
        # docstring, and the parity note in adapters/cache-manager's
        # mgetResolved / adapters/jcache's getManyBytesResolvingWrongNode.
        cache_keys = {self.make_and_validate_key(key, version=version): key for key in keys}
        if not cache_keys:
            return {}
        raw = self._run(lambda handle: _get_many_bytes_resolved(handle, list(cache_keys)))
        # Issue #332: decoded per key, not as one dict comprehension —
        # _decode_value's pickle.loads can raise on a single corrupt or
        # cross-version-incompatible entry (UnpicklingError, EOFError,
        # AttributeError, ImportError, ...; the exact exception depends on
        # *how* the bytes are malformed, so this deliberately isn't narrowed
        # to just UnpicklingError), and letting that abort the whole
        # comprehension would fail every other key in the batch along with
        # it. Django's own backends degrade per-key on a decode failure
        # instead, so a bad entry is simply left out of the result here —
        # matching what get() already does for a *missing* key, just for a
        # value that turned out to be unreadable instead of absent.
        decoded = {}
        for cache_key, value in raw.items():
            try:
                decoded[cache_keys[cache_key]] = _decode_value(value)
            except Exception:
                continue
        return decoded

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        # One wire round trip per involved node (issue #152), via the
        # SDK's set_many — vs. a set() per key. Always returns [] (the
        # "no failed keys" convention BaseCache's own default set_many
        # uses) since a failure here raises instead of being collected
        # per-key. wire_ttl is resolved once for the whole call, matching
        # set_many's own single-timeout signature.
        # Issue #439: a mid-batch partial failure (WrongNodeError/
        # PartialSetConnectionLostError) is retried once — the whole
        # batch, or just its still-unstored remainder — by
        # _set_many_resolved instead of propagating raw and failing an
        # otherwise-successful batch outright; see that helper's
        # docstring, and the parity note in adapters/cache-manager's
        # msetResolved / adapters/jcache's setManyBytesResolvingWrongNode.
        if not data:
            return []
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            self.delete_many(data.keys(), version=version)
            return []
        payload = {self.make_and_validate_key(key, version=version): _encode_value(value) for key, value in data.items()}
        self._run(lambda handle: _set_many_resolved(handle, payload, wire_ttl))
        return []

    def delete_many(self, keys, version=None):
        # Issue #233: fanned out concurrently (asyncio.gather) in one
        # _run round trip, rather than one delete() per key run fully
        # sequentially — each delete() previously blocked the calling
        # thread on its own separate run_coroutine_threadsafe().result()
        # before the next key's delete even started, even though the
        # wire has no bulk delete op for get_many/set_many's own
        # single-round-trip treatment to reuse (issue #152).
        cache_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        if not cache_keys:
            return
        self._run(lambda handle: _delete_all(handle, cache_keys))

    def clear(self):
        """This namespace's CLEAR (issue #106) — never the whole store,
        so other aliases/namespaces sharing a node or cluster are
        untouched. One ``c`` frame per node, fanned out by the SDK."""
        self._run(lambda handle: handle.clear())

    def incr(self, key, delta=1, version=None):
        """The SDK's own INCR (issue #129) — atomic on the node that owns
        the key, unlike ``add()``/``touch()`` above. Matches
        ``BaseCache.incr``'s own contract exactly: raises ``ValueError``
        if the key doesn't exist (never creates one), and returns the new
        value on success. If the key exists but holds a value this
        backend never wrote as a counter (e.g. a pickled non-int, or a
        counter that overflowed a signed 64-bit integer),
        ``nanocached.NotNumericError`` propagates as-is rather than being
        papered over as a ``ValueError`` — it names the actual condition
        BaseCache's own contract has no vocabulary for.

        As volatile as ``set()``: LRU eviction or this alias's TIMEOUT
        reclaim a counter the same as any other entry, so this is a fit
        for rate limiting or approximate counts, not for a count that
        must survive (billing, inventory).

        issue #414: ``OPTIONS.COMPRESS`` and ``incr``/``decr`` cannot
        coexist on the SDK client this backend holds (issue #321's
        ``CompressionIncompatibleError`` — the wire has no marker byte on
        an increment's result, so a compress-enabled client can't tell a
        counter reply from anything else on a later ``get()``). That
        exception is an SDK-internal type, outside what
        ``BaseCache.incr()`` callers are expected to catch, so it's
        translated to ``ValueError`` at this boundary instead — the same
        exception type this method already raises for a missing key,
        which is as close as Django's cache contract comes to "this
        combination of settings doesn't support incr()"."""
        cache_key = self.make_and_validate_key(key, version=version)
        try:
            new_value = self._run(lambda handle: handle.incr(cache_key, delta))
        except CompressionIncompatibleError as exc:
            raise ValueError(
                "nanocached_django: incr()/decr() cannot be used on a cache "
                "alias configured with OPTIONS.COMPRESS"
            ) from exc
        if new_value is None:
            raise ValueError(f"Key '{key}' not found")
        return new_value

    def decr(self, key, delta=1, version=None):
        """``incr(key, -delta, version)`` — decr never sends a separate
        wire op; see incr()."""
        return self.incr(key, -delta, version=version)
