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
import pickle
import re
import threading

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.exceptions import ImproperlyConfigured

from nanocached import NanocachedClient

# OPTIONS.NAMESPACE default — every key this backend touches lives under
# this namespace unless the CACHES entry overrides it (issue #105).
_DEFAULT_NAMESPACE = "django"

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
    return isinstance(value, int) and not isinstance(value, bool)


def _encode_value(value: object) -> bytes:
    if _is_counter_value(value):
        return str(value).encode("ascii")
    return pickle.dumps(value, pickle.HIGHEST_PROTOCOL)


def _decode_value(raw: bytes) -> object:
    if _DECIMAL_INT_RE.match(raw):
        return int(raw)
    return pickle.loads(raw)


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

        # The sync/async bridge's state — all None until _ensure_started()
        # first runs (lazily, on the first cache operation, not here:
        # __init__ runs synchronously wherever Django constructs this
        # backend, e.g. while importing settings, and must not block on
        # network I/O or spin up a thread nothing has asked for yet).
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._client: NanocachedClient | None = None
        self._namespace_handle = None
        # Guards start/close against concurrent callers on different
        # Django worker threads racing to lazily start (or to close) the
        # same backend instance — the loop and client themselves are
        # otherwise only ever touched from _run()'s single background
        # thread.
        self._lifecycle_lock = threading.Lock()

    # ── the sync/async bridge ───────────────────────────────────────

    def _ensure_started(self) -> None:
        if self._loop is not None:
            return
        with self._lifecycle_lock:
            if self._loop is not None:
                return
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
        )
        self._namespace_handle = self._client.namespace(self._namespace)

    def _run(self, make_coro):
        """Runs ``make_coro()`` — a zero-argument callable that builds
        the coroutine to await, e.g.
        ``lambda: self._namespace_handle.get_bytes(key)`` — on this
        instance's loop thread and blocks the calling thread for its
        result. Taking a *callable* rather than an already-built
        coroutine matters: building the coroutine touches
        ``self._namespace_handle``, which is only set once
        ``_ensure_started()`` below has connected, so that has to happen
        first — a coroutine built by the caller before calling this
        method would run against a still-``None`` handle.
        ``run_coroutine_threadsafe`` is the primitive for driving an
        asyncio object from a different thread than the one running its
        loop, which is what every sync SPI method here needs."""
        self._ensure_started()
        return asyncio.run_coroutine_threadsafe(make_coro(), self._loop).result()

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
        lazily reconnects."""
        with self._lifecycle_lock:
            loop, thread, client = self._loop, self._loop_thread, self._client
            self._loop = None
            self._loop_thread = None
            self._client = None
            self._namespace_handle = None
        if loop is None:
            return
        if client is not None:
            asyncio.run_coroutine_threadsafe(client.close(), loop).result()
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
        """Get-then-set, not atomic — the wire has no compare-and-set
        (same trade-off the Spring adapter documents for its
        ``putIfAbsent``): two racing callers can both observe "absent" and
        both write, and the later write wins."""
        cache_key = self.make_and_validate_key(key, version=version)
        wire_ttl = self.get_backend_timeout(timeout)
        existing = self._run(lambda: self._namespace_handle.get_bytes(cache_key))
        if existing is not None:
            return False
        if wire_ttl is _DO_NOT_CACHE:
            # Nothing to store, but the key really was absent — same
            # "logically added, immediately expired" contract other
            # backends give a zero/negative timeout (e.g. LocMemCache).
            return True
        encoded = _encode_value(value)
        self._run(lambda: self._namespace_handle.set(cache_key, encoded, ttl_seconds=wire_ttl))
        return True

    def get(self, key, default=None, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        raw = self._run(lambda: self._namespace_handle.get_bytes(cache_key))
        if raw is None:
            return default
        return _decode_value(raw)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            self._run(lambda: self._namespace_handle.delete(cache_key))
            return
        encoded = _encode_value(value)
        self._run(lambda: self._namespace_handle.set(cache_key, encoded, ttl_seconds=wire_ttl))

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        """get_bytes + re-set with the new timeout — also not atomic (a
        concurrent write between the two can be overwritten with the
        pre-touch value); returns False without writing anything if the
        key is already missing."""
        cache_key = self.make_and_validate_key(key, version=version)
        raw = self._run(lambda: self._namespace_handle.get_bytes(cache_key))
        if raw is None:
            return False
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            self._run(lambda: self._namespace_handle.delete(cache_key))
            return True
        # Re-sent as the raw bytes already read back — already encoded
        # (pickled, or a counter's decimal-ASCII form), so there's
        # nothing to gain (and a value round-trip to lose) by decoding
        # and re-encoding it just to change the TTL.
        self._run(lambda: self._namespace_handle.set(cache_key, raw, ttl_seconds=wire_ttl))
        return True

    def delete(self, key, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        return self._run(lambda: self._namespace_handle.delete(cache_key))

    def has_key(self, key, version=None):
        cache_key = self.make_and_validate_key(key, version=version)
        return self._run(lambda: self._namespace_handle.get_bytes(cache_key)) is not None

    def get_many(self, keys, version=None):
        # One wire round trip per involved node (issue #152), via the
        # SDK's get_many_bytes — vs. a get() per key. cache_keys maps
        # the namespaced wire key back to the caller's original key,
        # since get_many_bytes echoes back whatever keys it was given.
        cache_keys = {self.make_and_validate_key(key, version=version): key for key in keys}
        if not cache_keys:
            return {}
        raw = self._run(lambda: self._namespace_handle.get_many_bytes(list(cache_keys)))
        return {cache_keys[cache_key]: _decode_value(value) for cache_key, value in raw.items()}

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        # One wire round trip per involved node (issue #152), via the
        # SDK's set_many — vs. a set() per key. Always returns [] (the
        # "no failed keys" convention BaseCache's own default set_many
        # uses) since a failure here raises instead of being collected
        # per-key. wire_ttl is resolved once for the whole call, matching
        # set_many's own single-timeout signature.
        if not data:
            return []
        wire_ttl = self.get_backend_timeout(timeout)
        if wire_ttl is _DO_NOT_CACHE:
            self.delete_many(data.keys(), version=version)
            return []
        payload = {self.make_and_validate_key(key, version=version): _encode_value(value) for key, value in data.items()}
        self._run(lambda: self._namespace_handle.set_many(payload, ttl_seconds=wire_ttl))
        return []

    def delete_many(self, keys, version=None):
        for key in keys:
            self.delete(key, version=version)

    def clear(self):
        """This namespace's CLEAR (issue #106) — never the whole store,
        so other aliases/namespaces sharing a node or cluster are
        untouched. One ``c`` frame per node, fanned out by the SDK."""
        self._run(lambda: self._namespace_handle.clear())

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
        must survive (billing, inventory)."""
        cache_key = self.make_and_validate_key(key, version=version)
        new_value = self._run(lambda: self._namespace_handle.incr(cache_key, delta))
        if new_value is None:
            raise ValueError(f"Key '{key}' not found")
        return new_value

    def decr(self, key, delta=1, version=None):
        """``incr(key, -delta, version)`` — decr never sends a separate
        wire op; see incr()."""
        return self.incr(key, -delta, version=version)
