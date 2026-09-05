"""One already-identified connection to a single nanocached-node, speaking
the cache protocol (``G``/``S``/``D``, and their namespaced counterparts
``g``/``s``/``d`` — issue #105 — plus ``c``/``F`` to clear a namespace or
flush every namespace, issue #106, ``i`` to increment/decrement a counter,
issue #129, ``k``/``x`` for compare-and-set, issue #141, and ``m``/``o``
for batched get/set, issues #128/#150/#151 — the ``A`` identify exchange
happens in ``_identify`` before a Connection exists).

Requests are pipelined onto the socket and matched to responses in send
order (request pipelining): a dedicated read loop task consumes responses
and dispatches each to the oldest still-pending request, since
nanocached-node itself only ever answers in the order it received
requests. Pushing onto the pending queue and writing the frame happen
under one lock, so concurrent callers' queue order always matches the
order their frames actually hit the wire.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections import deque
from collections.abc import Callable, Sequence

from ._errors import ConnectionLostError, NanocachedError, NotNumericError, RetryableError, WrongNodeError

# The server's own request cap is 1 MiB; this constant doubles that as
# headroom, so a claimed length beyond it is definitely a corrupt or
# malicious frame, never just a legitimately large value.
_MAX_VALUE_LENGTH = 2 * 1024 * 1024

# An `I` response's <value> body (issue #129) is decimal ASCII text
# holding the server's own i64 counter — the same grammar and range as
# the request's own <delta> field (see client.py's _check_delta). Unlike
# <value-length>/<ttl-seconds> above, this SDK used to hand the raw bytes
# straight to Connection.incr()'s caller, which called plain int() on
# them with no guard: a malformed body (a desynced/corrupted reply, or a
# buggy proxy in front of the server) raised a bare ValueError instead of
# this SDK's own NanocachedError, which _with_wrong_node_retry's
# (WrongNodeError, ConnectionError, OSError) catch doesn't recognize (the
# Go SDK hits the same failure mode and wraps it as ErrProtocol —
# parseIncrValue in sdk/go/client.go). Bounding the digit count up front
# keeps int() cheap and side-steps CPython's own int<->str conversion
# digit limit (PEP 3101 / bpo-issue 3.11's default 4300-digit guard)
# entirely, rather than relying on it.
_MAX_I64 = 2**63 - 1
_MIN_I64 = -(2**63)
_INCR_VALUE_RE = re.compile(rb"-?[0-9]{1,19}")

# Every OTHER integer field this SDK parses off the wire — lengths,
# counts, ttls, response tags — is never signed, unlike _INCR_VALUE_RE's
# <value> body above, but needs the same digits-only-ASCII grammar
# (issue #462): Python's bare int() is far looser than the wire actually
# allows, silently accepting a leading '+', surrounding whitespace, and
# '_' as a digit-group separator (int("1_000") == 1000, int(" 5 ") == 5,
# int("+5") == 5). Leading zeros ARE allowed, matching the server's own
# byte-by-byte parse_length (src/command.rs), which imposes no
# leading-zero restriction. _identify.py imports this too — its own
# discovery-response integer fields (counts, name/addr lengths) are the
# same grammar, just parsed from a different reply.
_STRICT_UINT_RE = re.compile(rb"[0-9]+")


def _parse_strict_uint(field: bytes) -> int:
    """Drop-in replacement for a bare ``int(field)`` call parsing one
    non-negative wire integer field: raises ``ValueError`` — exactly
    what ``int()`` itself would raise on genuinely non-numeric input, so
    every existing ``except ValueError`` call site needs no other change
    — when `field` doesn't match the digits-only grammar above, instead
    of silently accepting int()'s looser one."""
    if not _STRICT_UINT_RE.fullmatch(field):
        raise ValueError(f"invalid strict uint literal: {field!r}")
    return int(field)


# Bounds the sum of every hit's declared length across one multi-get
# (`M`) reply (issue #207, follow-up to #179's Java fix, PR #201). Each
# individual length is already capped at _MAX_VALUE_LENGTH above, but
# that alone doesn't bound the reply as a whole: a node answering a
# 400-key multi-get with 400 x 2 MiB hits would force ~800 MB of
# allocation from a single reply. Reuses the same 64 MiB figure as
# _compression.py's own decompression-bomb cap. A module global only so
# tests can shrink it (mirrors _REQUEST_TIMEOUT above).
_MAX_MULTIGET_RESPONSE_BYTES = 64 * 1024 * 1024

# A tag is a u32 in decimal (echoed response tags).
_MAX_TAG = 0xFFFFFFFF

# Retryable-error status `R` (issue #125): a request answered `R` failed
# transiently (e.g. nanocached-proxy's upstream node was briefly
# unreachable) and must be retried on the SAME connection — up to 2
# retries (3 attempts total), sleeping 50ms before the first retry and
# 100ms before the second. A third `R` in a row raises RetryableError
# without closing or redialing the connection.
_MAX_RETRYABLE_ATTEMPTS = 3
_RETRYABLE_RETRY_DELAYS = (0.05, 0.10)

# Bounds how long the connection may go without progress while requests
# are outstanding (issue #42) — each response must arrive within this
# window of the previous one (or of its own send, when the queue was
# empty): without it, a half-open server that accepts the TCP connection
# but never writes back — or stops mid-stream — would hang get/set/delete
# forever. Generous versus the server's own 10s outbound timeouts, and
# the same 30s the Go and Rust SDKs use. A module global only so tests
# can shorten it.
_REQUEST_TIMEOUT = 30.0

# One parsed response's payload: plain bytes (V), (value, ttl_seconds)
# (I), a multi_get roster (M — list[bytes | None | _MultiWrongNode]), a
# multi_set roster (O — list[bool | _MultiWrongNode]), or None (every
# other marker). Widened for issues #128/#150/#151; every pre-existing
# marker's payload shape is unchanged.
_ResponsePayload = bytes | tuple[bytes, int | None] | list[object] | None


# Echoed response tags: on a tagged-mode connection every request header carries the
# client's tag as its last field, and the server echoes it in the
# response — `tag is None` is the untagged (pre-0019) form.
def _tag_field(tag: int | None) -> bytes:
    return b"" if tag is None else b" %d" % tag


# Namespaces (issue #105): the SDK rule (docs/protocol.html "g / s / d")
# is that the *default* (empty) namespace must keep sending the legacy
# uppercase frames byte-for-byte, so an unchanged client talking to an
# old, pre-namespace server keeps working — only a non-empty namespace
# switches to the lowercase g/s/d frames, which gain one leading
# <namespace-length> header field and namespace bytes leading the body.
# Every encoder below takes namespace last, defaulting to b"", so an
# un-namespaced call site reads exactly as it did before this feature.


def _encode_get(key: bytes, tag: int | None = None, namespace: bytes = b"") -> bytes:
    if namespace:
        return b"g %d %d%b\n%b%b" % (len(namespace), len(key), _tag_field(tag), namespace, key)
    return b"G %d%b\n%b" % (len(key), _tag_field(tag), key)


def _encode_set(
    key: bytes, value: bytes, ttl_seconds: int, tag: int | None = None, namespace: bytes = b""
) -> bytes:
    # 0 means no expiry — omitted from the wire exactly as the old
    # None/absent TTL was.
    if namespace:
        if ttl_seconds == 0:
            return b"s %d %d %d%b\n%b%b%b" % (
                len(namespace), len(key), len(value), _tag_field(tag), namespace, key, value
            )
        return b"s %d %d %d %d%b\n%b%b%b" % (
            len(namespace), len(key), len(value), ttl_seconds, _tag_field(tag), namespace, key, value
        )
    if ttl_seconds == 0:
        return b"S %d %d%b\n%b%b" % (len(key), len(value), _tag_field(tag), key, value)
    return b"S %d %d %d%b\n%b%b" % (len(key), len(value), ttl_seconds, _tag_field(tag), key, value)


def _encode_delete(key: bytes, tag: int | None = None, namespace: bytes = b"") -> bytes:
    if namespace:
        return b"d %d %d%b\n%b%b" % (len(namespace), len(key), _tag_field(tag), namespace, key)
    return b"D %d%b\n%b" % (len(key), _tag_field(tag), key)


# Clear a namespace / flush everything (issue #106): unlike g/s/d above,
# c/F have no legacy uppercase equivalent to fall back to for the default
# namespace — they're new commands, so the default namespace is just
# <namespace-length> 0 on the wire (docs/protocol.html "c / F"), never a
# different marker.
def _encode_clear(namespace: bytes = b"", tag: int | None = None) -> bytes:
    return b"c %d%b\n%b" % (len(namespace), _tag_field(tag), namespace)


def _encode_clear_all(tag: int | None = None) -> bytes:
    return b"F%b\n" % _tag_field(tag)


# Counters (issue #129): unlike g/s/d, INCR has no legacy uppercase
# form — it always carries <namespace-length>, 0 for the default
# namespace, exactly like c/F above. <delta> is signed decimal (Python's
# str(int) already produces the wire's canonical form: an optional
# leading '-', no leading zeros, no '+' — %d formats it identically).
def _encode_incr(key: bytes, delta: int, tag: int | None = None, namespace: bytes = b"") -> bytes:
    return b"i %d %d %d%b\n%b%b" % (len(namespace), len(key), delta, _tag_field(tag), namespace, key)


# Compare-and-set (issue #141): like i above, k/x always carry
# <namespace-length> (0 for the default namespace) — neither op has a
# pre-namespace legacy uppercase form. <cond> is a bare, un-length-
# prefixed token: CAS_ABSENT/CAS_PRESENT below, or a 32-character
# lowercase hex digest bytes object (see _digest.content_digest) callers
# build themselves — its own shape identifies which kind it is, so it
# rides in the header exactly where <cond> is documented
# (docs/protocol.html "k / x"), never length-prefixed or in the body.
CAS_ABSENT = b"A"
CAS_PRESENT = b"P"


def _encode_cas_set(
    key: bytes,
    value: bytes,
    cond: bytes,
    ttl_seconds: int = 0,
    tag: int | None = None,
    namespace: bytes = b"",
) -> bytes:
    # 0 means no expiry — omitted from the wire, exactly like _encode_set's
    # own ttl_seconds field.
    if ttl_seconds == 0:
        return b"k %d %d %d %b%b\n%b%b%b" % (
            len(namespace), len(key), len(value), cond, _tag_field(tag), namespace, key, value
        )
    return b"k %d %d %d %b %d%b\n%b%b%b" % (
        len(namespace), len(key), len(value), cond, ttl_seconds, _tag_field(tag), namespace, key, value
    )


def _encode_cas_delete(key: bytes, cond: bytes, tag: int | None = None, namespace: bytes = b"") -> bytes:
    # <cond> here is always a digest (never CAS_ABSENT/CAS_PRESENT) — an
    # absent- or present-only conditioned delete is already the plain,
    # unconditional D/d (docs/protocol.html "k / x").
    return b"x %d %d %b%b\n%b%b" % (len(namespace), len(key), cond, _tag_field(tag), namespace, key)


# Batched get and set (issues #128/#150/#151): like i/k/x above, m/o
# always carry <namespace-length> (0 for the default namespace) — neither
# op has a pre-namespace legacy uppercase form. Unlike every op above,
# the number of length fields in the header is variable (one, or one pair
# for m/o respectively, per key), so these can't reuse the fixed-arity
# `%`-formatting style the others use — the header is assembled field by
# field instead (docs/protocol.html "m / o — batched get and set").
def _encode_multi_get(
    keys: Sequence[bytes], tag: int | None = None, namespace: bytes = b""
) -> bytes:
    fields = [b"m", b"%d" % len(namespace), b"%d" % len(keys)]
    fields.extend(b"%d" % len(key) for key in keys)
    header = b" ".join(fields) + _tag_field(tag) + b"\n"
    return header + namespace + b"".join(keys)


def _encode_multi_set(
    keys: Sequence[bytes],
    values: Sequence[bytes],
    ttl_seconds: int = 0,
    tag: int | None = None,
    namespace: bytes = b"",
) -> bytes:
    fields = [b"o", b"%d" % len(namespace), b"%d" % len(keys)]
    for key, value in zip(keys, values):
        fields.append(b"%d" % len(key))
        fields.append(b"%d" % len(value))
    # 0 means no expiry — omitted from the wire, exactly like
    # _encode_set's own ttl_seconds field.
    if ttl_seconds != 0:
        fields.append(b"%d" % ttl_seconds)
    header = b" ".join(fields) + _tag_field(tag) + b"\n"
    body = namespace + b"".join(key + value for key, value in zip(keys, values))
    return header + body


class _MultiWrongNode:
    """Sentinel marking one key's per-key ``W`` inside a multi_get/
    multi_set roster (issues #128/#150/#151) — distinct from a clean
    miss (``None``), which only multi_get's roster can produce."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "WRONG_NODE"


WRONG_NODE = _MultiWrongNode()


class _PendingSlot:
    """One pipelined request's entry in ``Connection._pending`` (request
    pipelining) — the tag its request was sent under (None on an
    untagged connection), the future callers await, and whether this
    slot's own frame may already have reached the server (issue #225,
    conservatively widened by issue #412): ``sent`` starts False and
    flips to True as soon as ``_send()``'s ``writer.write()`` for THIS
    request returns without raising — deliberately *before* the
    following ``await writer.drain()``, still inside the write lock's
    critical section so it can never be read half-updated by a
    concurrent ``_poison()`` (asyncio is single-threaded; the flip and
    every read of it are both synchronous). This is a conservative
    "possibly sent" classification, not a "definitely acked" one:
    ``write()`` only hands the frame's bytes to the OS socket buffer,
    but a timeout or another pipelined request's failure can poison the
    connection while THIS request's own ``_send()`` call is still
    suspended inside ``drain()`` — by then the bytes may already be in
    flight to the server, so the slot must already read as sent for
    ``_poison()`` (running from that other context) to classify it
    correctly. Marking it only after ``drain()`` itself returns would
    leave a window where a request that's already on the wire is still
    reported unsent, letting ``_with_wrong_node_retry`` blindly resend
    it and double-apply a non-idempotent incr/CAS. ``_poison()`` uses
    ``sent`` to tell "this request never left the client" (``sent``
    still False — always safe to retry) apart from "this request may
    have reached the server before the reply was lost" (``sent`` True —
    see Connection._error_for)."""

    __slots__ = ("tag", "future", "sent")

    def __init__(self, future: "asyncio.Future[tuple[bytes, _ResponsePayload]]") -> None:
        self.tag: int | None = None
        self.future = future
        self.sent = False


class Connection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_close: Callable[[], None] | None = None,
        tagged: bool = False,
        on_transient_retry: Callable[[], None] | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        # Echoed response tags: negotiated during identify — when true, every request
        # carries a tag the server echoes, and _read_loop verifies the
        # echo against the oldest pending slot before resolving it.
        self._tagged = tagged
        self._next_tag = 0
        # Retryable-error status `R` (issue #125): invoked once per `R`
        # response this connection receives, whether it ends up retried
        # successfully or exhausts the budget into RetryableError — lets
        # the owning client maintain a `transient_retries` counter (see
        # ClientStats) without this module knowing anything about client
        # stats itself. Optional only to mirror on_close: every current
        # caller passes a callback.
        self._on_transient_retry = on_transient_retry
        # Serializes "enqueue the pending slot, then write the frame" —
        # not the whole round trip — across concurrent callers, so queue
        # order always matches wire order for the dedicated reader below.
        self._write_lock = asyncio.Lock()
        # Each slot pairs the future with the tag its request was sent
        # under (None on an untagged connection) — the expected echo
        # _read_loop checks the response against before handing it out.
        self._pending: deque[_PendingSlot] = deque()
        self._closed = False
        self._last_used = time.monotonic()
        self._on_close = on_close
        # The progress-based request deadline (issue #42): armed when the
        # pending queue goes from empty to non-empty, re-armed by
        # _read_loop each time a response is dispatched with more still
        # outstanding, cleared once nothing is. Never fires on an idle
        # connection.
        self._deadline_handle: asyncio.TimerHandle | None = None
        self._read_task: asyncio.Task[None] = asyncio.ensure_future(self._read_loop())

    @property
    def closed(self) -> bool:
        # A peer close (e.g. the server's 60s idle timeout) is only
        # observed on the next read, so also probe the transport.
        return self._closed or self._writer.is_closing() or self._reader.at_eof()

    def idle_seconds(self) -> float:
        return time.monotonic() - self._last_used

    def close(self) -> None:
        self._poison(ConnectionError("nanocached: connection closed"))

    async def wait_closed(self) -> None:
        """Reaps ``_read_task`` (issue #412) — call this after close()
        (or once ``closed`` is otherwise known True) so a short-lived
        program's event-loop teardown never logs "Task was destroyed
        but it is pending!" for this connection's read loop, the same
        problem issue #189 fixed for NanocachedClient's own keepalive
        task. close() only closes the writer, which merely nudges the
        read loop toward an EOF it may not observe until a future loop
        iteration; cancelling here forces it to finish now instead of
        leaving it pending for as long as nothing else happens to run
        the loop. Cancelling an already-finished task (the common case —
        _poison() already made the read loop return on its own after a
        real connection failure) is a harmless no-op, so this is safe to
        call unconditionally from NanocachedClient.close()'s own
        teardown, mirroring how it reaps every other background task."""
        if not self._read_task.done():
            self._read_task.cancel()
        try:
            await self._read_task
        except asyncio.CancelledError:
            pass

    async def get(self, key: bytes, namespace: bytes = b"") -> bytes | None:
        marker, value = await self._request(lambda tag: _encode_get(key, tag, namespace))
        if marker == b"V":
            return value
        if marker == b"N":
            return None
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def set(self, key: bytes, value: bytes, ttl_seconds: int, namespace: bytes = b"") -> None:
        marker, _ = await self._request(lambda tag: _encode_set(key, value, ttl_seconds, tag, namespace))
        if marker == b"W":
            raise WrongNodeError()
        if marker != b"S":
            raise self._mismatch(marker)

    async def delete(self, key: bytes, namespace: bytes = b"") -> bool:
        marker, _ = await self._request(lambda tag: _encode_delete(key, tag, namespace))
        if marker == b"D":
            return True
        if marker == b"N":
            return False
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def incr(
        self, key: bytes, delta: int, namespace: bytes = b""
    ) -> tuple[bytes, int | None] | None:
        """Sends one ``i`` request to *this* node only (issue #129) —
        cluster fan-out to replicas is the client's job (see
        NanocachedClient.incr's docstring: the primary's literal result
        is forwarded as a ``set``, the increment itself is never replayed
        on a replica), never this connection's. Returns ``(new_value,
        ttl_seconds)`` on success (``ttl_seconds`` is ``None`` when the
        entry has no expiry), ``None`` on a miss (matching get()'s own
        miss convention), and raises ``NotNumericError`` when the stored
        value isn't INCR's counter grammar or applying ``delta`` would
        overflow. ``new_value``'s own decimal-ASCII-i64 grammar is
        already validated by ``_read_one_response`` before this returns
        — a malformed body raises ``NanocachedError`` there (poisoning
        this connection like any other malformed response) rather than a
        bare ``ValueError`` escaping from a later ``int()`` call."""
        marker, payload = await self._request(lambda tag: _encode_incr(key, delta, tag, namespace))
        if marker == b"I":
            assert payload is not None
            return payload  # type: ignore[return-value]
        if marker == b"N":
            return None
        if marker == b"T":
            raise NotNumericError()
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def cas_set(
        self, key: bytes, value: bytes, cond: bytes, ttl_seconds: int = 0, namespace: bytes = b""
    ) -> bool:
        """Sends one ``k`` request to *this* node only (issue #141) —
        exactly like incr(), cluster fan-out to replicas is the client's
        job (see NanocachedClient's own CAS docstrings: the primary's
        literal result is forwarded as a ``set``, ``k`` is never replayed
        on a replica). Returns ``True`` on success (``S``), ``False`` on
        a condition mismatch (``N`` — reused as-is, no new response
        marker exists for this)."""
        marker, _ = await self._request(
            lambda tag: _encode_cas_set(key, value, cond, ttl_seconds, tag, namespace)
        )
        if marker == b"S":
            return True
        if marker == b"N":
            return False
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def cas_delete(self, key: bytes, cond: bytes, namespace: bytes = b"") -> bool:
        """Sends one ``x`` request to *this* node only (issue #141) — see
        cas_set(). Returns ``True`` on success (``D``), ``False`` on a
        mismatch or missing key (``N``)."""
        marker, _ = await self._request(lambda tag: _encode_cas_delete(key, cond, tag, namespace))
        if marker == b"D":
            return True
        if marker == b"N":
            return False
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def multi_get(
        self, keys: Sequence[bytes], namespace: bytes = b""
    ) -> list[bytes | None | _MultiWrongNode]:
        """Sends one ``m`` request for every key in ``keys`` (issues
        #128/#150/#151) and returns one roster entry per key, in request
        order: the hit's raw bytes, ``None`` for a clean miss, or
        WRONG_NODE for that key's own ``W``. Unlike get(), there is no
        whole-frame ``W`` to raise — a batch never fails as a whole — so
        any marker other than ``M`` is a desync, exactly like clear()'s
        own stance. A well-formed ``M`` whose entry count doesn't match
        ``len(keys)`` is a desync too (issue #181): a slice assignment
        onto the roster would silently shift every later key's value
        onto the wrong key instead of raising."""
        marker, entries = await self._request(lambda tag: _encode_multi_get(keys, tag, namespace))
        if marker == b"M":
            assert entries is not None
            if len(entries) != len(keys):
                raise self._desync(
                    "nanocached: multi-get reply has "
                    f"{len(entries)} entries for {len(keys)} keys (connection desynced)"
                )
            return entries  # type: ignore[return-value]
        raise self._mismatch(marker)

    async def multi_set(
        self, keys: Sequence[bytes], values: Sequence[bytes], ttl_seconds: int, namespace: bytes = b""
    ) -> list[bool | _MultiWrongNode]:
        """Sends one ``o`` request storing every (key, value) pair in
        ``keys``/``values`` under one shared TTL (issues
        #128/#150/#151) and returns one roster entry per key, in request
        order: ``True`` for stored, or WRONG_NODE for that key's own
        ``W`` — see multi_get(). Same entry-count desync check as
        multi_get() (issue #181)."""
        marker, entries = await self._request(
            lambda tag: _encode_multi_set(keys, values, ttl_seconds, tag, namespace)
        )
        if marker == b"O":
            assert entries is not None
            if len(entries) != len(keys):
                raise self._desync(
                    "nanocached: multi-set reply has "
                    f"{len(entries)} entries for {len(keys)} keys (connection desynced)"
                )
            return entries  # type: ignore[return-value]
        raise self._mismatch(marker)

    async def clear(self, namespace: bytes = b"") -> None:
        """Drops every entry in ``namespace`` on this node (issue #106) —
        a namespace-length of 0 clears the default namespace. Never
        answers ``W``: a clear isn't key-addressed (every node holds a
        share of every namespace's keys, see docs/protocol.html
        "c / F"), so any marker other than ``C`` is a desync exactly like
        get/set/delete's own _mismatch path."""
        marker, _ = await self._request(lambda tag: _encode_clear(namespace, tag))
        if marker != b"C":
            raise self._mismatch(marker)

    async def clear_all(self) -> None:
        """Drops every namespace on this node, the default one included
        (issue #106) — see clear()."""
        marker, _ = await self._request(lambda tag: _encode_clear_all(tag))
        if marker != b"C":
            raise self._mismatch(marker)

    def _mismatch(self, marker: bytes) -> ConnectionError:
        # A well-formed response of the wrong kind (a `V` answering a set)
        # means the request/response streams are misaligned — every later
        # response would answer the wrong request, silently returning
        # other keys' data. Poison the connection, and classify as a
        # connection error so the retry layer redials and retries once.
        # Requests still pending behind this one may already have been
        # resolved with misaligned data by the time this runs — an
        # inherent limitation of matching-by-order pipelining shared with
        # the TypeScript SDK's Connection (request pipelining), not
        # something this SDK introduces.
        return self._desync(
            f"nanocached: response {marker!r} does not match the request (connection desynced)"
        )

    def _desync(self, message: str) -> ConnectionError:
        # Shared by _mismatch (wrong marker) and multi_get/multi_set's own
        # entry-count check (issue #181): either way the response stream
        # can no longer be trusted to line up with requests, so poison the
        # connection exactly like _mismatch does and return a
        # ConnectionError for the caller to raise.
        error = ConnectionError(message)
        self._poison(error)
        return error

    def _poison(self, error: Exception) -> None:
        """Marks the connection closed, closes the writer, and rejects
        every still-pending request with error. Safe to call more than
        once — from a writer noticing a failed write, the read loop
        noticing a failed read, or an explicit close() — only the first
        call has any effect. Invariant: no ``await`` may ever be
        introduced between the ``self._closed`` check below and the
        ``self._closed = True`` that follows it (nor in the corresponding
        check inside ``_request``'s locked section) — without that, two
        coroutines could both observe ``_closed`` as false and interleave
        past this guard, each running the body below and double-poisoning
        the connection."""
        if self._closed:
            return
        self._closed = True
        self._clear_deadline()
        pending = list(self._pending)
        self._pending.clear()
        self._writer.close()
        for slot in pending:
            if not slot.future.cancelled():
                slot.future.set_exception(self._error_for(slot, error))
        if self._on_close is not None:
            self._on_close()

    @staticmethod
    def _error_for(slot: _PendingSlot, error: Exception) -> Exception:
        """Non-idempotent replay guard (issue #225): a slot whose frame
        had already been fully written (``slot.sent``) when the
        connection died might have reached and been applied by the
        server — only the reply was lost, a malformed reply, or a later
        pipelined request's own failure. Wrapping ``error`` as
        ConnectionLostError for that slot only (an unwritten slot behind
        it in the pipeline keeps the plain ``error``, unchanged) lets
        incr/CAS/delete_if_matches's own retry wrapper
        (NanocachedClient._with_wrong_node_retry, replay_safe=False) tell
        it apart from a request that never left this client at all and
        never replay it. get/set/delete/clear don't check for the
        distinction, so ConnectionLostError being a plain ConnectionError
        subclass keeps them behaving exactly as before either way."""
        if slot.sent and not isinstance(error, ConnectionLostError):
            wrapped = ConnectionLostError(str(error))
            wrapped.__cause__ = error
            return wrapped
        return error

    def _arm_deadline(self) -> None:
        self._clear_deadline()
        self._deadline_handle = asyncio.get_running_loop().call_later(
            _REQUEST_TIMEOUT, self._on_request_timeout
        )

    def _clear_deadline(self) -> None:
        if self._deadline_handle is not None:
            self._deadline_handle.cancel()
            self._deadline_handle = None

    def _on_request_timeout(self) -> None:
        # Poison, exactly like a read error: rejects the stalled request
        # and everything pipelined behind it, closes the writer (which
        # also unblocks the read loop with EOF), and the retry layer
        # redials.
        self._poison(
            ConnectionError(
                f"nanocached: no response from server within {_REQUEST_TIMEOUT}s (request timed out)"
            )
        )

    def _claim_tag(self) -> int:
        tag = self._next_tag
        self._next_tag = (self._next_tag + 1) & _MAX_TAG  # wrap at u32, matching the wire's width
        return tag

    async def _request(
        self, build: Callable[[int | None], bytes]
    ) -> tuple[bytes, _ResponsePayload]:
        """Sends ``build``'s request and returns its (marker, value)
        answer. Retryable-error status `R` (issue #125): when the answer
        is `R`, this request is transparently retried on this SAME
        connection — up to 2 retries (3 attempts total), sleeping 50ms
        before the first retry and 100ms before the second — instead of
        being handed back to the caller; every get/set/delete/clear
        caller below therefore never sees marker `R` at all. A third `R`
        in a row raises RetryableError instead of a fourth attempt,
        without closing or redialing the connection (`R` is never treated
        as a connection failure, a `W`, or an `E`). Each retry is just an
        ordinary fresh _send() call, so it naturally lands at the back of
        the pending queue behind whatever other pipelined requests were
        already written while this one was in flight — no manual queue
        reordering needed for request pipelining to stay correct."""
        attempt = 0
        while True:
            future = await self._send(build)
            marker, value = await future
            if marker != b"R":
                return marker, value
            if self._on_transient_retry is not None:
                self._on_transient_retry()
            attempt += 1
            if attempt >= _MAX_RETRYABLE_ATTEMPTS:
                raise RetryableError()
            await asyncio.sleep(_RETRYABLE_RETRY_DELAYS[attempt - 1])

    async def _send(
        self, build: Callable[[int | None], bytes]
    ) -> asyncio.Future[tuple[bytes, _ResponsePayload]]:
        """Enqueues one request frame and returns its pending future,
        unawaited — the raw "write one frame, get back a slot to await"
        primitive _request() calls once per attempt (including retries)."""
        if self.closed:
            raise ConnectionError("nanocached: connection is closed")

        future: asyncio.Future[
            tuple[bytes, _ResponsePayload]
        ] = asyncio.get_running_loop().create_future()
        async with self._write_lock:
            if self.closed:
                raise ConnectionError("nanocached: connection is closed")
            self._last_used = time.monotonic()
            # Echoed response tags: the tag is claimed in the same locked span that
            # enqueues the pending slot and writes the frame, so tag
            # order can never skew from queue/wire order (request pipelining's
            # enqueue+write atomicity). build() runs before the pending
            # slot is appended — a builder that raises must fail with
            # nothing queued, or the next response would resolve an
            # orphaned slot and desync the stream.
            tag = self._claim_tag() if self._tagged else None
            frame = build(tag)
            slot = _PendingSlot(future)
            slot.tag = tag
            self._pending.append(slot)
            # Armed only on the empty→non-empty transition: arming on
            # *every* request would let a continuous stream of new
            # requests push the deadline forever ahead of a server that
            # has stopped answering — exactly the half-open hang the
            # timeout exists to catch.
            if len(self._pending) == 1:
                self._arm_deadline()
            try:
                self._writer.write(frame)
            except OSError as error:
                wrapped = ConnectionError(f"nanocached: connection failed: {error}")
                wrapped.__cause__ = error
                self._poison(wrapped)
            else:
                # Non-idempotent replay guard (issue #225, widened by
                # issue #412): flips as soon as write() hands the
                # frame's bytes to the OS socket buffer — deliberately
                # *before* awaiting drain() below, not after it returns.
                # write() itself never suspends (it can't be interrupted
                # by CancelledError), so this is the earliest point at
                # which the bytes might already be irrevocably on their
                # way to the server. If the connection is poisoned (e.g.
                # a timeout) while this coroutine is still suspended
                # inside drain() just below, _poison() — running from
                # that other context — must already see this slot as
                # possibly-sent; marking it only after drain() returns
                # would leave a window where an in-flight request is
                # still misclassified as never-sent, letting
                # _with_wrong_node_retry blindly resend it and
                # double-apply a non-idempotent incr/CAS. See
                # _PendingSlot and _error_for.
                slot.sent = True
                try:
                    await self._writer.drain()
                except OSError as error:
                    wrapped = ConnectionError(f"nanocached: connection failed: {error}")
                    wrapped.__cause__ = error
                    self._poison(wrapped)
                except asyncio.CancelledError:
                    # Cancelled while awaiting drain(): the frame is
                    # already handed to the kernel (slot.sent is already
                    # True above), but every request queued behind this
                    # one on this connection is still desynced the same
                    # way a mid-write cancellation always was, so this
                    # still poisons the whole connection rather than
                    # scoping the failure to just this one request.
                    self._poison(ConnectionError("nanocached: connection failed: cancelled mid-write"))
                    raise

        # If cancelled here, the write already fully completed — the
        # response is still coming from the server and must still be
        # matched to this slot by the read loop, so the future is left
        # in _pending (cancelled(), unretrieved) rather than removed,
        # keeping queue order aligned with wire order for every request
        # behind it (request pipelining). The TypeScript SDK's Connection
        # has no cancellation to handle here at all — plain Promises
        # simply can't be cancelled out from under `pending`. This SDK's
        # asyncio Tasks can be, so this is a real improvement rather than
        # just preserving it (request pipelining): cancellation is supported, and
        # kept safe by leaving the cancelled future in place instead of
        # ripping it out from under the read loop.
        return future

    async def _read_loop(self) -> None:
        while True:
            try:
                marker, value, tag = await self._read_one_response()
            except (ConnectionError, NanocachedError) as error:
                self._poison(error)
                return
            except (asyncio.IncompleteReadError, OSError) as error:
                self._poison(ConnectionError(f"nanocached: connection failed: {error}"))
                return
            except asyncio.LimitOverrunError as error:
                # readuntil() found no `\n` within the stream's internal
                # buffer limit (64 KiB) — the connection is desynced
                # mid-frame, same as an out-of-range value length below.
                # LimitOverrunError is a ValueError subclass, not an
                # OSError, so it must be caught explicitly here or the
                # read task dies silently: _poison() never runs, the
                # writer never closes, and every pending/future request
                # hangs forever (issue #8).
                self._poison(ConnectionError(f"nanocached: connection failed: {error}"))
                return

            was_empty = not self._pending

            # An unsolicited "busy" response means the server hit its
            # connection limit right after accept and is about to close
            # the connection; it isn't an answer to anything we sent
            # (mirrors the TypeScript SDK's Connection.onData).
            if marker == b"B" and was_empty:
                self._poison(
                    ConnectionError("nanocached: server rejected the connection (connection limit reached)")
                )
                return
            if was_empty:
                self._poison(
                    ConnectionError(f"nanocached: unsolicited response {marker!r} from server (connection desynced)")
                )
                return

            slot = self._pending.popleft()
            expected_tag, future = slot.tag, slot.future

            # Progress-based deadline (see _request): a dispatched
            # response is progress, so the next-oldest request gets a
            # fresh window; with nothing left waiting, clear it so an
            # otherwise-idle connection is never closed by it.
            if not self._pending:
                self._clear_deadline()
            else:
                self._arm_deadline()

            # Echoed response tags: on a tagged connection, verify the echoed tag
            # against the request this response is about to answer —
            # *before* it can reach any caller. A mismatch means the
            # streams are misaligned; unlike the caller-side kind check
            # (_mismatch), catching it here stops the misdelivery instead
            # of merely noticing it later.
            if self._tagged and tag != expected_tag:
                error = ConnectionError(
                    f"nanocached: response tag {tag} does not answer request tag {expected_tag} "
                    f"(connection desynced)"
                )
                self._poison(error)
                # The popped future is no longer in _pending, so _poison()
                # didn't reject it — do that here; the rest drain via
                # _poison()'s own rejection of what's left.
                if not future.cancelled():
                    future.set_exception(error)
                return

            if not future.cancelled():
                future.set_result((marker, value))

    # _read_one_response and _parse_tag below are the raw wire-frame
    # parser — the direct analog of the TypeScript SDK's protocol.ts
    # (tryParseResponse), which raises its own plain NanocachedError
    # uniformly for every parse violation, never its connection-loss
    # ConnectionLostError. Every raise in this pair mirrors that split:
    # a malformed frame is this SDK's own protocol violation
    # (NanocachedError), not a transport-level failure (builtin
    # ConnectionError — or, once the failed request's own frame was
    # already fully written, this SDK's narrower ConnectionLostError
    # subclass, issue #225 — see errors.ts's own doc comment on that
    # parity), even though both are equally poisoning and equally
    # swallowable by the retry layer (_SWALLOWABLE_ERRORS covers both).
    async def _read_one_response(
        self,
    ) -> tuple[bytes, _ResponsePayload, int | None]:
        marker = await self._reader.readexactly(1)

        if marker == b"V":
            # Untagged: `V <length>\n<value>`. Tagged: `V <length> <tag>\n<value>`
            # (echoed response tags).
            header = await self._reader.readuntil(b"\n")
            fields = header[1:-1].split(b" ")
            if len(fields) != (2 if self._tagged else 1):
                raise NanocachedError("nanocached: invalid value header in response")
            try:
                length = _parse_strict_uint(fields[0])
            except ValueError:
                length = -1
            # A non-numeric, negative, or absurd length (the server caps
            # requests at 1 MiB) is protocol garbage; the connection is
            # desynced mid-frame and must be poisoned, and the error must
            # be connection-classified so the retry layer handles it
            # (issue #8).
            if length < 0 or length > _MAX_VALUE_LENGTH:
                raise NanocachedError("nanocached: invalid value length in response")
            tag = self._parse_tag(fields[1]) if self._tagged else None
            value = await self._reader.readexactly(length)
            return marker, value, tag

        if marker == b"I":
            # Counters (issue #129): `I <value-length> [<ttl-seconds>]\n
            # <value>` untagged, `I <value-length> [<ttl-seconds>] <tag>\n
            # <value>` tagged — the trailing TTL is optional (present only
            # when the entry has one) exactly like `S`'s own optional
            # [ttl] [tag] request-side fields (_encode_set): disambiguated
            # purely by field count against whether this connection is
            # tagged, never guessed frame by frame. The payload handed
            # back up is (value, ttl_seconds) instead of plain bytes —
            # the only marker whose "value" isn't just the raw stored
            # bytes.
            header = await self._reader.readuntil(b"\n")
            fields = header[1:-1].split(b" ")
            min_fields = 2 if self._tagged else 1
            if len(fields) not in (min_fields, min_fields + 1):
                raise NanocachedError("nanocached: invalid incr header in response")
            try:
                length = _parse_strict_uint(fields[0])
            except ValueError:
                length = -1
            if length < 0 or length > _MAX_VALUE_LENGTH:
                raise NanocachedError("nanocached: invalid value length in response")
            has_ttl = len(fields) == min_fields + 1
            ttl_seconds: int | None = None
            if has_ttl:
                try:
                    ttl_seconds = _parse_strict_uint(fields[1])
                except ValueError:
                    raise NanocachedError("nanocached: invalid ttl in incr response") from None
                if ttl_seconds < 0:
                    raise NanocachedError("nanocached: invalid ttl in incr response")
            tag = self._parse_tag(fields[2 if has_ttl else 1]) if self._tagged else None
            value = await self._reader.readexactly(length)
            # See _INCR_VALUE_RE's own doc comment: the body must be
            # INCR's own decimal-ASCII-i64 grammar, checked here — where
            # every other malformed piece of this same response (its
            # header, its length, its ttl) already is — rather than left
            # for Connection.incr()'s caller to discover via a bare int()
            # call.
            if not _INCR_VALUE_RE.fullmatch(value) or not (_MIN_I64 <= int(value) <= _MAX_I64):
                raise NanocachedError("nanocached: invalid incr value in response")
            return marker, (value, ttl_seconds), tag

        if marker == b"M":
            # Batched get's response (issues #128/#150/#151): `M <n>
            # <result-1>...<result-n> [<tag>]\n<hit bytes, concatenated in
            # request order>` (docs/protocol.html "m / o"). Unlike V/I
            # this has a variable number of header fields (n of them), so
            # the header line is read whole before any body byte is
            # touched — a lying n can never cause an out-of-bounds read,
            # it only ever fails the field-count check below against
            # whatever readuntil's own bounded read actually delivered.
            # Each token is a decimal byte length (a hit — read that many
            # body bytes next, in request order), "-" (a clean miss), or
            # "W" (this key's own wrong-node).
            header = await self._reader.readuntil(b"\n")
            fields = header[1:-1].split(b" ")
            if len(fields) < 1:
                raise NanocachedError("nanocached: invalid multi-get header in response")
            try:
                count = _parse_strict_uint(fields[0])
            except ValueError:
                count = -1
            if count < 0:
                raise NanocachedError("nanocached: invalid multi-get count in response")
            want_fields = 1 + count + (1 if self._tagged else 0)
            if len(fields) != want_fields:
                raise NanocachedError("nanocached: invalid multi-get header in response")
            tag = self._parse_tag(fields[1 + count]) if self._tagged else None
            entries: list[bytes | None | _MultiWrongNode] = []
            # Cumulative-bytes bound (issue #207): each individual
            # length is already checked against _MAX_VALUE_LENGTH below,
            # but that alone doesn't bound the reply as a whole — track
            # the running total and fail BEFORE readexactly() would
            # allocate the next body, so an oversized claim poisons the
            # connection instead of forcing the allocation first (mirrors
            # _identify.py's _read_entries tracking total_bytes against
            # _MAX_NODE_LIST_RESPONSE_LENGTH).
            total_bytes = 0
            for token in fields[1 : 1 + count]:
                if token == b"-":
                    entries.append(None)
                elif token == b"W":
                    entries.append(WRONG_NODE)
                else:
                    try:
                        length = _parse_strict_uint(token)
                    except ValueError:
                        length = -1
                    if length < 0 or length > _MAX_VALUE_LENGTH:
                        raise NanocachedError(
                            "nanocached: invalid multi-get result length in response"
                        )
                    total_bytes += length
                    if total_bytes > _MAX_MULTIGET_RESPONSE_BYTES:
                        raise NanocachedError(
                            "nanocached: multi-get response exceeds "
                            f"{_MAX_MULTIGET_RESPONSE_BYTES} bytes (connection desynced)"
                        )
                    entries.append(await self._reader.readexactly(length))
            return marker, entries, tag

        if marker == b"O":
            # Batched set's response (issues #128/#150/#151): `O <n>
            # <result-1>...<result-n> [<tag>]\n` — no body, unlike `M`'s
            # hit values (a set has nothing to echo back). Each token is
            # "S" (stored) or "W" (wrong node); parsing otherwise mirrors
            # `M` above. No cumulative-bytes bound needed here (issue
            # #207, unlike `M` above): ack tokens are fixed-width single
            # characters with no length-prefixed body, so this loop is
            # already O(count), and count is already bounded by the
            # header line's own length cap (readuntil's internal limit).
            header = await self._reader.readuntil(b"\n")
            fields = header[1:-1].split(b" ")
            if len(fields) < 1:
                raise NanocachedError("nanocached: invalid multi-set header in response")
            try:
                count = _parse_strict_uint(fields[0])
            except ValueError:
                count = -1
            if count < 0:
                raise NanocachedError("nanocached: invalid multi-set count in response")
            want_fields = 1 + count + (1 if self._tagged else 0)
            if len(fields) != want_fields:
                raise NanocachedError("nanocached: invalid multi-set header in response")
            tag = self._parse_tag(fields[1 + count]) if self._tagged else None
            ack_entries: list[bool | _MultiWrongNode] = []
            for token in fields[1 : 1 + count]:
                if token == b"S":
                    ack_entries.append(True)
                elif token == b"W":
                    ack_entries.append(WRONG_NODE)
                else:
                    raise NanocachedError("nanocached: invalid multi-set result token in response")
            return marker, ack_entries, tag

        if marker in (b"S", b"D", b"N", b"W", b"C", b"R", b"T"):
            if self._tagged:
                # `<marker> <tag>\n` (echoed response tags).
                header = await self._reader.readuntil(b"\n")
                if len(header) < 2 or header[0:1] != b" ":
                    raise NanocachedError("nanocached: response is missing its tag (connection desynced)")
                return marker, None, self._parse_tag(header[1:-1])
            trailer = await self._reader.readexactly(1)
            # The untagged form is always exactly `<marker>\n` — a byte
            # other than LF here means the server tagged a response on an
            # untagged connection (or some other desync), and every later
            # response would be misaligned too. Mirrors the TypeScript
            # SDK's protocol.ts (tryParseResponse) (audit finding,
            # unverified trailing byte on the untagged fast path).
            if trailer != b"\n":
                raise NanocachedError(
                    "nanocached: unexpected byte after response marker (connection desynced)"
                )
            return marker, None, None

        if marker == b"B":
            # `B\n` (busy) is always untagged — an unsolicited response
            # sent before auth, so it never carries a request's tag.
            await self._reader.readexactly(1)  # the trailing '\n'
            return marker, None, None

        raise NanocachedError(f"nanocached: unexpected response from server: {marker!r}")

    @staticmethod
    def _parse_tag(field: bytes) -> int:
        try:
            tag = _parse_strict_uint(field)
        except ValueError:
            tag = -1
        if tag < 0 or tag > _MAX_TAG:
            raise NanocachedError("nanocached: invalid response tag")
        return tag
