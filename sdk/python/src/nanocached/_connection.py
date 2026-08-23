"""One already-identified connection to a single nanocached-node, speaking
the cache protocol (``G``/``S``/``D``, and their namespaced counterparts
``g``/``s``/``d`` — issue #105 — plus ``c``/``F`` to clear a namespace or
flush every namespace, issue #106 — the ``A`` identify exchange happens
in ``_identify`` before a Connection exists).

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
import time
from collections import deque
from collections.abc import Callable

from ._errors import NanocachedError, WrongNodeError

# The server's own request cap is 1 MiB; this constant doubles that as
# headroom, so a claimed length beyond it is definitely a corrupt or
# malicious frame, never just a legitimately large value.
_MAX_VALUE_LENGTH = 2 * 1024 * 1024

# A tag is a u32 in decimal (echoed response tags).
_MAX_TAG = 0xFFFFFFFF

# Bounds how long the connection may go without progress while requests
# are outstanding (issue #42) — each response must arrive within this
# window of the previous one (or of its own send, when the queue was
# empty): without it, a half-open server that accepts the TCP connection
# but never writes back — or stops mid-stream — would hang get/set/delete
# forever. Generous versus the server's own 10s outbound timeouts, and
# the same 30s the Go and Rust SDKs use. A module global only so tests
# can shorten it.
_REQUEST_TIMEOUT = 30.0


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


class Connection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_close: Callable[[], None] | None = None,
        tagged: bool = False,
    ) -> None:
        self._reader = reader
        self._writer = writer
        # Echoed response tags: negotiated during identify — when true, every request
        # carries a tag the server echoes, and _read_loop verifies the
        # echo against the oldest pending slot before resolving it.
        self._tagged = tagged
        self._next_tag = 0
        # Serializes "enqueue the pending slot, then write the frame" —
        # not the whole round trip — across concurrent callers, so queue
        # order always matches wire order for the dedicated reader below.
        self._write_lock = asyncio.Lock()
        # Each slot pairs the future with the tag its request was sent
        # under (None on an untagged connection) — the expected echo
        # _read_loop checks the response against before handing it out.
        self._pending: deque[tuple[int | None, asyncio.Future[tuple[bytes, bytes | None]]]] = deque()
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
        error = ConnectionError(
            f"nanocached: response {marker!r} does not match the request (connection desynced)"
        )
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
        for _tag, future in pending:
            if not future.cancelled():
                future.set_exception(error)
        if self._on_close is not None:
            self._on_close()

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

    async def _request(self, build: Callable[[int | None], bytes]) -> tuple[bytes, bytes | None]:
        if self.closed:
            raise ConnectionError("nanocached: connection is closed")

        future: asyncio.Future[tuple[bytes, bytes | None]] = asyncio.get_running_loop().create_future()
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
            self._pending.append((tag, future))
            # Armed only on the empty→non-empty transition: arming on
            # *every* request would let a continuous stream of new
            # requests push the deadline forever ahead of a server that
            # has stopped answering — exactly the half-open hang the
            # timeout exists to catch.
            if len(self._pending) == 1:
                self._arm_deadline()
            try:
                self._writer.write(frame)
                await self._writer.drain()
            except OSError as error:
                wrapped = ConnectionError(f"nanocached: connection failed: {error}")
                wrapped.__cause__ = error
                self._poison(wrapped)
            except asyncio.CancelledError:
                # Cancelled mid-write: the frame may be only partially on
                # the wire, desyncing every request queued behind this
                # one too — unlike cancellation while awaiting the
                # response (below), this can't be scoped to just this
                # one request.
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
        return await future

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

            expected_tag, future = self._pending.popleft()

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
    # ConnectionError, this SDK's analog of the TypeScript SDK's
    # ConnectionLostError — see errors.ts's own doc comment on that
    # parity), even though both are equally poisoning and equally
    # swallowable by the retry layer (_SWALLOWABLE_ERRORS covers both).
    async def _read_one_response(self) -> tuple[bytes, bytes | None, int | None]:
        marker = await self._reader.readexactly(1)

        if marker == b"V":
            # Untagged: `V <length>\n<value>`. Tagged: `V <length> <tag>\n<value>`
            # (echoed response tags).
            header = await self._reader.readuntil(b"\n")
            fields = header[1:-1].split(b" ")
            if len(fields) != (2 if self._tagged else 1):
                raise NanocachedError("nanocached: invalid value header in response")
            try:
                length = int(fields[0])
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

        if marker in (b"S", b"D", b"N", b"W", b"C"):
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
            # SDK's protocol.ts (tryParseResponse) (issue: audit finding,
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
            tag = int(field)
        except ValueError:
            tag = -1
        if tag < 0 or tag > _MAX_TAG:
            raise NanocachedError("nanocached: invalid response tag")
        return tag
