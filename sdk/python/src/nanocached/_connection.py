"""One already-identified connection to a single nanocached-node, speaking
the cache protocol (``G``/``S``/``D`` — the ``A`` identify exchange happens
in ``_identify`` before a Connection exists).

Requests are pipelined onto the socket and matched to responses in send
order (doc/adr/0016-*.md): a dedicated read loop task consumes responses
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


def _encode_get(key: bytes) -> bytes:
    return b"G %d\n%b" % (len(key), key)


def _encode_set(key: bytes, value: bytes, ttl_seconds: int) -> bytes:
    # 0 means no expiry — omitted from the wire exactly as the old
    # None/absent TTL was.
    if ttl_seconds == 0:
        return b"S %d %d\n%b%b" % (len(key), len(value), key, value)
    return b"S %d %d %d\n%b%b" % (len(key), len(value), ttl_seconds, key, value)


def _encode_delete(key: bytes) -> bytes:
    return b"D %d\n%b" % (len(key), key)


class Connection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        # Serializes "enqueue the pending slot, then write the frame" —
        # not the whole round trip — across concurrent callers, so queue
        # order always matches wire order for the dedicated reader below.
        self._write_lock = asyncio.Lock()
        self._pending: deque[asyncio.Future[tuple[bytes, bytes | None]]] = deque()
        self._closed = False
        self._last_used = time.monotonic()
        self._on_close = on_close
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

    async def get(self, key: bytes) -> bytes | None:
        marker, value = await self._request(_encode_get(key))
        if marker == b"V":
            return value
        if marker == b"N":
            return None
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def set(self, key: bytes, value: bytes, ttl_seconds: int) -> None:
        marker, _ = await self._request(_encode_set(key, value, ttl_seconds))
        if marker == b"W":
            raise WrongNodeError()
        if marker != b"S":
            raise self._mismatch(marker)

    async def delete(self, key: bytes) -> bool:
        marker, _ = await self._request(_encode_delete(key))
        if marker == b"D":
            return True
        if marker == b"N":
            return False
        if marker == b"W":
            raise WrongNodeError()
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
        # the TypeScript SDK's Connection (doc/adr/0016-*.md), not
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
        pending = list(self._pending)
        self._pending.clear()
        self._writer.close()
        for future in pending:
            if not future.cancelled():
                future.set_exception(error)
        if self._on_close is not None:
            self._on_close()

    async def _request(self, frame: bytes) -> tuple[bytes, bytes | None]:
        if self.closed:
            raise ConnectionError("nanocached: connection is closed")

        future: asyncio.Future[tuple[bytes, bytes | None]] = asyncio.get_running_loop().create_future()
        async with self._write_lock:
            if self.closed:
                raise ConnectionError("nanocached: connection is closed")
            self._last_used = time.monotonic()
            self._pending.append(future)
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
        # behind it (doc/adr/0016-*.md). The TypeScript SDK's Connection
        # has no cancellation to handle here at all — plain Promises
        # simply can't be cancelled out from under `pending`. This SDK's
        # asyncio Tasks can be, so this is a real improvement rather than
        # just preserving it (ADR-0016): cancellation is supported, and
        # kept safe by leaving the cancelled future in place instead of
        # ripping it out from under the read loop.
        return await future

    async def _read_loop(self) -> None:
        while True:
            try:
                marker, value = await self._read_one_response()
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

            future = self._pending.popleft()
            if not future.cancelled():
                future.set_result((marker, value))

    async def _read_one_response(self) -> tuple[bytes, bytes | None]:
        marker = await self._reader.readexactly(1)

        if marker == b"V":
            # `V <length>\n<value>`
            header = await self._reader.readuntil(b"\n")
            try:
                length = int(header[1:-1])
            except ValueError:
                length = -1
            # A non-numeric, negative, or absurd length (the server caps
            # requests at 1 MiB) is protocol garbage; the connection is
            # desynced mid-frame and must be poisoned, and the error must
            # be connection-classified so the retry layer handles it
            # (issue #8).
            if length < 0 or length > _MAX_VALUE_LENGTH:
                raise ConnectionError("nanocached: invalid value length in response")
            value = await self._reader.readexactly(length)
            return marker, value

        if marker in (b"S", b"D", b"N", b"W", b"B"):
            await self._reader.readexactly(1)  # the trailing '\n'
            return marker, None

        raise NanocachedError(f"nanocached: unexpected response from server: {marker!r}")
