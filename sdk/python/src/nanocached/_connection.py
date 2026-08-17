"""One already-identified connection to a single nanocached-node, speaking
the cache protocol (``G``/``S``/``D`` — the ``A`` identify exchange happens
in ``_identify`` before a Connection exists).

Unlike the TypeScript SDK, requests are *serialized* per connection (an
asyncio lock around each request/response round trip) rather than
pipelined — a deliberate v1 simplification: nanocached-node answers in
arrival order, so serializing is always correct, just less concurrent.
Concurrent callers queue on the lock.
"""

from __future__ import annotations

import asyncio
import time

from ._errors import NanocachedError, WrongNodeError

# The server never stores values above its 1 MiB request limit, so a
# claimed length beyond this is a corrupt or malicious frame.
_MAX_VALUE_LENGTH = 2 * 1024 * 1024


def _encode_get(key: bytes) -> bytes:
    return b"G %d\n%b" % (len(key), key)


def _encode_set(key: bytes, value: bytes, ttl_seconds: int | None) -> bytes:
    if ttl_seconds is None:
        return b"S %d %d\n%b%b" % (len(key), len(value), key, value)
    return b"S %d %d %d\n%b%b" % (len(key), len(value), ttl_seconds, key, value)


def _encode_delete(key: bytes) -> bytes:
    return b"D %d\n%b" % (len(key), key)


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._lock = asyncio.Lock()
        self._closed = False
        self._last_used = time.monotonic()

    @property
    def closed(self) -> bool:
        # A peer close (e.g. the server's 30s idle timeout) is only
        # observed on the next read, so also probe the transport.
        return self._closed or self._writer.is_closing() or self._reader.at_eof()

    def idle_seconds(self) -> float:
        return time.monotonic() - self._last_used

    def close(self) -> None:
        self._closed = True
        self._writer.close()

    async def get(self, key: bytes) -> bytes | None:
        marker, value = await self._request(_encode_get(key))
        if marker == b"V":
            return value
        if marker == b"N":
            return None
        if marker == b"W":
            raise WrongNodeError()
        raise self._mismatch(marker)

    async def set(self, key: bytes, value: bytes, ttl_seconds: int | None) -> None:
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
        self.close()
        return ConnectionError(
            f"nanocached: response {marker!r} does not match the request (connection desynced)"
        )

    async def _request(self, frame: bytes) -> tuple[bytes, bytes | None]:
        if self.closed:
            raise ConnectionError("nanocached: connection is closed")

        async with self._lock:
            self._last_used = time.monotonic()
            try:
                self._writer.write(frame)
                await self._writer.drain()
                return await self._read_response()
            except (asyncio.IncompleteReadError, OSError) as error:
                # The stream state after a failed round trip is unknown —
                # poison the connection so the client redials lazily.
                self.close()
                raise ConnectionError(f"nanocached: connection failed: {error}") from error
            except asyncio.CancelledError:
                # The caller abandoned this request (asyncio.wait_for and
                # friends) after bytes may already be on the wire; a later
                # request would read THIS request's response and desync the
                # stream. The connection cannot be reused.
                self.close()
                raise

    async def _read_response(self) -> tuple[bytes, bytes | None]:
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
                self.close()
                raise ConnectionError("nanocached: invalid value length in response")
            value = await self._reader.readexactly(length)
            return marker, value

        if marker in (b"S", b"D", b"N", b"W"):
            await self._reader.readexactly(1)  # the trailing '\n'
            return marker, None

        if marker == b"B":
            # An unsolicited busy: the server hit its connection limit and
            # is closing this connection.
            self.close()
            raise ConnectionError("nanocached: server rejected the connection (connection limit reached)")

        self.close()
        raise NanocachedError(f"nanocached: unexpected response from server: {marker!r}")
