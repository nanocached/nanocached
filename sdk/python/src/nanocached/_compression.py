"""doc/adr/0013-*.md: transparent, opt-in value compression. Values are
prefixed with a one-byte marker once ``compress`` is enabled on a client —
``0x00`` for stored-as-is (below threshold, or compression didn't shrink
it), ``0x01`` for raw-DEFLATE-compressed (RFC 1951, no zlib/gzip
wrapper). Never present when ``compress`` is off."""

from __future__ import annotations

import zlib

from ._errors import NanocachedError

_MARKER_RAW = 0x00
_MARKER_DEFLATE = 0x01

# Raw DEFLATE (no zlib header/checksum): wbits=-15 selects it on both the
# compress and decompress side. See doc/adr/0013-*.md for why raw DEFLATE
# was chosen over a zlib/gzip wrapper.
_WBITS_RAW_DEFLATE = -15


class DecompressionError(NanocachedError):
    """Raised by get/get_bytes when a value with ``compress`` enabled
    can't be interpreted — almost always a ``compress`` mismatch between
    clients sharing this key (doc/adr/0013-*.md's compatibility caveat:
    every client touching a given keyspace must agree on ``compress``),
    not a transient failure."""


def compress_value(value: bytes, threshold: int) -> bytes:
    """Below ``threshold``, or when compressing doesn't actually shrink
    the value (incompressible data), the marker byte alone is added and
    the value is stored unchanged. Always returns a value with the marker
    byte prefixed."""
    if len(value) < threshold:
        return bytes([_MARKER_RAW]) + value

    # zlib.compress() always emits a zlib-wrapped stream; compressobj()
    # with a negative wbits is what produces the header-less raw DEFLATE
    # form this SDK's wire format uses (see the module docstring).
    compressor = zlib.compressobj(6, zlib.DEFLATED, _WBITS_RAW_DEFLATE)
    compressed = compressor.compress(value) + compressor.flush()

    if len(compressed) < len(value):
        return bytes([_MARKER_DEFLATE]) + compressed
    return bytes([_MARKER_RAW]) + value


def decompress_value(value: bytes) -> bytes:
    """The get/get_bytes counterpart to compress_value(). Only ever called
    when ``compress`` is enabled on this client — see doc/adr/0013-*.md."""
    if len(value) == 0:
        raise DecompressionError(
            "nanocached: compress is enabled but this value has no marker byte "
            "(it's empty) — was it written by a client with compress disabled?"
        )

    marker, body = value[0], value[1:]

    if marker == _MARKER_RAW:
        return body

    if marker == _MARKER_DEFLATE:
        try:
            decompressor = zlib.decompressobj(_WBITS_RAW_DEFLATE)
            return decompressor.decompress(body) + decompressor.flush()
        except zlib.error as error:
            raise DecompressionError(
                "nanocached: failed to decompress a value marked as compressed — "
                f"did every client sharing this key enable compress (doc/adr/0013-*.md)? ({error})"
            ) from error

    raise DecompressionError(
        f"nanocached: unrecognized compression marker byte {marker} — was this value "
        "written by a client with compress disabled (doc/adr/0013-*.md)?"
    )
