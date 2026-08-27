"""Compare-and-set (issue #141): the content digest the ``k``/``x`` ops'
``<cond>`` field carries when conditioning on exact prior content — see
docs/protocol.html "k / x" for the wire spec this implements against.

The digest is SHA-256 of the key's exact stored bytes — for a
compress-enabled client, that's the marker-byte-prefixed bytes ``V``/``I``
actually carry on the wire, since the server never decompresses (see
_compression.py's module docstring) — truncated to the first 16 bytes
(128 bits), lowercase hex-encoded (32 characters). Computed identically by
the server and every SDK; a fixed cross-language test vector pins the
agreement (see content_digest's docstring below)."""

from __future__ import annotations

import hashlib


def content_digest(value: bytes) -> str:
    """The CAS token for ``value``'s exact bytes: a 32-character lowercase
    hex string. Pass it as ``replace()``'s or ``delete_if_matches()``'s
    ``token`` to condition on an exact prior read — normally one obtained
    from ``get_with_token()``, though this function is exposed standalone
    so a caller (or a future framework adapter) can also derive an
    expected token from a value it already holds without a prior GET. See
    ``NanocachedClient.replace()`` for why that reconstruction path is
    only correct when it reproduces byte-identical wire content.

    Cross-language test vector: ``content_digest(b"nanocached-cas-vector")``
    is exactly ``"36287141940ca57acbd7695ccdde9d43"`` — the same fixed
    input/output pair pinned into the Rust server and every other SDK
    (docs/protocol.html "k / x"), so a mismatch here means CAS silently
    breaks across languages."""
    return hashlib.sha256(value).digest()[:16].hex()
