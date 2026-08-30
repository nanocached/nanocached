"""issue #231: OPTIONS only ever read NAMESPACE/SECRET/CLOSE_ON_REQUEST,
so a Django deployment had no way to opt into TLS, compression, fire-
and-forget replica writes, read repair, hedged reads or a non-default
reconnect cooldown — every other adapter exposes at least tls/ca/
compress. This module asserts each of OPTIONS.TLS/CA/COMPRESS/
COMPRESSION_THRESHOLD/FIRE_AND_FORGET_REPLICAS/READ_REPAIR/
READ_HEDGE_AFTER/RECONNECT_COOLDOWN reaches NanocachedClient.connect()
as its matching keyword argument, and that an OPTIONS key the CACHES
entry never mentions is simply absent from the connect() call — so the
SDK's own default stays in force there, rather than this backend
silently picking a second default of its own.

Framework-level per this suite's convention (test_auth.py): exercised
through NanocachedCache/OPTIONS, not by calling backend internals
directly. Unlike test_auth.py this doesn't need a real MockNode — the
whole point is to intercept the connect() call before it would ever
touch a socket, so NanocachedClient.connect is patched instead.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from nanocached_django import NanocachedCache
from nanocached_django.backend import NanocachedClient


class ConnectOptionsTests(unittest.TestCase):
    def _connect_kwargs(self, **options) -> dict:
        """Builds a NanocachedCache with the given extra OPTIONS, forces
        the lazy connect with NanocachedClient.connect() replaced by a
        stub that records what it was called with instead of touching
        the network, and returns just the keyword arguments (positional
        ``addresses`` and the always-present ``auth_secret`` stripped,
        since this module only cares about the issue #231 options)."""
        backend = NanocachedCache(
            "127.0.0.1:1", {"OPTIONS": {"NAMESPACE": "django", **options}}
        )
        self.addCleanup(backend.shutdown)

        fake_client = AsyncMock()
        fake_client.namespace = lambda ns: object()  # sync, unlike the rest of the client

        with patch.object(
            NanocachedClient, "connect", AsyncMock(return_value=fake_client)
        ) as mock_connect:
            backend._ensure_started()

        _, kwargs = mock_connect.call_args
        kwargs.pop("auth_secret", None)
        return kwargs

    def test_no_options_forwards_nothing_extra(self) -> None:
        # Nothing beyond NAMESPACE/SECRET's own auth_secret (stripped
        # above) — every SDK default must stay in force.
        self.assertEqual(self._connect_kwargs(), {})

    def test_tls_and_ca(self) -> None:
        self.assertEqual(
            self._connect_kwargs(TLS=True, CA="/etc/nanocached/ca.pem"),
            {"tls": True, "ca": "/etc/nanocached/ca.pem"},
        )

    def test_compress_and_threshold(self) -> None:
        self.assertEqual(
            self._connect_kwargs(COMPRESS=True, COMPRESSION_THRESHOLD=512),
            {"compress": True, "compression_threshold": 512},
        )

    def test_fire_and_forget_replicas(self) -> None:
        self.assertEqual(
            self._connect_kwargs(FIRE_AND_FORGET_REPLICAS=True),
            {"fire_and_forget_replicas": True},
        )

    def test_read_repair(self) -> None:
        self.assertEqual(self._connect_kwargs(READ_REPAIR=True), {"read_repair": True})

    def test_read_hedge_after(self) -> None:
        self.assertEqual(
            self._connect_kwargs(READ_HEDGE_AFTER=0.05), {"read_hedge_after": 0.05}
        )

    def test_reconnect_cooldown(self) -> None:
        self.assertEqual(
            self._connect_kwargs(RECONNECT_COOLDOWN=2.5), {"reconnect_cooldown": 2.5}
        )

    def test_a_falsy_but_explicit_option_is_still_forwarded(self) -> None:
        # OPTIONS.TLS=False is a deliberate choice, not the same as
        # OPTIONS never mentioning TLS at all — `in options`, not
        # truthiness, is what must gate forwarding.
        self.assertEqual(self._connect_kwargs(TLS=False), {"tls": False})

    def test_all_options_together(self) -> None:
        self.assertEqual(
            self._connect_kwargs(
                TLS=True,
                CA="/etc/nanocached/ca.pem",
                COMPRESS=True,
                COMPRESSION_THRESHOLD=256,
                FIRE_AND_FORGET_REPLICAS=True,
                READ_REPAIR=True,
                READ_HEDGE_AFTER=0.1,
                RECONNECT_COOLDOWN=1.0,
            ),
            {
                "tls": True,
                "ca": "/etc/nanocached/ca.pem",
                "compress": True,
                "compression_threshold": 256,
                "fire_and_forget_replicas": True,
                "read_repair": True,
                "read_hedge_after": 0.1,
                "reconnect_cooldown": 1.0,
            },
        )


if __name__ == "__main__":
    unittest.main()
