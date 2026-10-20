"""LOCATION parsing, through the backend's constructor: what reaches
``NanocachedClient.connect()`` as the address list for each spelling
Django's own ``RedisCache`` convention allows, plus the bracketed IPv6
form a host:port string needs to keep the address's colons apart from
the port."""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from django.core.exceptions import ImproperlyConfigured
from nanocached_django import NanocachedCache
from nanocached_django.backend import NanocachedClient


class LocationTests(unittest.TestCase):
    def _addresses(self, location) -> list[tuple[str, int]]:
        backend = NanocachedCache(location, {"OPTIONS": {"NAMESPACE": "django"}})
        self.addCleanup(backend.shutdown)

        fake_client = AsyncMock()
        fake_client.namespace = lambda ns: object()

        with patch.object(
            NanocachedClient, "connect", AsyncMock(return_value=fake_client)
        ) as mock_connect:
            backend._ensure_started()

        (addresses,), _ = mock_connect.call_args
        return list(addresses)

    def test_host_port_string(self) -> None:
        self.assertEqual(self._addresses("10.0.0.1:8357"), [("10.0.0.1", 8357)])

    def test_bracketed_ipv6_literal_loses_its_brackets(self) -> None:
        # The SDK wants the bare address; "[::1]" would be resolved as a
        # host name and fail with gaierror on the first cache call.
        self.assertEqual(self._addresses("[::1]:8357"), [("::1", 8357)])
        self.assertEqual(
            self._addresses(["[fe80::1%eth0]:8357", "[::1]:8358"]),
            [("fe80::1%eth0", 8357), ("::1", 8358)],
        )

    def test_separated_string_and_list_forms(self) -> None:
        self.assertEqual(
            self._addresses("a:1; [::1]:2 ,b:3"),
            [("a", 1), ("::1", 2), ("b", 3)],
        )
        self.assertEqual(self._addresses(["a:1", "b:2"]), [("a", 1), ("b", 2)])

    def test_invalid_entries_are_rejected_at_construction(self) -> None:
        for bad in ("no-port", "host:", "host:abc", "[::1]", "", None, []):
            with self.subTest(location=bad), self.assertRaises(ImproperlyConfigured):
                NanocachedCache(bad, {"OPTIONS": {"NAMESPACE": "django"}})


if __name__ == "__main__":
    unittest.main()
