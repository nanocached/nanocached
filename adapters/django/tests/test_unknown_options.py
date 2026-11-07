"""An OPTIONS key the backend does not know is a configuration mistake and
raises ImproperlyConfigured at construction, instead of being ignored: a
mistyped ``"Tls": True`` would otherwise leave TLS off and send the secret in
the clear. Django's own BaseCache keys stay valid."""

from __future__ import annotations

import unittest

import support  # noqa: F401 - configures settings.CACHES / django.setup()
from django.core.exceptions import ImproperlyConfigured
from nanocached_django import NanocachedCache


class UnknownOptionsTests(unittest.TestCase):
    def test_a_misspelled_key_is_rejected_and_named(self) -> None:
        with self.assertRaisesRegex(ImproperlyConfigured, "unknown OPTIONS key.*Tls"):
            NanocachedCache("127.0.0.1:1", {"OPTIONS": {"Tls": True}})

    def test_every_unknown_key_is_listed(self) -> None:
        with self.assertRaisesRegex(ImproperlyConfigured, "ALPHA, BETA"):
            NanocachedCache("127.0.0.1:1", {"OPTIONS": {"BETA": 1, "ALPHA": 2}})

    def test_known_keys_and_djangos_own_keys_are_accepted(self) -> None:
        NanocachedCache(
            "127.0.0.1:1",
            {"OPTIONS": {"NAMESPACE": "n", "TLS": False, "MAX_ENTRIES": 10, "cull_frequency": 3}},
        )
