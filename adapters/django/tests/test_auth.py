"""OPTIONS.SECRET is passed straight through to
NanocachedClient.connect(auth_secret=...) — a node that requires a secret
must be reachable when it's configured, and refuse an unauthenticated
client otherwise."""

from __future__ import annotations

import unittest

from support import SECRET_NODE
from django.core.cache import caches
from nanocached import AuthenticationError
from nanocached_django import NanocachedCache


class SecretOptionTests(unittest.TestCase):
    def test_correct_secret_authenticates(self) -> None:
        cache = caches["secret"]  # OPTIONS.SECRET="s3cr3t", see support.py
        cache.set("k", "v")
        self.assertEqual(cache.get("k"), "v")

    def test_missing_secret_is_rejected(self) -> None:
        backend = NanocachedCache(SECRET_NODE.address, {"OPTIONS": {"NAMESPACE": "django"}})
        self.addCleanup(backend.close)
        with self.assertRaises(AuthenticationError):
            backend.set("k", "v")


if __name__ == "__main__":
    unittest.main()
