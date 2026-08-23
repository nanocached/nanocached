"""Django timeout conventions translated to nanocached's wire TTL — see
NanocachedCache.get_backend_timeout's docstring for the mapping. Every
assertion here reads the TTL the mock node actually received, not just
whether the value round-trips, since the whole point is the *translation*."""

from __future__ import annotations

import unittest

from support import PREFIX_NODE, TIMEOUT_NODE
from django.core.cache import caches


class TimeoutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = caches["shortdefault"]  # TIMEOUT: 5, see support.py
        self.cache.clear()

    def _wire_ttl(self, key: str) -> int:
        wire_key = self.cache.make_key(key, version=self.cache.version).encode()
        return TIMEOUT_NODE.ttls[(b"timeouts", wire_key)]

    def test_default_timeout_reaches_the_wire(self) -> None:
        self.cache.set("uses-default", "value")
        self.assertEqual(self._wire_ttl("uses-default"), 5)

    def test_per_call_timeout_overrides_the_default(self) -> None:
        self.cache.set("overridden", "value", timeout=42)
        self.assertEqual(self._wire_ttl("overridden"), 42)

    def test_none_timeout_means_no_expiry_on_the_wire(self) -> None:
        # Django's None ("never expires") and nanocached's wire TTL 0
        # ("no expiry") are the one case where the two conventions agree.
        self.cache.set("eternal", "value", timeout=None)
        self.assertEqual(self._wire_ttl("eternal"), 0)

    def test_zero_timeout_stores_nothing(self) -> None:
        # The opposite polarity case: Django's timeout=0 means "expire
        # immediately", which the wire has no TTL for (wire 0 means
        # eternal) — so set() must not write the key at all.
        self.cache.set("never-cached", "value", timeout=0)
        self.assertIsNone(self.cache.get("never-cached"))
        wire_key = self.cache.make_key("never-cached", version=self.cache.version).encode()
        self.assertNotIn((b"timeouts", wire_key), TIMEOUT_NODE.ns_store)

    def test_zero_timeout_deletes_an_existing_value(self) -> None:
        self.cache.set("was-cached", "value", timeout=300)
        self.assertIsNotNone(self.cache.get("was-cached"))
        self.cache.set("was-cached", "value", timeout=0)
        self.assertIsNone(self.cache.get("was-cached"))

    def test_negative_timeout_also_stores_nothing(self) -> None:
        self.cache.set("negative", "value", timeout=-5)
        self.assertIsNone(self.cache.get("negative"))

    def test_sub_second_timeout_rounds_up_to_one_second(self) -> None:
        self.cache.set("sub-second", "value", timeout=0.2)
        self.assertEqual(self._wire_ttl("sub-second"), 1)

    def test_fractional_timeout_rounds_up(self) -> None:
        self.cache.set("fractional", "value", timeout=1.1)
        self.assertEqual(self._wire_ttl("fractional"), 2)


class KeyPrefixAndVersionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = caches["prefixed"]  # KEY_PREFIX="myprefix", VERSION=3
        self.cache.clear()

    def test_key_prefix_and_version_produce_the_documented_wire_key(self) -> None:
        self.cache.set("mykey", "value")
        # Django's default_key_func: "%s:%s:%s" % (key_prefix, version, key).
        expected_wire_key = b"myprefix:3:mykey"
        self.assertIn((b"prefixed", expected_wire_key), PREFIX_NODE.ns_store)

    def test_different_version_is_a_distinct_wire_key(self) -> None:
        self.cache.set("mykey", "v3-value")
        self.cache.set("mykey", "v7-value", version=7)
        self.assertEqual(self.cache.get("mykey"), "v3-value")
        self.assertEqual(self.cache.get("mykey", version=7), "v7-value")
        self.assertIn((b"prefixed", b"myprefix:3:mykey"), PREFIX_NODE.ns_store)
        self.assertIn((b"prefixed", b"myprefix:7:mykey"), PREFIX_NODE.ns_store)


if __name__ == "__main__":
    unittest.main()
