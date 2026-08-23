"""cache_page end-to-end (issue #108 spec's #1 lesson from #107 again: the
framework's own idioms, not just this backend's methods) — a real view
wrapped in django.views.decorators.cache.cache_page, driven through
django.test.RequestFactory, with a call counter proving the second
request is served from the cache instead of re-invoking the view."""

from __future__ import annotations

import unittest

from support import PAGE_NODE
from django.http import HttpResponse
from django.test import RequestFactory
from django.views.decorators.cache import cache_page

_view_calls = []


def _counting_view(request):
    _view_calls.append(1)
    return HttpResponse(f"call #{len(_view_calls)}")


_cached_view = cache_page(60, cache="pages")(_counting_view)


class CachePageIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        _view_calls.clear()
        from django.core.cache import caches

        caches["pages"].clear()
        self.factory = RequestFactory()

    def test_second_request_is_served_from_cache(self) -> None:
        request_one = self.factory.get("/cached-view/")
        response_one = _cached_view(request_one)
        self.assertEqual(response_one.status_code, 200)
        self.assertEqual(len(_view_calls), 1)

        request_two = self.factory.get("/cached-view/")
        response_two = _cached_view(request_two)
        self.assertEqual(response_two.status_code, 200)
        # Still 1: the view itself was not invoked a second time.
        self.assertEqual(len(_view_calls), 1)
        self.assertEqual(response_one.content, response_two.content)

    def test_cached_entries_are_visible_in_the_mock_store(self) -> None:
        request = self.factory.get("/another-cached-view/")
        _cached_view(request)
        # cache_page writes at least the response entry (plus a small
        # header entry keyed by the same URL) under the "pages" namespace
        # — assert something landed on the wire rather than pinning the
        # exact key shape CacheMiddleware happens to use internally.
        pages_entries = [key for ns, key in PAGE_NODE.ns_store if ns == b"pages"]
        self.assertTrue(pages_entries)


if __name__ == "__main__":
    unittest.main()
