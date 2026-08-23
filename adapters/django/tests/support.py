"""Shared Django bootstrap for every test module in this suite.

Django settings can only be configured once per process
(``settings.configure()`` raises on a second call), but
``python3 -m unittest discover`` imports every test module — running each
one's module-level code — before any test actually runs. A per-module
``settings.configure()`` would therefore only ever succeed for whichever
module discovery happens to import first. Instead, every test module
imports this one first (``from support import ...``) and everything here
runs exactly once, however many test modules end up importing it: a
handful of ``MockNode`` instances (one per test module's concerns, so
tests in different modules never see each other's mock-server state) and
one ``CACHES`` block naming a cache alias against each of them.
"""

from __future__ import annotations

import atexit

import django
from django.conf import settings

from mock_node import MockNode

# One MockNode per concern below, so a bug in one test module's traffic
# can never be mistaken for another's — mirrors giving each SDK test its
# own MockNode rather than sharing a single global one.
ROUNDTRIP_NODE = MockNode().start()
TIMEOUT_NODE = MockNode().start()
PREFIX_NODE = MockNode().start()
ISOLATION_NODE = MockNode().start()
PAGE_NODE = MockNode().start()
SECRET_NODE = MockNode(required_secret=b"s3cr3t").start()

_ALL_NODES = (
    ROUNDTRIP_NODE,
    TIMEOUT_NODE,
    PREFIX_NODE,
    ISOLATION_NODE,
    PAGE_NODE,
    SECRET_NODE,
)


def _backend(node: MockNode, *, namespace: str = "django", **extra_options) -> dict:
    entry = {
        "BACKEND": "nanocached_django.NanocachedCache",
        "LOCATION": node.address,
        "OPTIONS": {"NAMESPACE": namespace, **extra_options},
    }
    return entry


if not settings.configured:
    settings.configure(
        CACHES={
            "default": _backend(ROUNDTRIP_NODE),
            # Deliberately short so a test doesn't have to sleep long to
            # observe expiry-adjacent wire TTLs; TIMEOUT is Django's own
            # settings key, honored via BaseCache.__init__.
            "shortdefault": {**_backend(TIMEOUT_NODE, namespace="timeouts"), "TIMEOUT": 5},
            "prefixed": {
                **_backend(PREFIX_NODE, namespace="prefixed"),
                "KEY_PREFIX": "myprefix",
                "VERSION": 3,
            },
            "isolation_a": _backend(ISOLATION_NODE, namespace="alias-a"),
            "isolation_b": _backend(ISOLATION_NODE, namespace="alias-b"),
            "pages": _backend(PAGE_NODE, namespace="pages"),
            "secret": _backend(SECRET_NODE, SECRET="s3cr3t"),
        },
        USE_TZ=True,
        SECRET_KEY="nanocached-django-test-suite",
        ALLOWED_HOSTS=["testserver"],
    )
    django.setup()


def _shutdown_nodes() -> None:
    for node in _ALL_NODES:
        node.close()


atexit.register(_shutdown_nodes)
