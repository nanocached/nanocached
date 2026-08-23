# nanocached-django

Django cache backend for the [nanocached](https://github.com/nanocached/nanocached)
Python SDK: `django.core.cache.backends.base.BaseCache` implemented on
`nanocached.NanocachedClient`, so `django.core.cache.caches`, `@cache_page`
and friends run against a nanocached cluster.

- **Cache alias ⇄ namespace.** Each `NanocachedCache` instance binds to
  one nanocached namespace (`OPTIONS.NAMESPACE`, default `"django"`) — two
  aliases with different namespaces never collide, even sharing the same
  node, and `cache.clear()` is that namespace's `CLEAR` (an O(1) sub-map
  drop on every node, no key scan, and never the whole store).
- **`get`/`set`/`delete`/`has_key`/`touch`** map onto the SDK's namespaced
  get/set/delete, with all of its routing, replication, and retries.
- **`add`** and **`touch`** are get-then-set, not atomic — the wire has no
  compare-and-set. Two racing callers can both observe "absent" and both
  write; the later write wins. Same trade-off the Spring adapter's
  `putIfAbsent` documents.
- **`get_many`/`set_many`/`delete_many`** are client-side loops — the wire
  has no multi-key command.
- **`incr`/`decr` raise `NotImplementedError`.** The wire has no atomic
  counter, and `BaseCache`'s default get-then-set emulation would race
  silently under concurrent access; this backend refuses instead of
  pretending to be atomic.
- **Values are pickled** (`pickle.dumps(..., HIGHEST_PROTOCOL)`), the
  Django convention — anything picklable round-trips, `None` included
  (distinguished from a cache miss, which is nanocached's own "no value").

## Setup

Point `CACHES["default"]["BACKEND"]` at this class — adding the
`nanocached-django` dependency alone changes nothing:

```python
CACHES = {
    "default": {
        "BACKEND": "nanocached_django.NanocachedCache",
        "LOCATION": ["10.0.0.1:8357", "10.0.0.2:8357"],  # or "host:port"
        "OPTIONS": {
            "NAMESPACE": "django",   # default "django"
            "SECRET": "...",         # optional; passed to connect() as auth_secret
        },
        "TIMEOUT": 300,               # Django's own key, honored as usual
        "KEY_PREFIX": "...",
        "VERSION": 1,
    }
}
```

`LOCATION` may name a single nanocached node or one or more discovery
servers fronting a cluster — same as `NanocachedClient.connect()`, this
backend doesn't need to be told which; it finds out from the server's own
handshake. A `LOCATION` list is one address per entry; a single string may
also carry more than one address separated by `,`/`;`.

## The sync/async bridge

The Python SDK is asyncio-only; Django's cache SPI is sync-first. Each
`NanocachedCache` instance owns a dedicated daemon-thread event loop,
started lazily on first use, and every sync call (`get`/`set`/...) is
dispatched onto it with `asyncio.run_coroutine_threadsafe(...).result()` —
never `asyncio.run()` per call, which would redo the SDK's connect
handshake on every single cache operation instead of reusing one
persistent connection, and never requires an ambient running loop either.

Django's own `aget`/`aset`/`aadd`/... come for free: `BaseCache` wraps
every sync method in `sync_to_async(..., thread_sensitive=True)`, so this
backend implements no native async path of its own — doing so would just
be a second bridge fighting the first for the same event loop.

### Lifecycle note

Django connects `close_caches()` to the `request_finished` signal
unconditionally (`django/core/cache/__init__.py`), which calls `.close()`
on every cache alias a thread has already touched — after **every**
request. Honoring that literally would re-dial TCP (+ auth, + a discovery
refresh in cluster mode) once per request, defeating the persistent
connections the client is built around. So, like django-redis, this
backend's `close()` is a **no-op by default**: the loop thread and
connection survive the request cycle.

- `OPTIONS: {"CLOSE_ON_REQUEST": True}` opts into real per-request
  teardown, if short-lived processes matter more than connection reuse.
- `backend.shutdown()` always tears down (loop thread stopped, client
  closed); the next operation lazily reconnects. Use it at process exit
  or in tests.

## Timeouts

Django's `timeout` conventions are translated to nanocached's wire TTL
(whole seconds; on the wire, unlike Django's own convention, `0` means
"no expiry", not "expire now"):

| Django `timeout`                | wire TTL                              |
| -------------------------------- | -------------------------------------- |
| not passed (`DEFAULT_TIMEOUT`)   | this alias's `TIMEOUT` setting         |
| `None`                            | `0` (no expiry — the one case the two conventions agree) |
| `0` or negative                   | nothing is written; `set()` deletes any existing value instead |
| positive, sub-second              | rounds **up** to `1` (never down to `0`, which would mean eternal) |
| positive, ≥ 1s                    | rounds up to the next whole second |

## Usage

With `CACHES` pointed at this backend, use the standard API:

```python
from django.core.cache import cache, caches

cache.set("greeting", "hello", timeout=60)
cache.get("greeting")            # "hello"
cache.get("missing", "default")  # "default"

sessions = caches["sessions"]    # a second alias, its own NAMESPACE
```

`@cache_page` and the whole-site `CacheMiddleware` work unchanged:

```python
from django.views.decorators.cache import cache_page

@cache_page(60, cache="default")
def my_view(request):
    ...
```

## Consistency notes

The wire has single-key get/set/delete and no compare-and-set, so `add()`
and `touch()` are get-then-set (see above); `get_many`/`set_many`/
`delete_many` are client-side loops, so a failure partway through leaves
whichever keys were already processed changed rather than rolling back.

## Requirements

Python 3.11+, Django 4.2+, `nanocached` (Python SDK) 0.3+, a nanocached
server ≥ the release that ships namespaces and CLEAR (issues #105/#106).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for Django; other ecosystems get their own
idiomatic adapters (Spring Cache, `IDistributedCache`, cache-manager
store, JCache) rather than mirrors of this one.

[#25]: https://github.com/nanocached/nanocached/issues/25

## Testing

```sh
cd adapters/django
PYTHONPATH=src:../../sdk/python/src python3 -m unittest discover -s tests -v
```

Tests run against a small in-module `MockNode` (`tests/mock_node.py`, a
trimmed re-implementation of the SDK's own test double, which is private
to the SDK's suite) speaking the wire protocol directly — no server
binary needed, and no PyPI install of the SDK either, since the sibling
`sdk/python` sources are put on `PYTHONPATH` instead.

MIT license.
