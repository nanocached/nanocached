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
- **`get_many`/`set_many`** use the wire's batched multi-get/multi-set
  (issues #150/#152): one round trip per involved node instead of one per
  key. **`delete_many`** remains a concurrent client-side fan-out — the
  wire has no multi-key delete.
- **`incr`/`decr`** use the wire's own atomic counter (`INCR`, issue
  #129): atomic on the node that owns the key, unlike `add()`/`touch()`
  above. Matches `BaseCache`'s own contract — raises `ValueError` if the
  key doesn't exist — and is exactly as volatile as `set()`: LRU
  eviction or this alias's `TIMEOUT` reclaim a counter the same as any
  other entry, so it suits rate limiting and approximate counts, never a
  count that must survive (billing, inventory). See "Counter storage"
  below for how this changes what's on the wire.
- **Values are pickled** (`pickle.dumps(..., HIGHEST_PROTOCOL)`), the
  Django convention — anything picklable round-trips, `None` included
  (distinguished from a cache miss, which is nanocached's own "no value")
  — **except a plain `int`** (`bool` excluded), which is stored as
  `INCR`'s own decimal-ASCII counter grammar instead, so `incr`/`decr`
  can operate on it server-side. See "Counter storage" below.

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
            "TLS": True,             # optional; connect() tls, default False
            "CA": "/path/ca.pem",    # optional; connect() ca, only meaningful with TLS
            "COMPRESS": True,        # optional; connect() compress, default False
            "COMPRESSION_THRESHOLD": 512,     # optional; connect() compression_threshold
            "FIRE_AND_FORGET_REPLICAS": True, # optional; connect() fire_and_forget_replicas
            "READ_REPAIR": True,              # optional; connect() read_repair
            "READ_HEDGE_AFTER": 0.05,         # optional; connect() read_hedge_after (seconds)
            "RECONNECT_COOLDOWN": 1.0,        # optional; connect() reconnect_cooldown (seconds)
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

`TLS`/`CA`/`COMPRESS`/`COMPRESSION_THRESHOLD`/`FIRE_AND_FORGET_REPLICAS`/
`READ_REPAIR`/`READ_HEDGE_AFTER`/`RECONNECT_COOLDOWN` are forwarded
straight through to the matching `NanocachedClient.connect()` keyword
(`tls`, `ca`, `compress`, `compression_threshold`,
`fire_and_forget_replicas`, `read_repair`, `read_hedge_after`,
`reconnect_cooldown` — see that method's docstring for what each one
does) — each is optional and, when the OPTIONS entry omits it, the SDK's
own default is what actually takes effect, not some second default
chosen by this backend. `CA` is a filesystem path to a CA bundle and only
matters when `TLS` is also true. `READ_HEDGE_AFTER` and
`RECONNECT_COOLDOWN` are seconds (`float`); everything else here is a
`bool` except `COMPRESSION_THRESHOLD`, an `int` byte count.

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

### Preforking servers (Gunicorn `preload_app`, uWSGI)

The bridge is fork-aware (issue #393): the backend records which process
started its loop thread and rebuilds the loop, thread and client on the
first cache operation in a `fork()` child. Without that, a warm-up cache
touch in a preloading master (e.g. from `AppConfig.ready()`) would leave
every worker holding a loop whose driving thread died in the fork —
and each operation would block forever on
`run_coroutine_threadsafe(...).result()`. The parent's inherited sockets
are deliberately left untouched in the child; the parent remains their
owner. No configuration is needed — `preload_app = True` and uWSGI
without `lazy-apps` both just work.

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

`add()` and `touch()` are get-then-set (see above), not atomic.
`get_many`/`set_many` batch by owning node (issues #150/#152) and
`delete_many` fans out per key — in every case a failure partway through
leaves whichever keys were already processed changed rather than rolling
back. `incr`/`decr` (issue #129) are atomic on the node that owns the
key, since the wire has a real `INCR`.

## Counter storage

To make `incr`/`decr` possible, this backend changed what a plain `int`
value looks like on the wire: instead of the usual pickle round trip, an
`int` (excluding `bool`) is stored as `INCR`'s own decimal-ASCII grammar
— the same bytes the wire's atomic counter reads and writes directly.
Everything else — `str`, `dict`, model instances, `None`, `bool`, ... —
is still pickled exactly as before. `get()` tells the two apart by the
bytes themselves: a pickle stream always begins with the `0x80` PROTO
opcode (Python's pickle protocol 2+, which `HIGHEST_PROTOCOL` always is),
a byte that can never start an ASCII decimal integer, so there is no
ambiguity and no extra marker byte on the wire.

**This is a compatibility break for a rolling deploy that shares a cache
across old and new `nanocached-django` code**, not a nanocached-server
concern at all — the node just stores whatever opaque bytes it's given,
on any version. The risk is entirely between Django app instances:

- An **old** worker's `get()` unconditionally calls `pickle.loads(raw)`.
  If a **new** worker already wrote an `int` under that key (as raw
  decimal-ASCII, not pickled), the old worker's `get()`/`get_many()`
  **raises** (an unpickling error), not just returns a wrong value.
- A **new** worker's `get()` handles both encodings fine, including an
  `int` an old worker pickled — that direction round-trips correctly.

So the break is one-directional (old code choking on new-written ints)
and only matters while old and new code run concurrently against the
same shared keys — a normal rolling deploy window. It resolves itself
once the rollout finishes; nothing needs a manual fix-up. If a rolling
deploy of this adapter's version must never error on an `int` key, drain
or version-bump that key's namespace instead of upgrading it in place.

## Trust boundary / deserialization

Values (other than plain `int`, see "Counter storage" above) are read
back with `pickle.loads()` — the Django convention, and the same model as
Django's own memcached backend. That means **anyone who can write to this
cache's namespace can execute code in the process that reads it back** —
any holder of the cluster's auth secret, or, when no secret is
configured, any peer that can reach the server at all. This is inherent
to `pickle.loads()` on untrusted bytes, not specific to nanocached.

Mitigate with all of:

- **`OPTIONS.TLS` + `OPTIONS.SECRET`**, so only trusted processes can
  write to the namespace at all.
- **Never share a namespace (or its secret) with an untrusted writer** —
  `OPTIONS.NAMESPACE` is cheap and isolated (see above), so give each
  trust domain its own alias/namespace rather than reusing one across
  services with different trust levels.
- There is no pluggable serializer in this backend today; where writers
  aren't fully trusted, isolate this cache's namespace to trusted writers
  only rather than relying on the pickle stream itself for safety.

## Requirements

Python 3.11+, Django 4.2+, `nanocached` (Python SDK) 0.3+, a nanocached
server ≥ the release that ships namespaces and CLEAR (issues #105/#106).

## Policy note

Framework adapters are ecosystem-specific and live **outside** the
six-language SDK parity policy ([#25]): parity applies to the SDK core
only. This module exists for Django; other ecosystems get their own
idiomatic adapters (Spring Cache, `IDistributedCache`, cache-manager
store, [JCache](../jcache), [Keyv](../keyv)) rather than mirrors of this
one.

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
