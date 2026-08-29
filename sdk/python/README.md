# nanocached (Python)

asyncio client SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from the
server's own handshake, so the calling code is identical either way.

Requires Python 3.11+. No runtime dependencies.

## Install

```sh
pip install nanocached
```

## Quick start

```python
import asyncio
from nanocached import NanocachedClient

async def main():
    # Point at a single node, or at a discovery server fronting a
    # cluster — same call either way. `addresses` always takes a list;
    # a one-element list is the single-target case.
    client = await NanocachedClient.connect([("127.0.0.1", 8357)])

    await client.set("greeting", "hello", ttl_seconds=60)
    value = await client.get("greeting")   # str | None
    print(value)                           # hello
    existed = await client.delete("greeting")  # bool

    await client.close()

asyncio.run(main())
```

Or use it as an async context manager, which closes the client for you:

```python
async with await NanocachedClient.connect([("127.0.0.1", 8357)]) as client:
    await client.set("greeting", "hello")
    print(await client.get("greeting"))
```

Keys may be `str` (encoded as UTF-8) or `bytes`; values may likewise be
`str` or `bytes` on the way in. `get(key)` strictly decodes the stored
value as UTF-8 and returns `str | None` — a value that isn't valid UTF-8
raises `UnicodeDecodeError` rather than silently mangling it. Use
`get_bytes(key) -> bytes | None` for the raw bytes.

## Discovery replicas

When the cluster runs more than one discovery server, pass them all in
`addresses`; both the initial connect and every node-list refresh try them
in order. An address that is warming up after a restart (answers `B`) is
skipped like an unreachable one; if every address is warming up, `connect()`
raises `DiscoveryBusyError` — retry shortly.

```python
client = await NanocachedClient.connect([("10.0.0.1", 8357), ("10.0.0.2", 8357)])
```

## Proxy mode

`addresses` must still point at discovery server(s); `via_proxy=True` fetches
the *proxy* roster from them instead of the node roster, and connects to one
`nanocached-proxy` at random rather than opening a connection per node —
useful for a client fleet that would otherwise open far more connections
than the cluster needs:

```python
client = await NanocachedClient.connect(
    [("10.0.0.1", 8357), ("10.0.0.2", 8357)],
    via_proxy=True,
)
```

A proxy looks like a single node that owns every key, so once connected this
client is in its ordinary single-connection mode: no ring, no client-side
replication, and `read_hedge_after` is inert — there are no replicas to
hedge to, so it is simply ignored rather than rejected. Namespaces,
clear/clear_all, tags, keep-alive and compression all work unchanged.
Pointing `via_proxy` at an address that turns out to be a cache node (not a
discovery server) fails `connect()` outright with a clear error; an empty
proxy roster does the same. On reconnect, the SDK first retries the same
proxy (it may just have restarted) and, only if that fails too, re-fetches
the roster from discovery and picks another at random — the same
`reconnect_cooldown` governs both.

## Replication

The cluster's replication factor R rides along with the node list, so the
SDK needs no configuration: `set`/`delete` fan out to all R owners of a
key (the primary's result decides; a dead replica never fails a write),
and `get` asks the primary, falling over to the next owner only when the
holder is unreachable. `client.replication` exposes the factor in use.

## Namespaces

A namespace is a flat, opaque byte string that scopes a key: the same key
name in two namespaces (or in a namespace versus the default, un-namespaced
keyspace) is two independent entries. `client.namespace(ns)` returns a
lightweight handle with the same `get`/`get_bytes`/`set`/`delete` as the
client itself, just scoped to `ns`:

```python
users = client.namespace("users")
await users.set("alice", "admin")
await client.set("alice", "not the same entry")  # default namespace

print(await users.get("alice"))   # admin
print(await client.get("alice"))  # not the same entry
```

`ns` may be `str` (UTF-8 encoded) or `bytes`, with no length limit beyond
the same request-size rule keys and values already follow; it is never
interpreted — no delimiter, no escaping, no hierarchy. The handle is cheap,
shares the client's connections, and forwards to the same routing,
replication, hedged reads, response tags and compression the client itself
uses — a namespaced key's owners are computed from `(namespace, key)`
together, so two namespaces spread even identical key names across the
cluster rather than piling them on the same nodes. `client.namespace("")`
returns a handle equivalent to the client itself: the un-namespaced form is
the default namespace, and the SDK always speaks the wire protocol's legacy
frames for it, so an unchanged client talking to an older, pre-namespace
server keeps working. The handle becomes invalid, raising the same
`AlreadyClosedError` the client itself would, once the client it came from
is closed.

A namespace's keys are spread across every node by rendezvous hashing, so
clearing one isn't addressed to a single owner the way get/set/delete are.
`await users.clear()` drops every entry in that namespace; `namespace("")`
clears the default namespace rather than being rejected. `await
client.clear_all()` drops every namespace at once, the default one
included:

```python
await users.clear()       # only "users" is gone
await client.clear_all()  # every namespace, default included
```

Both fan a request out to every node the client currently knows about and
only succeed once each one has acknowledged it; a node that fails is given
one retry against a freshly refreshed node list before the call raises,
naming the node that still failed. Both are idempotent, so a caller can
simply retry a raised error, and both raise `AlreadyClosedError` after
`close()` like every other operation.

## Counters (incr/decr)

`incr`/`decr` atomically add (or subtract) an integer delta to a key and
return the new value:

```python
await client.set("visits", "0")
await client.incr("visits")        # 1
await client.incr("visits", 5)     # 6
await client.decr("visits", 2)     # 4 — decr(key, n) is just incr(key, -n);
                                    # it never sends a separate wire op
print(await client.incr("missing"))         # None — same miss convention as get()
```

`Namespace` has the same pair, scoped like its other operations. A miss
returns `None`, matching `get()`; a key whose stored value isn't an
integer INCR can operate on (or where applying the delta would overflow a
signed 64-bit integer) raises `NotNumericError`.

A counter is exactly as volatile as any other entry: LRU eviction and TTL
expiry reclaim it like a plain `set`, so this is a fit for rate limiting
and approximate counters, not durable counts (billing, inventory).

In a cluster, only the primary owner actually runs the increment; its
result is then forwarded to the remaining owners as an ordinary `set`
rather than replaying the increment on them, so a replica can never drift
onto a value the primary doesn't itself hold (e.g. from an earlier
dropped replica write, or the replica separately evicting the key).

## Batched get and set

`get_many`/`get_many_bytes` and `set_many` (the `m`/`o` frames) fetch or
store several keys in one round trip per owner instead of one round
trip per key:

```python
await client.set_many({"a": "1", "b": "2"}, ttl_seconds=60)  # one shared TTL for the whole batch
values = await client.get_many(["a", "b", "missing"])
# values == {"a": "1", "b": "2"} — "missing" is simply absent
```

A missing key is simply absent from the returned dict, the same "a miss
is not an error" shape `get`/`get_bytes` use. Keys may be `str` or
`bytes`, freely mixed within one call — the result dict is keyed by
whichever original object was passed in, not by its encoded bytes, so a
batch containing both `"a"` and `b"a"` is rejected up front with
`ValueError` rather than silently colliding. Both are also
namespace-scoped: `client.namespace(ns).get_many(...)`/`.set_many(...)`,
same as `get`/`set`.

**A batch never fails as a whole.** Each key's outcome is independent:
if some keys are still routed to the wrong node after one bounded
refresh-and-retry (the same policy `get`/`set` apply per key, not per
call), `get_many`/`get_many_bytes` raise `PartialWrongNodeError` — a
`WrongNodeError` subclass — whose `.partial_values` holds every key
that DID resolve, rather than discarding a mostly-successful batch;
`set_many` raises a plain `WrongNodeError` while every other key in the
batch was still stored. In single-node/proxy mode a `W` propagates
immediately, exactly like `get`/`set`'s own single-mode behavior — there
is no ring to refresh against.

Within one `set_many` batch, the same node can be one key's primary and
another key's replica at once; it receives exactly one `o` sub-frame
either way, and only its answer for the keys it is primary for decides
those keys' outcome — a replica-held key's failure is
logged-and-swallowed into `stats().replica_write_failures`, exactly like
a plain `set`'s own replica legs.

Very large batches are transparently split into more than one `m`/`o`
sub-frame per owner — callers never need to think about this. Hedged
reads and read repair do not apply to batches.

## Compare-and-set

`put_if_absent`, `replace_if_present`, `replace`, and `delete_if_matches`
condition a write or delete on the key's *current* stored bytes, atomic on
the node that owns the key:

```python
# add()/putIfAbsent(): succeeds only if the key is absent.
await client.put_if_absent("lock:job-1", "worker-a")   # True — stored
await client.put_if_absent("lock:job-1", "worker-b")   # False — already held

# replace(key, value): succeeds only if the key currently holds *any* value.
await client.replace_if_present("lock:job-1", "worker-a-retry")  # True

# replace(key, old, new): succeeds only if the key's current bytes match
# a token from a prior read exactly.
value, token = await client.get_with_token("lock:job-1")
await client.set("lock:job-1", "someone-else")           # changed out from under us
await client.replace("lock:job-1", token, "worker-a")     # False — stale token

# remove(key, old): a digest-conditioned delete.
value, token = await client.get_with_token("lock:job-1")
await client.delete_if_matches("lock:job-1", token)        # True
```

`token` is not the value itself — it's `content_digest()`'s 32-character
hex digest of the value's exact stored bytes, normally obtained from
`get_with_token()` (the `get`/`get_bytes` companion that returns a token
alongside the value). Reconstructing a token from a value you already
hold, instead of one from a real prior read, is only correct if your
reconstruction is byte-identical to what the server actually stores —
exactly as sensitive to encoding as memcached's own value-based CAS, and
not guaranteed at all if `compress` differs between whoever wrote the
value and whoever built the token. The read-then-write-back path shown
above is always correct.

Every one of these returns a plain `bool` — a condition mismatch is
`False`, not an exception, the same idiom `delete()` already uses for
"nothing here to act on". Available on `Namespace` too.

**Not a distributed lock.** LRU eviction reclaims a key exactly as it
would after a plain `set`, CAS or not: a "lock" built from
`put_if_absent` plus a TTL can be silently double-acquired if the key is
evicted under memory pressure between the two callers' attempts.
`put_if_absent`/`replace`/`delete_if_matches` are atomic against
concurrent requests on the node that currently owns the key, the same
guarantee `incr`/`decr` make and no stronger.

In a cluster, only the primary owner evaluates the condition; its result
is then forwarded to the remaining owners as an ordinary `set`/`delete`
rather than replaying the conditioned op on them — the same rule
`incr`/`decr` follow, and for the same reason: a replica evaluating the
same condition against its own possibly-different copy could reach a
different outcome than the primary just did. See
[`docs/protocol.html#cas`](../../docs/protocol.html#cas) for the wire
format and the digest algorithm.

## Fire-and-forget replica writes

Off by default. `set`/`delete` normally wait for every replica leg to
finish, same as the primary. Enabling `fire_and_forget_replicas` returns
as soon as the primary acks, letting replica legs finish in the
background (fire-and-forget replica writes):

```python
client = await NanocachedClient.connect(
    [("cache.internal", 8357)],
    fire_and_forget_replicas=True,
)
```

Unlike `compress`, this is a pure latency/durability trade for this
client's own writes — it carries no wire format, and different clients
may use different settings freely. At most 32 replica writes across the
whole client run in the background at once; past that cap, further
replica legs run synchronously exactly as with the option off (a
graceful degrade, not a queue or a drop). `close()` gives any
still-in-flight background replica writes a chance to finish before
tearing down their connections.

## Read repair

Off by default. A clean miss (the key's first-reached owner reports it
missing) is normally accepted as-is. Enabling `read_repair` probes the
remaining owners before accepting that, and repairs the primary in the
background if one still has the value (read repair):

```python
client = await NanocachedClient.connect(
    [("cache.internal", 8357)],
    read_repair=True,
)
```

Closes the narrow window after a primary restart where a replica still
holds a key its (fresh) primary doesn't, at the cost of extra reads only
on the misses that hit that window. The repair write carries a fixed 60-second TTL — the wire protocol's `G` response never returns the original one to preserve, and no TTL at all would immortalize already-expired keys — and,
unlike fire-and-forget replica writes, is uncapped and not drained on
`close()`: this only fires on an already-rare clean miss, and losing one
costs nothing beyond staying in the window for one more read.

## Hedged reads

Off by default. A read goes to the key's primary owner and moves on to
the next owner only when the primary *fails* — so one slow-but-alive
node (a saturated host, a bad link) makes every read that touches it
wait out its full round trip, and with `R` copies on `N` nodes that is
roughly `R/N` of all reads. Setting `read_hedge_after` (seconds) sends
the same read to the next owner as well once the primary has been
silent for that long, and takes the first answer:

```python
client = await NanocachedClient.connect(
    [("cache.internal", 8357)],
    read_hedge_after=0.01,  # hedge after 10 ms
)
```

A hit from any owner is final. A miss is only final from the primary: a
replica's miss is provisional (it may simply lack the copy), so the
primary's answer is still waited for and hedging never turns a hit into
a miss — a genuine miss on a slow primary still pays its round trip. Pick
a value a few times the healthy p99 so a fast cluster hedges rarely: each
hedge costs one extra read on another owner. Needs `R >= 2`; with a
single copy there is nobody to hedge to. Writes are unaffected — every
copy must be written, so a slow owner bounds writes to it regardless
(`fire_and_forget_replicas` moves only the replica legs off the caller's
path). The losing leg of a hedge is left to finish and is drained by
`close()`. Also inert with `via_proxy` (see Proxy mode) for the same
reason — a proxy connection has no replicas of its own to hedge to either.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent).

`connect()` itself tolerates a node that discovery still lists but that
can't be reached — typically one that just died and hasn't been evicted
yet (a window of seconds): the node is kept in the ring without a
connection, requests for its keys fail over per request exactly as they
would after a mid-life death, and it is redialed after the cooldown.
Only a cluster with no reachable node at all fails `connect()`.

An address whose redial just failed is treated as still down for
`reconnect_cooldown` seconds (default `1.0`, a `connect()` keyword
argument): requests routed to it during that window fail immediately with
the original dial error instead of each paying another full 5-second
connect timeout. Keep it short — a node that genuinely recovers is shut
out for at most this long.

## Errors and stats

Every failure this SDK raises on its own behalf is a `NanocachedError`
subclass — catch that one class when you don't care which:

- `AuthenticationError` — the `auth_secret` handshake failed. Never
  transient; retrying with the same configuration cannot succeed.
- `WrongNodeError` — normally handled internally (a stale routing table
  is refreshed and the call retried once); it only escapes in
  single-node mode, where there is no discovery to refresh from.
- `DiscoveryBusyError` — a discovery server is warming up after a
  restart. Try another address, or retry shortly.
- `NotNumericError` — `incr`/`decr` found a stored value that isn't an
  integer INCR can operate on, or applying the delta would overflow a
  signed 64-bit integer (see Counters (incr/decr)).
- `RetryableError` — a request was answered the retryable-error status
  `R` three times running. `R` means the request itself failed
  transiently — today, `nanocached-proxy` sends it when its upstream
  node was briefly unreachable and survived its own one refresh-and-
  retry — while the connection stayed perfectly healthy throughout. This
  SDK already retries transparently on `R`: up to 2 retries (3 attempts
  total, sleeping 50ms then 100ms between attempts) on the SAME
  connection, with no reconnect and no node-list refresh — `R` is never
  treated as a connection error, a `W`, or an `E`. `RetryableError` only
  surfaces once that budget is exhausted; the connection remains open
  and usable, so retrying the call itself (most likely against the same
  connection) is safe and often immediately successful.

`client.stats()` returns a `ClientStats` snapshot of counters for
failures this client swallows or retries by design, so they stay
observable instead of silently invisible:

```python
stats = client.stats()
stats.replica_write_failures  # swallowed dead/disagreeing replica legs (client-side replication)
stats.read_repair_failures    # swallowed read-repair write-back failures (read repair)
stats.refresh_failures        # failed node-list refresh attempts / per-node reconnects
stats.transient_retries       # every `R` received, whether the retry that followed succeeded
                               # or, after 3 attempts running, raised RetryableError
```

## Authentication and TLS

```python
client = await NanocachedClient.connect(
    [("cache.internal", 8357)],
    auth_secret="change-me",   # NANOCACHED_AUTH_SECRET on the server
    tls=True,                  # verifies against the platform trust store
)
```

For a self-signed or private-CA server, pass `ca` — a PEM file of trusted
root certificate(s), which replaces the default trust store:

```python
client = await NanocachedClient.connect(
    [("cache.internal", 8357)],
    tls=True,
    ca="cluster-ca.pem",
)
```

`ca` is only meaningful when `tls=True`; if `tls=False` it is silently
ignored. An unreadable or unparseable CA file is a connect-time error.

## Value compression

Off by default. When enabled, values at or above `compression_threshold`
bytes are transparently DEFLATE-compressed on `set` and decompressed on
`get`/`get_bytes` (value compression):

```python
client = await NanocachedClient.connect(
    [("cache.internal", 8357)],
    compress=True,
    compression_threshold=256,  # default; bytes, below which values are stored as-is
)
```

**Every client that reads or writes a given set of keys must agree on
`compress`.** This is a per-keyspace format decision, not a per-client
preference — enabling it prefixes every value this client writes with a
one-byte marker, so a client with `compress=False` reading one of those
values gets the marker byte back as if it were part of the value (wrong,
silently), and a client with `compress=True` reading a value written
before compression was enabled anywhere risks misreading that value's
first byte as the marker (a `DecompressionError`, or — if that byte
happens to be the "uncompressed" marker by chance — a silently wrong
read). There is no dual-mode migration path: only turn this on for a
fresh keyspace, or only after every client touching an existing one has
upgraded and enabled it together. Incompressible data (already-compressed
media, random bytes) is passed through unchanged rather than bloated.

## Notes

- Requests are pipelined per connection (request pipelining), matching
  the TypeScript SDK: concurrent callers on the same connection each pay
  only their own network latency, not everyone else's ahead of them.
- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server.
- `close()` is a coroutine (like aiohttp's `ClientSession.close`): it
  returns only after any in-flight background replica writes finish and
  every connection is torn down. It is idempotent, but calling it again
  on an already-closed client prints a warning to stderr — usually a sign the client's
  lifecycle was mismanaged. Likewise, calling `connect()` again for the
  same single address while a previous connection to it is still open
  prints a warning ("was close() forgotten?"); this check is skipped for
  multi-address configs, where concurrent clients sharing an address list
  are legitimate.
- Caller mistakes (a negative `ttl_seconds`, an empty address list)
  raise host-language builtins (`ValueError`), not `NanocachedError` —
  the SDK's error family covers failures the server or network produced,
  a convention shared across the SDKs (issue #47). Authentication
  failure is `AuthenticationError`, a `NanocachedError` subclass.

## License

MIT
