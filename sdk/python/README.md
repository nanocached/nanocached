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

## Replication

The cluster's replication factor R rides along with the node list, so the
SDK needs no configuration: `set`/`delete` fan out to all R owners of a
key (the primary's result decides; a dead replica never fails a write),
and `get` asks the primary, falling over to the next owner only when the
holder is unreachable. `client.replication` exposes the factor in use.

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

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent).

An address whose redial just failed is treated as still down for
`reconnect_cooldown` seconds (default `1.0`, a `connect()` keyword
argument): requests routed to it during that window fail immediately with
the original dial error instead of each paying another full 5-second
connect timeout. Keep it short — a node that genuinely recovers is shut
out for at most this long.

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
