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
    # cluster — same call either way.
    client = await NanocachedClient.connect("127.0.0.1", 8357)

    await client.set("greeting", "hello", ttl_seconds=60)
    value = await client.get("greeting")   # bytes | None
    print(value)                           # b"hello"
    existed = await client.delete("greeting")  # bool

    client.close()

asyncio.run(main())
```

Keys and values may be `str` (encoded as UTF-8) or `bytes`; values always
come back as `bytes` (`None` when the key is missing).

## Discovery replicas

When the cluster runs more than one discovery server, pass them all as
`seeds`; both the initial connect and every node-list refresh try them in
order. A seed that is warming up after a restart (answers `B`) is skipped
like an unreachable one; if every seed is warming up, `connect()` raises
`DiscoveryBusyError` — retry shortly.

```python
client = await NanocachedClient.connect(seeds=[("10.0.0.1", 8357), ("10.0.0.2", 8357)])
```

## Replication

The cluster's replication factor R rides along with the node list, so the
SDK needs no configuration: `set`/`delete` fan out to all R owners of a
key (the primary's result decides; a dead replica never fails a write),
and `get` asks the primary, falling over to the next owner only when the
holder is unreachable. `client.replication` exposes the factor in use.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 30 seconds; a request that
finds its connection dead transparently redials first (concurrent
requests share one dial). If that extra round trip matters, opt in to
keep-alive:

```python
client = await NanocachedClient.connect("127.0.0.1", 8357, keep_alive_interval=15.0)
```

## Authentication and TLS

```python
client = await NanocachedClient.connect(
    "cache.internal", 8357,
    auth_secret="change-me",   # NANOCACHED_AUTH_SECRET on the server
    tls=True,                  # or an ssl.SSLContext for a private CA
)
```

For a self-signed or private-CA server, build the context yourself:

```python
import ssl
context = ssl.create_default_context(cafile="cluster-ca.pem")
client = await NanocachedClient.connect("cache.internal", 8357, tls=context)
```

## Notes

- Requests are serialized per connection (one in flight at a time);
  concurrent callers queue. This is a deliberate v1 simplification over
  the TypeScript SDK's pipelining — identical semantics, lower peak
  throughput per connection.
- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server.

## License

MIT
