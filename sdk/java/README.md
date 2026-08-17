# nanocached (Java)

Java client SDK for [nanocached](https://github.com/t0k0sh1/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake, so the calling code is identical either way.

Requires Java 17+. No runtime dependencies. Group/artifact:
`org.nanocached:nanocached`.

## Quick start

```java
import org.nanocached.NanocachedClient;

// Point at a single node, or at a discovery server fronting a
// cluster — same call either way.
try (NanocachedClient client = NanocachedClient.connect("127.0.0.1", 8357)) {
    client.set("greeting", "hello", 60);          // TTL in seconds (optional)
    byte[] value = client.get("greeting");        // null when missing
    boolean existed = client.delete("greeting");
}
```

Keys and values are `byte[]`, with `String` convenience overloads
(encoded as UTF-8). The client is thread-safe; requests are serialized
per connection (concurrent callers queue).

## Discovery replicas

When the cluster runs more than one discovery server, list them all;
both the initial connect and every node-list refresh try them in order.
A seed that is warming up after a restart (answers `B`) is skipped like
an unreachable one; if every seed is warming up, `connect()` throws
`NanocachedException.DiscoveryBusy` — retry shortly.

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .host("10.0.0.1", 8357)
        .host("10.0.0.2", 8357));
```

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `set`/`delete` fan out to all R owners
of a key (the primary's result decides; a dead replica never fails a
write), and `get` asks the primary, falling over to the next owner only
when the holder is unreachable. `client.replication()` exposes the
factor in use. A write whose primary just died recovers automatically
once discovery drops the node (bounded by its liveness timeout): the
failed attempt forces a node-list refresh and one retry.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 30 seconds; a request that
finds its connection dead redials and retries once transparently (all
operations are idempotent). If that extra round trip matters, opt in to
keep-alive:

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .host("127.0.0.1", 8357)
        .keepAliveInterval(Duration.ofSeconds(15)));
```

## Authentication and TLS

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .host("cache.internal", 8357)
        .authSecret("change-me")            // NANOCACHED_AUTH_SECRET on the server
        .tls(SSLContext.getDefault()));     // or a context trusting a private CA
```

## Build

```sh
gradle test    # unit tests against in-process mock servers
gradle jar
```

This SDK speaks the current wire protocol (rendezvous hashing,
replication-aware `L`/`W`); it requires an up-to-date server.

## License

MIT
