# nanocached (Java)

Java client SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake, so the calling code is identical either way.

Requires Java 17+. No runtime dependencies. Group/artifact:
`org.nanocached:nanocached`.

## Quick start

```java
import org.nanocached.NanocachedClient;
import org.nanocached.NanocachedClient.Address;

import java.util.List;
import java.util.Optional;

// Point at a single node, or at a discovery server fronting a
// cluster — same call either way.
NanocachedClient.Options options = NanocachedClient.builder()
        .addresses(List.of(new Address("127.0.0.1", 8357)));

try (NanocachedClient client = NanocachedClient.connect(options)) {
    client.set("greeting", "hello", 60);          // TTL in seconds, 0 = no expiry
    Optional<String> value = client.get("greeting");   // empty when missing
    boolean existed = client.delete("greeting");
}
```

Keys and values are `byte[]`, with `String` convenience overloads
(encoded as UTF-8). `get`/`get(byte[])` decode the value as strict UTF-8
and return `Optional<String>` (a value that isn't valid UTF-8 throws
`java.io.UncheckedIOException`); `getBytes`/`getBytes(byte[])` return the
raw `Optional<byte[]>` without decoding. The client is thread-safe;
requests are serialized per connection (concurrent callers queue).

## Addresses and discovery replicas

`addresses` is a list of `Address(host, port)` pairs. A one-element list
is the single-target case; when the cluster runs more than one discovery
server, list them all — both the initial connect and every node-list
refresh try them in order. An address that is warming up after a restart
(answers `B`) is skipped like an unreachable one; if every address is
warming up, `connect()` throws `NanocachedException.DiscoveryBusy` — retry
shortly.

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(
                new Address("10.0.0.1", 8357),
                new Address("10.0.0.2", 8357))));
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

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent). There is nothing to configure.

## Authentication and TLS

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("cache.internal", 8357)))
        .authSecret("change-me")   // NANOCACHED_AUTH_SECRET on the server
        .tls(true)                 // verifies against the platform trust store
        .ca("/etc/nanocached/ca.pem")); // optional: trust this PEM CA instead
```

`tls` is a plain boolean, default `false`. `ca` names a PEM file of
trusted root certificate(s) — it's meaningful only when `tls` is `true`
(silently ignored otherwise), and an unreadable or unparseable CA file is
a connect-time error. `ca` accepts a `java.nio.file.Path`, or a `String`
path / `java.io.File` via convenience overloads.

## close()

`close()` is idempotent; a second call still succeeds but prints a
warning to stderr (`nanocached: close() called again on an
already-closed client`) since it usually means the caller lost track of
this client's lifecycle. Likewise, calling `connect()` again for the
same single address while a previous connection to it is still open
warns to stderr — `was close() forgotten?`.

## Build

```sh
gradle test    # unit tests against in-process mock servers
gradle jar
```

This SDK speaks the current wire protocol (rendezvous hashing,
replication-aware `L`/`W`); it requires an up-to-date server.

## License

MIT
