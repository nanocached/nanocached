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
requests are pipelined per connection (request pipelining) — concurrent
callers on the same connection each pay only their own network latency,
not everyone else's ahead of them.

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

## Namespaces

`client.namespace(ns)` returns a lightweight, namespace-scoped handle
with the same `get`/`getBytes`/`set`/`delete` surface as the client
itself — the same key name in two namespaces (or the default,
un-namespaced keyspace) is three independent entries, since the
namespace enters routing (HRW over `(namespace, key)`) alongside the
key. `ns` accepts a `String` (UTF-8 encoded) or raw `byte[]` — a
namespace is an opaque, binary-safe byte string with no delimiter, no
escaping, and no hierarchy, just like a key.

```java
NanocachedClient.Namespace tenant = client.namespace("tenant-a");
tenant.set("greeting", "hello", 60);
Optional<String> value = tenant.get("greeting");   // "hello" — isolated from client.get("greeting")
```

A handle is cheap (holds only the namespace bytes and shares the
client's connections), forwards every call to the client's own
networking rather than duplicating it, and is invalid once `close()`
has run (the same `AlreadyClosed` a direct call raises).
`client.namespace("")` returns a handle equivalent to the client itself
— same routing, same wire frames — rather than being rejected.
`namespace.namespace()` returns the handle's namespace bytes, useful
when passing a handle around generically.

Talking to a namespace uses the `g`/`s`/`d` wire commands, additive to
the pre-namespace protocol; a pre-namespace server answers `E` and
closes the connection, so every node in the cluster must be upgraded
before namespaces are used. The un-namespaced API is unchanged and
remains the default — every existing key keeps its placement across the
upgrade.

`tenant.clear()` drops every entry in that one namespace — an O(1)
sub-map drop on each node, not a key-by-key scan — and
`client.clearAll()` drops every namespace, the default one included.
Neither is key-addressed, so in a cluster the client sends the command
to *every* node rather than ranking owners for a key; success requires
every node to ack, and a node that failed gets one node-list refresh and
one retry (the same recovery a stale-routing `W` gets) before the call
raises, naming any node still failing. Both are idempotent, so a caller
that sees either throw can simply call it again. `tenant.clear()` on a
handle from `client.namespace("")` clears just the default namespace
(`c 0`), not every namespace — use `clearAll()` for that.

```java
tenant.clear();      // just "tenant-a"
client.clearAll();   // every namespace, including the default one
```

## Fire-and-forget replica writes

Off by default. `set`/`delete` normally wait for every replica leg to
finish, same as the primary. Enabling `fireAndForgetReplicas` returns
as soon as the primary acks, letting replica legs finish in the
background (fire-and-forget replica writes):

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("cache.internal", 8357)))
        .fireAndForgetReplicas(true));
```

Unlike `compress`, this is a pure latency/durability trade for this
client's own writes — it carries no wire format, and different clients
may use different settings freely. At most 32 replica writes across the
whole client run in the background at once; past that cap, further
replica legs run synchronously exactly as with the option off (a
graceful degrade, not a queue or a drop). `close()` waits for any
still-in-flight background replica writes before tearing down
connections.

## Read repair

Off by default. A clean miss (the key's first-reached owner reports it
missing) is normally accepted as-is. Enabling `readRepair` probes the
remaining owners before accepting that, and repairs the primary in the
background if one still has the value (read repair):

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("cache.internal", 8357)))
        .readRepair(true));
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
roughly `R/N` of all reads. Setting `readHedgeAfter` sends the same read
to the next owner as well once the primary has been silent for that
long, and takes the first answer:

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("cache.internal", 8357)))
        .readHedgeAfter(Duration.ofMillis(10)));
```

A hit from any owner is final. A miss is only final from the primary: a
replica's miss is provisional (it may simply lack the copy), so the
primary's answer is still waited for and hedging never turns a hit into
a miss — a genuine miss on a slow primary still pays its round trip. Pick
a value a few times the healthy p99 so a fast cluster hedges rarely: each
hedge costs one extra read on another owner. Needs `replication() >= 2`;
with a single copy there is nobody to hedge to. Writes are unaffected —
every copy must be written, so a slow owner bounds writes to it
regardless (`fireAndForgetReplicas` moves only the replica legs off the
caller's path). The losing leg of a hedge is left to finish and is
drained by `close()`.

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
`reconnectCooldown` (default `Duration.ofMillis(1000)`): requests routed
to it during that window fail immediately with the original dial error
instead of each paying another full 5-second connect timeout. Keep it
short — a node that genuinely recovers is shut out for at most this long.

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("cache.internal", 8357)))
        .reconnectCooldown(java.time.Duration.ofMillis(1000))); // default
```

## SDK proxy mode (`viaProxy`)

Off by default. `addresses` must name discovery server(s); enabling
`viaProxy` connects through exactly one `nanocached-proxy` instead of
joining the cluster ring:

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("discovery.internal", 8358)))
        .viaProxy(true));
```

The client fetches the proxy roster from discovery and picks one proxy
at random — spreading a client fleet across the whole proxy tier instead
of piling everyone onto the first one — failing over to another proxy at
random if the chosen one is unreachable. `connect()` fails fast, naming
the address, if it turns out to be a cache node instead of a discovery
server: proxy mode has nothing to fetch a roster from in that case. An
empty roster (no proxy currently registered) is the SDK's normal connect
error too.

A proxy answers exactly like a single node that owns every key (full
`get`/`set`/`delete`, namespaces, `clear`/`clearAll`, tags, keep-alive,
and compression all work unchanged) — so from here on this client is in
its existing single-connection mode. Two things follow from having only
one connection and no ring view:

- **No client-side replication.** There is nothing to fan a write out to
  or read a fallback from — the proxy (and whatever's behind it) owns
  that.
- **`readHedgeAfter` is inert.** Hedging needs a second owner to send the
  same read to; a single connection has none, so a configured hedge
  interval simply never fires here (it is not rejected — it may still
  apply to a different, non-proxy client sharing the same `Options`).

On a lost connection, the client first retries the same proxy (it may
simply have restarted); only if that also fails does it re-fetch the
roster and pick another at random — reusing the same reconnect cooldown
and counters as any other single-mode redial, not a second reconnect
path. `close()` is unchanged.

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

## Value compression

Off by default. When enabled, values at or above `compressionThreshold`
bytes are transparently DEFLATE-compressed on `set` and decompressed on
`get`/`getBytes` (value compression):

```java
NanocachedClient client = NanocachedClient.connect(NanocachedClient.builder()
        .addresses(List.of(new Address("cache.internal", 8357)))
        .compress(true)
        .compressionThreshold(256)); // default; bytes, below which values are stored as-is
```

**Every client that reads or writes a given set of keys must agree on
`compress`.** This is a per-keyspace format decision, not a per-client
preference — enabling it prefixes every value this client writes with a
one-byte marker, so a client with `compress(false)` reading one of those
values gets the marker byte back as if it were part of the value (wrong,
silently), and a client with `compress(true)` reading a value written
before compression was enabled anywhere risks misreading that value's
first byte as the marker (a `NanocachedException.DecompressionFailed`, or
— if that byte happens to be the "uncompressed" marker by chance — a
silently wrong read). There is no dual-mode migration path: only turn
this on for a fresh keyspace, or only after every client touching an
existing one has upgraded and enabled it together. Incompressible data
(already-compressed media, random bytes) is passed through unchanged
rather than bloated.

## close()

`close()` is idempotent; a second call still succeeds but prints a
warning to stderr (`nanocached: close() called again on an
already-closed client`) since it usually means the caller lost track of
this client's lifecycle. Likewise, calling `connect()` again for the
same single address while a previous connection to it is still open
warns to stderr — `was close() forgotten?`.

## Errors

Every exception this SDK throws extends `NanocachedException` —
including `AuthenticationFailed` for a rejected secret — so one catch
clause covers "an expected nanocached failure". Caller mistakes (an
invalid TTL, an empty address list) throw `IllegalArgumentException`
instead: they indicate a bug in the calling code, not a nanocached
failure, a convention shared across the SDKs (issue #47).

A `nanocached-proxy` may answer an individual request with a transient
failure (its upstream node was briefly unreachable and survived its own
one refresh-and-retry) instead of the fatal error-and-close a genuine
failure gets. The SDK retries that request transparently on the SAME
connection — up to 2 retries (3 attempts total), waiting 50ms then
100ms — before it ever surfaces to your code. If the third attempt is
still transient, `get`/`set`/`delete` throw
`NanocachedException.RetryableError`; the connection itself is
untouched — it is not closed or redialed, and stays usable for whatever
you call next. This never affects a `nanocached-node` or discovery
connection directly, but the SDK negotiates the capability on every
connection it dials regardless of what's on the other end.

`client.stats().transientRetries()` counts every retryable response
this client has received, including the final one on a request that
went on to throw `RetryableError` — next to the other by-design-swallow
counters (`replicaWriteFailures`, `readRepairFailures`,
`refreshFailures`, `backgroundWriteBugs`) that `stats()` already
exposes.

## Build

```sh
gradle test    # unit tests against in-process mock servers
gradle jar
```

This SDK speaks the current wire protocol (rendezvous hashing,
replication-aware `L`/`W`); it requires an up-to-date server.

## License

MIT
