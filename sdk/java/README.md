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

## Counters (`incr`/`decr`)

`client.incr(key, delta)` atomically adds `delta` (a signed `long` — a
negative delta decrements) to the numeric value stored at `key`,
returning the new value; `client.decr(key, amount)` is the same op with
the amount negated — there is no separate decrement opcode on the wire.
Both default to `1` when called with no delta/amount. A missing key
returns `OptionalLong.empty()` (this INCR never auto-vivifies a counter
the way some other systems' does); a key whose stored value isn't a
plain decimal integer, or whose result would overflow a 64-bit counter,
throws `NanocachedException.NotNumeric`. Both exist on
`client.namespace(ns)` handles too, scoped exactly like `get`/`set`.

```java
client.set("hits", "0");
client.incr("hits");         // 1
client.incr("hits", 9);      // 10
client.decr("hits", 4);      // 6
```

**A counter is exactly as volatile as `set`** — LRU eviction and TTL
expiry reclaim it like any other entry. Good for rate limiting and
approximate counters; not for durable counts (billing, inventory, or
anything you can't afford to silently lose to eviction).

In a cluster, only the primary owner runs the increment; the replicas
receive the primary's literal resulting value (and TTL) as an ordinary
`set` rather than replaying the increment themselves, so a replica that
missed an earlier write or independently evicted the key still converges
to the exact same bytes as the primary instead of drifting from it.

## Batched get and set

`getMany`/`getManyBytes` and `setMany`/`setManyBytes` (the `m`/`o`
frames) fetch or store several keys in one round trip per owner
instead of one round trip per key:

```java
client.setMany(Map.of("a", "1", "b", "2")); // shared ttlSeconds for the whole batch
Map<String, String> values = client.getMany(List.of("a", "b", "missing"));
// {"a": "1", "b": "2"} — "missing" is simply absent
```

A missing key is simply absent from the returned `Map`, the same "a
miss is not an error" shape `get`/`getBytes` use. Both are also
namespace-scoped: `client.namespace(ns).getMany(...)`/`.setMany(...)`,
same as `get`/`set`. Batch keys are always `String` — unlike
single-key `get`/`set`, `getMany`/`setMany` don't accept `byte[]`
keys, since a `byte[]` can't safely key a `Map` (identity, not
content, equality).

**A batch never fails as a whole.** Each key's outcome is independent:
if some keys are still routed to the wrong node after one bounded
refresh-and-retry (the same policy `get`/`set` apply per key, not per
call), `getMany`/`getManyBytes` throw
`NanocachedException.PartialWrongNode` — a `WrongNode` subclass whose
`partialValues` field holds every key that DID resolve, so existing
`catch (NanocachedException.WrongNode)` handling keeps working
unchanged while a caller that wants the partial results can read them
off the exception (`getMany`'s own decoded counterpart is
`NanocachedException.PartialWrongNodeStrings`):

```java
try {
    return client.getMany(keys);
} catch (NanocachedException.PartialWrongNodeStrings partial) {
    return partial.partialValues;
}
```

`setMany`/`setManyBytes` have nothing to return on success, so they
just throw a plain `NanocachedException.WrongNode` on the same
condition — every other key in the batch was still stored. In
single-node/proxy mode a `W` propagates immediately, exactly like
`get`/`set`'s own single-mode behavior — there is no ring to refresh
against.

Within one `setMany`/`setManyBytes` batch, the same node can be one
key's primary and another key's replica at once; it receives exactly
one `o` sub-frame either way, and only its answer for the keys it is
primary for decides those keys' outcome — a replica-held key's
failure is logged-and-swallowed into `stats().replicaWriteFailures`,
exactly like a plain `set`'s own replica legs.

Very large batches are transparently split into more than one `m`/`o`
sub-frame per owner — callers never need to think about this.

## Compare-and-set

`putIfAbsent`/`replaceIfPresent`/`replace`/`deleteIfMatches` are
content-based compare-and-set: each returns a plain `boolean` — `true`
means the condition held and the write/delete happened, `false` means it
didn't and nothing changed. A mismatch is a normal outcome, not an
exception, exactly like `delete` returning `false` for a miss.

```java
client.putIfAbsent("lock:job-1", "worker-a");       // true: acquired
client.putIfAbsent("lock:job-1", "worker-b");       // false: already held

client.replaceIfPresent("config", "v2");            // only if some value exists

NanocachedClient.CasEntry current = client.getWithToken("counter").orElseThrow();
client.replace("counter", current.token(), "42");   // only if unchanged since the read
client.deleteIfMatches("counter", current.token()); // only if unchanged since the read
```

`replace`/`deleteIfMatches` take a **token**, not a literal expected
value — a 32-character digest of the key's exact stored bytes, obtained
from `getWithToken` (or computed directly from a value already in hand via
the static `NanocachedClient.contentDigest(byte[])`, e.g. for a future
adapter that never needs to GET first). Reconstructing a token from a
value the caller already holds, rather than one taken from a real prior
read, is only correct if that reconstruction is byte-identical to what
the server actually stores — the same hazard memcached's own value-based
CAS has; `getWithToken` is always correct since it hashes the exact bytes
just read. All four exist on `client.namespace(ns)` handles too, scoped
exactly like `get`/`set`.

**Not a distributed lock.** LRU eviction reclaims a CAS-written key
exactly as it would after a plain `set` — a key used as a lock
(`putIfAbsent` to acquire, a TTL to eventually release) can be silently
double-acquired if it's evicted under memory pressure between one
caller's acquire and its release. `putIfAbsent`/`replace`/etc. are atomic
against concurrent requests on the node that owns the key, the same
guarantee `incr` makes and no stronger.

In a cluster, only the primary owner evaluates the condition; a success's
literal result is forwarded to the replicas as an ordinary `set`/`delete`,
never by replaying the conditioned op — the same rule `incr` follows and
for the same reason. See [`docs/protocol.html#cas`](../../docs/protocol.html#cas)
for the wire-level details.

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
