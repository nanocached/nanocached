# nanocached

TypeScript/Node.js SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake response, so the calling code is identical
either way.

Requires Node.js 20+. No runtime dependencies.

## Install

```sh
npm install nanocached
# or: pnpm add nanocached
```

## Quick start

```ts
import { NanocachedClient } from "nanocached";

// Point at a single node, or at a discovery server fronting a cluster —
// same options either way. `addresses` is always a list; a one-element
// list is the single-target case.
const client = await NanocachedClient.connect({ addresses: [{ host: "127.0.0.1", port: 11311 }] });

await client.set("greeting", "hello", 60); // ttlSeconds; omit or pass 0 for no expiry

const value = await client.get("greeting"); // string | null, strict UTF-8
console.log(value); // "hello"

const bytes = await client.getBytes("greeting"); // Buffer | null, raw bytes

const existed = await client.delete("greeting"); // boolean

await client.close();
```

Keys and values may be `string` (encoded as UTF-8) or `Uint8Array`.
`get()` decodes the value as UTF-8 and rejects (a native `TypeError`) if it
isn't valid UTF-8 — never a silent replacement character. Use `getBytes()`
to read a value's raw bytes instead, e.g. for values this client didn't
itself write as a UTF-8 string.

## Namespaces

A *namespace* scopes a key: the same key name in two namespaces is two
independent entries, and the un-namespaced API above always addresses the
*default* namespace (the empty one). `client.namespace(ns)` returns a
lightweight handle scoped to `ns`, exposing the same operations with
identical semantics — routing, replication fan-out, hedged reads, `W`
refresh-and-retry, response tags, compression, error types:

```ts
const users = client.namespace("users");

await users.set("alice", "hello");
await client.set("alice", "goodbye"); // a different entry — the default namespace

console.log(await users.get("alice")); // "hello"
console.log(await client.get("alice")); // "goodbye"
```

`ns` accepts the same key-ish types as a key: a `string` is UTF-8 encoded, a
`Uint8Array` is used as-is. Namespaces are opaque bytes — no delimiter, no
escaping, no hierarchy — so any bytes are valid, including ones that would
look like a path separator. `namespace("")` is not an error; it returns a
handle that behaves exactly like `client` itself, since it addresses the
very same default namespace. A handle is cheap to create, shares the
client's connections, and — like every client method — throws
`AlreadyClosedError` once the client is closed. `handle.namespace` exposes
the raw namespace bytes back.

On a cluster, namespace participates in routing: `(namespace, key)` hashes
together, so the same key name in different namespaces can land on
different owners — this is consensus-critical, and every SDK and the
server pin the same test vectors for it. A pre-namespace `nanocached-node`
answers a namespaced request with `E` and closes the connection, so every
node in a cluster must be upgraded before clients start using namespaces.

`handle.clear()` drops every entry in that namespace, and
`client.clearAll()` drops every namespace at once, the default one
included:

```ts
await users.clear(); // just "users" is gone
await client.clearAll(); // every namespace, including the default one, is gone
```

Neither is key-addressed, so — unlike `get`/`set`/`delete` — a clear can't
be routed to a single owner: it's sent to *every* node in the cluster
concurrently. It only resolves once every node has acknowledged; if any
node failed, the node list is refreshed once and the clear is retried
against the refreshed list, exactly like a `W`/dead-primary retry. A node
that still fails after that raises the usual `NanocachedError`, naming it
— a clear never silently succeeds on only part of the cluster, and since
it's idempotent, the caller can just retry it. In standalone (single-node)
mode it's simply sent to that one node.

## Authentication

If the server was started with `NANOCACHED_AUTH_SECRET`, pass the same
secret:

```ts
const client = await NanocachedClient.connect({
  addresses: [{ host: "cache.internal", port: 11311 }],
  authSecret: process.env.NANOCACHED_AUTH_SECRET,
});
```

## TLS

If the server was started with `--tls-cert`/`--tls-key`:

```ts
// Certificate issued by a publicly trusted CA:
const client = await NanocachedClient.connect({ addresses, tls: true });

// Self-signed / private CA — trust exactly that certificate instead:
const client = await NanocachedClient.connect({
  addresses,
  tls: true,
  ca: "cluster-ca.pem", // path to a PEM file, read once inside connect()
});
```

`ca` is only meaningful when `tls: true`; a `ca` set with `tls` unset or
`false` is silently ignored. An unreadable or unparseable CA file is a
connect-time error.

## Value compression

Off by default. When enabled, values at or above `compressionThreshold`
bytes are transparently DEFLATE-compressed on `set` and decompressed on
`get`/`getBytes` (value compression):

```ts
const client = await NanocachedClient.connect({
  addresses,
  compress: true,
  compressionThreshold: 256, // default; bytes, below which values are stored as-is
});
```

**Every client that reads or writes a given set of keys must agree on
`compress`.** This is a per-keyspace format decision, not a per-client
preference — enabling it prefixes every value this client writes with a
one-byte marker, so a client with `compress` off reading one of those
values gets the marker byte back as if it were part of the value (wrong,
silently), and a client with `compress` on reading a value written before
compression was enabled anywhere risks misreading that value's first byte
as the marker (a `DecompressionError`, or — if that byte happens to be
the "uncompressed" marker by chance — a silently wrong read). There is no
dual-mode migration path: only turn this on for a fresh keyspace, or only
after every client touching an existing one has upgraded and enabled it
together. Incompressible data (already-compressed media, random bytes) is
passed through unchanged rather than bloated.

## Cluster behavior

When `connect()` reaches a discovery server, the SDK fetches the node list,
opens one pipelined connection per node, and ranks each key's owners with
the same rendezvous hashing every other nanocached client and node uses —
so all parties agree on which nodes hold a key.

### Replication

The cluster's replication factor R (how many nodes hold each key — set by
`nanocached-discovery --replication-factor`, default 2) rides along with
the node list, so the SDK needs no configuration:

- `set`/`delete` fan out to all R owners in parallel. The primary's result
  is the operation's result; a dead replica never fails a write.
- `get` asks the primary and falls over to the next owner only when the
  holder is unreachable — a node death costs no cached data, not even a
  latency blip on keys whose primary survived.
- `client.replication` exposes the factor in use (1 against a single
  node).

The node list is re-fetched lazily when it is more than 30 seconds old. If a
node answers that it no longer owns a key (its view of the cluster changed),
the SDK refreshes the node list and retries that operation once. A discovery
outage degrades only topology updates: existing connections keep serving
traffic on the last-known node list.

### Fire-and-forget replica writes

Off by default. `set`/`delete` normally wait for every replica leg to
finish, same as the primary. Enabling `fireAndForgetReplicas` returns as
soon as the primary acks, letting replica legs finish in the background
(fire-and-forget replica writes):

```ts
const client = await NanocachedClient.connect({
  addresses: [{ host: "cache.internal", port: 8357 }],
  fireAndForgetReplicas: true,
});
```

Unlike `compress`, this is a pure latency/durability trade for this
client's own writes — it carries no wire format, and different clients
may use different settings freely. At most 32 replica writes across the
whole client run in the background at once; past that cap, further
replica legs run synchronously exactly as with the option off (a
graceful degrade, not a queue or a drop). `close()` gives any
still-in-flight background replica writes a chance to finish before
tearing down their connections.

### Read repair

Off by default. A clean miss (the key's first-reached owner reports it
missing) is normally accepted as-is. Enabling `readRepair` probes the
remaining owners before accepting that, and repairs the primary in the
background if one still has the value (read repair):

```ts
const client = await NanocachedClient.connect({
  addresses: [{ host: "cache.internal", port: 8357 }],
  readRepair: true,
});
```

Closes the narrow window after a primary restart where a replica still
holds a key its (fresh) primary doesn't, at the cost of extra reads only
on the misses that hit that window. The repair write carries a fixed 60-second TTL — the wire protocol's `G` response never returns the original one to preserve, and no TTL at all would immortalize already-expired keys — and,
unlike fire-and-forget replica writes, is uncapped and not drained on
`close()`: this only fires on an already-rare clean miss, and losing one
costs nothing beyond staying in the window for one more read.

### Hedged reads

Off by default. A read goes to the key's primary owner and moves on to
the next owner only when the primary *fails* — so one slow-but-alive
node (a saturated host, a bad link) makes every read that touches it
wait out its full round trip, and with R copies on N nodes that is
roughly R/N of all reads. Setting `readHedgeAfterMs` sends the same read
to the next owner as well once the primary has been silent for that
long, and takes the first answer:

```ts
const client = await NanocachedClient.connect({
  addresses: [{ host: "cache.internal", port: 8357 }],
  readHedgeAfterMs: 10, // hedge after 10 ms
});
```

A hit from any owner is final. A miss is only final from the primary: a
replica's miss is provisional (it may simply lack the copy), so the
primary's answer is still waited for and hedging never turns a hit into
a miss — a genuine miss on a slow primary still pays its round trip. Pick
a value a few times the healthy p99 so a fast cluster hedges rarely: each
hedge costs one extra read on another owner. Needs `R >= 2`; with a
single copy there is nobody to hedge to. Writes are unaffected — every
copy must be written, so a slow owner bounds writes to it regardless
(`fireAndForgetReplicas` moves only the replica legs off the caller's
path). The losing leg of a hedge is left to finish and is drained by
`close()`.

### Discovery replicas

When the cluster runs more than one discovery server, list them all in
`addresses`:

```ts
const client = await NanocachedClient.connect({
  addresses: [
    { host: "10.0.0.1", port: 8357 },
    { host: "10.0.0.2", port: 8357 },
  ],
});
```

Both the initial connect and every node-list refresh try the addresses in
order, so losing any one discovery replica costs nothing. An address that
is still warming up after a restart (it answers `B` while re-learning
cluster membership) is skipped like an unreachable one; if *every* address
is warming up, `connect()` rejects with `DiscoveryBusyError` — retry
shortly.

Addresses should point at discovery servers. If an address turns out to be
a cache node, the client pins itself to that one server (a single node
cannot provide cluster routing — the hash ring needs the name/address
pairs only discovery serves), and any remaining addresses go unused; when
several addresses were given, the client warns about this. Direct node
targets are meant for development or deliberate single-node deployments.

## SDK proxy mode (`viaProxy`)

Instead of routing directly to the cluster, a client can connect through
one `nanocached-proxy` — useful when the cluster's internal topology
shouldn't be exposed to every client (a fan-in/fan-out point, or a network
boundary discovery servers cross but individual nodes don't):

```ts
const client = await NanocachedClient.connect({
  addresses: [{ host: "cache.internal", port: 8357 }], // discovery addresses
  viaProxy: true,
});
```

`addresses` must still point at discovery servers — `viaProxy` fetches the
registered proxy roster from discovery (rather than the node list) and
connects to one proxy chosen at random, spreading a fleet of clients across
the available proxies; if the chosen proxy is unreachable, the client fails
over through the rest of the roster in random order. If the first address
reached turns out to be a cache node instead of a discovery server,
`connect()` fails fast with a clear error, since proxy mode has no
direct-node fallback.

A proxy looks exactly like a single node that owns every key, so once
connected the client is in the same single-connection mode a direct node
address puts it in: no ring view, no per-node connections, and **no hedged
reads** — `readHedgeAfterMs` is inert under `viaProxy`, since a proxy
connection has no replicas to hedge to. Namespaces, `clear`/`clearAll`,
tags, keep-alive, and compression all work unchanged over that one
connection.

On a connection loss, the client first retries the same proxy (it may have
simply restarted); if that fails, it re-fetches the proxy roster and picks
another at random, the same reconnect/refresh machinery cluster mode uses
for the node list.

## Idle connections, reconnect, and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent).

An address whose redial just failed is treated as still down for
`reconnectCooldownMs` (default 1000): requests routed to it during that
window fail immediately with the original dial error instead of each
paying another full 5-second connect timeout. Keep it short — a node that
genuinely recovers is shut out for at most this long.

`connect()` itself tolerates a node that discovery still lists but that
can't be reached — typically one that just died and hasn't been evicted
yet (a window of seconds): the node is kept in the ring without a
connection, requests for its keys fail over per request exactly as they
would after a mid-life death, and it is redialed after the cooldown. Only
a cluster with no reachable node at all fails `connect()`.

## Retryable-error status (`R`)

A node or proxy may answer any data operation (`get`/`set`/`delete`/
`clear`/`clearAll`) with a retryable-error status instead of the usual
reply: the request itself failed transiently — e.g. a `nanocached-proxy`'s
upstream node was briefly unreachable and stayed that way through the
proxy's own refresh-and-retry — but the connection is fine. Today only
`nanocached-proxy` emits this; the SDK handles it the same way on every
connection regardless.

The SDK retries such a request transparently, on the same connection: up
to 2 retries (3 attempts total), sleeping 50ms before the first retry and
100ms before the second. If the third attempt is still answered this way,
the operation rejects with `RetryableError` — but the connection itself is
never torn down or redialed over this, unlike every other error the SDK
raises; it stays open and immediately usable for the next operation.
Hedged reads (`readHedgeAfterMs`) need no special handling: a hedge leg
that hits this follows the same bounded retry on its own connection.

Every retry (whether it eventually succeeds or exhausts into
`RetryableError`) is counted in `client.stats().transientRetries` — see
[`client.stats()`](#api) below.

## API

- `NanocachedClient.connect(options)` —
  `options: { addresses, viaProxy?, authSecret?, tls?, ca?, compress?, compressionThreshold?, fireAndForgetReplicas?, readRepair?, readHedgeAfterMs?, reconnectCooldownMs? }`
  (`addresses` is a required, non-empty `NanocachedAddress[]`, each
  `{ host, port }`; see [SDK proxy mode](#sdk-proxy-mode-viaproxy) for
  `viaProxy`)
- `client.get(key)` — resolves `string | null`, strictly decoded as UTF-8
  (rejects with a `TypeError` if the value isn't valid UTF-8)
- `client.getBytes(key)` — resolves `Buffer | null`, the raw bytes
- `client.set(key, value, ttlSeconds = 0)` — `ttlSeconds` must be a
  non-negative integer; 0 (the default) means no expiry
- `client.delete(key)` — resolves `boolean` (whether the key existed)
- `client.namespace(ns)` — returns a `NanocachedNamespace` handle scoped to
  `ns` (a `string` or `Uint8Array`), exposing `get`/`getBytes`/`set`/
  `delete`/`clear` with the same signatures and semantics as above;
  `handle.namespace` exposes the raw namespace bytes
- `handle.clear()` — resolves (`Promise<void>`) once every node in the
  cluster (or the single node in standalone mode) has cleared that
  namespace; see [Namespaces](#namespaces)
- `client.clearAll()` — resolves (`Promise<void>`) once every namespace,
  the default one included, has been flushed on every node
- `client.close()` — resolves (`Promise<void>`) after any in-flight
  background replica writes finish and all connections are closed; later
  calls reject with `AlreadyClosedError`; a second `close()` warns but
  stays idempotent. Awaiting it is optional — un-awaited, teardown still
  happens once the drain settles
- `client.nodeUrls` — addresses currently connected to (introspection)
- `client.stats()` — a snapshot `ClientStats` of counters for failures and
  events this client swallows or retries by design instead of raising
  them to a caller: `replicaWriteFailures`, `readRepairFailures`,
  `refreshFailures`, and `transientRetries` (every `R` received — see
  [Retryable-error status](#retryable-error-status-r) above); each count
  is monotonic for the lifetime of this client

Every error the SDK itself raises — `AlreadyClosedError`,
`WrongNodeError`, `ConnectionLostError`, `RetryableError`,
`DecompressionError`, `DiscoveryBusyError`, and protocol/auth failures —
extends the exported `NanocachedError` base class, so `error instanceof
NanocachedError` distinguishes an expected nanocached failure from
everything else. Node system errors from the socket itself
(`ECONNREFUSED`, `ECONNRESET`, …) surface as-is, outside the family — and
so do caller mistakes caught by argument validation (e.g. an invalid
`ttlSeconds` throws a builtin `RangeError`): those indicate a bug in the
calling code, not a nanocached failure, a convention shared across the
SDKs (issue #47).

## License

MIT
