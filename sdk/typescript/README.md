# nanocached

TypeScript/Node.js SDK for [nanocached](https://github.com/t0k0sh1/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster with client-side consistent hashing —
the SDK figures out which from the server's own handshake response, so the
calling code is identical either way.

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
// same options either way.
const client = await NanocachedClient.connect({ host: "127.0.0.1", port: 11311 });

await client.set("greeting", "hello", { ttlSeconds: 60 });

const value = await client.get("greeting"); // Buffer | null
console.log(value?.toString()); // "hello"

const existed = await client.delete("greeting"); // boolean

client.close();
```

Keys and values may be `string` (encoded as UTF-8) or `Uint8Array`; values
always come back as `Buffer` (`null` when the key is missing).

## Authentication

If the server was started with `NANOCACHED_AUTH_SECRET`, pass the same
secret:

```ts
const client = await NanocachedClient.connect({
  host: "cache.internal",
  port: 11311,
  authSecret: process.env.NANOCACHED_AUTH_SECRET,
});
```

## TLS

If the server was started with `--tls-cert`/`--tls-key`:

```ts
// Certificate issued by a publicly trusted CA:
const client = await NanocachedClient.connect({ host, port, tls: true });

// Self-signed / private CA — trust exactly that certificate instead:
const client = await NanocachedClient.connect({
  host,
  port,
  tls: { ca: fs.readFileSync("cluster-ca.pem") },
});
```

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

### Discovery replicas

When the cluster runs more than one discovery server, pass them all as
`seeds` instead of a single `host`/`port`:

```ts
const client = await NanocachedClient.connect({
  seeds: [
    { host: "10.0.0.1", port: 8357 },
    { host: "10.0.0.2", port: 8357 },
  ],
});
```

Both the initial connect and every node-list refresh try the seeds in
order, so losing any one discovery replica costs nothing. A seed that is
still warming up after a restart (it answers `B` while re-learning cluster
membership) is skipped like an unreachable one; if *every* seed is warming
up, `connect()` rejects with `DiscoveryBusyError` — retry shortly.

Seeds should point at discovery servers. If a seed turns out to be a cache
node, the client pins itself to that one server (a single node cannot
provide cluster routing — the hash ring needs the name/address pairs only
discovery serves), and any remaining seeds go unused; when several seeds
were given, the client warns about this. Direct node targets are meant for
development or deliberate single-node deployments.

## Idle connections, reconnect, and keep-alive

`nanocached-node` closes connections that have been idle for 30 seconds.
The SDK handles this transparently: a request that finds its connection
dead reconnects to the same node first (concurrent requests share one
reconnect). The only cost is one extra round trip on the first request
after a long idle gap.

If that round trip matters, opt in to keep-alive:

```ts
const client = await NanocachedClient.connect({
  host,
  port,
  keepAliveIntervalMs: 15_000, // must sit below the server's 30s idle timeout
});
```

Every interval, each connection that real traffic has left idle for at
least that long gets a lightweight request, keeping the server's idle
timer from ever firing. This trades background load on every node (from
every long-lived client) for latency, so it is off by default.

## API

- `NanocachedClient.connect(options)` — `options: { host?, port?, seeds?, authSecret?, tls?, keepAliveIntervalMs? }`
  (give either `host`/`port` or a non-empty `seeds` list)
- `client.get(key)` — resolves `Buffer | null`
- `client.set(key, value, { ttlSeconds? })` — `ttlSeconds` must be a
  non-negative integer; omit it for no expiry
- `client.delete(key)` — resolves `boolean` (whether the key existed)
- `client.close()` — closes all connections; later calls reject with
  `AlreadyClosedError`
- `client.nodeUrls` — addresses currently connected to (introspection)

## License

MIT
