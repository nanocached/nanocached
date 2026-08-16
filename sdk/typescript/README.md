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
opens one pipelined connection per node, and routes each key with the same
consistent-hash ring (FNV-1a, 128 virtual nodes per node) every other
nanocached client and node uses — so all parties agree on which node owns a
key.

The node list is re-fetched lazily when it is more than 30 seconds old. If a
node answers that it no longer owns a key (its view of the cluster changed),
the SDK refreshes the node list and retries that operation once. A discovery
outage degrades only topology updates: existing connections keep serving
traffic on the last-known node list.

## API

- `NanocachedClient.connect(options)` — `options: { host, port, authSecret?, tls? }`
- `client.get(key)` — resolves `Buffer | null`
- `client.set(key, value, { ttlSeconds? })` — `ttlSeconds` must be a
  non-negative integer; omit it for no expiry
- `client.delete(key)` — resolves `boolean` (whether the key existed)
- `client.close()` — closes all connections; later calls reject with
  `AlreadyClosedError`
- `client.nodeUrls` — addresses currently connected to (introspection)

## License

MIT
