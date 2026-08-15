# nanocached (TypeScript client)

A minimal TypeScript/Node.js client for [nanocached](../../README.md)'s
binary-safe TCP protocol: `get`/`set`/`delete`, optional shared-secret
authentication, and optional TLS.

## Usage

```ts
import { NanocachedClient } from "nanocached";

const client = await NanocachedClient.connect({ host: "127.0.0.1", port: 8356 });

await client.set("name", "Alice");
await client.get("name"); // Buffer.from("Alice")
await client.delete("name"); // true

client.close();
```

With a TTL:

```ts
await client.set("session:123", token, { ttlSeconds: 3600 });
```

With authentication (see the root README's
[Authentication](../../README.md#authentication) section):

```ts
const client = await NanocachedClient.connect({
  host: "127.0.0.1",
  port: 8356,
  authSecret: process.env.NANOCACHED_AUTH_SECRET,
});
```

With TLS (see the root README's [TLS](../../README.md#tls) section):
pass `tls: true` if the server's certificate is issued by a trusted CA —
this verifies it against Node's default trust store, the normal way TLS
works:

```ts
const client = await NanocachedClient.connect({ host: "127.0.0.1", port: 8356, tls: true });
```

`tls` accepts a plain `boolean`, not just the literal `true`, so the same
code can toggle it per environment (e.g. off locally, on in staging/prod)
without an `x ? true : undefined` workaround:

```ts
const client = await NanocachedClient.connect({
  host: "127.0.0.1",
  port: 8356,
  tls: process.env.NANOCACHED_TLS === "1",
});
```

If there's no CA-issued certificate available — e.g. local development,
or a private cluster with no PKI of its own — nanocached-node can be run
with a self-signed certificate instead. Node's default trust store won't
recognize it, so pass the certificate to trust explicitly via `ca`, which
*replaces* the default trust store rather than adding to it (matching
`node:tls`'s own behavior for an explicit `ca`, and nanocached-node's own
`--tls-ca` semantics). `ca` takes PEM content directly, not a file path,
so read the certificate yourself:

```ts
import { readFileSync } from "node:fs";

const client = await NanocachedClient.connect({
  host: "127.0.0.1",
  port: 8356,
  tls: { ca: readFileSync("ca.pem") },
});
```

`get` returns `Buffer | null` (`null` for a missing key, matching the
protocol's `N` response); keys and values accept `string | Uint8Array`.

## Development

```sh
npm install
npm run build      # compiles src/ to dist/
npm run typecheck  # tsc --noEmit
npm test           # compiles src/+test/ to dist-test/ and runs them
```

Most of `test/client.test.ts` spawns the real `nanocached-node` binary
(`cargo build --bin nanocached-node` from the repository root) rather than
mocking the protocol, so those tests need it built first; they skip
themselves (not fail) if it isn't. The TLS tests additionally need
`openssl` on `PATH` to generate a throwaway self-signed certificate, and
skip themselves if it isn't available either.

There is no separate lint step: `tsc --strict` (via `typecheck`/`test`) is
the only static check, matching this repository's general preference for
fewer moving parts over more tooling.
