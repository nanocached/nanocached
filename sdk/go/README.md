# nanocached (Go)

Go client SDK for [nanocached](https://github.com/nanocached/nanocached),
a tiny distributed cache. Talks to either a single `nanocached-node` or a
`nanocached-discovery`-fronted cluster — the SDK figures out which from
the server's own handshake, so the calling code is identical either way.

Requires Go 1.22+. No dependencies outside the standard library.

## Install

```sh
go get github.com/nanocached/nanocached/sdk/go
```

## Quick start

```go
import nanocached "github.com/nanocached/nanocached/sdk/go"

// Point at a single node, or at a discovery server fronting a
// cluster — same call either way.
client, err := nanocached.Connect(nanocached.Config{
    Addresses: []nanocached.Address{{Host: "127.0.0.1", Port: 8357}},
})
if err != nil { ... }
defer client.Close()

err = client.Set("greeting", "hello", 60) // ttlSeconds; 0 = no expiry
value, ok, err := client.Get("greeting")  // ok=false when missing
existed, err := client.Delete("greeting")
```

Keys are `string`. Values are `string` via `Get`/`Set`, or raw `[]byte`
via `GetBytes`/`SetBytes` for binary data — `Get` decodes with a plain
`string(bytes)` conversion, which in Go is always lossless, so unlike
some other nanocached SDKs there is no decode-failure case to handle.
The client is safe for concurrent use; requests are serialized per
connection (concurrent callers queue).

## Discovery replicas

When the cluster runs more than one discovery server, list them all in
`Addresses`; both the initial connect and every node-list refresh try
them in order. An address that is warming up after a restart (answers
`B`) is skipped like an unreachable one; if every address is warming up,
`Connect` returns `ErrDiscoveryBusy` (match with `errors.Is`) — retry
shortly.

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `Set`/`SetBytes`/`Delete` fan out to all
R owners of a key (the primary's result decides; a dead replica never
fails a write), and `Get`/`GetBytes` ask the primary, falling over to
the next owner only when the holder is unreachable. `client.Replication()`
exposes the factor in use. A write whose primary just died recovers
automatically once discovery drops the node (bounded by its liveness
timeout): the failed attempt forces a node-list refresh and one retry.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 60 seconds; the SDK keeps
its connections warm automatically, pinging any connection that real
traffic has left idle for 30 seconds — so an idle timeout never severs a
healthy client, and a request that does find its connection dead (a node
restart, a network blip) redials and retries once transparently (all
operations are idempotent). There is nothing to configure.

## Authentication and TLS

```go
client, err := nanocached.Connect(nanocached.Config{
    Addresses:  []nanocached.Address{{Host: "cache.internal", Port: 8357}},
    AuthSecret: "change-me", // NANOCACHED_AUTH_SECRET on the server
    TLS:        true,        // system/platform trust store by default
    CA:         "",          // path to a PEM file of trusted root cert(s);
                              // only meaningful when TLS is true, replacing
                              // the default trust store
})
```

`CA` is silently ignored when `TLS` is `false`. An unreadable or
unparseable CA file when `TLS` is `true` fails `Connect`.

## Values and TTL

```go
err = client.Set("k", "hello", 0)              // string value, no expiry
err = client.SetBytes("k", []byte{0xff}, 300)  // raw bytes, 300s TTL
value, ok, err := client.Get("k")              // string
raw, ok, err := client.GetBytes("k")           // []byte
```

`ttlSeconds` is a whole number of seconds; `0` means no expiry. A
negative `ttlSeconds` is rejected before any network call.

## Notes

- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server. The hash
  pipeline is pinned to cross-language test vectors that the server and
  the TypeScript/Python/Java/Rust/.NET SDKs also assert.
- Errors: `ErrClosed`, `ErrWrongNode`, `ErrDiscoveryBusy`, and
  `ErrConnectionLost` are sentinels for `errors.Is`.
- `Close()` is idempotent; calling it a second time warns to stderr
  instead of erroring. `Connect()` also warns to stderr if it's called
  for a single address that a previous, still-open connection from this
  process already points at — a common sign that `Close()` was
  forgotten.

## License

MIT
