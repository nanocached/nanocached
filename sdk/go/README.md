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
    Seeds: []string{"127.0.0.1:8357"},
})
if err != nil { ... }
defer client.Close()

err = client.Set("greeting", []byte("hello"), 60*time.Second) // 0 = no expiry
value, ok, err := client.Get("greeting")                      // ok=false when missing
existed, err := client.Delete("greeting")
```

Keys are `string`, values `[]byte`. The client is safe for concurrent
use; requests are serialized per connection (concurrent callers queue).

## Discovery replicas

When the cluster runs more than one discovery server, list them all in
`Seeds`; both the initial connect and every node-list refresh try them
in order. A seed that is warming up after a restart (answers `B`) is
skipped like an unreachable one; if every seed is warming up, `Connect`
returns `ErrDiscoveryBusy` (match with `errors.Is`) — retry shortly.

## Replication

The cluster's replication factor R rides along with the node list, so
the SDK needs no configuration: `Set`/`Delete` fan out to all R owners
of a key (the primary's result decides; a dead replica never fails a
write), and `Get` asks the primary, falling over to the next owner only
when the holder is unreachable. `client.Replication()` exposes the
factor in use. A write whose primary just died recovers automatically
once discovery drops the node (bounded by its liveness timeout): the
failed attempt forces a node-list refresh and one retry.

## Reconnect and keep-alive

`nanocached-node` closes connections idle for 30 seconds; a request
that finds its connection dead redials and retries once transparently
(all operations are idempotent). If that extra round trip matters, opt
in to keep-alive:

```go
client, err := nanocached.Connect(nanocached.Config{
    Seeds:             []string{"127.0.0.1:8357"},
    KeepAliveInterval: 15 * time.Second, // below the server's 30s idle timeout
})
```

## Authentication and TLS

```go
client, err := nanocached.Connect(nanocached.Config{
    Seeds:      []string{"cache.internal:8357"},
    AuthSecret: "change-me",      // NANOCACHED_AUTH_SECRET on the server
    TLS:        &tls.Config{},    // system roots; set RootCAs for a private CA
})
```

## Notes

- This SDK speaks the current wire protocol (rendezvous hashing,
  replication-aware `L`/`W`); it requires an up-to-date server. The hash
  pipeline is pinned to cross-language test vectors that the server and
  the TypeScript/Python/Java/Rust/.NET SDKs also assert.
- Errors: `ErrClosed`, `ErrWrongNode`, `ErrDiscoveryBusy`, and
  `ErrConnectionLost` are sentinels for `errors.Is`.

## License

MIT
