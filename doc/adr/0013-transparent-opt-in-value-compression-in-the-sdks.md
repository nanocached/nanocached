# 13. Transparent, opt-in value compression in the SDKs

Date: 2026-08-19

## Status

Accepted

## Context

Values are opaque bytes to nanocached — the server never inspects or
transforms them — so nothing stops a client from compressing before `set`
and decompressing after `get`. For JSON/text payloads (the common case)
this typically buys 3-10x, more than offsetting [[0011]]'s R=2 capacity
cost, at the price of CPU on the client and a real interoperability
hazard: a value one client compressed is garbage to a client that doesn't
know to decompress it. This is exactly the class of decision [[0011]]'s
Consequences already accepted once (coordinated breaking change, no
dual-mode migration path) — the same trade shows up here at the SDK
layer instead of the wire layer, since the server never sees the format
change at all.

The six SDKs (TypeScript, Python, Java, Rust, .NET, Go) each already
expose a raw-bytes companion to their string `get`/`set` (`getBytes`,
`get_bytes`, `GetBytes`, ...) — the natural, single insertion point per
SDK for this, upstream of routing/fan-out so it applies identically
whether the target is one node or an R-owner set.

Compression format options considered:

- **Raw DEFLATE (RFC 1951, no zlib/gzip wrapper)**: smallest per-value
  overhead (no header, no checksum — TCP and the cache's own consistency
  model already cover integrity), available in every target language's
  standard library (Node `zlib.deflateRawSync`/`inflateRawSync`, Python
  `zlib` with negative `wbits`, Java `Deflater`/`Inflater` with
  `nowrap=true`, .NET `System.IO.Compression.DeflateStream` — despite the
  name, already header-less raw deflate — and Go `compress/flate`)
  **except Rust, which has no compression module in `std` at all**.
- **zlib-wrapped deflate or gzip**: adds a header/checksum this project
  doesn't need (nothing downstream re-derives a compressed value's
  integrity independently of the connection it arrived on), for no
  interoperability benefit — raw DEFLATE decompression is exactly as
  standardized and cross-implementation-portable as either wrapper. The
  cost of skipping the checksum: raw DEFLATE has no self-describing
  container, so a decoder fed bytes that aren't actually a DEFLATE stream
  doesn't reliably error — it may just produce short garbage output
  instead (confirmed during this ADR's review: Rust's `flate2` decodes
  three arbitrary bytes to five zero bytes without complaint, while
  Node's `zlib`, Python's `zlib`, and Java's `Inflater` all reject the
  same input). This is already covered by the mismatch caveat below —
  reading a value another format wrote is unsupported regardless — but it
  means "decompression failed" is a best-effort diagnostic, not a
  guaranteed one.
- **zstd**: better ratio/speed than DEFLATE, but not available in any of
  the six languages' standard libraries — would add a new dependency to
  *every* SDK, not just Rust.

Raw DEFLATE was verified end-to-end during this ADR's own review: a
single payload compressed once (Python `zlib`, level 6, `wbits=-15`) was
confirmed to decompress correctly via Node's `zlib`, Go's
`compress/flate`, Java's `Inflater`, .NET's `DeflateStream`, and Rust's
`flate2` (`rust_backend` feature — pure Rust, no C toolchain, keeping
Rust's cross-compiled build story intact the way [[0006]] cared about for
TLS). This confirms interoperability doesn't require byte-identical
compressor output across languages (DEFLATE's decoder is what's
standardized, not any particular encoder's choices) — only that every
SDK's decompressor accepts every other SDK's compressor output, which it
does.

Rust needing `flate2` as a new dependency conflicts with the SDKs' own
documented "no runtime dependencies outside the standard library, except
Rust's `tokio`" claim (`docs/sdks.html`). [[0006]] already established the
precedent for exactly this situation — TLS support ships as a Cargo
feature, default-on but compilable out — so compression follows the same
shape: a `compression` Cargo feature (default-on), gating `flate2` with
`default-features = false, features = ["rust_backend"]` to avoid a C
dependency, consistent with [[0006]]'s reasoning for choosing `rustls`'s
`ring` backend over one that needs CMake.

Detection format: the issue driving this (#18) specifies "magic-byte
prefix" — one byte, not a longer signature. A longer magic reduces (but,
against arbitrary binary payloads, can never eliminate) the chance of
misreading a value that predates compression being enabled; it does so at
the cost of overhead on *every* value, paid whether or not that value
ends up compressed. Given the feature is opt-in and its safe-use
condition (below) is already an absolute, documented requirement rather
than a probabilistic one, the extra margin a longer magic buys isn't
worth its constant per-value cost.

## Decision

Each SDK gets two new client options, off by default, following each
language's own existing option-naming convention (the same one `tls`/`ca`
already use — a boolean gate plus a companion value only meaningful when
the gate is on): `compress`/`Compress` (bool, default `false`) and
`compressionThreshold`/`compression_threshold`/`CompressionThreshold`
(integer byte count, default `256`).

Wire-visible format, applied only in the raw-bytes `set`/`get` path
(string `get`/`set` call through this, so they inherit it for free), and
**only when `compress` is enabled on this client**:

- `set`: if `value.len() < compressionThreshold`, store `0x00` followed
  by the value unchanged (below-threshold passthrough — not worth the
  CPU). Otherwise, raw-DEFLATE-compress the value; if the compressed form
  is smaller, store `0x01` followed by the compressed bytes; if it isn't
  (incompressible data — already-compressed media, random bytes, etc.,
  per the issue's "incompressible-data passthrough" requirement), store
  `0x00` followed by the original value instead. A value written with
  `compress` enabled therefore *always* carries the one-byte marker,
  compressed or not.
- `get`/`getBytes`: read the first byte. `0x00` → return the rest
  unchanged. `0x01` → raw-DEFLATE-decompress the rest and return that.
  Any other marker byte is a decompression error (see Consequences),
  raised as this SDK's normal error type with a message pointing at a
  `compress` mismatch as the likely cause. Decompressed output is capped
  at **64 MiB** in every SDK (added 2026-08-20, issue #41): the wire cap
  bounds only the *compressed* bytes received, so without this a tiny
  hostile or corrupt value could expand to an arbitrarily large
  allocation — a decompression bomb. Exceeding the cap is a
  decompression error like any other.
- With `compress` off (the default), `get`/`set` are entirely unchanged —
  no marker byte, no CPU cost, byte-for-byte what every existing
  deployment already does.

No server change, no wire protocol change: from nanocached's point of
view this is still just an opaque value. Every SDK's own test suite
gains a pinned cross-language vector — one canonical plaintext and its
raw-DEFLATE compressed bytes, produced once and hardcoded identically
into all six (the same "duplicated pinned constant, not a shared fixture
file" pattern the hash-ring FNV-1a/score vectors already use) — each
asserting it can decompress that exact byte sequence, not merely that it
round-trips its own output.

## Consequences

Easier:

- Meaningfully smaller values for the common JSON/text case, for free
  (zero server changes) and at zero cost to clients that don't opt in.
- Effectively offsets [[0011]]'s R=2 memory cost for compressible
  payloads, without touching the replication design itself.

Harder / risks to mitigate:

- **`compress` is a per-keyspace agreement, not a per-client
  preference.** Every client that reads or writes a given set of keys
  must use the same `compress` setting. A `compress`-off client reading
  a `compress`-on client's value sees the raw marker byte plus payload
  as "the value" (silently wrong, no error, since a disabled client never
  looks at the first byte specially). A `compress`-on client reading a
  value written before compression was enabled anywhere risks
  misinterpreting that value's first byte as a marker: if it happens to
  be `0x00`, the value is returned unchanged (harmless by luck); if it
  happens to be `0x01`, decompression is attempted against a body that
  was never really DEFLATE-compressed — usually this fails loudly, but
  raw DEFLATE has no checksum, so a decoder can occasionally produce
  short garbage output instead of erroring (see the format discussion
  above); any other marker byte always fails loudly, that check is a
  plain equality test. There is no dual-mode/migration path, matching
  [[0011]]'s precedent: turn `compress` on only for a fresh keyspace, or
  only after every client touching an existing one has upgraded and
  enabled it together.
- Compression is CPU cost traded for network/memory savings; values
  already compressed upstream (images, video, already-gzipped blobs)
  gain nothing and pay the compress-attempt cost once per `set` above the
  threshold (mitigated, not eliminated, by the incompressible-data
  passthrough).
- Rust's SDK now has a second real dependency (`flate2`, previously only
  `tokio` at baseline) — compiled out by disabling the default
  `compression` feature, the same escape hatch [[0006]] gives TLS.
