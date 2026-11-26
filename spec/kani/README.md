# Kani proof harnesses

Bounded model-checking harnesses for pure helper functions of the node
binary. They live in `#[cfg(kani)] mod verification` blocks at the end of
`src/command.rs`, `src/hash_ring.rs` and `src/response.rs`, so ordinary
builds and tests never compile them.

A success means no input within the harness's bounds violates its
assertions. It says nothing about inputs beyond those bounds or about the
code that calls these functions.

## Running

With Kani 0.68.0 (`cargo install --locked kani-verifier && cargo kani setup`),
one harness at a time:

```sh
cargo kani --bin nanocached-node --exact \
  --harness command::verification::parse_length_is_exact_decimal
```

## Harnesses

| Harness | Bounds | Rough time |
|---|---|---|
| `command::verification::parse_length_is_exact_decimal` | fields up to 21 bytes | 2 min |
| `command::verification::cas_digest_round_trips_and_nothing_else_parses` | every digest, every 32-byte field | 20 s |
| `command::verification::field_ends_are_checked_sums` | any start, three lengths | 5 s |
| `command::verification::header_line_end_is_the_first_lf` | buffers up to 6 bytes, any recorded progress | 5 s |
| `command::verification::joined_entry_stays_inside_the_buffer` | buffers up to 8 bytes, any cursor | 15 s |
| `hash_ring::verification::fnv1a_continue_hashes_the_concatenation` | any state, 4 bytes, every split | 1 min |
| `hash_ring::verification::fmix64_is_injective` | all of `u64` | 3 min |
| `response::verification::ttl_wire_seconds_rounds_up` | every `Duration` below `u64::MAX` seconds | 5 s |

## Not covered

The frame parser as a whole (`parse_resumable`) is not checked. Harnesses
over a `BytesMut` input did not finish within 300 s per harness, even for a
fully concrete two-byte frame parsed with tagging on.
