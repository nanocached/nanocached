/**
 * Client-side consistent hashing over a fixed node list (see
 * doc/adr/0002-*.md in the repository root). This is deliberately a
 * byte-for-byte port of the same algorithm `src/bin/bench.rs` uses on the
 * Rust side (FNV-1a, 128 virtual nodes per real node) — not just "a"
 * consistent hash, but *this specific* one. nanocached-node instances are
 * independent (no cross-node coordination or replication), so which node
 * holds a given key is entirely a client-side decision; if this SDK's ring
 * disagreed with another client's (or a node's own `src/hash_ring.rs`
 * copy, computing an ADR-0008 handoff diff), the two would disagree about
 * which node owns a key.
 *
 * Built from node *names*, not addresses (doc/adr/0009-*.md) — `route`
 * returns a name, which the caller then looks up in a separate name ->
 * address map to actually open a connection. This class itself doesn't
 * know or care what its string identifiers mean.
 */

const VIRTUAL_NODES_PER_NODE = 128;
const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = (1n << 64n) - 1n;

/** FNV-1a over 64 bits, matching Rust's `u64` wrapping arithmetic exactly
 * (hence BigInt, masked to 64 bits after every multiply — a plain `number`
 * only has 53 bits of safe integer precision). */
export function fnv1a(bytes: Uint8Array): bigint {
  let hash = FNV_OFFSET_BASIS;
  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & MASK_64;
  }
  return hash;
}

/** First index `i` such that `points[i][0] >= target`, given `points` is
 * sorted ascending by its first element — a "lower bound" binary search,
 * matching Rust's `slice::partition_point(|p| p.0 < target)`. */
function lowerBound(points: Array<[bigint, number]>, target: bigint): number {
  let low = 0;
  let high = points.length;

  while (low < high) {
    const mid = (low + high) >>> 1;
    if (points[mid][0] < target) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
}

/**
 * A consistent-hash ring over a fixed node list, built once from a
 * discovery server's node list. Routing a key never changes once the ring
 * is built — this class doesn't react to nodes joining or leaving after
 * construction (matching bench.rs's own documented behavior).
 */
export class HashRing {
  private readonly nodes: readonly string[];
  private readonly points: ReadonlyArray<[bigint, number]>;

  constructor(nodes: readonly string[]) {
    this.nodes = nodes;

    const points: Array<[bigint, number]> = [];
    for (let index = 0; index < nodes.length; index++) {
      for (let virtualId = 0; virtualId < VIRTUAL_NODES_PER_NODE; virtualId++) {
        const point = fnv1a(Buffer.from(`${nodes[index]}#${virtualId}`, "ascii"));
        points.push([point, index]);
      }
    }
    points.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

    this.points = points;
  }

  route(key: Uint8Array): string {
    const hash = fnv1a(key);
    let position = lowerBound(this.points as Array<[bigint, number]>, hash);
    if (position === this.points.length) position = 0;

    const [, nodeIndex] = this.points[position];
    return this.nodes[nodeIndex];
  }
}
