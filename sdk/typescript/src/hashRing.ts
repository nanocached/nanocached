/**
 * Rendezvous (highest-random-weight) hashing over a fixed node list (see
 * doc/adr/0011-*.md, which replaced ADR-0002's virtual-node ring: FNV-1a's
 * weak high-bit avalanche clustered ring points into narrow bands, skewing
 * node shares by up to ~2×; HRW measures within 2% of fair and yields
 * replica sets for free). This is deliberately a byte-for-byte port of the
 * same computation every other nanocached participant uses (the Rust
 * node, the Python/Java/Rust/Go/.NET SDKs) — not just "a" rendezvous
 * hash, but *this specific* one: if this SDK's ranking disagreed with a
 * node's own copy, the two would disagree about which nodes hold a key.
 *
 * For each (node, key) pair, `score = fmix64(fnv1a(name) ^ fnv1a(key))`; a
 * key's owners are the `replicas` highest-scoring nodes in descending
 * score order (ties — effectively impossible at 64 bits — break toward the
 * lexicographically smaller name), and its primary is the top one.
 *
 * Built from node *names*, not addresses (doc/adr/0009-*.md) — `owners`
 * returns names, which the caller then looks up in a separate name ->
 * address map to actually open connections.
 */

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

/** MurmurHash3's 64-bit finalizer: a full-avalanche bijective mix, which
 * is what FNV-1a alone lacks (see the module docs). */
export function fmix64(hash: bigint): bigint {
  hash ^= hash >> 33n;
  hash = (hash * 0xff51afd7ed558ccdn) & MASK_64;
  hash ^= hash >> 33n;
  hash = (hash * 0xc4ceb9fe1a85ec53n) & MASK_64;
  hash ^= hash >> 33n;
  return hash;
}

/**
 * A rendezvous-hash ranking over a fixed node list, built once from a
 * discovery server's node list. Ranking a key never changes once built —
 * this class doesn't react to nodes joining or leaving after construction.
 */
export class HashRing {
  private readonly nodes: readonly string[];
  private readonly nodeHashes: readonly bigint[];

  constructor(nodes: readonly string[]) {
    this.nodes = nodes;
    this.nodeHashes = nodes.map((node) => fnv1a(Buffer.from(node, "utf8")));
  }

  /** The key's owners: the `replicas` highest-scoring nodes, primary
   * first. Returns fewer than `replicas` when the cluster is smaller. */
  owners(key: Uint8Array, replicas: number): string[] {
    const keyHash = fnv1a(key);

    const scored = this.nodes.map((node, index) => ({
      score: fmix64(this.nodeHashes[index] ^ keyHash),
      node,
    }));

    // Descending by score; ties toward the lexicographically smaller
    // name — a total order, so every implementation agrees.
    scored.sort((a, b) => {
      if (a.score !== b.score) return a.score < b.score ? 1 : -1;
      return a.node < b.node ? -1 : 1;
    });

    return scored.slice(0, replicas).map(({ node }) => node);
  }

  /** The key's primary — `owners(key, 1)[0]`. */
  route(key: Uint8Array): string {
    if (this.nodes.length === 0) {
      // owners(key, 1) would silently return [] here, and [0] on that is
      // `undefined` — a caller expecting a string back deserves a clear
      // failure instead (issue #47 audit: this used to return `undefined`
      // uncaught).
      throw new RangeError("nanocached: cannot route on an empty hash ring");
    }
    return this.owners(key, 1)[0];
  }
}
