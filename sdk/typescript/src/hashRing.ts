/**
 * Rendezvous (highest-random-weight) hashing over a fixed node list (see
 * Client-side replication, which replaced client-side consistent hashing with a discovery server's virtual-node ring: FNV-1a's
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
 * Built from node *names*, not addresses (node identity decoupled from address) — `owners`
 * returns names, which the caller then looks up in a separate name ->
 * address map to actually open connections.
 *
 * Namespaces (issue #105) enter the key side of the score — see `keyHash`
 * — and are consensus-critical across the server, all six SDKs, and
 * `verify-staged-join` (`src/hash_ring.rs` on the Rust side is the
 * canonical definition this file ports byte-for-byte, same as the rest of
 * this module).
 */

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = (1n << 64n) - 1n;

/** The default namespace — see `keyHash`. Kept local to this file rather
 * than imported from `protocol.ts` so this module stays what it always
 * was: a dependency-free, standalone port of the scoring algorithm. */
const EMPTY_NAMESPACE: Uint8Array = new Uint8Array(0);

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

/** The canonical key-side hash (issue #105) — same two forms as
 * `key_hash` in src/hash_ring.rs:
 *
 * - default (empty) namespace: `fnv1a(key)`, byte-identical to the
 *   pre-namespace form, so every existing key keeps its placement across
 *   a rolling upgrade (no cluster-wide hit-rate cliff, and mixed-version
 *   clients still agree on placement);
 * - non-empty namespace: `fnv1a(be32(len(ns)) || ns || key)` — the
 *   namespace length as a 4-byte big-endian integer, then the namespace
 *   bytes, then the key bytes, hashed as one stream. Concatenating the
 *   three into a single buffer before hashing is equivalent to feeding
 *   FNV-1a's running state each piece in turn (it processes one byte at a
 *   time regardless), so this needs no separate "continue" form the way
 *   the Rust side's allocation-averse `fnv1a_continue` does. Length-
 *   prefixed so `("ab", "c")` and `("a", "bc")` never share an input;
 *   including the namespace at all is what keeps placement balanced —
 *   hashing the key alone would pile every namespace's common singleton
 *   keys (e.g. `config`) onto the same nodes.
 */
export function keyHash(key: Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): bigint {
  if (namespace.length === 0) return fnv1a(key);

  const namespaceLength = Buffer.alloc(4);
  namespaceLength.writeUInt32BE(namespace.length, 0);
  return fnv1a(Buffer.concat([namespaceLength, namespace, key]));
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

  /**
   * Issue #461 (mirrors src/hash_ring.rs's `HashRing::new` dedupe from
   * issue #328, and the Python/Go SDKs' own copies, issues #360/#389):
   * deduplicates `nodes`, keeping the first occurrence, so construction
   * order is otherwise unaffected. A repeated name would otherwise score
   * independently for each of its slots in `owners()`'s bounded
   * top-`replicas` insertion, occupying more than one place in the
   * returned set and inflating its effective share of the ring. Callers
   * (client.ts) already pass a deduped node list — this is defense in
   * depth for the constructor accepting a plain array.
   */
  constructor(nodes: readonly string[]) {
    const seen = new Set<string>();
    const deduped: string[] = [];
    for (const node of nodes) {
      if (seen.has(node)) continue;
      seen.add(node);
      deduped.push(node);
    }
    this.nodes = deduped;
    this.nodeHashes = deduped.map((node) => fnv1a(Buffer.from(node, "utf8")));
  }

  /** The key's owners: the `replicas` highest-scoring nodes, primary
   * first. Returns fewer than `replicas` when the cluster is smaller.
   * `namespace` (issue #105) defaults to the default (empty) namespace,
   * which scores exactly as it did before namespaces existed — see
   * `keyHash`. */
  owners(key: Uint8Array, replicas: number, namespace: Uint8Array = EMPTY_NAMESPACE): string[] {
    const hash = keyHash(key, namespace);

    const scored = this.nodes.map((node, index) => ({
      score: fmix64(this.nodeHashes[index] ^ hash),
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

  /** The key's primary — `owners(key, 1, namespace)[0]`. */
  route(key: Uint8Array, namespace: Uint8Array = EMPTY_NAMESPACE): string {
    if (this.nodes.length === 0) {
      // owners(key, 1) would silently return [] here, and [0] on that is
      // `undefined` — a caller expecting a string back deserves a clear
      // failure instead (issue #47 audit: this used to return `undefined`
      // uncaught).
      throw new RangeError("nanocached: cannot route on an empty hash ring");
    }
    return this.owners(key, 1, namespace)[0];
  }
}
