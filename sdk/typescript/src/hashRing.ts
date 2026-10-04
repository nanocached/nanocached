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

// ---------------------------------------------------------------------
// BigInt-free scoring for `HashRing.owners`' hot loop. A 64-bit value is
// carried as two u32 halves (`hi`, `lo`) in the module scratch pair below
// (JS is single-threaded and none of these re-enter, so one pair is safe);
// every function here computes exactly what its BigInt counterpart above
// does, and hashRing.test.ts checks them against those over many inputs.
// ---------------------------------------------------------------------
let partsHi = 0;
let partsLo = 0;

/** The high 32 bits of the 64-bit product of two u32 values. */
function mulHi32(a: number, b: number): number {
  const a0 = a & 0xffff;
  const a1 = a >>> 16;
  const b0 = b & 0xffff;
  const b1 = b >>> 16;
  const low = a0 * b0;
  const mid1 = a1 * b0 + (low >>> 16);
  const mid2 = a0 * b1 + (mid1 & 0xffff);
  return (a1 * b1 + (mid1 >>> 16) + (mid2 >>> 16)) >>> 0;
}

/** `keyHash(key, namespace)` into `partsHi`/`partsLo` — FNV-1a fed the
 * `be32(len(ns))`, namespace and key bytes one after the other, which is
 * the same stream `keyHash` concatenates (see its doc comment). */
function keyHashParts(key: Uint8Array, namespace: Uint8Array): void {
  let hi = 0xcbf29ce4;
  let lo = 0x84222325;
  const feed = (byte: number): void => {
    lo = (lo ^ byte) >>> 0;
    // Multiply by the FNV prime 0x100000001b3 = 2^40 + 0x1b3 (mod 2^64):
    // `h << 40` only reaches the high word, as `lo << 8`.
    const product = lo * 0x1b3; // < 2^41, exact in a double
    const carry = Math.floor(product / 4294967296);
    hi = (Math.imul(hi, 0x1b3) + carry + (lo << 8)) >>> 0;
    lo = product >>> 0;
  };
  if (namespace.length > 0) {
    const length = namespace.length;
    feed(length >>> 24);
    feed((length >>> 16) & 0xff);
    feed((length >>> 8) & 0xff);
    feed(length & 0xff);
    for (let i = 0; i < namespace.length; i++) feed(namespace[i]);
  }
  for (let i = 0; i < key.length; i++) feed(key[i]);
  partsHi = hi;
  partsLo = lo;
}

/** `fmix64` of the value `hi:lo` (each taken mod 2^32), into
 * `partsHi`/`partsLo`. */
function scoreParts(hiIn: number, loIn: number): void {
  let hi = hiIn >>> 0;
  let lo = loIn >>> 0;
  // x ^= x >> 33 touches only the low word: (x >> 33) is `hi >>> 1`.
  lo = (lo ^ (hi >>> 1)) >>> 0;
  // x *= 0xff51afd7ed558ccd (mod 2^64)
  let nextLo = Math.imul(lo, 0xed558ccd) >>> 0;
  hi = (mulHi32(lo, 0xed558ccd) + Math.imul(lo, 0xff51afd7) + Math.imul(hi, 0xed558ccd)) >>> 0;
  lo = nextLo;
  lo = (lo ^ (hi >>> 1)) >>> 0;
  // x *= 0xc4ceb9fe1a85ec53 (mod 2^64)
  nextLo = Math.imul(lo, 0x1a85ec53) >>> 0;
  hi = (mulHi32(lo, 0x1a85ec53) + Math.imul(lo, 0xc4ceb9fe) + Math.imul(hi, 0x1a85ec53)) >>> 0;
  lo = nextLo;
  lo = (lo ^ (hi >>> 1)) >>> 0;
  partsHi = hi;
  partsLo = lo;
}

/** Whether candidate `a` ranks strictly ahead of `b` in owner order:
 * higher score first; ties toward the lexicographically smaller name — a
 * total order, so every implementation agrees. */
function outranks(aHi: number, aLo: number, aNode: string, bHi: number, bLo: number, bNode: string): boolean {
  if (aHi !== bHi) return aHi > bHi;
  if (aLo !== bLo) return aLo > bLo;
  return aNode < bNode;
}

// Above this many requested owners, `owners` sorts every node instead of
// keeping a best-first prefix: the bounded insertion is O(n * replicas),
// which only beats an O(n log n) sort while `replicas` stays small (the
// replication factor rides in from discovery and is not capped on the
// wire).
const MAX_BOUNDED_SELECTION = 32;

/**
 * A rendezvous-hash ranking over a fixed node list, built once from a
 * discovery server's node list. Ranking a key never changes once built —
 * this class doesn't react to nodes joining or leaving after construction.
 */
export class HashRing {
  private readonly nodes: readonly string[];
  // Each node's `fnv1a(name)` as two u32 halves: `owners` scores every
  // node for every lookup, and BigInt arithmetic there cost more than
  // everything else in it (see `scoreParts`).
  private readonly nodeHashHi: Uint32Array;
  private readonly nodeHashLo: Uint32Array;

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
    this.nodeHashHi = new Uint32Array(deduped.length);
    this.nodeHashLo = new Uint32Array(deduped.length);
    deduped.forEach((node, index) => {
      const hash = fnv1a(Buffer.from(node, "utf8"));
      this.nodeHashHi[index] = Number(hash >> 32n);
      this.nodeHashLo[index] = Number(hash & 0xffffffffn);
    });
  }

  /** The key's owners: the `replicas` highest-scoring nodes, primary
   * first. Returns fewer than `replicas` when the cluster is smaller.
   * `namespace` (issue #105) defaults to the default (empty) namespace,
   * which scores exactly as it did before namespaces existed — see
   * `keyHash`. */
  owners(key: Uint8Array, replicas: number, namespace: Uint8Array = EMPTY_NAMESPACE): string[] {
    const count = this.nodes.length;
    if (count === 0 || replicas === 0) return [];
    keyHashParts(key, namespace);
    const keyHi = partsHi;
    const keyLo = partsLo;

    // `replicas` is typically a handful next to the cluster size, so
    // instead of sorting every node (O(n log n)), keep just the best
    // `replicas` seen so far in best-first order and insert into it —
    // O(n * replicas), the same bounded insertion the Go/Rust/Java/.NET
    // SDKs use. A large (or odd — negative, fractional) `replicas` takes
    // the plain sort below, which also keeps `slice`'s own handling of
    // such values; both produce the same order.
    if (!Number.isInteger(replicas) || replicas < 0 || replicas > MAX_BOUNDED_SELECTION) {
      const scored: { hi: number; lo: number; node: string }[] = [];
      for (let i = 0; i < count; i++) {
        scoreParts(this.nodeHashHi[i] ^ keyHi, this.nodeHashLo[i] ^ keyLo);
        scored.push({ hi: partsHi, lo: partsLo, node: this.nodes[i] });
      }
      scored.sort((a, b) => (outranks(a.hi, a.lo, a.node, b.hi, b.lo, b.node) ? -1 : 1));
      return scored.slice(0, replicas).map(({ node }) => node);
    }

    const topHi = new Uint32Array(replicas);
    const topLo = new Uint32Array(replicas);
    const topNode: string[] = new Array(replicas);
    let kept = 0;
    for (let i = 0; i < count; i++) {
      scoreParts(this.nodeHashHi[i] ^ keyHi, this.nodeHashLo[i] ^ keyLo);
      const hi = partsHi;
      const lo = partsLo;
      const node = this.nodes[i];
      if (kept === replicas && !outranks(hi, lo, node, topHi[kept - 1], topLo[kept - 1], topNode[kept - 1])) {
        continue; // no better than the worst candidate currently kept
      }
      let pos = kept;
      while (pos > 0 && outranks(hi, lo, node, topHi[pos - 1], topLo[pos - 1], topNode[pos - 1])) pos--;
      if (kept < replicas) kept++;
      for (let j = kept - 1; j > pos; j--) {
        topHi[j] = topHi[j - 1];
        topLo[j] = topLo[j - 1];
        topNode[j] = topNode[j - 1];
      }
      topHi[pos] = hi;
      topLo[pos] = lo;
      topNode[pos] = node;
    }
    return topNode.slice(0, kept);
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
