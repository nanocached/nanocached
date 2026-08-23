"""Rendezvous (highest-random-weight) hashing over a fixed node list (see
Client-side replication). This is deliberately a byte-for-byte port of the same
computation every other nanocached participant uses (the Rust node, the
TypeScript/Java/Rust/Go/.NET SDKs) — not just "a" rendezvous hash, but
*this specific* one: if this SDK's ranking disagreed with a node's own
copy, the two would disagree about which nodes hold a key. Cross-language
test vectors pin the pipeline.

For each (node, key) pair, ``score = fmix64(fnv1a(name) ^ key_hash)``; a
key's owners are the ``replicas`` highest-scoring nodes in descending score
order (ties — effectively impossible at 64 bits — break toward the
lexicographically smaller name), and its primary is the top one.

Built from node *names*, not addresses (node identity decoupled from address).

Namespaces (issue #105) enter the key side of the score — see key_hash()
below — and are consensus-critical: this must stay a byte-for-byte port of
src/hash_ring.rs's key_hash, or this SDK would place a namespaced key on a
different node than the server (and every other SDK) does.
"""

from __future__ import annotations

_MASK_64 = (1 << 64) - 1
_FNV_OFFSET_BASIS = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3


def fnv1a(data: bytes) -> int:
    """FNV-1a over 64 bits, matching Rust's wrapping u64 arithmetic."""
    return _fnv1a_continue(_FNV_OFFSET_BASIS, data)


def _fnv1a_continue(value: int, data: bytes) -> int:
    """Feeds ``data`` into an in-progress FNV-1a state, so a multi-part
    input (namespace length, namespace, key) hashes exactly as its
    concatenation would, with no intermediate ``bytes`` allocation —
    mirrors src/hash_ring.rs's ``fnv1a_continue``."""
    for byte in data:
        value ^= byte
        value = (value * _FNV_PRIME) & _MASK_64
    return value


def key_hash(namespace: bytes, key: bytes) -> int:
    """The canonical key-side hash fed into HRW scoring (issue #105):

    - default (empty) namespace: ``fnv1a(key)`` — byte-identical to the
      pre-namespace form, so an unnamespaced key's placement never moves
      (no cluster-wide hit-rate cliff on an upgrade, and mixed-version
      clients agree on placement).
    - non-empty namespace: ``fnv1a(be32(len(namespace)) || namespace ||
      key)`` — the namespace length as a 4-byte big-endian integer, then
      the namespace bytes, then the key bytes, hashed as one stream.
      Length-prefixed so ``("ab", "c")`` and ``("a", "bc")`` never share
      an input; including the namespace at all is what keeps placement
      balanced — hashing the key alone would pile every namespace's
      common singleton keys (e.g. ``config``) onto the same nodes.

    Byte-for-byte port of src/hash_ring.rs's ``key_hash`` — see the
    module docs.
    """
    if not namespace:
        return fnv1a(key)
    # A namespace is bounded by the request-size limit (see client.py's
    # _MAX_REQUEST_BYTES), so this always fits a u32; the 4-byte
    # big-endian encoding is the canonical width every implementation
    # uses, not a truncation that could ever occur in practice.
    namespace_length = len(namespace).to_bytes(4, "big")
    value = _fnv1a_continue(_FNV_OFFSET_BASIS, namespace_length)
    value = _fnv1a_continue(value, namespace)
    return _fnv1a_continue(value, key)


def fmix64(value: int) -> int:
    """MurmurHash3's 64-bit finalizer: the full-avalanche mix FNV-1a lacks."""
    value ^= value >> 33
    value = (value * 0xFF51AFD7ED558CCD) & _MASK_64
    value ^= value >> 33
    value = (value * 0xC4CEB9FE1A85EC53) & _MASK_64
    value ^= value >> 33
    return value


class HashRing:
    """A rendezvous-hash ranking over a fixed node list, built once from a
    discovery server's node list. Ranking never changes once built."""

    def __init__(self, nodes: list[str]) -> None:
        self._nodes = list(nodes)
        self._node_hashes = [fnv1a(node.encode("utf-8")) for node in self._nodes]

    def owners(self, key: bytes, replicas: int, *, namespace: bytes = b"") -> list[str]:
        """The key's owners: the ``replicas`` highest-scoring nodes, primary
        first. Returns fewer when the cluster is smaller. ``namespace``
        defaults to the empty (default) namespace, which hashes
        byte-identically to the pre-namespace form — see key_hash()."""
        hashed_key = key_hash(namespace, key)
        scored = [
            (fmix64(node_hash ^ hashed_key), node)
            for node_hash, node in zip(self._node_hashes, self._nodes)
        ]
        # Descending by score; ties toward the lexicographically smaller
        # name — a total order every implementation agrees on.
        scored.sort(key=lambda pair: (-pair[0], pair[1]))
        return [node for _, node in scored[:replicas]]

    def route(self, key: bytes, *, namespace: bytes = b"") -> str:
        """The key's primary — ``owners(key, 1, namespace=namespace)[0]``."""
        if not self._nodes:
            # owners(key, 1) would silently return [], and [0] on that is a
            # bare IndexError — not one of this SDK's own error types.
            # Matches this SDK's own convention for other eager
            # input-validation errors (e.g. client.py's ttl_seconds check).
            raise ValueError("nanocached: cannot route on an empty hash ring")
        return self.owners(key, 1, namespace=namespace)[0]
