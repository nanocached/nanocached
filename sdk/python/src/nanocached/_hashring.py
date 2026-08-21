"""Rendezvous (highest-random-weight) hashing over a fixed node list (see
doc/adr/0011-*.md). This is deliberately a byte-for-byte port of the same
computation every other nanocached participant uses (the Rust node, the
TypeScript/Java/Rust/Go/.NET SDKs) — not just "a" rendezvous hash, but
*this specific* one: if this SDK's ranking disagreed with a node's own
copy, the two would disagree about which nodes hold a key. Cross-language
test vectors pin the pipeline.

For each (node, key) pair, ``score = fmix64(fnv1a(name) ^ fnv1a(key))``; a
key's owners are the ``replicas`` highest-scoring nodes in descending score
order (ties — effectively impossible at 64 bits — break toward the
lexicographically smaller name), and its primary is the top one.

Built from node *names*, not addresses (doc/adr/0009-*.md).
"""

from __future__ import annotations

_MASK_64 = (1 << 64) - 1
_FNV_OFFSET_BASIS = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3


def fnv1a(data: bytes) -> int:
    """FNV-1a over 64 bits, matching Rust's wrapping u64 arithmetic."""
    value = _FNV_OFFSET_BASIS
    for byte in data:
        value ^= byte
        value = (value * _FNV_PRIME) & _MASK_64
    return value


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

    def owners(self, key: bytes, replicas: int) -> list[str]:
        """The key's owners: the ``replicas`` highest-scoring nodes, primary
        first. Returns fewer when the cluster is smaller."""
        key_hash = fnv1a(key)
        scored = [
            (fmix64(node_hash ^ key_hash), node)
            for node_hash, node in zip(self._node_hashes, self._nodes)
        ]
        # Descending by score; ties toward the lexicographically smaller
        # name — a total order every implementation agrees on.
        scored.sort(key=lambda pair: (-pair[0], pair[1]))
        return [node for _, node in scored[:replicas]]

    def route(self, key: bytes) -> str:
        """The key's primary — ``owners(key, 1)[0]``."""
        if not self._nodes:
            # owners(key, 1) would silently return [], and [0] on that is a
            # bare IndexError — not one of this SDK's own error types.
            # Matches this SDK's own convention for other eager
            # input-validation errors (e.g. client.py's ttl_seconds check).
            raise ValueError("nanocached: cannot route on an empty hash ring")
        return self.owners(key, 1)[0]
