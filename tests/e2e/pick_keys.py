"""pick_keys.py <leaver> <displaced> <other> <joiner> N: classify k0..kN-1 by their
owners (R=2) before and after the joiner joins {leaver, displaced, other}, with
the SDK's own ring (same HRW as the node).

Prints one line per class: `<class> k.. k..`.
  target   R0 owners {leaver, displaced} and R1 owners {leaver, joiner}: the
           displaced node loses the key to the joiner and the leaver keeps it,
           so the leaver's decommission hands it back to the displaced node as
           the entrant. The class #582 lost.
  control  R0 owners {leaver, displaced} and R1 owners the same: the displaced
           node keeps the key through the join; must never miss.
"""
import sys
from nanocached._hashring import HashRing

n1, n3, n2, n4 = sys.argv[1:5]; n = int(sys.argv[5])  # leaver, displaced, other, joiner
r0 = HashRing([n1, n2, n3]); r1 = HashRing([n1, n2, n3, n4]) if n4 != "-" else None
# joiner "-": only the "pre" class, keys owned by {leaver, displaced} before any join
cls = {"pre": []} if r1 is None else {"target": [], "control": []}
for i in range(n):
    k = f"k{i}"; b = k.encode()
    o0 = set(r0.owners(b, 2))
    if r1 is None:
        if o0 == {n1, n3}: cls["pre"].append(k)
        continue
    o1 = set(r1.owners(b, 2))
    if o0 == {n1, n3} and o1 == {n1, n4}: cls["target"].append(k)
    elif o0 == {n1, n3} and o1 == {n1, n3}: cls["control"].append(k)
for c, ks in cls.items():
    print(c, *ks)
