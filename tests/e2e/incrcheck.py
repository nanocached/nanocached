"""incrcheck.py --state <incrload state file> --nodes host:port,host:port,...

Per-owner raw check of incrload.py's keys, read from EVERY live node with a
plain `G` (a node that owns no copy answers `W`, so the owners identify
themselves). For every key that had a turn (ia parallel counters, is serial
counters):

  an owner answering `N` is i?_missing_on_owner, one holding something that
  is not a decimal integer i?_other_reply, one above acknowledged + maybe
  i?_over_on_owner (an increment applied twice or one nobody sent). A key
  with no owner at all is i?_no_owner.

  An owner below the acknowledged count (i?_short_on_owner, with the missing
  amount in i?_lost_min_on_owner) and owners that disagree with each other
  (i?_owners_differ) are printed by the caller, not asserted: a lagging
  forward rewinds a counter, accepted behaviour. The primary is the one
  owner that answers a delta-0 `i` (the others answer `W`): a short copy on
  the primary is i?_short_on_primary, one only on a replica
  i?_short_on_replica (the replica write of an older answer landed last).

Prints one JSON summary line (counts, first examples) and exits 0; the
caller decides what to assert. Every socket has a 5 s timeout.
"""
import argparse, json, socket

ap = argparse.ArgumentParser()
ap.add_argument("--state", required=True); ap.add_argument("--nodes", required=True)
ap.add_argument("--max-examples", type=int, default=20)
a = ap.parse_args()
st = json.load(open(a.state))
nodes = [n.rsplit(":", 1) for n in a.nodes.split(",")]


class Conn:
    def __init__(self, host, port):
        self.s = socket.create_connection((host, int(port)), timeout=5); self.buf = b""
    def _line(self):
        while b"\n" not in self.buf:
            d = self.s.recv(65536)
            if not d: raise EOFError("closed")
            self.buf += d
        line, _, self.buf = self.buf.partition(b"\n"); return line
    def _bytes(self, n):
        while len(self.buf) < n:
            d = self.s.recv(65536)
            if not d: raise EOFError("closed")
            self.buf += d
        data, self.buf = self.buf[:n], self.buf[n:]; return data
    def incr0(self, key):
        """A delta-0 `i`: `I <len> [<ttl>]` plus the value on the key's primary (the value is read off the wire), `W` anywhere else."""
        self.s.sendall(f"i 0 {len(key)} 0\n".encode() + key)
        line = self._line()
        if line.startswith(b"I "): self._bytes(int(line.split()[1]))
        return line.split()[0].decode()
    def get(self, key):
        self.s.sendall(f"G {len(key)}\n".encode() + key)
        line = self._line()
        if line.startswith(b"V "): return ("V", self._bytes(int(line.split()[1])))
        return (line.decode(), None)


conns = {f"{h}:{p}": Conn(h, p) for h, p in nodes}
out = dict(nodes=list(conns), n=st["n"], owner_counts={}, examples=[])
def ex(*row):
    if len(out["examples"]) < a.max_examples: out["examples"].append(row)
def bump(k, by=1): out[k] = out.get(k, 0) + by

def owners_of(key):
    owners = {}
    for n, c in conns.items():
        try: r = c.get(key)
        except Exception as e: r = ("ERR:" + type(e).__name__, None)
        if r[0] != "W": owners[n] = r
    oc = out["owner_counts"]; oc[str(len(owners))] = oc.get(str(len(owners)), 0) + 1
    return owners

for cls in "as":
    p = f"i{cls}_"
    for i in range(st["n"]):
        acked, maybe = st["acked"][cls][i], st["maybe"][cls][i]
        if not acked and not maybe: bump(p + "unturned"); continue
        key = f"i{cls}{i}".encode()
        owners = owners_of(key)
        if not owners: bump(p + "no_owner"); ex(p + "no_owner", key.decode()); continue
        bad, values = False, set()
        for n, r in owners.items():
            if r[0] == "N": bump(p + "missing_on_owner"); ex(p + "missing_on_owner", key.decode(), n, acked); bad = True; continue
            try: val = int(r[1]) if r[0] == "V" else None
            except ValueError: val = None
            if val is None: bump(p + "other_reply"); ex(p + "other_reply", key.decode(), n, r[0], r[1] and r[1][:16].decode("latin-1")); bad = True; continue
            values.add(val)
            if val > acked + maybe: bump(p + "over_on_owner"); ex(p + "over_on_owner", key.decode(), n, val, acked, maybe); bad = True
            if val < acked:
                bump(p + "short_on_owner"); bump(p + "lost_min_on_owner", acked - val); bad = True
                try: primary = conns[n].incr0(key) == "I"
                except Exception: primary = None
                bump(p + ("short_on_primary" if primary else "short_on_replica" if primary is False else "short_primary_unknown"))
                ex(p + "short_on_owner", key.decode(), n, val, acked, maybe, "primary" if primary else "replica")
        if len(values) > 1: bump(p + "owners_differ"); ex(p + "owners_differ", key.decode(), sorted(values))
        if not bad: bump(p + "ok_everywhere")

print(json.dumps(out), flush=True)
