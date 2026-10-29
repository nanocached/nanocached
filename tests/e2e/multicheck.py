"""multicheck.py --state <multiload state file> --nodes host:port,host:port,...

Per-owner raw check of multiload.py's keys (md<i> in the default
namespace, mn<i> in namespace "mns"), read from EVERY live node with a
plain `g` (a node that owns no copy answers `W`, so the owners identify
themselves). Per class (m?_):

  no_owner          no node answers for the key
  missing_on_owner  an owner answers `N`
  other_reply       an owner answers anything but V, N
  corrupt_on_owner  the value is not "<i>:<g>" of this key
  future_on_owner   the generation is above the last attempted one
  stale_on_owner    below the last acknowledged one (stale_keys: keys with
                    such a copy; stale_on_primary / stale_on_replica: which
                    kind of owner held it, a delta-0 `i` tells them apart)
  owners_differ     the owners hold different generations

no_owner, missing_on_owner, other_reply, corrupt_on_owner and
future_on_owner are the caller's to assert, the rest are accepted
behaviour (a lagging forward). Prints one JSON summary line and exits 0.
Every socket has a 5 s timeout.
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
    def probe_primary(self, ns, key):
        """True when this node is the key's primary: a delta-0 `i` gets `T` (not an integer) there and `W` anywhere else."""
        self.s.sendall(f"i {len(ns)} {len(key)} 0\n".encode() + ns + key)
        return self._line().split()[0] != b"W"
    def get(self, ns, key):
        self.s.sendall(f"g {len(ns)} {len(key)}\n".encode() + ns + key)
        line = self._line()
        if line.startswith(b"V "): return ("V", self._bytes(int(line.split()[1])))
        return (line.decode(), None)


conns = {f"{h}:{p}": Conn(h, p) for h, p in nodes}
out = dict(nodes=list(conns), n=st["n"], owner_counts={}, examples=[])
def ex(*row):
    if len(out["examples"]) < a.max_examples: out["examples"].append(row)
def bump(k, by=1): out[k] = out.get(k, 0) + by

for cls, ns in (("d", b""), ("n", b"mns")):
    p = f"m{cls}_"
    for i in range(st["n"]):
        key = f"m{cls}{i}".encode(); ack, att = st["acked"][cls][i], st["attempted"][cls][i]
        owners = {}
        for n, c in conns.items():
            try: r = c.get(ns, key)
            except Exception as e: r = ("ERR:" + type(e).__name__, None)
            if r[0] != "W": owners[n] = r
        oc = out["owner_counts"]; oc[str(len(owners))] = oc.get(str(len(owners)), 0) + 1
        if not owners: bump(p + "no_owner"); ex("no_owner", key.decode()); continue
        gens, stale = set(), False
        c_of = conns
        for n, r in owners.items():
            if r[0] == "N": bump(p + "missing_on_owner"); ex("missing_on_owner", key.decode(), n, ack); continue
            if r[0] != "V": bump(p + "other_reply"); ex("other_reply", key.decode(), n, r[0]); continue
            try:
                ii, g = (int(x) for x in r[1].decode().split(":"))
                if ii != i: raise ValueError
            except Exception:
                bump(p + "corrupt_on_owner"); ex("corrupt_on_owner", key.decode(), n, r[1][:16].decode("latin-1")); continue
            gens.add(g)
            if g > att: bump(p + "future_on_owner"); ex("future_on_owner", key.decode(), n, g, att)
            elif g < ack:
                bump(p + "stale_on_owner"); stale = True
                try: primary = c_of[n].probe_primary(ns, key)
                except Exception: primary = None
                bump(p + ("stale_on_primary" if primary else "stale_on_replica" if primary is False else "stale_primary_unknown"))
                ex("stale_on_owner", key.decode(), n, g, ack, "primary" if primary else "replica")
        if stale: bump(p + "stale_keys")
        if len(gens) > 1: bump(p + "owners_differ"); ex("owners_differ", key.decode(), sorted(gens))
print(json.dumps(out), flush=True)
