"""cascheck.py --state <casload state file> --nodes host:port,host:port,...

Per-owner raw check of casload.py's keys, read from EVERY live node with a
plain `G` (a node that owns no copy answers `W`, so the owners identify
themselves):

  cc  chain: every owner holds a value that starts with "S;" and contains
      every tag whose replace was acknowledged, once. An owner answering
      `N` is cc_missing_on_owner, an owner without an acknowledged tag
      cc_lost_on_owner, a tag twice cc_dup_on_owner. cc_lost_on_owner and
      owners that disagree with each other (cc_owners_differ) are printed
      by the caller, not asserted: a lagging forward leaves the older
      value on an owner, accepted behaviour.
  cp  race: every owner holds one of the two contenders' values, the
      winner's when there is one. `N` is cp_missing_on_owner (when a
      contender wrote or found the key), another value cp_wrong_on_owner.

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
    def probe_primary(self, key):
        """True when this node is the key's primary: a delta-0 `i` gets `T` (not an integer) there and `W` anywhere else."""
        self.s.sendall(f"i 0 {len(key)} 0\n".encode() + key)
        return self._line().split()[0] != b"W"
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

for i in range(st["n"]):
    acked, maybe = st["acked"][i], st["maybe"][i]
    if not acked and not maybe: bump("cc_unturned"); continue
    key = f"cc{i}".encode()
    owners = owners_of(key)
    if not owners: bump("cc_no_owner"); ex("cc_no_owner", key.decode()); continue
    bad, values = False, set()
    for n, r in owners.items():
        if r[0] == "N": bump("cc_missing_on_owner"); ex("cc_missing_on_owner", key.decode(), n, acked); bad = True; continue
        if r[0] != "V": bump("cc_other_reply"); ex("cc_other_reply", key.decode(), n, r[0]); bad = True; continue
        values.add(r[1])
        parts = r[1].decode("latin-1").split(";")
        tags = [p for p in parts[1:] if p]
        gone = [t for t in acked if t not in tags]
        if parts[0] != "S": bump("cc_head_on_owner"); ex("cc_head_on_owner", key.decode(), n, r[1][:32].decode("latin-1")); bad = True
        if gone:
            bump("cc_lost_on_owner"); bad = True
            try: primary = conns[n].probe_primary(key)
            except Exception: primary = None
            bump("cc_lost_on_primary" if primary else "cc_lost_on_replica" if primary is False else "cc_lost_primary_unknown")
            ex("cc_lost_on_owner", key.decode(), n, gone, tags, "primary" if primary else "replica")
        if len(set(tags)) != len(tags): bump("cc_dup_on_owner"); ex("cc_dup_on_owner", key.decode(), n, tags); bad = True
    if len(values) > 1: bump("cc_owners_differ"); ex("cc_owners_differ", key.decode(), sorted(v.decode("latin-1") for v in values))
    if not bad: bump("cc_ok_everywhere")

for i in range(st["n"]):
    r = st["race"][i]
    if r is None: bump("cp_unturned"); continue
    key = f"cp{i}".encode()
    vals = [f"{i}:{n}".encode() for n in range(len(r))]
    wins = [n for n, x in enumerate(r) if x == "T"]
    wrote = bool(wins) or "F" in r      # a contender wrote the key, or found it there
    owners = owners_of(key)
    if not owners:
        if wrote: bump("cp_no_owner"); ex("cp_no_owner", key.decode(), r)
        continue
    bad = False
    for n, o in owners.items():
        if o[0] == "N":
            if wrote: bump("cp_missing_on_owner"); ex("cp_missing_on_owner", key.decode(), n, r); bad = True
        elif o[0] != "V": bump("cp_other_reply"); ex("cp_other_reply", key.decode(), n, o[0]); bad = True
        elif o[1] not in vals or (len(wins) == 1 and o[1] != vals[wins[0]]):
            bump("cp_wrong_on_owner"); ex("cp_wrong_on_owner", key.decode(), n, r, o[1][:16].decode("latin-1")); bad = True
    if not bad: bump("cp_ok_everywhere")
print(json.dumps(out), flush=True)
