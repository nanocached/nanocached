"""clearcheck.py --state <clearload state file> --nodes host:port,host:port,...

Per-owner raw check of clearload.py's keys, read from EVERY live node with
a plain `g` (a node that owns no copy answers `W`, so the owners identify
themselves):

  old  a key whose set was acknowledged before a clear that returned is
       absent on every node: a `V` from any node is a lingering copy
       (old_lingering_keys / _copies, cycles_old_lingering). Old keys are
       o<m>, new keys n<m>, in a namespace cn<j> of their own.
  new  a key whose set was acknowledged after the clear returned: every
       owner holds "n<j>:<m>". An owner answering `N` is
       new_missing_on_owner (new_keys_with_missing_copy), any other value
       new_wrong_value.
  ctl  the control keys (never cleared): every owner holds the written
       value; `N` is ctl_missing_on_owner, no owner at all ctl_no_owner.

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
    def get(self, ns, key):
        self.s.sendall(f"g {len(ns)} {len(key)}\n".encode() + ns + key)
        line = self._line()
        if line.startswith(b"V "): return ("V", self._bytes(int(line.split()[1])))
        return (line.decode(), None)


conns = {f"{h}:{p}": Conn(h, p) for h, p in nodes}
out = dict(nodes=list(conns), cycles=st["cycles"], owner_counts={}, examples=[])
def ex(*row):
    if len(out["examples"]) < a.max_examples: out["examples"].append(row)
def bump(k, by=1): out[k] = out.get(k, 0) + by

def read_all(ns, key):
    copies = {}
    for n, c in conns.items():
        try: copies[n] = c.get(ns, key)
        except Exception as e: copies[n] = ("ERR:" + type(e).__name__, None)
    return copies

def owners_of(ns, key):
    owners = {n: r for n, r in read_all(ns, key).items() if r[0] != "W"}
    oc = out["owner_counts"]; oc[str(len(owners))] = oc.get(str(len(owners)), 0) + 1
    return owners

for j, cy in enumerate(st["per_cycle"]):
    if cy["clear_ack"] == 0.0: bump("cycles_unknown"); continue
    ns = f"cn{j}".encode(); lingering_cycle = False
    for m in cy["old"]:
        key = f"o{m}".encode()
        present = {}
        for n, r in read_all(ns, key).items():
            if r[0] == "V": present[n] = r[1][:16].decode("latin-1")
            elif r[0] not in ("N", "W"): bump("old_other_reply"); ex("old_other_reply", ns.decode(), m, n, r[0])
        if present:
            bump("old_lingering_keys"); bump("old_lingering_copies", len(present)); ex("old_lingering", ns.decode(), m, present)
            lingering_cycle = True
    if lingering_cycle: bump("cycles_old_lingering")
    for m in cy["new"]:
        key = f"n{m}".encode(); want = f"n{j}:{m}".encode()
        owners = owners_of(ns, key)
        if not owners: bump("new_no_owner"); ex("new_no_owner", ns.decode(), m); continue
        missing = [n for n, r in owners.items() if r[0] == "N"]
        wrong = [n for n, r in owners.items() if r[0] == "V" and r[1] != want]
        other = [n for n, r in owners.items() if r[0] not in ("N", "V")]
        if missing: bump("new_missing_on_owner", len(missing)); bump("new_keys_with_missing_copy"); ex("new_missing_on_owner", ns.decode(), m, missing, {n: r[0] for n, r in owners.items()})
        if wrong: bump("new_wrong_value", len(wrong)); ex("new_wrong_value", ns.decode(), m, wrong)
        if other: bump("new_other_reply", len(other)); ex("new_other_reply", ns.decode(), m, {n: owners[n][0] for n in other})
        if not (missing or wrong or other): bump("new_all_owners_ok")

for ns, key_of, val_of in ((b"", lambda m: f"ckd{m}".encode(), lambda m: f"d{m}".encode()),
                           (b"ck", lambda m: f"k{m}".encode(), lambda m: f"k{m}".encode())):
    for m in range(st["control"]):
        owners = owners_of(ns, key_of(m)); want = val_of(m)
        if not owners: bump("ctl_no_owner"); ex("ctl_no_owner", ns.decode(), m); continue
        missing = [n for n, r in owners.items() if r[0] == "N"]
        wrong = [n for n, r in owners.items() if r[0] == "V" and r[1] != want]
        other = [n for n, r in owners.items() if r[0] not in ("N", "V")]
        if missing: bump("ctl_missing_on_owner", len(missing)); ex("ctl_missing_on_owner", ns.decode(), m, missing)
        if wrong: bump("ctl_wrong_value", len(wrong)); ex("ctl_wrong_value", ns.decode(), m, wrong)
        if other: bump("ctl_other_reply", len(other)); ex("ctl_other_reply", ns.decode(), m, {n: owners[n][0] for n in other})
        if not (missing or wrong or other): bump("ctl_all_owners_ok")
print(json.dumps(out), flush=True)
