"""delcheck.py --state <delload state file> --nodes host:port,host:port,...

Per-owner raw check of delload.py's keys, read from EVERY live node with a
plain `G` (a node that owns no copy answers `W`, so the owners identify
themselves):

  dx  a key whose delete was acknowledged is absent on every node: a `V`
      from any node is a lingering copy (dx_lingering_keys / _copies).
  dr  a key whose re-creation was acknowledged: every owner holds
      "<i>:2". An owner answering `N` is a missing copy
      (dr_missing_on_owner), an owner holding "<i>:1" is the deleted state
      back (dr_stale_on_owner), any other value is dr_wrong_value.

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

def read_all(key):
    copies = {}
    for n, c in conns.items():
        try: copies[n] = c.get(key)
        except Exception as e: copies[n] = ("ERR:" + type(e).__name__, None)
    return copies

for i in range(st["n"]):
    key = f"dx{i}".encode()
    if st["x_del_ack"][i] == 0.0: bump("x_unknown"); continue
    present = {}
    for n, r in read_all(key).items():
        if r[0] == "V": present[n] = r[1][:16].decode("latin-1")
        elif r[0] not in ("N", "W"): bump("x_other_reply"); ex("x_other_reply", key.decode(), n, r[0])
    if present: bump("x_lingering_keys"); bump("x_lingering_copies", len(present)); ex("x_lingering", key.decode(), present)
    else: bump("x_absent_everywhere")
for i in range(st["n"]):
    key = f"dr{i}".encode(); want = f"{i}:2".encode(); old = f"{i}:1".encode()
    if st["r_set_ack"][i] == 0.0: bump("r_unknown"); continue
    owners = {n: r for n, r in read_all(key).items() if r[0] != "W"}
    oc = out["owner_counts"]; oc[str(len(owners))] = oc.get(str(len(owners)), 0) + 1
    if not owners: bump("dr_no_owner"); ex("dr_no_owner", key.decode()); continue
    missing = [n for n, r in owners.items() if r[0] == "N"]
    stale = [n for n, r in owners.items() if r[0] == "V" and r[1] == old]
    wrong = [n for n, r in owners.items() if r[0] == "V" and r[1] not in (want, old)]
    other = [n for n, r in owners.items() if r[0] not in ("N", "V")]
    if missing: bump("dr_missing_on_owner", len(missing)); bump("dr_keys_with_missing_copy"); ex("dr_missing_on_owner", key.decode(), missing, {n: r[0] for n, r in owners.items()})
    if stale: bump("dr_stale_on_owner", len(stale)); bump("dr_keys_with_stale_copy"); ex("dr_stale_on_owner", key.decode(), stale)
    if wrong: bump("dr_wrong_value", len(wrong)); ex("dr_wrong_value", key.decode(), wrong)
    if other: bump("dr_other_reply", len(other)); ex("dr_other_reply", key.decode(), {n: owners[n][0] for n in other})
    if not (missing or stale or wrong or other): bump("dr_all_owners_ok")
print(json.dumps(out), flush=True)
