"""ttlcheck.py --state <ttlload state file> --nodes host:port,host:port,...

Raw check of ttlload.py's keys on EVERY live node with a plain `G` (a node
that owns no copy answers `W`): a key whose last acknowledged set with a TTL
is older than its TTL plus the loader's margin must be absent on every node.
A `V` from any node is a lingering copy (ts_ / tp_lingering_keys / _copies):
it kept a longer TTL than the one set, or the older no-TTL value (tp) came
back. Keys not yet that old, or never acknowledged, are counted as not_yet /
unknown, not asserted.

Prints one JSON summary line and exits 0; the caller decides what to assert.
Every socket has a 5 s timeout.
"""
import argparse, json, socket, time

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
out = dict(nodes=list(conns), n=st["n"], examples=[])
def ex(*row):
    if len(out["examples"]) < a.max_examples: out["examples"].append(row)
def bump(k, by=1): out[k] = out.get(k, 0) + by

now = time.time()
for cls in ("s", "p"):
    acks = st["ack_" + cls]
    for i in range(st["n"]):
        ttl = st["ttl_long"] if i % 3 == 2 else st["ttl_short"]
        if acks[i] == 0.0: bump(f"t{cls}_unknown"); continue
        if now < acks[i] + ttl + st["margin"]: bump(f"t{cls}_not_yet"); continue
        key = f"t{cls}{i}".encode()
        present = {}
        for n, c in conns.items():
            try: r = c.get(key)
            except Exception as e: r = ("ERR:" + type(e).__name__, None)
            if r[0] == "V": present[n] = r[1][:16].decode("latin-1")
            elif r[0] not in ("N", "W"): bump("other_reply"); ex("other_reply", key.decode(), n, r[0])
        if present:
            bump(f"t{cls}_lingering_keys"); bump(f"t{cls}_lingering_copies", len(present))
            ex("lingering", key.decode(), round(now - acks[i] - ttl, 1), present)
        else:
            bump(f"t{cls}_absent_everywhere")
print(json.dumps(out), flush=True)
