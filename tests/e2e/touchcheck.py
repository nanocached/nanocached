"""touchcheck.py --state <touchload state file> --nodes host:port,host:port,...

Per-owner raw check of touchload.py's keys, read from EVERY live node with a
plain `G` (a node that owns no copy answers `W`, so the owners identify
themselves) and, for the keys expected present, the remaining TTL from the
primary with a delta-0 `i` (`I <len> [<ttl>]`; INCR is primary-only, so
exactly one owner answers `I` and the others `W`; it never changes a TTL):

  tk  every owner holds the value; the primary's remaining TTL is within
      [ttl - (now - last_ack) - 2, ttl]
  tn  (keys whose removal was acknowledged) every owner holds the value
      and the primary reports no TTL. A copy that missed the removal
      expired n_short seconds after the shortening, so a key is probative
      once that long has passed (n_not_yet_probative counts the rest).
  tx  a key whose acknowledged expiry has elapsed is absent on every node:
      a `V` from any node is a lingering copy.

Prints one JSON summary line (counts, first examples) and exits 0; the
caller decides what to assert. Every socket has a 5 s timeout.
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
    def incr0(self, key):
        self.s.sendall(f"i 0 {len(key)} 0\n".encode() + key)
        line = self._line()
        if line.startswith(b"I "):
            parts = line.split(); value = self._bytes(int(parts[1]))
            return ("I", value, int(parts[2]) if len(parts) > 2 else None)
        return (line.decode(), None, None)


conns = {f"{h}:{p}": Conn(h, p) for h, p in nodes}
out = dict(nodes=list(conns), n=st["n"], owner_counts={}, examples=[])
def ex(*row):
    if len(out["examples"]) < a.max_examples: out["examples"].append(row)
def bump(k, by=1): out[k] = out.get(k, 0) + by

def owners_of(key):
    copies = {}
    for n, c in conns.items():
        try: copies[n] = c.get(key)
        except Exception as e: copies[n] = ("ERR:" + type(e).__name__, None)
    return {n: v for n, v in copies.items() if v[0] != "W"}

def primary_ttl(key, owners):
    """(ttl_or_None, status) from the one owner that answers the delta-0 i; status counts I replies."""
    replies = {}
    for n in owners:
        try: replies[n] = conns[n].incr0(key)
        except Exception as e: replies[n] = ("ERR:" + type(e).__name__, None, None)
    i_replies = {n: r for n, r in replies.items() if r[0] == "I"}
    if len(i_replies) != 1:
        bump("primaries_not_one"); ex("primaries_not_one", key.decode(), {n: r[0] for n, r in replies.items()}); return None, False
    return next(iter(i_replies.values()))[2], True

now = time.time()
for cls in "kn":
    for i in range(st["n"]):
        key = f"t{cls}{i}".encode(); want = str(i).encode()
        if cls == "n":
            if st["none_ack"][i] == 0.0: bump("n_unknown"); continue
            if now - st["n_short_ack"][i] <= st["n_short"] + 2: bump("n_not_yet_probative"); continue
        owners = owners_of(key)
        oc = out["owner_counts"]; oc[str(len(owners))] = oc.get(str(len(owners)), 0) + 1
        if not owners: bump(f"{cls}_no_owner"); ex(f"{cls}_no_owner", key.decode()); continue
        missing = [n for n, v in owners.items() if v[0] == "N"]
        wrong = [n for n, v in owners.items() if v[0] == "V" and v[1] != want]
        other = [n for n, v in owners.items() if v[0] not in ("N", "V")]
        if missing: bump(f"{cls}_missing_on_owner", len(missing)); bump(f"{cls}_keys_with_missing_copy"); ex(f"{cls}_missing_on_owner", key.decode(), missing, {n: v[0] for n, v in owners.items()})
        if wrong: bump(f"{cls}_wrong_value", len(wrong)); ex(f"{cls}_wrong_value", key.decode(), wrong)
        if other: bump(f"{cls}_other_reply", len(other)); ex(f"{cls}_other_reply", key.decode(), {n: owners[n][0] for n in other})
        if missing or wrong or other: continue
        bump(f"{cls}_all_owners_ok")
        live = [n for n, v in owners.items() if v[0] == "V"]
        ttl, ok = primary_ttl(key, live)
        if not ok: continue
        if cls == "k":
            since = now - st["last_ack"][i]; lo = st["ttl"] - since - 2
            if ttl is None: bump("k_ttl_missing_on_primary"); ex("k_ttl_missing", key.decode())
            elif ttl > st["ttl"] or ttl < lo: bump("k_ttl_out_of_band"); ex("k_ttl_out_of_band", key.decode(), ttl, round(since, 1))
            else: bump("k_ttl_in_band")
        else:
            if ttl is not None: bump("n_ttl_not_removed_on_primary"); ex("n_ttl_not_removed", key.decode(), ttl)
            else: bump("n_ttl_removed_ok")
for i in range(st["n"]):
    key = f"tx{i}".encode(); acked = st["exp_ack"][i]
    if acked is None: bump("x_unknown"); continue
    t, ttl = acked
    if t + ttl + 2 > now: bump("x_not_yet"); continue
    present = {}
    for n, c in conns.items():
        try: r = c.get(key)
        except Exception as e: r = ("ERR:" + type(e).__name__, None)
        if r[0] == "V": present[n] = r[1][:16].decode("latin-1")
        elif r[0] not in ("N", "W"): bump("x_other_reply"); ex("x_other_reply", key.decode(), n, r[0])
    if present: bump("x_lingering_keys"); bump("x_lingering_copies", len(present)); ex("x_lingering", key.decode(), ttl, round(now - t), present)
    else: bump("x_absent_everywhere")
print(json.dumps(out), flush=True)
