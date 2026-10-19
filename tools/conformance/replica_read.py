"""replica_read.py <out-dir> <host:port,host:port,...> [--mix]

Reads every key a case file touched from EVERY node with a raw `G` (a node that is not an owner of the
key answers `W`, so the owners identify themselves) and compares the owners' copies:

  agree      all owners hold the same value (or all report the key absent)
  oracle     the copy equals what the writing SDK's last `get` returned (that read is served by the
             primary), or the value the case defines; skipped for --mix (several writers)
  ttl        for keys written with a TTL: the remaining TTL read from each owner with a delta-0 `i`
             (INCR never changes the TTL) differs by at most 1 s

Output: <out-dir>/replica_report.json and a one-screen summary on stdout. Nothing waits without a bound:
every socket has a 5 s timeout.
"""
import base64
import glob
import hashlib
import json
import os
import socket
import sys

out_dir = sys.argv[1]
nodes = [n.rsplit(":", 1) for n in sys.argv[2].split(",")]
mix = "--mix" in sys.argv
doc = json.load(open(os.path.join(out_dir, "cases.json")))


def gen_value(i, length):
    return bytes((i + j) % 251 for j in range(length))


class Conn:
    def __init__(self, host, port):
        self.s = socket.create_connection((host, int(port)), timeout=5)
        self.buf = b""

    def _line(self):
        while b"\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise EOFError("closed")
            self.buf += d
        line, _, self.buf = self.buf.partition(b"\n")
        return line

    def _bytes(self, n):
        while len(self.buf) < n:
            d = self.s.recv(65536)
            if not d:
                raise EOFError("closed")
            self.buf += d
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def get(self, key):
        self.s.sendall(f"G {len(key)}\n".encode() + key)
        line = self._line()
        if line.startswith(b"V "):
            return ("V", self._bytes(int(line[2:])))
        return (line.decode(), None)  # N or W (or something unexpected)

    def incr0(self, key):
        self.s.sendall(f"i 0 {len(key)} 0\n".encode() + key)
        line = self._line()
        if line.startswith(b"I "):
            parts = line.split()
            value = self._bytes(int(parts[1]))
            ttl = int(parts[2]) if len(parts) > 2 else None
            return ("I", value, ttl)
        return (line.decode(), None, None)


# --- the keys, and what each oracle says --------------------------------------------------------
keys = {}  # key bytes -> {"case":..., "ttl": bool}
final_get = {}  # key bytes -> sha or None (absent)
define = {}  # key bytes -> sha (value defined by the case)

records = {}
for f in glob.glob(os.path.join(out_dir, "*.main.jsonl")):
    for line in open(f):
        line = line.strip()
        if line.startswith("{"):
            r = json.loads(line)
            records[(r["sdk"], r["case"], r["step"])] = r

sdk_names = sorted({k[0] for k in records})
for case in doc["cases"]:
    ttl_case = case["id"].startswith("rep.ttl")
    for index, st in enumerate(case["steps"]):
        if st["op"] in ("many_set_gen", "many_get_gen"):
            prefix = base64.b64decode(st["prefix"])
            for i in range(st["n"]):
                k = prefix + str(i).encode()
                keys[k] = {"case": case["id"], "ttl": False}
                if st["op"] == "many_set_gen":
                    define[k] = hashlib.sha256(gen_value(i, st["vlen"])).hexdigest()
            continue
        if "key" not in st or st.get("ns") is not None:
            continue
        k = base64.b64decode(st["key"]["b64"])
        keys.setdefault(k, {"case": case["id"], "ttl": ttl_case})
        if st["op"] == "get" and not mix:
            for sdk in sdk_names:
                r = records.get((sdk, case["id"], index))
                if r is not None and r["outcome"] == "ok":
                    final_get[k] = r["sha256"] if r.get("hit") else None

report = {"cases": {}, "nodes": [":".join(n) for n in nodes], "sdks": sdk_names, "stats": {}}
for (sdk, case, step), r in records.items():
    if case.startswith("_stats"):
        report["stats"].setdefault(sdk, {})[case] = {k: v for k, v in r.items() if k.endswith("failures") or k.endswith("retries") or k.endswith("bugs")}

conns = [Conn(h, p) for h, p in nodes]
for k, meta in sorted(keys.items()):
    copies = {}
    for (h, p), c in zip(nodes, conns):
        status, value = c.get(k)
        copies[f"{h}:{p}"] = (status, value)
    owners = {n: v for n, v in copies.items() if v[0] != "W"}
    c = report["cases"].setdefault(meta["case"], {"keys": 0, "owner_counts": {}, "disagree": 0, "vs_oracle": 0, "oracle_checked": 0, "ttl_checked": 0, "ttl_spread_gt1": 0, "ttl_unreadable": 0, "examples": []})
    c["keys"] += 1
    c["owner_counts"][len(owners)] = c["owner_counts"].get(len(owners), 0) + 1
    shown = {n: ("absent" if v[0] == "N" else (hashlib.sha256(v[1]).hexdigest()[:8] + f"/{len(v[1])}B" if v[0] == "V" else v[0])) for n, v in owners.items()}
    distinct = {(v[0], hashlib.sha256(v[1]).hexdigest() if v[1] is not None else None) for v in owners.values()}
    if len(distinct) > 1:
        c["disagree"] += 1
        if len(c["examples"]) < 3:
            c["examples"].append({"key": k.decode("latin-1")[-40:], "owners": shown, "values": {n: (v[1][:24].decode("latin-1") if v[1] else None) for n, v in owners.items()}})
    expected = define.get(k, final_get.get(k, "unset"))
    if expected != "unset":
        c["oracle_checked"] += 1
        want = expected
        got_set = {(hashlib.sha256(v[1]).hexdigest() if v[0] == "V" else None) for v in owners.values()}
        if got_set != {want}:
            c["vs_oracle"] += 1
            if len(c["examples"]) < 3:
                c["examples"].append({"key": k.decode("latin-1")[-40:], "oracle": (want or "absent")[:8], "owners": shown})
    if meta["ttl"]:
        ttls = {}
        for (h, p), cc in zip(nodes, conns):
            if f"{h}:{p}" not in owners:
                continue
            status, value, ttl = cc.incr0(k)
            ttls[f"{h}:{p}"] = (status, ttl)
        vals = [t for s, t in ttls.values() if s == "I" and t is not None]
        if len(vals) < len(owners):
            c["ttl_unreadable"] += 1
            if len(c["examples"]) < 3:
                c["examples"].append({"key": k.decode("latin-1")[-40:], "ttl_replies": {n: list(v) for n, v in ttls.items()}})
        else:
            c["ttl_checked"] += 1
            if max(vals) - min(vals) > 1:
                c["ttl_spread_gt1"] += 1
                if len(c["examples"]) < 3:
                    c["examples"].append({"key": k.decode("latin-1")[-40:], "ttls": {n: v[1] for n, v in ttls.items()}})

json.dump(report, open(os.path.join(out_dir, "replica_report.json"), "w"), indent=1)
print(f"nodes={len(nodes)} sdks={','.join(sdk_names)} mix={mix}")
for case, c in report["cases"].items():
    print(f"  {case:18} keys={c['keys']:4} owners/key={c['owner_counts']} disagree={c['disagree']} vs_oracle={c['vs_oracle']}/{c['oracle_checked']} ttl: ok={c['ttl_checked'] - c['ttl_spread_gt1']} spread>1s={c['ttl_spread_gt1']} unreadable={c['ttl_unreadable']}")
    for e in c["examples"]:
        print("      e.g.", json.dumps(e)[:260])
for sdk, st in report["stats"].items():
    print(f"  stats[{sdk}]:", json.dumps(st.get("_stats") or st.get("_stats_before_close")))
