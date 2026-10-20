"""member_read.py <out-dir> <host:port,...> <R>

Reads every key member_workload.py judged from EVERY live node with a raw g/G (a node that is not an
owner answers W), compares the owners' copies with what the acknowledged operations imply, and groups
the verdicts by (command class, offset bucket). The offset is the start of the sequence relative to the
`go` mark. Output: <out-dir>/member_report.json and a table on stdout.

Per key, one of: ok | lost (absent where a value was expected) | wrong (another value) |
resurrected (present where absent was expected) | partial (owners disagree) | counter (wrong total) |
ttl_extended | ttl_shortened | ttl_spread | ttl_unreadable (no owner reported a TTL; a replica's W on the
increment-by-0 probe is expected, the primary alone answers it) | cas_bad | pia_bad.  Keys whose sequence raised are counted as
`err` and not judged.
"""
import base64
import collections
import json
import os
import socket
import sys
import time

out_dir = sys.argv[1]
nodes = [n.rsplit(":", 1) for n in sys.argv[2].split(",")]
R = int(sys.argv[3])
EDGES = [0, 5, 15, 30, 65]
LABELS = ["<0", "0-5", "5-15", "15-30", "30-65", ">=65"]


def bucket(off):
    for i, e in enumerate(EDGES):
        if off < e:
            return LABELS[i]
    return LABELS[-1]


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

    def get(self, ns, key):
        self.s.sendall(f"g {len(ns)} {len(key)}\n".encode() + ns + key)
        line = self._line()
        if line.startswith(b"V "):
            return ("V", self._bytes(int(line[2:])))
        return (line.decode(), None)

    def ttl(self, ns, key):
        """The key's remaining TTL as the node reports it on an increment by 0: an int, None when the node
        answered without one, or "W" when it declined because it is an owner but not the key's primary
        (i is the primary's alone: a replica answers W even though it holds the copy)."""
        self.s.sendall(f"i {len(ns)} {len(key)} 0\n".encode() + ns + key)
        line = self._line()
        if line.startswith(b"I "):
            p = line.split()
            self._bytes(int(p[1]))
            return int(p[2]) if len(p) > 2 else None
        if line == b"W":
            return "W"
        return None


ops = []
marks = {}
for line in open(os.path.join(out_dir, "ops.jsonl")):
    r = json.loads(line)
    if "mark" in r:
        marks[r["mark"]] = r
    else:
        ops.append(r)
go = marks["go"]["t"]
conns = {f"{h}:{p}": Conn(h, p) for h, p in nodes}
now = time.time()

table = collections.defaultdict(lambda: collections.Counter())
examples = collections.defaultdict(list)
owner_counts = collections.Counter()
bad_by_class = collections.defaultdict(list)  # class -> offsets of bad sequences

for r in ops:
    cls = r["cls"] + ("@ns" if r["ns"] and r["cls"] != "clear" else "")
    if r["cls"] == "clear":
        cls = "clear"
    off = r["t0"] - go
    cell = (cls, bucket(off))
    if r["status"] != "ok":
        table[cell]["err"] += 1
        continue
    if not r["exp"]:
        table[cell]["skipped"] += 1
        continue
    bad = None
    for e in r["exp"]:
        key = base64.b64decode(e["key"])
        ns = e.get("nsraw", r["ns"]).encode()
        k = e["k"]
        if k in ("cas_bad", "pia_bad"):
            bad = (k, e)
            break
        got = {}
        for name, c in conns.items():
            st, val = c.get(ns, key)
            if st != "W":
                got[name] = (st, val)
        owner_counts[len(got)] += 1
        vals = {(st, val) for st, val in got.values()}
        if k == "absent_after" and now < e["tw"] + e["ttl"] + 3:
            continue  # not expired yet when we read: nothing to judge
        if k == "absent" or k == "absent_after":
            if any(st == "V" for st, _ in got.values()):
                bad = ("resurrected", {"owners": {n: (st, (v or b"")[:12].decode("latin-1")) for n, (st, v) in got.items()}})
                break
        elif k in ("val", "ttl"):
            want = base64.b64decode(e["v"])
            present = {n for n, (st, v) in got.items() if st == "V"}
            if not present:
                bad = ("lost", {"want": want.decode("latin-1")})
                break
            if any(st == "V" and v != want for st, v in got.values()):
                kind = "partial" if any(st == "V" and v == want for st, v in got.values()) or len(vals) > 1 else "wrong"
                bad = (kind, {"want": want.decode("latin-1"), "owners": {n: (st, (v or b"")[:12].decode("latin-1")) for n, (st, v) in got.items()}})
                break
            if len(present) < len(got):
                bad = ("partial", {"want": want.decode("latin-1"), "owners": {n: (st, (v or b"")[:12].decode("latin-1")) for n, (st, v) in got.items()}})
                break
            if k == "ttl":
                ttls = {n: conns[n].ttl(ns, key) for n in present}
                vs = [t for t in ttls.values() if isinstance(t, int)]
                expect = e["ttl"] - (time.time() - e["tw"])
                if not vs or any(t is None for t in ttls.values()):  # a replica's W is expected; no TTL at all is not
                    bad = ("ttl_unreadable", {"ttls": ttls})
                    break
                if max(vs) - min(vs) > 2:
                    bad = ("ttl_spread", {"ttls": ttls})
                    break
                if max(vs) > expect + 2:
                    bad = ("ttl_extended", {"ttls": ttls, "expect": round(expect)})
                    break
                if min(vs) < expect - 2 - (r["t1"] - r["t0"]):  # the node stored the TTL when it got the write, up to the whole call earlier than the ack
                    bad = ("ttl_shortened", {"ttls": ttls, "expect": round(expect)})
                    break
        elif k == "ctr":
            want = str(e["n"]).encode()
            present = {n: v for n, (st, v) in got.items() if st == "V"}
            if not present:
                bad = ("lost", {"want": e["n"]})
                break
            if any(v != want for v in present.values()) or len(present) < len(got):
                bad = ("counter", {"want": e["n"], "owners": {n: (st, (v or b"").decode("latin-1")) for n, (st, v) in got.items()}, "results": r.get("results")})
                break
    if bad:
        table[cell][bad[0]] += 1
        bad_by_class[cls].append(off)
        if len(examples[(cls, bad[0])]) < 2:
            examples[(cls, bad[0])].append({"off": round(off, 1), "key": base64.b64decode(r["key"]).decode("latin-1")[-30:] if r["key"] else "", **{"detail": bad[1]}})
    else:
        table[cell]["ok"] += 1

report = {"R": R, "nodes": [":".join(n) for n in nodes], "go": go, "marks": {k: v.get("t", None) for k, v in marks.items()}, "owner_counts": dict(owner_counts), "stats": marks.get("stats", {}).get("stats"),
          "cells": {f"{c}|{b}": dict(v) for (c, b), v in sorted(table.items())},
          "last_bad_offset": {c: round(max(v), 1) for c, v in bad_by_class.items()},
          "first_bad_offset": {c: round(min(v), 1) for c, v in bad_by_class.items()},
          "examples": {f"{c}|{k}": v for (c, k), v in examples.items()}}
json.dump(report, open(os.path.join(out_dir, "member_report.json"), "w"), indent=1)

classes = sorted({c for c, _ in table})
print(f"R={R} nodes={len(nodes)} owners/key={dict(owner_counts)} marks={ {k: round(v.get('t', 0) - go, 1) for k, v in marks.items() if 't' in v} }")
print(f"{'class':12}" + "".join(f"{l:>14}" for l in LABELS))
for c in classes:
    row = ""
    for l in LABELS:
        t = table.get((c, l))
        if not t:
            row += f"{'-':>14}"
            continue
        n = sum(v for k, v in t.items() if k not in ("err", "skipped"))
        b = n - t.get("ok", 0)
        row += f"{(str(b) + '/' + str(n)) + (' e' + str(t['err']) if t.get('err') else ''):>14}"
    print(f"{c:12}{row}")
print("bad/judged per bucket (e = sequences that raised, not judged)")
for (c, k), v in sorted(examples.items()):
    for ex in v:
        print(f"  e.g. {c} {k}:", json.dumps(ex)[:300])
print("first/last bad offset (s after go):", {c: (report["first_bad_offset"][c], report["last_bad_offset"][c]) for c in report["last_bad_offset"]})
print("client stats:", report["stats"])
