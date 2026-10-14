"""Classify every MISS of a restart.sh run against the node/discovery logs.

usage: python3 analyze_restart.py tests/e2e/logs/rdiag-<run>
"""
import glob, json, os, re, sys, collections

d = sys.argv[1]
ts = re.compile(r"^\S*T(\d\d:\d\d:\d\d\.\d{3})\d*Z ")


def t_of(line):
    m = ts.match(line)
    return m.group(1) if m else None


ip = {}
for f in ("ipmap.txt", "ipmap2.txt"):
    p = os.path.join(d, f)
    if os.path.exists(p):
        for l in open(p):
            parts = l.split()
            if len(parts) == 2:
                ip.setdefault(parts[1], parts[0])

disc = open(os.path.join(d, "disc.log"), errors="replace").read().splitlines()
evicted = [(t_of(l), re.search(r"node evicted: (\S+)", l).group(1)) for l in disc if "node evicted" in l]
registered = [(t_of(l), re.search(r"node registered: (\S+) at (\S+) ", l).groups()) for l in disc if "node registered" in l]
promoted = [(t_of(l), re.search(r"join promoted: (\S+)", l).group(1)) for l in disc if "join promoted" in l]
print("evicted:", evicted)
print("registered:", registered[-1:])
print("promoted:", promoted[-1:])
dead_name = evicted[-1][1] if evicted else None
new_name = promoted[-1][1] if promoted else None
dead_addr = next((a for _, (n, a) in registered if n == dead_name), None)
new_addr = next((a for _, (n, a) in registered if n == new_name), None)
print("dead:", dead_name, dead_addr, "new:", new_name, new_addr)

node_events = []
for f in sorted(glob.glob(os.path.join(d, "nrd-n*.log"))):
    n = os.path.basename(f)[:-4]
    for l in open(f, errors="replace"):
        if any(k in l for k in ("migration started", "migration completed", "re-replication after", "membership updated", "joined the cluster", "listening", "confirmed by discovery", "evicted by discovery")):
            node_events.append((t_of(l), n, l.split("Z ", 1)[1].strip()[:110]))
node_events.sort()

load = open(os.path.join(d, "load.log"), errors="replace").read().splitlines()
misses, probes, rings, errs = [], [], [], []
for l in load:
    body = l.split("Z ", 1)[1] if "Z " in l else l
    if body.startswith("MISS "):
        _, t, js = body.split(" ", 2); misses.append((t, json.loads(js)))
    elif body.startswith("PROBE "):
        _, t, js = body.split(" ", 2); probes.append((t, json.loads(js)))
    elif body.startswith("RING "):
        _, t, js = body.split(" ", 2); rings.append((t, json.loads(js)))
    elif body.startswith("ERR "):
        _, t, js = body.split(" ", 2); errs.append((t, json.loads(js)))

print(f"\nmisses={len(misses)} probes={len(probes)} ring_refreshes={len(rings)} errs={len(errs)}")
if misses:
    print("first miss", misses[0][0], "last miss", misses[-1][0])
cls = collections.Counter()
by_sec = collections.Counter()
prim_names = collections.Counter()
for t, m in misses:
    o = m.get("owners") or []
    oa = m.get("owner_addrs") or []
    p = o[0] if o else None
    pa = oa[0] if oa else None
    if p == dead_name:
        c = "A:primary=dead-name(addr %s)" % pa
    elif p == new_name:
        c = "N:primary=new-joiner"
    else:
        c = "B:primary=survivor %s" % ip.get((pa or "").split(":")[0], pa)
    cls[c] += 1
    by_sec[t[:8]] += 1
    prim_names[(p or "")[:8] + " ring=%d" % m.get("ring_size", -1)] += 1
print("\nclassification:")
for k, v in cls.most_common():
    print(f"  {v:5d}  {k}")
print("\nmisses per second:")
for k in sorted(by_sec):
    print(f"  {k}  {by_sec[k]}")
print("\nprimary name / ring size:")
for k, v in prim_names.most_common(8):
    print(f"  {v:5d}  {k}")
print("\nprobe re-reads:", collections.Counter(p["reread"] for _, p in probes))
print("\nerrors:", collections.Counter(e["error"] for _, e in errs))

print("\nring refreshes around the window (members by name prefix -> addr):")
lo = misses[0][0][:8] if misses else "00:00:00"
for t, r in rings:
    if misses and not (lo <= t[:8] <= misses[-1][0][:8]):
        continue
    mem = r.get("members", {})
    print(" ", t, "client", r.get("client"), "new" if r.get("new") else "refresh", sorted(f"{k[:8]}@{v.split(':')[0].split('.')[-1]}" for k, v in mem.items()))

print("\nnode events in the window:")
for t, n, msg in node_events:
    if misses and (misses[0][0][:8] <= t[:8] <= misses[-1][0][:8] or abs(int(t[6:8]) - int(misses[0][0][6:8])) <= 2 and t[:5] == misses[0][0][:5]):
        print(" ", t, n, msg)
