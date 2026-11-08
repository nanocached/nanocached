"""Cross-check of the serial INCR counters against the node traces.

Run by chaos.sh after every node's log is saved. It needs a node built with
the `trace` cargo feature (src/trace.rs) and started with
NANOCACHED_TRACE_PREFIX covering the `is` keys; without that there are no
TRACE lines and it only says so.

Two checks, per serial key `is<i>` of incrload.py (incrstate.json):

1. Applications against acknowledgements. Every increment the cache
   applies writes one `TRACE incr key=... old=... new=... delta=...` line
   on the node that applied it (incrcheck.py's delta-0 probes are left
   out). The loader counts the increments it saw acknowledged and the ones
   that may or may not have been applied (timed out, connection lost after
   sending). More applications than acknowledged + maybe is an increment
   nobody asked for: a retried request applied twice, or a forwarded result
   applied as an increment. Fewer than acknowledged is printed only: it
   means a line was lost or an application path is not traced.

2. Value from nowhere. An increment's old value above every value any
   increment on any node had produced for that key up to that moment
   (plus a slack for the log timestamps, which dockerd assigns when it
   reads each container's stderr) is a value no increment produced: the
   extra increment a phantom answer needs, arriving by some other path.

The last line is a JSON summary; chaos.sh asserts over_applied and
value_from_nowhere. Above it, the full timeline of each offending key.
"""
import argparse
import collections
import datetime
import glob
import json
import re

ap = argparse.ArgumentParser()
ap.add_argument("--state", required=True, help="incrstate.json written by incrload.py")
ap.add_argument("--logs", required=True, help="directory with the saved node logs")
ap.add_argument("--nodes", required=True, help="node log name prefix, e.g. nch-n (matches nch-n*.log)")
ap.add_argument("--slack", type=float, default=1.0, help="seconds of timestamp slack for the value-from-nowhere check")
ap.add_argument("--show", nargs="*", default=[], help="keys whose timeline is printed regardless")
a = ap.parse_args()

TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z?$")


def parse_ts(s):
    m = TS.match(s)
    if not m:
        return None
    t = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    frac = (m.group(2) or "0")[:6].ljust(6, "0")
    return t.timestamp() + int(frac) / 1e6


state = json.load(open(a.state))
n_keys = state["n"]
pat = re.compile(r"^(\S+) TRACE (\w+) key=(is\d+) (.*)$")
events = collections.defaultdict(list)
trace_lines = 0
for f in sorted(glob.glob(f"{a.logs}/{a.nodes}*.log")):
    node = f.rsplit("/", 1)[1][:-4]
    for line in open(f, errors="replace"):
        m = pat.search(line)
        if not m:
            continue
        ts, kind, key, rest = m.groups()
        t = parse_ts(ts)
        if t is None:
            continue
        trace_lines += 1
        events[key].append((t, ts, node, kind, rest.strip()))

INCR = re.compile(r"old=(-?\d+) new=(-?\d+) delta=(-?\d+)")
apps = collections.Counter()
nodes = collections.defaultdict(collections.Counter)
nowhere = []
for key, evs in events.items():
    evs.sort()
    incrs = []
    for t, ts, node, kind, rest in evs:
        if kind != "incr":
            continue
        m = INCR.search(rest)
        if not m:
            continue
        old, new, delta = map(int, m.groups())
        if delta == 0:
            continue
        apps[key] += 1
        nodes[key][node] += 1
        incrs.append((t, ts, node, old, new))
    for i, (t, ts, node, old, new) in enumerate(incrs):
        # the largest value any other increment had produced by t + slack
        produced = [n2 for j, (t2, _, _, _, n2) in enumerate(incrs) if j != i and t2 <= t + a.slack]
        if produced and old > max(produced):
            nowhere.append({"key": key, "at": ts, "node": node, "old": old, "max_produced": max(produced)})

over, under, within = [], [], 0
for i in range(n_keys):
    k = f"is{i}"
    lo = state["acked"]["s"][i]
    hi = lo + state["maybe"]["s"][i]
    if apps[k] > hi:
        over.append({"key": k, "applications": apps[k], "acked": lo, "maybe": hi - lo, "nodes": dict(nodes[k])})
    elif apps[k] < lo:
        under.append({"key": k, "applications": apps[k], "acked": lo, "maybe": hi - lo, "nodes": dict(nodes[k])})
    else:
        within += 1

for row in over:
    print("OVER", json.dumps(row))
for row in under[:20]:
    print("UNDER", json.dumps(row))
for row in nowhere:
    print("NOWHERE", json.dumps(row))
show = set(a.show) | {r["key"] for r in over[:5]} | {r["key"] for r in nowhere[:5]}
for key in sorted(show):
    print(f"=== timeline {key}")
    for _, ts, node, kind, rest in events[key]:
        print(f"  {ts} {node} {kind} {rest}")

print(json.dumps({
    "trace_lines": trace_lines,
    "keys": n_keys,
    "applications": sum(apps.values()),
    "acked": sum(state["acked"]["s"]),
    "maybe": sum(state["maybe"]["s"]),
    "over_applied": len(over),
    "under_applied": len(under),
    "within_bounds": within,
    "value_from_nowhere": len(nowhere),
    "over_keys": [r["key"] for r in over[:20]],
    "nowhere_keys": sorted({r["key"] for r in nowhere})[:20],
}))
