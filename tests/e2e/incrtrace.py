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

Printed, not asserted:

3. Sequence jumps. Two consecutive applications (across nodes, by time)
   whose new values differ by 2 or more: between them a write that was
   not an increment (a handoff or a forward carrying a copy that was
   ahead) moved the key, or an increment went untraced. The loader's
   `is_phantom` is this seen from the client side, so each one is printed
   with every trace line of the key around it.
4. Unmatched answers. When the loader saved its answers (`hist_s` in
   incrstate.json), each answer is matched to an application with that new
   value within the match window; an answer no node applied points at the
   client side (a reply attributed to the wrong request). The window is
   wider than the loader's op timeout (15 s), the most a reply can trail
   its application and still be an answer: a CI cell had three answers
   (of 22,108) arrive 5.00 s after their application, during a kill,
   which a 5 s window counted as unmatched; the cause of that lag is not
   identified.

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
ap.add_argument("--match-window", type=float, default=20.0, help="seconds between a loader answer and the application it is matched to (above the loader's 15 s op timeout)")
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
jumps = []
incrs_by_key = {}
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
    incrs_by_key[key] = incrs
    for i, (t, ts, node, old, new) in enumerate(incrs):
        # the largest value any other increment had produced by t + slack
        produced = [n2 for j, (t2, _, _, _, n2) in enumerate(incrs) if j != i and t2 <= t + a.slack]
        if produced and old > max(produced):
            nowhere.append({"key": key, "at": ts, "node": node, "old": old, "max_produced": max(produced)})
        if i > 0 and new - incrs[i - 1][4] >= 2:
            jumps.append({"key": key, "from": {"at": incrs[i - 1][1], "node": incrs[i - 1][2], "new": incrs[i - 1][4]},
                          "to": {"at": ts, "node": node, "old": old, "new": new}})

# Loader answers (when saved) against the applications: every answer should
# be the new value of some application within the window.
unmatched = []
hist = state.get("hist_s")
if hist:
    for i, rows in enumerate(hist):
        key = f"is{i}"
        pool = [(t, new) for (t, _, _, _, new) in incrs_by_key.get(key, [])]
        used = set()
        for at, ans in rows:
            if not isinstance(ans, int):
                continue
            # the loader stamps HH:MM:SS.mmm of its own clock (same host as dockerd)
            try:
                hh, mm, rest = at.split(":"); ssec = float(rest)
                day_t = int(hh) * 3600 + int(mm) * 60 + ssec
            except ValueError:
                continue
            best = None
            for j, (t, new) in enumerate(pool):
                if j in used or new != ans:
                    continue
                dt = abs((t % 86400) - day_t)
                dt = min(dt, 86400 - dt)
                if dt <= a.match_window and (best is None or dt < best[0]):
                    best = (dt, j)
            if best is None:
                unmatched.append({"key": key, "at": at, "answer": ans})
            else:
                used.add(best[1])

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
for row in jumps[:20]:
    print("JUMP", json.dumps(row))
for row in unmatched[:20]:
    print("UNMATCHED", json.dumps(row))
show = set(a.show) | {r["key"] for r in over[:5]} | {r["key"] for r in nowhere[:5]} | {r["key"] for r in jumps[:5]} | {r["key"] for r in unmatched[:5]}
for key in sorted(show):
    print(f"=== timeline {key}")
    for _, ts, node, kind, rest in events[key]:
        print(f"  {ts} {node} {kind} {rest}")
    if hist:
        i = int(key[2:])
        print("  loader:", " ".join(f"{at}={ans}" for at, ans in hist[i]))

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
    "sequence_jumps": len(jumps),
    "answers_unmatched": len(unmatched) if hist else None,
    "over_keys": [r["key"] for r in over[:20]],
    "nowhere_keys": sorted({r["key"] for r in nowhere})[:20],
    "jump_keys": sorted({r["key"] for r in jumps})[:20],
}))
