#!/usr/bin/env python3
"""report_scen.py <scenario-dir> [--md]: tabulate the JSONL a scenario run produced.

conn-style cases (v6): one row per case, one column per SDK: the first event that did not succeed
(connect / set / get), its error class, a shortened message and the time it took.
churn (v5): per SDK error window and counters. life (v7): the life events per test.
"""
import collections
import glob
import json
import os
import re
import sys

SDKS = ["python", "typescript", "go", "java", "dotnet", "rust"]
d = sys.argv[1]
by = collections.defaultdict(dict)  # case -> sdk -> [events]
for f in sorted(glob.glob(os.path.join(d, "*.jsonl"))):
    m = re.match(r"(.+)\.(python|typescript|go|java|dotnet|rust)\.jsonl$", os.path.basename(f))
    if not m:
        continue
    events = []
    for line in open(f):
        line = line.strip()
        if line.startswith("{"):
            try:
                events.append(json.loads(line))
            except ValueError:
                pass
    by[m.group(1)][m.group(2)] = events


def short(s, n=70):
    s = re.sub(r"\s+", " ", str(s or ""))
    return s if len(s) <= n else s[: n - 1] + "…"


def conn_cell(events):
    if not events:
        return "(no output)"
    for e in events:
        if e["event"] in ("connect", "set", "get") and e.get("outcome") != "ok":
            what = e.get("error_class") or e["outcome"]
            return f"{e['event']}: {what} — {short(e.get('error_message'))} [{e.get('ms', '')} ms]"
    return "ok" + (f" (get={events[-1].get('value')})" if any(e["event"] == "get_value" for e in events) else "")


out = []
conn_cases = [c for c in by if not c.startswith(("churn", "cycles", "abandon", "reconnect", "storm", "exit_noclose", "close_inflight", "after_close", "double_close", "failover"))]
if conn_cases:
    out.append("## Connection / status cases (first non-ok event)\n")
    out.append("| case | " + " | ".join(SDKS) + " |")
    out.append("|---|" + "---|" * len(SDKS))
    for c in conn_cases:
        out.append(f"| {c} | " + " | ".join(conn_cell(by[c].get(s)).replace("|", "/") for s in SDKS) + " |")
    out.append("")

for c in sorted(k for k in by if k.startswith("churn")):
    out.append(f"## {c}\n")
    out.append("| sdk | ops | errors | first error (ms) | last error (ms) | stale | miss | corrupt | error classes |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for s in SDKS:
        sm = next((e for e in by[c].get(s, []) if e["event"] == "summary"), None)
        out.append(f"| {s} | " + (" | ".join(str(sm.get(k)) for k in ("ops", "errors", "first_err_ms", "last_err_ms", "stale", "miss", "corrupt")) + f" | {sm.get('classes')} |" if sm else "(no summary) |" + " |" * 7))
    out.append("")

for c in ["close_inflight", "after_close", "double_close", "abandon", "cycles", "reconnect", "storm", "exit_noclose"]:
    if c not in by:
        continue
    out.append(f"## life: {c}\n")
    for s in SDKS:
        ev = by[c].get(s, [])
        lines = []
        for e in ev:
            keep = {k: v for k, v in e.items() if k not in ("sdk", "t_ms")}
            if "error_message" in keep:
                keep["error_message"] = short(keep["error_message"], 90)
            lines.append(json.dumps(keep, ensure_ascii=False, separators=(",", ":")))
        out.append(f"- **{s}**: " + (" ; ".join(lines) if lines else "(no output)"))
    out.append("")

# secret hygiene
leaks = os.path.join(d, "secret_leaks.txt")
if os.path.exists(leaks):
    out.append("## Secret hygiene\n")
    out.append("Files containing the secret: " + (open(leaks).read().strip() or "none") + "\n")

text = "\n".join(out)
open(os.path.join(d, "report.md"), "w").write(text)
print(text)
