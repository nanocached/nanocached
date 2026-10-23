#!/usr/bin/env python3
"""hostile_check.py <run-dir>: judge the hostile-server cases against the bound table.

The run is `scenarios.sh hostile`: every SDK's driver ran `host <case>` against fixtures/hostile_node.py and
wrote <case>.<sdk>.jsonl (events: connect, op, peak). The limits are fixtures/hostile_cases.json, written once:

  expect ok       the call returns the value the fixture sent (len) within max_wall_s
  expect error    the call fails within max_wall_s; a value, or no answer, is a failure
  expect settles  every one of the concurrent calls has returned (ok or failed) within max_wall_s
  max_peak_over_baseline_mib
                  the container's cgroup peak memory in the case minus the same SDK's `baseline` case; or, when
                  the driver reports `harness_floor_kb` (its own concurrency in place, no SDK call yet), minus that

Cell:  PASS   within every bound
       FAIL   outside one (the reasons are listed below the matrix)
       --     no record of the case for that SDK
Writes <run-dir>/hostile_report.md, prints the matrix, and exits 1 when any cell is not PASS.
"""
import collections
import json
import os
import sys

SDKS = ["python", "typescript", "go", "java", "dotnet", "rust"]
BOUNDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "hostile_cases.json")


def events(run_dir, case, sdk):
    path = os.path.join(run_dir, f"{case}.{sdk}.jsonl")
    out = collections.defaultdict(dict)
    if not os.path.exists(path):
        return None
    for line in open(path):
        line = line.strip()
        if line.startswith("{"):
            r = json.loads(line)
            out[r.get("event")] = r
    return out


def judge(case, ev, baseline_peak):
    """The reasons this SDK's run of the case is outside its bounds (empty when it is within them)."""
    op = ev.get("op")
    if op is None:
        conn = ev.get("connect", {})
        return [f"no op event (connect: {conn.get('outcome', 'none')} {conn.get('error_message', '')})".strip()]
    why = []
    outcome, ms, limit_ms = op.get("outcome"), op.get("ms", 0), case["max_wall_s"] * 1000
    if outcome == "timeout":
        why.append(f"no answer in the driver's own limit ({ms} ms > {limit_ms} ms)")
    else:
        if case["expect"] == "ok":
            if outcome != "ok":
                why.append(f"{outcome}: {op.get('error_class', '')} {op.get('error_message', '')}".strip())
            elif op.get("len") != case["len"]:
                why.append(f"len={op.get('len')} want {case['len']}")
        elif case["expect"] == "error":
            if outcome != "error":
                why.append(f"expected an error, got {outcome}" + (f" with a {op['len']}-byte value" if op.get("len") is not None else ""))
        elif case["expect"] == "settles" and outcome != "settled":
            why.append(f"{outcome}: {op.get('error_message', '')}".strip())
        if ms > limit_ms:
            why.append(f"took {ms} ms, bound {limit_ms} ms")
    bound = case.get("max_peak_over_baseline_mib")
    peak = ev.get("peak", {}).get("cgroup_peak_kb")
    # a driver whose own concurrency costs memory (a blocked call per platform thread) reports the peak with that
    # concurrency in place and no SDK call made; the case is then judged against it instead of the baseline run
    floor = ev.get("peak", {}).get("harness_floor_kb")
    reference, what = (floor, "the driver's own threads") if floor is not None else (baseline_peak, "the baseline")
    if bound is not None:
        if peak is None or reference is None:
            why.append("peak memory not measured (no cgroup memory.peak, or no baseline run)")
        elif (peak - reference) / 1024 > bound:
            why.append(f"peak {(peak - reference) / 1024:.0f} MiB over {what}, bound {bound} MiB")
    return why


def main(run_dir):
    cases = json.load(open(BOUNDS))["cases"]
    rows, details = [], []
    for case in cases:
        row = {}
        for sdk in SDKS:
            ev = events(run_dir, case["id"], sdk)
            if ev is None or not ev:
                row[sdk] = "--"
                details.append((case["id"], sdk, ["no record"]))
                continue
            base = (events(run_dir, "baseline", sdk) or {}).get("peak", {}).get("cgroup_peak_kb")
            why = judge(case, ev, base)
            row[sdk] = "FAIL" if why else "PASS"
            if why:
                details.append((case["id"], sdk, why))
        rows.append((case["id"], row))
    lines = [f"# Hostile-server cases: {os.path.basename(run_dir.rstrip('/'))}", "", "| case | " + " | ".join(SDKS) + " |", "|---|" + "---|" * len(SDKS)]
    lines += [f"| {cid} | " + " | ".join(row[s] for s in SDKS) + " |" for cid, row in rows]
    total = collections.Counter(v for _, row in rows for v in row.values())
    lines += ["", "cells: " + ", ".join(f"{k}={v}" for k, v in sorted(total.items())), "", "## Cells outside their bounds", ""]
    for cid, sdk, why in details:
        lines.append(f"- **{cid}** / {sdk}")
        lines += [f"    - {w}" for w in why]
    text = "\n".join(lines) + "\n"
    open(os.path.join(run_dir, "hostile_report.md"), "w").write(text)
    print(text)
    sys.exit(1 if details else 0)


main(sys.argv[1])
