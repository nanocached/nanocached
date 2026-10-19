#!/usr/bin/env python3
"""compare.py <run-dir>: build the case × SDK matrix from the drivers' JSONL.

Two independent checks (a shared bug passes the second and fails the first):
  oracle     each step's observation against the step's ``expect``
  agreement  the SDKs against each other, step by step, on a normalised
             signature (outcome category + hit/len/sha/deleted/usable).
             Error classes and messages differ by language and are shown,
             not compared.

Cell:  PASS   matches the oracle and every other SDK
       FAIL   contradicts the oracle (expect)
       DIFF   differs from the other SDKs (an 'any' step, or all-pass on the
              oracle but a different signature)
       N/A    the driver said the argument cannot be expressed in the SDK
       --     no record (driver did not run / timed out earlier in the case)
Writes <run-dir>/report.md and prints the matrix.
"""

import collections
import glob
import json
import os
import sys

SDKS = ["python", "typescript", "go", "java", "dotnet", "rust"]


def load(run_dir):
    doc = json.load(open(os.path.join(run_dir, "cases.json")))
    records = collections.defaultdict(dict)  # (case, step) -> sdk -> record
    for path in glob.glob(os.path.join(run_dir, "*.jsonl")):
        for line in open(path):
            line = line.strip()
            if not line.startswith("{"):
                continue
            r = json.loads(line)
            records[(r["case"], r["step"])][r["sdk"]] = r
    return doc, records


def signature(r):
    if r["outcome"] == "na":
        return None
    sig = [r["outcome"] if r["outcome"] != "ok" else "ok"]
    for k in ("hit", "len", "sha256", "deleted", "usable", "n", "miss", "result", "token", "count", "checksum", "items"):
        if k in r:
            sig.append(f"{k}={r[k]}")
    return " ".join(str(s) for s in sig)


def oracle(expect, r):
    """None if the observation satisfies ``expect``, else a reason."""
    if expect == "any" or r["outcome"] == "na":
        return None
    if expect.get("error"):
        return None if r["outcome"] == "error" else f"expected an error, got {r['outcome']} {({k: v for k, v in r.items() if k in ('n', 'miss', 'result', 'hit')})}"
    if r["outcome"] != "ok":
        return f"{r['outcome']}: {r.get('error_class', '')} {r.get('error_message', '')}".strip()
    if "hit" in expect:
        if r.get("hit") != expect["hit"]:
            return f"hit={r.get('hit')} want {expect['hit']}"
        if expect["hit"] and "sha256" in expect and (r.get("sha256") != expect["sha256"] or r.get("len") != expect["len"]):
            return f"value differs: len={r.get('len')} want {expect['len']}, sha {str(r.get('sha256'))[:8]} want {expect['sha256'][:8]}"
        if expect["hit"] and "token" in expect and r.get("token") != expect["token"]:
            return f"token {r.get('token')} want {expect['token']}"
    if "deleted" in expect and r.get("deleted") != expect["deleted"]:
        return f"deleted={r.get('deleted')} want {expect['deleted']}"
    if "n" in expect and r.get("n") != expect["n"]:
        return f"n={r.get('n')} want {expect['n']}{' (miss)' if r.get('miss') else ''}"
    if expect.get("miss") and not r.get("miss"):
        return f"expected a miss, got {({k: v for k, v in r.items() if k in ('n', 'result')})}"
    if "result" in expect and r.get("result") != expect["result"]:
        return f"result={r.get('result')} want {expect['result']}"
    if "items" in expect:
        got = {k: v for k, v in r.get("items", [])}
        if got != expect["items"]:
            missing = [k for k in expect["items"] if k not in got]
            extra = [k for k in got if k not in expect["items"]]
            wrong = [k for k in got if k in expect["items"] and got[k] != expect["items"][k]]
            return f"items differ: {len(got)} returned, {len(expect['items'])} expected; missing {len(missing)}, extra {len(extra)}, wrong value {len(wrong)}"
    if "count" in expect and (r.get("count") != expect["count"] or r.get("checksum") != expect["checksum"]):
        return f"count={r.get('count')} want {expect['count']}; checksum {'ok' if r.get('checksum') == expect['checksum'] else 'differs'}"
    return None


def main(run_dir):
    doc, records = load(run_dir)
    sdks = [s for s in SDKS if any(s in v for v in records.values())]
    rows, details = [], []

    def cell_for(case_id, steps):
        """Per SDK: worst status over the case's steps."""
        status, why = {s: "PASS" for s in sdks}, {s: [] for s in sdks}
        rank = {"PASS": 0, "N/A": 1, "DIFF": 2, "--": 3, "FAIL": 4}
        for index, expect in steps:
            recs = records.get((case_id, index), {})
            sigs = {s: signature(recs[s]) for s in sdks if s in recs and recs[s]["outcome"] != "na"}
            counts = collections.Counter(sigs.values())
            majority = counts.most_common(1)[0][0] if counts else None
            for s in sdks:
                new, note = "PASS", None
                if s not in recs:
                    new, note = "--", f"step {index}: no record"
                elif recs[s]["outcome"] == "na":
                    new, note = "N/A", f"step {index}: {recs[s].get('error_message', 'not expressible')}"
                else:
                    bad = oracle(expect, recs[s])
                    if bad:
                        new, note = "FAIL", f"step {index} ({recs[s]['op']}): {bad}"
                    elif len(counts) > 1 and sigs[s] != majority:
                        new, note = "DIFF", f"step {index} ({recs[s]['op']}): {sigs[s]}  (majority: {majority})"
                if rank[new] > rank[status[s]]:
                    status[s] = new
                if note:
                    why[s].append(note)
        return status, why

    for case in doc["cases"]:
        steps = [(i, st.get("expect", {"ok": True})) for i, st in enumerate(case["steps"])]
        status, why = cell_for(case["id"], steps)
        rows.append((case["id"], status))
        for s in sdks:
            if status[s] != "PASS":
                details.append((case["id"], case["title"], s, status[s], why[s]))

    # cross-SDK: every SDK reads every writer's keys
    for item in doc["xsdk"]:
        status = {}
        for reader in sdks:
            ok, bad = True, []
            for writer in SDKS:
                r = records.get((f"xsdk.read.{writer}.{item['name']}", 0), {}).get(reader)
                if r is None:
                    ok, bad = False, bad + [f"{writer}: no record"]
                elif r["outcome"] != "ok" or not r.get("hit") or r.get("sha256") != item["expect_sha"] or r.get("len") != item["expect_len"]:
                    ok, bad = False, bad + [f"{writer}->{reader}: {r['outcome']} {r.get('error_message', '') or ('hit=' + str(r.get('hit')) + ' len=' + str(r.get('len')))}"]
            status[reader] = "PASS" if ok else "FAIL"
            if not ok:
                details.append((f"xsdk.{item['name']}", "written by each SDK, read by each SDK", reader, "FAIL", bad))
        rows.append((f"xsdk.{item['name']}", status))

    lines = [f"# Conformance matrix: {os.path.basename(run_dir)}", "", "```", open(os.path.join(run_dir, "env.txt")).read().strip(), "```", "",
             "| case | " + " | ".join(sdks) + " |", "|---|" + "---|" * len(sdks)]
    for case_id, status in rows:
        lines.append(f"| {case_id} | " + " | ".join(status.get(s, "--") for s in sdks) + " |")
    total = collections.Counter(v for _, status in rows for v in status.values())
    lines += ["", "cells: " + ", ".join(f"{k}={v}" for k, v in sorted(total.items())), "", "## Non-PASS cells", ""]
    for case_id, title, s, st, why in details:
        lines.append(f"- **{case_id}** / {s}: **{st}** — {title}")
        for w in why[:4]:
            lines.append(f"    - {w}")
    text = "\n".join(lines) + "\n"
    open(os.path.join(run_dir, "report.md"), "w").write(text)
    print(text)


main(sys.argv[1])
