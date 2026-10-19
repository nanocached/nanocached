"""Conformance driver for the Python SDK: driver.py <cases.json> <phase> <path> <topology>

Reads the case file from make_cases.py and prints one JSON line per step.
phase  main  runs every case, then writes this SDK's cross-SDK keys
       read  reads every SDK's cross-SDK keys back
Connection comes from the same environment as tools/aws-harness:
NANOTEST_ADDRESSES, NANOTEST_VIA_PROXY, NANOTEST_SECRET.
"""

import asyncio
import base64
import hashlib
import json
import os
import sys
import time

from nanocached import NanocachedClient

SDK = "python"
ALL_SDKS = ["python", "typescript", "go", "java", "dotnet", "rust"]
STEP_TIMEOUT = 10.0


def materialise(spec):
    if "text" in spec:
        return spec["text"].encode()
    if "b64" in spec:
        return base64.b64decode(spec["b64"])
    return bytes(i % 251 for i in range(spec["len"]))


def addresses():
    out = []
    for part in os.environ["NANOTEST_ADDRESSES"].split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    return out


def emit(record, meta):
    record.update(meta)
    print(json.dumps(record, separators=(",", ":")), flush=True)


def ttl_arg(ttl):
    if ttl is None:
        return 0
    if isinstance(ttl, str):
        return int(ttl)
    return ttl


REGS = {}


def gen_value(i, length):
    return bytes((i + j) % 251 for j in range(length))


def target(client, step):
    ns = step.get("ns")
    if ns is None:
        return client
    return client.namespace(b"" if ns in ("", "=") else base64.b64decode(ns))


def key_bytes(b64):
    return b"" if b64 in ("", "=") else base64.b64decode(b64)


def token_of(step):
    reg = step["reg"]
    return reg[4:] if reg.startswith("lit:") else REGS[reg]


def many_fields(found):
    """found: {key(bytes|str): bytes} -> sorted (key, sha) pairs and an aggregate checksum."""
    pairs = sorted((k if isinstance(k, bytes) else k.encode(), hashlib.sha256(v).hexdigest()) for k, v in found.items())
    agg = hashlib.sha256()
    for k, h in pairs:
        agg.update(k + b"\0" + h.encode())
    return pairs, agg.hexdigest()


def hit(value):
    if value is None:
        return {"outcome": "ok", "hit": False}
    return {"outcome": "ok", "hit": True, "len": len(value), "sha256": hashlib.sha256(value).hexdigest()}


async def one(client, step):
    """Run one step; return the observed fields (never raises)."""
    op = step["op"]
    if op == "sleep":
        await asyncio.sleep(step["ms"] / 1000)
        return {"outcome": "ok"}
    if op == "probe":
        key = b"probe:" + os.urandom(6).hex().encode()
        await client.set(key, b"p")
        got = await client.get_bytes(key)
        return {"outcome": "ok", "usable": got == b"p"}
    if op == "flush":
        await client.clear_all()
        return {"outcome": "ok"}
    t = target(client, step)
    if op == "clear":
        await t.clear()
        return {"outcome": "ok"}
    key = key_bytes(step["key"]["b64"]) if "key" in step else None
    ttl = ttl_arg(step.get("ttl"))
    if op == "set":
        await t.set(key, materialise(step["value"]), ttl_seconds=ttl)
        return {"outcome": "ok"}
    if op == "get":
        return hit(await t.get_bytes(key))
    if op == "get_text":
        value = await t.get(key)
        return hit(value.encode() if value is not None else None)
    if op == "delete":
        return {"outcome": "ok", "deleted": bool(await t.delete(key))}
    if op in ("incr", "decr"):
        value = await (t.incr if op == "incr" else t.decr)(key, step["delta"])
        return {"outcome": "ok", "miss": True} if value is None else {"outcome": "ok", "n": value}
    if op == "incr_par":
        async def worker():
            for _ in range(step["m"]):
                await t.incr(key, 1)
        await asyncio.gather(*[worker() for _ in range(step["w"])])
        return {"outcome": "ok", "n": await t.incr(key, 0)}
    if op == "cas_get":
        got = await t.get_with_token(key)
        if got is None:
            return {"outcome": "ok", "hit": False}
        REGS[step["reg"]] = got[1]
        return {**hit(got[0]), "token": got[1]}
    if op == "cas_replace":
        return {"outcome": "ok", "result": bool(await t.replace(key, token_of(step), materialise(step["value"]), ttl_seconds=ttl))}
    if op == "cas_delete":
        return {"outcome": "ok", "result": bool(await t.delete_if_matches(key, token_of(step)))}
    if op == "put_if_absent":
        return {"outcome": "ok", "result": bool(await t.put_if_absent(key, materialise(step["value"]), ttl_seconds=ttl))}
    if op == "replace_if_present":
        return {"outcome": "ok", "result": bool(await t.replace_if_present(key, materialise(step["value"]), ttl_seconds=ttl))}
    if op == "cas_race":
        got = await t.get_with_token(key)
        results = await asyncio.gather(*[t.replace(key, got[1], f"w{i}".encode()) for i in range(step["w"])])
        return {"outcome": "ok", "n": sum(1 for r in results if r)}
    if op in ("set_par", "setdel_par"):
        async def writer(i):
            for j in range(step["m"]):
                if op == "setdel_par" and i % 2 == 1:
                    await t.delete(key)
                else:
                    await t.set(key, f"{SDK}-{i}-{j}".encode(), ttl_seconds=ttl)
        await asyncio.gather(*[writer(i) for i in range(step["w"])])
        return {"outcome": "ok"}
    if op == "many_set":
        await t.set_many({key_bytes(k): materialise(v) for k, v in step["kv"]}, ttl_seconds=ttl)
        return {"outcome": "ok"}
    if op == "many_get":
        found = await t.get_many_bytes([key_bytes(k) for k in step["keys"]])
        pairs, _ = many_fields(found)
        return {"outcome": "ok", "items": [[base64.b64encode(k).decode(), h] for k, h in pairs]}
    if op == "many_set_gen":
        prefix = base64.b64decode(step["prefix"])
        await t.set_many({prefix + str(i).encode(): gen_value(i, step["vlen"]) for i in range(step["n"])}, ttl_seconds=ttl)
        return {"outcome": "ok"}
    if op == "many_get_gen":
        prefix = base64.b64decode(step["prefix"])
        found = await t.get_many_bytes([prefix + str(i).encode() for i in range(step["n"])])
        pairs, checksum = many_fields(found)
        return {"outcome": "ok", "count": len(pairs), "checksum": checksum}
    raise ValueError(op)


async def timed(client, step):
    t0 = time.monotonic()
    try:
        if step["op"] == "sleep":
            result = await one(client, step)
        else:
            result = await asyncio.wait_for(one(client, step), STEP_TIMEOUT)
    except asyncio.TimeoutError:
        result = {"outcome": "timeout"}
    except Exception as error:  # noqa: BLE001 — the point is to record every failure
        result = {"outcome": "error", "error_class": type(error).__name__, "error_message": str(error)[:300]}
    result["ms"] = int((time.monotonic() - t0) * 1000)
    return result


async def run_cases(client, doc, meta):
    for case in doc["cases"]:
        for index, step in enumerate(case["steps"]):
            result = await timed(client, step)
            emit({"case": case["id"], "step": index, "op": step["op"], **result}, meta)
            if result["outcome"] == "timeout":
                break  # a hung call: do not stack more on this case


async def write_xsdk(client, doc, meta):
    for item in doc["xsdk"]:
        step = {"op": "set", "key": {"b64": base64.b64encode(item["key_template"].replace("{sdk}", SDK).encode()).decode()},
                "value": item["value"], "ttl": item["ttl"]}
        emit({"case": "xsdk.write." + item["name"], "step": 0, "op": "set", **await timed(client, step)}, meta)


async def read_xsdk(client, doc, meta):
    for writer in ALL_SDKS:
        for item in doc["xsdk"]:
            key = item["key_template"].replace("{sdk}", writer).encode()
            step = {"op": "get", "key": {"b64": base64.b64encode(key).decode()}}
            emit({"case": f"xsdk.read.{writer}.{item['name']}", "step": 0, "op": "get", **await timed(client, step)}, meta)


def client_options():
    """NANOTEST_OPTS: comma list of ff (fire-and-forget replica writes), rr (read repair), hedge=<ms>."""
    out = {}
    for item in filter(None, os.environ.get("NANOTEST_OPTS", "").split(",")):
        if item == "ff":
            out["fire_and_forget_replicas"] = True
        elif item == "rr":
            out["read_repair"] = True
        elif item.startswith("hedge="):
            out["read_hedge_after"] = int(item[6:]) / 1000
    return out


def stats_line(client, label):
    st = client.stats()
    emit({"case": label, "step": 0, "op": "stats", "outcome": "ok", **{k: getattr(st, k) for k in ("replica_write_failures", "read_repair_failures", "refresh_failures", "transient_retries")}}, {"sdk": SDK})


async def main():
    cases_path, phase, path, topology = sys.argv[1:5]
    doc = json.load(open(cases_path))
    meta = {"sdk": SDK, "path": path, "topology": topology}
    client = await asyncio.wait_for(
        NanocachedClient.connect(
            addresses(),
            auth_secret=os.environ.get("NANOTEST_SECRET") or None,
            via_proxy=os.environ.get("NANOTEST_VIA_PROXY") == "1",
            **client_options(),
        ),
        STEP_TIMEOUT,
    )
    try:
        if phase == "main":
            await run_cases(client, doc, meta)
            await write_xsdk(client, doc, meta)
        else:
            await read_xsdk(client, doc, meta)
    finally:
        if phase == "main":
            stats_line(client, "_stats_before_close")
        await asyncio.wait_for(client.close(), STEP_TIMEOUT)
        if phase == "main":
            try:
                stats_line(client, "_stats")
            except Exception as error:  # noqa: BLE001
                emit({"case": "_stats", "step": 0, "op": "stats", "outcome": "error", "error_message": str(error)[:200]}, {"sdk": SDK})


asyncio.run(main())
