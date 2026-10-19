"""member_workload.py <out-file>   (every writing command around a membership change)

Runs inside a container on the cluster's Docker network with the working-tree Python SDK. Stdin is the
control channel (the orchestrator writes a line when it changes the cluster):

    go      the first change happens now (offset 0)
    go2     a second change happens now

Sequence: preload -> READY on stdout -> workers run short serial sequences, one fresh key (or one
preloaded key) per sequence, until 75 s after `go`. Every sequence is one JSON line in <out-file> with
the time it started/finished and what the key must hold once everything has settled. A sequence that
raised is recorded as status=err and is not judged. Clear tasks run at fixed offsets.

    ops.jsonl   {cls, ns, key(b64), t0, t1, status, exp, results...}
    marks       {"mark": "go", "t": ...}
"""
import asyncio
import base64
import json
import os
import sys
import time

from nanocached import NanocachedClient

OUT = sys.argv[1]
RUN = os.environ.get("RUN", "r")
LOOP_AFTER_GO = float(os.environ.get("LOOP_AFTER_GO", "75"))
WORKERS = int(os.environ.get("WORKERS", "6"))
PRE = int(os.environ.get("PRELOAD", "400"))
NS = os.environ.get("NS_NAME", "nsA").encode()
OP_LIMIT = 20.0

PRELOADED = {"set", "delete", "delset", "incr", "incrpar", "cas", "casdel", "casseq"}
CLASSES = ["set", "settl", "expire", "multi", "delete", "setdel", "delset", "incr", "incrpar", "cas", "casdel", "pia", "casseq"]
NS_CLASSES = ["set", "delete", "incr", "cas", "pia", "settl"]
ALL = [(c, False) for c in CLASSES] + [(c, True) for c in NS_CLASSES]

out = open(OUT, "w", buffering=1)
state = {"go": None, "go2": None, "stop": False}


def b64(b):
    return base64.b64encode(b).decode()


def rec(**kw):
    out.write(json.dumps(kw, separators=(",", ":")) + "\n")


def keyname(cls, ns, tag):
    return f"m:{RUN}:{'n' if ns else 'd'}:{cls}:{tag}".encode()


async def ack(coro):
    return await asyncio.wait_for(coro, OP_LIMIT)


async def run_seq(cls, ns, key, handle, pre):
    """One serial sequence. Returns (exp list, extra fields). Raises on a client-side error."""
    v = lambda s: s.encode()
    if cls == "set":
        await ack(handle.set(key, v("v1")))
        await ack(handle.set(key, v("v2")))
        return [{"key": key, "k": "val", "v": b64(v("v2"))}], {}
    if cls == "settl":
        t = time.time()
        await ack(handle.set(key, v("700"), ttl_seconds=900))
        return [{"key": key, "k": "ttl", "v": b64(v("700")), "ttl": 900, "tw": time.time()}], {"tw0": t}
    if cls == "expire":
        t = time.time()
        await ack(handle.set(key, v("ve"), ttl_seconds=6))
        return [{"key": key, "k": "absent_after", "tw": time.time(), "ttl": 6}], {}
    if cls == "multi":
        keys = [key + b":%d" % i for i in range(8)]
        await ack(handle.set_many({k: v("mv") for k in keys}))
        return [{"key": k, "k": "val", "v": b64(v("mv"))} for k in keys], {}
    if cls == "delete":
        d = await ack(handle.delete(key))
        return [{"key": key, "k": "absent"}], {"deleted": d}
    if cls == "setdel":
        await ack(handle.set(key, v("v1")))
        d = await ack(handle.delete(key))
        return [{"key": key, "k": "absent"}], {"deleted": d}
    if cls == "delset":
        await ack(handle.delete(key))
        await ack(handle.set(key, v("v1")))
        return [{"key": key, "k": "val", "v": b64(v("v1"))}], {}
    if cls == "incr":
        res = [await ack(handle.incr(key, 1)) for _ in range(5)]
        return [{"key": key, "k": "ctr", "n": 105}], {"results": res}
    if cls == "incrpar":
        async def three():
            return [await ack(handle.incr(key, 1)) for _ in range(3)]
        res = await asyncio.gather(*[three() for _ in range(4)])
        return [{"key": key, "k": "ctr", "n": 112}], {"results": [r for rs in res for r in rs]}
    if cls == "cas":
        got = await ack(handle.get_with_token(key))
        if got is None:
            return [], {"note": "preloaded key missing before cas"}
        _, token = got
        outcomes = await asyncio.gather(*[ack(handle.replace(key, token, v(f"w{i}"))) for i in range(4)])
        winners = [i for i, o in enumerate(outcomes) if o]
        if len(winners) == 1:
            return [{"key": key, "k": "val", "v": b64(v(f"w{winners[0]}"))}], {"winners": 1}
        return [{"key": key, "k": "cas_bad", "winners": len(winners)}], {"winners": len(winners)}
    if cls == "casdel":
        got = await ack(handle.get_with_token(key))
        if got is None:
            return [], {"note": "preloaded key missing before casdel"}
        d = await ack(handle.delete_if_matches(key, got[1]))
        return ([{"key": key, "k": "absent"}] if d else [{"key": key, "k": "cas_bad", "winners": 0}]), {"deleted": d}
    if cls == "pia":
        a = await ack(handle.put_if_absent(key, v("first")))
        b = await ack(handle.put_if_absent(key, v("second")))
        if a and not b:
            return [{"key": key, "k": "val", "v": b64(v("first"))}], {}
        return [{"key": key, "k": "pia_bad", "a": a, "b": b}], {"a": a, "b": b}
    if cls == "casseq":
        got = await ack(handle.get_with_token(key))
        if got is None:
            return [], {"note": "preloaded key missing before casseq"}
        r1 = await ack(handle.replace(key, got[1], v("v1")))
        got2 = await ack(handle.get_with_token(key))
        r2 = await ack(handle.replace(key, got2[1], v("v2"))) if got2 else False
        if r1 and r2:
            return [{"key": key, "k": "val", "v": b64(v("v2"))}], {}
        return [{"key": key, "k": "cas_bad", "winners": int(r1) + int(r2)}], {"r1": r1, "r2": r2}
    raise RuntimeError(cls)


def bucket_pool(client, nsh):
    pools = {}
    for cls, ns in ALL:
        if cls in PRELOADED:
            pools[(cls, ns)] = [keyname(cls, ns, f"p{i}") for i in range(PRE)]
    return pools


async def preload(client, nsh, pools):
    for (cls, ns), keys in pools.items():
        h = nsh if ns else client
        val = b"100" if cls in ("incr", "incrpar") else b"v0"
        for i in range(0, len(keys), 100):
            await h.set_many({k: val for k in keys[i:i + 100]})


async def clear_task(client, nsh_for, label, at_fn):
    await at_fn()
    ns = f"clr{label}".encode()
    h = client.namespace(ns)
    a_keys = [b"a%d" % i for i in range(20)]
    b_keys = [b"b%d" % i for i in range(20)]
    t0 = time.time()
    try:
        await asyncio.gather(*[ack(h.set(k, b"A")) for k in a_keys])
        await ack(h.clear())
        t_clear = time.time()
        await asyncio.gather(*[ack(h.set(k, b"B")) for k in b_keys])
        t1 = time.time()
    except Exception as e:  # noqa: BLE001
        rec(cls="clear", ns=ns.decode(), key="", t0=t0, t1=time.time(), status="err", err=f"{type(e).__name__}: {e}"[:200], exp=[])
        return
    exp = [{"key": k, "k": "absent", "nsraw": ns.decode()} for k in a_keys] + [{"key": k, "k": "val", "v": b64(b"B"), "nsraw": ns.decode()} for k in b_keys]
    rec(cls="clear", ns=ns.decode(), key="", t0=t0, t1=t1, t_clear=t_clear, status="ok", exp=[{**e, "key": b64(e["key"])} for e in exp])


async def read_stdin():
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return
        line = line.strip()
        now = time.time()
        if line == "go":
            state["go"] = now
            rec(mark="go", t=now)
        elif line == "go2":
            state["go2"] = now
            rec(mark="go2", t=now)


async def main():
    addrs = [(h, int(p)) for h, p in (a.rsplit(":", 1) for a in os.environ["NANOTEST_ADDRESSES"].split(","))]
    client = await NanocachedClient.connect(addrs)
    nsh = client.namespace(NS)
    pools = bucket_pool(client, nsh)
    await preload(client, nsh, pools)
    asyncio.ensure_future(read_stdin())
    ready_at = time.time()
    rec(mark="ready", t=ready_at)
    print("READY", flush=True)

    counters = {}
    seq = [0]

    async def worker(w):
        while True:
            if state["go"] is not None and time.time() > state["go"] + LOOP_AFTER_GO:
                return
            i = seq[0]
            seq[0] += 1
            cls, ns = ALL[i % len(ALL)]
            n = counters.get((cls, ns), 0)
            counters[(cls, ns)] = n + 1
            handle = nsh if ns else client
            if cls in PRELOADED:
                if n >= PRE:
                    await asyncio.sleep(0.05)
                    continue
                key = pools[(cls, ns)][n]
            else:
                key = keyname(cls, ns, f"w{w}-{n}")
            t0 = time.time()
            try:
                exp, extra = await run_seq(cls, ns, key, handle, True)
                rec(cls=cls, ns=NS.decode() if ns else "", key=b64(key), t0=t0, t1=time.time(), status="ok",
                    exp=[{**e, "key": b64(e["key"])} for e in exp], **extra)
            except Exception as e:  # noqa: BLE001
                rec(cls=cls, ns=NS.decode() if ns else "", key=b64(key), t0=t0, t1=time.time(), status="err", err=f"{type(e).__name__}: {e}"[:200], exp=[])
            await asyncio.sleep(0.1)

    async def wait_go(offset):
        while state["go"] is None:
            await asyncio.sleep(0.05)
        d = state["go"] + offset - time.time()
        if d > 0:
            await asyncio.sleep(d)

    async def before_go(back):  # about `back` seconds before the orchestrator sends go (it waits 6 s after READY)
        d = ready_at + (6 - back) - time.time()
        if d > 0:
            await asyncio.sleep(d)

    clears = [clear_task(client, None, "m3", lambda: before_go(3))]
    offsets = (0, 1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30, 50, 70) if os.environ.get("CLEAR_DENSE") else (0, 3, 10, 25, 50, 70)
    for o in offsets:
        clears.append(clear_task(client, None, f"p{o}", (lambda o=o: wait_go(o))))
    await asyncio.gather(*[worker(w) for w in range(WORKERS)], *clears)
    st = client.stats()
    rec(mark="stats", stats={k: getattr(st, k) for k in dir(st) if not k.startswith("_") and isinstance(getattr(st, k), (int, float))})
    try:
        await asyncio.wait_for(client.close(), 30)
    except Exception as e:  # noqa: BLE001
        rec(mark="close_error", err=str(e)[:200])
    out.close()
    os._exit(0)  # the stdin reader thread would keep the interpreter alive until stdin closes


asyncio.run(main())
