"""Versioned writer for the stale-read measurements of #563 / #565 / #582.

stalewrite.py --addr host:port [--n 20000] [--workers 4] [--prefix s]
              [--size 200] [--duration 0] [--settle 15]

Seeds `<prefix>0..<prefix>N-1` with version 1, then rewrites random keys
as fast as one SDK client with --workers concurrent writers allows, each
value `<key>|<version>|<pad>`.
Every worker owns a disjoint slice of the keyspace and writes it
sequentially, so a key never has two writes in flight and "the last
acknowledged version" is well defined. On SIGTERM (or after --duration)
it stops writing, waits --settle seconds for forwarding windows and
handoffs to finish, reads every key back through the SDK and prints one
JSON line:

  ok / miss / stale (read version < last acknowledged) /
  future (read version > last acknowledged: a write that failed at the
  client but landed) / corrupt, plus the first stale keys and the SDK's
  replica_write_failures.

chaos.sh starts it next to load.py and stops it the moment the last round
ends, so the count is the divergence the rounds left behind, not what
further rewrites happened to repair.
"""
import argparse, asyncio, json, random, signal, sys, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=20000)
ap.add_argument("--workers", type=int, default=4); ap.add_argument("--prefix", default="s")
ap.add_argument("--size", type=int, default=200); ap.add_argument("--duration", type=float, default=0)
ap.add_argument("--settle", type=float, default=15); ap.add_argument("--report-every", type=float, default=30)
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
pad = "x" * a.size
acked = [0] * a.n          # last acknowledged version per key (0 = never)
res = dict(writes=0, write_err=0, errors={})
stop = asyncio.Event()

def key(i): return f"{a.prefix}{i}"
def val(i, v): return f"{key(i)}|{v}|{pad}".encode()

async def write(c, i, v):
    try:
        await c.set(key(i), val(i, v)); acked[i] = v; res["writes"] += 1; return True
    except Exception as e:
        res["write_err"] += 1; n = type(e).__name__; res["errors"][n] = res["errors"].get(n, 0) + 1
        return False

async def worker(w, c):
    mine = list(range(w, a.n, a.workers))
    # Phase 1: seed version 1. A key whose seed failed is retried by the
    # rewrite phase like any other (its acked version stays 0 until then).
    for i in mine: await write(c, i, 1)
    return mine

async def rewrite(w, c, mine):
    while not stop.is_set():
        i = random.choice(mine)
        await write(c, i, acked[i] + 1)

async def check(c):
    out = dict(ok=0, miss=0, stale=0, future=0, corrupt=0, read_err=0, unseeded=0, examples=[])
    sem = asyncio.Semaphore(64)
    async def one(i):
        async with sem:
            try: v = await c.get_bytes(key(i))
            except Exception:
                out["read_err"] += 1; return
        want = acked[i]
        if want == 0: out["unseeded"] += 1; return
        if v is None:
            out["miss"] += 1; ex = ("miss", want, None)
        else:
            parts = v.decode(errors="replace").split("|", 2)
            if len(parts) != 3 or parts[0] != key(i) or not parts[1].isdigit():
                out["corrupt"] += 1; return
            got = int(parts[1])
            if got == want: out["ok"] += 1; return
            kind = "stale" if got < want else "future"
            out[kind] += 1; ex = (kind, want, got)
        if len(out["examples"]) < a.max_examples: out["examples"].append((key(i),) + ex)
    await asyncio.gather(*[one(i) for i in range(a.n)])
    return out

async def reporter(t0):
    last = 0
    while not stop.is_set():
        try: await asyncio.wait_for(stop.wait(), a.report_every)
        except asyncio.TimeoutError: pass
        now = res["writes"]
        print(json.dumps(dict(t=int(time.time() - t0), phase="rewrite", writes_s=int((now - last) / a.report_every),
                              writes=now, write_err=res["write_err"])), flush=True)
        last = now

async def main():
    t0 = time.time()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT): loop.add_signal_handler(s, stop.set)
    c = await NanocachedClient.connect(addrs)
    slices = await asyncio.gather(*[worker(w, c) for w in range(a.workers)])
    print(json.dumps(dict(t=int(time.time() - t0), phase="seeded", n=a.n, write_err=res["write_err"])), flush=True)
    if a.duration > 0: loop.call_later(a.duration, stop.set)
    rep = asyncio.create_task(reporter(t0))
    await asyncio.gather(*[rewrite(w, c, m) for w, m in enumerate(slices)])
    rep.cancel()
    t_stop = time.time()
    print(json.dumps(dict(t=int(t_stop - t0), phase="stopped", writes=res["writes"], write_err=res["write_err"],
                          settle=a.settle)), flush=True)
    await asyncio.sleep(a.settle)
    out = await check(c)
    stats = c.stats()
    await c.close()
    print(json.dumps(dict(t=int(time.time() - t0), phase="checked", n=a.n, writes=res["writes"], write_err=res["write_err"],
                          errors=res["errors"], replica_write_failures=stats.replica_write_failures,
                          **out)), flush=True)

asyncio.run(main())
