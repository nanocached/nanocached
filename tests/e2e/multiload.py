"""Multi-set workload with an oracle, for the churn scenarios (o under join,
decommission, eviction and kill-restart).

multiload.py --addr host:port [--n 2000] [--workers 4] [--batch 10] [--turns 3] [--single]
             [--bursts 0 --burst-secs 12] [--span 900] [--duration 0]
             [--settle 15] [--state logs/multistate.json]

Two key classes, both seeded one key at a time with generation 0 (value
"<i>:0"):

  md<i>  default namespace
  mn<i>  namespace "mns" (a different ring position per key)

A turn is one set_many() of --batch random keys of one class, each key
with its next generation ("<i>:<g>"); with --single the same keys go as
concurrent set() calls, for an A/B of the two write paths. Every worker owns a disjoint slice
of both classes and runs one batch at a time, so a key never has two
writes in flight and "the last acknowledged generation" is well defined.
set_many() returning is an acknowledgement of every key in the batch; a
PartialSetConnectionLostError acknowledges the keys in partial_keys only,
a WrongNodeError or any other error acknowledges none (the keys stay
attempted, not acknowledged: the batch may have landed).

--turns is how many batches each key is written in, in all; they are
spread like delload.py's: evenly over --span seconds, or with --bursts N
one N-th of them over the --burst-secs after each SIGUSR1 (the scenario
sends one as each round starts), so the batches land in the join /
decommission / kill windows.

On SIGTERM (or after --duration) no new batches are taken and the batch in
flight is finished; after --settle seconds every key is read back through
the SDK, one JSON line is printed (phase "checked") and --state is written
for multicheck.py's per-owner raw check. Per class (m?_):

  ok       the generation read is the last acknowledged one or a later,
           attempted one (a failed batch that landed)
  stale    older than the last acknowledged one
  future   newer than the last attempted one, corrupt: not "<i>:<g>"
  miss     the key is gone
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient, PartialSetConnectionLostError

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--workers", type=int, default=4); ap.add_argument("--batch", type=int, default=10)
ap.add_argument("--turns", type=int, default=3)
ap.add_argument("--single", action="store_true", help="write each batch as concurrent single set() calls instead of one set_many(), for A/B")
ap.add_argument("--span", type=float, default=900)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=15)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more batches
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0
CLASSES = ("d", "n")
NS = b"mns"

def key(cls, i): return f"m{cls}{i}"
def val(i, g): return f"{i}:{g}".encode()
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

attempted = {c: [0] * a.n for c in CLASSES}    # highest generation sent
acked = {c: [0] * a.n for c in CLASSES}        # highest generation acknowledged
counts = dict(batches=0, keys_acked=0, partial=0, wrong_node=0, other_err=0, seed_retry=0)
errors, examples = {}, {}

def err(e, where):
    n = type(e).__name__; errors[n] = errors.get(n, 0) + 1
    k = f"err:{where}:{n}"; examples[k] = examples.get(k, 0) + 1
    if examples[k] <= 3:
        print(json.dumps(dict(at=now_s(), t=int(time.time() - T0), error=n, where=where, msg=str(e)[:160])), flush=True)

async def bounded_sleep(secs):
    try: await asyncio.wait_for(done.wait(), secs)
    except asyncio.TimeoutError: pass

def handle(c, cls):
    return c.namespace(NS) if cls == "n" else c

async def seed(c, cls):
    sem = asyncio.Semaphore(64); failed = 0
    h = handle(c, cls)
    async def one(i):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await h.set(key(cls, i), val(i, 0)); return
                except Exception as e:
                    err(e, f"seed-{cls}"); counts["seed_retry"] += 1; await asyncio.sleep(0.2)
            failed += 1
    await asyncio.gather(*[one(i) for i in range(a.n)])
    return failed

async def batch(c, cls, idx):
    items, gens = {}, {}
    for i in idx:
        g = attempted[cls][i] + 1; attempted[cls][i] = g; gens[key(cls, i)] = (i, g)
        items[key(cls, i)] = val(i, g)
    counts["batches"] += 1
    try:
        if a.single:
            res = await asyncio.gather(*[handle(c, cls).set(k, v) for k, v in items.items()], return_exceptions=True)
            stored = {k for k, r in zip(items, res) if not isinstance(r, BaseException)}
            for r in res:
                if isinstance(r, BaseException): counts["other_err"] += 1; err(r, "set")
        else:
            await handle(c, cls).set_many(items)
            stored = set(items)
    except PartialSetConnectionLostError as e:
        counts["partial"] += 1; err(e, "set_many")
        stored = {k.decode() if isinstance(k, bytes) else k for k in e.partial_keys}
    except Exception as e:
        counts["wrong_node" if type(e).__name__ == "WrongNodeError" else "other_err"] += 1; err(e, "set_many")
        stored = set()
    for k in stored:
        i, g = gens[k]; acked[cls][i] = max(acked[cls][i], g); counts["keys_acked"] += 1

async def worker(w, c):
    mine = {cls: list(range(w, a.n, a.workers)) for cls in CLASSES}
    total = a.turns * sum(len(v) for v in mine.values()) // a.batch
    due = []
    bursts_taken = 0
    t_run = time.time()
    left = total
    if a.bursts <= 0:
        due = [t_run + (j + 1) / total * a.span for j in range(total)]; left = 0
    while not done.is_set():
        now = time.time()
        if not stop.is_set():
            if a.bursts > 0 and bursts_taken < bursts_signalled and left:
                bursts_taken += 1; share = min(left, -(-total // a.bursts)); left -= share
                due += [now + (j + 1) / share * a.burst_secs for j in range(share)]
            ready = [t for t in due if t <= now]
            if ready:
                due = [t for t in due if t > now]
                for _ in ready:
                    if stop.is_set(): break
                    cls = random.choice(CLASSES)
                    idx = random.sample(mine[cls], min(a.batch, len(mine[cls])))
                    await batch(c, cls, idx)
        await bounded_sleep(0.05)
    return dict(unsent=left + len(due), bursts_taken=bursts_taken)

async def sdk_check(c):
    out = dict(read_err=0, examples=[])
    for cls in CLASSES:
        for k in ("ok", "stale", "future", "corrupt", "miss"): out[f"m{cls}_{k}"] = 0
    sem = asyncio.Semaphore(64)
    def ex(*row):
        if len(out["examples"]) < a.max_examples: out["examples"].append(row)
    async def one(cls, i):
        async with sem:
            try: v = await handle(c, cls).get_bytes(key(cls, i))
            except Exception: out["read_err"] += 1; return
            p = f"m{cls}_"
            if v is None: out[p + "miss"] += 1; ex("miss", key(cls, i), acked[cls][i], attempted[cls][i]); return
            try:
                ii, g = (int(x) for x in v.decode().split(":"))
                if ii != i: raise ValueError
            except Exception:
                out[p + "corrupt"] += 1; ex("corrupt", key(cls, i), v[:16].decode("latin-1")); return
            if g > attempted[cls][i]: out[p + "future"] += 1; ex("future", key(cls, i), g, attempted[cls][i])
            elif g < acked[cls][i]: out[p + "stale"] += 1; ex("stale", key(cls, i), g, acked[cls][i])
            else: out[p + "ok"] += 1
    await asyncio.gather(*[one(cls, i) for cls in CLASSES for i in range(a.n)])
    return out

async def reporter():
    while not done.is_set():
        await bounded_sleep(a.report_every)
        print(json.dumps(dict(t=int(time.time() - T0), phase="run", counts=counts, errors=errors)), flush=True)

async def main():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT): loop.add_signal_handler(s, stop.set)
    def on_burst():
        global bursts_signalled
        bursts_signalled += 1; print(json.dumps(dict(at=now_s(), t=int(time.time() - T0), burst=bursts_signalled)), flush=True)
    loop.add_signal_handler(signal.SIGUSR1, on_burst)
    c = await NanocachedClient.connect(addrs)
    fd = await seed(c, "d"); fn = await seed(c, "n")
    print(json.dumps(dict(t=int(time.time() - T0), phase="seeded", n=a.n, seed_failed=dict(d=fd, n=fn))), flush=True)
    if a.duration > 0: loop.call_later(a.duration, stop.set)
    rep = asyncio.create_task(reporter())
    workers = [asyncio.create_task(worker(w, c)) for w in range(a.workers)]
    await stop.wait()
    print(json.dumps(dict(t=int(time.time() - T0), phase="stopped", counts=counts, errors=errors,
                          bursts_signalled=bursts_signalled, settle=a.settle)), flush=True)
    await asyncio.sleep(a.settle)
    done.set()
    wres = await asyncio.gather(*workers)
    rep.cancel()
    agg = {k: sum(r[k] for r in wres) for k in wres[0]}
    out = await sdk_check(c)
    st = c.stats()
    print(json.dumps(dict(t=int(time.time() - T0), phase="checked", n=a.n, counts=counts, errors=errors, workers=agg,
                          replica_write_failures=st.replica_write_failures, **out)), flush=True)
    if a.state:
        json.dump(dict(n=a.n, t_check=time.time(), acked=acked, attempted=attempted), open(a.state, "w"))
    await c.close()

asyncio.run(main())
