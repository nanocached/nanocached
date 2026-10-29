"""Clear workload with an oracle, for the churn scenarios (c under join,
decommission, eviction and kill-restart).

clearload.py --addr host:port [--cycles 600] [--k 20] [--workers 4]
             [--control 500] [--bursts 0 --burst-secs 12] [--span 900]
             [--duration 0] [--settle 15] [--state logs/clearstate.json]

One cycle, in a namespace of its own (cn<j>): set --k "old" keys
(o<m>, value "o<j>:<m>"), wait for every set, clear the namespace (retried
until it returns), then set --k "new" keys (n<m>, value "n<j>:<m>"), the same
shape the accepted-behaviour measurements of clear() used. Once the
settle is over:

  old  an old key whose set was acknowledged, before a clear that
       returned, must be absent. A copy that is still there is a
       forward, a re-replication pass or a drain delivering a copy read
       before the clear: stale_old (keys) and the cycles holding any
       (cycles_stale_old, stale_max_in_cycle).
  new  a new key whose set was acknowledged after the clear returned
       must hold "n<j>:<m>" on every owner. Missing is new_miss (a copy
       the clear's late forward dropped), anything else new_corrupt.

Control keys are written once and never cleared, --control in the
default namespace (ckd<m>) and --control in a namespace of their own
(ck, key k<m>): clearing cn<j> must leave them alone (control_miss,
control_corrupt).

Turns are spread like delload.py's: evenly over --span seconds, or with
--bursts N one N-th of the cycles over the --burst-secs after each
SIGUSR1. A cycle whose turn had not come, or whose clear never returned
when the run stops, is reported as unknown, not asserted.

On SIGTERM (or after --duration) no new cycles are started and cycles in
flight are finished; after --settle seconds every key is read back
through the SDK, one JSON line is printed (phase "checked") and --state
is written for clearcheck.py's per-owner raw check.
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--cycles", type=int, default=600)
ap.add_argument("--k", type=int, default=20); ap.add_argument("--workers", type=int, default=4)
ap.add_argument("--control", type=int, default=500)
ap.add_argument("--span", type=float, default=900)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=15)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more cycles, cycles in flight are finished
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0

def ns_of(j): return f"cn{j}"
def old_val(j, m): return f"o{j}:{m}".encode()
def new_val(j, m): return f"n{j}:{m}".encode()
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

# per cycle: acknowledged old sets, acknowledged new sets, time the clear returned (0 = never)
cycles = [dict(old=[], new=[], clear_ack=0.0, clear_tries=0) for _ in range(a.cycles)]
counts = dict(cycles_done=0, clears=0, old_sets=0, new_sets=0, ops_err=0)
anom = dict(clear_retry=0)
errors, examples = {}, {}

def err(e, where):
    counts["ops_err"] += 1
    n = type(e).__name__; errors[n] = errors.get(n, 0) + 1
    k = f"err:{where}:{n}"; examples[k] = examples.get(k, 0) + 1
    if examples[k] <= 3:
        print(json.dumps(dict(at=now_s(), t=int(time.time() - T0), error=n, where=where, msg=str(e)[:160])), flush=True)

async def bounded_sleep(secs):
    try: await asyncio.wait_for(done.wait(), secs)
    except asyncio.TimeoutError: pass

async def seed_control(c):
    sem = asyncio.Semaphore(64); failed = 0
    kns = c.namespace("ck")
    async def one(m, ns, key, v):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await (ns.set(key, v) if ns else c.set(key, v)); return
                except Exception as e:
                    err(e, "seed-control"); await asyncio.sleep(0.2)
            failed += 1
    jobs = [one(m, None, f"ckd{m}", f"d{m}".encode()) for m in range(a.control)]
    jobs += [one(m, kns, f"k{m}", f"k{m}".encode()) for m in range(a.control)]
    await asyncio.gather(*jobs)
    return failed

async def run_set(ns, j, m, v, bucket, counter):
    try:
        await ns.set(f"{bucket[0]}{m}", v); cycles[j][bucket].append(m); counts[counter] += 1
    except Exception as e:
        err(e, f"set-{bucket}")

async def one_cycle(c, j):
    ns = c.namespace(ns_of(j))
    await asyncio.gather(*[run_set(ns, j, m, old_val(j, m), "old", "old_sets") for m in range(a.k)])
    while True:
        cycles[j]["clear_tries"] += 1
        try:
            await ns.clear(); break
        except Exception as e:
            err(e, "clear"); anom["clear_retry"] += 1
            if done.is_set(): return False
            await bounded_sleep(0.3)
    cycles[j]["clear_ack"] = time.time(); counts["clears"] += 1
    await asyncio.gather(*[run_set(ns, j, m, new_val(j, m), "new", "new_sets") for m in range(a.k)])
    counts["cycles_done"] += 1
    return True

async def worker(w, c):
    mine = list(range(w, a.cycles, a.workers))
    queue = list(mine); random.shuffle(queue)
    due = []           # (due time, cycle): released turns, taken once due
    bursts_taken = 0
    t_run = time.time()
    if a.bursts <= 0:
        due = [(t_run + (n + 1) / len(queue) * a.span, j) for n, j in enumerate(queue)]
        queue = []
    unfinished = 0
    while not done.is_set():
        now = time.time()
        if not stop.is_set():
            if a.bursts > 0 and bursts_taken < bursts_signalled and queue:
                bursts_taken += 1; share = -(-len(mine) // a.bursts)
                released = [queue.pop() for _ in range(min(share, len(queue)))]
                due += [(now + (n + 1) / len(released) * a.burst_secs, j) for n, j in enumerate(released)]
            ready = [j for t, j in due if t <= now]
            if ready: due = [d for d in due if d[0] > now]
            for j in ready:
                if not await one_cycle(c, j): unfinished += 1
        await bounded_sleep(0.2)
    return dict(unsent=len(queue) + len(due), unfinished=unfinished, bursts_taken=bursts_taken)

async def sdk_check(c):
    out = dict(control_ok=0, control_miss=0, control_corrupt=0, stale_old=0, cycles_stale_old=0, stale_max_in_cycle=0,
               new_ok=0, new_miss=0, new_corrupt=0, cycles_new_miss=0, cycles_checked=0, cycles_unknown=0,
               read_err=0, examples=[])
    sem = asyncio.Semaphore(64)
    def ex(*row):
        if len(out["examples"]) < a.max_examples: out["examples"].append(row)
    async def control(ns, key, want):
        async with sem:
            try: v = await (ns.get_bytes(key) if ns else c.get_bytes(key))
            except Exception: out["read_err"] += 1; return
            if v == want: out["control_ok"] += 1
            elif v is None: out["control_miss"] += 1; ex("control_miss", key)
            else: out["control_corrupt"] += 1; ex("control_corrupt", key, v[:16].decode("latin-1"))
    async def read(ns, kind, m):
        async with sem:
            try: return await ns.get_bytes(f"{kind}{m}"), True
            except Exception: out["read_err"] += 1; return None, False
    async def one_cycle_check(j):
        cy = cycles[j]
        if cy["clear_ack"] == 0.0: out["cycles_unknown"] += 1; return
        out["cycles_checked"] += 1
        ns = c.namespace(ns_of(j))
        olds = await asyncio.gather(*[read(ns, "o", m) for m in cy["old"]])
        news = await asyncio.gather(*[read(ns, "n", m) for m in cy["new"]])
        stale = 0
        for m, (v, ok) in zip(cy["old"], olds):
            if ok and v is not None: stale += 1; ex("stale_old", ns_of(j), m, v[:16].decode("latin-1"))
        if stale: out["stale_old"] += stale; out["cycles_stale_old"] += 1; out["stale_max_in_cycle"] = max(out["stale_max_in_cycle"], stale)
        miss = 0
        for m, (v, ok) in zip(cy["new"], news):
            if not ok: continue
            if v == new_val(j, m): out["new_ok"] += 1
            elif v is None: miss += 1; ex("new_miss", ns_of(j), m)
            else: out["new_corrupt"] += 1; ex("new_corrupt", ns_of(j), m, v[:16].decode("latin-1"))
        if miss: out["new_miss"] += miss; out["cycles_new_miss"] += 1
    kns = c.namespace("ck")
    await asyncio.gather(*[control(None, f"ckd{m}", f"d{m}".encode()) for m in range(a.control)],
                         *[control(kns, f"k{m}", f"k{m}".encode()) for m in range(a.control)],
                         *[one_cycle_check(j) for j in range(a.cycles)])
    return out

async def reporter():
    while not done.is_set():
        await bounded_sleep(a.report_every)
        print(json.dumps(dict(t=int(time.time() - T0), phase="run", counts=counts, anomalies=anom, errors=errors)), flush=True)

async def main():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT): loop.add_signal_handler(s, stop.set)
    def on_burst():
        global bursts_signalled
        bursts_signalled += 1; print(json.dumps(dict(at=now_s(), t=int(time.time() - T0), burst=bursts_signalled)), flush=True)
    loop.add_signal_handler(signal.SIGUSR1, on_burst)
    c = await NanocachedClient.connect(addrs)
    failed = await seed_control(c)
    print(json.dumps(dict(t=int(time.time() - T0), phase="seeded", cycles=a.cycles, k=a.k, control=a.control, seed_failed=failed)), flush=True)
    if a.duration > 0: loop.call_later(a.duration, stop.set)
    rep = asyncio.create_task(reporter())
    workers = [asyncio.create_task(worker(w, c)) for w in range(a.workers)]
    await stop.wait()
    print(json.dumps(dict(t=int(time.time() - T0), phase="stopped", counts=counts, anomalies=anom, errors=errors,
                          bursts_signalled=bursts_signalled, settle=a.settle)), flush=True)
    await asyncio.sleep(a.settle)
    done.set()
    wres = await asyncio.gather(*workers)
    rep.cancel()
    agg = {k: sum(r[k] for r in wres) for k in wres[0]}
    out = await sdk_check(c)
    st = c.stats()
    print(json.dumps(dict(t=int(time.time() - T0), phase="checked", cycles=a.cycles, k=a.k, counts=counts, anomalies=anom,
                          errors=errors, workers=agg, replica_write_failures=st.replica_write_failures, **out)), flush=True)
    if a.state:
        json.dump(dict(cycles=a.cycles, k=a.k, control=a.control, t_check=time.time(), per_cycle=cycles), open(a.state, "w"))
    await c.close()

asyncio.run(main())
