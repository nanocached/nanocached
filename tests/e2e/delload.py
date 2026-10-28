"""Delete workload with an oracle, for the churn scenarios (D under join,
decommission and kill-restart).

delload.py --addr host:port [--n 2000] [--workers 4] [--gap-min 1 --gap-max 6]
           [--bursts 0 --burst-secs 12] [--span 900] [--duration 0]
           [--settle 15] [--state logs/delstate.json]

Two key classes, both seeded with the value "<i>:1":

  dx<i>  deleted: one client delete, retried until it gets a definitive
         answer (True or False both mean "absent now"). From then on no
         node may answer the key with a value: a copy that comes back is
         one a join transfer, a re-replication pass, a relay or a window
         forward carried from before the delete.
  dr<i>  deleted and created again: a delete, then --gap-min..--gap-max
         seconds later a set of "<i>:2" (both retried until acknowledged).
         Once the set is acknowledged every owner must hold "<i>:2". A
         missing copy is a legitimate copy refused (a tombstone the
         delete left outlives the set, or a dead copy was swept), a
         "<i>:1" is the deleted state coming back.

Turns are spread like touchload.py's: evenly over --span seconds, or with
--bursts N one N-th of each class over the --burst-secs after each SIGUSR1
(the scenario sends one as each round starts), so the deletes land in the
join / decommission / kill windows and the re-creations, seconds later,
land in the next ones. A key whose turn had not come, or whose operation
never got a definitive answer when the run stops, is reported as unknown,
not asserted.

On SIGTERM (or after --duration) no new turns are taken and cycles in
flight are finished; after --settle seconds every key is read back
through the SDK, one JSON line is printed (phase "checked") and --state
is written for delcheck.py's per-owner raw check.
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--workers", type=int, default=4)
ap.add_argument("--gap-min", type=float, default=1.0); ap.add_argument("--gap-max", type=float, default=6.0)
ap.add_argument("--span", type=float, default=900)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=15)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more turns, cycles in flight are finished
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0

def key(cls, i): return f"d{cls}{i}"
def val(i, gen): return f"{i}:{gen}".encode()
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

x_del_ack = [0.0] * a.n        # dx: acknowledged delete (0 = not yet)
r_del_ack = [0.0] * a.n        # dr: acknowledged delete
r_set_ack = [0.0] * a.n        # dr: acknowledged re-creation
counts = dict(delete_x=0, delete_r=0, create_r=0, ops_err=0)
anom = dict(corrupt=0, delete_retry=0, create_retry=0)
errors, examples = {}, {}

def note(kind, **kw):
    anom[kind] = anom.get(kind, 0) + 1
    if anom[kind] <= a.max_examples:
        print(json.dumps(dict(at=now_s(), t=int(time.time() - T0), anomaly=kind, **kw)), flush=True)

def err(e, where):
    counts["ops_err"] += 1
    n = type(e).__name__; errors[n] = errors.get(n, 0) + 1
    k = f"err:{where}:{n}"; examples[k] = examples.get(k, 0) + 1
    if examples[k] <= 3:
        print(json.dumps(dict(at=now_s(), t=int(time.time() - T0), error=n, where=where, msg=str(e)[:160])), flush=True)

async def bounded_sleep(secs):
    try: await asyncio.wait_for(done.wait(), secs)
    except asyncio.TimeoutError: pass

async def seed(c, cls):
    sem = asyncio.Semaphore(64); failed = 0
    async def one(i):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await c.set(key(cls, i), val(i, 1)); return
                except Exception as e:
                    err(e, f"seed-{cls}"); await asyncio.sleep(0.2)
            failed += 1
    await asyncio.gather(*[one(i) for i in range(a.n)])
    return failed

async def delete_x(c, i):
    try:
        t = time.time(); await c.delete(key("x", i)); x_del_ack[i] = t; counts["delete_x"] += 1; return True
    except Exception as e:
        err(e, "delete-x"); anom["delete_retry"] += 1; return False

async def delete_r(c, i):
    try:
        t = time.time(); await c.delete(key("r", i)); r_del_ack[i] = t; counts["delete_r"] += 1; return True
    except Exception as e:
        err(e, "delete-r"); anom["delete_retry"] += 1; return False

async def create_r(c, i):
    try:
        t = time.time(); await c.set(key("r", i), val(i, 2)); r_set_ack[i] = t; counts["create_r"] += 1; return True
    except Exception as e:
        err(e, "create-r"); anom["create_retry"] += 1; return False

async def worker(w, c):
    mine = list(range(w, a.n, a.workers))
    x_queue = list(mine); random.shuffle(x_queue)
    r_queue = list(mine); random.shuffle(r_queue)
    due = []           # (due time, "x"|"r", key index): released turns, taken once due
    x_retry, r_retry = [], []      # deletes without a definitive answer yet
    create_wait = []   # (ready time, key index): deleted, to be created again at the ready time
    bursts_taken = 0
    t_run = time.time()
    if a.bursts <= 0:
        due = [(t_run + (j + 1) / len(x_queue) * a.span, "x", i) for j, i in enumerate(x_queue)]
        due += [(t_run + (j + 1) / len(r_queue) * a.span, "r", i) for j, i in enumerate(r_queue)]
        x_queue, r_queue = [], []
    while not done.is_set():
        now = time.time()
        if not stop.is_set():
            if a.bursts > 0 and bursts_taken < bursts_signalled and (x_queue or r_queue):
                bursts_taken += 1; share = -(-len(mine) // a.bursts)
                for cls, q in (("x", x_queue), ("r", r_queue)):
                    released = [q.pop() for _ in range(min(share, len(q)))]
                    due += [(now + (j + 1) / len(released) * a.burst_secs, cls, i) for j, i in enumerate(released)]
            ready = [(cls, i) for t, cls, i in due if t <= now]
            if ready: due = [d for d in due if d[0] > now]
            for cls, i in ready:
                if cls == "x":
                    if not await delete_x(c, i): x_retry.append(i)
                else:
                    if await delete_r(c, i): create_wait.append((time.time() + random.uniform(a.gap_min, a.gap_max), i))
                    else: r_retry.append(i)
        # retries and re-creations go on through the stop and the settle
        x_retry = [i for i in x_retry if not await delete_x(c, i)]
        again = []
        for i in r_retry:
            if await delete_r(c, i): create_wait.append((time.time() + random.uniform(a.gap_min, a.gap_max), i))
            else: again.append(i)
        r_retry = again
        now = time.time()
        go = [i for t, i in create_wait if t <= now]
        if go:
            create_wait = [(t, i) for t, i in create_wait if t > now]
            for i in go:
                if not await create_r(c, i): create_wait.append((time.time() + 0.5, i))
        await bounded_sleep(0.2)
    return dict(x_unsent=len(x_queue) + sum(1 for d in due if d[1] == "x"), x_unknown=len(x_retry),
                r_unsent=len(r_queue) + sum(1 for d in due if d[1] == "r"), r_unknown=len(r_retry) + len(create_wait),
                bursts_taken=bursts_taken)

async def sdk_check(c):
    out = dict(x_absent=0, x_present=0, x_unknown=0, r_ok=0, r_miss=0, r_stale=0, r_corrupt=0, r_unknown=0,
               read_err=0, examples=[])
    sem = asyncio.Semaphore(64)
    def ex(*row):
        if len(out["examples"]) < a.max_examples: out["examples"].append(row)
    async def one_x(i):
        async with sem:
            if x_del_ack[i] == 0.0: out["x_unknown"] += 1; return
            try: v = await c.get_bytes(key("x", i))
            except Exception: out["read_err"] += 1; return
            if v is None: out["x_absent"] += 1
            else: out["x_present"] += 1; ex("x_present", key("x", i), v[:16].decode("latin-1"))
    async def one_r(i):
        async with sem:
            if r_set_ack[i] == 0.0: out["r_unknown"] += 1; return
            try: v = await c.get_bytes(key("r", i))
            except Exception: out["read_err"] += 1; return
            if v == val(i, 2): out["r_ok"] += 1
            elif v is None: out["r_miss"] += 1; ex("r_miss", key("r", i))
            elif v == val(i, 1): out["r_stale"] += 1; ex("r_stale", key("r", i))
            else: out["r_corrupt"] += 1; ex("r_corrupt", key("r", i), v[:16].decode("latin-1"))
    await asyncio.gather(*[one_x(i) for i in range(a.n)], *[one_r(i) for i in range(a.n)])
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
    fx = await seed(c, "x"); fr = await seed(c, "r")
    print(json.dumps(dict(t=int(time.time() - T0), phase="seeded", n=a.n, seed_failed=dict(x=fx, r=fr))), flush=True)
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
    print(json.dumps(dict(t=int(time.time() - T0), phase="checked", n=a.n, counts=counts, anomalies=anom, errors=errors,
                          workers=agg, replica_write_failures=st.replica_write_failures, **out)), flush=True)
    if a.state:
        json.dump(dict(n=a.n, t_check=time.time(), x_del_ack=x_del_ack, r_del_ack=r_del_ack, r_set_ack=r_set_ack),
                  open(a.state, "w"))
    await c.close()

asyncio.run(main())
