"""Touch workload with an oracle, for the churn scenarios (t / y under join,
decommission and kill-restart).

touchload.py --addr host:port [--n 2000] [--workers 4] [--ttl 120]
             [--pass-secs 20] [--n-short 40] [--expire-span 900] [--short 60]
             [--bursts 0 --burst-secs 12] [--duration 0] [--settle 15]
             [--state logs/touchstate.json]

Three key classes, every value the key's index as bare decimal digits (so a
delta-0 `i` can read the remaining TTL on the primary, see touchcheck.py):

  tk<i>  keep-alive: seeded with --ttl, then every key of a worker's slice is
         touched once per pass (a pass is paced to --pass-secs) with the same
         TTL, the API rotating per chunk: touch / get_and_touch /
         touch_many / get_many_and_touch. With --ttl 120 and 20 s passes a
         copy that silently keeps an old expiry (a lost or misapplied touch
         on some owner) expires within the run and shows up as a miss, a
         touch that answers False, or a copy missing on an owner.
  tn<i>  TTL removed: seeded without a TTL; on its turn a key is touched to
         --n-short seconds and, a few seconds later, to None (both retried
         until acknowledged). A copy that misses the removal expires
         --n-short seconds after the shortening and is missing at the end;
         every owner must hold the key and the primary must report no TTL.
         Probative once --n-short seconds have passed since the shortening
         (the scenario's --settle covers the last ones).
  tx<i>  expired by a touch: seeded without a TTL, then expired with a 0 TTL
         (or --short, one key in three), single and batched. A copy that
         misses the expiry lingers and is found by the raw check.
         Both tn and tx take their turns on one of two schedules: evenly
         over --expire-span seconds, or, with --bursts N, one N-th of the
         keys over the --burst-secs after each SIGUSR1 (the scenario sends
         one as each round starts, so the touches land in the join /
         decommission / kill windows and not between them). Keys whose turn
         had not come, or whose op never got a definitive answer, when the
         run stops are reported as unknown, not asserted.
         An op that fails is retried until it
         gets a definitive answer (True or False both mean "absent now"). A
         key whose acknowledged expiry has elapsed must read as absent
         everywhere afterwards: a copy that comes back is the forwarded or
         handed-off copy that did not get the expiry.

Every anomaly is printed as it happens with a wall-clock timestamp (first
50 of each kind) so it can be lined up against the scenario's round log.
On SIGTERM (or after --duration) the workers stop, the loader waits --settle
seconds, reads every key back through the SDK, prints one JSON line
(phase "checked") and writes --state (per-key acknowledgements) for
touchcheck.py's per-owner raw check.
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--workers", type=int, default=4); ap.add_argument("--ttl", type=int, default=120)
ap.add_argument("--pass-secs", type=float, default=20); ap.add_argument("--chunk", type=int, default=20)
ap.add_argument("--n-short", type=int, default=40); ap.add_argument("--n-gap", type=float, default=6)
ap.add_argument("--expire-span", type=float, default=900); ap.add_argument("--short", type=int, default=60)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=15)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more turns; the keep-alive passes go on through the settle
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0   # SIGUSR1 count (--bursts mode)

def key(cls, i): return f"t{cls}{i}"
def val(i): return str(i).encode()
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

# Oracle state. Times are wall-clock (time.time()).
last_ack = [0.0] * a.n        # tk: last acknowledged keep-alive touch
n_short_ack = [0.0] * a.n     # tn: acknowledged shortening to --n-short (0 = not yet)
none_ack = [0.0] * a.n        # tn: acknowledged TTL removal (0 = not yet)
exp_ack = [None] * a.n        # tx: (ack time, ttl used) once definitively expired
seed_time = {"k": 0.0, "n": 0.0, "x": 0.0}
counts = dict(touch=0, gat=0, touch_many=0, gmat=0, n_short=0, none=0, expire0=0, expire_short=0, ops_err=0)
anom = dict(touch_false=0, gat_miss=0, corrupt=0, expire_retry=0, n_retry=0, n_expired_before_removal=0)
errors = {}
examples = {}

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

async def seed(c, cls, ttl):
    sem = asyncio.Semaphore(64); failed = 0
    async def one(i):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await c.set(key(cls, i), val(i), ttl_seconds=ttl); return
                except Exception as e:
                    err(e, f"seed-{cls}"); await asyncio.sleep(0.2)
            failed += 1
    await asyncio.gather(*[one(i) for i in range(a.n)])
    seed_time[cls] = time.time()
    return failed

async def keepalive_chunk(c, idx, chunk):
    keys = [key("k", i) for i in chunk]
    mode = idx % 4
    try:
        if mode == 0:
            for i, k in zip(chunk, keys):
                t = time.time(); ok = await c.touch(k, a.ttl); counts["touch"] += 1
                if ok: last_ack[i] = t
                else: note("touch_false", key=k, api="touch")
        elif mode == 1:
            for i, k in zip(chunk, keys):
                t = time.time(); v = await c.get_and_touch_bytes(k, a.ttl); counts["gat"] += 1
                if v is None: note("gat_miss", key=k, api="get_and_touch")
                elif v != val(i): note("corrupt", key=k, api="get_and_touch", got=v[:32].decode("latin-1"))
                else: last_ack[i] = t
        elif mode == 2:
            t = time.time(); r = await c.touch_many(keys, a.ttl); counts["touch_many"] += 1
            for i, k in zip(chunk, keys):
                if r.get(k): last_ack[i] = t
                else: note("touch_false", key=k, api="touch_many")
        else:
            t = time.time(); r = await c.get_many_and_touch_bytes(keys, a.ttl); counts["gmat"] += 1
            for i, k in zip(chunk, keys):
                v = r.get(k)
                if v is None: note("gat_miss", key=k, api="get_many_and_touch")
                elif v != val(i): note("corrupt", key=k, api="get_many_and_touch", got=v[:32].decode("latin-1"))
                else: last_ack[i] = t
    except Exception as e:
        err(e, f"keepalive-mode{mode}")

async def n_shorten(c, batch):
    """Touches tn keys to --n-short; returns the ones without a definitive answer."""
    keys = [key("n", i) for i in batch]
    try:
        t = time.time(); r = await c.touch_many(keys, a.n_short); counts["n_short"] += 1
        for i, k in zip(batch, keys):
            if r.get(k): n_short_ack[i] = t
            else: note("touch_false", key=k, api="touch_many(n_short)")
        return []
    except Exception as e:
        err(e, "n-shorten"); anom["n_retry"] += len(batch); return list(batch)

async def n_remove(c, batch):
    """Removes the TTL of tn keys shortened earlier; returns the ones without a definitive answer."""
    keys = [key("n", i) for i in batch]
    try:
        t = time.time(); r = await c.touch_many(keys, None); counts["none"] += 1
        for i, k in zip(batch, keys):
            if r.get(k): none_ack[i] = t
            elif t - n_short_ack[i] > a.n_short - 1: anom["n_expired_before_removal"] += 1   # our own delay, not the server's
            else: note("touch_false", key=k, api="touch_many(None)", since_short=round(t - n_short_ack[i], 1))
        return []
    except Exception as e:
        err(e, "n-remove"); anom["n_retry"] += len(batch); return list(batch)

async def expire(c, batch):
    """Expires tx keys; a definitive reply (True or False) means the key is absent now.
    Returns the keys still without a definitive reply."""
    ttl = 0 if random.random() < 0.67 else a.short
    left = []
    if len(batch) == 1 or random.random() < 0.5:
        for i in batch:
            try:
                t = time.time(); await c.touch(key("x", i), ttl)
                exp_ack[i] = (t, ttl); counts["expire0" if ttl == 0 else "expire_short"] += 1
            except Exception as e:
                err(e, "expire-single"); left.append(i)
    else:
        keys = [key("x", i) for i in batch]
        try:
            t = time.time(); await c.touch_many(keys, ttl)
            for i in batch: exp_ack[i] = (t, ttl)
            counts["expire0" if ttl == 0 else "expire_short"] += len(batch)
        except Exception as e:
            err(e, "expire-batch"); left.extend(batch)
    if left: anom["expire_retry"] += len(left)
    return left

async def worker(w, c):
    mine = list(range(w, a.n, a.workers))
    exp_queue = list(mine); random.shuffle(exp_queue)
    n_queue = list(mine); random.shuffle(n_queue)
    due = []            # (due time, "x"|"n", key index): released turns, taken once due
    exp_retry, n_short_retry, n_remove_retry = [], [], []
    n_wait = []         # (ready time, key index): shortened, removal due at ready time
    bursts_taken = 0
    t_run = time.time()
    if a.bursts <= 0:   # even schedule over --expire-span
        due = [(t_run + (j + 1) / len(exp_queue) * a.expire_span, "x", i) for j, i in enumerate(exp_queue)]
        due += [(t_run + (j + 1) / len(n_queue) * a.expire_span, "n", i) for j, i in enumerate(n_queue)]
        exp_queue, n_queue = [], []
    passes = 0
    while not done.is_set():
        t_pass = time.time()
        # keep-alive pass over the whole slice (it goes on through the settle, so no key is left to expire
        # on its own before the check), the other classes' turns interleaved so they land all through it
        for ci, b in enumerate(range(0, len(mine), a.chunk)):
            if done.is_set(): break
            await keepalive_chunk(c, passes * 1000 + ci, mine[b:b + a.chunk])
            if stop.is_set(): continue
            if a.bursts > 0 and bursts_taken < bursts_signalled and (exp_queue or n_queue):
                bursts_taken += 1; share = -(-len(mine) // a.bursts); now = time.time()
                for cls, q in (("x", exp_queue), ("n", n_queue)):
                    released = [q.pop() for _ in range(min(share, len(q)))]
                    due += [(now + (j + 1) / len(released) * a.burst_secs, cls, i) for j, i in enumerate(released)]
            now = time.time()
            ready_x = [i for t, cls, i in due if t <= now and cls == "x"]
            ready_n = [i for t, cls, i in due if t <= now and cls == "n"]
            if ready_x or ready_n: due = [d for d in due if d[0] > now]
            for b2 in range(0, len(ready_x), 10): exp_retry += await expire(c, ready_x[b2:b2 + 10])
            if exp_retry: exp_retry = await expire(c, exp_retry)
            for b2 in range(0, len(ready_n), 10): n_short_retry += await n_shorten(c, ready_n[b2:b2 + 10])
            if n_short_retry: n_short_retry = await n_shorten(c, n_short_retry)
            n_wait += [(now + a.n_gap, i) for i in ready_n]
            now = time.time()
            rm = [i for t, i in n_wait if t <= now and n_short_ack[i] > 0]
            if rm:
                n_wait = [(t, i) for t, i in n_wait if t > now or n_short_ack[i] == 0]
                for b2 in range(0, len(rm), 10): n_remove_retry += await n_remove(c, rm[b2:b2 + 10])
            if n_remove_retry: n_remove_retry = await n_remove(c, n_remove_retry)
        passes += 1
        await bounded_sleep(max(0.0, a.pass_secs - (time.time() - t_pass)))
    # keys whose turn never came, or whose op never got a definitive answer, stay "unknown"
    return dict(passes=passes, x_unsent=len(exp_queue) + sum(1 for d in due if d[1] == "x"), x_unknown=len(exp_retry),
                n_unsent=len(n_queue) + sum(1 for d in due if d[1] == "n"), n_unknown=len(n_short_retry) + len(n_remove_retry) + len(n_wait),
                bursts_taken=bursts_taken)

async def sdk_check(c):
    out = dict(k_ok=0, k_miss=0, k_corrupt=0, k_touch_false=0, n_ok=0, n_miss=0, n_corrupt=0, n_unknown=0,
               x_absent=0, x_present=0, x_not_yet=0, x_unknown=0, read_err=0, examples=[])
    sem = asyncio.Semaphore(64); now = time.time()
    def ex(*row):
        if len(out["examples"]) < a.max_examples: out["examples"].append(row)
    async def one_k(i):
        async with sem:
            try:
                v = await c.get_bytes(key("k", i))
                if v is None: out["k_miss"] += 1; ex("k_miss", key("k", i), round(now - last_ack[i], 1))
                elif v != val(i): out["k_corrupt"] += 1; ex("k_corrupt", key("k", i))
                else: out["k_ok"] += 1
                if not await c.touch(key("k", i), a.ttl): out["k_touch_false"] += 1; ex("k_touch_false", key("k", i))
                else: last_ack[i] = time.time()
            except Exception: out["read_err"] += 1
    async def one_n(i):
        async with sem:
            if none_ack[i] == 0.0: out["n_unknown"] += 1; return
            try:
                v = await c.get_bytes(key("n", i))
                if v is None: out["n_miss"] += 1; ex("n_miss", key("n", i), round(now - n_short_ack[i], 1))
                elif v != val(i): out["n_corrupt"] += 1; ex("n_corrupt", key("n", i))
                else: out["n_ok"] += 1
            except Exception: out["read_err"] += 1
    async def one_x(i):
        async with sem:
            if exp_ack[i] is None: out["x_unknown"] += 1; return
            t, ttl = exp_ack[i]
            try:
                v = await c.get_bytes(key("x", i))
            except Exception: out["read_err"] += 1; return
            if v is None: out["x_absent"] += 1
            elif t + ttl + 2 > now: out["x_not_yet"] += 1
            else: out["x_present"] += 1; ex("x_present", key("x", i), ttl, round(now - t, 1))
    await asyncio.gather(*[one_k(i) for i in range(a.n)], *[one_n(i) for i in range(a.n)], *[one_x(i) for i in range(a.n)])
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
    fk = await seed(c, "k", a.ttl); fn = await seed(c, "n", 0); fx = await seed(c, "x", 0)
    print(json.dumps(dict(t=int(time.time() - T0), phase="seeded", n=a.n, seed_failed=dict(k=fk, n=fn, x=fx))), flush=True)
    if a.duration > 0: loop.call_later(a.duration, stop.set)
    rep = asyncio.create_task(reporter())
    workers = [asyncio.create_task(worker(w, c)) for w in range(a.workers)]
    await stop.wait()
    print(json.dumps(dict(t=int(time.time() - T0), phase="stopped", counts=counts, anomalies=anom, errors=errors, bursts_signalled=bursts_signalled, settle=a.settle)), flush=True)
    await asyncio.sleep(a.settle)
    done.set()
    wres = await asyncio.gather(*workers)
    rep.cancel()
    agg = {k: sum(r[k] for r in wres) for k in wres[0]}
    out = await sdk_check(c)
    st = c.stats()
    now = time.time()
    n_probative = sum(1 for i in range(a.n) if none_ack[i] and now - n_short_ack[i] > a.n_short + 2)
    print(json.dumps(dict(t=int(time.time() - T0), phase="checked", n=a.n, counts=counts, anomalies=anom, errors=errors,
                          workers=agg, replica_write_failures=st.replica_write_failures, n_probative=n_probative,
                          **out)), flush=True)
    if a.state:
        json.dump(dict(n=a.n, ttl=a.ttl, n_short=a.n_short, short=a.short, t_check=time.time(), seed_time=seed_time,
                       last_ack=last_ack, n_short_ack=n_short_ack, none_ack=none_ack, exp_ack=exp_ack), open(a.state, "w"))
    await c.close()

asyncio.run(main())
