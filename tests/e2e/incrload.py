"""INCR workload with an oracle, for the churn scenarios (i under join,
decommission and kill-restart).

incrload.py --addr host:port [--n 2000] [--workers 4] [--turns 3]
            [--bursts 0 --burst-secs 12] [--span 900] [--duration 0]
            [--settle 20] [--op-timeout 15] [--state logs/incrstate.json]

Two key classes, every key seeded "0" (INCR never creates a key) and two
clients with their own connections (their own ring views):

  ia<i>  parallel: a turn is four concurrent increments by one, two on each
         client. An increment that answers is acknowledged; one that fails
         (the request may or may not have reached the primary, and INCR is
         never retried) is "maybe". Asserted at the end: no key vanishes,
         every value is a decimal integer, and neither the value nor any
         answer exceeds acknowledged + maybe (more would be an increment
         applied twice or one nobody sent).
  is<i>  serial: a turn is four increments one after the other from one
         client; each answer must follow the previous one by exactly one,
         plus one for every maybe in between. An answer above that is a
         phantom increment (asserted); an answer at or below the previous
         one (is_rewind) is printed, not asserted.

A value below the number of acknowledged increments (ia_short, is_short,
with the missing amount in *_lost_min) and answers that repeat on one key
(ia_dup_returns) are lost increments: a lagging forward in a join's or a
kill's rejoin window rewound the counter. That is accepted behaviour
(docs/architecture.html), so it is counted and printed, not asserted. So is
a replica left behind the primary by two clients incrementing one key at
once (only incrcheck.py sees it: the SDK reads the primary); --clients 1
runs every increment through one client, which does not show it.

Turns are spread like touchload.py's: evenly over --span seconds, or with
--bursts N one N-th of each class over the --burst-secs after each SIGUSR1
(the scenario sends one as each round starts). A key whose turns never came
is reported as unturned, not asserted.

On SIGTERM (or after --duration) no new turns are taken and turns in flight
are finished; after --settle seconds every key is read back through the
SDK, one JSON line is printed (phase "checked") and --state is written for
incrcheck.py's per-owner raw check.
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--workers", type=int, default=4); ap.add_argument("--turns", type=int, default=3)
ap.add_argument("--span", type=float, default=900)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=20)
ap.add_argument("--op-timeout", type=float, default=15); ap.add_argument("--clients", type=int, default=2)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more turns, turns in flight are finished
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0
SERIAL_STEPS = 4

def key(cls, i): return f"i{cls}{i}"
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

# per key: increments acknowledged, increments that failed (may have been applied), the answers
acked = {c: [0] * a.n for c in "as"}
maybe = {c: [0] * a.n for c in "as"}
rets = {c: [[] for _ in range(a.n)] for c in "as"}
maybe_mark = [0] * a.n      # serial keys: failed increments already accounted for by the last answer
counts = dict(ia_turns=0, is_turns=0, ops_err=0)
anom = dict(counter_miss=0, not_numeric=0, is_phantom=0, is_rewind=0, is_skip=0)
errors, examples = {}, {}

rewind_at = {}    # is_rewind per 10 s of wall clock, "HH:MM:S0", for lining up with the rounds

def note(kind, **kw):
    anom[kind] = anom.get(kind, 0) + 1
    if kind == "is_rewind":
        b = time.strftime("%H:%M:%S")[:-1] + "0"; rewind_at[b] = rewind_at.get(b, 0) + 1
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

def op(coro): return asyncio.wait_for(coro, a.op_timeout)

async def seed(c):
    sem = asyncio.Semaphore(64); failed = 0
    async def one(cls, i):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await op(c.set(key(cls, i), b"0")); return
                except Exception as e:
                    err(e, "seed-" + cls); await asyncio.sleep(0.2)
            failed += 1
    await asyncio.gather(*[one(cls, i) for cls in "as" for i in range(a.n)])
    return failed

async def incr_once(c, cls, i):
    """One increment by one; the answer, or None when it failed (counted as maybe) or the key was gone."""
    k = key(cls, i)
    try:
        r = await op(c.incr(k))
    except Exception as e:
        err(e, "incr-" + cls)
        if type(e).__name__ == "NotNumericError": note("not_numeric", key=k)
        maybe[cls][i] += 1; return None
    if r is None:
        note("counter_miss", key=k); return None
    acked[cls][i] += 1; rets[cls][i].append(r)
    return r

async def ia_turn(clients, i):
    counts["ia_turns"] += 1
    await asyncio.gather(*[incr_once(c, "a", i) for c in clients for _ in range(4 // len(clients))])

async def is_turn(clients, i):
    counts["is_turns"] += 1
    c = clients[i % len(clients)]
    for _ in range(SERIAL_STEPS):
        prev = rets["s"][i][-1] if rets["s"][i] else None
        r = await incr_once(c, "s", i)
        if r is None: continue
        # increments that failed since the previous answer may each have been applied
        allowed = maybe["s"][i] - maybe_mark[i]; maybe_mark[i] = maybe["s"][i]
        if prev is None: continue
        gap = r - prev
        if gap <= 0: note("is_rewind", key=key("s", i), prev=prev, got=r)
        elif gap - 1 > allowed: note("is_phantom", key=key("s", i), prev=prev, got=r, maybes=allowed)
        elif gap > 1: note("is_skip", key=key("s", i), prev=prev, got=r)

async def worker(w, clients):
    mine = list(range(w, a.n, a.workers))
    queues = {"a": random.sample(mine * a.turns, len(mine) * a.turns), "s": random.sample(mine * a.turns, len(mine) * a.turns)}
    due = []           # (due time, class, key index): released turns, taken once due
    bursts_taken = 0
    t_run = time.time()
    if a.bursts <= 0:
        for cls, q in queues.items():
            due += [(t_run + (j + 1) / len(q) * a.span, cls, i) for j, i in enumerate(q)]
            q.clear()
    while not done.is_set():
        now = time.time()
        if not stop.is_set():
            if a.bursts > 0 and bursts_taken < bursts_signalled and any(queues.values()):
                bursts_taken += 1
                for cls, q in queues.items():
                    share = -(-(len(mine) * a.turns) // a.bursts)
                    released = [q.pop() for _ in range(min(share, len(q)))]
                    due += [(now + (j + 1) / len(released) * a.burst_secs, cls, i) for j, i in enumerate(released)]
            ready = [(cls, i) for t, cls, i in due if t <= now]
            if ready: due = [d for d in due if d[0] > now]
            for cls, i in ready:
                if cls == "a": await ia_turn(clients, i)
                else: await is_turn(clients, i)
        await bounded_sleep(0.2)
    return dict(a_unsent=len(queues["a"]) + sum(1 for d in due if d[1] == "a"),
                s_unsent=len(queues["s"]) + sum(1 for d in due if d[1] == "s"), bursts_taken=bursts_taken)

async def sdk_check(c):
    out = dict(read_err=0, examples=[])
    for cls in "as":
        for k in ("ok", "unturned", "vanished", "corrupt", "over", "ret_over", "short", "lost_min", "dup_returns"):
            out[f"i{cls}_{k}"] = 0
    sem = asyncio.Semaphore(64)
    def ex(*row):
        if len(out["examples"]) < a.max_examples: out["examples"].append(row)
    async def one(cls, i):
        p = f"i{cls}_"
        async with sem:
            if not acked[cls][i] and not maybe[cls][i]: out[p + "unturned"] += 1; return
            try: v = await op(c.get_bytes(key(cls, i)))
            except Exception: out["read_err"] += 1; return
            hi = acked[cls][i] + maybe[cls][i]
            bad = False
            if v is None: out[p + "vanished"] += 1; ex(p + "vanished", key(cls, i), acked[cls][i], maybe[cls][i]); return
            try: val = int(v)
            except ValueError: out[p + "corrupt"] += 1; ex(p + "corrupt", key(cls, i), v[:16].decode("latin-1")); return
            if val > hi: out[p + "over"] += 1; ex(p + "over", key(cls, i), val, acked[cls][i], maybe[cls][i]); bad = True
            if rets[cls][i] and max(rets[cls][i]) > hi: out[p + "ret_over"] += 1; ex(p + "ret_over", key(cls, i), max(rets[cls][i]), hi); bad = True
            if val < acked[cls][i]:
                out[p + "short"] += 1; out[p + "lost_min"] += acked[cls][i] - val; ex(p + "short", key(cls, i), val, acked[cls][i], maybe[cls][i], sorted(rets[cls][i])); bad = True
            if len(set(rets[cls][i])) != len(rets[cls][i]): out[p + "dup_returns"] += 1; bad = True
            if not bad: out[p + "ok"] += 1
    await asyncio.gather(*[one(cls, i) for cls in "as" for i in range(a.n)])
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
    clients = [await NanocachedClient.connect(addrs) for _ in range(a.clients)]
    fc = await seed(clients[0])
    print(json.dumps(dict(t=int(time.time() - T0), phase="seeded", n=a.n, seed_failed=fc)), flush=True)
    if a.duration > 0: loop.call_later(a.duration, stop.set)
    rep = asyncio.create_task(reporter())
    workers = [asyncio.create_task(worker(w, clients)) for w in range(a.workers)]
    await stop.wait()
    print(json.dumps(dict(t=int(time.time() - T0), phase="stopped", counts=counts, anomalies=anom, errors=errors,
                          bursts_signalled=bursts_signalled, settle=a.settle)), flush=True)
    await asyncio.sleep(a.settle)
    done.set()
    wres = await asyncio.gather(*workers)
    rep.cancel()
    agg = {k: sum(r[k] for r in wres) for k in wres[0]}
    out = await sdk_check(clients[0])
    st = [c.stats() for c in clients]
    print(json.dumps(dict(t=int(time.time() - T0), phase="checked", n=a.n, counts=counts, anomalies=anom, errors=errors,
                          workers=agg, rewind_at=rewind_at, replica_write_failures=sum(s.replica_write_failures for s in st),
                          acked_a=sum(acked["a"]), maybe_a=sum(maybe["a"]), acked_s=sum(acked["s"]), maybe_s=sum(maybe["s"]), **out)), flush=True)
    if a.state:
        json.dump(dict(n=a.n, acked=acked, maybe=maybe), open(a.state, "w"))
    for c in clients: await c.close()

asyncio.run(main())
