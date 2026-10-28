"""Set-with-TTL workload with an oracle, for the churn scenarios (S with a TTL
under join, decommission and kill-restart).

ttlload.py --addr host:port [--n 2000] [--workers 4] [--ttl-short 20 --ttl-long 40]
           [--bursts 0 --burst-secs 12] [--span 900] [--duration 0]
           [--early-frac 0.5] [--settle 30] [--state logs/ttlstate.json]

Two key classes, every value "<i>:1":

  ts<i>  new key: set with a TTL of --ttl-short (two keys in three) or
         --ttl-long. Absent in the store before its turn.
  tp<i>  overwritten: seeded without a TTL, then set again with the TTL. The
         copy a node got from the seed must not keep the key alive past the
         new TTL (a lagging forward of the older write would).

  Both are set until the client has an answer. A retry restarts the TTL, so
  a key's expiry lies between first-attempt + TTL and last-ack + TTL.
  Asserted (the scenario's reads, not the server's word):
    early    a key read at --early-frac of its TTL after the first attempt
             (and after the ack) must answer with its value: a miss is a
             copy lost or expired before its time.
    lingering  once last-ack + TTL + --margin has passed, no node may
             answer the key with a value: the SDK read-back at the end
             (x_present) and ttlcheck.py's raw read of every live node. A
             copy that is still there kept a longer TTL than the one set
             (a handoff, a pass or a window forward carried it without the
             remaining TTL, or the older no-TTL value back).

Turns are spread like touchload.py's: evenly over --span seconds, or with
--bursts N one N-th of each class over the --burst-secs after each SIGUSR1
(the scenario sends one as each round starts). A key whose turn had not come,
or whose set never got an answer when the run stops, is reported as unknown,
not asserted. On SIGTERM (or after --duration) no new turns are taken;
after --settle seconds every key is read back through the SDK, one JSON line
is printed (phase "checked") and --state is written for ttlcheck.py.
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--workers", type=int, default=4)
ap.add_argument("--ttl-short", type=int, default=20); ap.add_argument("--ttl-long", type=int, default=40)
ap.add_argument("--early-frac", type=float, default=0.5); ap.add_argument("--margin", type=float, default=10)
ap.add_argument("--span", type=float, default=900)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=30)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more turns
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0

def key(cls, i): return f"t{cls}{i}"
def val(i): return f"{i}:1".encode()
def ttl_of(i): return a.ttl_long if i % 3 == 2 else a.ttl_short
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

# per key, per class: first attempt, the time of the last acknowledgement (0 = none yet)
first = {c: [0.0] * a.n for c in "sp"}
ack = {c: [0.0] * a.n for c in "sp"}
counts = dict(set_s=0, set_p=0, early_reads=0, early_skipped_late=0, ops_err=0)
anom = dict(early_miss=0, early_corrupt=0, set_retry=0)
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

async def seed(c):
    sem = asyncio.Semaphore(64); failed = 0
    async def one(i):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await c.set(key("p", i), val(i)); return
                except Exception as e:
                    err(e, "seed-p"); await asyncio.sleep(0.2)
            failed += 1
    await asyncio.gather(*[one(i) for i in range(a.n)])
    return failed

async def set_ttl(c, cls, i):
    t = time.time()
    if first[cls][i] == 0.0: first[cls][i] = t
    try:
        await c.set(key(cls, i), val(i), ttl_seconds=ttl_of(i))
        ack[cls][i] = time.time(); counts["set_" + cls] += 1; return True
    except Exception as e:
        err(e, "set-" + cls); anom["set_retry"] += 1; return False

readers = set()   # pending early reads; kept so the tasks are not collected

async def early_read_at(c, t_read, cls, i):
    await bounded_sleep(max(0.0, t_read - time.time()))
    if not done.is_set(): await early_read(c, cls, i)

async def early_read(c, cls, i):
    # A worker stuck in retries (a node just killed) reaches its reads late; a read
    # that would only start after the earliest expiry, first attempt + TTL, proves
    # nothing, and one that starts within a second of it can race the expiry.
    if time.time() > first[cls][i] + ttl_of(i) - 1.0:
        counts["early_skipped_late"] += 1; return
    counts["early_reads"] += 1
    try: v = await c.get_bytes(key(cls, i))
    except Exception as e: err(e, "early-read"); return
    if time.time() > first[cls][i] + ttl_of(i) - 0.5:
        counts["early_skipped_late"] += 1; counts["early_reads"] -= 1; return
    if v is None: note("early_miss", key=key(cls, i), ttl=ttl_of(i))
    elif v != val(i): note("early_corrupt", key=key(cls, i), got=v[:16].decode("latin-1"))

async def worker(w, c):
    mine = list(range(w, a.n, a.workers))
    queues = {cls: random.sample(mine, len(mine)) for cls in "sp"}
    due = []           # (due time, class, key index): released turns, taken once due
    retry = []         # (class, key index): sets without an answer yet
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
                bursts_taken += 1; share = -(-len(mine) // a.bursts)
                for cls, q in queues.items():
                    released = [q.pop() for _ in range(min(share, len(q)))]
                    due += [(now + (j + 1) / len(released) * a.burst_secs, cls, i) for j, i in enumerate(released)]
            ready = [(cls, i) for t, cls, i in due if t <= now]
            if ready: due = [d for d in due if d[0] > now]
            for cls, i in ready:
                if await set_ttl(c, cls, i):
                    # The earliest the key can expire is first + TTL; read at a fraction of
                    # that, from a task of its own so that a worker stuck in retries (a
                    # node just killed) does not hold the read back past the expiry.
                    t_read = first[cls][i] + a.early_frac * ttl_of(i)
                    if t_read > time.time(): readers.add(asyncio.create_task(early_read_at(c, t_read, cls, i)))
                else:
                    retry.append((cls, i))
        retry = [(cls, i) for cls, i in retry if not await set_ttl(c, cls, i)]
        await bounded_sleep(0.2)
    return dict(s_unsent=len(queues["s"]) + sum(1 for d in due if d[1] == "s"),
                p_unsent=len(queues["p"]) + sum(1 for d in due if d[1] == "p"),
                set_unknown=len(retry), bursts_taken=bursts_taken)

async def sdk_check(c):
    out = dict(x_absent=0, x_present=0, x_unknown=0, x_not_yet=0, read_err=0, examples=[])
    sem = asyncio.Semaphore(64)
    now = time.time()
    async def one(cls, i):
        async with sem:
            if ack[cls][i] == 0.0: out["x_unknown"] += 1; return
            if now < ack[cls][i] + ttl_of(i) + a.margin: out["x_not_yet"] += 1; return
            try: v = await c.get_bytes(key(cls, i))
            except Exception: out["read_err"] += 1; return
            if v is None: out["x_absent"] += 1
            else:
                out["x_present"] += 1
                if len(out["examples"]) < a.max_examples: out["examples"].append((key(cls, i), v[:16].decode("latin-1")))
    await asyncio.gather(*[one(cls, i) for cls in "sp" for i in range(a.n)])
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
    fp = await seed(c)
    print(json.dumps(dict(t=int(time.time() - T0), phase="seeded", n=a.n, seed_failed=fp)), flush=True)
    if a.duration > 0: loop.call_later(a.duration, stop.set)
    rep = asyncio.create_task(reporter())
    workers = [asyncio.create_task(worker(w, c)) for w in range(a.workers)]
    await stop.wait()
    print(json.dumps(dict(t=int(time.time() - T0), phase="stopped", counts=counts, anomalies=anom, errors=errors,
                          bursts_signalled=bursts_signalled, settle=a.settle)), flush=True)
    await asyncio.sleep(a.settle)
    done.set()
    await asyncio.gather(*readers, return_exceptions=True)
    wres = await asyncio.gather(*workers)
    rep.cancel()
    agg = {k: sum(r[k] for r in wres) for k in wres[0]}
    out = await sdk_check(c)
    st = c.stats()
    print(json.dumps(dict(t=int(time.time() - T0), phase="checked", n=a.n, counts=counts, anomalies=anom, errors=errors,
                          workers=agg, replica_write_failures=st.replica_write_failures, **out)), flush=True)
    if a.state:
        json.dump(dict(n=a.n, t_check=time.time(), margin=a.margin, ttl_short=a.ttl_short, ttl_long=a.ttl_long,
                       ack_s=ack["s"], ack_p=ack["p"]), open(a.state, "w"))
    await c.close()

asyncio.run(main())
