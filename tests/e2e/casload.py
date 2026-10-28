"""Compare-and-set workload with an oracle, for the churn scenarios (k under
join, decommission and kill-restart).

casload.py --addr host:port [--n 2000] [--workers 4] [--turns 3]
           [--bursts 0 --burst-secs 12] [--span 900] [--duration 0]
           [--settle 20] [--op-timeout 15] [--state logs/casstate.json]

Two key classes and two clients (their own connections, so their own ring
views and their own moments of learning about a join); the two contenders
of every turn run concurrently, one on each client:

  cc<i>  chain: seeded "S;". A turn is two contenders, each appending its
         own unique tag ("<tag>;") with get_with_token + replace until the
         replace answers True (a False is a lost race: read again; a
         failure is ambiguous, the tag may or may not have been applied,
         and the contender goes on with a new tag). Asserted at the end:
         every tag whose replace answered True is in the key's value
         exactly once, every tag in the value is one that was tried, no key
         vanished. A tag missing from the value (cc_lost_ack) is a lost
         update: a lagging forward in a join's or a kill's rejoin window
         rewound the key and the next replace succeeded on the older value.
         That is accepted behaviour (docs/architecture.html), so it is
         counted and printed, not asserted.
  cp<i>  race: absent before its turn. Two put_if_absent, one per client,
         with different values and no retry. At most one may answer True
         (a second is a double win); the value must be one of the two, and
         the winner's if there is one; a key one of them found present
         (False) or wrote (True) must not have disappeared.

Turns are spread like touchload.py's: evenly over --span seconds, or with
--bursts N one N-th of each class over the --burst-secs after each SIGUSR1
(the scenario sends one as each round starts). A turn that never got an
answer, or never came, is reported as unknown, not asserted.

On SIGTERM (or after --duration) no new turns are taken and turns in flight
are finished; after --settle seconds every key is read back through the
SDK, one JSON line is printed (phase "checked") and --state is written for
cascheck.py's per-owner raw check.
"""
import argparse, asyncio, json, random, signal, time
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--workers", type=int, default=4); ap.add_argument("--turns", type=int, default=3)
ap.add_argument("--span", type=float, default=900)
ap.add_argument("--bursts", type=int, default=0); ap.add_argument("--burst-secs", type=float, default=12)
ap.add_argument("--duration", type=float, default=0); ap.add_argument("--settle", type=float, default=20)
ap.add_argument("--op-timeout", type=float, default=15); ap.add_argument("--max-attempts", type=int, default=30)
ap.add_argument("--report-every", type=float, default=30); ap.add_argument("--state", default="")
ap.add_argument("--max-examples", type=int, default=50); ap.add_argument("--oplog", default="")
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
T0 = time.time()
stop = asyncio.Event()    # SIGTERM / --duration: no more turns, turns in flight are finished
done = asyncio.Event()    # settle over: workers exit, then the SDK check
bursts_signalled = 0

def key(cls, i): return f"c{cls}{i}"
def now_s(): return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

tag_seq = 0
def new_tag():
    global tag_seq
    tag_seq += 1
    return f"t{tag_seq}"

# chain: per key, tags acknowledged (replace answered True) and tags that may have been applied
acked = [[] for _ in range(a.n)]
maybe = [[] for _ in range(a.n)]
cc_unfinished = [0] * a.n          # contenders that ran out of attempts
oplog = []                         # (time, client, key index, tag, base length, outcome): written to --oplog when given
# race: per key, what each contender's put_if_absent came to ("T", "F" or "?" for no answer)
race = [None] * a.n
counts = dict(cc_turns=0, cc_acked=0, cc_conflicts=0, cp_turns=0, ops_err=0)
anom = dict(chain_miss=0, chain_read_err=0)
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

def op(coro): return asyncio.wait_for(coro, a.op_timeout)

async def seed(c):
    sem = asyncio.Semaphore(64); failed = 0
    async def one(i):
        nonlocal failed
        async with sem:
            for _ in range(5):
                try:
                    await op(c.set(key("c", i), b"S;")); return
                except Exception as e:
                    err(e, "seed-c"); await asyncio.sleep(0.2)
            failed += 1
    await asyncio.gather(*[one(i) for i in range(a.n)])
    return failed

async def contender(c, i, ci):
    """Append one tag to the chain; True once the replace answered True."""
    k = key("c", i)
    for _ in range(a.max_attempts):
        try:
            got = await op(c.get_with_token(k))
        except Exception as e:
            err(e, "cc-read"); await asyncio.sleep(0.3); continue
        if got is None:
            note("chain_miss", key=k); return False
        value, token = got
        tag = new_tag()
        try:
            ok = await op(c.replace(k, token, value + tag.encode() + b";"))
        except Exception as e:
            err(e, "cc-replace"); maybe[i].append(tag); oplog.append((time.time(), ci, i, tag, len(value), "err:" + type(e).__name__))
            await asyncio.sleep(0.3); continue
        oplog.append((time.time(), ci, i, tag, len(value), "T" if ok else "F"))
        if ok:
            acked[i].append(tag); counts["cc_acked"] += 1; return True
        counts["cc_conflicts"] += 1
        await asyncio.sleep(random.uniform(0, 0.05))
    cc_unfinished[i] += 1
    return False

async def cc_turn(clients, i):
    counts["cc_turns"] += 1
    await asyncio.gather(*[contender(c, i, ci) for ci, c in enumerate(clients)])

async def cp_turn(clients, i):
    counts["cp_turns"] += 1
    k = key("p", i)
    async def one(c, v):
        try: return "T" if await op(c.put_if_absent(k, v)) else "F"
        except Exception as e: err(e, "cp-put"); return "?"
    res = await asyncio.gather(*[one(c, f"{i}:{n}".encode()) for n, c in enumerate(clients)])
    race[i] = "".join(res)

async def worker(w, clients):
    mine = list(range(w, a.n, a.workers))
    queues = {"c": random.sample(mine * a.turns, len(mine) * a.turns), "p": random.sample(mine, len(mine))}
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
                    share = -(-(len(mine) * (a.turns if cls == "c" else 1)) // a.bursts)
                    released = [q.pop() for _ in range(min(share, len(q)))]
                    due += [(now + (j + 1) / len(released) * a.burst_secs, cls, i) for j, i in enumerate(released)]
            ready = [(cls, i) for t, cls, i in due if t <= now]
            if ready: due = [d for d in due if d[0] > now]
            for cls, i in ready:
                if cls == "c": await cc_turn(clients, i)
                else: await cp_turn(clients, i)
        await bounded_sleep(0.2)
    return dict(c_unsent=len(queues["c"]) + sum(1 for d in due if d[1] == "c"),
                p_unsent=len(queues["p"]) + sum(1 for d in due if d[1] == "p"), bursts_taken=bursts_taken)

def tags_of(v):
    parts = v.decode("latin-1").split(";")
    return parts[0], [p for p in parts[1:] if p]   # head ("S"), tags

async def sdk_check(c):
    out = dict(cc_ok=0, cc_unturned=0, cc_vanished=0, cc_lost_ack=0, cc_lost_ack_tags=0, cc_dup=0, cc_foreign=0, cc_head=0,
               cp_ok=0, cp_unturned=0, cp_unknown=0, cp_double_win=0, cp_lost=0, cp_corrupt=0, cp_winner_overwritten=0,
               cc_unfinished=sum(1 for x in cc_unfinished if x), read_err=0, examples=[])
    sem = asyncio.Semaphore(64)
    def ex(*row):
        if len(out["examples"]) < a.max_examples: out["examples"].append(row)
    async def one_c(i):
        async with sem:
            if not acked[i] and not maybe[i] and not cc_unfinished[i]: out["cc_unturned"] += 1; return
            try: v = await op(c.get_bytes(key("c", i)))
            except Exception: out["read_err"] += 1; return
            if v is None: out["cc_vanished"] += 1; ex("cc_vanished", key("c", i), acked[i], maybe[i]); return
            head, tags = tags_of(v)
            bad = False
            if head != "S": out["cc_head"] += 1; ex("cc_head", key("c", i), v[:32].decode("latin-1")); bad = True
            if len(set(tags)) != len(tags): out["cc_dup"] += 1; ex("cc_dup", key("c", i), tags); bad = True
            gone = [t for t in acked[i] if t not in tags]
            if gone: out["cc_lost_ack"] += 1; out["cc_lost_ack_tags"] += len(gone); ex("cc_lost_ack", key("c", i), gone, tags); bad = True
            if any(t not in acked[i] and t not in maybe[i] for t in tags): out["cc_foreign"] += 1; ex("cc_foreign", key("c", i), tags); bad = True
            if not bad: out["cc_ok"] += 1
    async def one_p(i):
        async with sem:
            r = race[i]
            if r is None: out["cp_unturned"] += 1; return
            try: v = await op(c.get_bytes(key("p", i)))
            except Exception: out["read_err"] += 1; return
            vals = [f"{i}:{n}".encode() for n in range(len(r))]
            wins = [n for n, x in enumerate(r) if x == "T"]
            if len(wins) > 1: out["cp_double_win"] += 1; ex("cp_double_win", key("p", i), r, v and v[:16].decode("latin-1"))
            if v is None:
                if wins or "F" in r: out["cp_lost"] += 1; ex("cp_lost", key("p", i), r)
                else: out["cp_unknown"] += 1
            elif v not in vals: out["cp_corrupt"] += 1; ex("cp_corrupt", key("p", i), r, v[:16].decode("latin-1"))
            elif len(wins) == 1 and v != vals[wins[0]]: out["cp_winner_overwritten"] += 1; ex("cp_winner_overwritten", key("p", i), r, v.decode())
            elif len(wins) <= 1: out["cp_ok"] += 1
    await asyncio.gather(*[one_c(i) for i in range(a.n)], *[one_p(i) for i in range(a.n)])
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
    clients = [await NanocachedClient.connect(addrs), await NanocachedClient.connect(addrs)]
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
                          workers=agg, replica_write_failures=sum(s.replica_write_failures for s in st), **out)), flush=True)
    if a.state:
        json.dump(dict(n=a.n, acked=acked, maybe=maybe, race=race), open(a.state, "w"))
    if a.oplog: json.dump(oplog, open(a.oplog, "w"))
    for c in clients: await c.close()

asyncio.run(main())
