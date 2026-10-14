"""load.py with per-miss diagnostics (restart.sh).

Same workload as tests/e2e/load.py. On every miss it prints one JSON line
(prefix MISS) with the absolute time, worker, key, the client's current
ring members (name -> address) and the owners it routed the key to, so the
miss can be placed against the node logs. The node-list refresh is hooked
to print one line (prefix RING) per refresh. The first PROBE_MAX misses are
re-read after PROBE_DELAY seconds through the same client (did the key come
back?). Reports every --report-every seconds like load.py.
"""
import argparse, asyncio, collections, json, random, time
from datetime import datetime, timezone
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--conns", type=int, default=8)
ap.add_argument("--duration", type=float, default=30); ap.add_argument("--keyspace", type=int, default=10000)
ap.add_argument("--value-size", type=int, default=200); ap.add_argument("--get-ratio", type=float, default=0.7)
ap.add_argument("--reconnect-on-error", action="store_true"); ap.add_argument("--report-every", type=float, default=30)
ap.add_argument("--label", default="load"); ap.add_argument("--hedge", type=float, default=0.0)
ap.add_argument("--probe-max", type=int, default=60); ap.add_argument("--probe-delay", type=float, default=1.0)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
val = ("x" * a.value_size).encode()
res = dict(ops=0, get=0, set=0, miss=0, corrupt=0, err_total=0, errors=collections.Counter(), sdk_stats=[])
probes_left = a.probe_max


def now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def members_of(c):
    try:
        return {name: m.address for name, m in c._members.items()}
    except Exception:
        return {}


def owners_of(c, k):
    try:
        return c._ring.owners(k.encode(), c._replication, namespace=b"")
    except Exception:
        return []


# Hook the refresh so every ring change the client sees is on record.
_orig_refresh = NanocachedClient._refresh_node_list


async def _logged_refresh(self):
    before = sorted(members_of(self))
    await _orig_refresh(self)
    after = members_of(self)
    if sorted(after) != before:
        print("RING", now(), json.dumps(dict(client=id(self) % 100000, members=after)), flush=True)


NanocachedClient._refresh_node_list = _logged_refresh


async def worker(idx):
    global probes_left
    kw = {}
    if a.hedge > 0:
        kw["read_hedge_after"] = a.hedge
    c = await NanocachedClient.connect(addrs, **kw)
    print("RING", now(), json.dumps(dict(client=id(c) % 100000, members=members_of(c), new=True)), flush=True)
    try:
        end = time.time() + a.duration
        while time.time() < end:
            k = f"k{random.randrange(a.keyspace)}"
            try:
                if random.random() < a.get_ratio:
                    v = await c.get_bytes(k); res["get"] += 1
                    if v is None:
                        res["miss"] += 1
                        t_miss = now(); owners = owners_of(c, k); mem = members_of(c)
                        rec = dict(worker=idx, client=id(c) % 100000, key=k, owners=owners,
                                   owner_addrs=[mem.get(o) for o in owners], ring_size=len(mem))
                        print("MISS", t_miss, json.dumps(rec), flush=True)
                        if probes_left > 0:
                            probes_left -= 1
                            await asyncio.sleep(a.probe_delay)
                            try:
                                v2 = await c.get_bytes(k)
                                r2 = "hit" if v2 is not None else "miss"
                            except Exception as e:
                                r2 = f"err:{type(e).__name__}"
                            print("PROBE", now(), json.dumps(dict(key=k, first_miss=t_miss, reread=r2,
                                  owners_now=owners_of(c, k), ring_size=len(members_of(c)))), flush=True)
                else:
                    await c.set(k, val); res["set"] += 1
                res["ops"] += 1
            except Exception as e:
                res["err_total"] += 1; res["errors"][type(e).__name__] += 1
                print("ERR", now(), json.dumps(dict(worker=idx, key=k, error=type(e).__name__, msg=str(e)[:120])), flush=True)
                if a.reconnect_on_error:
                    try:
                        await c.close()
                    except Exception:
                        pass
                    c = await NanocachedClient.connect(addrs, **kw)
                    print("RING", now(), json.dumps(dict(client=id(c) % 100000, members=members_of(c), new=True, worker=idx)), flush=True)
    finally:
        try:
            res["sdk_stats"].append(str(c.stats()))
        except Exception:
            pass
        await c.close()


async def reporter():
    last = 0; t0 = time.time()
    while True:
        await asyncio.sleep(a.report_every)
        n = res["ops"]
        print(json.dumps(dict(t=int(time.time() - t0), at=now(), ops_s=int((n - last) / a.report_every),
                              err=res["err_total"], miss=res["miss"])), flush=True)
        last = n


async def main():
    rep = asyncio.ensure_future(reporter())
    await asyncio.gather(*[worker(i) for i in range(a.conns)])
    rep.cancel()
    print(json.dumps(dict(label=a.label, ops=res["ops"], ops_s=int(res["ops"] / a.duration), get=res["get"], set=res["set"],
                          miss=res["miss"], corrupt=0, err_total=res["err_total"], errors=dict(res["errors"]),
                          samples=[], sdk_stats=res["sdk_stats"][:1])))


asyncio.run(main())
