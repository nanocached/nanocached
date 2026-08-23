"""#96 probe: long-lived Python client under address-churn.

Prints one JSON line every REPORT seconds with the client's member count,
the size/contents of the private reconnect-cooldown map, RSS, and op/err
counters.  The cooldown map is white-box (a private attribute) — the point
of the experiment is to see whether RSS alone (black-box) would have
revealed the growth, or only the white-box counter does.
"""
import asyncio, json, os, signal, sys, time, tracemalloc, gc
from nanocached import NanocachedClient

addr = sys.argv[1]
REPORT = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
KEYS = 1000


def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return -1


async def main():
    h, p = addr.rsplit(":", 1)
    TM = os.environ.get("TRACEMALLOC"); snap0 = None
    if TM: tracemalloc.start(25)
    client = await NanocachedClient.connect([(h, int(p))])
    ops = errs = 0
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    for i in range(KEYS):
        try:
            await client.set(f"k{i}", f"v{i}")
        except Exception:
            errs += 1

    t0 = time.monotonic(); last = t0; i = 0
    while not stop.is_set():
        try:
            await client.get(f"k{i % KEYS}")
            ops += 1
        except Exception:
            errs += 1
        i += 1
        await asyncio.sleep(0.002)
        now = time.monotonic()
        if now - last >= REPORT:
            last = now
            cd = client._redial_cooldowns
            if TM and snap0 is None and now - t0 > 20: gc.collect(); snap0 = tracemalloc.take_snapshot()
            tm_kb = tracemalloc.get_traced_memory()[0] // 1024 if TM else -1
            print(json.dumps({
                "t": round(now - t0),
                "members": len(client._members),
                "cooldowns": len(cd),
                "cooldown_addrs": sorted(cd),
                "redials_inflight": len(client._redials),
                "rss_kb": rss_kb(), "traced_kb": tm_kb,
                "ops": ops, "errs": errs,
            }), flush=True)
    await client.close()
    if TM and snap0 is not None:
        gc.collect(); snap1 = tracemalloc.take_snapshot()
        for st in snap1.compare_to(snap0, "traceback")[:8]:
            print("TM +%dkB (+%d blocks)" % (st.size_diff // 1024, st.count_diff), flush=True)
            for line in st.traceback.format()[-6:]: print("   ", line, flush=True)
        print("TM objects by type:", flush=True)
        import collections; c = collections.Counter(type(o).__name__ for o in gc.get_objects()); print("   ", c.most_common(12), flush=True)
    print(json.dumps({"final": True, "cooldowns": len(client._redial_cooldowns),
                      "cooldown_addrs": sorted(client._redial_cooldowns),
                      "rss_kb": rss_kb(), "ops": ops, "errs": errs}), flush=True)


asyncio.run(main())
