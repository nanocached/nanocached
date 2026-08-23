"""#91 probe: hedge leg registered after close() has drained _hedged_reads.

close() promises (its own docstring) to return only after every in-flight
hedge leg has finished and the connections are torn down.  But _read_hedged's
start() adds a leg to _hedged_reads with no _closed re-check, and the
get()-caller task keeps spawning legs in its while-loop independently of
close() draining that set.  So a leg can be registered — and can dial/use a
connection — after close() has already drained the set and torn down.

Detection, per iteration (fresh client each time):
  * spawn several concurrent get() tasks with read_hedge_after≈1ms so legs
    churn constantly;
  * after a randomised sub-ms delay, call close() concurrently;
  * the instant close() returns, sample len(client._hedged_reads).  Non-zero
    == a leg outlived close()'s drain (contract violation).
  * also count legs whose underlying op runs after _teardown fired, via a
    tripwire on the member connections' close().
"""
import asyncio, json, sys, time
from nanocached import NanocachedClient
from nanocached import _connection as _conn

addr = sys.argv[1]
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 400
CONC = int(sys.argv[3]) if len(sys.argv) > 3 else 6

# deterministic-ish jitter without Math.random (varies by iteration)
def jitter(i):
    return ((i * 2654435761) % 997) / 997.0  # 0..1


async def one(i, host, port):
    leftover = 0
    post_teardown_io = 0
    client = await NanocachedClient.connect([(host, int(port))], read_hedge_after=0.001)
    # seed a few keys so gets hit values (hits still spawn hedge legs at 1ms)
    for k in range(6):
        try:
            await client.set(f"h{k}", "v")
        except Exception:
            pass

    stop = False

    async def hammer():
        j = 0
        while not stop:
            try:
                await client.get(f"h{j % 6}")
            except Exception:
                pass
            j += 1
            # yield every iteration: after close() sets _closed, get() raises
            # AlreadyClosedError synchronously (no await), so without this the
            # loop would spin without ever ceding to the event loop.
            await asyncio.sleep(0)

    # tripwire: once _teardown runs, any further connection use is post-close
    torn = {"at": None}
    post_io = {"n": 0}
    orig_teardown = client._teardown
    def teardown_spy():
        torn["at"] = time.monotonic()
        orig_teardown()
    client._teardown = teardown_spy
    # a leg doing connection work after teardown is the actual harm
    orig_mc = client._member_connection
    async def mc_spy(name):
        if torn["at"] is not None:
            post_io["n"] += 1
        return await orig_mc(name)
    client._member_connection = mc_spy
    # the decisive signal: a leg added to _hedged_reads AFTER teardown began
    # (i.e. after close() finished draining that very set) is a registration
    # the drain can never have awaited — the exact contract gap.
    add_after_drain = {"n": 0}
    class SpySet(set):
        def add(self, task):
            if torn["at"] is not None:
                add_after_drain["n"] += 1
            super().add(task)
    client._hedged_reads = SpySet(client._hedged_reads)

    tasks = [asyncio.ensure_future(hammer()) for _ in range(CONC)]
    # let legs build up
    await asyncio.sleep(0.02 + 0.01 * jitter(i))
    pending_at_close = len(client._hedged_reads)
    close_hung = False
    try:
        await asyncio.wait_for(asyncio.shield(client.close()), timeout=5)
    except asyncio.TimeoutError:
        close_hung = True
    leftover = len(client._hedged_reads)
    torn_at = torn["at"]
    # Let stragglers run: a get() that passed _before_operation before close()
    # set _closed keeps going (nothing between _read and start() re-checks it),
    # so it reaches start() and registers a leg only *after* close() returned.
    # Cancelling immediately (as before) killed them before they got there.
    await asyncio.sleep(0.12)
    stop = True
    for t in tasks:
        t.cancel()
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=3)
    except asyncio.TimeoutError:
        pass
    # drain whatever the leak left; bounded so a wedged leg can't hang the run
    if client._hedged_reads:
        try:
            await asyncio.wait_for(
                asyncio.gather(*list(client._hedged_reads), return_exceptions=True), timeout=3
            )
        except asyncio.TimeoutError:
            pass
    return {"leftover": leftover, "close_hung": close_hung,
            "post_teardown_io": post_io["n"], "pending_at_close": pending_at_close,
            "add_after_drain": add_after_drain["n"], "torn": torn_at is not None}


async def main():
    host, port = addr.rsplit(":", 1)
    leftover_iters = 0
    postio_iters = 0
    max_left = 0
    hung = 0
    active_close_iters = 0  # iters where hedge legs were pending when close() ran
    examples = []
    for i in range(ITERS):
        try:
            r = await asyncio.wait_for(one(i, host, port), timeout=20)
        except asyncio.TimeoutError:
            hung += 1
            print(json.dumps({"iter": i, "ITER_TIMEOUT": True}), flush=True)
            continue
        if r["close_hung"]:
            hung += 1
        if r["pending_at_close"] > 0:
            active_close_iters += 1
        flagged = False
        if r["leftover"] > 0:
            leftover_iters += 1
            max_left = max(max_left, r["leftover"])
            flagged = True
        if r["post_teardown_io"] > 0 or r["add_after_drain"] > 0:
            postio_iters += 1
            flagged = True
        if flagged and len(examples) < 6:
            examples.append({"iter": i, **r})
        if (i + 1) % 25 == 0:
            print(json.dumps({"done": i + 1, "leftover_iters": leftover_iters,
                              "post_teardown_io_iters": postio_iters,
                              "close_hung": hung, "active_close_iters": active_close_iters, "max_leftover": max_left}), flush=True)
    print(json.dumps({"final": True, "iters": ITERS,
                      "leftover_iters": leftover_iters,
                      "post_teardown_io_iters": postio_iters,
                      "close_hung_iters": hung, "active_close_iters": active_close_iters, "max_leftover": max_left,
                      "examples": examples}), flush=True)


asyncio.run(main())
