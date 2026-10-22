"""Scenario driver for the Python SDK (V4 failover, V5 churn, V6 connections, V7 lifecycle).

    scenario.py <scenario> [args]       one JSON line per event on stdout

Connection from NANOTEST_ADDRESSES / NANOTEST_VIA_PROXY / NANOTEST_SECRET.
CONN_TIMEOUT_S bounds every call (default 60).
Scenarios:
  conn                       connect, set, get, describe, close; each reported
  churn <secs> <label>       get/set loop that records error windows and stale reads
  seed <prefix> <n>          set_many n keys                       (failover)
  gm <prefix> <n> <secs>     get_many of the n keys once a second   (failover)
  host <case> [args]         one request against a hostile fake node (fixtures/hostile_cases.json); reports the
                             outcome, the wall time and the container's peak memory
  life <test> [args]         close_inflight | after_close | double_close | concurrent_close
                             | abandon | cycles <n> | reconnect <secs> | storm <secs> | exit_noclose
"""

import asyncio
import json
import os
import sys
import time

from nanocached import NanocachedClient

SDK = "python"
LIMIT = float(os.environ.get("CONN_TIMEOUT_S", "60"))
T0 = time.monotonic()


def emit(**kw):
    kw.setdefault("sdk", SDK)
    kw["t_ms"] = int((time.monotonic() - T0) * 1000)
    print(json.dumps(kw, separators=(",", ":"), default=str), flush=True)


def addresses():
    out = []
    for part in os.environ["NANOTEST_ADDRESSES"].split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    return out


async def connect():
    return await NanocachedClient.connect(
        addresses(),
        auth_secret=os.environ.get("NANOTEST_SECRET") or None,
        via_proxy=os.environ.get("NANOTEST_VIA_PROXY") == "1",
    )


def err_fields(e):
    return {"outcome": "error", "error_class": type(e).__name__, "error_message": str(e)[:300]}


async def step(name, coro, limit=None):
    t = time.monotonic()
    try:
        r = await asyncio.wait_for(coro, limit or LIMIT)
        emit(event=name, outcome="ok", ms=int((time.monotonic() - t) * 1000))
        return r
    except asyncio.TimeoutError:
        emit(event=name, outcome="timeout", ms=int((time.monotonic() - t) * 1000))
    except Exception as e:  # noqa: BLE001
        emit(event=name, ms=int((time.monotonic() - t) * 1000), **err_fields(e))
    return None


def proc_stats():
    fds = len(os.listdir("/proc/self/fd"))
    threads = rss = 0
    for line in open("/proc/self/status"):
        if line.startswith("Threads:"):
            threads = int(line.split()[1])
        elif line.startswith("VmRSS:"):
            rss = int(line.split()[1])
    return {"fds": fds, "threads": threads, "rss_kb": rss}


# ---------------------------------------------------------------- conn
async def sc_conn():
    client = await step("connect", connect())
    if client is None:
        return
    await step("set", client.set("conn:k", "v"))
    got = await step("get", client.get("conn:k"))
    emit(event="get_value", value=got)
    emit(event="describe", text=repr(client))
    await step("close", client.close())


# --------------------------------------------------------------- churn
async def sc_churn(secs, label):
    secs = float(secs)
    client = await connect()
    keys = [f"ch:{label}:{SDK}:{i}".encode() for i in range(200)]
    acked = [0] * len(keys)
    ops = errors = stale = miss = corrupt = 0
    classes = {}
    first_err = last_err = None
    end = time.monotonic() + secs
    i = 0
    while time.monotonic() < end:
        n = i % len(keys)
        i += 1
        ops += 1
        try:
            if i % 2:
                version = acked[n] + 1
                await asyncio.wait_for(client.set(keys[n], f"v{version}".encode()), 10)
                acked[n] = version
            else:
                got = await asyncio.wait_for(client.get_bytes(keys[n]), 10)
                if acked[n]:
                    if got is None:
                        miss += 1
                    elif not got.startswith(b"v"):
                        corrupt += 1
                    elif int(got[1:]) < acked[n]:
                        stale += 1
        except Exception as e:  # noqa: BLE001
            errors += 1
            name = type(e).__name__
            classes[name] = classes.get(name, 0) + 1
            now = int((time.monotonic() - T0) * 1000)
            first_err = now if first_err is None else first_err
            last_err = now
        await asyncio.sleep(0.005)
    emit(event="summary", ops=ops, errors=errors, stale=stale, miss=miss, corrupt=corrupt, classes=classes, first_err_ms=first_err, last_err_ms=last_err)
    await client.close()


# ------------------------------------------------------------ failover
async def sc_seed(prefix, n):
    client = await connect()
    await client.set_many({f"{prefix}{i}".encode(): b"x" * 64 for i in range(int(n))})
    emit(event="seeded", n=int(n))
    await client.close()


async def sc_gm(prefix, n, secs):
    client = await connect()
    keys = [f"{prefix}{i}".encode() for i in range(int(n))]
    end = time.monotonic() + float(secs)
    while time.monotonic() < end:
        t = time.monotonic()
        try:
            found = await asyncio.wait_for(client.get_many_bytes(keys), 20)
            emit(event="gm", outcome="ok", count=len(found), want=len(keys), ms=int((time.monotonic() - t) * 1000))
        except Exception as e:  # noqa: BLE001
            partial = getattr(e, "partial_values", None)
            emit(event="gm", ms=int((time.monotonic() - t) * 1000), partial=None if partial is None else len(partial), want=len(keys), **err_fields(e))
        await asyncio.sleep(1)
    await client.close()


# ----------------------------------------------------------- lifecycle
async def life_close_inflight():
    client = await connect()
    await client.set(b"life:big", b"b" * 900_000)
    tasks = [asyncio.ensure_future(client.get_bytes(b"life:big")) for _ in range(4)]
    await asyncio.sleep(0.2)
    t = time.monotonic()
    await step("close", client.close())
    emit(event="close_ms", ms=int((time.monotonic() - t) * 1000))
    for i, task in enumerate(tasks):
        try:
            v = await asyncio.wait_for(task, 20)
            emit(event="pending", i=i, outcome="ok", len=None if v is None else len(v))
        except Exception as e:  # noqa: BLE001
            emit(event="pending", i=i, **err_fields(e))


async def life_after_close():
    client = await connect()
    await client.set(b"life:k", b"v")
    await client.close()
    for name, call in [("get", lambda: client.get(b"life:k")), ("set", lambda: client.set(b"life:k", b"v")), ("delete", lambda: client.delete(b"life:k")),
                       ("incr", lambda: client.incr(b"life:k", 1)), ("get_many", lambda: client.get_many([b"life:k"]))]:
        await step("after_close." + name, call())


async def life_double_close():
    client = await connect()
    await step("close1", client.close())
    await step("close2", client.close())
    client = await connect()
    await step("concurrent_close", asyncio.gather(client.close(), client.close(), client.close()))


async def life_abandon():
    """The target is reached through the delay relay, so a request is still in flight when the caller gives up."""
    client = await connect()
    await client.set(b"life:a", b"value-a")
    await client.set(b"life:b", b"value-b")
    task = asyncio.ensure_future(client.get_bytes(b"life:a"))
    await asyncio.sleep(0.3)
    task.cancel()  # real cancellation in asyncio
    emit(event="cancelled")
    results = {}
    for name, key in (("b", b"life:b"), ("a", b"life:a")):
        try:
            results[name] = await asyncio.wait_for(client.get_bytes(key), 30)
        except Exception as e:  # noqa: BLE001
            results[name] = f"{type(e).__name__}: {e}"[:120]
    emit(event="after_cancel", b=results["b"], a=results["a"], ok=results["b"] == b"value-b" and results["a"] == b"value-a")
    await client.close()


async def life_cycles(n):
    base = proc_stats()
    for i in range(int(n)):
        client = await connect()
        await client.set(b"life:c", b"v")
        await client.close()
    await asyncio.sleep(1)
    emit(event="cycles", n=int(n), before=base, after=proc_stats())


async def life_reconnect(secs):
    client = await connect()
    end = time.monotonic() + float(secs)
    first_fail = last_fail = first_ok_after = None
    ok = fail = 0
    while time.monotonic() < end:
        try:
            await asyncio.wait_for(client.get_bytes(b"life:r"), 10)
            ok += 1
            if first_fail is not None and first_ok_after is None:
                first_ok_after = int((time.monotonic() - T0) * 1000)
        except Exception:  # noqa: BLE001
            fail += 1
            now = int((time.monotonic() - T0) * 1000)
            first_fail = first_fail if first_fail is not None else now
            last_fail = now
        await asyncio.sleep(0.1)
    emit(event="reconnect", ok=ok, fail=fail, first_fail_ms=first_fail, last_fail_ms=last_fail, first_ok_after_ms=first_ok_after)
    await client.close()


async def life_storm(secs):
    client = await connect()
    end = time.monotonic() + float(secs)
    attempts = 0
    while time.monotonic() < end:
        attempts += 1
        try:
            await asyncio.wait_for(client.get_bytes(b"life:s"), 5)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(0.05)
    emit(event="storm", attempts=attempts)
    await client.close()


async def life_exit_noclose():
    client = await connect()
    await client.set(b"life:e", b"v")
    emit(event="exiting_without_close")
    # no close: the process must still exit promptly


async def sc_life(test, *args):
    await {
        "close_inflight": life_close_inflight, "after_close": life_after_close, "double_close": life_double_close,
        "concurrent_close": life_double_close, "abandon": life_abandon, "cycles": life_cycles, "reconnect": life_reconnect,
        "storm": life_storm, "exit_noclose": life_exit_noclose,
    }[test](*args)


# ------------------------------------------------------------- hostile
def cgroup_peak_kb():
    """The container's peak memory (cgroup v2), the one measure every runtime can report the same way."""
    try:
        return int(open("/sys/fs/cgroup/memory.peak").read()) // 1024
    except (OSError, ValueError):
        return None


async def sc_host(case, *args):
    compress = "compress" in args
    client = await step("connect", NanocachedClient.connect(addresses(), compress=compress))
    if client is None:
        emit(event="peak", cgroup_peak_kb=cgroup_peak_kb())
        return
    t = time.monotonic()
    if case == "backpressure":
        n, size = int(args[0]), int(args[1])
        value = bytes(size)  # one buffer, owned by the caller and shared by every set
        done = asyncio.gather(*[client.set(b"host:bp:%d" % i, value) for i in range(n)], return_exceptions=True)
        try:
            results = await asyncio.wait_for(done, LIMIT)
            failed = sum(1 for r in results if isinstance(r, BaseException))
            emit(event="op", outcome="settled", ok=n - failed, failed=failed, ms=int((time.monotonic() - t) * 1000))
        except asyncio.TimeoutError:
            emit(event="op", outcome="timeout", ms=int((time.monotonic() - t) * 1000))
    else:
        try:
            got = await asyncio.wait_for(client.get_bytes(b"host:k"), LIMIT)
            emit(event="op", outcome="ok", len=None if got is None else len(got), ms=int((time.monotonic() - t) * 1000))
        except asyncio.TimeoutError:
            emit(event="op", outcome="timeout", ms=int((time.monotonic() - t) * 1000))
        except Exception as e:  # noqa: BLE001
            emit(event="op", ms=int((time.monotonic() - t) * 1000), **err_fields(e))
    emit(event="peak", cgroup_peak_kb=cgroup_peak_kb())
    await step("close", client.close(), 10)


async def main():
    name, args = sys.argv[1], sys.argv[2:]
    await {"conn": sc_conn, "churn": sc_churn, "seed": sc_seed, "gm": sc_gm, "life": sc_life, "host": sc_host}[name](*args)


asyncio.run(main())
