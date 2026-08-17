"""Cluster-scenario driver for the AWS live tests.

Uses the Python SDK against a discovery-fronted cluster. Seeds come from
the NANOTEST_SEEDS env var ("host:port,host:port").

Commands:
  write <label> <count>          write x:<label>:<i> = v-<label>-<i>
  read <label> <count>           read those back and verify
  readall <l1,l2,...> <count>    verify every label's keys
  preload <count>                write bulk:<i> (~100-byte values)
  verify <count>                 read every bulk key, report missing
  churn <seconds> <outfile>      continuous get/set, log failures as JSON
  nodes                          raw L query against the first seed
"""

import asyncio
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdk", "python", "src"))
from nanocached import NanocachedClient  # noqa: E402


def seeds():
    raw = os.environ["NANOTEST_SEEDS"]
    out = []
    for part in raw.split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    return out


def bulk_value(i: int) -> bytes:
    return (f"v-bulk-{i}-" + "x" * 80).encode()


async def cmd_write(label: str, count: int) -> int:
    client = await NanocachedClient.connect(seeds=seeds())
    for i in range(count):
        await client.set(f"x:{label}:{i}", f"v-{label}-{i}")
    client.close()
    print(f"wrote {count} keys for label {label}")
    return 0


async def cmd_read(label: str, count: int) -> int:
    client = await NanocachedClient.connect(seeds=seeds())
    bad = []
    for i in range(count):
        value = await client.get(f"x:{label}:{i}")
        if value != f"v-{label}-{i}".encode():
            bad.append(i)
    client.close()
    if bad:
        print(f"label {label}: {len(bad)}/{count} BAD (sample {bad[:5]})")
        return 1
    print(f"label {label}: {count}/{count} OK")
    return 0


async def cmd_readall(labels: str, count: int) -> int:
    rc = 0
    for label in labels.split(","):
        rc |= await cmd_read(label, count)
    return rc


async def cmd_preload(count: int) -> int:
    client = await NanocachedClient.connect(seeds=seeds())
    for start in range(0, count, 100):
        await asyncio.gather(
            *(client.set(f"bulk:{i}", bulk_value(i)) for i in range(start, min(start + 100, count)))
        )
    client.close()
    print(f"preloaded {count} keys (replication={client.replication})")
    return 0


async def cmd_verify(count: int) -> int:
    client = await NanocachedClient.connect(seeds=seeds())
    missing, wrong = [], []

    async def check(i: int):
        value = await client.get(f"bulk:{i}")
        if value is None:
            missing.append(i)
        elif value != bulk_value(i):
            wrong.append(i)

    for start in range(0, count, 100):
        await asyncio.gather(*(check(i) for i in range(start, min(start + 100, count))))
    client.close()
    print(json.dumps({
        "checked": count,
        "missing": len(missing),
        "wrong": len(wrong),
        "missing_sample": missing[:10],
    }))
    return 0 if not missing and not wrong else 1


async def cmd_churn(seconds: float, outfile: str) -> int:
    client = await NanocachedClient.connect(seeds=seeds())
    t0 = time.monotonic()
    events = []
    ops = fails = 0
    i = 0
    while time.monotonic() - t0 < seconds:
        i += 1
        for op in ("get", "set"):
            t = round(time.monotonic() - t0, 3)
            try:
                if op == "get":
                    key = f"bulk:{random.randrange(0, 2000)}"
                    await asyncio.wait_for(client.get(key), timeout=5)
                else:
                    await asyncio.wait_for(client.set(f"churn:{i}", f"c-{i}"), timeout=5)
                ops += 1
            except Exception as exc:  # noqa: BLE001 — record every failure kind
                ops += 1
                fails += 1
                events.append({"t": t, "op": op, "error": type(exc).__name__, "detail": str(exc)[:120]})
        await asyncio.sleep(0.05)
    client.close()
    summary = {
        "duration": round(time.monotonic() - t0, 1),
        "ops": ops,
        "failures": fails,
        "first_failure_t": events[0]["t"] if events else None,
        "last_failure_t": events[-1]["t"] if events else None,
        "events": events[:50],
    }
    with open(outfile, "w") as f:
        json.dump(summary, f, indent=1)
    print(json.dumps({k: v for k, v in summary.items() if k != "events"}))
    return 0


async def cmd_nodes() -> int:
    host, port = seeds()[0]
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"A 1\n\x00")
    await writer.drain()
    ack = await reader.readexactly(3)
    if ack != b"Od\n":
        print(f"unexpected handshake: {ack!r}")
        return 1
    writer.write(b"L\n")
    await writer.drain()
    header = (await reader.readline()).decode().strip()
    print(f"header: {header}")
    if header.startswith("N "):
        count = int(header.split()[1])
        for _ in range(count):
            lengths = (await reader.readline()).decode().split()
            name_len, addr_len = int(lengths[0]), int(lengths[1])
            body = await reader.readexactly(name_len + addr_len + 1)
            print(f"  {body[:name_len].decode()} @ {body[name_len:name_len + addr_len].decode()}")
    writer.close()
    return 0


def main() -> int:
    cmd = sys.argv[1]
    if cmd == "write":
        return asyncio.run(cmd_write(sys.argv[2], int(sys.argv[3])))
    if cmd == "read":
        return asyncio.run(cmd_read(sys.argv[2], int(sys.argv[3])))
    if cmd == "readall":
        return asyncio.run(cmd_readall(sys.argv[2], int(sys.argv[3])))
    if cmd == "preload":
        return asyncio.run(cmd_preload(int(sys.argv[2])))
    if cmd == "verify":
        return asyncio.run(cmd_verify(int(sys.argv[2])))
    if cmd == "churn":
        return asyncio.run(cmd_churn(float(sys.argv[2]), sys.argv[3]))
    if cmd == "nodes":
        return asyncio.run(cmd_nodes())
    print(f"unknown command {cmd}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
