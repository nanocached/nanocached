"""Cluster-scenario driver for the AWS live tests.

Uses the Python SDK against a discovery-fronted cluster. Addresses come
from the NANOTEST_ADDRESSES env var ("host:port,host:port").

Commands:
  write <label> <count>          write x:<label>:<i> = v-<label>-<i>
  read <label> <count>           read those back and verify
  readall <l1,l2,...> <count>    verify every label's keys
  preload <count>                write bulk:<i> (~100-byte values)
  verify <count>                 read every bulk key, report missing
  churn <seconds> <outfile>      continuous get/set, log failures as JSON
  nodes                          raw L query against the first address
"""

import asyncio
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdk", "python", "src"))
from nanocached import NanocachedClient  # noqa: E402

# Bound on the raw-socket reads cmd_nodes does directly against discovery
# (bypassing the SDK's own connect_and_identify, which already has this
# kind of bound). Without it, a discovery server that accepts the
# connection and then goes silent (crashed-but-open, deadlocked, or a bug
# on the other end) would hang this command forever. Matches
# sdk/python/src/nanocached/_identify.py's CONNECT_DEADLINE.
_IO_TIMEOUT = 5.0

# Mirrors sdk/python/src/nanocached/_identify.py's caps
# (_MAX_NODE_COUNT / _MAX_NODE_FIELD_LENGTH / _MAX_NODE_LIST_RESPONSE_LENGTH)
# on the same length-prefixed `L`-response fields: a corrupt or hostile
# discovery reply must not drive an unbounded read or allocation here
# either, since this command parses the wire response itself instead of
# going through the SDK.
_MAX_NODE_COUNT = 1 << 16
_MAX_NODE_FIELD_LENGTH = 64 * 1024
_MAX_NODE_LIST_RESPONSE_LENGTH = 16 * 1024 * 1024


def addresses():
    raw = os.environ["NANOTEST_ADDRESSES"]
    out = []
    for part in raw.split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    return out


def bulk_value(i: int) -> bytes:
    return (f"v-bulk-{i}-" + "x" * 80).encode()


async def cmd_write(label: str, count: int) -> int:
    client = await NanocachedClient.connect(addresses())
    for i in range(count):
        await client.set(f"x:{label}:{i}", f"v-{label}-{i}")
    await client.close()
    print(f"wrote {count} keys for label {label}")
    return 0


async def cmd_read(label: str, count: int) -> int:
    client = await NanocachedClient.connect(addresses())
    bad = []
    for i in range(count):
        value = await client.get(f"x:{label}:{i}")
        if value != f"v-{label}-{i}":
            bad.append(i)
    await client.close()
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
    client = await NanocachedClient.connect(addresses())
    for start in range(0, count, 100):
        await asyncio.gather(
            *(client.set(f"bulk:{i}", bulk_value(i)) for i in range(start, min(start + 100, count)))
        )
    await client.close()
    print(f"preloaded {count} keys (replication={client.replication})")
    return 0


async def cmd_verify(count: int) -> int:
    client = await NanocachedClient.connect(addresses())
    missing, wrong = [], []

    async def check(i: int):
        value = await client.get_bytes(f"bulk:{i}")
        if value is None:
            missing.append(i)
        elif value != bulk_value(i):
            wrong.append(i)

    for start in range(0, count, 100):
        await asyncio.gather(*(check(i) for i in range(start, min(start + 100, count))))
    await client.close()
    print(json.dumps({
        "checked": count,
        "missing": len(missing),
        "wrong": len(wrong),
        "missing_sample": missing[:10],
    }))
    return 0 if not missing and not wrong else 1


async def cmd_churn(seconds: float, outfile: str) -> int:
    client = await NanocachedClient.connect(addresses())
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
                    await asyncio.wait_for(client.get(key), timeout=15)
                else:
                    await asyncio.wait_for(client.set(f"churn:{i}", f"c-{i}"), timeout=15)
                ops += 1
            except Exception as exc:  # noqa: BLE001 — record every failure kind
                ops += 1
                fails += 1
                events.append({"t": t, "op": op, "error": type(exc).__name__, "detail": str(exc)[:120]})
        await asyncio.sleep(0.05)
    await client.close()
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
    host, port = addresses()[0]
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(b"A 1\n\x00")
        await writer.drain()
        ack = await asyncio.wait_for(reader.readexactly(3), timeout=_IO_TIMEOUT)
        if ack != b"Od\n":
            print(f"unexpected handshake: {ack!r}")
            return 1
        writer.write(b"L\n")
        await writer.drain()
        header_line = await asyncio.wait_for(reader.readline(), timeout=_IO_TIMEOUT)
        header = header_line.decode().strip()
        print(f"header: {header}")
        # Aggregate cap on the whole L response, header included, mirroring
        # _identify.py's _read_entries — independent of the per-field caps
        # below, so a long run of small-but-valid entries can't add up to
        # an unbounded amount of memory either.
        total_bytes = len(header_line)
        if header.startswith("N "):
            fields = header.split()
            if len(fields) < 2:
                print(f"malformed node-list header: {header!r}")
                return 1
            try:
                count = int(fields[1])
            except ValueError:
                print(f"malformed node-list header: {header!r}")
                return 1
            if count < 0 or count > _MAX_NODE_COUNT:
                print(f"node count {count} out of bounds (max {_MAX_NODE_COUNT})")
                return 1
            for _ in range(count):
                entry_header_line = await asyncio.wait_for(reader.readline(), timeout=_IO_TIMEOUT)
                total_bytes += len(entry_header_line)
                lengths = entry_header_line.decode().split()
                if len(lengths) != 2:
                    print(f"malformed entry header: {lengths!r}")
                    return 1
                try:
                    name_len, addr_len = int(lengths[0]), int(lengths[1])
                except ValueError:
                    print(f"malformed entry header: {lengths!r}")
                    return 1
                if (
                    name_len < 0
                    or addr_len < 0
                    or name_len > _MAX_NODE_FIELD_LENGTH
                    or addr_len > _MAX_NODE_FIELD_LENGTH
                ):
                    print(f"entry lengths out of bounds: name={name_len} addr={addr_len}")
                    return 1
                total_bytes += name_len + addr_len + 1
                if total_bytes > _MAX_NODE_LIST_RESPONSE_LENGTH:
                    print("node-list response exceeds maximum size")
                    return 1
                body = await asyncio.wait_for(
                    reader.readexactly(name_len + addr_len + 1), timeout=_IO_TIMEOUT
                )
                print(f"  {body[:name_len].decode()} @ {body[name_len:name_len + addr_len].decode()}")
        return 0
    finally:
        writer.close()


# argv length required for each command, including the command name
# itself at sys.argv[1] (i.e. len(sys.argv) must equal this). Checked up
# front so a missing argument prints a usage message and exits non-zero
# instead of crashing with an IndexError deep inside a command.
_ARGC = {
    "write": 4,
    "read": 4,
    "readall": 4,
    "preload": 3,
    "verify": 3,
    "churn": 4,
    "nodes": 2,
}


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    expected_argc = _ARGC.get(cmd)
    if expected_argc is None:
        print(f"unknown command {cmd!r}", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        return 2
    if len(sys.argv) != expected_argc:
        print(f"nanotest.py {cmd}: wrong number of arguments", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        return 2
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
    return asyncio.run(cmd_nodes())


if __name__ == "__main__":
    raise SystemExit(main())
