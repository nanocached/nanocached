"""hostile_node.py <hostile_cases.json>: a fake nanocached node, and a fake discovery server, that misbehave in a
different way on each port.

It answers the SDKs' handshake (`A <n>[ T]\\n<secret>` -> `On\\n` for a node, `Od\\n` for a discovery server,
no tag capability, so replies are untagged) and then, on a `G` (get) or an `L` (node list), does what the case on
that port says. Only the cases of hostile_cases.json are served; the case ids pick the behaviour:

  baseline               V 1024, an honest reply
  trickle_ok             an honest 40-byte reply, one byte every 100 ms
  stall_after_handshake  reads the request, never answers
  stall_mid_reply        `V 1000\\n` and 10 bytes of the value, then silence
  oversize_just_over     declares 2 MiB + 1 B and sends all of it
  oversize_huge          declares 4294967295 B and sends nothing
  bomb                   a 1 MiB raw-DEFLATE value (marker 0x01) that expands to 1 GiB
  backpressure           completes the handshake and then reads nothing at all

  roster_count_over      (discovery) `N 65537 2`: one more node than the SDKs accept
  roster_bytes_over      (discovery) entries of 64 KiB + 64 KiB until the client hangs up, past the 16 MiB cap
  roster_unreachable     (discovery) 128 entries, none of them reachable (TEST-NET-3 addresses)

Every wait is bounded by the container's lifetime (the run script removes it). Sets are read and
acknowledged, so a client that writes before it reads is not stuck on a half-open fixture.
"""
import asyncio
import json
import sys
import zlib

CASES = {c["id"]: c for c in json.load(open(sys.argv[1]))["cases"]}
ONE_GIB = 1 << 30


def bomb_value():
    compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
    chunk = bytes(1 << 20)
    out = [b"\x01"]
    for _ in range(ONE_GIB >> 20):
        out.append(compressor.compress(chunk))
    out.append(compressor.flush())
    return b"".join(out)


BOMB = bomb_value() if "bomb" in CASES else b""


async def read_request(reader):
    """One request: returns (command byte, header fields) after consuming its body, or None at EOF."""
    line = await reader.readline()
    if not line:
        return None
    fields = line.split()
    cmd = fields[0][:1]
    body = 0
    if cmd == b"A":
        body = int(fields[1])
    elif cmd == b"G":
        body = int(fields[1])
    elif cmd == b"S":
        body = int(fields[1]) + int(fields[2])
    if body:
        await reader.readexactly(body)
    return cmd, fields


async def serve(case_id, reader, writer):
    try:
        if case_id == "backpressure":
            request = await read_request(reader)  # the handshake only
            if request and request[0] == b"A":
                writer.write(b"On\n")
                await writer.drain()
            await asyncio.sleep(3600)  # then nothing is ever read
            return
        while True:
            request = await read_request(reader)
            if request is None:
                return
            cmd, _ = request
            if cmd == b"A":
                writer.write(b"Od\n" if case_id.startswith("roster_") else b"On\n")
            elif cmd == b"S":
                writer.write(b"S\n")
            elif cmd == b"G":
                await answer_get(case_id, writer)
            elif cmd == b"L":
                await answer_list(case_id, writer)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def answer_get(case_id, writer):
    if case_id == "baseline":
        writer.write(b"V 1024\n" + b"x" * 1024)
    elif case_id == "trickle_ok":
        reply = b"V 40\n" + b"t" * 40
        for i in range(len(reply)):
            writer.write(reply[i:i + 1])
            await writer.drain()
            await asyncio.sleep(0.1)
    elif case_id == "stall_after_handshake":
        await asyncio.sleep(3600)
    elif case_id == "stall_mid_reply":
        writer.write(b"V 1000\n" + b"s" * 10)
        await writer.drain()
        await asyncio.sleep(3600)
    elif case_id == "oversize_just_over":
        size = 2 * 1024 * 1024 + 1
        writer.write(b"V %d\n" % size)
        for _ in range(size // 65536):
            writer.write(b"o" * 65536)
            await writer.drain()
        writer.write(b"o" * (size % 65536))
    elif case_id == "oversize_huge":
        writer.write(b"V 4294967295\n")
        await writer.drain()
        await asyncio.sleep(3600)
    elif case_id == "bomb":
        writer.write(b"V %d\n" % len(BOMB) + BOMB)


def entry(name, addr):
    return b"%d %d\n%b%b\n" % (len(name), len(addr), name, addr)


async def answer_list(case_id, writer):
    """`N <count> <replication>\\n` and then `<name-len> <addr-len>\\n<name><addr>\\n` per node."""
    if case_id == "roster_count_over":
        writer.write(b"N 65537 2\n")
        await writer.drain()
        await asyncio.sleep(3600)
    elif case_id == "roster_bytes_over":
        writer.write(b"N 65536 2\n")
        big = entry(b"n" * 65536, b"a" * 65536)
        for _ in range(65536):  # the client hangs up long before; the write then raises
            writer.write(big)
            await writer.drain()
    elif case_id == "roster_unreachable":
        writer.write(b"N 128 2\n")
        for i in range(128):
            # TEST-NET-3 is reserved for documentation: nothing answers there, so every dial runs into its deadline
            writer.write(entry(b"hostile-%d" % i, b"203.0.113.%d:%d" % (1 + i % 254, 8356 + i // 254)))


async def main():
    servers = []
    for case_id, case in CASES.items():
        servers.append(await asyncio.start_server(lambda r, w, c=case_id: serve(c, r, w), "0.0.0.0", case["port"]))
    print("hostile_node: " + ", ".join(f"{c}:{CASES[c]['port']}" for c in CASES), flush=True)
    await asyncio.gather(*[s.serve_forever() for s in servers])


asyncio.run(main())
