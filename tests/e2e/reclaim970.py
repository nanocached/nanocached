"""reclaim970.py seed|apply|check <host:port> <n> [node,node,...]

The phases of reclaim970.sh (issue #970): `seed` writes r<i>, e<i>, k<i>
(value v1, no TTL); `apply` expires every r key with touch(key, 0),
deletes every e key and rewrites every k key to v2 through the SDK;
`check` reads every key from every listed node raw (G; a non-owner
answers W) and counts, per class, the keys some node still holds the
pre-apply state of: an expired or deleted key present anywhere, a
rewritten key still v1.
"""
import asyncio, json, socket, sys
from nanocached import NanocachedClient
mode, addr, n = sys.argv[1], sys.argv[2], int(sys.argv[3]); nodes = sys.argv[4].split(",") if len(sys.argv) > 4 else []
async def main():
    h, p = addr.rsplit(":", 1); c = await NanocachedClient.connect([(h, int(p))])
    if mode == "seed":
        await asyncio.gather(*[c.set(f"{cls}{i}", b"v1") for cls in "rek" for i in range(n)]); print("seeded", 3 * n)
    else:
        sem = asyncio.Semaphore(32); errs = {}
        async def one(coro):
            async with sem:
                try: await coro
                except Exception as e: errs[type(e).__name__] = errs.get(type(e).__name__, 0) + 1
        await asyncio.gather(*[one(c.touch(f"r{i}", 0)) for i in range(n)], *[one(c.delete(f"e{i}")) for i in range(n)], *[one(c.set(f"k{i}", b"v2")) for i in range(n)])
        print(json.dumps(dict(phase="applied", errors=errs, ring=sorted(c._members) if hasattr(c, "_members") else None)))
    await c.close()
def raw():
    class C:
        def __init__(s, nn): s.s = socket.create_connection((nn, 8356), timeout=5); s.b = b""
        def line(s):
            while b"\n" not in s.b: s.b += s.s.recv(65536)
            l, _, s.b = s.b.partition(b"\n"); return l
        def body(s, k):
            while len(s.b) < k: s.b += s.s.recv(65536)
            d, s.b = s.b[:k], s.b[k:]; return d
        def get(s, k):
            s.s.sendall(f"G {len(k)}\n".encode() + k); l = s.line()
            if l.startswith(b"V "): return "V:" + s.body(int(l.split()[1])).decode()
            return l.decode()
    cs = {nn: C(nn) for nn in nodes}; out = {}
    for cls, bad in (("r", lambda v: v.startswith("V")), ("e", lambda v: v.startswith("V")), ("k", lambda v: v == "V:v1")):
        hit = {nn: 0 for nn in nodes}; keys = []
        for i in range(n):
            r = {nn: cs[nn].get(f"{cls}{i}".encode()) for nn in nodes}
            if any(bad(v) for v in r.values()):
                keys.append((f"{cls}{i}", r))
                for nn, v in r.items():
                    if bad(v): hit[nn] += 1
        out[cls] = dict(stale_or_resurrected_keys=len(keys), per_node=hit, examples=keys[:3])
    print(json.dumps(out))
if mode == "check": raw()
else: asyncio.run(main())
