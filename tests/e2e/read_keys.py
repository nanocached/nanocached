"""read_keys.py --addr host:port --keys-file F [--expect-version V]

Reads every listed key through a fresh SDK client and prints counts:
ok (value present, version matches when --expect-version is given), miss,
other (present with another version), err, plus the first missed keys.
"""
import argparse, asyncio, json
from nanocached import NanocachedClient

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--keys-file", required=True)
ap.add_argument("--expect-version", default=None)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]

async def main():
    keys = [k for k in open(a.keys_file).read().split() if k]
    c = await NanocachedClient.connect(addrs)
    out = dict(n=len(keys), ok=0, miss=0, other=0, err=0, missed=[])
    sem = asyncio.Semaphore(32)
    async def one(k):
        async with sem:
            try: v = await c.get_bytes(k)
            except Exception: out["err"] += 1; return
        if v is None:
            out["miss"] += 1
            if len(out["missed"]) < 20: out["missed"].append(k)
        elif a.expect_version is None or v.split(b"|", 2)[1:2] == [a.expect_version.encode()]: out["ok"] += 1
        else: out["other"] += 1
    await asyncio.gather(*[one(k) for k in keys])
    await c.close()
    print(json.dumps(out), flush=True)

asyncio.run(main())
