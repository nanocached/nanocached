"""write_once.py --addr host:port --keys-file F --go-file G [--version V] [--timeout S]
              [--direct host:port] [--poll-node host:port --poll-version V --deadline S]

Connects (so its node list is the ring of *this* moment), waits for
--go-file to appear, then writes every key in --keys-file once with value
`<key>|<version>|<pad>` and prints a JSON line.

--direct host:port   write through one raw identified connection to that node
                     instead of the SDK (no replication, no forwarding): used
                     to give one owner a copy the other does not have.
--poll-node ...      instead of writing all keys at once, poll that node
                     directly and write a key the moment the node answers with
                     --poll-version (a decommission handoff just delivered it),
                     until every key is written or --deadline seconds pass.
                     The keys written are listed in --written-file.
"""
import argparse, asyncio, json, os, time
from nanocached import NanocachedClient
from nanocached._identify import connect_and_identify, NodeTarget
from nanocached._connection import Connection

ap = argparse.ArgumentParser()
ap.add_argument("--addr", required=True); ap.add_argument("--keys-file", required=True)
ap.add_argument("--go-file", required=True); ap.add_argument("--timeout", type=float, default=120)
ap.add_argument("--size", type=int, default=256); ap.add_argument("--version", default="2")
ap.add_argument("--direct", default=None)
ap.add_argument("--poll-node", default=None); ap.add_argument("--poll-version", default="2")
ap.add_argument("--deadline", type=float, default=6.0); ap.add_argument("--written-file", default=None)
a = ap.parse_args()
addrs = [(x.rsplit(":", 1)[0], int(x.rsplit(":", 1)[1])) for x in a.addr.split(",")]
def val(k): return (f"{k}|{a.version}|" + "x" * a.size).encode()

async def raw(hp):
    host, port = hp.rsplit(":", 1)
    t = await connect_and_identify(host, int(port), None, None)
    assert isinstance(t, NodeTarget)
    return Connection(t.reader, t.writer, tagged=t.tagged)

def now(): return time.strftime("%H:%M:%S", time.gmtime()) + ".%03d" % int((time.time() % 1) * 1000)

async def main():
    if a.direct:
        keys = [k for k in open(a.keys_file).read().split() if k]
        c = await raw(a.direct); ok = err = 0
        for k in keys:
            try: await c.set(k.encode(), val(k), 0); ok += 1
            except Exception: err += 1
        c.close(); print(json.dumps(dict(phase="direct", node=a.direct, n=len(keys), ok=ok, err=err, version=a.version)), flush=True); return
    c = await NanocachedClient.connect(addrs)
    members = sorted(c._members) if hasattr(c, "_members") else []
    print(json.dumps(dict(phase="connected", members=members)), flush=True)
    t0 = time.time()
    while not os.path.exists(a.go_file):
        if time.time() - t0 > a.timeout:
            print(json.dumps(dict(phase="timeout"))); await c.close(); return
        await asyncio.sleep(0.02)
    keys = [k for k in open(a.keys_file).read().split() if k]  # written by the harness after the join starts
    t1 = time.time(); ok = err = 0; errors = {}; written = []
    sem = asyncio.Semaphore(32)
    async def one(k):
        nonlocal ok, err
        async with sem:
            try: await c.set(k, val(k)); ok += 1; written.append(k)
            except Exception as e: err += 1; errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
    if a.poll_node:
        p = await raw(a.poll_node); pending = list(keys); rounds = 0; first_delivery = None
        while pending and time.time() - t1 < a.deadline:
            rounds += 1
            async def ask(k):
                try: v = await p.get(k.encode())
                except Exception: return k, None
                return k, (v.split(b"|", 2)[1].decode() if v and v.count(b"|") >= 2 else None)
            answers = await asyncio.gather(*[ask(k) for k in pending])
            ready = [k for k, ver in answers if ver == a.poll_version]
            if ready and first_delivery is None: first_delivery = now()
            if ready:
                await asyncio.gather(*[one(k) for k in ready]); pending = [k for k in pending if k not in set(ready)]
            else:
                await asyncio.sleep(0.01)
        p.close()
        extra = dict(poll_rounds=rounds, first_delivery_seen=first_delivery, unwritten=len(pending))
    else:
        await asyncio.gather(*[one(k) for k in keys]); extra = {}
    if a.written_file: open(a.written_file, "w").write("\n".join(written) + "\n")
    st = c.stats(); await c.close()
    print(json.dumps(dict(phase="written", n=len(keys), ok=ok, err=err, errors=errors, secs=round(time.time() - t1, 2),
                          replica_write_failures=st.replica_write_failures, members=members, version=a.version, **extra)), flush=True)

asyncio.run(main())
