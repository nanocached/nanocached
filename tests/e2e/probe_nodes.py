"""Ask every node directly whether it holds a key: V (value), N (not here) or
W (not an owner — the node refuses, it may or may not hold a copy).

probe_nodes.py --nodes name=host:port,... --keys k1,k2,...  [--values]

One line per key: `<key> name1=V(<ver>) name2=N name3=W ...`. With --values
the version field of a `<key>|<ver>|...` value is shown, else the length.
Opens one identified connection per node (no discovery, no ring), so a
`W` tells that the node does not consider itself an owner, which the SDK's
routed read never shows.
"""
import argparse, asyncio
from nanocached._identify import connect_and_identify, NodeTarget
from nanocached._connection import Connection

ap = argparse.ArgumentParser()
ap.add_argument("--nodes", required=True); ap.add_argument("--keys", required=True)
ap.add_argument("--values", action="store_true")
a = ap.parse_args()
nodes = [n.split("=", 1) for n in a.nodes.split(",")]
keys = [k for k in a.keys.split(",") if k]

async def open_node(host, port):
    t = await connect_and_identify(host, int(port), None, None)
    assert isinstance(t, NodeTarget), f"{host}:{port} answered as discovery"
    return Connection(t.reader, t.writer, tagged=t.tagged)

async def main():
    conns = {}
    for name, hp in nodes:
        host, port = hp.rsplit(":", 1)
        try: conns[name] = await open_node(host, port)
        except Exception as e: conns[name] = e
    for k in keys:
        out = []
        for name, c in conns.items():
            if isinstance(c, Exception): out.append(f"{name}=ERR({type(c).__name__})"); continue
            try:
                v = await c.get(k.encode())
                if v is None: out.append(f"{name}=N")
                elif a.values:
                    parts = v.split(b"|", 2); out.append(f"{name}=V({parts[1].decode() if len(parts) == 3 else len(v)})")
                else: out.append(f"{name}=V({len(v)})")
            except Exception as e:
                out.append(f"{name}=W" if type(e).__name__ == "WrongNodeError" else f"{name}=ERR({type(e).__name__})")
        print(k, *out, flush=True)
    for c in conns.values():
        if not isinstance(c, Exception): c.close()

asyncio.run(main())
