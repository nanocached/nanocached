"""#93 probe: watch one node (A) answer W for keys it legitimately owns.

seed: picks N keys routed to A in the current (pre-join) ring, SETs them on A.
watch: every INTERVAL seconds, raw-GETs all of them on A and prints a line
whenever the (V, N, W, err) histogram changes.  Raw protocol, untagged,
no SDK routing/refresh in the way.
"""
import socket, sys, time, json
from nanocached._hashring import HashRing

node_addr, disc_addr, mode = sys.argv[1], sys.argv[2], sys.argv[3]
N = int(sys.argv[4]) if len(sys.argv) > 4 else 200
INTERVAL = 0.1
PREFIX = b"p93-"


def fetch_roster(addr):
    h, p = addr.rsplit(":", 1)
    s = socket.create_connection((h, int(p)), timeout=5); f = s.makefile("rb"); s.sendall(b"L\n")
    line = f.readline(); assert line.startswith(b"N"), line
    cnt = int(line.split()[1]); out = []
    for _ in range(cnt):
        nl, al = map(int, f.readline().split()); n = f.read(nl).decode(); a = f.read(al).decode(); f.readline()
        out.append((n, a))
    s.close(); return out


def my_name(roster):
    h, p = node_addr.rsplit(":", 1); ip = socket.gethostbyname(h)
    for n, a in roster:
        if a == f"{ip}:{p}":
            return n
    raise SystemExit(f"node {node_addr} ({ip}:{p}) not in roster {roster}")


def a_keys():
    roster = fetch_roster(disc_addr); me = my_name(roster)
    ring = HashRing([n for n, _ in roster]); keys = []; i = 0
    while len(keys) < N:
        k = PREFIX + str(i).encode()
        if ring.route(k) == me:
            keys.append(k)
        i += 1
    return me, keys


def connect():
    h, p = node_addr.rsplit(":", 1)
    s = socket.create_connection((h, int(p)), timeout=5); return s, s.makefile("rb")


def read_resp(f):
    m = f.read(1)
    if m == b"V":
        hdr = f.readline(); ln = int(hdr.split()[0]); f.read(ln); return "V"
    if m in (b"S", b"N", b"W", b"D"):
        f.read(1); return m.decode()
    if m == b"E":
        return "E:" + f.readline().decode().strip()
    return "?" + repr(m)


if mode == "seed":
    me, keys = a_keys(); s, f = connect()
    for k in keys:
        s.sendall(b"S %d %d\n%b%b" % (len(k), 5, k, b"hello"))
        r = read_resp(f); assert r == "S", (k, r)
    print(f"seeded {len(keys)} keys owned by {me}", flush=True); sys.exit(0)

# watch
me, keys = a_keys(); print(f"watching {len(keys)} keys owned by {me}", flush=True)
s, f = connect(); last = None; t0 = time.time()
while True:
    hist = {"V": 0, "N": 0, "W": 0, "other": 0}
    try:
        for k in keys:
            s.sendall(b"G %d\n%b" % (len(k), k))
            r = read_resp(f); hist[r if r in hist else "other"] += 1
    except Exception as e:
        hist = {"err": str(e)}
        try: s.close()
        except Exception: pass
        time.sleep(0.5); s, f = connect()
    key = json.dumps(hist, sort_keys=True)
    if key != last:
        print(json.dumps({"ts": time.strftime("%H:%M:%S") + ".%03d" % (int(time.time() * 1000) % 1000), "t": round(time.time() - t0, 2), **hist}), flush=True)
        last = key
    time.sleep(INTERVAL)
