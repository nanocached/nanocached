"""wire_probe.py <host:port> <label>: protocol-level facts, no SDK in the way.

Each probe opens its own connection (an error reply usually closes it) and
prints one JSON line: what was sent (summary) and the raw reply. These are the
oracle for the SDK-level cases: what the node/proxy itself accepts, as opposed
to what an SDK lets through.
"""
import json
import socket
import sys
import time

host, port = sys.argv[1].rsplit(":", 1)
label = sys.argv[2]
MAX = 1024 * 1024  # the node's and the proxy's MAX_REQUEST_SIZE (whole request, header included)


def talk(frames, wait=0.5):
    """Send each frame in turn, return the list of replies (bytes)."""
    s = socket.create_connection((host, int(port)), timeout=5)
    out = []
    try:
        for f in frames:
            if isinstance(f, (int, float)):
                time.sleep(f)
                continue
            s.sendall(f)
            s.settimeout(wait if len(f) < 100_000 else 3)
            data = b""
            try:
                while True:
                    chunk = s.recv(65536)
                    if not chunk:
                        data += b"<EOF>"
                        break
                    data += chunk
                    if data.endswith(b"\n") and (data.startswith((b"S\n", b"N\n", b"D\n", b"E\n", b"W\n", b"T\n", b"B\n", b"R\n")) or data.startswith(b"V ") and b"\n" in data):
                        # a V reply is complete once header + declared bytes are in
                        if data.startswith(b"V "):
                            head, _, rest = data.partition(b"\n")
                            if len(rest) < int(head[2:]):
                                continue
                        break
            except socket.timeout:
                data += b"<TIMEOUT>"
            out.append(data)
    except (BrokenPipeError, ConnectionResetError) as e:
        out.append(b"<" + type(e).__name__.encode() + b">")
    finally:
        s.close()
    return out


def show(b):
    return b.decode("latin-1") if len(b) < 80 else f"{len(b)} bytes: " + b[:40].decode("latin-1") + "..."


def probe(name, summary, frames, wait=0.5):
    replies = talk(frames, wait)
    print(json.dumps({"sdk": "wire", "label": label, "case": name, "sent": summary, "replies": [show(r) for r in replies]}, separators=(",", ":")), flush=True)


def S(key, value, ttl=None):
    t = f" {ttl}" if ttl is not None else ""
    return f"S {len(key)} {len(value)}{t}\n".encode() + key + value


def G(key):
    return f"G {len(key)}\n".encode() + key


def D(key):
    return f"D {len(key)}\n".encode() + key


r = str(int(time.time() * 1000))
k = lambda n: f"w:{r}:{n}".encode()

probe("ttl.literal0", "S ttl=0, then G", [S(k("t0"), b"v", 0), G(k("t0"))])
probe("ttl.1", "S ttl=1, G, wait 2.2s, G", [S(k("t1"), b"v", 1), G(k("t1")), 2.2, G(k("t1"))])
probe("ttl.none", "S no ttl, G", [S(k("t2"), b"v"), G(k("t2"))])
for name, ttl in [("neg", "-1"), ("frac", "1.5"), ("u32+1", "4294967296"), ("i64max", "9223372036854775807"),
                  ("u64max", "18446744073709551615"), ("u64max+1", "18446744073709551616"), ("text", "abc")]:
    probe("ttl." + name, f"S ttl={ttl}, then G", [S(k("tt" + name), b"v", ttl), G(k("tt" + name))])
probe("key.empty", "S/G/D with a 0-byte key", [S(b"", b"v"), G(b""), D(b"")])
probe("value.empty", "S with a 0-byte value, then G", [S(k("ve"), b""), G(k("ve"))])
probe("get.missing", "G of an absent key", [G(k("absent"))])
probe("delete", "S, D, D", [S(k("d"), b"v"), D(k("d")), D(k("d"))])

# whole-request bound: header + key + value <= MAX
for delta in (0, 1):
    key = b"k"
    n = 1
    while True:  # find value length so that len(header)+len(key)+n == MAX + delta
        total = len(f"S {len(key)} {n}\n") + len(key) + n
        if total >= MAX + delta:
            break
        n += max(1, (MAX + delta - total) - 7)
    n = max(1, n - 16)
    while len(f"S {len(key)} {n}\n") + len(key) + n < MAX + delta:
        n += 1
    req = S(key, b"x" * n)
    assert len(req) == MAX + delta, (len(req), MAX + delta)
    probe(f"size.request={MAX + delta}", f"S with key 1 B, value {n} B: whole request {len(req)} B", [req, G(key)], 5)

# the largest key that still fits as a whole request, with an empty value
klen = MAX - len("S 0 0\n")
while len(f"S {klen} 0\n") + klen > MAX:
    klen -= 1
for dk in (0, 1):
    key = b"K" * (klen + dk)
    probe(f"size.key_only={len(f'S {len(key)} 0\n'.encode()) + len(key)}", f"S with key {len(key)} B and empty value", [S(key, b"")], 5)
