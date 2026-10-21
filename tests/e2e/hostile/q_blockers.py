"""Opens connections with a tiny receive buffer, asks `Q`, and never reads.

usage: q_blockers.py host:port connections hold_seconds

SO_RCVBUF is set before connect so the window stays tiny: the kernel cannot
absorb a large reply, and the server's write stays blocked, holding whatever
the reply cost it, until its own write timeout closes the connection.
"""
import json
import socket
import sys
import time

host, port = sys.argv[1].rsplit(":", 1)
connections, hold = int(sys.argv[2]), float(sys.argv[3])
held, failed = [], 0
for _ in range(connections):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    sock.settimeout(10)
    try:
        sock.connect((host, int(port)))
        sock.sendall(b"Q\n")
        held.append(sock)
    except OSError:
        failed += 1
        sock.close()
print(json.dumps({"held": len(held), "failed": failed}), flush=True)
time.sleep(hold)
