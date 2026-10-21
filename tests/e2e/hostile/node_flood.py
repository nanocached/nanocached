"""Floods a node with `s` frames that each carry a unique ~1 MB namespace.

usage: node_flood.py host:port connections seconds [namespace_bytes]

The key and value are one byte each, so the write itself is tiny; the frame
that carries it is not. A node that queues such a write for a stalled joiner
by slicing the client's frame pins the whole namespace for every one. The
two-byte ack is read, so a node that holds the ack until the write fits just
slows this client down instead of failing it.
"""
import json
import socket
import sys
import threading
import time

host, port = sys.argv[1].rsplit(":", 1)
connections, seconds = int(sys.argv[2]), float(sys.argv[3])
ns_bytes = int(sys.argv[4]) if len(sys.argv) > 4 else 1_000_000
deadline = time.time() + seconds
lock = threading.Lock()
totals = {"acked": 0, "refused": 0, "wire_mb": 0, "ended": {}}


def worker(index):
    sent = acked = refused = 0
    ended = "deadline"
    try:
        sock = socket.create_connection((host, int(port)), timeout=10)
        sock.settimeout(10)
        while time.time() < deadline:
            prefix = b"%03d-%08d-" % (index, sent)  # unique per frame
            namespace = prefix + b"n" * (ns_bytes - len(prefix))
            sock.sendall(b"s %d 1 1\n" % len(namespace) + namespace + b"kv")
            sent += 1
            reply = sock.recv(2)
            if reply == b"S\n":
                acked += 1
            elif reply == b"":
                ended = "closed"
                break
            else:
                refused += 1  # W or an error: the node stopped taking it
                if refused >= 3:
                    ended = "refused"
                    break
    except socket.timeout:
        ended = "blocked"
    except OSError as error:
        ended = type(error).__name__
    with lock:
        totals["acked"] += acked
        totals["refused"] += refused
        totals["wire_mb"] += sent * ns_bytes // 1_000_000
        totals["ended"][ended] = totals["ended"].get(ended, 0) + 1


threads = [threading.Thread(target=worker, args=(i,)) for i in range(connections)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
print(json.dumps(totals))
