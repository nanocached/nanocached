"""Registers many proxies with discovery (`Y`) and keeps them registered.

usage: announce_proxies.py host:port count name_bytes [rounds [first]]

Without a shared secret `Y` is open, so any client can fill the proxy
registry. Entries expire without a re-announce, so this repeats every 8 s
(`rounds` times, or until killed). It registers proxies `first` ..
`first + count - 1`, so several copies can share one registry.

Discovery takes at most ANNOUNCE_INSERT_BURST (64) new names from one
source address at once, then one every 2 s; re-announcing a name already
registered is not limited. To fill the registry quickly, run one copy per
64 names, each from its own address (its own container).
"""
import socket
import sys
import time

host, port = sys.argv[1].rsplit(":", 1)
count, name_bytes = int(sys.argv[2]), int(sys.argv[3])
rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 10**9
first = int(sys.argv[5]) if len(sys.argv) > 5 else 0


def announce(index):
    name = ("proxy-%04d-" % index).encode().ljust(name_bytes, b"x")
    token = b"token-%04d" % index
    frame = b"Y %d 8358 %d\n" % (len(name), len(token)) + name + token
    with socket.create_connection((host, int(port)), timeout=10) as sock:
        sock.sendall(frame)
        return sock.recv(2)


for round_number in range(rounds):
    started = time.time()
    replies = {}
    for index in range(first, first + count):
        reply = announce(index)
        replies[reply] = replies.get(reply, 0) + 1
    print(f"round {round_number}: {replies}", flush=True)
    time.sleep(max(0.0, 8 - (time.time() - started)))
