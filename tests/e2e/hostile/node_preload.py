"""Stores a few small keys on a node so a join has something to hand off."""
import socket
import sys

host, port = sys.argv[1].rsplit(":", 1)
count = int(sys.argv[2])
sock = socket.create_connection((host, int(port)), timeout=10)
reader = sock.makefile("rb")
for index in range(count):
    key = b"preload-%05d" % index
    sock.sendall(b"S %d 8\n" % len(key) + key + b"12345678")
    assert reader.readline() == b"S\n", f"set {index} not acknowledged"
print(f"preloaded {count}")
