"""One `S` and one `G` through the proxy, so it holds a backend connection."""
import socket
import sys

host, port = sys.argv[1].rsplit(":", 1)
sock = socket.create_connection((host, int(port)), timeout=10)
reader = sock.makefile("rb")
sock.sendall(b"S 4 5\nwarmvalue")
assert reader.readline() == b"S\n", "set not acknowledged"
sock.sendall(b"G 4\nwarm")
header = reader.readline()
assert header.startswith(b"V "), header
print("warm ok")
