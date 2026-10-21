"""Pipelines large `S` frames through the proxy without ever reading a reply.

usage: proxy_pipeline.py host:port frames value_bytes seconds

With the backend stalled the replies never come, so a proxy that bounds a
client's requests by count rather than bytes keeps reading (and holding) the
whole stream. A bounded one stops reading, and this client's `sendall` blocks
until its timeout: that is the end of the run, not an error.
"""
import json
import socket
import sys
import time

host, port = sys.argv[1].rsplit(":", 1)
frames, size, seconds = int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
key = b"hostile1"
frame = b"S %d %d\n" % (len(key), size) + key + b"v" * size

sock = socket.create_connection((host, int(port)), timeout=10)
sock.settimeout(5)
deadline = time.time() + seconds
sent, ended = 0, "frames"
try:
    while sent < frames:
        if time.time() >= deadline:
            ended = "deadline"
            break
        sock.sendall(frame)
        sent += 1
except socket.timeout:
    ended = "blocked"  # the proxy stopped reading
except OSError as error:
    ended = type(error).__name__
print(json.dumps({"frames_sent": sent, "wire_mb": sent * len(frame) // 1_000_000, "ended": ended}))
