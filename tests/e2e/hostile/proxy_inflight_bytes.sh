#!/bin/bash
# Proxy: one connection pipelines 1 MB writes while the backend is stalled.
# The proxy bounded a client's in-flight requests by count, not bytes, so
# ~2.2 MB of each frame piled up until a 512 MiB proxy was OOM-killed.
#
# PASS: the proxy is never OOM-killed and its peak RSS stays under 50% of
# its limit, while the client really did try to send far more than that.
source "$(dirname "$0")/lib.sh"
hl_begin proxy-inflight
LIMIT=512

disc_up disc 128m --replication-factor 1
node_up node 256m
wait_count nodes 1 90 || { T "FAIL: the node never registered"; exit 1; }
proxy_up proxy ${LIMIT}m
wait_count proxies 1 90 || { T "FAIL: the proxy never registered"; exit 1; }

py proxy_warm.py "$P-proxy:8358" || { T "FAIL: warm-up through the proxy"; exit 1; }
T "node paused: its connection stays open, so replies never come"
docker pause "$P-node" >/dev/null

# 300 x 1 MB is what OOM-killed the unbounded proxy after ~213 frames.
result=$(py proxy_pipeline.py "$P-proxy:8358" 300 1000000 60)
T "client: $result"
sleep 2
verdict "$P-proxy" $LIMIT 50 "pipelined 1 MB S with the node stalled"
hl_result
