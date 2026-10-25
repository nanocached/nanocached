#!/bin/bash
# Node: a joiner stops answering mid-join while clients keep writing.
# Each write the node queued for the joiner pinned the client's whole
# request frame, uncounted, so a tiny write carrying a 1 MB namespace cost
# 1 MB and a 256 MiB node was OOM-killed after ~186 frames.
#
# PASS: node A is never OOM-killed and its peak RSS stays under 85% of its
# limit: up to --max-memory (64 MiB) of cache, the forward queue's own
# 64 MiB bound, and working memory.
source "$(dirname "$0")/lib.sh"
hl_begin node-joiner
LIMIT=256

disc_up disc 128m --replication-factor 2
node_up a ${LIMIT}m --max-memory 67108864
node_up b 256m
wait_count nodes 2 120 || { T "FAIL: the first two nodes never registered"; exit 1; }
py node_preload.py "$P-a:8356" 1000 || { T "FAIL: preload"; exit 1; }

T "starting the joiner; pausing it as soon as A begins the handoff"
node_up c 256m
if ! wait_log "$P-a" "migration started" 60; then
  T "FAIL: A never started a migration"; exit 1
fi
docker pause "$P-c" >/dev/null

# 8 connections, ~1 MB unique namespaces: the shape that pinned 425 MB.
result=$(py node_flood.py "$P-a:8356" 8 40)
T "client: $result"
# 111 MB went over the wire when this was written; a client that never
# connected or was refused at once sends none.
require_at_least "client wire MB" "$(json_int "$result" wire_mb)" 20
require_at_least "client acked writes" "$(json_int "$result" acked)" 1
sleep 2
verdict "$P-a" $LIMIT 85 "1 MB-namespace writes with the joiner stalled"
hl_result
