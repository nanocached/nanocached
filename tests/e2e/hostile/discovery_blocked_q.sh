#!/bin/bash
# Discovery: many clients ask `Q` with a tiny receive buffer and never read.
# Each `Q` built the whole proxy-registry reply per request and held it
# while the write stayed blocked, so 703 such connections OOM-killed a
# 256 MiB discovery with a full (1,024-entry) registry.
#
# PASS: discovery is never OOM-killed and its peak RSS stays under 50% of
# its limit.
source "$(dirname "$0")/lib.sh"
hl_begin discovery-q
LIMIT=256

disc_up disc ${LIMIT}m --replication-factor 1

# Fill the proxy registry (the 1,024-entry cap) and keep it filled.
py_bg announcer announce_proxies.py "$P-disc:8357" 1024 128
wait_count proxies 1024 180 || { T "FAIL: the registry never filled"; docker logs "$P-announcer" 2>&1 | tail -3; exit 1; }
T "registry full: 1,024 proxies"

# Four source IPs (the per-IP connection cap is 256), each holding 256
# connections that asked `Q` and never read.
for i in 1 2 3 4; do
  py_bg "blocker$i" q_blockers.py "$P-disc:8357" 256 25
done
sleep 20
held=0
for i in 1 2 3 4; do
  line=$(docker logs "$P-blocker$i" 2>&1 | head -1)
  T "blocker$i: $line"
  held=$(( held + $(json_int "$line" held) ))
done
require_at_least "connections held asking Q" "$held" 1000
verdict "$P-disc" $LIMIT 50 "1,024 clients asking Q with a 1 KiB receive buffer"
hl_result
