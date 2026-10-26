#!/bin/bash
# Issue #970: a key deleted or expired by a touch while a former owner holds
# the dead copy a join took from it came back when a leave made that node an
# owner again before the mark sweep had removed the copy. Three nodes at R=2,
# settled past the bootstrap forwarding windows; 1,800 keys without a TTL;
# join d and wait GAP seconds after promotion; through the SDK (its ring has
# d) expire every r key (touch 0), delete every e key, rewrite every k key;
# then decommission a (KILL=1: kill it instead, so the change is a liveness
# eviction ~15 s later) after DELAY seconds; 10 s after a left, read every
# key from b, c, d raw, and once more 130 s later. Nothing runs between the
# join and the leave but the writes, and GAP defaults to 0: the leave must
# land before discovery's confirming heartbeat (up to 5 s) and the mark sweep
# (every 5 s) have removed the copies the join displaced, or there is nothing
# to revive — a few seconds of docker round trips there once hid the bug.
# PASS: 0 keys in every class at both checks. Before the fix: 99-106 of 600
# r and e keys back on the node the join had displaced (0 k: the leaver's
# unconditional handoff carried v2), still there 130 s later; DELAY=130
# (leave after the reclaim grace) and KILL=1 (eviction after the sweep) were
# already 0.
#   docker build --target node -t nanocached-node:dev .
#   docker build --target discovery -t nanocached-discovery:dev .
#   NODE=nanocached-node:dev DISC=nanocached-discovery:dev [GAP=0] [DELAY=0] [KILL=0] ./reclaim970.sh
export NET=e2e-reclaim970
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=r970; D=$P-disc:8357
GAP=${GAP:-0}; DELAY=${DELAY:-0}; KILL=${KILL:-0}
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
client(){ docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python "$@"; }
ent(){ for n in "$@"; do printf "%s=%s " $n "$(docker run --rm --network $NET $CLIENT python -c "import urllib.request;b=urllib.request.urlopen('http://$P-$n:9100/metrics',timeout=5).read().decode().splitlines();print([l.split()[1] for l in b if l.startswith('nanocached_node_entries ')][0])" 2>/dev/null || echo '?')"; done; echo; }
cleanup; docker network create $NET >/dev/null
fail=0
disc_up $P-disc --replication-factor 2; sleep 1
i=0; for n in a b c; do i=$((i+1)); node_up $P-$n $D --drain-timeout 20 --metrics-port 9100; wait_members $i $D 60 || { T "FAIL: bootstrap"; cleanup; exit 1; }; done
T "settle 70s (past the bootstrap joins' forwarding windows)"; sleep 70
client reclaim970.py seed $D 600
T "join d, then wait ${GAP}s"; node_up $P-d $D --drain-timeout 20 --metrics-port 9100; wait_members 4 $D 60 || { T "FAIL: join"; fail=1; }; sleep $GAP
T "expire r*, delete e*, rewrite k* (a fresh client: its ring has d)"; client reclaim970.py apply $D 600
[ "$DELAY" -gt 0 ] && { T "wait ${DELAY}s before the leave"; sleep $DELAY; }
if [ "$KILL" = 1 ]; then T "kill a (liveness eviction)"; docker kill $P-a >/dev/null; else T "decommission a (docker stop -t 40)"; docker stop -t 40 $P-a >/dev/null; fi
wait_members 3 $D 60 || { T "FAIL: membership after a left"; fail=1; }; sleep 10
T "entries 10s after the leave: $(ent b c d)"
check1=$(tmo 120 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python reclaim970.py check $D 600 $P-b,$P-c,$P-d); echo "=== check: ${check1:-NO RESULT}"
T "same check 130s later (past the reclaim grace)"; sleep 130
check2=$(tmo 120 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python reclaim970.py check $D 600 $P-b,$P-c,$P-d); echo "=== check: ${check2:-NO RESULT}"
for c in "$check1" "$check2"; do
  echo "$c" | grep -q '"stale_or_resurrected_keys": 0' || { T "FAIL: no check result"; fail=1; }
  echo "$c" | grep -q '"stale_or_resurrected_keys": [1-9]' && { T "FAIL: a key came back or stayed stale"; fail=1; }
done
for n in b c d; do echo "== $P-$n"; docker logs $P-$n 2>&1 | grep -i -E "reclaim|re-replication|dead cop|membership updated" | sed -E 's/^[^ ]+ //' | cut -c1-160 | tail -4; done
cleanup
if [ $fail = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
