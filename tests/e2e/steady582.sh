#!/bin/bash
# Steady-state measurement of the #582 ownership reclaim: one join, nothing
# else, under a writer and a mixed load.
#
#   3 nodes at R=2, seeded (k0..k29999 by verify.py, s0..s19999 by the
#   versioned writer), load.py on k0..k19999 and stalewrite.py rewriting
#   s* for the whole run. After the ring has been stable for longer than
#   RECLAIM_GRACE, n4 joins. Every node's nanocached_node_entries and its
#   "INFO reclaimed" count are sampled every 10 s for WATCH seconds, so the
#   delay from "membership updated" (4 members) to the first reclaim is
#   measured per node. Then n4 is killed (the only other holder of what the
#   reclaim removed from the old nodes is gone), and after its eviction
#   every seeded key is read back: load misses, untouched misses (k20000..),
#   k0..k19999 misses and the writer's stale/miss must all be 0.
#
#   NODE=nanocached-node:<tag> DISC=nanocached-discovery:<tag> [WATCH=360] ./steady582.sh
export NET=e2e-steady
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=st; D=$P-disc:8357
WATCH=${WATCH:-360}; STABLE=${STABLE:-135}
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/steady-$RUN"; rm -rf "$L"; mkdir -p "$L"
LOAD_SECS=$(( STABLE + WATCH + 150 ))
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
client(){ docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python "$@"; }
entries_of(){ docker run --rm --network $NET e2e-client python -c "import urllib.request;print([l.split()[1] for l in urllib.request.urlopen('http://$1:9100/metrics',timeout=5).read().decode().splitlines() if l.startswith('nanocached_node_entries ')][0])" 2>/dev/null || echo "?"; }
reclaims_of(){ docker logs $1 2>&1 | grep -c "INFO reclaimed"; }
snapshot(){ local line="$1"; for n in "${@:2}"; do line="$line $n=$(entries_of $n)/r$(reclaims_of $n)"; done; echo "$line" | tee -a "$L/entries.txt"; }
wait_log(){ local c=$1 pat=$2 want=${3:-1} to=${4:-60} t0=$(date +%s); while :; do n=$(docker logs $c 2>&1 | grep -c -- "$pat"); [ "$n" -ge "$want" ] && return 0; [ $(( $(date +%s)-t0 )) -ge $to ] && { T "TIMEOUT waiting '$pat' x$want in $c"; return 1; }; sleep 0.2; done; }
ts(){ docker logs -t $P-$1 2>&1 | grep -- "$2" | head -1 | cut -c12-23; }
ts_last(){ docker logs -t $P-$1 2>&1 | grep -- "$2" | tail -1 | cut -c12-23; }
save_logs(){ for c in disc n1 n2 n3 n4 swriter load; do docker logs -t $P-$c > "$L/$c.log" 2>&1; done; }
die(){ T "FAIL: $*"; save_logs; cleanup; exit 1; }
fail=0

cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
for i in 1 2 3; do node_up $P-n$i $D --drain-timeout 20 --metrics-port 9100; wait_members $i $D 60 >/dev/null || die "bootstrap n$i"; done
T "seed k0..k29999"
client verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
docker run -d --name $P-swriter --network $NET -v "$E:/w" -w /w e2e-client python stalewrite.py --addr $D --n 20000 --workers 4 --prefix s --size 256 --settle 15 --report-every 30 >/dev/null
wait_log $P-swriter '"phase": "seeded"' 1 120 || die "versioned writer did not seed"
docker run -d --name $P-load --network $NET -v "$E:/w" -w /w e2e-client python load.py --addr $D --conns 8 --duration $LOAD_SECS --keyspace 20000 --value-size 256 --label steady --reconnect-on-error --hedge 0.01 --report-every 30 >/dev/null
snapshot "seeded" $P-n1 $P-n2 $P-n3
T "ring stable for ${STABLE}s (longer than the reclaim grace) before the join"
sleep "$STABLE"
snapshot "pre-join" $P-n1 $P-n2 $P-n3
T "join n4"
promoted_before=$(docker logs $P-disc 2>&1 | grep -c "join promoted:")   # the bootstrap joins printed it too
node_up $P-n4 $D --drain-timeout 20 --metrics-port 9100
wait_log $P-disc "join promoted:" $(( promoted_before + 1 )) 120 || die "n4 not promoted"
T "n4 promoted at $(ts_last disc 'join promoted:') UTC"
wait_members 4 $D 120 >/dev/null || die "membership after the join"
t0=$(date +%s)
while [ $(( $(date +%s) - t0 )) -lt "$WATCH" ]; do
  snapshot "+$(( $(date +%s) - t0 ))s" $P-n1 $P-n2 $P-n3 $P-n4
  sleep 10
done
# A sender installs the post-join ring itself when its handoff completes
# (no "membership updated" line for it), so the delay is anchored on
# "migration completed": the ack that first names the joiner follows within
# a heartbeat, then the 120 s grace, then the next 30 s reclaim tick.
echo "=== per node: handoff to n4 completed -> join confirmed -> first reclaim (UTC)"
for i in 1 2 3; do
  c=$(ts_last n$i "migration completed"); j=$(ts_last n$i "join of .* confirmed"); r=$(ts n$i "INFO reclaimed")
  d=$(python3 -c "
import sys,datetime as D
def t(x): return D.datetime.strptime(x,'%H:%M:%S.%f')
c,r=sys.argv[1],sys.argv[2]
print(round((t(r)-t(c)).total_seconds(),1) if r else 'none')" "$c" "$r")
  echo "n$i completed=$c confirmed=$j first_reclaim=${r:-none} delay=${d}s lines: $(docker logs $P-n$i 2>&1 | grep "INFO reclaimed" | sed 's/.*INFO //' | head -3 | tr '\n' ';')"
done | tee "$L/reclaim-times.txt"
T "kill n4"
docker kill $P-n4 >/dev/null
wait_members 3 $D 120 >/dev/null || die "n4 not evicted"
sleep 5
T "stop the versioned writer (settle 15 s, then it reads every s* key back)"
tmo 300 docker stop -t 240 $P-swriter >/dev/null
versioned=$(docker logs $P-swriter 2>&1 | grep '"phase": "checked"' | tail -1)
echo "writer: $(echo "$versioned" | cut -c1-300)"
T "wait for the load to end"
tmo $(( LOAD_SECS + 60 )) docker wait $P-load >/dev/null || { T "FAIL: load did not finish in time"; fail=1; docker kill $P-load >/dev/null 2>&1; }
load=$(docker logs $P-load 2>/dev/null | grep '^{"label"' | tail -1)
echo "load: $(echo "$load" | cut -c1-300)"
T "read back k0..k19999 (load keyspace, miss only) and k20000..k29999 (untouched, exact value)"
seq 0 19999 | sed 's/^/k/' > "$L/kload.txt"
client read_keys.py --addr $D --keys-file /w/logs/steady-$RUN/kload.txt | tee "$L/read-kload.json"
client verify.py check --addr $D,$D --n 10000 --start 20000 --size 256 | tail -1 | tee "$L/untouched.json"
save_logs
echo "=== entries:"; cat "$L/entries.txt"
echo "=== reclaim WARN/ERROR lines:"; grep -h "WARN\|ERROR" "$L"/n?.log | grep -v "accepted connection" | sed 's/[0-9.]*:[0-9]*:/X:/' | sort | uniq -c | sort -rn | head -8
cleanup
if [ "$fail" = 0 ]; then T "DONE (logs in $L)"; else T "DONE with a FAIL above (logs in $L)"; fi
exit "$fail"
