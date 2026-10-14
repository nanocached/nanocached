#!/bin/bash
# Decommission / kill a node that joined seconds earlier, under load.
#   MODE=decom-fresh      join n4 -> GAP s -> docker stop -t 40 n4
#   MODE=decom-lingering  join n4 -> join n5 -> GAP s -> docker stop -t 40 n4 (n4 is a lingering joiner on n1..n3)
#   MODE=kill-lingering   join n4 -> join n5 -> GAP s -> SIGKILL n4 -> eviction -> docker start n4
#   MODE=decom-join       docker stop -t 40 n1 (oldest) and spawn a joiner n4 at once (M lands on a draining node)
#   MODE=kill-fresh       join n4 -> GAP s -> SIGKILL n4 -> eviction -> docker start n4
#   *-rejoin              after the leave/eviction, spawn one more joiner (replaces the senders' slot)
#   NODE=... DISC=... MODE=... GAP=3 RUN=x ./freshjoin.sh
export NET=e2e-fresh
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=nfr; D=$P-disc:8357; MODE=${MODE:-decom-fresh}; GAP=${GAP:-3}
T(){ echo "### $(date -u +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/fresh-$RUN"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
live=(); seq_no=0
spawn(){ seq_no=$((seq_no+1)); spawned=$P-n$seq_no; node_up $spawned $D --drain-timeout 20 --metrics-port 9100; live+=("$spawned"); }
for _ in 1 2 3; do spawn; wait_members ${#live[@]} $D 120 || { T "FAIL: bootstrap"; cleanup; exit 1; }; done
docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
LOAD_SECS=${LOAD_SECS:-90}
docker run -d --name $P-load --network $NET -v "$E:/w" -w /w e2e-client python load.py --addr $D --conns 8 --duration $LOAD_SECS --keyspace 20000 --value-size 256 --label fresh --reconnect-on-error --hedge 0.01 --report-every 5 >/dev/null
sleep 3; t0=$(date +%s); fail=0
if [ "$MODE" = decom-join ]; then
  T "STOP nfr-n1 (docker stop -t 40) and JOIN at once"; docker stop -t 40 $P-n1 >/dev/null & STOP=$!
  sleep ${GAP:-0.5}; spawn; n4=$spawned; T "JOIN $n4 while $P-n1 drains"
  wait $STOP; rc=$(docker inspect -f '{{.State.ExitCode}}' $P-n1); T "leaver $P-n1 exited rc=$rc after $(( $(date +%s)-t0 ))s"; [ "$rc" = 0 ] || { T "FAIL: leaver rc=$rc"; fail=1; }
  wait_members 3 $D 120 || { T "FAIL: membership (want 3 = n2 n3 n4)"; fail=1; }; T "members settled after $(( $(date +%s)-t0 ))s"
  docker logs -t $P-n1 > "$L/$P-n1.log" 2>&1
  docker wait $P-load >/dev/null
else
spawn; n4=$spawned; T "JOIN $n4"; wait_members 4 $D 60 || { T "FAIL: join n4"; fail=1; }
members=4
if [[ "$MODE" == *lingering* ]]; then spawn; n5=$spawned; T "JOIN $n5"; wait_members 5 $D 60 || { T "FAIL: join n5"; fail=1; }; members=5; fi
sleep $GAP
case $MODE in
  decom-*)
    T "STOP $n4 (docker stop -t 40)"; docker stop -t 40 $n4 >/dev/null; rc=$(docker inspect -f '{{.State.ExitCode}}' $n4)
    T "leaver exited rc=$rc after $(( $(date +%s)-t0 ))s"; [ "$rc" = 0 ] || { T "FAIL: leaver rc=$rc"; fail=1; }
    wait_members $((members-1)) $D 60 || { T "FAIL: membership after leave"; fail=1; }
    docker logs -t $n4 > "$L/$n4.log" 2>&1
    if [[ "$MODE" == *rejoin ]]; then sleep ${REJOIN_AFTER:-10}; spawn; T "JOIN $spawned after the leave (replaces the senders' slot)"; wait_members $members $D 60 || { T "FAIL: rejoin"; fail=1; }; T "joined after $(( $(date +%s)-t0 ))s"; fi;;
  kill-*)
    T "KILL $n4"; docker kill $n4 >/dev/null
    wait_members $((members-1)) $D 60 || { T "FAIL: eviction"; fail=1; }
    T "evicted after $(( $(date +%s)-t0 ))s; restarting $n4"; docker start $n4 >/dev/null
    wait_members $members $D 60 || { T "FAIL: rejoin"; fail=1; }; T "rejoined after $(( $(date +%s)-t0 ))s"
    if [[ "$MODE" == *rejoin ]]; then sleep ${REJOIN_AFTER:-10}; spawn; T "JOIN $spawned after the restart"; wait_members $((members+1)) $D 60 || { T "FAIL: extra join"; fail=1; }; fi;;
esac
docker wait $P-load >/dev/null
fi
docker logs $P-load > "$L/load.log" 2>&1; load=$(tail -1 "$L/load.log"); echo "=== load: $load"
check=$(docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
echo "=== entries per node (metrics)"; for c in $(docker ps --filter name=$P-n --format '{{.Names}}' | sort); do printf "%s " $c; docker run --rm --network $NET e2e-client python -c "import urllib.request,re,sys;t=urllib.request.urlopen('http://$c:9100/metrics',timeout=3).read().decode();print(' '.join(l for l in t.splitlines() if re.search('entries|marked',l) and not l.startswith('#')))"; done
docker logs -t $P-disc > "$L/disc.log" 2>&1
for c in $(docker ps -aq --filter name=$P-); do n=$(docker inspect -f "{{.Name}}" $c | tr -d /); echo "$n $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $c)" >> "$L/ipmap.txt"; [ -f "$L/$n.log" ] || docker logs -t $c > "$L/$n.log" 2>&1; done
echo "=== WARN/ERROR categories"; cat "$L"/$P-n*.log "$L/disc.log" | grep -i "warn\|error\|panic" | sed -E 's/^[^ ]+Z //; s/[0-9a-f]{8}-[0-9a-f-]{27}/UUID/g; s/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+/ADDR/g; s/attempt [0-9]+\/[0-9]+/attempt X/g; s/after [0-9]+ attempt/after N attempt/g; s/[0-9]+ key/N key/g; s/[0-9]+ (entries|dead)/N \1/g' | sort | uniq -c | sort -rn | head -12
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { T "FAIL: untouched-range verify not clean"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { T "FAIL: corrupt"; fail=1; }
echo "$load" | grep -q '"miss": 0' || { T "FAIL: load saw misses"; fail=1; }
cleanup
echo "=== RESULT run=$RUN mode=$MODE gap=$GAP fail=$fail"
