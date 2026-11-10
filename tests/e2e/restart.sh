#!/bin/bash
# #570: kill a node and start it again on the same IP the moment discovery
# evicts it, under load with the diagnostic loader (load_diag.py, which
# prints the client's ring and routed owners on every miss). 3 nodes ->
# seed -> load -> join n4 -> join n5 -> SIGKILL n5 -> eviction -> docker
# start n5 at once. Records container IPs and the restart time so each
# MISS line can be placed against the node logs (analyze_restart.py).
# PASS: miss 0 and every membership step reached (exit 1 otherwise). Before #570, 150-870 misses per run: clients whose
# bootstrap dial to the dead member was still pending connected to the
# restarted process under the dead name and read N from it before its
# promotion. NEWIP=1 blocks the old IP with a placeholder container (the
# restart lands elsewhere), DELAY=n delays the restart by n seconds.
#   NODE=... DISC=... RUN=x1 ./repro_diag.sh
export NET=e2e-rdiag
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=nrd; D=$P-disc:8357
T(){ echo "### $(date -u +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter "name=^/$P-") >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
trap cleanup EXIT  # Ctrl-C or a failed step must not leave the run's containers behind
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }  # bounds a command to N seconds
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/rdiag-$RUN"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
live=(); seq_no=0
spawn(){ seq_no=$((seq_no+1)); spawned=$P-n$seq_no; node_up $spawned $D --drain-timeout 20; live+=("$spawned"); }
ipmap(){ for c in $(docker ps -q --filter "name=^/$P-"); do docker inspect -f '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $c | tr -d /; done | sort > "$L/${1:-ipmap}.txt"; }
for _ in 1 2 3; do spawn; wait_members ${#live[@]} $D 120 || { T "FAIL: bootstrap"; cleanup; exit 1; }; done
docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
LOAD_SECS=${LOAD_SECS:-75}
docker run -d --name $P-load --network $NET -v "$E:/w" -w /w e2e-client python load_diag.py --addr $D --conns 8 --duration $LOAD_SECS --keyspace 20000 --value-size 256 --label rdiag --reconnect-on-error --hedge 0.01 --report-every 5 --probe-max ${PROBE_MAX:-60} >/dev/null
sleep 3
t0=$(date +%s); fail=0
spawn; T "JOIN $spawned"; wait_members 4 $D 60 || { T "FAIL: join n4"; fail=1; }
sleep ${GAP:-2}
spawn; n5=$spawned; T "JOIN $n5"; wait_members 5 $D 60 || { T "FAIL: join n5"; fail=1; }
ipmap
sleep ${GAP:-2}
T "KILL $n5"; docker kill $n5 >/dev/null
wait_members 4 $D 60 || { T "FAIL: eviction"; fail=1; }
T "evicted after $(( $(date +%s)-t0 ))s; restarting $n5"
if [ -n "$NEWIP" ]; then docker run -d --name $P-hold --network $NET alpine:3.21 sleep 600 >/dev/null; T "placeholder $P-hold holds the old IP"; fi
[ -n "$DELAY" ] && { sleep $DELAY; T "delayed restart by ${DELAY}s"; }
docker start $n5 >/dev/null
T "docker start returned"
wait_members 5 $D 60 || { T "FAIL: rejoin"; fail=1; }
T "rejoined after $(( $(date +%s)-t0 ))s"; ipmap ipmap2
tmo $(( LOAD_SECS + 60 )) docker wait $P-load >/dev/null || { T "FAIL: load did not finish in time"; fail=1; docker kill $P-load >/dev/null 2>&1; }
docker logs -t $P-load > "$L/load.log" 2>&1; load=$(grep '"label"' "$L/load.log" | tail -1)
echo "=== load: $load"
[ -n "$load" ] || { T "FAIL: the load printed no result line (it did not run to the end)"; fail=1; }
check=$(docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
docker logs -t $P-disc > "$L/disc.log" 2>&1
for c in $(docker ps -aq --filter "name=^/$P-"); do n=$(docker inspect -f "{{.Name}}" $c | tr -d /); docker logs -t $c > "$L/$n.log" 2>&1; done
miss=$(grep -c '^.*MISS ' "$L/load.log")
[ "${miss:-x}" = 0 ] || { T "FAIL: load saw ${miss:-no count of} misses (want 0)"; fail=1; }
echo "=== RESULT run=$RUN miss=$miss probes=$(grep -c 'PROBE ' "$L/load.log") fail=$fail"
cleanup
[ "$fail" = 0 ] || exit 1
