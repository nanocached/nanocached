#!/bin/bash
# Every writing command around a membership change.
#
#   tools/conformance/member.sh <R> <join|leave|evict|joinjoin|joinleave|leavejoin> [budget]
#
# Starts a cluster (3 nodes for join/joinjoin, else 4), waits until no node has logged a migration for 62 s,
# starts member_workload.py (working-tree Python SDK) which preloads keys and then runs serial sequences of
# every writing command, and changes the cluster while it runs:
#   join       start node d                         evict      SIGKILL node a
#   leave      SIGTERM node a                       joinjoin   start d, 3 s later start e
#   joinleave  SIGTERM a, 2 s later start e         leavejoin  start e, 3 s later SIGTERM a
# After the workload ended (75 s after the change) and the cluster was quiet for 62 s, member_read.py reads
# every judged key from every live node. `budget` starts the nodes with --namespace-budget nsA=8000000.
# Env: RUN, WATCHDOG, LOOP_AFTER_GO. Output: tools/conformance/results/<RUN>/member-<scenario>-R<R>[-budget]/
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"; REPO="$(cd "$HERE/../.." && pwd)"
R=${1:?R}; SCEN=${2:?scenario}; BUDGET=${3:-}
RUN=${RUN:-$(date +%H%M%S)}
NODE=${NODE:-nanocached-node:conformance}; DISC=${DISC:-nanocached-discovery:conformance}
NET=conf-$RUN-mb; P=cm$RUN
OUT=${OUT:-$HERE/results/$RUN/member-$SCEN-R$R${BUDGET:+-budget}}; WATCHDOG=${WATCHDOG:-1500}
case $SCEN in join|joinjoin) N0=3;; *) N0=4;; esac
NARGS=(); [ -n "$BUDGET" ] && NARGS=(--namespace-budget nsA=8000000)
mkdir -p "$OUT"; rm -f "$OUT/ctl"
cleanup() { exec 3>&- 2>/dev/null; docker rm -f $(docker ps -aq --filter "name=$P-") >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
trap cleanup EXIT
( sleep "$WATCHDOG"; echo "WATCHDOG fired after ${WATCHDOG}s" >&2; cleanup; pkill -P $$ >/dev/null 2>&1 ) 2>/dev/null &
WD=$!
docker network create "$NET" >/dev/null

members() { docker run --rm --network "$NET" -v "$REPO/tests/e2e:/w" -w /w e2e-client python disc.py "$1" 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'; }
wait_members() { for _ in $(seq 1 120); do [ "$(members "$1")" = "$2" ] && return 0; sleep 1; done; echo "TIMEOUT waiting for $2 members" >&2; return 1; }
start_node() { docker run -d --name $P-$1 --network "$NET" "$NODE" --host 0.0.0.0 --port 8356 --discovery $P-disc:8357 ${NARGS[@]+"${NARGS[@]}"} >/dev/null; }
live_nodes() { local out=""; for n in a b c d e; do
    [ "$(docker inspect -f '{{.State.Running}}' $P-$n 2>/dev/null)" = true ] && out="$out,$P-$n:8356"; done; echo "${out#,}"; }
quiet_wait() { # max seconds
  local t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$1" ]; do
    local busy=0; for n in a b c d e; do
      [ "$(docker inspect -f '{{.State.Running}}' $P-$n 2>/dev/null)" = true ] || continue
      docker logs --since 62s $P-$n 2>&1 | grep -q "migration" && busy=1; done
    [ $busy = 0 ] && { echo $(( $(date +%s) - t0 )); return 0; }; sleep 5; done
  echo "-$(( $(date +%s) - t0 ))"; return 1; }

docker run -d --name $P-disc --network "$NET" "$DISC" --host 0.0.0.0 --port 8357 --replication-factor "$R" >/dev/null; sleep 1
i=0; for n in $(echo a b c d | cut -d' ' -f1-$N0); do i=$((i+1)); start_node $n; wait_members $P-disc:8357 $i || exit 1; done
Q1=$(quiet_wait 300)

mkfifo "$OUT/ctl"
docker run -i --rm --name $P-wl --network "$NET" -e NANOTEST_ADDRESSES=$P-disc:8357 -e RUN=$RUN -e LOOP_AFTER_GO=${LOOP_AFTER_GO:-75} -e CLEAR_DENSE=${CLEAR_DENSE:-} \
  -v "$REPO:/repo:ro" -v "$OUT:/out" -w /repo/tools/conformance -e PYTHONPATH=/repo/sdk/python/src e2e-client \
  python member_workload.py /out/ops.jsonl < "$OUT/ctl" > "$OUT/workload.out" 2> "$OUT/workload.err" &
WL=$!
exec 3> "$OUT/ctl"
ready=0; for _ in $(seq 1 180); do grep -q READY "$OUT/workload.out" 2>/dev/null && { ready=1; break; }; sleep 1; done
[ $ready = 1 ] || { echo "workload never became ready" >&2; cat "$OUT/workload.err" >&2; exit 1; }
sleep 6
echo go >&3
case $SCEN in
  join) start_node d ;;
  leave) docker kill -s TERM $P-a >/dev/null ;;
  evict) docker kill $P-a >/dev/null ;;
  joinjoin) start_node d; sleep 3; echo go2 >&3; start_node e ;;
  joinleave) docker kill -s TERM $P-a >/dev/null; sleep 2; echo go2 >&3; start_node e ;;
  leavejoin) start_node e; sleep 3; echo go2 >&3; docker kill -s TERM $P-a >/dev/null ;;
  *) echo "unknown scenario $SCEN" >&2; exit 2 ;;
esac
for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Running}}' $P-wl 2>/dev/null)" = true ] || break; sleep 5; done
docker rm -f $P-wl >/dev/null 2>&1; wait $WL 2>/dev/null
exec 3>&-
Q2=$(quiet_wait 400); sleep 10
NODES=$(live_nodes)
{ echo "run=$RUN R=$R scenario=$SCEN start_nodes=$N0 budget=${BUDGET:-none} commit=$(git -C "$REPO" rev-parse --short HEAD)"
  echo "quiet_before_s=$Q1 quiet_after_s=$Q2 live=$NODES members=$(members $P-disc:8357)"; } > "$OUT/env.txt"
docker run --rm --network "$NET" -v "$HERE:/h:ro" -v "$OUT:/out" e2e-client python /h/member_read.py /out "$NODES" "$R" 2>&1 | tee "$OUT/reader.txt"
kill $WD >/dev/null 2>&1
mkdir -p "$OUT/logs"; for c in $(docker ps -a --format '{{.Names}}' --filter "name=$P-"); do docker logs "$c" > "$OUT/logs/$c.log" 2>&1; done
rm -f "$OUT/ctl"; echo "results in $OUT"
