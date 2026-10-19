#!/bin/bash
# Scenarios that change the cluster while clients run (V5) or exercise the client lifecycle (V7).
#
#   tools/conformance/scenarios_dynamic.sh <v5|v7> [sdk ...]
#
#   v5  six clients (one per SDK, in parallel) run a versioned get/set loop against a 3-node cluster
#       while the harness: joins a node, decommissions one (SIGTERM), kills one (SIGKILL).
#       Reported per SDK: error window, error classes, stale / miss / corrupt reads. Also a via-proxy pass.
#   v7  close / after-close / double close / abandon (desync) / create-close cycles / reconnect after a
#       node restart / dial storm / exit without close, one SDK at a time.
#
# Output: tools/conformance/results/<run>/<set>/<case>.<sdk>.jsonl  (+ logs/). Every wait is bounded and a
# watchdog removes the run's containers after WATCHDOG s. Same prelude as scenarios.sh.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"; REPO="$(cd "$HERE/../.." && pwd)"
WHAT=${1:?v5|v7}; shift
SDKS=${*:-python typescript go java dotnet rust}
RUN=${RUN:-$(date +%H%M%S)}
NODE=${NODE:-nanocached-node:conformance}; DISC=${DISC:-nanocached-discovery:conformance}; PROXY=${PROXY:-nanocached-proxy:conformance}
NET=conf-$RUN-$WHAT; P=cd$RUN-$WHAT
OUT=${OUT:-$HERE/results/$RUN/$WHAT}; WATCHDOG=${WATCHDOG:-2400}
mkdir -p "$OUT"
cleanup() { docker rm -f $(docker ps -aq --filter "name=$P-") >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
trap cleanup EXIT
( sleep "$WATCHDOG"; echo "WATCHDOG fired after ${WATCHDOG}s" >&2; cleanup; pkill -P $$ >/dev/null 2>&1 ) 2>/dev/null &
WD=$!
docker network create "$NET" >/dev/null

disc() { docker run -d --name $P-$1 --network "$NET" "$DISC" --host 0.0.0.0 --port 8357 --replication-factor 2 >/dev/null; }
node() { local n=$1; shift; docker run -d --name $P-$n --network "$NET" "$NODE" --host 0.0.0.0 --port 8356 "$@" >/dev/null; }
members() { docker run --rm --network "$NET" -v "$REPO/tests/e2e:/w" -w /w e2e-client python disc.py "$1" 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'; }
wait_members() { for _ in $(seq 1 120); do [ "$(members "$1")" = "$2" ] && return 0; sleep 1; done; echo "TIMEOUT waiting for $2 members" >&2; return 1; }
cluster() { disc disc; sleep 1; local i=0; for n in $(echo a b c d e | cut -d' ' -f1-$1); do i=$((i+1)); node $n --discovery $P-disc:8357 --drain-timeout 20; wait_members $P-disc:8357 $i || return 1; done; }
proxy() { docker run -d --name $P-$1 --network "$NET" "$PROXY" --host 0.0.0.0 --port 8358 --discovery $P-disc:8357 >/dev/null; }

sc() { # sdk outfile addr via secret limit scenario args...
  local sdk=$1 file=$2 addr=$3 via=$4 secret=$5 limit=$6; shift 6
  local env=(-e NANOTEST_ADDRESSES=$addr -e NANOTEST_VIA_PROXY=$via -e NANOTEST_SECRET=$secret -e CONN_TIMEOUT_S=$limit -v "$REPO:/repo:ro" -v "$OUT:/out" --network "$NET")
  case $sdk in
    python) docker run --rm --name $P-c-$sdk-$RANDOM "${env[@]}" -w /repo/tools/conformance -e PYTHONPATH=/repo/sdk/python/src e2e-client python drivers/python/scenario.py "$@" ;;
    *) "$HERE/drivers/$sdk/scenario.sh" "$P-c-$sdk-$RANDOM" "${env[@]}" -- "$@" ;;
  esac > "$file" 2> "${file%.jsonl}.err"
}
fixture() { # name mode target-or-"" seconds-or-""
  docker run -d --name $P-$1 --network "$NET" -v "$OUT:/out" -v "$HERE/fixtures:/f" e2e-client python /f/fake_server.py "$2" 8356 ${3:-} ${4:-} >/dev/null; }
now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

case $WHAT in
v5)
  for mode in direct proxy; do
    cluster 3 || exit 1
    via=0; addr=$P-disc:8357
    [ $mode = proxy ] && { proxy px; via=1; sleep 4; }
    DUR=75
    for sdk in $SDKS; do ( sc "$sdk" "$OUT/churn-$mode.$sdk.jsonl" $addr $via "" 60 churn $DUR $mode ) & done
    START=$(now_ms)
    sleep 12; echo "event join d at +$(( $(now_ms) - START )) ms" | tee -a "$OUT/events-$mode.txt"; node d --discovery $P-disc:8357 --drain-timeout 20
    sleep 20; echo "event decommission a (SIGTERM) at +$(( $(now_ms) - START )) ms" | tee -a "$OUT/events-$mode.txt"; docker stop -t 30 $P-a >/dev/null &
    sleep 16; echo "event kill b (SIGKILL) at +$(( $(now_ms) - START )) ms" | tee -a "$OUT/events-$mode.txt"; docker kill $P-b >/dev/null
    wait $(jobs -p | grep -v "^$WD$") 2>/dev/null
    docker rm -f $(docker ps -aq --filter "name=$P-") >/dev/null 2>&1
  done ;;
v7)
  node n1; sleep 2; A=$P-n1:8356
  for sdk in $SDKS; do
    for t in close_inflight after_close double_close; do sc "$sdk" "$OUT/$t.$sdk.jsonl" $A 0 "" 60 life $t; done
    sc "$sdk" "$OUT/cycles.$sdk.jsonl" $A 0 "" 300 life cycles 300
    # abandon: through a relay that delays every reply by 2 s, so the request is still in flight when the caller gives up
    fixture relay delay $P-n1:8356 2; sleep 1
    sc "$sdk" "$OUT/abandon.$sdk.jsonl" $P-relay:8356 0 "" 90 life abandon
    docker rm -f $P-relay >/dev/null
    # reconnect: restart the node while the client polls
    ( sc "$sdk" "$OUT/reconnect.$sdk.jsonl" $A 0 "" 90 life reconnect 14 ) &
    sleep 4; docker restart -t 1 $P-n1 >/dev/null; wait $(jobs -p | grep -v "^$WD$") 2>/dev/null
    # dial storm: a listener that accepts and closes
    rm -f "$OUT/accepts.log"; fixture storm accept_close; sleep 1
    sc "$sdk" "$OUT/storm.$sdk.jsonl" $P-storm:8356 0 "" 60 life storm 10
    cp "$OUT/accepts.log" "$OUT/storm_accepts.$sdk.log" 2>/dev/null; docker rm -f $P-storm >/dev/null
    # exit without close: the process must end on its own
    S=$(now_ms)
    sc "$sdk" "$OUT/exit_noclose.$sdk.jsonl" $A 0 "" 60 life exit_noclose
    echo "{\"sdk\":\"$sdk\",\"event\":\"process_exit_ms\",\"ms\":$(( $(now_ms) - S ))}" >> "$OUT/exit_noclose.$sdk.jsonl"
  done ;;
v7x)
  # re-runs of v7 cases that were not exercised properly the first time:
  # close_inflight through a relay that delays every reply by 2 s, and the .NET create/close cycles
  node n1; sleep 2; A=$P-n1:8356
  for sdk in $SDKS; do
    fixture relay delay $P-n1:8356 2; sleep 1
    sc "$sdk" "$OUT/close_inflight_slow.$sdk.jsonl" $P-relay:8356 0 "" 90 life close_inflight
    docker rm -f $P-relay >/dev/null
  done
  sc dotnet "$OUT/cycles.dotnet.jsonl" $A 0 "" 300 life cycles 300 ;;
*) echo "unknown $WHAT"; exit 2 ;;
esac
kill $WD >/dev/null 2>&1
mkdir -p "$OUT/logs"; for c in $(docker ps -a --format '{{.Names}}' --filter "name=$P-"); do docker logs "$c" > "$OUT/logs/$c.log" 2>&1; done
echo "results in $OUT"
