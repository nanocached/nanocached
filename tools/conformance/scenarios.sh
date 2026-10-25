#!/bin/bash
# Orchestrated scenarios (need containers started, stopped or put behind a fixture):
#
#   tools/conformance/scenarios.sh <v6|v5|v7|v4> [sdk ...]
#
#   v6  connection failures, auth, busy, retryable, secret hygiene     (verify: server statuses / connection failures)
#   v5  join / decommission / kill under load: error windows per SDK    (verify: convergence on topology change)
#   v7  close, after-close, abandon, create/close cycles, restart, ...  (verify: client lifecycle)
#   v4  a node dies between seeding and get_many                        (verify: multi-get owner fallback)
#   hostile  a fake node that trickles, stalls, oversizes, bombs, backs up (verify: bounded time and memory, one table for all six SDKs; judged by hostile_check.py)
#
# Every client runs in a container on the scenario's network (discovery hands
# out container IPs). Output: tools/conformance/results/<run>/<scenario>/<case>.<sdk>.jsonl
# Every wait is bounded; a watchdog removes the run's containers after WATCHDOG s.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"; REPO="$(cd "$HERE/../.." && pwd)"
WHAT=${1:?v6|v5|v7|v4|hostile}; shift
SDKS=${*:-python typescript go java dotnet rust}
RUN=${RUN:-$(date +%H%M%S)}
CLIENT=${CLIENT:-e2e-client}  # the image the fixtures and the Python driver run in: python on the PATH, nothing else
NODE=${NODE:-nanocached-node:conformance}; DISC=${DISC:-nanocached-discovery:conformance}; PROXY=${PROXY:-nanocached-proxy:conformance}
NET=conf-$RUN-$WHAT; P=cs$RUN-$WHAT
OUT=${OUT:-$HERE/results/$RUN/$WHAT}; WATCHDOG=${WATCHDOG:-1500}
mkdir -p "$OUT"
cleanup() { docker rm -f $(docker ps -aq --filter "name=$P-") >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
trap cleanup EXIT
( sleep "$WATCHDOG"; echo "WATCHDOG fired after ${WATCHDOG}s" >&2; cleanup; pkill -P $$ >/dev/null 2>&1 ) 2>/dev/null &
WD=$!
docker network create "$NET" >/dev/null

disc() { docker run -d --name $P-$1 --network "$NET" "$DISC" --host 0.0.0.0 --port 8357 --replication-factor 2 >/dev/null; }
node() { # name [extra flags...] ; env NODE_ENV_ARGS may carry -e VAR=..
  local n=$1; shift; docker run -d --name $P-$n --network "$NET" ${NODE_ENV_ARGS:-} "$NODE" --host 0.0.0.0 --port 8356 "$@" >/dev/null; }
members() { docker run --rm --network "$NET" -v "$REPO/tests/e2e:/w" -w /w "$CLIENT" python disc.py "$1" 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'; }
wait_members() { for _ in $(seq 1 90); do [ "$(members "$1")" = "$2" ] && return 0; sleep 1; done; echo "TIMEOUT waiting for $2 members" >&2; return 1; }
cluster() { # n nodes behind discovery $P-disc
  disc disc; sleep 1; local i=0; for n in $(echo a b c d e | cut -d' ' -f1-$1); do i=$((i+1)); node $n --discovery $P-disc:8357; wait_members $P-disc:8357 $i || return 1; done; }
proxy() { docker run -d --name $P-$1 --network "$NET" ${PROXY_ENV_ARGS:-} "$PROXY" --host 0.0.0.0 --port 8358 --discovery $P-disc:8357 >/dev/null; }
wait_http_ok() { for _ in $(seq 1 60); do docker run --rm --network "$NET" "$CLIENT" python -c "import socket,sys;s=socket.create_connection(('$1',$2),2);s.sendall(b'L\n');print(s.recv(10))" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }

# run one scenario program for one SDK: sc <sdk> <outfile> <addr> <via> <secret> <conn-timeout-s> <scenario> [args]
sc() {
  local sdk=$1 file=$2 addr=$3 via=$4 secret=$5 limit=$6; shift 6
  local env=(-e NANOTEST_ADDRESSES=$addr -e NANOTEST_VIA_PROXY=$via -e NANOTEST_SECRET=$secret -e CONN_TIMEOUT_S=$limit -v "$REPO:/repo:ro" -v "$OUT:/out" --network "$NET")
  case $sdk in
    python) docker run --rm --name $P-c-$sdk "${env[@]}" -w /repo/tools/conformance -e PYTHONPATH=/repo/sdk/python/src "$CLIENT" python drivers/python/scenario.py "$@" ;;
    *) [ -x "$HERE/drivers/$sdk/scenario.sh" ] && "$HERE/drivers/$sdk/scenario.sh" "$P-c-$sdk" "${env[@]}" -- "$@" ;;
  esac > "$file" 2> "${file%.jsonl}.err"
}
each_sdk() { local case=$1; shift; for sdk in $SDKS; do [ "${SKIP_DONE:-}" = 1 ] && [ -s "$OUT/$case.$sdk.jsonl" ] && continue; sc "$sdk" "$OUT/$case.$sdk.jsonl" "$@"; done; }

fixture() { # name mode target-or-"" seconds-or-""
  docker run -d --name $P-$1 --network "$NET" -v "$OUT:/out" -v "$HERE/fixtures:/f" "$CLIENT" python /f/fake_server.py "$2" 8356 ${3:-} ${4:-} >/dev/null; }

case $WHAT in
v6)
  SECRET="sekret-$RUN-$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  NODE_ENV_ARGS="-e NANOCACHED_AUTH_SECRET=$SECRET" node locked; NODE_ENV_ARGS="" node open; sleep 2
  each_sdk auth_ok          $P-locked:8356 0 "$SECRET" 30 conn
  each_sdk auth_wrong       $P-locked:8356 0 "WRONG-$SECRET" 30 conn
  each_sdk auth_missing     $P-locked:8356 0 "" 30 conn
  each_sdk auth_unneeded    $P-open:8356 0 "$SECRET" 30 conn
  each_sdk refused          $P-open:9 0 "" 30 conn
  each_sdk dns_failure      no-such-host.invalid:8356 0 "" 30 conn
  each_sdk connect_timeout  203.0.113.1:8356 0 "" 90 conn
  fixture silent silent; sleep 1
  each_sdk silent_server    $P-silent:8356 0 "" 90 conn
  fixture closemid close_mid; sleep 1
  each_sdk close_mid        $P-closemid:8356 0 "" 30 conn
  # busy: a node that accepts one connection; a holder takes it, then each SDK connects
  node tiny --max-connections 1; sleep 1
  docker run -d --name $P-holder --network "$NET" "$CLIENT" python -c "import socket,time;s=socket.create_connection(('$P-tiny',8356));time.sleep(600)" >/dev/null; sleep 1
  each_sdk busy             $P-tiny:8356 0 "" 30 conn
  docker rm -f $P-holder >/dev/null
  # retryable: via the proxy with every node gone
  cluster 2 && proxy px && sleep 3 && wait_http_ok $P-px 8358
  docker rm -f $P-a $P-b >/dev/null; sleep 1
  each_sdk retryable_proxy  $P-disc:8357 1 "" 60 conn
  # secret hygiene: the distinctive secret must appear in none of the outputs
  grep -l -F -- "$SECRET" "$OUT"/*.jsonl "$OUT"/*.err 2>/dev/null > "$OUT/secret_leaks.txt"; echo "secret appears in $(wc -l < "$OUT/secret_leaks.txt") output files (secret=$SECRET)" ;;
v4)
  cluster 3 && sleep 2
  PFX="fo:$RUN:"
  sc python /dev/null $P-disc:8357 0 "" 60 seed "$PFX" 3000
  for sdk in $SDKS; do
    ( sc "$sdk" "$OUT/failover.$sdk.jsonl" $P-disc:8357 0 "" 60 gm "$PFX" 3000 40 ) &
  done
  sleep 12; docker kill $P-b >/dev/null; echo "killed b at ~12s"
  wait $(jobs -p | grep -v "^$WD$") 2>/dev/null ;;
hostile)
  # fixtures/hostile_cases.json is the one table: a port, the driver's arguments and the bounds of every case
  docker run -d --name $P-fx --network "$NET" -v "$HERE/fixtures:/f:ro" "$CLIENT" python /f/hostile_node.py /f/hostile_cases.json >/dev/null
  ready=0; for _ in $(seq 1 90); do docker logs $P-fx 2>&1 | grep -q '^hostile_node:' && { ready=1; break; }; sleep 1; done
  [ $ready = 1 ] || { echo "the hostile fixture never came up" >&2; docker logs $P-fx >&2; exit 1; }
  python3 -c 'import json,sys; [print(c["id"], c["port"], *c["args"]) for c in json.load(open(sys.argv[1]))["cases"]]' "$HERE/fixtures/hostile_cases.json" > "$OUT/cases.txt"
  while read -r id port cargs; do
    # the baseline is the memory reference the other cases are judged against
    [ -n "${ONLY:-}" ] && [ "$id" != "$ONLY" ] && [ "$id" != baseline ] && continue
    # the six SDKs at once, each in a container of its own: the case's memory is the container's cgroup peak
    for sdk in $SDKS; do sc "$sdk" "$OUT/$id.$sdk.jsonl" $P-fx:$port 0 "" "${LIMIT:-90}" host "$id" $cargs & done
    wait $(jobs -p | grep -v "^$WD$")
    echo "== $id"
  done < "$OUT/cases.txt"
  python3 "$HERE/hostile_check.py" "$OUT" ${ONLY:+"$ONLY"}; VERDICT=$? ;;
*) echo "unknown scenario set $WHAT"; exit 2 ;;
esac
kill $WD >/dev/null 2>&1
mkdir -p "$OUT/logs"; for c in $(docker ps -a --format '{{.Names}}' --filter "name=$P-"); do docker logs "$c" > "$OUT/logs/$c.log" 2>&1; done
echo "results in $OUT"
exit "${VERDICT:-0}"
