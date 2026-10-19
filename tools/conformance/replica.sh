#!/bin/bash
# Replica convergence: do all owners of a key hold the same copy after the writes have finished?
#
#   tools/conformance/replica.sh <R> <nodes> <direct|proxy> <seq|mix> [sdk ...]
#
# seq  each SDK in turn, on keys of its own (group `rep`); after each SDK the reader compares the owners
# mix  all SDKs at once on the same keys (group `repmix`)
# Env: NANOTEST_OPTS=ff,rr,hedge=<ms> (Python and Go only), RUN, WATCHDOG.
# The cluster is left alone until no node has logged a migration for 62 s (the forwarding window is 60 s).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"; REPO="$(cd "$HERE/../.." && pwd)"
R=${1:?R}; N=${2:?nodes}; PATHK=${3:?direct|proxy}; MODE=${4:?seq|mix}; shift 4
SDKS=${*:-python typescript go java dotnet rust}
RUN=${RUN:-$(date +%H%M%S)}
NODE=${NODE:-nanocached-node:conformance}; DISC=${DISC:-nanocached-discovery:conformance}; PROXY=${PROXY:-nanocached-proxy:conformance}
NET=conf-$RUN-rep; P=cr$RUN
OPTSTAG=""; [ -n "${NANOTEST_OPTS:-}" ] && OPTSTAG="-$(echo "$NANOTEST_OPTS" | tr ',=' '--')"
OUT=${OUT:-$HERE/results/$RUN/replica-R$R-N$N-$PATHK-$MODE$OPTSTAG}; WATCHDOG=${WATCHDOG:-3000}
mkdir -p "$OUT"
cleanup() { docker rm -f $(docker ps -aq --filter "name=$P-") >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
trap cleanup EXIT
( sleep "$WATCHDOG"; echo "WATCHDOG fired after ${WATCHDOG}s" >&2; cleanup; pkill -P $$ >/dev/null 2>&1 ) 2>/dev/null &
WD=$!
docker network create "$NET" >/dev/null

members() { docker run --rm --network "$NET" -v "$REPO/tests/e2e:/w" -w /w e2e-client python disc.py "$1" 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'; }
wait_members() { for _ in $(seq 1 120); do [ "$(members "$1")" = "$2" ] && return 0; sleep 1; done; echo "TIMEOUT waiting for $2 members" >&2; return 1; }

docker run -d --name $P-disc --network "$NET" "$DISC" --host 0.0.0.0 --port 8357 --replication-factor "$R" >/dev/null; sleep 1
NODES=""; i=0
for n in $(echo a b c d e f | cut -d' ' -f1-$N); do i=$((i+1))
  docker run -d --name $P-$n --network "$NET" "$NODE" --host 0.0.0.0 --port 8356 --discovery $P-disc:8357 >/dev/null
  wait_members $P-disc:8357 $i || exit 1; NODES="$NODES,$P-$n:8356"; done
NODES=${NODES#,}
T0=$(date +%s)
quiet=0
for _ in $(seq 1 60); do
  busy=0; for n in $(echo a b c d e f | cut -d' ' -f1-$N); do docker logs --since 62s $P-$n 2>&1 | grep -q "migration" && busy=1; done
  [ $busy = 0 ] && { quiet=1; break; }; sleep 5; done
WAITED=$(( $(date +%s) - T0 ))
ADDR=$P-disc:8357; VIA=0
if [ "$PATHK" = proxy ]; then
  docker run -d --name $P-proxy --network "$NET" "$PROXY" --host 0.0.0.0 --port 8358 --discovery $P-disc:8357 >/dev/null; sleep 4; VIA=1; fi
{ echo "run=$RUN R=$R nodes=$N path=$PATHK mode=$MODE opts=${NANOTEST_OPTS:-none} commit=$(git -C "$REPO" rev-parse --short HEAD)"; echo "quiet_before_start=$quiet waited_after_formation_s=$WAITED"; } > "$OUT/env.txt"

# run one SDK's main phase on a cases directory: drive <sdk> <dir>
drive() {
  local sdk=$1 dir=$2
  case $sdk in
    python) docker run --rm --name $P-c-$sdk-$RANDOM --network "$NET" -e NANOTEST_ADDRESSES=$ADDR -e NANOTEST_VIA_PROXY=$VIA -e NANOTEST_OPTS="${NANOTEST_OPTS:-}" -v "$REPO:/repo:ro" -v "$dir:/out" -w /repo/tools/conformance -e PYTHONPATH=/repo/sdk/python/src e2e-client python drivers/python/driver.py /out/cases.json main $PATHK cluster ;;
    *) "$HERE/drivers/$sdk/run.sh" "$NET" "$P-c-$sdk-$RANDOM" "$ADDR" "$VIA" "$dir" /out/cases.json main $PATHK cluster ;;
  esac > "$dir/$sdk.main.jsonl" 2> "$dir/$sdk.main.err"
}
read_owners() { # dir [--mix]
  docker run --rm --network "$NET" -v "$HERE:/h:ro" -v "$1:/out" e2e-client python /h/replica_read.py /out "$NODES" ${2:-} 2>&1 | tee "$1/reader.txt"; }

if [ "$MODE" = seq ]; then
  for sdk in $SDKS; do
    d=$OUT/$sdk; mkdir -p "$d"
    python3 "$HERE/make_cases.py" "$RUN-$sdk" --group rep > "$d/cases.json"; python3 "$HERE/make_cases.py" "$RUN-$sdk" --group rep --tsv > "$d/cases.tsv"
    echo "=== $sdk"; drive "$sdk" "$d"; sleep 2; read_owners "$d"
  done
else
  d=$OUT/mix; mkdir -p "$d"
  i=$OUT/init; mkdir -p "$i"
  python3 "$HERE/make_cases.py" "$RUN-mix" --group repmix_init > "$i/cases.json"; python3 "$HERE/make_cases.py" "$RUN-mix" --group repmix_init --tsv > "$i/cases.tsv"
  drive python "$i"
  python3 "$HERE/make_cases.py" "$RUN-mix" --group repmix > "$d/cases.json"; python3 "$HERE/make_cases.py" "$RUN-mix" --group repmix --tsv > "$d/cases.tsv"
  for sdk in $SDKS; do ( drive "$sdk" "$d" ) & done
  wait $(jobs -p | grep -v "^$WD$") 2>/dev/null
  # the init keys are part of the same key space: the reader needs them too
  python3 - "$d" "$i" <<'PY'
import json,sys
d,i=sys.argv[1:3]
doc=json.load(open(d+'/cases.json')); doc['cases']+= json.load(open(i+'/cases.json'))['cases']
json.dump(doc,open(d+'/cases.json','w'))
PY
  sleep 2; read_owners "$d" --mix
fi
kill $WD >/dev/null 2>&1
mkdir -p "$OUT/logs"; for c in $(docker ps -a --format '{{.Names}}' --filter "name=$P-"); do docker logs "$c" > "$OUT/logs/$c.log" 2>&1; done
echo "results in $OUT"
