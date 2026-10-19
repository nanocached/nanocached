#!/bin/bash
# Conformance run: every SDK driver against one topology/path, then compare.
#
#   tools/conformance/run.sh <single|cluster> <direct|proxy> [sdk ...]
#
# single   one standalone node (no discovery); only the direct path exists
# cluster  discovery + 3 nodes (R=2); direct goes through discovery
# proxy    cluster + one nanocached-proxy; the SDKs use via_proxy
#
# The clients run in containers on the same Docker network as the nodes
# (discovery hands out container IPs, which the host cannot reach on macOS).
# Images: NODE / DISC / PROXY (default :conformance, built from the checked-out tree
# with `docker build --target <node|discovery|proxy> -t nanocached-<..>:conformance .`).
# Results land in $OUT (default tools/conformance/results/<run>/<topo>-<path>).
# A watchdog removes every container of the run after WATCHDOG seconds.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
TOPO=${1:?single|cluster}; PATHK=${2:?direct|proxy}; shift 2
SDKS=${*:-python typescript go java dotnet rust}
RUN=${RUN:-$(date +%H%M%S)}
NODE=${NODE:-nanocached-node:conformance}; DISC=${DISC:-nanocached-discovery:conformance}; PROXY=${PROXY:-nanocached-proxy:conformance}
NET=conf-$RUN-$TOPO-$PATHK; P=cf$RUN
GROUP=${GROUP:-basic}
SUFFIX=""; [ "$GROUP" != basic ] && SUFFIX="-$GROUP"
OUT=${OUT:-$HERE/results/$RUN/$TOPO-$PATHK$SUFFIX}; WATCHDOG=${WATCHDOG:-900}
mkdir -p "$OUT"
case "$TOPO/$PATHK" in single/proxy) echo "no proxy path on a single node (the proxy needs discovery)"; exit 2;; esac

cleanup() { docker rm -f $(docker ps -aq --filter "name=$P-") >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
trap cleanup EXIT
( sleep "$WATCHDOG"; echo "WATCHDOG fired after ${WATCHDOG}s" >&2; cleanup; pkill -P $$ >/dev/null 2>&1 ) 2>/dev/null &
WD=$!

python3 "$HERE/make_cases.py" "$RUN" --group "$GROUP" > "$OUT/cases.json"
python3 "$HERE/make_cases.py" "$RUN" --group "$GROUP" --tsv > "$OUT/cases.tsv"
docker network create "$NET" >/dev/null

wait_members() { # <disc> <want>
  for _ in $(seq 1 90); do
    c=$(docker run --rm --network "$NET" -v "$REPO/tests/e2e:/w" -w /w e2e-client python disc.py "$1" 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p')
    [ "$c" = "$2" ] && return 0; sleep 1; done
  echo "TIMEOUT waiting for $2 members" >&2; return 1; }

if [ "$TOPO" = single ]; then
  docker run -d --name $P-node --network "$NET" "$NODE" --host 0.0.0.0 --port 8356 >/dev/null
  ADDR=$P-node:8356; sleep 1
else
  docker run -d --name $P-disc --network "$NET" "$DISC" --host 0.0.0.0 --port 8357 --replication-factor 2 >/dev/null; sleep 1
  i=0; for n in a b c; do i=$((i+1))
    docker run -d --name $P-$n --network "$NET" "$NODE" --host 0.0.0.0 --port 8356 --discovery $P-disc:8357 >/dev/null
    wait_members $P-disc:8357 $i || exit 1; done
  ADDR=$P-disc:8357
  if [ "$PATHK" = proxy ]; then
    docker run -d --name $P-proxy --network "$NET" "$PROXY" --host 0.0.0.0 --port 8358 --discovery $P-disc:8357 >/dev/null; sleep 2; fi
fi
VIA=0; [ "$PATHK" = proxy ] && VIA=1
{ echo "run=$RUN topology=$TOPO path=$PATHK commit=$(git -C "$REPO" rev-parse --short HEAD)"; docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' | grep -E "^nanocached-(node|discovery|proxy):conformance"; } > "$OUT/env.txt"

# One client container per SDK and phase; $1 = sdk, $2 = phase.
run_sdk() {
  local sdk=$1 phase=$2 img args env
  env=(-e NANOTEST_ADDRESSES=$ADDR -e NANOTEST_VIA_PROXY=$VIA -v "$REPO:/repo:ro" -v "$OUT:/out" -w /repo/tools/conformance)
  args="/out/cases.json $phase $PATHK $TOPO"
  case $sdk in
    python)     docker run --rm --name $P-c-$sdk --network "$NET" "${env[@]}" -e PYTHONPATH=/repo/sdk/python/src e2e-client python drivers/python/driver.py $args ;;
    *)          [ -x "$HERE/drivers/$sdk/run.sh" ] && "$HERE/drivers/$sdk/run.sh" "$NET" "$P-c-$sdk" "$ADDR" "$VIA" "$OUT" $args ;;
  esac > "$OUT/$sdk.$phase.jsonl" 2> "$OUT/$sdk.$phase.err"
}
PHASES="main read"; [ "$GROUP" != basic ] && PHASES="main"
for phase in $PHASES; do for sdk in $SDKS; do echo "== $sdk $phase"; run_sdk "$sdk" "$phase"; done; done

WT=""; [ "$GROUP" = basic ] && [ "$TOPO/$PATHK" = single/direct ] && WT=$P-node:8356; [ "$GROUP" = basic ] && [ "$PATHK" = proxy ] && WT=$P-proxy:8358
[ -n "$WT" ] && { echo "== wire probe $WT"; docker run --rm --network "$NET" -v "$HERE:/w" -w /w e2e-client python wire_probe.py "$WT" "$TOPO-$PATHK" > "$OUT/wire.txt" 2> "$OUT/wire.err"; }
kill $WD >/dev/null 2>&1
mkdir -p "$OUT/logs"; for c in $(docker ps -a --format "{{.Names}}" --filter "name=$P-"); do docker logs "$c" > "$OUT/logs/$c.log" 2>&1; done
python3 "$HERE/compare.py" "$OUT"
