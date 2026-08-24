#!/bin/bash
E=${E:-"$(cd "$(dirname "$0")" && pwd)"}
# Env-overridable so scenarios needing newer builds can point at locally
# built images (docker build --target node -t nanocached-node:dev . etc.)
NODE=${NODE:-nanocached-node:0.3.0}; DISC=${DISC:-nanocached-discovery:0.3.0}; PROXY=${PROXY:-nanocached-proxy:0.3.0}
NET=${NET:-e2e-b}
disc_up(){ local n=$1; shift; docker run -d --name $n --network $NET ${DENV} $DISC --host 0.0.0.0 --port 8357 "$@" >/dev/null; }
node_up(){ local n=$1; local d=$2; shift 2; docker run -d --name $n --network $NET ${NENV} $NODE --host 0.0.0.0 --port 8356 --discovery $d "$@" >/dev/null; }
proxy_up(){ local n=$1; local d=$2; shift 2; docker run -d --name $n --network $NET ${PENV} $PROXY --host 0.0.0.0 --port 8358 --discovery $d "$@" >/dev/null; }
members(){ docker run --rm --network $NET -v $E:/w e2e-client python disc.py ${1:-b-disc:8357} ${2:-}; }
wait_members(){
  local want=$1 d=${2:-b-disc:8357} to=${3:-120} t0=$(date +%s)
  while :; do c=$(members $d 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'); [ "$c" = "$want" ] && { echo "members=$want after $(( $(date +%s)-t0 ))s"; return 0; }
    [ $(( $(date +%s)-t0 )) -gt $to ] && { echo "TIMEOUT waiting members=$want (got $c)"; return 1; }; sleep 1; done; }
load(){
  local l=$1; shift
  docker run --rm --name load-$l --network $NET -v $E:/w e2e-client python load.py --label $l "$@" > $E/logs/$l.json 2> $E/logs/$l.progress; }
summ(){ python3 -c "
import json,sys;d=json.load(open('$E/logs/$1.json'));print({k:d[k] for k in ('ops','ops_s','get','set','delete','miss','corrupt','stale','future_version','connects','err_total','errors')}); [print('  ',s) for s in d['samples'][:5]]"; }
warns(){ for c in "$@"; do echo "== $c"; docker logs $c 2>&1 | grep -v "accepted connection" | grep -i "warn\|error\|panic" | sed 's/[0-9.]*:[0-9]*:/X:/' | sort | uniq -c | sort -rn | head -8; done; }
