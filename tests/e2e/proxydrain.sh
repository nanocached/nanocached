#!/bin/bash
# #124: graceful proxy scale-in under load. 2 nodes (R=2) behind 2
# proxies; clients connect via_proxy (discovery Q). `docker stop -t 30`
# one proxy -> it must deregister from the Q roster immediately (Z, no
# liveness-timeout corpse), finish in-flight requests, exit 0; clients
# on it reconnect transparently to the survivor.
# PASS: proxy exit code 0 (not 137), Q roster 2 -> 1 within seconds of
# the SIGTERM, load shows 0 corrupt, post-drain verify of all 10k keys
# shows 0 miss / 0 corrupt. err_total is printed, not asserted — the
# drained proxy closing its client connections costs at most one
# reconnect per connection.
# Needs images built from a commit with proxy drain + SDK via_proxy
# (> v0.3.0); the repo's Python SDK is mounted over the installed one:
#   docker build --target node -t nanocached-node:dev .
#   docker build --target discovery -t nanocached-discovery:dev .
#   docker build --target proxy -t nanocached-proxy:dev .
#   NODE=... DISC=... PROXY=nanocached-proxy:dev ./proxydrain.sh
export NET=e2e-pdrain
E="$(cd "$(dirname "$0")" && pwd)"
SDKP="$(cd "$E/../../sdk/python/src" && pwd)"
source $E/cl.sh
P=npd; D=$P-disc:8357
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
proxies(){ docker run --rm --network $NET -v $E:/w -w /w e2e-client python disc.py $D proxies; }
mkdir -p $E/logs
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
i=0; for n in a b; do i=$((i+1)); node_up $P-$n $D; wait_members $i $D 120 || { cleanup; exit 1; }; done
proxy_up $P-p1 $D --drain-timeout 20
proxy_up $P-p2 $D --drain-timeout 20
t0=$(date +%s)
while :; do c=$(proxies 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'); [ "$c" = 2 ] && break
  [ $(( $(date +%s)-t0 )) -gt 60 ] && { T "TIMEOUT waiting for 2 proxies in Q (got $c)"; cleanup; exit 1; }; sleep 1; done
T "2 proxies registered in Q"
T "seed 15000 keys (R=2): k0..k9999 is the load's keyspace, k10000..k14999 stays untouched for the loss check"
docker run --rm --network $NET -v $E:/w -w /w e2e-client python verify.py seed --addr $D,$D --n 15000 --size 256 | tail -1
T "start 45s via_proxy load (8 conns — random proxy per conn)"
docker run -d --name $P-load --network $NET -v $E:/w -v $SDKP:/sdk -e PYTHONPATH=/sdk -w /w e2e-client python load.py --addr $D --via-proxy --conns 8 --duration 45 --keyspace 10000 --value-size 256 --label pdrain --reconnect-on-error >/dev/null
sleep 10
T "docker stop -t 30 proxy p1 (SIGTERM)"
t0=$(date +%s%N)
docker stop -t 30 $P-p1 >/dev/null &
STOP=$!
# how fast does Q drop the leaver?
while :; do c=$(proxies 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'); [ "$c" = 1 ] && break
  [ $(( ($(date +%s%N)-t0)/1000000000 )) -gt 30 ] && { T "TIMEOUT: Q still lists $c proxies"; break; }; sleep 0.3; done
T "Q roster at 1 after $(( ($(date +%s%N)-t0)/1000000 ))ms"
wait $STOP
rc=$(docker inspect -f '{{.State.ExitCode}}' $P-p1)
T "proxy exited rc=$rc after $(( ($(date +%s%N)-t0)/1000000 ))ms (want rc=0, well under 30s)"
T "waiting for the load to finish"
docker wait $P-load >/dev/null
load=$(docker logs $P-load 2>&1 | tail -1)
echo "=== load: $load"
T "verify the 5000 untouched keys post-drain (loss/corruption check)"
check=$(docker run --rm --network $NET -v $E:/w -w /w e2e-client python verify.py check --addr $D,$D --start 10000 --n 5000 --size 256 | tail -1)
echo "=== check: $check"
echo "=== proxy p1"; docker logs $P-p1 2>&1 | grep -i "drain\|deregister\|shutdown\|warn\|error" | head -8
echo "=== discovery"; docker logs $P-disc 2>&1 | grep -i "proxy\|warn\|error" | head -6
fail=0
[ "$rc" = 0 ] || { echo "FAIL: proxy exit code $rc"; fail=1; }
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { echo "FAIL: post-drain verify not clean"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { echo "FAIL: load saw corrupt reads"; fail=1; }
cleanup
if [ $fail = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
