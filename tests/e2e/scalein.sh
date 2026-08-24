#!/bin/bash
# #124: graceful node scale-in under load. R=2, 3 nodes, mixed load;
# `docker stop -t 40` (SIGTERM + grace, like ECS stopTimeout / EKS
# terminationGracePeriodSeconds) one node -> it must decommission: hand
# its keys to their post-leave owners (U), leave membership immediately
# (V), keep forwarding during the grace window, exit 0 well inside the
# stop grace.
# PASS: leaver exit code 0 (not the SIGKILL 137), membership 3 -> 2,
# post-drain verify of all 20k keys shows 0 miss / 0 corrupt, load shows
# 0 corrupt. err_total is printed, not asserted: the moment the leaver's
# listener closes, in-flight ops on its connections fail once and the
# SDK reconnects — that blip is the "bounded dip" to eyeball, data loss
# is what must be zero.
# Images must be built from a commit with node decommission (> v0.3.0):
#   docker build --target node -t nanocached-node:dev .
#   docker build --target discovery -t nanocached-discovery:dev .
#   NODE=nanocached-node:dev DISC=nanocached-discovery:dev ./scalein.sh
export NET=e2e-scalein
E="$(cd "$(dirname "$0")" && pwd)"
source $E/cl.sh
P=nsi; D=$P-disc:8357
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
mkdir -p $E/logs
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
i=0; for n in a b c; do i=$((i+1)); node_up $P-$n $D --drain-timeout 20; wait_members $i $D 120 || { cleanup; exit 1; }; done
T "seed 30000 keys (R=2): k0..k19999 is the load's keyspace, k20000..k29999 stays untouched for the loss check"
docker run --rm --network $NET -v $E:/w -w /w e2e-client python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
T "start 60s mixed load (8 conns, get 70%)"
docker run -d --name $P-load --network $NET -v $E:/w -w /w e2e-client python load.py --addr $D --conns 8 --duration 60 --keyspace 20000 --value-size 256 --label scalein --reconnect-on-error >/dev/null
sleep 10
T "docker stop -t 40 node a (SIGTERM; drain budget 20s)"
t0=$(date +%s)
docker stop -t 40 $P-a >/dev/null
rc=$(docker inspect -f '{{.State.ExitCode}}' $P-a)
T "leaver exited rc=$rc after $(( $(date +%s)-t0 ))s (want rc=0, well under 40s)"
wait_members 2 $D 30
T "waiting for the load to finish"
docker wait $P-load >/dev/null
load=$(docker logs $P-load 2>&1 | tail -1)
echo "=== load: $load"
T "verify the 10000 untouched keys post-drain (loss/corruption check)"
check=$(docker run --rm --network $NET -v $E:/w -w /w e2e-client python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
echo "=== leaver (a)"; docker logs $P-a 2>&1 | grep -i "decommission\|shutdown\|warn\|error" | head -8
echo "=== discovery"; docker logs $P-disc 2>&1 | grep -i "left\|warn\|error" | head -5
fail=0
[ "$rc" = 0 ] || { echo "FAIL: leaver exit code $rc"; fail=1; }
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { echo "FAIL: post-drain verify not clean"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { echo "FAIL: load saw corrupt reads"; fail=1; }
# Pre-seeded keyspace + R=2 + graceful drain: reads must never miss.
echo "$load" | grep -q '"miss": 0' || { echo "FAIL: load saw misses (hit-rate dip)"; fail=1; }
cleanup
if [ $fail = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
