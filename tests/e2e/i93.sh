#!/bin/bash
# #93: join abandoned AFTER this node's handoff completed -> does the node
# answer W for its own keys until the next heartbeat?
# 3 nodes (R=1) + joiner D. B and C are paused so only A completes its
# share; then D is killed so discovery abandons the join and sends X.
export NET=e2e-93
E="$(cd "$(dirname "$0")" && pwd)"
source $E/cl.sh
P=n93; D=$P-disc:8357
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker unpause $P-b $P-c >/dev/null 2>&1; docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 1 --liveness-timeout 20; sleep 1
i=0; for n in a b c; do i=$((i+1)); node_up $P-$n $D; wait_members $i $D 120; done
# enough keys that A's share is non-trivial (still completes in ~1s)
docker run --rm --network $NET -v $E:/w e2e-client python verify.py seed --addr $D,$D --n 20000 --size 256 >/dev/null
docker run --rm --network $NET -v $E:/w -w /w e2e-client python probe93.py $P-a:8356 $D seed 200
docker run -d --name $P-probe --network $NET -v $E:/w -w /w e2e-client python probe93.py $P-a:8356 $D watch 200 >/dev/null
sleep 2
T "pause b,c"; docker pause $P-b $P-c
T "start joiner d"; node_up $P-d $D
t0=$(date +%s)
while ! docker logs $P-a 2>&1 | grep -q "migration completed"; do sleep 0.2; [ $(( $(date +%s)-t0 )) -gt 90 ] && { T "TIMEOUT waiting A's handoff"; break; }; done
T "A completed: $(docker logs $P-a 2>&1 | grep 'migration completed' | tail -1)"
sleep 1
T "kill d"; docker kill $P-d >/dev/null
sleep 8
T "unpause b,c"; docker unpause $P-b $P-c
sleep 8
docker stop -t 2 $P-probe >/dev/null
echo "=== probe (A's view of its own 200 keys)"; docker logs $P-probe 2>&1
echo "=== discovery"; docker logs -t $P-disc 2>&1 | grep -i "join\|abandon\|registered" | tail -8
echo "=== node A"; docker logs -t $P-a 2>&1 | grep -iv "accepted connection" | grep -i "migration\|warn\|abandon\|adopt\|membership" | tail -8
cleanup; T DONE
