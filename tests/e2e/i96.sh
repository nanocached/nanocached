#!/bin/bash
# #96: address-churn scenario. Each round brings up a node under a NEW name
# (= new address), lets the client connect to it, kills it so the client's
# redial fails (cooldown entry), waits for eviction, removes the container.
# Same as lr.sh's kill/restart rounds except the address is never reused.
ROUNDS=${1:-20}; export NET=e2e-96
E="$(cd "$(dirname "$0")" && pwd)"
source $E/cl.sh
P=c96; D=$P-disc:8357
T(){ echo "### $(date +%T) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 1 --liveness-timeout 6; sleep 1
node_up $P-node0 $D; wait_members 1 $D 60
docker run -d --name $P-probe ${PROBE_ENV} --network $NET -v $E:/w -w /w e2e-client python churn96.py $D 5 >/dev/null
sleep 6; T "probe started"; docker logs $P-probe 2>&1 | tail -1
for r in $(seq 1 $ROUNDS); do
  n=$P-n$r
  # distinct port per round: docker recycles the IP of a removed container
  # within seconds, so IP alone would make every round's address identical
  docker run -d --name $n --network $NET $NODE --host 0.0.0.0 --port $((8400+r)) --discovery $D >/dev/null
  wait_members 2 $D 120 >/dev/null
  sleep 10                                   # let the client refresh + dial the new node
  docker kill $n >/dev/null; sleep 5         # client redials the dead address -> cooldown entry
  wait_members 1 $D 60 >/dev/null            # eviction
  docker rm $n >/dev/null
  sleep 3
  T "round $r: $(docker logs $P-probe 2>&1 | tail -1 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("members=%d cooldowns=%d rss=%dkB errs=%d"%(d["members"],d["cooldowns"],d["rss_kb"],d["errs"]))')"
done
sleep 35; T "after final refresh window"; docker logs $P-probe 2>&1 | tail -1
docker stop -t 10 $P-probe >/dev/null; docker logs $P-probe > $E/logs/i96.log 2>&1
T "final"; tail -1 $E/logs/i96.log
T "first report"; sed -n 1p $E/logs/i96.log
cleanup; T DONE
