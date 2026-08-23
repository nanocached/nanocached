#!/bin/bash
TAG=$1; SECS=${2:-3600}; export NET=e2e-lr; docker network create $NET >/dev/null 2>&1; source cl.sh
NODE=nanocached-node:$TAG; DISC=nanocached-discovery:$TAG; P=lr; T(){ echo "### $(date +%T) [$P] $*"; }
D="$P-disc1:8357,$P-disc2:8357"
for i in 1 2; do disc_up $P-disc$i --replication-factor 2; done; sleep 1
for i in 1 2 3; do node_up $P-node$i $D; wait_members $i $P-disc1:8357 300; done
docker run --rm --network $NET -v $E:/w e2e-client python verify.py seed --addr $D --n 30000 --size 512 | cut -c1-80
( bash stats.sh $E/logs/lr-$TAG-stats.csv $P- 15 ) & STATS=$!
( load LR-py --addr $D --conns 8 --duration $SECS --keyspace 30000 --value-size 512 --get-ratio 0.8 --think 0.002 --report-every 60 --reconnect-on-error --hedge 0.01 ) &
( docker run --rm --name $P-dotnet --network $NET probe-dotnet $P-disc1:8357 $SECS 30000 > $E/logs/lr-$TAG-dotnet.log 2>&1 ) &
rounds=$(( SECS / 540 )); for r in $(seq 1 $rounds); do sleep 540; n=$P-node$(( (r-1)%3+1 )); T "round $r: kill $n"; docker kill $n >/dev/null; sleep 30; docker start $n >/dev/null; wait_members 3 $P-disc1:8357 400; docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' $P-disc1 $P-node1 $P-node2 $P-node3 | tr '\n' ' '; echo; done
wait $(jobs -p | grep -v $STATS) 2>/dev/null; kill $STATS 2>/dev/null
T "python load"; summ LR-py | head -1; T ".NET probe"; tail -2 $E/logs/lr-$TAG-dotnet.log
docker run --rm --network $NET -v $E:/w e2e-client python verify.py check --addr $D --n 30000 --size 512 | sed 's/"samples.*//' | cut -c1-120
echo "abandoned: $(docker logs $P-disc1 2>&1 | grep -c 'join abandoned')"; warns $P-disc1 $P-disc2 $P-node1 $P-node2 $P-node3
python3 memtrend.py $E/logs/lr-$TAG-stats.csv 2>/dev/null | head -12
T DONE
