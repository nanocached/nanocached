#!/bin/bash
# 1-hour soak/churn long-run: ./lr.sh <image tag> [secs]. No verdict: it prints the
# loads, the .NET probe, the untouched-range check and the memory trend, and
# exits 0; it exits 1 only when a membership wait is not met.
TAG=$1; SECS=${2:-3600}; export NET=e2e-lr; docker network create $NET >/dev/null 2>&1; source cl.sh
NODE=nanocached-node:$TAG; DISC=nanocached-discovery:$TAG; export NODE DISC  # cl.sh's node_up/disc_up read them
P=lr; T(){ echo "### $(date +%T) [$P] $*"; }
D="$P-disc1:8357,$P-disc2:8357"
BG=()  # PIDs of the backgrounded stats.sh, load and .NET probe subshells; cleanup stops them (bounded), or a failed wait would leave them running
stop_bg(){
  local p k i alive all=()
  [ ${#BG[@]} -gt 0 ] || return 0
  # the subshells' children too (list them before the subshell dies: they are re-parented then)
  for p in "${BG[@]}"; do all+=("$p"); for k in $(pgrep -P "$p" 2>/dev/null); do all+=("$k"); done; done
  kill -TERM "${all[@]}" 2>/dev/null
  for i in 1 2 3 4 5; do  # at most ~5 s for them to go
    alive=0; for p in "${all[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ $alive = 0 ] && break; sleep 1
  done
  kill -KILL "${all[@]}" 2>/dev/null
  for p in "${BG[@]}"; do wait "$p" 2>/dev/null; done  # killed, so this returns at once
  BG=()
}
# the load container is named load-LR-py (not $P-...), so the prefix filter below does not catch it
cleanup(){ stop_bg; docker rm -f load-LR-py $(docker ps -aq --filter name="^/$P-") >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
trap cleanup EXIT  # Ctrl-C or a failed step must not leave the run's containers behind
for i in 1 2; do disc_up $P-disc$i --replication-factor 2; done; sleep 1
for i in 1 2 3; do node_up $P-node$i $D; wait_members $i $P-disc1:8357 300 || { T "FAIL: bootstrap node$i"; exit 1; }; done
# k0..k19999 is both loads' keyspace; k20000..k29999 stays untouched for the loss check.
docker run --rm --network $NET -v $E:/w e2e-client python verify.py seed --addr $D --n 30000 --size 512 | cut -c1-80
( bash stats.sh $E/logs/lr-$TAG-stats.csv $P- 15 ) & STATS=$!; BG+=("$STATS")
( load LR-py --addr $D --conns 8 --duration $SECS --keyspace 20000 --value-size 512 --get-ratio 0.8 --think 0.002 --report-every 60 --reconnect-on-error --hedge 0.01 ) & BG+=($!)
( docker run --rm --name $P-dotnet --network $NET probe-dotnet $P-disc1:8357 $SECS 30000 > $E/logs/lr-$TAG-dotnet.log 2>&1 ) & BG+=($!)
rounds=$(( SECS / 540 )); for (( r=1; r<=rounds; r++ )); do sleep 540; n=$P-node$(( (r-1)%3+1 )); T "round $r: kill $n"; docker kill $n >/dev/null; sleep 30; docker start $n >/dev/null; wait_members 3 $P-disc1:8357 400 || { T "FAIL: round $r: $n did not rejoin"; exit 1; }; docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' $P-disc1 $P-node1 $P-node2 $P-node3 | tr '\n' ' '; echo; done
wait $(jobs -p | grep -v $STATS) 2>/dev/null; kill $STATS 2>/dev/null
T "python load"; summ LR-py | head -1; T ".NET probe"; tail -2 $E/logs/lr-$TAG-dotnet.log
docker run --rm --network $NET -v $E:/w e2e-client python verify.py check --addr $D --start 20000 --n 10000 --size 512 | sed 's/"samples.*//' | cut -c1-120
echo "abandoned: $(docker logs $P-disc1 2>&1 | grep -c 'join abandoned')"; warns $P-disc1 $P-disc2 $P-node1 $P-node2 $P-node3
python3 memtrend.py $E/logs/lr-$TAG-stats.csv 2>/dev/null | head -12
T DONE
cleanup
