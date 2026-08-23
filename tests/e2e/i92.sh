#!/bin/bash
# #92: point a real node at a hostile discovery that floods the heartbeat
# ack with a newline-less roster line. Watch the node's RSS.
MODE=${1:-flood}; SECS=${2:-40}
export NET=e2e-92
E="$(cd "$(dirname "$0")" && pwd)"
NODE=nanocached-node:0.3.0
P=n92; F=$P-disc:8357
T(){ echo "### $(date +%T) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
cleanup; docker network create $NET >/dev/null
docker run -d --name $P-disc --network $NET -v $E:/w -w /w -e MODE=$MODE e2e-client python fakedisc92.py 8357 >/dev/null
sleep 1
# tight memory cap so an unbounded heartbeat-task allocation is obvious and
# so the process actually OOM-dies rather than eating the whole host
docker run -d --name $P-node --network $NET --memory 256m --memory-swap 256m \
  $NODE --host 0.0.0.0 --port 8356 --discovery $F --max-memory 33554432 >/dev/null
T "node started (256m container cap, 32MiB cache cap), mode=$MODE"
t0=$(date +%s)
while [ $(( $(date +%s)-t0 )) -lt $SECS ]; do
  st=$(docker inspect -f '{{.State.Status}} {{.State.OOMKilled}} {{.State.ExitCode}}' $P-node 2>/dev/null)
  rss=$(docker stats --no-stream --format '{{.MemUsage}}' $P-node 2>/dev/null)
  T "t=$(( $(date +%s)-t0 ))s state=[$st] mem=$rss"
  case "$st" in
    "exited"*) T "NODE EXITED — $(docker logs $P-node 2>&1 | tail -2)"; break;;
  esac
  sleep 3
done
T "fake discovery said: $(docker logs $P-disc 2>&1 | grep flood | tail -1)"
T "node last logs:"; docker logs $P-node 2>&1 | tail -4
cleanup; T DONE
