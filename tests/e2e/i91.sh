#!/bin/bash
# #91: hedge leg registration races close(). R=2 cluster, no netem — a 1ms
# hedge interval makes legs churn constantly so a concurrently-timed close()
# lands in the window.
ITERS=${1:-400}
export NET=e2e-91
E="$(cd "$(dirname "$0")" && pwd)"
source $E/cl.sh
P=n91; D=$P-disc:8357
T(){ echo "### $(date +%T) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
i=0; for n in a b c; do i=$((i+1)); node_up $P-$n $D; wait_members $i $D 120 >/dev/null; done
# netem: add ~40ms to every node so each read stalls well past the 10ms
# hedge interval, keeping legs pending for a wide window around close().
for n in a b c; do
  docker run --rm --network container:$P-$n --cap-add NET_ADMIN alpine:3.22 \
    sh -c 'apk add --no-cache iproute2-tc >/dev/null 2>&1 && tc qdisc add dev eth0 root netem delay 40ms' >/dev/null 2>&1
done
T "cluster up (R=2, 3 nodes, +40ms netem each)"
docker run --rm --name $P-probe --network $NET -v $E:/w -w /w e2e-client python probe91.py $D $ITERS 40
T DONE; cleanup
