#!/bin/bash
# Via-proxy load while a node is killed, evicted and restarted on the same IP.
#   NODE=... DISC=... PROXY=... RUN=x ./proxykill.sh
export NET=e2e-pkill
E="$(cd "$(dirname "$0")" && pwd)"
SDKP="$(cd "$E/../../sdk/python/src" && pwd)"
source "$E/cl.sh"
P=npk; D=$P-disc:8357
T(){ echo "### $(date -u +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
proxies(){ docker run --rm --network $NET -v $E:/w -w /w e2e-client python disc.py $D proxies; }
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/pkill-$RUN"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
i=0; for n in a b c; do i=$((i+1)); node_up $P-$n $D --drain-timeout 20; wait_members $i $D 120 || { cleanup; exit 1; }; done
proxy_up $P-p1 $D --drain-timeout 20; proxy_up $P-p2 $D --drain-timeout 20
t0=$(date +%s); while :; do c=$(proxies 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p'); [ "$c" = 2 ] && break; [ $(( $(date +%s)-t0 )) -gt 60 ] && { T "TIMEOUT proxies"; cleanup; exit 1; }; sleep 1; done
docker run --rm --network $NET -v $E:/w -w /w e2e-client python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
LOAD_SECS=${LOAD_SECS:-90}
docker run -d --name $P-load --network $NET -v $E:/w -v $SDKP:/sdk -e PYTHONPATH=/sdk -w /w e2e-client python load.py --addr $D --via-proxy --conns 8 --duration $LOAD_SECS --keyspace 20000 --value-size 256 --label pkill --reconnect-on-error --report-every 5 >/dev/null
sleep 10; t0=$(date +%s); fail=0
T "KILL $P-c"; docker kill $P-c >/dev/null
wait_members 2 $D 60 || { T "FAIL: eviction"; fail=1; }
T "evicted after $(( $(date +%s)-t0 ))s; restarting $P-c"; docker start $P-c >/dev/null
wait_members 3 $D 60 || { T "FAIL: rejoin"; fail=1; }; T "rejoined after $(( $(date +%s)-t0 ))s"
docker wait $P-load >/dev/null
docker logs $P-load > "$L/load.log" 2>&1; load=$(tail -1 "$L/load.log"); echo "=== load: $load"
check=$(docker run --rm --network $NET -v $E:/w -w /w e2e-client python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
for c in $(docker ps -aq --filter name=$P-); do n=$(docker inspect -f "{{.Name}}" $c | tr -d /); echo "$n $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $c)" >> "$L/ipmap.txt"; docker logs -t $c > "$L/$n.log" 2>&1; done
echo "=== WARN/ERROR categories (nodes, proxies, discovery)"; cat "$L"/$P-[abcpd]*.log | grep -v "^.*load" | grep -i "warn\|error\|panic" | sed -E 's/^[^ ]+Z //; s/[0-9a-f]{8}-[0-9a-f-]{27}/UUID/g; s/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+/ADDR/g; s/attempt [0-9]+\/[0-9]+/attempt X/g; s/after [0-9]+ attempt/after N attempt/g; s/[0-9]+ key/N key/g; s/[0-9]+ (entries|dead)/N \1/g' | sort | uniq -c | sort -rn | head -12
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { T "FAIL: untouched-range verify not clean"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { T "FAIL: corrupt"; fail=1; }
echo "$load" | grep -q '"miss": 0' || { T "FAIL: load saw misses"; fail=1; }
cleanup
echo "=== RESULT run=$RUN fail=$fail"
