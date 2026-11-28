#!/bin/bash
# One heartbeat left unanswered on a live connection must not evict the node.
# discovery (R=2, --liveness-timeout 15, the default and minimum) + 3 nodes
# under hedged Python load, 30,000 keys seeded (k20000..k29999 never touched
# by the load). Each round blackholes the established heartbeat connection of
# one node to discovery (iptables DROP on that connection's source port, both
# directions, in a NET_ADMIN sidecar sharing the node's netns): no ack, no FIN,
# no RST, while new connections pass. The node's next heartbeat on it goes
# unanswered; it must give up on it and redial within two intervals (10 s) of
# its last answered one. The rule stays HOLD s (25), past the 20 s silence an
# unbounded ack wait plus a full interval's wait before the redial gave, then
# goes; ROUNDS (3) rounds, one node each (a, b, c, a, ...).
# PASS: the faulted node opens a new connection to discovery while the rule
# is in place (else the fault did not land), discovery logs no `node evicted`,
# no node logs the rejoin cache drop ("dropping the cache and joining
# again"), membership stays 3, the load sees 0 miss / 0 corrupt with at least
# MIN_OPS_S (10) ops per second, and the untouched range reads back clean.
# Exit 1 on any FAIL. With the ack read under a 10 s timeout followed by a
# 5 s wait before the redial, every round evicts the node at about 15 s,
# answers its announce at about 20 s with `G` and the node clears its cache.
#   NODE=nanocached-node:<tag> DISC=nanocached-discovery:<tag> CLIENT=e2e-client:<tag> ./hbloss.sh
#   knobs: ROUNDS=3 HOLD=25 SETTLE=8 RUN=<label>
export NET=e2e-hbloss
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=nhb; D=$P-disc:8357
ROUNDS=${ROUNDS:-3}; HOLD=${HOLD:-25}; SETTLE=${SETTLE:-8}
T(){ echo "### $(date -u +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter "name=^/$P-") >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
trap cleanup EXIT  # Ctrl-C or a failed step must not leave the run's containers behind
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }  # bounds a command to N seconds
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/hbloss-$RUN"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2 --liveness-timeout 15; sleep 1
nodes=(a b c); i=0
for n in "${nodes[@]}"; do i=$((i+1)); node_up $P-$n $D --drain-timeout 20; wait_members $i $D 120 || { T "FAIL: bootstrap"; exit 1; }; done
dip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $P-disc)
# One sidecar per node, in its netns, with iptables/ss installed once.
for n in "${nodes[@]}"; do
  docker run -d --name $P-fx-$n --network container:$P-$n --cap-add NET_ADMIN alpine:3.24 sleep 3600 >/dev/null
  tmo 120 docker exec $P-fx-$n apk add -q iptables iproute2 >/dev/null 2>&1 || { T "FAIL: sidecar setup for $n"; exit 1; }
done
tmo 300 docker run --rm --name $P-seed --network $NET -v "$E:/w" -w /w $CLIENT python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1; [ "${PIPESTATUS[0]}" = 0 ] || { T "FAIL: verify.py seed failed or timed out (300s)"; exit 1; }
LOAD_SECS=${LOAD_SECS:-$(( 10 + ROUNDS * (HOLD + SETTLE + 5) ))}
docker run -d --name $P-load --network $NET -v "$E:/w" -w /w $CLIENT python load.py --addr $D --conns 8 --duration "$LOAD_SECS" --keyspace 20000 --value-size 256 --label hbloss --reconnect-on-error --hedge 0.01 --report-every 5 >/dev/null
sleep 5; fail=0
# Local ports of the node's established connections to discovery.
dports(){ docker exec $P-fx-$1 ss -Htn state established "( dport = :8357 )" 2>/dev/null | awk -v d="$dip" '$4 ~ "^"d":8357$" {n=split($3,a,":"); print a[n]}'; }
for r in $(seq 1 "$ROUNDS"); do
  n=${nodes[$(( (r-1) % 3 ))]}
  ports=$(dports "$n" | tr '\n' ' ')
  [ -n "$ports" ] || { T "FAIL: round $r: $n has no established connection to discovery"; fail=1; continue; }
  ev0=$(docker logs $P-disc 2>&1 | grep -c "node evicted")
  T "round $r: blackhole $n's connection(s) to discovery from port(s) $ports for ${HOLD}s"
  for p in $ports; do
    docker exec $P-fx-$n sh -c "iptables -I OUTPUT -p tcp -d $dip --dport 8357 --sport $p -j DROP && iptables -I INPUT -p tcp -s $dip --sport 8357 --dport $p -j DROP" || { T "FAIL: iptables"; fail=1; }
  done
  t0=$(date +%s); redial=""
  while [ $(( $(date +%s)-t0 )) -lt "$HOLD" ]; do
    if [ -z "$redial" ]; then
      for q in $(dports "$n"); do case " $ports " in *" $q "*) ;; *) redial=$(( $(date +%s)-t0 ));; esac; done
    fi
    sleep 1
  done
  docker exec $P-fx-$n sh -c "iptables -F OUTPUT && iptables -F INPUT"
  ev=$(( $(docker logs $P-disc 2>&1 | grep -c "node evicted") - ev0 ))
  T "round $r: rule removed; new connection after ${redial:-never (within ${HOLD})}s; evictions this round: $ev"
  [ -n "$redial" ] || { T "FAIL: round $r: $n opened no new connection to discovery within ${HOLD}s (the fault did not take effect, or the node never redialed)"; fail=1; }
  [ "$ev" = 0 ] || { T "FAIL: round $r: discovery evicted a node: $(docker logs $P-disc 2>&1 | grep "node evicted" | tail -n "$ev" | tr '\n' ' ')"; fail=1; }
  wait_members 3 $D 90 >/dev/null || { T "FAIL: round $r: membership did not return to 3 within 90s"; fail=1; }
  sleep "$SETTLE"
done
T "waiting for the load to finish"
tmo $(( LOAD_SECS + 60 )) docker wait $P-load >/dev/null || { T "FAIL: load did not finish in time"; fail=1; docker kill $P-load >/dev/null 2>&1; }
load=$(docker logs $P-load 2>/dev/null | grep '^{"label"' | tail -1)
echo "=== load: $load"
[ -n "$load" ] || { T "FAIL: the load printed no result line"; fail=1; }
check=$(tmo 300 docker run --rm --name $P-check --network $NET -v "$E:/w" -w /w $CLIENT python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
for c in disc a b c; do docker logs -t $P-$c > "$L/$c.log" 2>&1; done
evicted=$(grep -c "node evicted" "$L/disc.log"); dropped=$(cat "$L"/[abc].log | grep -c "dropping the cache and joining again")
echo "=== discovery evictions: $evicted, node cache drops: $dropped"
grep -h "node evicted" "$L/disc.log" | head -5
[ "$evicted" = 0 ] || { T "FAIL: discovery evicted a node $evicted time(s)"; fail=1; }
[ "$dropped" = 0 ] || { T "FAIL: a node dropped its cache to rejoin $dropped time(s)"; fail=1; }
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { T "FAIL: untouched-range verify not clean (or no result within 300s)"; fail=1; }
echo "$load" | grep -q '"miss": 0' || { T "FAIL: load saw misses"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { T "FAIL: load saw corrupt reads"; fail=1; }
ops=$(load_ops "$load"); [ "${ops:-0}" -ge $(( LOAD_SECS * ${MIN_OPS_S:-10} )) ] || { T "FAIL: load completed only ${ops:-0} ops in ${LOAD_SECS}s (want >= ${MIN_OPS_S:-10}/s)"; fail=1; }
echo "=== RESULT run=$RUN evicted=$evicted dropped=$dropped fail=$fail (logs: $L)"
if [ "$fail" = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
