#!/bin/bash
# Join/decommission/kill chaos under load. R=2, 3 nodes to start, mixed
# Python-SDK load (hedged reads on) over k0..k19999 while each round does
# one of: JOIN a new node, DECOMMISSION the oldest node (docker stop -t 40
# -> SIGTERM drain, exit code must be 0), or KILL a node (SIGKILL, wait for
# liveness eviction, start it again -> it rejoins as a fresh identity).
# Membership never drops below 3 by construction (join before the next
# decommission/kill), so with R=2 every key keeps at least one live copy
# through a kill and gets re-homed by the following join/decommission.
# PASS: every leaver exits 0, the load sees 0 corrupt and 0 miss (pre-seeded
# keyspace, no deletes), and the untouched range k20000..k29999 verifies
# with 0 miss / 0 corrupt / 0 err at the end. err_total is printed, not
# asserted (in-flight ops on a killed node's connections fail once).
#   docker build --target node -t nanocached-node:dev .
#   docker build --target discovery -t nanocached-discovery:dev .
#   NODE=nanocached-node:dev DISC=nanocached-discovery:dev ROUNDS=6 ./chaos.sh
export NET=e2e-chaos
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=nch; D=$P-disc:8357
ROUNDS=${ROUNDS:-6}
# Generous per-round budget: join migration of ~30k keys plus a drain or an
# eviction wait. The load runs for the whole schedule plus a tail.
ROUND_BUDGET=${ROUND_BUDGET:-150}
LOAD_SECS=$(( ROUNDS * ROUND_BUDGET + 30 ))
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
# Portable `timeout` (macOS has none): kill the command after N seconds.
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/chaos-$RUN"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
fail=0
disc_up $P-disc --replication-factor 2; sleep 1
live=()   # container names of live nodes, oldest first
seq_no=0
# Sets $spawned (no command substitution: a subshell would lose live/seq_no).
spawn(){ seq_no=$((seq_no+1)); spawned=$P-n$seq_no; node_up $spawned $D --drain-timeout 20; live+=("$spawned"); echo "$spawned $(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $spawned)" >> "$L/ipmap.txt"; }
for _ in 1 2 3; do spawn; wait_members ${#live[@]} $D 120 || { T "FAIL: bootstrap member $spawned"; cleanup; exit 1; }; done
T "seed 30000 keys (R=2): k0..k19999 is the load's keyspace, k20000..k29999 stays untouched"
docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
T "start ${LOAD_SECS}s mixed load (8 conns, get 70%, hedge 10ms, reconnect on error)"
docker run -d --name $P-load --network $NET -v "$E:/w" -w /w e2e-client python load.py --addr $D --conns 8 --duration $LOAD_SECS --keyspace 20000 --value-size 256 --label chaos --reconnect-on-error --hedge 0.01 --report-every 30 >/dev/null
sleep 5
actions=(join decommission kill)
for r in $(seq 1 $ROUNDS); do
  a=${actions[$(( (r-1) % 3 ))]}
  t0=$(date +%s)
  case $a in
    join)
      spawn; n=$spawned; T "round $r JOIN $n (members ${#live[@]})"
      wait_members ${#live[@]} $D $ROUND_BUDGET || { T "FAIL: join of $n did not complete"; fail=1; }
      ;;
    decommission)
      n=${live[0]}; live=("${live[@]:1}"); T "round $r DECOMMISSION $n (docker stop -t 40, members -> ${#live[@]})"
      docker stop -t 40 $n >/dev/null
      rc=$(docker inspect -f '{{.State.ExitCode}}' $n)
      T "leaver $n exited rc=$rc after $(( $(date +%s)-t0 ))s"
      [ "$rc" = 0 ] || { T "FAIL: leaver $n exit code $rc"; fail=1; }
      wait_members ${#live[@]} $D 60 || { T "FAIL: membership after decommission of $n"; fail=1; }
      docker logs $n > "$L/$n.log" 2>&1; docker rm $n >/dev/null
      ;;
    kill)
      # Kill the newest node (a join just completed, so the oldest ones hold
      # the copies the joiner was handed) and start it again: the process
      # comes back as a fresh identity, so discovery first evicts the dead
      # one (liveness) and then admits the restart as a normal join.
      n=${live[${#live[@]}-1]}; T "round $r KILL $n (SIGKILL; expect eviction to $(( ${#live[@]}-1 )) then rejoin to ${#live[@]})"
      docker kill $n >/dev/null
      wait_members $(( ${#live[@]}-1 )) $D $ROUND_BUDGET || { T "FAIL: $n was not evicted"; fail=1; }
      T "evicted after $(( $(date +%s)-t0 ))s; restarting $n"
      docker start $n >/dev/null
      wait_members ${#live[@]} $D $ROUND_BUDGET || { T "FAIL: restarted $n did not rejoin"; fail=1; }
      ;;
  esac
  T "round $r done in $(( $(date +%s)-t0 ))s; live=[${live[*]}]"
done
T "waiting for the load to finish (<= $LOAD_SECS s total)"
tmo $(( LOAD_SECS + 60 )) docker wait $P-load >/dev/null || { T "FAIL: load did not finish in time"; fail=1; docker kill $P-load >/dev/null 2>&1; }
load=$(docker logs $P-load 2>&1 | tail -1)
echo "=== load: $load"
docker logs $P-load > "$L/load.log" 2>&1
T "verify the 10000 untouched keys (loss/corruption check)"
check=$(tmo 300 docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
echo "=== discovery: abandoned=$(docker logs $P-disc 2>&1 | grep -c 'join abandoned') evicted=$(docker logs $P-disc 2>&1 | grep -ci 'evict')"
warns $P-disc "${live[@]}"
for f in "$L"/"$P"-n*.log; do [ -f "$f" ] && { echo "== $(basename "$f")"; grep -i "warn\|error\|panic" "$f" | sed 's/[0-9.]*:[0-9]*/X/g' | sort | uniq -c | sort -rn | head -5; }; done
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { T "FAIL: untouched-range verify not clean"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { T "FAIL: load saw corrupt reads"; fail=1; }
echo "$load" | grep -q '"miss": 0' || { T "FAIL: load saw misses"; fail=1; }
for c in $(docker ps -aq --filter name=$P-); do n=$(docker inspect -f "{{.Name}}" $c | tr -d /); ip=$(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $c); echo "$n $ip" >> "$L/ipmap.txt"; [ -f "$L/$n.log" ] || docker logs $c > "$L/$n.log" 2>&1; done
echo "=== logs in $L"; cat "$L/ipmap.txt"
cleanup
if [ $fail = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
