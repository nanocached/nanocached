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
# Printed, not asserted (#563 / #565 / #582): a versioned writer
# (stalewrite.py, s0..s19999) is stopped the moment the last round ends and
# reports how many keys read back older than their last acknowledged
# write ("stale"), and every node's nanocached_node_entries is sampled
# after each round into $L/entries.txt (a displaced node holding far more
# than its share is #563's leftover copies).
# Asserted as well: touchload.py runs t/y next to the load (keep-alive touches
# with a 60 s TTL every 4 s, TTL shortened then removed, expiries by touch,
# single and batched; the loader gets SIGUSR1 as each round starts and lands
# one round's share of the shorten/remove/expire turns in that round's window)
# and reports through the SDK after a 50 s settle; touchcheck.py then reads
# every touch key from every live node raw (owners, primary TTL, lingering
# copies of expired keys). PASS needs 0 lost copies, 0 lingering copies and
# every primary TTL as touched (see the two checks at the end). Replica-side
# nanocached_node_touch_misses_total (entries.txt) is expected: for a minute
# after a join an old owner forwards a touch to the joiner as the value with
# its new TTL, so the client's own leg to the joiner finds an expiring touch
# already applied. Output in $L/touch.log.
# DELETE_ORACLE=1 adds delload.py: client deletes (dx: absent everywhere
# afterwards) and deletes followed 1-6 s later by a set (dr: every owner holds
# the new value), one burst of turns per round like the touch loader, read back
# through the SDK and then raw on every live node by delcheck.py. Asserted: 0
# lingering dx copies, 0 dr keys missing or wrong through the SDK, 0 dr copies
# of another value on an owner; a dr copy missing on an owner (a tombstone
# refusing a fresh copy after a membership change) and one holding the deleted
# value (a lagging forward) are printed, not asserted: both are accepted
# behaviour in docs/architecture.html. Output in $L/del.log. Off by default
# until it has run clean a few times in a row on main.
# TTL_ORACLE=1 adds ttlload.py: sets with a TTL of 20/40 s (ts: new keys, tp:
# keys that held a no-TTL value), one burst of turns per round. Asserted: a
# key read at half its TTL answers (no early miss), and once TTL + 10 s has
# passed no node holds it (SDK read-back, then ttlcheck.py raw on every live
# node). Output in $L/ttl.log. Off by default until it has run clean a few
# times in a row on main.
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
# A reused RUN name must not keep the previous run's node logs: the end-of-run
# capture skips files that already exist (a leaver's log is saved when it leaves).
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/chaos-$RUN"; rm -rf "$L"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
fail=0
disc_up $P-disc --replication-factor 2; sleep 1
live=()   # container names of live nodes, oldest first
seq_no=0
# Sets $spawned (no command substitution: a subshell would lose live/seq_no).
spawn(){ seq_no=$((seq_no+1)); spawned=$P-n$seq_no; node_up $spawned $D --drain-timeout 20 --metrics-port 9100; live+=("$spawned"); echo "$spawned $(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $spawned)" >> "$L/ipmap.txt"; }
for _ in 1 2 3; do spawn; wait_members ${#live[@]} $D 120 || { T "FAIL: bootstrap member $spawned"; cleanup; exit 1; }; done
T "seed 30000 keys (R=2): k0..k19999 is the load's keyspace, k20000..k29999 stays untouched"
docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python verify.py seed --addr $D,$D --n 30000 --size 256 | tail -1
# nanocached_node_entries, touches_total and touch_misses_total of every live node, one line per node: "<label> <node> <entries> <touches> <touch_misses>".
entries_snapshot(){ for n in "${live[@]}"; do echo "$1 $n $(docker run --rm --network $NET $CLIENT python -c "import urllib.request,sys;b=urllib.request.urlopen('http://$n:9100/metrics',timeout=5).read().decode().splitlines();print(' '.join([l.split()[1] for l in b if l.startswith(m+' ')][0] for m in ('nanocached_node_entries','nanocached_node_touches_total','nanocached_node_touch_misses_total')))" 2>/dev/null || echo "?")"; done >> "$L/entries.txt"; }
T "start the versioned writer (4 workers over s0..s19999; stopped when the last round ends)"
docker run -d --name $P-swriter --network $NET -v "$E:/w" -w /w $CLIENT python stalewrite.py --addr $D --n 20000 --workers 4 --prefix s --size 256 --settle 15 --report-every 30 >/dev/null
for _ in $(seq 1 120); do docker logs $P-swriter 2>&1 | grep -q '"phase": "seeded"' && break; sleep 1; done
docker logs $P-swriter 2>&1 | grep -q '"phase": "seeded"' || { T "FAIL: versioned writer did not finish seeding"; fail=1; }
T "start the touch workload (touchload.py: tk/tn/tx 2000 keys each, one burst of turns per round on SIGUSR1; stopped with the writer)"
docker run -d --name $P-touch --network $NET -v "$E:/w" -w /w $CLIENT python touchload.py --addr $D --n 2000 --workers 4 --ttl 60 --pass-secs 4 --n-short 40 --n-gap 6 --bursts $ROUNDS --burst-secs 15 --short 30 --settle 50 --report-every 30 --state logs/chaos-$RUN/touchstate.json >/dev/null
for _ in $(seq 1 120); do docker logs $P-touch 2>&1 | grep -q '"phase": "seeded"' && break; sleep 1; done
docker logs $P-touch 2>&1 | grep -q '"phase": "seeded"' || { T "FAIL: touch workload did not finish seeding"; fail=1; }
if [ "${DELETE_ORACLE:-0}" = 1 ]; then
  T "start the delete workload (delload.py: dx/dr 2000 keys each, one burst of turns per round on SIGUSR1; stopped with the writer)"
  docker run -d --name $P-del --network $NET -v "$E:/w" -w /w $CLIENT python delload.py --addr $D --n 2000 --workers 4 --bursts $ROUNDS --burst-secs 15 --gap-min 1 --gap-max 6 --settle 30 --report-every 30 --state logs/chaos-$RUN/delstate.json >/dev/null
  for _ in $(seq 1 120); do docker logs $P-del 2>&1 | grep -q '"phase": "seeded"' && break; sleep 1; done
  docker logs $P-del 2>&1 | grep -q '"phase": "seeded"' || { T "FAIL: delete workload did not finish seeding"; fail=1; }
fi
if [ "${TTL_ORACLE:-0}" = 1 ]; then
  T "start the set-with-TTL workload (ttlload.py: ts/tp 2000 keys each, TTL 20/40 s, one burst of turns per round on SIGUSR1; stopped with the writer)"
  docker run -d --name $P-ttl --network $NET -v "$E:/w" -w /w $CLIENT python ttlload.py --addr $D --n 2000 --workers 4 --ttl-short 20 --ttl-long 40 --bursts $ROUNDS --burst-secs 15 --settle 30 --report-every 30 --state logs/chaos-$RUN/ttlstate.json >/dev/null
  for _ in $(seq 1 120); do docker logs $P-ttl 2>&1 | grep -q '"phase": "seeded"' && break; sleep 1; done
  docker logs $P-ttl 2>&1 | grep -q '"phase": "seeded"' || { T "FAIL: set-with-TTL workload did not finish seeding"; fail=1; }
fi
entries_snapshot seeded
T "start ${LOAD_SECS}s mixed load (8 conns, get 70%, hedge 10ms, reconnect on error)"
docker run -d --name $P-load --network $NET -v "$E:/w" -w /w $CLIENT python ${LOADER:-load.py} --addr $D --conns 8 --duration $LOAD_SECS --keyspace 20000 --value-size 256 --label chaos --reconnect-on-error --hedge 0.01 --report-every 30 >/dev/null
sleep 5
actions=(join decommission kill)
for r in $(seq 1 $ROUNDS); do
  a=${actions[$(( (r-1) % 3 ))]}
  t0=$(date +%s)
  docker kill -s USR1 $P-touch >/dev/null 2>&1   # this round's share of the touch loader's shorten/remove/expire turns
  [ "${DELETE_ORACLE:-0}" = 1 ] && docker kill -s USR1 $P-del >/dev/null 2>&1   # and of the delete loader's turns
  [ "${TTL_ORACLE:-0}" = 1 ] && docker kill -s USR1 $P-ttl >/dev/null 2>&1   # and of the set-with-TTL loader's turns
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
      docker logs -t $n > "$L/$n.log" 2>&1; docker rm $n >/dev/null
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
  entries_snapshot "round$r-$a"
done
T "stop the versioned writer and the touch workload (settle 15s / 50s, then read back every key)"
tmo 300 docker stop -t 240 $P-swriter $P-touch $( [ "${DELETE_ORACLE:-0}" = 1 ] && echo $P-del ) $( [ "${TTL_ORACLE:-0}" = 1 ] && echo $P-ttl ) >/dev/null
versioned=$(docker logs $P-swriter 2>&1 | grep '"phase": "checked"' | tail -1)
echo "=== versioned: ${versioned:-NO RESULT}"
docker logs $P-swriter > "$L/swriter.log" 2>&1
touched=$(docker logs $P-touch 2>&1 | grep '"phase": "checked"' | tail -1)
echo "=== touch (sdk): ${touched:-NO RESULT}"
docker logs $P-touch > "$L/touch.log" 2>&1
sleep 20; entries_snapshot final
T "per-owner raw check of the touch keys on every live node (touchcheck.py)"
nodes=$(printf "%s:8356," "${live[@]}"); nodes=${nodes%,}
tcheck=$(tmo 600 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python touchcheck.py --state logs/chaos-$RUN/touchstate.json --nodes $nodes | tail -1)
echo "=== touch (owners): ${tcheck:-NO RESULT}"
dsdk=; dcheck=
if [ "${DELETE_ORACLE:-0}" = 1 ]; then
  dsdk=$(docker logs $P-del 2>&1 | grep '"phase": "checked"' | tail -1)
  echo "=== delete (sdk): ${dsdk:-NO RESULT}"
  docker logs $P-del > "$L/del.log" 2>&1
  T "per-owner raw check of the delete keys on every live node (delcheck.py)"
  dcheck=$(tmo 600 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python delcheck.py --state logs/chaos-$RUN/delstate.json --nodes $nodes | tail -1)
  echo "=== delete (owners): ${dcheck:-NO RESULT}"
fi
tsdk=; tcheck2=
if [ "${TTL_ORACLE:-0}" = 1 ]; then
  tsdk=$(docker logs $P-ttl 2>&1 | grep '"phase": "checked"' | tail -1)
  echo "=== ttl (sdk): ${tsdk:-NO RESULT}"
  docker logs $P-ttl > "$L/ttl.log" 2>&1
  T "per-owner raw check of the set-with-TTL keys on every live node (ttlcheck.py)"
  tcheck2=$(tmo 600 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python ttlcheck.py --state logs/chaos-$RUN/ttlstate.json --nodes $nodes | tail -1)
  echo "=== ttl (owners): ${tcheck2:-NO RESULT}"
fi
T "waiting for the load to finish (<= $LOAD_SECS s total)"
tmo $(( LOAD_SECS + 60 )) docker wait $P-load >/dev/null || { T "FAIL: load did not finish in time"; fail=1; docker kill $P-load >/dev/null 2>&1; }
load=$(docker logs $P-load 2>/dev/null | grep '^{"label"' | tail -1)
echo "=== load: $load"
docker logs $P-load > "$L/load.log" 2>&1
T "verify the 10000 untouched keys (loss/corruption check)"
check=$(tmo 300 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python verify.py check --addr $D,$D --start 20000 --n 10000 --size 256 | tail -1)
echo "=== check: $check"
entries_snapshot end
echo "=== discovery: abandoned=$(docker logs $P-disc 2>&1 | grep -c 'join abandoned') evicted=$(docker logs $P-disc 2>&1 | grep -ci 'evict')"
warns $P-disc "${live[@]}"
for f in "$L"/"$P"-n*.log; do [ -f "$f" ] && { echo "== $(basename "$f")"; grep -i "warn\|error\|panic" "$f" | sed 's/[0-9.]*:[0-9]*/X/g' | sort | uniq -c | sort -rn | head -5; }; done
echo "$check" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { T "FAIL: untouched-range verify not clean"; fail=1; }
echo "$load" | grep -q '"corrupt": 0' || { T "FAIL: load saw corrupt reads"; fail=1; }
echo "$load" | grep -q '"miss": 0' || { T "FAIL: load saw misses"; fail=1; }
# Touch oracle: lost copies, lingering copies of expired keys, a primary TTL other than the one touched.
tbad=$(python3 -c "
import json,sys
sdk=json.loads(sys.argv[1]) if sys.argv[1].startswith('{') else {}; own=json.loads(sys.argv[2]) if sys.argv[2].startswith('{') else {}
if not sdk: print('no sdk result')
if not own: print('no owners result')
for k in ('k_miss','k_corrupt','k_touch_false','n_miss','n_corrupt','x_present','read_err'):
  if sdk.get(k): print(f'sdk {k}={sdk[k]}')
for k in ('corrupt','gat_miss','touch_false'):
  if sdk.get('anomalies',{}).get(k): print(f'sdk {k}={sdk[\"anomalies\"][k]}')
for k in ('k_no_owner','n_no_owner','k_missing_on_owner','n_missing_on_owner','k_wrong_value','n_wrong_value','k_other_reply','n_other_reply','primaries_not_one','k_ttl_missing_on_primary','k_ttl_out_of_band','n_ttl_not_removed_on_primary','x_lingering_keys','x_other_reply'):
  if own.get(k): print(f'owners {k}={own[k]}')
" "$touched" "$tcheck")
[ -z "$tbad" ] || { T "FAIL: touch oracle: $(echo $tbad | tr '\n' ' ')"; fail=1; }
if [ "${DELETE_ORACLE:-0}" = 1 ]; then
  dbad=$(python3 -c "
import json,sys
sdk=json.loads(sys.argv[1]) if sys.argv[1].startswith('{') else {}; own=json.loads(sys.argv[2]) if sys.argv[2].startswith('{') else {}
if not sdk: print('no sdk result')
if not own: print('no owners result')
for k in ('x_present','r_miss','r_corrupt','read_err'):
  if sdk.get(k): print(f'sdk {k}={sdk[k]}')
for k in ('dr_no_owner','dr_wrong_value','dr_other_reply','x_lingering_keys','x_other_reply'):
  if own.get(k): print(f'owners {k}={own[k]}')
" "$dsdk" "$dcheck")
  [ -z "$dbad" ] || { T "FAIL: delete oracle: $(echo $dbad | tr '\n' ' ')"; fail=1; }
  echo "$dsdk $dcheck" | grep -oE '"(r_stale|dr_stale_on_owner|dr_keys_with_stale_copy|dr_missing_on_owner|dr_keys_with_missing_copy)": [0-9]+' | tr '\n' ' ' | sed 's/^/=== delete accepted (printed, not asserted): /'; echo
fi
if [ "${TTL_ORACLE:-0}" = 1 ]; then
  xbad=$(python3 -c "
import json,sys
sdk=json.loads(sys.argv[1]) if sys.argv[1].startswith('{') else {}; own=json.loads(sys.argv[2]) if sys.argv[2].startswith('{') else {}
if not sdk: print('no sdk result')
if not own: print('no owners result')
for k in ('x_present','read_err'):
  if sdk.get(k): print(f'sdk {k}={sdk[k]}')
for k in ('early_miss','early_corrupt'):
  if sdk.get('anomalies',{}).get(k): print(f'sdk {k}={sdk[\"anomalies\"][k]}')
for k in ('ts_lingering_keys','tp_lingering_keys','other_reply'):
  if own.get(k): print(f'owners {k}={own[k]}')
" "$tsdk" "$tcheck2")
  [ -z "$xbad" ] || { T "FAIL: ttl oracle: $(echo $xbad | tr '\n' ' ')"; fail=1; }
fi
for c in $(docker ps -aq --filter name=$P-); do n=$(docker inspect -f "{{.Name}}" $c | tr -d /); ip=$(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" $c); echo "$n $ip" >> "$L/ipmap.txt"; [ -f "$L/$n.log" ] || docker logs -t $c > "$L/$n.log" 2>&1; done
echo "=== entries:"; cat "$L/entries.txt"
echo "=== logs in $L"; cat "$L/ipmap.txt"
cleanup
if [ $fail = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
