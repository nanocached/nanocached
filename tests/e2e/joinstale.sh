#!/bin/bash
# Stale-by-one after a join (versioned writer): the forwarding grace lets an
# old owner forward a client write to the joiner as an unconditional U,
# asynchronously after acking the client, so a lagging forward of an older
# version can land after a newer direct write and overwrite it; the relay
# of such a forward is then coalesced over a newer value waiting in the
# relay queue. A,B -> join C -> (KILL=1: kill C, wait for its eviction) ->
# join D; the writer is stopped TAIL seconds after D is promoted and reads
# every key back (RECHECK=N reads once more N s later). Observed: KILL=1
# stale 2..246 of 20000 in 5/5 runs, KILL=0 stale 0 in 5/5 runs (issue #589).
#   NODE=nanocached-node:dev DISC=nanocached-discovery:dev KILL=1 TAIL=3 RECHECK=45 ./joinstale.sh
JOINS=${JOINS:-2}; GAP=${GAP:-10}; KILL=${KILL:-1}; TAIL=${TAIL:-15}; WITH_LOAD=${WITH_LOAD:-1}
export NET=e2e-js; E="$(cd "$(dirname "$0")" && pwd)"; source "$E/cl.sh"
P=js; D=$P-disc:8357; T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
RUN=${RUN:-js$(date +%H%M%S)}; L="$E/logs/joinstale-$RUN"; rm -rf "$L"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
node_up $P-a $D --metrics-port 9100; wait_members 1 $D 120 >/dev/null
node_up $P-b $D --metrics-port 9100; wait_members 2 $D 120 >/dev/null
T "2 nodes up (R=2); seed load keyspace"
docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python verify.py seed --addr $D,$D --n 20000 --size 256 | tail -1 | cut -c1-100
docker run -d --name $P-swriter --network $NET -v "$E:/w" -w /w e2e-client python stalewrite.py --addr $D --n 20000 --workers 4 --prefix s --size 256 --settle 15 --report-every 10 --max-examples 20 --recheck-after ${RECHECK:-0} >/dev/null
for _ in $(seq 1 120); do docker logs $P-swriter 2>&1 | grep -q '"phase": "seeded"' && break; sleep 1; done
T "writer seeded"
if [ "$WITH_LOAD" = 1 ]; then
  docker run -d --name $P-load --network $NET -v "$E:/w" -w /w e2e-client python load.py --addr $D --conns 8 --duration $(( 10 + JOINS*GAP + TAIL + 40 + KILL*40 )) --keyspace 20000 --value-size 256 --label js --reconnect-on-error --hedge 0.01 --report-every 30 >/dev/null
fi
sleep 10
T "join C"; node_up $P-c $D --metrics-port 9100; wait_members 3 $D 120
if [ "${KILL:-0}" = 1 ]; then sleep ${KILL_AFTER:-3}; T "kill C (forwarding windows to C stay open on A/B)"; docker kill $P-c >/dev/null; wait_members 2 $D 120; fi
if [ "$JOINS" -ge 2 ]; then sleep $GAP; T "join D"; NENV="${NENV_D:-$NENV}" node_up $P-d $D --metrics-port 9100; wait_members $(( 4 - KILL )) $D 120; fi
sleep $TAIL
T "stop the writer (settle 15 s, then read back)"
tmo 300 docker stop -t 240 $P-swriter >/dev/null
versioned=$(docker logs $P-swriter 2>&1 | grep '"phase": "checked"' | tail -1)
echo "=== versioned: ${versioned:-NO RESULT}"
[ "${RECHECK:-0}" != 0 ] && { for _ in $(seq 1 $(( RECHECK + 60 ))); do docker logs $P-swriter 2>&1 | grep -q '"phase": "rechecked"' && break; sleep 1; done; echo "=== rechecked: $(docker logs $P-swriter 2>&1 | grep '"phase": "rechecked"' | tail -1)"; }
docker logs $P-swriter > "$L/swriter.log" 2>&1
[ "$WITH_LOAD" = 1 ] && { tmo 120 docker wait $P-load >/dev/null; echo "=== load: $(docker logs $P-load 2>/dev/null | grep '^{"label"' | tail -1 | cut -c1-200)"; }
for n in a b c d; do docker logs -t $P-$n > "$L/$n.log" 2>&1; done; docker logs -t $P-disc > "$L/disc.log" 2>&1; docker logs -t $P-swriter > "$L/swriter.log" 2>&1
grep -hE 'node registered|join promoted' "$L/disc.log" | cut -c1-140
grep -hE 'migration completed|re-replication|TRACE' "$L"/[abcd].log | cut -c1-200 | head -40
T "DONE (logs in $L)"; cleanup
