#!/bin/bash
# Deterministic reproduction of the #582 loss: a copy handed back to a node by
# a decommission, re-marked dead by a client write the node serves while the
# join that displaced it is unconfirmed, and swept under the pre-leave ring.
#
#   3 nodes seeded at R=2; an SDK client connects now (ring R0) and waits.
#   n4 joins. The moment discovery promotes it, n1 gets SIGTERM (chaos.sh's
#   spacing). WRITE_AFTER seconds after that, the R0 client writes once every
#   "target" key: owners {n1, DISPLACED} before the join and {n1, n4} after it
#   (the displaced node lost it to n4, n1 keeps it, and n1's handoff gives it
#   back to the displaced node as the entrant). Half the targets are written,
#   the other half is a handoff-only control; keys the displaced node owns
#   throughout are a second control. After n1 has left, the displaced node is
#   probed directly, then n4 (the only other holder of the targets) is killed
#   and, once it is evicted, every class is read back through the SDK.
#
# Knobs (the loss needs the displaced node's confirmation of n4's join to
# arrive after the write and before n1's leave notice, and its sweep tick to
# fire before the membership ack that drops n1):
#   DISPLACED=n2|n3   the node whose M arrives last is confirmed at once (its
#                     completion report already sees the promotion), so use n2.
#   SEED=N            more keys = a longer handoff = a wider window.
#   DELAY_MS=N        netem delay on discovery's interface (NET_ADMIN sidecar),
#                     so the heartbeat ack lands after the local sweep tick.
#   WRITE_AFTER=S     seconds after n1's SIGTERM to write (handoff in flight).
#   CLIENT_DELAY_MS=N netem delay on the writer's packets to the displaced node
#                     only, so the leaver's forward of a write lands there first.
#   NENV              extra docker run flags for nodes, e.g. -e NANOCACHED_LOG_MARKS=1
# Expected on the fix: misses on written targets that n1 had already handed
# back when the write landed; none elsewhere. Expected on main: no misses.
#   NODE=nanocached-node:<tag> DISC=nanocached-discovery:<tag> ./repro582.sh
export NET=e2e-repro
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=rp; D=$P-disc:8357
DISPLACED=${DISPLACED:-n2}; SEED=${SEED:-20000}; WRITE_AFTER=${WRITE_AFTER:-0.3}
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/repro-$RUN"; rm -rf "$L"; mkdir -p "$L"
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
cleanup(){ docker rm -f $(docker ps -aq --filter name=$P-) >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
wait_log(){ local c=$1 pat=$2 want=${3:-1} to=${4:-60} t0=$(date +%s); while :; do n=$(docker logs $c 2>&1 | grep -c -- "$pat"); [ "$n" -ge "$want" ] && return 0; [ $(( $(date +%s)-t0 )) -ge $to ] && { T "TIMEOUT waiting '$pat' x$want in $c"; return 1; }; sleep 0.05; done; }
name_of(){ local ip; ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $1); docker logs $P-disc 2>&1 | grep "node registered:" | grep " at $ip:" | tail -1 | sed 's/.*node registered: \([^ ]*\) at.*/\1/'; }
client(){ docker run --rm --network $NET -v "$E:/w" -w /w e2e-client python "$@"; }
save_logs(){ for c in disc n1 n2 n3 n4 writer; do docker logs -t $P-$c > "$L/$c.log" 2>&1; done; }
die(){ T "FAIL: $*"; save_logs; cleanup; exit 1; }
ts(){ docker logs -t $P-$1 2>&1 | grep -- "$2" | head -1 | cut -c12-23; }

cleanup; docker network create $NET >/dev/null
disc_up $P-disc --replication-factor 2; sleep 1
if [ -n "$DELAY_MS" ]; then
  docker run --rm --cap-add NET_ADMIN --network container:$P-disc alpine sh -c "apk add -q iproute2 >/dev/null 2>&1; tc qdisc add dev eth0 root netem delay ${DELAY_MS}ms" || die "netem"
  T "discovery egress delayed by ${DELAY_MS}ms"
fi
for i in 1 2 3; do node_up $P-n$i $D --drain-timeout 20; wait_members $i $D 60 >/dev/null || die "bootstrap n$i"; done
N1=$(name_of $P-n1); N2=$(name_of $P-n2); N3=$(name_of $P-n3)
case $DISPLACED in n2) ND=$N2; NO=$N3;; n3) ND=$N3; NO=$N2;; *) die "DISPLACED must be n2 or n3";; esac
T "seed $SEED keys (R=2); leaver n1, displaced $DISPLACED"; client verify.py seed --addr $D,$D --n $SEED --size 256 | tail -1
# Give n1 alone version 2 of every key it shares with the displaced node, so a
# key's version on the displaced node tells whether n1's handoff has delivered it.
client pick_keys.py $N1 $ND $NO - $SEED | cut -d' ' -f2- | tr ' ' '\n' | grep . > "$L/pre.txt"
T "write version 2 of $(wc -l < "$L/pre.txt" | tr -d ' ') keys directly to n1 only"
client write_once.py --addr $D --keys-file /w/logs/repro-$RUN/pre.txt --go-file /dev/null --direct $P-n1:8356 --version 2 | tail -1
sleep 2
T "start the R0 writer client (connects now, writes on go)"
docker run -d --name $P-writer --network $NET -v "$E:/w" -w /w e2e-client python write_once.py --addr $D --keys-file /w/logs/repro-$RUN/keys.txt --go-file /w/logs/repro-$RUN/go --timeout 180 --poll-node $P-$DISPLACED:8356 --poll-version 2 --version 3 --deadline ${DEADLINE:-8} --written-file /w/logs/repro-$RUN/written.txt >/dev/null
wait_log $P-writer '"phase": "connected"' 1 30 || die "writer did not connect"
if [ -n "$CLIENT_DELAY_MS" ]; then
  # Delay only the writer's packets to the displaced node, so the leaver's
  # forward of the same write (HandoffSet, which unmarks) reaches the
  # displaced node before the client's own replica leg (which the fix marks).
  dip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $P-$DISPLACED)
  docker run --rm --cap-add NET_ADMIN --network container:$P-writer alpine sh -c "apk add -q iproute2 >/dev/null 2>&1; tc qdisc add dev eth0 root handle 1: prio && tc qdisc add dev eth0 parent 1:3 handle 30: netem delay ${CLIENT_DELAY_MS}ms && tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst $dip flowid 1:3" || die "client netem"
  T "writer's packets to $DISPLACED ($dip) delayed by ${CLIENT_DELAY_MS}ms"
fi
T "join n4"
node_up $P-n4 $D --drain-timeout 20
wait_log $P-disc "node registered:" 4 30 || die "n4 did not register"
N4=$(name_of $P-n4)
client pick_keys.py $N1 $ND $NO $N4 $SEED > "$L/classes.txt"
grep '^target ' "$L/classes.txt" | cut -d' ' -f2- | tr ' ' '\n' | grep . > "$L/target-all.txt"
grep '^control ' "$L/classes.txt" | cut -d' ' -f2- | tr ' ' '\n' | grep . > "$L/control.txt"
awk 'NR%2==1' "$L/target-all.txt" > "$L/keys.txt"; awk 'NR%2==0' "$L/target-all.txt" > "$L/target-unwritten.txt"
T "classes: target=$(wc -l < "$L/target-all.txt" | tr -d ' ') (written $(wc -l < "$L/keys.txt" | tr -d ' '), unwritten $(wc -l < "$L/target-unwritten.txt" | tr -d ' ')) control=$(wc -l < "$L/control.txt" | tr -d ' ')"
wait_log $P-disc "join promoted: $N4" 1 90 || die "n4 not promoted"
T "n4 promoted; SIGTERM n1; write in ${WRITE_AFTER}s"
( docker stop -t 40 $P-n1 >/dev/null ) & stop_pid=$!
sleep "$WRITE_AFTER"
touch "$L/go"
wait_log $P-writer '"phase": "written"' 1 60 || die "writer did not write"
T "writer: $(docker logs $P-writer 2>&1 | grep '"written"' | cut -c1-230)"
[ -s "$L/written.txt" ] || die "nothing was written (handoff never seen on $DISPLACED before the deadline)"
wait_log $P-n1 "decommission: handed off" 1 60 || die "n1 did not hand off"
wait_log $P-$DISPLACED "join of $N4 confirmed" 1 30 || die "$DISPLACED never confirmed n4's join"
T "times (UTC): n1 SIGTERM $(ts n1 'shutdown signal') | write $(ts writer '"written"') | n1 handed off $(ts n1 'handed off') | $DISPLACED confirmed $(ts $DISPLACED "join of $N4 confirmed")"
wait $stop_pid; T "n1 exited rc=$(docker inspect -f '{{.State.ExitCode}}' $P-n1)"
wait_members 3 $D 60 >/dev/null || die "membership after n1 left"
sleep 8
docker logs $P-$DISPLACED 2>&1 | grep "mark-on-write" | awk '{print $3}' | grep -x -F -f "$L/written.txt" | sort -u > "$L/marked.txt"
T "$DISPLACED mark-on-write lines: $(docker logs $P-$DISPLACED 2>&1 | grep -c "mark-on-write") (written keys among them, i.e. written before its confirmation: $(wc -l < "$L/marked.txt" | tr -d ' '))"
T "$DISPLACED's direct answer per class (n1 gone: it owns them again): written / marked / unwritten target / control"
for f in written marked target-unwritten control; do [ -s "$L/$f.txt" ] || { echo "$f: (empty)"; continue; }; printf "%-18s " $f; client probe_nodes.py --nodes "$P-$DISPLACED=$P-$DISPLACED:8356" --keys "$(tr '\n' ',' < "$L/$f.txt" | sed 's/,$//')" --values | awk '{print $2}' | sed 's/(.*//' | sort | uniq -c | tr '\n' ' ' | tee "$L/probe-$f.txt"; echo; done
T "kill n4"
docker kill $P-n4 >/dev/null
wait_members 2 $D 60 >/dev/null || die "n4 not evicted"
sleep 2
for f in written marked target-unwritten control; do [ -s "$L/$f.txt" ] || continue; T "read $f through the SDK"; client read_keys.py --addr $D --keys-file /w/logs/repro-$RUN/$f.txt | tee "$L/read-$f.json"; done
save_logs
echo "=== $DISPLACED: confirmation / sweep / membership order"; grep -E "sweep call|confirmed by|membership updated|migration completed" "$L/$DISPLACED.log" | cut -c12-23,32-120
cleanup
T "DONE (logs in $L)"
