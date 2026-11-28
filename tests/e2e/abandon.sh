#!/bin/bash
# A join abandoned after the joiner got part of its handoff, then the same
# joiner joining again: it must not keep that attempt's copies. The next
# handoff is put-if-absent, so a kept copy wins over the owners' newer
# value, and a key deleted in between is not in the handoff at all.
# Three nodes a, b, c at R=2; 2,000 keys per class r, e, k at v1
# (reclaim970.py) and an untouched range (verify.py, k100000..k119999).
# Pause c, start joiner j and wait until a and b have completed their share
# of the handoff (c's stays pending). Then cut j's discovery connection
# (iptables DROP of port 8357 in j's netns); discovery's TCP keepalive fails
# the read after about 25 s and it abandons the join ("joining node
# disconnected"). Unpause c; through the SDK (roster a, b, c) expire every
# r key (touch 0), delete every e key, rewrite every k key to v2. Lift the
# block: j's node redials and joins again; once j is listed, read every key
# from every node raw (an owner answers G, a non-owner W).
# PASS: no owner serves an r or e key, none holds k at v1, the untouched
# range reads back 0 miss / 0 corrupt / 0 err. FAIL (exit 1): any of those,
# or a step that timed out after the abandon. INCONCLUSIVE (exit 3): the
# abandon never happened (no "join abandoned" in the discovery log), or a
# and b had not finished their share first, so the run says nothing either
# way. Reverting the fix (#2252) makes j keep its copies.
#   docker build --target node -t nanocached-node:dev .
#   docker build --target discovery -t nanocached-discovery:dev .
# Node logs and the raw check go to logs/abandon-$RUN/. About 3 min, ~2 of
# them discovery's startup grace (it refuses L for its liveness timeout).
#   NODE=nanocached-node:dev DISC=nanocached-discovery:dev [N=2000] [RUN=x] ./abandon.sh
export NET=e2e-abandon
E="$(cd "$(dirname "$0")" && pwd)"
source "$E/cl.sh"
P=abn; D=$P-disc:8357; N=${N:-2000}
NETTOOLS=${NETTOOLS:-e2e-nettools:abandon}
T(){ echo "### $(date +%T.%N | cut -c1-12) [$P] $*"; }
tmo(){ perl -e 'alarm shift; exec @ARGV' "$@"; }  # bounds a command to N seconds
cleanup(){ docker unpause $P-c >/dev/null 2>&1; docker rm -f $(docker ps -aq --filter "name=^/$P-") >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1; }
trap cleanup EXIT  # Ctrl-C or a failed step must not leave the run's containers behind
client(){ tmo 300 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python "$@"; }
# iptables in the joiner's netns (a sidecar sharing it, NET_ADMIN)
jnet(){ tmo 60 docker run --rm --net container:$P-j --cap-add NET_ADMIN $NETTOOLS sh -c "$1"; }
# "migration completed" lines in a node's log (one per join it handed off to)
done_count(){ docker logs $1 2>&1 | grep -c "migration completed"; }
ent(){ for n in "$@"; do printf "%s=%s " $n "$(docker run --rm --network $NET $CLIENT python -c "import urllib.request;b=urllib.request.urlopen('http://$P-$n:9100/metrics',timeout=5).read().decode().splitlines();print([l.split()[1] for l in b if l.startswith('nanocached_node_entries ')][0])" 2>/dev/null || echo '?')"; done; echo; }
inconclusive(){ T "INCONCLUSIVE: $*"; for c in disc a b c j; do docker logs -t $P-$c > "$L/$c.log" 2>&1; done; cleanup; exit 3; }
docker image inspect $NETTOOLS >/dev/null 2>&1 || printf 'FROM alpine:3.24\nRUN apk add --no-cache iptables iproute2\n' | docker build -q -t $NETTOOLS - >/dev/null || { T "FAIL: could not build $NETTOOLS"; exit 1; }
RUN=${RUN:-$(date +%H%M%S)}; L="$E/logs/abandon-$RUN"; mkdir -p "$L"
cleanup; docker network create $NET >/dev/null
t_start=$(date +%s); fail=0
# liveness well above c's pause (up to ~100 s), so the abandon is the
# joiner's, not c's eviction. Discovery refuses L for its first liveness
# timeout (startup grace), so the bootstrap is waited for as a whole.
disc_up $P-disc --replication-factor 2 --liveness-timeout 120; sleep 1
for n in a b c; do node_up $P-$n $D --drain-timeout 5 --metrics-port 9100; sleep 2; done
wait_members 3 $D 200 || { T "FAIL: bootstrap"; cleanup; exit 1; }
client reclaim970.py seed $D "$N" || { T "FAIL: reclaim970.py seed failed or timed out (300s)"; cleanup; exit 1; }
seed=$(client verify.py seed --addr $D,$D --start 100000 --n 20000 --size 256 | tail -1); echo "=== seed untouched: $seed"
echo "$seed" | grep -q '"err": 0' || { T "FAIL: verify.py seed not clean"; cleanup; exit 1; }
before_a=$(done_count $P-a); before_b=$(done_count $P-b)
T "pause c (its share of the handoff stays pending), start joiner j"; docker pause $P-c >/dev/null
node_up $P-j $D --drain-timeout 5 --metrics-port 9100
t0=$(date +%s)
until [ "$(done_count $P-a)" -gt "$before_a" ] && [ "$(done_count $P-b)" -gt "$before_b" ]; do
  [ $(( $(date +%s)-t0 )) -gt 60 ] && inconclusive "a and b did not complete their share to j within 60s"
  sleep 0.5; done
T "a and b completed their share after $(( $(date +%s)-t0 ))s; entries: $(ent a b j)"
docker logs $P-disc 2>&1 | grep -q "join abandoned" && inconclusive "a join was abandoned before the cut"
dip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $P-disc)
T "cut j <-> discovery ($dip:8357) until discovery abandons the join"
jnet "iptables -I OUTPUT -p tcp -d $dip --dport 8357 -j DROP && iptables -I INPUT -p tcp -s $dip --sport 8357 -j DROP" || { T "FAIL: iptables in j's netns"; cleanup; exit 1; }
t0=$(date +%s)
until docker logs $P-disc 2>&1 | grep -q "join abandoned"; do
  [ $(( $(date +%s)-t0 )) -gt 75 ] && { docker unpause $P-c >/dev/null 2>&1; inconclusive "no 'join abandoned' in the discovery log within 75s of the cut"; }
  sleep 0.5; done
T "abandoned after $(( $(date +%s)-t0 ))s: $(docker logs $P-disc 2>&1 | grep 'join abandoned' | tail -1 | cut -c1-160)"
T "unpause c; entries: $(ent a b j)"; docker unpause $P-c >/dev/null
sleep 3
T "gap: expire r*, delete e*, rewrite k* through the SDK (j blocked from discovery)"
apply=$(client reclaim970.py apply $D "$N"); echo "=== apply: ${apply:-NO RESULT}"
echo "$apply" | grep -q '"errors": {}' || { T "FAIL: the gap writes did not all succeed (an unacknowledged write proves nothing)"; fail=1; }
T "lift the block; j joins again"; jnet "iptables -F" || { T "FAIL: could not lift the block"; fail=1; }
t0=$(date +%s)
wait_members 4 $D 150 || { T "FAIL: j was not promoted"; fail=1; }
T "j listed after $(( $(date +%s)-t0 ))s; entries: $(ent a b c j)"
sleep 5
check=$(tmo 180 docker run --rm --network $NET -v "$E:/w" -w /w $CLIENT python reclaim970.py check $D "$N" $P-a,$P-b,$P-c,$P-j); echo "=== check: ${check:-NO RESULT}"
if echo "$check" | grep -q '"stale_or_resurrected_keys": [1-9]'; then T "FAIL: an owner serves a deleted or expired key, or k at v1"; fail=1
elif [ "$(echo "$check" | grep -o '"stale_or_resurrected_keys": 0' | wc -l)" -ne 3 ]; then T "FAIL: no check result (want all three classes at 0)"; fail=1; fi
# a joiner answering W or N to everything would read 0 above: it must serve its share of k at v2
jv2=$(echo "$check" | grep -oE "\"$P-j\": [0-9]+\}" | tail -1 | grep -oE '[0-9]+'); T "j serves ${jv2:-?} of $N k keys at v2"
[ "${jv2:-0}" -ge $(( N / 4 )) ] || { T "FAIL: j serves too few k keys at v2 (want >= $(( N / 4 )), about half at R=2 of 4)"; fail=1; }
untouched=$(client verify.py check --addr $D,$D --start 100000 --n 20000 --size 256 | tail -1); echo "=== untouched: ${untouched:-NO RESULT}"
echo "$untouched" | grep -q '"miss": 0, "corrupt": 0, "err": 0' || { T "FAIL: untouched range not clean"; fail=1; }
for c in disc a b c j; do docker logs -t $P-$c > "$L/$c.log" 2>&1; done; echo "$check" > "$L/check.json"
echo "=== discovery"; docker logs $P-disc 2>&1 | grep -i -E "abandon|clear what|join" | cut -c1-200 | tail -8
echo "=== j"; docker logs $P-j 2>&1 | grep -i -E "WARN|joined" | grep -v "accepted connection" | cut -c1-200 | sort | uniq -c | sort -rn | head -8
cleanup
T "wall $(( $(date +%s)-t_start ))s"
if [ $fail = 0 ]; then T "DONE (PASS)"; else T "DONE (FAIL)"; exit 1; fi
