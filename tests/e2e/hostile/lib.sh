#!/bin/bash
# Shared helpers for the hostile-load memory suite (see ../README.md).
#
# Every wait here has a deadline, and every scenario is run under a
# watchdog by run.sh, so a hung container cannot hold a CI job.

H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE=${NODE:-nanocached-node:hostile}
DISC=${DISC:-nanocached-discovery:hostile}
PROXY=${PROXY:-nanocached-proxy:hostile}
# The clients are plain sockets in a stock image: no SDK to install.
CLIENT=${CLIENT:-python:3.12-alpine}

# hl_begin <scenario>: a network and a name prefix of its own.
hl_begin() {
  SCENARIO=$1
  NET=hl-$1
  P=hl-$1
  FAIL=0
  hl_cleanup
  docker network create "$NET" >/dev/null
  # run.sh's watchdog ends a stuck scenario with SIGTERM; make that run the
  # EXIT trap too, so its containers do not outlive it.
  trap 'exit 143' TERM INT
  trap hl_cleanup EXIT
}

hl_cleanup() {
  local ids
  ids=$(docker ps -aq --filter "name=^${P}-" 2>/dev/null)
  # `docker rm -f` also removes a paused container.
  [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  return 0
}

T() { echo "### $(date -u +%T) [$SCENARIO] $*"; }

disc_up() { # name memory_limit [args...]
  local name=$1 limit=$2; shift 2
  docker run -d --name "$P-$name" --network "$NET" --memory "$limit" --memory-swap "$limit" \
    "$DISC" --host 0.0.0.0 --port 8357 "$@" >/dev/null
}

node_up() { # name memory_limit [args...]
  local name=$1 limit=$2; shift 2
  docker run -d --name "$P-$name" --network "$NET" --memory "$limit" --memory-swap "$limit" \
    "$NODE" --host 0.0.0.0 --port 8356 --discovery "$P-disc:8357" "$@" >/dev/null
}

proxy_up() { # name memory_limit [args...]
  local name=$1 limit=$2; shift 2
  docker run -d --name "$P-$name" --network "$NET" --memory "$limit" --memory-swap "$limit" \
    "$PROXY" --host 0.0.0.0 --port 8358 --discovery "$P-disc:8357" "$@" >/dev/null
}
# py <script> [args...]: a client container that runs to completion.
py() {
  local script=$1; shift
  docker run --rm --network "$NET" -v "$H/..:/e2e:ro" "$CLIENT" python "/e2e/hostile/$script" "$@"
}

# py_bg <name> <script> [args...]: the same, detached.
py_bg() {
  local name=$1 script=$2; shift 2
  docker run -d --name "$P-$name" --network "$NET" -v "$H/..:/e2e:ro" "$CLIENT" \
    python "/e2e/hostile/$script" "$@" >/dev/null
}

# wait_log <container> <pattern> <seconds>
wait_log() {
  local c=$1 pattern=$2 limit=$3 t0
  t0=$(date +%s)
  while :; do
    docker logs "$c" 2>&1 | grep -q "$pattern" && return 0
    [ $(( $(date +%s) - t0 )) -ge "$limit" ] && return 1
    sleep 0.5
  done
}

# wait_count <mode: nodes|proxies> <want> <seconds>
wait_count() {
  local mode=$1 want=$2 limit=$3 t0 got
  t0=$(date +%s)
  while :; do
    got=$(py ../disc.py "$P-disc:8357" "$mode" 2>/dev/null | head -1 | sed -n 's/count=\([0-9]*\).*/\1/p')
    [ "$got" = "$want" ] && return 0
    [ $(( $(date +%s) - t0 )) -ge "$limit" ] && return 1
    sleep 1
  done
}

# peak_rss_mb <container>: the process's high-water RSS (VmHWM), which
# needs no sampling: the kernel keeps the peak.
peak_rss_mb() {
  docker exec "$1" sh -c 'grep VmHWM /proc/1/status' 2>/dev/null | awk '{printf "%d", $2 / 1024}'
}

# verdict <container> <limit_mb> <max_percent> <label>
# Passes when the container was never OOM-killed, is still running, and
# its peak RSS stayed under max_percent of its memory limit.
verdict() {
  local c=$1 limit=$2 pct=$3 label=$4 oom running peak ceiling
  oom=$(docker inspect -f '{{.State.OOMKilled}}' "$c" 2>/dev/null)
  running=$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)
  ceiling=$(( limit * pct / 100 ))
  if [ "$oom" = "true" ] || [ "$running" != "true" ]; then
    T "FAIL $label: ${c#"$P"-} OOMKilled=$oom running=$running (limit ${limit} MiB)"
    docker logs --tail 15 "$c" 2>&1 | sed 's/^/    | /'
    FAIL=1
    return
  fi
  peak=$(peak_rss_mb "$c")
  if [ -z "$peak" ] || [ "$peak" -ge "$ceiling" ]; then
    T "FAIL $label: ${c#"$P"-} peak RSS ${peak:-?} MiB >= ${ceiling} MiB (${pct}% of ${limit} MiB)"
    FAIL=1
    return
  fi
  T "ok   $label: ${c#"$P"-} peak RSS ${peak} MiB < ${ceiling} MiB (${pct}% of ${limit} MiB), not OOM-killed"
}

hl_result() {
  if [ "$FAIL" = 0 ]; then echo "=== RESULT $SCENARIO PASS"; else echo "=== RESULT $SCENARIO FAIL"; fi
  return "$FAIL"
}
