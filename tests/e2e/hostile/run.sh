#!/bin/bash
# Runs the hostile-load memory scenarios, each under a watchdog, and prints
# a table. Exit status is 1 if any scenario failed.
#
#   NODE=... DISC=... PROXY=... ./run.sh [scenario ...]
#
# Needs Docker and the three images (`docker build --target node|discovery|
# proxy -t nanocached-<target>:hostile .`); NODE, DISC and PROXY override the
# image names. SCENARIO_TIMEOUT (seconds, default 600) bounds each scenario.
set -u
H="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(proxy_inflight_bytes node_stalled_joiner discovery_blocked_q)
LIMIT=${SCENARIO_TIMEOUT:-600}
LOGS=${HOSTILE_LOGS:-$H/../logs/hostile}
mkdir -p "$LOGS"

# run_bounded <seconds> <log> <command...>: exit status of the command, or
# 124 if the watchdog had to stop it. The scenario traps SIGTERM, so its own
# cleanup runs before it exits.
run_bounded() {
  local secs=$1 log=$2 pid watchdog status marker
  shift 2
  marker=$(mktemp)
  rm -f "$marker"
  "$@" > "$log" 2>&1 &
  pid=$!
  # The watchdog must not hold this script's stdout (a pipe reader would wait
  # for its `sleep`), and it sleeps in one-second steps so that stopping it
  # leaves nothing running for long.
  (
    exec >/dev/null 2>&1
    for ((elapsed = 0; elapsed < secs; elapsed++)); do sleep 1; done
    : > "$marker"
    kill -TERM "$pid"
    sleep 10
    kill -KILL "$pid"
  ) &
  watchdog=$!
  wait "$pid"
  status=$?
  kill "$watchdog" 2>/dev/null
  wait "$watchdog" 2>/dev/null
  if [ -e "$marker" ]; then rm -f "$marker"; status=124; fi
  return $status
}

failed=0
summary=()
for name in "${SCENARIOS[@]}"; do
  log="$LOGS/$name.log"
  started=$(date +%s)
  run_bounded "$LIMIT" "$log" bash "$H/$name.sh"
  status=$?
  secs=$(( $(date +%s) - started ))
  case $status in
    0) outcome=PASS ;;
    124) outcome="FAIL (watchdog: over ${LIMIT}s)"; failed=1 ;;
    *) outcome=FAIL; failed=1 ;;
  esac
  [ "$status" -ne 0 ] && { echo "----- $name"; cat "$log"; }
  summary+=("$(printf '%-24s %-30s %4ss' "$name" "$outcome" "$secs")")
  # The verdict lines (peak RSS against its ceiling) go in the table too: a
  # passing run's log is not printed, and the margin is what shows a bound
  # wearing thin before it fails.
  while IFS= read -r line; do
    summary+=("    ${line#*] }")
  done < <(grep -E '\] (ok  |FAIL) .*(peak RSS|OOMKilled=)' "$log")
done

# A scenario that died hard may have left containers behind.
leftovers=$(docker ps -aq --filter "name=^hl-")
# shellcheck disable=SC2086
[ -n "$leftovers" ] && docker rm -f $leftovers >/dev/null 2>&1

echo
echo "hostile-load memory suite"
printf '%s\n' "${summary[@]}"
exit $failed
