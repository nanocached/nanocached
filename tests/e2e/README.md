# E2E scenarios

Docker-based end-to-end scenarios used to reproduce and regression-check
cluster-level behaviour (data loss on join/eviction, slow-node tail latency,
SDK shutdown/leak behaviour, hostile-peer resource exhaustion).

**These are not run in CI** — except the hostile-load memory suite
(`hostile/`, below), which has its own nightly workflow. Several take minutes
to an hour and need Docker, multiple containers, `NET_ADMIN` (netem), and
published images. CI only *lints* the scripts (shell syntax + `shellcheck`,
Python `py_compile`) so they stay runnable; it never executes them. Run them
by hand when investigating or verifying a fix.

## Prerequisites

- Docker.
- Images: `nanocached-node:<tag>` and `nanocached-discovery:<tag>` (build from
  this repo, or pull `ghcr.io/nanocached/...`), and an `e2e-client` image with
  the Python SDK installed (`pip install nanocached==<ver>`). A few scenarios
  also build `probe-dotnet` / `alpine` helpers on the fly.
- Scripts resolve their own directory via `$0`, so they run from anywhere.

## Scenarios

Each `i*.sh` is one scenario; the paired `*.py` is its probe/loader.

| script | issue | what it shows |
|---|---|---|
| `i96.sh` + `churn96.py` | #96 | address-churn: reconnect-cooldown map never purges departed addresses, and Python re-raises the stored exception so its traceback grows on every cooldown hit (white-box + tracemalloc). Needs a *new port* per round — Docker reuses a removed container's IP immediately. |
| `i93.sh` + `probe93.py` | #93 | a join abandoned after a node's local handoff completes leaves `known_ring` stale, so the node answers `W` for its own keys until the next heartbeat (window ≤ 5 s). |
| `i92.sh` + `fakedisc92.py` | #92 | a hostile discovery flooding the heartbeat ack with a newline-less roster line OOM-kills a node in seconds (`read_heartbeat_ack`'s unbounded `read_until`), bypassing `--max-memory`. `honest` mode is the control. |
| `i91.sh` + `probe91.py` | #91 | hedge-leg-vs-`close()` race. Did **not** reproduce on the Python SDK (80/80 closes raced active hedged reads, zero leaks); kept as a regression guard and a template for the other SDKs. |
| `lr.sh` | — | 1-hour soak/churn long-run (kill/restart rounds, dual Python+.NET load, memory-trend analysis). |
| `scalein.sh` | #124 | graceful node scale-in under load: R=2, 3 nodes, SIGTERM one mid-load → decommission (handoff, immediate leave, forwarding window), exit 0. Asserts zero errors/misses/corruption in the load and zero loss on an untouched key range. Needs `:dev` images built from a post-v0.3.0 commit (see the script header). |
| `proxydrain.sh` | #124 | graceful proxy scale-in under via-proxy load: 2 proxies, SIGTERM one → immediate `Q` deregistration, in-flight finish, exit 0, clients reconnect to the survivor. Same zero-error/zero-loss assertions. Mounts the repo's Python SDK for `via_proxy`. |
| `chaos.sh` | #266 | join → decommission → kill/restart rounds under hedged Python load (R=2, 3 nodes). Asserts leaver exit 0, load 0 miss / 0 corrupt, and 0 loss on an untouched key range. Must **PASS**: the loss it was written to expose (an eviction never re-replicating, issue #266) was fixed in #270 — it has passed since, with 0 miss / 0 corrupt / 0 err on the untouched range — and the two bugs it exposed on the way (#264/#267) are fixed too. A failure here is a real regression, never a known loss. Also runs `touchload.py` next to the load and asserts its oracle (below); `DELETE_ORACLE=1` adds the delete oracle. `ROUNDS=6 NODE=... DISC=... ./chaos.sh` |
| `reclaim970.sh` + `reclaim970.py` | #970 | a key deleted or expired by a touch while a former owner holds the dead copy a join took from it came back when a leave made that node an owner again before the mark sweep had removed the copy (and the same dead copy was handed on by that node's re-replication, decommission drain or join transfer). 3 nodes, join `d`, `GAP` s later expire / delete / rewrite 600 keys each through the SDK, decommission `a` (`KILL=1`: kill it) after `DELAY` s, read every key from every node raw 10 s and 140 s after the leave. Must be 0 in every class; before the fix 99–106 of 600 expired and deleted keys were back. `NODE=... DISC=... [GAP=0] [DELAY=0] [KILL=0] ./reclaim970.sh` |
| `touchload.py` + `touchcheck.py` | — | the touch workload (`t` / `y`: touch, get-and-touch, batched forms, 0 / short / removed TTLs) with an oracle, run by `chaos.sh` so every form crosses join, decommission and kill-restart windows (the scenario sends the loader `SIGUSR1` as each round starts; one round's share of the expiring and TTL-removing turns then lands in that window). Three key classes, every value the key's index in decimal: keep-alive keys touched every pass with the same TTL (a touch answering `False` or a get-and-touch missing is a lost copy); keys shortened to a TTL and then touched to no TTL (a copy that missed the removal expires and is missing at the end); keys expired by a touch (a copy that missed it lingers). `touchload.py` reports through the SDK after a settle (anomalies are also printed as they happen, timestamped, to line up with the round log); `touchcheck.py` then reads every key from every live node raw: owners (`G`, a non-owner answers `W`), the primary's remaining TTL (a delta-0 `i`, which never changes it), lingering copies of expired keys. Both are also usable standalone against any cluster. |
| `delload.py` + `delcheck.py` | — | the delete workload with an oracle, run by `chaos.sh` when `DELETE_ORACLE=1` (off by default until it has run clean a few times in a row on main). Two key classes: keys deleted by a client `D` (no node may answer them afterwards: a copy that comes back is one a transfer, a pass, a relay or a window forward carried from before the delete) and keys deleted and then set again 1-6 s later (every key must read back the new value through the SDK and no owner may hold another value; an owner without the key, refused a fresh copy by the tombstone the delete left after a membership change, and an owner holding the deleted value, the lagging forward, are printed, not asserted, as accepted behaviour). Turns come in bursts on `SIGUSR1` like the touch loader's. `delload.py` reports through the SDK, `delcheck.py` reads every key from every live node raw. Both are also usable standalone. |
| `ttlload.py` + `ttlcheck.py` | — | the set-with-TTL workload with an oracle, run by `chaos.sh` when `TTL_ORACLE=1` (off by default until it has run clean a few times in a row on main). Two key classes: new keys set with a TTL of 20 s or 40 s, and keys seeded without a TTL and then set again with one. Every key is read back at half its TTL (a miss is a copy lost or expired early; a read that would only start after the earliest expiry, because the worker was stuck retrying against a killed node, is skipped and counted) and, once the last acknowledgement plus the TTL plus a margin has passed, no node may answer it with a value (a copy that is still there kept a longer TTL than the one set, or is the older no-TTL value back). Turns come in bursts on `SIGUSR1` like the touch loader's. `ttlload.py` reports through the SDK, `ttlcheck.py` reads every key from every live node raw. Both are also usable standalone. |
| `casload.py` + `cascheck.py` | — | the compare-and-set workload with an oracle, run by `chaos.sh` when `CAS_ORACLE=1` (off by default until it has run clean a few times in a row on main). Two clients, each with its own ring view, race on every key. Chain keys: both append a unique tag with `get_with_token` and `replace` until the replace answers true; asserted are no tag twice, no tag nobody tried and no key vanishing. Race keys: both `put_if_absent` a different value; at most one may win, the value left must be the winner's, and a key that was written or found present must not disappear. Acknowledged tags missing from the final value, or from one owner, are printed, not asserted: a lagging forward in a join's or a kill's rejoin window rewinds the key and the next replace succeeds on the older value, accepted behaviour. `casload.py` reports through the SDK (and writes every operation with `--oplog`), `cascheck.py` reads every key from every live node raw. Turns come in bursts on `SIGUSR1` like the touch loader's. Both are also usable standalone. |
| `freshjoin.sh` | #571, #573 | a node that joined seconds earlier is decommissioned or killed under load (`MODE=decom-fresh` / `kill-fresh`, with `GAP` seconds after promotion), with an earlier joiner still lingering (`decom-lingering` / `kill-lingering`), or a join lands while the oldest node drains (`decom-join`); `*-rejoin` adds one more join afterwards. Prints the nodes' entry counts and the WARN/ERROR categories. Found #571 (`decom-fresh GAP=1`: a joiner that leaves within one heartbeat interval was never seen by the senders) and was the regression set for #572/#573. `NODE=... DISC=... MODE=decom-fresh GAP=1 ./freshjoin.sh` |
| `proxykill.sh` | #570 | via-proxy load (2 proxies, 3 nodes, R=2) while a node is killed, evicted and started again on the same IP. Asserts 0 miss / 0 corrupt and 0 loss on the untouched range; the `RetryableError`s in the load are the proxy's `R` for a dead primary until eviction (#125). Needs `PROXY=...` as well. |
| `restart.sh` + `load_diag.py` + `analyze_restart.py` | #570 | kill a node and start it again on the same IP the moment it is evicted, under a loader that prints the client's ring and routed owners on every miss; `analyze_restart.py <logdir>` classifies the misses against the node and discovery logs. Must be 0 misses: before #570 a client whose bootstrap dial to the dead member was still pending connected to the restarted process under the dead name and read `N` from it before promotion (150-870 per run). `NEWIP=1` blocks the old IP, `DELAY=n` delays the restart. `PROBE_MAX=0 NODE=... DISC=... ./restart.sh` |

## Hostile-load memory suite (`hostile/`)

The real node, proxy and discovery binaries in containers with a hard memory
limit, driven by inputs that once OOM-killed them. Each scenario passes when
the process is **never OOM-killed and its peak RSS (`VmHWM`) stays under a
stated fraction of its limit**; every wait has a deadline and `run.sh` puts
each scenario under a watchdog. The clients are plain-socket Python scripts
in a stock `python:3.12-alpine` container, so nothing but Docker and the
three images is needed:

```
docker build --target node      -t nanocached-node:hostile .
docker build --target discovery -t nanocached-discovery:hostile .
docker build --target proxy     -t nanocached-proxy:hostile .
tests/e2e/hostile/run.sh [scenario ...]       # all three by default
```

`NODE`, `DISC` and `PROXY` override the image names (to point a scenario at
a build with a fix reverted), `SCENARIO_TIMEOUT` the per-scenario watchdog
(seconds, default 600). It runs nightly and on demand in the *Hostile load*
workflow, not per push: three image builds and several minutes, on a runner
whose memory limits are noisier than a unit test's.

| scenario | container limit | what it does | passes when | history |
|---|---|---|---|---|
| `proxy_inflight_bytes.sh` | proxy 512 MiB | one connection pipelines 300 x 1 MB `S` while the node is `docker pause`d (its connection stays open, replies never come) | peak RSS < 50%; the proxy stops reading instead (the client's send blocks) | #691 (in-flight requests bounded by count, not bytes); #657, #658, #692, #693 share the byte budget |
| `node_stalled_joiner.sh` | node 256 MiB, `--max-memory` 64 MiB | a third node joins and is paused as soon as the handoff starts; 8 connections write `s` frames with a unique ~1 MB namespace and a 2-byte write | peak RSS < 85%; the node holds the clients' acks instead of queueing without bound | #694 (forward queue with no byte bound pinning whole frames); #659 |
| `discovery_blocked_q.sh` | discovery 256 MiB | 1,024 proxies registered with `Y`, then 4 containers x 256 connections with a 1 KiB receive buffer ask `Q` and never read | peak RSS < 50% | #703 (`Q` reply built per request and held while the write blocks) |

Reverting the fix for #691, #694 or #703 makes its scenario fail (the node
and discovery are OOM-killed; the proxy reaches ~500 of its 512 MiB). The
known patterns not yet covered, each a new scenario here: #696 (1,002
half-open ~1 MiB frames from four source IPs), #701 (`PendingClears`), #695
(`/metrics` with many namespaces), #697 (`--max-memory` against tiny entries),
#692 (an `m`/`o` frame of one-byte keys).

## Shared helpers

- `cl.sh` — sourced by the scenarios: `disc_up`, `node_up`, `members`,
  `wait_members`, `load`, `summ`, `warns`.
- `disc.py` (query `L`), `verify.py` (seed/check), `load.py` (mixed load,
  `--hedge`), `stats.sh` + `memtrend.py` (RSS sampling + slope).
- `CLIENT=<image>` overrides the `e2e-client` image name in `cl.sh` and
  `chaos.sh` (the other scenarios still use the literal name).

## Scenario catalogue

[`SCENARIOS.md`](SCENARIOS.md) is the full catalogue — every scenario across
all rounds (single-node, cluster, long-run, the v0.2.0→v0.3.0 verification
rounds, and the post-v0.3.0 review reproductions) with its measured result.

## History

These grew out of the v0.2.0 → v0.3.0 verification effort (issues #61–#68, then
the post-v0.3.0 review #91–#97).
