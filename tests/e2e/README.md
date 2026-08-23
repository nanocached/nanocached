# E2E scenarios

Docker-based end-to-end scenarios used to reproduce and regression-check
cluster-level behaviour (data loss on join/eviction, slow-node tail latency,
SDK shutdown/leak behaviour, hostile-peer resource exhaustion).

**These are not run in CI.** Several take minutes to an hour and need Docker,
multiple containers, `NET_ADMIN` (netem), and published images. CI only
*lints* the scripts (shell syntax + `shellcheck`, Python `py_compile`) so they
stay runnable; it never executes them. Run them by hand when investigating or
verifying a fix.

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

## Shared helpers

- `cl.sh` — sourced by the scenarios: `disc_up`, `node_up`, `members`,
  `wait_members`, `load`, `summ`, `warns`.
- `disc.py` (query `L`), `verify.py` (seed/check), `load.py` (mixed load,
  `--hedge`), `stats.sh` + `memtrend.py` (RSS sampling + slope).

## History

These grew out of the v0.2.0 → v0.3.0 verification effort (issues #61–#68, then
the post-v0.3.0 review #91–#97). The scenario catalogue with measured results
lives in the verification notes for those rounds.
