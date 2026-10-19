# SDK conformance harness

Runs the same cases through all six SDKs against a real cluster (Docker) and
checks what each one observed against the protocol/SDK-doc oracle and against
the other five.

Build the server images from the checked-out tree, then the drivers
(needs docker, npm, go, a JDK 17+ and the .NET SDK; Rust builds in a container):

```sh
for t in node discovery proxy; do docker build --target $t -t nanocached-$t:conformance .; done
tools/conformance/drivers/build.sh
```

(`NODE` / `DISC` / `PROXY` override the image names.)

## Cases

`make_cases.py` defines the cases once; every driver in `drivers/<sdk>/`
reads the JSON it prints and reports the outcome of each step.
`compare.py` checks the reports against each step's `expect` and the six
reports against each other.

```sh
tools/conformance/run.sh <single|cluster> <direct|proxy> [sdk ...]
```

`single` is one standalone node, `cluster` is discovery plus three nodes
(R=2), `proxy` adds a `nanocached-proxy` and the SDKs use `via_proxy`.
Results land in `results/<run>/<topology>-<path>` (git-ignored).

## Other scripts

| script | what it checks |
|---|---|
| `scenarios.sh <v6\|v5\|v7\|v4>` | scenarios that need containers started, stopped or put behind a fixture: connection failures and auth, join / decommission / kill under load, client lifecycle, a node dying before a multi-get |
| `scenarios_dynamic.sh <v5\|v7>` | the join / decommission / kill and client-lifecycle scenarios, one run per SDK (`report_scen.py` summarises) |
| `replica.sh` | do all owners of a key hold the same copy after the writes have finished |
| `member.sh`, `member_matrix.sh` | every writing command around join / leave / evict (`member_workload.py` writes, `member_read.py` reads back) |
| `wire_probe.py` | raw protocol replies for hand-built frames |

The clients run in containers on the same Docker network as the nodes
(discovery hands out container IPs, which the host cannot reach on macOS).
A watchdog removes every container of a run after `WATCHDOG` seconds.
