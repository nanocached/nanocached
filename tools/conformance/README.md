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
| `member.sh`, `member_matrix.sh` | every writing command around join / leave / evict (`member_workload.py` writes, `member_read.py` reads back); exits 1 on a violation of the invariants below. Four of its cells run nightly in CI |
| `wire_probe.py` | raw protocol replies for hand-built frames |

The clients run in containers on the same Docker network as the nodes
(discovery hands out container IPs, which the host cannot reach on macOS).
A watchdog removes every container of a run after `WATCHDOG` seconds.

## Membership invariants

`member.sh` runs every writing command in serial sequences while the cluster
changes (a node joins, leaves, is killed, or two of those overlap), waits for
the cluster to go quiet, then `member_read.py` reads every key from every live
node with a raw `g` and compares what the owners hold with what the
acknowledged operations imply. The run fails when any key breaks one of these:

| invariant | the reader's verdict when it breaks |
|---|---|
| an acknowledged `set` (with or without a TTL, in the default namespace or another) is readable with its latest value from every owner | `lost` (absent where a value was expected), `wrong` (another value), `partial` (the owners disagree) |
| an acknowledged `delete`, `clear` or expiry is never undone | `resurrected` |
| the result of `incr` / `decr` is not lost across a membership change, and every owner holds the final total | `counter`, `lost` |
| compare-and-set and put-if-absent give the answer their sequence implies | `cas_bad`, `pia_bad` |
| a key's remaining TTL is the same on every owner and neither extended nor shortened by the change | `ttl_extended`, `ttl_shortened`, `ttl_spread`, `ttl_unreadable` |
| once the cluster is quiet, exactly R nodes claim each key (answer anything but `W`) and each holds it | `owners`, `partial` |

The remaining-TTL check allows the copy to be up to four seconds short: a
handoff sends the remaining TTL in whole seconds, so each join a copy goes
through can cost it up to one more.

What this matrix does not catch: a client whose node list lags a second
change, or a join followed at once by a decommission and a kill. Reverting the
fix that keeps an earlier joiner's forwarding window open when the next join
starts was seen to fail one of eight `joinjoin` cells, so the unit test of that
window is the guard for it, not this matrix.

A sequence that raised while the cluster changed (a killed node's connections
fail until it is evicted) is not judged. A run in which more than a quarter of
the sequences raised, or none was judged, fails too: it proves nothing.

**Accepted behaviour is an explicit exception, so a failure means something
new.** `ACCEPTED` in `member_read.py` lists what `docs/architecture.html`
("Accepted behaviour") allows and the workload does produce: increments of one
counter issued in parallel during a join's forwarding window can lose a few
(the owners end below the expected total, never above it, and never by more
than six; one to three is usual, five was the most seen). The other accepted items need a shape this workload does not
produce (two callers racing one key, a client whose node list is older than
two membership changes, two membership changes inside one drain window) and
are not tolerated here: if one of them fails a cell, that is the point to
read the log and either fix the code or add the exception with the evidence.
