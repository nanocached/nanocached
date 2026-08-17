# AWS live-cluster SDK harness

Minimal programs, one per SDK, for exercising a real discovery-fronted
cluster from a test host — used for the AWS scenario tests (node join,
node death, mid-migration death, queued joins) that verified v1.0.0.
Every program reads its seeds from the same environment variable:

```sh
export NANOTEST_SEEDS="10.0.0.1:8357,10.0.0.2:8357"   # discovery replicas, in order
```

## The scenario driver (Python)

`python/nanotest.py` is the orchestration driver; the rest are smoke
programs. It uses the Python SDK from this repository (no install):

```sh
python3 python/nanotest.py nodes            # raw L query: members + R
python3 python/nanotest.py preload 2000     # write bulk:<i> keys
python3 python/nanotest.py verify 2000      # read them all back, report missing
python3 python/nanotest.py churn 90 out.json  # continuous get/set, log failures
python3 python/nanotest.py write <label> <n>  # x:<label>:<i> keys
python3 python/nanotest.py read <label> <n>
python3 python/nanotest.py readall ts,py,go <n>
```

`churn` is the scenario probe: run it while killing/adding nodes, then
inspect the JSON for the failure window (first/last failure timestamps,
per-event error types).

## The per-SDK smoke programs

Each takes `<write|read> <label> <count>` and verifies
`x:<label>:<i> = v-<label>-<i>`, so any SDK can read what any other
wrote — the cross-language routing check:

| SDK | Build / run |
|---|---|
| TypeScript | build `sdk/typescript` first, then `node ts/main.mjs …` |
| Python | `python3 python/nanotest.py …` |
| Java | `javac -d classes java/Main.java ../../sdk/java/src/main/java/org/nanocached/*.java && java -cp classes Main …` |
| Rust | `cargo build --release` in `rust/`, run `target/release/nanotest …` |
| .NET | `dotnet publish -c Release -o bin` in `dotnet/`, run `bin/nanotest …` |
| Go | `go build -o nanotest-go .` in `go/`, run `./nanotest-go …` |

All reference the SDKs in this repository by path, so what you test is
the working tree, not a published package.

## Recreating the AWS setup, briefly

The 2026-08 test run used: ECS Fargate tasks (ARM64 images built from
the repo `Dockerfile`, pushed to ECR) — two `nanocached-discovery`
tasks started first so their private IPs could be passed to every
node's `--discovery` (same list, same order) and to `NANOTEST_SEEDS`;
cache nodes as standalone `run-task`s (no service, so a stopped task
stays dead); one security group allowing 8356-8358 from itself; and an
EC2 (Graviton, AL2023) test host in the same subnet/SG driven over SSM.
Scale nodes with `run-task`, kill them with `stop-task`, and watch the
churn output.
