#!/bin/bash
# Build every non-Python driver from the checked-out tree. Run once before run.sh.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"; REPO="$(cd "$HERE/../../.." && pwd)"
ARCH=$(docker info --format '{{.Architecture}}'); case $ARCH in aarch64|arm64) GOARCH=arm64;; *) GOARCH=amd64;; esac
echo "== typescript"; (cd "$REPO/sdk/typescript" && npm run build >/dev/null)
echo "== go ($GOARCH)"; (cd "$HERE/go" && GOFLAGS=-mod=mod CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH go build -o conf-go . && GOFLAGS=-mod=mod CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH go build -o conf-go-scenario ./scenario)
echo "== java"; rm -rf "$HERE/java/classes"; javac --release 17 -d "$HERE/java/classes" "$REPO"/sdk/java/src/main/java/org/nanocached/*.java "$HERE/java/Driver.java" "$HERE/java/Scenario.java"
echo "== dotnet"; (cd "$HERE/dotnet" && dotnet publish -c Release -o bin >/dev/null) && (cd "$HERE/dotnet-scenario" && dotnet publish -c Release -o bin >/dev/null)
echo "== rust (in a container, cargo caches kept in volumes)"
docker run --rm -v "$REPO:/repo" -v conf-cargo-registry:/usr/local/cargo/registry -v conf-cargo-target:/target -e CARGO_TARGET_DIR=/target \
  -w /repo/tools/conformance/drivers/rust rust:latest sh -c 'cargo build --release 2>&1 | tail -3 && cp /target/release/conformance /target/release/scenario /repo/tools/conformance/drivers/rust/'
