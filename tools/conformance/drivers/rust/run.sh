#!/bin/bash
source "$(dirname "$0")/../lib.sh"
exec docker run "${DENV[@]}" rust:1.96 /repo/tools/conformance/drivers/rust/conformance $CASES $PHASE $PATHK $TOPO
