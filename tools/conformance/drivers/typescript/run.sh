#!/bin/bash
source "$(dirname "$0")/../lib.sh"
exec docker run "${DENV[@]}" -w /repo/tools/conformance/drivers/typescript node:22 node driver.mjs $CASES $PHASE $PATHK $TOPO
