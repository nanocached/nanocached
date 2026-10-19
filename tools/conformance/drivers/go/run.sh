#!/bin/bash
source "$(dirname "$0")/../lib.sh"
exec docker run "${DENV[@]}" alpine:3.22 /repo/tools/conformance/drivers/go/conf-go $CASES $PHASE $PATHK $TOPO
