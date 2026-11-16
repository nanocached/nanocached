#!/bin/bash
source "$(dirname "$0")/../lib.sh"
source "$(dirname "$0")/image.sh"
exec docker run "${DENV[@]}" "$RUST_IMAGE" /repo/tools/conformance/drivers/rust/conformance $CASES $PHASE $PATHK $TOPO
