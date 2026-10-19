#!/bin/bash
source "$(dirname "$0")/../lib.sh"
exec docker run "${DENV[@]}" eclipse-temurin:17-jre java -cp /repo/tools/conformance/drivers/java/classes Driver $CASES $PHASE $PATHK $TOPO
