#!/bin/bash
source "$(dirname "$0")/../lib.sh"
exec docker run "${DENV[@]}" mcr.microsoft.com/dotnet/runtime:8.0 dotnet /repo/tools/conformance/drivers/dotnet/bin/conformance.dll $CASES $PHASE $PATHK $TOPO
