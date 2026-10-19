#!/bin/bash
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
exec docker run --rm --name "$NAME" "${A[@]}" mcr.microsoft.com/dotnet/runtime:8.0 dotnet /repo/tools/conformance/drivers/dotnet-scenario/bin/scenario.dll "$@"
