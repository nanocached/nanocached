#!/bin/bash
# scenario.sh <container-name> <docker run env/network args...> -- <scenario args...>
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
exec docker run --rm --name "$NAME" "${A[@]}" -w /repo/tools/conformance/drivers/typescript node:22 node scenario.mjs "$@"
