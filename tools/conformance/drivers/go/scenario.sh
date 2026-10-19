#!/bin/bash
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
exec docker run --rm --name "$NAME" "${A[@]}" alpine:3.22 /repo/tools/conformance/drivers/go/conf-go-scenario "$@"
