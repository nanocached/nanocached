#!/bin/bash
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
exec docker run --rm --name "$NAME" "${A[@]}" rust:1.99 /repo/tools/conformance/drivers/rust/scenario "$@"
