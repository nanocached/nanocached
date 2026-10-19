#!/bin/bash
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
exec docker run --rm --name "$NAME" "${A[@]}" rust:latest /repo/tools/conformance/drivers/rust/scenario "$@"
