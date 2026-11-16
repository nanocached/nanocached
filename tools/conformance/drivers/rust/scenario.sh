#!/bin/bash
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
source "$(dirname "$0")/image.sh"
exec docker run --rm --name "$NAME" "${A[@]}" "$RUST_IMAGE" /repo/tools/conformance/drivers/rust/scenario "$@"
