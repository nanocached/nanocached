#!/bin/bash
NAME=$1; shift; A=(); while [ "$1" != "--" ]; do A+=("$1"); shift; done; shift
exec docker run --rm --name "$NAME" "${A[@]}" eclipse-temurin:17-jre java -cp /repo/tools/conformance/drivers/java/classes Scenario "$@"
