# shellcheck shell=bash
# Sourced by drivers/build.sh, run.sh and scenario.sh: the rust image matching
# the toolchain the repository pins in rust-toolchain.toml (which Dependabot
# bumps), so the driver builds and runs on the same Rust as the server.
_toolchain="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)/rust-toolchain.toml"
_channel=$(sed -n 's/^channel = "\(.*\)"$/\1/p' "$_toolchain")
[ -n "$_channel" ] || { echo "no channel in $_toolchain" >&2; exit 1; }
# shellcheck disable=SC2034 # used by the scripts that source this file
RUST_IMAGE="rust:$_channel"
unset _toolchain _channel
