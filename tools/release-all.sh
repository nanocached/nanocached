#!/usr/bin/env bash
# Release every component (six SDKs + server images) at one aligned version:
#   tools/release-all.sh 0.2.0
#
# Verifies each manifest already carries the version, then pushes the release
# tags ONE AT A TIME — pushing four or more tags in a single `git push`
# generates no GitHub events, so none of the release workflows would trigger.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

ver="${1:?usage: tools/release-all.sh <version>  (e.g. 0.2.0)}"

fail() { echo "error: $1 does not carry version $ver" >&2; exit 1; }

grep -q "^version = \"$ver\"\$" Cargo.toml                 || fail Cargo.toml
grep -q "^version = \"$ver\"\$" sdk/rust/Cargo.toml        || fail sdk/rust/Cargo.toml
grep -q "^version = \"$ver\"\$" sdk/python/pyproject.toml  || fail sdk/python/pyproject.toml
grep -q "^version = '$ver'\$"   sdk/java/build.gradle      || fail sdk/java/build.gradle
grep -q "<Version>$ver</Version>" sdk/dotnet/src/Nanocached/Nanocached.csproj \
                                                           || fail Nanocached.csproj
[ "$(node -p 'require("./sdk/typescript/package.json").version')" = "$ver" ] \
                                                           || fail sdk/typescript/package.json

if ! git diff --quiet HEAD; then
    echo "error: uncommitted changes — commit and push before releasing" >&2
    exit 1
fi

for tag in "sdk/rust/v$ver" "sdk/python/v$ver" "sdk/go/v$ver" "sdk/java/v$ver" \
           "sdk/typescript/v$ver" "sdk/dotnet/v$ver" "server/v$ver"; do
    git tag "$tag"
    git push origin "$tag"
    sleep 2
done

echo "All release tags for $ver pushed. Watch: gh run list --limit 10"
