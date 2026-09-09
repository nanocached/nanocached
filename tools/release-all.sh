#!/usr/bin/env bash
# Release every component — six SDKs, the server images and the seven
# framework adapters — at one aligned version:
#   tools/release-all.sh 0.4.4
#
# Every release ships all fourteen, whether or not a component changed:
# one version number means "these were built and tested together".
#
# Verifies each manifest already carries the version, then pushes the release
# tags ONE AT A TIME — pushing four or more tags in a single `git push`
# generates no GitHub events, so none of the release workflows would trigger.
# SDK tags go first: every adapter's published manifest depends on the SDK
# release of the same version, so the SDK must exist on its registry before
# anyone can install the adapter.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

ver="${1:?usage: tools/release-all.sh <version>  (e.g. 0.4.4)}"

fail() { echo "error: $1 does not carry version $ver" >&2; exit 1; }

# server + six SDKs
grep -q "^version = \"$ver\"\$" Cargo.toml                 || fail Cargo.toml
grep -q "^version = \"$ver\"\$" sdk/rust/Cargo.toml        || fail sdk/rust/Cargo.toml
grep -q "^version = \"$ver\"\$" sdk/python/pyproject.toml  || fail sdk/python/pyproject.toml
grep -q "^__version__ = \"$ver\"\$" sdk/python/src/nanocached/__init__.py \
                                                           || fail 'sdk/python __init__.py (__version__)'
grep -q "^version = '$ver'\$"   sdk/java/build.gradle      || fail sdk/java/build.gradle
grep -q "<Version>$ver</Version>" sdk/dotnet/src/Nanocached/Nanocached.csproj \
                                                           || fail Nanocached.csproj
[ "$(node -p 'require("./sdk/typescript/package.json").version')" = "$ver" ] \
                                                           || fail sdk/typescript/package.json

# seven adapters — the version, and the pin on the SDK of the same version
grep -q "^version = \"$ver\"\$" adapters/django/pyproject.toml || fail adapters/django/pyproject.toml
grep -q "\"nanocached>=$ver\"" adapters/django/pyproject.toml  || fail 'adapters/django (nanocached pin)'
grep -q "<Version>$ver</Version>" adapters/dotnet/src/Nanocached.Caching/Nanocached.Caching.csproj \
                                                               || fail Nanocached.Caching.csproj
for a in spring jcache spring-boot-starter; do
    grep -q "^version = '$ver'\$" "adapters/$a/build.gradle"  || fail "adapters/$a/build.gradle"
done
grep -q "org.nanocached:nanocached:$ver'" adapters/spring/build.gradle || fail 'adapters/spring (nanocached pin)'
grep -q "org.nanocached:nanocached:$ver'" adapters/jcache/build.gradle || fail 'adapters/jcache (nanocached pin)'
grep -q "org.nanocached:nanocached-spring:$ver'" adapters/spring-boot-starter/build.gradle \
                                                               || fail 'adapters/spring-boot-starter (nanocached-spring pin)'
for a in cache-manager keyv; do
    [ "$(node -p "require('./adapters/$a/package.json').version")" = "$ver" ] \
                                                               || fail "adapters/$a/package.json"
done
# The Java adapters' READMEs quote the Maven coordinates with a version.
for a in spring jcache spring-boot-starter; do
    grep -q "org.nanocached:nanocached-$a:$ver'" "adapters/$a/README.md" \
                                                               || fail "adapters/$a/README.md (install snippet)"
done

if ! git diff --quiet HEAD; then
    echo "error: uncommitted changes — commit and push before releasing" >&2
    exit 1
fi

# Resumable: a push that fails partway (network, expired auth) leaves some
# components released and others not, so a re-run must pick up where it
# stopped — a tag already created locally is reused if it points at HEAD
# (and refused otherwise), and one already on the remote is skipped.
head="$(git rev-parse HEAD)"
for tag in "sdk/rust/v$ver" "sdk/python/v$ver" "sdk/go/v$ver" "sdk/java/v$ver" \
           "sdk/typescript/v$ver" "sdk/dotnet/v$ver" "server/v$ver" \
           "adapter/django/v$ver" "adapter/dotnet/v$ver" \
           "adapter/cache-manager/v$ver" "adapter/keyv/v$ver" \
           "adapter/spring/v$ver" "adapter/jcache/v$ver" "adapter/spring-boot-starter/v$ver"; do
    if local_target="$(git rev-parse -q --verify "refs/tags/$tag^{commit}")"; then
        if [ "$local_target" != "$head" ]; then
            echo "error: tag $tag already exists locally but points at $local_target, not HEAD ($head)" >&2
            exit 1
        fi
    else
        git tag "$tag"
    fi

    if remote_target="$(git ls-remote --tags origin "refs/tags/$tag" | cut -f1)" && [ -n "$remote_target" ]; then
        if [ "$remote_target" != "$head" ]; then
            echo "error: tag $tag already exists on origin but points at $remote_target, not HEAD ($head)" >&2
            exit 1
        fi
        echo "skip: $tag is already on origin"
        continue
    fi

    git push origin "$tag"
    sleep 2
done

echo "All release tags for $ver pushed. Watch: gh run list --limit 20"
