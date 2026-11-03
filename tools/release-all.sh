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
#
# Releases only a commit that is already the tip of origin/main, in a clean
# tree (untracked files count), and whose CI run on main is green: a tag push
# publishes to the registries, and a tag on a commit that is not on a pushed
# branch, or that CI has not passed, publishes something nobody reviewed.

# The checks are functions so that they can be tried in a scratch repository:
# `source tools/release-all.sh` defines them and runs nothing else.

# No modified, staged or untracked (not ignored) file.
check_clean_tree() {
    local dirty
    dirty="$(git status --porcelain --untracked-files=all)" || { echo "error: git status failed" >&2; return 1; }
    if [ -n "$dirty" ]; then
        echo "error: the working tree is not clean (modified, staged or untracked files); commit and push them, or remove them:" >&2
        printf '%s\n' "$dirty" | head -10 >&2
        return 1
    fi
}

# Is any release tag of <version> on origin at <sha>?
release_tag_on_origin_at() {
    git ls-remote --tags origin "refs/tags/*/v$1" "refs/tags/*/*/v$1" | cut -f1 | grep -qx "$2"
}

# HEAD must be origin/main as just fetched. The one exception is resuming a
# release that was already started from HEAD (a tag of this version is on
# origin at HEAD): main may have moved on since, e.g. with the docs PR.
check_head_is_origin_main() { # <version>
    local head main
    git fetch --quiet origin "+refs/heads/main:refs/remotes/origin/main" \
        || { echo "error: could not fetch origin/main" >&2; return 1; }
    head="$(git rev-parse HEAD)"; main="$(git rev-parse refs/remotes/origin/main)"
    [ "$head" = "$main" ] && return 0
    if git merge-base --is-ancestor "$head" "$main" && release_tag_on_origin_at "$1" "$head"; then
        echo "note: HEAD is behind origin/main, but a $1 tag is already on origin at HEAD; resuming that release" >&2
        return 0
    fi
    echo "error: HEAD ($head) is not origin/main ($main); push it to main (and let CI pass) first, or check out origin/main" >&2
    return 1
}

# The newest CI run (push event) on <sha> must have completed successfully.
check_ci_green() { # <sha>
    local res
    command -v gh >/dev/null || { echo "error: gh is needed to check CI for $1" >&2; return 1; }
    res="$(gh run list --workflow ci.yaml --commit "$1" --event push --limit 20 --json status,conclusion \
            --jq 'if length == 0 then "none" else "\(.[0].status) \(.[0].conclusion)" end')" \
        || { echo "error: could not list CI runs for $1" >&2; return 1; }
    case "$res" in
        "completed success") return 0 ;;
        none) echo "error: no CI run found for $1 (has it been pushed to main?)" >&2 ;;
        *)    echo "error: the newest CI run on $1 is '$res', not 'completed success'" >&2 ;;
    esac
    return 1
}

[ "${BASH_SOURCE[0]}" = "$0" ] || return 0

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

check_clean_tree
check_head_is_origin_main "$ver"
check_ci_green "$(git rev-parse HEAD)"

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
