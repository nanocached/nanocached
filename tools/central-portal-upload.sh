#!/usr/bin/env bash
# Uploads a local Maven repository holding a release's signed artifacts to the
# Maven Central Portal as ONE deployment, and waits until the Portal has
# validated it and started publishing:
#
#   tools/central-portal-upload.sh <maven-repository> <deployment-name>
#
# One deployment per version, not one per module: the free tier counts release
# events ("a distinct publish operation to Maven Central"), seven a month, and
# the four Maven modules published one by one cost four of them.
#
# The deployment is AUTOMATIC (published as soon as it validates), as
# `publishToMavenCentral(true)` in the builds was. A failed validation fails
# the script with the Portal's errors; the deployment then waits in the Portal
# (https://central.sonatype.com/publishing/deployments) to be dropped.
#
# Environment:
#   MAVEN_CENTRAL_USERNAME, MAVEN_CENTRAL_PASSWORD  the Portal user token
#   CENTRAL_PORTAL_URL    default https://central.sonatype.com
#   CENTRAL_DRY_RUN=1     build and list the bundle, upload nothing
#   CENTRAL_POLL_SECONDS  default 10; CENTRAL_DEADLINE_SECONDS  default 1800
#
# API: https://central.sonatype.org/publish/publish-portal-api/
set -euo pipefail

repository="${1:?usage: central-portal-upload.sh <maven-repository> <deployment-name>}"
deployment_name="${2:?usage: central-portal-upload.sh <maven-repository> <deployment-name>}"
portal="${CENTRAL_PORTAL_URL:-https://central.sonatype.com}"
poll="${CENTRAL_POLL_SECONDS:-10}"
deadline_after="${CENTRAL_DEADLINE_SECONDS:-1800}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bundle="$work/bundle.zip"
response="$work/response"

# Central needs a .asc, .md5 and .sha1 next to each file; .sha256/.sha512 are
# optional and signatures need no checksums
# (https://central.sonatype.org/publish/requirements/). Every file counts
# against the monthly file limit, so the bundle leaves out the rest of what
# Gradle writes: maven-metadata.xml (Central makes its own), .sha256, .sha512
# and the signatures' checksums.
files="$(cd "$repository" && find . -type f \
    ! -name 'maven-metadata*' ! -name '*.sha256' ! -name '*.sha512' \
    ! -name '*.asc.md5' ! -name '*.asc.sha1' | sed 's|^\./||' | LC_ALL=C sort)"
if [ -z "$files" ]; then
    echo "::error::no artifacts under $repository"
    exit 1
fi

# Every artifact must carry its signature and both checksums, or the Portal
# rejects the whole deployment after the upload; catch it before.
missing=0
while IFS= read -r f; do
    case "$f" in *.asc|*.md5|*.sha1) continue ;; esac
    for ext in asc md5 sha1; do
        if ! grep -qxF "$f.$ext" <<<"$files"; then
            echo "::error::$f has no .$ext"
            missing=1
        fi
    done
done <<<"$files"
[ "$missing" = 0 ] || exit 1

(cd "$repository" && zip -q -X "$bundle" -@ <<<"$files")
echo "bundle: $(wc -l <<<"$files" | tr -d ' ') files, $(wc -c <"$bundle" | tr -d ' ') bytes zipped"
# The components in it, one line each (group/artifact/version directories).
sed -n 's|/[^/]*$||p' <<<"$files" | LC_ALL=C sort -u | sed 's|^|  |'

if [ "${CENTRAL_DRY_RUN:-}" = 1 ]; then
    echo "dry run: not uploading"
    exit 0
fi

: "${MAVEN_CENTRAL_USERNAME:?}" "${MAVEN_CENTRAL_PASSWORD:?}"
bearer="$(printf '%s:%s' "$MAVEN_CENTRAL_USERNAME" "$MAVEN_CENTRAL_PASSWORD" | base64 | tr -d '\n')"

call_portal() { # <curl arguments>; leaves the response body in $response
    rm -f "$response"
    # The token goes through a config file on a pipe, so it stays out of the
    # process list. Without the Accept header the Portal answers in XML.
    if ! curl --silent --show-error --fail-with-body --output "$response" \
            --config <(printf 'header = "Authorization: Bearer %s"\n' "$bearer") \
            --header 'Accept: application/json' "$@"; then
        [ -s "$response" ] && { cat "$response"; echo; }
        return 1
    fi
}

# Not retried: a retry after a lost response would create a second
# deployment, and with it a second release event.
call_portal --connect-timeout 30 --max-time 900 --request POST \
    --url-query "name=$deployment_name" --url-query "publishingType=AUTOMATIC" \
    --form "bundle=@$bundle;type=application/octet-stream" \
    "$portal/api/v1/publisher/upload" || exit 1
deployment_id="$(tr -d '[:space:]' <"$response")"
if [ -z "$deployment_id" ]; then
    echo "::error::the Portal accepted the upload but returned no deployment id"
    exit 1
fi
echo "uploaded $deployment_name as deployment $deployment_id"

deadline=$((SECONDS + deadline_after))
last=""
while :; do
    # A status read is safe to repeat; one failed read is not a failed release.
    if call_portal --connect-timeout 10 --max-time 30 --retry 3 --request POST \
            --url-query "id=$deployment_id" "$portal/api/v1/publisher/status"; then
        state="$(jq -r '.deploymentState // empty' "$response")"
        if [ "$state" != "$last" ]; then
            echo "deployment $deployment_id: $state"
            last="$state"
        fi
        case "$state" in
            # VALIDATED is passed through on the way to PUBLISHING for an
            # AUTOMATIC deployment.
            PENDING|VALIDATING|VALIDATED) ;;
            # Validation is over and Central has the artifacts; the sync to
            # repo1.maven.org can take an hour, and nothing here can fail it.
            PUBLISHING|PUBLISHED) exit 0 ;;
            FAILED)
                echo "::error::the Central Portal rejected deployment $deployment_id"
                jq '.errors' "$response"
                exit 1 ;;
            *)
                echo "::error::unexpected deployment state '$state'"
                cat "$response"; echo
                exit 1 ;;
        esac
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "::error::deployment $deployment_id is still '${last:-unknown}' after ${deadline_after}s; check $portal/publishing/deployments"
        exit 1
    fi
    sleep "$poll"
done
