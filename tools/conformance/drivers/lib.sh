# sourced by drivers/<sdk>/run.sh. Args: NET NAME ADDR VIA OUT cases.json phase path topo
NET=$1; NAME=$2; ADDR=$3; VIA=$4; OUT=$5; PHASE=$7; PATHK=$8; TOPO=$9
CASES=/out/cases.tsv
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DENV=(--rm --name "$NAME" --network "$NET" -e NANOTEST_ADDRESSES="$ADDR" -e NANOTEST_VIA_PROXY="$VIA" -e NANOTEST_OPTS="${NANOTEST_OPTS:-}" -v "$REPO:/repo:ro" -v "$OUT:/out")
