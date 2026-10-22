#!/bin/bash
# Run the membership-change matrix: member_matrix.sh <rep-tag> [parallel]   (cells below, 4 at a time)
# Exits non-zero when a cell failed.
HERE="$(cd "$(dirname "$0")" && pwd)"; TAG=${1:?tag}; PAR=${2:-4}
CELLS=${CELLS:-"1 join|2 join|3 join|1 leave|2 leave|3 leave|1 evict|2 evict|3 evict|2 joinjoin|2 joinleave|2 leavejoin|2 join budget|2 leave budget|3 joinjoin"}
mkdir -p "$HERE/results"
i=0; for c in $(echo "$CELLS" | tr ' |' '_ ' ); do i=$((i+1)); echo "$TAG$i ${c//_/ }"; done \
 | xargs -P "$PAR" -L 1 sh -c 'RUN=$0 "'"$HERE"'/member.sh" $@ > "'"$HERE"'/results/matrix-$0.log" 2>&1'
