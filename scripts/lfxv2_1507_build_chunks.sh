#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Split the clean cohort into ConfigMap-sized chunk files.
#
# Reads lfxv2_1507_cohort_categorized.csv, keeps only rows flagged "clean",
# and writes their usernames, in cohort order (most recently active first),
# to lfxv2_1507_chunk01.txt, lfxv2_1507_chunk02.txt, and so on. Each chunk is
# capped by bytes rather than lines so that every one fits in a ConfigMap
# (1 MiB limit) regardless of username length.
#
# Usage:
#   scripts/lfxv2_1507_build_chunks.sh [--max-bytes N] [--force]
#
# --force overwrites existing chunk files. Without it the script refuses, so
# a rebuilt cohort cannot silently renumber chunks that are already deployed.

set -eu

LC_ALL=C
export LC_ALL

CATEGORIZED="lfxv2_1507_cohort_categorized.csv"
MAX_BYTES=950000
FORCE=false

while [ $# -gt 0 ]; do
    case "$1" in
        --max-bytes) MAX_BYTES="${2:?--max-bytes needs a value}"; shift 2 ;;
        --force) FORCE=true; shift ;;
        *) echo "error: unknown argument '$1'" >&2; exit 1 ;;
    esac
done

if [ ! -f "$CATEGORIZED" ]; then
    echo "error: $CATEGORIZED not found; run scripts/lfxv2_1507_cohort.sh first" >&2
    exit 1
fi

if ls lfxv2_1507_chunk[0-9]*.txt >/dev/null 2>&1; then
    if ! $FORCE; then
        echo "error: chunk files already exist; pass --force to rebuild them" >&2
        exit 1
    fi
    rm -f lfxv2_1507_chunk[0-9]*.txt
fi

# Usernames are arbitrary LDAP uids. A comma in one would shift every field,
# so any row with an unexpected field count aborts the whole build rather
# than emitting a mangled username.
awk -F, -v max="$MAX_BYTES" '
NR == 1 { nf = NF; next }
NF != nf {
    printf "error: line %d has %d fields, expected %d: %s\n", NR, NF, nf, $0 > "/dev/stderr"
    bad = 1
    exit 1
}
$NF != "clean" { flagged++; next }
{
    u = $1
    gsub(/"/, "", u)
    if (u == "") next
    len = length(u) + 1
    if (out == "" || bytes + len > max) {
        if (out != "") close(out)
        chunk++
        out = sprintf("lfxv2_1507_chunk%02d.txt", chunk)
        bytes = 0
    }
    print u > out
    bytes += len
    clean++
}
END {
    if (bad) exit 1
    printf "%d clean usernames in %d chunks, %d flagged/excluded\n", clean, chunk, flagged
}
' "$CATEGORIZED" || {
    rm -f lfxv2_1507_chunk[0-9]*.txt
    exit 1
}

for f in lfxv2_1507_chunk[0-9]*.txt; do
    printf '  %s: %s users, %s bytes\n' "$f" \
        "$(wc -l < "$f" | tr -d ' ')" "$(wc -c < "$f" | tr -d ' ')"
done

echo ""
echo "next:"
echo "  scripts/lfxv2_1507_deploy_chunk.sh lfxv2_1507_chunk01.txt"
