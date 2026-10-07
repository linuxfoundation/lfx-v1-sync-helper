#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Full-population cohort extraction.
#
# Runs scripts/lfxv2_1507_cohort_usernames.sql once, writing every v1 user
# with something to sync, most recently active first, to
# lfxv2_1507_cohort_usernames.csv. Then categorizes it against resolved.csv
# (LFXV2-2662 flagged accounts) into lfxv2_1507_cohort_categorized.csv.
#
# Each username appears exactly once, so no dedup is needed downstream.
#
# Usage:
#   scripts/lfxv2_1507_cohort.sh
#
# Requires: snowsql configured with rsa_key.p8 in the working directory, and
# a freshly regenerated resolved.csv.

set -eu

OUT="lfxv2_1507_cohort_usernames.csv"
CATEGORIZED="lfxv2_1507_cohort_categorized.csv"

if [ ! -f resolved.csv ]; then
    echo "error: resolved.csv not found; the cohort must be screened against" >&2
    echo "       the LFXV2-2662 flagged-account list before chunking." >&2
    exit 1
fi

rm -f "$OUT"

snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
    --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
    -o friendly=false -o header=true -o timing=false \
    -o output_format=csv -o output_file="$OUT" \
    -f "$(dirname "$0")/lfxv2_1507_cohort_usernames.sql"

echo "wrote $OUT ($(($(wc -l < "$OUT") - 1)) usernames)"

"$(dirname "$0")/lfxv2_1507_categorize.sh" "$OUT" resolved.csv > "$CATEGORIZED"
echo "wrote $CATEGORIZED"
awk -F, 'NR > 1 {print $NF}' "$CATEGORIZED" | sort | uniq -c

echo ""
echo "next:"
echo "  scripts/lfxv2_1507_build_chunks.sh"
