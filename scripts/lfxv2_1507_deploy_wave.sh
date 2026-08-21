#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Build the clean username list for a wave and create the k8s
# ConfigMap for the batch sync Job.
#
# Takes the categorized CSV from lfxv2_1507_wave_usernames.sh, removes
# non-clean rows (LFXV2-2662 flagged), deduplicates against prior waves'
# categorized CSVs (already synced), and creates the sync-users-batch-list
# ConfigMap in the target namespace.
#
# Usage:
#   scripts/lfxv2_1507_deploy_wave.sh <wave> [--dry-run]
#
# Prerequisites:
#   - lfxv2_1507_wave<N>_categorized.csv must exist (run wave_usernames.sh
#     first).
#   - Prior waves' categorized CSVs (wave0, wave1, ...) should be present
#     for deduplication.
#   - aws-vault and kubectl context configured for prod.

set -eu

WAVE="${1:?usage: $0 <wave> [--dry-run]}"
DRY_RUN="${2:-}"
CATEGORIZED="lfxv2_1507_wave${WAVE}_categorized.csv"
CLEAN_TXT="lfxv2_1507_wave${WAVE}_clean.txt"
CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
CONFIGMAP="sync-users-batch-list"

if [ ! -f "$CATEGORIZED" ]; then
    echo "error: $CATEGORIZED not found; run scripts/lfxv2_1507_wave_usernames.sh $WAVE first" >&2
    exit 1
fi

# Extract clean usernames from this wave.
clean_count=0
flagged_count=0
while IFS= read -r line; do
    flag=$(echo "$line" | awk -F, '{print $NF}')
    case "$flag" in
        clean)
            echo "$line" | awk -F, '{gsub(/"/,"",$1); print $1}'
            clean_count=$((clean_count + 1))
            ;;
        lfxv2_2662_flag) ;;  # Header row.
        *)
            flagged_count=$((flagged_count + 1))
            ;;
    esac
done < "$CATEGORIZED" > "$CLEAN_TXT.tmp"

echo "wave $WAVE: $clean_count clean, $flagged_count flagged/excluded"

# Deduplicate against prior waves' categorized CSVs. A username present in
# any earlier wave's clean set has already been synced.
prior_usernames=$(mktemp)
for prior in lfxv2_1507_wave*_categorized.csv; do
    [ "$prior" = "$CATEGORIZED" ] && continue
    # Extract the wave number and skip if it's not a prior (lower) wave.
    prior_wave=$(echo "$prior" | gsed -n 's/.*wave\([0-9]*\)_.*/\1/p')
    [ "$prior_wave" -ge "$WAVE" ] 2>/dev/null && continue
    # Extract clean usernames from the prior wave.
    awk -F, '$NF == "clean" {gsub(/"/,"",$1); print tolower($1)}' "$prior" >> "$prior_usernames"
done

deduped_count=0
already_synced=0
while IFS= read -r user; do
    lower_user=$(echo "$user" | tr '[:upper:]' '[:lower:]')
    if grep -qx "$lower_user" "$prior_usernames" 2>/dev/null; then
        already_synced=$((already_synced + 1))
    else
        echo "$user"
        deduped_count=$((deduped_count + 1))
    fi
done < "$CLEAN_TXT.tmp" > "$CLEAN_TXT"

rm -f "$CLEAN_TXT.tmp" "$prior_usernames"

echo "wave $WAVE: $deduped_count after dedup ($already_synced already synced in prior waves)"

if [ "$deduped_count" -eq 0 ]; then
    echo "no usernames to sync for wave $WAVE"
    exit 0
fi

# ConfigMap size check (1 MiB limit).
size=$(wc -c < "$CLEAN_TXT" | tr -d ' ')
if [ "$size" -gt 1000000 ]; then
    echo "error: $CLEAN_TXT is ${size} bytes, approaching 1 MiB ConfigMap limit" >&2
    exit 1
fi

if [ "$DRY_RUN" = "--dry-run" ]; then
    echo "[dry-run] would create ConfigMap $CONFIGMAP in $NAMESPACE from $CLEAN_TXT ($deduped_count usernames, ${size} bytes)"
    exit 0
fi

# Delete existing ConfigMap if present (ignore errors).
aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    delete configmap "$CONFIGMAP" 2>/dev/null || true

aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    create configmap "$CONFIGMAP" --from-file=usernames.txt="$CLEAN_TXT"

echo "created ConfigMap $CONFIGMAP in $NAMESPACE ($deduped_count usernames, ${size} bytes)"
echo ""
echo "next:"
echo "  scripts/lfxv2_1507_run_batch.sh"
