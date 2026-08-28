#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Build the clean username list for an active-user bucket and
# create the k8s ConfigMap for the batch sync Job.
#
# Parallel track to lfxv2_1507_deploy_wave.sh. Deliberately kept separate:
# deploy_wave.sh dedups across the glob lfxv2_1507_wave*_categorized.csv, so
# naming these files "wave30/60/90" would corrupt the slug track's dedup
# baseline. This track is independent of the slug waves by design -- all sync
# operations are idempotent, so a user appearing in both tracks is a no-op the
# second time.
#
# Usage:
#   scripts/lfxv2_1507_deploy_active.sh <7|14|21|28|35|42|49|56|63|70> [--dry-run]
#
# Prerequisites:
#   - lfxv2_1507_active<N>_categorized.csv (run active_usernames.sh first).
#   - aws-vault and kubectl context configured for prod.

set -eu

BUCKET="${1:?usage: $0 <7|14|21|28|35|42|49|56|63|70> [--dry-run]}"
DRY_RUN="${2:-}"

case "$BUCKET" in
    7|14|21|28|35|42|49|56|63|70) ;;
    *) echo "error: bucket must be one of 7 14 21 28 35 42 49 56 63 70 (got '$BUCKET')" >&2; exit 1 ;;
esac

CATEGORIZED="lfxv2_1507_active${BUCKET}_categorized.csv"
CLEAN_TXT="lfxv2_1507_active${BUCKET}_clean.txt"
CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
CONFIGMAP="sync-users-batch-list"

if [ ! -f "$CATEGORIZED" ]; then
    echo "error: $CATEGORIZED not found; run scripts/lfxv2_1507_active_usernames.sh first" >&2
    exit 1
fi

# Extract clean usernames from this bucket.
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

echo "active$BUCKET: $clean_count clean, $flagged_count flagged/excluded"

# The buckets are disjoint by construction (each user is placed by their most
# recent login only), so this is a safety net rather than a real filter. It
# deliberately does NOT consider the slug waves -- this track is independent.
prior_usernames=$(mktemp)
for prior in lfxv2_1507_active*_categorized.csv; do
    [ "$prior" = "$CATEGORIZED" ] && continue
    prior_bucket=$(echo "$prior" | gsed -n 's/.*active\([0-9]*\)_.*/\1/p')
    [ "$prior_bucket" -ge "$BUCKET" ] 2>/dev/null && continue
    awk -F, '$NF == "clean" {gsub(/"/,"",$1); print tolower($1)}' "$prior" >> "$prior_usernames"
done

deduped_count=0
already_synced=0
while IFS= read -r user; do
    lower_user=$(echo "$user" | tr '[:upper:]' '[:lower:]')
    if grep -qxF "$lower_user" "$prior_usernames" 2>/dev/null; then
        already_synced=$((already_synced + 1))
    else
        echo "$user"
        deduped_count=$((deduped_count + 1))
    fi
done < "$CLEAN_TXT.tmp" > "$CLEAN_TXT"

rm -f "$CLEAN_TXT.tmp" "$prior_usernames"

echo "active$BUCKET: $deduped_count after dedup ($already_synced already synced in a lower bucket)"

if [ "$deduped_count" -eq 0 ]; then
    echo "no usernames to sync for active$BUCKET"
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

aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    delete configmap "$CONFIGMAP" 2>/dev/null || true

aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    create configmap "$CONFIGMAP" --from-file=usernames.txt="$CLEAN_TXT"

echo "created ConfigMap $CONFIGMAP in $NAMESPACE ($deduped_count usernames, ${size} bytes)"
echo ""
echo "next:"
echo "  scripts/lfxv2_1507_run_batch.sh"
