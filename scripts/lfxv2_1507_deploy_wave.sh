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
#   scripts/lfxv2_1507_deploy_wave.sh <wave> [--dry-run] [--force]
#
# Prerequisites:
#   - lfxv2_1507_wave<N>_categorized.csv must exist (run wave_usernames.sh
#     first).
#   - Prior waves' categorized CSVs (wave0, wave1, ...) should be present
#     for deduplication.
#   - aws-vault and kubectl context configured for prod.
#
# --force skips the resolved.csv freshness check. It does not skip the
# running-Job check, which guards against corrupting a run in progress.

set -eu

# Usernames are not all ASCII: at least one carries CJK characters. Dedup
# compares usernames byte-wise, so pin the collation to keep comparisons
# here consistent with any external verification of the same sets.
LC_ALL=C
export LC_ALL

WAVE="${1:?usage: $0 <wave> [--dry-run] [--force]}"
shift
DRY_RUN=""
FORCE=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="--dry-run" ;;
        --force) FORCE="--force" ;;
        *) echo "error: unknown option '$arg'" >&2; exit 1 ;;
    esac
done

CATEGORIZED="lfxv2_1507_wave${WAVE}_categorized.csv"
CLEAN_TXT="lfxv2_1507_wave${WAVE}_clean.txt"
CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
CONFIGMAP="sync-users-batch-list"
JOB_NAME="sync-users-batch"
RESOLVED_MAX_AGE_DAYS=14

if [ ! -f "$CATEGORIZED" ]; then
    echo "error: $CATEGORIZED not found; run scripts/lfxv2_1507_wave_usernames.sh $WAVE first" >&2
    exit 1
fi

# resolved.csv freshness. It is a per-batch triage artifact, not a census,
# and it goes stale silently: a wave 5 account tripped the de-facto-primary
# guard because the copy on disk predated its remediation. Two failure modes
# are checked, both fatal without --force.
if [ ! -f resolved.csv ]; then
    echo "error: resolved.csv not found, so $CATEGORIZED carries no LFXV2-2662" >&2
    echo "       exclusions. Regenerate it before deploying, or pass --force." >&2
    [ -n "$FORCE" ] || exit 1
else
    # The categorized CSV must be newer than the list it was screened against.
    if [ resolved.csv -nt "$CATEGORIZED" ]; then
        echo "error: resolved.csv is newer than $CATEGORIZED, so the wave was" >&2
        echo "       screened against an older flagged-account list." >&2
        echo "       Re-run scripts/lfxv2_1507_wave_usernames.sh $WAVE, or pass --force." >&2
        [ -n "$FORCE" ] || exit 1
    fi
    resolved_age_days=$(( ( $(date +%s) - $(stat -f %m resolved.csv) ) / 86400 ))
    if [ "$resolved_age_days" -gt "$RESOLVED_MAX_AGE_DAYS" ]; then
        echo "error: resolved.csv is ${resolved_age_days} days old (limit ${RESOLVED_MAX_AGE_DAYS})." >&2
        echo "       Regenerate it per LFXV2-2662_SCRIPTS.md, or pass --force." >&2
        [ -n "$FORCE" ] || exit 1
    fi
    echo "resolved.csv: ${resolved_age_days}d old, $(( $(wc -l < resolved.csv) - 1 )) rows"
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
    if grep -qxF "$lower_user" "$prior_usernames" 2>/dev/null; then
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

# A running Job mounts this ConfigMap. Replacing it mid-run would feed the
# next wave's usernames to the current pod, so refuse while one is active.
# --force does not override this.
job_active=$(aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    get "job/$JOB_NAME" -o jsonpath='{.status.active}' 2>/dev/null || true)
if [ -n "$job_active" ] && [ "$job_active" != "0" ]; then
    echo "error: job/$JOB_NAME has ${job_active} active pod(s); a run is in progress." >&2
    echo "       Wait for it to finish, then delete the Job and the ConfigMap" >&2
    echo "       in separate commands before deploying the next wave." >&2
    exit 1
fi

# Delete existing ConfigMap if present (ignore errors). The ConfigMap has no
# TTL, unlike the Job, so a finished run always leaves one behind.
aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    delete configmap "$CONFIGMAP" 2>/dev/null || true

aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    create configmap "$CONFIGMAP" --from-file=usernames.txt="$CLEAN_TXT"

echo "created ConfigMap $CONFIGMAP in $NAMESPACE ($deduped_count usernames, ${size} bytes)"
echo ""
echo "next:"
echo "  scripts/lfxv2_1507_run_batch.sh"
