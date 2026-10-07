#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Create the batch sync ConfigMap from a cohort chunk file.
#
# Takes a chunk file from lfxv2_1507_build_chunks.sh (or a resumed chunk from
# lfxv2_1507_resume_chunk.sh) and creates the sync-users-batch-list ConfigMap
# from it. The chunk is used as-is: it is already screened against
# LFXV2-2662 and unique within the cohort.
#
# Usage:
#   scripts/lfxv2_1507_deploy_chunk.sh <chunk-file> [--dry-run]

set -eu

CHUNK="${1:?usage: $0 <chunk-file> [--dry-run]}"
DRY_RUN="${2:-}"
CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
CONFIGMAP="sync-users-batch-list"
JOB_NAME="sync-users-batch"

if [ ! -s "$CHUNK" ]; then
    echo "error: $CHUNK not found or empty" >&2
    exit 1
fi

count=$(wc -l < "$CHUNK" | tr -d ' ')
size=$(wc -c < "$CHUNK" | tr -d ' ')
if [ "$size" -gt 1000000 ]; then
    echo "error: $CHUNK is ${size} bytes, approaching 1 MiB ConfigMap limit" >&2
    exit 1
fi

if [ "$DRY_RUN" = "--dry-run" ]; then
    echo "[dry-run] would create ConfigMap $CONFIGMAP in $NAMESPACE from $CHUNK ($count usernames, ${size} bytes)"
    exit 0
fi

# A running Job mounts this ConfigMap. Replacing it mid-run would feed a
# different chunk to the current pod, so refuse while one is active.
job_active=$(aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    get "job/$JOB_NAME" -o jsonpath='{.status.active}' 2>/dev/null || true)
if [ -n "$job_active" ] && [ "$job_active" != "0" ]; then
    echo "error: job/$JOB_NAME has ${job_active} active pod(s); a run is in progress." >&2
    exit 1
fi

aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    delete configmap "$CONFIGMAP" 2>/dev/null || true

aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    create configmap "$CONFIGMAP" --from-file=usernames.txt="$CHUNK"

echo "created ConfigMap $CONFIGMAP in $NAMESPACE from $CHUNK ($count usernames, ${size} bytes)"
echo ""
echo "next:"
echo "  scripts/lfxv2_1507_run_batch.sh"
