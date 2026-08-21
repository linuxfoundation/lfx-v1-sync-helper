#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Delete any existing batch sync Job and create a fresh one.
#
# Assumes the sync-users-batch-list ConfigMap is already created (via
# lfxv2_1507_deploy_wave.sh). Deletes the prior Job if present, then
# applies the manifest.
#
# Usage:
#   scripts/lfxv2_1507_run_batch.sh [manifest]
#
# Default manifest: manifests/sync-users-batch-prod.yaml

set -eu

MANIFEST="${1:-manifests/sync-users-batch-prod.yaml}"
CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
JOB_NAME="sync-users-batch"

if [ ! -f "$MANIFEST" ]; then
    echo "error: manifest not found: $MANIFEST" >&2
    exit 1
fi

# Verify the ConfigMap exists.
if ! aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    get configmap sync-users-batch-list >/dev/null 2>&1; then
    echo "error: ConfigMap sync-users-batch-list not found; run scripts/lfxv2_1507_deploy_wave.sh first" >&2
    exit 1
fi

# Delete existing Job if present (ignore errors).
aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    delete "job/$JOB_NAME" 2>/dev/null && echo "deleted existing job/$JOB_NAME" || true

# Apply the manifest.
aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" \
    apply -f "$MANIFEST"

echo ""
echo "monitor with:"
echo "  scripts/lfxv2_1507_monitor.sh --save-log"
