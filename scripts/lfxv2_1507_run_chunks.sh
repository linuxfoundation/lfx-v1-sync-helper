#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Run chunk files back to back, starting each as soon as the
# previous Job finishes.
#
# For each chunk: deploys the ConfigMap, (re)creates the Job, waits for the
# Job to finish, then confirms in CloudWatch that the pod logged its
# "batch user sync completed" summary. A Job that ends without that line
# (e.g. evicted) stops the chain and prints the resume command.
#
# If a Job is already running when this starts, it is waited on first and
# the chain proceeds only if it completed.
#
# Run from the directory holding the chunk files and manifests/, and keep the
# machine awake for the duration (e.g. prefix with "caffeinate -i").
#
# Usage:
#   scripts/lfxv2_1507_run_chunks.sh [--poll <seconds>] <chunk-file>...

set -eu

POLL=300
if [ "${1:-}" = "--poll" ]; then
    POLL="${2:?--poll requires seconds}"
    shift 2
fi
[ "$#" -gt 0 ] || { echo "usage: $0 [--poll <seconds>] <chunk-file>..." >&2; exit 2; }

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
JOB_NAME="sync-users-batch"
LOG_GROUP="/aws/containerinsights/lfx-v2/application"

for f in "$@"; do
    [ -s "$f" ] || { echo "error: $f not found or empty" >&2; exit 1; }
done

kubectl_cmd() {
    aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" "$@"
}

aws_cmd() {
    aws-vault exec lfx-prod -s -- aws "$@"
}

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

job_pod() {
    kubectl_cmd get pods -l "job-name=$JOB_NAME" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

# Prints Complete, Failed, Running, or Missing.
job_state() {
    _c=$(kubectl_cmd get "job/$JOB_NAME" \
        -o jsonpath='{range .status.conditions[?(@.status=="True")]}{.type}{" "}{end}' 2>&1) || {
            case "$_c" in
                *NotFound*|*"not found"*) echo Missing ;;
                *) echo Unknown ;;
            esac
            return 0
        }
    case " $_c " in
        *" Complete "*) echo Complete ;;
        *" Failed "*) echo Failed ;;
        *) echo Running ;;
    esac
}

# Wait for the current Job to finish; prints its pod name.
wait_for_job() {
    _pod=""
    while :; do
        _state=$(job_state)
        [ -n "$_pod" ] || _pod=$(job_pod)
        case "$_state" in
            Complete|Failed|Missing) break ;;
        esac
        sleep "$POLL"
    done
    echo "$_pod"
}

# Succeeds if the pod logged its completion summary; prints the summary.
pod_completed() {
    _start=$(( $(date -u +%s) - 4 * 86400 ))
    _qid=$(aws_cmd logs start-query --log-group-name "$LOG_GROUP" \
        --start-time "$_start" --end-time "$(date -u +%s)" \
        --query-string "fields log
| filter kubernetes.pod_name = \"$1\"
| filter log like /batch user sync completed/
| limit 1" --output text --query queryId) || return 1
    _i=0
    while [ "$_i" -lt 60 ]; do
        sleep 5
        _r=$(aws_cmd logs get-query-results --query-id "$_qid" --output json 2>/dev/null) || continue
        case "$(echo "$_r" | jq -r .status)" in
            Complete)
                _line=$(echo "$_r" | jq -r '.results[0][]? | select(.field=="log") | .value')
                [ -n "$_line" ] || return 1
                echo "$_line"
                return 0 ;;
            Failed|Cancelled) return 1 ;;
        esac
        _i=$((_i + 1))
    done
    return 1
}

check_finished() {
    _chunk="$1"
    _pod="$2"
    if [ -z "$_pod" ]; then
        echo "$(ts) could not determine the pod for $_chunk; stopping" >&2
        exit 1
    fi
    # Give CloudWatch time to ingest the final lines.
    sleep 60
    if _summary=$(pod_completed "$_pod"); then
        echo "$(ts) $_chunk done ($_pod): $_summary"
    else
        echo "$(ts) $_chunk did NOT complete ($_pod); stopping. Resume with:" >&2
        echo "  $SCRIPTS/lfxv2_1507_resume_chunk.sh $_chunk --pod $_pod" >&2
        exit 1
    fi
}

if [ "$(job_state)" = "Running" ]; then
    _pod=$(job_pod)
    echo "$(ts) waiting for the running Job ($_pod) before starting the chain..."
    _pod=$(wait_for_job)
    check_finished "running Job" "$_pod"
fi

for chunk in "$@"; do
    echo "$(ts) starting $chunk"
    "$SCRIPTS/lfxv2_1507_deploy_chunk.sh" "$chunk"
    "$SCRIPTS/lfxv2_1507_run_batch.sh" >/dev/null
    sleep 30
    pod=$(wait_for_job)
    check_finished "$chunk" "$pod"
done

echo "$(ts) all chunks finished"
