#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Monitor a running batch sync Job and report a summary when
# it completes.
#
# Polls the Job status every 30s, tailing the last few log lines for
# progress. When the Job finishes (Completed or Failed), fetches the full
# log, extracts summary counters, and prints a report.
#
# Usage:
#   scripts/lfxv2_1507_monitor.sh [--save-log]
#
# With --save-log, the full log is written to sync-users-batch-<timestamp>.log.

set -eu

SAVE_LOG=false
[ "${1:-}" = "--save-log" ] && SAVE_LOG=true

CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
JOB_NAME="sync-users-batch"

kubectl_cmd() {
    aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" "$@"
}

echo "monitoring job/$JOB_NAME in $NAMESPACE..."
echo ""

# Poll until job completes.
while true; do
    # Check job completion via jsonpath on conditions. Kubernetes jobs set
    # condition type=Complete or type=Failed with status=True.
    status=""
    conditions=$(kubectl_cmd get job "$JOB_NAME" -o jsonpath='{range .status.conditions[*]}{.type}={.status}{"\n"}{end}' 2>/dev/null || echo "")
    case "$conditions" in
        *"Complete=True"*) status="Complete" ;;
        *"Failed=True"*)   status="Failed" ;;
    esac

    # Fallback: check succeeded/failed counts if conditions aren't set yet.
    if [ -z "$status" ]; then
        succeeded=$(kubectl_cmd get job "$JOB_NAME" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "")
        failed=$(kubectl_cmd get job "$JOB_NAME" -o jsonpath='{.status.failed}' 2>/dev/null || echo "")
        active=$(kubectl_cmd get job "$JOB_NAME" -o jsonpath='{.status.active}' 2>/dev/null || echo "")
        if [ "$succeeded" = "1" ]; then
            status="Complete"
        elif [ "$failed" = "1" ] && [ "$active" != "1" ]; then
            status="Failed"
        fi
    fi

    if [ "$status" = "Complete" ] || [ "$status" = "Failed" ]; then
        break
    fi

    # Show progress from the last "syncing user" log line.
    progress_line=$(kubectl_cmd logs "job/$JOB_NAME" --tail=50 2>/dev/null | grep '"syncing user"' | tail -1 || echo "")
    if [ -n "$progress_line" ]; then
        index=$(echo "$progress_line" | grep -o '"index":[0-9]*' | cut -d: -f2)
        total=$(echo "$progress_line" | grep -o '"total":[0-9]*' | cut -d: -f2)
        if [ -n "$index" ] && [ -n "$total" ]; then
            pct=$((index * 100 / total))
            printf "  progress: %s/%s (%d%%)\n" "$index" "$total" "$pct"
        fi
    fi

    sleep 30
done

echo ""
echo "job status: $status"
echo ""

# Fetch full log.
LOG=$(kubectl_cmd logs "job/$JOB_NAME" 2>&1)

if $SAVE_LOG; then
    logfile="sync-users-batch-$(date -u +%Y%m%dT%H%M%SZ).log"
    echo "$LOG" > "$logfile"
    echo "full log saved to $logfile"
fi

# Extract summary line.
summary=$(echo "$LOG" | grep '"batch user sync completed"' || echo "")
if [ -n "$summary" ]; then
    echo ""
    echo "=== summary ==="
    echo "$summary" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read())
print(f'  users processed:  {d.get(\"users_processed\", \"?\")}')
print(f'  users succeeded:  {d.get(\"users_succeeded\", \"?\")}')
print(f'  users failed:     {d.get(\"users_failed\", \"?\")}')
print(f'  emails linked:    {d.get(\"emails_linked\", \"?\")}')
print(f'  profiles synced:  {d.get(\"profiles_synced\", \"?\")}')
" 2>/dev/null || echo "  (could not parse summary line)"
    echo ""
fi

# Count specific outcomes from log lines.
failed_users=$(echo "$LOG" | grep -c '"user sync failed, continuing"' || echo "0")
linked=$(echo "$LOG" | grep -c '"linked email identity"\|"would link alternate email"' || echo "0")
profiles=$(echo "$LOG" | grep -c '"profile synced"\|"would sync profile"' || echo "0")
no_sfid=$(echo "$LOG" | grep -c '"no v1 SFID found"' || echo "0")

echo "=== log-based counts ==="
echo "  emails linked/would-link: $linked"
echo "  profiles synced/would-sync: $profiles"
echo "  users failed: $failed_users"
echo "  no v1 SFID: $no_sfid"

# List failed usernames if any.
if [ "$failed_users" -gt 0 ]; then
    echo ""
    echo "=== failed usernames ==="
    echo "$LOG" | grep '"user sync failed, continuing"' | grep -o '"username":"[^"]*"' | cut -d'"' -f4 | sort
fi
