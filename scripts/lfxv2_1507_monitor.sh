#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Monitor a running batch sync Job and report a summary when
# it completes.
#
# Polls the Job status every 30s, tailing the last few log lines for
# progress. When the Job finishes (Completed or Failed), fetches the full
# log from CloudWatch Logs, extracts summary counters, and prints a report.
#
# The full log is always fetched from CloudWatch rather than "kubectl logs".
# The kubelet rotates container logs at 10Mi, so "kubectl logs" silently
# truncates any run above roughly 13k users; CloudWatch retains the whole
# stream. The saved log is verified against the event count CloudWatch
# reports for the stream, and a loud warning is printed on any mismatch.
#
# Usage:
#   scripts/lfxv2_1507_monitor.sh [--save-log]
#   scripts/lfxv2_1507_monitor.sh --pod <pod-name> [--since <hours>]
#
# With --save-log, the full log is written to sync-users-batch-<timestamp>.log.
# With --pod, polling is skipped and the log is collected straight from
# CloudWatch. This works after the Job's ttlSecondsAfterFinished has removed
# it, so a finished run can still be collected hours later.

set -eu

SAVE_LOG=false
POD_NAME=""
SINCE_HOURS=24

while [ $# -gt 0 ]; do
    case "$1" in
        --save-log) SAVE_LOG=true; shift ;;
        --pod)      POD_NAME="${2:?--pod requires a pod name}"; SAVE_LOG=true; shift 2 ;;
        --since)    SINCE_HOURS="${2:?--since requires hours}"; shift 2 ;;
        *) echo "usage: $0 [--save-log] | --pod <pod-name> [--since <hours>]" >&2; exit 2 ;;
    esac
done

CONTEXT="prod-lfx-v2"
NAMESPACE="v1-sync-helper"
JOB_NAME="sync-users-batch"
LOG_GROUP="/aws/containerinsights/lfx-v2/application"

kubectl_cmd() {
    aws-vault exec lfx-prod -s -- kubectl --context="$CONTEXT" -n "$NAMESPACE" "$@"
}

aws_cmd() {
    aws-vault exec lfx-prod -s -- aws "$@"
}

# Run a CloudWatch Logs Insights query and leave the raw results in $1.
run_insights_query() {
    _out="$1"
    _query="$2"
    _start=$(( $(date -u +%s) - SINCE_HOURS * 3600 ))
    _qid=$(aws_cmd logs start-query \
        --log-group-name "$LOG_GROUP" \
        --start-time "$_start" \
        --end-time "$(date -u +%s)" \
        --query-string "$_query" \
        --output text --query queryId 2>&1) || {
            echo "  start-query failed: $_qid" >&2; return 1; }
    _i=0
    while [ "$_i" -lt 120 ]; do
        sleep 5
        aws_cmd logs get-query-results --query-id "$_qid" > "$_out" 2>/dev/null || true
        _status=$(python3 -c "
import json,sys
try: print(json.load(open('$_out'),strict=False)['status'])
except Exception: print('Pending')" 2>/dev/null || echo Pending)
        [ "$_status" = "Complete" ] && return 0
        [ "$_status" = "Failed" ] || [ "$_status" = "Cancelled" ] && {
            echo "  query $_status" >&2; return 1; }
        _i=$((_i + 1))
    done
    echo "  query timed out" >&2
    return 1
}

# Resolve the pod for the Job, preferring the newer batch.kubernetes.io label.
resolve_pod_name() {
    _p=$(kubectl_cmd get pods -l "batch.kubernetes.io/job-name=$JOB_NAME" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    [ -n "$_p" ] || _p=$(kubectl_cmd get pods -l "job-name=$JOB_NAME" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    echo "$_p"
}

# Fetch every event for a pod's log stream from CloudWatch into $2.
#
# Termination is on the forward token ceasing to advance. get-log-events can
# return an empty page while more data remains, so breaking on an empty page
# truncates the log -- that mistake cost a 33% short log on wave 3b.
fetch_cloudwatch_log() {
    _pod="$1"
    _dest="$2"

    echo "  locating log stream for $_pod..."
    run_insights_query /tmp/lfxv2_1507_stream.json "fields @logStream
| filter kubernetes.pod_name = \"$_pod\"
| stats count(*) as n by @logStream
| limit 10" || return 1

    _streams=$(python3 -c "
import json
d=json.load(open('/tmp/lfxv2_1507_stream.json'),strict=False)
for r in d['results']:
    f={c['field']:c.get('value','') for c in r}
    print(f\"{f.get('n','0')}\t{f.get('@logStream','')}\")")

    if [ -z "$_streams" ]; then
        echo "  no CloudWatch stream found for pod $_pod (try a larger --since)" >&2
        return 1
    fi

    _expected=$(echo "$_streams" | awk -F'\t' '{s+=$1} END{print s+0}')
    _nstreams=$(echo "$_streams" | wc -l | tr -d ' ')
    echo "  streams: $_nstreams, events reported: $_expected"

    : > /tmp/lfxv2_1507_events.jsonl
    echo "$_streams" | while IFS="$(printf '\t')" read -r _n _stream; do
        [ -n "$_stream" ] || continue
        _token=""
        _page=0
        while [ "$_page" -lt 500 ]; do
            if [ -z "$_token" ]; then
                aws_cmd logs get-log-events --log-group-name "$LOG_GROUP" \
                    --log-stream-name "$_stream" --start-from-head \
                    --limit 10000 --output json > /tmp/lfxv2_1507_page.json 2>/dev/null || break
            else
                aws_cmd logs get-log-events --log-group-name "$LOG_GROUP" \
                    --log-stream-name "$_stream" --start-from-head \
                    --limit 10000 --next-token "$_token" \
                    --output json > /tmp/lfxv2_1507_page.json 2>/dev/null || break
            fi
            _next=$(python3 -c "
import json
d=json.load(open('/tmp/lfxv2_1507_page.json'),strict=False)
ev=d.get('events',[])
with open('/tmp/lfxv2_1507_events.jsonl','a') as fh:
    for e in ev: fh.write(json.dumps(e)+chr(10))
print(d.get('nextForwardToken',''))")
            # Only the token going stable means end of stream.
            [ "$_next" = "$_token" ] && break
            _token="$_next"
            _page=$((_page + 1))
        done
    done

    python3 - "$_dest" "$_expected" <<'PY'
import json, sys
dest, expected = sys.argv[1], int(sys.argv[2])
seen, out = set(), []
raw = 0
with open('/tmp/lfxv2_1507_events.jsonl') as fh:
    for line in fh:
        raw += 1
        e = json.loads(line)
        k = (e['timestamp'], e['message'])
        if k in seen:
            continue
        seen.add(k)
        try:
            msg = json.loads(e['message'], strict=False).get('log', '').rstrip()
        except Exception:
            msg = e['message'].rstrip()
        out.append((e['timestamp'], msg))
out.sort(key=lambda x: x[0])
with open(dest, 'w') as fh:
    fh.write('\n'.join(m for _, m in out) + '\n')
print(f"  fetched {raw} raw, {len(out)} after dedup (expected {expected})")
if len(out) < expected:
    print(f"  *** WARNING: log is short by {expected - len(out)} events -- DO NOT trust counts ***")
PY
}

# Poll until the Job completes, unless collecting a named pod directly.
if [ -z "$POD_NAME" ]; then
    echo "monitoring job/$JOB_NAME in $NAMESPACE..."
    echo ""

    POD_NAME=$(resolve_pod_name)
    [ -n "$POD_NAME" ] && echo "  pod: $POD_NAME" && echo ""

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

        # Capture the pod name as soon as it exists, so the log can still be
        # collected after ttlSecondsAfterFinished removes the Job.
        [ -n "$POD_NAME" ] || POD_NAME=$(resolve_pod_name)

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
    # A "Failed" Job is expected whenever any user failed: the batch command
    # exits 1 after printing its summary. Check the summary, not this status.
    [ "$status" = "Failed" ] && \
        echo "  (exit 1 is expected when users_failed > 0 -- see summary below)"
    echo ""
fi

if [ -z "$POD_NAME" ]; then
    echo "could not determine pod name; re-run with --pod <pod-name>" >&2
    exit 1
fi

logfile="sync-users-batch-$(date -u +%Y%m%dT%H%M%SZ).log"
echo "collecting full log from CloudWatch..."
if ! fetch_cloudwatch_log "$POD_NAME" "$logfile"; then
    echo "  CloudWatch collection failed; falling back to kubectl logs (MAY BE TRUNCATED)" >&2
    kubectl_cmd logs "job/$JOB_NAME" > "$logfile" 2>&1 || true
fi
LOG=$(cat "$logfile")

if $SAVE_LOG; then
    echo "full log saved to $logfile"
else
    echo "full log in $logfile (pass --save-log to keep it deliberately)"
fi

# Extract summary line.
summary=$(echo "$LOG" | grep '"batch user sync completed"' | tail -1 || true)
if [ -n "$summary" ]; then
    echo ""
    echo "=== summary ==="
    echo "$summary" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read())
print(f'  users processed:  {d.get(\"users_processed\", \"?\")}')
print(f'  users succeeded:  {d.get(\"users_succeeded\", \"?\")}')
print(f'  users failed:     {d.get(\"users_failed\", \"?\")}')
" 2>/dev/null || echo "  (could not parse summary line)"
    # emails_linked and profiles_synced are always 0 in --sync-users-file
    # mode, so they are derived from the individual log lines below instead.
    echo ""
else
    echo ""
    echo "  *** no completion line found -- run may be incomplete or log truncated ***"
    echo ""
fi

# Count specific outcomes from log lines.
failures=$(echo "$LOG" | grep '"user sync failed, continuing"' || true)
failed_users=$(echo "$LOG" | grep -c '"user sync failed, continuing"' || true)
linked=$(echo "$LOG" | grep -c '"linked email identity to Auth0 user"\|"would link alternate email"' || true)
profiles=$(echo "$LOG" | grep -c '"profile synced"\|"would sync profile"' || true)
blocked=$(echo "$LOG" | grep -c '"Auth0 user is blocked' || true)

# Taxonomy is counted from the failure lines only, so unrelated lines that
# happen to mention a 404 are not miscounted. The patterns are deliberately
# unanchored substrings: the error text continues past the phrase (e.g.
# "no v1 SFID found for username \"x\""), so a quoted pattern never matches.
no_sfid=$(echo "$failures" | grep -c 'no v1 SFID' || true)
not_found=$(echo "$failures" | grep -c '404 Not Found' || true)

echo "=== log-based counts ==="
echo "  emails linked/would-link:   $linked"
echo "  profiles synced/would-sync: $profiles"
echo "  users failed:               $failed_users"
echo "  blocked (skipped):          $blocked"

echo ""
echo "=== failure taxonomy ==="
echo "  no v1 SFID (LFID-only, no-op):        $no_sfid"
echo "  Auth0 404 (legacy LDAP-only, no-op):  $not_found"
known=$((no_sfid + not_found))
if [ "$failed_users" -gt "$known" ]; then
    echo "  *** $((failed_users - known)) failure(s) match neither known mode -- investigate ***"
    echo "$failures" | grep -v 'no v1 SFID' | grep -v '404 Not Found' | head -10
fi
