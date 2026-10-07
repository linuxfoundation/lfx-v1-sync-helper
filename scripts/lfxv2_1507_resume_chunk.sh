#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Resume an interrupted chunk run from where it stopped.
#
# Collects the interrupted pod's full log from CloudWatch (via
# lfxv2_1507_monitor.sh), finds the highest "syncing user" index it reached,
# and writes the remaining usernames -- starting at that index, inclusive --
# to a new chunk file with the next letter suffix (chunk01.txt -> chunk01b.txt
# -> chunk01c.txt). It then deploys that file as the ConfigMap.
#
# The resume point is inclusive because the user at that index may not have
# finished; every sync operation is idempotent, so redoing it is safe. Taking
# the highest index rather than the last line also means a log that is short
# can only move the resume point earlier (redoing more users), never later.
#
# Usage:
#   scripts/lfxv2_1507_resume_chunk.sh <deployed-chunk-file> --pod <pod-name> [--since <hours>] [--dry-run]
#
# <deployed-chunk-file> must be the exact file the interrupted pod ran; its
# line count is checked against the "total" the pod logged.

set -eu

CHUNK="${1:?usage: $0 <deployed-chunk-file> --pod <pod-name> [--since <hours>] [--dry-run]}"
shift
POD_NAME=""
SINCE_HOURS=24
DRY_RUN=""

while [ $# -gt 0 ]; do
    case "$1" in
        --pod) POD_NAME="${2:?--pod requires a pod name}"; shift 2 ;;
        --since) SINCE_HOURS="${2:?--since requires hours}"; shift 2 ;;
        --dry-run) DRY_RUN="--dry-run"; shift ;;
        *) echo "error: unknown argument '$1'" >&2; exit 1 ;;
    esac
done

if [ -z "$POD_NAME" ]; then
    echo "error: --pod is required (the interrupted pod's name)" >&2
    exit 1
fi
if [ ! -s "$CHUNK" ]; then
    echo "error: $CHUNK not found or empty" >&2
    exit 1
fi

# chunk01.txt -> chunk01b.txt; chunk01b.txt -> chunk01c.txt.
base="${CHUNK%.txt}"
case "$base" in
    *[a-y])
        last=$(printf '%s' "$base" | tail -c 1)
        next=$(printf '%s' "$last" | tr 'a-y' 'b-z')
        NEXT="${base%?}${next}.txt"
        ;;
    *[0-9]) NEXT="${base}b.txt" ;;
    *) echo "error: cannot derive a resume name from $CHUNK" >&2; exit 1 ;;
esac
if [ -e "$NEXT" ]; then
    echo "error: $NEXT already exists; refusing to overwrite" >&2
    exit 1
fi

LOG="sync-users-batch-${POD_NAME}.log"
"$(dirname "$0")/lfxv2_1507_monitor.sh" --pod "$POD_NAME" --since "$SINCE_HOURS" --out "$LOG"

# Highest index reached, and the total the pod was running against.
# shellcheck disable=SC2046 # Splitting into two positional values is intended.
set -- $(grep '"syncing user"' "$LOG" | awk '
    match($0, /"index":[0-9]+/) { i = substr($0, RSTART + 8, RLENGTH - 8) + 0; if (i > max) max = i }
    match($0, /"total":[0-9]+/) { total = substr($0, RSTART + 8, RLENGTH - 8) + 0 }
    END { print max + 0, total + 0 }')
index="$1"
total="$2"
lines=$(wc -l < "$CHUNK" | tr -d ' ')

if grep -q '"batch user sync completed"' "$LOG"; then
    echo "error: $LOG contains a completion line; the run finished, nothing to resume" >&2
    exit 1
fi
if [ "$index" -eq 0 ]; then
    echo "error: no \"syncing user\" lines found in $LOG; redeploy $CHUNK unchanged" >&2
    exit 1
fi
if [ "$total" -ne "$lines" ]; then
    echo "error: pod logged total=$total but $CHUNK has $lines lines; wrong chunk file?" >&2
    exit 1
fi

tail -n "+$index" "$CHUNK" > "$NEXT"
remaining=$(wc -l < "$NEXT" | tr -d ' ')
echo "pod reached $index/$total; wrote $NEXT ($remaining users, resuming at index $index inclusive)"

"$(dirname "$0")/lfxv2_1507_deploy_chunk.sh" "$NEXT" $DRY_RUN
