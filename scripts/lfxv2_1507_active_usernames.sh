#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Active-user extraction from Auth0 CloudWatch logs.
#
# Parallel track to the foundation-slug waves (lfxv2_1507_wave_usernames.sh).
# Instead of selecting users by foundation engagement, this selects users by
# recent Auth0 login activity across the LFX-relevant client list, and splits
# them into ten disjoint one-week recency buckets over a 10-week scope:
#
#   active7  -> last login days  0-6   (week 1)
#   active14 -> last login days  7-13  (week 2)
#   ...
#   active70 -> last login days 63-69  (week 10)
#
# Each user appears in exactly one bucket (their most recent login wins), so
# the buckets are self-deduplicating and are intended to be run in ascending
# order, 7 first.
#
# Uniform 7-day windows are deliberate. Widening the later windows to 14 days
# was considered and rejected: doubling the width outpaces the activity decay,
# which made the day 14-27 bucket the largest of the set (~15.3k, larger than
# week 1) and pushed its estimated pod log past 7 MB -- close enough to the
# kubelet containerLogMaxSize of 10Mi that rotation could silently drop the
# earliest lines from `kubectl logs`. One week per bucket keeps the largest
# bucket at ~12.4k users / ~6 MB and caps any single job near 3.5h, which also
# limits how much progress is lost if a pod is evicted (backoffLimit is 0, so
# there is no retry and no resume).
#
# This track is independent of the slug waves and does not deduplicate against
# them; roughly 2.5k users overlap and are re-processed. All sync operations
# are idempotent, so that is a no-op beyond the extra runtime.
#
# CloudWatch Insights caps a result set at 10,000 rows, and LFX peaks at
# ~5,100 distinct users/day, so the scan is chunked one UTC day per query and
# merged locally. Whole-window queries would silently truncate.
#
# Only records with an `auth0|` user_id are collected -- these are the LDAP/
# database-backed accounts the backfill handles. Pure social/enterprise
# logins (google-oauth2|..., samlp|...) are out of scope.
#
# Outputs (current directory):
#   lfxv2_1507_active<N>_usernames.csv    username,last_active_day
#   lfxv2_1507_active<N>_categorized.csv  + lfxv2_2662_flag (if resolved.csv)
#
# Usage:
#   scripts/lfxv2_1507_active_usernames.sh [--days 70] [--concurrency 8]
#
# Requires: aws-vault profile "lfit", jq.

set -eu

DAYS=70
CONCURRENCY=8
LOG_GROUP="/aws/events/auth0-logging-production"
PROFILE="lfit"

while [ $# -gt 0 ]; do
    case "$1" in
        --days) DAYS="${2:?--days needs a value}"; shift 2 ;;
        --concurrency) CONCURRENCY="${2:?--concurrency needs a value}"; shift 2 ;;
        *) echo "error: unknown argument '$1'" >&2; exit 1 ;;
    esac
done

# Bucket upper bounds, in days: one per week across the 10-week scope. Bucket
# N holds users whose most recent login was in [N-7, N).
BUCKETS="7 14 21 28 35 42 49 56 63 70"

# 10 weeks is the agreed scope, and is also the widest the bucket boundaries
# above are defined for. The log group retention begins 2026-05-29.
if [ "$DAYS" -gt 70 ]; then
    echo "error: --days $DAYS exceeds the 10-week bucket scope (70)" >&2
    exit 1
fi

# BSD date uses -r for epoch input, GNU date uses -d @. Detect once.
if date -u -r 0 +%F >/dev/null 2>&1; then
    fmt_day() { date -u -r "$1" +%F; }
else
    fmt_day() { date -u -d "@$1" +%F; }
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Start of the current UTC day, so buckets land on clean day boundaries.
NOW=$(date -u +%s)
TODAY_START=$(( NOW - NOW % 86400 ))

# The client allow-list. Kept in sync with the LFX Auth0 application set;
# excludes Training/Education/JIRA/LFID-only clients, which are not LFX One
# surfaces.
CLIENTS='"LF IT Automation - Beta","LFX Project Control Center","LF My Profile","CB People","LF Registration","LF Join Form","LFX","LF DA","LF CLA","CB Funding","LF Security","LFX Meetings","LFX Developer Forum","LFX Chat","LFX CM","LFX Document Manager","LFX Drive","Crowd.dev","LFX One","LFX One Profile","LFX Changelog","LFX Insights"'

# The username is taken from detail.data.user_name, which carries the real
# LDAP uid.
#
# Do NOT derive the username by stripping the "auth0|" prefix off
# detail.data.user_id. That suffix is a sanitized identifier minted by the
# LDAP REST Proxy so that uids are within Auth0's spec. Where no sanitizing
# was needed it equals the uid, which makes the two look interchangeable, but
# any uid that did need sanitizing (for example one containing a space, or a
# UTF-8 symbol, which may be present from historical, less-conservative signup
# requirements) is replaced by an opaque hash. It has never been safe to treat
# as a username.
#
# type s=success login, ssa=success silent auth, sertft=success exchange
# refresh token.
QUERY=$(cat <<EOF
filter (detail.data.type="s" or detail.data.type="ssa" or detail.data.type="sertft")
| filter strcontains(detail.data.user_id, "auth0|")
| filter detail.data.client_name in [${CLIENTS}]
| stats count(*) as hits by detail.data.user_name as uname
| limit 10000
EOF
)

# Run one UTC day and write the bare usernames to $WORK/day-<date>.txt.
run_day() {
    day_start="$1"
    day_end=$(( day_start + 86400 ))
    day_label=$(fmt_day "$day_start")
    out="$WORK/day-$day_label.txt"

    qid=$(aws-vault exec "$PROFILE" -s -- aws logs start-query \
        --log-group-name "$LOG_GROUP" \
        --start-time "$day_start" --end-time "$day_end" \
        --query-string "$QUERY" \
        --output text --query queryId 2>&1) || true
    case "$qid" in
        *ERROR*|*error*|"")
            echo "  $day_label: START FAILED: $qid" >&2
            echo "FAILED" > "$WORK/fail-$day_label"
            return 0
            ;;
    esac

    # Poll. Days are ~100-200k matched records and settle well under 5 min.
    i=0
    while [ "$i" -lt 100 ]; do
        sleep 5
        i=$(( i + 1 ))
        res=$(aws-vault exec "$PROFILE" -s -- aws logs get-query-results \
            --query-id "$qid" 2>/dev/null) || continue
        status=$(printf '%s' "$res" | jq -r .status 2>/dev/null || echo "")
        case "$status" in
            Complete)
                printf '%s' "$res" \
                    | jq -r '.results[] | .[] | select(.field=="uname") | .value' \
                    | grep -v '^$' > "$out" || true
                n=$(wc -l < "$out" | tr -d ' ')
                # A day at exactly the cap means the result set was truncated.
                if [ "$n" -ge 10000 ]; then
                    echo "  $day_label: $n users (WARNING: hit 10000 row cap, truncated)" >&2
                    echo "TRUNCATED" > "$WORK/fail-$day_label"
                else
                    echo "  $day_label: $n users"
                fi
                return 0
                ;;
            Failed|Cancelled|Timeout)
                echo "  $day_label: query $status" >&2
                echo "$status" > "$WORK/fail-$day_label"
                return 0
                ;;
        esac
    done
    echo "  $day_label: poll timed out" >&2
    echo "POLLTIMEOUT" > "$WORK/fail-$day_label"
}

# Warm the aws-vault session in the foreground first. The per-day queries run
# as background subshells, which cannot reach the terminal to prompt for MFA,
# so an unwarmed session deadlocks the whole run instead of prompting.
echo "warming aws-vault session for profile $PROFILE (may prompt for MFA)..."
if ! aws-vault exec "$PROFILE" -s -- aws sts get-caller-identity >/dev/null; then
    echo "error: could not establish an aws-vault session for '$PROFILE'" >&2
    exit 1
fi

echo "scanning $DAYS days of $LOG_GROUP (concurrency $CONCURRENCY)"

running=0
d=0
while [ "$d" -lt "$DAYS" ]; do
    day_start=$(( TODAY_START - d * 86400 ))
    run_day "$day_start" &
    running=$(( running + 1 ))
    if [ "$running" -ge "$CONCURRENCY" ]; then
        wait
        running=0
    fi
    d=$(( d + 1 ))
done
wait

# Any failed or truncated day would silently under-report a bucket, which
# would look like a successful run with fewer users. Refuse to continue.
if ls "$WORK"/fail-* >/dev/null 2>&1; then
    echo "" >&2
    echo "error: $(ls "$WORK"/fail-* | wc -l | tr -d ' ') day(s) failed or truncated; refusing to emit partial buckets" >&2
    for f in "$WORK"/fail-*; do
        echo "  $(basename "$f" | gsed 's/^fail-//'): $(cat "$f")" >&2
    done
    exit 1
fi

# Merge newest day first: the first time a username is seen is its most
# recent login, which determines its bucket. Case is preserved from the log
# (Auth0 usernames are case-sensitive) but dedup is case-insensitive.
echo ""
echo "merging..."

for bucket in $BUCKETS; do
    : > "$WORK/bucket-$bucket.csv"
done

d=0
while [ "$d" -lt "$DAYS" ]; do
    day_start=$(( TODAY_START - d * 86400 ))
    day_label=$(fmt_day "$day_start")
    f="$WORK/day-$day_label.txt"
    [ -f "$f" ] || { d=$(( d + 1 )); continue; }

    # Uniform one-week windows: day 0-6 -> 7, day 7-13 -> 14, and so on.
    bucket=$(( ( d / 7 + 1 ) * 7 ))

    awk -v day="$day_label" -v seen="$WORK/seen.txt" -v out="$WORK/bucket-$bucket.csv" '
    BEGIN {
        while ((getline line < seen) > 0) already[line] = 1
        close(seen)
    }
    {
        key = tolower($0)
        if (key in already) next
        already[key] = 1
        print $0 "," day >> out
        print key >> seen
    }
    ' "$f"

    d=$(( d + 1 ))
done

# Usernames are arbitrary LDAP uids and may contain spaces or UTF-8 symbols.
# A comma would silently corrupt the downstream CSVs, which categorize.sh and
# deploy_active.sh both parse with -F, and read as $1.
if grep -l ',.*,' "$WORK"/bucket-*.csv >/dev/null 2>&1; then
    echo "error: a username contains a comma; downstream CSV parsing would break" >&2
    grep -h ',.*,' "$WORK"/bucket-*.csv | head >&2
    exit 1
fi

for bucket in $BUCKETS; do
    out="lfxv2_1507_active${bucket}_usernames.csv"
    { echo "PLATFORM_USERNAME,LAST_ACTIVE_DAY"; cat "$WORK/bucket-$bucket.csv"; } > "$out"
    echo "wrote $out ($(wc -l < "$WORK/bucket-$bucket.csv" | tr -d ' ') usernames)"

    if [ -f resolved.csv ]; then
        "$(dirname "$0")/lfxv2_1507_categorize.sh" "$out" resolved.csv \
            > "lfxv2_1507_active${bucket}_categorized.csv"
        echo "wrote lfxv2_1507_active${bucket}_categorized.csv"
        awk -F, '{print $NF}' "lfxv2_1507_active${bucket}_categorized.csv" \
            | tail -n +2 | sort | uniq -c
    fi
done

if [ ! -f resolved.csv ]; then
    echo "resolved.csv not found; skipping LFXV2-2662 categorization" >&2
    exit 0
fi

echo ""
echo "next:"
echo "  scripts/lfxv2_1507_deploy_active.sh 7"
