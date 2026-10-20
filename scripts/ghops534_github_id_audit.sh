#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# lfx-self-serve-ops#534: audit (and optionally fix) v1 GithubID against
# Auth0's github identity, via Snowflake.
#
# Runs scripts/ghops534_github_id_audit.sql through snowsql to classify every
# platform user into NOT_SET, MISSING_IN_V1, DIFFERS, V1_ONLY, or OK, then (by
# default, dry-run) prints the PATCH that would bring MISSING_IN_V1 and
# DIFFERS rows into agreement. V1_ONLY rows are reported but never touched —
# they need manual review, not an automated fix. Pass --apply to actually
# PATCH the User Service, pausing --sleep seconds (default 2) after every
# --batch-size (default 25) PATCHes.
#
# Usage:
#   export SNOWFLAKE_ACCOUNT=""
#   export SNOWFLAKE_USER=""
#   export SNOWFLAKE_WAREHOUSE=""
#   export SNOWFLAKE_ROLE=""
#   export SNOWFLAKE_PRIVATE_KEY_PATH=""
#   export API_GATEWAY_URL=""  # trailing slash optional; normalized for both the Auth0 audience (needs one) and the PATCH URL (must not have one)
#   export AUTH0_DOMAIN=""
#   export USER_SERVICE_CLIENT_ID=""     # M2M client authorized for user-service
#   export USER_SERVICE_CLIENT_SECRET=""
#   ./scripts/ghops534_github_id_audit.sh [--apply] [--batch-size N] [--sleep S] [--csv path]
#
# By default a fresh CSV is generated via snowsql. Pass --csv to re-run
# against an already-generated CSV (e.g. to apply a reviewed subset) without
# re-querying Snowflake.

set -euo pipefail

APPLY=false
BATCH_SIZE=25
SLEEP_SECONDS=2
CSV_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --batch-size) BATCH_SIZE="$2"; shift 2 ;;
        --sleep) SLEEP_SECONDS="$2"; shift 2 ;;
        --csv) CSV_FILE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# Reject a missing/nonnumeric/nonpositive --batch-size up front: a zero value
# would otherwise reach the modulo check further down and abort mid-run with
# a division-by-zero after some PATCHes have already been applied.
if ! [[ "$BATCH_SIZE" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --batch-size must be a positive integer, got \"$BATCH_SIZE\"." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "$CSV_FILE" ]]; then
    for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_WAREHOUSE SNOWFLAKE_ROLE SNOWFLAKE_PRIVATE_KEY_PATH; do
        if [[ -z "${!var:-}" ]]; then
            echo "Error: $var must be set (or pass --csv <path> to reuse an existing audit)." >&2
            exit 1
        fi
    done

    CSV_FILE="ghops534_github_id_audit.csv"
    rm -f "$CSV_FILE"

    echo "Querying Snowflake..."
    snowsql --accountname "$SNOWFLAKE_ACCOUNT" --username "$SNOWFLAKE_USER" \
        --warehouse "$SNOWFLAKE_WAREHOUSE" --rolename "$SNOWFLAKE_ROLE" \
        --private-key-path "$SNOWFLAKE_PRIVATE_KEY_PATH" \
        -o friendly=false -o header=true -o timing=false \
        -o output_format=csv -o output_file="$CSV_FILE" \
        -f "$SCRIPT_DIR/ghops534_github_id_audit.sql"
fi

if [[ ! -f "$CSV_FILE" ]]; then
    echo "Error: CSV file not found: $CSV_FILE" >&2
    exit 1
fi

if [[ "$APPLY" == "true" ]]; then
    for var in API_GATEWAY_URL AUTH0_DOMAIN USER_SERVICE_CLIENT_ID USER_SERVICE_CLIENT_SECRET; do
        if [[ -z "${!var:-}" ]]; then
            echo "Error: $var must be set for --apply." >&2
            exit 1
        fi
    done

    # Auth0 registers the API's audience with a trailing slash
    # (https://api-gw.platform.linuxfoundation.org/); requesting a token
    # without it fails authorization. Normalize here so API_GATEWAY_URL can
    # be set either way and the PATCH URL below (which must NOT have a
    # trailing slash before /user-service/...) stays correct regardless.
    API_GATEWAY_BASE="${API_GATEWAY_URL%/}"
    API_GATEWAY_AUDIENCE="${API_GATEWAY_BASE}/"

    TOKEN_RESPONSE=$(curl -s -X POST "https://${AUTH0_DOMAIN}/oauth/token" \
        -H "Content-Type: application/json" \
        -d "{\"grant_type\":\"client_credentials\",\"client_id\":\"${USER_SERVICE_CLIENT_ID}\",\"client_secret\":\"${USER_SERVICE_CLIENT_SECRET}\",\"audience\":\"${API_GATEWAY_AUDIENCE}\"}")
    ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token', ''))")
    if [[ -z "$ACCESS_TOKEN" ]]; then
        echo "Error: failed to obtain M2M access token: $TOKEN_RESPONSE" >&2
        exit 1
    fi
fi

COUNTS_OK=0
COUNTS_NOT_SET=0
COUNTS_MISSING=0
COUNTS_DIFFERS=0
COUNTS_V1_ONLY=0
COUNTS_SKIPPED=0
COUNTS_FIXED=0
COUNTS_ERRORS=0
COUNT=0
ROWS_SEEN=0

CSV_READER_FIFO="$(mktemp -u)"
mkfifo "$CSV_READER_FIFO"

python3 -c "
import csv, signal, sys
# Restore default SIGPIPE handling so an early-terminated consumer (e.g. the
# bash loop exiting on Ctrl-C) just stops this process silently instead of
# raising a BrokenPipeError traceback.
signal.signal(signal.SIGPIPE, signal.SIG_DFL)
with open(sys.argv[1], newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader, None)  # header
    for row in reader:
        # Fields are free text (v1 GITHUB_ID__C in particular can contain
        # literal commas or newlines) and were quoted by snowsql's CSV
        # output accordingly. Re-emit delimited by ASCII Unit Separator
        # (0x1F) — not a comma/tab/space, so it's guaranteed not to appear
        # in a field and, unlike tab, bash's 'read' won't treat runs of it
        # as 'IFS whitespace' and collapse adjacent empty fields. Embedded
        # newlines/carriage returns are collapsed to a space since 'read'
        # still splits on those as record (line) separators.
        print('\x1f'.join(field.replace('\x1f', ' ').replace('\n', ' ').replace('\r', ' ') for field in row))
" "$CSV_FILE" > "$CSV_READER_FIFO" &
CSV_READER_PID=$!

while IFS=$'\x1f' read -r platform_username contact_sfid v1_github_id auth0_github_id auth0_identity_updated_at auth0_github_identity_count category; do
    ROWS_SEEN=$((ROWS_SEEN + 1))
    if [[ $((ROWS_SEEN % 100000)) -eq 0 ]]; then
        echo "  ... scanned $ROWS_SEEN rows (ok=$COUNTS_OK not_set=$COUNTS_NOT_SET missing_in_v1=$COUNTS_MISSING differs=$COUNTS_DIFFERS v1_only=$COUNTS_V1_ONLY)" >&2
    fi

    case "$category" in
        NOT_SET) COUNTS_NOT_SET=$((COUNTS_NOT_SET + 1)); continue ;;
        OK) COUNTS_OK=$((COUNTS_OK + 1)); continue ;;
        V1_ONLY) COUNTS_V1_ONLY=$((COUNTS_V1_ONLY + 1)); continue ;;
        MISSING_IN_V1) COUNTS_MISSING=$((COUNTS_MISSING + 1)) ;;
        DIFFERS) COUNTS_DIFFERS=$((COUNTS_DIFFERS + 1)) ;;
        *) echo "Skipping unrecognized category \"$category\" for $platform_username" >&2; COUNTS_SKIPPED=$((COUNTS_SKIPPED + 1)); continue ;;
    esac

    if [[ -z "$contact_sfid" || -z "$auth0_github_id" ]]; then
        echo "Skipping $platform_username: missing contact_sfid or auth0_github_id" >&2
        COUNTS_SKIPPED=$((COUNTS_SKIPPED + 1))
        continue
    fi

    COUNT=$((COUNT + 1))

    if [[ "$APPLY" != "true" ]]; then
        echo "[dry-run] $platform_username ($contact_sfid): $category, v1=\"$v1_github_id\" -> \"$auth0_github_id\""
        continue
    fi

    echo "[$COUNT] $category $platform_username ($contact_sfid): v1=\"$v1_github_id\" -> \"$auth0_github_id\""

    # Written as a bare username (no https://github.com/ prefix), even though
    # most existing v1 GITHUB_ID__C values are URL-formatted. This matches
    # the current canonical write path: the Auth0 "user_svc_event" Action
    # (auth0-terraform/src/actions/user_svc_event.js, githubNickname())
    # sends the bare Auth0 identity nickname as githubId on the
    # UserAuthenticated event, and lfx-pet-userservice's CreatePUserStruct
    # writes that value straight through with no URL wrapping. The
    # URL-formatted majority in the DB today comes from some other
    # (older/manual) write path, not this one.
    HTTP_CODE=$(curl -s -o /tmp/ghops534_patch_resp.json -w "%{http_code}" \
        -X PATCH "${API_GATEWAY_BASE}/user-service/v1/users/${contact_sfid}" \
        -H "Authorization: Bearer ${ACCESS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"GithubID\": \"${auth0_github_id}\"}")

    if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
        echo "  ERROR: PATCH HTTP $HTTP_CODE for $platform_username" >&2
        cat /tmp/ghops534_patch_resp.json >&2
        echo "" >&2
        COUNTS_ERRORS=$((COUNTS_ERRORS + 1))
    else
        COUNTS_FIXED=$((COUNTS_FIXED + 1))
    fi

    if [[ $((COUNT % BATCH_SIZE)) -eq 0 ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi
done < "$CSV_READER_FIFO"

# `wait` is the only way to observe the exit status of the background reader
# above; without this, set -e/pipefail never see a CSV-parsing crash (e.g. an
# invalid-UTF-8 file), and the loop would fall through to "Done" as if it had
# read every row.
CSV_READER_EXIT=0
wait "$CSV_READER_PID" || CSV_READER_EXIT=$?
rm -f "$CSV_READER_FIFO"
if [[ "$CSV_READER_EXIT" -ne 0 ]]; then
    echo "Error: CSV reader failed (exit $CSV_READER_EXIT); results above are incomplete." >&2
    exit 1
fi

echo ""
echo "Done: ok=$COUNTS_OK not_set=$COUNTS_NOT_SET missing_in_v1=$COUNTS_MISSING differs=$COUNTS_DIFFERS v1_only=$COUNTS_V1_ONLY skipped=$COUNTS_SKIPPED apply=$APPLY fixed=$COUNTS_FIXED errors=$COUNTS_ERRORS"
if [[ "$APPLY" != "true" && $((COUNTS_MISSING + COUNTS_DIFFERS)) -gt 0 ]]; then
    echo "Re-run with --apply to PATCH the $((COUNTS_MISSING + COUNTS_DIFFERS)) MISSING_IN_V1/DIFFERS users above."
fi
if [[ "$COUNTS_V1_ONLY" -gt 0 ]]; then
    echo "$COUNTS_V1_ONLY V1_ONLY users have a v1 GithubID with no corresponding Auth0 github identity; left for manual review."
fi

if [[ "$COUNTS_ERRORS" -gt 0 ]]; then
    echo "Error: $COUNTS_ERRORS PATCH request(s) failed; see ERROR lines above." >&2
    exit 1
fi
