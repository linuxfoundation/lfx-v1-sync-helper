#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# lfx-self-serve-ops#534: audit (and optionally fix) v1 GithubID against
# Auth0's github identity, via Snowflake.
#
# Runs scripts/ghops534_github_id_audit.sql through snowsql to classify every
# platform user into MISSING_IN_V1, DIFFERS, V1_ONLY, or OK, then (by
# default, dry-run) prints the PATCH that would bring MISSING_IN_V1 and
# DIFFERS rows into agreement. V1_ONLY rows are reported but never touched —
# they need manual review, not an automated fix. Pass --apply to actually
# PATCH the User Service.
#
# Usage:
#   export SNOWFLAKE_ACCOUNT="JNMHVWD-XPB85243"
#   export SNOWFLAKE_USER="DEV_ERIC"
#   export SNOWFLAKE_WAREHOUSE="VIEWER"
#   export SNOWFLAKE_ROLE="DATA_DEV"
#   export SNOWFLAKE_PRIVATE_KEY_PATH="rsa_key.p8"
#   export API_GATEWAY_URL="https://api-gw.platform.linuxfoundation.org"
#   export AUTH0_DOMAIN="linuxfoundation.auth0.com"
#   export USER_SERVICE_CLIENT_ID="..."     # M2M client authorized for user-service
#   export USER_SERVICE_CLIENT_SECRET="..."
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

    TOKEN_RESPONSE=$(curl -s -X POST "https://${AUTH0_DOMAIN}/oauth/token" \
        -H "Content-Type: application/json" \
        -d "{\"grant_type\":\"client_credentials\",\"client_id\":\"${USER_SERVICE_CLIENT_ID}\",\"client_secret\":\"${USER_SERVICE_CLIENT_SECRET}\",\"audience\":\"${API_GATEWAY_URL}\"}")
    ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
    if [[ -z "$ACCESS_TOKEN" ]]; then
        echo "Error: failed to obtain M2M access token: $TOKEN_RESPONSE" >&2
        exit 1
    fi
fi

COUNTS_OK=0
COUNTS_MISSING=0
COUNTS_DIFFERS=0
COUNTS_V1_ONLY=0
COUNTS_SKIPPED=0
COUNTS_FIXED=0
COUNTS_ERRORS=0
COUNT=0

while IFS=',' read -r platform_username contact_sfid v1_github_id auth0_github_id auth0_identity_updated_at category; do
    platform_username="${platform_username//\"/}"
    contact_sfid="${contact_sfid//\"/}"
    v1_github_id="${v1_github_id//\"/}"
    auth0_github_id="${auth0_github_id//\"/}"
    category="${category//\"/}"

    case "$category" in
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

    HTTP_CODE=$(curl -s -o /tmp/ghops534_patch_resp.json -w "%{http_code}" \
        -X PATCH "${API_GATEWAY_URL}/user-service/v1/users/${contact_sfid}" \
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

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: ok=$COUNTS_OK missing_in_v1=$COUNTS_MISSING differs=$COUNTS_DIFFERS v1_only=$COUNTS_V1_ONLY skipped=$COUNTS_SKIPPED apply=$APPLY fixed=$COUNTS_FIXED errors=$COUNTS_ERRORS"
if [[ "$APPLY" != "true" && $((COUNTS_MISSING + COUNTS_DIFFERS)) -gt 0 ]]; then
    echo "Re-run with --apply to PATCH the $((COUNTS_MISSING + COUNTS_DIFFERS)) MISSING_IN_V1/DIFFERS users above."
fi
if [[ "$COUNTS_V1_ONLY" -gt 0 ]]; then
    echo "$COUNTS_V1_ONLY V1_ONLY users have a v1 GithubID with no corresponding Auth0 github identity; left for manual review."
fi
