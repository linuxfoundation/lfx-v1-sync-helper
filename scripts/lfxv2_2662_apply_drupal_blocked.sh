#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Sync Drupal block status to Auth0 for ALIGNED / DRUPAL_BLOCKED
# users by setting blocked=true via the Auth0 Management API.
#
# Matrixed drilldown cell:
#   ALIGNED / DRUPAL_BLOCKED
#
# These users are blocked in Drupal but not in Auth0. Blocking them in Auth0
# aligns the block state across systems so the v1-sync-helper no-op guard
# (PR #136) treats them consistently.
#
# For each user, the script first GETs the current blocked state and skips
# users that are already blocked (idempotent re-runs), then PATCHes
# {"blocked": true}.
#
# Uses columns: platform_username, auth0_id
#
# Usage:
#   export AUTH0_DOMAIN="linuxfoundation.auth0.com"
#   export AUTH0_MGMT_TOKEN="..."  # Management API token with read:users and update:users scopes
#   ./scripts/lfxv2_2662_apply_drupal_blocked.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
#
# Generate CSV:
#   rm -f resolved.csv && \
#   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
#     --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
#     -o friendly=false -o header=true -o timing=false \
#     -o output_format=csv -o output_file=resolved.csv \
#     -o variable_substitution=true \
#     -D USERNAMES="user1,user2,user3" \
#     -f scripts/lfxv2_2662_resolve_usernames.sql
#
# CSV columns (from resolve_usernames.sql):
#   platform_username,contact_sfid,auth0_id,auth0_email,ldap_email,
#   flagged_primary_email,flagged_email_sfid,matching_email_sfid

set -euo pipefail

DRY_RUN=false
BATCH_SIZE=10
SLEEP_SECONDS=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --batch-size) BATCH_SIZE="$2"; shift 2 ;;
        --sleep) SLEEP_SECONDS="$2"; shift 2 ;;
        *) CSV_FILE="$1"; shift ;;
    esac
done

if [[ -z "${CSV_FILE:-}" ]]; then
    echo "Usage: $0 [--dry-run] [--batch-size N] [--sleep S] <csv_file>" >&2
    exit 1
fi

if [[ ! -f "$CSV_FILE" ]]; then
    echo "Error: CSV file not found: $CSV_FILE" >&2
    exit 1
fi

if [[ -z "${AUTH0_DOMAIN:-}" || -z "${AUTH0_MGMT_TOKEN:-}" ]]; then
    echo "Error: AUTH0_DOMAIN and AUTH0_MGMT_TOKEN must be set." >&2
    exit 1
fi

TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (batch_size=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s, dry_run=$DRY_RUN)"

COUNT=0
BLOCKED=0
SKIPPED=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    auth0_id="${auth0_id//\"/}"

    COUNT=$((COUNT + 1))

    if [[ -z "$auth0_id" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: no auth0_id resolved" >&2
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # Check current blocked state so re-runs are idempotent.
    HTTP_CODE=$(curl -s -o /tmp/auth0_get_resp.json -w "%{http_code}" \
        -X GET "https://${AUTH0_DOMAIN}/api/v2/users/${auth0_id}?fields=blocked" \
        -H "Authorization: Bearer ${AUTH0_MGMT_TOKEN}")

    if [[ "$HTTP_CODE" != "200" ]]; then
        echo "[$COUNT/$TOTAL] ERROR: GET HTTP $HTTP_CODE for $platform_username ($auth0_id)" >&2
        cat /tmp/auth0_get_resp.json >&2
        echo "" >&2
        ERRORS=$((ERRORS + 1))
        continue
    fi

    if jq -e '.blocked == true' /tmp/auth0_get_resp.json > /dev/null; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username ($auth0_id): already blocked"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username ($auth0_id): would set blocked=true"
    else
        echo "[$COUNT/$TOTAL] Blocking $platform_username ($auth0_id)"
        HTTP_CODE=$(curl -s -o /tmp/auth0_resp.json -w "%{http_code}" \
            -X PATCH "https://${AUTH0_DOMAIN}/api/v2/users/${auth0_id}" \
            -H "Authorization: Bearer ${AUTH0_MGMT_TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"blocked": true}')

        if [[ "$HTTP_CODE" != "200" ]]; then
            echo "  ERROR: PATCH HTTP $HTTP_CODE for $platform_username" >&2
            cat /tmp/auth0_resp.json >&2
            echo "" >&2
            ERRORS=$((ERRORS + 1))
        else
            BLOCKED=$((BLOCKED + 1))
        fi
    fi

    # Throttle every batch_size rows.
    if [[ $((COUNT % BATCH_SIZE)) -eq 0 && "$DRY_RUN" != "true" ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: $COUNT processed, $BLOCKED blocked, $SKIPPED already blocked, $ERRORS errors."
