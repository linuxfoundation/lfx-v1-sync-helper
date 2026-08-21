#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Push Platform-verified primary email to Auth0 for
# WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED users.
#
# Matrixed drilldown cells:
#   WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED+NEWER_AUTH0
#   WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED+NEWER_LDAP
#   WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED+TS_UNKNOWN
#
# Resolves usernames via lfxv2_2662_resolve_usernames.sql, then PATCHes
# Auth0 Management API to set the Platform flagged primary as Auth0's email.
#
# Uses columns: platform_username, auth0_id, auth0_email (current),
#               flagged_primary_email (target)
#
# Usage:
#   export AUTH0_DOMAIN="linuxfoundation.auth0.com"
#   export AUTH0_MGMT_TOKEN="..."  # Management API token with update:users scope
#   ./scripts/lfxv2_2662_apply_wrong_primary_verified.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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
SKIPPED=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id flagged_email_other_contact_sfid meeting_count_other_sfid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    auth0_id="${auth0_id//\"/}"
    auth0_email="${auth0_email//\"/}"
    flagged_primary_email="${flagged_primary_email//\"/}"
    flagged_email_other_auth0_id="${flagged_email_other_auth0_id//\"/}"
    flagged_email_other_auth0_id="${flagged_email_other_auth0_id//[$'\r\n ']/}"
    flagged_email_other_ldap_uid="${flagged_email_other_ldap_uid//\"/}"
    flagged_email_other_ldap_uid="${flagged_email_other_ldap_uid//[$'\r\n ']/}"

    COUNT=$((COUNT + 1))

    # Conflict guard: the flagged primary email belongs to a DIFFERENT Auth0
    # or LDAP account. Auth0 validates email availability against
    # LDAP/Identity, so either ownership blocks the push. These need a
    # Platform-side fix or an account merge instead — skip.
    if [[ -n "$flagged_email_other_auth0_id" || -n "$flagged_email_other_ldap_uid" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: flagged email $flagged_primary_email owned by other account (auth0=$flagged_email_other_auth0_id ldap=$flagged_email_other_ldap_uid)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # No Auth0 account: nothing to push, and the conflict check cannot see
    # collisions without an Auth0 row. Needs manual investigation.
    if [[ -z "$auth0_id" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: no Auth0 account (LDAP/Drupal only) — manual review"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username ($auth0_id): $auth0_email -> $flagged_primary_email"
    else
        echo "[$COUNT/$TOTAL] Updating $platform_username ($auth0_id): $auth0_email -> $flagged_primary_email"
        HTTP_CODE=$(curl -s -o /tmp/auth0_resp.json -w "%{http_code}" \
            -X PATCH "https://${AUTH0_DOMAIN}/api/v2/users/${auth0_id}" \
            -H "Authorization: Bearer ${AUTH0_MGMT_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"email\": \"${flagged_primary_email}\", \"email_verified\": true}")

        if [[ "$HTTP_CODE" != "200" ]]; then
            echo "  ERROR: HTTP $HTTP_CODE for $platform_username" >&2
            cat /tmp/auth0_resp.json >&2
            echo "" >&2
            ERRORS=$((ERRORS + 1))
        fi
    fi

    # Throttle every batch_size rows.
    if [[ $((COUNT % BATCH_SIZE)) -eq 0 && "$DRY_RUN" != "true" ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: $COUNT processed, $SKIPPED skipped (conflicts), $ERRORS errors."
