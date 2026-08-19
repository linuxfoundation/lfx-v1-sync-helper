#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Sync Auth0 email from LDAP for PLATFORM_OUT_OF_SYNC_WITH_AUTH0
# users by triggering a fake login via LDAP REST Proxy temp password + ROPG.
#
# Matrixed drilldown cells:
#   PLATFORM_OUT_OF_SYNC_WITH_AUTH0 / PLATFORM_UNVERIFIED+NEWER_LDAP
#   PLATFORM_OUT_OF_SYNC_WITH_AUTH0 / PLATFORM_VERIFIED+NEWER_LDAP
#
# Resolves usernames via lfxv2_2662_resolve_usernames.sql, then for each user:
# sets a 10-second temp password in LDAP, then performs a resource-owner
# password grant against Auth0. The login triggers Auth0's LDAP database
# connection to re-sync the user's profile (including email) from LDAP.
#
# Uses columns: platform_username, auth0_email (current), ldap_email (target)
#
# Usage:
#   export AUTH0_DOMAIN="linuxfoundation.auth0.com"
#   export AUTH0_CLIENT_ID="..."       # "User Registration Pages" client
#   export AUTH0_CLIENT_SECRET="..."
#   export LDAP_PROXY_URL="https://ldap-rest-proxy-prod.int.linuxfoundation.org"
#   export LDAP_PROXY_TOKEN="..."      # M2M token with update:users scope
#   ./scripts/lfxv2_2662_apply_auth0_out_of_sync.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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
SLEEP_SECONDS=2

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

for var in AUTH0_DOMAIN AUTH0_CLIENT_ID AUTH0_CLIENT_SECRET LDAP_PROXY_URL LDAP_PROXY_TOKEN; do
    if [[ -z "${!var:-}" ]]; then
        echo "Error: $var must be set." >&2
        exit 1
    fi
done

# Generate a one-time temp password (reused across all users in this run).
TEMP_PASS=$(openssl rand -base64 24)

TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (batch_size=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s, dry_run=$DRY_RUN)"

COUNT=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    auth0_email="${auth0_email//\"/}"
    ldap_email="${ldap_email//\"/}"

    COUNT=$((COUNT + 1))

    # URL-encode the username (handles spaces, special chars).
    encoded_username=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$platform_username', safe=''))")

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username: $auth0_email -> $ldap_email (fake login)"
        continue
    fi

    echo "[$COUNT/$TOTAL] Syncing $platform_username: $auth0_email -> $ldap_email"

    # Step 1: Set temp password in LDAP (valid for ~10 seconds).
    HTTP_CODE=$(curl -s -o /tmp/ldap_temp_resp.json -w "%{http_code}" \
        -X PUT "${LDAP_PROXY_URL}/users/${encoded_username}/temp_password" \
        -H "Authorization: Bearer ${LDAP_PROXY_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"password\": \"${TEMP_PASS}\"}")

    if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
        echo "  ERROR: temp_password HTTP $HTTP_CODE for $platform_username" >&2
        cat /tmp/ldap_temp_resp.json >&2
        echo "" >&2
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # Step 2: Trigger ROPG login to force Auth0 to re-sync from LDAP.
    HTTP_CODE=$(curl -s -o /tmp/auth0_ropg_resp.json -w "%{http_code}" \
        -X POST "https://${AUTH0_DOMAIN}/oauth/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=http://auth0.com/oauth/grant-type/password-realm" \
        -d "realm=Username-Password-Authentication" \
        -d "client_id=${AUTH0_CLIENT_ID}" \
        -d "client_secret=${AUTH0_CLIENT_SECRET}" \
        -d "username=${platform_username}" \
        -d "password=${TEMP_PASS}")

    if [[ "$HTTP_CODE" != "200" ]]; then
        echo "  ERROR: ROPG HTTP $HTTP_CODE for $platform_username" >&2
        cat /tmp/auth0_ropg_resp.json >&2
        echo "" >&2
        ERRORS=$((ERRORS + 1))
    else
        echo "  OK: login succeeded, Auth0 should now reflect LDAP email"
    fi

    # Throttle every batch_size rows.
    if [[ $((COUNT % BATCH_SIZE)) -eq 0 ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: $COUNT processed, $ERRORS errors."
