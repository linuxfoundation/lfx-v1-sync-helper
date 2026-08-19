#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Update LDAP email via LDAP REST Proxy for
# PLATFORM_OUT_OF_SYNC_WITH_LDAP users.
#
# Matrixed drilldown cells:
#   PLATFORM_OUT_OF_SYNC_WITH_LDAP / PLATFORM_VERIFIED+NEWER_LDAP
#   PLATFORM_OUT_OF_SYNC_WITH_LDAP / PLATFORM_VERIFIED+NEWER_AUTH0
#   PLATFORM_OUT_OF_SYNC_WITH_LDAP / PLATFORM_UNVERIFIED+NEWER_LDAP
#   PLATFORM_OUT_OF_SYNC_WITH_LDAP / MANGLED_LDAP+PLATFORM_VERIFIED+NEWER_LDAP
#
# Resolves usernames via lfxv2_2662_resolve_usernames.sql, then updates each
# user's LDAP mail attribute via PUT /users/:name/email on the LDAP REST Proxy.
# Target email is auth0_email (Platform+Auth0 agree, LDAP is stale).
#
# Uses columns: platform_username, ldap_email (current), auth0_email (target)
#
# Usage:
#   export LDAP_PROXY_URL="https://ldap-rest-proxy-prod.int.linuxfoundation.org"
#   export LDAP_PROXY_TOKEN="..."  # M2M token with update:users scope
#   ./scripts/lfxv2_2662_apply_ldap_out_of_sync.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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

if [[ -z "${LDAP_PROXY_URL:-}" || -z "${LDAP_PROXY_TOKEN:-}" ]]; then
    echo "Error: LDAP_PROXY_URL and LDAP_PROXY_TOKEN must be set." >&2
    exit 1
fi

TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (batch_size=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s, dry_run=$DRY_RUN)"

COUNT=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    auth0_email="${auth0_email//\"/}"
    ldap_email="${ldap_email//\"/}"

    COUNT=$((COUNT + 1))

    # URL-encode the username (handles spaces, special chars).
    encoded_username=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$platform_username', safe=''))")

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username: $ldap_email -> $auth0_email"
    else
        echo "[$COUNT/$TOTAL] Updating $platform_username: $ldap_email -> $auth0_email"
        HTTP_CODE=$(curl -s -o /tmp/ldap_proxy_resp.json -w "%{http_code}" \
            -X PUT "${LDAP_PROXY_URL}/users/${encoded_username}/email" \
            -H "Authorization: Bearer ${LDAP_PROXY_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"email\": \"${auth0_email}\"}")

        if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
            echo "  ERROR: HTTP $HTTP_CODE for $platform_username" >&2
            cat /tmp/ldap_proxy_resp.json >&2
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
echo "Done: $COUNT processed, $ERRORS errors."
