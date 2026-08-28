#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Delete LDAP entries for MISSING_FROM_AUTH0 / DRUPAL_BLOCKED
# users via DELETE /users/:name on the LDAP REST Proxy.
#
# Matrixed drilldown cells:
#   MISSING_FROM_AUTH0 / DRUPAL_BLOCKED
#   MISSING_FROM_AUTH0 / DRUPAL_BLOCKED+MANGLED_LDAP
#   MISSING_FROM_AUTH0 / DRUPAL_BLOCKED+MANGLED_PLATFORM+MANGLED_LDAP
#
# These are incomplete deletions: the user is already gone from Auth0 and
# blocked in Drupal, but the LDAP entry remains. Deleting the LDAP entry
# completes the deletion (the proxy auto-tombstones verified users, with a
# blank email for blocked users). A 404 from the proxy is treated as already
# deleted (idempotent re-runs).
#
# After a successful LDAP delete (or 404), the Platform DB username__c is
# blanked for the contact, since the username no longer exists in LDAP.
#
# Uses columns: platform_username, contact_sfid
#
# Usage:
#   export LDAP_PROXY_URL="https://ldap-rest-proxy-prod.int.linuxfoundation.org"
#   export LDAP_PROXY_TOKEN="..."  # M2M token with delete:users scope
#   ./scripts/lfxv2_2662_apply_ldap_delete.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
#
# Requires:
#   - PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE set, or passed via psql args
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
#   (variable_substitution must be enabled for "-D" to work)
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
DELETED=0
NOT_FOUND=0
BLANKED=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id flagged_email_other_contact_sfid meeting_count_other_sfid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    contact_sfid="${contact_sfid//\"/}"
    ldap_email="${ldap_email//\"/}"

    COUNT=$((COUNT + 1))

    # URL-encode the username (handles spaces, special chars).
    encoded_username=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$platform_username', safe=''))")

    # Guard on both sfid and current username to avoid blanking the wrong row.
    BLANK_SQL="UPDATE salesforce.merged_user SET username__c = NULL WHERE sfid = '${contact_sfid}' AND username__c = '${platform_username}';"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username (ldap_email=$ldap_email): would DELETE /users/$encoded_username and blank username__c on $contact_sfid"
        continue
    fi

    echo "[$COUNT/$TOTAL] Deleting $platform_username from LDAP"
    HTTP_CODE=$(curl -s -o /tmp/ldap_delete_resp.json -w "%{http_code}" \
        -X DELETE "${LDAP_PROXY_URL}/users/${encoded_username}" \
        -H "Authorization: Bearer ${LDAP_PROXY_TOKEN}")

    case "$HTTP_CODE" in
        204)
            echo "  OK: deleted"
            DELETED=$((DELETED + 1))
            ;;
        404)
            echo "  SKIP: not found in LDAP (already deleted)"
            NOT_FOUND=$((NOT_FOUND + 1))
            ;;
        *)
            echo "  ERROR: DELETE HTTP $HTTP_CODE for $platform_username" >&2
            cat /tmp/ldap_delete_resp.json >&2
            echo "" >&2
            ERRORS=$((ERRORS + 1))
            # Do not blank the username if the LDAP delete failed.
            continue
            ;;
    esac

    # Blank the Platform DB username now that it no longer exists in LDAP.
    if [[ -z "$contact_sfid" ]]; then
        echo "  WARN: no contact_sfid for $platform_username; username__c not blanked" >&2
        ERRORS=$((ERRORS + 1))
    elif psql -c "$BLANK_SQL" 2>&1; then
        echo "  OK: username__c blanked on $contact_sfid"
        BLANKED=$((BLANKED + 1))
    else
        echo "  ERROR: username__c blank failed for $platform_username ($contact_sfid)" >&2
        ERRORS=$((ERRORS + 1))
    fi

    # Throttle every batch_size rows.
    if [[ $((COUNT % BATCH_SIZE)) -eq 0 && "$DRY_RUN" != "true" ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: $COUNT processed, $DELETED deleted, $NOT_FOUND not found, $BLANKED usernames blanked, $ERRORS errors."
