#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Complete incomplete deletions for PLATFORM_OUT_OF_SYNC_WITH_BOTH
# blocked/mangled cells by deleting the user via the Auth0 Management API and
# blanking username__c on the Platform DB.
#
# Matrixed drilldown cells (under PLATFORM_OUT_OF_SYNC_WITH_BOTH):
#   BLOCKED+MANGLED_AUTH0+MANGLED_LDAP+PLATFORM_{VERIFIED,UNVERIFIED}+NEWER_{LDAP,PLATFORM}
#   AUTH0_BLOCKED+MANGLED_AUTH0+MANGLED_LDAP+PLATFORM_{VERIFIED,UNVERIFIED}+NEWER_LDAP
#   DRUPAL_BLOCKED+MANGLED_AUTH0+MANGLED_LDAP+PLATFORM_VERIFIED+NEWER_LDAP
#
# Per the Jul 21 decision audit: Auth0 deletion cascades to LDAP deletion,
# and the cascade places a blank-email tombstone when the user is blocked in
# Drupal/Identity (the proxy reads the block state from Drupal, not Auth0).
# Pre-step NOT handled by this script:
#   - AUTH0_BLOCKED-only users: block in Drupal first (sso-tools), so the
#     cascade tombstone gets a blank email.
# DRUPAL_BLOCKED-only users need no pre-step: Identity is already blocked,
# which is what determines tombstone behavior (supersedes the Jul 21 "sync
# block to Auth0 first" note).
#
# Per-user steps:
#   1. DELETE /api/v2/users/{auth0_id} on the Auth0 Management API.
#   2. Poll GET /users/:name on the LDAP REST Proxy until 404 (cascade done);
#      falls back to a direct proxy DELETE if the cascade does not land.
#   3. Blank username__c on the Platform DB (guarded on sfid + username).
#
# NOTE: anoopcs9 required an sso-tools merge with anoopcs before deletion;
# that merge was completed (confirmed against live data 2026-08-18), so it
# may be processed normally. If re-running from a pre-merge warehouse
# snapshot, verify the auth0_id for anoopcs9 against live data first.
#
# Uses columns: platform_username, contact_sfid, auth0_id, ldap_email
#
# Usage:
#   export AUTH0_DOMAIN="linuxfoundation-prod.auth0.com"
#   export AUTH0_MGMT_TOKEN="..."   # Management API token with delete:users
#   export LDAP_PROXY_URL="https://ldap-rest-proxy-prod.int.linuxfoundation.org"
#   export LDAP_PROXY_TOKEN="..."   # M2M token with read:users + delete:users (delete only used as cascade fallback)
#   ./scripts/lfxv2_2662_apply_auth0_delete.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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
BATCH_SIZE=5
SLEEP_SECONDS=2
CASCADE_POLL_ATTEMPTS=10
CASCADE_POLL_INTERVAL=3

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

if [[ -z "${LDAP_PROXY_URL:-}" || -z "${LDAP_PROXY_TOKEN:-}" ]]; then
    echo "Error: LDAP_PROXY_URL and LDAP_PROXY_TOKEN must be set." >&2
    exit 1
fi

TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (batch_size=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s, dry_run=$DRY_RUN)"

COUNT=0
DELETED=0
BLANKED=0
SKIPPED=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id flagged_email_other_contact_sfid meeting_count_other_sfid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    contact_sfid="${contact_sfid//\"/}"
    auth0_id="${auth0_id//\"/}"
    ldap_email="${ldap_email//\"/}"

    COUNT=$((COUNT + 1))

    if [[ -z "$auth0_id" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: no auth0_id in CSV" >&2
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    encoded_username=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$platform_username', safe=''))")
    encoded_auth0_id=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$auth0_id', safe=''))")

    BLANK_SQL="UPDATE salesforce.merged_user SET username__c = NULL WHERE sfid = '${contact_sfid}' AND username__c = '${platform_username}';"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username (auth0_id=$auth0_id, ldap_email=$ldap_email):"
        echo "  would DELETE Auth0 user, verify LDAP cascade, blank username__c on $contact_sfid"
        continue
    fi

    # Step 1: Auth0 delete (idempotent: 204 even if already gone).
    echo "[$COUNT/$TOTAL] Deleting $platform_username via Auth0 ($auth0_id)"
    HTTP_CODE=$(curl -s -o /tmp/auth0_delete_resp.json -w "%{http_code}" \
        -X DELETE "https://${AUTH0_DOMAIN}/api/v2/users/${encoded_auth0_id}" \
        -H "Authorization: Bearer ${AUTH0_MGMT_TOKEN}")

    if [[ "$HTTP_CODE" != "204" ]]; then
        echo "  ERROR: Auth0 DELETE HTTP $HTTP_CODE for $platform_username" >&2
        cat /tmp/auth0_delete_resp.json >&2
        echo "" >&2
        ERRORS=$((ERRORS + 1))
        continue
    fi
    echo "  OK: Auth0 user deleted"
    DELETED=$((DELETED + 1))

    # Step 2: wait for the LDAP cascade to complete.
    CASCADED=false
    for _ in $(seq 1 "$CASCADE_POLL_ATTEMPTS"); do
        LDAP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            "${LDAP_PROXY_URL}/users/${encoded_username}" \
            -H "Authorization: Bearer ${LDAP_PROXY_TOKEN}")
        if [[ "$LDAP_CODE" == "404" ]]; then
            CASCADED=true
            break
        fi
        sleep "$CASCADE_POLL_INTERVAL"
    done
    if [[ "$CASCADED" != "true" ]]; then
        echo "  WARN: LDAP entry still present after polling; deleting via proxy directly"
        DEL_CODE=$(curl -s -o /tmp/ldap_delete_resp.json -w "%{http_code}" \
            -X DELETE "${LDAP_PROXY_URL}/users/${encoded_username}" \
            -H "Authorization: Bearer ${LDAP_PROXY_TOKEN}")
        if [[ "$DEL_CODE" != "204" && "$DEL_CODE" != "404" ]]; then
            echo "  ERROR: LDAP proxy DELETE HTTP $DEL_CODE for $platform_username" >&2
            cat /tmp/ldap_delete_resp.json >&2
            echo "" >&2
            ERRORS=$((ERRORS + 1))
            continue
        fi
    fi


    # Step 3: blank the Platform DB username.
    if [[ -z "$contact_sfid" ]]; then
        echo "  WARN: no contact_sfid for $platform_username; username__c not blanked" >&2
        ERRORS=$((ERRORS + 1))
    else
        # Check the command tag, not just psql's exit status: the guarded
        # WHERE matches no rows when username__c is already blank or the row
        # has drifted, yet psql still exits 0.
        if ! PSQL_OUT=$(PGOPTIONS='--client-min-messages=warning' psql -tA -c "$BLANK_SQL" 2>&1); then
            echo "  ERROR: username__c blank failed for $platform_username ($contact_sfid)" >&2
            echo "$PSQL_OUT" >&2
            ERRORS=$((ERRORS + 1))
        elif grep -q '^UPDATE 1$' <<<"$PSQL_OUT"; then
            echo "  OK: username__c blanked on $contact_sfid"
            BLANKED=$((BLANKED + 1))
        else
            echo "  SKIP: username__c already blank or row drifted on $contact_sfid ($PSQL_OUT)"
        fi
    fi

    # Throttle every batch_size rows.
    if [[ $((COUNT % BATCH_SIZE)) -eq 0 && "$DRY_RUN" != "true" ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: $COUNT processed, $DELETED deleted, $BLANKED usernames blanked, $SKIPPED skipped, $ERRORS errors."
