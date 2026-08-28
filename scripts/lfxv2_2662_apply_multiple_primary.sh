#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Fix MULTIPLE_PRIMARY_RESOLVED_BY_MATCH users on the Platform DB.
#
# These contacts have more than one alternate_email__c row flagged as
# primary_email__c, but exactly one of them matches the user's own Auth0
# email. Fix: keep the primary flag on the matching row
# (matching_email_sfid) and unset it on every other primary-flagged row
# belonging to the contact.
#
# One transaction per user:
#   1. UPDATE matching row: primary_email__c = true (idempotent).
#   2. UPDATE all other primary rows for the contact: primary_email__c = false.
#
# IMPORTANT: each row fix fires a live sync event. Batch carefully and monitor.
#
# Uses columns: platform_username, contact_sfid, auth0_email,
#               matching_email_sfid
#
# Usage:
#   ./scripts/lfxv2_2662_apply_multiple_primary.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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
#   flagged_primary_email,flagged_email_sfid,matching_email_sfid,
#   flagged_email_other_auth0_id,flagged_email_other_ldap_uid,meeting_count

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

TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (batch_size=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s, dry_run=$DRY_RUN)"

COUNT=0
FIXED=0
SKIPPED=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id flagged_email_other_contact_sfid meeting_count_other_sfid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    contact_sfid="${contact_sfid//\"/}"
    auth0_email="${auth0_email//\"/}"
    matching_email_sfid="${matching_email_sfid//\"/}"

    COUNT=$((COUNT + 1))

    if [[ -z "$contact_sfid" || -z "$matching_email_sfid" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: missing contact_sfid or matching_email_sfid — manual review" >&2
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    SQL="BEGIN;
UPDATE salesforce.alternate_email__c SET primary_email__c = true WHERE sfid = '${matching_email_sfid}' AND primary_email__c IS NOT TRUE;
UPDATE salesforce.alternate_email__c SET primary_email__c = false WHERE leadorcontactid = '${contact_sfid}' AND primary_email__c = true AND sfid != '${matching_email_sfid}';
COMMIT;"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username ($contact_sfid): keep primary $auth0_email ($matching_email_sfid); unset all other primaries"
    else
        echo "[$COUNT/$TOTAL] Fixing $platform_username ($contact_sfid): keep primary $auth0_email; unset all other primaries"
        # Check the command tags, not just psql's exit status: a WHERE that
        # matches no rows still exits 0. Unlike the other apply scripts, both
        # UPDATEs here may legitimately affect zero rows (the intended primary
        # is already flagged, and there may be no other primaries left), so
        # only a combined zero — a complete no-op — is reported, as a skip
        # rather than an error.
        if ! PSQL_OUT=$(PGOPTIONS='--client-min-messages=warning' psql -tA -c "$SQL" 2>&1); then
            echo "  ERROR: failed for $platform_username" >&2
            echo "$PSQL_OUT" >&2
            ERRORS=$((ERRORS + 1))
        else
            ROWS=$(grep '^UPDATE [0-9]' <<<"$PSQL_OUT" | awk '{s += $2} END {print s + 0}')
            if [[ "$ROWS" -eq 0 ]]; then
                echo "  SKIP: no rows changed (already correct, or live rows drifted from snapshot)"
                SKIPPED=$((SKIPPED + 1))
            else
                FIXED=$((FIXED + 1))
            fi
        fi
    fi

    # Throttle every batch_size rows.
    if [[ $((COUNT % BATCH_SIZE)) -eq 0 && "$DRY_RUN" != "true" ]]; then
        echo "  ... sleeping ${SLEEP_SECONDS}s after batch of $BATCH_SIZE"
        sleep "$SLEEP_SECONDS"
    fi

done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: $COUNT processed, $FIXED fixed, $SKIPPED skipped, $ERRORS errors."
