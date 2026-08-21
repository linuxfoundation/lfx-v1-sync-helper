#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Swap primary_email__c flags on Platform DB for
# WRONG_PRIMARY_FLAG / PLATFORM_UNVERIFIED users.
#
# Matrixed drilldown cells:
#   WRONG_PRIMARY_FLAG / PLATFORM_UNVERIFIED+NEWER_AUTH0
#   WRONG_PRIMARY_FLAG / PLATFORM_UNVERIFIED+NEWER_LDAP
#   WRONG_PRIMARY_FLAG / PLATFORM_UNVERIFIED+TS_UNKNOWN
#
# Resolves usernames via lfxv2_2662_resolve_usernames.sql, then swaps
# primary_email__c on the Platform DB: set true on the matching row
# (matching_email_sfid) and false on the flagged row (flagged_email_sfid).
#
# Uses columns: platform_username, auth0_email (correct), flagged_primary_email
#               (wrong), matching_email_sfid (correct sfid), flagged_email_sfid
#               (wrong sfid)
#
# IMPORTANT: each row fix fires a live sync event. Batch carefully and monitor.
#
# Usage:
#   ./scripts/lfxv2_2662_apply_wrong_primary_unverified.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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
# CSV columns (from resolve_usernames.sql):
#   platform_username,contact_sfid,auth0_id,auth0_email,ldap_email,
#   flagged_primary_email,flagged_email_sfid,matching_email_sfid

set -euo pipefail

DRY_RUN=false
BATCH_SIZE=50
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

# Skip header row, count data rows.
TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (batch_size=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s, dry_run=$DRY_RUN)"

COUNT=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id flagged_email_other_contact_sfid meeting_count_other_sfid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    auth0_email="${auth0_email//\"/}"
    flagged_primary_email="${flagged_primary_email//\"/}"
    flagged_email_sfid="${flagged_email_sfid//\"/}"
    matching_email_sfid="${matching_email_sfid//\"/}"

    COUNT=$((COUNT + 1))

    SQL="BEGIN;
UPDATE salesforce.alternate_email__c SET primary_email__c = true WHERE sfid = '${matching_email_sfid}';
UPDATE salesforce.alternate_email__c SET primary_email__c = false WHERE sfid = '${flagged_email_sfid}';
COMMIT;"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username: $flagged_primary_email -> $auth0_email"
        echo "  correct_sfid=$matching_email_sfid  wrong_sfid=$flagged_email_sfid"
    else
        echo "[$COUNT/$TOTAL] Fixing $platform_username: $flagged_primary_email -> $auth0_email"
        if ! psql -c "$SQL" 2>&1; then
            echo "  ERROR: failed for $platform_username" >&2
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
