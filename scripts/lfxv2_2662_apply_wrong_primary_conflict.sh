#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Fix WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED conflict rows on the
# Platform DB: flip primary_email__c to the row matching the user's own Auth0
# email, and DELETE the flagged alternate_email__c row, which belongs to a
# different LFID (another Auth0 account owns that email).
#
# Input rows are the ones lfxv2_2662_apply_wrong_primary_verified.sh skips
# with "(conflict)": flagged_email_other_auth0_id is non-empty. Rows without
# a conflict are skipped here (use the verified push script for those).
#
# Both operations run in one transaction per user:
#   1. UPDATE matching row (matching_email_sfid): primary_email__c = true.
#   2. DELETE flagged row (flagged_email_sfid) — email owned by another LFID.
#
# IMPORTANT: each row fix fires a live sync event. Batch carefully and monitor.
#
# Uses columns: platform_username, auth0_email, flagged_primary_email,
#               flagged_email_sfid, matching_email_sfid,
#               flagged_email_other_auth0_id
#
# Usage:
#   ./scripts/lfxv2_2662_apply_wrong_primary_conflict.sh [--dry-run] [--batch-size N] [--sleep S] <csv_file>
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
#   flagged_primary_email,flagged_email_sfid,matching_email_sfid,
#   flagged_email_other_auth0_id

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
    auth0_email="${auth0_email//\"/}"
    flagged_primary_email="${flagged_primary_email//\"/}"
    flagged_email_sfid="${flagged_email_sfid//\"/}"
    matching_email_sfid="${matching_email_sfid//\"/}"
    flagged_email_other_auth0_id="${flagged_email_other_auth0_id//\"/}"
    flagged_email_other_auth0_id="${flagged_email_other_auth0_id//[$'\r\n ']/}"
    flagged_email_other_ldap_uid="${flagged_email_other_ldap_uid//\"/}"
    flagged_email_other_ldap_uid="${flagged_email_other_ldap_uid//[$'\r\n ']/}"

    COUNT=$((COUNT + 1))

    if [[ -z "$flagged_email_other_auth0_id" && -z "$flagged_email_other_ldap_uid" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: no conflict — use the verified push script"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    if [[ -z "$matching_email_sfid" || -z "$flagged_email_sfid" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: missing matching or flagged sfid — manual review" >&2
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Protection: never swap/delete for users with recorded meeting attendance.
    meeting_count="${meeting_count//\"/}"
    meeting_count="${meeting_count//[$'\r\n ']/}"
    if [[ "${meeting_count:-0}" != "0" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: meeting_count=$meeting_count — manual review" >&2
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Gate: only swap/delete when the other username also exists in TI
    # (Thought Industries) — separate training history signals the accounts
    # should NOT be merged. Otherwise route: Identity-only other account
    # (no Auth0, no TI) -> LDAP proxy delete; other account in Auth0 ->
    # defer to a support merge.
    flagged_email_other_ti_id="${flagged_email_other_ti_id//\"/}"
    flagged_email_other_ti_id="${flagged_email_other_ti_id//[$'\r\n ']/}"
    if [[ -z "$flagged_email_other_ti_id" ]]; then
        if [[ -n "$flagged_email_other_auth0_id" ]]; then
            echo "[$COUNT/$TOTAL] SKIP $platform_username: other account $flagged_email_other_auth0_id has no TI — defer to support merge" >&2
        else
            echo "[$COUNT/$TOTAL] SKIP $platform_username: other account $flagged_email_other_ldap_uid is Identity-only, no TI — LDAP proxy delete candidate" >&2
        fi
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    SQL="BEGIN;
UPDATE salesforce.alternate_email__c SET primary_email__c = true WHERE sfid = '${matching_email_sfid}';
DELETE FROM salesforce.alternate_email__c WHERE sfid = '${flagged_email_sfid}';
COMMIT;"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username: primary -> $auth0_email ($matching_email_sfid); delete $flagged_primary_email ($flagged_email_sfid, owned by auth0=$flagged_email_other_auth0_id ldap=$flagged_email_other_ldap_uid)"
    else
        echo "[$COUNT/$TOTAL] Fixing $platform_username: primary -> $auth0_email; deleting $flagged_primary_email (owned by auth0=$flagged_email_other_auth0_id ldap=$flagged_email_other_ldap_uid)"
        if psql -c "$SQL" 2>&1; then
            FIXED=$((FIXED + 1))
        else
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
echo "Done: $COUNT processed, $FIXED fixed, $SKIPPED skipped, $ERRORS errors."
