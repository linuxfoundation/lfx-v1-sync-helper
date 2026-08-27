#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Flip active__c=true on the Platform primary email row for
# ALIGNED / INACTIVE_PRIMARY users.
#
# These users are fully aligned (Platform flagged primary == Auth0 == LDAP,
# case-insensitive) but the primary alternate_email__c row is marked
# active__c=false. Remediation is a pure flag flip on the SAME row: no email
# value changes anywhere, so meeting registrations (ICS destinations) are
# unaffected and no Auth0/LDAP writes are needed.
#
# Every UPDATE is fully self-guarding: sfid, contact, email value, primary
# flag, and current active=false are all in the WHERE clause, so any row
# whose live state has drifted from the warehouse snapshot no-ops (UPDATE 0)
# and is reported as a skip. The script is idempotent.
#
# Uses columns: platform_username, contact_sfid, flagged_primary_email,
#               flagged_email_sfid, matching_email_sfid
#
# Usage:
#   export PGPASSWORD="..."  # lfit password for prod sfdc-connector DB
#   ./scripts/lfxv2_2662_apply_inactive_primary.sh [--dry-run] <csv_file>
#
# Generate CSV: see lfxv2_2662_resolve_usernames.sql header (15 columns).

set -euo pipefail

PGHOST="prod-sfdc-connector-database.c29fcqwjdpo7.us-east-2.rds.amazonaws.com"
PGDB="sfdc"
PGUSER="lfit"

DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        *) CSV_FILE="$1"; shift ;;
    esac
done

if [[ -z "${CSV_FILE:-}" ]]; then
    echo "Usage: $0 [--dry-run] <csv_file>" >&2
    exit 1
fi

if [[ ! -f "$CSV_FILE" ]]; then
    echo "Error: CSV file not found: $CSV_FILE" >&2
    exit 1
fi

if [[ -z "${PGPASSWORD:-}" ]]; then
    echo "Error: PGPASSWORD must be set." >&2
    exit 1
fi

TOTAL=$(tail -n +2 "$CSV_FILE" | wc -l | tr -d ' ')
echo "Processing $TOTAL users (dry_run=$DRY_RUN)"

COUNT=0
UPDATED=0
SKIPPED=0
ERRORS=0

while IFS=',' read -r platform_username contact_sfid auth0_id auth0_email ldap_email flagged_primary_email flagged_email_sfid matching_email_sfid flagged_email_other_auth0_id flagged_email_other_ldap_uid meeting_count ti_id flagged_email_other_ti_id flagged_email_other_contact_sfid meeting_count_other_sfid; do
    # Strip surrounding quotes.
    platform_username="${platform_username//\"/}"
    contact_sfid="${contact_sfid//\"/}"
    contact_sfid="${contact_sfid//[$'\r\n ']/}"
    flagged_primary_email="${flagged_primary_email//\"/}"
    flagged_email_sfid="${flagged_email_sfid//\"/}"
    flagged_email_sfid="${flagged_email_sfid//[$'\r\n ']/}"
    matching_email_sfid="${matching_email_sfid//\"/}"
    matching_email_sfid="${matching_email_sfid//[$'\r\n ']/}"

    COUNT=$((COUNT + 1))

    # Sanity guard: expect a resolved flagged primary row.
    if [[ -z "$flagged_email_sfid" || -z "$contact_sfid" || -z "$flagged_primary_email" ]]; then
        echo "[$COUNT/$TOTAL] SKIP $platform_username: missing flagged_email_sfid/contact_sfid/email"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Alignment guard. Two accepted shapes:
    #   ALIGNED: the row matching Auth0+LDAP is the flagged primary row.
    #   MISSING_FROM_AUTH0: no Auth0 account, so matching_email_sfid is
    #     empty; require the flagged primary to equal the LDAP email
    #     (case-insensitive) instead.
    if [[ "$matching_email_sfid" != "$flagged_email_sfid" ]]; then
        auth0_id_clean="${auth0_id//\"/}"
        ldap_email_clean="${ldap_email//\"/}"
        if [[ -z "$auth0_id_clean" && -n "$ldap_email_clean" \
            && "$(tr '[:upper:]' '[:lower:]' <<<"$flagged_primary_email")" == "$(tr '[:upper:]' '[:lower:]' <<<"$ldap_email_clean")" ]]; then
            : # No Auth0 account but Platform primary matches LDAP: proceed.
        else
            echo "[$COUNT/$TOTAL] SKIP $platform_username: matching row $matching_email_sfid != flagged row $flagged_email_sfid — not aligned"
            SKIPPED=$((SKIPPED + 1))
            continue
        fi
    fi

    # Escape single quotes for the SQL literal (defense in depth; guarded
    # values are SFIDs and an email that already matched Auth0/LDAP).
    email_sql="${flagged_primary_email//\'/\'\'}"

    SQL="UPDATE salesforce.alternate_email__c
SET active__c = true
WHERE sfid = '${flagged_email_sfid}'
  AND leadorcontactid = '${contact_sfid}'
  AND LOWER(alternate_email_address__c) = LOWER('${email_sql}')
  AND primary_email__c = true
  AND active__c = false;"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$COUNT/$TOTAL] DRY-RUN $platform_username: activate $flagged_email_sfid ($flagged_primary_email)"
        continue
    fi

    # Suppress trigger NOTICE chatter (replication capture trigger emits
    # 'NOTICE: TABLE: alternate_email__c') so RESULT is the command tag only.
    RESULT=$(PGOPTIONS='--client-min-messages=warning' psql -h "$PGHOST" -U "$PGUSER" "$PGDB" -tA -c "$SQL" 2>&1) || {
        echo "  ERROR: psql failed for $platform_username: $RESULT" >&2
        ERRORS=$((ERRORS + 1))
        continue
    }

    if [[ "$RESULT" == "UPDATE 1" ]]; then
        echo "[$COUNT/$TOTAL] UPDATED $platform_username: $flagged_email_sfid ($flagged_primary_email)"
        UPDATED=$((UPDATED + 1))
    else
        echo "[$COUNT/$TOTAL] SKIP $platform_username: live row drifted from snapshot ($RESULT) — $flagged_email_sfid"
        SKIPPED=$((SKIPPED + 1))
    fi
done < <(tail -n +2 "$CSV_FILE")

echo ""
echo "Done: updated=$UPDATED skipped=$SKIPPED errors=$ERRORS total=$COUNT"
