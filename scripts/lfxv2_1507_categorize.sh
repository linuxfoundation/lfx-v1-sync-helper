#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Categorize affected usernames against the LFXV2-2662 flagged
# account list (resolved.csv).
#
# Input 1: wave usernames CSV from lfxv2_1507_affected_usernames.sql
#          (platform_username, contact_sfid, engagements, foundations)
# Input 2: resolved.csv from lfxv2_2662_resolve_usernames.sql. Matching is by
#          username (case-insensitive) against:
#            - column 1  PLATFORM_USERNAME      -> flagged
#            - column 9  FLAGGED_EMAIL_OTHER_AUTH0_ID (auth0|<username>)
#                                               -> conflict_auth0
#            - column 10 FLAGGED_EMAIL_OTHER_LDAP_UID -> conflict_ldap
#          Note: columns 9/10 hold usernames (Auth0 ID / LDAP uid), not
#          SFIDs; contact_sfid from column 2 is also matched as a fallback
#          (covers username renames between extracts).
#
# Output: input CSV plus a lfxv2_2662_flag column (clean, flagged,
# conflict_auth0, conflict_ldap, flagged_sfid). Non-clean usernames must be
# excluded from the wave sync run until their 2662 remediation completes.
#
# Usage: lfxv2_1507_categorize.sh <wave_usernames.csv> <resolved.csv>

set -eu

WAVE_CSV="${1:?usage: $0 <wave_usernames.csv> <resolved.csv>}"
RESOLVED_CSV="${2:?usage: $0 <wave_usernames.csv> <resolved.csv>}"

awk -F, '
# Strip surrounding double quotes and lowercase.
function norm(s) {
    gsub(/^"|"$/, "", s)
    return tolower(s)
}
NR == FNR {
    if (FNR == 1) next
    u = norm($1)
    if (u != "") flagged[u] = 1
    sfid = norm($2)
    if (sfid != "") flagged_sfid[sfid] = 1
    a0 = norm($9)
    sub(/^auth0\|/, "", a0)
    if (a0 != "") conflict_auth0[a0] = 1
    ldap = norm($10)
    if (ldap != "") conflict_ldap[ldap] = 1
    next
}
FNR == 1 {
    print $0 ",lfxv2_2662_flag"
    next
}
{
    u = norm($1)
    sfid = norm($2)
    flag = "clean"
    if (u in flagged) flag = "flagged"
    else if (u in conflict_auth0) flag = "conflict_auth0"
    else if (u in conflict_ldap) flag = "conflict_ldap"
    else if (sfid != "" && sfid in flagged_sfid) flag = "flagged_sfid"
    print $0 "," flag
}
' "$RESOLVED_CSV" "$WAVE_CSV"
