# LFXV2-2662: Data Cleanup Scripts

Uncommitted ad hoc scripts for primary email alignment remediation. Not part of
the release build; kept locally for reference and execution during remediation.

## Analysis (Snowflake)

| File | Purpose |
|------|---------|
| `lfxv2_2662_primary_email_alignment.sql` | Base alignment query — classifies all platform users into alignment buckets (ALIGNED, WRONG_PRIMARY_FLAG, MISSING_FROM_AUTH0, etc.) |
| `lfxv2_2662_matrixed_buckets.sql` | Extended matrix — crosses alignment_status with drilldown signals (BLOCKED, GDPR, MANGLED, EXT, PHONY_USERNAME, timestamps) and outputs counts + up to 850 sample usernames per cell |
| `lfxv2_2662_secondary_email_alignment.sql` | Secondary/alternate email alignment analysis |

## Username Resolver (Snowflake)

Single generic script used by all apply scripts to look up user details from
the matrixed sample usernames. Pass usernames via `-D USERNAMES="user1,user2,..." (no inner quotes)` and
include `-o variable_substitution=true`, or `-D` is silently ignored and the
literal `&USERNAMES` causes a SQL compilation error.

| File | Purpose |
|------|---------|
| `lfxv2_2662_resolve_usernames.sql` | Snowflake → CSV: takes comma-separated usernames, outputs all columns any apply script needs (platform_username, contact_sfid, auth0_id, auth0_email, ldap_email, flagged_primary_email, flagged_email_sfid, matching_email_sfid, flagged_email_other_auth0_id, flagged_email_other_ldap_uid, meeting_count, ti_id, flagged_email_other_ti_id, flagged_email_other_contact_sfid, meeting_count_other_sfid) |
| `lfxv2_2662_join_buckets.awk` | `awk -f scripts/lfxv2_2662_join_buckets.awk matrixed_buckets.csv resolved.csv > resolved_with_buckets.csv` — prepends `ALIGNMENT_STATUS` and `DRILLDOWN` from the matrix so the resolver output doubles as a manual-remediation reference. Case-insensitive username match; unmatched rows keep empty bucket columns. Feed the apply scripts the plain `resolved.csv` (they read columns positionally) |

## Workflow

1. Run `lfxv2_2662_matrixed_buckets.sql` to get per-cell counts and sample usernames.
2. Copy the comma-separated samples from the target drilldown cell(s), or extract
   several cells at once from the saved matrix CSV:

   ```bash
   USERNAMES=$(awk -F'","' '{s=$1; sub(/^"/,"",s); d=$2} \
     s ~ /^PLATFORM_OUT_OF_SYNC_WITH_/ || s=="WRONG_PRIMARY_FLAG" || d ~ /INACTIVE_PRIMARY/ \
     {u=$4; sub(/"$/,"",u); print u}' matrixed_buckets.csv | paste -sd, -)
   ```

3. Run `lfxv2_2662_resolve_usernames.sql` with `-D USERNAMES="<samples>"` to generate a CSV.
4. Feed the CSV into the appropriate apply script. Optionally run
   `lfxv2_2662_join_buckets.awk` first for a bucket-annotated reference copy.
5. Re-run the matrixed query to verify the cell counts dropped and get the next batch.

## Apply Scripts

### WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED (Auth0 Management API PATCH)

Drilldown cells: `PLATFORM_VERIFIED+NEWER_AUTH0`, `PLATFORM_VERIFIED+NEWER_LDAP`,
`PLATFORM_VERIFIED+TS_UNKNOWN`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_wrong_primary_verified.sh` | PATCHes Auth0 email to the Platform flagged primary (verified). Skips rows where the resolver's conflict columns (`flagged_email_other_auth0_id`, `flagged_email_other_ldap_uid`) show the flagged email is owned by a different account — Auth0 validates email availability against LDAP/Identity, so either ownership blocks the push. Also skips users with no Auth0 account. Supports `--dry-run`, `--batch-size`, `--sleep` |
| `lfxv2_2662_apply_wrong_primary_conflict.sh` | For the conflict rows skipped above: on the Platform DB, flips `primary_email__c` to the row matching the user's own Auth0 email and DELETEs the flagged `alternate_email__c` row (email belongs to a different LFID). Only acts when the other username also exists in TI (separate training history = do not merge); otherwise skips with routing: Identity-only other account → LDAP proxy delete, Auth0 other account → support merge. Also skips users with non-zero meeting_count. One transaction per user; fires live sync events. Supports `--dry-run`, `--batch-size`, `--sleep` |
| `lfxv2_2662_apply_multiple_primary.sh` | For MULTIPLE_PRIMARY_RESOLVED_BY_MATCH users: keeps `primary_email__c` on the row matching the user's own Auth0 email (`matching_email_sfid`) and unsets it on every other primary-flagged row for the contact. One transaction per user; fires live sync events. Supports `--dry-run`, `--batch-size`, `--sleep` |

### WRONG_PRIMARY_FLAG / PLATFORM_UNVERIFIED (Platform DB flag swap)

Drilldown cells: `PLATFORM_UNVERIFIED+NEWER_AUTH0`, `PLATFORM_UNVERIFIED+NEWER_LDAP`,
`PLATFORM_UNVERIFIED+TS_UNKNOWN`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_wrong_primary_unverified.sh` | Swaps primary_email__c flags on Platform DB (matching row ON, flagged row OFF). Supports `--dry-run`, `--batch-size`, `--sleep` |

### INACTIVE_PRIMARY (Platform DB active flag flip)

Drilldown cells: `ALIGNED / INACTIVE_PRIMARY`, `MISSING_FROM_AUTH0 / INACTIVE_PRIMARY`
(and BLOCKED/MANGLED combinations thereof)

The Platform primary email row is marked `active__c=false` while all systems
agree on the address. Pure flag flip on the same row — no email value changes,
so no Auth0/LDAP writes and no meeting (ICS) impact. Verified: itx-service-zoom
selects by `IsPrimary` only (SDK does not deserialize `Active`) and user-service
returns inactive emails unfiltered.

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_inactive_primary.sh` | Sets `active__c=true` on the flagged primary row via self-guarding idempotent UPDATEs (sfid, contact, email value, primary=true, active=false all in the WHERE; `UPDATE 0` = live drift, reported as skip). Alignment guard: `matching_email_sfid` must equal `flagged_email_sfid`, or — for users with no Auth0 account — the flagged primary must equal the LDAP email (case-insensitive). Needs `PGPASSWORD`. Supports `--dry-run` |

### PLATFORM_OUT_OF_SYNC_WITH_LDAP (LDAP REST Proxy email update)

Drilldown cells: `PLATFORM_VERIFIED+NEWER_LDAP`, `PLATFORM_VERIFIED+NEWER_AUTH0`,
`PLATFORM_UNVERIFIED+NEWER_LDAP`, `MANGLED_LDAP+PLATFORM_VERIFIED+NEWER_LDAP`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_ldap_out_of_sync.sh` | Updates LDAP mail attribute via PUT /users/:name/email. Supports `--dry-run`, `--batch-size`, `--sleep` |

### ALIGNED / DRUPAL_BLOCKED (Auth0 Management API block)

Drilldown cell: `DRUPAL_BLOCKED` (under `ALIGNED`)

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_drupal_blocked.sh` | Sets `blocked=true` on Auth0 users blocked in Drupal but not Auth0. Skips already-blocked users (idempotent). Supports `--dry-run`, `--batch-size`, `--sleep` |

### MISSING_FROM_AUTH0 / DRUPAL_BLOCKED (LDAP REST Proxy delete)

Drilldown cells: `DRUPAL_BLOCKED`, `DRUPAL_BLOCKED+MANGLED_LDAP`,
`DRUPAL_BLOCKED+MANGLED_PLATFORM+MANGLED_LDAP` (under `MISSING_FROM_AUTH0`)

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_ldap_delete.sh` | Completes incomplete deletions by DELETE /users/:name on the LDAP REST Proxy (requires `delete:users` scope; proxy auto-tombstones verified/blocked users), then blanks `username__c` on the Platform DB (needs PG* env vars). Treats 404 as already deleted. Supports `--dry-run`, `--batch-size`, `--sleep` |

### PLATFORM_OUT_OF_SYNC_WITH_BOTH blocked/mangled (Auth0 delete)

Drilldown cells: `BLOCKED/AUTH0_BLOCKED/DRUPAL_BLOCKED + MANGLED_AUTH0+MANGLED_LDAP + ...`
(under `PLATFORM_OUT_OF_SYNC_WITH_BOTH`)

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_auth0_delete.sh` | Completes incomplete deletions: DELETE the Auth0 user (cascades to LDAP with auto-tombstone), verify the cascade via the LDAP REST Proxy (direct proxy DELETE as fallback), then blank `username__c` on the Platform DB. Pre-step: block AUTH0_BLOCKED-only users in Drupal via sso-tools (Identity block state determines tombstone behavior); DRUPAL_BLOCKED-only users need no pre-step. Supports `--dry-run`, `--batch-size`, `--sleep` |

### PLATFORM_OUT_OF_SYNC_WITH_AUTH0 / NEWER_LDAP (fake login to re-sync)

Drilldown cells: `PLATFORM_UNVERIFIED+NEWER_LDAP`, `PLATFORM_VERIFIED+NEWER_LDAP`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_auth0_out_of_sync.sh` | Sets LDAP temp password + ROPG login to force Auth0 to re-sync email from LDAP. Supports `--dry-run`, `--batch-size`, `--sleep` |
