# LFXV2-2662: Data Cleanup Scripts

Uncommitted ad hoc scripts for primary email alignment remediation. Not part of
the release build; kept locally for reference and execution during remediation.

## Analysis (Snowflake)

| File | Purpose |
|------|---------|
| `lfxv2_2662_primary_email_alignment.sql` | Base alignment query — classifies all platform users into alignment buckets (ALIGNED, WRONG_PRIMARY_FLAG, MISSING_FROM_AUTH0, etc.) |
| `lfxv2_2662_matrixed_buckets.sql` | Extended matrix — crosses alignment_status with drilldown signals (BLOCKED, GDPR, MANGLED, EXT, PHONY_USERNAME, timestamps) and outputs counts + up to 50 sample usernames per cell |
| `lfxv2_2662_secondary_email_alignment.sql` | Secondary/alternate email alignment analysis |

## Username Resolver (Snowflake)

Single generic script used by all apply scripts to look up user details from
the matrixed sample usernames. Pass usernames via `-D USERNAMES="'...'"`.

| File | Purpose |
|------|---------|
| `lfxv2_2662_resolve_usernames.sql` | Snowflake → CSV: takes comma-separated usernames, outputs all columns any apply script needs (platform_username, contact_sfid, auth0_id, auth0_email, ldap_email, flagged_primary_email, flagged_email_sfid, matching_email_sfid) |

## Workflow

1. Run `lfxv2_2662_matrixed_buckets.sql` to get per-cell counts and sample usernames.
2. Copy the comma-separated samples from the target drilldown cell(s).
3. Run `lfxv2_2662_resolve_usernames.sql` with `-D USERNAMES="'<samples>'"` to generate a CSV.
4. Feed the CSV into the appropriate apply script.
5. Re-run the matrixed query to verify the cell counts dropped and get the next batch.

## Apply Scripts

### WRONG_PRIMARY_FLAG / PLATFORM_VERIFIED (Auth0 Management API PATCH)

Drilldown cells: `PLATFORM_VERIFIED+NEWER_AUTH0`, `PLATFORM_VERIFIED+NEWER_LDAP`,
`PLATFORM_VERIFIED+TS_UNKNOWN`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_wrong_primary_verified.sh` | PATCHes Auth0 email to the Platform flagged primary (verified). Supports `--dry-run`, `--batch-size`, `--sleep` |

### WRONG_PRIMARY_FLAG / PLATFORM_UNVERIFIED (Platform DB flag swap)

Drilldown cells: `PLATFORM_UNVERIFIED+NEWER_AUTH0`, `PLATFORM_UNVERIFIED+NEWER_LDAP`,
`PLATFORM_UNVERIFIED+TS_UNKNOWN`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_wrong_primary_unverified.sh` | Swaps primary_email__c flags on Platform DB (matching row ON, flagged row OFF). Supports `--dry-run`, `--batch-size`, `--sleep` |

### PLATFORM_OUT_OF_SYNC_WITH_LDAP (LDAP REST Proxy email update)

Drilldown cells: `PLATFORM_VERIFIED+NEWER_LDAP`, `PLATFORM_VERIFIED+NEWER_AUTH0`,
`PLATFORM_UNVERIFIED+NEWER_LDAP`, `MANGLED_LDAP+PLATFORM_VERIFIED+NEWER_LDAP`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_ldap_out_of_sync.sh` | Updates LDAP mail attribute via PUT /users/:name/email. Supports `--dry-run`, `--batch-size`, `--sleep` |

### PLATFORM_OUT_OF_SYNC_WITH_AUTH0 / NEWER_LDAP (fake login to re-sync)

Drilldown cells: `PLATFORM_UNVERIFIED+NEWER_LDAP`, `PLATFORM_VERIFIED+NEWER_LDAP`

| File | Purpose |
|------|---------|
| `lfxv2_2662_apply_auth0_out_of_sync.sh` | Sets LDAP temp password + ROPG login to force Auth0 to re-sync email from LDAP. Supports `--dry-run`, `--batch-size`, `--sleep` |
