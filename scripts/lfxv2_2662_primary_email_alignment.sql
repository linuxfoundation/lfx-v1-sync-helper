-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT

-- LFXV2-2662: Validate primary email sync before alternate email sync
--
-- Ad hoc Snowflake validation comparing primary email across Platform (SFDC),
-- Auth0, and LDAP. This is a raw analysis script (not a dbt model) meant to be
-- run directly via snowsql against fivetran_ingest raw tables and the
-- ANALYTICS_DEV.DEV_ERIC scratch tables (LDAP snapshot + LFID tombstones).
--
-- Population: platform users with a username (LFID) that is not tombstoned.
--
-- Run with:
--   rm -f primary_email_alignment.csv && \
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false \
--     -o output_format=csv -o output_file=primary_email_alignment.csv \
--     -f scripts/lfxv2_2662_primary_email_alignment.sql
--
-- Users are joined three ways by username:
--   - LDAP:     analytics_dev.dev_eric.ldap_users.uid
--   - Auth0:    fivetran_ingest.auth0.users.username, restricted to the
--               Username-Password-Authentication identity (the LFID-backed
--               connection; "email" connection is not yet in use, see notes
--               in LFXV2-2662)
--   - Platform: fivetran_ingest.sfdc_connector_prod_salesforce.merged_user,
--               joined to all (non-deleted) alternate_email__c rows for that
--               contact.
--
-- Manual spot-checking of individual users turned up cases the naive "trust
-- primary_email__c" comparison got wrong:
--   - primary_email__c is sometimes flagged on a garbled/wrong row while a
--     DIFFERENT row (not flagged primary, any verified__c/active__c status)
--     is the one that actually matches both Auth0 and LDAP. Neither
--     verified__c nor active__c reliably predicts which row is "correct" --
--     e.g. one user's correct row is unverified, while an unrelated,
--     verified row is not the answer.
--   - Some contacts have 2+ rows flagged primary_email__c = TRUE at once
--     (a Platform data integrity violation), but there is often still an
--     unambiguous answer: whichever row's email matches BOTH Auth0 and LDAP.
--
-- So instead of trusting primary_email__c (or active__c/verified__c) as the
-- source of truth, this query looks for a row matching BOTH Auth0's login
-- email and LDAP's mail across ALL of a contact's alternate_email__c rows,
-- and uses that as the "resolved" answer whenever one exists -- falling back
-- to the flagged/lone row only when no such match exists. See
-- alignment_status below for the resulting categories.
--
-- IMPORTANT -- live sync caveat: Platform -> Auth0/LDAP sync is LIVE, not
-- batch. Any ad hoc fix applied directly to a Platform record (e.g.
-- correcting which alternate_email__c row is flagged primary_email__c) will
-- immediately fire the sync pipeline for that individual contact. This
-- means WRONG_PRIMARY_FLAG / MULTIPLE_PRIMARY_RESOLVED_BY_MATCH rows found
-- here can NOT be bulk-corrected in Platform as a "silent" pre-backfill
-- cleanup step -- each correction is itself a live sync event and needs to
-- be evaluated (and likely throttled/sequenced) like any other production
-- change, not just queued up for a future backfill run.
--
-- Run with:
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse DBT_DEV --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false -f \
--     analysis/lfxv2_2662_primary_email_alignment.sql

WITH platform_alt_emails AS (
    SELECT
        platform_users.sfid,
        platform_users.username__c AS platform_username,
        platform_emails.alternate_email_address__c AS email,
        platform_emails.primary_email__c AS is_primary
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
        -- Deliberately NOT filtering on active__c: spot checks showed the
        -- row that actually matches Auth0+LDAP is sometimes active__c=FALSE
        -- (e.g. dpuzikov, jariastest3), so active__c is not a reliable
        -- filter for "the" primary email either.
        AND platform_users.username__c IS NOT NULL
        AND platform_users.username__c <> ''
),

auth0_primary AS (
    SELECT
        a0_users.username AS auth0_username,
        a0_users.email AS auth0_primary_email,
        a0_users.email_verified AS auth0_email_verified,
        a0_users.blocked AS auth0_blocked
    FROM fivetran_ingest.auth0.users a0_users
    INNER JOIN fivetran_ingest.auth0.user_identities a0_user_ids
        ON a0_users.id = a0_user_ids.users_id
    WHERE
        a0_user_ids.connection = 'Username-Password-Authentication'
        AND a0_user_ids._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
    -- A handful of Auth0 users (9 as of this writing) have 2+ identity rows
    -- for the same connection type; dedupe to avoid fanning out joins below.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a0_users.username ORDER BY a0_user_ids.index) = 1
),

ldap_primary AS (
    SELECT
        uid AS ldap_username,
        mail AS ldap_primary_email
    FROM analytics_dev.dev_eric.ldap_users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY _sdc_extracted_at DESC) = 1
),

-- Tag every Platform alt-email row with whether ITS OWN email matches
-- Auth0's login email and/or LDAP's mail, regardless of that row's
-- primary_email__c/active__c/verified__c flags.
platform_rows_with_matches AS (
    SELECT
        platform_alt_emails.sfid,
        platform_alt_emails.platform_username,
        platform_alt_emails.email,
        platform_alt_emails.is_primary,
        (LOWER(platform_alt_emails.email) = LOWER(auth0_primary.auth0_primary_email)) AS matches_auth0,
        (LOWER(platform_alt_emails.email) = LOWER(ldap_primary.ldap_primary_email)) AS matches_ldap
    FROM platform_alt_emails
    LEFT JOIN auth0_primary
        ON LOWER(platform_alt_emails.platform_username) = LOWER(auth0_primary.auth0_username)
    LEFT JOIN ldap_primary
        ON LOWER(platform_alt_emails.platform_username) = LOWER(ldap_primary.ldap_username)
),

platform_primary_candidates AS (
    SELECT
        sfid,
        platform_username,
        MAX(CASE WHEN is_primary THEN email END) AS flagged_primary_email,
        MIN(email) AS lone_email,
        COUNT(*) AS total_rows,
        COUNT_IF(is_primary) AS primary_row_count,
        -- The row (if any) whose email matches BOTH external systems --
        -- this is the high-confidence "resolved" answer regardless of how
        -- Platform's own flags are set on that row.
        MAX(CASE WHEN matches_auth0 AND matches_ldap THEN email END) AS matched_both_email
    FROM platform_rows_with_matches
    GROUP BY sfid, platform_username
),

platform_primary AS (
    SELECT
        sfid,
        platform_username,
        matched_both_email,
        primary_row_count,
        CASE
            WHEN primary_row_count = 1 THEN flagged_primary_email
            -- Lone, non-primary-flagged row: promote per auth0-db-sync.js's
            -- checkPlatformEmails "treating ... as though it were marked
            -- primary" heuristic.
            WHEN primary_row_count = 0 AND total_rows = 1 THEN lone_email
            ELSE NULL
        END AS flagged_or_lone_email,
        (primary_row_count = 0 AND total_rows = 1) AS promoted_lone_email,
        (primary_row_count = 0 AND total_rows > 1) AS ambiguous_no_primary
    FROM platform_primary_candidates
)

SELECT * FROM (
    SELECT
        platform_primary.sfid,
        platform_primary.platform_username,
        platform_primary.flagged_or_lone_email AS platform_flagged_email,
        platform_primary.matched_both_email AS platform_resolved_email,
        platform_primary.primary_row_count,
        ldap_primary.ldap_primary_email,
        auth0_primary.auth0_primary_email,
        auth0_primary.auth0_email_verified,
        auth0_primary.auth0_blocked,
        CASE
            WHEN ldap_primary.ldap_username IS NULL THEN 'MISSING_FROM_LDAP'
            WHEN auth0_primary.auth0_username IS NULL THEN 'MISSING_FROM_AUTH0'
            -- A row matching both external systems exists somewhere among
            -- this contact's alt-email rows.
            WHEN platform_primary.matched_both_email IS NOT NULL
                AND platform_primary.primary_row_count >= 2
                THEN 'MULTIPLE_PRIMARY_RESOLVED_BY_MATCH'
            WHEN platform_primary.matched_both_email IS NOT NULL
                AND LOWER(platform_primary.matched_both_email) = LOWER(platform_primary.flagged_or_lone_email)
                THEN 'ALIGNED'
            WHEN platform_primary.matched_both_email IS NOT NULL
                THEN 'WRONG_PRIMARY_FLAG'
            -- No single row matches both external systems.
            WHEN platform_primary.primary_row_count >= 2 THEN 'AMBIGUOUS_MULTIPLE_PRIMARY'
            WHEN platform_primary.ambiguous_no_primary THEN 'AMBIGUOUS_NO_PRIMARY'
            WHEN platform_primary.flagged_or_lone_email IS NULL THEN 'UNKNOWN'
            WHEN LOWER(platform_primary.flagged_or_lone_email) != LOWER(ldap_primary.ldap_primary_email)
                AND LOWER(platform_primary.flagged_or_lone_email) != LOWER(auth0_primary.auth0_primary_email)
                THEN 'PLATFORM_OUT_OF_SYNC_WITH_BOTH'
            WHEN LOWER(platform_primary.flagged_or_lone_email) != LOWER(ldap_primary.ldap_primary_email)
                THEN 'PLATFORM_OUT_OF_SYNC_WITH_LDAP'
            WHEN LOWER(platform_primary.flagged_or_lone_email) != LOWER(auth0_primary.auth0_primary_email)
                THEN 'PLATFORM_OUT_OF_SYNC_WITH_AUTH0'
            ELSE 'UNKNOWN'
        END AS alignment_status
    FROM platform_primary
    LEFT JOIN ldap_primary
        ON LOWER(platform_primary.platform_username) = LOWER(ldap_primary.ldap_username)
    LEFT JOIN auth0_primary
        ON LOWER(platform_primary.platform_username) = LOWER(auth0_primary.auth0_username)
    LEFT JOIN analytics_dev.dev_eric.lfid_tombstone_prod tombstones
        ON LOWER(platform_primary.platform_username) = tombstones.username
    WHERE tombstones.username IS NULL
)
WHERE alignment_status != 'ALIGNED'
ORDER BY alignment_status, platform_username
-- Remove or raise LIMIT once ready for a full count; useful for spot checks
-- while iterating on the query.
LIMIT 200
;

-- Summary counts by alignment_status, for reporting scale of the problem.
WITH platform_alt_emails AS (
    SELECT
        platform_users.sfid,
        platform_users.username__c AS platform_username,
        platform_emails.alternate_email_address__c AS email,
        platform_emails.primary_email__c AS is_primary
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
        AND platform_users.username__c IS NOT NULL
        AND platform_users.username__c <> ''
),

auth0_primary AS (
    SELECT
        a0_users.username AS auth0_username,
        a0_users.email AS auth0_primary_email
    FROM fivetran_ingest.auth0.users a0_users
    INNER JOIN fivetran_ingest.auth0.user_identities a0_user_ids
        ON a0_users.id = a0_user_ids.users_id
    WHERE
        a0_user_ids.connection = 'Username-Password-Authentication'
        AND a0_user_ids._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
    -- A handful of Auth0 users (9 as of this writing) have 2+ identity rows
    -- for the same connection type; dedupe to avoid fanning out joins below.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a0_users.username ORDER BY a0_user_ids.index) = 1
),

ldap_primary AS (
    SELECT
        uid AS ldap_username,
        mail AS ldap_primary_email
    FROM analytics_dev.dev_eric.ldap_users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY _sdc_extracted_at DESC) = 1
),

platform_rows_with_matches AS (
    SELECT
        platform_alt_emails.sfid,
        platform_alt_emails.platform_username,
        platform_alt_emails.email,
        platform_alt_emails.is_primary,
        (LOWER(platform_alt_emails.email) = LOWER(auth0_primary.auth0_primary_email)) AS matches_auth0,
        (LOWER(platform_alt_emails.email) = LOWER(ldap_primary.ldap_primary_email)) AS matches_ldap
    FROM platform_alt_emails
    LEFT JOIN auth0_primary
        ON LOWER(platform_alt_emails.platform_username) = LOWER(auth0_primary.auth0_username)
    LEFT JOIN ldap_primary
        ON LOWER(platform_alt_emails.platform_username) = LOWER(ldap_primary.ldap_username)
),

platform_primary_candidates AS (
    SELECT
        sfid,
        platform_username,
        MAX(CASE WHEN is_primary THEN email END) AS flagged_primary_email,
        MIN(email) AS lone_email,
        COUNT(*) AS total_rows,
        COUNT_IF(is_primary) AS primary_row_count,
        MAX(CASE WHEN matches_auth0 AND matches_ldap THEN email END) AS matched_both_email
    FROM platform_rows_with_matches
    GROUP BY sfid, platform_username
),

platform_primary AS (
    SELECT
        sfid,
        platform_username,
        matched_both_email,
        primary_row_count,
        CASE
            WHEN primary_row_count = 1 THEN flagged_primary_email
            WHEN primary_row_count = 0 AND total_rows = 1 THEN lone_email
            ELSE NULL
        END AS flagged_or_lone_email,
        (primary_row_count = 0 AND total_rows > 1) AS ambiguous_no_primary
    FROM platform_primary_candidates
),

joined AS (
    SELECT
        platform_primary.platform_username,
        CASE
            WHEN ldap_primary.ldap_username IS NULL THEN 'MISSING_FROM_LDAP'
            WHEN auth0_primary.auth0_username IS NULL THEN 'MISSING_FROM_AUTH0'
            WHEN platform_primary.matched_both_email IS NOT NULL
                AND platform_primary.primary_row_count >= 2
                THEN 'MULTIPLE_PRIMARY_RESOLVED_BY_MATCH'
            WHEN platform_primary.matched_both_email IS NOT NULL
                AND LOWER(platform_primary.matched_both_email) = LOWER(platform_primary.flagged_or_lone_email)
                THEN 'ALIGNED'
            WHEN platform_primary.matched_both_email IS NOT NULL
                THEN 'WRONG_PRIMARY_FLAG'
            WHEN platform_primary.primary_row_count >= 2 THEN 'AMBIGUOUS_MULTIPLE_PRIMARY'
            WHEN platform_primary.ambiguous_no_primary THEN 'AMBIGUOUS_NO_PRIMARY'
            WHEN platform_primary.flagged_or_lone_email IS NULL THEN 'UNKNOWN'
            WHEN LOWER(platform_primary.flagged_or_lone_email) != LOWER(ldap_primary.ldap_primary_email)
                AND LOWER(platform_primary.flagged_or_lone_email) != LOWER(auth0_primary.auth0_primary_email)
                THEN 'PLATFORM_OUT_OF_SYNC_WITH_BOTH'
            WHEN LOWER(platform_primary.flagged_or_lone_email) != LOWER(ldap_primary.ldap_primary_email)
                THEN 'PLATFORM_OUT_OF_SYNC_WITH_LDAP'
            WHEN LOWER(platform_primary.flagged_or_lone_email) != LOWER(auth0_primary.auth0_primary_email)
                THEN 'PLATFORM_OUT_OF_SYNC_WITH_AUTH0'
            ELSE 'UNKNOWN'
        END AS alignment_status
    FROM platform_primary
    LEFT JOIN ldap_primary
        ON LOWER(platform_primary.platform_username) = LOWER(ldap_primary.ldap_username)
    LEFT JOIN auth0_primary
        ON LOWER(platform_primary.platform_username) = LOWER(auth0_primary.auth0_username)
    LEFT JOIN analytics_dev.dev_eric.lfid_tombstone_prod tombstones
        ON LOWER(platform_primary.platform_username) = tombstones.username
    WHERE tombstones.username IS NULL
),

-- Rank usernames within each bucket so we can pick 5 arbitrary but
-- deterministic sample usernames per alignment_status for spot-checking.
joined_ranked AS (
    SELECT
        alignment_status,
        platform_username,
        ROW_NUMBER() OVER (PARTITION BY alignment_status ORDER BY platform_username) AS rn
    FROM joined
),

bucket_counts AS (
    SELECT alignment_status, COUNT(*) AS user_count
    FROM joined
    GROUP BY alignment_status
),

bucket_samples AS (
    SELECT
        alignment_status,
        MAX(CASE WHEN rn = 1 THEN platform_username END) AS sample_username_1,
        MAX(CASE WHEN rn = 2 THEN platform_username END) AS sample_username_2,
        MAX(CASE WHEN rn = 3 THEN platform_username END) AS sample_username_3,
        MAX(CASE WHEN rn = 4 THEN platform_username END) AS sample_username_4,
        MAX(CASE WHEN rn = 5 THEN platform_username END) AS sample_username_5
    FROM joined_ranked
    WHERE rn <= 5
    GROUP BY alignment_status
)

SELECT
    bucket_counts.alignment_status,
    bucket_counts.user_count,
    bucket_samples.sample_username_1,
    bucket_samples.sample_username_2,
    bucket_samples.sample_username_3,
    bucket_samples.sample_username_4,
    bucket_samples.sample_username_5
FROM bucket_counts
INNER JOIN bucket_samples
    ON bucket_counts.alignment_status = bucket_samples.alignment_status
ORDER BY bucket_counts.user_count DESC
;
