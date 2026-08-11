-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT

-- LFXV2-2662: Validate primary email sync before alternate email sync
--
-- Ad hoc Snowflake validation comparing secondary/alternate emails between
-- Platform (SFDC) and Auth0. LDAP has no concept of a secondary email, so
-- this comparison is two-way only.
--
-- Auth0's secondary emails live on `connection = 'email'` identities. The
-- actual email address and verified flag are name/value pairs in
-- fivetran_ingest.auth0.user_profile_data, keyed by (users_id,
-- user_identities_index) back to user_identities.index -- this table was
-- added via SUPPORT-41832 specifically to expose these profile attributes;
-- it did not exist when this ticket started.
--
-- Three checks, unioned together with a check_type column:
--   MISSING  - Platform has an alternate email with no matching Auth0
--              'email' connection identity for that user.
--   EXTRA    - Auth0 has an 'email' connection identity with no matching
--              Platform alternate email for that user.
--   CONFLICT - Platform's alternate email address is claimed as the LDAP
--              primary email (`mail`) of a *different* username than the
--              platform user it's attached to. Syncing this as a secondary
--              identity in Auth0 would collide with another account.
--
-- Run with:
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse DBT_DEV --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false -f \
--     analysis/lfxv2_2662_secondary_email_alignment.sql

WITH platform_secondary AS (
    SELECT
        platform_users.sfid,
        platform_users.username__c AS platform_username,
        platform_emails.alternate_email_address__c AS platform_secondary_email,
        platform_emails.verified__c AS platform_secondary_verified
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
        AND platform_emails.active__c = TRUE
        -- Secondary emails are the non-primary rows in alternate_email__c.
        AND platform_emails.primary_email__c = FALSE
        AND platform_users.username__c IS NOT NULL
        AND platform_users.username__c <> ''
        AND platform_emails.alternate_email_address__c IS NOT NULL
),

auth0_secondary AS (
    SELECT
        a0_users.username AS auth0_username,
        MAX(CASE WHEN upd.name = 'email' THEN upd.value::string END) AS auth0_secondary_email,
        MAX(CASE WHEN upd.name = 'email_verified' THEN upd.value::boolean END) AS auth0_secondary_verified
    FROM fivetran_ingest.auth0.user_identities ui
    INNER JOIN fivetran_ingest.auth0.users a0_users
        ON ui.users_id = a0_users.id
    INNER JOIN fivetran_ingest.auth0.user_profile_data upd
        ON ui.users_id = upd.users_id AND ui.index = upd.user_identities_index
    WHERE
        ui.connection = 'email'
        AND ui._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
        AND upd._fivetran_deleted = FALSE
    GROUP BY a0_users.username, ui.index
),

ldap_by_email AS (
    SELECT
        uid AS ldap_username,
        mail AS ldap_email
    FROM analytics_dev.dev_eric.ldap_users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY mail ORDER BY _sdc_extracted_at DESC) = 1
),

active_tombstones AS (
    SELECT DISTINCT username
    FROM analytics_dev.dev_eric.lfid_tombstone_prod
),

missing_from_auth0 AS (
    SELECT
        'MISSING' AS check_type,
        platform_secondary.sfid,
        platform_secondary.platform_username,
        platform_secondary.platform_secondary_email AS email,
        NULL AS conflicting_ldap_username
    FROM platform_secondary
    LEFT JOIN auth0_secondary
        ON LOWER(platform_secondary.platform_username) = LOWER(auth0_secondary.auth0_username)
        AND LOWER(platform_secondary.platform_secondary_email) = LOWER(auth0_secondary.auth0_secondary_email)
    LEFT JOIN active_tombstones
        ON LOWER(platform_secondary.platform_username) = active_tombstones.username
    WHERE
        auth0_secondary.auth0_username IS NULL
        AND active_tombstones.username IS NULL
),

extra_in_auth0 AS (
    SELECT
        'EXTRA' AS check_type,
        NULL AS sfid,
        auth0_secondary.auth0_username AS platform_username,
        auth0_secondary.auth0_secondary_email AS email,
        NULL AS conflicting_ldap_username
    FROM auth0_secondary
    LEFT JOIN platform_secondary
        ON LOWER(auth0_secondary.auth0_username) = LOWER(platform_secondary.platform_username)
        AND LOWER(auth0_secondary.auth0_secondary_email) = LOWER(platform_secondary.platform_secondary_email)
    WHERE platform_secondary.platform_username IS NULL
),

conflicting AS (
    SELECT
        'CONFLICT' AS check_type,
        platform_secondary.sfid,
        platform_secondary.platform_username,
        platform_secondary.platform_secondary_email AS email,
        ldap_by_email.ldap_username AS conflicting_ldap_username
    FROM platform_secondary
    INNER JOIN ldap_by_email
        ON LOWER(platform_secondary.platform_secondary_email) = LOWER(ldap_by_email.ldap_email)
    LEFT JOIN active_tombstones
        ON LOWER(platform_secondary.platform_username) = active_tombstones.username
    WHERE
        LOWER(platform_secondary.platform_username) != LOWER(ldap_by_email.ldap_username)
        AND active_tombstones.username IS NULL
)

SELECT * FROM missing_from_auth0
UNION ALL
SELECT * FROM extra_in_auth0
UNION ALL
SELECT * FROM conflicting
ORDER BY check_type, platform_username
LIMIT 200
;

-- Summary counts by check_type, for reporting scale of the problem.
WITH platform_secondary AS (
    SELECT
        platform_users.sfid,
        platform_users.username__c AS platform_username,
        platform_emails.alternate_email_address__c AS platform_secondary_email
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
        AND platform_emails.active__c = TRUE
        AND platform_emails.primary_email__c = FALSE
        AND platform_users.username__c IS NOT NULL
        AND platform_users.username__c <> ''
        AND platform_emails.alternate_email_address__c IS NOT NULL
),

auth0_secondary AS (
    SELECT
        a0_users.username AS auth0_username,
        MAX(CASE WHEN upd.name = 'email' THEN upd.value::string END) AS auth0_secondary_email
    FROM fivetran_ingest.auth0.user_identities ui
    INNER JOIN fivetran_ingest.auth0.users a0_users
        ON ui.users_id = a0_users.id
    INNER JOIN fivetran_ingest.auth0.user_profile_data upd
        ON ui.users_id = upd.users_id AND ui.index = upd.user_identities_index
    WHERE
        ui.connection = 'email'
        AND ui._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
        AND upd._fivetran_deleted = FALSE
    GROUP BY a0_users.username, ui.index
),

ldap_by_email AS (
    SELECT
        uid AS ldap_username,
        mail AS ldap_email
    FROM analytics_dev.dev_eric.ldap_users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY mail ORDER BY _sdc_extracted_at DESC) = 1
),

active_tombstones AS (
    SELECT DISTINCT username
    FROM analytics_dev.dev_eric.lfid_tombstone_prod
),

missing_from_auth0 AS (
    SELECT 'MISSING' AS check_type
    FROM platform_secondary
    LEFT JOIN auth0_secondary
        ON LOWER(platform_secondary.platform_username) = LOWER(auth0_secondary.auth0_username)
        AND LOWER(platform_secondary.platform_secondary_email) = LOWER(auth0_secondary.auth0_secondary_email)
    LEFT JOIN active_tombstones
        ON LOWER(platform_secondary.platform_username) = active_tombstones.username
    WHERE
        auth0_secondary.auth0_username IS NULL
        AND active_tombstones.username IS NULL
),

extra_in_auth0 AS (
    SELECT 'EXTRA' AS check_type
    FROM auth0_secondary
    LEFT JOIN platform_secondary
        ON LOWER(auth0_secondary.auth0_username) = LOWER(platform_secondary.platform_username)
        AND LOWER(auth0_secondary.auth0_secondary_email) = LOWER(platform_secondary.platform_secondary_email)
    WHERE platform_secondary.platform_username IS NULL
),

conflicting AS (
    SELECT 'CONFLICT' AS check_type
    FROM platform_secondary
    INNER JOIN ldap_by_email
        ON LOWER(platform_secondary.platform_secondary_email) = LOWER(ldap_by_email.ldap_email)
    LEFT JOIN active_tombstones
        ON LOWER(platform_secondary.platform_username) = active_tombstones.username
    WHERE
        LOWER(platform_secondary.platform_username) != LOWER(ldap_by_email.ldap_username)
        AND active_tombstones.username IS NULL
)

SELECT check_type, COUNT(*) AS row_count FROM missing_from_auth0 GROUP BY check_type
UNION ALL
SELECT check_type, COUNT(*) AS row_count FROM extra_in_auth0 GROUP BY check_type
UNION ALL
SELECT check_type, COUNT(*) AS row_count FROM conflicting GROUP BY check_type
ORDER BY row_count DESC
;
