-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT

-- LFXV2-2662: Generic username resolver for all remediation apply scripts.
--
-- Takes a comma-separated list of usernames (from the matrixed_buckets samples
-- column) and outputs all columns any apply script might need. Each apply
-- script calls this with its own username list and picks the columns it needs.
--
-- Output columns:
--   platform_username      — LFID username
--   contact_sfid           — Platform merged_user SFID
--   auth0_id               — Auth0 user ID (e.g. auth0|username)
--   auth0_email            — current Auth0 primary email
--   ldap_email             — current LDAP mail attribute
--   flagged_primary_email  — Platform row flagged primary_email__c=true
--   flagged_email_sfid     — SFID of the flagged-primary alternate_email__c row
--   matching_email_sfid    — SFID of the Platform row matching Auth0 (non-primary)
--   flagged_email_other_auth0_id — Auth0 user ID of a DIFFERENT account that
--                            owns the flagged primary email (conflict: do not
--                            push this email to Auth0; empty when no conflict)
--   flagged_email_other_ldap_uid — LDAP uid of a DIFFERENT account that owns
--                            the flagged primary email (catches LDAP/Drupal
--                            accounts with no Auth0 row; Auth0 checks LDAP
--                            for email availability and rejects the push)
--
-- Run with:
--   rm -f resolved_usernames.csv && \
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false \
--     -o output_format=csv -o output_file=resolved_usernames.csv \
--     -D USERNAMES="user1,user2,user3" \
--     -f scripts/lfxv2_2662_resolve_usernames.sql

WITH username_list AS (
    SELECT TRIM(value) AS username
    FROM TABLE(SPLIT_TO_TABLE('&USERNAMES', ','))
),

platform_alt_emails AS (
    SELECT
        platform_users.sfid AS contact_sfid,
        platform_users.username__c AS platform_username,
        platform_emails.sfid AS email_sfid,
        platform_emails.alternate_email_address__c AS email,
        platform_emails.primary_email__c AS is_primary
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    INNER JOIN username_list u
        ON LOWER(platform_users.username__c) = LOWER(u.username)
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
),

auth0_primary AS (
    SELECT
        a0_users.id AS auth0_id,
        a0_users.username AS auth0_username,
        a0_users.email AS auth0_email
    FROM fivetran_ingest.auth0.users a0_users
    INNER JOIN fivetran_ingest.auth0.user_identities a0_user_ids
        ON a0_users.id = a0_user_ids.users_id
    INNER JOIN username_list u
        ON LOWER(a0_users.username) = LOWER(u.username)
    WHERE
        a0_user_ids.connection = 'Username-Password-Authentication'
        AND a0_user_ids._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a0_users.username ORDER BY a0_user_ids.index) = 1
),

ldap_primary AS (
    SELECT
        uid AS ldap_username,
        mail AS ldap_email
    FROM analytics_dev.dev_eric.ldap_users
    INNER JOIN username_list u
        ON LOWER(uid) = LOWER(u.username)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY _sdc_extracted_at DESC) = 1
),

-- Per-user: flagged primary row and the row matching Auth0 (if different).
per_user AS (
    SELECT
        u.username AS platform_username,
        MAX(p_any.contact_sfid) AS contact_sfid,
        a.auth0_id,
        a.auth0_email,
        l.ldap_email,
        MAX(CASE WHEN p_any.is_primary THEN p_any.email END) AS flagged_primary_email,
        MAX(CASE WHEN p_any.is_primary THEN p_any.email_sfid END) AS flagged_email_sfid,
        MAX(CASE WHEN NOT p_any.is_primary
                      AND LOWER(p_any.email) = LOWER(a.auth0_email)
                 THEN p_any.email_sfid END) AS matching_email_sfid
    FROM username_list u
    LEFT JOIN auth0_primary a
        ON LOWER(u.username) = LOWER(a.auth0_username)
    LEFT JOIN ldap_primary l
        ON LOWER(u.username) = LOWER(l.ldap_username)
    LEFT JOIN platform_alt_emails p_any
        ON LOWER(u.username) = LOWER(p_any.platform_username)
    GROUP BY u.username, a.auth0_id, a.auth0_email, l.ldap_email
),

-- Conflict check: does the flagged primary email belong to a different
-- Auth0 account (Username-Password-Authentication connection)?
flagged_email_conflicts AS (
    SELECT
        pu.platform_username,
        MAX(a0_other.id) AS flagged_email_other_auth0_id
    FROM per_user pu
    INNER JOIN fivetran_ingest.auth0.users a0_other
        ON LOWER(a0_other.email) = LOWER(pu.flagged_primary_email)
    INNER JOIN fivetran_ingest.auth0.user_identities a0_other_ids
        ON a0_other.id = a0_other_ids.users_id
    WHERE
        a0_other_ids.connection = 'Username-Password-Authentication'
        AND a0_other_ids._fivetran_deleted = FALSE
        AND a0_other._fivetran_deleted = FALSE
        AND LOWER(a0_other.username) != LOWER(pu.platform_username)
    GROUP BY pu.platform_username
),

-- Conflict check 2: does the flagged primary email belong to a different
-- LDAP account? Catches LDAP/Drupal-only accounts with no Auth0 row; Auth0
-- validates email availability against LDAP/Identity before an email update
-- and rejects the push.
flagged_email_ldap_conflicts AS (
    SELECT
        pu.platform_username,
        MAX(ldap_other.uid) AS flagged_email_other_ldap_uid
    FROM per_user pu
    INNER JOIN (
        SELECT uid, mail
        FROM analytics_dev.dev_eric.ldap_users
        QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY _sdc_extracted_at DESC) = 1
    ) ldap_other
        ON LOWER(ldap_other.mail) = LOWER(pu.flagged_primary_email)
    WHERE LOWER(ldap_other.uid) != LOWER(pu.platform_username)
    GROUP BY pu.platform_username
)

SELECT
    pu.platform_username,
    pu.contact_sfid,
    pu.auth0_id,
    pu.auth0_email,
    pu.ldap_email,
    pu.flagged_primary_email,
    pu.flagged_email_sfid,
    pu.matching_email_sfid,
    c.flagged_email_other_auth0_id,
    lc.flagged_email_other_ldap_uid
FROM per_user pu
LEFT JOIN flagged_email_conflicts c
    ON pu.platform_username = c.platform_username
LEFT JOIN flagged_email_ldap_conflicts lc
    ON pu.platform_username = lc.platform_username
ORDER BY pu.platform_username
;
