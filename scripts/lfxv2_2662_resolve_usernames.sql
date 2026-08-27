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
--   matching_email_sfid    — SFID of the Platform row matching Auth0 (any primary state)
--   flagged_email_other_auth0_id — Auth0 user ID of a DIFFERENT account that
--                            owns the flagged primary email (conflict: do not
--                            push this email to Auth0; empty when no conflict)
--   flagged_email_other_ldap_uid — LDAP uid of a DIFFERENT account that owns
--                            the flagged primary email (catches LDAP/Drupal
--                            accounts with no Auth0 row; Auth0 checks LDAP
--                            for email availability and rejects the push)
--   meeting_count          — distinct current/upcoming non-cancelled meeting
--                            occurrences the user is registered for
--                            (protection: destructive fixes skip users with a
--                            non-zero count)
--   ti_id                  — Thought Industries (Training LMS) user ID for
--                            this username; empty when not in TI
--   flagged_email_other_ti_id — TI user ID for the OTHER username (LDAP uid,
--                            or Auth0 username when no LDAP conflict) that
--                            owns the flagged email. Non-empty signals the
--                            accounts should NOT be merged: swap/delete is
--                            safe. Empty: LDAP proxy delete if the other
--                            account is Identity-only, or defer to a support
--                            merge if it has an Auth0 row.
--   flagged_email_other_contact_sfid — merged_user contact SFID of a DIFFERENT
--                            contact that owns an alternate_email__c row with
--                            the flagged primary email. Catches stub contacts
--                            without usernames that may need merging.
--   meeting_count_other_sfid — distinct current/upcoming non-cancelled meeting
--                            occurrences registered under the OTHER contact
--                            SFID (protection: merging/deleting a stub contact
--                            with registrations may change ICS calendar
--                            destinations)
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
        MAX(CASE WHEN LOWER(p_any.email) = LOWER(a.auth0_email)
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
        MAX(a0_other.id) AS flagged_email_other_auth0_id,
        MAX(a0_other.username) AS flagged_email_other_auth0_username
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
),

-- Current/upcoming scheduled meeting participation: protection signal —
-- apply scripts skip destructive fixes for users registered for any
-- non-cancelled current or upcoming meeting occurrence.
meeting_counts AS (
    SELECT
        pu.platform_username,
        COUNT(DISTINCT mr.meeting_and_occurrence_id) AS meeting_count
    FROM per_user pu
    INNER JOIN analytics.silver_fact.meeting_registrant mr
        ON (LOWER(mr.username) = LOWER(pu.platform_username)
            OR mr.user_id = pu.contact_sfid)
    WHERE
        mr.is_past = FALSE
        AND mr.is_cancelled = FALSE
    GROUP BY pu.platform_username
),

-- Conflict check 3: does the flagged primary email OR the Auth0 email
-- appear on an alternate_email__c row of a DIFFERENT merged_user contact?
-- Catches stub contacts without usernames that the Auth0/LDAP checks cannot
-- see (typically the Auth0 email living on a username-less stub contact);
-- these may need a contact merge on the Platform side.
flagged_email_other_contacts AS (
    SELECT
        pu.platform_username,
        MAX(mu_other.sfid) AS flagged_email_other_contact_sfid
    FROM per_user pu
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c ae_other
        ON LOWER(ae_other.alternate_email_address__c) IN (
            LOWER(pu.flagged_primary_email), LOWER(pu.auth0_email)
        )
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.merged_user mu_other
        ON mu_other.sfid = ae_other.leadorcontactid
    WHERE
        ae_other._fivetran_deleted = FALSE
        AND mu_other._fivetran_deleted = FALSE
        AND mu_other.sfid != pu.contact_sfid
    GROUP BY pu.platform_username
),

-- Meeting participation registered under the OTHER contact SFID: protection
-- signal — merging or deleting a stub contact with current/upcoming
-- registrations may change ICS calendar destinations.
meeting_counts_other AS (
    SELECT
        oc.platform_username,
        COUNT(DISTINCT mr.meeting_and_occurrence_id) AS meeting_count_other_sfid
    FROM flagged_email_other_contacts oc
    INNER JOIN analytics.silver_fact.meeting_registrant mr
        ON mr.user_id = oc.flagged_email_other_contact_sfid
    WHERE
        mr.is_past = FALSE
        AND mr.is_cancelled = FALSE
    GROUP BY oc.platform_username
),

-- Thought Industries (Training LMS) presence, matched on lowercased
-- externalcustomerid. Deduplicated to latest activity per username.
ti_users AS (
    SELECT
        LOWER(externalcustomerid) AS ti_username,
        id AS ti_id
    FROM census_ingest.ti_redshift.users
    WHERE externalcustomerid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(externalcustomerid)
        ORDER BY lastactiveat DESC NULLS LAST
    ) = 1
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
    lc.flagged_email_other_ldap_uid,
    COALESCE(m.meeting_count, 0) AS meeting_count,
    ti_self.ti_id AS ti_id,
    ti_other.ti_id AS flagged_email_other_ti_id,
    oc.flagged_email_other_contact_sfid,
    COALESCE(mo.meeting_count_other_sfid, 0) AS meeting_count_other_sfid
FROM per_user pu
LEFT JOIN flagged_email_conflicts c
    ON pu.platform_username = c.platform_username
LEFT JOIN flagged_email_ldap_conflicts lc
    ON pu.platform_username = lc.platform_username
LEFT JOIN meeting_counts m
    ON pu.platform_username = m.platform_username
LEFT JOIN ti_users ti_self
    ON ti_self.ti_username = LOWER(pu.platform_username)
LEFT JOIN ti_users ti_other
    ON ti_other.ti_username = LOWER(COALESCE(
        lc.flagged_email_other_ldap_uid,
        c.flagged_email_other_auth0_username
    ))
LEFT JOIN flagged_email_other_contacts oc
    ON pu.platform_username = oc.platform_username
LEFT JOIN meeting_counts_other mo
    ON pu.platform_username = mo.platform_username
ORDER BY pu.platform_username
;
