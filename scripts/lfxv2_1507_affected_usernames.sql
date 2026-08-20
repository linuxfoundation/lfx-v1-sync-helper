-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT

-- LFXV2-1507: Affected-username extraction for the phased alternate-email
-- backfill. Given a comma-separated list of foundation slugs (one onboarding
-- wave from waves.md), finds every LFID username with any of these engagement
-- types on the foundation or any project rolling up to it via the project
-- spine:
--
--   committee    — ANALYTICS.SILVER_DIM.SILVER_DIM_COMMITTEE_MEMBERS
--   meeting      — ANALYTICS.SILVER_FACT.SILVER_FACT_MEETING_ATTENDANCE
--                  (all invitees, past and upcoming)
--   mailing_list — ANALYTICS.SILVER_FACT.SILVER_FACT_MAILING_LISTS
--                  (current subscribers; email-keyed, resolved to usernames
--                  via platform alternate_email__c rows)
--   key_contact  — ANALYTICS.SILVER_DIM._SILVER_DIM_CORPORATE_KEY_CONTACTS
--                  (contacts of currently-active member accounts; also
--                  email-keyed, resolved the same way)
--
-- Project scope is expanded with ANALYTICS.SILVER_DIM.PROJECT_SPINE: the
-- foundation slugs themselves plus every base project whose spine maps it to
-- one of the slugs. Known spine anomalies (e.g. Jupyter) are handled upstream
-- by listing both slugs in waves.md — no anomaly logic here.
--
-- Output columns:
--   platform_username — LFID username (lowercased)
--   contact_sfid      — platform merged_user SFID (when resolvable)
--   engagements       — comma-separated engagement types found
--   foundations       — comma-separated foundation slugs matched
--
-- Run with (see lfxv2_1507_wave_usernames.sh for the per-wave wrapper):
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false \
--     -o output_format=csv -o output_file=wave_usernames.csv \
--     -D SLUGS="slug1,slug2" \
--     -f scripts/lfxv2_1507_affected_usernames.sql

WITH slug_list AS (
    SELECT LOWER(TRIM(value)) AS slug
    FROM TABLE(SPLIT_TO_TABLE('&SLUGS', ','))
),

-- Foundation roots plus all projects rolling up to them via the spine. The
-- spine includes the self-mapping row (hierarchy_level 1), but union the
-- root explicitly in case a slug has no spine rows at all.
scoped_projects AS (
    SELECT DISTINCT
        spine.base_project_id AS project_id,
        s.slug AS foundation_slug
    FROM analytics.silver_dim.project_spine spine
    INNER JOIN slug_list s
        ON LOWER(spine.mapped_project_slug) = s.slug

    UNION

    SELECT DISTINCT
        spine.base_project_id,
        s.slug
    FROM analytics.silver_dim.project_spine spine
    INNER JOIN slug_list s
        ON LOWER(spine.base_project_slug) = s.slug
),

-- Platform email rows: the platform DB (merged_user + alternate_email__c)
-- is the source of truth the backfill reads from, so email-keyed engagement
-- sources are resolved to usernames through it.
platform_emails AS (
    SELECT
        LOWER(platform_emails.alternate_email_address__c) AS email,
        platform_users.username__c AS platform_username,
        platform_users.sfid AS contact_sfid
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
        AND platform_emails.active__c = TRUE
        AND platform_users.username__c IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(platform_emails.alternate_email_address__c)
        -- Prefer the flagged-primary owner when an email appears on
        -- multiple accounts.
        ORDER BY platform_emails.primary_email__c DESC, platform_users.sfid
    ) = 1
),

-- Username -> SFID lookup for username-keyed engagement sources.
platform_usernames AS (
    SELECT
        LOWER(username__c) AS platform_username_lower,
        sfid AS contact_sfid
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user
    WHERE
        _fivetran_deleted = FALSE
        AND username__c IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(username__c) ORDER BY sfid
    ) = 1
),

committee_engagement AS (
    SELECT DISTINCT
        LOWER(cm.user_name) AS platform_username,
        cm.user_id AS contact_sfid,
        'committee' AS engagement,
        sp.foundation_slug
    FROM analytics.silver_dim.silver_dim_committee_members cm
    INNER JOIN scoped_projects sp
        ON cm.project_id = sp.project_id
    WHERE cm.user_name IS NOT NULL
),

meeting_engagement AS (
    SELECT DISTINCT
        LOWER(ma.invitee_lf_sso) AS platform_username,
        ma.invitee_lf_user_id AS contact_sfid,
        'meeting' AS engagement,
        sp.foundation_slug
    FROM analytics.silver_fact.silver_fact_meeting_attendance ma
    INNER JOIN scoped_projects sp
        ON ma.project_id = sp.project_id
    WHERE ma.invitee_lf_sso IS NOT NULL
),

mailing_list_engagement AS (
    SELECT DISTINCT
        LOWER(pe.platform_username) AS platform_username,
        pe.contact_sfid,
        'mailing_list' AS engagement,
        sp.foundation_slug
    FROM analytics.silver_fact.silver_fact_mailing_lists ml
    INNER JOIN scoped_projects sp
        ON ml.project_id = sp.project_id
    INNER JOIN platform_emails pe
        ON LOWER(ml.member_email) = pe.email
    WHERE ml.is_subscribed_current = TRUE
),

key_contact_engagement AS (
    SELECT DISTINCT
        LOWER(pe.platform_username) AS platform_username,
        pe.contact_sfid,
        'key_contact' AS engagement,
        sp.foundation_slug
    FROM analytics.silver_dim._silver_dim_corporate_key_contacts kc
    INNER JOIN scoped_projects sp
        ON kc.project_id = sp.project_id
    INNER JOIN platform_emails pe
        ON LOWER(kc.user_email) = pe.email
    WHERE kc.is_currently_active = TRUE
),

all_engagement AS (
    SELECT * FROM committee_engagement
    UNION ALL
    SELECT * FROM meeting_engagement
    UNION ALL
    SELECT * FROM mailing_list_engagement
    UNION ALL
    SELECT * FROM key_contact_engagement
)

SELECT
    e.platform_username,
    COALESCE(MAX(e.contact_sfid), MAX(pu.contact_sfid)) AS contact_sfid,
    LISTAGG(DISTINCT e.engagement, ',') WITHIN GROUP (ORDER BY e.engagement)
        AS engagements,
    LISTAGG(DISTINCT e.foundation_slug, ',')
        WITHIN GROUP (ORDER BY e.foundation_slug) AS foundations
FROM all_engagement e
LEFT JOIN platform_usernames pu
    ON e.platform_username = pu.platform_username_lower
GROUP BY e.platform_username
ORDER BY e.platform_username
;
