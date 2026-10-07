-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT

-- LFXV2-1507: Full-population cohort extraction for the alternate-email and
-- profile backfill. Selects every v1 user with a usable username who has
-- something to sync to Auth0:
--
--   (a) more than one row in alternate_email__c (alternate emails to link)
--   (b) any v1ToAuth0Fields profile column non-blank
--   (c) accountid set to a real organization (excluding the catch-all
--       "Individual - No Account" orgs)
--   (d) any user_skills row
--
-- Each username appears exactly once. Rows are ordered by most recent
-- activity first, taken as the greater of last_updated_ts and last_login_ts
-- from analytics.silver_dim.users, so the most active users sync first.
-- Users with no recency data sort last.
--
-- Output columns:
--   platform_username — LFID username (original case)
--   contact_sfid      — platform merged_user SFID
--   last_active_ts    — recency sort key (may be empty)
--
-- Run with lfxv2_1507_cohort.sh.

WITH users AS (
    SELECT
        sfid,
        username__c,
        accountid,
        title,
        mailingstreet,
        mailingcity,
        mailingstate,
        mailingcountry,
        mailingpostalcode,
        phone,
        tshirt_size__c,
        photo_url__c,
        timezone__c,
        bio__c
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user
    WHERE
        _fivetran_deleted = FALSE
        AND COALESCE(isdeleted, FALSE) = FALSE
        AND TRIM(COALESCE(username__c, '')) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(username__c) ORDER BY sfid
    ) = 1
),

multi_email AS (
    SELECT leadorcontactid AS sfid
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c
    WHERE _fivetran_deleted = FALSE
    GROUP BY leadorcontactid
    HAVING COUNT(*) > 1
),

skills AS (
    SELECT DISTINCT LOWER(lfid) AS username_lower
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.user_skills
    WHERE _fivetran_deleted = FALSE AND lfid IS NOT NULL
),

recency AS (
    SELECT
        LOWER(lf_username) AS username_lower,
        MAX(GREATEST(
            COALESCE(last_updated_ts, '1970-01-01'::TIMESTAMP_TZ),
            COALESCE(last_login_ts, '1970-01-01'::TIMESTAMP_TZ)
        )) AS last_active_ts
    FROM analytics.silver_dim.users
    WHERE lf_username IS NOT NULL
    GROUP BY LOWER(lf_username)
)

SELECT
    u.username__c AS platform_username,
    u.sfid AS contact_sfid,
    NULLIF(r.last_active_ts, '1970-01-01'::TIMESTAMP_TZ) AS last_active_ts
FROM users u
LEFT JOIN multi_email me ON u.sfid = me.sfid
LEFT JOIN skills s ON LOWER(u.username__c) = s.username_lower
LEFT JOIN recency r ON LOWER(u.username__c) = r.username_lower
WHERE
    me.sfid IS NOT NULL
    OR TRIM(COALESCE(u.title, '')) != ''
    OR TRIM(COALESCE(u.mailingstreet, '')) != ''
    OR TRIM(COALESCE(u.mailingcity, '')) != ''
    OR TRIM(COALESCE(u.mailingstate, '')) != ''
    OR TRIM(COALESCE(u.mailingcountry, '')) != ''
    OR TRIM(COALESCE(u.mailingpostalcode, '')) != ''
    OR TRIM(COALESCE(u.phone, '')) != ''
    OR TRIM(COALESCE(u.tshirt_size__c, '')) != ''
    OR TRIM(COALESCE(u.photo_url__c, '')) != ''
    OR TRIM(COALESCE(u.timezone__c, '')) != ''
    OR TRIM(COALESCE(u.bio__c, '')) != ''
    OR (
        TRIM(COALESCE(u.accountid, '')) != ''
        AND u.accountid NOT IN (
            '0014100001NrJWnAAN',
            '001QP00000d1O8QYAU',
            '001Kc000002THcsIAG'
        )
    )
    OR s.username_lower IS NOT NULL
ORDER BY r.last_active_ts DESC NULLS LAST, LOWER(u.username__c)
;
