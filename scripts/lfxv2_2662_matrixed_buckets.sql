-- LFXV2-2662: Revised matrixed bucketing (scratch, not committed).
-- Base alignment_status (as in scripts/lfxv2_2662_primary_email_alignment.sql)
-- crossed with drilldown signals: blocked (Auth0 AND Drupal), gdpr markers,
-- rewrite markers, platform-primary verified flag, and Auth0-vs-LDAP
-- timestamp direction. 50 sample usernames per (status, drilldown) cell.
--
-- IMPORTANT: Auth0's "blocked" flag republishes the underlying Drupal/LDAP
-- profile's active=false state, but that only happens for users who HAVE an
-- Auth0 account. Users classified MISSING_FROM_AUTH0 have no Auth0 record at
-- all to carry a "blocked" flag, but many are still Drupal-inactive (e.g.
-- rsguhr, status=0 in lf_identity_users) -- surfaced separately here as
-- drupal_blocked so those don't silently read as "no BLOCKED signal".
--
-- Rewrite markers are split into two kinds, per manual review:
--   - EXT ("+old", an address extension/plus-tag): does NOT change the
--     actual mailbox -- e.g. a "+old" tag and the untagged base address
--     both deliver to the same inbox. Benign;
--     several EXT users are still actively logging in with the tagged
--     address years later.
--   - MANGLED ("_old"/"+old" digits aside, or ".old" appended to the
--     DOMAIN): a genuinely different, likely-broken address (e.g.
--     "user@example.com.old" is not a real domain). This is the
--     concerning case that needs remediation, especially when ALIGNED
--     across all three sources (meaning the breakage was never corrected
--     and nobody has a working address on file).
--
-- Run with:
--   rm -f matrixed_buckets.csv && \
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false \
--     -o output_format=csv -o output_file=matrixed_buckets.csv \
--     -f scripts/lfxv2_2662_matrixed_buckets.sql

WITH platform_alt_emails AS (
    SELECT
        platform_users.sfid,
        platform_users.username__c AS platform_username,
        platform_emails.alternate_email_address__c AS email,
        platform_emails.primary_email__c AS is_primary,
        platform_emails.email_verified__c AS is_verified,
        platform_emails.lastmodifieddate AS platform_email_lastmod
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user platform_users
    INNER JOIN fivetran_ingest.sfdc_connector_prod_salesforce.alternate_email__c platform_emails
        ON platform_users.sfid = platform_emails.leadorcontactid
    WHERE
        platform_users._fivetran_deleted = FALSE
        AND platform_emails._fivetran_deleted = FALSE
        AND platform_users.username__c IS NOT NULL
        AND platform_users.username__c <> ''
),

-- Auth0 users with gdpr/deleted markers in user_metadata or app_metadata
-- (key name or value), or in the profile name fields.
auth0_gdpr_meta AS (
    SELECT DISTINCT users_id
    FROM (
        SELECT users_id, name, value FROM fivetran_ingest.auth0.user_metadata
        WHERE _fivetran_deleted = FALSE
        UNION ALL
        SELECT users_id, name, value FROM fivetran_ingest.auth0.user_app_metadata
        WHERE _fivetran_deleted = FALSE
    )
    WHERE LOWER(name) LIKE '%gdpr%' OR LOWER(TO_VARCHAR(value)) LIKE '%gdpr%'
),

auth0_primary AS (
    SELECT
        a0_users.username AS auth0_username,
        a0_users.email AS auth0_primary_email,
        a0_users.email_verified AS auth0_email_verified,
        a0_users.blocked AS auth0_blocked,
        a0_users.updated_at AS auth0_updated_at,
        (
            auth0_gdpr_meta.users_id IS NOT NULL
            OR LOWER(COALESCE(a0_users.name, '')) LIKE '%gdpr%'
            OR LOWER(COALESCE(a0_users.name, '')) LIKE '%deleted%'
        ) AS auth0_gdpr_marker
    FROM fivetran_ingest.auth0.users a0_users
    INNER JOIN fivetran_ingest.auth0.user_identities a0_user_ids
        ON a0_users.id = a0_user_ids.users_id
    LEFT JOIN auth0_gdpr_meta
        ON a0_users.id = auth0_gdpr_meta.users_id
    WHERE
        a0_user_ids.connection = 'Username-Password-Authentication'
        AND a0_user_ids._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a0_users.username ORDER BY a0_user_ids.index) = 1
),

ldap_primary AS (
    SELECT
        uid AS ldap_username,
        mail AS ldap_primary_email,
        TRY_TO_TIMESTAMP_NTZ(modifytimestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ldap_modified_at,
        (
            LOWER(COALESCE(cn, '')) LIKE '%gdpr%'
            OR LOWER(COALESCE(givenname, '')) LIKE '%gdpr%'
            OR LOWER(COALESCE(sn, '')) LIKE '%gdpr%'
        ) AS ldap_gdpr_marker
    FROM analytics_dev.dev_eric.ldap_users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY _sdc_extracted_at DESC) = 1
),

-- Drupal (LF Identity) account status -- status=0 means Drupal-blocked.
-- This is the same underlying flag that republishes into Auth0's "blocked"
-- field for users who have an Auth0 account, but it's the ONLY blocked
-- signal available for users with no Auth0 account at all.
drupal_status AS (
    SELECT
        name AS drupal_username,
        (status = 0) AS drupal_blocked
    FROM analytics_dev.dev_eric.lf_identity_users
),

platform_rows_with_matches AS (
    SELECT
        platform_alt_emails.sfid,
        platform_alt_emails.platform_username,
        platform_alt_emails.email,
        platform_alt_emails.is_primary,
        platform_alt_emails.is_verified,
        platform_alt_emails.platform_email_lastmod,
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
        MAX(CASE WHEN matches_auth0 AND matches_ldap THEN email END) AS matched_both_email,
        BOOLOR_AGG(CASE WHEN is_primary THEN COALESCE(is_verified, FALSE) ELSE FALSE END) AS flagged_primary_verified,
        MAX(CASE WHEN NOT is_primary THEN COALESCE(is_verified, FALSE) END) AS lone_verified,
        MAX(CASE WHEN is_primary THEN platform_email_lastmod END) AS flagged_primary_lastmod,
        MAX(CASE WHEN NOT is_primary THEN platform_email_lastmod END) AS lone_lastmod
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
        CASE
            WHEN primary_row_count = 1 THEN flagged_primary_verified
            WHEN primary_row_count = 0 AND total_rows = 1 THEN COALESCE(lone_verified, FALSE)
            ELSE NULL
        END AS platform_primary_verified,
        CASE
            WHEN primary_row_count = 1 THEN flagged_primary_lastmod
            WHEN primary_row_count = 0 AND total_rows = 1 THEN lone_lastmod
            ELSE NULL
        END AS platform_primary_lastmod,
        (primary_row_count = 0 AND total_rows > 1) AS ambiguous_no_primary
    FROM platform_primary_candidates
),

joined AS (
    SELECT
        platform_primary.platform_username,
        -- EXT: "+old" or "+disabled" address extension (plus-tag) in the
        -- local part only -- does not change the underlying mailbox.
        (
            platform_primary.flagged_or_lone_email IS NOT NULL
            AND REGEXP_LIKE(LOWER(SPLIT_PART(platform_primary.flagged_or_lone_email, '@', 1)), '.*\\+(old|disabled)[0-9]*$')
        ) AS old_ext_platform,
        (
            auth0_primary.auth0_primary_email IS NOT NULL
            AND REGEXP_LIKE(LOWER(SPLIT_PART(auth0_primary.auth0_primary_email, '@', 1)), '.*\\+(old|disabled)[0-9]*$')
        ) AS old_ext_auth0,
        (
            ldap_primary.ldap_primary_email IS NOT NULL
            AND REGEXP_LIKE(LOWER(SPLIT_PART(ldap_primary.ldap_primary_email, '@', 1)), '.*\\+(old|disabled)[0-9]*$')
        ) AS old_ext_ldap,
        -- MANGLED: "_old" or "_disabled" in the local part, or ".old"
        -- appended to the domain -- a genuinely different (and likely
        -- broken) address.
        (
            platform_primary.flagged_or_lone_email IS NOT NULL AND (
                REGEXP_LIKE(LOWER(SPLIT_PART(platform_primary.flagged_or_lone_email, '@', 1)), '.*_(old|disabled)[0-9]*$')
                OR REGEXP_LIKE(LOWER(SPLIT_PART(platform_primary.flagged_or_lone_email, '@', 2)), '.*\\.old[0-9]*$')
            )
        ) AS old_mangled_platform,
        (
            auth0_primary.auth0_primary_email IS NOT NULL AND (
                REGEXP_LIKE(LOWER(SPLIT_PART(auth0_primary.auth0_primary_email, '@', 1)), '.*_(old|disabled)[0-9]*$')
                OR REGEXP_LIKE(LOWER(SPLIT_PART(auth0_primary.auth0_primary_email, '@', 2)), '.*\\.old[0-9]*$')
            )
        ) AS old_mangled_auth0,
        (
            ldap_primary.ldap_primary_email IS NOT NULL AND (
                REGEXP_LIKE(LOWER(SPLIT_PART(ldap_primary.ldap_primary_email, '@', 1)), '.*_(old|disabled)[0-9]*$')
                OR REGEXP_LIKE(LOWER(SPLIT_PART(ldap_primary.ldap_primary_email, '@', 2)), '.*\\.old[0-9]*$')
            )
        ) AS old_mangled_ldap,
        -- MANGLED_USERNAME: the platform LFID itself is "_old"/".old"
        -- suffixed (a rename-style deactivation of the username, not the
        -- email). Distinct from a phony/synthetic username -- this
        -- represents a REAL user (e.g. iantivey.old) whose LFID rewrite
        -- likely breaks v1 interaction and alt-email sync. Needs proper
        -- deletion/tombstoning, not just blanking username__c.
        (
            REGEXP_LIKE(LOWER(platform_primary.platform_username), '.*(_|\\.)(old|disabled)[0-9]*$')
        ) AS mangled_username,
        -- PHONY_USERNAME: never a real user -- synthetic/test data (e.g.
        -- itx.test.user.*) or a leaked non-LFID value used as a username
        -- placeholder (e.g. a bare email address, from SCORM/TI DB
        -- leakage). Safe to blank username__c outright; no deletion needed
        -- since there was never a real identity here.
        (
            REGEXP_LIKE(LOWER(platform_primary.platform_username), '^itx\\.test\\.user\\.')
            OR platform_primary.platform_username LIKE '%@%'
        ) AS phony_username,
        COALESCE(auth0_primary.auth0_blocked, FALSE) AS auth0_blocked,
        COALESCE(drupal_status.drupal_blocked, FALSE) AS drupal_blocked,
        (COALESCE(auth0_primary.auth0_gdpr_marker, FALSE) OR COALESCE(ldap_primary.ldap_gdpr_marker, FALSE)
            -- GDPR placeholder emails on Platform (e.g. gdprr298remove@gmail.com, gdpr+xyz@linuxfoundation.org).
            OR (platform_primary.flagged_or_lone_email IS NOT NULL AND (
                REGEXP_LIKE(LOWER(platform_primary.flagged_or_lone_email), 'gdpr.*remove.*@.*')
                OR REGEXP_LIKE(LOWER(platform_primary.flagged_or_lone_email), 'gdpr\\+.*@.*linux.*')
            ))
        ) AS gdpr_marker,
        platform_primary.platform_primary_verified,
        -- For OUT_OF_SYNC_WITH_BOTH: compare platform primary lastmod vs
        -- LDAP modifyTimestamp to decide direction.
        CASE
            WHEN platform_primary.platform_primary_lastmod IS NULL OR ldap_primary.ldap_modified_at IS NULL THEN 'PLAT_TS_UNKNOWN'
            WHEN platform_primary.platform_primary_lastmod > ldap_primary.ldap_modified_at THEN 'NEWER_PLATFORM'
            ELSE 'NEWER_LDAP'
        END AS platform_vs_ldap_ts,
        CASE
            WHEN auth0_primary.auth0_updated_at IS NULL OR ldap_primary.ldap_modified_at IS NULL THEN 'TS_UNKNOWN'
            WHEN CONVERT_TIMEZONE('UTC', auth0_primary.auth0_updated_at)::TIMESTAMP_NTZ > ldap_primary.ldap_modified_at THEN 'NEWER_AUTH0'
            ELSE 'NEWER_LDAP'
        END AS newer_source,
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
    LEFT JOIN drupal_status
        ON LOWER(platform_primary.platform_username) = LOWER(drupal_status.drupal_username)
    LEFT JOIN analytics_dev.dev_eric.lfid_tombstone_prod tombstones
        ON LOWER(platform_primary.platform_username) = tombstones.username
    WHERE tombstones.username IS NULL
),

joined_with_drilldown AS (
    SELECT
        alignment_status,
        platform_username,
        COALESCE(NULLIF(ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
            -- Three-state blocked signal:
            --   BLOCKED = both Auth0 and Drupal blocked (deletion-ready).
            --   AUTH0_BLOCKED = Auth0 blocked, Drupal still active (needs
            --     sync block to Drupal before deletion, or tombstone will
            --     store real email instead of blanking it).
            --   DRUPAL_BLOCKED = Drupal blocked, Auth0 not blocked (or no
            --     Auth0 account). Deletion flow works correctly but Auth0
            --     account may still be live/writable.
            IFF(auth0_blocked AND drupal_blocked, 'BLOCKED', NULL),
            IFF(auth0_blocked AND NOT drupal_blocked, 'AUTH0_BLOCKED', NULL),
            IFF(drupal_blocked AND NOT auth0_blocked, 'DRUPAL_BLOCKED', NULL),
            IFF(gdpr_marker, 'GDPR', NULL),
            IFF(mangled_username, 'MANGLED_USERNAME', NULL),
            IFF(phony_username, 'PHONY_USERNAME', NULL),
            IFF(old_mangled_platform, 'MANGLED_PLATFORM', NULL),
            IFF(old_mangled_auth0, 'MANGLED_AUTH0', NULL),
            IFF(old_mangled_ldap, 'MANGLED_LDAP', NULL),
            IFF(old_ext_platform, 'EXT_PLATFORM', NULL),
            IFF(old_ext_auth0, 'EXT_AUTH0', NULL),
            IFF(old_ext_ldap, 'EXT_LDAP', NULL),
            -- Verified flag on the resolved platform primary: only relevant
            -- for buckets where the platform email disagrees with an
            -- external source.
            CASE
                WHEN alignment_status IN (
                    'WRONG_PRIMARY_FLAG',
                    'PLATFORM_OUT_OF_SYNC_WITH_BOTH',
                    'PLATFORM_OUT_OF_SYNC_WITH_LDAP',
                    'PLATFORM_OUT_OF_SYNC_WITH_AUTH0'
                ) THEN IFF(COALESCE(platform_primary_verified, FALSE), 'PLATFORM_VERIFIED', 'PLATFORM_UNVERIFIED')
            END,
            -- Timestamp direction: only relevant where Auth0 and LDAP
            -- disagree with each other.
            CASE
                WHEN alignment_status IN (
                    'PLATFORM_OUT_OF_SYNC_WITH_AUTH0',
                    'PLATFORM_OUT_OF_SYNC_WITH_LDAP'
                ) THEN newer_source
            END,
            -- Platform vs LDAP timestamp: only relevant where platform
            -- disagrees with BOTH Auth0 and LDAP.
            CASE
                WHEN alignment_status = 'PLATFORM_OUT_OF_SYNC_WITH_BOTH'
                THEN platform_vs_ldap_ts
            END
        ), '+'), ''), 'NONE') AS drilldown
    FROM joined
),

ranked AS (
    SELECT
        alignment_status,
        drilldown,
        platform_username,
        ROW_NUMBER() OVER (PARTITION BY alignment_status, drilldown ORDER BY platform_username) AS rn
    FROM joined_with_drilldown
)

SELECT
    alignment_status,
    drilldown,
    COUNT(*) AS user_count,
    LISTAGG(CASE WHEN rn <= 650 THEN platform_username END, ',') WITHIN GROUP (ORDER BY rn) AS samples
FROM ranked
GROUP BY alignment_status, drilldown
ORDER BY alignment_status, user_count DESC
;
