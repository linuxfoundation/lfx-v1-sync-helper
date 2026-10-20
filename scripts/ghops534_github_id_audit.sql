-- Copyright The Linux Foundation and each contributor to LFX.
-- SPDX-License-Identifier: MIT

-- lfx-self-serve-ops#534: GithubID audit (Snowflake).
--
-- Classifies every platform user with a username into one of:
--   NOT_SET        - neither v1 nor Auth0 has a GithubID/github identity
--                    (not part of this ticket's scope; reported separately
--                    from OK so "no drift found" isn't inflated by users
--                    who simply never set a GitHub account)
--   MISSING_IN_V1  - Auth0 has >=1 github identity, v1 GithubID is empty
--   DIFFERS        - v1 GithubID is set, Auth0 has >=1 github identity, but
--                    none of them match v1 (see "match-any" note below)
--   V1_ONLY        - v1 has a GithubID but Auth0 has no github identity
--   OK             - v1 GithubID is set AND matches at least one linked
--                    Auth0 github identity
--
-- Auth0 supports linking more than one GitHub account to the same user.
-- DIFFERS/OK is therefore a match-any comparison: v1's value is checked
-- against every linked github identity's `nickname`, not just the most
-- recently updated one — matching an older linked identity still counts as
-- OK, since that avoids flagging a real (if stale) match as a fix-up
-- candidate. The `auth0_github_id` column still reports the single
-- most-recently-updated identity (the value a --apply fix-up would write),
-- and `auth0_github_identity_count` reports how many are linked so a
-- MISSING_IN_V1/DIFFERS row touching a multi-identity user can be
-- distinguished from a single-identity one during review.
--
-- v1 GITHUB_ID__C is free text, not a constrained/validated field: values
-- are commonly a full profile URL (https://github.com/<username>) but not
-- universally, while Auth0's github identity profile data stores the bare
-- username as `nickname`. Comparison strips a leading http(s)://[www.]
-- github.com/ prefix where present (matched case-insensitively, and with any
-- trailing slash also removed) and compares case-insensitively (GitHub
-- usernames are case-insensitive) before classifying.
--
-- The v1-username-to-Auth0-username join is likewise case-insensitive, so a
-- case-mismatched pair (e.g. v1 "Alice" vs. Auth0 "alice") still matches up
-- instead of silently hiding real drift behind NOT_SET/V1_ONLY. This only
-- affects which rows join for classification — it has no bearing on what a
-- --apply fix-up would write, which is always the literal Auth0 nickname.
--
-- Run with:
--   rm -f ghops534_github_id_audit.csv && \
--   snowsql --accountname "" --username "" \
--     --warehouse "" --rolename "" --private-key-path "" \
--     -o friendly=false -o header=true -o timing=false \
--     -o output_format=csv -o output_file=ghops534_github_id_audit.csv \
--     -f scripts/ghops534_github_id_audit.sql
--
-- CSV columns:
--   platform_username,contact_sfid,v1_github_id,auth0_github_id,
--   auth0_identity_updated_at,auth0_github_identity_count,category

WITH github_identities AS (
    SELECT
        ids.users_id,
        ids.index AS identity_index,
        profile.name,
        profile.value::VARCHAR AS value
    FROM fivetran_ingest.auth0.user_identities ids
    INNER JOIN fivetran_ingest.auth0.user_profile_data profile
        ON ids.users_id = profile.users_id
        AND ids.index = profile.user_identities_index
    WHERE
        ids.provider = 'github'
        AND ids._fivetran_deleted = FALSE
        AND profile._fivetran_deleted = FALSE
),

github_identity_nicknames AS (
    SELECT
        nickname.users_id,
        nickname.identity_index,
        nickname.value AS github_nickname,
        updated_at.value AS identity_updated_at
    FROM github_identities nickname
    LEFT JOIN github_identities updated_at
        ON nickname.users_id = updated_at.users_id
        AND nickname.identity_index = updated_at.identity_index
        AND updated_at.name = 'updated_at'
    WHERE nickname.name = 'nickname'
),

-- The single github identity a --apply fix-up would write: most recently
-- updated wins. Classification itself uses ALL of a user's linked github
-- identities (see auth0_github_match below), not just this one.
auth0_github_latest AS (
    SELECT
        users_id,
        github_nickname,
        identity_updated_at
    FROM github_identity_nicknames
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY users_id
        ORDER BY identity_updated_at DESC NULLS LAST, identity_index DESC
    ) = 1
),

platform_users AS (
    SELECT
        sfid AS contact_sfid,
        username__c AS platform_username,
        github_id__c AS v1_github_id,
        -- Normalize the same way the User Service controller does on write:
        -- strip a leading http(s)://[www.]github.com/ prefix when present
        -- (matched case-insensitively — GitHub's own hostname and the
        -- http(s) scheme aren't case-sensitive) and any trailing slash left
        -- over from a profile URL. GITHUB_ID__C is free text and commonly
        -- (but not universally) holds a full profile URL rather than a bare
        -- username.
        RTRIM(
            REGEXP_REPLACE(
                TRIM(github_id__c), '^https?://(www\\.)?github\\.com/', '', 1, 0, 'i'
            ),
            '/'
        ) AS v1_github_username
    FROM fivetran_ingest.sfdc_connector_prod_salesforce.merged_user
    WHERE
        _fivetran_deleted = FALSE
        AND username__c IS NOT NULL
        AND username__c <> ''
),

-- One Auth0 users_id per username/password account, used to key the
-- platform_username -> Auth0 users_id lookup before joining github identities.
auth0_accounts AS (
    SELECT
        a0_users.id AS users_id,
        a0_users.username AS auth0_username
    FROM fivetran_ingest.auth0.users a0_users
    INNER JOIN fivetran_ingest.auth0.user_identities a0_ids
        ON a0_users.id = a0_ids.users_id
    WHERE
        a0_ids.connection = 'Username-Password-Authentication'
        AND a0_ids._fivetran_deleted = FALSE
        AND a0_users._fivetran_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a0_users.username ORDER BY a0_ids.index) = 1
),

-- Match-any: does v1's (normalized) value equal ANY of the user's linked
-- github identity nicknames, and how many identities does the user have.
auth0_github_match AS (
    SELECT
        auth0_accounts.auth0_username AS platform_username,
        COUNT(github_identity_nicknames.identity_index) AS identity_count,
        BOOLOR_AGG(
            LOWER(github_identity_nicknames.github_nickname) = LOWER(platform_users.v1_github_username)
        ) AS v1_matches_any_identity
    FROM platform_users
    INNER JOIN auth0_accounts
        ON LOWER(auth0_accounts.auth0_username) = LOWER(platform_users.platform_username)
    LEFT JOIN github_identity_nicknames
        ON github_identity_nicknames.users_id = auth0_accounts.users_id
    GROUP BY auth0_accounts.auth0_username
)

SELECT
    platform_users.platform_username,
    platform_users.contact_sfid,
    platform_users.v1_github_id,
    auth0_github_latest.github_nickname AS auth0_github_id,
    auth0_github_latest.identity_updated_at AS auth0_identity_updated_at,
    COALESCE(auth0_github_match.identity_count, 0) AS auth0_github_identity_count,
    CASE
        WHEN COALESCE(auth0_github_match.identity_count, 0) = 0 AND COALESCE(platform_users.v1_github_username, '') = '' THEN 'NOT_SET'
        WHEN COALESCE(auth0_github_match.identity_count, 0) = 0 THEN 'V1_ONLY'
        WHEN COALESCE(platform_users.v1_github_username, '') = '' THEN 'MISSING_IN_V1'
        WHEN auth0_github_match.v1_matches_any_identity THEN 'OK'
        ELSE 'DIFFERS'
    END AS category
FROM platform_users
LEFT JOIN auth0_accounts
    ON LOWER(auth0_accounts.auth0_username) = LOWER(platform_users.platform_username)
LEFT JOIN auth0_github_latest
    ON auth0_github_latest.users_id = auth0_accounts.users_id
LEFT JOIN auth0_github_match
    ON LOWER(auth0_github_match.platform_username) = LOWER(platform_users.platform_username)
ORDER BY category, platform_users.platform_username;
