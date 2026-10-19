-- lfx-self-serve-ops#534: GithubID audit (Snowflake).
--
-- Classifies every platform user with a username into one of:
--   MISSING_IN_V1  - Auth0 has a github identity, v1 GithubID is empty
--   DIFFERS        - Auth0 and v1 disagree on a non-empty GithubID
--   V1_ONLY        - v1 has a GithubID but Auth0 has no github identity
--   OK             - Auth0 and v1 agree (including both empty)
--
-- Expected GithubID selection rule: a user's github identity's `nickname`
-- profileData value, most-recently-updated when more than one github
-- identity exists for the same Auth0 user. Confirm this matches the actual
-- selection rule before relying on MISSING_IN_V1/DIFFERS counts for a bulk
-- fix-up run.
--
-- Run with:
--   rm -f ghops534_github_id_audit.csv && \
--   snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
--     --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
--     -o friendly=false -o header=true -o timing=false \
--     -o output_format=csv -o output_file=ghops534_github_id_audit.csv \
--     -f scripts/ghops534_github_id_audit.sql
--
-- CSV columns:
--   platform_username,contact_sfid,v1_github_id,auth0_github_id,
--   auth0_identity_updated_at,category

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

-- One github identity per Auth0 user: most recently updated wins.
auth0_github AS (
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
        githubid__c AS v1_github_id
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
)

SELECT
    platform_users.platform_username,
    platform_users.contact_sfid,
    platform_users.v1_github_id,
    auth0_github.github_nickname AS auth0_github_id,
    auth0_github.identity_updated_at AS auth0_identity_updated_at,
    CASE
        WHEN COALESCE(auth0_github.github_nickname, '') = '' AND COALESCE(platform_users.v1_github_id, '') = '' THEN 'OK'
        WHEN COALESCE(auth0_github.github_nickname, '') = COALESCE(platform_users.v1_github_id, '') THEN 'OK'
        WHEN COALESCE(platform_users.v1_github_id, '') = '' THEN 'MISSING_IN_V1'
        WHEN COALESCE(auth0_github.github_nickname, '') = '' THEN 'V1_ONLY'
        ELSE 'DIFFERS'
    END AS category
FROM platform_users
LEFT JOIN auth0_accounts
    ON auth0_accounts.auth0_username = platform_users.platform_username
LEFT JOIN auth0_github
    ON auth0_github.users_id = auth0_accounts.users_id
ORDER BY category, platform_users.platform_username;
