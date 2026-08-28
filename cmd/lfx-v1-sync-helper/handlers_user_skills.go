// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import "context"

// syncSkillsToAuth0Fn is the function handleUserSkillsUpdate calls to push a
// user's skill list to Auth0. Swappable in tests.
var syncSkillsToAuth0Fn = syncSkillsToAuth0

// handleUserSkillsUpdate processes salesforce.user_skills insert/update/delete
// events. Unlike most synced tables, it always re-reads the user's full
// current skill list from the v1 platform DB rather than reacting to the
// single changed row: Auth0 stores one whole-field string, so a partial
// update would be meaningless, and re-reading makes this handler naturally
// correct for both additions and removals (including the delete path, which
// routes here via the same lfid lookup rather than needing separate logic).
//
// v1Data is nil for a true hard KV delete with no recoverable payload (see
// handleKVDelete); without a payload there is no lfid to resolve, so this is
// a no-op — any resulting drift is corrected by the next --backfill-profiles
// pass, which re-reads skills for every user.
// Returns true if the operation should be retried, false otherwise.
func handleUserSkillsUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	if v1Data == nil {
		logger.With("key", key).WarnContext(ctx, "user_skills hard-deleted with no payload, cannot resolve lfid, skipping")
		return false
	}

	lfid, ok := v1Data["lfid"].(string)
	if !ok || normalizeUserIdentifier(lfid) == "" {
		logger.With("key", key).WarnContext(ctx, "user_skills missing lfid, skipping")
		return false
	}

	skillNames, err := getSkillsForUserFn(ctx, lfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "lfid", lfid).
			ErrorContext(ctx, "failed to read v1 skills for user, skipping")
		return false
	}

	auth0UserID := mapUsernameToAuthSub(lfid)

	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()

	auth0User, err := fetchAuth0User(syncCtx, auth0UserID)
	if err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				WarnContext(ctx, "retryable Auth0 error fetching user for skills sync, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
			ErrorContext(ctx, "failed to fetch Auth0 user for skills sync, dropping non-retryable error")
		return false
	}

	if _, err := syncSkillsToAuth0Fn(syncCtx, auth0UserID, auth0User, skillNames, false); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				WarnContext(ctx, "retryable Auth0 error during skills sync, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
			ErrorContext(ctx, "failed to sync skills to Auth0, dropping non-retryable error")
	}
	return false
}

// handleUserSkillsDelete processes a user_skills soft/hard delete. The
// changed row's own ID is irrelevant here — the handler always re-reads the
// user's full current skill list, so a delete is handled by the same lookup
// as an update. rowID is accepted only to match the handleResourceDelete
// dispatch signature.
func handleUserSkillsDelete(ctx context.Context, key, rowID string, v1Data map[string]any) bool {
	_ = rowID
	return handleUserSkillsUpdate(ctx, key, v1Data)
}
