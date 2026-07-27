// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// One-shot backfill commands for syncing v1 alternate emails and profiles to Auth0.
//
// Three modes are provided:
//
//   --backfill-alternate-emails [--limit N] [--dry-run]
//     Iterates Auth0 users (Username-Password-Authentication connection only),
//     sorted by updated_at ascending, and links any v1 verified alternate emails
//     that are not yet linked as Auth0 email-connection identities.
//     Stores a cursor (updated_at of last processed user) in the v1-mappings KV
//     bucket under the key "backfill.alternate-emails.cursor" so the run is
//     resumable: re-run with the same --limit to advance the cursor.
//     The cursor uses an inclusive range query ([cursor TO *]), so the last user
//     processed in the previous run will be re-processed on the next run. This is
//     accepted/expected behavior and all operations are idempotent.
//
//   --backfill-profiles [--limit N] [--dry-run]
//     Same Auth0-user-centric iteration, but syncs v1 profile fields
//     (name, title, address, etc.) to Auth0 user_metadata instead.
//     Cursor stored at "backfill.profiles.cursor".
//     Same inclusive-cursor behavior as --backfill-alternate-emails.
//     Replaces the legacy PROFILE_SYNC_BACKFILL env-var approach.
//
//   --sync-user <username> [--dry-run]
//     Performs a single-user sync of both profile and alternate emails.
//     Useful for debugging or re-syncing an individual user without a
//     full backfill run.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/auth0/go-auth0/management"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// backfillAltEmailsCursorKey is the v1-mappings KV key used to store the
	// alternate-emails backfill cursor (updated_at of last processed Auth0 user).
	backfillAltEmailsCursorKey = "backfill.alternate-emails.cursor"

	// backfillProfilesCursorKey is the v1-mappings KV key used to store the
	// profiles backfill cursor.
	backfillProfilesCursorKey = "backfill.profiles.cursor"

	// backfillAuth0Connection is the Auth0 connection filter for backfill iteration.
	// Only Username-Password-Authentication users have v1 platform SFID mappings.
	backfillAuth0Connection = "Username-Password-Authentication"

	// backfillPageSize is the number of Auth0 users fetched per Management API page.
	backfillPageSize = 100

	// backfillCallTimeout bounds each per-user Auth0 and KV operation to stay
	// well under reasonable wall-clock limits.
	backfillCallTimeout = 30 * time.Second
)

// backfillEmailsResult summarises one run of backfillAlternateEmails.
type backfillEmailsResult struct {
	usersProcessed int
	emailsLinked   int
	emailsSkipped  int
}

// backfillProfilesResult summarises one run of backfillProfiles.
type backfillProfilesResult struct {
	usersProcessed int
	usersUpdated   int
	usersSkipped   int
}

// getAlternateEmailDetailsFn is injectable for tests.
var getAlternateEmailDetailsFn = getAlternateEmailDetails

// loadBackfillCursor reads the cursor value from the v1-mappings KV bucket.
// Returns ("", nil) when the key does not exist (first run).
func loadBackfillCursor(ctx context.Context, cursorKey string) (string, error) {
	entry, err := mappingsKV.Get(ctx, cursorKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return "", nil
		}
		return "", fmt.Errorf("failed to read cursor %s: %w", cursorKey, err)
	}
	return strings.TrimSpace(string(entry.Value())), nil
}

// saveBackfillCursor writes the cursor value to the v1-mappings KV bucket.
func saveBackfillCursor(ctx context.Context, cursorKey, value string) error {
	if _, err := mappingsKV.Put(ctx, cursorKey, []byte(value)); err != nil {
		return fmt.Errorf("failed to save cursor %s: %w", cursorKey, err)
	}
	return nil
}

// listAuth0UserPage fetches one page of Auth0 users for the given connection,
// sorted by updated_at ascending. cursor is the updated_at of the last
// processed user from the previous run (RFC3339Nano), or "" for the first run;
// the inclusive lower bound means the boundary user is re-processed on resume,
// which is safe because all operations are idempotent. runPage is the
// zero-based page offset within this run's query (used for within-run
// pagination; it is not persisted between runs).
//
// PerPage is always backfillPageSize so that the Auth0 page offset
// (page * per_page) remains stable as runPage increments. Callers enforce
// the --limit cap by stopping when enough users have been processed, not by
// shrinking the page size on the final request.
func listAuth0UserPage(ctx context.Context, cursor string, runPage int) (*management.UserList, error) {
	query := fmt.Sprintf(`identities.connection:"%s"`, backfillAuth0Connection)
	if cursor != "" {
		query += fmt.Sprintf(` AND updated_at:[%s TO *]`, cursor)
	}

	return auth0Users.Search(ctx,
		management.Query(query),
		management.Parameter("sort", "updated_at:1"),
		management.PerPage(backfillPageSize),
		management.Page(runPage),
	)
}

// backfillAlternateEmails iterates Auth0 users and links any v1 alternate emails
// that are verified and not yet linked. Advances the cursor on each successful
// page. Returns when limit users have been processed or no more users remain.
func backfillAlternateEmails(ctx context.Context, limit int, dryRun bool) (*backfillEmailsResult, error) {
	result := &backfillEmailsResult{}

	cursor, err := loadBackfillCursor(ctx, backfillAltEmailsCursorKey)
	if err != nil {
		return nil, fmt.Errorf("loading cursor: %w", err)
	}

	logger.With(
		"cursor", cursor,
		"limit", limit,
		"dry_run", dryRun,
	).Info("starting alternate-emails backfill")

	remaining := limit
	runPage := 0
	nextCursor := cursor
	for remaining > 0 {
		// Always query with the original cursor so that offset pagination within
		// this run stays stable. nextCursor is advanced per-user and only
		// persisted for cross-run resumability. PerPage is always backfillPageSize
		// (never clamped) so that page*per_page offsets stay correct as runPage
		// increments; remaining enforces the --limit cap after the fact.
		page, err := listAuth0UserPage(ctx, cursor, runPage)
		if err != nil {
			return result, fmt.Errorf("listing Auth0 users: %w", err)
		}

		if len(page.Users) == 0 {
			logger.Info("alternate-emails backfill: no more users to process")
			break
		}

		for _, auth0User := range page.Users {
			auth0UserID := auth0User.GetID()
			username := auth0User.GetUsername()
			if username == "" {
				// All users in the Username-Password-Authentication connection must
				// have a username. A missing username indicates a data inconsistency
				// that would cause silent skips; stop and surface it rather than
				// silently advancing the cursor past affected users.
				return result, fmt.Errorf("Auth0 user %s has no username in connection %s; stopping backfill to avoid silent skips", auth0UserID, backfillAuth0Connection)
			}

			if err := auth0RateLimiter.Wait(ctx); err != nil {
				return result, fmt.Errorf("rate limiter: %w", err)
			}

			userCtx, cancel := context.WithTimeout(ctx, backfillCallTimeout)
			err := backfillEmailsForUser(userCtx, auth0UserID, username, dryRun, result)
			cancel()
			if err != nil {
				logger.With(errKey, err, "auth0_user_id", auth0UserID).
					Warn("aborting alternate-emails backfill after error")
				if !dryRun {
					// nextCursor holds the last successfully processed user's
					// updated_at, so the failing user will be retried on the
					// next run (inclusive cursor query).
					if saveErr := saveBackfillCursor(ctx, backfillAltEmailsCursorKey, nextCursor); saveErr != nil {
						logger.With(errKey, saveErr).Warn("failed to save backfill cursor on abort")
					}
				}
				return result, fmt.Errorf("processing user %s: %w", auth0UserID, err)
			}

			result.usersProcessed++
			if updatedAt := auth0User.GetUpdatedAt(); !updatedAt.IsZero() {
				nextCursor = updatedAt.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
			}
		}

		// Advance the run-local page counter; the query lower-bound (cursor)
		// stays fixed for this run so offset pagination is correct.
		runPage++

		// Persist the next-run cursor after each page so partial runs are resumable.
		if !dryRun {
			if saveErr := saveBackfillCursor(ctx, backfillAltEmailsCursorKey, nextCursor); saveErr != nil {
				logger.With(errKey, saveErr).Warn("failed to save backfill cursor, progress may be lost on restart")
			}
		}

		remaining -= len(page.Users)

		logger.With(
			"users_processed", result.usersProcessed,
			"emails_linked", result.emailsLinked,
			"emails_skipped", result.emailsSkipped,
			"cursor", nextCursor,
		).Info("alternate-emails backfill page complete")
	}

	return result, nil
}

// backfillEmailsForUser resolves all v1 alternate email SFIDs for the given user
// and links any that are verified and not yet linked to Auth0.
func backfillEmailsForUser(ctx context.Context, auth0UserID, username string, dryRun bool, result *backfillEmailsResult) error {
	// Resolve v1 user SFID from username secondary index.
	userSfid, err := ResolveV1UserSFIDByUsername(ctx, username)
	if err != nil {
		return fmt.Errorf("resolving v1 SFID for %s: %w", username, err)
	}
	if userSfid == "" {
		logger.With("username", username).
			Debug("no v1 SFID found for Auth0 user, skipping")
		result.emailsSkipped++
		return nil
	}

	// Fetch the list of alternate email SFIDs from the v1-mappings KV bucket.
	mappingKey := kvKeyAlternateEmailsPrefix + userSfid
	entry, err := mappingsKV.Get(ctx, mappingKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			// No alternate emails registered for this user.
			result.emailsSkipped++
			return nil
		}
		return fmt.Errorf("fetching alternate email SFIDs for %s: %w", userSfid, err)
	}

	var emailSfids []string
	if err := json.Unmarshal(entry.Value(), &emailSfids); err != nil {
		return fmt.Errorf("parsing email SFIDs for %s: %w", userSfid, err)
	}

	// Single pass: collect candidate (non-primary, active, verified) emails to
	// link, while also counting "qualifying" emails (active and either
	// verified or primary) across ALL of the user's alternate email rows. If
	// only one email qualifies, that row is a lone alternate_email__c acting
	// as a de-facto primary — v1 lazy-sync may not have created/synced the
	// primary row yet — so it should not be linked as a secondary identity,
	// even if it was collected as a link candidate (see LFXV2-2662; this
	// mirrors auth0-db-sync.js's heuristic).
	type emailCandidate struct {
		emailSfid string
		email     string
	}
	var candidates []emailCandidate
	qualifying := 0
	sawPrimary := false
	for _, emailSfid := range emailSfids {
		email, isPrimary, isVerified, isActive, err := getAlternateEmailDetailsFn(ctx, emailSfid)
		if err != nil {
			// A read failure makes the qualifying count unreliable, and
			// guessing either way (sole vs. non-sole) risks a wrong link
			// decision, so abort rather than silently proceed. This user
			// will be retried on the next run (the cursor doesn't advance
			// past them); a persistently-bad row requires a manual data fix
			// to unblock, which is preferable to silently mislinking.
			return fmt.Errorf("getting alternate email details for %s (auth0 user %s): %w", emailSfid, auth0UserID, err)
		}
		if isActive && (isVerified || isPrimary) {
			qualifying++
			if isPrimary {
				sawPrimary = true
			}
		}
		if isPrimary || !isActive || !isVerified || email == "" {
			result.emailsSkipped++
			continue
		}
		candidates = append(candidates, emailCandidate{emailSfid: emailSfid, email: email})
	}

	if qualifying <= 1 {
		logger.With("user_sfid", userSfid, "auth0_user_id", auth0UserID).
			Debug("sole qualifying alternate email, treating as de-facto primary, skipping link candidates")
		result.emailsSkipped += len(candidates)
		return nil
	}
	if !sawPrimary {
		// More than one row qualifies but none is flagged primary: which one
		// is the de-facto primary is genuinely ambiguous, so abort rather
		// than linking all of them as secondary identities.
		return fmt.Errorf("user %s (auth0 user %s) has %d qualifying alternate emails and none is flagged primary; cannot determine de-facto primary", userSfid, auth0UserID, qualifying)
	}

	for _, c := range candidates {
		if dryRun {
			logger.With(
				"auth0_user_id", auth0UserID,
				"email", c.email,
				"email_sfid", c.emailSfid,
			).Info("[dry-run] would link alternate email to Auth0 user")
			result.emailsLinked++
			continue
		}

		if err := linkEmailIdentityFn(ctx, auth0UserID, c.email); err != nil {
			return fmt.Errorf("linking email %s: %w", c.email, err)
		}
		result.emailsLinked++
	}

	return nil
}

// backfillProfiles iterates Auth0 users and syncs v1 profile fields to Auth0
// user_metadata for each. Advances the cursor on each successful page.
func backfillProfiles(ctx context.Context, limit int, dryRun bool) (*backfillProfilesResult, error) {
	result := &backfillProfilesResult{}

	cursor, err := loadBackfillCursor(ctx, backfillProfilesCursorKey)
	if err != nil {
		return nil, fmt.Errorf("loading cursor: %w", err)
	}

	logger.With(
		"cursor", cursor,
		"limit", limit,
		"dry_run", dryRun,
	).Info("starting profiles backfill")

	remaining := limit
	runPage := 0
	nextCursor := cursor
	for remaining > 0 {
		// Always query with the original cursor so that offset pagination within
		// this run stays stable. nextCursor is advanced per-user and only
		// persisted for cross-run resumability. PerPage is always backfillPageSize
		// (never clamped) so that page*per_page offsets stay correct as runPage
		// increments; remaining enforces the --limit cap after the fact.
		page, err := listAuth0UserPage(ctx, cursor, runPage)
		if err != nil {
			return result, fmt.Errorf("listing Auth0 users: %w", err)
		}

		if len(page.Users) == 0 {
			logger.Info("profiles backfill: no more users to process")
			break
		}

		for _, auth0User := range page.Users {
			auth0UserID := auth0User.GetID()
			username := auth0User.GetUsername()
			if username == "" {
				// All users in the Username-Password-Authentication connection must
				// have a username. Stop to avoid silent skips.
				return result, fmt.Errorf("Auth0 user %s has no username in connection %s; stopping backfill to avoid silent skips", auth0UserID, backfillAuth0Connection)
			}

			if err := auth0RateLimiter.Wait(ctx); err != nil {
				return result, fmt.Errorf("rate limiter: %w", err)
			}

			userCtx, cancel := context.WithTimeout(ctx, backfillCallTimeout)
			err := backfillProfileForUser(userCtx, auth0UserID, username, dryRun, result)
			cancel()
			if err != nil {
				logger.With(errKey, err, "auth0_user_id", auth0UserID).
					Warn("aborting profiles backfill after error")
				if !dryRun {
					// nextCursor holds the last successfully processed user's
					// updated_at, so the failing user will be retried on the
					// next run (inclusive cursor query).
					if saveErr := saveBackfillCursor(ctx, backfillProfilesCursorKey, nextCursor); saveErr != nil {
						logger.With(errKey, saveErr).Warn("failed to save backfill cursor on abort")
					}
				}
				return result, fmt.Errorf("processing user %s: %w", auth0UserID, err)
			}

			result.usersProcessed++
			if updatedAt := auth0User.GetUpdatedAt(); !updatedAt.IsZero() {
				nextCursor = updatedAt.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
			}
		}

		// Advance the run-local page counter; the query lower-bound (cursor)
		// stays fixed for this run so offset pagination is correct.
		runPage++

		// Persist the next-run cursor after each page so partial runs are resumable.
		if !dryRun {
			if saveErr := saveBackfillCursor(ctx, backfillProfilesCursorKey, nextCursor); saveErr != nil {
				logger.With(errKey, saveErr).Warn("failed to save backfill cursor, progress may be lost on restart")
			}
		}

		remaining -= len(page.Users)

		logger.With(
			"users_processed", result.usersProcessed,
			"users_updated", result.usersUpdated,
			"users_skipped", result.usersSkipped,
			"cursor", nextCursor,
		).Info("profiles backfill page complete")
	}

	return result, nil
}

// backfillProfileForUser looks up the v1 merged_user record for the given
// username and syncs its profile fields to Auth0 user_metadata.
func backfillProfileForUser(ctx context.Context, auth0UserID, username string, dryRun bool, result *backfillProfilesResult) error {
	userSfid, err := ResolveV1UserSFIDByUsername(ctx, username)
	if err != nil {
		return fmt.Errorf("resolving v1 SFID for %s: %w", username, err)
	}
	if userSfid == "" {
		result.usersSkipped++
		return nil
	}

	v1Data, exists, err := getV1ObjectData(ctx, v1MergedUserKVPrefix+userSfid)
	if err != nil {
		return fmt.Errorf("fetching v1 merged_user for %s: %w", userSfid, err)
	}
	if !exists {
		result.usersSkipped++
		return nil
	}

	if dryRun {
		logger.With("auth0_user_id", auth0UserID, "user_sfid", userSfid).
			Info("[dry-run] would sync profile to Auth0 user_metadata")
		result.usersUpdated++
		return nil
	}

	if err := syncProfileToAuth0Fn(ctx, auth0UserID, v1Data); err != nil {
		return fmt.Errorf("syncing profile for %s: %w", auth0UserID, err)
	}
	result.usersUpdated++
	return nil
}

// syncSingleUser performs a full sync (profile + alternate emails) for a single
// user identified by their Auth0 username. Intended for debugging and targeted
// re-sync without a full backfill run.
func syncSingleUser(ctx context.Context, username string, dryRun bool) error {
	auth0UserID := mapUsernameToAuthSub(username)

	logger.With("username", username, "auth0_user_id", auth0UserID, "dry_run", dryRun).
		Info("starting single-user sync")

	// Resolve v1 SFID.
	userSfid, err := ResolveV1UserSFIDByUsername(ctx, username)
	if err != nil {
		return fmt.Errorf("resolving v1 SFID: %w", err)
	}
	if userSfid == "" {
		return fmt.Errorf("no v1 SFID found for username %q", username)
	}

	logger.With("username", username, "user_sfid", userSfid).Info("resolved v1 SFID")

	// Sync profile.
	v1Data, exists, err := getV1ObjectData(ctx, v1MergedUserKVPrefix+userSfid)
	if err != nil {
		return fmt.Errorf("fetching v1 merged_user: %w", err)
	}
	if !exists {
		logger.With("user_sfid", userSfid).Warn("v1 merged_user record not found, skipping profile sync")
	} else if dryRun {
		logger.With("auth0_user_id", auth0UserID, "user_sfid", userSfid).
			Info("[dry-run] would sync profile to Auth0 user_metadata")
	} else {
		if err := syncProfileToAuth0Fn(ctx, auth0UserID, v1Data); err != nil {
			logger.With("error", err, "auth0_user_id", auth0UserID).
				Warn("profile sync failed")
		} else {
			logger.With("auth0_user_id", auth0UserID).Info("profile synced")
		}
	}

	// Sync alternate emails.
	mappingKey := kvKeyAlternateEmailsPrefix + userSfid
	entry, err := mappingsKV.Get(ctx, mappingKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			logger.With("user_sfid", userSfid).Info("no alternate email SFIDs found in v1-mappings, skipping email sync")
			return nil
		}
		return fmt.Errorf("fetching alternate email SFIDs: %w", err)
	}

	var emailSfids []string
	if err := json.Unmarshal(entry.Value(), &emailSfids); err != nil {
		return fmt.Errorf("parsing email SFIDs: %w", err)
	}

	// Single pass: collect candidate (non-primary, active, verified) emails to
	// link, while also counting "qualifying" emails (active and either
	// verified or primary) across ALL of the user's alternate email rows. If
	// only one email qualifies, that row is a lone alternate_email__c acting
	// as a de-facto primary — v1 lazy-sync may not have created/synced the
	// primary row yet — so it should not be linked as a secondary identity,
	// even if it was collected as a link candidate (see LFXV2-2662; this
	// mirrors auth0-db-sync.js's heuristic).
	type emailCandidate struct {
		emailSfid string
		email     string
	}
	var candidates []emailCandidate
	qualifying := 0
	sawPrimary := false
	for _, emailSfid := range emailSfids {
		email, isPrimary, isVerified, isActive, err := getAlternateEmailDetailsFn(ctx, emailSfid)
		if err != nil {
			// A read failure makes the qualifying count unreliable, and
			// guessing either way (sole vs. non-sole) risks a wrong link
			// decision, so abort rather than silently proceed. Re-run
			// --sync-user for this user once the underlying read problem
			// (or bad data) is resolved.
			return fmt.Errorf("getting alternate email details for %s (auth0 user %s): %w", emailSfid, auth0UserID, err)
		}
		if isActive && (isVerified || isPrimary) {
			qualifying++
			if isPrimary {
				sawPrimary = true
			}
		}
		if isPrimary || !isActive {
			continue
		}
		if !isVerified {
			logger.With("email", email, "email_sfid", emailSfid).
				Debug("email not verified, skipping")
			continue
		}
		if email == "" {
			continue
		}
		candidates = append(candidates, emailCandidate{emailSfid: emailSfid, email: email})
	}

	if qualifying <= 1 {
		logger.With("user_sfid", userSfid, "auth0_user_id", auth0UserID).
			Debug("sole qualifying alternate email, treating as de-facto primary, skipping link candidates")
		candidates = nil
	} else if !sawPrimary {
		// More than one row qualifies but none is flagged primary: which one
		// is the de-facto primary is genuinely ambiguous, so abort rather
		// than linking all of them as secondary identities.
		return fmt.Errorf("user %s (auth0 user %s) has %d qualifying alternate emails and none is flagged primary; cannot determine de-facto primary", userSfid, auth0UserID, qualifying)
	}

	for _, c := range candidates {
		if dryRun {
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("[dry-run] would link alternate email to Auth0 user")
			continue
		}

		if err := linkEmailIdentityFn(ctx, auth0UserID, c.email); err != nil {
			logger.With("error", err, "auth0_user_id", auth0UserID, "email", c.email).
				Warn("failed to link email, skipping")
		} else {
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("linked email identity")
		}
	}

	logger.With("username", username, "auth0_user_id", auth0UserID).Info("single-user sync complete")
	return nil
}
