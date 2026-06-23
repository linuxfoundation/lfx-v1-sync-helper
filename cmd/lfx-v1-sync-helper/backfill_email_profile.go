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
//
//   --backfill-profiles [--limit N] [--dry-run]
//     Same Auth0-user-centric iteration, but syncs v1 profile fields
//     (name, title, address, etc.) to Auth0 user_metadata instead.
//     Cursor stored at "backfill.profiles.cursor".
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
	errors         int
}

// backfillProfilesResult summarises one run of backfillProfiles.
type backfillProfilesResult struct {
	usersProcessed int
	usersUpdated   int
	usersSkipped   int
	errors         int
}

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
// sorted by updated_at ascending, starting after the cursor.
// cursor is the updated_at timestamp of the last processed user (RFC3339),
// or "" for the first run. The page is returned as a *management.UserList.
func listAuth0UserPage(ctx context.Context, cursor string, limit int) (*management.UserList, error) {
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	// Build the Lucene query: filter to the connection; advance past the cursor
	// by requiring updated_at > cursor when one is present.
	query := fmt.Sprintf(`identities.connection:"%s"`, backfillAuth0Connection)
	if cursor != "" {
		// Auth0 v3 search supports range queries on updated_at in ISO8601.
		query += fmt.Sprintf(` AND updated_at:{%s TO *}`, cursor)
	}

	return auth0Users.Search(ctx,
		management.Query(query),
		management.Parameter("sort", "updated_at:1"),
		management.PerPage(limit),
		management.Page(0),
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
	for remaining > 0 {
		pageSize := remaining
		if pageSize > backfillPageSize {
			pageSize = backfillPageSize
		}

		page, err := listAuth0UserPage(ctx, cursor, pageSize)
		if err != nil {
			return result, fmt.Errorf("listing Auth0 users: %w", err)
		}

		if len(page.Users) == 0 {
			logger.Info("alternate-emails backfill: no more users to process")
			break
		}

		for _, auth0User := range page.Users {
			auth0UserID := auth0User.GetID()
			username := extractUsernameFromAuth0ID(auth0UserID)
			if username == "" {
				logger.With("auth0_user_id", auth0UserID).
					Debug("skipping user with non-Username-Password-Authentication ID")
				result.emailsSkipped++
				continue
			}

			userCtx, cancel := context.WithTimeout(ctx, backfillCallTimeout)
			err := backfillEmailsForUser(userCtx, auth0UserID, username, dryRun, result)
			cancel()
			if err != nil {
				logger.With("error", err, "auth0_user_id", auth0UserID).
					Warn("error processing user during alternate-emails backfill, continuing")
				result.errors++
			}

			result.usersProcessed++
			// Advance cursor to the updated_at of this user.
			if updatedAt := auth0User.GetUpdatedAt(); !updatedAt.IsZero() {
				cursor = updatedAt.UTC().Format(time.RFC3339)
			}
		}

		// Persist cursor after each page so partial runs are resumable.
		if !dryRun {
			if saveErr := saveBackfillCursor(ctx, backfillAltEmailsCursorKey, cursor); saveErr != nil {
				logger.With("error", saveErr).Warn("failed to save backfill cursor, progress may be lost on restart")
			}
		}

		remaining -= len(page.Users)

		logger.With(
			"users_processed", result.usersProcessed,
			"emails_linked", result.emailsLinked,
			"emails_skipped", result.emailsSkipped,
			"errors", result.errors,
			"cursor", cursor,
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

	for _, emailSfid := range emailSfids {
		email, isPrimary, isVerified, isTombstoned, err := getAlternateEmailDetailsFn(ctx, emailSfid)
		if err != nil {
			logger.With("error", err, "email_sfid", emailSfid, "auth0_user_id", auth0UserID).
				Warn("failed to get alternate email details, skipping")
			result.emailsSkipped++
			continue
		}
		if isPrimary || isTombstoned || !isVerified || email == "" {
			result.emailsSkipped++
			continue
		}

		if dryRun {
			logger.With(
				"auth0_user_id", auth0UserID,
				"email", email,
				"email_sfid", emailSfid,
			).Info("[dry-run] would link alternate email to Auth0 user")
			result.emailsLinked++
			continue
		}

		if err := linkEmailIdentityFn(ctx, auth0UserID, email); err != nil {
			logger.With("error", err, "auth0_user_id", auth0UserID, "email", email).
				Warn("failed to link alternate email, skipping")
			result.errors++
			continue
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
	for remaining > 0 {
		pageSize := remaining
		if pageSize > backfillPageSize {
			pageSize = backfillPageSize
		}

		page, err := listAuth0UserPage(ctx, cursor, pageSize)
		if err != nil {
			return result, fmt.Errorf("listing Auth0 users: %w", err)
		}

		if len(page.Users) == 0 {
			logger.Info("profiles backfill: no more users to process")
			break
		}

		for _, auth0User := range page.Users {
			auth0UserID := auth0User.GetID()
			username := extractUsernameFromAuth0ID(auth0UserID)
			if username == "" {
				result.usersSkipped++
				continue
			}

			userCtx, cancel := context.WithTimeout(ctx, backfillCallTimeout)
			err := backfillProfileForUser(userCtx, auth0UserID, username, dryRun, result)
			cancel()
			if err != nil {
				logger.With("error", err, "auth0_user_id", auth0UserID).
					Warn("error processing user during profiles backfill, continuing")
				result.errors++
			}

			result.usersProcessed++
			if updatedAt := auth0User.GetUpdatedAt(); !updatedAt.IsZero() {
				cursor = updatedAt.UTC().Format(time.RFC3339)
			}
		}

		// Persist cursor after each page so partial runs are resumable.
		if !dryRun {
			if saveErr := saveBackfillCursor(ctx, backfillProfilesCursorKey, cursor); saveErr != nil {
				logger.With("error", saveErr).Warn("failed to save backfill cursor, progress may be lost on restart")
			}
		}

		remaining -= len(page.Users)

		logger.With(
			"users_processed", result.usersProcessed,
			"users_updated", result.usersUpdated,
			"users_skipped", result.usersSkipped,
			"errors", result.errors,
			"cursor", cursor,
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

	for _, emailSfid := range emailSfids {
		email, isPrimary, isVerified, isTombstoned, err := getAlternateEmailDetailsFn(ctx, emailSfid)
		if err != nil {
			logger.With("error", err, "email_sfid", emailSfid).Warn("failed to get email details, skipping")
			continue
		}
		if isPrimary || isTombstoned {
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

		if dryRun {
			logger.With("auth0_user_id", auth0UserID, "email", email).
				Info("[dry-run] would link alternate email to Auth0 user")
			continue
		}

		if err := linkEmailIdentityFn(ctx, auth0UserID, email); err != nil {
			logger.With("error", err, "auth0_user_id", auth0UserID, "email", email).
				Warn("failed to link email, skipping")
		} else {
			logger.With("auth0_user_id", auth0UserID, "email", email).
				Info("linked email identity")
		}
	}

	logger.With("username", username, "auth0_user_id", auth0UserID).Info("single-user sync complete")
	return nil
}

// extractUsernameFromAuth0ID extracts the username from an Auth0 user ID of
// the form "auth0|<username>". Returns "" for any other format (social logins,
// etc.), which are not v1 platform users.
func extractUsernameFromAuth0ID(auth0UserID string) string {
	prefix := "auth0|"
	if !strings.HasPrefix(auth0UserID, prefix) {
		return ""
	}
	return strings.TrimPrefix(auth0UserID, prefix)
}
