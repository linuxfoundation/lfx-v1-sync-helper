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
//     (name, title, address, etc., plus skills) to Auth0 user_metadata
//     instead.
//     Cursor stored at "backfill.profiles.cursor.v2" (versioned when skills
//     were added to this backfill, so the first post-upgrade run revisits
//     every user rather than resuming past ones a prior run already synced
//     without skills).
//     Same inclusive-cursor behavior as --backfill-alternate-emails.
//     Replaces the legacy PROFILE_SYNC_BACKFILL env-var approach.
//
//   --sync-user <username> [--dry-run]
//     Performs a single-user sync of both profile and alternate emails.
//     Useful for debugging or re-syncing an individual user without a
//     full backfill run.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/auth0/go-auth0/management"
)

const (
	// backfillAltEmailsCursorKey is the v1-mappings KV key used to store the
	// alternate-emails backfill cursor (updated_at of last processed Auth0 user).
	backfillAltEmailsCursorKey = "backfill.alternate-emails.cursor"

	// backfillProfilesCursorKey is the v1-mappings KV key used to store the
	// profiles backfill cursor.
	//
	// Versioned "v2" (was "backfill.profiles.cursor"): this backfill now also
	// syncs skills (see syncProfileToAuth0's includeSkills), and user_skills
	// is WAL-only with no other path into Auth0 for historical rows. A prior
	// completed run's cursor already sits past most users, so reusing that
	// key would silently skip backfilling skills for everyone before it.
	// Bumping the key forces this deploy's first run to start from scratch
	// and revisit every user; the old key is left in place, unused.
	backfillProfilesCursorKey = "backfill.profiles.cursor.v2"

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

// getAlternateEmailsForUserFn is injectable for tests.
var getAlternateEmailsForUserFn = dbGetAlternateEmailsForUser

// emailCandidate is a linkable alternate email row (non-primary, active,
// verified, non-empty address).
type emailCandidate struct {
	emailSfid string
	email     string
}

// collectEmailLinkCandidates fetches a user's alternate email rows from the
// v1 platform database and returns the candidate (non-primary, active,
// verified) emails to link, deduplicated by address, along with the count of
// "qualifying" rows (active and either verified or primary) and whether any
// qualifying row is flagged primary. See LFXV2-2662 for the qualifying-count
// heuristic — a sole qualifying row is a de-facto primary and must not be
// linked as a secondary identity.
//
// rejected counts rows that were fetched but did not become candidates
// (primary, inactive, unverified, empty address, or a duplicate address) so
// callers can attribute them in their skipped-email totals.
func collectEmailLinkCandidates(ctx context.Context, userSfid string) (candidates []emailCandidate, qualifying int, sawPrimary bool, rejected int, err error) {
	rows, err := getAlternateEmailsForUserFn(ctx, userSfid)
	if err != nil {
		return nil, 0, false, 0, fmt.Errorf("fetching alternate emails for %s: %w", userSfid, err)
	}

	// Deduplicate candidates by email address (case-insensitive). v1
	// enforces email uniqueness per user, so duplicates are not expected,
	// but deduplicating here avoids a wasted Auth0 Search call if two
	// rows somehow resolve to the same address — the pre-fetched user's
	// identity list would be stale after the first successful link.
	seenEmails := make(map[string]bool)
	for i := range rows {
		row := &rows[i]
		email := row.EmailAddress.String
		isPrimary := row.IsPrimary.Valid && row.IsPrimary.Bool
		isVerified := row.IsVerified.Valid && row.IsVerified.Bool
		isActive := emailRowIsActive(row)
		if isActive && (isVerified || isPrimary) {
			qualifying++
			if isPrimary {
				sawPrimary = true
			}
		}
		if isPrimary || !isActive || !isVerified || email == "" {
			rejected++
			continue
		}
		lower := strings.ToLower(email)
		if seenEmails[lower] {
			rejected++
			continue
		}
		seenEmails[lower] = true
		candidates = append(candidates, emailCandidate{emailSfid: row.SFID, email: email})
	}
	return candidates, qualifying, sawPrimary, rejected, nil
}

// loadBackfillCursor reads the cursor value from the v1-mappings KV bucket.
// Returns ("", nil) when the key does not exist (first run).
func loadBackfillCursor(ctx context.Context, cursorKey string) (string, error) {
	entry, err := mappingStore.Get(ctx, cursorKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read cursor %s: %w", cursorKey, err)
	}
	return strings.TrimSpace(string(entry.Value)), nil
}

// saveBackfillCursor writes the cursor value to the v1-mappings KV bucket.
func saveBackfillCursor(ctx context.Context, cursorKey, value string) error {
	if _, err := mappingStore.Put(ctx, cursorKey, []byte(value)); err != nil {
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

	// Collect candidate emails and qualifying counts live from the v1
	// platform database.
	candidates, qualifying, sawPrimary, rejected, err := collectEmailLinkCandidates(ctx, userSfid)
	if err != nil {
		return fmt.Errorf("collecting email candidates (auth0 user %s): %w", auth0UserID, err)
	}
	result.emailsSkipped += rejected

	if qualifying <= 1 {
		logger.With("user_sfid", userSfid, "auth0_user_id", auth0UserID).
			Debug("sole qualifying alternate email, treating as de-facto primary, skipping link candidates")
		result.emailsSkipped += len(candidates)
		return nil
	}
	if !sawPrimary {
		return fmt.Errorf("user %s (auth0 user %s) has %d qualifying alternate emails and none is flagged primary; cannot determine de-facto primary", userSfid, auth0UserID, qualifying)
	}

	// No candidates to link — skip the Auth0 read entirely.
	if len(candidates) == 0 {
		return nil
	}

	// Fetch the Auth0 user once for all candidate emails.
	auth0User, err := fetchAuth0User(ctx, auth0UserID)
	if err != nil {
		return fmt.Errorf("fetching Auth0 user %s: %w", auth0UserID, err)
	}

	for _, c := range candidates {
		if dryRun {
			if !emailLinkEligibility(ctx, auth0User, c.email) {
				result.emailsSkipped++
				continue
			}
			logger.With(
				"auth0_user_id", auth0UserID,
				"email", c.email,
				"email_sfid", c.emailSfid,
			).Info("[dry-run] would link alternate email to Auth0 user")
			result.emailsLinked++
			continue
		}

		linked, err := linkEmailIdentityFn(ctx, auth0User, c.email)
		if err != nil {
			return fmt.Errorf("linking email %s: %w", c.email, err)
		}
		if linked {
			result.emailsLinked++
		} else {
			result.emailsSkipped++
		}
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

	auth0User, err := fetchAuth0User(ctx, auth0UserID)
	if err != nil {
		return fmt.Errorf("fetching Auth0 user %s: %w", auth0UserID, err)
	}

	updated, err := syncProfileToAuth0Fn(ctx, auth0UserID, auth0User, v1Data, true, dryRun)
	if err != nil {
		return fmt.Errorf("syncing profile for %s: %w", auth0UserID, err)
	}
	if updated {
		result.usersUpdated++
	} else {
		result.usersSkipped++
	}
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

	// Fetch the Auth0 user once for both profile sync and email linking.
	auth0User, err := fetchAuth0User(ctx, auth0UserID)
	if err != nil {
		return fmt.Errorf("fetching Auth0 user: %w", err)
	}

	// Sync profile.
	v1Data, exists, err := getV1ObjectData(ctx, v1MergedUserKVPrefix+userSfid)
	if err != nil {
		return fmt.Errorf("fetching v1 merged_user: %w", err)
	}
	if !exists {
		logger.With("user_sfid", userSfid).Warn("v1 merged_user record not found, skipping profile sync")
	} else if updated, err := syncProfileToAuth0Fn(ctx, auth0UserID, auth0User, v1Data, true, dryRun); err != nil {
		logger.With("error", err, "auth0_user_id", auth0UserID).
			Warn("profile sync failed")
	} else if updated {
		if dryRun {
			logger.With("auth0_user_id", auth0UserID).Info("[dry-run] would sync profile")
		} else {
			logger.With("auth0_user_id", auth0UserID).Info("profile synced")
		}
	} else {
		if dryRun {
			logger.With("auth0_user_id", auth0UserID).Info("[dry-run] profile sync would be skipped (no-op)")
		} else {
			logger.With("auth0_user_id", auth0UserID).Info("profile sync skipped (no-op)")
		}
	}

	// Sync alternate emails, collected live from the v1 platform database.
	candidates, qualifying, sawPrimary, _, err := collectEmailLinkCandidates(ctx, userSfid)
	if err != nil {
		return fmt.Errorf("collecting email candidates (auth0 user %s): %w", auth0UserID, err)
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
			if !emailLinkEligibility(ctx, auth0User, c.email) {
				logger.With("auth0_user_id", auth0UserID, "email", c.email).
					Info("[dry-run] email link not eligible, would skip")
				continue
			}
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("[dry-run] would link alternate email to Auth0 user")
			continue
		}

		if linked, err := linkEmailIdentityFn(ctx, auth0User, c.email); err != nil {
			logger.With("error", err, "auth0_user_id", auth0UserID, "email", c.email).
				Warn("failed to link email, skipping")
		} else if linked {
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("linked email identity")
		} else {
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("email link skipped (no-op)")
		}
	}

	logger.With("username", username, "auth0_user_id", auth0UserID).Info("single-user sync complete")
	return nil
}
