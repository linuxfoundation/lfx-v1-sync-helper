// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// One-shot and batch commands for syncing v1 alternate emails and profiles to Auth0.
//
// Two modes are provided:
//
//   --sync-user <username> [--dry-run]
//     Performs a single-user sync of both profile and alternate emails.
//     Useful for debugging or re-syncing an individual user without a
//     full batch run.
//
//   --sync-users-file <path> [--dry-run]
//     Runs syncSingleUser for each username listed in a file, one per line.
//     See LFXV2-1507; replaces the earlier Auth0-cursor-driven "all users"
//     backfills (--backfill-alternate-emails / --backfill-profiles), which
//     walked the entire Auth0 population instead of a targeted cohort.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

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

// syncSingleUserFn is injectable for tests.
var syncSingleUserFn = syncSingleUser

// syncSingleUser performs a full sync (profile + alternate emails) for a single
// user identified by their Auth0 username. Intended for debugging and targeted
// re-sync without a full backfill run. Profile sync and email linking failures
// are logged individually but accumulated and returned as a single combined
// error, so a failure in either phase marks the user as failed rather than
// succeeding silently.
func syncSingleUser(ctx context.Context, username string, dryRun bool) error {
	auth0UserID := mapUsernameToAuthSub(username)
	var errs []error

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
		errs = append(errs, fmt.Errorf("syncing profile: %w", err))
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
			errs = append(errs, fmt.Errorf("linking email %s: %w", c.email, err))
		} else if linked {
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("linked email identity")
		} else {
			logger.With("auth0_user_id", auth0UserID, "email", c.email).
				Info("email link skipped (no-op)")
		}
	}

	logger.With("username", username, "auth0_user_id", auth0UserID).Info("single-user sync complete")
	return errors.Join(errs...)
}

// syncUsersFileResult holds summary counters for a batch --sync-users-file run.
type syncUsersFileResult struct {
	processed int
	succeeded int
	failed    int
}

// syncUserTimeout bounds a single syncSingleUser call within a batch run, so
// one stalled upstream request cannot stall the remaining cohort
// indefinitely.
const syncUserTimeout = 2 * time.Minute

// readUsernames reads a newline-delimited file of usernames, skipping blank
// lines and lines starting with "#". Extracted from syncUsersFromFile so the
// parsing rules are independently testable.
func readUsernames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening users file: %w", err)
	}
	defer f.Close() //nolint:errcheck // Best-effort close on read-only file.

	var usernames []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		usernames = append(usernames, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading users file: %w", err)
	}
	return usernames, nil
}

// syncUsersFromFile reads a newline-delimited file of usernames and runs
// syncSingleUser for each one, reusing the same authenticated clients for the
// entire batch. Pacing uses the same auth0RateLimiter as the backfill loops.
// Errors on individual users are logged but do not abort the batch.
func syncUsersFromFile(ctx context.Context, path string, dryRun bool) (syncUsersFileResult, error) {
	usernames, err := readUsernames(path)
	if err != nil {
		return syncUsersFileResult{}, err
	}

	logger.With("file", path, "users", len(usernames), "dry_run", dryRun).
		Info("batch user sync starting")

	var result syncUsersFileResult
	for i, username := range usernames {
		// Pace entries, not exits: the limiter only waits for the remainder of
		// the window after the previous user's work, so elapsed work counts
		// toward the interval instead of being added to it.
		if err := auth0RateLimiter.Wait(ctx); err != nil {
			return result, fmt.Errorf("rate limiter: %w", err)
		}

		result.processed++

		logger.With(
			"username", username,
			"index", i+1,
			"total", len(usernames),
			"dry_run", dryRun,
		).Info("syncing user")

		userCtx, cancel := context.WithTimeout(ctx, syncUserTimeout)
		err := syncSingleUserFn(userCtx, username, dryRun)
		cancel()
		if err != nil {
			result.failed++
			logger.With(
				"error", err,
				"username", username,
				"index", i+1,
			).Warn("user sync failed, continuing")
		} else {
			result.succeeded++
		}
	}

	return result, nil
}
