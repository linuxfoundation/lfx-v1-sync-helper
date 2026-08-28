// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Auth0 Management API client for syncing v1 profile data to Auth0 user_metadata.
//
// This client uses the same Auth0 credentials as the v1 API gateway client but
// targets the Management API audience (https://{tenant}.auth0.com/api/v2/) with
// read:users and update:users scopes granted via auth0-terraform M2M config.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
	"golang.org/x/time/rate"
)

// auth0UserAPI defines the subset of Auth0 Management API user operations
// used by this package. Satisfied by *management.UserManager in production
// and by a fake in tests.
type auth0UserAPI interface {
	Read(ctx context.Context, id string, opts ...management.RequestOption) (*management.User, error)
	ListByEmail(ctx context.Context, email string, opts ...management.RequestOption) ([]*management.User, error)
	Search(ctx context.Context, opts ...management.RequestOption) (*management.UserList, error)
	Create(ctx context.Context, u *management.User, opts ...management.RequestOption) error
	Update(ctx context.Context, id string, u *management.User, opts ...management.RequestOption) error
	Link(ctx context.Context, id string, il *management.UserIdentityLink, opts ...management.RequestOption) ([]management.UserIdentity, error)
	Unlink(ctx context.Context, id, provider, userID string, opts ...management.RequestOption) ([]management.UserIdentity, error)
}

// auth0Users is the user operations interface, set at init.
// Tests can replace this with a fake.
var auth0Users auth0UserAPI

// getSkillsForUserFn is injectable for tests, mirroring getAlternateEmailsForUserFn.
var getSkillsForUserFn = dbGetSkillsForUser

// v1ToAuth0Fields maps v1 platform DB column names to Auth0 user_metadata keys.
// Address fields use the Salesforce MailingAddress columns (mailingstreet, etc.);
// merged_user has no bare street/city/state/country/postalcode columns.
// Name fields (given_name, family_name, name) are intentionally omitted: they
// are owned by whoever receives the profile-update action (auth service for v2
// writes; v1 pushes them directly on its own path). The same rationale applies
// as for primary email — v1-sync-helper is not the authoritative writer.
var v1ToAuth0Fields = map[string]string{
	"title":             "job_title",
	"mailingstreet":     "address",
	"mailingcity":       "city",
	"mailingstate":      "state_province",
	"mailingcountry":    "country",
	"mailingpostalcode": "postal_code",
	"phone":             "phone_number",
	"tshirt_size__c":    "t_shirt_size",
	"photo_url__c":      "picture",
	"timezone__c":       "zoneinfo",
	"bio__c":            "bio",
}

// v1NoAccountPlaceholder is the v1 sentinel org name for individuals with no
// company affiliation. It is not a real organization and is never written to
// Auth0 user_metadata; when resolved as a user's org it is treated as "no org".
const v1NoAccountPlaceholder = "Individual - No Account"

// initAuth0MgmtClient initializes the Auth0 Management API client using
// private key JWT with no SDK-level retries. All callers (live handler and
// backfill) use WithNoRetries: the live path NACKs on retryable errors so
// JetStream redelivery provides backoff; the backfill path aborts on error
// and saves its cursor.
func initAuth0MgmtClient(cfg *Config) error {
	domain := fmt.Sprintf("%s.auth0.com", cfg.Auth0Tenant)

	opts := []management.Option{
		management.WithClientCredentialsPrivateKeyJwt(
			context.Background(),
			cfg.Auth0ClientID,
			cfg.Auth0PrivateKey,
			"RS256",
		),
		management.WithNoRetries(),
	}

	mgmt, err := management.New(domain, opts...)
	if err != nil {
		return fmt.Errorf("failed to create Auth0 Management API client: %w", err)
	}

	auth0Users = mgmt.User
	return nil
}

// isRetryableAuth0Error reports whether an Auth0 Management API error is
// transient and safe to retry via JetStream redelivery. Both the live handler
// path (which NACKs on retryable errors) and the backfill path (which aborts
// and saves its cursor) consult this function.
//
// Retryable: HTTP 429 and any 5xx, plus network-level errors (timeouts, DNS
// failures, connection resets) that surface as net.Error or context deadline /
// cancellation errors from the per-call timeout wrapper.
func isRetryableAuth0Error(err error) bool {
	if err == nil {
		return false
	}
	var mgmtErr management.Error
	if errors.As(err, &mgmtErr) {
		status := mgmtErr.Status()
		return status == 429 || status >= 500
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// buildAuth0Metadata diffs v1 platform DB fields against the existing Auth0
// user_metadata and returns a patch map containing only the keys that changed.
// The orgName parameter is the resolved organization name (empty or the
// individual placeholder to skip org mapping). The skills parameter is the
// resolved, comma-joined skill list; nil means "not resolved this call, leave
// untouched" and a non-nil empty string means "user has no skills, clear the
// field" — the same nil-vs-empty convention as orgName, needed because skills
// (like organization) lives outside v1Data (it comes from a separate
// salesforce.user_skills join, not a merged_user column). The patch map is
// safe to send as the user_metadata body of an Auth0 Management API PATCH:
// Auth0 merges top-level keys, so sending only changed keys avoids
// unnecessary writes and is race-safer than sending the full metadata object.
// An empty patch map means nothing changed.
func buildAuth0Metadata(existing map[string]interface{}, v1Data map[string]any, orgName string, skills *string) map[string]interface{} {
	patch := make(map[string]interface{})

	// Map each v1 field to the corresponding Auth0 user_metadata key.
	for v1Key, auth0Key := range v1ToAuth0Fields {
		v1Val, _ := v1Data[v1Key].(string)
		existingVal, _ := existing[auth0Key].(string)

		if v1Val != existingVal {
			patch[auth0Key] = v1Val
		}
	}

	// Organization mapping: skip v1's individual-placeholder sentinel entirely;
	// the v2 organization field is either already set by the auth service or is
	// legitimately empty — the v1 placeholder is not meaningful in v2.
	if orgName != "" && orgName != v1NoAccountPlaceholder {
		existingOrg, _ := existing["organization"].(string)
		if orgName != existingOrg {
			patch["organization"] = orgName
		}
	}

	// Skills mapping: nil means the caller didn't resolve a v1 username for
	// this user, so skills are left untouched rather than wiped.
	if skills != nil {
		existingSkills, _ := existing["skills"].(string)
		if *skills != existingSkills {
			patch["skills"] = *skills
		}
	}

	return patch
}

// auth0RateLimiter throttles Auth0 Management API calls in the backfill outer
// loops (one token consumed per user before processing). Not used on the live
// handler path, which relies on NACK-based backoff instead.
var auth0RateLimiter = rate.NewLimiter(rate.Limit(10), 1)

// luceneQuoteEscape escapes the two characters that have meaning inside an
// Auth0 v3 search-engine quoted phrase: backslash and double-quote.
func luceneQuoteEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// fetchAuth0User reads a single Auth0 user by ID. Callers that need multiple
// checks on the same user (blocked, identities, metadata) should call this
// once and pass the result through, avoiding redundant Management API reads.
func fetchAuth0User(ctx context.Context, auth0UserID string) (*management.User, error) {
	user, err := auth0Users.Read(ctx, auth0UserID)
	if err != nil {
		return nil, fmt.Errorf("failed to read Auth0 user %s: %w", auth0UserID, err)
	}
	return user, nil
}

// syncProfileToAuth0 maps v1 merged_user fields to Auth0 user_metadata and
// pushes the update via the Management API. The caller must pass the
// pre-fetched Auth0 user so that multiple operations on the same user share a
// single Management API read. The returned bool is true only when
// user_metadata was actually written (or, in dry-run mode, would have been
// written), so callers (in particular backfill summaries) can distinguish an
// update from a no-op skip. When dryRun is true, all eligibility and diff
// checks still run but the Management API write is skipped.
//
// includeSkills controls whether this call also resolves and writes the
// "skills" field. handleUserSkillsUpdate (see handlers_user_skills.go) is a
// second, independent live writer of that same field, keyed and serialized
// by lfid via userSkillsStaleGuard — but this function is keyed by SFID and
// isn't serialized against that guard at all, so if both ran live at once, a
// merged_user event here could read a stale skill list and overwrite a
// concurrent user_skills write with an older value. To avoid that race, the
// live merged_user handler (handlers_users.go) passes includeSkills=false,
// leaving skills exclusively owned by the live user_skills handler; only the
// administrative, on-demand backfill/single-user-resync call sites
// (backfill_email_profile.go) pass true, since those aren't a second
// continuously-racing live writer.
func syncProfileToAuth0(ctx context.Context, auth0UserID string, primaryUser *management.User, v1Data map[string]any, includeSkills, dryRun bool) (bool, error) {
	// Blocked accounts are treated as inactive/deprovisioned: don't push new
	// profile data to them. This is a no-op skip, not an error.
	if primaryUser.GetBlocked() {
		logger.With("auth0_user_id", auth0UserID).
			WarnContext(ctx, "Auth0 user is blocked, skipping profile sync")
		return false, nil
	}

	// Start from existing user_metadata (or empty map) for diffing.
	existingMetadata := make(map[string]interface{})
	if primaryUser.UserMetadata != nil {
		for k, v := range *primaryUser.UserMetadata {
			existingMetadata[k] = v
		}
	}

	// Resolve organization name from v1 accountid. A lookup failure is surfaced
	// to the caller so backfill runs can log it explicitly; the caller treats
	// it as non-retryable and ACKs the message so the backfill keeps moving.
	var orgName string
	if accountID, ok := v1Data["accountid"].(string); ok && accountID != "" {
		org, orgErr := lookupV1Org(ctx, accountID)
		if orgErr != nil {
			return false, fmt.Errorf("failed to resolve v1 org %s: %w", accountID, orgErr)
		}
		if org != nil && org.Name != "" {
			orgName = org.Name
		}
	}

	// Resolve skills from salesforce.user_skills, keyed by v1 username (lfid) —
	// skills has no merged_user column, so it can't come from v1Data directly.
	// A missing/blank username__c leaves skills nil (untouched), matching how
	// orgName is skipped above when accountid is absent. Skipped entirely when
	// includeSkills is false (see the doc comment above for why).
	var skills *string
	if includeSkills {
		if username, ok := v1Data["username__c"].(string); ok && normalizeUserIdentifier(username) != "" {
			skillNames, skillErr := getSkillsForUserFn(ctx, username)
			if skillErr != nil {
				return false, fmt.Errorf("failed to resolve v1 skills for %s: %w", username, skillErr)
			}
			joined := strings.Join(skillNames, ", ")
			skills = &joined
		}
	}

	metadata := buildAuth0Metadata(existingMetadata, v1Data, orgName, skills)

	if len(metadata) == 0 {
		logger.With("auth0_user_id", auth0UserID).
			DebugContext(ctx, "no profile field changes detected, skipping Auth0 update")
		return false, nil
	}

	if dryRun {
		logger.With("auth0_user_id", auth0UserID).
			InfoContext(ctx, "[dry-run] would sync profile to Auth0 user_metadata")
		return true, nil
	}

	// Push the updated user_metadata to Auth0.
	if err := auth0Users.Update(ctx, auth0UserID, &management.User{
		UserMetadata: &metadata,
	}); err != nil {
		return false, fmt.Errorf("failed to update Auth0 user %s: %w", auth0UserID, err)
	}

	logger.With("auth0_user_id", auth0UserID).
		InfoContext(ctx, "synced v1 profile to Auth0 user_metadata")
	return true, nil
}

// syncSkillsToAuth0 pushes a user's current v1 skill list to Auth0
// user_metadata, touching only the "skills" key. This exists separately from
// syncProfileToAuth0 because the user_skills WAL trigger never carries
// merged_user's other profile columns (job_title, bio, address, ...) —
// running those through buildAuth0Metadata's full field diff would read every
// missing column as "cleared" and wipe out unrelated profile data that this
// event has nothing to do with. The caller must pass the pre-fetched Auth0
// user. The returned bool is true only when user_metadata was actually
// written (or would have been, in dry-run mode).
func syncSkillsToAuth0(ctx context.Context, auth0UserID string, primaryUser *management.User, skillNames []string, dryRun bool) (bool, error) {
	if primaryUser.GetBlocked() {
		logger.With("auth0_user_id", auth0UserID).
			WarnContext(ctx, "Auth0 user is blocked, skipping skills sync")
		return false, nil
	}

	var existingSkills string
	if primaryUser.UserMetadata != nil {
		existingSkills, _ = (*primaryUser.UserMetadata)["skills"].(string)
	}

	joined := strings.Join(skillNames, ", ")
	if joined == existingSkills {
		logger.With("auth0_user_id", auth0UserID).
			DebugContext(ctx, "no skills change detected, skipping Auth0 update")
		return false, nil
	}

	if dryRun {
		logger.With("auth0_user_id", auth0UserID).
			InfoContext(ctx, "[dry-run] would sync skills to Auth0 user_metadata")
		return true, nil
	}

	metadata := map[string]interface{}{"skills": joined}
	if err := auth0Users.Update(ctx, auth0UserID, &management.User{
		UserMetadata: &metadata,
	}); err != nil {
		return false, fmt.Errorf("failed to update Auth0 user %s skills: %w", auth0UserID, err)
	}

	logger.With("auth0_user_id", auth0UserID).
		InfoContext(ctx, "synced v1 skills to Auth0 user_metadata")
	return true, nil
}

// emailLinkEligibility checks whether linkEmailIdentity would actually attempt
// to link the given email to the pre-fetched primary Auth0 user: it returns
// false when the account is blocked or when the email matches the primary
// account's own top-level email (redundant link).
func emailLinkEligibility(ctx context.Context, primaryUser *management.User, email string) bool {
	primaryAuth0ID := primaryUser.GetID()

	// Blocked accounts are treated as inactive/deprovisioned: don't link new
	// secondary identities to them. This is a no-op skip, not an error.
	if primaryUser.GetBlocked() {
		logger.With("auth0_user_id", primaryAuth0ID, "email", email).
			WarnContext(ctx, "Auth0 user is blocked, skipping email link")
		return false
	}

	// If the email matches the primary account's own login email, there is
	// nothing to link: the account already has this email as its top-level
	// identity. Linking it as a secondary "email" identity would just create
	// a redundant duplicate.
	if strings.EqualFold(primaryUser.GetEmail(), email) {
		logger.With("auth0_user_id", primaryAuth0ID, "email", email).
			WarnContext(ctx, "email matches primary account's own email, skipping link")
		return false
	}

	return true
}

// linkEmailIdentity creates an email connection user in Auth0 and links it to the
// primary account. This is the two-step M2M flow: create secondary user, then link.
// It is idempotent: if the email is already linked to this user, it returns
// (true, nil). The caller must pass the pre-fetched primary Auth0 user so that
// multiple link operations on the same user share a single Management API read.
// The returned bool is false only for the no-op skip cases (blocked account,
// or email matching the primary account's own email).
func linkEmailIdentity(ctx context.Context, primaryUser *management.User, email string) (bool, error) {
	primaryAuth0ID := primaryUser.GetID()

	eligible := emailLinkEligibility(ctx, primaryUser, email)
	if !eligible {
		return false, nil
	}

	// Check if the email is already linked to this user.
	for _, identity := range primaryUser.Identities {
		if identity.GetProvider() == "email" && identity.GetConnection() == "email" {
			if profileEmail, _ := identity.GetProfileData()["email"].(string); strings.EqualFold(profileEmail, email) {
				logger.With("auth0_user_id", primaryAuth0ID, "email", email).
					DebugContext(ctx, "email already linked to user, skipping")
				return true, nil
			}
		}
	}

	// Check if the email is already linked as a secondary identity on any
	// Auth0 user. ListByEmail can't see this — it only matches users whose
	// *primary* account email is the queried address. Use the v3 user search
	// against nested-identity profile data instead, which surfaces the
	// primary user that owns the linked identity (and never returns the
	// detached email|... account, since its own primary identity has no
	// profileData).
	query := fmt.Sprintf(`identities.profileData.email:"%s" AND identities.provider:"email"`, luceneQuoteEscape(email))
	searchResult, err := auth0Users.Search(ctx, management.Query(query))
	if err != nil {
		return false, fmt.Errorf("failed to search Auth0 users by linked email %s: %w", email, err)
	}
	for _, u := range searchResult.Users {
		// The Lucene query is loose (matches any identity with this email OR
		// any identity with provider=email). Walk identities to confirm both
		// match on the same identity entry before treating it as a conflict.
		for _, identity := range u.Identities {
			if identity.GetProvider() != "email" {
				continue
			}
			profileEmail, _ := identity.GetProfileData()["email"].(string)
			if !strings.EqualFold(profileEmail, email) {
				continue
			}
			// Email is already linked somewhere. Even if it happens to be
			// to our primary user (rare race after the Read above), abort:
			// idempotency is satisfied either way.
			logger.With("auth0_user_id", primaryAuth0ID, "email", email, "other_user", u.GetID()).
				WarnContext(ctx, "email already linked as a secondary identity, aborting link")
			return true, nil
		}
	}

	// Step 1: Create secondary user in the "email" connection with email_verified=true.
	secondaryUser := &management.User{
		Connection:    auth0.String("email"),
		Email:         auth0.String(email),
		EmailVerified: auth0.Bool(true),
	}
	// Create unmarshals the API response back into secondaryUser, populating user_id.
	err = auth0Users.Create(ctx, secondaryUser)
	if err != nil {
		// If user already exists (409), find it and proceed to link.
		var mgmtErr management.Error
		if errors.As(err, &mgmtErr) && mgmtErr.Status() == http.StatusConflict {
			// Find the existing email user to get its ID for linking.
			users, listErr := auth0Users.ListByEmail(ctx, email)
			if listErr != nil {
				return false, fmt.Errorf("failed to find existing email user for %s: %w", email, listErr)
			}
			var found bool
			for _, u := range users {
				if strings.HasPrefix(u.GetID(), "email|") {
					secondaryUser = u
					found = true
					break
				}
			}
			if !found {
				return false, fmt.Errorf("email user conflict for %s but could not find existing email| user", email)
			}
		} else {
			return false, fmt.Errorf("failed to create email user for %s: %w", email, err)
		}
	}

	// Extract the secondary user ID (strip the "email|" prefix for the link call).
	secondaryID := secondaryUser.GetID()
	secondaryID = strings.TrimPrefix(secondaryID, "email|")

	// Step 2: Link the secondary user to the primary.
	_, err = auth0Users.Link(ctx, primaryAuth0ID, &management.UserIdentityLink{
		Provider: auth0.String("email"),
		UserID:   auth0.String(secondaryID),
	})
	if err != nil {
		// 409 means already linked (idempotent). After the upstream Lucene
		// pre-check this should be rare — log it as a warning so we have
		// visibility into races between the pre-check and the link call.
		var mgmtErr management.Error
		if errors.As(err, &mgmtErr) && mgmtErr.Status() == http.StatusConflict {
			logger.With("auth0_user_id", primaryAuth0ID, "email", email).
				WarnContext(ctx, "email identity already linked (conflict on link call)")
			return true, nil
		}
		return false, fmt.Errorf("failed to link email %s to user %s: %w", email, primaryAuth0ID, err)
	}

	logger.With("auth0_user_id", primaryAuth0ID, "email", email).
		InfoContext(ctx, "linked email identity to Auth0 user")
	return true, nil
}

// unlinkEmailIdentity removes a linked email identity from the primary Auth0 account.
// It is idempotent: if the email is not linked, it returns nil. The caller must
// pass the pre-fetched primary Auth0 user to avoid a redundant Management API read.
func unlinkEmailIdentity(ctx context.Context, primaryUser *management.User, email string) error {
	primaryAuth0ID := primaryUser.GetID()

	// Find the email identity matching this email address.
	var secondaryUserID string
	for _, identity := range primaryUser.Identities {
		if identity.GetProvider() == "email" && identity.GetConnection() == "email" {
			if profileEmail, _ := identity.GetProfileData()["email"].(string); strings.EqualFold(profileEmail, email) {
				secondaryUserID = identity.GetUserID()
				break
			}
		}
	}

	if secondaryUserID == "" {
		// Backfill seeds an Auth0 linked identity for every active v1 alt
		// email, so an unlink request that finds nothing is unexpected and
		// worth surfacing. Still idempotent — return nil.
		logger.With("auth0_user_id", primaryAuth0ID, "email", email).
			WarnContext(ctx, "email not linked to user, nothing to unlink")
		return nil
	}

	// Unlink the identity.
	_, err := auth0Users.Unlink(ctx, primaryAuth0ID, "email", secondaryUserID)
	if err != nil {
		// 404 means already unlinked (idempotent).
		var mgmtErr management.Error
		if errors.As(err, &mgmtErr) && mgmtErr.Status() == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to unlink email %s from user %s: %w", email, primaryAuth0ID, err)
	}

	logger.With("auth0_user_id", primaryAuth0ID, "email", email).
		InfoContext(ctx, "unlinked email identity from Auth0 user")
	return nil
}
