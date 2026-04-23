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

// auth0Mgmt is the Auth0 Management API client, initialized once at startup.
var auth0Mgmt *management.Management

// auth0Users is the user operations interface, set to auth0Mgmt.User at init.
// Tests can replace this with a fake.
var auth0Users auth0UserAPI

// v1ToAuth0Fields maps v1 platform DB column names to Auth0 user_metadata keys.
var v1ToAuth0Fields = map[string]string{
	"firstname":      "given_name",
	"lastname":       "family_name",
	"title":          "job_title",
	"street":         "address",
	"city":           "city",
	"state":          "state_province",
	"country":        "country",
	"postalcode":     "postal_code",
	"phone":          "phone_number",
	"tshirt_size__c": "t_shirt_size",
	"photo_url__c":   "picture",
	"timezone__c":    "zoneinfo",
}

// v1NoAccountPlaceholder is the v1 placeholder org name that should not overwrite
// a real v2 organization value.
const v1NoAccountPlaceholder = "Individual - No Account"

// initAuth0MgmtClient initializes the Auth0 Management API client using private key JWT.
//
// The client's retry strategy depends on the profile-sync mode:
//   - Live mode (backfill=false): SDK retries on 429/5xx so the async
//     fire-and-forget goroutine absorbs transient failures on its own.
//   - Backfill mode (backfill=true): no SDK retries. The sync-path handler
//     NACKs on retryable errors so JetStream redelivery becomes the backoff,
//     throttling Management API throughput naturally.
func initAuth0MgmtClient(cfg *Config, backfill bool) error {
	domain := fmt.Sprintf("%s.auth0.com", cfg.Auth0Tenant)

	opts := []management.Option{
		management.WithClientCredentialsPrivateKeyJwt(
			context.Background(),
			cfg.Auth0ClientID,
			cfg.Auth0PrivateKey,
			"RS256",
		),
	}
	if backfill {
		opts = append(opts, management.WithNoRetries())
	} else {
		opts = append(opts, management.WithRetries(3, []int{429, 500, 502, 503, 504}))
	}

	mgmt, err := management.New(domain, opts...)
	if err != nil {
		return fmt.Errorf("failed to create Auth0 Management API client: %w", err)
	}

	auth0Mgmt = mgmt
	auth0Users = mgmt.User
	return nil
}

// isRetryableAuth0Error reports whether an Auth0 Management API error is
// transient and safe to retry via JetStream redelivery. It is only consulted
// on the sync (backfill) path; the async path relies on SDK-level retries and
// always ACKs.
//
// Retryable: HTTP 429 and any 5xx, plus network-level errors (timeouts, DNS
// failures, connection resets) that surface as net.Error / wrapped errors
// before a Management API response is returned.
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
	return false
}

// buildAuth0Metadata merges v1 platform DB fields into an existing Auth0
// user_metadata map and reports whether anything changed. The orgName parameter
// is the resolved organization name (empty to skip org mapping).
func buildAuth0Metadata(existing map[string]interface{}, v1Data map[string]any, orgName string) (merged map[string]interface{}, changed bool) {
	merged = make(map[string]interface{}, len(existing))
	for k, v := range existing {
		merged[k] = v
	}

	// Map each v1 field to the corresponding Auth0 user_metadata key.
	for v1Key, auth0Key := range v1ToAuth0Fields {
		v1Val, _ := v1Data[v1Key].(string)
		existingVal, _ := merged[auth0Key].(string)

		if v1Val != existingVal {
			merged[auth0Key] = v1Val
			changed = true
		}
	}

	// Derive the full name from first + last (never read v1 "name" column).
	firstName, _ := v1Data["firstname"].(string)
	lastName, _ := v1Data["lastname"].(string)
	derivedName := strings.TrimSpace(firstName + " " + lastName)
	existingName, _ := merged["name"].(string)
	if derivedName != existingName {
		merged["name"] = derivedName
		changed = true
	}

	// Organization mapping: don't overwrite a real org with the placeholder.
	if orgName != "" {
		existingOrg, _ := merged["organization"].(string)
		isPlaceholder := orgName == v1NoAccountPlaceholder && existingOrg != ""
		if !isPlaceholder && orgName != existingOrg {
			merged["organization"] = orgName
			changed = true
		}
	}

	return merged, changed
}

// auth0RateLimiter throttles Auth0 Management API calls in the email-identity
// link/unlink flow to avoid rate limits, especially during KV consumer replay
// (backfill). The profile sync flow (syncProfileToAuth0) is not rate-limited
// because it runs in a delayed goroutine with natural backpressure.
var auth0RateLimiter = rate.NewLimiter(rate.Limit(20), 5)

// luceneQuoteEscape escapes the two characters that have meaning inside an
// Auth0 v3 search-engine quoted phrase: backslash and double-quote.
func luceneQuoteEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// syncProfileToAuth0 maps v1 merged_user fields to Auth0 user_metadata and
// pushes the update via the Management API. It reads the current user_metadata
// first to avoid clobbering fields we don't own.
func syncProfileToAuth0(ctx context.Context, auth0UserID string, v1Data map[string]any) error {
	// Read the current Auth0 user to get existing user_metadata.
	existing, err := auth0Users.Read(ctx, auth0UserID)
	if err != nil {
		return fmt.Errorf("failed to read Auth0 user %s: %w", auth0UserID, err)
	}

	// Start from existing user_metadata (or empty map).
	existingMetadata := make(map[string]interface{})
	if existing.UserMetadata != nil {
		for k, v := range *existing.UserMetadata {
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
			return fmt.Errorf("failed to resolve v1 org %s: %w", accountID, orgErr)
		}
		if org != nil && org.Name != "" {
			orgName = org.Name
		}
	}

	metadata, changed := buildAuth0Metadata(existingMetadata, v1Data, orgName)

	if !changed {
		logger.With("auth0_user_id", auth0UserID).
			DebugContext(ctx, "no profile field changes detected, skipping Auth0 update")
		return nil
	}

	// Push the updated user_metadata to Auth0.
	err = auth0Users.Update(ctx, auth0UserID, &management.User{
		UserMetadata: &metadata,
	})
	if err != nil {
		return fmt.Errorf("failed to update Auth0 user %s: %w", auth0UserID, err)
	}

	logger.With("auth0_user_id", auth0UserID).
		InfoContext(ctx, "synced v1 profile to Auth0 user_metadata")
	return nil
}

// linkEmailIdentity creates an email connection user in Auth0 and links it to the
// primary account. This is the two-step M2M flow: create secondary user, then link.
// It is idempotent: if the email is already linked to this user, it returns nil.
func linkEmailIdentity(ctx context.Context, primaryAuth0ID, email string) error {
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}

	// Check if the email is already linked to this user.
	primaryUser, err := auth0Users.Read(ctx, primaryAuth0ID)
	if err != nil {
		return fmt.Errorf("failed to read primary user %s: %w", primaryAuth0ID, err)
	}
	for _, identity := range primaryUser.Identities {
		if identity.GetProvider() == "email" && identity.GetConnection() == "email" {
			if profileEmail, _ := identity.GetProfileData()["email"].(string); strings.EqualFold(profileEmail, email) {
				logger.With("auth0_user_id", primaryAuth0ID, "email", email).
					DebugContext(ctx, "email already linked to user, skipping")
				return nil
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
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}
	query := fmt.Sprintf(`identities.profileData.email:"%s" AND identities.provider:"email"`, luceneQuoteEscape(email))
	searchResult, err := auth0Users.Search(ctx, management.Query(query))
	if err != nil {
		return fmt.Errorf("failed to search Auth0 users by linked email %s: %w", email, err)
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
			return nil
		}
	}

	// Step 1: Create secondary user in the "email" connection with email_verified=true.
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}
	secondaryUser := &management.User{
		Connection:    auth0.String("email"),
		Email:         auth0.String(email),
		EmailVerified: auth0.Bool(true),
	}
	err = auth0Users.Create(ctx, secondaryUser)
	if err != nil {
		// If user already exists (409), find it and proceed to link.
		var mgmtErr management.Error
		if errors.As(err, &mgmtErr) && mgmtErr.Status() == http.StatusConflict {
			// Find the existing email user to get its ID for linking.
			if waitErr := auth0RateLimiter.Wait(ctx); waitErr != nil {
				return fmt.Errorf("rate limiter: %w", waitErr)
			}
			users, listErr := auth0Users.ListByEmail(ctx, email)
			if listErr != nil {
				return fmt.Errorf("failed to find existing email user for %s: %w", email, listErr)
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
				return fmt.Errorf("email user conflict for %s but could not find existing email| user", email)
			}
		} else {
			return fmt.Errorf("failed to create email user for %s: %w", email, err)
		}
	}

	// Extract the secondary user ID (strip the "email|" prefix for the link call).
	secondaryID := secondaryUser.GetID()
	secondaryID = strings.TrimPrefix(secondaryID, "email|")

	// Step 2: Link the secondary user to the primary.
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}
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
			return nil
		}
		return fmt.Errorf("failed to link email %s to user %s: %w", email, primaryAuth0ID, err)
	}

	logger.With("auth0_user_id", primaryAuth0ID, "email", email).
		InfoContext(ctx, "linked email identity to Auth0 user")
	return nil
}

// unlinkEmailIdentity removes a linked email identity from the primary Auth0 account.
// It is idempotent: if the email is not linked, it returns nil.
func unlinkEmailIdentity(ctx context.Context, primaryAuth0ID, email string) error {
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}

	// Read the primary user to find the linked email identity.
	primaryUser, err := auth0Users.Read(ctx, primaryAuth0ID)
	if err != nil {
		return fmt.Errorf("failed to read primary user %s: %w", primaryAuth0ID, err)
	}

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
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}
	_, err = auth0Users.Unlink(ctx, primaryAuth0ID, "email", secondaryUserID)
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
