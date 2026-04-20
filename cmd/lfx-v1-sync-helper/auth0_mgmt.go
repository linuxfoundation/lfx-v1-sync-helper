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
	"fmt"
	"net/http"
	"strings"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
	"golang.org/x/time/rate"
)

// auth0Mgmt is the Auth0 Management API client, initialized once at startup.
var auth0Mgmt *management.Management

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
func initAuth0MgmtClient(cfg *Config) error {
	domain := fmt.Sprintf("%s.auth0.com", cfg.Auth0Tenant)

	mgmt, err := management.New(
		domain,
		management.WithClientCredentialsPrivateKeyJwt(
			context.Background(),
			cfg.Auth0ClientID,
			cfg.Auth0PrivateKey,
			"RS256",
		),
		// No SDK-level retries: the handler runs in a fire-and-forget goroutine
		// so we can't block on backoff. Errors are logged and the next KV update
		// will naturally retry.
	)
	if err != nil {
		return fmt.Errorf("failed to create Auth0 Management API client: %w", err)
	}

	auth0Mgmt = mgmt
	return nil
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

// auth0RateLimiter throttles Auth0 Management API calls to avoid rate limits,
// especially important during KV consumer replay (backfill).
var auth0RateLimiter = rate.NewLimiter(rate.Limit(20), 5)

// syncProfileToAuth0 maps v1 merged_user fields to Auth0 user_metadata and
// pushes the update via the Management API. It reads the current user_metadata
// first to avoid clobbering fields we don't own.
func syncProfileToAuth0(ctx context.Context, auth0UserID string, v1Data map[string]any) error {
	// Read the current Auth0 user to get existing user_metadata.
	existing, err := auth0Mgmt.User.Read(ctx, auth0UserID)
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

	// Resolve organization name from v1 accountid.
	var orgName string
	if accountID, ok := v1Data["accountid"].(string); ok && accountID != "" {
		org, orgErr := lookupV1Org(ctx, accountID)
		if orgErr != nil {
			logger.With(errKey, orgErr, "accountid", accountID).
				WarnContext(ctx, "failed to resolve v1 org for profile sync, skipping organization field")
		} else if org != nil && org.Name != "" {
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
	err = auth0Mgmt.User.Update(ctx, auth0UserID, &management.User{
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
	primaryUser, err := auth0Mgmt.User.Read(ctx, primaryAuth0ID)
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

	// Check if the email belongs to a different Auth0 user.
	existingUsers, err := auth0Mgmt.User.ListByEmail(ctx, email)
	if err != nil {
		return fmt.Errorf("failed to search users by email %s: %w", email, err)
	}
	for _, u := range existingUsers {
		if u.GetID() != primaryAuth0ID {
			logger.With("auth0_user_id", primaryAuth0ID, "email", email, "other_user", u.GetID()).
				WarnContext(ctx, "email belongs to a different Auth0 user, skipping link")
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
	err = auth0Mgmt.User.Create(ctx, secondaryUser)
	if err != nil {
		// If user already exists (409), find it and proceed to link.
		if mgmtErr, ok := err.(management.Error); ok && mgmtErr.Status() == http.StatusConflict {
			// Find the existing email user to get its ID for linking.
			users, listErr := auth0Mgmt.User.ListByEmail(ctx, email)
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
	_, err = auth0Mgmt.User.Link(ctx, primaryAuth0ID, &management.UserIdentityLink{
		Provider: auth0.String("email"),
		UserID:   auth0.String(secondaryID),
	})
	if err != nil {
		// 409 means already linked (idempotent).
		if mgmtErr, ok := err.(management.Error); ok && mgmtErr.Status() == http.StatusConflict {
			logger.With("auth0_user_id", primaryAuth0ID, "email", email).
				DebugContext(ctx, "email identity already linked (conflict on link call)")
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
	primaryUser, err := auth0Mgmt.User.Read(ctx, primaryAuth0ID)
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
		logger.With("auth0_user_id", primaryAuth0ID, "email", email).
			DebugContext(ctx, "email not linked to user, nothing to unlink")
		return nil
	}

	// Unlink the identity.
	if err := auth0RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}
	_, err = auth0Mgmt.User.Unlink(ctx, primaryAuth0ID, "email", secondaryUserID)
	if err != nil {
		// 404 means already unlinked (idempotent).
		if mgmtErr, ok := err.(management.Error); ok && mgmtErr.Status() == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to unlink email %s from user %s: %w", email, primaryAuth0ID, err)
	}

	logger.With("auth0_user_id", primaryAuth0ID, "email", email).
		InfoContext(ctx, "unlinked email identity from Auth0 user")
	return nil
}

