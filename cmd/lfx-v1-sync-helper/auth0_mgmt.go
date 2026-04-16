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
	"strings"

	"github.com/auth0/go-auth0/management"
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
		// Respect Auth0 rate limits (~50 req/s) during backfill replay.
		management.WithRetries(3, []int{429}),
	)
	if err != nil {
		return fmt.Errorf("failed to create Auth0 Management API client: %w", err)
	}

	auth0Mgmt = mgmt
	return nil
}

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
	metadata := make(map[string]interface{})
	if existing.UserMetadata != nil {
		for k, v := range *existing.UserMetadata {
			metadata[k] = v
		}
	}

	// Track whether anything actually changed to avoid unnecessary API calls.
	changed := false

	// Map each v1 field to the corresponding Auth0 user_metadata key.
	for v1Key, auth0Key := range v1ToAuth0Fields {
		v1Val, _ := v1Data[v1Key].(string)
		existingVal, _ := metadata[auth0Key].(string)

		if v1Val != existingVal {
			metadata[auth0Key] = v1Val
			changed = true
		}
	}

	// Derive the full name from first + last (never read v1 "name" column).
	firstName, _ := v1Data["firstname"].(string)
	lastName, _ := v1Data["lastname"].(string)
	derivedName := strings.TrimSpace(firstName + " " + lastName)
	existingName, _ := metadata["name"].(string)
	if derivedName != existingName {
		metadata["name"] = derivedName
		changed = true
	}

	// Organization: resolve account name from KV.
	if accountID, ok := v1Data["accountid"].(string); ok && accountID != "" {
		org, orgErr := lookupV1Org(ctx, accountID)
		if orgErr != nil {
			logger.With(errKey, orgErr, "accountid", accountID).
				WarnContext(ctx, "failed to resolve v1 org for profile sync, skipping organization field")
		} else if org != nil && org.Name != "" {
			existingOrg, _ := metadata["organization"].(string)
			// Don't overwrite a real org with the placeholder.
			if org.Name == v1NoAccountPlaceholder && existingOrg != "" {
				logger.With("auth0_user_id", auth0UserID).
					DebugContext(ctx, "skipping org update: v1 has placeholder, v2 already has a value")
			} else if org.Name != existingOrg {
				metadata["organization"] = org.Name
				changed = true
			}
		}
	}

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

// isRetryableAuth0Error returns true for transient errors (network blips,
// timeouts) that should trigger JetStream re-delivery. Permanent errors
// (404, 403, validation) return false to avoid infinite retry loops.
// Note: 429 rate-limit errors are already handled by the SDK's WithRetries.
func isRetryableAuth0Error(err error) bool {
	if err == nil {
		return false
	}

	// Network-level errors are transient.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Auth0 Management API errors expose the HTTP status code.
	var mErr management.Error
	if errors.As(err, &mErr) {
		switch mErr.Status() {
		case 408, 429, 500, 502, 503, 504:
			return true
		default:
			return false
		}
	}

	// Unknown error type — don't retry to avoid infinite loops.
	return false
}
