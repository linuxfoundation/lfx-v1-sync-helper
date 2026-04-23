// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// v2-to-v1 profile sync: receives lfx.user_profile.updated events from
// auth-service and pushes the changes to the v1 platform DB via user-service.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	nats "github.com/nats-io/nats.go"
)

// userProfileUpdatedEvent matches the event published by auth-service after a
// successful user_metadata update.
type userProfileUpdatedEvent struct {
	UserID    string         `json:"user_id"`
	Principal string         `json:"principal"`
	Metadata  map[string]any `json:"user_metadata"`
	Timestamp time.Time      `json:"timestamp"`
}

// auth0ToV1Fields maps Auth0 user_metadata keys to top-level fields on the
// user-service PATCH body (schema: update-partial-user).
var auth0ToV1Fields = map[string]string{
	"given_name":   "FirstName",
	"family_name":  "LastName",
	"job_title":    "Title",
	"phone_number": "Phone",
	"t_shirt_size": "TShirtSize",
	"picture":      "LogoURL",
}

// auth0ToV1AddressFields maps Auth0 user_metadata keys to fields on the nested
// Address object of the user-service PATCH body (schema: address-partial-update).
// user-service does not accept Street/City/State/Country/PostalCode at the top
// level — they must be under "Address".
var auth0ToV1AddressFields = map[string]string{
	"address":        "Street",
	"city":           "City",
	"state_province": "State",
	"country":        "Country",
	"postal_code":    "PostalCode",
}

// Note: Auth0's `zoneinfo` has no equivalent in user-service's update-partial-user
// schema, so timezone edits in v2 cannot propagate to v1 today.

// handleUserProfileUpdated processes lfx.user_profile.updated events from
// auth-service and syncs the profile to v1 via user-service.
func handleUserProfileUpdated(msg *nats.Msg) {
	ctx := context.Background()

	var event userProfileUpdatedEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal user_profile.updated event")
		return
	}

	log := logger.With("user_id", event.UserID, "principal", event.Principal)

	// Defensive loop guard. Auth-service currently sets principal = JWT sub =
	// user_id (the authenticated user updating their own metadata), and PR #87
	// writes v1->Auth0 directly via the Management API without going through
	// auth-service, so no event published today can have principal equal to
	// our service identity. This check exists for a future flow where v1->Auth0
	// writes are brokered through auth-service (which would publish events
	// with principal = "{AUTH0_CLIENT_ID}@clients" for our M2M token).
	ourServiceID := cfg.Auth0ClientID + "@clients"
	if event.Principal == ourServiceID {
		log.DebugContext(ctx, "skipping profile event that originated from us")
		return
	}

	auth0UserID, err := extractAuth0UserIDSuffix(event.UserID)
	if err != nil {
		log.With(errKey, err).WarnContext(ctx, "cannot safely derive v1 username from auth0 user ID, skipping")
		return
	}

	// ResolveV1UserSFIDByUsername uses the encoded secondary-index key
	// (matching what handleMergedUserUpdate writes) and validates the resolved
	// SFID by fetching the user and confirming the username still matches.
	sfid, err := ResolveV1UserSFIDByUsername(ctx, auth0UserID)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to resolve v1 user SFID")
		return
	}
	if sfid == "" {
		log.WarnContext(ctx, "no v1 user found for auth0 user ID, skipping")
		return
	}

	payload := mapMetadataToV1Payload(event.Metadata)
	if len(payload) == 0 {
		log.DebugContext(ctx, "no mappable fields in event, skipping user-service update")
		return
	}

	if err := patchV1User(ctx, sfid, payload); err != nil {
		log.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to patch v1 user")
		return
	}

	log.With("sfid", sfid).InfoContext(ctx, "synced v2 profile to v1 user-service")
}

// extractAuth0UserIDSuffix returns the portion of an Auth0 user_id after the
// "auth0|" prefix when it is safe to use as a v1 username lookup key.
//
// The suffix of "auth0|{id}" is NOT guaranteed to be the original v1 username:
//   - mapUsernameToAuthSub() hashes usernames that are >60 chars, contain
//     special characters, or look like a 24+ char hex Auth0 native ID; the
//     hash is one-way (SHA-512 + base58, ~80 chars).
//   - Auth0 native DB connections use numeric/hex identifiers, and Auth0 is
//     expected to begin issuing wholly-numeric user IDs.
//
// In both cases the only way to recover the underlying v1 username is a
// Management API round-trip against the sub, which is out of scope here.
// Rather than risk patching the wrong v1 user, skip events whose suffix is
// ambiguous.
func extractAuth0UserIDSuffix(userID string) (string, error) {
	suffix, ok := strings.CutPrefix(userID, "auth0|")
	if !ok || suffix == "" {
		return "", fmt.Errorf("user_id missing auth0| prefix or has empty suffix")
	}
	if len(suffix) > 60 {
		return "", fmt.Errorf("auth0 user ID suffix is longer than 60 chars (likely a hashed legacy username)")
	}
	if isAllDigits(suffix) {
		return "", fmt.Errorf("auth0 user ID suffix is wholly numeric (likely a future Auth0 native ID)")
	}
	return suffix, nil
}

// isAllDigits reports whether s is non-empty and consists only of ASCII digits.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// mapMetadataToV1Payload builds the v1 user-service PATCH payload from an
// Auth0 user_metadata map. Top-level fields are translated via
// auth0ToV1Fields; address fields are translated via auth0ToV1AddressFields
// and nested under "Address". Non-string values and unknown keys are ignored.
// Returns an empty map when nothing mapped so the caller can skip the HTTP call.
func mapMetadataToV1Payload(metadata map[string]any) map[string]any {
	payload := make(map[string]any, len(auth0ToV1Fields)+1)
	for auth0Key, v1Key := range auth0ToV1Fields {
		if val, ok := metadata[auth0Key].(string); ok {
			payload[v1Key] = val
		}
	}

	address := make(map[string]string, len(auth0ToV1AddressFields))
	for auth0Key, v1Key := range auth0ToV1AddressFields {
		if val, ok := metadata[auth0Key].(string); ok {
			address[v1Key] = val
		}
	}
	if len(address) > 0 {
		payload["Address"] = address
	}

	return payload
}

// patchV1User sends a PATCH request to user-service to update a v1 user record.
func patchV1User(ctx context.Context, sfid string, fields map[string]any) error {
	apiURL := fmt.Sprintf("%suser-service/v1/users/%s", cfg.LFXAPIGateway.String(), sfid)

	body, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("failed to marshal user-service payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create user-service request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send user-service request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("user-service returned status %d for user %s: %s", resp.StatusCode, sfid, string(respBody))
	}

	return nil
}
