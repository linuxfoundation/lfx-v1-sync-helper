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
	"github.com/nats-io/nats.go/jetstream"
)

// userProfileUpdatedEvent matches the event published by auth-service after a
// successful user_metadata update.
type userProfileUpdatedEvent struct {
	UserID    string         `json:"user_id"`
	Principal string         `json:"principal"`
	Metadata  map[string]any `json:"user_metadata"`
	Timestamp time.Time      `json:"timestamp"`
}

// auth0ToV1Fields is the reverse of v1ToAuth0Fields: Auth0 user_metadata key -> v1 user-service field.
var auth0ToV1Fields = map[string]string{
	"given_name":     "FirstName",
	"family_name":    "LastName",
	"job_title":      "Title",
	"address":        "Street",
	"city":           "City",
	"state_province": "State",
	"country":        "Country",
	"postal_code":    "PostalCode",
	"phone_number":   "Phone",
	"t_shirt_size":   "TShirtSize",
	"picture":        "PhotoURL",
	"zoneinfo":       "Timezone",
}

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

	// Loop prevention: if the principal is our own client ID, this update
	// originated from the v1-to-v2 direction and we must not echo it back.
	ourServiceID := cfg.Auth0ClientID + "@clients"
	if event.Principal == ourServiceID {
		log.DebugContext(ctx, "skipping profile event that originated from us")
		return
	}

	// Resolve the v1 user SFID from the Auth0 user ID.
	// Auth0 user IDs are "auth0|{username}" for the custom DB connection.
	username := strings.TrimPrefix(event.UserID, "auth0|")
	if username == "" || username == event.UserID {
		log.WarnContext(ctx, "cannot extract username from auth0 user ID, skipping")
		return
	}

	sfid, err := resolveV1UserSFIDByUsername(ctx, username)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to resolve v1 user SFID")
		return
	}
	if sfid == "" {
		log.WarnContext(ctx, "no v1 user found for username, skipping")
		return
	}

	// Map Auth0 user_metadata fields to the v1 user-service PATCH payload.
	payload := make(map[string]string)
	for auth0Key, v1Key := range auth0ToV1Fields {
		if val, ok := event.Metadata[auth0Key].(string); ok {
			payload[v1Key] = val
		}
	}

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

// resolveV1UserSFIDByUsername looks up a v1 user SFID via the username
// secondary index in the v1-mappings KV bucket (written by #86's
// handleMergedUserUpdate). Returns ("", nil) if the index entry doesn't
// exist or is tombstoned.
func resolveV1UserSFIDByUsername(ctx context.Context, username string) (string, error) {
	normalizedUsername := strings.ToLower(strings.TrimSpace(username))
	if normalizedUsername == "" {
		return "", nil
	}

	indexKey := "v1-user.username." + normalizedUsername
	entry, err := mappingsKV.Get(ctx, indexKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
			return "", nil
		}
		return "", fmt.Errorf("failed to get username index %s: %w", indexKey, err)
	}

	if isTombstonedMapping(entry.Value()) {
		return "", nil
	}

	return string(entry.Value()), nil
}

// patchV1User sends a PATCH request to user-service to update a v1 user record.
func patchV1User(ctx context.Context, sfid string, fields map[string]string) error {
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

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("user-service returned status %d for user %s: %s", resp.StatusCode, sfid, string(respBody))
	}

	return nil
}
