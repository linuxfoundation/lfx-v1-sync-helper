// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// getRootProjectUID makes a NATS request to get the ROOT project UID.
func getRootProjectUID(ctx context.Context) (string, error) {
	// Create context with timeout for the NATS request.
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger.DebugContext(ctx, "requesting ROOT project UID via NATS")

	// Make a NATS request to the slug_to_uid subject.
	resp, err := natsConn.RequestWithContext(requestCtx, "lfx.projects-api.slug_to_uid", []byte("ROOT"))
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to request ROOT project UID")
		return "", fmt.Errorf("failed to request ROOT project UID: %w", err)
	}

	// The response should be the UUID string.
	rootUID := strings.TrimSpace(string(resp.Data))
	if rootUID == "" {
		logger.ErrorContext(ctx, "received empty ROOT project UID response")
		return "", fmt.Errorf("empty ROOT project UID response")
	}

	logger.With("root_uid", rootUID).DebugContext(ctx, "successfully retrieved ROOT project UID")
	return rootUID, nil
}

// shouldSkipSync checks if the record was last modified by this service and
// should be skipped, because it originated in v2, and therefore does not need
// to be synced from v1.
func shouldSkipSync(ctx context.Context, v1Data map[string]any) bool {
	if lastModifiedBy, ok := v1Data["lastmodifiedbyid"].(string); ok && lastModifiedBy != "" {
		// Check if the lastmodifiedbyid matches our Auth0 Client ID with @clients suffix.
		ourServiceID := cfg.Auth0ClientID + "@clients"
		if lastModifiedBy == ourServiceID {
			logger.With("lastmodifiedbyid", lastModifiedBy).DebugContext(ctx, "skipping record that originated in v2")
			return true
		}
	}
	return false
}

// extractUserInfo extracts user information from v1 data for API calls and JWT impersonation.
func extractUserInfo(ctx context.Context, v1Data map[string]any, mappingsKV jetstream.KeyValue) UserInfo {
	// Extract platform ID from lastmodifiedbyid
	if lastModifiedBy, ok := v1Data["lastmodifiedbyid"].(string); ok && lastModifiedBy != "" {
		// Check if this is a machine user with @clients suffix
		if strings.HasSuffix(lastModifiedBy, "@clients") {
			// Machine user - pass through with @clients only on principal
			return UserInfo{
				Username:  strings.TrimSuffix(lastModifiedBy, "@clients"), // Subject without @clients
				Email:     "",                                             // No email for machine users
				Principal: lastModifiedBy,                                 // Principal includes @clients
			}
		}

		// Regular platform ID - look up via v1 API
		userInfo, err := getUserInfoFromV1(ctx, lastModifiedBy, mappingsKV)
		if err != nil || userInfo.Username == "" {
			logger.With(errKey, err, "platform_id", lastModifiedBy).WarnContext(ctx, "failed to get user info from v1 API, falling back to service account")
			return UserInfo{} // Empty UserInfo triggers fallback to v1_sync_helper@clients
		}

		return userInfo
	}
	return UserInfo{}
}

// kvHandler processes KV bucket updates from Meltano
func kvHandler(entry jetstream.KeyValueEntry, v1KV jetstream.KeyValue, mappingsKV jetstream.KeyValue) {
	ctx := context.Background()

	key := entry.Key()
	operation := entry.Operation()

	logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "processing KV entry")

	// Handle different operations
	switch operation {
	case jetstream.KeyValuePut:
		handleKVPut(ctx, entry, v1KV, mappingsKV)
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		handleKVDelete(ctx, entry, v1KV, mappingsKV)
	default:
		logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "ignoring KV operation")
	}
}

// handleKVPut processes a KV put operation (create/update)
func handleKVPut(ctx context.Context, entry jetstream.KeyValueEntry, _ jetstream.KeyValue, mappingsKV jetstream.KeyValue) {
	key := entry.Key()

	// Parse the JSON data
	var v1Data map[string]any
	if err := json.Unmarshal(entry.Value(), &v1Data); err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to unmarshal KV entry data")
		return
	}

	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	// Extract user information for API calls and JWT impersonation
	userInfo := extractUserInfo(ctx, v1Data, mappingsKV)

	// Determine the object type based on the key pattern
	if strings.HasPrefix(key, "salesforce-project__c.") {
		handleProjectUpdate(ctx, key, v1Data, userInfo, mappingsKV)
	} else if strings.HasPrefix(key, "platform-collaboration__c.") {
		handleCommitteeUpdate(ctx, key, v1Data, userInfo, mappingsKV)
	} else if strings.HasPrefix(key, "platform-community__c.") {
		handleCommitteeMemberUpdate(ctx, key, v1Data, userInfo, mappingsKV)
	} else {
		logger.With("key", key).DebugContext(ctx, "unknown object type, ignoring")
	}
}

// handleKVDelete processes a KV delete operation
func handleKVDelete(ctx context.Context, entry jetstream.KeyValueEntry, _ jetstream.KeyValue, _ jetstream.KeyValue) {
	key := entry.Key()

	// For deletes, we would need to look up the mapping and call delete APIs
	// This is a simplified implementation
	logger.With("key", key).InfoContext(ctx, "delete operation not yet implemented")
}

// extractDateOnly extracts the date part from an ISO 8601 datetime string.
// Input: "2020-03-01T00:00:00+00:00"
// Output: "2020-03-01"
func extractDateOnly(dateTimeStr string) string {
	if dateTimeStr == "" {
		return ""
	}

	// Extract just the date part from ISO 8601 datetime format.
	if datePart := strings.Split(dateTimeStr, "T")[0]; datePart != "" {
		return datePart
	}

	return ""
}
