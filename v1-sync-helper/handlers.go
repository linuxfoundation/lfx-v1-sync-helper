// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

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

// kvHandler processes KV bucket updates from Meltano
func kvHandler(entry jetstream.KeyValueEntry) {
	ctx := context.Background()

	key := entry.Key()
	operation := entry.Operation()

	logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "processing KV entry")

	// Handle different operations
	switch operation {
	case jetstream.KeyValuePut:
		handleKVPut(ctx, entry)
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		handleKVDelete(ctx, entry)
	default:
		logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "ignoring KV operation")
	}
}

// handleKVPut processes a KV put operation (create/update)
func handleKVPut(ctx context.Context, entry jetstream.KeyValueEntry) {
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

	// Extract the prefix (everything before the first period) for faster lookup.
	prefix := key
	if dotIndex := strings.Index(key, "."); dotIndex != -1 {
		prefix = key[:dotIndex]
	}

	// Determine the object type based on the key prefix.
	switch prefix {
	case "salesforce-project__c":
		handleProjectUpdate(ctx, key, v1Data)
	case "platform-collaboration__c":
		handleCommitteeUpdate(ctx, key, v1Data)
	case "platform-community__c":
		handleCommitteeMemberUpdate(ctx, key, v1Data)
	case "itx-zoom-meetings-v2":
		handleZoomMeetingUpdate(ctx, key, v1Data)
	case "itx-zoom-meetings-registrants-v2":
		handleZoomMeetingRegistrantUpdate(ctx, key, v1Data)
	case "itx-zoom-meetings-mappings-v2":
		handleZoomMeetingMappingUpdate(ctx, key, v1Data)
	case "itx-zoom-past-meetings-attendees":
		handleZoomPastMeetingAttendeeUpdate(ctx, key, v1Data)
	case "itx-zoom-past-meetings-invitees":
		handleZoomPastMeetingInviteeUpdate(ctx, key, v1Data)
	case "itx-zoom-past-meetings-mappings":
		handleZoomPastMeetingMappingUpdate(ctx, key, v1Data)
	case "itx-zoom-past-meetings-recordings":
		handleZoomPastMeetingRecordingUpdate(ctx, key, v1Data)
	case "itx-zoom-past-meetings-summaries":
		handleZoomPastMeetingSummaryUpdate(ctx, key, v1Data)
	case "itx-zoom-past-meetings":
		handleZoomPastMeetingUpdate(ctx, key, v1Data)
	default:
		logger.With("key", key).WarnContext(ctx, "unknown object type, ignoring")
	}
}

// handleKVDelete processes a KV delete operation
func handleKVDelete(ctx context.Context, entry jetstream.KeyValueEntry) {
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
