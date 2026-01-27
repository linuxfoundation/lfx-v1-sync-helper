// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// tombstoneMarker is used to mark deleted mappings in the KV store.
	tombstoneMarker = "!del"
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

// kvHandler processes KV bucket updates from Meltano.
// Returns true if the operation should be retried, false otherwise.
func kvHandler(entry jetstream.KeyValueEntry) bool {
	ctx := context.Background()

	key := entry.Key()
	operation := entry.Operation()

	logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "processing KV entry")

	// Handle different operations
	switch operation {
	case jetstream.KeyValuePut:
		return handleKVPut(ctx, entry)
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		return handleKVDelete(ctx, entry)
	default:
		logger.With("key", key, "operation", operation.String()).DebugContext(ctx, "ignoring KV operation")
		return false
	}
}

// handleKVPut processes a KV put operation (create/update).
// Returns true if the operation should be retried, false otherwise.
func handleKVPut(ctx context.Context, entry jetstream.KeyValueEntry) bool {
	key := entry.Key()

	// Parse the data (try JSON first, then msgpack)
	var v1Data map[string]any
	if err := json.Unmarshal(entry.Value(), &v1Data); err != nil {
		// JSON failed, try msgpack
		if msgErr := msgpack.Unmarshal(entry.Value(), &v1Data); msgErr != nil {
			logger.With(errKey, err, "msgpack_error", msgErr, "key", key).ErrorContext(ctx, "failed to unmarshal KV entry data as JSON or msgpack")
			return false
		}
		logger.With("key", key).DebugContext(ctx, "successfully unmarshalled msgpack data")
	} else {
		logger.With("key", key).DebugContext(ctx, "successfully unmarshalled JSON data")
	}

	// Check if this is a soft delete (record has _sdc_deleted_at field).
	if deletedAt, exists := v1Data["_sdc_deleted_at"]; exists && deletedAt != nil && deletedAt != "" {
		logger.With("key", key, "_sdc_deleted_at", deletedAt).InfoContext(ctx, "processing soft delete from WAL")
		return handleKVSoftDelete(ctx, key, v1Data)
	}

	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
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
		return false
	case "platform-collaboration__c":
		handleCommitteeUpdate(ctx, key, v1Data)
		return false
	case "platform-community__c":
		handleCommitteeMemberUpdate(ctx, key, v1Data)
		return false
	case "itx-poll":
		handleVoteUpdate(ctx, key, v1Data)
		return false
	case "itx-poll-vote":
		handleVoteResponseUpdate(ctx, key, v1Data)
		return false
	case "itx-surveys":
		handleSurveyUpdate(ctx, key, v1Data)
		return false
	case "itx-survey-responses":
		handleSurveyResponseUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-meetings-v2":
		handleZoomMeetingUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-meetings-registrants-v2":
		handleZoomMeetingRegistrantUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-meetings-mappings-v2":
		handleZoomMeetingMappingUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-meetings-invite-responses-v2":
		handleZoomMeetingInviteResponseUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-past-meetings-attendees":
		handleZoomPastMeetingAttendeeUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-past-meetings-invitees":
		handleZoomPastMeetingInviteeUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-past-meetings-mappings":
		handleZoomPastMeetingMappingUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-past-meetings-recordings":
		handleZoomPastMeetingRecordingUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-past-meetings-summaries":
		handleZoomPastMeetingSummaryUpdate(ctx, key, v1Data)
		return false
	case "itx-zoom-past-meetings":
		handleZoomPastMeetingUpdate(ctx, key, v1Data)
		return false
	case "salesforce-merged_user":
		// Merged user records are used on-demand during user lookups from v1-objects KV bucket.
		// No special processing needed - just log for debugging.
		logger.With("key", key).DebugContext(ctx, "salesforce-merged_user record updated")
		return false
	case "salesforce-alternate_email__c":
		return handleAlternateEmailUpdate(ctx, key, v1Data)
	default:
		logger.With("key", key).WarnContext(ctx, "unknown object type, ignoring")
		return false
	}
}

// handleKVDelete processes a KV delete operation (hard delete from KV bucket).
// Returns true if the operation should be retried, false otherwise.
func handleKVDelete(ctx context.Context, entry jetstream.KeyValueEntry) bool {
	key := entry.Key()

	logger.With("key", key).InfoContext(ctx, "processing hard delete from KV bucket")
	return handleResourceDelete(ctx, key, "")
}

// handleKVSoftDelete processes a soft delete (record with _sdc_deleted_at field).
// Returns true if the operation should be retried, false otherwise.
func handleKVSoftDelete(ctx context.Context, key string, v1Data map[string]any) bool {
	// Extract v1 principal for soft deletes.
	v1Principal := extractV1Principal(ctx, v1Data)
	return handleResourceDelete(ctx, key, v1Principal)
}

// handleResourceDelete handles deletion of resources by key prefix with specified principal.
// Returns true if the operation should be retried, false otherwise.
func handleResourceDelete(ctx context.Context, key string, v1Principal string) bool {
	// Extract the prefix (everything before the first period) for faster lookup.
	prefix := key
	if dotIndex := strings.Index(key, "."); dotIndex != -1 {
		prefix = key[:dotIndex]
	}

	// Extract SFID from key (everything after the first period).
	sfid := ""
	if dotIndex := strings.Index(key, "."); dotIndex != -1 && dotIndex < len(key)-1 {
		sfid = key[dotIndex+1:]
	}

	if sfid == "" {
		logger.With("key", key).WarnContext(ctx, "cannot extract SFID from key for deletion")
		return false
	}

	// Determine the object type based on the key prefix and handle deletion.
	switch prefix {
	case "salesforce-project__c":
		return handleProjectDelete(ctx, key, sfid, v1Principal)
	case "platform-collaboration__c":
		return handleCommitteeDelete(ctx, key, sfid, v1Principal)
	case "platform-community__c":
		return handleCommitteeMemberDelete(ctx, key, sfid, v1Principal)
	case "salesforce-merged_user":
		// Merged user records are used on-demand during user lookups from the v1-objects KV bucket.
		// No special processing needed here for hard deletes; this handler does not write a KV tombstone.
		// TODO: Should clean up (tombstone) any per-user mappings, like the user sfid->email sfid index mapping.
		logger.With("key", key).DebugContext(ctx, "salesforce-merged_user record deleted")
		return false
	case "salesforce-alternate_email__c":
		// Alternate email records remain in v1-objects KV bucket with _sdc_deleted_at set by WAL handler.
		// The email mapping index also remains, but lookups will detect the soft-delete and skip the email.
		// TODO: Should clean up (remove) soft-deleted email SFIDs from v1-merged-user.alternate-emails.{userSfid} mapping records.
		logger.With("key", key).DebugContext(ctx, "salesforce-alternate_email__c record deleted")
		return false
	case "itx-zoom-meetings-v2", "itx-zoom-meetings-registrants-v2", "itx-zoom-meetings-mappings-v2", "itx-zoom-meetings-invite-responses-v2", "itx-zoom-past-meetings-attendees", "itx-zoom-past-meetings-invitees", "itx-zoom-past-meetings-mappings", "itx-zoom-past-meetings-recordings", "itx-zoom-past-meetings-summaries", "itx-zoom-past-meetings":
		logger.With("key", key).DebugContext(ctx, "meeting-related delete not yet implemented")
		return false
	default:
		logger.With("key", key).WarnContext(ctx, "unknown object type for deletion, ignoring")
		return false
	}
}

// tombstoneMapping stores a tombstone marker in the mapping KV store.
func tombstoneMapping(ctx context.Context, mappingKey string) error {
	if _, err := mappingsKV.Put(ctx, mappingKey, []byte(tombstoneMarker)); err != nil {
		return fmt.Errorf("failed to tombstone mapping %s: %w", mappingKey, err)
	}
	return nil
}

// isTombstonedMapping checks if a mapping is tombstoned.
func isTombstonedMapping(mappingValue []byte) bool {
	return string(mappingValue) == tombstoneMarker
}

// extractV1Principal extracts the v1 principal from v1 data.
// For soft deletes, only uses lastmodifiedbyid if lastmodifieddate is within 1 second of _sdc_deleted_at.
// For upserts, returns lastmodifiedbyid immediately if _sdc_deleted_at is not present.
func extractV1Principal(ctx context.Context, v1Data map[string]any) string {
	lastModifiedBy, hasModifiedBy := v1Data["lastmodifiedbyid"].(string)

	// If no lastmodifiedbyid, return empty (system principal).
	if !hasModifiedBy || lastModifiedBy == "" {
		return ""
	}

	deletedAt, hasDeletedAt := v1Data["_sdc_deleted_at"]

	// If this is not a soft delete (no _sdc_deleted_at), return principal immediately.
	if !hasDeletedAt || deletedAt == nil || deletedAt == "" {
		logger.With("lastmodifiedbyid", lastModifiedBy).
			DebugContext(ctx, "using v1 principal from upsert")
		return lastModifiedBy
	}

	// This is a soft delete - need to validate timestamps for safety.
	lastModifiedDate, hasModifiedDate := v1Data["lastmodifieddate"].(string)
	deletedAtStr, isDeletedAtString := deletedAt.(string)

	// If we don't have required timestamp fields for validation, fall back to system principal.
	if !hasModifiedDate || !isDeletedAtString {
		logger.With("has_modified_date", hasModifiedDate, "has_deleted_at_string", isDeletedAtString).
			DebugContext(ctx, "missing timestamp fields for soft delete validation, using system principal")
		return ""
	}

	// Parse timestamps.
	modifiedTime, err := parseTimestamp(lastModifiedDate)
	if err != nil {
		logger.With(errKey, err, "lastmodifieddate", lastModifiedDate).
			WarnContext(ctx, "failed to parse lastmodifieddate: using system principal instead of lastmodifiedbyid for deletion")
		return ""
	}

	deletedTime, err := parseTimestamp(deletedAtStr)
	if err != nil {
		logger.With(errKey, err, "_sdc_deleted_at", deletedAtStr).
			WarnContext(ctx, "failed to parse _sdc_deleted_at: using system principal instead of lastmodifiedbyid for deletion")
		return ""
	}

	// Check if timestamps are within 1 second of each other.
	timeDiff := deletedTime.Sub(modifiedTime)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}

	if timeDiff <= 1*time.Second {
		logger.With("lastmodifiedbyid", lastModifiedBy, "time_diff_seconds", timeDiff.Seconds()).
			DebugContext(ctx, "using v1 principal from soft delete")
		return lastModifiedBy
	}

	logger.With("lastmodifiedbyid", lastModifiedBy, "time_diff_seconds", timeDiff.Seconds()).
		DebugContext(ctx, "timestamps too far apart, using system principal for soft delete")
	return ""
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
