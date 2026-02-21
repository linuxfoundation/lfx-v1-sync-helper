// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// MessageAction represents the type of action being performed on a resource.
type MessageAction string

const (
	// MessageActionCreated indicates a new resource is being created.
	MessageActionCreated MessageAction = "created"
	// MessageActionUpdated indicates an existing resource is being updated.
	MessageActionUpdated MessageAction = "updated"
	// MessageActionDeleted indicates a resource is being deleted.
	MessageActionDeleted MessageAction = "deleted"
)

// NATS subject constants for meeting operations.
const (
	// IndexV1MeetingSubject is the subject for the v1 meeting indexing.
	IndexV1MeetingSubject = "lfx.index.v1_meeting"

	// UpdateAccessV1MeetingSubject is the subject for the v1 meeting access control updates.
	UpdateAccessV1MeetingSubject = "lfx.update_access.v1_meeting"

	// IndexV1MeetingRegistrantSubject is the subject for the v1 meeting registrant indexing.
	IndexV1MeetingRegistrantSubject = "lfx.index.v1_meeting_registrant"

	// V1MeetingRegistrantPutSubject is the subject for adding v1 meeting registrants.
	V1MeetingRegistrantPutSubject = "lfx.put_registrant.v1_meeting"

	// V1MeetingRegistrantRemoveSubject is the subject for removing a v1 meeting registrant's access.
	V1MeetingRegistrantRemoveSubject = "lfx.remove_registrant.v1_meeting"

	// IndexV1MeetingInviteResponseSubject is the subject for the v1 meeting invite response indexing.
	IndexV1MeetingInviteResponseSubject = "lfx.index.v1_meeting_rsvp"

	// DeleteAllAccessV1MeetingSubject is the subject for deleting all access control entries for a v1 meeting.
	DeleteAllAccessV1MeetingSubject = "lfx.delete_all_access.v1_meeting"

	// IndexV1PastMeetingSubject is the subject for the v1 past meeting indexing.
	IndexV1PastMeetingSubject = "lfx.index.v1_past_meeting"

	// V1PastMeetingUpdateAccessSubject is the subject for the v1 past meeting access control updates.
	V1PastMeetingUpdateAccessSubject = "lfx.update_access.v1_past_meeting"

	// IndexV1PastMeetingParticipantSubject is the subject for the v1 past meeting participant indexing.
	IndexV1PastMeetingParticipantSubject = "lfx.index.v1_past_meeting_participant"

	// V1PastMeetingParticipantPutSubject is the subject for the v1 past meeting participant access control updates.
	V1PastMeetingParticipantPutSubject = "lfx.put_participant.v1_past_meeting"

	// IndexV1PastMeetingRecordingSubject is the subject for the v1 past meeting recording indexing.
	IndexV1PastMeetingRecordingSubject = "lfx.index.v1_past_meeting_recording"

	// V1PastMeetingRecordingUpdateAccessSubject is the subject for the v1 past meeting recording access control updates.
	V1PastMeetingRecordingUpdateAccessSubject = "lfx.update_access.v1_past_meeting_recording"

	// IndexV1PastMeetingTranscriptSubject is the subject for the v1 past meeting transcript indexing.
	IndexV1PastMeetingTranscriptSubject = "lfx.index.v1_past_meeting_transcript"

	// V1PastMeetingTranscriptUpdateAccessSubject is the subject for the v1 past meeting transcript access control updates.
	V1PastMeetingTranscriptUpdateAccessSubject = "lfx.update_access.v1_past_meeting_transcript"

	// IndexV1PastMeetingSummarySubject is the subject for the v1 past meeting summary indexing.
	IndexV1PastMeetingSummarySubject = "lfx.index.v1_past_meeting_summary"

	// V1PastMeetingSummaryUpdateAccessSubject is the subject for the v1 past meeting summary access control updates.
	V1PastMeetingSummaryUpdateAccessSubject = "lfx.update_access.v1_past_meeting_summary"
)

// MeetingIndexerMessage is a NATS message schema for sending messages related to meetings CRUD operations.
type MeetingIndexerMessage struct {
	Action  MessageAction     `json:"action"`
	Headers map[string]string `json:"headers"`
	Data    any               `json:"data"`
	// Tags is a list of tags to be set on the indexed resource for search.
	Tags []string `json:"tags"`
}

// sendIndexerMessage sends the message to the NATS server for the indexer.
func sendIndexerMessage(ctx context.Context, subject string, action MessageAction, data any, tags []string) error {
	headers := make(map[string]string)

	// Extract authorization from context if available
	if authorization, ok := ctx.Value("authorization").(string); ok {
		headers["authorization"] = authorization
	} else {
		// Fallback for system-generated events that don't have user auth context
		// This is just a dummy value so that the indexer service can still process the message,
		// given that it requires an authorization header.
		headers["authorization"] = "Bearer v1-sync-helper"
	}

	// Extract principal from context if available
	if principal, ok := ctx.Value("principal").(string); ok {
		headers["x-on-behalf-of"] = principal
	}

	// Construct the indexer message
	message := MeetingIndexerMessage{
		Action:  action,
		Headers: headers,
		Data:    data,
		Tags:    tags,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal indexer message for subject %s: %w", subject, err)
	}

	logger.With("subject", subject, "action", action, "tags_count", len(tags)).DebugContext(ctx, "constructed indexer message")

	// Publish the message to NATS
	if err := natsConn.Publish(subject, messageBytes); err != nil {
		return fmt.Errorf("failed to publish indexer message to subject %s: %w", subject, err)
	}

	return nil
}

// sendAccessMessage sends a pre-marshalled message to the NATS server.
// This is a generic function that can be used for access control updates, put operations, etc.
func sendAccessMessage(subject string, messageBytes []byte) error {
	// Publish the message to NATS
	if err := natsConn.Publish(subject, messageBytes); err != nil {
		return fmt.Errorf("failed to publish message to subject %s: %w", subject, err)
	}

	return nil
}

// MeetingAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
type MeetingAccessMessage struct {
	UID        string   `json:"meeting_id"`
	Public     bool     `json:"public"`
	ProjectUID string   `json:"project_uid"`
	Organizers []string `json:"organizers"`
	Committees []string `json:"committees"`
}

// convertMapToInputMeeting converts a map[string]any to an InputMeeting struct.
func convertMapToInputMeeting(ctx context.Context, v1Data map[string]any) (*meetingInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for meeting: %w", err)
	}

	// Unmarshal JSON bytes into InputMeeting struct
	var meeting meetingInput
	if err := json.Unmarshal(jsonBytes, &meeting); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into meetingInput: %w", err)
	}

	// We need to populate the ID for the v2 system
	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		meeting.ID = meetingID
	}

	// Convert the v1 project ID since the json key is different,
	// then use that to get the v2 project UID.
	if projectSFID, ok := v1Data["proj_id"].(string); ok && projectSFID != "" {
		meeting.ProjectSFID = projectSFID

		// Take the v1 project salesforce ID and look up the v2 project UID.
		projectMappingKey := fmt.Sprintf("project.sfid.%s", meeting.ProjectSFID)
		if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
			meeting.ProjectUID = string(entry.Value())
		}
	}

	// Set show_meeting_attendees (an attribute that does not exist in PCC)
	meeting.ShowMeetingAttendees = shouldShowMeetingAttendees(meeting)

	// Convert v1 named fields to v2 named fields.
	if title, ok := v1Data["topic"].(string); ok && title != "" {
		meeting.Title = title
	}
	if description, ok := v1Data["agenda"].(string); ok && description != "" {
		meeting.Description = description
	}

	// Use the recording access value to set the artifact visibility.
	// Otherwise, fallback to the transcript or summary access values.
	// And as a last resort, fallback to the default value of "meeting_hosts".
	if recordingAccess, ok := v1Data["recording_access"].(string); ok && recordingAccess != "" {
		meeting.ArtifactVisibility = recordingAccess
	} else if transcriptAccess, ok := v1Data["transcript_access"].(string); ok && transcriptAccess != "" {
		meeting.ArtifactVisibility = transcriptAccess
	} else if summaryAccess, ok := v1Data["ai_summary_access"].(string); ok && summaryAccess != "" {
		meeting.ArtifactVisibility = summaryAccess
	} else {
		meeting.ArtifactVisibility = "meeting_hosts"
	}
	meeting.ZoomConfig = ZoomConfig{}
	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		meeting.ZoomConfig.MeetingID = meetingID
	}
	if passcode, ok := v1Data["passcode"].(string); ok && passcode != "" {
		meeting.ZoomConfig.Passcode = passcode
	}
	if aiCompanionEnabled, ok := v1Data["zoom_ai_enabled"].(bool); ok {
		meeting.ZoomConfig.AICompanionEnabled = aiCompanionEnabled
	}
	if aiSummaryRequireApproval, ok := v1Data["ai_summary_require_approval"].(bool); ok {
		meeting.ZoomConfig.AISummaryRequireApproval = aiSummaryRequireApproval
	}
	// Map v1 topic and agenda fields to v2 title and description in updated_occurrences
	// Also convert duration from string to int
	if updatedOccurrencesData, ok := v1Data["updated_occurrences"].([]any); ok {
		for i, occData := range updatedOccurrencesData {
			if occMap, ok := occData.(map[string]any); ok && i < len(meeting.UpdatedOccurrences) {
				// Map v1 topic field to v2 title field
				if topic, ok := occMap["topic"].(string); ok {
					meeting.UpdatedOccurrences[i].Title = topic
				}
				// Map v1 agenda field to v2 description field
				if agenda, ok := occMap["agenda"].(string); ok {
					meeting.UpdatedOccurrences[i].Description = agenda
				}
			}
		}
	}
	if updatedAt, ok := v1Data["modified_at"].(string); ok && updatedAt != "" {
		meeting.UpdatedAt = updatedAt
	}

	occurrences, err := calculateOccurrences(ctx, meeting, false, false, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate occurrences for meeting %s: %w", meeting.ID, err)
	}
	meeting.Occurrences = occurrences

	return &meeting, nil
}

func getMeetingTags(meeting *meetingInput) []string {
	tags := []string{
		meeting.ID,
		fmt.Sprintf("meeting_id:%s", meeting.ID),
		fmt.Sprintf("project_uid:%s", meeting.ProjectUID),
		fmt.Sprintf("title:%s", meeting.Title),
		fmt.Sprintf("meeting_type:%s", meeting.MeetingType),
	}
	for _, committee := range meeting.Committees {
		tags = append(tags, fmt.Sprintf("committee_uid:%s", committee.UID))
	}
	return tags
}

// handleZoomMeetingUpdate processes a zoom meeting update from itx-zoom-meetings-v2 records.
func handleZoomMeetingUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom meeting update")

	// Convert v1Data map to InputMeeting struct
	meeting, err := convertMapToInputMeeting(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to InputMeeting")
		return
	}

	// Extract the meeting ID
	meetingID := meeting.ID
	if meetingID == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid meeting_id in v1 meeting data")
		return
	}
	funcLogger = funcLogger.With("meeting_id", meetingID)

	// Check if parent project exists in mappings before proceeding. Because
	// convertMapToInputMeeting has already looked up the SFID project ID
	// mapping, we don't need to do it again: we can just check if ProjectID (v2
	// UID) is set.
	if meeting.ProjectUID == "" {
		funcLogger.With("project_sfid", meeting.ProjectSFID).InfoContext(ctx, "skipping meeting sync - parent project not found in mappings")
		return
	}

	// Try to get committee mappings from the index first
	var committees []string
	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.meeting-mappings.%s", meetingID)
	indexEntry, err := mappingsKV.Get(ctx, indexKey)
	if err == nil && indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to unmarshal meeting mapping index")
		} else {
			// Extract committee IDs from the mappings
			for committeeID := range committeeMappings {
				committees = append(committees, committeeID)
			}
		}
	}

	// Fallback: Extract committees from v1Data if no mappings found
	if len(committees) == 0 {
		if committeesData, ok := v1Data["committees"].([]any); ok {
			for _, c := range committeesData {
				if committee, ok := c.(map[string]any); ok {
					if committeeUID, ok := committee["uid"].(string); ok && committeeUID != "" {
						committees = append(committees, committeeUID)
					}
				}
			}
		}
	}

	mappingKey := fmt.Sprintf("v1_meetings.%s", meetingID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getMeetingTags(meeting)
	if err := sendIndexerMessage(ctx, IndexV1MeetingSubject, indexerAction, meeting, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send meeting indexer message")
		return
	}

	accessMsg := MeetingAccessMessage{
		UID:        meetingID,
		Public:     meeting.Visibility == "public",
		ProjectUID: meeting.ProjectUID,
		Organizers: []string{},
		Committees: committees,
	}

	accessMsgBytes, err := json.Marshal(accessMsg)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal access message")
		return
	}

	if err := sendAccessMessage(UpdateAccessV1MeetingSubject, accessMsgBytes); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send meeting access message")
		return
	}

	if meetingID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store meeting mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent meeting indexer and access messages")
}

// meetingDeleteConfig holds the configuration for deleting a meeting-related resource.
type meetingDeleteConfig struct {
	// indexerSubject is the NATS subject to send the indexer delete message to.
	indexerSubject string
	// deleteAllAccessSubject is the NATS subject to send the delete-all-access message to.
	// Leave empty to skip sending an access control delete message.
	deleteAllAccessSubject string
	// tombstoneKeyFmts are fmt format strings (each with one %s for the ID) for
	// mappings that should be tombstoned on delete.
	tombstoneKeyFmts []string
}

// handleMeetingTypeDelete is a generic delete handler for meeting-related resources.
// It sends the indexer delete message, optionally sends a delete-all-access message,
// and tombstones any configured mapping keys.
// message is the pre-built payload for the access message; callers are responsible for constructing it.
// Returns true if the operation should be retried, false otherwise.
func handleMeetingTypeDelete(ctx context.Context, key, id string, message []byte, cfg meetingDeleteConfig) bool {
	funcLogger := logger.With("key", key, "id", id)
	funcLogger.DebugContext(ctx, "processing meeting-related delete")

	if err := sendIndexerMessage(ctx, cfg.indexerSubject, MessageActionDeleted, id, []string{}); err != nil {
		funcLogger.With(errKey, err, "subject", cfg.indexerSubject).ErrorContext(ctx, "failed to send delete indexer message")
		return true
	}

	if cfg.deleteAllAccessSubject != "" {
		if err := sendAccessMessage(cfg.deleteAllAccessSubject, message); err != nil {
			funcLogger.With(errKey, err, "subject", cfg.deleteAllAccessSubject).ErrorContext(ctx, "failed to send delete-all-access message")
			return true
		}
	}

	for _, keyFmt := range cfg.tombstoneKeyFmts {
		if err := tombstoneMapping(ctx, fmt.Sprintf(keyFmt, id)); err != nil {
			funcLogger.With(errKey, err, "mapping_key", fmt.Sprintf(keyFmt, id)).WarnContext(ctx, "failed to tombstone mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully processed delete")
	return false
}

// handleZoomMeetingDelete processes a deletion of an itx-zoom-meetings-v2 record.
// Returns true if the operation should be retried, false otherwise.
func handleZoomMeetingDelete(ctx context.Context, key string, meetingID string) bool {
	funcLogger := logger.With("key", key, "meeting_id", meetingID)

	// Skip if already tombstoned — prevents double processing when the DynamoDB path
	// has already handled the delete before the KV watcher fires.
	mappingKey := fmt.Sprintf("v1_meetings.%s", meetingID)
	if entry, err := mappingsKV.Get(ctx, mappingKey); err == nil && isTombstonedMapping(entry.Value()) {
		funcLogger.DebugContext(ctx, "meeting delete already processed, skipping")
		return false
	}

	return handleMeetingTypeDelete(ctx, key, meetingID, []byte(meetingID), meetingDeleteConfig{
		indexerSubject:         IndexV1MeetingSubject,
		deleteAllAccessSubject: DeleteAllAccessV1MeetingSubject,
		tombstoneKeyFmts:       []string{"v1_meetings.%s", "v1-mappings.meeting-mappings.%s"},
	})
}

// handleZoomMeetingRegistrantDelete processes a deletion of an itx-zoom-meetings-registrants-v2 record.
// v1Data should be the old_image from the source event when available; nil falls back to sending registrantID only.
// Returns true if the operation should be retried, false otherwise.
func handleZoomMeetingRegistrantDelete(ctx context.Context, key string, registrantID string, v1Data map[string]any) bool {
	funcLogger := logger.With("key", key, "registrant_id", registrantID)

	// Skip if already tombstoned — prevents double processing when the DynamoDB path
	// has already handled the delete before the KV watcher fires.
	mappingKey := fmt.Sprintf("v1_meeting_registrants.%s", registrantID)
	if entry, err := mappingsKV.Get(ctx, mappingKey); err == nil && isTombstonedMapping(entry.Value()) {
		funcLogger.DebugContext(ctx, "registrant delete already processed, skipping")
		return false
	}

	if v1Data == nil {
		funcLogger.WarnContext(ctx, "no v1Data available for registrant delete, skipping")
		return false
	}

	// Extract meeting_id - return early if missing, consistent with update handler.
	meetingID, ok := v1Data["meeting_id"].(string)
	if !ok || meetingID == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid meeting_id in v1Data for registrant delete")
		return false
	}
	funcLogger = funcLogger.With("meeting_id", meetingID)

	// Extract username and host fields.
	username, _ := v1Data["username"].(string)
	host, _ := v1Data["host"].(bool)

	var message []byte
	var deleteAllAccessSubject string

	// Only construct and send the access message if username is present, consistent with update handler.
	// Without a username, access control cannot identify which user to remove access for.
	if username != "" {
		accessMsg := MeetingRegistrantAccessMessage{
			ID:        registrantID,
			MeetingID: meetingID,
			Username:  mapUsernameToAuthSub(username),
			Host:      host,
		}
		var err error
		if message, err = json.Marshal(accessMsg); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal registrant access message")
			return false
		}
		deleteAllAccessSubject = V1MeetingRegistrantRemoveSubject
	} else {
		// No username - skip access control message (cannot identify user without username).
		funcLogger.DebugContext(ctx, "no username in v1Data, skipping access control message for registrant delete")
		message = []byte(registrantID)
		deleteAllAccessSubject = "" // Empty string skips access control message
	}

	return handleMeetingTypeDelete(ctx, key, registrantID, message, meetingDeleteConfig{
		indexerSubject:         IndexV1MeetingRegistrantSubject,
		deleteAllAccessSubject: deleteAllAccessSubject,
		tombstoneKeyFmts:       []string{"v1_meeting_registrants.%s"},
	})
}

// convertMapToInputMeeting converts a map[string]any to an InputMeeting struct.
func convertMapToInputMeetingMapping(v1Data map[string]any) (*ZoomMeetingMappingDB, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for meeting mapping: %w", err)
	}

	// Unmarshal JSON bytes into ZoomMeetingMappingDB struct
	var mapping ZoomMeetingMappingDB
	if err := json.Unmarshal(jsonBytes, &mapping); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into ZoomMeetingMappingDB: %w", err)
	}

	return &mapping, nil
}

// mappingCommittee represents committee mapping data.
type mappingCommittee struct {
	CommitteeID      string   `json:"committee_id"`
	CommitteeFilters []string `json:"committee_filters"`
}

// handleZoomMeetingMappingUpdate processes a zoom meeting mapping update from itx-zoom-meetings-mappings-v2 records.
// When a mapping is created/updated, we need to fetch the associated meeting and trigger a re-index with updated committees.
func handleZoomMeetingMappingUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom meeting mapping update")

	// Convert v1Data map to ZoomMeetingMappingDB struct
	mapping, err := convertMapToInputMeetingMapping(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to ZoomMeetingMappingDB")
		return
	}

	// Extract the meeting ID from the mapping
	meetingID := mapping.MeetingID
	if meetingID == "" {
		funcLogger.ErrorContext(ctx, "missing meeting_id in mapping data")
		return
	}
	mappingKey := fmt.Sprintf("v1_meetings.%s", meetingID)

	// Extract the committee ID from the mapping
	committeeID := mapping.CommitteeID
	if committeeID == "" {
		funcLogger.With("meeting_id", meetingID).WarnContext(ctx, "mapping has no committee_id")
		return
	}

	// Fetch and parse the meeting data.
	meetingKey := fmt.Sprintf("itx-zoom-meetings-v2.%s", meetingID)
	meetingData, exists, err := getV1ObjectData(ctx, meetingKey)
	if err != nil {
		funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to get meeting data from KV bucket")
		return
	}
	if !exists {
		funcLogger.With("meeting_id", meetingID).WarnContext(ctx, "meeting data not found or deleted in KV bucket")
		return
	}

	// Convert meeting data to typed struct
	meeting, err := convertMapToInputMeeting(ctx, meetingData)
	if err != nil {
		funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to convert meeting data")
		return
	}

	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.meeting-mappings.%s", meetingID)
	indexEntry, _ := mappingsKV.Get(ctx, indexKey)
	if indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).WarnContext(ctx, "failed to unmarshal meeting mapping index")
			return
		}
	}

	if meeting != nil {
		committees := []string{}
		meeting.Committees = []Committee{}
		for _, committee := range committeeMappings {
			committees = append(committees, committee.CommitteeID)
			meeting.Committees = append(meeting.Committees, Committee{
				UID:                   committee.CommitteeID,
				AllowedVotingStatuses: committee.CommitteeFilters,
			})
		}

		// Determine indexer action based on mapping existence
		indexerAction := MessageActionCreated
		if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
			indexerAction = MessageActionUpdated
		}

		// Send meeting indexer message with the meeting data
		tags := getMeetingTags(meeting)
		if err := sendIndexerMessage(ctx, IndexV1MeetingSubject, indexerAction, meeting, tags); err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to send meeting indexer message")
			return
		}

		// Send meeting access message with updated committees
		accessMsg := MeetingAccessMessage{
			UID:        meetingID,
			Public:     meeting.Visibility == "public",
			ProjectUID: meeting.ProjectUID,
			Organizers: []string{},
			Committees: committees,
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to marshal access message")
			return
		}

		if err := sendAccessMessage(UpdateAccessV1MeetingSubject, accessMsgBytes); err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to send meeting access message")
			return
		}
	}

	// Store the mapping
	if meetingID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).WarnContext(ctx, "failed to store meeting mapping")
		}
	}

	// Only add the committee mapping if it doesn't already exist.
	if _, ok := committeeMappings[mapping.ID]; !ok {
		committeeMappings[mapping.ID] = mappingCommittee{
			CommitteeID:      committeeID,
			CommitteeFilters: mapping.CommitteeFilters,
		}
		indexKey = fmt.Sprintf("v1-mappings.meeting-mappings.%s", meetingID)
		committeeMappingsBytes, err := json.Marshal(committeeMappings)
		if err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to marshal committee mappings")
			return
		}
		if _, err := mappingsKV.Put(ctx, indexKey, committeeMappingsBytes); err != nil {
			funcLogger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to store committee mappings")
			return
		}
	}

	funcLogger.With("meeting_id", meetingID, "committee_id", committeeID).InfoContext(ctx, "successfully triggered meeting re-index with updated committees")
}

// convertMapToInputRegistrant converts a map[string]any to a RegistrantInput struct.
func convertMapToInputRegistrant(v1Data map[string]any) (*registrantInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for registrant: %w", err)
	}

	// Unmarshal JSON bytes into RegistrantInput struct
	var registrant registrantInput
	if err := json.Unmarshal(jsonBytes, &registrant); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into registrantInput: %w", err)
	}

	if registrantID, ok := v1Data["registrant_id"].(string); ok && registrantID != "" {
		registrant.UID = registrantID
	}

	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		registrant.MeetingID = meetingID
	}

	if committeeUID, ok := v1Data["committee_id"].(string); ok && committeeUID != "" {
		registrant.CommitteeUID = committeeUID
	}

	if orgName, ok := v1Data["org"].(string); ok && orgName != "" {
		registrant.OrgName = orgName
	}

	if avatarURL, ok := v1Data["profile_picture"].(string); ok && avatarURL != "" {
		registrant.AvatarURL = avatarURL
	}

	if modifiedAt, ok := v1Data["modified_at"].(string); ok && modifiedAt != "" {
		registrant.UpdatedAt = modifiedAt
	}

	return &registrant, nil
}

// MeetingRegistrantAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
type MeetingRegistrantAccessMessage struct {
	ID        string `json:"id"`
	MeetingID string `json:"meeting_id"`
	Username  string `json:"username"`
	Host      bool   `json:"host"`
}

func getRegistrantTags(registrant *registrantInput) []string {
	tags := []string{
		registrant.UID,
		fmt.Sprintf("registrant_uid:%s", registrant.UID),
		fmt.Sprintf("meeting_id:%s", registrant.MeetingID),
		fmt.Sprintf("committee_uid:%s", registrant.CommitteeUID),
		fmt.Sprintf("first_name:%s", registrant.FirstName),
		fmt.Sprintf("last_name:%s", registrant.LastName),
		fmt.Sprintf("email:%s", registrant.Email),
	}
	if registrant.Username != "" {
		tags = append(tags, fmt.Sprintf("username:%s", registrant.Username))
	}
	return tags
}

// handleZoomMeetingRegistrantUpdate processes a zoom meeting registrant update from itx-zoom-meetings-registrants-v2 records.
// Returns true if the operation should be retried, false otherwise.
func handleZoomMeetingRegistrantUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom meeting registrant update")

	// Convert v1Data map to RegistrantInput struct
	registrant, err := convertMapToInputRegistrant(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to registrantInput")
		return false
	}

	// Extract the registrant ID
	registrantID := registrant.UID
	if registrantID == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid id in v1 registrant data")
		return false
	}
	funcLogger = funcLogger.With("registrant_id", registrantID)

	// If username is blank but we have a v1 Platform ID (user_id), lookup the username.
	if registrant.Username == "" && registrant.UserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, registrant.UserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			registrant.Username = v1User.Username
			funcLogger.With("user_id", registrant.UserID, "username", v1User.Username).DebugContext(ctx, "looked up username for registrant")
		} else {
			if lookupErr != nil {
				funcLogger.With(errKey, lookupErr, "user_id", registrant.UserID).WarnContext(ctx, "failed to lookup v1 user for registrant")
			}
		}
	}

	// Check if parent meeting exists in mappings before proceeding.
	if registrant.MeetingID == "" {
		funcLogger.ErrorContext(ctx, "meeting registrant missing required parent meeting ID")
		return false
	}
	funcLogger = funcLogger.With("meeting_id", registrant.MeetingID)
	meetingMappingKey := fmt.Sprintf("v1_meetings.%s", registrant.MeetingID)
	if _, err := mappingsKV.Get(ctx, meetingMappingKey); err != nil {
		funcLogger.With(errKey, err).InfoContext(ctx, "parent meeting not found in mappings, will retry meeting registrant sync")
		return true // Retry - meeting might be stored shortly
	}

	mappingKey := fmt.Sprintf("v1_meeting_registrants.%s", registrantID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getRegistrantTags(registrant)
	if err := sendIndexerMessage(ctx, IndexV1MeetingRegistrantSubject, indexerAction, registrant, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send registrant indexer message")
		return false
	}

	// We only send the access message if the registrant has a username.
	if registrant.Username != "" {
		// Map username to Auth0 "sub" format for v2 compatibility.
		authSub := mapUsernameToAuthSub(registrant.Username)
		accessMsg := MeetingRegistrantAccessMessage{
			ID:        registrantID,
			MeetingID: registrant.MeetingID,
			Username:  authSub,
			Host:      *registrant.Host,
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal access message")
			return false
		}

		if err := sendAccessMessage(V1MeetingRegistrantPutSubject, accessMsgBytes); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send registrant put message")
			return false
		}
	}

	if registrantID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store registrant mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent registrant indexer and put messages")
	return false
}

func convertMapToInputInviteResponse(v1Data map[string]any) (*inviteResponseInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for invite response: %w", err)
	}

	// Unmarshal JSON bytes into InviteResponseInput struct
	var inviteResponse inviteResponseInput
	if err := json.Unmarshal(jsonBytes, &inviteResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into inviteResponseInput: %w", err)
	}

	// Convert the v1 response type to the v2 response type.
	inviteResponse.Response = "" // reset the response to an empty string to avoid keeping the v1 value.
	if response, ok := v1Data["response"].(string); ok {
		// There are technically other response types in v1, but they are very rare and the v2 system
		// doesn't care about the other types so they can be ignored.
		switch response {
		case "ACCEPTED":
			inviteResponse.Response = RSVPResponseAccepted
		case "TENTATIVE":
			inviteResponse.Response = RSVPResponseMaybe
		case "DECLINED":
			inviteResponse.Response = RSVPResponseDeclined
		}
	}

	// Convert the v1 scope type to the v2 scope type.
	// The conversion is based on the occurrence_id and is_response_recurring fields,
	// which helps indicate whether the response is for one occurrence, recurring from an occurrence onward, or for all occurrences.
	if _, ok := v1Data["occurrence_id"].(string); ok {
		if isResponseRecurring, ok := v1Data["is_response_recurring"].(bool); ok && isResponseRecurring {
			inviteResponse.Scope = RSVPScopeThisAndFollowing
		} else {
			inviteResponse.Scope = RSVPScopeSingle
		}
	} else {
		inviteResponse.Scope = RSVPScopeAll
	}

	return &inviteResponse, nil
}

func getInviteResponseTags(inviteResponse *inviteResponseInput) []string {
	tags := []string{
		inviteResponse.ID,
		fmt.Sprintf("invite_response_uid:%s", inviteResponse.ID),
		fmt.Sprintf("meeting_and_occurrence_id:%s", inviteResponse.MeetingAndOccurrenceID),
		fmt.Sprintf("meeting_id:%s", inviteResponse.MeetingID),
		fmt.Sprintf("registrant_uid:%s", inviteResponse.RegistrantID),
		fmt.Sprintf("email:%s", inviteResponse.Email),
	}
	if inviteResponse.Username != "" {
		tags = append(tags, fmt.Sprintf("username:%s", inviteResponse.Username))
	}
	return tags
}

// handleZoomMeetingInviteResponseUpdate processes a zoom meeting invite response update from itx-zoom-meetings-invite-responses-v2 records.
// Returns true if the operation should be retried, false otherwise.
func handleZoomMeetingInviteResponseUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom meeting invite response update")

	// Convert v1Data map to InviteResponseInput struct
	inviteResponse, err := convertMapToInputInviteResponse(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to inviteResponseInput")
		return false
	}

	// Skip sync for Mailer Daemon email addresses.
	if inviteResponse.Email == "MAILER-DAEMON@us-west-2.amazonses.com" {
		return false
	}

	// If username is blank but we have a v1 Platform ID (user_id), lookup the username.
	if inviteResponse.Username == "" && inviteResponse.UserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, inviteResponse.UserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			inviteResponse.Username = mapUsernameToAuthSub(v1User.Username)
			funcLogger.With("user_id", inviteResponse.UserID, "username", v1User.Username).DebugContext(ctx, "looked up username for invite response")
		} else {
			if lookupErr != nil {
				funcLogger.With(errKey, lookupErr, "user_id", inviteResponse.UserID).WarnContext(ctx, "failed to lookup v1 user for invite response")
			}
		}
	}

	// Extract the invite response ID
	inviteResponseID := inviteResponse.ID
	if inviteResponseID == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid id in v1 invite response data")
		return false
	}

	funcLogger = funcLogger.With("invite_response_id", inviteResponseID)

	// Check if parent meeting exists in mappings before proceeding.
	if inviteResponse.MeetingID == "" {
		funcLogger.ErrorContext(ctx, "invite response missing required parent meeting ID")
		return false
	}
	funcLogger = funcLogger.With("meeting_id", inviteResponse.MeetingID)
	meetingMappingKey := fmt.Sprintf("v1_meetings.%s", inviteResponse.MeetingID)
	if _, err := mappingsKV.Get(ctx, meetingMappingKey); err != nil {
		funcLogger.With(errKey, err).InfoContext(ctx, "parent meeting not found in mappings, will retry invite response sync")
		return true
	}

	mappingKey := fmt.Sprintf("v1_invite_responses.%s", inviteResponseID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getInviteResponseTags(inviteResponse)
	if err := sendIndexerMessage(ctx, IndexV1MeetingInviteResponseSubject, indexerAction, inviteResponse, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send invite response indexer message")
		return false
	}

	if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
		funcLogger.With(errKey, err).WarnContext(ctx, "failed to store invite response mapping")
	}

	funcLogger.InfoContext(ctx, "successfully sent invite response indexer message")
	return false
}

// PastMeetingAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
// Past meetings don't have organizers, but they have a reference to the original meeting.
type PastMeetingAccessMessage struct {
	UID        string   `json:"uid"`
	MeetingUID string   `json:"meeting_uid"`
	Public     bool     `json:"public"`
	ProjectUID string   `json:"project_uid"`
	Committees []string `json:"committees"`
}

// convertMapToInputPastMeeting converts a map[string]any to a PastMeetingInput struct.
func convertMapToInputPastMeeting(ctx context.Context, v1Data map[string]any) (*pastMeetingInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for past meeting: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingInput struct
	var pastMeeting pastMeetingInput
	if err := json.Unmarshal(jsonBytes, &pastMeeting); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into pastMeetingInput: %w", err)
	}

	// We need to populate the ID for the v2 system
	if meetingAndOccurrenceID, ok := v1Data["meeting_and_occurrence_id"].(string); ok && meetingAndOccurrenceID != "" {
		pastMeeting.MeetingAndOccurrenceID = meetingAndOccurrenceID
		pastMeeting.ID = meetingAndOccurrenceID
	}

	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		pastMeeting.MeetingID = meetingID
		pastMeeting.PlatformMeetingID = meetingID
	}

	// Convert the v1 project ID since the json key is different,
	// then use that to get the v2 project UID.
	if projectSFID, ok := v1Data["proj_id"].(string); ok && projectSFID != "" {
		pastMeeting.ProjectSFID = projectSFID
	}

	// Take the v1 project salesforce ID and look up the v2 project UID.
	projectMappingKey := fmt.Sprintf("project.sfid.%s", pastMeeting.ProjectSFID)
	if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
		pastMeeting.ProjectUID = string(entry.Value())
	}

	// Convert v1 named fields to v2 named fields.
	if title, ok := v1Data["topic"].(string); ok && title != "" {
		pastMeeting.Title = title
	}
	if description, ok := v1Data["agenda"].(string); ok && description != "" {
		pastMeeting.Description = description
	}

	pastMeeting.ZoomConfig = &ZoomConfig{}
	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		pastMeeting.ZoomConfig.MeetingID = meetingID
	}
	if passcode, ok := v1Data["passcode"].(string); ok && passcode != "" {
		pastMeeting.ZoomConfig.Passcode = passcode
	}
	if aiCompanionEnabled, ok := v1Data["zoom_ai_enabled"].(bool); ok {
		pastMeeting.ZoomConfig.AICompanionEnabled = aiCompanionEnabled
	}
	if aiSummaryRequireApproval, ok := v1Data["ai_summary_require_approval"].(bool); ok {
		pastMeeting.ZoomConfig.AISummaryRequireApproval = aiSummaryRequireApproval
	}

	// Use the recording access value to set the artifact visibility.
	// Otherwise, fallback to the transcript or summary access values.
	// And as a last resort, fallback to the default value of "meeting_hosts".
	if recordingAccess, ok := v1Data["recording_access"].(string); ok && recordingAccess != "" {
		pastMeeting.ArtifactVisibility = recordingAccess
	} else if transcriptAccess, ok := v1Data["transcript_access"].(string); ok && transcriptAccess != "" {
		pastMeeting.ArtifactVisibility = transcriptAccess
	} else if summaryAccess, ok := v1Data["ai_summary_access"].(string); ok && summaryAccess != "" {
		pastMeeting.ArtifactVisibility = summaryAccess
	} else {
		pastMeeting.ArtifactVisibility = "meeting_hosts"
	}

	if modifiedAt, ok := v1Data["modified_at"].(string); ok && modifiedAt != "" {
		pastMeeting.UpdatedAt = modifiedAt
	}

	return &pastMeeting, nil
}

func getPastMeetingTags(pastMeeting *pastMeetingInput) []string {
	tags := []string{
		pastMeeting.MeetingAndOccurrenceID,
		fmt.Sprintf("meeting_and_occurrence_id:%s", pastMeeting.MeetingAndOccurrenceID),
		fmt.Sprintf("meeting_id:%s", pastMeeting.MeetingID),
		fmt.Sprintf("project_uid:%s", pastMeeting.ProjectUID),
		fmt.Sprintf("occurrence_id:%s", pastMeeting.OccurrenceID),
		fmt.Sprintf("title:%s", pastMeeting.Title),
	}
	for _, committee := range pastMeeting.Committees {
		tags = append(tags, fmt.Sprintf("committee_uid:%s", committee.UID))
	}
	return tags
}

// handleZoomPastMeetingUpdate processes a zoom past meeting update from itx-zoom-past-meetings-v2 records.
func handleZoomPastMeetingUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom past meeting update")

	// Convert v1Data map to PastMeetingInput struct
	pastMeeting, err := convertMapToInputPastMeeting(ctx, v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to pastMeetingInput")
		return
	}

	// Extract the past meeting UID (MeetingAndOccurrenceID)
	uid := pastMeeting.MeetingAndOccurrenceID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid meeting_and_occurrence_id in v1 past meeting data")
		return
	}
	funcLogger = funcLogger.With("meeting_and_occurrence_id", uid)

	// Check if parent meeting exists in mappings before proceeding.
	if pastMeeting.MeetingID == "" {
		funcLogger.ErrorContext(ctx, "past meeting missing required parent meeting ID")
		return
	}
	funcLogger = funcLogger.With("meeting_id", pastMeeting.MeetingID)
	meetingMappingKey := fmt.Sprintf("v1_meetings.%s", pastMeeting.MeetingID)
	if _, err := mappingsKV.Get(ctx, meetingMappingKey); err != nil {
		funcLogger.InfoContext(ctx, "skipping past meeting sync - parent meeting not found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("v1_past_meetings.%s", uid)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getPastMeetingTags(pastMeeting)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingSubject, indexerAction, pastMeeting, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send past meeting indexer message")
		return
	}

	// Try to get committee mappings from the index first
	var committees []string
	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.past-meeting-mappings.%s", uid)
	indexEntry, err := mappingsKV.Get(ctx, indexKey)
	if err == nil && indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to unmarshal past meeting mapping index")
		} else {
			// Extract committee IDs from the mappings
			for committeeID := range committeeMappings {
				committees = append(committees, committeeID)
			}
		}
	}

	// Fallback: Extract committees from v1Data if no mappings found
	if len(committees) == 0 {
		if committeesData, ok := v1Data["committees"].([]any); ok {
			for _, c := range committeesData {
				if committee, ok := c.(map[string]any); ok {
					if committeeUID, ok := committee["uid"].(string); ok && committeeUID != "" {
						committees = append(committees, committeeUID)
					}
				}
			}
		}
	}

	accessMsg := PastMeetingAccessMessage{
		UID:        uid,
		MeetingUID: pastMeeting.MeetingID,
		Public:     pastMeeting.Visibility == "public",
		ProjectUID: pastMeeting.ProjectUID,
		Committees: committees,
	}

	accessMsgBytes, err := json.Marshal(accessMsg)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal past meeting access message")
		return
	}

	if err := sendAccessMessage(V1PastMeetingUpdateAccessSubject, accessMsgBytes); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send past meeting access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store past meeting mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent past meeting indexer and access messages")
}

// convertMapToInputPastMeetingMapping converts a map[string]any to a ZoomPastMeetingMappingDB struct.
func convertMapToInputPastMeetingMapping(v1Data map[string]any) (*ZoomPastMeetingMappingDB, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for past meeting mapping: %w", err)
	}

	// Unmarshal JSON bytes into ZoomPastMeetingMappingDB struct
	var mapping ZoomPastMeetingMappingDB
	if err := json.Unmarshal(jsonBytes, &mapping); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into ZoomPastMeetingMappingDB: %w", err)
	}

	return &mapping, nil
}

// handleZoomPastMeetingMappingUpdate processes a zoom past meeting mapping update from itx-zoom-past-meetings-mappings records.
// When a mapping is created/updated, we need to fetch the associated past meeting and trigger a re-index with updated committees.
func handleZoomPastMeetingMappingUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom past meeting mapping update")

	// Convert v1Data map to ZoomPastMeetingMappingDB struct
	mapping, err := convertMapToInputPastMeetingMapping(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to ZoomPastMeetingMappingDB")
		return
	}

	// Extract the meeting_and_occurrence_id from the mapping
	meetingAndOccurrenceID := mapping.MeetingAndOccurrenceID
	if meetingAndOccurrenceID == "" {
		funcLogger.ErrorContext(ctx, "missing meeting_and_occurrence_id in mapping data")
		return
	}
	funcLogger = funcLogger.With("meeting_and_occurrence_id", meetingAndOccurrenceID)
	mappingKey := fmt.Sprintf("v1_past_meetings.%s", meetingAndOccurrenceID)

	// Extract the committee ID from the mapping
	committeeID := mapping.CommitteeID
	if committeeID == "" {
		funcLogger.WarnContext(ctx, "mapping has no committee_id")
		return
	}

	// Fetch and parse the past meeting data.
	pastMeetingKey := fmt.Sprintf("itx-zoom-past-meetings.%s", meetingAndOccurrenceID)
	pastMeetingData, exists, err := getV1ObjectData(ctx, pastMeetingKey)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to get past meeting data from KV bucket")
		return
	}
	if !exists {
		funcLogger.WarnContext(ctx, "past meeting data not found or deleted in KV bucket")
		return
	}

	// Convert past meeting data to typed struct
	pastMeeting, err := convertMapToInputPastMeeting(ctx, pastMeetingData)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert past meeting data")
		return
	}

	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.past-meeting-mappings.%s", meetingAndOccurrenceID)
	indexEntry, _ := mappingsKV.Get(ctx, indexKey)
	if indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to unmarshal past meeting mapping index")
			return
		}
	}

	if pastMeeting != nil {
		committees := []string{}
		for _, committee := range committeeMappings {
			committees = append(committees, committee.CommitteeID)
		}

		// Determine indexer action based on mapping existence
		indexerAction := MessageActionCreated
		if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
			indexerAction = MessageActionUpdated
		}

		// Send past meeting indexer message with the past meeting data
		tags := getPastMeetingTags(pastMeeting)
		if err := sendIndexerMessage(ctx, IndexV1PastMeetingSubject, indexerAction, pastMeeting, tags); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send past meeting indexer message")
			return
		}

		// Send past meeting access message with updated committees
		accessMsg := PastMeetingAccessMessage{
			UID:        meetingAndOccurrenceID,
			MeetingUID: pastMeeting.MeetingID,
			Public:     pastMeeting.Visibility == "public",
			ProjectUID: pastMeeting.ProjectUID,
			Committees: committees,
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal access message")
			return
		}

		if err := sendAccessMessage(V1PastMeetingUpdateAccessSubject, accessMsgBytes); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send past meeting access message")
			return
		}
	}

	// Store the mapping
	if meetingAndOccurrenceID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store past meeting mapping")
		}
	}

	// Only add the committee mapping if it doesn't already exist.
	if _, ok := committeeMappings[mapping.ID]; !ok {
		committeeMappings[mapping.ID] = mappingCommittee{
			CommitteeID:      committeeID,
			CommitteeFilters: mapping.CommitteeFilters,
		}
		indexKey = fmt.Sprintf("v1-mappings.past-meeting-mappings.%s", meetingAndOccurrenceID)
		committeeMappingsBytes, err := json.Marshal(committeeMappings)
		if err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal committee mappings")
			return
		}
		if _, err := mappingsKV.Put(ctx, indexKey, committeeMappingsBytes); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to store committee mappings")
			return
		}
	}

	funcLogger.InfoContext(ctx, "successfully triggered past meeting re-index with updated committees")
}

// PastMeetingParticipantAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
type PastMeetingParticipantAccessMessage struct {
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id"`
	Username               string `json:"username"`
	Host                   bool   `json:"host"`
	IsInvited              bool   `json:"is_invited"`
	IsAttended             bool   `json:"is_attended"`
}

// convertMapToInputPastMeetingInvitee converts a map[string]any to a ZoomPastMeetingInviteeDatabase struct.
func convertMapToInputPastMeetingInvitee(v1Data map[string]any) (*pastMeetingInviteeInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for past meeting invitee: %w", err)
	}

	// Unmarshal JSON bytes into ZoomPastMeetingInviteeDatabase struct
	var invitee pastMeetingInviteeInput
	if err := json.Unmarshal(jsonBytes, &invitee); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into ZoomPastMeetingInviteeDatabase: %w", err)
	}

	if inviteeID, ok := v1Data["invitee_id"].(string); ok && inviteeID != "" {
		invitee.ID = inviteeID
	}

	return &invitee, nil
}

func getPastMeetingParticipantTags(participant *V2PastMeetingParticipant) []string {
	tags := []string{
		participant.UID,
		fmt.Sprintf("past_meeting_participant_uid:%s", participant.UID),
		fmt.Sprintf("meeting_and_occurrence_id:%s", participant.MeetingAndOccurrenceID),
		fmt.Sprintf("meeting_id:%s", participant.MeetingID),
		fmt.Sprintf("first_name:%s", participant.FirstName),
		fmt.Sprintf("last_name:%s", participant.LastName),
		fmt.Sprintf("email:%s", participant.Email),
	}
	if participant.Username != "" {
		tags = append(tags, fmt.Sprintf("username:%s", participant.Username))
	}
	return tags
}

// handleZoomPastMeetingInviteeUpdate processes a zoom past meeting invitee update from itx-zoom-past-meetings-invitees records.
// Returns true if the operation should be retried, false otherwise.
func handleZoomPastMeetingInviteeUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom past meeting invitee update")

	// Convert v1Data map to PastMeetingInviteeInput struct
	invitee, err := convertMapToInputPastMeetingInvitee(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to PastMeetingInviteeInput")
		return false
	}

	// Extract the invitee ID
	inviteeID := invitee.ID
	if inviteeID == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid id in v1 past meeting invitee data")
		return false
	}
	funcLogger = funcLogger.With("invitee_id", inviteeID)

	// Check if parent past meeting exists in mappings before proceeding.
	if invitee.MeetingAndOccurrenceID == "" {
		funcLogger.ErrorContext(ctx, "past meeting invitee missing required parent past meeting ID")
		return false
	}
	funcLogger = funcLogger.With("meeting_and_occurrence_id", invitee.MeetingAndOccurrenceID)
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", invitee.MeetingAndOccurrenceID)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		funcLogger.With(errKey, err).InfoContext(ctx, "parent past meeting not found in mappings, will retry past meeting invitee sync")
		return true
	}

	// Determine if this invitee is a host by looking up their registrant record
	isHost := false
	registrantID := invitee.RegistrantID
	if registrantID != "" {
		// Look up the registrant in the v1-objects KV bucket.
		registrantKey := fmt.Sprintf("itx-zoom-meetings-registrants-v2.%s", registrantID)
		registrantData, exists, err := getV1ObjectData(ctx, registrantKey)
		if err != nil {
			funcLogger.With(errKey, err, "registrant_id", registrantID).WarnContext(ctx, "failed to get registrant data")
		} else if exists {
			// Check if the registrant has the host field set to true
			if hostValue, ok := registrantData["host"].(bool); ok {
				isHost = hostValue
			}
		}
	}

	v2Participant, err := convertInviteeToV2Participant(invitee, isHost)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert invitee to V2 participant")
		return false
	}

	// If username is blank but we have a v1 Platform ID (lf_user_id), lookup the username.
	if v2Participant.Username == "" && invitee.LFUserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, invitee.LFUserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			v2Participant.Username = mapUsernameToAuthSub(v1User.Username)
			invitee.LFSSO = v1User.Username // Update the invitee data for access message
			funcLogger.With("lf_user_id", invitee.LFUserID, "username", v1User.Username).DebugContext(ctx, "looked up username for past meeting invitee")
		} else {
			if lookupErr != nil {
				funcLogger.With(errKey, lookupErr, "lf_user_id", invitee.LFUserID).WarnContext(ctx, "failed to lookup v1 user for past meeting invitee")
			}
		}
	}

	mappingKey := fmt.Sprintf("v1_past_meeting_invitees.%s", inviteeID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getPastMeetingParticipantTags(v2Participant)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingParticipantSubject, indexerAction, v2Participant, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send invitee indexer message")
		return false
	}

	if invitee.LFSSO != "" {
		// For invitees, is_invited is always true since they are invitees
		// Map username to Auth0 "sub" format for v2 compatibility.
		authSub := mapUsernameToAuthSub(invitee.LFSSO)
		accessMsg := PastMeetingParticipantAccessMessage{
			MeetingAndOccurrenceID: invitee.MeetingAndOccurrenceID,
			Username:               authSub,
			Host:                   isHost,
			IsInvited:              true,
			IsAttended:             false, // TODO: we need to ensure that the invitee event is handled before the attendee event so that this value doesn't get reset if the order is reversed
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal invitee access message")
			return false
		}

		if err := sendAccessMessage(V1PastMeetingParticipantPutSubject, accessMsgBytes); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send invitee access message")
			return false
		}
	}

	if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
		funcLogger.With(errKey, err).WarnContext(ctx, "failed to store past meeting invitee mapping")
	}

	funcLogger.InfoContext(ctx, "successfully sent invitee indexer and access messages")
	return false
}

func convertInviteeToV2Participant(invitee *pastMeetingInviteeInput, isHost bool) (*V2PastMeetingParticipant, error) {
	pastMeetingParticipant := V2PastMeetingParticipant{
		UID:                    invitee.ID,
		MeetingAndOccurrenceID: invitee.MeetingAndOccurrenceID,
		MeetingID:              invitee.MeetingID,
		Email:                  invitee.Email,
		FirstName:              invitee.FirstName,
		LastName:               invitee.LastName,
		Host:                   isHost,
		JobTitle:               invitee.JobTitle,
		OrgName:                invitee.Org,
		AvatarURL:              invitee.ProfilePicture,
		Username:               mapUsernameToAuthSub(invitee.LFSSO),
		IsInvited:              true,
		IsAttended:             false,                  // TODO: we need to ensure that the invitee event is handled before the attendee event so that this value doesn't get reset if the order is reversed
		Sessions:               []ParticipantSession{}, // TODO: we need to determine the sessions for the invitee from the attendee event
	}

	if invitee.CreatedAt != "" {
		createdAt, err := time.Parse(time.RFC3339, invitee.CreatedAt)
		if err != nil {
			logger.With(errKey, err,
				"created_at", invitee.CreatedAt,
				"invitee_id", invitee.ID,
				"meeting_and_occurrence_id", invitee.MeetingAndOccurrenceID,
			).Warn("failed to parse created_at for invitee")
		} else {
			pastMeetingParticipant.CreatedAt = &createdAt
		}
	}

	if invitee.ModifiedAt != "" {
		modifiedAt, err := time.Parse(time.RFC3339, invitee.ModifiedAt)
		if err != nil {
			logger.With(errKey, err,
				"modified_at", invitee.ModifiedAt,
				"invitee_id", invitee.ID,
				"meeting_and_occurrence_id", invitee.MeetingAndOccurrenceID,
			).Warn("failed to parse modified_at for invitee")
		} else {
			pastMeetingParticipant.UpdatedAt = &modifiedAt
		}
	}
	if invitee.OrgIsMember != nil {
		pastMeetingParticipant.OrgIsMember = *invitee.OrgIsMember
	}

	if invitee.OrgIsProjectMember != nil {
		pastMeetingParticipant.OrgIsProjectMember = *invitee.OrgIsProjectMember
	}

	return &pastMeetingParticipant, nil
}

// convertMapToInputPastMeetingAttendee converts a map[string]any to a PastMeetingAttendeeInput struct.
func convertMapToInputPastMeetingAttendee(v1Data map[string]any) (*pastMeetingAttendeeInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for past meeting attendee: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingAttendeeInput struct
	var attendee pastMeetingAttendeeInput
	if err := json.Unmarshal(jsonBytes, &attendee); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingAttendeeInput: %w", err)
	}

	return &attendee, nil
}

// handleZoomPastMeetingAttendeeUpdate processes a zoom past meeting attendee update from itx-zoom-past-meetings-attendees-v2 records.
// Returns true if the operation should be retried, false otherwise.
func handleZoomPastMeetingAttendeeUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom past meeting attendee update")

	// Convert v1Data map to PastMeetingAttendeeInput struct
	attendee, err := convertMapToInputPastMeetingAttendee(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to PastMeetingAttendeeInput")
		return false
	}

	// Extract the attendee ID
	attendeeID := attendee.ID
	if attendeeID == "" {
		funcLogger.ErrorContext(ctx, "missing or invalid id in v1 past meeting attendee data")
		return false
	}
	funcLogger = funcLogger.With("attendee_id", attendeeID)

	// Check if parent past meeting exists in mappings before proceeding.
	if attendee.MeetingAndOccurrenceID == "" {
		funcLogger.ErrorContext(ctx, "past meeting attendee missing required parent past meeting ID")
		return false
	}
	funcLogger = funcLogger.With("meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID)
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", attendee.MeetingAndOccurrenceID)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		funcLogger.With(errKey, err).InfoContext(ctx, "parent past meeting not found in mappings, will retry past meeting attendee sync")
		return true
	}

	// Determine if this attendee is a host by looking up their registrant record
	isHost := false
	isRegistrant := false
	registrantID := attendee.RegistrantID
	if registrantID != "" {
		// Look up the registrant in the v1-objects KV bucket.
		registrantKey := fmt.Sprintf("itx-zoom-meetings-registrants-v2.%s", registrantID)
		registrantData, exists, err := getV1ObjectData(ctx, registrantKey)
		if err != nil {
			funcLogger.With(errKey, err, "registrant_id", registrantID).WarnContext(ctx, "failed to get registrant data")
		} else if exists {
			isRegistrant = true
			// Check if the registrant has the host field set to true
			if hostValue, ok := registrantData["host"].(bool); ok {
				isHost = hostValue
			}
		}
	}

	mappingKey := fmt.Sprintf("v1_past_meeting_attendees.%s", attendeeID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	v2Participant, err := convertAttendeeToV2Participant(attendee, isHost, isRegistrant)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert attendee to V2 participant")
		return false
	}

	// If username is blank but we have a v1 Platform ID (lf_user_id), lookup the username.
	if v2Participant.Username == "" && attendee.LFUserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, attendee.LFUserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			v2Participant.Username = mapUsernameToAuthSub(v1User.Username)
			attendee.LFSSO = v1User.Username // Update the attendee data for access message
			funcLogger.With("lf_user_id", attendee.LFUserID, "username", v1User.Username).DebugContext(ctx, "looked up username for past meeting attendee")
		} else {
			if lookupErr != nil {
				funcLogger.With(errKey, lookupErr, "lf_user_id", attendee.LFUserID).WarnContext(ctx, "failed to lookup v1 user for past meeting attendee")
			}
		}
	}

	tags := getPastMeetingParticipantTags(v2Participant)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingParticipantSubject, indexerAction, v2Participant, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send attendee indexer message")
		return false
	}

	if attendee.LFSSO != "" {
		// For attendees, is_attended is always true since they attended the meeting
		// Map username to Auth0 "sub" format for v2 compatibility.
		authSub := mapUsernameToAuthSub(attendee.LFSSO)
		accessMsg := PastMeetingParticipantAccessMessage{
			MeetingAndOccurrenceID: attendee.MeetingAndOccurrenceID,
			Username:               authSub,
			Host:                   isHost,
			IsInvited:              isRegistrant,
			IsAttended:             true,
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal attendee access message")
			return false
		}

		if err := sendAccessMessage(V1PastMeetingParticipantPutSubject, accessMsgBytes); err != nil {
			funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send attendee access message")
			return false
		}
	}

	if attendeeID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store past meeting attendee mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent attendee indexer and access messages")
	return false
}

func convertAttendeeToV2Participant(attendee *pastMeetingAttendeeInput, isHost bool, isRegistrant bool) (*V2PastMeetingParticipant, error) {
	var firstName, lastName string
	namesSplit := strings.Split(attendee.Name, " ")
	if len(namesSplit) >= 2 {
		firstName = namesSplit[0]
		lastName = strings.Join(namesSplit[1:], " ")
	} else if len(namesSplit) == 1 {
		firstName = namesSplit[0]
		lastName = ""
	}

	pastMeetingParticipant := V2PastMeetingParticipant{
		UID:                    attendee.ID,
		MeetingAndOccurrenceID: attendee.MeetingAndOccurrenceID,
		MeetingID:              attendee.MeetingID,
		Email:                  attendee.Email,
		FirstName:              firstName,
		LastName:               lastName,
		Host:                   isHost,
		JobTitle:               attendee.JobTitle,
		OrgName:                attendee.Org,
		AvatarURL:              attendee.ProfilePicture,
		Username:               mapUsernameToAuthSub(attendee.LFSSO),
		IsInvited:              isRegistrant,
		IsAttended:             true,
		Sessions:               []ParticipantSession{}, // TODO: we need to determine the sessions for the invitee from the attendee event
	}

	if attendee.CreatedAt != "" {
		createdAt, err := time.Parse(time.RFC3339, attendee.CreatedAt)
		if err != nil {
			logger.With(errKey, err,
				"created_at", attendee.CreatedAt,
				"attendee_id", attendee.ID,
				"meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID,
			).Warn("failed to parse created_at for attendee")
		} else {
			pastMeetingParticipant.CreatedAt = &createdAt
		}
	}

	if attendee.ModifiedAt != "" {
		modifiedAt, err := time.Parse(time.RFC3339, attendee.ModifiedAt)
		if err != nil {
			logger.With(errKey, err,
				"modified_at", attendee.ModifiedAt,
				"attendee_id", attendee.ID,
				"meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID,
			).Warn("failed to parse modified_at for attendee")
		} else {
			pastMeetingParticipant.UpdatedAt = &modifiedAt
		}
	}

	if attendee.OrgIsMember != nil {
		pastMeetingParticipant.OrgIsMember = *attendee.OrgIsMember
	}

	if attendee.OrgIsProjectMember != nil {
		pastMeetingParticipant.OrgIsProjectMember = *attendee.OrgIsProjectMember
	}

	for _, session := range attendee.Sessions {
		participantSession := ParticipantSession{
			UID:         session.ParticipantUUID,
			LeaveReason: session.LeaveReason,
		}

		if session.JoinTime != "" {
			joinTime, err := time.Parse(time.RFC3339, session.JoinTime)
			if err != nil {
				logger.With(errKey, err,
					"join_time", session.JoinTime,
					"session_id", session.ParticipantUUID,
					"attendee_id", attendee.ID,
					"meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID,
				).Warn("failed to parse join_time for attendee")
			} else {
				participantSession.JoinTime = &joinTime
			}
		}

		if session.LeaveTime != "" {
			leaveTime, err := time.Parse(time.RFC3339, session.LeaveTime)
			if err != nil {
				logger.With(errKey, err,
					"leave_time", session.LeaveTime,
					"session_id", session.ParticipantUUID,
					"attendee_id", attendee.ID,
					"meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID,
				).Warn("failed to parse leave_time for attendee")
			} else {
				participantSession.LeaveTime = &leaveTime
			}
		}

		pastMeetingParticipant.Sessions = append(pastMeetingParticipant.Sessions, participantSession)
	}

	return &pastMeetingParticipant, nil
}

// PastMeetingRecordingAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for recordings.
type PastMeetingRecordingAccessMessage struct {
	ID                     string `json:"id"`
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id"`
	RecordingAccess        string `json:"recording_access"`
}

// PastMeetingTranscriptAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for transcripts.
type PastMeetingTranscriptAccessMessage struct {
	ID                     string `json:"id"`
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id"`
	TranscriptAccess       string `json:"transcript_access"`
}

// convertMapToInputPastMeetingRecording converts a map[string]any to a PastMeetingRecordingInput struct.
func convertMapToInputPastMeetingRecording(v1Data map[string]any) (*pastMeetingRecordingInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for past meeting recording: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingRecordingInput struct
	var recording pastMeetingRecordingInput
	if err := json.Unmarshal(jsonBytes, &recording); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingRecordingInput: %w", err)
	}

	recording.Platform = "Zoom"

	// Populate the ID for the v2 system with the partition key from v1.
	if meetingAndOccurrenceID, ok := v1Data["meeting_and_occurrence_id"].(string); ok && meetingAndOccurrenceID != "" {
		recording.ID = meetingAndOccurrenceID
		recording.MeetingAndOccurrenceID = meetingAndOccurrenceID
	}

	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		recording.MeetingID = meetingID
		recording.PlatformMeetingID = meetingID
	}

	// Convert v1 named fields to v2 named fields.
	if title, ok := v1Data["topic"].(string); ok && title != "" {
		recording.Title = title
	}

	// We should set a default recording access if it is not set, to ensure it can have its access relationships
	// created by the fga-sync service, which requires it to be set.
	if recording.RecordingAccess == "" {
		// This recording access comes from the enum for the field: [public, meeting_hosts, meeting_participants]
		// We want to default to meeting hosts only access to keep it as restrictive as possible.
		recording.RecordingAccess = "meeting_hosts"
	}

	// Check if the recording has a transcript and set the transcript enabled and access if it does
	// and wasn't already populated from the v1 data.
	var hasTranscript = false
	for _, file := range recording.RecordingFiles {
		if file.FileType == "TRANSCRIPT" || file.FileType == "TIMELINE" {
			hasTranscript = true
			break
		}
	}
	if hasTranscript && !recording.TranscriptEnabled {
		recording.TranscriptEnabled = true
	}
	if hasTranscript && recording.TranscriptAccess == "" {
		recording.TranscriptAccess = "meeting_hosts"
	}

	// Convert recording_count from string to int
	if modifiedAt, ok := v1Data["modified_at"].(string); ok && modifiedAt != "" {
		recording.UpdatedAt = modifiedAt
	}

	return &recording, nil
}

func getPastMeetingRecordingTags(recording *pastMeetingRecordingInput) []string {
	tags := []string{
		recording.ID,
		fmt.Sprintf("past_meeting_recording_id:%s", recording.ID),
		fmt.Sprintf("meeting_and_occurrence_id:%s", recording.MeetingAndOccurrenceID),
		"platform:Zoom",
		fmt.Sprintf("platform_meeting_id:%s", recording.MeetingID),
	}
	for _, session := range recording.Sessions {
		tags = append(tags, fmt.Sprintf("platform_meeting_instance_id:%s", session.UUID))
	}
	return tags
}

// Note: the input and tags are almost the exact same as [getPastMeetingRecordingTags]
// because the source for the transcript record is the same as the recording record.
// Ultimately they are indexed as separate records, so they need their own tags.
func getPastMeetingTranscriptTags(recording *pastMeetingRecordingInput) []string {
	tags := []string{
		recording.ID,
		fmt.Sprintf("past_meeting_transcript_id:%s", recording.ID),
		fmt.Sprintf("meeting_and_occurrence_id:%s", recording.MeetingAndOccurrenceID),
		"platform:Zoom",
		fmt.Sprintf("platform_meeting_id:%s", recording.MeetingID),
	}
	for _, session := range recording.Sessions {
		tags = append(tags, fmt.Sprintf("platform_meeting_instance_id:%s", session.UUID))
	}
	return tags
}

// handleZoomPastMeetingRecordingUpdate handles the v1 past meeting recording update event.
// It sends NATS messages for both recording and transcript indexing and access control.
// handleZoomPastMeetingRecordingUpdate processes a zoom past meeting recording update from itx-zoom-past-meetings-recordings records.
// Returns true if the operation should be retried, false otherwise.
func handleZoomPastMeetingRecordingUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom past meeting recording update")

	// Convert the v1Data map to PastMeetingRecordingInput struct
	recordingInput, err := convertMapToInputPastMeetingRecording(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to PastMeetingRecordingInput")
		return false
	}

	// Extract the ID (MeetingAndOccurrenceID)
	id := recordingInput.MeetingAndOccurrenceID
	if id == "" {
		funcLogger.ErrorContext(ctx, "missing meeting_and_occurrence_id in past meeting recording data")
		return false
	}
	funcLogger = funcLogger.With("meeting_and_occurrence_id", id)

	// Check if parent past meeting exists in mappings before proceeding.
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", id)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		funcLogger.With(errKey, err).InfoContext(ctx, "parent past meeting not found in mappings, will retry past meeting recording sync")
		return true
	}

	// Determine action based on mapping existence
	mappingKey := fmt.Sprintf("v1_past_meeting_recordings.%s", id)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	// Send recording indexer message
	recordingTags := getPastMeetingRecordingTags(recordingInput)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingRecordingSubject, indexerAction, recordingInput, recordingTags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send recording indexer message")
		return false
	}

	// Construct recording access message
	recordingAccessMsg := PastMeetingRecordingAccessMessage{
		ID:                     id,
		MeetingAndOccurrenceID: id,
		RecordingAccess:        string(recordingInput.RecordingAccess),
	}

	// Marshal recording access message
	recordingAccessMsgBytes, err := json.Marshal(recordingAccessMsg)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal recording access message")
		return false
	}

	// Send recording access message
	if err := sendAccessMessage(V1PastMeetingRecordingUpdateAccessSubject, recordingAccessMsgBytes); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send recording access message")
		return false
	}

	// Send transcript indexer message
	transcriptTags := getPastMeetingTranscriptTags(recordingInput)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingTranscriptSubject, indexerAction, recordingInput, transcriptTags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send transcript indexer message")
		return false
	}

	// Construct transcript access message
	transcriptAccessMsg := PastMeetingTranscriptAccessMessage{
		ID:                     id,
		MeetingAndOccurrenceID: id,
		TranscriptAccess:       string(recordingInput.TranscriptAccess),
	}

	// Marshal transcript access message
	transcriptAccessMsgBytes, err := json.Marshal(transcriptAccessMsg)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal transcript access message")
		return false
	}

	// Send transcript access message
	if err := sendAccessMessage(V1PastMeetingTranscriptUpdateAccessSubject, transcriptAccessMsgBytes); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send transcript access message")
		return false
	}

	if id != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store past meeting recording mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent recording and transcript indexer and access messages")
	return false
}

// PastMeetingSummaryAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for summaries.
type PastMeetingSummaryAccessMessage struct {
	ID                     string `json:"id"`
	MeetingAndOccurrenceID string `json:"meeting_and_occurrence_id"`
	SummaryAccess          string `json:"summary_access"`
}

// convertMapToInputPastMeetingSummary converts a map[string]any to a PastMeetingSummaryInput struct.
func convertMapToInputPastMeetingSummary(v1Data map[string]any) (*pastMeetingSummaryInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal v1Data to JSON for past meeting summary: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingSummaryInput struct
	var summary pastMeetingSummaryInput
	if err := json.Unmarshal(jsonBytes, &summary); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingSummaryInput: %w", err)
	}

	if summaryID, ok := v1Data["id"].(string); ok && summaryID != "" {
		summary.ID = summaryID
	}
	if pastMeetingUID, ok := v1Data["meeting_and_occurrence_id"].(string); ok && pastMeetingUID != "" {
		summary.MeetingAndOccurrenceID = pastMeetingUID
	}
	summary.ZoomConfig = PastMeetingSummaryZoomConfig{}
	if meetingID, ok := v1Data["meeting_id"].(string); ok && meetingID != "" {
		summary.MeetingID = meetingID
		summary.ZoomConfig.MeetingID = meetingID
	}
	if meetingUUID, ok := v1Data["zoom_meeting_uuid"].(string); ok && meetingUUID != "" {
		summary.ZoomConfig.MeetingUUID = meetingUUID
	}
	summary.Platform = "Zoom"

	// Construct the content (one field) for the v2 data from the different sparse fields in the v1 data.
	summaryContent := ""
	if summary.SummaryOverview != "" {
		summaryContent += fmt.Sprintf("## Overview\n%s\n\n", summary.SummaryOverview)
	}
	if len(summary.SummaryDetails) > 0 {
		summaryContent += "## Key Topics\n"
		for _, detail := range summary.SummaryDetails {
			summaryContent += fmt.Sprintf("### %s\n%s", detail.Label, detail.Summary)
		}
		summaryContent += "\n\n"
	}
	if len(summary.NextSteps) > 0 {
		summaryContent += "## Next Steps\n"
		for _, nextStep := range summary.NextSteps {
			summaryContent += fmt.Sprintf("- %s\n", nextStep)
		}
	}
	summary.Content = summaryContent

	// Edited summary content
	editedSummaryContent := ""
	if summary.EditedSummaryOverview != "" {
		editedSummaryContent += fmt.Sprintf("## Overview\n%s\n\n", summary.EditedSummaryOverview)
	}
	if len(summary.EditedSummaryDetails) > 0 {
		editedSummaryContent += "## Key Topics\n"
		for _, detail := range summary.EditedSummaryDetails {
			editedSummaryContent += fmt.Sprintf("### %s\n%s", detail.Label, detail.Summary)
		}
		editedSummaryContent += "\n\n"
	}
	if len(summary.EditedNextSteps) > 0 {
		editedSummaryContent += "## Next Steps\n"
		for _, nextStep := range summary.EditedNextSteps {
			editedSummaryContent += fmt.Sprintf("- %s\n", nextStep)
		}
	}
	summary.EditedContent = editedSummaryContent

	if modifiedAt, ok := v1Data["modified_at"].(string); ok && modifiedAt != "" {
		summary.UpdatedAt = modifiedAt
	}

	return &summary, nil
}

func getPastMeetingSummaryTags(summary *pastMeetingSummaryInput) []string {
	tags := []string{
		summary.ID,
		fmt.Sprintf("past_meeting_summary_id:%s", summary.ID),
		fmt.Sprintf("meeting_and_occurrence_id:%s", summary.MeetingAndOccurrenceID),
		fmt.Sprintf("meeting_id:%s", summary.MeetingID),
		"platform:Zoom",
		fmt.Sprintf("title:%s", summary.SummaryTitle),
	}
	return tags
}

// handleZoomPastMeetingSummaryUpdate handles the v1 past meeting summary update event.
// It sends NATS messages for summary indexing and access control.
// handleZoomPastMeetingSummaryUpdate processes a zoom past meeting summary update from itx-zoom-past-meetings-summaries records.
// Returns true if the operation should be retried, false otherwise.
func handleZoomPastMeetingSummaryUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	funcLogger := logger.With("key", key)

	funcLogger.DebugContext(ctx, "processing zoom past meeting summary update")

	// Convert the v1Data map to PastMeetingSummaryInput struct
	summaryInput, err := convertMapToInputPastMeetingSummary(v1Data)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to convert v1Data to PastMeetingSummaryInput")
		return false
	}

	// Extract the UID (ID)
	uid := summaryInput.ID
	if uid == "" {
		funcLogger.ErrorContext(ctx, "missing id in past meeting summary data")
		return false
	}
	funcLogger = funcLogger.With("summary_id", uid)

	// Check if parent past meeting exists in mappings before proceeding.
	if summaryInput.MeetingAndOccurrenceID == "" {
		funcLogger.ErrorContext(ctx, "past meeting summary missing required parent past meeting ID")
		return false
	}
	funcLogger = funcLogger.With("meeting_and_occurrence_id", summaryInput.MeetingAndOccurrenceID)
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", summaryInput.MeetingAndOccurrenceID)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		funcLogger.With(errKey, err).InfoContext(ctx, "parent past meeting not found in mappings, will retry past meeting summary sync")
		return true
	}

	// Determine action based on mapping existence
	mappingKey := fmt.Sprintf("v1_past_meeting_summaries.%s", uid)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	// Send summary indexer message
	tags := getPastMeetingSummaryTags(summaryInput)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingSummarySubject, indexerAction, summaryInput, tags); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send summary indexer message")
		return false
	}

	aiSummaryAccess := ""
	if summaryInput.MeetingAndOccurrenceID != "" {
		pastMeetingKey := fmt.Sprintf("itx-zoom-past-meetings.%s", summaryInput.MeetingAndOccurrenceID)
		pastMeetingData, exists, err := getV1ObjectData(ctx, pastMeetingKey)
		if err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to get past meeting data")
		} else if exists {
			if aiSummaryAccessValue, ok := pastMeetingData["ai_summary_access"].(string); ok && aiSummaryAccessValue != "" {
				aiSummaryAccess = aiSummaryAccessValue
			}
		}
	}

	summaryAccessMsg := PastMeetingSummaryAccessMessage{
		ID:                     uid,
		MeetingAndOccurrenceID: summaryInput.MeetingAndOccurrenceID,
		SummaryAccess:          aiSummaryAccess,
	}

	// Marshal summary access message
	summaryAccessMsgBytes, err := json.Marshal(summaryAccessMsg)
	if err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to marshal summary access message")
		return false
	}

	// Send summary access message
	if err := sendAccessMessage(V1PastMeetingSummaryUpdateAccessSubject, summaryAccessMsgBytes); err != nil {
		funcLogger.With(errKey, err).ErrorContext(ctx, "failed to send summary access message")
		return false
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			funcLogger.With(errKey, err).WarnContext(ctx, "failed to store past meeting summary mapping")
		}
	}

	funcLogger.InfoContext(ctx, "successfully sent summary indexer and access messages")
	return false
}

func shouldShowMeetingAttendees(m meetingInput) bool {
	allowedShowMeetingAttendeesSFDCIds := []string{"a0941000002wBz9AAE"}
	return strings.EqualFold(m.MeetingType, "board") && slices.Contains(allowedShowMeetingAttendeesSFDCIds, m.ProjectSFID)
}
