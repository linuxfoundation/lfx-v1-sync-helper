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
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to marshal indexer message")
		return fmt.Errorf("failed to marshal indexer message: %w", err)
	}

	logger.With("subject", subject, "action", action, "tags_count", len(tags)).DebugContext(ctx, "constructed indexer message")

	// Publish the message to NATS
	if err := natsConn.Publish(subject, messageBytes); err != nil {
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to publish indexer message")
		return fmt.Errorf("failed to publish indexer message to subject %s: %w", subject, err)
	}

	logger.With("subject", subject).InfoContext(ctx, "successfully published indexer message")
	return nil
}

// sendAccessMessage sends a pre-marshalled message to the NATS server.
// This is a generic function that can be used for access control updates, put operations, etc.
func sendAccessMessage(ctx context.Context, subject string, messageBytes []byte) error {
	logger.With("subject", subject, "message_size", len(messageBytes)).DebugContext(ctx, "publishing message")

	// Publish the message to NATS
	if err := natsConn.Publish(subject, messageBytes); err != nil {
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to publish message")
		return fmt.Errorf("failed to publish message to subject %s: %w", subject, err)
	}

	logger.With("subject", subject).InfoContext(ctx, "successfully published message")
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
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into InputMeeting struct
	var meeting meetingInput
	if err := json.Unmarshal(jsonBytes, &meeting); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into InputMeeting")
		return nil, fmt.Errorf("failed to unmarshal JSON into InputMeeting: %w", err)
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
			meeting.ProjectID = string(entry.Value())
		}
	}

	occurrences, err := calculateOccurrences(ctx, meeting, false, false, 100)
	if err != nil {
		logger.With(errKey, err, "meeting_id", meeting.ID).ErrorContext(ctx, "failed to calculate occurrences")
		return nil, fmt.Errorf("failed to calculate occurrences: %w", err)
	}
	meeting.Occurrences = occurrences

	return &meeting, nil
}

func getMeetingTags(meeting *meetingInput) []string {
	tags := []string{
		fmt.Sprintf("%s", meeting.ID),
		fmt.Sprintf("meeting_uid:%s", meeting.ID),
		fmt.Sprintf("project_uid:%s", meeting.ProjectID),
		fmt.Sprintf("title:%s", meeting.Topic),
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

	logger.With("key", key).DebugContext(ctx, "processing zoom meeting update")

	// Convert v1Data map to InputMeeting struct
	meeting, err := convertMapToInputMeeting(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to InputMeeting")
		return
	}

	// Extract the meeting UID
	uid := meeting.ID
	if uid == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid uid in v1 meeting data")
		return
	}

	// Check if parent project exists in mappings before proceeding. Because
	// convertMapToInputMeeting has already looked up the SFID project ID
	// mapping, we don't need to do it again: we can just check if ProjectID (v2
	// UID) is set.
	if meeting.ProjectID == "" {
		logger.With("project_sfid", meeting.ProjectSFID, "meeting_id", uid).InfoContext(ctx, "skipping meeting sync - parent project not found in mappings")
		return
	}

	// Try to get committee mappings from the index first
	var committees []string
	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.meeting-mappings.%s", uid)
	indexEntry, err := mappingsKV.Get(ctx, indexKey)
	if err == nil && indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			logger.With(errKey, err, "meeting_id", uid, "key", key).WarnContext(ctx, "failed to unmarshal meeting mapping index")
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

	mappingKey := fmt.Sprintf("v1_meetings.%s", uid)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getMeetingTags(meeting)
	if err := sendIndexerMessage(ctx, IndexV1MeetingSubject, indexerAction, meeting, tags); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send meeting indexer message")
		return
	}

	accessMsg := MeetingAccessMessage{
		UID:        uid,
		Public:     meeting.Visibility == "public",
		ProjectUID: meeting.ProjectID,
		Organizers: []string{},
		Committees: committees,
	}

	accessMsgBytes, err := json.Marshal(accessMsg)
	if err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to marshal access message")
		return
	}

	if err := sendAccessMessage(ctx, UpdateAccessV1MeetingSubject, accessMsgBytes); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send meeting access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "uid", uid).WarnContext(ctx, "failed to store meeting mapping")
		}
	}

	logger.With("uid", uid, "key", key).InfoContext(ctx, "successfully sent meeting indexer and access messages")
}

// convertMapToInputMeeting converts a map[string]any to an InputMeeting struct.
func convertMapToInputMeetingMapping(ctx context.Context, v1Data map[string]any) (*ZoomMeetingMappingDB, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into ZoomMeetingMappingDB struct
	var mapping ZoomMeetingMappingDB
	if err := json.Unmarshal(jsonBytes, &mapping); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into ZoomMeetingMappingDB")
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

	logger.With("key", key).DebugContext(ctx, "processing zoom meeting mapping update")

	// Convert v1Data map to ZoomMeetingMappingDB struct
	mapping, err := convertMapToInputMeetingMapping(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to ZoomMeetingMappingDB")
		return
	}

	// Extract the meeting ID from the mapping
	meetingID := mapping.MeetingID
	if meetingID == "" {
		logger.With("key", key).ErrorContext(ctx, "missing meeting_id in mapping data")
		return
	}
	mappingKey := fmt.Sprintf("v1_meetings.%s", meetingID)

	// Extract the committee ID from the mapping
	committeeID := mapping.CommitteeID
	if committeeID == "" {
		logger.With("key", key, "meeting_id", meetingID).WarnContext(ctx, "mapping has no committee_id")
		return
	}

	// Fetch the meeting object from v1-objects KV bucket
	meetingKey := fmt.Sprintf("itx-zoom-meetings-v2.%s", meetingID)
	meetingEntry, err := v1KV.Get(ctx, meetingKey)
	if err != nil {
		logger.With(errKey, err, "meeting_id", meetingID, "key", key).WarnContext(ctx, "failed to fetch meeting from KV bucket, cannot trigger re-index")
		return
	}

	// Parse the meeting data
	var meetingData map[string]any
	if err := json.Unmarshal(meetingEntry.Value(), &meetingData); err != nil {
		logger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to unmarshal meeting data")
		return
	}

	// Convert meeting data to typed struct
	meeting, err := convertMapToInputMeeting(ctx, meetingData)
	if err != nil {
		logger.With(errKey, err, "meeting_id", meetingID).ErrorContext(ctx, "failed to convert meeting data")
		return
	}

	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.meeting-mappings.%s", meetingID)
	indexEntry, _ := mappingsKV.Get(ctx, indexKey)
	if indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			logger.With(errKey, err, "meeting_id", meetingID, "key", key).WarnContext(ctx, "failed to unmarshal meeting mapping index")
			return
		}
	}

	if meeting != nil {
		committees := []string{}
		meeting.Committees = []Committee{}
		for _, committee := range committeeMappings {
			committees = append(committees, committee.CommitteeID)
			meeting.Committees = append(meeting.Committees, Committee{
				UID:     committee.CommitteeID,
				Filters: committee.CommitteeFilters,
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
			logger.With(errKey, err, "meeting_id", meetingID, "key", key).ErrorContext(ctx, "failed to send meeting indexer message")
			return
		}

		// Send meeting access message with updated committees
		accessMsg := MeetingAccessMessage{
			UID:        meetingID,
			Public:     meeting.Visibility == "public",
			ProjectUID: meeting.ProjectID,
			Organizers: []string{},
			Committees: committees,
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			logger.With(errKey, err, "meeting_id", meetingID, "key", key).ErrorContext(ctx, "failed to marshal access message")
			return
		}

		if err := sendAccessMessage(ctx, UpdateAccessV1MeetingSubject, accessMsgBytes); err != nil {
			logger.With(errKey, err, "meeting_id", meetingID, "key", key).ErrorContext(ctx, "failed to send meeting access message")
			return
		}
	}

	// Store the mapping
	if meetingID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "meeting_id", meetingID).WarnContext(ctx, "failed to store meeting mapping")
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
			logger.With(errKey, err, "meeting_id", meetingID, "key", key).ErrorContext(ctx, "failed to marshal committee mappings")
			return
		}
		if _, err := mappingsKV.Put(ctx, indexKey, committeeMappingsBytes); err != nil {
			logger.With(errKey, err, "meeting_id", meetingID, "key", key).ErrorContext(ctx, "failed to store committee mappings")
			return
		}
	}

	logger.With("meeting_id", meetingID, "committee_id", committeeID, "key", key).InfoContext(ctx, "successfully triggered meeting re-index with updated committees")
}

// convertMapToInputRegistrant converts a map[string]any to a RegistrantInput struct.
func convertMapToInputRegistrant(ctx context.Context, v1Data map[string]any) (*registrantInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into RegistrantInput struct
	var registrant registrantInput
	if err := json.Unmarshal(jsonBytes, &registrant); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into registrantInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into registrantInput: %w", err)
	}

	if registrantID, ok := v1Data["registrant_id"].(string); ok && registrantID != "" {
		registrant.ID = registrantID
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
		fmt.Sprintf("%s", registrant.ID),
		fmt.Sprintf("registrant_uid:%s", registrant.ID),
		fmt.Sprintf("meeting_uid:%s", registrant.MeetingID),
		fmt.Sprintf("committee_uid:%s", registrant.CommitteeID),
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
func handleZoomMeetingRegistrantUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom meeting registrant update")

	// Convert v1Data map to RegistrantInput struct
	registrant, err := convertMapToInputRegistrant(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to registrantInput")
		return
	}

	// Extract the registrant ID
	registrantID := registrant.ID
	if registrantID == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid id in v1 registrant data")
		return
	}

	// If username is blank but we have a v1 Platform ID (user_id), lookup the username.
	if registrant.Username == "" && registrant.UserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, registrant.UserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			registrant.Username = v1User.Username
			logger.With("user_id", registrant.UserID, "username", v1User.Username).DebugContext(ctx, "looked up username for registrant")
		} else {
			if lookupErr != nil {
				logger.With(errKey, lookupErr, "user_id", registrant.UserID).WarnContext(ctx, "failed to lookup v1 user for registrant")
			}
		}
	}

	// Check if parent meeting exists in mappings before proceeding.
	if registrant.MeetingID == "" {
		logger.With("registrant_id", registrantID).ErrorContext(ctx, "meeting registrant missing required parent meeting ID")
		return
	}
	meetingMappingKey := fmt.Sprintf("v1_meetings.%s", registrant.MeetingID)
	if _, err := mappingsKV.Get(ctx, meetingMappingKey); err != nil {
		logger.With("meeting_id", registrant.MeetingID, "registrant_id", registrantID).InfoContext(ctx, "skipping meeting registrant sync - parent meeting not found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("v1_meeting_registrants.%s", registrantID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getRegistrantTags(registrant)
	if err := sendIndexerMessage(ctx, IndexV1MeetingRegistrantSubject, indexerAction, registrant, tags); err != nil {
		logger.With(errKey, err, "id", registrantID, "key", key).ErrorContext(ctx, "failed to send registrant indexer message")
		return
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
			logger.With(errKey, err, "id", registrantID, "key", key).ErrorContext(ctx, "failed to marshal access message")
			return
		}

		if err := sendAccessMessage(ctx, V1MeetingRegistrantPutSubject, accessMsgBytes); err != nil {
			logger.With(errKey, err, "id", registrantID, "key", key).ErrorContext(ctx, "failed to send registrant put message")
			return
		}
	}

	if registrantID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "id", registrantID).WarnContext(ctx, "failed to store registrant mapping")
		}
	}

	logger.With("id", registrantID, "meeting_id", registrant.MeetingID, "key", key).InfoContext(ctx, "successfully sent registrant indexer and put messages")
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
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingInput struct
	var pastMeeting pastMeetingInput
	if err := json.Unmarshal(jsonBytes, &pastMeeting); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into pastMeetingInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into pastMeetingInput: %w", err)
	}

	// We need to populate the ID for the v2 system
	if meetingAndOccurrenceID, ok := v1Data["meeting_and_occurrence_id"].(string); ok && meetingAndOccurrenceID != "" {
		pastMeeting.ID = meetingAndOccurrenceID
	}

	// Convert the v1 project ID since the json key is different,
	// then use that to get the v2 project UID.
	if projectSFID, ok := v1Data["proj_id"].(string); ok && projectSFID != "" {
		pastMeeting.ProjectSFID = projectSFID
	}

	// Take the v1 project salesforce ID and look up the v2 project UID.
	projectMappingKey := fmt.Sprintf("project.sfid.%s", pastMeeting.ProjectSFID)
	if entry, err := mappingsKV.Get(ctx, projectMappingKey); err == nil {
		pastMeeting.ProjectID = string(entry.Value())
	}

	return &pastMeeting, nil
}

func getPastMeetingTags(pastMeeting *pastMeetingInput) []string {
	tags := []string{
		fmt.Sprintf("%s", pastMeeting.MeetingAndOccurrenceID),
		fmt.Sprintf("past_meeting_uid:%s", pastMeeting.MeetingAndOccurrenceID),
		fmt.Sprintf("meeting_uid:%s", pastMeeting.MeetingID),
		fmt.Sprintf("project_uid:%s", pastMeeting.ProjectID),
		fmt.Sprintf("occurrence_id:%s", pastMeeting.OccurrenceID),
		fmt.Sprintf("title:%s", pastMeeting.Topic),
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

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting update")

	// Convert v1Data map to PastMeetingInput struct
	pastMeeting, err := convertMapToInputPastMeeting(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to pastMeetingInput")
		return
	}

	// Extract the past meeting UID (MeetingAndOccurrenceID)
	uid := pastMeeting.MeetingAndOccurrenceID
	if uid == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid meeting_and_occurrence_id in v1 past meeting data")
		return
	}

	// Check if parent meeting exists in mappings before proceeding.
	if pastMeeting.MeetingID == "" {
		logger.With("past_meeting_id", uid).ErrorContext(ctx, "past meeting missing required parent meeting ID")
		return
	}
	meetingMappingKey := fmt.Sprintf("v1_meetings.%s", pastMeeting.MeetingID)
	if _, err := mappingsKV.Get(ctx, meetingMappingKey); err != nil {
		logger.With("meeting_id", pastMeeting.MeetingID, "past_meeting_id", uid).InfoContext(ctx, "skipping past meeting sync - parent meeting not found in mappings")
		return
	}

	mappingKey := fmt.Sprintf("v1_past_meetings.%s", uid)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	tags := getPastMeetingTags(pastMeeting)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingSubject, indexerAction, pastMeeting, tags); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send past meeting indexer message")
		return
	}

	// Try to get committee mappings from the index first
	var committees []string
	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.past-meeting-mappings.%s", uid)
	indexEntry, err := mappingsKV.Get(ctx, indexKey)
	if err == nil && indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			logger.With(errKey, err, "meeting_and_occurrence_id", uid, "key", key).WarnContext(ctx, "failed to unmarshal past meeting mapping index")
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
		ProjectUID: pastMeeting.ProjectID,
		Committees: committees,
	}

	accessMsgBytes, err := json.Marshal(accessMsg)
	if err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to marshal past meeting access message")
		return
	}

	if err := sendAccessMessage(ctx, V1PastMeetingUpdateAccessSubject, accessMsgBytes); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send past meeting access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "uid", uid).WarnContext(ctx, "failed to store past meeting mapping")
		}
	}

	logger.With("uid", uid, "meeting_id", pastMeeting.MeetingID, "key", key).InfoContext(ctx, "successfully sent past meeting indexer and access messages")
}

// convertMapToInputPastMeetingMapping converts a map[string]any to a ZoomPastMeetingMappingDB struct.
func convertMapToInputPastMeetingMapping(ctx context.Context, v1Data map[string]any) (*ZoomPastMeetingMappingDB, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into ZoomPastMeetingMappingDB struct
	var mapping ZoomPastMeetingMappingDB
	if err := json.Unmarshal(jsonBytes, &mapping); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into ZoomPastMeetingMappingDB")
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

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting mapping update")

	// Convert v1Data map to ZoomPastMeetingMappingDB struct
	mapping, err := convertMapToInputPastMeetingMapping(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to ZoomPastMeetingMappingDB")
		return
	}

	// Extract the meeting_and_occurrence_id from the mapping
	meetingAndOccurrenceID := mapping.MeetingAndOccurrenceID
	if meetingAndOccurrenceID == "" {
		logger.With("key", key).ErrorContext(ctx, "missing meeting_and_occurrence_id in mapping data")
		return
	}
	mappingKey := fmt.Sprintf("v1_past_meetings.%s", meetingAndOccurrenceID)

	// Extract the committee ID from the mapping
	committeeID := mapping.CommitteeID
	if committeeID == "" {
		logger.With("key", key, "meeting_and_occurrence_id", meetingAndOccurrenceID).WarnContext(ctx, "mapping has no committee_id")
		return
	}

	// Fetch the past meeting object from v1-objects KV bucket
	pastMeetingKey := fmt.Sprintf("itx-zoom-past-meetings.%s", meetingAndOccurrenceID)
	pastMeetingEntry, err := v1KV.Get(ctx, pastMeetingKey)
	if err != nil {
		logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).WarnContext(ctx, "failed to fetch past meeting from KV bucket, cannot trigger re-index")
		return
	}

	// Parse the past meeting data
	var pastMeetingData map[string]any
	if err := json.Unmarshal(pastMeetingEntry.Value(), &pastMeetingData); err != nil {
		logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID).ErrorContext(ctx, "failed to unmarshal past meeting data")
		return
	}

	// Convert past meeting data to typed struct
	pastMeeting, err := convertMapToInputPastMeeting(ctx, pastMeetingData)
	if err != nil {
		logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID).ErrorContext(ctx, "failed to convert past meeting data")
		return
	}

	committeeMappings := make(map[string]mappingCommittee)
	indexKey := fmt.Sprintf("v1-mappings.past-meeting-mappings.%s", meetingAndOccurrenceID)
	indexEntry, _ := mappingsKV.Get(ctx, indexKey)
	if indexEntry != nil {
		if err := json.Unmarshal(indexEntry.Value(), &committeeMappings); err != nil {
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).WarnContext(ctx, "failed to unmarshal past meeting mapping index")
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
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).ErrorContext(ctx, "failed to send past meeting indexer message")
			return
		}

		// Send past meeting access message with updated committees
		accessMsg := PastMeetingAccessMessage{
			UID:        meetingAndOccurrenceID,
			MeetingUID: pastMeeting.MeetingID,
			Public:     pastMeeting.Visibility == "public",
			ProjectUID: pastMeeting.ProjectID,
			Committees: committees,
		}

		accessMsgBytes, err := json.Marshal(accessMsg)
		if err != nil {
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).ErrorContext(ctx, "failed to marshal access message")
			return
		}

		if err := sendAccessMessage(ctx, V1PastMeetingUpdateAccessSubject, accessMsgBytes); err != nil {
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).ErrorContext(ctx, "failed to send past meeting access message")
			return
		}
	}

	// Store the mapping
	if meetingAndOccurrenceID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID).WarnContext(ctx, "failed to store past meeting mapping")
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
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).ErrorContext(ctx, "failed to marshal committee mappings")
			return
		}
		if _, err := mappingsKV.Put(ctx, indexKey, committeeMappingsBytes); err != nil {
			logger.With(errKey, err, "meeting_and_occurrence_id", meetingAndOccurrenceID, "key", key).ErrorContext(ctx, "failed to store committee mappings")
			return
		}
	}

	logger.With("meeting_and_occurrence_id", meetingAndOccurrenceID, "committee_id", committeeID, "key", key).InfoContext(ctx, "successfully triggered past meeting re-index with updated committees")
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
func convertMapToInputPastMeetingInvitee(ctx context.Context, v1Data map[string]any) (*ZoomPastMeetingInviteeDatabase, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into ZoomPastMeetingInviteeDatabase struct
	var invitee ZoomPastMeetingInviteeDatabase
	if err := json.Unmarshal(jsonBytes, &invitee); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into ZoomPastMeetingInviteeDatabase")
		return nil, fmt.Errorf("failed to unmarshal JSON into ZoomPastMeetingInviteeDatabase: %w", err)
	}

	if inviteeID, ok := v1Data["invitee_id"].(string); ok && inviteeID != "" {
		invitee.ID = inviteeID
	}

	return &invitee, nil
}

func getPastMeetingParticipantTags(participant *V2PastMeetingParticipant) []string {
	tags := []string{
		fmt.Sprintf("%s", participant.UID),
		fmt.Sprintf("past_meeting_participant_uid:%s", participant.UID),
		fmt.Sprintf("past_meeting_uid:%s", participant.PastMeetingUID),
		fmt.Sprintf("meeting_uid:%s", participant.MeetingUID),
		fmt.Sprintf("first_name:%s", participant.FirstName),
		fmt.Sprintf("last_name:%s", participant.LastName),
		fmt.Sprintf("email:%s", participant.Email),
	}
	if participant.Username != "" {
		tags = append(tags, fmt.Sprintf("username:%s", participant.Username))
	}
	return tags
}

// handleZoomPastMeetingInviteeUpdate processes a zoom past meeting invitee update from itx-zoom-past-meetings-invitees-v2 records.
func handleZoomPastMeetingInviteeUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting invitee update")

	// Convert v1Data map to PastMeetingInviteeInput struct
	invitee, err := convertMapToInputPastMeetingInvitee(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to PastMeetingInviteeInput")
		return
	}

	// Extract the invitee ID
	inviteeID := invitee.ID
	if inviteeID == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid id in v1 past meeting invitee data")
		return
	}

	// Check if parent past meeting exists in mappings before proceeding.
	if invitee.MeetingAndOccurrenceID == "" {
		logger.With("invitee_id", inviteeID).ErrorContext(ctx, "past meeting invitee missing required parent past meeting ID")
		return
	}
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", invitee.MeetingAndOccurrenceID)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		logger.With("past_meeting_id", invitee.MeetingAndOccurrenceID, "invitee_id", inviteeID).InfoContext(ctx, "skipping past meeting invitee sync - parent past meeting not found in mappings")
		return
	}

	// Determine if this invitee is a host by looking up their registrant record
	isHost := false
	registrantID := invitee.RegistrantID
	if registrantID != "" {
		// Look up the registrant in the v1-objects KV bucket
		registrantKey := fmt.Sprintf("itx-zoom-meetings-registrants-v2.%s", registrantID)
		registrantEntry, err := v1KV.Get(ctx, registrantKey)
		if err == nil && registrantEntry != nil {
			// Parse the registrant data
			var registrantData map[string]any
			if err := json.Unmarshal(registrantEntry.Value(), &registrantData); err == nil {
				// Check if the registrant has the host field set to true
				if hostValue, ok := registrantData["host"].(bool); ok {
					isHost = hostValue
				}
			} else {
				logger.With(errKey, err, "registrant_id", registrantID).WarnContext(ctx, "failed to unmarshal registrant data")
			}
		} else {
			logger.With(errKey, err, "registrant_id", registrantID).WarnContext(ctx, "failed to fetch registrant from KV bucket")
		}
	}

	v2Participant, err := convertInviteeToV2Participant(ctx, invitee, isHost)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert invitee to V2 participant")
		return
	}

	// If username is blank but we have a v1 Platform ID (lf_user_id), lookup the username.
	if v2Participant.Username == "" && invitee.LFUserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, invitee.LFUserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			v2Participant.Username = mapUsernameToAuthSub(v1User.Username)
			invitee.LFSSO = v1User.Username // Update the invitee data for access message
			logger.With("lf_user_id", invitee.LFUserID, "username", v1User.Username).DebugContext(ctx, "looked up username for past meeting invitee")
		} else {
			if lookupErr != nil {
				logger.With(errKey, lookupErr, "lf_user_id", invitee.LFUserID).WarnContext(ctx, "failed to lookup v1 user for past meeting invitee")
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
		logger.With(errKey, err, "id", inviteeID, "key", key).ErrorContext(ctx, "failed to send invitee indexer message")
		return
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
			logger.With(errKey, err, "id", inviteeID, "key", key).ErrorContext(ctx, "failed to marshal invitee access message")
			return
		}

		if err := sendAccessMessage(ctx, V1PastMeetingParticipantPutSubject, accessMsgBytes); err != nil {
			logger.With(errKey, err, "id", inviteeID, "key", key).ErrorContext(ctx, "failed to send invitee access message")
			return
		}
	}

	if inviteeID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "id", inviteeID).WarnContext(ctx, "failed to store past meeting invitee mapping")
		}
	}

	logger.With("id", inviteeID, "meeting_and_occurrence_id", invitee.MeetingAndOccurrenceID, "key", key).InfoContext(ctx, "successfully sent invitee indexer and access messages")
}

func convertInviteeToV2Participant(ctx context.Context, invitee *ZoomPastMeetingInviteeDatabase, isHost bool) (*V2PastMeetingParticipant, error) {
	pastMeetingParticipant := V2PastMeetingParticipant{
		UID:            invitee.ID,
		PastMeetingUID: invitee.MeetingAndOccurrenceID,
		MeetingUID:     invitee.MeetingID,
		Email:          invitee.Email,
		FirstName:      invitee.FirstName,
		LastName:       invitee.LastName,
		Host:           isHost,
		JobTitle:       invitee.JobTitle,
		OrgName:        invitee.Org,
		AvatarURL:      invitee.ProfilePicture,
		Username:       mapUsernameToAuthSub(invitee.LFSSO),
		IsInvited:      true,
		IsAttended:     false,                  // TODO: we need to ensure that the invitee event is handled before the attendee event so that this value doesn't get reset if the order is reversed
		Sessions:       []ParticipantSession{}, // TODO: we need to determine the sessions for the invitee from the attendee event
	}

	if invitee.CreatedAt != "" {
		createdAt, err := time.Parse(time.RFC3339, invitee.CreatedAt)
		if err != nil {
			logger.With(errKey, err, "created_at", invitee.CreatedAt).ErrorContext(ctx, "failed to parse created_at")
			return nil, fmt.Errorf("failed to parse created_at: %w", err)
		}
		pastMeetingParticipant.CreatedAt = &createdAt
	}

	if invitee.ModifiedAt != "" {
		modifiedAt, err := time.Parse(time.RFC3339, invitee.ModifiedAt)
		if err != nil {
			logger.With(errKey, err, "modified_at", invitee.ModifiedAt).ErrorContext(ctx, "failed to parse modified_at")
			return nil, fmt.Errorf("failed to parse modified_at: %w", err)
		}
		pastMeetingParticipant.UpdatedAt = &modifiedAt
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
func convertMapToInputPastMeetingAttendee(ctx context.Context, v1Data map[string]any) (*PastMeetingAttendeeInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingAttendeeInput struct
	var attendee PastMeetingAttendeeInput
	if err := json.Unmarshal(jsonBytes, &attendee); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into PastMeetingAttendeeInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingAttendeeInput: %w", err)
	}

	return &attendee, nil
}

// handleZoomPastMeetingAttendeeUpdate processes a zoom past meeting attendee update from itx-zoom-past-meetings-attendees-v2 records.
func handleZoomPastMeetingAttendeeUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting attendee update")

	// Convert v1Data map to PastMeetingAttendeeInput struct
	attendee, err := convertMapToInputPastMeetingAttendee(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to PastMeetingAttendeeInput")
		return
	}

	// Extract the attendee ID
	attendeeID := attendee.ID
	if attendeeID == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid id in v1 past meeting attendee data")
		return
	}

	// Check if parent past meeting exists in mappings before proceeding.
	if attendee.MeetingAndOccurrenceID == "" {
		logger.With("attendee_id", attendeeID).ErrorContext(ctx, "past meeting attendee missing required parent past meeting ID")
		return
	}
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", attendee.MeetingAndOccurrenceID)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		logger.With("past_meeting_id", attendee.MeetingAndOccurrenceID, "attendee_id", attendeeID).InfoContext(ctx, "skipping past meeting attendee sync - parent past meeting not found in mappings")
		return
	}

	// Determine if this attendee is a host by looking up their registrant record
	isHost := false
	isRegistrant := false
	registrantID := attendee.RegistrantID
	if registrantID != "" {
		// Look up the registrant in the v1-objects KV bucket
		registrantKey := fmt.Sprintf("itx-zoom-meetings-registrants-v2.%s", registrantID)
		registrantEntry, err := v1KV.Get(ctx, registrantKey)
		if err == nil && registrantEntry != nil {
			isRegistrant = true
			// Parse the registrant data
			var registrantData map[string]any
			if err := json.Unmarshal(registrantEntry.Value(), &registrantData); err == nil {
				// Check if the registrant has the host field set to true
				if hostValue, ok := registrantData["host"].(bool); ok {
					isHost = hostValue
				}
			} else {
				logger.With(errKey, err, "registrant_id", registrantID).WarnContext(ctx, "failed to unmarshal registrant data")
			}
		} else {
			logger.With(errKey, err, "registrant_id", registrantID).WarnContext(ctx, "failed to fetch registrant from KV bucket")
		}
	}

	mappingKey := fmt.Sprintf("v1_past_meeting_attendees.%s", attendeeID)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	v2Participant, err := convertAttendeeToV2Participant(ctx, attendee, isHost, isRegistrant)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert attendee to V2 participant")
		return
	}

	// If username is blank but we have a v1 Platform ID (lf_user_id), lookup the username.
	if v2Participant.Username == "" && attendee.LFUserID != "" {
		if v1User, lookupErr := lookupV1User(ctx, attendee.LFUserID); lookupErr == nil && v1User != nil && v1User.Username != "" {
			v2Participant.Username = mapUsernameToAuthSub(v1User.Username)
			attendee.LFSSO = v1User.Username // Update the attendee data for access message
			logger.With("lf_user_id", attendee.LFUserID, "username", v1User.Username).DebugContext(ctx, "looked up username for past meeting attendee")
		} else {
			if lookupErr != nil {
				logger.With(errKey, lookupErr, "lf_user_id", attendee.LFUserID).WarnContext(ctx, "failed to lookup v1 user for past meeting attendee")
			}
		}
	}

	tags := getPastMeetingParticipantTags(v2Participant)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingParticipantSubject, indexerAction, v2Participant, tags); err != nil {
		logger.With(errKey, err, "id", attendeeID, "key", key).ErrorContext(ctx, "failed to send attendee indexer message")
		return
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
			logger.With(errKey, err, "id", attendeeID, "key", key).ErrorContext(ctx, "failed to marshal attendee access message")
			return
		}

		if err := sendAccessMessage(ctx, V1PastMeetingParticipantPutSubject, accessMsgBytes); err != nil {
			logger.With(errKey, err, "id", attendeeID, "key", key).ErrorContext(ctx, "failed to send attendee access message")
			return
		}
	}

	if attendeeID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "id", attendeeID).WarnContext(ctx, "failed to store past meeting attendee mapping")
		}
	}

	logger.With("id", attendeeID, "meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID, "key", key).InfoContext(ctx, "successfully sent attendee indexer and access messages")
}

func convertAttendeeToV2Participant(ctx context.Context, attendee *PastMeetingAttendeeInput, isHost bool, isRegistrant bool) (*V2PastMeetingParticipant, error) {
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
		UID:            attendee.ID,
		PastMeetingUID: attendee.MeetingAndOccurrenceID,
		MeetingUID:     attendee.MeetingID,
		Email:          attendee.Email,
		FirstName:      firstName,
		LastName:       lastName,
		Host:           isHost,
		JobTitle:       attendee.JobTitle,
		OrgName:        attendee.Org,
		AvatarURL:      attendee.ProfilePicture,
		Username:       mapUsernameToAuthSub(attendee.LFSSO),
		IsInvited:      isRegistrant,
		IsAttended:     true,
		Sessions:       []ParticipantSession{}, // TODO: we need to determine the sessions for the invitee from the attendee event
	}

	if attendee.CreatedAt != "" {
		createdAt, err := time.Parse(time.RFC3339, attendee.CreatedAt)
		if err != nil {
			logger.With(errKey, err, "created_at", attendee.CreatedAt).ErrorContext(ctx, "failed to parse created_at")
			return nil, fmt.Errorf("failed to parse created_at: %w", err)
		}
		pastMeetingParticipant.CreatedAt = &createdAt
	}

	if attendee.ModifiedAt != "" {
		modifiedAt, err := time.Parse(time.RFC3339, attendee.ModifiedAt)
		if err != nil {
			logger.With(errKey, err, "modified_at", attendee.ModifiedAt).ErrorContext(ctx, "failed to parse modified_at")
			return nil, fmt.Errorf("failed to parse modified_at: %w", err)
		}
		pastMeetingParticipant.UpdatedAt = &modifiedAt
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
				logger.With(errKey, err, "join_time", session.JoinTime).ErrorContext(ctx, "failed to parse join_time")
				return nil, fmt.Errorf("failed to parse join_time: %w", err)
			}
			participantSession.JoinTime = &joinTime
		}

		if session.LeaveTime != "" {
			leaveTime, err := time.Parse(time.RFC3339, session.LeaveTime)
			if err != nil {
				logger.With(errKey, err, "leave_time", session.LeaveTime).ErrorContext(ctx, "failed to parse leave_time")
				return nil, fmt.Errorf("failed to parse leave_time: %w", err)
			}
			participantSession.LeaveTime = &leaveTime
		}

		pastMeetingParticipant.Sessions = append(pastMeetingParticipant.Sessions, participantSession)
	}

	return &pastMeetingParticipant, nil
}

// PastMeetingRecordingAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for recordings.
type PastMeetingRecordingAccessMessage struct {
	ID              string `json:"id"`
	PastMeetingUID  string `json:"meeting_and_occurrence_id"`
	RecordingAccess string `json:"recording_access"`
}

// PastMeetingTranscriptAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for transcripts.
type PastMeetingTranscriptAccessMessage struct {
	ID               string `json:"id"`
	PastMeetingUID   string `json:"meeting_and_occurrence_id"`
	TranscriptAccess string `json:"transcript_access"`
}

// convertMapToInputPastMeetingRecording converts a map[string]any to a PastMeetingRecordingInput struct.
func convertMapToInputPastMeetingRecording(ctx context.Context, v1Data map[string]any) (*PastMeetingRecordingInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingRecordingInput struct
	var recording PastMeetingRecordingInput
	if err := json.Unmarshal(jsonBytes, &recording); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into PastMeetingRecordingInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingRecordingInput: %w", err)
	}

	// Populate the ID for the v2 system with the partition key from v1.
	if meetingAndOccurrenceID, ok := v1Data["meeting_and_occurrence_id"].(string); ok && meetingAndOccurrenceID != "" {
		recording.ID = meetingAndOccurrenceID
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

	return &recording, nil
}

func getPastMeetingRecordingTags(recording *PastMeetingRecordingInput) []string {
	tags := []string{
		fmt.Sprintf("%s", recording.MeetingAndOccurrenceID),
		fmt.Sprintf("past_meeting_recording_uid:%s", recording.MeetingAndOccurrenceID),
		fmt.Sprintf("past_meeting_uid:%s", recording.MeetingAndOccurrenceID),
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
func getPastMeetingTranscriptTags(recording *PastMeetingRecordingInput) []string {
	tags := []string{
		fmt.Sprintf("%s", recording.MeetingAndOccurrenceID),
		fmt.Sprintf("past_meeting_transcript_uid:%s", recording.MeetingAndOccurrenceID),
		fmt.Sprintf("past_meeting_uid:%s", recording.MeetingAndOccurrenceID),
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
func handleZoomPastMeetingRecordingUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting recording update")

	// Convert the v1Data map to PastMeetingRecordingInput struct
	recordingInput, err := convertMapToInputPastMeetingRecording(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to PastMeetingRecordingInput")
		return
	}

	// Extract the UID (MeetingAndOccurrenceID)
	uid := recordingInput.MeetingAndOccurrenceID
	if uid == "" {
		logger.With("key", key).ErrorContext(ctx, "missing meeting_and_occurrence_id in past meeting recording data")
		return
	}

	// Check if parent past meeting exists in mappings before proceeding.
	if uid == "" {
		logger.With("key", key).ErrorContext(ctx, "past meeting recording missing required parent past meeting ID")
		return
	}
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", uid)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		logger.With("past_meeting_id", uid).InfoContext(ctx, "skipping past meeting recording sync - parent past meeting not found in mappings")
		return
	}

	// Determine action based on mapping existence
	mappingKey := fmt.Sprintf("v1_past_meeting_recordings.%s", uid)
	indexerAction := MessageActionCreated
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdated
	}

	// Send recording indexer message
	recordingTags := getPastMeetingRecordingTags(recordingInput)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingRecordingSubject, indexerAction, recordingInput, recordingTags); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send recording indexer message")
		return
	}

	// Construct recording access message
	recordingAccessMsg := PastMeetingRecordingAccessMessage{
		ID:              uid,
		PastMeetingUID:  uid,
		RecordingAccess: string(recordingInput.RecordingAccess),
	}

	// Marshal recording access message
	recordingAccessMsgBytes, err := json.Marshal(recordingAccessMsg)
	if err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to marshal recording access message")
		return
	}

	// Send recording access message
	if err := sendAccessMessage(ctx, V1PastMeetingRecordingUpdateAccessSubject, recordingAccessMsgBytes); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send recording access message")
		return
	}

	// Send transcript indexer message
	transcriptTags := getPastMeetingTranscriptTags(recordingInput)
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingTranscriptSubject, indexerAction, recordingInput, transcriptTags); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send transcript indexer message")
		return
	}

	// Construct transcript access message
	transcriptAccessMsg := PastMeetingTranscriptAccessMessage{
		ID:               uid,
		PastMeetingUID:   uid,
		TranscriptAccess: string(recordingInput.TranscriptAccess),
	}

	// Marshal transcript access message
	transcriptAccessMsgBytes, err := json.Marshal(transcriptAccessMsg)
	if err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to marshal transcript access message")
		return
	}

	// Send transcript access message
	if err := sendAccessMessage(ctx, V1PastMeetingTranscriptUpdateAccessSubject, transcriptAccessMsgBytes); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send transcript access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "uid", uid).WarnContext(ctx, "failed to store past meeting recording mapping")
		}
	}

	logger.With("uid", uid, "key", key).InfoContext(ctx, "successfully sent recording and transcript indexer and access messages")
}

// PastMeetingSummaryAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for summaries.
type PastMeetingSummaryAccessMessage struct {
	ID             string `json:"id"`
	PastMeetingUID string `json:"meeting_and_occurrence_id"`
	SummaryAccess  string `json:"summary_access"`
}

// convertMapToInputPastMeetingSummary converts a map[string]any to a PastMeetingSummaryInput struct.
func convertMapToInputPastMeetingSummary(ctx context.Context, v1Data map[string]any) (*PastMeetingSummaryInput, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingSummaryInput struct
	var summary PastMeetingSummaryInput
	if err := json.Unmarshal(jsonBytes, &summary); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into PastMeetingSummaryInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingSummaryInput: %w", err)
	}

	return &summary, nil
}

func getPastMeetingSummaryTags(summary *PastMeetingSummaryInput) []string {
	tags := []string{
		fmt.Sprintf("%s", summary.ID),
		fmt.Sprintf("past_meeting_summary_uid:%s", summary.ID),
		fmt.Sprintf("past_meeting_uid:%s", summary.MeetingAndOccurrenceID),
		fmt.Sprintf("meeting_uid:%s", summary.MeetingID),
		"platform:Zoom",
		fmt.Sprintf("title:%s", summary.SummaryTitle),
	}
	return tags
}

// handleZoomPastMeetingSummaryUpdate handles the v1 past meeting summary update event.
// It sends NATS messages for summary indexing and access control.
func handleZoomPastMeetingSummaryUpdate(ctx context.Context, key string, v1Data map[string]any) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting summary update")

	// Convert the v1Data map to PastMeetingSummaryInput struct
	summaryInput, err := convertMapToInputPastMeetingSummary(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to PastMeetingSummaryInput")
		return
	}

	// Extract the UID (ID)
	uid := summaryInput.ID
	if uid == "" {
		logger.With("key", key).ErrorContext(ctx, "missing id in past meeting summary data")
		return
	}

	// Check if parent past meeting exists in mappings before proceeding.
	if summaryInput.MeetingAndOccurrenceID == "" {
		logger.With("summary_id", uid).ErrorContext(ctx, "past meeting summary missing required parent past meeting ID")
		return
	}
	pastMeetingMappingKey := fmt.Sprintf("v1_past_meetings.%s", summaryInput.MeetingAndOccurrenceID)
	if _, err := mappingsKV.Get(ctx, pastMeetingMappingKey); err != nil {
		logger.With("past_meeting_id", summaryInput.MeetingAndOccurrenceID, "summary_id", uid).InfoContext(ctx, "skipping past meeting summary sync - parent past meeting not found in mappings")
		return
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
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send summary indexer message")
		return
	}

	aiSummaryAccess := ""
	if summaryInput.MeetingAndOccurrenceID != "" {
		pastMeetingKey := fmt.Sprintf("itx-zoom-past-meetings.%s", summaryInput.MeetingAndOccurrenceID)
		pastMeetingEntry, err := v1KV.Get(ctx, pastMeetingKey)
		if err == nil && pastMeetingEntry != nil {
			var pastMeetingData map[string]any
			if err := json.Unmarshal(pastMeetingEntry.Value(), &pastMeetingData); err == nil {
				if aiSummaryAccessValue, ok := pastMeetingData["ai_summary_access"].(string); ok && aiSummaryAccessValue != "" {
					aiSummaryAccess = aiSummaryAccessValue
				}
			} else {
				logger.With(errKey, err, "meeting_and_occurrence_id", summaryInput.MeetingAndOccurrenceID).WarnContext(ctx, "failed to unmarshal past meeting data")
			}
		} else {
			logger.With(errKey, err, "meeting_and_occurrence_id", summaryInput.MeetingAndOccurrenceID).WarnContext(ctx, "failed to fetch past meeting from KV bucket")
		}
	}

	summaryAccessMsg := PastMeetingSummaryAccessMessage{
		ID:             uid,
		PastMeetingUID: summaryInput.MeetingAndOccurrenceID,
		SummaryAccess:  aiSummaryAccess,
	}

	// Marshal summary access message
	summaryAccessMsgBytes, err := json.Marshal(summaryAccessMsg)
	if err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to marshal summary access message")
		return
	}

	// Send summary access message
	if err := sendAccessMessage(ctx, V1PastMeetingSummaryUpdateAccessSubject, summaryAccessMsgBytes); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send summary access message")
		return
	}

	if uid != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "uid", uid).WarnContext(ctx, "failed to store past meeting summary mapping")
		}
	}

	logger.With("uid", uid, "meeting_and_occurrence_id", summaryInput.MeetingAndOccurrenceID, "key", key).InfoContext(ctx, "successfully sent summary indexer and access messages")
}
