package main

import (
	"context"
	"encoding/json"
	"fmt"

	zoomModels "github.com/linuxfoundation-it/itx-service-zoom/pkg/models"
	"github.com/nats-io/nats.go/jetstream"
)

// MessageAction represents the type of action being performed on a resource.
type MessageAction string

const (
	// MessageActionCreate indicates a new resource is being created.
	MessageActionCreate MessageAction = "create"
	// MessageActionUpdate indicates an existing resource is being updated.
	MessageActionUpdate MessageAction = "update"
	// MessageActionDelete indicates a resource is being deleted.
	MessageActionDelete MessageAction = "delete"
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
		headers["Authorization"] = authorization
	} else {
		// Fallback for system-generated events that don't have user auth context
		// This is just a dummy value so that the indexer service can still process the message,
		// given that it requires an authorization header.
		headers["Authorization"] = "Bearer v1-sync-helper"
	}

	// Extract principal from context if available
	if principal, ok := ctx.Value("principal").(string); ok {
		headers["X-On-Behalf-Of"] = principal
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

	logger.With("subject", subject).DebugContext(ctx, "successfully published indexer message")
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

	logger.With("subject", subject).DebugContext(ctx, "successfully published message")
	return nil
}

// MeetingAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
type MeetingAccessMessage struct {
	UID        string   `json:"uid"`
	Public     bool     `json:"public"`
	ProjectUID string   `json:"project_uid"`
	Organizers []string `json:"organizers"`
	Committees []string `json:"committees"`
}

// convertMapToInputMeeting converts a map[string]any to an InputMeeting struct.
func convertMapToInputMeeting(ctx context.Context, v1Data map[string]any) (*zoomModels.ZoomMeetingDatabase, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into InputMeeting struct
	var meeting zoomModels.ZoomMeetingDatabase
	if err := json.Unmarshal(jsonBytes, &meeting); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into InputMeeting")
		return nil, fmt.Errorf("failed to unmarshal JSON into InputMeeting: %w", err)
	}

	return &meeting, nil
}

// handleZoomMeetingUpdate processes a zoom meeting update from itx-zoom-meetings-v2 records.
func handleZoomMeetingUpdate(ctx context.Context, key string, v1Data map[string]any, mappingsKV jetstream.KeyValue) {
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

	mappingKey := fmt.Sprintf("v1_meetings.%s", uid)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	tags := []string{fmt.Sprintf("topic:%s", meeting.Topic)}
	if err := sendIndexerMessage(ctx, IndexV1MeetingSubject, indexerAction, v1Data, tags); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send meeting indexer message")
		return
	}

	// Extract committees - this should be an array of committee UIDs
	var committees []string
	if committeesData, ok := v1Data["committees"].([]any); ok {
		for _, c := range committeesData {
			if committee, ok := c.(map[string]any); ok {
				if committeeUID, ok := committee["uid"].(string); ok && committeeUID != "" {
					committees = append(committees, committeeUID)
				}
			}
		}
	}

	accessMsg := MeetingAccessMessage{
		UID:        uid,
		Public:     meeting.Visibility == "public",
		ProjectUID: meeting.Project,
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

	logger.With("uid", uid, "key", key).DebugContext(ctx, "successfully sent meeting indexer and access messages")
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

// handleZoomMeetingMappingUpdate processes a zoom meeting mapping update from itx-zoom-meetings-mappings-v2 records.
// When a mapping is created/updated, we need to fetch the associated meeting and trigger a re-index with updated committees.
func handleZoomMeetingMappingUpdate(ctx context.Context, key string, v1Data map[string]any, v1KV jetstream.KeyValue, mappingsKV jetstream.KeyValue) {
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

	// Extract the committee ID from the mapping
	committeeID := mapping.CommitteeID
	if committeeID == "" {
		logger.With("key", key, "meeting_id", meetingID).WarnContext(ctx, "mapping has no committee_id")
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

	// Extract existing committees from the meeting object
	var committees []string
	if committeesData, ok := meetingData["committees"].([]any); ok {
		for _, c := range committeesData {
			if committee, ok := c.(map[string]any); ok {
				if committeeUID, ok := committee["uid"].(string); ok && committeeUID != "" {
					committees = append(committees, committeeUID)
				}
			}
		}
	}

	// Add the committee from this mapping if it's not already in the list
	if committeeID != "" {
		found := false
		for _, existingCommittee := range committees {
			if existingCommittee == committeeID {
				found = true
				break
			}
		}
		if !found {
			committees = append(committees, committeeID)
		}
	}

	// Determine indexer action based on mapping existence
	mappingKey := fmt.Sprintf("v1_meetings.%s", meetingID)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	// Send meeting indexer message with the meeting data
	tags := []string{fmt.Sprintf("topic:%s", meeting.Topic)}
	if err := sendIndexerMessage(ctx, IndexV1MeetingSubject, indexerAction, meetingData, tags); err != nil {
		logger.With(errKey, err, "meeting_id", meetingID, "key", key).ErrorContext(ctx, "failed to send meeting indexer message")
		return
	}

	// Send meeting access message with updated committees
	accessMsg := MeetingAccessMessage{
		UID:        meetingID,
		Public:     meeting.Visibility == "public",
		ProjectUID: meeting.Project,
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

	// Store the mapping
	if meetingID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "meeting_id", meetingID).WarnContext(ctx, "failed to store meeting mapping")
		}
	}

	logger.With("meeting_id", meetingID, "committee_id", committeeID, "total_committees", len(committees), "key", key).DebugContext(ctx, "successfully triggered meeting re-index with updated committees")
}

// convertMapToInputRegistrant converts a map[string]any to a RegistrantInput struct.
func convertMapToInputRegistrant(ctx context.Context, v1Data map[string]any) (*zoomModels.ZoomMeetingRegistrant, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into RegistrantInput struct
	var registrant zoomModels.ZoomMeetingRegistrant
	if err := json.Unmarshal(jsonBytes, &registrant); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into RegistrantInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into RegistrantInput: %w", err)
	}

	return &registrant, nil
}

// MeetingRegistrantAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
type MeetingRegistrantAccessMessage struct {
	UID        string `json:"uid"`
	MeetingUID string `json:"meeting_uid"`
	Username   string `json:"username"`
	Host       bool   `json:"host"`
}

// handleZoomMeetingRegistrantUpdate processes a zoom meeting registrant update from itx-zoom-meetings-registrants-v2 records.
func handleZoomMeetingRegistrantUpdate(ctx context.Context, key string, v1Data map[string]any, mappingsKV jetstream.KeyValue) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom meeting registrant update")

	// Convert v1Data map to RegistrantInput struct
	registrant, err := convertMapToInputRegistrant(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to RegistrantInput")
		return
	}

	// Extract the registrant ID
	registrantID := registrant.ID
	if registrantID == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid id in v1 registrant data")
		return
	}

	mappingKey := fmt.Sprintf("v1_meeting_registrants.%s", registrantID)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	if err := sendIndexerMessage(ctx, IndexV1MeetingRegistrantSubject, indexerAction, v1Data, []string{}); err != nil {
		logger.With(errKey, err, "id", registrantID, "key", key).ErrorContext(ctx, "failed to send registrant indexer message")
		return
	}

	accessMsg := MeetingRegistrantAccessMessage{
		UID:        registrantID,
		MeetingUID: registrant.MeetingID,
		Username:   registrant.Username,
		Host:       *registrant.Host,
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

	if registrantID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "id", registrantID).WarnContext(ctx, "failed to store registrant mapping")
		}
	}

	logger.With("id", registrantID, "meeting_id", registrant.MeetingID, "key", key).DebugContext(ctx, "successfully sent registrant indexer and put messages")
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
func convertMapToInputPastMeeting(ctx context.Context, v1Data map[string]any) (*zoomModels.ZoomPastMeeting, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingInput struct
	var pastMeeting zoomModels.ZoomPastMeeting
	if err := json.Unmarshal(jsonBytes, &pastMeeting); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into PastMeetingInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingInput: %w", err)
	}

	return &pastMeeting, nil
}

// handleZoomPastMeetingUpdate processes a zoom past meeting update from itx-zoom-past-meetings-v2 records.
func handleZoomPastMeetingUpdate(ctx context.Context, key string, v1Data map[string]any, mappingsKV jetstream.KeyValue) {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return
	}

	logger.With("key", key).DebugContext(ctx, "processing zoom past meeting update")

	// Convert v1Data map to PastMeetingInput struct
	pastMeeting, err := convertMapToInputPastMeeting(ctx, v1Data)
	if err != nil {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to convert v1Data to PastMeetingInput")
		return
	}

	// Extract the past meeting UID (MeetingAndOccurrenceID)
	uid := pastMeeting.MeetingAndOccurrenceID
	if uid == "" {
		logger.With("key", key).ErrorContext(ctx, "missing or invalid meeting_and_occurrence_id in v1 past meeting data")
		return
	}

	mappingKey := fmt.Sprintf("v1_past_meetings.%s", uid)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	if err := sendIndexerMessage(ctx, IndexV1PastMeetingSubject, indexerAction, v1Data, []string{}); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send past meeting indexer message")
		return
	}

	// Extract committees from the v1Data if present
	var committees []string
	if committeesData, ok := v1Data["committees"].([]any); ok {
		for _, c := range committeesData {
			if committee, ok := c.(map[string]any); ok {
				if committeeUID, ok := committee["uid"].(string); ok && committeeUID != "" {
					committees = append(committees, committeeUID)
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

	logger.With("uid", uid, "meeting_id", pastMeeting.MeetingID, "key", key).DebugContext(ctx, "successfully sent past meeting indexer and access messages")
}

// PastMeetingParticipantAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions.
type PastMeetingParticipantAccessMessage struct {
	Username   string `json:"username"`
	Host       bool   `json:"host"`
	IsInvited  bool   `json:"is_invited"`
	IsAttended bool   `json:"is_attended"`
}

// convertMapToInputPastMeetingInvitee converts a map[string]any to a PastMeetingInviteeInput struct.
func convertMapToInputPastMeetingInvitee(ctx context.Context, v1Data map[string]any) (*zoomModels.ZoomPastMeetingInviteeDatabase, error) {
	// Convert map to JSON bytes
	jsonBytes, err := json.Marshal(v1Data)
	if err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to marshal v1Data to JSON")
		return nil, fmt.Errorf("failed to marshal v1Data to JSON: %w", err)
	}

	// Unmarshal JSON bytes into PastMeetingInviteeInput struct
	var invitee zoomModels.ZoomPastMeetingInviteeDatabase
	if err := json.Unmarshal(jsonBytes, &invitee); err != nil {
		logger.With(errKey, err).ErrorContext(ctx, "failed to unmarshal JSON into PastMeetingInviteeInput")
		return nil, fmt.Errorf("failed to unmarshal JSON into PastMeetingInviteeInput: %w", err)
	}

	return &invitee, nil
}

// handleZoomPastMeetingInviteeUpdate processes a zoom past meeting invitee update from itx-zoom-past-meetings-invitees-v2 records.
func handleZoomPastMeetingInviteeUpdate(ctx context.Context, key string, v1Data map[string]any, v1KV jetstream.KeyValue, mappingsKV jetstream.KeyValue) {
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

	mappingKey := fmt.Sprintf("v1_past_meeting_invitees.%s", inviteeID)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	if err := sendIndexerMessage(ctx, IndexV1PastMeetingParticipantSubject, indexerAction, v1Data, []string{}); err != nil {
		logger.With(errKey, err, "id", inviteeID, "key", key).ErrorContext(ctx, "failed to send invitee indexer message")
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

	// For invitees, is_invited is always true since they are invitees
	accessMsg := PastMeetingParticipantAccessMessage{
		Username:   invitee.LFSSO,
		Host:       isHost,
		IsInvited:  true,
		IsAttended: false, // TODO: we need to ensure that the invitee event is handled before the attendee event so that this value doesn't get reset if the order is reversed
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

	if inviteeID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "id", inviteeID).WarnContext(ctx, "failed to store past meeting invitee mapping")
		}
	}

	logger.With("id", inviteeID, "meeting_and_occurrence_id", invitee.MeetingAndOccurrenceID, "key", key).DebugContext(ctx, "successfully sent invitee indexer and access messages")
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
func handleZoomPastMeetingAttendeeUpdate(ctx context.Context, key string, v1Data map[string]any, v1KV jetstream.KeyValue, mappingsKV jetstream.KeyValue) {
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

	mappingKey := fmt.Sprintf("v1_past_meeting_attendees.%s", attendeeID)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	if err := sendIndexerMessage(ctx, IndexV1PastMeetingParticipantSubject, indexerAction, v1Data, []string{}); err != nil {
		logger.With(errKey, err, "id", attendeeID, "key", key).ErrorContext(ctx, "failed to send attendee indexer message")
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

	// For attendees, is_attended is always true since they attended the meeting
	accessMsg := PastMeetingParticipantAccessMessage{
		Username:   attendee.LFSSO,
		Host:       isHost,
		IsInvited:  isRegistrant,
		IsAttended: true,
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

	if attendeeID != "" {
		if _, err := mappingsKV.Put(ctx, mappingKey, []byte("1")); err != nil {
			logger.With(errKey, err, "id", attendeeID).WarnContext(ctx, "failed to store past meeting attendee mapping")
		}
	}

	logger.With("id", attendeeID, "meeting_and_occurrence_id", attendee.MeetingAndOccurrenceID, "key", key).DebugContext(ctx, "successfully sent attendee indexer and access messages")
}

// PastMeetingRecordingAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for recordings.
type PastMeetingRecordingAccessMessage struct {
	UID                string              `json:"uid"`
	PastMeetingUID     string              `json:"past_meeting_uid"`
	ArtifactVisibility string              `json:"artifact_visibility"`
	Participants       []AccessParticipant `json:"participants"`
}

// PastMeetingTranscriptAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for transcripts.
type PastMeetingTranscriptAccessMessage struct {
	UID                string              `json:"uid"`
	PastMeetingUID     string              `json:"past_meeting_uid"`
	ArtifactVisibility string              `json:"artifact_visibility"`
	Participants       []AccessParticipant `json:"participants"`
}

// AccessParticipant represents a simplified participant for access control messages.
type AccessParticipant struct {
	Username string `json:"username"`
	Host     bool   `json:"host"`
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

	return &recording, nil
}

// handleZoomPastMeetingRecordingUpdate handles the v1 past meeting recording update event.
// It sends NATS messages for both recording and transcript indexing and access control.
func handleZoomPastMeetingRecordingUpdate(ctx context.Context, key string, v1Data map[string]any, mappingsKV jetstream.KeyValue) {
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

	// Determine action based on mapping existence
	mappingKey := fmt.Sprintf("v1_past_meeting_recordings.%s", uid)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	// Convert input to output for indexing
	recordingOutput := PastMeetingRecordingOutput{
		MeetingAndOccurrenceID: recordingInput.MeetingAndOccurrenceID,
		ProjectID:              recordingInput.ProjectID,
		ProjectSlug:            recordingInput.ProjectSlug,
		HostEmail:              recordingInput.HostEmail,
		HostID:                 recordingInput.HostID,
		MeetingID:              recordingInput.MeetingID,
		OccurrenceID:           recordingInput.OccurrenceID,
		RecordingAccess:        recordingInput.RecordingAccess,
		Topic:                  recordingInput.Topic,
		TranscriptAccess:       recordingInput.TranscriptAccess,
		TranscriptEnabled:      recordingInput.TranscriptEnabled,
		Visibility:             recordingInput.Visibility,
		RecordingCount:         recordingInput.RecordingCount,
		RecordingFiles:         recordingInput.RecordingFiles,
		Sessions:               recordingInput.Sessions,
		StartTime:              recordingInput.StartTime,
		TotalSize:              recordingInput.TotalSize,
		CreatedAt:              recordingInput.CreatedAt,
		ModifiedAt:             recordingInput.ModifiedAt,
		CreatedBy:              recordingInput.CreatedBy,
		UpdatedBy:              recordingInput.UpdatedBy,
	}

	// Send recording indexer message
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingRecordingSubject, indexerAction, recordingOutput, nil); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send recording indexer message")
		return
	}

	// Construct recording access message
	recordingAccessMsg := PastMeetingRecordingAccessMessage{
		UID:                uid,
		PastMeetingUID:     uid,
		ArtifactVisibility: string(recordingInput.RecordingAccess),
		Participants:       []AccessParticipant{}, // TODO: figure out how to get the participants from the past meeting
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
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingTranscriptSubject, indexerAction, recordingOutput, nil); err != nil {
		logger.With(errKey, err, "uid", uid, "key", key).ErrorContext(ctx, "failed to send transcript indexer message")
		return
	}

	// Construct transcript access message
	transcriptAccessMsg := PastMeetingTranscriptAccessMessage{
		UID:                uid,
		PastMeetingUID:     uid,
		ArtifactVisibility: string(recordingInput.TranscriptAccess),
		Participants:       []AccessParticipant{}, // TODO: figure out how to get the participants from the past meeting
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

	logger.With("uid", uid, "key", key).DebugContext(ctx, "successfully sent recording and transcript indexer and access messages")
}

// PastMeetingSummaryAccessMessage is the schema for the data in the message sent to the fga-sync service.
// These are the fields that the fga-sync service needs in order to update the OpenFGA permissions for summaries.
type PastMeetingSummaryAccessMessage struct {
	UID                string              `json:"uid"`
	PastMeetingUID     string              `json:"past_meeting_uid"`
	ArtifactVisibility string              `json:"artifact_visibility"`
	Participants       []AccessParticipant `json:"participants"`
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

// handleZoomPastMeetingSummaryUpdate handles the v1 past meeting summary update event.
// It sends NATS messages for summary indexing and access control.
func handleZoomPastMeetingSummaryUpdate(ctx context.Context, key string, v1Data map[string]any, v1KV jetstream.KeyValue, mappingsKV jetstream.KeyValue) {
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

	// Determine action based on mapping existence
	mappingKey := fmt.Sprintf("v1_past_meeting_summaries.%s", uid)
	indexerAction := MessageActionCreate
	if _, err := mappingsKV.Get(ctx, mappingKey); err == nil {
		indexerAction = MessageActionUpdate
	}

	// Convert input to output for indexing
	summaryOutput := PastMeetingSummaryOutput{
		ID:                      summaryInput.ID,
		MeetingAndOccurrenceID:  summaryInput.MeetingAndOccurrenceID,
		MeetingID:               summaryInput.MeetingID,
		OccurrenceID:            summaryInput.OccurrenceID,
		ZoomMeetingUUID:         summaryInput.ZoomMeetingUUID,
		ZoomMeetingHostID:       summaryInput.ZoomMeetingHostID,
		ZoomMeetingHostEmail:    summaryInput.ZoomMeetingHostEmail,
		ZoomMeetingTopic:        summaryInput.ZoomMeetingTopic,
		ZoomWebhookEvent:        summaryInput.ZoomWebhookEvent,
		Password:                summaryInput.Password,
		SummaryCreatedTime:      summaryInput.SummaryCreatedTime,
		SummaryLastModifiedTime: summaryInput.SummaryLastModifiedTime,
		SummaryStartTime:        summaryInput.SummaryStartTime,
		SummaryEndTime:          summaryInput.SummaryEndTime,
		SummaryTitle:            summaryInput.SummaryTitle,
		SummaryOverview:         summaryInput.SummaryOverview,
		SummaryDetails:          summaryInput.SummaryDetails,
		NextSteps:               summaryInput.NextSteps,
		EditedSummaryOverview:   summaryInput.EditedSummaryOverview,
		EditedSummaryDetails:    summaryInput.EditedSummaryDetails,
		EditedNextSteps:         summaryInput.EditedNextSteps,
		RequiresApproval:        summaryInput.RequiresApproval,
		Approved:                summaryInput.Approved,
		EmailSent:               summaryInput.EmailSent,
		CreatedAt:               summaryInput.CreatedAt,
		CreatedBy:               summaryInput.CreatedBy,
		ModifiedAt:              summaryInput.ModifiedAt,
		ModifiedBy:              summaryInput.ModifiedBy,
	}

	// Send summary indexer message
	if err := sendIndexerMessage(ctx, IndexV1PastMeetingSummarySubject, indexerAction, summaryOutput, nil); err != nil {
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
		UID:                uid,
		PastMeetingUID:     summaryInput.MeetingAndOccurrenceID,
		ArtifactVisibility: aiSummaryAccess,
		Participants:       []AccessParticipant{},
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

	logger.With("uid", uid, "meeting_and_occurrence_id", summaryInput.MeetingAndOccurrenceID, "key", key).DebugContext(ctx, "successfully sent summary indexer and access messages")
}
