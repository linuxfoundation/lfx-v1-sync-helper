// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"time"

	nats "github.com/nats-io/nats.go"
)

// indexingEvent mirrors the IndexingEvent published by the indexer service after a
// successful OpenSearch write. Subject format: lfx.{object_type}.{action}
// (e.g., lfx.committee.created, lfx.committee_member.deleted).
type indexingEvent struct {
	DocumentID string          `json:"document_id"` // "object_type:object_id"
	ObjectID   string          `json:"object_id"`
	ObjectType string          `json:"object_type"`
	Action     string          `json:"action"` // past-tense: "created", "updated", "deleted"
	Body       json.RawMessage `json:"body"`   // TransactionBody written to OpenSearch
	Timestamp  time.Time       `json:"timestamp"`
}

// indexingEventBody contains the fields from TransactionBody we care about.
type indexingEventBody struct {
	Data map[string]any `json:"data"`
}

// committeeIndexerEventHandler handles lfx.committee.{created,updated,deleted} events
// published by the indexer service after successful OpenSearch writes.
func committeeIndexerEventHandler(msg *nats.Msg) {
	ctx := context.Background()

	var event indexingEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err, "subject", msg.Subject).ErrorContext(ctx, "failed to unmarshal committee indexing event")
		return
	}

	logger.With(
		"subject", msg.Subject,
		"object_id", event.ObjectID,
		"action", event.Action,
	).InfoContext(ctx, "received committee indexing event")

	var body indexingEventBody
	if len(event.Body) > 0 {
		if err := json.Unmarshal(event.Body, &body); err != nil {
			logger.With(errKey, err, "committee_uid", event.ObjectID).
				ErrorContext(ctx, "failed to unmarshal committee event body, skipping")
			return
		}
	}

	switch event.Action {
	case "created":
		// Resolve project SFID from project_uid in the committee data.
		projectUID, _ := body.Data["project_uid"].(string)
		if projectUID == "" {
			logger.With("committee_uid", event.ObjectID).
				WarnContext(ctx, "no project_uid in committee event body, skipping")
			return
		}
		projectEntry, err := mappingsKV.Get(ctx, "project.uid."+projectUID)
		if err != nil || isTombstonedMapping(projectEntry.Value()) {
			logger.With(errKey, err, "project_uid", projectUID, "committee_uid", event.ObjectID).
				WarnContext(ctx, "could not resolve project SFID from project UID, skipping")
			return
		}
		projectSFID := string(projectEntry.Value())
		if projectSFID == "" {
			logger.With("committee_uid", event.ObjectID).
				WarnContext(ctx, "no project SFID found, skipping")
			return
		}
		logger.With("committee_uid", event.ObjectID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee created in v2 — ensuring v1 is in sync")
		syncCommitteeCreateToV1(ctx, event.ObjectID, projectSFID, body.Data)

	case "updated":
		projectSFID := ""
		committeeSFID := ""
		if entry, err := mappingsKV.Get(ctx, "committee.uid."+event.ObjectID); err == nil {
			projectSFID, committeeSFID, _ = splitTwoParts(string(entry.Value()))
		}
		if projectSFID == "" || committeeSFID == "" {
			logger.With("committee_uid", event.ObjectID).
				WarnContext(ctx, "no project SFID or committee SFID found, skipping")
			return
		}
		logger.With("committee_uid", event.ObjectID, "committee_sfid", committeeSFID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee updated in v2 — syncing to v1")
		syncCommitteeUpdateToV1(ctx, event.ObjectID, projectSFID, committeeSFID, body.Data)

	case "deleted":
		projectSFID := ""
		committeeSFID := ""
		if entry, err := mappingsKV.Get(ctx, "committee.uid."+event.ObjectID); err == nil {
			projectSFID, committeeSFID, _ = splitTwoParts(string(entry.Value()))
		}
		if projectSFID == "" || committeeSFID == "" {
			logger.With("committee_uid", event.ObjectID).
				WarnContext(ctx, "no project SFID or committee SFID found, skipping")
			return
		}
		logger.With("committee_uid", event.ObjectID, "committee_sfid", committeeSFID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee deleted in v2 — syncing deletion to v1")
		syncCommitteeDeleteToV1(ctx, event.ObjectID, projectSFID, committeeSFID)

	default:
		logger.With("action", event.Action, "subject", msg.Subject).
			WarnContext(ctx, "unknown action in committee indexing event, skipping")
	}
}

// committeeMemberIndexerEventHandler handles lfx.committee_member.{created,updated,deleted} events
// published by the indexer service after successful OpenSearch writes.
func committeeMemberIndexerEventHandler(msg *nats.Msg) {
	ctx := context.Background()

	var event indexingEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err, "subject", msg.Subject).ErrorContext(ctx, "failed to unmarshal committee member indexing event")
		return
	}

	logger.With(
		"subject", msg.Subject,
		"object_id", event.ObjectID,
		"action", event.Action,
	).InfoContext(ctx, "received committee member indexing event")

	// Look up the reverse mapping: member v2 UID -> "projectSFID:collaborationSFID:memberSFID"
	// stored by handleCommitteeMemberUpdate.
	reverseMappingKey := "committee_member.uid." + event.ObjectID
	entry, err := mappingsKV.Get(ctx, reverseMappingKey)
	if err != nil {
		logger.With(errKey, err, "member_uid", event.ObjectID, "subject", msg.Subject).
			WarnContext(ctx, "no reverse mapping for committee member UID, cannot sync to v1")
		return
	}

	projectSFID, collaborationSFID, memberSFID, ok := splitThreeParts(string(entry.Value()))
	if !ok {
		logger.With("mapping_value", string(entry.Value()), "member_uid", event.ObjectID).
			WarnContext(ctx, "committee member reverse mapping has unexpected format, skipping")
		return
	}

	// Resolve committee v2 UID from the collaboration SFID.
	committeeEntry, err := mappingsKV.Get(ctx, "committee.sfid."+collaborationSFID)
	if err != nil {
		logger.With(errKey, err, "collaboration_sfid", collaborationSFID, "member_uid", event.ObjectID).
			WarnContext(ctx, "could not resolve committee UID from collaboration SFID, skipping")
		return
	}
	committeeUID := string(committeeEntry.Value())

	var body indexingEventBody
	if len(event.Body) > 0 {
		if err := json.Unmarshal(event.Body, &body); err != nil {
			logger.With(errKey, err, "member_uid", event.ObjectID).
				WarnContext(ctx, "failed to unmarshal committee member event body, proceeding without body data")
		}
	}

	switch event.Action {
	case "created":
		logger.With("member_uid", event.ObjectID, "committee_uid", committeeUID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee member created in v2 — ensuring v1 is in sync")
		syncCommitteeMemberCreateToV1(ctx, event.ObjectID, projectSFID, committeeUID, body.Data)

	case "updated":
		logger.With("member_uid", event.ObjectID, "member_sfid", memberSFID, "committee_uid", committeeUID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee member updated in v2 — syncing to v1")
		syncCommitteeMemberUpdateToV1(ctx, event.ObjectID, projectSFID, committeeUID, memberSFID, body.Data)

	case "deleted":
		logger.With("member_uid", event.ObjectID, "member_sfid", memberSFID, "committee_uid", committeeUID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee member deleted in v2 — syncing deletion to v1")
		syncCommitteeMemberDeleteToV1(ctx, event.ObjectID, projectSFID, committeeUID, memberSFID)

	default:
		logger.With("action", event.Action, "subject", msg.Subject).
			WarnContext(ctx, "unknown action in committee member indexing event, skipping")
	}
}

// syncCommitteeCreateToV1 ensures a v2-created committee exists in v1.
// If a reverse mapping already exists the record originated in v1 — skip to avoid loops.
func syncCommitteeCreateToV1(ctx context.Context, committeeUID, projectSFID string, data map[string]any) {
	// A non-tombstoned reverse mapping means this was created from v1; skip.
	reverseKey := "committee.uid." + committeeUID
	if entry, err := mappingsKV.Get(ctx, reverseKey); err == nil && !isTombstonedMapping(entry.Value()) {
		logger.With("committee_uid", committeeUID).DebugContext(ctx, "committee originated from v1 — skipping reverse sync")
		return
	}

	name, _ := data["name"].(string)
	if name == "" || projectSFID == "" {
		logger.With("committee_uid", committeeUID).WarnContext(ctx, "missing name or project SFID for committee create sync, skipping")
		return
	}

	payload := projectServiceCommitteeCreate{Name: name}
	category, _ := data["category"].(string)
	mapped := mapV2CategoryToV1(category)
	logger.With("committee_uid", committeeUID, "v2_category", category, "v1_category", mapped).
		InfoContext(ctx, "mapping v2 committee category to v1")
	payload.Category = mapped
	if desc, ok := data["description"].(string); ok {
		payload.Description = desc
	}
	if website, ok := data["website"].(string); ok {
		payload.Website = website
	}

	logger.With("committee_uid", committeeUID, "project_sfid", projectSFID, "payload_category", payload.Category).
		InfoContext(ctx, "creating committee in v1")

	result, err := createV1Committee(ctx, projectSFID, payload)
	if err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID, "project_sfid", projectSFID).
			ErrorContext(ctx, "failed to create committee in v1")
		return
	}

	// Store forward mapping (v1 SFID -> v2 UID) and reverse mapping (v2 UID -> projectSFID:committeeSFID).
	committeeSFID := result.ID
	if _, err := mappingsKV.Put(ctx, "committee.sfid."+committeeSFID, []byte(committeeUID)); err != nil {
		logger.With(errKey, err, "committee_sfid", committeeSFID, "committee_uid", committeeUID).
			WarnContext(ctx, "failed to store committee forward mapping after v1 create")
	}
	reverseMappingValue := projectSFID + ":" + committeeSFID
	if _, err := mappingsKV.Put(ctx, "committee.uid."+committeeUID, []byte(reverseMappingValue)); err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID, "committee_sfid", committeeSFID).
			WarnContext(ctx, "failed to store committee reverse mapping after v1 create")
	}

	logger.With("committee_uid", committeeUID, "committee_sfid", committeeSFID, "project_sfid", projectSFID).
		InfoContext(ctx, "successfully created committee in v1 from indexer event")
}

// syncCommitteeUpdateToV1 patches a v1 committee to match the v2 state.
func syncCommitteeUpdateToV1(ctx context.Context, committeeUID, projectSFID, committeeSFID string, data map[string]any) {
	payload := projectServiceCommitteeUpdate{}
	name, _ := data["name"].(string)
	if name != "" {
		payload.Name = name
	}
	if category, ok := data["category"].(string); ok {
		payload.Category = mapV2CategoryToV1(category)
	}
	if desc, ok := data["description"].(string); ok {
		payload.Description = desc
	}
	if website, ok := data["website"].(string); ok {
		payload.Website = website
	}

	if err := updateV1Committee(ctx, projectSFID, committeeSFID, payload); err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID, "project_sfid", projectSFID).
			ErrorContext(ctx, "failed to update committee in v1")
		return
	}

	logger.With("committee_uid", committeeUID, "project_sfid", projectSFID).
		InfoContext(ctx, "successfully updated committee in v1 from indexer event")
}

// syncCommitteeDeleteToV1 deletes a v1 committee that was deleted in v2.
func syncCommitteeDeleteToV1(ctx context.Context, committeeUID, projectSFID, committeeSFID string) {
	if err := deleteV1Committee(ctx, projectSFID, committeeSFID); err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID, "project_sfid", projectSFID).
			ErrorContext(ctx, "failed to delete committee in v1")
		return
	}

	if err := tombstoneMapping(ctx, "committee.sfid."+committeeSFID); err != nil {
		logger.With(errKey, err, "committee_sfid", committeeSFID).WarnContext(ctx, "failed to tombstone committee forward mapping after v1 delete")
	}
	if err := tombstoneMapping(ctx, "committee.uid."+committeeUID); err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID).WarnContext(ctx, "failed to tombstone committee reverse mapping after v1 delete")
	}

	logger.With("committee_uid", committeeUID, "project_sfid", projectSFID).
		InfoContext(ctx, "successfully deleted committee in v1 from indexer event")
}

// syncCommitteeMemberCreateToV1 ensures a v2-created committee member exists in v1.
func syncCommitteeMemberCreateToV1(ctx context.Context, memberUID, projectSFID, committeeUID string, data map[string]any) {
	// A non-tombstoned reverse mapping means this was created from v1; skip.
	reverseKey := "committee_member.uid." + memberUID
	if entry, err := mappingsKV.Get(ctx, reverseKey); err == nil && !isTombstonedMapping(entry.Value()) {
		logger.With("member_uid", memberUID).DebugContext(ctx, "committee member originated from v1 — skipping reverse sync")
		return
	}

	email, _ := data["email"].(string)
	if email == "" {
		logger.With("member_uid", memberUID).WarnContext(ctx, "missing email for committee member create sync, skipping")
		return
	}

	payload := projectServiceCommitteeMemberCreate{Email: email}
	if firstName, ok := data["first_name"].(string); ok {
		payload.FirstName = firstName
	}
	if lastName, ok := data["last_name"].(string); ok {
		payload.LastName = lastName
	}
	if role, ok := data["role"].(string); ok {
		payload.Role = role
	}
	if status, ok := data["status"].(string); ok {
		payload.Status = status
	}

	result, err := createV1CommitteeMember(ctx, projectSFID, committeeUID, payload)
	if err != nil {
		logger.With(errKey, err, "member_uid", memberUID, "committee_uid", committeeUID).
			ErrorContext(ctx, "failed to create committee member in v1")
		return
	}

	logger.With("member_uid", memberUID, "v1_id", result.ID, "committee_uid", committeeUID).
		InfoContext(ctx, "successfully created committee member in v1 from indexer event")
}

// syncCommitteeMemberUpdateToV1 patches a v1 committee member to match the v2 state.
func syncCommitteeMemberUpdateToV1(ctx context.Context, memberUID, projectSFID, committeeUID, _ string, data map[string]any) {
	payload := projectServiceCommitteeMemberUpdate{}
	if email, ok := data["email"].(string); ok {
		payload.Email = email
	}
	if role, ok := data["role"].(string); ok {
		payload.Role = role
	}
	if status, ok := data["status"].(string); ok {
		payload.Status = status
	}
	if title, ok := data["title"].(string); ok {
		payload.Title = title
	}

	if err := updateV1CommitteeMember(ctx, projectSFID, committeeUID, memberUID, payload); err != nil {
		logger.With(errKey, err, "member_uid", memberUID, "committee_uid", committeeUID).
			ErrorContext(ctx, "failed to update committee member in v1")
		return
	}

	logger.With("member_uid", memberUID, "committee_uid", committeeUID).
		InfoContext(ctx, "successfully updated committee member in v1 from indexer event")
}

// syncCommitteeMemberDeleteToV1 deletes a v1 committee member that was deleted in v2.
func syncCommitteeMemberDeleteToV1(ctx context.Context, memberUID, projectSFID, committeeUID, memberSFID string) {
	if err := deleteV1CommitteeMember(ctx, projectSFID, committeeUID, memberUID); err != nil {
		logger.With(errKey, err, "member_uid", memberUID, "committee_uid", committeeUID).
			ErrorContext(ctx, "failed to delete committee member in v1")
		return
	}

	if err := tombstoneMapping(ctx, "committee_member.sfid."+memberSFID); err != nil {
		logger.With(errKey, err, "member_sfid", memberSFID).WarnContext(ctx, "failed to tombstone committee member forward mapping after v1 delete")
	}
	if err := tombstoneMapping(ctx, "committee_member.uid."+memberUID); err != nil {
		logger.With(errKey, err, "member_uid", memberUID).WarnContext(ctx, "failed to tombstone committee member reverse mapping after v1 delete")
	}

	logger.With("member_uid", memberUID, "committee_uid", committeeUID).
		InfoContext(ctx, "successfully deleted committee member in v1 from indexer event")
}

// splitTwoParts splits an "a:b" string into its two parts.
func splitTwoParts(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// splitThreeParts splits an "a:b:c" string into its three parts.
func splitThreeParts(s string) (string, string, string, bool) {
	first := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			if first == -1 {
				first = i
			} else {
				return s[:first], s[first+1 : i], s[i+1:], true
			}
		}
	}
	return "", "", "", false
}

// mapV2CategoryToV1 converts a v2 committee category to the equivalent v1 API value.
// v1 uses a combined "Technical Oversight Committee/Technical Advisory Committee" for both
// separate v2 values. All other v2 values match v1 directly.
// Returns "Other" as a fallback for unrecognized values.
func mapV2CategoryToV1(category string) string {
	switch category {
	case "Technical Oversight Committee", "Technical Advisory Committee":
		return "Technical Oversight Committee/Technical Advisory Committee"
	case "Ambassador", "Board", "Code of Conduct", "Committers", "Expert Group",
		"Finance Committee", "Government Advisory Council", "Legal Committee", "Maintainers",
		"Marketing Committee/Sub Committee", "Marketing Mailing List",
		"Marketing Oversight Committee/Marketing Advisory Committee", "Other",
		"Product Security", "Special Interest Group", "Technical Mailing List",
		"Technical Steering Committee", "Working Group":
		return category
	default:
		return "Other"
	}
}
