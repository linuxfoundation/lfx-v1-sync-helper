// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	var body indexingEventBody
	if len(event.Body) > 0 {
		if err := json.Unmarshal(event.Body, &body); err != nil {
			logger.With(errKey, err, "member_uid", event.ObjectID).
				WarnContext(ctx, "failed to unmarshal committee member event body, proceeding without body data")
		}
	}

	switch event.Action {
	case "created":
		committeeUID, _ := body.Data["committee_uid"].(string)
		if committeeUID == "" {
			logger.With("member_uid", event.ObjectID).
				WarnContext(ctx, "no committee_uid in committee member event body, skipping")
			return
		}
		committeeEntry, err := mappingsKV.Get(ctx, "committee.uid."+committeeUID)
		if err != nil || isTombstonedMapping(committeeEntry.Value()) {
			logger.With(errKey, err, "committee_uid", committeeUID, "member_uid", event.ObjectID).
				WarnContext(ctx, "could not resolve project SFID from committee UID, skipping")
			return
		}
		projectSFID, committeeSFID, ok := splitTwoParts(string(committeeEntry.Value()))
		if !ok || projectSFID == "" || committeeSFID == "" {
			logger.With("committee_uid", committeeUID, "member_uid", event.ObjectID).
				WarnContext(ctx, "committee reverse mapping has unexpected format, skipping")
			return
		}
		logger.With("member_uid", event.ObjectID, "committee_uid", committeeUID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee member created in v2 — ensuring v1 is in sync")
		syncCommitteeMemberCreateToV1(ctx, event.ObjectID, committeeUID, projectSFID, committeeSFID, body.Data)

	case "updated":
		reverseMappingKey := "committee_member.uid." + event.ObjectID
		entry, err := mappingsKV.Get(ctx, reverseMappingKey)
		if err != nil {
			logger.With(errKey, err, "member_uid", event.ObjectID, "subject", msg.Subject).
				WarnContext(ctx, "no reverse mapping for committee member UID, cannot sync to v1")
			return
		}
		projectSFID, committeeSFID, recordSFID, contactSFID, ok := parseCommitteeMemberReverseMapping(string(entry.Value()))
		if !ok {
			logger.With("mapping_value", string(entry.Value()), "member_uid", event.ObjectID).
				WarnContext(ctx, "committee member reverse mapping has unexpected format, skipping")
			return
		}
		// Prefer the contact SFID (v1 API "MemberID"); fall back to the record SFID
		// for mappings written before this field existed.
		memberSFID := contactSFID
		if memberSFID == "" {
			memberSFID = recordSFID
		}
		logger.With("member_uid", event.ObjectID, "member_sfid", memberSFID, "committee_sfid", committeeSFID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee member updated in v2 — syncing to v1")
		syncCommitteeMemberUpdateToV1(ctx, event.ObjectID, projectSFID, committeeSFID, memberSFID, body.Data)

	case "deleted":
		reverseMappingKey := "committee_member.uid." + event.ObjectID
		entry, err := mappingsKV.Get(ctx, reverseMappingKey)
		if err != nil {
			logger.With(errKey, err, "member_uid", event.ObjectID, "subject", msg.Subject).
				WarnContext(ctx, "no reverse mapping for committee member UID, cannot sync to v1")
			return
		}
		projectSFID, committeeSFID, recordSFID, contactSFID, ok := parseCommitteeMemberReverseMapping(string(entry.Value()))
		if !ok {
			logger.With("mapping_value", string(entry.Value()), "member_uid", event.ObjectID).
				WarnContext(ctx, "committee member reverse mapping has unexpected format, skipping")
			return
		}
		// Prefer the contact SFID (v1 API "MemberID"); fall back to the record SFID
		// for mappings not yet backfilled to carry a contact SFID.
		apiMemberSFID := contactSFID
		if apiMemberSFID == "" {
			apiMemberSFID = recordSFID
		}
		// The record sfid needed to tombstone the v1 forward mapping is not part of
		// the reverse mapping value itself (see parseCommitteeMemberReverseMapping);
		// resolve it from the auxiliary key when the reverse mapping's own third
		// field wasn't a record sfid.
		if recordSFID == "" {
			recordSFID = resolveCommitteeMemberRecordSFID(ctx, event.ObjectID)
		}
		logger.With("member_uid", event.ObjectID, "member_sfid", apiMemberSFID, "record_sfid", recordSFID, "committee_sfid", committeeSFID, "project_sfid", projectSFID).
			InfoContext(ctx, "committee member deleted in v2 — syncing deletion to v1")
		syncCommitteeMemberDeleteToV1(ctx, event.ObjectID, projectSFID, committeeSFID, apiMemberSFID, recordSFID)

	default:
		logger.With("action", event.Action, "subject", msg.Subject).
			WarnContext(ctx, "unknown action in committee member indexing event, skipping")
	}
}

// syncCommitteeCreateToV1 ensures a v2-created committee exists in v1.
// If a reverse mapping already exists the record originated in v1 — skip to avoid loops.
func syncCommitteeCreateToV1(ctx context.Context, committeeUID, projectSFID string, data map[string]any) {
	log := logger.With("committee_uid", committeeUID, "project_sfid", projectSFID)

	// A non-tombstoned reverse mapping means this was created from v1; skip.
	reverseKey := "committee.uid." + committeeUID
	if entry, err := mappingsKV.Get(ctx, reverseKey); err == nil && !isTombstonedMapping(entry.Value()) {
		log.DebugContext(ctx, "committee originated from v1 — skipping reverse sync")
		return
	}

	name, _ := data["name"].(string)
	if name == "" || projectSFID == "" {
		log.WarnContext(ctx, "missing name or project SFID for committee create sync, skipping")
		return
	}

	payload := projectServiceCommitteeCreate{Name: name}
	category, _ := data["category"].(string)
	mapped := mapV2CategoryToV1(category)
	log.With("v2_category", category, "v1_category", mapped).InfoContext(ctx, "mapping v2 committee category to v1")
	payload.Category = mapped
	if desc, ok := data["description"].(string); ok {
		payload.Description = desc
	}
	if website, ok := data["website"].(string); ok {
		payload.Website = website
	}
	if joinMode, ok := data["join_mode"].(string); ok {
		payload.JoinMode = joinMode
	}
	if mailingListEmail, ok := data["mailing_list_email"].(string); ok {
		payload.MailingList = mailingListEmail
	}
	if chatChannel, ok := data["chat_channel"].(string); ok {
		payload.ChatChannel = chatChannel
	}
	if ssoGroupName, ok := data["sso_group_name"].(string); ok && ssoGroupName != "" {
		payload.SSOGroupName = ssoGroupName
	}

	log.With("payload_category", payload.Category).InfoContext(ctx, "creating committee in v1")

	result, err := createV1Committee(ctx, projectSFID, payload)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to create committee in v1")
		return
	}

	// Store forward mapping (v1 SFID -> v2 UID) and reverse mapping (v2 UID -> projectSFID:committeeSFID).
	committeeSFID := result.ID
	if _, err := mappingsKV.Put(ctx, "committee.sfid."+committeeSFID, []byte(committeeUID)); err != nil {
		log.With(errKey, err, "committee_sfid", committeeSFID).
			WarnContext(ctx, "failed to store committee forward mapping after v1 create")
	}
	reverseMappingValue := projectSFID + ":" + committeeSFID
	if _, err := mappingsKV.Put(ctx, "committee.uid."+committeeUID, []byte(reverseMappingValue)); err != nil {
		log.With(errKey, err, "committee_sfid", committeeSFID).
			WarnContext(ctx, "failed to store committee reverse mapping after v1 create")
	}

	log.With("committee_sfid", committeeSFID).InfoContext(ctx, "successfully created committee in v1 from indexer event")
}

// syncCommitteeUpdateToV1 patches a v1 committee to match the v2 state.
func syncCommitteeUpdateToV1(ctx context.Context, committeeUID, projectSFID, committeeSFID string, data map[string]any) {
	log := logger.With("committee_uid", committeeUID, "project_sfid", projectSFID, "committee_sfid", committeeSFID)

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
	if joinMode, ok := data["join_mode"].(string); ok {
		payload.JoinMode = joinMode
	}
	if mailingListEmail, ok := data["mailing_list_email"].(string); ok {
		payload.MailingList = mailingListEmail
	}
	if chatChannel, ok := data["chat_channel"].(string); ok {
		payload.ChatChannel = chatChannel
	}
	if ssoGroupName, ok := data["sso_group_name"].(string); ok && ssoGroupName != "" {
		payload.SSOGroupName = ssoGroupName
	}

	if err := updateV1Committee(ctx, projectSFID, committeeSFID, payload); err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to update committee in v1")
		return
	}

	log.InfoContext(ctx, "successfully updated committee in v1 from indexer event")
}

// syncCommitteeDeleteToV1 deletes a v1 committee that was deleted in v2.
func syncCommitteeDeleteToV1(ctx context.Context, committeeUID, projectSFID, committeeSFID string) {
	log := logger.With("committee_uid", committeeUID, "project_sfid", projectSFID, "committee_sfid", committeeSFID)

	if err := deleteV1Committee(ctx, projectSFID, committeeSFID); err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to delete committee in v1")
		return
	}

	if err := tombstoneMapping(ctx, "committee.sfid."+committeeSFID); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to tombstone committee forward mapping after v1 delete")
	}
	if err := tombstoneMapping(ctx, "committee.uid."+committeeUID); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to tombstone committee reverse mapping after v1 delete")
	}

	log.InfoContext(ctx, "successfully deleted committee in v1 from indexer event")
}

// syncCommitteeMemberCreateToV1 ensures a v2-created committee member exists in v1.
func syncCommitteeMemberCreateToV1(ctx context.Context, memberUID, committeeUID, projectSFID, committeeSFID string, data map[string]any) {
	log := logger.With("member_uid", memberUID, "committee_uid", committeeUID, "project_sfid", projectSFID, "committee_sfid", committeeSFID)

	// A non-tombstoned reverse mapping means this was created from v1; skip.
	reverseKey := "committee_member.uid." + memberUID
	if entry, err := mappingsKV.Get(ctx, reverseKey); err == nil && !isTombstonedMapping(entry.Value()) {
		log.DebugContext(ctx, "committee member originated from v1 — skipping reverse sync")
		return
	}

	email, _ := data["email"].(string)
	if email == "" {
		log.WarnContext(ctx, "missing email for committee member create sync, skipping")
		return
	}

	payload := projectServiceCommitteeMemberCreate{Email: email}
	if firstName, ok := data["first_name"].(string); ok {
		payload.FirstName = firstName
	}
	if lastName, ok := data["last_name"].(string); ok {
		payload.LastName = lastName
	}
	if jobTitle, ok := data["job_title"].(string); ok {
		payload.Title = jobTitle
	}
	if role, ok := data["role"].(string); ok {
		payload.Role = role
	}
	if status, ok := data["status"].(string); ok {
		payload.Status = status
	}
	if appointedBy, ok := data["appointed_by"].(string); ok {
		payload.AppointedBy = appointedBy
	}
	if agency, ok := data["agency"].(string); ok {
		payload.Agency = agency
	}
	if country, ok := data["country"].(string); ok {
		payload.Country = country
	}
	if voting, ok := data["voting"].(map[string]any); ok {
		if vs, ok := voting["status"].(string); ok {
			payload.VotingStatus = vs
		}
		if vsd, ok := voting["start_date"].(string); ok {
			payload.VotingStartDate = vsd
		}
		if ved, ok := voting["end_date"].(string); ok {
			payload.VotingEndDate = ved
		}
	}
	if orgID, err := resolveOrgIDFromEventData(ctx, data); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to resolve organization ID, proceeding without org")
	} else if orgID != "" {
		payload.OrganizationID = orgID
	}

	// Attempt to resolve the v1 user SFID by email to populate MemberID.
	// This links the committee member to an existing v1 platform user if one exists.
	if userSFID, err := ResolveV1UserSFIDByEmail(ctx, email); err != nil {
		log.With(errKey, err, "email", email).WarnContext(ctx, "failed to resolve user SFID by email, proceeding without MemberID")
	} else if userSFID != "" {
		payload.MemberID = userSFID
		log.With("member_id", userSFID, "email", email).DebugContext(ctx, "resolved user SFID for committee member")
	}

	result, err := createV1CommitteeMember(ctx, projectSFID, committeeSFID, payload)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to create committee member in v1")
		return
	}

	// Store forward mapping (v1 SFID -> committeeUID:memberUID) and reverse mapping (v2 UID -> projectSFID:committeeSFID:memberSFID).
	memberSFID := result.MemberID
	forwardMappingValue := committeeUID + ":" + memberUID
	if _, err := mappingsKV.Put(ctx, "committee_member.sfid."+memberSFID, []byte(forwardMappingValue)); err != nil {
		log.With(errKey, err, "member_sfid", memberSFID).
			WarnContext(ctx, "failed to store committee member forward mapping after v1 create")
	}
	reverseMappingValue := projectSFID + ":" + committeeSFID + ":" + memberSFID
	if _, err := mappingsKV.Put(ctx, "committee_member.uid."+memberUID, []byte(reverseMappingValue)); err != nil {
		log.With(errKey, err, "member_sfid", memberSFID).
			WarnContext(ctx, "failed to store committee member reverse mapping after v1 create")
	}

	log.With("member_sfid", memberSFID).InfoContext(ctx, "successfully created committee member in v1 from indexer event")
}

// syncCommitteeMemberUpdateToV1 patches a v1 committee member to match the v2 state.
func syncCommitteeMemberUpdateToV1(ctx context.Context, memberUID, projectSFID, committeeSFID, memberSFID string, data map[string]any) {
	log := logger.With("member_uid", memberUID, "project_sfid", projectSFID, "committee_sfid", committeeSFID, "member_sfid", memberSFID)

	payload := projectServiceCommitteeMemberUpdate{}
	if email, ok := data["email"].(string); ok {
		payload.Email = email
	}
	if jobTitle, ok := data["job_title"].(string); ok {
		payload.Title = jobTitle
	}
	if role, ok := data["role"].(string); ok {
		payload.Role = role
	}
	if status, ok := data["status"].(string); ok {
		payload.Status = status
	}
	if appointedBy, ok := data["appointed_by"].(string); ok {
		payload.AppointedBy = appointedBy
	}
	if agency, ok := data["agency"].(string); ok {
		payload.Agency = agency
	}
	if country, ok := data["country"].(string); ok {
		payload.Country = country
	}
	if voting, ok := data["voting"].(map[string]any); ok {
		if vs, ok := voting["status"].(string); ok {
			payload.VotingStatus = vs
		}
		if vsd, ok := voting["start_date"].(string); ok {
			payload.VotingStartDate = vsd
		}
		if ved, ok := voting["end_date"].(string); ok {
			payload.VotingEndDate = ved
		}
	}
	if orgID, err := resolveOrgIDFromEventData(ctx, data); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to resolve organization ID, proceeding without org")
	} else if orgID != "" {
		payload.OrganizationID = orgID
	}

	if err := updateV1CommitteeMember(ctx, projectSFID, committeeSFID, memberSFID, payload); err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to update committee member in v1")
		return
	}

	log.InfoContext(ctx, "successfully updated committee member in v1 from indexer event")
}

// syncCommitteeMemberDeleteToV1 deletes a v1 committee member that was deleted in v2.
// memberSFID is the contact SFID (v1 API "MemberID") used for the delete call itself.
// recordSFID is the platform-community__c record sfid that the forward mapping
// "committee_member.sfid.<sfid>" (handlers_committees.go) is keyed on for
// v1-originated members. It is tombstoned separately so a later v1-originated delete
// for the same record does not find a stale mapping and retry deleting the
// already-removed v2 member.
//
// recordSFID is empty both for members created via the v2->v1 create path
// (syncCommitteeMemberCreateToV1 — those key their forward mapping on the contact
// SFID itself, there being no distinct record sfid) and for v1-originated members
// whose record-sfid companion key is missing or unreadable. Since those two cases
// are indistinguishable from recordSFID alone, memberSFID is only used as the
// forward-tombstone key when the "committee_member.sfid.<memberSFID>" mapping
// actually exists and its value points back at this same memberUID — otherwise the
// forward tombstone is skipped rather than risk tombstoning an unrelated mapping
// (or a no-op) while the real v1-originated forward mapping stays live.
func syncCommitteeMemberDeleteToV1(ctx context.Context, memberUID, projectSFID, committeeSFID, memberSFID, recordSFID string) {
	log := logger.With("member_uid", memberUID, "project_sfid", projectSFID, "committee_sfid", committeeSFID, "member_sfid", memberSFID, "record_sfid", recordSFID)

	if err := deleteV1CommitteeMember(ctx, projectSFID, committeeSFID, memberSFID); err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to delete committee member in v1")
		return
	}

	forwardSFID := recordSFID
	if forwardSFID == "" {
		if entry, err := mappingsKV.Get(ctx, "committee_member.sfid."+memberSFID); err == nil && !isTombstonedMapping(entry.Value()) {
			if _, ownerUID, ok := splitTwoParts(string(entry.Value())); ok && ownerUID == memberUID {
				forwardSFID = memberSFID
			}
		}
	}
	if forwardSFID == "" {
		log.WarnContext(ctx, "cannot determine committee member forward mapping key, skipping forward tombstone")
	} else if err := tombstoneMapping(ctx, "committee_member.sfid."+forwardSFID); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to tombstone committee member forward mapping after v1 delete")
	}
	if recordSFID != "" {
		if err := tombstoneMapping(ctx, committeeMemberRecordSFIDKey(memberUID)); err != nil {
			log.With(errKey, err).WarnContext(ctx, "failed to tombstone committee member record sfid mapping after v1 delete")
		}
	}
	if err := tombstoneMapping(ctx, "committee_member.uid."+memberUID); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to tombstone committee member reverse mapping after v1 delete")
	}

	log.InfoContext(ctx, "successfully deleted committee member in v1 from indexer event")
}

// resolveOrgIDFromEventData extracts and resolves an organization SFID from committee member event data.
// Returns empty string (no error) if no organization data is present or fields are all empty.
// Only 15- or 18-char Salesforce ID-shaped organization.id values are sent to v1 as
// OrganizationID; any other organization.id value is ignored so sync can resolve from
// name/website or proceed without org.
func resolveOrgIDFromEventData(ctx context.Context, data map[string]any) (string, error) {
	org, ok := data["organization"].(map[string]any)
	if !ok {
		return "", nil
	}
	orgID, _ := org["id"].(string)
	orgName, _ := org["name"].(string)
	orgWebsite, _ := org["website"].(string)
	orgID = strings.TrimSpace(orgID)
	if orgID != "" && !sfid.IsValid(orgID) {
		logger.With("organization_id", orgID, "organization_name", orgName, "organization_website", orgWebsite).
			InfoContext(ctx, "ignoring non-SFID organization id on committee member, resolving from name/website")
		orgID = ""
	}
	if orgID != "" {
		return orgID, nil
	}
	return resolveV1OrgID(ctx, orgName, orgWebsite)
}

// projectIndexerEventHandler handles lfx.project.{created,updated,deleted} events
// published by the indexer service after successful OpenSearch writes.
//
// Mirrors committeeIndexerEventHandler for the project object type:
//   - created: if no reverse mapping exists yet, create the corresponding v1 project
//     record and persist project.sfid.<sfid> ↔ project.uid.<v2uid> mappings.
//   - updated: patch the mapped v1 project.
//   - deleted: delete the mapped v1 project and tombstone both mappings.
//
// The loop with the v1→v2 direction is broken on the v1 side by shouldSkipSync
// (handlers.go): when the WAL re-emits our own v1 write, lastmodifiedbyid matches
// our service's Auth0 client ID and handleProjectUpdate returns early. The create
// path additionally uses the presence of a non-tombstoned reverse mapping as a
// v1-origination signal to skip the write entirely.
//
// This handler covers the ProjectBase fields only. Settings-only fields
// (mission_statement, announcement_date, executive_director, program_manager,
// opportunity_owner) live on a separate lfx.project_settings.{created,updated}
// indexer subject and are intentionally out of scope for this change — the v1 side
// will show empty settings fields until a follow-up subscribes to that subject.
func projectIndexerEventHandler(msg *nats.Msg) {
	ctx := context.Background()

	var event indexingEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		logger.With(errKey, err, "subject", msg.Subject).ErrorContext(ctx, "failed to unmarshal project indexing event")
		return
	}

	logger.With(
		"subject", msg.Subject,
		"object_id", event.ObjectID,
		"action", event.Action,
	).InfoContext(ctx, "received project indexing event")

	var body indexingEventBody
	if len(event.Body) > 0 {
		if err := json.Unmarshal(event.Body, &body); err != nil {
			logger.With(errKey, err, "project_uid", event.ObjectID).
				ErrorContext(ctx, "failed to unmarshal project event body, skipping")
			return
		}
	}

	switch event.Action {
	case "created":
		syncProjectCreateToV1(ctx, event.ObjectID, body.Data)

	case "updated":
		projectSFID, err := lookupV1ProjectSFIDForEvent(ctx, event.ObjectID)
		if err != nil {
			// lookupV1ProjectSFIDForEvent already logged at the appropriate
			// severity (WARN for genuine miss / tombstoned, ERROR for transient
			// KV failures — those need ops attention because core NATS cannot
			// redeliver this event).
			return
		}
		logger.With("project_uid", event.ObjectID, "project_sfid", projectSFID).
			InfoContext(ctx, "project updated in v2 — syncing to v1")
		syncProjectUpdateToV1(ctx, event.ObjectID, projectSFID, body.Data)

	case "deleted":
		projectSFID, err := lookupV1ProjectSFIDForEvent(ctx, event.ObjectID)
		if err != nil {
			return
		}
		logger.With("project_uid", event.ObjectID, "project_sfid", projectSFID).
			InfoContext(ctx, "project deleted in v2 — syncing deletion to v1")
		syncProjectDeleteToV1(ctx, event.ObjectID, projectSFID)

	default:
		logger.With("action", event.Action, "subject", msg.Subject).
			WarnContext(ctx, "unknown action in project indexing event, skipping")
	}
}

// syncProjectCreateToV1 ensures a v2-created project exists in v1.
// If a non-tombstoned reverse mapping already exists the project originated in v1
// (or was already synced) — skip to avoid a duplicate v1 write / loop.
//
// The loop-guard read uses getMappingEntryWithRetry (handlers.go) so a
// concurrent v1→v2 handler that is about to write the mapping — see
// handlers_projects.go, which writes project.uid.<v2uid> only after the v2
// create returns — has a short window to complete before we treat the miss as
// final. Without the retry, races between the v2 project-service's synchronous
// indexer publish and the v1→v2 handler's mapping write can produce duplicate
// v1 records on the v1→v2→v1 round-trip.
//
// The read distinguishes four outcomes:
//   - mapping present and non-tombstoned → skip (v1-originated or already synced)
//   - mapping tombstoned → proceed with create (previously deleted)
//   - mapping absent (jetstream.ErrKeyNotFound after retries) → proceed with create
//   - transient KV failure → skip and log at ERROR level rather than risk
//     creating a duplicate v1 record when the mapping actually exists but is
//     temporarily unreadable. Core NATS has no NAK for this indexer subject,
//     so ops must replay the event manually if this happens.
func syncProjectCreateToV1(ctx context.Context, projectUID string, data map[string]any) {
	log := logger.With("project_uid", projectUID)

	reverseKey := "project.uid." + projectUID
	entry, err := getMappingEntryWithRetry(ctx, reverseKey)
	switch {
	case err == nil:
		if !isTombstonedMapping(entry.Value()) {
			log.DebugContext(ctx, "project originated from v1 or already synced — skipping reverse sync")
			return
		}
		// Tombstoned — proceed with create.
	case errors.Is(err, jetstream.ErrKeyNotFound):
		// Genuine miss even after bounded retry — proceed with create.
	default:
		log.With(errKey, err).
			ErrorContext(ctx, "transient KV failure reading project reverse mapping — skipping create to avoid duplicate v1 write; manual replay may be required")
		return
	}

	payload, err := mapV2DataToV1ProjectCreatePayload(ctx, data)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to map v2 project data to v1 create payload, skipping")
		return
	}

	log.With("payload_name", payload.Name, "payload_slug", payload.Slug, "payload_parent", payload.Parent).
		InfoContext(ctx, "creating project in v1")

	result, err := createV1Project(ctx, *payload)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to create project in v1")
		return
	}

	projectSFID := result.ID
	if projectSFID == "" {
		log.WarnContext(ctx, "v1 project create returned empty ID, not persisting mapping")
		return
	}

	// Store forward mapping (v1 SFID → v2 UID) and reverse mapping (v2 UID → v1 SFID).
	// Same key format as the v1→v2 direction (handlers_projects.go) so lookups
	// (including the lfx.lookup_v1_mapping NATS request/reply) resolve either
	// direction regardless of origin.
	//
	// Both writes use putMappingWithRetry (handlers.go): the v1 record already
	// exists, and losing either mapping is a durable inconsistency because core
	// NATS will not redeliver this create event.
	//
	// The reverse mapping is the more critical of the two: it doubles as the
	// create-path loop guard and drives the SFID lookup for subsequent
	// update/delete events. Losing it means a replay would create a duplicate v1
	// record and any future v2 update / delete would be silently dropped. Its
	// failure is escalated to ERROR with the SFID + UID so ops can reconcile
	// manually (write the mapping directly or delete the stray v1 record).
	if err := putMappingWithRetry(ctx, "project.sfid."+projectSFID, []byte(projectUID)); err != nil {
		log.With(errKey, err, "project_sfid", projectSFID).
			ErrorContext(ctx, "failed to store project forward mapping after v1 create — lookup_v1_mapping may return stale results until reconciled")
	}
	if err := putMappingWithRetry(ctx, reverseKey, []byte(projectSFID)); err != nil {
		log.With(errKey, err, "project_sfid", projectSFID).
			ErrorContext(ctx, "failed to store project reverse mapping after v1 create — v1 record is orphaned; future update/delete events will be dropped and a replay will duplicate; manual reconciliation required (write mapping or delete v1 record)")
	}

	log.With("project_sfid", projectSFID).InfoContext(ctx, "successfully created project in v1 from indexer event")
}

// syncProjectUpdateToV1 patches a v1 project to match the v2 state.
func syncProjectUpdateToV1(ctx context.Context, projectUID, projectSFID string, data map[string]any) {
	log := logger.With("project_uid", projectUID, "project_sfid", projectSFID)

	payload, err := mapV2DataToV1ProjectUpdatePayload(ctx, data)
	if err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to map v2 project data to v1 update payload, skipping")
		return
	}

	if err := updateV1Project(ctx, projectSFID, *payload); err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to update project in v1")
		return
	}

	log.InfoContext(ctx, "successfully updated project in v1 from indexer event")
}

// syncProjectDeleteToV1 deletes a v1 project that was deleted in v2.
func syncProjectDeleteToV1(ctx context.Context, projectUID, projectSFID string) {
	log := logger.With("project_uid", projectUID, "project_sfid", projectSFID)

	if err := deleteV1Project(ctx, projectSFID); err != nil {
		log.With(errKey, err).ErrorContext(ctx, "failed to delete project in v1")
		return
	}

	if err := tombstoneMapping(ctx, "project.sfid."+projectSFID); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to tombstone project forward mapping after v1 delete")
	}
	if err := tombstoneMapping(ctx, "project.uid."+projectUID); err != nil {
		log.With(errKey, err).WarnContext(ctx, "failed to tombstone project reverse mapping after v1 delete")
	}

	log.InfoContext(ctx, "successfully deleted project in v1 from indexer event")
}

// mapV2DataToV1ProjectCreatePayload converts a v2 project.created event body's
// data map into a v1 project-service create payload.
//
// The data map holds the JSON encoding of a lfx-v2-project-service ProjectBase
// (see internal/domain/models/project.go). Field names here match that JSON
// contract (snake_case). The mapping is deliberately the inverse of
// mapV1DataToProjectCreatePayload in handlers_projects.go.
//
// Parent resolution: v2 parent_uid → v1 Parent SFID via project.uid.<v2uid>
// mapping. If the parent UID is present but has no mapping, we return an error
// so the caller logs and skips (matches the committee sync behavior; there is
// no NAK on core NATS to retry).
func mapV2DataToV1ProjectCreatePayload(ctx context.Context, data map[string]any) (*projectServiceProjectCreate, error) {
	name, _ := data["name"].(string)
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("missing required v2 field: name")
	}

	payload := &projectServiceProjectCreate{
		Name: name,
		// ProjectType is required by v1; default to "Project". v2's is_foundation
		// flag can override for foundation-level records ("Project Group" is the
		// v1 value observed in SFDC for foundations).
		ProjectType: "Project",
	}
	if isFoundation, ok := data["is_foundation"].(bool); ok && isFoundation {
		payload.ProjectType = "Project Group"
	}

	if slug, ok := data["slug"].(string); ok && slug != "" {
		payload.Slug = slug
	}
	if desc, ok := data["description"].(string); ok && desc != "" {
		payload.Description = desc
	}
	if stage, ok := data["stage"].(string); ok && stage != "" {
		payload.Status = stage
	}
	if category, ok := data["category"].(string); ok && category != "" {
		payload.Category = category
	}
	if legalEntityType, ok := data["legal_entity_type"].(string); ok && legalEntityType != "" {
		payload.EntityType = legalEntityType
	}
	if legalEntityName, ok := data["legal_entity_name"].(string); ok && legalEntityName != "" {
		payload.EntityName = legalEntityName
	}
	if funding, ok := data["funding"].(string); ok && funding != "" {
		payload.Funding = funding
	}
	if fundingModels, ok := extractStringSlice(data["funding_model"]); ok {
		payload.Model = fundingModels
	}
	if charterURL, ok := data["charter_url"].(string); ok && charterURL != "" {
		payload.CharterURL = charterURL
	}
	if autojoin, ok := data["autojoin_enabled"].(bool); ok {
		val := autojoin
		payload.AutoJoinEnabled = &val
	}
	if formationDate, ok := data["formation_date"].(string); ok && formationDate != "" {
		payload.StartDate = extractDateOnly(formationDate)
	}
	if logoURL, ok := data["logo_url"].(string); ok && logoURL != "" {
		payload.ProjectLogo = logoURL
	}
	if repoURL, ok := data["repository_url"].(string); ok && repoURL != "" {
		payload.RepositoryURL = repoURL
	}
	if websiteURL, ok := data["website_url"].(string); ok && websiteURL != "" {
		payload.Website = websiteURL
	}
	if dissolutionDate, ok := data["entity_dissolution_date"].(string); ok && dissolutionDate != "" {
		payload.ProjectEntityDissolutionDate = extractDateOnly(dissolutionDate)
	}
	if formationDocURL, ok := data["entity_formation_document_url"].(string); ok && formationDocURL != "" {
		payload.ProjectEntityFormationDocument = formationDocURL
	}

	// Parent SFID resolution: v2 parent_uid → v1 Parent (project SFID).
	if parentUID, ok := data["parent_uid"].(string); ok && strings.TrimSpace(parentUID) != "" {
		parentUID = strings.TrimSpace(parentUID)
		parentSFID, err := resolveV1ProjectSFIDFromUID(ctx, parentUID)
		if err != nil {
			return nil, fmt.Errorf("could not resolve v1 project SFID for parent_uid %s: %w", parentUID, err)
		}
		payload.Parent = parentSFID
	}

	// Legal parent SFID resolution: v2 legal_parent_uid → v1 LegalParentID.
	if legalParentUID, ok := data["legal_parent_uid"].(string); ok && strings.TrimSpace(legalParentUID) != "" {
		legalParentUID = strings.TrimSpace(legalParentUID)
		legalParentSFID, err := resolveV1ProjectSFIDFromUID(ctx, legalParentUID)
		if err != nil {
			return nil, fmt.Errorf("could not resolve v1 project SFID for legal_parent_uid %s: %w", legalParentUID, err)
		}
		payload.LegalParentID = legalParentSFID
	}

	return payload, nil
}

// mapV2DataToV1ProjectUpdatePayload converts a v2 project.updated event body's
// data map into a v1 project-service update payload. Only fields present in the
// event body are set on the payload; omitempty on the struct fields keeps unset
// fields out of the PATCH request so v1 defaults / existing values are preserved.
func mapV2DataToV1ProjectUpdatePayload(ctx context.Context, data map[string]any) (*projectServiceProjectUpdate, error) {
	payload := &projectServiceProjectUpdate{}

	if name, ok := data["name"].(string); ok && name != "" {
		payload.Name = name
	}
	// is_foundation may switch on/off across updates (rare but possible when a
	// project graduates or is reclassified); mirror the create-path mapping so
	// v1 ProjectType tracks the v2 flag rather than silently drifting.
	if isFoundation, ok := data["is_foundation"].(bool); ok {
		if isFoundation {
			payload.ProjectType = "Project Group"
		} else {
			payload.ProjectType = "Project"
		}
	}
	if slug, ok := data["slug"].(string); ok && slug != "" {
		payload.Slug = slug
	}
	if desc, ok := data["description"].(string); ok && desc != "" {
		payload.Description = desc
	}
	if stage, ok := data["stage"].(string); ok && stage != "" {
		payload.Status = stage
	}
	if category, ok := data["category"].(string); ok && category != "" {
		payload.Category = category
	}
	if legalEntityType, ok := data["legal_entity_type"].(string); ok && legalEntityType != "" {
		payload.EntityType = legalEntityType
	}
	if legalEntityName, ok := data["legal_entity_name"].(string); ok && legalEntityName != "" {
		payload.EntityName = legalEntityName
	}
	if funding, ok := data["funding"].(string); ok && funding != "" {
		payload.Funding = funding
	}
	if fundingModels, ok := extractStringSlice(data["funding_model"]); ok {
		payload.Model = fundingModels
	}
	if charterURL, ok := data["charter_url"].(string); ok && charterURL != "" {
		payload.CharterURL = charterURL
	}
	if autojoin, ok := data["autojoin_enabled"].(bool); ok {
		val := autojoin
		payload.AutoJoinEnabled = &val
	}
	if formationDate, ok := data["formation_date"].(string); ok && formationDate != "" {
		payload.StartDate = extractDateOnly(formationDate)
	}
	if logoURL, ok := data["logo_url"].(string); ok && logoURL != "" {
		payload.ProjectLogo = logoURL
	}
	if repoURL, ok := data["repository_url"].(string); ok && repoURL != "" {
		payload.RepositoryURL = repoURL
	}
	if websiteURL, ok := data["website_url"].(string); ok && websiteURL != "" {
		payload.Website = websiteURL
	}
	if dissolutionDate, ok := data["entity_dissolution_date"].(string); ok && dissolutionDate != "" {
		payload.ProjectEntityDissolutionDate = extractDateOnly(dissolutionDate)
	}
	if formationDocURL, ok := data["entity_formation_document_url"].(string); ok && formationDocURL != "" {
		payload.ProjectEntityFormationDocument = formationDocURL
	}

	if parentUID, ok := data["parent_uid"].(string); ok && strings.TrimSpace(parentUID) != "" {
		parentUID = strings.TrimSpace(parentUID)
		parentSFID, err := resolveV1ProjectSFIDFromUID(ctx, parentUID)
		if err != nil {
			return nil, fmt.Errorf("could not resolve v1 project SFID for parent_uid %s: %w", parentUID, err)
		}
		payload.Parent = parentSFID
	}

	if legalParentUID, ok := data["legal_parent_uid"].(string); ok && strings.TrimSpace(legalParentUID) != "" {
		legalParentUID = strings.TrimSpace(legalParentUID)
		legalParentSFID, err := resolveV1ProjectSFIDFromUID(ctx, legalParentUID)
		if err != nil {
			return nil, fmt.Errorf("could not resolve v1 project SFID for legal_parent_uid %s: %w", legalParentUID, err)
		}
		payload.LegalParentID = legalParentSFID
	}

	return payload, nil
}

// resolveV1ProjectSFIDFromUID looks up a v2 project UID in mappingsKV and returns
// its v1 SFID. Returns an error when the key is missing or tombstoned. The
// underlying KV error is wrapped with %w so callers can use errors.Is against
// jetstream.ErrKeyNotFound to distinguish genuine misses from transient failures.
//
// The Get is done through getMappingEntryWithRetry (handlers.go) so the same
// bounded backoff protects parent / legal-parent SFID lookups here as
// lookupV1ProjectSFIDForEvent uses for the event's own SFID — a parent project
// whose mapping was written by handlers_projects.go moments before the child's
// indexer event arrives has a short window to become visible before the child
// sync is treated as un-lookable.
func resolveV1ProjectSFIDFromUID(ctx context.Context, projectUID string) (string, error) {
	entry, err := getMappingEntryWithRetry(ctx, "project.uid."+projectUID)
	if err != nil {
		return "", fmt.Errorf("mapping lookup failed: %w", err)
	}
	if isTombstonedMapping(entry.Value()) {
		return "", fmt.Errorf("mapping is tombstoned")
	}
	sfid := strings.TrimSpace(string(entry.Value()))
	if sfid == "" {
		return "", fmt.Errorf("mapping value is empty")
	}
	return sfid, nil
}

// lookupV1ProjectSFIDForEvent resolves the v1 project SFID for a v2 project UID
// carried by an indexer update or delete event. Returns a non-nil error whenever
// the caller must skip the event; the function itself logs at the appropriate
// severity so the caller does not have to interpret the outcome:
//
//   - genuine miss (mapping never existed or has been tombstoned) → WARN log,
//     nil sfid, non-nil err. Expected when the v2 project was never round-tripped
//     to v1 (e.g., a v2-native creation whose earlier create event was itself
//     dropped) or when the project has been deleted end-to-end.
//   - transient KV failure → ERROR log, nil sfid, non-nil err. Ops must notice
//     because core NATS carries these indexer subjects and cannot NAK/redeliver.
//   - happy path → nil err, the resolved SFID.
//
// The Get is done through getMappingEntryWithRetry (handlers.go) so that
// dispatch-order races across the lfx.project.{created,updated,deleted}
// subscriptions are absorbed: a fast update or delete goroutine can land here
// before the create goroutine's mapping write completes, and the bounded retry
// gives that write a short window to appear before the event is treated as
// un-lookable.
func lookupV1ProjectSFIDForEvent(ctx context.Context, projectUID string) (string, error) {
	entry, err := getMappingEntryWithRetry(ctx, "project.uid."+projectUID)
	switch {
	case err == nil:
		if isTombstonedMapping(entry.Value()) {
			logger.With("project_uid", projectUID).
				WarnContext(ctx, "v1 project SFID mapping is tombstoned, skipping event")
			return "", fmt.Errorf("mapping tombstoned for %s", projectUID)
		}
		sfid := strings.TrimSpace(string(entry.Value()))
		if sfid == "" {
			logger.With("project_uid", projectUID).
				WarnContext(ctx, "v1 project SFID mapping is empty, skipping event")
			return "", fmt.Errorf("mapping empty for %s", projectUID)
		}
		return sfid, nil
	case errors.Is(err, jetstream.ErrKeyNotFound):
		logger.With("project_uid", projectUID).
			WarnContext(ctx, "no v1 project SFID mapping found for event even after bounded retry, skipping")
		return "", err
	default:
		logger.With(errKey, err, "project_uid", projectUID).
			ErrorContext(ctx, "transient KV failure reading project SFID mapping — event dropped; manual replay may be required")
		return "", err
	}
}

// extractStringSlice normalizes a JSON-decoded value that may be represented as
// either []any (from encoding/json) or []string (from a strongly typed source)
// into []string. Returns ok=false when v is nil or of an unsupported shape.
func extractStringSlice(v any) ([]string, bool) {
	switch typed := v.(type) {
	case nil:
		return nil, false
	case []string:
		if len(typed) == 0 {
			return nil, false
		}
		return typed, true
	case []any:
		if len(typed) == 0 {
			return nil, false
		}
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	default:
		return nil, false
	}
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

// parseCommitteeMemberReverseMapping parses a "committee_member.uid.<v2-member-uid>"
// reverse-mapping value into its constituent SFIDs.
//
// The value is always three colon-separated fields — projectSFID:committeeSFID:X —
// deliberately kept at the same field count as before LFXV2-2673 so a rolling deploy
// never has an old pod (still running this exact splitThreeParts-based parser) misparse
// a value written by a new pod, or vice versa. Only the meaning of X has changed over
// time:
//   - projectSFID:committeeSFID:recordSFID  (pre-fix; X is the platform-community__c
//     record sfid, a UUID; this is the poisoned form that causes v1 deletes to 404)
//   - projectSFID:committeeSFID:contactSFID (post-fix; X is contact_name__c, the v1 API
//     "MemberID" that DELETE/PATCH .../committees/{c}/members/{MemberID} matches on)
//
// The two forms are disambiguated by isUUID(X): a UUID third field is treated as
// recordSFID (contactSFID unknown); anything else must be a valid contact SFID (checked
// with sfid.IsValid) or the value is rejected as malformed. recordSFID needed for the
// forward-mapping tombstone on delete is instead looked up from the separate
// "committee_member.record_sfid.<uid>" key (see resolveCommitteeMemberRecordSFID) — kept
// out of this value entirely for the same rollout-compatibility reason.
//
// recordSFID and/or contactSFID may come back empty when unknown; ok is false when the
// value cannot be parsed into exactly three colon-separated fields, or when the third
// field is neither a UUID nor a valid SFID — e.g. a legacy 4-field
// "recordSFID:contactSFID" value whose extra colon splitThreeParts folds into the third
// field, which must not be misclassified as a usable contact SFID.
func parseCommitteeMemberReverseMapping(s string) (projectSFID, committeeSFID, recordSFID, contactSFID string, ok bool) {
	projectSFID, committeeSFID, third, ok := splitThreeParts(s)
	if !ok {
		return "", "", "", "", false
	}
	if isUUID(third) {
		return projectSFID, committeeSFID, third, "", true
	}
	if !sfid.IsValid(third) {
		return "", "", "", "", false
	}
	return projectSFID, committeeSFID, "", third, true
}

// committeeMemberRecordSFIDKey returns the mapping key that stores the
// platform-community__c record sfid for a v2 committee member UID. Kept as a
// separate key (rather than a fourth field on the reverse mapping) so the reverse
// mapping's wire format never changes field count — see parseCommitteeMemberReverseMapping.
func committeeMemberRecordSFIDKey(memberUID string) string {
	return "committee_member.record_sfid." + memberUID
}

// resolveCommitteeMemberRecordSFID looks up the platform-community__c record sfid for
// a v2 committee member UID from its auxiliary mapping key. Returns "" if the key is
// absent, tombstoned, or unreadable — all treated as "unknown", since this value is
// only used to tombstone the forward mapping as a best effort on delete.
func resolveCommitteeMemberRecordSFID(ctx context.Context, memberUID string) string {
	entry, err := mappingsKV.Get(ctx, committeeMemberRecordSFIDKey(memberUID))
	if err != nil || isTombstonedMapping(entry.Value()) {
		return ""
	}
	return string(entry.Value())
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
		"Marketing Oversight Committee/Marketing Advisory Committee", "Newsletter", "Other",
		"Product Security", "Special Interest Group", "Technical Mailing List",
		"Technical Steering Committee", "Working Group":
		return category
	default:
		return "Other"
	}
}
