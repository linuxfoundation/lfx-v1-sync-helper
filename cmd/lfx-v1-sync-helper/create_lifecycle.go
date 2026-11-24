// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

var (
	fetchMemberForCreate   = fetchCommitteeMember
	readMemberCreateData   = currentMemberCreateData
	readMemberCleanupState = dbReadMemberCleanupState
)

// Deletion intent is insert-only and never expires. It is separate from the
// reverse mapping: a delete can arrive before a POST has returned its v1 ID.
func createDeletionKey(kind, uid string) string { return "create_deleted." + kind + "." + uid }

func recordCreateDeletion(ctx context.Context, kind, uid string) error {
	_, err := mappingStore.Create(ctx, createDeletionKey(kind, uid), []byte("1"))
	if errors.Is(err, ErrKeyExists) {
		return nil
	}
	return err
}

func hasCreateDeletion(ctx context.Context, kind, uid string) (bool, error) {
	_, err := mappingStore.Get(ctx, createDeletionKey(kind, uid))
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Claim absent keys before cleanup, and refuse to delete a result mapped to
// another owner. Existing tombstones are retained, never converted back to live.
type createMappingPair struct{ key, value string }

func claimCreateCleanup(ctx context.Context, pairs []createMappingPair) error {
	for _, pair := range pairs {
		entry, err := mappingStore.Get(ctx, pair.key)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err == nil && string(entry.Value) != pair.value && !isTombstonedMapping(entry.Value) {
			return fmt.Errorf("%w: recorded cleanup mapping conflicts", errCommitteeCreateManual)
		}
	}
	for _, pair := range pairs {
		_, err := mappingStore.Create(ctx, pair.key, []byte(pair.value))
		if errors.Is(err, ErrKeyExists) {
			entry, readErr := mappingStore.Get(ctx, pair.key)
			if readErr != nil {
				return readErr
			}
			if string(entry.Value) != pair.value && !isTombstonedMapping(entry.Value) {
				return fmt.Errorf("%w: recorded cleanup mapping conflicts", errCommitteeCreateManual)
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

func verifyCreateCleanup(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		entry, err := mappingStore.Get(ctx, key)
		if err != nil {
			return err
		}
		if !isTombstonedMapping(entry.Value) {
			return errors.New("recorded create cleanup mappings not yet tombstoned")
		}
	}
	return nil
}

func cleanupRecordedCommitteeCreate(ctx context.Context, uid string) error {
	attempt, _, err := readCommitteeCreateAttempt(ctx, uid)
	if errors.Is(err, ErrKeyNotFound) {
		return nil // No POST owner; the retained fence blocks any future create.
	}
	if err != nil {
		return err
	}
	if attempt.V1ID == "" {
		// No payload/name is retained, so a deleted V2 object cannot authorize
		// same-name adoption. A late successful POST can still record its ID.
		return fmt.Errorf("%w: deleted committee has an unresolved v1 create outcome", errCommitteeCreatePending)
	}
	forward, reverse := "committee.sfid."+attempt.V1ID, "committee.uid."+uid
	pairs := []createMappingPair{
		{forward, uid}, {reverse, attempt.ProjectSFID + ":" + attempt.V1ID},
	}
	if err := claimCreateCleanup(ctx, pairs); err != nil {
		return err
	}
	writePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceCommittee, attempt.V1ID)
	if err := deleteV1Committee(ctx, attempt.ProjectSFID, attempt.V1ID); err != nil {
		deletePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceCommittee, attempt.V1ID)
		return err
	}
	// The generic delete helper retargets tombstones to fresh revisions without
	// checking ownership. Recorded cleanup must preserve an intervening owner.
	if err := tombstoneRecordedMappings(ctx, pairs); err != nil {
		return err
	}
	return verifyCreateCleanup(ctx, forward, reverse)
}

// Only IDs are retained, not profile fields. An insert-only pre-POST record also
// prevents concurrent deliveries or an ambiguous member response repeating POST.
// Unknown outcomes require manual reconciliation; no member adoption is attempted.
type memberCreateResult struct {
	Version       int    `json:"version"`
	ProjectSFID   string `json:"project_sfid"`
	CommitteeSFID string `json:"committee_sfid"`
	CommitteeUID  string `json:"committee_uid"`
	MemberID      string `json:"member_id,omitempty"`
	RecordID      string `json:"record_id,omitempty"`
	Fingerprint   string `json:"fingerprint,omitempty"`
}

func memberCreateResultKey(uid string) string { return "create_attempt.committee_member." + uid }

func readMemberCreateResult(ctx context.Context, uid string) (*memberCreateResult, uint64, error) {
	entry, err := mappingStore.Get(ctx, memberCreateResultKey(uid))
	if err != nil {
		return nil, 0, err
	}
	var result memberCreateResult
	if json.Unmarshal(entry.Value, &result) != nil || result.Version != 1 || result.ProjectSFID == "" || result.CommitteeSFID == "" || result.CommitteeUID == "" {
		return nil, 0, fmt.Errorf("%w: invalid member create record", errCommitteeCreateManual)
	}
	return &result, entry.Revision, nil
}

func cleanupRecordedMemberCreate(ctx context.Context, uid string, result *memberCreateResult) error {
	if result.MemberID == "" {
		return fmt.Errorf("%w: deleted member has an unresolved v1 create outcome", errCommitteeCreatePending)
	}
	forwardID := result.RecordID
	if forwardID == "" {
		forwardID = result.MemberID
	}
	forward, reverse := "committee_member.sfid."+forwardID, "committee_member.uid."+uid
	pairs := []createMappingPair{
		{forward, result.CommitteeUID + ":" + uid},
		{reverse, result.ProjectSFID + ":" + result.CommitteeSFID + ":" + result.MemberID},
	}
	if result.RecordID != "" {
		pairs = append(pairs, createMappingPair{committeeMemberRecordSFIDKey(uid), result.RecordID})
	}
	keys := []string{forward, reverse}
	if result.RecordID != "" {
		keys = append(keys, committeeMemberRecordSFIDKey(uid))
	}
	return finishRecordedMemberCleanup(ctx, uid, result, pairs, keys)
}

func currentMemberCreateData(ctx context.Context, committeeUID, uid string) (map[string]any, error) {
	member, _, err := fetchMemberForCreate(ctx, committeeUID, uid)
	if err != nil {
		var missing *committeeservice.NotFoundError
		if errors.As(err, &missing) {
			return nil, recordCreateDeletion(ctx, "committee_member", uid)
		}
		return nil, err
	}
	if member == nil || member.UID == nil || *member.UID != uid || member.CommitteeUID == nil || *member.CommitteeUID != committeeUID {
		return nil, fmt.Errorf("%w: current member identity differs", errCommitteeCreateManual)
	}
	// Goa service types have no JSON tags; marshal/unmarshal would produce
	// CamelCase fields, not the snake_case indexer shape used by the mapper.
	text := func(value *string) string {
		if value == nil {
			return ""
		}
		return *value
	}
	data := map[string]any{
		"email": text(member.Email), "first_name": text(member.FirstName),
		"last_name": text(member.LastName), "job_title": text(member.JobTitle),
		"status": member.Status, "appointed_by": member.AppointedBy, "voting": nil,
		"role": map[string]any{"name": "", "start_date": "", "end_date": ""},
	}
	if member.Role != nil {
		data["role"] = map[string]any{"name": member.Role.Name, "start_date": text(member.Role.StartDate), "end_date": text(member.Role.EndDate)}
	}
	if member.Voting != nil {
		data["voting"] = map[string]any{"status": member.Voting.Status, "start_date": text(member.Voting.StartDate), "end_date": text(member.Voting.EndDate)}
	}
	if member.Organization != nil {
		data["organization"] = map[string]any{"id": text(member.Organization.ID), "name": text(member.Organization.Name), "website": text(member.Organization.Website)}
	}
	return data, nil
}
