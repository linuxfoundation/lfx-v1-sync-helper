// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Unlike committee DELETE, member DELETE addresses a reusable contact ID. Retain
// dispatch and confirmed-deletion phases BEFORE tombstoning mappings, so replay
// cannot delete a replacement membership after a lost response or partial write.
type memberCleanupAttempt struct {
	Version     int    `json:"version"`
	Fingerprint string `json:"fingerprint"`
	Phase       string `json:"phase"`
}

func memberCleanupKey(uid string) string { return "cleanup_attempt.committee_member." + uid }

func readMemberCleanupAttempt(ctx context.Context, uid, fingerprint string) (*memberCleanupAttempt, uint64, error) {
	entry, err := mappingStore.Get(ctx, memberCleanupKey(uid))
	if err != nil {
		return nil, 0, err
	}
	var attempt memberCleanupAttempt
	if json.Unmarshal(entry.Value, &attempt) != nil || attempt.Version != 1 || attempt.Fingerprint != fingerprint ||
		(attempt.Phase != "dispatched" && attempt.Phase != "deleted" && attempt.Phase != "complete") {
		return nil, 0, fmt.Errorf("%w: invalid member cleanup attempt", errCommitteeCreateManual)
	}
	return &attempt, entry.Revision, nil
}

func saveMemberCleanupPhase(ctx context.Context, uid string, attempt *memberCleanupAttempt, revision uint64, phase string) (uint64, error) {
	attempt.Phase = phase
	data, err := json.Marshal(attempt)
	if err != nil {
		return 0, err
	}
	return mappingStore.Update(ctx, memberCleanupKey(uid), data, revision)
}

func inspectMemberCleanupMappings(ctx context.Context, pairs []createMappingPair) (int, error) {
	tombstones := 0
	for _, pair := range pairs {
		entry, err := mappingStore.Get(ctx, pair.key)
		if errors.Is(err, ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if isTombstonedMapping(entry.Value) {
			tombstones++
			continue
		}
		if string(entry.Value) != pair.value {
			return 0, fmt.Errorf("%w: recorded member cleanup mapping conflicts", errCommitteeCreateManual)
		}
	}
	return tombstones, nil
}

// CAS only the recorded owner's keys; never call the contact-addressed DELETE
// again just because a tombstone failed to persist. A concurrent change retries
// or fails closed without overwriting another owner.
func tombstoneRecordedMappings(ctx context.Context, pairs []createMappingPair) error {
	for _, pair := range pairs {
		entry, err := mappingStore.Get(ctx, pair.key)
		if errors.Is(err, ErrKeyNotFound) {
			_, err = mappingStore.Create(ctx, pair.key, []byte(tombstoneMarker))
			if err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if isTombstonedMapping(entry.Value) {
			continue
		}
		if string(entry.Value) != pair.value {
			return fmt.Errorf("%w: recorded cleanup ownership changed", errCommitteeCreateManual)
		}
		if _, err := mappingStore.Update(ctx, pair.key, []byte(tombstoneMarker), entry.Revision); err != nil {
			return err
		}
	}
	return nil
}

func finishRecordedMemberCleanup(ctx context.Context, uid string, result *memberCreateResult, pairs []createMappingPair, keys []string) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fingerprint := committeeCreateHash(encoded)
	attempt, revision, err := readMemberCleanupAttempt(ctx, uid, fingerprint)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if attempt != nil && attempt.Phase == "complete" {
		return nil
	}
	tombstones, err := inspectMemberCleanupMappings(ctx, pairs)
	if err != nil {
		return err
	}
	if tombstones == len(pairs) {
		return nil // Includes completed pre-upgrade cleanup.
	}
	if attempt != nil && attempt.Phase == "dispatched" {
		// The original request may still be in flight, or its response/phase
		// write was lost. Repeating DELETE could remove a re-added contact.
		return fmt.Errorf("%w: member DELETE outcome unresolved; do not repeat; manual reconciliation required", errCommitteeCreatePending)
	}
	if attempt == nil {
		if tombstones != 0 {
			return fmt.Errorf("%w: legacy partial member cleanup has no confirmed DELETE result", errCommitteeCreateManual)
		}
		shouldDelete, err := verifyMemberCleanupTarget(ctx, result)
		if err != nil {
			return err
		}
		phase := "deleted" // Recorded membership is already absent/replaced.
		if shouldDelete {
			phase = "dispatched"
		}
		attempt = &memberCleanupAttempt{Version: 1, Fingerprint: fingerprint, Phase: phase}
		data, err := json.Marshal(attempt)
		if err != nil {
			return err
		}
		revision, err = mappingStore.Create(ctx, memberCleanupKey(uid), data)
		if errors.Is(err, ErrKeyExists) {
			return errCommitteeCreatePending
		}
		if err != nil {
			return err // No DELETE without a durable dispatch fence.
		}
		if shouldDelete {
			writePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceCommitteeMember, result.RecordID)
			if err := deleteV1CommitteeMember(ctx, result.ProjectSFID, result.CommitteeSFID, result.MemberID); err != nil {
				deletePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceCommitteeMember, result.RecordID)
				logger.With(errKey, err, "member_uid", uid, "record_sfid", result.RecordID).
					ErrorContext(ctx, "member DELETE outcome unresolved; dispatch fence prevents another DELETE; manual reconciliation may be required")
				return fmt.Errorf("%w: member DELETE result not confirmed; retained dispatch fence", errCommitteeCreatePending)
			}
			revision, err = saveMemberCleanupPhase(ctx, uid, attempt, revision, "deleted")
			if err != nil {
				return err // Unknown persistence is not permission to DELETE again.
			}
		}
	}
	if err := tombstoneRecordedMappings(ctx, pairs); err != nil {
		return err
	}
	if err := verifyCreateCleanup(ctx, keys...); err != nil {
		return err
	}
	_, err = saveMemberCleanupPhase(ctx, uid, attempt, revision, "complete")
	return err
}
