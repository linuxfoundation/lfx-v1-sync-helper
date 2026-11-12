// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"encoding/json"
	"fmt"
)

func memberUpdateFingerprint(payload projectServiceCommitteeMemberUpdate) string {
	data, _ := json.Marshal(payload) // Only JSON-safe scalar fields.
	return committeeCreateHash(data)
}

func verifyRecordedMappingOwners(ctx context.Context, pairs []createMappingPair) error {
	for _, pair := range pairs {
		entry, err := mappingStore.Get(ctx, pair.key)
		if err != nil {
			return err
		}
		if string(entry.Value) != pair.value {
			return fmt.Errorf("%w: recorded member mapping ownership changed", errCommitteeCreateManual)
		}
	}
	return nil
}

func recordedMemberDeletion(ctx context.Context, uid string, result *memberCreateResult) (bool, error) {
	for kind, id := range map[string]string{"committee": result.CommitteeUID, "committee_member": uid} {
		deleted, err := hasCreateDeletion(ctx, kind, id)
		if err != nil || deleted {
			return deleted, err
		}
	}
	return false, nil
}

func verifyMemberRecoveryTarget(ctx context.Context, result *memberCreateResult) error {
	owned, err := verifyMemberCleanupTarget(ctx, result)
	if err != nil {
		return err
	}
	if !owned {
		return fmt.Errorf("%w: original membership is absent, deleted or replaced; refusing recovery PATCH", errCommitteeCreateManual)
	}
	return nil
}

// Mapping recovery must account for updates ACKed while the reverse key was
// missing. Re-read after claims and PATCH current fields, never another POST.
// Replays always PATCH: no cached "last applied" hash can certify v1 after a
// lost response or competing PATCH. The original hash only avoids an unnecessary
// PATCH on an unchanged, first successful create.
func reconcileRecordedMemberCreate(ctx context.Context, uid string, result *memberCreateResult, pairs []createMappingPair, replay bool) error {
	current, err := readMemberCreateData(ctx, result.CommitteeUID, uid)
	if err != nil {
		return err
	}
	if current == nil {
		return cleanupRecordedMemberCreate(ctx, uid, result)
	}
	payload, err := mapV2MemberUpdatePayload(ctx, current)
	if err != nil {
		return err
	}
	fingerprint := memberUpdateFingerprint(payload)
	if err := verifyRecordedMappingOwners(ctx, pairs); err != nil {
		return err
	}
	if deleted, err := recordedMemberDeletion(ctx, uid, result); err != nil {
		return err
	} else if deleted {
		return cleanupRecordedMemberCreate(ctx, uid, result)
	}
	if replay || result.Fingerprint != fingerprint {
		// Mapping/WAL lag cannot prove that the contact still addresses the
		// original join record. Unlike cleanup, absence is not PATCH authority.
		if err := verifyMemberRecoveryTarget(ctx, result); err != nil {
			return err
		}
		// Do not consume a v1-originated update marker here: this is mandatory
		// current-state reconciliation, not an indexer echo of an event body.
		if err := updateV1CommitteeMember(ctx, result.ProjectSFID, result.CommitteeSFID, result.MemberID, payload); err != nil {
			return err
		}
	}
	if deleted, err := recordedMemberDeletion(ctx, uid, result); err != nil {
		return err
	} else if deleted {
		return cleanupRecordedMemberCreate(ctx, uid, result)
	}
	if err := verifyRecordedMappingOwners(ctx, pairs); err != nil {
		return err
	}
	if replay || result.Fingerprint != fingerprint {
		if err := verifyMemberRecoveryTarget(ctx, result); err != nil {
			return err
		}
	}
	latest, err := readMemberCreateData(ctx, result.CommitteeUID, uid)
	if err != nil {
		return err
	}
	if latest == nil {
		return cleanupRecordedMemberCreate(ctx, uid, result)
	}
	latestPayload, err := mapV2MemberUpdatePayload(ctx, latest)
	if err != nil {
		return err
	}
	if memberUpdateFingerprint(latestPayload) != fingerprint {
		return fmt.Errorf("%w: member changed during create reconciliation", errCommitteeCreatePending)
	}
	return nil
}
