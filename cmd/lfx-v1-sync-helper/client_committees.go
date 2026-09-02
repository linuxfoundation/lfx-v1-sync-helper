// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// fetchCommitteeBase fetches an existing committee base from the Committee Service API.
func fetchCommitteeBase(ctx context.Context, committeeUID string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return nil, "", err
	}

	result, err := committeeClient.GetCommitteeBase(ctx, &committeeservice.GetCommitteeBasePayload{
		BearerToken: &token,
		UID:         &committeeUID,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch committee base: %w", err)
	}

	etag := ""
	if result.Etag != nil {
		etag = *result.Etag
	}

	return result.CommitteeBase, etag, nil
}

// createCommittee creates a new committee via the Committee Service API.
func createCommittee(ctx context.Context, payload *committeeservice.CreateCommitteePayload, v1Principal string) (*committeeservice.CommitteeFullWithReadonlyAttributes, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return nil, err
	}

	payload.BearerToken = &token

	result, err := committeeClient.CreateCommittee(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create committee: %w", err)
	}

	return result, nil
}

// updateCommittee syncs a V1 committee record to V2 by fetching the current base,
// merging V1 fields on top (V1 wins where present; current values are preserved for
// absent V1 keys), and calling UpdateCommitteeBase only when a synced field differs.
//
// The mapper is called with currentBase as the default seed so that absent V1 keys
// do not clobber existing V2 values with the Go zero value for plain-bool payload
// fields (EnableVoting, SsoGroupEnabled, Public).
func updateCommittee(ctx context.Context, committeeUID string, v1Data map[string]any, v1Principal string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, error) {
	// Fetch current committee base + ETag.
	currentBase, baseETag, err := fetchCommitteeBase(ctx, committeeUID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current committee base: %w", err)
	}

	// Build a fully-merged update payload: start from currentBase, overlay V1 fields.
	payload, err := mapV1DataToCommitteeUpdateBasePayload(ctx, committeeUID, v1Data, currentBase)
	if err != nil {
		return nil, err
	}

	// Snapshot the merged payload as a base struct for change detection.
	updatedBase := committeeBaseFromUpdatePayload(currentBase.UID, payload)

	// Skip the call if no synced field has changed.
	if committeeBasesEqual(currentBase, updatedBase) {
		return nil, nil
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return nil, fmt.Errorf("failed to generate token for committee base update: %w", err)
	}

	payload.BearerToken = &token
	payload.IfMatch = stringToStringPtr(baseETag)

	result, err := committeeClient.UpdateCommitteeBase(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to update committee base: %w", err)
	}

	return result, nil
}

// committeeBaseFromUpdatePayload projects an UpdateCommitteeBasePayload into a
// CommitteeBaseWithReadonlyAttributes for change detection against the current
// server-side base.
func committeeBaseFromUpdatePayload(uid *string, payload *committeeservice.UpdateCommitteeBasePayload) *committeeservice.CommitteeBaseWithReadonlyAttributes {
	return &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             uid,
		Name:            stringToStringPtr(payload.Name),
		ProjectUID:      stringToStringPtr(payload.ProjectUID),
		Category:        stringToStringPtr(payload.Category),
		Description:     payload.Description,
		Website:         payload.Website,
		MailingList:     payload.MailingList,
		ChatChannel:     payload.ChatChannel,
		DisplayName:     payload.DisplayName,
		EnableVoting:    payload.EnableVoting,
		SsoGroupEnabled: payload.SsoGroupEnabled,
		Public:          payload.Public,
	}
}

// committeeBasesEqual compares two CommitteeBaseWithReadonlyAttributes objects across
// every field the V1→V2 sync writes. Fields not managed by the sync (e.g. RequiresReview,
// ParentUID, JoinMode, Repository, Scope, Deliverables, KeyDates, Calendar) are ignored
// so that admin edits made in V2 do not force redundant updates.
func committeeBasesEqual(a, b *committeeservice.CommitteeBaseWithReadonlyAttributes) bool {
	return stringPtrToString(a.Name) == stringPtrToString(b.Name) &&
		stringPtrToString(a.ProjectUID) == stringPtrToString(b.ProjectUID) &&
		stringPtrToString(a.Category) == stringPtrToString(b.Category) &&
		stringPtrToString(a.Description) == stringPtrToString(b.Description) &&
		stringPtrToString(a.Website) == stringPtrToString(b.Website) &&
		stringPtrToString(a.MailingList) == stringPtrToString(b.MailingList) &&
		stringPtrToString(a.ChatChannel) == stringPtrToString(b.ChatChannel) &&
		stringPtrToString(a.DisplayName) == stringPtrToString(b.DisplayName) &&
		a.EnableVoting == b.EnableVoting &&
		a.SsoGroupEnabled == b.SsoGroupEnabled &&
		a.Public == b.Public
}

// createCommitteeMember creates a new committee member via the Committee Service API.
func createCommitteeMember(ctx context.Context, payload *committeeservice.CreateCommitteeMemberPayload, v1Principal string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return nil, err
	}

	payload.BearerToken = &token

	result, err := committeeClient.CreateCommitteeMember(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create committee member: %w", err)
	}

	return result, nil
}

// fetchCommitteeMember fetches an existing committee member from the Committee Service API.
func fetchCommitteeMember(ctx context.Context, committeeUID, memberUID string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return nil, "", err
	}

	result, err := committeeClient.GetCommitteeMember(ctx, &committeeservice.GetCommitteeMemberPayload{
		BearerToken: &token,
		UID:         committeeUID,
		MemberUID:   memberUID,
		Version:     "1",
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch committee member: %w", err)
	}

	etag := ""
	if result.Etag != nil {
		etag = *result.Etag
	}

	return result.Member, etag, nil
}

// updateCommitteeMember updates an existing committee member via the Committee Service API.
func updateCommitteeMember(ctx context.Context, payload *committeeservice.UpdateCommitteeMemberPayload, v1Principal string) error {
	// Fetch current committee member for comparison.
	currentMember, etag, err := fetchCommitteeMember(ctx, payload.UID, payload.MemberUID)
	if err != nil {
		return fmt.Errorf("failed to fetch current committee member: %w", err)
	}

	// Check if member has changes (basic comparison).
	memberChanged := !committeeMembersEqual(currentMember, payload)

	if memberChanged {
		token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
		if err != nil {
			return fmt.Errorf("failed to generate token for committee member update: %w", err)
		}

		payload.BearerToken = &token
		payload.IfMatch = stringToStringPtr(etag)

		_, err = committeeClient.UpdateCommitteeMember(ctx, payload)
		if err != nil {
			return fmt.Errorf("failed to update committee member: %w", err)
		}
	}

	return nil
}

// deleteCommittee deletes a committee by UID.
func deleteCommittee(ctx context.Context, committeeUID string, v1Principal string) error {
	// Fetch current committee base to get etag.
	_, etag, err := fetchCommitteeBase(ctx, committeeUID)
	if err != nil {
		return fmt.Errorf("failed to fetch committee base for deletion: %w", err)
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return fmt.Errorf("failed to generate token for committee deletion: %w", err)
	}

	payload := &committeeservice.DeleteCommitteePayload{
		BearerToken: &token,
		UID:         &committeeUID,
		IfMatch:     stringToStringPtr(etag),
	}

	err = committeeClient.DeleteCommittee(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed to delete committee: %w", err)
	}

	return nil
}

// deleteCommitteeMember deletes a committee member by committee UID and member UID.
func deleteCommitteeMember(ctx context.Context, committeeUID, memberUID string, v1Principal string, skipNotification bool) error {
	// Fetch current committee member to get etag.
	_, etag, err := fetchCommitteeMember(ctx, committeeUID, memberUID)
	if err != nil {
		return fmt.Errorf("failed to fetch committee member for deletion: %w", err)
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return fmt.Errorf("failed to generate token for committee member deletion: %w", err)
	}

	payload := &committeeservice.DeleteCommitteeMemberPayload{
		BearerToken:      &token,
		UID:              committeeUID,
		MemberUID:        memberUID,
		Version:          "1",
		IfMatch:          stringToStringPtr(etag),
		SkipNotification: skipNotification,
	}

	err = committeeClient.DeleteCommitteeMember(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed to delete committee member: %w", err)
	}

	return nil
}

// committeeMembersEqual compares a committee member with an update payload for equality.
func committeeMembersEqual(current *committeeservice.CommitteeMemberFullWithReadonlyAttributes, update *committeeservice.UpdateCommitteeMemberPayload) bool {
	// Compare basic fields.
	if stringPtrToString(current.Username) != stringPtrToString(update.Username) ||
		stringPtrToString(current.Email) != update.Email ||
		stringPtrToString(current.FirstName) != stringPtrToString(update.FirstName) ||
		stringPtrToString(current.LastName) != stringPtrToString(update.LastName) ||
		stringPtrToString(current.JobTitle) != stringPtrToString(update.JobTitle) ||
		current.AppointedBy != update.AppointedBy ||
		current.Status != update.Status {
		return false
	}

	// Compare role information.
	if current.Role != nil && update.Role != nil {
		if current.Role.Name != update.Role.Name ||
			stringPtrToString(current.Role.StartDate) != stringPtrToString(update.Role.StartDate) ||
			stringPtrToString(current.Role.EndDate) != stringPtrToString(update.Role.EndDate) {
			return false
		}
	} else if current.Role != update.Role {
		return false
	}

	// Compare voting information.
	if current.Voting != nil && update.Voting != nil {
		if current.Voting.Status != update.Voting.Status ||
			stringPtrToString(current.Voting.StartDate) != stringPtrToString(update.Voting.StartDate) ||
			stringPtrToString(current.Voting.EndDate) != stringPtrToString(update.Voting.EndDate) {
			return false
		}
	} else if current.Voting != update.Voting {
		return false
	}

	// Compare organization information.
	if current.Organization != nil && update.Organization != nil {
		if stringPtrToString(current.Organization.Name) != stringPtrToString(update.Organization.Name) ||
			stringPtrToString(current.Organization.Website) != stringPtrToString(update.Organization.Website) {
			return false
		}
	} else if current.Organization != update.Organization {
		return false
	}

	return true
}
