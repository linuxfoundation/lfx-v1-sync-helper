// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Committee-specific client operations for the v1-sync-helper service.
package main

import (
	"context"
	"fmt"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// fetchCommitteeBase fetches an existing committee base from the Committee Service API.
func fetchCommitteeBase(ctx context.Context, committeeUID string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
	token, err := generateCachedJWTToken(committeeServiceAudience, UserInfo{})
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
func createCommittee(ctx context.Context, payload *committeeservice.CreateCommitteePayload, userInfo UserInfo) (*committeeservice.CommitteeFullWithReadonlyAttributes, error) {
	token, err := generateCachedJWTToken(committeeServiceAudience, userInfo)
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

// updateCommittee updates a committee by separately handling base and settings if there are changes.
func updateCommittee(ctx context.Context, payload *committeeservice.UpdateCommitteeBasePayload, userInfo UserInfo) error {
	// Fetch current committee base.
	currentBase, baseETag, err := fetchCommitteeBase(ctx, *payload.UID)
	if err != nil {
		return fmt.Errorf("failed to fetch current committee base: %w", err)
	}

	// Create updated base for comparison.
	updatedBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:         currentBase.UID,
		Name:        stringToStringPtr(payload.Name),
		ProjectUID:  stringToStringPtr(payload.ProjectUID),
		Category:    stringToStringPtr(payload.Category),
		Description: payload.Description,
		Website:     payload.Website,
	}

	// Check if base has changes.
	baseChanged := !committeeBasesEqual(currentBase, updatedBase)

	if baseChanged {
		token, err := generateCachedJWTToken(committeeServiceAudience, userInfo)
		if err != nil {
			return fmt.Errorf("failed to generate token for committee base update: %w", err)
		}

		payload.BearerToken = &token
		payload.IfMatch = stringToStringPtr(baseETag)

		_, err = committeeClient.UpdateCommitteeBase(ctx, payload)
		if err != nil {
			return fmt.Errorf("failed to update committee base: %w", err)
		}
	}

	// For now, assuming all fields are in base. If there are settings-specific fields,
	// we would handle them similarly here with fetchCommitteeSettings and UpdateCommitteeSettings.

	return nil
}

// committeeBasesEqual compares two CommitteeBaseWithReadonlyAttributes objects for equality.
func committeeBasesEqual(a, b *committeeservice.CommitteeBaseWithReadonlyAttributes) bool {
	return stringPtrToString(a.Name) == stringPtrToString(b.Name) &&
		stringPtrToString(a.ProjectUID) == stringPtrToString(b.ProjectUID) &&
		stringPtrToString(a.Category) == stringPtrToString(b.Category) &&
		stringPtrToString(a.Description) == stringPtrToString(b.Description) &&
		stringPtrToString(a.Website) == stringPtrToString(b.Website)
}

// createCommitteeMember creates a new committee member via the Committee Service API.
func createCommitteeMember(ctx context.Context, payload *committeeservice.CreateCommitteeMemberPayload, userInfo UserInfo) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, error) {
	token, err := generateCachedJWTToken(committeeServiceAudience, userInfo)
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
	token, err := generateCachedJWTToken(committeeServiceAudience, UserInfo{})
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
func updateCommitteeMember(ctx context.Context, payload *committeeservice.UpdateCommitteeMemberPayload, userInfo UserInfo) error {
	// Fetch current committee member for comparison.
	currentMember, etag, err := fetchCommitteeMember(ctx, payload.UID, payload.MemberUID)
	if err != nil {
		return fmt.Errorf("failed to fetch current committee member: %w", err)
	}

	// Check if member has changes (basic comparison).
	memberChanged := !committeeMembersEqual(currentMember, payload)

	if memberChanged {
		token, err := generateCachedJWTToken(committeeServiceAudience, userInfo)
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

// committeeMembersEqual compares a committee member with an update payload for equality.
func committeeMembersEqual(current *committeeservice.CommitteeMemberFullWithReadonlyAttributes, update *committeeservice.UpdateCommitteeMemberPayload) bool {
	// Compare basic fields.
	if stringPtrToString(current.Username) != stringPtrToString(update.Username) ||
		stringPtrToString(current.Email) != update.Email ||
		stringPtrToString(current.FirstName) != stringPtrToString(update.FirstName) ||
		stringPtrToString(current.LastName) != stringPtrToString(update.LastName) ||
		stringPtrToString(current.JobTitle) != stringPtrToString(update.JobTitle) ||
		current.AppointedBy != update.AppointedBy ||
		current.Status != update.Status ||
		stringPtrToString(current.Agency) != stringPtrToString(update.Agency) ||
		stringPtrToString(current.Country) != stringPtrToString(update.Country) {
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
