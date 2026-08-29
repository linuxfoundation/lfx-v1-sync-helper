// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"strings"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// backfillCommitteeMemberNamesResult summarizes a backfill run.
type backfillCommitteeMemberNamesResult struct {
	inspected  int
	skipped    int // already have a name, or tombstoned/malformed mapping
	noMapping  int // no usable reverse mapping to resolve the contact SFID
	noName     int // contact SFID found but merged_user row has no name
	updated    int // successfully patched
	dryRun     int // would have patched (dry-run mode)
	errored    int // fetch or update failed
}

// backfillCommitteeMemberNames patches V2 committee member records whose
// first_name and last_name are both empty. This affects members who had no
// LFX account at sync time — lookupMergedUser returned an error because
// merged_user had no username for them, so the name fields were silently
// dropped from the sync payload.
//
// For each forward mapping (committee_member.sfid.*) the backfill:
//  1. Parses committeeUID and memberUID from the mapping value.
//  2. Fetches the V2 member record; skips it if either name field is already set.
//  3. Looks up the contact SFID from the reverse mapping
//     (committee_member.uid.<memberUID> → projectSFID:committeeSFID:contactSFID).
//  4. Reads first_name/last_name directly from salesforce.merged_user via
//     the contact SFID — no username required.
//  5. Calls UpdateCommitteeMember with SkipEnrichment=true so the committee
//     service stores the supplied names as-is without attempting another
//     username / auth-service lookup (which would fail again for these members).
func backfillCommitteeMemberNames(ctx context.Context, dryRun bool) (*backfillCommitteeMemberNamesResult, error) {
	const (
		kvMappingsStream = "KV_v1-mappings"

		// forwardSubject is the subject filter for all forward committee-member
		// mappings: committee_member.sfid.<recordSFID> → committeeUID:memberUID.
		forwardSubject = "$KV.v1-mappings.committee_member.sfid.*"
		forwardPrefix  = "$KV.v1-mappings.committee_member.sfid."
	)

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	subjectData, err := ScanSubjectData(ctx, jsContext, kvMappingsStream, forwardSubject, opTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to scan committee member forward mappings: %w", err)
	}

	res := &backfillCommitteeMemberNamesResult{}

	for subject, data := range subjectData {
		if !strings.HasPrefix(subject, forwardPrefix) {
			continue
		}
		res.inspected++

		// Skip tombstoned entries.
		if isTombstonedMapping(data) {
			res.skipped++
			continue
		}

		// Parse committeeUID:memberUID from the mapping value.
		parts := strings.SplitN(string(data), ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			logger.With("subject", subject, "value", string(data)).
				WarnContext(ctx, "skipping malformed committee member forward mapping")
			res.skipped++
			continue
		}
		committeeUID, memberUID := parts[0], parts[1]

		// Fetch the current V2 member record.
		member, etag, fetchErr := fetchCommitteeMember(ctx, committeeUID, memberUID)
		if fetchErr != nil {
			logger.With(errKey, fetchErr, "committee_uid", committeeUID, "member_uid", memberUID).
				WarnContext(ctx, "backfill: failed to fetch committee member, skipping")
			res.errored++
			continue
		}

		// Skip if either name field is already populated.
		if stringPtrToString(member.FirstName) != "" || stringPtrToString(member.LastName) != "" {
			res.skipped++
			continue
		}

		// Look up the contact SFID from the reverse mapping.
		reverseKey := "committee_member.uid." + memberUID
		reverseEntry, kvErr := mappingsKV.Get(ctx, reverseKey)
		if kvErr != nil || isTombstonedMapping(reverseEntry.Value()) {
			logger.With("member_uid", memberUID, "committee_uid", committeeUID).
				WarnContext(ctx, "backfill: no usable reverse mapping for member, cannot resolve contact SFID")
			res.noMapping++
			continue
		}

		_, _, _, contactSFID, ok := parseCommitteeMemberReverseMapping(string(reverseEntry.Value()))
		if !ok || contactSFID == "" {
			logger.With("member_uid", memberUID, "reverse_value", string(reverseEntry.Value())).
				WarnContext(ctx, "backfill: reverse mapping has no contact SFID (may be an old-format or poisoned entry)")
			res.noMapping++
			continue
		}

		// Read first_name/last_name from the V1 merged_user row directly.
		row, rowErr := dbLookupMergedUserRowBySFID(ctx, contactSFID)
		if rowErr != nil || row == nil {
			logger.With("member_uid", memberUID, "contact_sfid", contactSFID).
				WarnContext(ctx, "backfill: no merged_user row found for contact SFID")
			res.noName++
			continue
		}

		firstName := row.FirstName.String
		lastName := row.LastName.String
		if firstName == "" && lastName == "" {
			logger.With("member_uid", memberUID, "contact_sfid", contactSFID).
				WarnContext(ctx, "backfill: merged_user row has no first or last name")
			res.noName++
			continue
		}

		if dryRun {
			logger.With(
				"committee_uid", committeeUID,
				"member_uid", memberUID,
				"first_name", firstName,
				"last_name", lastName,
			).Info("backfill: dry-run — would update committee member name")
			res.dryRun++
			continue
		}

		// Build the update payload from the current member record, adding the names.
		payload := memberToUpdatePayload(member, committeeUID, memberUID, etag)
		if firstName != "" {
			payload.FirstName = &firstName
		}
		if lastName != "" {
			payload.LastName = &lastName
		}
		// SkipEnrichment tells the committee service to store the names as-is
		// without attempting another auth-service/username lookup, which would
		// fail the same way for members who have no LFX account.
		payload.SkipEnrichment = true

		token, tokenErr := generateCachedJWTToken(ctx, committeeServiceAudience, "")
		if tokenErr != nil {
			logger.With(errKey, tokenErr, "member_uid", memberUID).
				WarnContext(ctx, "backfill: failed to generate token, skipping member")
			res.errored++
			continue
		}
		payload.BearerToken = &token

		if _, updateErr := committeeClient.UpdateCommitteeMember(ctx, payload); updateErr != nil {
			logger.With(errKey, updateErr, "committee_uid", committeeUID, "member_uid", memberUID).
				WarnContext(ctx, "backfill: failed to update committee member name")
			res.errored++
			continue
		}

		logger.With(
			"committee_uid", committeeUID,
			"member_uid", memberUID,
			"first_name", firstName,
			"last_name", lastName,
		).Info("backfill: updated committee member name")
		res.updated++
	}

	return res, nil
}

// memberToUpdatePayload builds an UpdateCommitteeMemberPayload from a fetched
// CommitteeMemberFullWithReadonlyAttributes, preserving all existing fields so
// the update does not unintentionally clear any data.
func memberToUpdatePayload(m *committeeservice.CommitteeMemberFullWithReadonlyAttributes, committeeUID, memberUID, etag string) *committeeservice.UpdateCommitteeMemberPayload {
	p := &committeeservice.UpdateCommitteeMemberPayload{
		UID:         committeeUID,
		MemberUID:   memberUID,
		Version:     "1",
		IfMatch:     stringToStringPtr(etag),
		Username:    m.Username,
		Email:       stringPtrToString(m.Email),
		FirstName:   m.FirstName,
		LastName:    m.LastName,
		JobTitle:    m.JobTitle,
		AppointedBy: m.AppointedBy,
		Status:      m.Status,
	}

	if m.Role != nil {
		p.Role = &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{
			Name:      m.Role.Name,
			StartDate: m.Role.StartDate,
			EndDate:   m.Role.EndDate,
		}
	}

	if m.Voting != nil {
		p.Voting = &struct {
			Status    string
			StartDate *string
			EndDate   *string
		}{
			Status:    m.Voting.Status,
			StartDate: m.Voting.StartDate,
			EndDate:   m.Voting.EndDate,
		}
	}

	if m.Organization != nil {
		p.Organization = &struct {
			ID      *string
			Name    *string
			Website *string
		}{
			ID:      m.Organization.ID,
			Name:    m.Organization.Name,
			Website: m.Organization.Website,
		}
	}

	return p
}
