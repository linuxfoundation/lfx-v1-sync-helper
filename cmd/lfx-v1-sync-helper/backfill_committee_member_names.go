// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
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
// Prerequisites: run --backfill-committee-member-mappings first so that any
// old-format "poisoned" reverse mappings (whose third field is the record UUID
// rather than the contact SFID) are repaired. This backfill additionally
// falls back to resolving the contact SFID from the v1-objects KV bucket for
// entries that could not be repaired by that pass, mirroring the logic in
// backfill_committee_member_mappings.go.
//
// For each forward mapping (committee_member.sfid.*) the backfill:
//  1. Parses committeeUID and memberUID from the mapping value.
//  2. Fetches the V2 member record; skips it if either name field is already set.
//  3. Looks up the contact SFID from the reverse mapping
//     (committee_member.uid.<memberUID> → projectSFID:committeeSFID:contactSFID).
//     For old-format "poisoned" entries where the third field is a UUID,
//     resolves the contact SFID from the v1-objects record via the record SFID
//     extracted directly from that third field of the reverse mapping.
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
		forwardSubject    = "$KV.v1-mappings.committee_member.sfid.*"
		forwardPrefix     = "$KV.v1-mappings.committee_member.sfid."
		v1ObjectKeyPrefix = "platform-community__c."
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
				WarnContext(ctx, "backfill: skipping malformed committee member forward mapping")
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

		// Resolve the contact SFID needed to look up the name in merged_user.
		kvGetFn := func(ctx context.Context, key string) ([]byte, error) {
			entry, err := mappingsKV.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			return entry.Value(), nil
		}
		contactSFID, resolveErr := resolveContactSFIDForMember(ctx, memberUID, v1ObjectKeyPrefix, kvGetFn, getV1ObjectData)
		if resolveErr != nil {
			logger.With(errKey, resolveErr, "member_uid", memberUID, "committee_uid", committeeUID).
				WarnContext(ctx, "backfill: failed to resolve contact SFID, skipping")
			res.errored++
			continue
		}
		if contactSFID == "" {
			logger.With("member_uid", memberUID, "committee_uid", committeeUID).
				WarnContext(ctx, "backfill: no usable contact SFID for member, cannot resolve name")
			res.noMapping++
			continue
		}

		// Read first_name/last_name from the V1 merged_user row directly.
		row, rowErr := dbLookupMergedUserRowBySFID(ctx, contactSFID)
		if rowErr != nil {
			logger.With(errKey, rowErr, "member_uid", memberUID).
				WarnContext(ctx, "backfill: error looking up merged_user row")
			res.errored++
			continue
		}
		if row == nil {
			logger.With("member_uid", memberUID).
				WarnContext(ctx, "backfill: no merged_user row found for contact SFID")
			res.noName++
			continue
		}

		firstName := row.FirstName.String
		lastName := row.LastName.String
		if firstName == "" && lastName == "" {
			logger.With("member_uid", memberUID).
				WarnContext(ctx, "backfill: merged_user row has no first or last name")
			res.noName++
			continue
		}

		if dryRun {
			logger.With(
				"committee_uid", committeeUID,
				"member_uid", memberUID,
				"has_first_name", firstName != "",
				"has_last_name", lastName != "",
			).InfoContext(ctx, "backfill: dry-run — would update committee member name")
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
			"has_first_name", firstName != "",
			"has_last_name", lastName != "",
		).InfoContext(ctx, "backfill: updated committee member name")
		res.updated++
	}

	return res, nil
}

// resolveContactSFIDForMember returns the contact SFID (contact_name__c) for a
// V2 committee member UID. It reads the reverse mapping
// (committee_member.uid.<memberUID>) and returns the contact SFID directly when
// present. For old-format "poisoned" entries whose third field is a record UUID
// rather than a contact SFID (parseCommitteeMemberReverseMapping returns
// recordSFID!="", contactSFID==""), it resolves the contact SFID from the
// v1-objects KV record. Returns ("", nil) when the contact SFID cannot be
// determined but no transient error occurred.
//
// kvGet fetches a raw value from the mappings KV (nil, ErrKeyNotFound means
// absent). v1ObjectLookup reads a V1 WAL object by key; (nil, false, nil) means
// not found. Both are injected to allow unit testing without live NATS/KV.
func resolveContactSFIDForMember(
	ctx context.Context,
	memberUID, v1ObjectKeyPrefix string,
	kvGet func(ctx context.Context, key string) ([]byte, error),
	v1ObjectLookup func(ctx context.Context, key string) (map[string]any, bool, error),
) (string, error) {
	reverseKey := "committee_member.uid." + memberUID
	val, kvErr := kvGet(ctx, reverseKey)
	if kvErr != nil {
		if kvErr == jetstream.ErrKeyNotFound || kvErr == jetstream.ErrKeyDeleted {
			return "", nil
		}
		return "", fmt.Errorf("reading reverse mapping: %w", kvErr)
	}
	if isTombstonedMapping(val) {
		return "", nil
	}

	_, _, recordSFID, contactSFID, ok := parseCommitteeMemberReverseMapping(string(val))
	if !ok {
		// Malformed mapping — cannot resolve.
		return "", nil
	}
	if contactSFID != "" {
		// Happy path: reverse mapping already contains the contact SFID.
		return contactSFID, nil
	}

	// Poisoned entry: third field is the record UUID. Resolve the contact SFID
	// from the v1-objects record, mirroring backfill_committee_member_mappings.go.
	if recordSFID == "" {
		return "", nil
	}
	obj, found, err := v1ObjectLookup(ctx, v1ObjectKeyPrefix+recordSFID)
	if err != nil {
		return "", fmt.Errorf("reading v1 object for record sfid %s: %w", recordSFID, err)
	}
	if !found {
		return "", nil
	}
	resolved, _ := obj["contact_name__c"].(string)
	return strings.TrimSpace(resolved), nil
}

// memberToUpdatePayload builds an UpdateCommitteeMemberPayload from a fetched
// CommitteeMemberFullWithReadonlyAttributes, preserving all mutable fields so
// the update does not unintentionally clear any data.
func memberToUpdatePayload(m *committeeservice.CommitteeMemberFullWithReadonlyAttributes, committeeUID, memberUID, etag string) *committeeservice.UpdateCommitteeMemberPayload {
	p := &committeeservice.UpdateCommitteeMemberPayload{
		UID:             committeeUID,
		MemberUID:       memberUID,
		Version:         "1",
		IfMatch:         stringToStringPtr(etag),
		Username:        m.Username,
		Email:           stringPtrToString(m.Email),
		FirstName:       m.FirstName,
		LastName:        m.LastName,
		JobTitle:        m.JobTitle,
		LinkedinProfile: m.LinkedinProfile,
		AppointedBy:     m.AppointedBy,
		Status:          m.Status,
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
