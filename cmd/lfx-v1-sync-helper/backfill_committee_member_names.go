// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go/jetstream"
)

// committeeMemberKVRecord is the minimal set of fields read from the
// committee-members KV bucket (JSON-encoded by the committee service).
// Unknown fields are ignored by encoding/json; keep only what this backfill uses.
type committeeMemberKVRecord struct {
	UID          string `json:"uid"`
	CommitteeUID string `json:"committee_uid"`
	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name"`
}

// backfillCommitteeMemberNamesResult summarizes a backfill run.
type backfillCommitteeMemberNamesResult struct {
	inspected int
	skipped   int // already have a name, missing uid/committee_uid, or name set concurrently
	noMapping int // no usable reverse mapping to resolve the contact SFID
	noName    int // contact SFID found but neither merged_user nor salesforce.contact has a name
	updated   int // successfully patched
	dryRun    int // would have patched (dry-run mode)
	errored   int // fetch or update failed
}

// classifyCommitteeMemberKey decodes and classifies a single entry from the
// committee-members KV bucket.
//
// isLookup is true when the key is a secondary-index entry (prefix "lookup/")
// that must be skipped without counting toward the inspected total.
// needsBackfill is true when the record has both name fields empty and both
// uid / committee_uid present — the only case that proceeds to the SFID lookup.
// err is non-nil when the JSON payload cannot be decoded.
// When isLookup is true the other return values are zero/false.
func classifyCommitteeMemberKey(key string, value []byte) (rec committeeMemberKVRecord, isLookup bool, needsBackfill bool, err error) {
	if strings.HasPrefix(key, "lookup/") {
		return rec, true, false, nil
	}
	if decodeErr := json.Unmarshal(value, &rec); decodeErr != nil {
		return rec, false, false, decodeErr
	}
	named := rec.FirstName != "" || rec.LastName != ""
	missingIDs := rec.UID == "" || rec.CommitteeUID == ""
	needsBackfill = !named && !missingIDs
	return rec, false, needsBackfill, nil
}

// backfillCommitteeMemberNames patches V2 committee member records whose
// first_name and last_name are both empty. This affects members who had no
// LFX account at sync time — lookupMergedUser returned an error because
// merged_user had no username for them, so the name fields were silently
// dropped from the sync payload.
//
// Instead of scanning the entire v1-mappings stream, this implementation
// iterates the committee-members KV bucket directly (owned by the committee
// service, JSON-encoded, keyed by memberUID). Only members with both name
// fields empty proceed to the V1 DB lookup — the committee service API is
// never called just to check whether names are present.
//
// For each nameless member the backfill:
//  1. Looks up the contact SFID from the v1-mappings reverse mapping
//     (committee_member.uid.<memberUID> → projectSFID:committeeSFID:contactSFID).
//     For old-format "poisoned" entries where the third field is a UUID,
//     resolves the contact SFID from the v1-objects record.
//  2. Reads first_name/last_name via the contact SFID: tries
//     salesforce.merged_user first (contacts with or without an LFX account),
//     then falls back to salesforce.contact (all Salesforce contacts).
//  3. Calls UpdateCommitteeMember with the resolved names. SkipEnrichment is
//     NOT set: enrichMember cannot overwrite non-empty names, and leaving it
//     enabled preserves email→username resolution and avatar population.
func backfillCommitteeMemberNames(ctx context.Context, dryRun bool) (*backfillCommitteeMemberNamesResult, error) {
	const (
		committeeMembersStream     = "KV_committee-members"
		committeeMembersSubject    = "$KV.committee-members.*"
		committeeMembersSubjectPfx = "$KV.committee-members."
		v1ObjectKeyPrefix          = "platform-community__c."
	)

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	// Use ScanSubjectData instead of KeyValue.ListKeys so that enumeration
	// errors are propagated rather than silently causing an incomplete result.
	// ListKeys drives a push consumer whose channel closure cannot distinguish
	// normal stream-end from a missed heartbeat, risking silent truncation on
	// high-latency connections. ScanSubjectData uses sequential GetMsg calls
	// with per-call context deadlines and propagates every non-ErrMsgNotFound
	// error to the caller. See nats_scan.go for the full analysis.
	subjectMap, err := ScanSubjectData(ctx, jsContext, committeeMembersStream, committeeMembersSubject, opTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate %s: %w", committeeMembersStream, err)
	}

	kvGetFn := func(ctx context.Context, key string) ([]byte, error) {
		entry, err := mappingsKV.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return entry.Value(), nil
	}

	res := &backfillCommitteeMemberNamesResult{}

	for subject, value := range subjectMap {
		key := strings.TrimPrefix(subject, committeeMembersSubjectPfx)

		rec, isLookup, needsBackfill, classErr := classifyCommitteeMemberKey(key, value)
		if isLookup {
			continue
		}
		res.inspected++
		if classErr != nil {
			logger.With(errKey, classErr, "key", key).
				WarnContext(ctx, "backfill: failed to decode committee-member record, skipping")
			res.errored++
			continue
		}
		if !needsBackfill {
			if rec.UID == "" || rec.CommitteeUID == "" {
				logger.With("key", key).
					WarnContext(ctx, "backfill: record missing uid or committee_uid, skipping")
			}
			res.skipped++
			continue
		}

		memberUID := rec.UID
		committeeUID := rec.CommitteeUID

		// Resolve the contact SFID needed to look up the name in merged_user.
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

		// Read first_name/last_name from V1 via the shared resolver: tries
		// merged_user first, then falls back per-field to salesforce.contact.
		firstName, lastName, nameErr := resolveContactNames(ctx, contactSFID)
		if nameErr != nil {
			logger.With(errKey, nameErr, "committee_uid", committeeUID, "member_uid", memberUID).
				WarnContext(ctx, "backfill: error looking up contact names from DB")
			res.errored++
			continue
		}
		if firstName == "" && lastName == "" {
			logger.With("committee_uid", committeeUID, "member_uid", memberUID).
				WarnContext(ctx, "backfill: no name found in merged_user or contact for member")
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

		// Fetch the live record to get an ETag for the conditional update.
		member, etag, fetchErr := fetchCommitteeMember(ctx, committeeUID, memberUID)
		if fetchErr != nil {
			logger.With(errKey, fetchErr, "committee_uid", committeeUID, "member_uid", memberUID).
				WarnContext(ctx, "backfill: failed to fetch committee member for update, skipping")
			res.errored++
			continue
		}

		// Re-check after fetch — another process may have set the name already.
		if stringPtrToString(member.FirstName) != "" || stringPtrToString(member.LastName) != "" {
			res.skipped++
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
		// SkipEnrichment is intentionally not set: enrichMember only fills name
		// fields when they are empty, so it cannot overwrite the names we
		// supply, and leaving enrichment enabled preserves email→username
		// resolution and avatar population for these members.

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
