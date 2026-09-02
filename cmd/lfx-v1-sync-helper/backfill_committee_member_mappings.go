// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
)

// backfillCommitteeMemberMappingsResult summarizes a backfill run.
type backfillCommitteeMemberMappingsResult struct {
	inspected  int
	poisoned   int
	fixed      int
	alreadyOK  int
	unresolved int
	malformed  int
	tombstoned int
	conflicted int
}

// backfillCommitteeMemberMappings repairs committee-member reverse mappings whose
// member field holds only the platform-community__c record sfid (the v1 API "ID")
// instead of the contact SFID (the v1 API "MemberID"). See LFXV2-2673 and the fix in
// handlers_committees.go.
//
// The reverse mapping "committee_member.uid.<v2-member-uid>" stores the value
// "projectSFID:committeeSFID:X", where X is disambiguated by isUUID(X) (see
// parseCommitteeMemberReverseMapping in ingest_indexer.go): a UUID is the poisoned
// record sfid; anything else is the contact SFID. contactSFID becomes the {MemberID}
// path segment of the v1 project-service call:
//
//	DELETE/PATCH .../projects/{p}/committees/{c}/members/{contactSFID}
//
// That endpoint matches the member on contact_name__c (the contact SFID), so
// contactSFID must be present. Mappings written from the v1 ingest before this fix
// only stored the record sfid (a UUID) in that position; those 404 on delete and
// leave members on the committee / meeting invites. The field count of the reverse
// mapping is deliberately kept at three (matching the pre-fix format) rather than
// growing to carry both SFIDs, so a rolling deploy never has an old pod misparse a
// value written by a new pod. recordSFID is instead preserved in the separate
// committeeMemberRecordSFIDKey mapping, so the delete path can still tombstone the
// forward mapping "committee_member.sfid.<recordSFID>" written by
// handlers_committees.go.
//
// Enumeration uses ScanSubjectData (sequential GetMsg / next_by_subj), NOT an
// ephemeral consumer: KV_v1-mappings has ~34M sequences and consumer enumeration
// saturates the NATS server (see nats_scan.go). The authoritative contact SFID is
// read from the member's v1-objects platform-community__c record via a single
// direct Get, and only for poisoned (recordSFID-only) mappings. Writes are guarded
// by the KV revision read during repair (KeyValue.Update), so a concurrent write by
// the live sync-helper is reported as conflicted rather than overwritten.
func backfillCommitteeMemberMappings(ctx context.Context, dryRun bool) (backfillCommitteeMemberMappingsResult, error) {
	const (
		// kvMappingsStream is the JetStream stream backing the v1-mappings KV bucket.
		kvMappingsStream = "KV_v1-mappings"

		// reverseSubject is the next_by_subj filter for committee-member reverse
		// mappings. The v2 member UID contains no dots, so a single-token wildcard
		// matches every entry.
		reverseSubject = "$KV.v1-mappings.committee_member.uid.*"

		// reversePrefix is stripped from the subject to recover the v2 member UID.
		reversePrefix = "$KV.v1-mappings.committee_member.uid."

		// v1ObjectKeyPrefix is the v1-objects key prefix for committee-member
		// (platform-community__c) records, keyed by their record sfid.
		v1ObjectKeyPrefix = "platform-community__c."
	)

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	subjectData, err := ScanSubjectData(ctx, jsContext, kvMappingsStream, reverseSubject, opTimeout)
	if err != nil {
		return backfillCommitteeMemberMappingsResult{}, fmt.Errorf("failed to scan committee member reverse mappings: %w", err)
	}

	var res backfillCommitteeMemberMappingsResult
	for subject, data := range subjectData {
		if !strings.HasPrefix(subject, reversePrefix) {
			continue
		}
		res.inspected++
		memberUID := subject[len(reversePrefix):]

		class := classifyReverseMapping(data)
		switch class.outcome {
		case reverseMappingTombstoned:
			res.tombstoned++
			continue
		case reverseMappingMalformed:
			res.malformed++
			logger.With("member_uid", memberUID, "mapping_value", string(data)).
				WarnContext(ctx, "skipping malformed committee member reverse mapping")
			continue
		case reverseMappingAlreadyOK:
			// A mapping that already has a contact SFID can already drive v1
			// delete/update; nothing to repair here.
			res.alreadyOK++
			continue
		}

		projectSFID, committeeSFID, recordSFID := class.projectSFID, class.committeeSFID, class.recordSFID

		// This entry is a confirmed poisoned (record-sfid-only) mapping regardless of
		// whether the contact SFID below can actually be resolved; count it here so
		// unresolved entries aren't left out of the poisoned total.
		res.poisoned++

		// recordSFID is the poisoned platform-community__c record sfid (a UUID)
		// previously (mis)used as the v1 API member id; resolve the contact SFID from
		// the corresponding v1-objects record.
		obj, found, err := getV1ObjectData(ctx, v1ObjectKeyPrefix+recordSFID)
		if err != nil {
			return res, fmt.Errorf("failed to read v1 committee-member object for %s: %w", recordSFID, err)
		}
		if !found {
			// UUID member field but no committee-member record: the record was
			// deleted/tombstoned, so the contact SFID cannot be resolved here.
			res.unresolved++
			logger.With("member_uid", memberUID, "record_sfid", recordSFID).
				WarnContext(ctx, "poisoned reverse mapping but no v1 committee-member record; cannot resolve contact SFID")
			continue
		}

		resolvedContactSFID, _ := obj["contact_name__c"].(string)
		resolvedContactSFID = strings.TrimSpace(resolvedContactSFID)
		if resolvedContactSFID == "" {
			res.unresolved++
			logger.With("member_uid", memberUID, "record_sfid", recordSFID).
				WarnContext(ctx, "v1 committee-member record has no contact_name__c; cannot repair reverse mapping")
			continue
		}
		if !sfid.IsValid(resolvedContactSFID) {
			// parseCommitteeMemberReverseMapping rejects a non-UUID, non-SFID third
			// field as malformed, so writing this value would immediately be
			// rejected by every reader (and re-flagged as malformed by the next
			// backfill run) instead of being repaired.
			res.unresolved++
			logger.With("member_uid", memberUID, "record_sfid", recordSFID, "resolved_contact_sfid", resolvedContactSFID).
				WarnContext(ctx, "v1 committee-member record has an invalid contact_name__c; cannot repair reverse mapping")
			continue
		}

		// Replace the poisoned record sfid with the resolved contact SFID, keeping the
		// reverse mapping at three fields (see the doc comment above). The record sfid
		// is preserved separately in committeeMemberRecordSFIDKey so the delete path
		// can still tombstone the forward mapping.
		newVal := buildRepairedReverseMappingValue(projectSFID, committeeSFID, resolvedContactSFID)
		log := logger.With(
			"member_uid", memberUID,
			"record_sfid", recordSFID,
			"new_contact_sfid", resolvedContactSFID,
			"committee_sfid", committeeSFID,
			"project_sfid", projectSFID,
		)
		if dryRun {
			log.InfoContext(ctx, "[dry-run] would rewrite committee member reverse mapping to include contact SFID")
			res.fixed++
			continue
		}

		reverseKey := "committee_member.uid." + memberUID
		entry, getErr := mappingStore.Get(ctx, reverseKey)
		if getErr != nil {
			if errors.Is(getErr, ErrKeyNotFound) {
				// The mapping was deleted (e.g. tombstoned by live sync) since the scan
				// read it; nothing to repair.
				res.conflicted++
				log.With(errKey, getErr).WarnContext(ctx, "committee member reverse mapping changed since scan (no longer readable), skipping")
				continue
			}
			// A transient/infrastructure error (timeout, disconnect, auth) is not a
			// conflict: surface it so the run is reported as incomplete rather than
			// silently skipping a mapping that still needs repair.
			return res, fmt.Errorf("failed to re-read committee member reverse mapping %s: %w", reverseKey, getErr)
		}
		if string(entry.Value) != string(data) {
			// The mapping was changed by the live sync-helper since the scan read it;
			// skip rather than overwrite the newer value. A later run will re-evaluate it.
			res.conflicted++
			log.WarnContext(ctx, "committee member reverse mapping changed since scan, skipping to avoid overwriting concurrent write")
			continue
		}
		// Write the record-sfid companion before publishing the reverse mapping as
		// "alreadyOK" (contact SFID present): once the reverse mapping is updated, a
		// later backfill run classifies it as alreadyOK and never revisits it, so a
		// companion that failed to write here would never get repaired.
		//
		// The companion write is revision-guarded (Create, since the only case that
		// reaches it is "companion absent") rather than a blind Put, and is rolled
		// back if the reverse-mapping Update below loses a race.
		companionKey := committeeMemberRecordSFIDKey(memberUID)
		companionEntry, companionGetErr := mappingStore.Get(ctx, companionKey)
		companionExisted := companionGetErr == nil
		if companionGetErr != nil && !errors.Is(companionGetErr, ErrKeyNotFound) {
			return res, fmt.Errorf("failed to read committee member record sfid mapping %s: %w", companionKey, companionGetErr)
		}

		var (
			companionRevision uint64
			companionWritten  bool
		)
		switch {
		case companionExisted && string(companionEntry.Value) == recordSFID:
			// Already holds the value we'd write; nothing to change.
		case companionExisted && string(companionEntry.Value) == tombstoneMarker:
			// The live delete path (syncCommitteeMemberDeleteToV1, ingest_indexer.go)
			// tombstones this companion before the reverse mapping, so a tombstoned
			// companion here means a delete is likely racing us, in flight between
			// its two tombstones. Resurrecting it would strand a live companion
			// behind a reverse mapping the delete is about to tombstone anyway —
			// skip instead of overwriting.
			res.conflicted++
			log.WarnContext(ctx, "committee member record sfid mapping is tombstoned, likely racing a concurrent delete, skipping")
			continue
		case companionExisted:
			// Holds some other value than the scan expected: changed concurrently
			// to point at a different record. Don't blindly overwrite a value we
			// don't understand.
			res.conflicted++
			log.With("companion_value", string(companionEntry.Value)).
				WarnContext(ctx, "committee member record sfid mapping holds an unexpected value, skipping")
			continue
		default:
			rev, err := mappingStore.Create(ctx, companionKey, []byte(recordSFID))
			if err != nil {
				if errors.Is(err, ErrRevisionMismatch) || errors.Is(err, ErrKeyExists) {
					res.conflicted++
					log.With(errKey, err).WarnContext(ctx, "committee member record sfid mapping created concurrently, skipping")
					continue
				}
				return res, fmt.Errorf("failed to store committee member record sfid mapping %s: %w", companionKey, err)
			}
			companionRevision = rev
			companionWritten = true
		}

		if _, err := mappingStore.Update(ctx, reverseKey, []byte(newVal), entry.Revision); err != nil {
			rollbackCompanion := func() {
				// Only the "companion absent" case above ever writes it, so only that
				// case needs a rollback; the tombstone is revision-guarded so a
				// concurrent writer that has since touched the companion isn't
				// clobbered — we just log and leave it in that case.
				if !companionWritten {
					return
				}
				if _, rbErr := mappingStore.Update(ctx, companionKey, []byte(tombstoneMarker), companionRevision); rbErr != nil {
					log.With(errKey, rbErr).WarnContext(ctx, "failed to roll back committee member record sfid mapping after reverse mapping conflict")
				}
			}

			if errors.Is(err, ErrRevisionMismatch) {
				// The CAS was rejected outright, so the write definitely did not
				// apply: safe to roll back unconditionally.
				rollbackCompanion()
				res.conflicted++
				log.With(errKey, err).WarnContext(ctx, "committee member reverse mapping revision conflict, skipping to avoid overwriting concurrent write")
				continue
			}

			// Any other error (timeout, disconnect, etc.) is ambiguous: NATS may
			// have committed newVal before the acknowledgement was lost. Re-read
			// the reverse key to decide whether to roll back the companion —
			// blindly rolling back here could strand a mapping that was actually
			// fixed without its companion, and the next backfill run would then
			// skip it as alreadyOK and never repair it.
			reReadEntry, reReadErr := mappingStore.Get(ctx, reverseKey)
			switch {
			case reReadErr == nil && string(reReadEntry.Value) == newVal:
				// Confirmed committed: keep the companion.
				log.With(errKey, err).WarnContext(ctx, "reverse mapping update reported an error but the write committed; keeping companion")
				log.InfoContext(ctx, "rewrote committee member reverse mapping to include contact SFID")
				res.fixed++
				continue
			case reReadErr == nil, errors.Is(reReadErr, ErrKeyNotFound):
				// Confirmed absent (holds something else, or gone): the write did
				// not apply, safe to roll back.
				rollbackCompanion()
				return res, fmt.Errorf("failed to write corrected reverse mapping %s: %w", reverseKey, err)
			default:
				// The verification read itself is inconclusive (timeout,
				// disconnect, etc.): we can't tell whether the write applied, so
				// don't roll back a companion that might be needed. Surface the
				// original error so the run is reported as incomplete instead of
				// silently guessing.
				return res, fmt.Errorf("failed to write corrected reverse mapping %s (and could not verify whether it applied): %w", reverseKey, err)
			}
		}
		log.InfoContext(ctx, "rewrote committee member reverse mapping to include contact SFID")
		res.fixed++
	}

	return res, nil
}

// reverseMappingOutcome is the pre-lookup triage outcome for a scanned
// committee-member reverse mapping entry, computed without any NATS/v1-objects
// access so it can be unit tested in isolation.
type reverseMappingOutcome string

const (
	reverseMappingTombstoned reverseMappingOutcome = "tombstoned"
	reverseMappingMalformed  reverseMappingOutcome = "malformed"
	reverseMappingAlreadyOK  reverseMappingOutcome = "alreadyOK"
	reverseMappingNeedsFix   reverseMappingOutcome = "needsFix"
)

// reverseMappingClassification is the result of classifyReverseMapping.
type reverseMappingClassification struct {
	outcome       reverseMappingOutcome
	projectSFID   string
	committeeSFID string
	// recordSFID is only populated when outcome is reverseMappingNeedsFix (it is
	// empty for reverseMappingAlreadyOK, whose third field is a contact SFID, not a
	// record SFID).
	recordSFID string
}

// classifyReverseMapping triages a scanned committee-member reverse mapping value
// into one of four outcomes, without performing any NATS or v1-objects access:
//
//   - reverseMappingTombstoned: explicitly tombstoned value (tombstoneMarker).
//   - reverseMappingMalformed: empty, or neither a record nor a contact SFID can be
//     parsed out. ScanSubjectData already filters out native KV deletes/purges before
//     this is called, so an empty value here is not a legitimate tombstone — it can
//     only be a corrupted/malformed write, and is surfaced accordingly rather than
//     silently skipped as already-handled.
//   - reverseMappingAlreadyOK: a contact SFID is already present; no repair needed.
//   - reverseMappingNeedsFix: only a (poisoned) record SFID is present; the caller
//     must resolve the contact SFID from the v1-objects bucket to repair it.
func classifyReverseMapping(data []byte) reverseMappingClassification {
	if isTombstonedMapping(data) {
		return reverseMappingClassification{outcome: reverseMappingTombstoned}
	}
	if len(data) == 0 {
		return reverseMappingClassification{outcome: reverseMappingMalformed}
	}

	projectSFID, committeeSFID, recordSFID, contactSFID, ok := parseCommitteeMemberReverseMapping(string(data))
	if !ok || (recordSFID == "" && contactSFID == "") {
		return reverseMappingClassification{outcome: reverseMappingMalformed}
	}

	if contactSFID != "" {
		return reverseMappingClassification{
			outcome:       reverseMappingAlreadyOK,
			projectSFID:   projectSFID,
			committeeSFID: committeeSFID,
			recordSFID:    recordSFID,
		}
	}

	return reverseMappingClassification{
		outcome:       reverseMappingNeedsFix,
		projectSFID:   projectSFID,
		committeeSFID: committeeSFID,
		recordSFID:    recordSFID,
	}
}

// buildRepairedReverseMappingValue formats a repaired committee-member reverse
// mapping value: "projectSFID:committeeSFID:contactSFID". Kept at three fields —
// see the doc comment on backfillCommitteeMemberMappings for why.
func buildRepairedReverseMappingValue(projectSFID, committeeSFID, contactSFID string) string {
	return fmt.Sprintf("%s:%s:%s", projectSFID, committeeSFID, contactSFID)
}

// isUUID reports whether s is a canonical 8-4-4-4-12 hyphenated hex UUID.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
