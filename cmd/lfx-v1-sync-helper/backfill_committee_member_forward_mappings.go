// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
	"github.com/nats-io/nats.go/jetstream"
)

// backfillCommitteeMemberForwardMappingsResult summarizes a backfill run.
type backfillCommitteeMemberForwardMappingsResult struct {
	inspected        int
	migrated         int
	skippedRecordKey int
	malformed        int
	tombstoned       int
	unresolved       int
	conflicted       int
}

// backfillCommitteeMemberForwardMappings migrates committee-member forward mappings
// written by the v2->v1 create path (syncCommitteeMemberCreateToV1) from the flat key
// "committee_member.sfid.<memberSFID>" to the committee-scoped key
// committeeMemberForwardKey(committeeSFID, memberSFID). See LFXV2-2709.
//
// memberSFID there is the v1 API "MemberID" (the contact SFID), which the same contact
// reuses across every committee it belongs to. Before this fix, the create path's key
// carried no committee scope, so adding the same contact to a second committee silently
// overwrote the first committee's forward mapping under the same key — orphaning it, so
// a later v1-WAL delete for that first committee's member could no longer find it.
//
// This is deliberately kept separate from backfillCommitteeMemberMappings (the
// LFXV2-2673 reverse-mapping repair): different key, different collision, and keeping
// them apart leaves that reviewed backfill untouched.
//
// Enumeration uses ScanSubjectData (sequential GetMsg / next_by_subj), matching the
// reverse-mapping backfill (see its doc comment for why: KV_v1-mappings has ~34M
// sequences and ephemeral-consumer enumeration saturates the NATS server). The single
// trailing wildcard in forwardSubject only matches the flat, single-token key format;
// the newer two-token committee-scoped keys have an extra "." and are never matched, so
// they're never revisited by this backfill.
//
// Writes are guarded by a fresh KV read/CAS at write time, so a concurrent write by the
// live sync-helper is reported as conflicted rather than overwritten.
//
// Migrating also races a concurrent syncCommitteeMemberDeleteToV1 for the same member:
// this backfill checks the member's reverse mapping is live before creating the scoped
// key, then re-checks it immediately after, undoing the create (revision-guarded, not a
// blind write) if the membership died in between. syncCommitteeMemberDeleteToV1
// tombstones that same reverse mapping before it checks the scoped key, so between the
// two of them most interleavings are caught by one side or the other — except the case
// where that reverse-mapping tombstone write itself fails; see that function's doc
// comment for why that's an accepted, pre-existing limitation rather than something
// this handshake closes.
//
// Known limitation: for a contact whose collision already overwrote an earlier
// committee's forward mapping before this fix shipped, only the surviving (last-write)
// entry is present to migrate — the orphaned committee's original mapping is gone. This
// backfill is cleanup (stop new key collisions, migrate what's left), not recovery of
// data already lost to the bug.
func backfillCommitteeMemberForwardMappings(ctx context.Context, dryRun bool) (backfillCommitteeMemberForwardMappingsResult, error) {
	const (
		// kvMappingsStream is the JetStream stream backing the v1-mappings KV bucket.
		kvMappingsStream = "KV_v1-mappings"

		// forwardSubject is the next_by_subj filter for flat committee-member forward
		// mappings. A single-token wildcard matches only "committee_member.sfid.<x>"
		// (one further dot-free token) — it does not match the newer two-token
		// committee-scoped keys "committee_member.sfid.<committeeSFID>.<memberSFID>".
		forwardSubject = "$KV.v1-mappings.committee_member.sfid.*"

		// forwardPrefix is stripped from the subject to recover the key token.
		forwardPrefix = "$KV.v1-mappings.committee_member.sfid."
	)

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	subjectData, err := ScanSubjectData(ctx, jsContext, kvMappingsStream, forwardSubject, opTimeout)
	if err != nil {
		return backfillCommitteeMemberForwardMappingsResult{}, fmt.Errorf("failed to scan committee member forward mappings: %w", err)
	}

	var res backfillCommitteeMemberForwardMappingsResult
	for subject, data := range subjectData {
		if !strings.HasPrefix(subject, forwardPrefix) {
			continue
		}
		res.inspected++
		token := subject[len(forwardPrefix):]
		flatKey := "committee_member.sfid." + token

		class := classifyForwardMapping(token, data)
		switch class.outcome {
		case forwardMappingTombstoned:
			res.tombstoned++
			continue
		case forwardMappingRecordKey:
			// v1-ingest forward key (handlers_committees.go), keyed on the globally
			// unique platform-community__c record sfid; not affected by LFXV2-2709.
			res.skippedRecordKey++
			continue
		case forwardMappingMalformed:
			res.malformed++
			logger.With("member_sfid", token, "mapping_value", string(data)).
				WarnContext(ctx, "skipping malformed committee member forward mapping")
			continue
		}

		// forwardMappingNeedsMigration: token is the contact SFID from a v2->v1
		// create-path key; data is "committeeUID:memberUID".
		committeeUID, memberUID := class.committeeUID, class.memberUID
		log := logger.With("member_sfid", token, "committee_uid", committeeUID, "member_uid", memberUID)

		committeeEntry, err := mappingsKV.Get(ctx, "committee.uid."+committeeUID)
		if err != nil && err != jetstream.ErrKeyNotFound && err != jetstream.ErrKeyDeleted {
			// A transient read failure (timeout, disconnect) is not the same as the
			// committee genuinely being gone — treating it as unresolved would
			// tombstone the sole surviving mapping over a blip. Abort the run instead.
			return res, fmt.Errorf("failed to read committee reverse mapping committee.uid.%s: %w", committeeUID, err)
		}
		committeeSFID := ""
		if err == nil && !isTombstonedMapping(committeeEntry.Value()) {
			if _, csfid, ok := splitTwoParts(string(committeeEntry.Value())); ok && csfid != "" {
				committeeSFID = csfid
			}
		}

		// Resolving the committee alone isn't enough: the member itself may have since
		// been deleted (its "committee_member.uid.<memberUID>" reverse mapping
		// tombstoned) or reassigned, in which case migrating would resurrect a live
		// scoped mapping for a membership that no longer exists. Require the reverse
		// mapping to be live and to still agree with this committee and contact.
		memberLive, err := memberReverseMappingLive(ctx, memberUID, committeeSFID, token)
		if err != nil {
			return res, err
		}

		if committeeSFID == "" || !memberLive {
			// Either the committee this stale mapping points at can no longer be
			// resolved (deleted, tombstoned, or malformed reverse mapping), or the
			// member reverse mapping is gone/tombstoned/no longer matches this
			// committee and contact — either way there's no live membership to scope
			// the key by, so the stale flat entry is just dead weight. Tombstone it
			// rather than migrate a mapping nothing points at anymore.
			log.WarnContext(ctx, "cannot verify live committee membership for stale committee member forward mapping, tombstoning instead of migrating")
			if dryRun {
				log.InfoContext(ctx, "[dry-run] would tombstone unresolvable committee member forward mapping")
				res.unresolved++
				continue
			}
			conflicted, tombErr := tombstoneFlatForwardMappingIfUnchanged(ctx, flatKey, data)
			if tombErr != nil {
				return res, tombErr
			}
			if conflicted {
				res.conflicted++
				continue
			}
			res.unresolved++
			continue
		}

		scopedKey := committeeMemberForwardKey(committeeSFID, token)
		log = log.With("committee_sfid", committeeSFID, "scoped_key", scopedKey)

		if dryRun {
			// Read-only preview of the same check createScopedForwardMapping does live,
			// so dry-run can report the conflicted outcome instead of always claiming
			// migrated.
			entry, getErr := mappingsKV.Get(ctx, scopedKey)
			if getErr == nil && string(entry.Value()) != string(data) {
				log.WarnContext(ctx, "[dry-run] committee member scoped forward mapping already holds an unexpected value, would skip")
				res.conflicted++
				continue
			}
			if getErr != nil && getErr != jetstream.ErrKeyNotFound && getErr != jetstream.ErrKeyDeleted {
				return res, fmt.Errorf("failed to read committee member scoped forward mapping %s: %w", scopedKey, getErr)
			}
			log.InfoContext(ctx, "[dry-run] would migrate committee member forward mapping to committee-scoped key")
			res.migrated++
			continue
		}

		conflicted, created, scopedRevision, err := createScopedForwardMapping(ctx, scopedKey, data)
		if err != nil {
			return res, err
		} else if conflicted {
			res.conflicted++
			log.WarnContext(ctx, "committee member scoped forward mapping already holds an unexpected value, skipping")
			continue
		}

		// syncCommitteeMemberDeleteToV1 tombstones the reverse mapping before checking
		// this scoped key (see its doc comment), so a delete racing the check above can
		// still land between that check and the Create just above. Re-verify now: if
		// the membership died in between, undo the scoped key we just created rather
		// than leak it. The undo is revision-guarded against scopedRevision (the exact
		// entry we just wrote), not a blind Put, so an unrelated write racing in
		// afterward — e.g. the live create path recreating this same key for a new
		// membership — causes a conflict here instead of being clobbered.
		//
		// Only do this when created is true: if createScopedForwardMapping instead
		// observed a pre-existing equal value, that write isn't ours to undo — it may
		// belong to a live create still finishing (e.g. it wrote the scoped key but
		// hasn't published the reverse mapping yet), and undoing by revision would
		// clobber that legitimate write, not ours.
		if created {
			stillLive, err := memberReverseMappingLive(ctx, memberUID, committeeSFID, token)
			if err != nil {
				// We don't know whether the reverse mapping is still live, so leaving
				// the scoped key we just created in place risks orphaning it exactly
				// like the case below. Best-effort roll it back before aborting: the
				// revision guard means this only removes it if it's still exactly
				// what we wrote. If the rollback itself fails for a reason other than
				// a revision mismatch, we genuinely don't know whether it succeeded —
				// log that ambiguity explicitly rather than swallowing it.
				if _, rbErr := mappingsKV.Update(ctx, scopedKey, []byte(tombstoneMarker), scopedRevision); rbErr != nil && !isRevisionMismatchError(rbErr) {
					log.With(errKey, rbErr).WarnContext(ctx, "failed to roll back committee member scoped forward mapping after reverse-mapping recheck error, cleanup outcome unknown")
				}
				return res, err
			}
			if !stillLive {
				if _, err := mappingsKV.Update(ctx, scopedKey, []byte(tombstoneMarker), scopedRevision); err != nil {
					if !isRevisionMismatchError(err) {
						return res, fmt.Errorf("failed to tombstone committee member scoped forward mapping %s after losing the reverse-mapping race: %w", scopedKey, err)
					}
					log.WarnContext(ctx, "committee member scoped forward mapping changed since it was created, leaving the newer write in place")
				}
				res.conflicted++
				log.WarnContext(ctx, "member reverse mapping changed since scoped key was created, undoing migration")
				continue
			}
		}

		conflicted, tombErr := tombstoneFlatForwardMappingIfUnchanged(ctx, flatKey, data)
		if tombErr != nil {
			return res, tombErr
		}
		if conflicted {
			res.conflicted++
			log.WarnContext(ctx, "committee member flat forward mapping changed since scan, skipping tombstone")
			continue
		}

		log.InfoContext(ctx, "migrated committee member forward mapping to committee-scoped key")
		res.migrated++
	}

	return res, nil
}

// memberReverseMappingLive reports whether the "committee_member.uid.<memberUID>"
// reverse mapping is live (not absent, not tombstoned) and still agrees with
// committeeSFID and contactSFID. Used both before creating a scoped forward mapping and
// to re-verify immediately after, since syncCommitteeMemberDeleteToV1 can tombstone this
// same reverse mapping concurrently — see that function's doc comment for the ordering
// this depends on.
func memberReverseMappingLive(ctx context.Context, memberUID, committeeSFID, contactSFID string) (bool, error) {
	entry, err := mappingsKV.Get(ctx, "committee_member.uid."+memberUID)
	if err != nil {
		if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
			return false, nil
		}
		return false, fmt.Errorf("failed to read committee member reverse mapping committee_member.uid.%s: %w", memberUID, err)
	}
	if isTombstonedMapping(entry.Value()) {
		return false, nil
	}
	_, mCommitteeSFID, _, mContactSFID, ok := parseCommitteeMemberReverseMapping(string(entry.Value()))
	return ok && mCommitteeSFID == committeeSFID && mContactSFID == contactSFID, nil
}

// createScopedForwardMapping writes value under scopedKey unless it's already present
// with that exact value (idempotent re-run) or holds something else (conflict, don't
// clobber). Returns conflicted=true if the caller should count this as a skip rather
// than treat it as written. created reports whether this call is the one that actually
// wrote value via Create — false when an equal value was merely observed already
// present (e.g. a prior run, or the live create path, wrote it). The caller must use
// created (not just conflicted) to decide whether it owns the write: only a value this
// call created may be undone by revision-guarded Update later, since an observed
// pre-existing value might belong to a live write still in progress and undoing it by
// revision would clobber that legitimate write rather than the backfill's own.
func createScopedForwardMapping(ctx context.Context, scopedKey string, value []byte) (conflicted, created bool, revision uint64, err error) {
	entry, getErr := mappingsKV.Get(ctx, scopedKey)
	switch {
	case getErr == nil && string(entry.Value()) == string(value):
		// Already migrated (e.g. a prior run crashed after this write but before
		// tombstoning the flat key, or the live create path wrote it directly) — not
		// this call's write, so created stays false.
		return false, false, entry.Revision(), nil
	case getErr == nil:
		// Holds something else (tombstoned, or a different committee/member pairing) —
		// don't overwrite a value we don't understand.
		return true, false, 0, nil
	case getErr != jetstream.ErrKeyNotFound && getErr != jetstream.ErrKeyDeleted:
		return false, false, 0, fmt.Errorf("failed to read committee member scoped forward mapping %s: %w", scopedKey, getErr)
	}

	rev, err := mappingsKV.Create(ctx, scopedKey, value)
	if err != nil {
		if isRevisionMismatchError(err) || err == jetstream.ErrKeyExists {
			return true, false, 0, nil
		}
		return false, false, 0, fmt.Errorf("failed to create committee member scoped forward mapping %s: %w", scopedKey, err)
	}
	return false, true, rev, nil
}

// tombstoneFlatForwardMappingIfUnchanged revision-guards a tombstone write for a flat
// committee_member.sfid.<token> key: it re-reads the key and only tombstones it if the
// value still matches what the scan observed, treating a value that has since changed
// (or a key that's already gone) as a conflict rather than an error, since the live
// sync-helper or another backfill run may have already handled it.
func tombstoneFlatForwardMappingIfUnchanged(ctx context.Context, flatKey string, expectedValue []byte) (conflicted bool, err error) {
	entry, getErr := mappingsKV.Get(ctx, flatKey)
	if getErr != nil {
		if getErr == jetstream.ErrKeyNotFound || getErr == jetstream.ErrKeyDeleted {
			return true, nil
		}
		return false, fmt.Errorf("failed to re-read committee member forward mapping %s: %w", flatKey, getErr)
	}
	if string(entry.Value()) != string(expectedValue) {
		return true, nil
	}
	if _, err := mappingsKV.Update(ctx, flatKey, []byte(tombstoneMarker), entry.Revision()); err != nil {
		if isRevisionMismatchError(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to tombstone committee member forward mapping %s: %w", flatKey, err)
	}
	return false, nil
}

// forwardMappingOutcome is the pre-lookup triage outcome for a scanned flat
// committee-member forward mapping entry, computed without any NATS access so it can be
// unit tested in isolation.
type forwardMappingOutcome string

const (
	forwardMappingTombstoned     forwardMappingOutcome = "tombstoned"
	forwardMappingRecordKey      forwardMappingOutcome = "recordKey"
	forwardMappingMalformed      forwardMappingOutcome = "malformed"
	forwardMappingNeedsMigration forwardMappingOutcome = "needsMigration"
)

// forwardMappingClassification is the result of classifyForwardMapping.
type forwardMappingClassification struct {
	outcome forwardMappingOutcome
	// committeeUID and memberUID are only populated when outcome is
	// forwardMappingNeedsMigration.
	committeeUID string
	memberUID    string
}

// classifyForwardMapping triages a scanned flat committee-member forward mapping
// (key token and value) into one of four outcomes, without performing any NATS access:
//
//   - forwardMappingTombstoned: explicitly tombstoned value (tombstoneMarker).
//   - forwardMappingRecordKey: the key token is a platform-community__c record sfid
//     (a UUID) — the v1-ingest forward key, globally unique, unaffected by LFXV2-2709.
//   - forwardMappingMalformed: token isn't a UUID or a valid SFID, or the value isn't
//     parseable as "committeeUID:memberUID".
//   - forwardMappingNeedsMigration: token is a contact SFID from the v2->v1 create
//     path; the caller must resolve the committee SFID to migrate it.
func classifyForwardMapping(token string, data []byte) forwardMappingClassification {
	if isTombstonedMapping(data) {
		return forwardMappingClassification{outcome: forwardMappingTombstoned}
	}
	if isUUID(token) {
		return forwardMappingClassification{outcome: forwardMappingRecordKey}
	}
	if !sfid.IsValid(token) {
		return forwardMappingClassification{outcome: forwardMappingMalformed}
	}
	committeeUID, memberUID, ok := splitTwoParts(string(data))
	if !ok || committeeUID == "" || memberUID == "" {
		return forwardMappingClassification{outcome: forwardMappingMalformed}
	}
	return forwardMappingClassification{
		outcome:      forwardMappingNeedsMigration,
		committeeUID: committeeUID,
		memberUID:    memberUID,
	}
}
