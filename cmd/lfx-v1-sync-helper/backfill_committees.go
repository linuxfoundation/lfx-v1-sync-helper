// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Backfill v1 committees that have no v2 mapping because their parent
// project was itself unmapped at the time the committee's KV message was
// processed (LFXV2-3220 removed the project allowlist; see
// backfill_projects.go for the project-side half of this gap).
//
// Mechanism: re-PUT the existing v1-objects value for
// "platform-collaboration__c.<sfid>" and let the running deployment's
// durable KV consumer (kvHandler -> handleKVPut -> handleCommitteeUpdate)
// perform the actual create. No committee-create logic is duplicated here.
//
// Why no depth ordering is needed here (unlike backfill_projects.go):
// handleCommitteeUpdate's only create-path dependency is the committee's
// parent project (project_name__c -> project.sfid.* mapping); committees do
// not depend on other committees. This flag is meant to run after
// --backfill-projects has finished settling, so every candidate's parent
// project mapping is expected to already be live — candidates whose parent
// is still unmapped are reported as skipped_parent_unmapped rather than
// resolved level by level.
//
// Why handleCommitteeUpdate drops these permanently: its parent-project miss
// unconditionally returns false (ACK, no redelivery) rather than true
// (NAK/redeliver) — see the "skipping committee creation - parent project
// not found in mappings" log site. A flat re-emit is safe: unlike projects,
// a committee's own dependents (committee members) are handled by a
// separate mapping-key lookup that itself skips (rather than hard-errors)
// on a missing parent committee, so no committee-side cascade exists to
// order around.
import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/time/rate"
)

const (
	// committeeObjectSubject is the NATS subject filter for v1 committee
	// object entries in the v1-objects KV bucket.
	committeeObjectSubject = "$KV.v1-objects.platform-collaboration__c.*"

	// committeeObjectSubjectPrefix is stripped from the subject to extract
	// the SFID.
	committeeObjectSubjectPrefix = "$KV.v1-objects.platform-collaboration__c."

	// committeeSFIDMappingKeyPrefix is the v1-mappings key prefix for
	// committee SFID -> v2 UID mappings.
	committeeSFIDMappingKeyPrefix = "committee.sfid."

	// minCommitteeMappingsSafetyFloor is a conservative lower bound on the
	// number of live+tombstoned committee SFID mappings expected in prod,
	// guarding a live run against a truncated scan or a stale mappings
	// bucket mass-re-emitting the entire v1 committee population.
	minCommitteeMappingsSafetyFloor = 100

	// committeeSettlePollInterval is the delay between settle-poll attempts
	// while waiting for an emitted committee's mapping to appear.
	committeeSettlePollInterval = 2 * time.Second

	// committeeSettlePollTimeout is the maximum time to wait for an emitted
	// committee's mapping to appear before counting it as a settle timeout.
	committeeSettlePollTimeout = 2 * time.Minute
)

// backfillCommitteesOptions configures a --backfill-committees run.
type backfillCommitteesOptions struct {
	dryRun   bool
	limit    int
	emitRate float64
	force    bool
}

// backfillCommitteesResult summarizes a backfill run.
type backfillCommitteesResult struct {
	scanned            int
	mappingsLive       int
	mappingsTombstoned int
	lowMappingsCount   bool
	candidates         int
	emitted            int
	remainingUnmapped  int
	errors             int

	skippedAlreadyMapped   int
	skippedTombstoned      int
	skippedSoftDeleted     int
	skippedEmptyValue      int
	skippedUndecodable     int
	skippedNoSFID          int
	skippedMissingRequired int
	skippedV2Authored      int
	skippedParentUnmapped  int
	skippedRevisionRace    int
	skippedLimit           int
	skippedMissing         int
	skippedSettleTimeout   int
}

// logFields returns the complete counter set as alternating key/value pairs
// for logger.With, so the dry-run report and the final summary log the same
// fields instead of each hand-maintaining its own drifting subset.
func (res backfillCommitteesResult) logFields() []any {
	return []any{
		"scanned", res.scanned,
		"mappings_live", res.mappingsLive,
		"mappings_tombstoned", res.mappingsTombstoned,
		"low_mappings_count", res.lowMappingsCount,
		"candidates", res.candidates,
		"emitted", res.emitted,
		"remaining_unmapped", res.remainingUnmapped,
		"errors", res.errors,
		"skipped_already_mapped", res.skippedAlreadyMapped,
		"skipped_tombstoned", res.skippedTombstoned,
		"skipped_soft_deleted", res.skippedSoftDeleted,
		"skipped_empty_value", res.skippedEmptyValue,
		"skipped_undecodable", res.skippedUndecodable,
		"skipped_no_sfid", res.skippedNoSFID,
		"skipped_missing_required", res.skippedMissingRequired,
		"skipped_v2_authored", res.skippedV2Authored,
		"skipped_parent_unmapped", res.skippedParentUnmapped,
		"skipped_revision_race", res.skippedRevisionRace,
		"skipped_limit", res.skippedLimit,
		"skipped_missing", res.skippedMissing,
		"skipped_settle_timeout", res.skippedSettleTimeout,
	}
}

// committeeCandidate is a v1 committee row selected for re-emission.
type committeeCandidate struct {
	sfid        string
	key         string
	projectSFID string
}

// reEmitCommitteeV1ObjectFn and lookupCommitteeProjectMappingFn are
// package-level function variables so tests can stub the NATS-dependent
// emit/poll steps without a live cluster.
var (
	reEmitCommitteeV1ObjectFn     = reEmitCommitteeV1Object
	lookupCommitteeMappingFn      = lookupCommitteeMapping
	lookupCommitteeProjectMapping = lookupProjectMappingErr
)

// backfillCommittees scans v1-objects for platform-collaboration__c rows
// with no live v1-mappings entry, then re-emits every candidate whose
// parent project is already mapped so the running deployment's KV consumer
// creates each committee in v2.
func backfillCommittees(ctx context.Context, opts backfillCommitteesOptions) (backfillCommitteesResult, error) {
	res := backfillCommitteesResult{}

	if opts.emitRate <= 0 {
		return res, fmt.Errorf("--emit-rate must be greater than 0, got %v", opts.emitRate)
	}
	if opts.limit < 0 {
		return res, fmt.Errorf("--limit must be 0 (unlimited) or greater, got %d", opts.limit)
	}
	if cfg.Auth0ClientID == "" {
		return res, fmt.Errorf(
			"AUTH0_CLIENT_ID is not set — shouldSkipSync cannot distinguish v2-authored rows " +
				"from genuine v1 changes without it, which would silently disable loop prevention",
		)
	}

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	objects, err := ScanSubjectData(ctx, jsContext, kvObjectsStream, committeeObjectSubject, opTimeout)
	if err != nil {
		return res, fmt.Errorf("failed to scan v1 committee objects: %w", err)
	}
	res.scanned = len(objects)

	liveMappings, tombstonedMappings, err := collectCommitteeSFIDMappingStates(ctx)
	if err != nil {
		return res, fmt.Errorf("failed to collect committee SFID mappings: %w", err)
	}
	res.mappingsLive = len(liveMappings)
	res.mappingsTombstoned = len(tombstonedMappings)

	if !opts.dryRun && !opts.force && res.mappingsLive+res.mappingsTombstoned < minCommitteeMappingsSafetyFloor {
		return res, fmt.Errorf(
			"only %d live + %d tombstoned committee SFID mappings found (expected at least %d) — "+
				"this looks like a truncated scan or a stale mappings bucket, not a genuine near-empty "+
				"environment; pass --force to proceed anyway",
			res.mappingsLive, res.mappingsTombstoned, minCommitteeMappingsSafetyFloor,
		)
	}
	res.lowMappingsCount = res.mappingsLive+res.mappingsTombstoned < minCommitteeMappingsSafetyFloor

	candidates := selectCommitteeCandidates(ctx, objects, liveMappings, tombstonedMappings, &res)
	res.candidates = len(candidates)
	// Captured before the emit loop runs: selectCommitteeCandidates is the
	// only mutator of skippedParentUnmapped so far, so this reflects
	// exactly the rows excluded from candidates for that reason (they never
	// appear in the candidates slice, so len(candidates) alone undercounts
	// remaining work). The emit loop below can also increment
	// skippedParentUnmapped (a fresh re-check finding the parent still
	// unmapped), but those candidates DO remain in the candidates slice, so
	// counting them via res.skippedParentUnmapped again at that point would
	// double-count them alongside the settled-state sweep.
	scanTimeParentUnmapped := res.skippedParentUnmapped
	res.remainingUnmapped = len(candidates) + scanTimeParentUnmapped

	if opts.dryRun {
		logger.With(res.logFields()...).InfoContext(ctx, "[dry-run] committee backfill candidate report")
		if res.errors > 0 {
			// selectCommitteeCandidates recorded transient parent-lookup
			// failures: the candidate/remaining_unmapped counts above are
			// incomplete, so a caller checking only the error return must not
			// see this dry-run reported as clean.
			return res, fmt.Errorf("committee backfill dry-run completed with %d errors", res.errors)
		}
		return res, nil
	}

	limiter := rate.NewLimiter(rate.Limit(opts.emitRate), 1)

	settled := make(map[string]bool, len(candidates))
	var emitted []string
	limitReached := false
	for _, c := range candidates {
		if limitReached {
			res.skippedLimit++
			continue
		}
		if opts.limit > 0 && res.emitted >= opts.limit {
			limitReached = true
			res.skippedLimit++
			continue
		}
		if err := limiter.Wait(ctx); err != nil {
			return res, fmt.Errorf("rate limiter wait cancelled: %w", err)
		}

		skipReason, err := reEmitCommitteeV1ObjectFn(ctx, c.key)
		if err != nil {
			res.errors++
			logger.With(errKey, err, "sfid", c.sfid, "key", c.key).ErrorContext(ctx, "failed to re-emit committee for backfill")
			continue
		}
		if skipReason != "" {
			recordCommitteeSkip(&res, skipReason)
			switch skipReason {
			case "already_mapped":
				// reEmitCommitteeV1Object's own mapping lookup just confirmed
				// a live mapping exists, so this candidate is resolved
				// without needing to be settle-polled below.
				settled[c.sfid] = true
			case "revision_race":
				// The object write lost to a concurrent update, which may
				// itself be creating the mapping. Settle-poll it like a
				// normal emission instead of leaving it permanently
				// unresolved.
				emitted = append(emitted, c.sfid)
			}
			continue
		}
		res.emitted++
		emitted = append(emitted, c.sfid)
	}

	nowSettled := settleCommitteePoll(ctx, emitted, committeeSettlePollTimeout)
	for _, sfid := range emitted {
		if nowSettled[sfid] {
			settled[sfid] = true
		} else {
			res.skippedSettleTimeout++
		}
	}

	// remainingUnmapped was seeded from the full candidate set before
	// emission; recompute it from final settle state over the full candidate
	// set (not just emitted) so it also reflects candidates dropped by the
	// limit, emit errors, or a re-check skip discovered on fresh read (e.g.
	// tombstoned/parent_unmapped/missing) — an operator reading only this
	// field should never see 0 while committees remain unmapped. Seed from
	// the captured scan-time count, not the live res.skippedParentUnmapped:
	// a fresh re-check parent_unmapped candidate stays in the candidates
	// slice and unsettled, so the loop below already counts it once — adding
	// the live (emit-loop-mutated) counter here would double-count it.
	notSettled := scanTimeParentUnmapped
	for _, c := range candidates {
		if !settled[c.sfid] {
			notSettled++
		}
	}
	res.remainingUnmapped = notSettled

	if res.errors > 0 || res.skippedSettleTimeout > 0 {
		return res, fmt.Errorf(
			"committee backfill completed with %d errors and %d settle timeouts",
			res.errors, res.skippedSettleTimeout,
		)
	}
	return res, nil
}

// recordCommitteeSkip increments the counter matching a skip reason string
// returned by reEmitCommitteeV1Object's re-applied filters.
func recordCommitteeSkip(res *backfillCommitteesResult, reason string) {
	switch reason {
	case "already_mapped":
		res.skippedAlreadyMapped++
	case "tombstoned_mapping":
		res.skippedTombstoned++
	case "soft_deleted":
		res.skippedSoftDeleted++
	case "empty_value":
		res.skippedEmptyValue++
	case "undecodable":
		res.skippedUndecodable++
	case "no_sfid":
		res.skippedNoSFID++
	case "missing_required":
		res.skippedMissingRequired++
	case "v2_authored":
		res.skippedV2Authored++
	case "revision_race":
		res.skippedRevisionRace++
	case "missing":
		res.skippedMissing++
	case "parent_unmapped":
		res.skippedParentUnmapped++
	}
}

// decodeV1CommitteeRow decodes a raw v1-objects value, trying JSON then
// msgpack, matching handleKVPut's dual-format handling.
func decodeV1CommitteeRow(raw []byte) (map[string]any, error) {
	var v1Data map[string]any
	if err := json.Unmarshal(raw, &v1Data); err != nil {
		if mpErr := msgpack.Unmarshal(raw, &v1Data); mpErr != nil {
			return nil, fmt.Errorf("decode: json=%v msgpack=%v", err, mpErr)
		}
	}
	return v1Data, nil
}

// evaluateCommitteeRow applies the same pre-filters as handleKVPut and
// handleCommitteeUpdate (in the same order) to a decoded v1 committee row,
// so candidate selection and the emit step's re-filter never count or emit
// a row the live consumer would drop. Returns a non-empty skipReason when
// the row should not be treated as a candidate.
func evaluateCommitteeRow(ctx context.Context, key string, raw []byte) (committeeCandidate, string) {
	if len(raw) == 0 {
		return committeeCandidate{}, "empty_value"
	}

	v1Data, err := decodeV1CommitteeRow(raw)
	if err != nil {
		return committeeCandidate{}, "undecodable"
	}

	if v1RowIsSoftDeleted(v1Data) {
		return committeeCandidate{}, "soft_deleted"
	}

	if shouldSkipSync(ctx, v1Data) {
		return committeeCandidate{}, "v2_authored"
	}

	sfid, _ := v1Data["sfid"].(string)
	sfid = strings.TrimSpace(sfid)
	if sfid == "" {
		return committeeCandidate{}, "no_sfid"
	}

	name, _ := v1Data["mailing_list__c"].(string)
	projectSFID, _ := v1Data["project_name__c"].(string)
	projectSFID = strings.TrimSpace(projectSFID)
	if strings.TrimSpace(name) == "" || projectSFID == "" {
		return committeeCandidate{}, "missing_required"
	}

	return committeeCandidate{
		sfid:        sfid,
		key:         key,
		projectSFID: projectSFID,
	}, ""
}

// selectCommitteeCandidates builds the unmapped-committee candidate set:
// live object rows minus live and tombstoned committee.sfid mappings, then
// filtered through evaluateCommitteeRow and a parent-project-mapped check.
func selectCommitteeCandidates(
	ctx context.Context,
	objects map[string][]byte,
	liveMappings map[string]string,
	tombstonedMappings map[string]struct{},
	res *backfillCommitteesResult,
) []committeeCandidate {
	candidates := make([]committeeCandidate, 0, len(objects))

	for subject, raw := range objects {
		if !strings.HasPrefix(subject, committeeObjectSubjectPrefix) {
			continue
		}
		sfid := subject[len(committeeObjectSubjectPrefix):]
		key := "platform-collaboration__c." + sfid

		if _, ok := tombstonedMappings[sfid]; ok {
			res.skippedTombstoned++
			continue
		}
		if _, ok := liveMappings[sfid]; ok {
			res.skippedAlreadyMapped++
			continue
		}

		candidate, skipReason := evaluateCommitteeRow(ctx, key, raw)
		if skipReason != "" {
			recordCommitteeSkip(res, skipReason)
			continue
		}

		parentMapped, err := lookupCommitteeProjectMapping(ctx, candidate.projectSFID)
		if err != nil {
			// A transient lookup failure is not the same as "no parent
			// mapping": treating it as absent would silently drop an
			// otherwise-eligible committee from this run without recording
			// an error.
			res.errors++
			continue
		}
		if !parentMapped {
			res.skippedParentUnmapped++
			continue
		}

		candidates = append(candidates, candidate)
	}

	// objects is a Go map, so ranging over it above yields candidates in
	// nondeterministic order across runs. Sort by sfid so --limit
	// truncation and --dry-run output are reproducible run-to-run.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].sfid < candidates[j].sfid })

	return candidates
}

// collectCommitteeSFIDMappingStates reads all committee.sfid.* keys from
// KV_v1-mappings and returns both live (SFID -> v2 UID) and tombstoned
// (SFID set) mappings.
func collectCommitteeSFIDMappingStates(ctx context.Context) (map[string]string, map[string]struct{}, error) {
	const (
		kvMappingsStream           = "KV_v1-mappings"
		committeeSFIDSubject       = "$KV.v1-mappings.committee.sfid.*"
		committeeSFIDSubjectPrefix = "$KV.v1-mappings.committee.sfid."
	)

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	subjectData, err := ScanSubjectData(ctx, jsContext, kvMappingsStream, committeeSFIDSubject, opTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan committee SFID mappings: %w", err)
	}

	live := make(map[string]string, len(subjectData))
	tombstoned := make(map[string]struct{})

	for subject, data := range subjectData {
		if !strings.HasPrefix(subject, committeeSFIDSubjectPrefix) {
			continue
		}
		sfid := subject[len(committeeSFIDSubjectPrefix):]
		switch {
		case len(data) == 0:
			continue
		case isTombstonedMapping(data):
			tombstoned[sfid] = struct{}{}
		default:
			live[sfid] = string(data)
		}
	}

	return live, tombstoned, nil
}

// reEmitCommitteeV1Object re-reads the current v1-objects value for key,
// re-applies the same filters as the live consumer would (including a fresh
// parent-project-mapped check), and (if the row still qualifies) writes it
// back with a revision-checked Update so the durable KV consumer redelivers
// it and creates the committee. Never replays bytes from an earlier scan:
// the objects scan can run for a long time, and replaying stale bytes could
// clobber a newer WAL write.
//
// Returns a non-empty skipReason (see recordCommitteeSkip) instead of an
// error for every outcome that is not itself a failure: the row
// disappearing, no longer qualifying, or losing a concurrent-write race are
// all expected, re-run-safe outcomes.
func reEmitCommitteeV1Object(ctx context.Context, key string) (skipReason string, err error) {
	entry, err := v1KV.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
			return "missing", nil
		}
		return "", fmt.Errorf("failed to re-read %s: %w", key, err)
	}

	candidate, reason := evaluateCommitteeRow(ctx, key, entry.Value())
	if reason != "" {
		return reason, nil
	}

	// The scan-time parent check can go stale on a long, rate-limited run —
	// re-verify against the current mapping state before emitting.
	parentMapped, err := lookupCommitteeProjectMapping(ctx, candidate.projectSFID)
	if err != nil {
		return "", fmt.Errorf("failed to look up parent project mapping for %s: %w", candidate.sfid, err)
	}
	if !parentMapped {
		return "parent_unmapped", nil
	}

	mappingKey := committeeSFIDMappingKeyPrefix + candidate.sfid
	mapping, mErr := mappingsKV.Get(ctx, mappingKey)
	switch mErr {
	case nil:
		if isTombstonedMapping(mapping.Value()) {
			return "tombstoned_mapping", nil
		}
		if len(mapping.Value()) > 0 {
			return "already_mapped", nil
		}
	case jetstream.ErrKeyNotFound, jetstream.ErrKeyDeleted:
		// No mapping exists yet — genuinely unmapped, proceed to re-emit.
	default:
		// A transient lookup failure is not the same as "no mapping":
		// treating it as absent could re-emit a committee that's already
		// mapped, sending the durable consumer down a duplicate-create path.
		return "", fmt.Errorf("failed to look up mapping %s: %w", mappingKey, mErr)
	}

	if _, err := v1KV.Update(ctx, key, entry.Value(), entry.Revision()); err != nil {
		if isRevisionMismatchError(err) {
			return "revision_race", nil
		}
		return "", fmt.Errorf("failed to re-emit %s: %w", key, err)
	}

	return "", nil
}

// lookupCommitteeMapping reports whether a live (non-tombstoned)
// committee.sfid mapping exists for sfid.
func lookupCommitteeMapping(ctx context.Context, sfid string) bool {
	entry, err := mappingsKV.Get(ctx, committeeSFIDMappingKeyPrefix+sfid)
	if err != nil {
		return false
	}
	return len(entry.Value()) > 0 && !isTombstonedMapping(entry.Value())
}

// settleCommitteePoll polls lookupCommitteeMappingFn for every sfid in sfids
// until each either settles (mapping appears) or timeout elapses, returning
// which ones settled.
func settleCommitteePoll(ctx context.Context, sfids []string, timeout time.Duration) map[string]bool {
	settled := make(map[string]bool, len(sfids))
	if len(sfids) == 0 {
		return settled
	}

	deadline := time.Now().Add(timeout)
	pending := append([]string{}, sfids...)

	for len(pending) > 0 {
		// Bound each lookup to the overall settle deadline: without this, a
		// NATS outage lets every sequential mappingsKV.Get in this round
		// block on the parent (unbounded) context, and the deadline check
		// below never runs until all of them return.
		lookupCtx, cancel := context.WithDeadline(ctx, deadline)
		var stillPending []string
		for _, sfid := range pending {
			if lookupCommitteeMappingFn(lookupCtx, sfid) {
				settled[sfid] = true
			} else {
				stillPending = append(stillPending, sfid)
			}
		}
		cancel()
		pending = stillPending
		if len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return settled
		case <-time.After(committeeSettlePollInterval):
		}
	}

	return settled
}
