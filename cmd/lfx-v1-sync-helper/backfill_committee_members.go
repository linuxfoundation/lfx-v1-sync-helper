// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// --backfill-committee-members (lfx-self-serve-ops#2100) re-emits v1
// platform-community__c rows that have no committee_member.sfid.* mapping but
// whose committee is mapped. Like --backfill-committees it re-PUTs the existing
// v1-objects value, so the running deployment's KV consumer
// (handleCommitteeMemberUpdate) does the create.
//
// Most of these members were rejected by committee-service because their
// committee requires an organization (enable_voting or business_email_required)
// and the member has none, has a non-B2B org without a domain, or its v1 org
// lookup failed. The handler ACKs that 400, so the member is never retried.
// With --relax-committee-settings, each such committee is processed alone:
// record the current v2 settings in a durable hold (committee_settings_hold.go),
// turn off the blocking settings, re-emit the members, wait for their mappings,
// then restore the recorded values even if an earlier step failed. A later run
// first restores every hold an interrupted run left behind.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	sfutil "github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/time/rate"
)

const (
	committeeMemberObjectSubject       = "$KV.v1-objects.platform-community__c.*"
	committeeMemberObjectSubjectPrefix = "$KV.v1-objects.platform-community__c."
	committeeMemberObjectKeyPrefix     = "platform-community__c."
	committeeMemberSFIDMappingPrefix   = "committee_member.sfid."
	kvMappingsSubjectPrefix            = "$KV.v1-mappings."

	defaultMemberSettleTimeout  = 3 * time.Minute
	memberSettlePollInterval    = 2 * time.Second
	committeeSettingMaxAttempts = 3
)

// Member causes. The first three are reported only and never emitted.
const (
	memberCauseDeletedCommittee  = "deleted_committee"
	memberCauseCommitteeUnmapped = "committee_unmapped"
	memberCauseDuplicateContact  = "duplicate_contact"
	memberCauseCommitteeNotInV2  = "committee_not_in_v2"
	memberCauseOrgMissing        = "org_missing"
	memberCauseOrgNotB2BNoDomain = "org_not_b2b_no_domain"
	memberCauseOrgLookupFailed   = "org_lookup_failed"
	memberCauseOrgLookupError    = "org_lookup_error"
	memberCauseNotBlocked        = "not_blocked"
)

// memberCauseOrder fixes the order of the per-cause summary fields.
var memberCauseOrder = []string{
	memberCauseOrgMissing, memberCauseOrgNotB2BNoDomain, memberCauseOrgLookupFailed, memberCauseNotBlocked, memberCauseOrgLookupError,
	memberCauseDeletedCommittee, memberCauseCommitteeUnmapped, memberCauseDuplicateContact, memberCauseCommitteeNotInV2,
}

// memberCauseBlocked reports whether a cause needs the committee's settings relaxed.
func memberCauseBlocked(cause string) bool {
	return cause == memberCauseOrgMissing || cause == memberCauseOrgNotB2BNoDomain || cause == memberCauseOrgLookupFailed
}

type backfillCommitteeMembersOptions struct {
	dryRun          bool
	relaxSettings   bool
	committees      []string // committee SFIDs or v2 UIDs; empty means all
	excludeProjects []string // project SFIDs
	limit           int      // committees; 0 = unlimited
	emitRate        float64
	settleTimeout   time.Duration
}

// memberRow is a platform-community__c row that passed the live handler's pre-filters.
type memberRow struct {
	key           string
	sfid          string
	committeeSFID string
	contactSFID   string
	accountSFID   string
	projectSFID   string
	cause         string
}

// memberCommittee groups the unmapped members of one mapped committee.
type memberCommittee struct {
	sfid                  string
	uid                   string
	projectSFID           string
	enableVoting          bool
	businessEmailRequired bool
	members               []memberRow
}

func (c *memberCommittee) hasBlocked() bool {
	for _, m := range c.members {
		if memberCauseBlocked(m.cause) {
			return true
		}
	}
	return false
}

func (c *memberCommittee) causeCounts() map[string]int {
	counts := map[string]int{}
	for _, m := range c.members {
		counts[m.cause]++
	}
	return counts
}

type backfillCommitteeMembersResult struct {
	dryRun bool

	scanned            int
	skippedEmptyValue  int
	skippedUndecodable int
	skippedSoftDeleted int
	skippedV2Authored  int
	skippedNoSFID      int
	skippedBlankEmail  int
	skippedNoCommittee int
	skippedMapped      int
	skippedTombstoned  int
	candidates         int
	causes             map[string]int
	committees         int
	committeesFiltered int

	committeesProcessed      int
	committeesRelaxed        int
	committeesSkippedBlocked int
	skippedBlockedMembers    int
	skippedLimit             int
	emitted                  int
	settled                  int
	unsettled                int
	emitSkipped              int
	holdsPending             int
	holdsResumed             int
	restoreFailures          int
	verified                 int
	verifyMismatches         int
	errors                   int
}

func (r *backfillCommitteeMembersResult) logFields() []any {
	fields := []any{
		"dry_run", r.dryRun,
		"scanned", r.scanned,
		"candidates", r.candidates,
		"committees", r.committees,
		"committees_filtered", r.committeesFiltered,
		"committees_processed", r.committeesProcessed,
		"committees_relaxed", r.committeesRelaxed,
		"committees_skipped_blocked", r.committeesSkippedBlocked,
		"skipped_blocked_members", r.skippedBlockedMembers,
		"skipped_limit", r.skippedLimit,
		"emitted", r.emitted,
		"settled", r.settled,
		"unsettled", r.unsettled,
		"emit_skipped", r.emitSkipped,
		"holds_pending", r.holdsPending,
		"holds_resumed", r.holdsResumed,
		"restore_failures", r.restoreFailures,
		"verified", r.verified,
		"verify_mismatches", r.verifyMismatches,
		"errors", r.errors,
		"skipped_empty_value", r.skippedEmptyValue,
		"skipped_undecodable", r.skippedUndecodable,
		"skipped_soft_deleted", r.skippedSoftDeleted,
		"skipped_v2_authored", r.skippedV2Authored,
		"skipped_no_sfid", r.skippedNoSFID,
		"skipped_blank_email", r.skippedBlankEmail,
		"skipped_no_committee", r.skippedNoCommittee,
		"skipped_mapped", r.skippedMapped,
		"skipped_tombstoned", r.skippedTombstoned,
	}
	for _, cause := range memberCauseOrder {
		fields = append(fields, "cause_"+cause, r.causes[cause])
	}
	return fields
}

func (r *backfillCommitteeMembersResult) recordSkip(reason string) {
	switch reason {
	case "empty_value":
		r.skippedEmptyValue++
	case "undecodable":
		r.skippedUndecodable++
	case "soft_deleted":
		r.skippedSoftDeleted++
	case "v2_authored":
		r.skippedV2Authored++
	case "no_sfid":
		r.skippedNoSFID++
	case "blank_email":
		r.skippedBlankEmail++
	case "no_committee":
		r.skippedNoCommittee++
	}
}

// Seams for tests: every network-dependent step goes through one of these.
var (
	scanSubjectDataFn               = ScanSubjectData
	memberBackfillLookupOrgFn       = lookupV1Org
	memberBackfillIsB2BOrgFn        = isB2BOrgAccount
	reEmitCommitteeMemberV1ObjectFn = reEmitCommitteeMemberV1Object
	lookupCommitteeMemberMappingFn  = lookupCommitteeMemberMapping
	listMappingsWithPrefixFn        = listMappingsWithPrefix
	updateCommitteeBaseFn           = updateCommitteeBaseAPI
	updateCommitteeSettingsFn       = updateCommitteeSettingsAPI
)

// backfillCommitteeMembers runs the dry run or the live pass.
func backfillCommitteeMembers(ctx context.Context, opts backfillCommitteeMembersOptions) (backfillCommitteeMembersResult, error) {
	res := backfillCommitteeMembersResult{dryRun: opts.dryRun, causes: map[string]int{}}

	if opts.emitRate <= 0 {
		return res, fmt.Errorf("--emit-rate must be greater than 0, got %v", opts.emitRate)
	}
	if opts.limit < 0 {
		return res, fmt.Errorf("--limit must be 0 (unlimited) or greater, got %d", opts.limit)
	}
	if opts.settleTimeout <= 0 {
		return res, fmt.Errorf("--settle-timeout must be greater than 0, got %v", opts.settleTimeout)
	}
	if cfg.Auth0ClientID == "" {
		return res, fmt.Errorf("AUTH0_CLIENT_ID is not set — shouldSkipSync cannot recognize v2-authored rows without it")
	}

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	objects, err := scanSubjectDataFn(ctx, jsContext, kvObjectsStream, committeeMemberObjectSubject, opTimeout)
	if err != nil {
		return res, fmt.Errorf("failed to scan v1 committee member objects: %w", err)
	}
	memberMappings, err := listMappingsWithPrefixFn(ctx, committeeMemberSFIDMappingPrefix)
	if err != nil {
		return res, fmt.Errorf("failed to list committee member mappings: %w", err)
	}
	committeeMappings, err := listMappingsWithPrefixFn(ctx, committeeSFIDMappingKeyPrefix)
	if err != nil {
		return res, fmt.Errorf("failed to list committee mappings: %w", err)
	}
	committeeLive, committeeTombstoned := splitCommitteeMappings(committeeMappings)

	rows := selectMemberCandidates(ctx, objects, memberMappings, &res)
	committees := groupMemberCandidates(rows, committeeLive, committeeTombstoned, &res)
	committees = filterMemberCommittees(committees, opts, &res)
	classifyMemberCommittees(ctx, committees, &res)
	for _, c := range committees {
		counts := c.causeCounts()
		fields := []any{
			"committee_sfid", c.sfid, "committee_uid", c.uid, "project_sfid", c.projectSFID,
			"enable_voting", c.enableVoting, "business_email_required", c.businessEmailRequired,
			"members", len(c.members),
		}
		for _, cause := range memberCauseOrder {
			if counts[cause] > 0 {
				fields = append(fields, "cause_"+cause, counts[cause])
			}
		}
		logger.With(fields...).InfoContext(ctx, "committee with unmapped v1 members")
	}

	// Only one live run at a time: resume below assumes every unrestored hold
	// was abandoned. The lease is taken before the holds are listed, so no
	// other run can add a hold this run does not see. Losing the lease cancels
	// the run; the committee in progress is still restored on a detached
	// context. Dry runs only read, so they take no lease.
	if !opts.dryRun {
		lease, err := acquireMemberBackfillLock(ctx)
		if err != nil {
			return res, err
		}
		defer lease.Release(context.WithoutCancel(ctx))
		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()
		go func() {
			select {
			case <-lease.Lost:
				cancelRun()
			case <-runCtx.Done():
			}
		}()
		ctx = runCtx
	}

	holds, err := listCommitteeSettingsHolds(ctx)
	if err != nil {
		return res, err
	}
	var pending []*committeeSettingsHold
	for _, h := range holds {
		if h.hold.State != holdStateRestored {
			pending = append(pending, h.hold)
			res.holdsPending++
			logger.With("committee_uid", h.hold.CommitteeUID, "committee_sfid", h.hold.CommitteeSFID, "state", h.hold.State).
				WarnContext(ctx, "committee settings hold from an earlier run is not restored")
		}
	}

	if opts.dryRun {
		logger.With(res.logFields()...).InfoContext(ctx, "committee member backfill dry run")
		if res.errors > 0 {
			return res, fmt.Errorf("dry run found %d errors", res.errors)
		}
		return res, nil
	}

	// Resume: restore every hold an interrupted run left behind before
	// starting anything new. Each restore runs on a detached, bounded context
	// like the per-committee restore, so a SIGTERM or lost lease cannot cut
	// it short. One failure does not stop the others from being restored, but
	// the run does not start new committees while any hold is unrestored.
	var resumeFailures []string
	for _, h := range holds {
		if h.hold.State == holdStateRestored {
			continue
		}
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), committeeRestoreTimeout)
		err := restoreCommitteeSettings(restoreCtx, h.hold.CommitteeUID)
		cancel()
		if err != nil {
			res.restoreFailures++
			resumeFailures = append(resumeFailures, h.hold.CommitteeUID)
			logger.With(errKey, err, "committee_uid", h.hold.CommitteeUID, "committee_sfid", h.hold.CommitteeSFID).
				ErrorContext(ctx, "failed to restore committee settings from an earlier run")
			continue
		}
		res.holdsResumed++
	}
	if len(resumeFailures) > 0 {
		return res, fmt.Errorf("%w for %d committee(s) from an earlier run: %s", errCommitteeRestoreFailed, len(resumeFailures), strings.Join(resumeFailures, ", "))
	}
	if ctx.Err() != nil {
		return res, ctx.Err()
	}

	limiter := rate.NewLimiter(rate.Limit(opts.emitRate), 1)
	var touched []string
	for _, c := range committees {
		if opts.limit > 0 && res.committeesProcessed >= opts.limit {
			res.skippedLimit++
			continue
		}
		relaxed, err := processMemberCommittee(ctx, c, opts, limiter, &res)
		if relaxed {
			touched = append(touched, c.uid)
		}
		if errors.Is(err, errCommitteeRestoreFailed) {
			res.restoreFailures++
			logger.With(errKey, err, "committee_uid", c.uid, "committee_sfid", c.sfid).
				ErrorContext(ctx, "committee settings restore failed; stopping the run, re-run to restore")
			return res, err
		}
		if err != nil {
			res.errors++
			logger.With(errKey, err, "committee_uid", c.uid, "committee_sfid", c.sfid).
				ErrorContext(ctx, "committee member backfill failed for committee")
		}
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
	}

	for _, h := range pending {
		touched = append(touched, h.CommitteeUID)
	}
	verifyCommitteeSettingsHolds(ctx, touched, &res)

	logger.With(res.logFields()...).InfoContext(ctx, "committee member backfill summary")
	switch {
	case res.errors > 0:
		return res, fmt.Errorf("committee member backfill finished with %d errors", res.errors)
	case res.unsettled > 0:
		return res, fmt.Errorf("%d re-emitted members did not get a mapping; check the deployment logs for \"failed to sync committee member\"", res.unsettled)
	case res.verifyMismatches > 0:
		return res, fmt.Errorf("%d committees do not match their recorded settings", res.verifyMismatches)
	}
	return res, nil
}

// evaluateMemberRow applies handleKVPut's and handleCommitteeMemberUpdate's
// pre-filters, in their order, so the pass never counts or emits a row the
// live consumer would drop.
func evaluateMemberRow(ctx context.Context, key string, raw []byte) (memberRow, string) {
	if len(raw) == 0 {
		return memberRow{}, "empty_value"
	}
	data, err := decodeV1CommitteeRow(raw)
	if err != nil {
		return memberRow{}, "undecodable"
	}
	if v1RowIsSoftDeleted(data) {
		return memberRow{}, "soft_deleted"
	}
	if shouldSkipSync(ctx, data) {
		return memberRow{}, "v2_authored"
	}
	sfid, _ := data["sfid"].(string)
	if sfid == "" {
		return memberRow{}, "no_sfid"
	}
	if email, _ := data["contactemail__c"].(string); email == "" {
		return memberRow{}, "blank_email"
	}
	committeeSFID, _ := data["collaboration_name__c"].(string)
	if committeeSFID == "" {
		return memberRow{}, "no_committee"
	}
	contactSFID, _ := data["contact_name__c"].(string)
	accountSFID, _ := data["account__c"].(string)
	projectSFID, _ := data["project_name__c"].(string)
	return memberRow{
		key:           key,
		sfid:          sfid,
		committeeSFID: committeeSFID,
		contactSFID:   strings.TrimSpace(contactSFID),
		accountSFID:   strings.TrimSpace(accountSFID),
		projectSFID:   strings.TrimSpace(projectSFID),
	}, ""
}

// splitCommitteeMappings turns committee.sfid.* mappings into live
// (sfid -> uid) and tombstoned sets.
func splitCommitteeMappings(mappings map[string][]byte) (map[string]string, map[string]struct{}) {
	live := map[string]string{}
	tombstoned := map[string]struct{}{}
	for key, value := range mappings {
		sfid := strings.TrimPrefix(key, committeeSFIDMappingKeyPrefix)
		switch {
		case len(value) == 0:
		case isTombstonedMapping(value):
			tombstoned[sfid] = struct{}{}
		default:
			live[sfid] = string(value)
		}
	}
	return live, tombstoned
}

// listMappingsWithPrefix returns every mapping whose key starts with prefix,
// keyed by mapping key, from the backend mappingStore reads: Postgres in
// postgres mode (the KV bucket is no longer written there), otherwise the KV
// bucket, which is authoritative in kv and dual modes. Tombstones are returned
// as the tombstone sentinel.
func listMappingsWithPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	if pg, ok := mappingStore.(*pgMappingStore); ok {
		return pg.ListPrefix(ctx, prefix)
	}
	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}
	data, err := scanSubjectDataFn(ctx, jsContext, "KV_v1-mappings", kvMappingsSubjectPrefix+prefix+"*", opTimeout)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte, len(data))
	for subject, value := range data {
		result[strings.TrimPrefix(subject, kvMappingsSubjectPrefix)] = value
	}
	return result, nil
}

// selectMemberCandidates returns the rows with no live or tombstoned member
// mapping. Rows that already have a live mapping are kept as committee+contact
// pairs so a second v1 row for the same person can be reported as a duplicate.
func selectMemberCandidates(ctx context.Context, objects, memberMappings map[string][]byte, res *backfillCommitteeMembersResult) []memberRow {
	res.scanned = len(objects)
	mapped := map[string]bool{}
	var unmapped []memberRow
	for subject, raw := range objects {
		if !strings.HasPrefix(subject, committeeMemberObjectSubjectPrefix) {
			continue
		}
		key := committeeMemberObjectKeyPrefix + strings.TrimPrefix(subject, committeeMemberObjectSubjectPrefix)
		row, reason := evaluateMemberRow(ctx, key, raw)
		if reason != "" {
			res.recordSkip(reason)
			continue
		}
		mapping, ok := memberMappings[committeeMemberSFIDMappingPrefix+row.sfid]
		switch {
		case ok && isTombstonedMapping(mapping):
			res.skippedTombstoned++
		case ok && len(mapping) > 0:
			res.skippedMapped++
			mapped[row.committeeSFID+":"+row.contactSFID] = true
		default:
			unmapped = append(unmapped, row)
		}
	}
	sort.Slice(unmapped, func(i, j int) bool { return unmapped[i].sfid < unmapped[j].sfid })

	// A second unmapped row for the same committee and contact would be
	// rejected as a duplicate member, so only the first one is a candidate.
	var rows []memberRow
	for _, row := range unmapped {
		pair := row.committeeSFID + ":" + row.contactSFID
		if row.contactSFID != "" && mapped[pair] {
			row.cause = memberCauseDuplicateContact
		} else if row.contactSFID != "" {
			mapped[pair] = true
		}
		rows = append(rows, row)
	}
	res.candidates = len(rows)
	return rows
}

// groupMemberCandidates groups candidates by mapped committee. Members of
// deleted or unmapped committees and duplicates are counted, not grouped.
func groupMemberCandidates(rows []memberRow, committeeLive map[string]string, committeeTombstoned map[string]struct{}, res *backfillCommitteeMembersResult) []*memberCommittee {
	bySFID := map[string]*memberCommittee{}
	for _, row := range rows {
		switch {
		case row.cause == memberCauseDuplicateContact:
		case hasKey(committeeTombstoned, row.committeeSFID):
			row.cause = memberCauseDeletedCommittee
		case committeeLive[row.committeeSFID] == "":
			row.cause = memberCauseCommitteeUnmapped
		}
		if row.cause != "" {
			res.causes[row.cause]++
			continue
		}
		c := bySFID[row.committeeSFID]
		if c == nil {
			c = &memberCommittee{sfid: row.committeeSFID, uid: committeeLive[row.committeeSFID], projectSFID: row.projectSFID}
			bySFID[row.committeeSFID] = c
		}
		c.members = append(c.members, row)
	}
	committees := make([]*memberCommittee, 0, len(bySFID))
	for _, c := range bySFID {
		committees = append(committees, c)
	}
	sort.Slice(committees, func(i, j int) bool { return committees[i].sfid < committees[j].sfid })
	res.committees = len(committees)
	return committees
}

func hasKey(set map[string]struct{}, key string) bool {
	_, ok := set[key]
	return ok
}

// filterMemberCommittees applies --committee and --exclude-project.
func filterMemberCommittees(committees []*memberCommittee, opts backfillCommitteeMembersOptions, res *backfillCommitteeMembersResult) []*memberCommittee {
	include := map[string]bool{}
	for _, id := range opts.committees {
		include[id] = true
	}
	exclude := map[string]bool{}
	for _, id := range opts.excludeProjects {
		exclude[id] = true
	}
	var kept []*memberCommittee
	for _, c := range committees {
		if (len(include) > 0 && !include[c.sfid] && !include[c.uid]) || exclude[c.projectSFID] {
			res.committeesFiltered++
			continue
		}
		kept = append(kept, c)
	}
	return kept
}

// classifyMemberCommittees reads each committee's current v2 settings and
// gives every member its cause.
func classifyMemberCommittees(ctx context.Context, committees []*memberCommittee, res *backfillCommitteeMembersResult) {
	for _, c := range committees {
		enableVoting, businessEmailRequired, err := readCommitteeOrgSettings(ctx, c.uid)
		var notFound *committeeservice.NotFoundError
		if errors.As(err, &notFound) {
			for i := range c.members {
				c.members[i].cause = memberCauseCommitteeNotInV2
			}
		} else if err != nil {
			res.errors++
			logger.With(errKey, err, "committee_uid", c.uid, "committee_sfid", c.sfid).
				ErrorContext(ctx, "failed to read committee settings; leaving its members unclassified")
			for i := range c.members {
				c.members[i].cause = memberCauseCommitteeNotInV2
			}
			continue
		}
		c.enableVoting, c.businessEmailRequired = enableVoting, businessEmailRequired
		requiresOrg := enableVoting || businessEmailRequired
		for i := range c.members {
			if c.members[i].cause == "" {
				c.members[i].cause = classifyMemberOrg(ctx, c.members[i], requiresOrg, res)
			}
			res.causes[c.members[i].cause]++
		}
	}
}

// readCommitteeOrgSettings returns the two settings that make committee-service
// require a member organization. A committee with no settings record has
// business_email_required off.
func readCommitteeOrgSettings(ctx context.Context, uid string) (bool, bool, error) {
	base, _, err := fetchCommitteeBase(ctx, uid)
	if err != nil {
		return false, false, err
	}
	if base == nil {
		return false, false, fmt.Errorf("empty committee base response for %s", uid)
	}
	settings, _, err := fetchCommitteeSettings(ctx, uid)
	var notFound *committeeservice.NotFoundError
	if errors.As(err, &notFound) {
		return base.EnableVoting, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return base.EnableVoting, settings.BusinessEmailRequired, nil
}

// classifyMemberOrg mirrors the org mapping in
// mapV1DataToCommitteeMemberCreatePayload and committee-service's
// org-required check: an org passes when it is a B2B org or has a domain.
func classifyMemberOrg(ctx context.Context, row memberRow, requiresOrg bool, res *backfillCommitteeMembersResult) string {
	if !requiresOrg {
		return memberCauseNotBlocked
	}
	if row.accountSFID == "" {
		return memberCauseOrgMissing
	}
	org, err := memberBackfillLookupOrgFn(ctx, row.accountSFID)
	if err != nil {
		if !errors.Is(err, errV1OrgUnresolved) {
			// A database error is not evidence that the org is missing: with
			// the settings relaxed the live handler would create the member
			// without an org it may have. Leave it for a later run.
			res.errors++
			logger.With(errKey, err, "member_sfid", row.sfid, "account_sfid", row.accountSFID).
				ErrorContext(ctx, "v1 organization lookup failed; skipping member")
			return memberCauseOrgLookupError
		}
		return memberCauseOrgLookupFailed
	}
	if strings.TrimSpace(org.Domain) != "" {
		return memberCauseNotBlocked
	}
	isB2B, err := memberBackfillIsB2BOrgFn(ctx, row.accountSFID)
	if err != nil {
		res.errors++
		logger.With(errKey, err, "member_sfid", row.sfid, "account_sfid", row.accountSFID).
			ErrorContext(ctx, "failed to check B2B org account; skipping member")
		return memberCauseOrgLookupError
	}
	if isB2B {
		return memberCauseNotBlocked
	}
	return memberCauseOrgNotB2BNoDomain
}

// isB2BOrgAccount reports whether the account is a live member org in the
// salesforce_b2b-Account replica, which is what a v2 b2b_org is built from.
func isB2BOrgAccount(ctx context.Context, accountSFID string) (bool, error) {
	candidates := []string{accountSFID}
	if normalized, err := sfutil.Normalize18(accountSFID); err == nil && normalized != accountSFID {
		candidates = append(candidates, normalized)
	}
	for _, sfid := range candidates {
		entry, err := v1KV.Get(ctx, "salesforce_b2b-Account."+sfid)
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			continue
		}
		if err != nil {
			return false, err
		}
		return isLiveMemberOrgAccount(entry.Value())
	}
	return false, nil
}

var errCommitteeRestoreFailed = errors.New("committee settings restore failed")

// committeeRestoreTimeout bounds the deferred restore. The Job's
// terminationGracePeriodSeconds must exceed it. A var so tests can shorten it.
var committeeRestoreTimeout = time.Minute

// processMemberCommittee re-emits one committee's members, relaxing and then
// restoring its settings when any member is blocked. relaxed reports whether
// a hold was written, so the caller verifies it.
func processMemberCommittee(ctx context.Context, c *memberCommittee, opts backfillCommitteeMembersOptions, limiter *rate.Limiter, res *backfillCommitteeMembersResult) (relaxed bool, err error) {
	var emit []memberRow
	blocked := 0
	for _, m := range c.members {
		switch {
		case m.cause == memberCauseNotBlocked:
			emit = append(emit, m)
		case memberCauseBlocked(m.cause):
			blocked++
			if opts.relaxSettings {
				emit = append(emit, m)
			}
		}
	}
	if blocked > 0 && !opts.relaxSettings {
		res.committeesSkippedBlocked++
		res.skippedBlockedMembers += blocked
	}
	if len(emit) == 0 {
		return false, nil
	}
	res.committeesProcessed++

	if blocked > 0 && opts.relaxSettings {
		log := logger.With("committee_uid", c.uid, "committee_sfid", c.sfid)
		hold, relaxErr := relaxCommitteeSettings(ctx, c)
		if hold != nil {
			relaxed = true
			res.committeesRelaxed++
			// Restore even if relaxing, emitting or settling failed, and even
			// if ctx was cancelled. A restore failure replaces the returned
			// error so the caller stops the run.
			defer func() {
				restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), committeeRestoreTimeout)
				defer cancel()
				if restoreErr := restoreCommitteeSettings(restoreCtx, c.uid); restoreErr != nil {
					if err != nil {
						err = fmt.Errorf("%w: %w (after: %v)", errCommitteeRestoreFailed, restoreErr, err)
					} else {
						err = fmt.Errorf("%w: %w", errCommitteeRestoreFailed, restoreErr)
					}
					return
				}
				log.InfoContext(ctx, "restored committee settings")
			}()
		}
		if relaxErr != nil {
			return relaxed, fmt.Errorf("failed to relax committee settings: %w", relaxErr)
		}
		log.With("relaxed_fields", hold.RelaxedFields).InfoContext(ctx, "relaxed committee settings for member backfill")
	}

	var settle []string
	for _, m := range emit {
		if err := limiter.Wait(ctx); err != nil {
			return relaxed, err
		}
		reason, err := reEmitCommitteeMemberV1ObjectFn(ctx, m.key, c.uid)
		switch {
		case err != nil:
			res.errors++
			logger.With(errKey, err, "member_sfid", m.sfid).ErrorContext(ctx, "failed to re-emit committee member")
		case reason == "" || reason == "revision_race":
			// A concurrent writer re-emitted the row first; still wait for it.
			res.emitted++
			settle = append(settle, m.sfid)
		case reason == "already_mapped":
			res.settled++
		default:
			res.emitSkipped++
			logger.With("member_sfid", m.sfid, "reason", reason).InfoContext(ctx, "skipped committee member re-emit")
		}
	}

	settled := settleCommitteeMemberPoll(ctx, settle, opts.settleTimeout)
	for _, sfid := range settle {
		if settled[sfid] {
			res.settled++
			continue
		}
		res.unsettled++
		logger.With("member_sfid", sfid, "committee_uid", c.uid).
			WarnContext(ctx, "re-emitted committee member has no mapping after the settle timeout")
	}
	return relaxed, nil
}

// reEmitCommitteeMemberV1Object re-reads the row, re-applies the filters,
// checks that its committee is still mapped to committeeUID and that the member
// is still unmapped, then writes the same value back with a revision check.
func reEmitCommitteeMemberV1Object(ctx context.Context, key, committeeUID string) (string, error) {
	entry, err := v1KV.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			return "missing", nil
		}
		return "", fmt.Errorf("failed to re-read %s: %w", key, err)
	}
	row, reason := evaluateMemberRow(ctx, key, entry.Value())
	if reason != "" {
		return reason, nil
	}
	committee, err := mappingStore.Get(ctx, committeeSFIDMappingKeyPrefix+row.committeeSFID)
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return "committee_unmapped", nil
	case err != nil:
		return "", fmt.Errorf("failed to look up committee mapping for %s: %w", row.sfid, err)
	case string(committee.Value) != committeeUID:
		return "committee_changed", nil
	}
	mapping, err := mappingStore.Get(ctx, committeeMemberSFIDMappingPrefix+row.sfid)
	switch {
	case err == nil && isTombstonedMapping(mapping.Value):
		return "tombstoned_mapping", nil
	case err == nil && len(mapping.Value) > 0:
		return "already_mapped", nil
	case err != nil && !errors.Is(err, ErrKeyNotFound):
		return "", fmt.Errorf("failed to look up member mapping for %s: %w", row.sfid, err)
	}
	if _, err := v1KV.Update(ctx, key, entry.Value(), entry.Revision()); err != nil {
		if isRevisionMismatchError(err) {
			return "revision_race", nil
		}
		return "", fmt.Errorf("failed to re-emit %s: %w", key, err)
	}
	return "", nil
}

// lookupCommitteeMemberMapping reports whether a live member mapping exists.
func lookupCommitteeMemberMapping(ctx context.Context, sfid string) bool {
	entry, err := mappingStore.Get(ctx, committeeMemberSFIDMappingPrefix+sfid)
	return err == nil && len(entry.Value) > 0 && !isTombstonedMapping(entry.Value)
}

// settleCommitteeMemberPoll follows settleCommitteePoll: every round's lookups
// share the overall deadline so a NATS outage cannot stretch the wait.
func settleCommitteeMemberPoll(ctx context.Context, sfids []string, timeout time.Duration) map[string]bool {
	settled := make(map[string]bool, len(sfids))
	deadline := time.Now().Add(timeout)
	pending := append([]string{}, sfids...)
	for len(pending) > 0 {
		lookupCtx, cancel := context.WithDeadline(ctx, deadline)
		var stillPending []string
		for _, sfid := range pending {
			if lookupCommitteeMemberMappingFn(lookupCtx, sfid) {
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
		case <-time.After(memberSettlePollInterval):
		}
	}
	return settled
}

// relaxCommitteeSettings records the committee's current settings in a hold,
// then turns off each one that is on. It returns the hold once it is stored,
// even with an error, so the caller restores it.
func relaxCommitteeSettings(ctx context.Context, c *memberCommittee) (*committeeSettingsHold, error) {
	base, baseETag, err := fetchCommitteeBase(ctx, c.uid)
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, fmt.Errorf("empty committee base response for %s", c.uid)
	}
	var businessEmailRequired bool
	settings, settingsETag, err := fetchCommitteeSettings(ctx, c.uid)
	var notFound *committeeservice.NotFoundError
	switch {
	case errors.As(err, &notFound):
	case err != nil:
		return nil, err
	default:
		businessEmailRequired = settings.BusinessEmailRequired
	}

	hold := &committeeSettingsHold{
		CommitteeUID:                  c.uid,
		CommitteeSFID:                 c.sfid,
		ProjectSFID:                   c.projectSFID,
		OriginalEnableVoting:          base.EnableVoting,
		OriginalBusinessEmailRequired: businessEmailRequired,
		BaseETag:                      baseETag,
		SettingsETag:                  settingsETag,
		State:                         holdStateCaptured,
		CreatedAt:                     time.Now().UTC(),
	}
	if v1Data, exists, err := fetchV1CommitteeData(ctx, c.sfid); err != nil {
		return nil, err
	} else if exists {
		hold.OriginalV1EnableVoting = boolField(v1Data, "enable_voting__c")
		hold.OriginalV1BusinessEmailRequired = boolField(v1Data, v1BusinessEmailRequiredField)
	}
	if base.EnableVoting {
		hold.RelaxedFields = append(hold.RelaxedFields, holdFieldEnableVoting)
	}
	if businessEmailRequired {
		hold.RelaxedFields = append(hold.RelaxedFields, holdFieldBusinessEmailRequired)
	}
	if len(hold.RelaxedFields) == 0 {
		return nil, fmt.Errorf("committee %s no longer requires an organization; re-run to classify its members again", c.uid)
	}

	revision, err := createCommitteeSettingsHold(ctx, hold)
	if err != nil {
		return nil, err
	}
	for _, field := range hold.RelaxedFields {
		if err := setCommitteeSetting(ctx, c.uid, field, false); err != nil {
			return hold, err
		}
	}
	hold.State = holdStateRelaxed
	if _, err := putCommitteeSettingsHold(ctx, hold, revision); err != nil {
		// The settings are relaxed either way; captured restores the same way.
		logger.With(errKey, err, "committee_uid", c.uid).WarnContext(ctx, "failed to mark committee settings hold as relaxed")
	}
	return hold, nil
}

func boolField(data map[string]any, field string) *bool {
	if v, ok := data[field].(bool); ok {
		return &v
	}
	return nil
}

// createCommitteeSettingsHold stores a new hold. A restored hold from an
// earlier run is replaced; an unrestored one is never overwritten, because it
// holds the only record of the committee's original settings.
func createCommitteeSettingsHold(ctx context.Context, hold *committeeSettingsHold) (uint64, error) {
	value, err := json.Marshal(hold)
	if err != nil {
		return 0, err
	}
	key := committeeSettingsHoldKey(hold.CommitteeUID)
	revision, err := mappingStore.Create(ctx, key, value)
	if !errors.Is(err, ErrKeyExists) {
		return revision, err
	}
	existing, existingRevision, err := getCommitteeSettingsHold(ctx, hold.CommitteeUID)
	if err != nil {
		return 0, err
	}
	if existing.State != holdStateRestored {
		return 0, fmt.Errorf("committee %s already has an unrestored settings hold (state %s)", hold.CommitteeUID, existing.State)
	}
	return mappingStore.Update(ctx, key, value, existingRevision)
}

func putCommitteeSettingsHold(ctx context.Context, hold *committeeSettingsHold, revision uint64) (uint64, error) {
	value, err := json.Marshal(hold)
	if err != nil {
		return 0, err
	}
	return mappingStore.Update(ctx, committeeSettingsHoldKey(hold.CommitteeUID), value, revision)
}

// restoreCommitteeSettings writes the hold's original values back, confirms
// them with a fresh read, and marks the hold restored. It is safe to repeat.
func restoreCommitteeSettings(ctx context.Context, committeeUID string) error {
	hold, revision, err := getCommitteeSettingsHold(ctx, committeeUID)
	if err != nil {
		return err
	}
	if hold.State == holdStateRestored {
		return nil
	}
	markRestored := func(note string) error {
		now := time.Now().UTC()
		hold.State = holdStateRestored
		hold.RestoredAt = &now
		hold.RestoreNote = note
		if _, err := putCommitteeSettingsHold(ctx, hold, revision); err != nil {
			return fmt.Errorf("marking hold restored: %w", err)
		}
		return nil
	}
	// A committee deleted from v2 has nothing left to restore. Without this,
	// its hold could never be restored and would block every later live run.
	var notFound *committeeservice.NotFoundError
	originals := map[string]bool{
		holdFieldEnableVoting:          hold.OriginalEnableVoting,
		holdFieldBusinessEmailRequired: hold.OriginalBusinessEmailRequired,
	}
	for _, field := range hold.RelaxedFields {
		if err := setCommitteeSetting(ctx, committeeUID, field, originals[field]); err != nil {
			if errors.As(err, &notFound) {
				break // Confirmed by the read below.
			}
			return fmt.Errorf("restoring %s: %w", field, err)
		}
	}
	enableVoting, businessEmailRequired, err := readCommitteeOrgSettings(ctx, committeeUID)
	if errors.As(err, &notFound) {
		if _, _, baseErr := fetchCommitteeBase(ctx, committeeUID); errors.As(baseErr, &notFound) {
			logger.With("committee_uid", committeeUID, "committee_sfid", hold.CommitteeSFID).
				WarnContext(ctx, "committee no longer exists in v2; marking its settings hold restored without a restore")
			return markRestored(holdNoteCommitteeDeleted)
		}
	}
	if err != nil {
		return fmt.Errorf("confirming restore: %w", err)
	}
	if (hold.relaxes(holdFieldEnableVoting) && enableVoting != hold.OriginalEnableVoting) ||
		(hold.relaxes(holdFieldBusinessEmailRequired) && businessEmailRequired != hold.OriginalBusinessEmailRequired) {
		return fmt.Errorf("committee %s still differs from its recorded settings after restore (enable_voting=%v, business_email_required=%v)",
			committeeUID, enableVoting, businessEmailRequired)
	}
	return markRestored("")
}

// setCommitteeSetting sets one org-requiring setting with a fresh read and its
// ETag, as the service principal, retrying a lost ETag race. It writes the
// v1→v2 pending marker first so the indexer echo is not sent to v1 (the hold
// also blocks it) and removes the marker when the write fails.
func setCommitteeSetting(ctx context.Context, uid, field string, value bool) error {
	var err error
	for attempt := 0; attempt < committeeSettingMaxAttempts; attempt++ {
		switch field {
		case holdFieldEnableVoting:
			err = setCommitteeEnableVoting(ctx, uid, value)
		case holdFieldBusinessEmailRequired:
			err = setCommitteeBusinessEmailRequired(ctx, uid, value)
		default:
			return fmt.Errorf("unknown committee setting %q", field)
		}
		var conflict *committeeservice.ConflictError
		if !errors.As(err, &conflict) {
			return err
		}
	}
	return err
}

func setCommitteeEnableVoting(ctx context.Context, uid string, value bool) error {
	base, etag, err := fetchCommitteeBase(ctx, uid)
	if err != nil {
		return err
	}
	if base == nil {
		return fmt.Errorf("empty committee base response for %s", uid)
	}
	if base.EnableVoting == value {
		return nil
	}
	if etag == "" {
		return fmt.Errorf("missing ETag for committee base %s", uid)
	}
	payload := committeeBaseUpdateFromCurrent(uid, base)
	payload.EnableVoting = value
	payload.IfMatch = &etag
	writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommittee, uid)
	if err := updateCommitteeBaseFn(ctx, payload); err != nil {
		deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommittee, uid)
		return err
	}
	return nil
}

func setCommitteeBusinessEmailRequired(ctx context.Context, uid string, value bool) error {
	current, etag, err := fetchCommitteeSettings(ctx, uid)
	if err != nil {
		return err
	}
	if current.BusinessEmailRequired == value {
		return nil
	}
	if etag == "" {
		return fmt.Errorf("missing ETag for committee settings %s", uid)
	}
	// Omit writers/auditors and the chat webhook: the service keeps absent
	// values, and sending writers/auditors could send access emails.
	payload := &committeeservice.UpdateCommitteeSettingsPayload{
		UID: &uid, IfMatch: &etag,
		BusinessEmailRequired:         value,
		MembersCanViewSocialListening: current.MembersCanViewSocialListening,
		ShowMeetingAttendees:          current.ShowMeetingAttendees,
		MemberVisibility:              current.MemberVisibility,
		LastReviewedAt:                current.LastReviewedAt,
		LastReviewedBy:                current.LastReviewedBy,
	}
	writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, uid)
	if err := updateCommitteeSettingsFn(ctx, payload); err != nil {
		deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, uid)
		return err
	}
	return nil
}

// committeeBaseUpdateFromCurrent copies every writable field of the current
// base: the base update replaces the whole record, so an omitted field would
// be cleared. An absent charter is the one field the service preserves.
func committeeBaseUpdateFromCurrent(uid string, base *committeeservice.CommitteeBaseWithReadonlyAttributes) *committeeservice.UpdateCommitteeBasePayload {
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:            &uid,
		ProjectUID:     stringPtrToString(base.ProjectUID),
		Name:           stringPtrToString(base.Name),
		RequiresReview: base.RequiresReview,
		Calendar:       base.Calendar,
		JoinMode:       base.JoinMode,
		Repository:     base.Repository,
		Scope:          base.Scope,
		Deliverables:   base.Deliverables,
		KeyDates:       base.KeyDates,
	}
	seedCommitteeUpdateFromCurrentBase(payload, base)
	return payload
}

func updateCommitteeBaseAPI(ctx context.Context, payload *committeeservice.UpdateCommitteeBasePayload) error {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return err
	}
	payload.BearerToken = &token
	_, err = committeeClient.UpdateCommitteeBase(ctx, payload)
	return wrapCommitteeServiceError("failed to update committee base", err)
}

func updateCommitteeSettingsAPI(ctx context.Context, payload *committeeservice.UpdateCommitteeSettingsPayload) error {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return err
	}
	payload.BearerToken = &token
	_, err = committeeClient.UpdateCommitteeSettings(ctx, payload)
	return wrapCommitteeServiceError("failed to update committee settings", err)
}

type storedCommitteeSettingsHold struct {
	hold *committeeSettingsHold
}

// listCommitteeSettingsHolds lists every hold in the mapping store.
func listCommitteeSettingsHolds(ctx context.Context) ([]storedCommitteeSettingsHold, error) {
	data, err := listMappingsWithPrefixFn(ctx, committeeSettingsHoldKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list committee settings holds: %w", err)
	}
	keys := make([]string, 0, len(data))
	for key, value := range data {
		if !isTombstonedMapping(value) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	holds := make([]storedCommitteeSettingsHold, 0, len(keys))
	for _, key := range keys {
		var hold committeeSettingsHold
		if err := json.Unmarshal(data[key], &hold); err != nil {
			return nil, fmt.Errorf("decoding committee settings hold %s: %w", key, err)
		}
		holds = append(holds, storedCommitteeSettingsHold{hold: &hold})
	}
	return holds, nil
}

// verifyCommitteeSettingsHolds compares v2 and v1 with each touched
// committee's hold. v1 is compared with its own recorded values, so drift that
// predates the run is not reported as a change made by it.
func verifyCommitteeSettingsHolds(ctx context.Context, committeeUIDs []string, res *backfillCommitteeMembersResult) {
	seen := map[string]bool{}
	for _, uid := range committeeUIDs {
		if seen[uid] {
			continue
		}
		seen[uid] = true
		hold, _, err := getCommitteeSettingsHold(ctx, uid)
		if err != nil {
			res.verifyMismatches++
			logger.With(errKey, err, "committee_uid", uid).ErrorContext(ctx, "failed to read committee settings hold for verification")
			continue
		}
		res.verified++
		log := logger.With("committee_uid", uid, "committee_sfid", hold.CommitteeSFID, "state", hold.State)
		if hold.RestoreNote == holdNoteCommitteeDeleted {
			log.WarnContext(ctx, "committee was deleted from v2 while held; nothing to verify")
			continue
		}
		var mismatches []string
		if hold.State != holdStateRestored {
			mismatches = append(mismatches, "hold not restored")
		}
		enableVoting, businessEmailRequired, err := readCommitteeOrgSettings(ctx, uid)
		if err != nil {
			mismatches = append(mismatches, "v2 read failed: "+err.Error())
		} else {
			if enableVoting != hold.OriginalEnableVoting {
				mismatches = append(mismatches, fmt.Sprintf("v2 enable_voting=%v, recorded %v", enableVoting, hold.OriginalEnableVoting))
			}
			if businessEmailRequired != hold.OriginalBusinessEmailRequired {
				mismatches = append(mismatches, fmt.Sprintf("v2 business_email_required=%v, recorded %v", businessEmailRequired, hold.OriginalBusinessEmailRequired))
			}
		}
		v1Data, exists, err := fetchV1CommitteeData(ctx, hold.CommitteeSFID)
		switch {
		case err != nil:
			mismatches = append(mismatches, "v1 read failed: "+err.Error())
		case !exists:
			mismatches = append(mismatches, "v1 committee missing")
		default:
			if diff := compareRecordedBool(v1Data, "enable_voting__c", hold.OriginalV1EnableVoting); diff != "" {
				mismatches = append(mismatches, diff)
			}
			if diff := compareRecordedBool(v1Data, v1BusinessEmailRequiredField, hold.OriginalV1BusinessEmailRequired); diff != "" {
				mismatches = append(mismatches, diff)
			}
		}
		if len(mismatches) > 0 {
			res.verifyMismatches++
			log.With("mismatches", mismatches).ErrorContext(ctx, "committee settings do not match the recorded values")
			continue
		}
		log.InfoContext(ctx, "committee settings match the recorded values in v1 and v2")
	}
}

func compareRecordedBool(data map[string]any, field string, recorded *bool) string {
	current := boolField(data, field)
	switch {
	case recorded == nil && current == nil:
		return ""
	case recorded == nil || current == nil || *recorded != *current:
		return fmt.Sprintf("v1 %s=%s, recorded %s", field, formatOptionalBool(current), formatOptionalBool(recorded))
	}
	return ""
}

func formatOptionalBool(v *bool) string {
	if v == nil {
		return "unset"
	}
	return fmt.Sprint(*v)
}
