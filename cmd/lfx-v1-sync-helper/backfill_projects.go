// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Backfill v1 projects that have no v2 mapping (LFXV2-3220 removed the
// project allowlist; projects created before 17 Aug 2026 and never modified
// since were never synced).
//
// Mechanism: re-PUT the existing v1-objects value for
// "salesforce-project__c.<sfid>" and let the running deployment's durable KV
// consumer (kvMessageHandler -> handleKVPut -> handleProjectUpdate) perform
// the actual create. No project-create logic is duplicated here.
//
// Why depth ordering is required: handlers.go's handleKVPut unconditionally
// ACKs project messages (case "salesforce-project__c" returns false), so the
// NAK/retry path is never reached for projects. mapV1DataToProjectCreatePayload
// hard-errors and drops the message when a project's parent_project__c (or
// parent_entity_relationship__c) mapping is missing. A flat re-emit of every
// unmapped project would therefore silently fail for any candidate whose
// parent is itself unmapped. This file re-emits level by level (depth 0 =
// no parent or parent already mapped), waiting for each level's mappings to
// appear before emitting the next, so parents are always created first.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/time/rate"
)

const (
	// projectObjectSubject is the NATS subject filter for v1 project object
	// entries in the v1-objects KV bucket.
	projectObjectSubject = "$KV.v1-objects.salesforce-project__c.*"

	// projectObjectSubjectPrefix is stripped from the subject to extract the
	// SFID.
	projectObjectSubjectPrefix = "$KV.v1-objects.salesforce-project__c."

	// projectSFIDMappingKeyPrefix is the v1-mappings key prefix for project
	// SFID -> v2 UID mappings.
	projectSFIDMappingKeyPrefix = "project.sfid."

	// minMappingsSafetyFloor is the minimum number of live+tombstoned project
	// SFID mappings expected in prod (~12K). A count below this suggests a
	// truncated scan, a renamed subject, or a stale mappings bucket (see
	// LFXV2-2985) rather than a genuine near-empty environment, any of which
	// would otherwise cause a live run to re-emit every project in v1.
	minMappingsSafetyFloor = 1000

	// formationStagePrefix is the case-insensitive stage prefix identifying
	// formation-staged projects (e.g. "Formation - Confidential").
	formationStagePrefix = "formation"

	// projectSettlePollInterval is the delay between settle-poll attempts
	// while waiting for a depth level's emitted mappings to appear.
	projectSettlePollInterval = 2 * time.Second

	// projectSettlePollTimeout is the maximum time to wait for a single
	// depth level to settle before treating its unmapped members' descendants
	// as blocked.
	projectSettlePollTimeout = 2 * time.Minute
)

// backfillProjectsOptions configures a --backfill-projects run.
type backfillProjectsOptions struct {
	dryRun               bool
	limit                int
	emitRate             float64
	excludeStagePrefixes []string
	includeStagePrefixes []string
	allowFormation       bool
	checkSlugs           bool
	force                bool
}

// backfillProjectsResult summarizes a backfill run.
type backfillProjectsResult struct {
	scanned             int
	mappingsLive        int
	mappingsTombstoned  int
	lowMappingsCount    bool
	candidates          int
	emitted             int
	levels              int
	formationCandidates int
	remainingUnmapped   int
	errors              int

	skippedAlreadyMapped   int
	skippedTombstoned      int
	skippedSoftDeleted     int
	skippedEmptyValue      int
	skippedUndecodable     int
	skippedNoSFID          int
	skippedMissingRequired int
	skippedV2Authored      int
	skippedStageFiltered   int
	skippedParentUnmapped  int
	skippedParentBlocked   int
	skippedRevisionRace    int
	skippedLimit           int
	skippedSlugConflict    int
	skippedMissing         int
	skippedSettleTimeout   int
	slugConflicts          []slugConflict
	duplicateSlugs         map[string][]string
	stageHistogram         map[string]int
}

// logFields returns the complete counter set as alternating key/value pairs
// for logger.With, so every log site (dry-run report, final summary) reports
// the same fields instead of each hand-maintaining its own drifting subset.
func (res backfillProjectsResult) logFields() []any {
	return []any{
		"scanned", res.scanned,
		"mappings_live", res.mappingsLive,
		"mappings_tombstoned", res.mappingsTombstoned,
		"low_mappings_count", res.lowMappingsCount,
		"candidates", res.candidates,
		"emitted", res.emitted,
		"levels", res.levels,
		"formation_candidates", res.formationCandidates,
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
		"skipped_stage_filtered", res.skippedStageFiltered,
		"skipped_parent_unmapped", res.skippedParentUnmapped,
		"skipped_parent_blocked", res.skippedParentBlocked,
		"skipped_revision_race", res.skippedRevisionRace,
		"skipped_limit", res.skippedLimit,
		"skipped_slug_conflict", res.skippedSlugConflict,
		"skipped_missing", res.skippedMissing,
		"skipped_settle_timeout", res.skippedSettleTimeout,
		"slug_conflicts", len(res.slugConflicts),
		"duplicate_slugs", len(res.duplicateSlugs),
		"stage_histogram", res.stageHistogram,
	}
}

// slugConflict records a candidate whose slug already resolves to a v2
// project UID: it needs a mapping repair, not a create (out of scope for
// this backfill; see the plan's "Out of scope" section).
type slugConflict struct {
	sfid       string
	slug       string
	projectUID string
}

// projectCandidate is a v1 project row selected for re-emission.
type projectCandidate struct {
	sfid            string
	key             string
	data            map[string]any
	parentSFID      string
	legalParentSFID string
	stage           string
	slug            string
}

// dependencySFIDs returns every parent SFID (project and legal-entity) that
// must be settled before sfid can safely be emitted — both are hard-error
// dependencies in mapV1DataToProjectCreatePayload (handlers_projects.go).
func (c projectCandidate) dependencySFIDs() []string {
	var deps []string
	if c.parentSFID != "" {
		deps = append(deps, c.parentSFID)
	}
	if c.legalParentSFID != "" && c.legalParentSFID != c.parentSFID {
		deps = append(deps, c.legalParentSFID)
	}
	return deps
}

// reEmitV1ObjectFn and lookupProjectMappingFn are package-level function
// variables so tests can stub the NATS-dependent emit/poll steps without a
// live cluster (this repo does not mock NATS; see ingest_workspaces_test.go).
var (
	reEmitV1ObjectFn       = reEmitV1Object
	lookupProjectMappingFn = lookupProjectMapping
	getProjectUIDBySlugFn  = getProjectUIDBySlug
)

// backfillProjects scans v1-objects for salesforce-project__c rows with no
// live v1-mappings entry, then re-emits them (oldest ancestors first) so the
// running deployment's KV consumer creates each project in v2.
func backfillProjects(ctx context.Context, opts backfillProjectsOptions) (backfillProjectsResult, error) {
	res := backfillProjectsResult{
		duplicateSlugs: map[string][]string{},
		stageHistogram: map[string]int{},
	}

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

	objects, err := ScanSubjectData(ctx, jsContext, kvObjectsStream, projectObjectSubject, opTimeout)
	if err != nil {
		return res, fmt.Errorf("failed to scan v1 project objects: %w", err)
	}
	res.scanned = len(objects)

	liveMappings, tombstonedMappings, err := collectProjectSFIDMappingStates(ctx)
	if err != nil {
		return res, fmt.Errorf("failed to collect project SFID mappings: %w", err)
	}
	res.mappingsLive = len(liveMappings)
	res.mappingsTombstoned = len(tombstonedMappings)

	if !opts.dryRun && !opts.force && res.mappingsLive+res.mappingsTombstoned < minMappingsSafetyFloor {
		return res, fmt.Errorf(
			"only %d live + %d tombstoned project SFID mappings found (expected at least %d) — "+
				"this looks like a truncated scan or a stale mappings bucket, not a genuine near-empty "+
				"environment; pass --force to proceed anyway",
			res.mappingsLive, res.mappingsTombstoned, minMappingsSafetyFloor,
		)
	}
	res.lowMappingsCount = res.mappingsLive+res.mappingsTombstoned < minMappingsSafetyFloor

	candidates := selectProjectCandidates(ctx, objects, liveMappings, tombstonedMappings, opts, &res)

	if opts.checkSlugs {
		candidates, err = filterSlugConflicts(ctx, candidates, &res)
		if err != nil {
			return res, err
		}
	}

	for slug, sfids := range slugsBySFID(candidates) {
		if len(sfids) > 1 {
			res.duplicateSlugs[slug] = sfids
		}
	}

	if !opts.dryRun {
		if err := validateNoDuplicateSlugs(res.duplicateSlugs); err != nil {
			return res, err
		}
	}

	if !opts.dryRun && !opts.allowFormation {
		if err := validateNoFormationMix(candidates, includesFormation(opts.includeStagePrefixes)); err != nil {
			return res, err
		}
	}

	res.candidates = len(candidates)

	levels, unresolved := orderCandidatesByDepth(candidates, liveMappings)
	res.levels = len(levels)
	res.skippedParentUnmapped += len(unresolved)
	res.remainingUnmapped = len(unresolved)

	if opts.dryRun {
		logger.With(res.logFields()...).InfoContext(ctx, "[dry-run] project backfill candidate report")
		return res, nil
	}

	limiter := rate.NewLimiter(rate.Limit(opts.emitRate), 1)
	settled := make(map[string]bool, len(liveMappings))
	for sfid := range liveMappings {
		settled[sfid] = true
	}

	limitReached := false
	for _, level := range levels {
		if limitReached {
			res.skippedLimit += len(level)
			continue
		}

		var emittedThisLevel []string
		for _, c := range level {
			if !allDepsResolved(c, settled) {
				// A dependency (project or legal parent) never settled, so
				// emitting c would hit the same missing-parent hard-error
				// this ordering exists to prevent. Its own descendants will
				// cascade into this branch too, since c's SFID never joins
				// settled below.
				res.skippedParentBlocked++
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

			skipReason, err := reEmitV1ObjectFn(ctx, c.key, opts)
			if err != nil {
				res.errors++
				logger.With(errKey, err, "sfid", c.sfid, "key", c.key).ErrorContext(ctx, "failed to re-emit project for backfill")
				continue
			}
			if skipReason != "" {
				recordSkip(&res, skipReason)
				switch skipReason {
				case "already_mapped":
					// reEmitV1Object's own mapping lookup just confirmed a live
					// mapping exists, so children depending on c can unblock
					// immediately rather than waiting to be settle-polled below.
					settled[c.sfid] = true
				case "revision_race":
					// The object write lost to a concurrent update, which may
					// itself be creating the mapping (e.g. a live sync racing
					// this backfill). Settle-poll it like a normal emission
					// instead of leaving it permanently unresolved.
					emittedThisLevel = append(emittedThisLevel, c.sfid)
				}
				continue
			}
			res.emitted++
			emittedThisLevel = append(emittedThisLevel, c.sfid)
		}

		// Always settle-poll whatever was emitted in this level, even if the
		// limit was hit partway through it: skipping this when limitReached
		// left those SFIDs out of settled, so the final recompute below
		// double-counted successful creates as still unmapped.
		nowSettled := settlePoll(ctx, emittedThisLevel, projectSettlePollTimeout)
		for _, sfid := range emittedThisLevel {
			if nowSettled[sfid] {
				settled[sfid] = true
			} else {
				// Emitted successfully but never confirmed mapped: a
				// downstream create failure or a settle-poll timeout, not an
				// intentional skip. Distinct from skippedLimit/skippedRevisionRace,
				// which are expected and not treated as failures.
				res.skippedSettleTimeout++
			}
		}
	}

	// remainingUnmapped was seeded from the structurally-unresolved set before
	// any emission; recompute it from final settle state so it also reflects
	// candidates dropped by the limit, emit errors, revision races, or a
	// blocked/failed dependency — an operator reading only this field should
	// never see 0 while projects remain unmapped.
	notSettled := len(unresolved)
	for _, level := range levels {
		for _, c := range level {
			if !settled[c.sfid] {
				notSettled++
			}
		}
	}
	res.remainingUnmapped = notSettled

	if res.errors > 0 || res.skippedSettleTimeout > 0 {
		return res, fmt.Errorf(
			"project backfill completed with %d errors and %d settle timeouts",
			res.errors, res.skippedSettleTimeout,
		)
	}
	return res, nil
}

// recordSkip increments the counter matching a skip reason string returned by
// reEmitV1Object's re-applied filters.
func recordSkip(res *backfillProjectsResult, reason string) {
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
	case "stage_filtered":
		res.skippedStageFiltered++
	}
}

// v1RowIsSoftDeleted reports whether a decoded v1 row is soft-deleted, using
// the same two checks as handleKVPut (handlers.go): a non-empty
// _sdc_deleted_at (WAL-driven soft delete) or isdeleted=true (SFDC-semantic
// soft delete). Factored out so handleKVPut and the backfill's candidate
// selection cannot drift apart.
func v1RowIsSoftDeleted(v1Data map[string]any) bool {
	if deletedAt, exists := v1Data["_sdc_deleted_at"]; exists && deletedAt != nil && deletedAt != "" {
		return true
	}
	if isDeleted, ok := v1Data["isdeleted"].(bool); ok && isDeleted {
		return true
	}
	return false
}

// decodeV1ProjectRow decodes a raw v1-objects value, trying JSON then
// msgpack, matching handleKVPut's dual-format handling.
func decodeV1ProjectRow(raw []byte) (map[string]any, error) {
	var v1Data map[string]any
	if err := json.Unmarshal(raw, &v1Data); err != nil {
		if mpErr := msgpack.Unmarshal(raw, &v1Data); mpErr != nil {
			return nil, fmt.Errorf("decode: json=%v msgpack=%v", err, mpErr)
		}
	}
	return v1Data, nil
}

// evaluateProjectRow applies the same pre-filters as handleKVPut (in the same
// order) to a decoded v1 project row, so candidate selection and the emit
// step's re-filter never count or emit a row the live consumer would drop.
// Returns a non-empty skipReason when the row should not be treated as a
// candidate.
func evaluateProjectRow(ctx context.Context, key string, raw []byte) (projectCandidate, string) {
	if len(raw) == 0 {
		return projectCandidate{}, "empty_value"
	}

	v1Data, err := decodeV1ProjectRow(raw)
	if err != nil {
		return projectCandidate{}, "undecodable"
	}

	if v1RowIsSoftDeleted(v1Data) {
		return projectCandidate{}, "soft_deleted"
	}

	if shouldSkipSync(ctx, v1Data) {
		return projectCandidate{}, "v2_authored"
	}

	sfid, _ := v1Data["sfid"].(string)
	sfid = strings.TrimSpace(sfid)
	if sfid == "" {
		return projectCandidate{}, "no_sfid"
	}

	name, _ := v1Data["name"].(string)
	slug, _ := v1Data["slug__c"].(string)
	if strings.TrimSpace(name) == "" || strings.TrimSpace(slug) == "" {
		return projectCandidate{}, "missing_required"
	}

	parentSFID, _ := v1Data["parent_project__c"].(string)
	legalParentSFID, _ := v1Data["parent_entity_relationship__c"].(string)
	stage, _ := v1Data["project_status__c"].(string)

	return projectCandidate{
		sfid:            sfid,
		key:             key,
		data:            v1Data,
		parentSFID:      strings.TrimSpace(parentSFID),
		legalParentSFID: strings.TrimSpace(legalParentSFID),
		stage:           stage,
		// Lowercased to match mapV1DataToProjectCreatePayload's canonicalization
		// (handlers_projects.go), so slug-conflict lookup and duplicate-slug
		// detection compare against the same slug creation will actually use.
		slug: strings.ToLower(strings.TrimSpace(slug)),
	}, ""
}

// selectProjectCandidates builds the unmapped-project candidate set: live
// object rows minus live and tombstoned project.sfid mappings, then filtered
// through evaluateProjectRow and the stage-prefix flags.
func selectProjectCandidates(
	ctx context.Context,
	objects map[string][]byte,
	liveMappings map[string]string,
	tombstonedMappings map[string]struct{},
	opts backfillProjectsOptions,
	res *backfillProjectsResult,
) []projectCandidate {
	candidates := make([]projectCandidate, 0, len(objects))

	for subject, raw := range objects {
		if !strings.HasPrefix(subject, projectObjectSubjectPrefix) {
			continue
		}
		sfid := subject[len(projectObjectSubjectPrefix):]
		key := "salesforce-project__c." + sfid

		if _, ok := tombstonedMappings[sfid]; ok {
			res.skippedTombstoned++
			continue
		}
		if _, ok := liveMappings[sfid]; ok {
			res.skippedAlreadyMapped++
			continue
		}

		candidate, skipReason := evaluateProjectRow(ctx, key, raw)
		if skipReason != "" {
			recordSkip(res, skipReason)
			continue
		}

		// Recorded before the stage-prefix filter below so a shipped run that
		// always excludes Formation (e.g. the default manifest) still reports
		// how many Formation projects exist, per the linked issue's rollout
		// gate requiring that count ahead of the bulk run.
		res.stageHistogram[candidate.stage]++
		if isFormationStage(candidate.stage) {
			res.formationCandidates++
		}

		if !stagePassesFilters(candidate.stage, opts.excludeStagePrefixes, opts.includeStagePrefixes) {
			res.skippedStageFiltered++
			continue
		}

		candidates = append(candidates, candidate)
	}

	return candidates
}

// stagePassesFilters applies --exclude-stage-prefix and --include-stage-prefix
// (comma-separated, case-insensitive prefix match). Exclude wins over
// include when both would otherwise apply to the same stage.
func stagePassesFilters(stage string, excludePrefixes, includePrefixes []string) bool {
	if stageMatchesAnyPrefix(stage, excludePrefixes) {
		return false
	}
	if len(includePrefixes) > 0 && !stageMatchesAnyPrefix(stage, includePrefixes) {
		return false
	}
	return true
}

// stageMatchesAnyPrefix reports whether stage has any of prefixes as a
// case-insensitive prefix. An empty prefixes list never matches.
func stageMatchesAnyPrefix(stage string, prefixes []string) bool {
	lowerStage := strings.ToLower(stage)
	for _, p := range prefixes {
		if p == "" {
			continue
		}
		if strings.HasPrefix(lowerStage, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// isFormationStage reports whether stage is a Formation sub-stage (e.g.
// "Formation - Confidential").
func isFormationStage(stage string) bool {
	return stageMatchesAnyPrefix(stage, []string{formationStagePrefix})
}

// includesFormation reports whether any of includePrefixes itself denotes
// Formation (e.g. "Formation" or "Formation - Confidential"). This is the
// only affirmative signal that the operator chose Formation via
// --include-stage-prefix; an --exclude-stage-prefix, even one unrelated to
// Formation like "Draft", does not authorize a Formation run just because it
// happens to leave an all-Formation candidate set behind.
func includesFormation(includePrefixes []string) bool {
	for _, p := range includePrefixes {
		if stageMatchesAnyPrefix(p, []string{formationStagePrefix}) {
			return true
		}
	}
	return false
}

// validateNoFormationMix returns an error if candidates contains any
// formation-staged project unless the run's --include-stage-prefix filter
// itself denotes Formation (formationIncluded). A generic stage filter is not
// enough: an unrelated filter, e.g. --exclude-stage-prefix Draft, can still
// leave an all-Formation candidate set behind without the operator ever
// choosing Formation, so only an include prefix that actually names Formation
// (or --allow-formation, checked by the caller) authorizes a Formation run.
func validateNoFormationMix(candidates []projectCandidate, formationIncluded bool) error {
	hasFormation, hasNonFormation := false, false
	for _, c := range candidates {
		if isFormationStage(c.stage) {
			hasFormation = true
		} else {
			hasNonFormation = true
		}
	}
	if hasFormation && (hasNonFormation || !formationIncluded) {
		return fmt.Errorf(
			"candidates include formation-staged projects; run " +
				"--exclude-stage-prefix Formation and --include-stage-prefix Formation as two " +
				"separate invocations, or pass --allow-formation for a single combined run",
		)
	}
	return nil
}

// parseStagePrefixList splits a comma-separated --exclude-stage-prefix /
// --include-stage-prefix flag value, trimming whitespace and dropping empty
// entries.
func parseStagePrefixList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// filterSlugConflicts removes candidates whose slug already resolves to a v2
// project UID (a lost mapping, not a missing project) and records them for
// the out-of-scope mapping-repair follow-up rather than re-emitting a create
// that would fail on every run. A transport/lookup error is not the same as
// a confirmed no-conflict response: --check-slugs exists to prevent
// duplicate creates, so a lookup failure must not silently disable it by
// falling through as "no conflict found."
func filterSlugConflicts(ctx context.Context, candidates []projectCandidate, res *backfillProjectsResult) ([]projectCandidate, error) {
	kept := make([]projectCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.slug == "" {
			kept = append(kept, c)
			continue
		}
		uid, err := getProjectUIDBySlugFn(ctx, c.slug)
		if err != nil {
			if errors.Is(err, errSlugNotFound) {
				kept = append(kept, c)
				continue
			}
			return nil, fmt.Errorf("--check-slugs lookup failed for slug %s (sfid %s): %w", c.slug, c.sfid, err)
		}
		res.skippedSlugConflict++
		res.slugConflicts = append(res.slugConflicts, slugConflict{sfid: c.sfid, slug: c.slug, projectUID: uid})
		logger.With("sfid", c.sfid, "slug", c.slug, "project_uid", uid).
			WarnContext(ctx, "candidate slug already resolves to a v2 project; needs mapping repair, not a create — skipping")
	}
	return kept, nil
}

// slugsBySFID groups candidate SFIDs by slug, for duplicate-slug reporting.
func slugsBySFID(candidates []projectCandidate) map[string][]string {
	out := map[string][]string{}
	for _, c := range candidates {
		if c.slug == "" {
			continue
		}
		out[c.slug] = append(out[c.slug], c.sfid)
	}
	for slug := range out {
		sort.Strings(out[slug])
	}
	return out
}

// validateNoDuplicateSlugs returns an error if duplicateSlugs is non-empty.
// The v2 project service rejects a second create for a slug that already
// exists, so a live run with same-run candidates sharing a slug would create
// one candidate per colliding group while its siblings never settle,
// guaranteeing a partially-applied backfill. Refuse the run up front and
// name the colliding groups so an operator can reconcile them first.
func validateNoDuplicateSlugs(duplicateSlugs map[string][]string) error {
	if len(duplicateSlugs) == 0 {
		return nil
	}
	return fmt.Errorf(
		"candidates include %d colliding slug(s), the v2 project service "+
			"rejects a second create for the same slug so one candidate in "+
			"each group would be created while its siblings never settle: %s",
		len(duplicateSlugs), formatDuplicateSlugs(duplicateSlugs),
	)
}

// formatDuplicateSlugs renders duplicateSlugs as "slug: [sfid, sfid], ..."
// groups, sorted by slug, so the error message is deterministic and an
// operator can reconcile each colliding group before re-running.
func formatDuplicateSlugs(duplicateSlugs map[string][]string) string {
	slugs := make([]string, 0, len(duplicateSlugs))
	for slug := range duplicateSlugs {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)

	groups := make([]string, 0, len(slugs))
	for _, slug := range slugs {
		groups = append(groups, fmt.Sprintf("%s: %v", slug, duplicateSlugs[slug]))
	}
	return strings.Join(groups, "; ")
}

// orderCandidatesByDepth partitions candidates into depth levels: level 0 is
// every candidate with no dependencies (dependencySFIDs) or whose
// dependencies are already mapped (liveMappings); level n is every remaining
// candidate whose dependencies are all in an earlier level. A candidate
// depends on both its project parent (parent_project__c) and its legal
// parent (parent_entity_relationship__c) — mapV1DataToProjectCreatePayload
// hard-errors on either being unmapped. Iterates to a fixpoint; any
// candidates left over (a dependency is itself unmapped and never becomes a
// candidate at any level, i.e. a cycle or a dependency excluded by stage
// filtering) are returned as unresolved.
func orderCandidatesByDepth(candidates []projectCandidate, liveMappings map[string]string) (levels [][]projectCandidate, unresolved []projectCandidate) {
	resolved := make(map[string]bool, len(liveMappings)+len(candidates))
	for sfid := range liveMappings {
		resolved[sfid] = true
	}

	remaining := candidates
	for len(remaining) > 0 {
		var level, next []projectCandidate
		for _, c := range remaining {
			if allDepsResolved(c, resolved) {
				level = append(level, c)
			} else {
				next = append(next, c)
			}
		}
		if len(level) == 0 {
			unresolved = append(unresolved, next...)
			break
		}
		levels = append(levels, level)
		for _, c := range level {
			resolved[c.sfid] = true
		}
		remaining = next
	}
	return levels, unresolved
}

// allDepsResolved reports whether every dependency SFID for c is already
// resolved (mapped or settled in an earlier level).
func allDepsResolved(c projectCandidate, resolved map[string]bool) bool {
	for _, dep := range c.dependencySFIDs() {
		if !resolved[dep] {
			return false
		}
	}
	return true
}

// collectProjectSFIDMappingStates reads all project.sfid.* keys from
// KV_v1-mappings and returns both live (SFID -> v2 UID) and tombstoned (SFID
// set) mappings. Unlike collectProjectSFIDMappings (ingest_acs_project.go),
// tombstones are preserved rather than discarded: the backfill's candidate
// selection must exclude tombstoned SFIDs (a deliberate v2 delete) from the
// unmapped set, not just live ones.
func collectProjectSFIDMappingStates(ctx context.Context) (map[string]string, map[string]struct{}, error) {
	const (
		kvMappingsStream         = "KV_v1-mappings"
		projectSFIDSubject       = "$KV.v1-mappings.project.sfid.*"
		projectSFIDSubjectPrefix = "$KV.v1-mappings.project.sfid."
	)

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	subjectData, err := ScanSubjectData(ctx, jsContext, kvMappingsStream, projectSFIDSubject, opTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan project SFID mappings: %w", err)
	}

	live := make(map[string]string, len(subjectData))
	tombstoned := make(map[string]struct{})

	for subject, data := range subjectData {
		if !strings.HasPrefix(subject, projectSFIDSubjectPrefix) {
			continue
		}
		sfid := subject[len(projectSFIDSubjectPrefix):]
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

// reEmitV1Object re-reads the current v1-objects value for key, re-applies
// the same filters as the live consumer would, and (if the row still
// qualifies) writes it back with a revision-checked Update so the durable KV
// consumer redelivers it and creates the project. Never replays bytes from
// an earlier scan: the objects scan can run for a long time, and replaying
// stale bytes could clobber a newer WAL write.
//
// Also re-applies the stage-prefix filters and Formation authorization,
// not just evaluateProjectRow's generic checks: a long or rate-limited run
// can take long enough for a candidate to change stage (e.g. Active ->
// Formation) between the initial scan and this re-read, and re-emitting it
// unchecked would bypass the run's stage scope and Formation rollout gate.
//
// Returns a non-empty skipReason (see recordSkip) instead of an error for
// every outcome that is not itself a failure: the row disappearing, no
// longer qualifying, or losing a concurrent-write race are all expected,
// re-run-safe outcomes.
func reEmitV1Object(ctx context.Context, key string, opts backfillProjectsOptions) (skipReason string, err error) {
	entry, err := v1KV.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
			return "missing", nil
		}
		return "", fmt.Errorf("failed to re-read %s: %w", key, err)
	}

	candidate, reason := evaluateProjectRow(ctx, key, entry.Value())
	if reason != "" {
		return reason, nil
	}

	if !stagePassesFilters(candidate.stage, opts.excludeStagePrefixes, opts.includeStagePrefixes) {
		return "stage_filtered", nil
	}
	if isFormationStage(candidate.stage) && !opts.allowFormation && !includesFormation(opts.includeStagePrefixes) {
		return "stage_filtered", nil
	}

	mappingKey := projectSFIDMappingKeyPrefix + candidate.sfid
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
		// A transient lookup failure is not the same as "no mapping": treating
		// it as absent could re-emit a project that's already mapped, sending
		// the durable consumer down a duplicate-create path.
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

// lookupProjectMapping reports whether a live (non-tombstoned) project.sfid
// mapping exists for sfid.
func lookupProjectMapping(ctx context.Context, sfid string) bool {
	entry, err := mappingsKV.Get(ctx, projectSFIDMappingKeyPrefix+sfid)
	if err != nil {
		return false
	}
	return len(entry.Value()) > 0 && !isTombstonedMapping(entry.Value())
}

// settlePoll polls lookupProjectMappingFn for every sfid in sfids until each
// either settles (mapping appears) or timeout elapses, returning which ones
// settled.
func settlePoll(ctx context.Context, sfids []string, timeout time.Duration) map[string]bool {
	settled := make(map[string]bool, len(sfids))
	if len(sfids) == 0 {
		return settled
	}

	deadline := time.Now().Add(timeout)
	pending := append([]string{}, sfids...)

	for len(pending) > 0 {
		// Bound each lookup to the overall settle deadline: without this, a
		// NATS outage lets every sequential mappingsKV.Get in this round block
		// on the parent (unbounded) context, and the deadline check below
		// never runs until all of them return — turning a nominal two-minute
		// settle phase into hours.
		lookupCtx, cancel := context.WithDeadline(ctx, deadline)
		var stillPending []string
		for _, sfid := range pending {
			if lookupProjectMappingFn(lookupCtx, sfid) {
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
		case <-time.After(projectSettlePollInterval):
		}
	}

	return settled
}
