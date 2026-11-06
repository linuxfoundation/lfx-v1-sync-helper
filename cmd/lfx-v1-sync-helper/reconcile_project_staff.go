// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Project staff reconciliation (lfx-self-serve#5034): compares the executive
// director and program manager of explicitly selected projects between v2
// (Self Serve) and v1 (PCC), and repairs v1 only for the roles a reviewer
// approved. It reuses the v2-to-v1 staff bridge's contact resolution and
// partial PATCH (handlers_projects_v2tov1.go) rather than replaying events.
//
// Report mode (the default) only reads. Apply mode needs a reviewed report
// with per-role approvals, re-checks every version the review was bound to,
// and PATCHes v1 one project at a time.
//
// Reports and logs carry no staff identity: no names, emails, usernames or
// contact SFIDs. A reviewer checks the actual assignments in PCC and Self
// Serve; the recorded versions (v2 ETag, v1-objects KV revision, mapping
// revision) bind the approval to what was reviewed.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
)

const (
	projectStaffReportSchemaVersion = 2
	projectStaffReportModeReport    = "report"
	projectStaffReportModeApply     = "apply"

	// projectStaffMaxProjects caps the selected cohort; the command never
	// enumerates every project.
	projectStaffMaxProjects = 1000
	// projectStaffMaxInputBytes bounds the selection file and approved plan.
	projectStaffMaxInputBytes = 8 << 20

	projectStaffRoleExecutiveDirector = "executive_director"
	projectStaffRoleProgramManager    = "program_manager"

	projectStaffActionAssign = "assign"
	projectStaffActionClear  = "clear"
)

// Outcome codes written to reports and logs. None of them carries identity.
const (
	// Report and apply outcomes.
	projectStaffOutcomeInSync            = "in_sync"
	projectStaffOutcomeDrift             = "drift"
	projectStaffOutcomeUnresolvedContact = "unresolved_contact"
	projectStaffOutcomeAmbiguousContact  = "ambiguous_contact"
	projectStaffOutcomeNoProjectMapping  = "no_project_mapping"
	projectStaffOutcomeNoV1Record        = "no_v1_record"
	projectStaffOutcomeV2ProjectNotFound = "v2_project_not_found"
	projectStaffOutcomeError             = "error"

	// Apply-only outcomes.
	projectStaffOutcomePatched           = "patched"
	projectStaffOutcomePatchedUnverified = "patched_unconverged"
	projectStaffOutcomeNowInSync         = "now_in_sync"
	projectStaffOutcomeClearNotApproved  = "clear_not_approved"
	projectStaffOutcomeStaleMapping      = "stale_mapping"
	projectStaffOutcomeStaleV2           = "stale_v2"
	projectStaffOutcomeStaleV1           = "stale_v1"
	projectStaffOutcomeStaleAction       = "stale_action"
	projectStaffOutcomeStaleTarget       = "stale_target"
	projectStaffOutcomeV1ProjectNotFound = "v1_project_not_found"
	projectStaffOutcomeRejected          = "rejected"
	projectStaffOutcomeUnconfirmed       = "unconfirmed"
	projectStaffOutcomeNotAttempted      = "not_attempted"
)

// projectStaffRoleSpec ties a role name to its v2 settings field, its
// v1-objects field and its v1 PATCH key. Opportunity owner is SFDC-owned and
// deliberately absent.
type projectStaffRoleSpec struct {
	role       string
	v1Field    string
	payloadKey string
	v2User     func(*projectservice.ProjectSettings) *projectservice.UserInfo
}

var projectStaffRoles = []projectStaffRoleSpec{
	{
		role:       projectStaffRoleExecutiveDirector,
		v1Field:    "executive_director__c",
		payloadKey: "ExecutiveDirectorID",
		v2User:     func(s *projectservice.ProjectSettings) *projectservice.UserInfo { return s.ExecutiveDirector },
	},
	{
		role:       projectStaffRoleProgramManager,
		v1Field:    "program_manager__c",
		payloadKey: "ProgramManagerID",
		v2User:     func(s *projectservice.ProjectSettings) *projectservice.UserInfo { return s.ProgramManager },
	},
}

func projectStaffRoleByName(role string) (projectStaffRoleSpec, bool) {
	for _, spec := range projectStaffRoles {
		if spec.role == role {
			return spec, true
		}
	}
	return projectStaffRoleSpec{}, false
}

// projectStaffReport is the version-2 report and approval document. Report
// mode writes it; a reviewer sets approved (and clearApproved for a clear) on
// the entries to repair; apply mode reads it back. Target fingerprints use a
// fresh salt per report and never include raw contact identifiers.
type projectStaffReport struct {
	SchemaVersion   int                       `json:"schemaVersion"`
	Mode            string                    `json:"mode"`
	GatewayOrigin   string                    `json:"gatewayOrigin"`
	FingerprintSalt string                    `json:"fingerprintSalt"`
	Summary         map[string]int            `json:"summary"`
	Entries         []projectStaffReportEntry `json:"entries"`
}

// projectStaffReportEntry is one project + role.
type projectStaffReportEntry struct {
	ProjectUID        string `json:"projectUid"`
	ProjectSFID       string `json:"projectSfid,omitempty"`
	Role              string `json:"role"`
	Action            string `json:"action,omitempty"`
	Outcome           string `json:"outcome"`
	Reason            string `json:"reason,omitempty"`
	V2ETag            string `json:"v2Etag,omitempty"`
	V1Revision        uint64 `json:"v1Revision,omitempty"`
	MappingRevision   uint64 `json:"mappingRevision,omitempty"`
	TargetFingerprint string `json:"targetFingerprint,omitempty"`
	Approved          bool   `json:"approved"`
	ClearApproved     bool   `json:"clearApproved"`
}

// projectStaffReconcileOptions are the command-line inputs of
// --reconcile-project-staff-file.
type projectStaffReconcileOptions struct {
	selectionPath    string
	reportPath       string
	apply            bool
	approvedPlanPath string
	dryRun           bool
	emitRate         float64
}

// requested reports whether any project staff flag was passed.
func (o projectStaffReconcileOptions) requested() bool {
	return o.selectionPath != "" || o.reportPath != "" || o.apply || o.approvedPlanPath != ""
}

// validate rejects missing and contradictory flags. It reads no files.
func (o projectStaffReconcileOptions) validate() error {
	if o.selectionPath == "" {
		return errors.New("--project-staff-report, --apply-project-staff and --project-staff-approved-plan require --reconcile-project-staff-file")
	}
	if o.emitRate <= 0 || math.IsNaN(o.emitRate) || math.IsInf(o.emitRate, 0) {
		return fmt.Errorf("--emit-rate must be finite and greater than 0, got %v", o.emitRate)
	}
	if !o.apply {
		if o.approvedPlanPath != "" {
			return errors.New("--project-staff-approved-plan is only used with --apply-project-staff; omit it for a report-only run")
		}
		if o.reportPath == "" {
			return errors.New("--project-staff-report is required for a report-only run")
		}
		return nil
	}
	// Pacing shares the per-project operation budget. Reject intervals that
	// can never fit before starting clients or sending the first PATCH.
	if minimum := 1 / projectStaffOperationTimeout.Seconds(); o.emitRate < minimum {
		return fmt.Errorf("--emit-rate must be at least %g per second (one write per %s operation budget)", minimum, projectStaffOperationTimeout)
	}
	if o.dryRun {
		return errors.New("--dry-run contradicts --apply-project-staff; omit --apply-project-staff for a report-only run")
	}
	if o.approvedPlanPath == "" {
		return errors.New("--apply-project-staff requires --project-staff-approved-plan")
	}
	if o.reportPath != "" && o.reportPath == o.approvedPlanPath {
		return errors.New("--project-staff-report must not be the approved plan file")
	}
	return nil
}

// projectStaffApprovalGroup is the approved entries of one project, all bound
// to the same observed versions.
type projectStaffApprovalGroup struct {
	projectUID      string
	projectSFID     string
	v2ETag          string
	v1Revision      uint64
	mappingRevision uint64
	fingerprintSalt string
	entries         []projectStaffReportEntry
}

// projectStaffReconcileInput is the validated input of one run.
type projectStaffReconcileInput struct {
	opts        projectStaffReconcileOptions
	projectUIDs []string
	// Apply mode only.
	planGatewayOrigin string
	fingerprintSalt   string
	approvals         []projectStaffApprovalGroup
	// Set by checkGateway from the loaded configuration.
	gatewayOrigin string
}

// prepareProjectStaffReconcile validates the flags and reads the selection
// file and, in apply mode, the approved plan. It runs before any client is
// initialized, so a bad input fails without touching NATS, Postgres or an API.
func prepareProjectStaffReconcile(opts projectStaffReconcileOptions) (*projectStaffReconcileInput, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if opts.reportPath != "" {
		if _, err := os.Lstat(opts.reportPath); err == nil {
			return nil, fmt.Errorf("--project-staff-report %s already exists; choose a new file", opts.reportPath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("--project-staff-report: %w", err)
		}
	}
	projectUIDs, err := loadProjectStaffSelection(opts.selectionPath)
	if err != nil {
		return nil, err
	}
	in := &projectStaffReconcileInput{opts: opts, projectUIDs: projectUIDs}
	if opts.apply {
		plan, err := readProjectStaffReport(opts.approvedPlanPath)
		if err != nil {
			return nil, err
		}
		approvals, err := validateProjectStaffApprovals(plan, projectUIDs)
		if err != nil {
			return nil, err
		}
		in.planGatewayOrigin = plan.GatewayOrigin
		in.fingerprintSalt = plan.FingerprintSalt
		in.approvals = approvals
	}
	return in, nil
}

// checkGateway binds the run to the configured v1 API Gateway. An approved
// plan made against another environment is rejected before any write.
func (in *projectStaffReconcileInput) checkGateway(gateway *url.URL) error {
	origin := projectStaffGatewayOrigin(gateway)
	if origin == "" {
		return errors.New("LFX API Gateway URL is not configured")
	}
	if in.opts.apply && origin != in.planGatewayOrigin {
		return fmt.Errorf("approved plan was generated against %s but this run targets %s", in.planGatewayOrigin, origin)
	}
	in.gatewayOrigin = origin
	return nil
}

// projectStaffGatewayOrigin returns the scheme and host of the gateway URL.
func projectStaffGatewayOrigin(gateway *url.URL) string {
	if gateway == nil || gateway.Scheme == "" || gateway.Host == "" {
		return ""
	}
	return strings.ToLower(gateway.Scheme + "://" + gateway.Host)
}

// readProjectStaffInput reads a bounded, UTF-8 input file.
func readProjectStaffInput(path, flagName string) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("%s: %w", flagName, err)
	}
	defer f.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(f, projectStaffMaxInputBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", flagName, err)
	}
	if len(data) > projectStaffMaxInputBytes {
		return nil, fmt.Errorf("%s: file is larger than %d bytes", flagName, projectStaffMaxInputBytes)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%s: file is not valid UTF-8", flagName)
	}
	return data, nil
}

// loadProjectStaffSelection reads the selected v2 project UIDs, one per line.
// Blank lines and lines starting with # are ignored, duplicates are dropped,
// and the cohort is capped at projectStaffMaxProjects. Errors name the line
// number, not its content.
func loadProjectStaffSelection(path string) ([]string, error) {
	const flagName = "--reconcile-project-staff-file"
	data, err := readProjectStaffInput(path, flagName)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var uids []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), projectStaffMaxInputBytes)
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		uid, ok := normalizeProjectStaffUID(text)
		if !ok {
			return nil, fmt.Errorf("%s: line %d is not a project UID", flagName, line)
		}
		if seen[uid] {
			continue
		}
		seen[uid] = true
		uids = append(uids, uid)
		if len(uids) > projectStaffMaxProjects {
			return nil, fmt.Errorf("%s: more than %d projects selected", flagName, projectStaffMaxProjects)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", flagName, err)
	}
	if len(uids) == 0 {
		return nil, fmt.Errorf("%s: no project UIDs selected", flagName)
	}
	return uids, nil
}

// normalizeProjectStaffUID accepts only the canonical 36-character UUID form.
func normalizeProjectStaffUID(s string) (string, bool) {
	if len(s) != 36 {
		return "", false
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return "", false
	}
	return u.String(), true
}

// readProjectStaffReport decodes a version-2 report, rejecting unknown fields.
func readProjectStaffReport(path string) (*projectStaffReport, error) {
	const flagName = "--project-staff-approved-plan"
	data, err := readProjectStaffInput(path, flagName)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var plan projectStaffReport
	if err := dec.Decode(&plan); err != nil {
		return nil, fmt.Errorf("%s: invalid report: %w", flagName, err)
	}
	// More checks for another array/object element, not end-of-input; a
	// second decode must see EOF so stray delimiters cannot be ignored.
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: trailing data after the report", flagName)
	}
	if plan.SchemaVersion != projectStaffReportSchemaVersion {
		return nil, fmt.Errorf("%s: unsupported schemaVersion %d (want %d)", flagName, plan.SchemaVersion, projectStaffReportSchemaVersion)
	}
	if plan.Mode != projectStaffReportModeReport {
		return nil, fmt.Errorf("%s: mode %q is not a report-only run; approve a fresh report", flagName, plan.Mode)
	}
	if plan.GatewayOrigin == "" {
		return nil, fmt.Errorf("%s: gatewayOrigin is missing", flagName)
	}
	return &plan, nil
}

// validateProjectStaffApprovals checks every entry of a reviewed report and
// returns its approved entries grouped by project, in selection order.
// Anything unsupported, contradictory or outside the selection fails the
// whole run before a write.
func validateProjectStaffApprovals(plan *projectStaffReport, selection []string) ([]projectStaffApprovalGroup, error) {
	const flagName = "--project-staff-approved-plan"
	if !validProjectStaffFingerprint(plan.FingerprintSalt) {
		return nil, fmt.Errorf("%s: missing or invalid fingerprintSalt; generate a fresh report", flagName)
	}
	selected := make(map[string]bool, len(selection))
	for _, uid := range selection {
		selected[uid] = true
	}

	type roleKey struct{ project, role string }
	byKey := map[roleKey]projectStaffReportEntry{}
	groups := map[string]*projectStaffApprovalGroup{}
	for i, entry := range plan.Entries {
		uid, ok := normalizeProjectStaffUID(entry.ProjectUID)
		if !ok {
			return nil, fmt.Errorf("%s: entry %d has an invalid projectUid", flagName, i)
		}
		entry.ProjectUID = uid
		if _, ok := projectStaffRoleByName(entry.Role); !ok {
			return nil, fmt.Errorf("%s: entry %d has unsupported role %q", flagName, i, entry.Role)
		}
		if entry.Action != "" && entry.Action != projectStaffActionAssign && entry.Action != projectStaffActionClear {
			return nil, fmt.Errorf("%s: entry %d has unsupported action %q", flagName, i, entry.Action)
		}
		if entry.ClearApproved && (!entry.Approved || entry.Action != projectStaffActionClear) {
			return nil, fmt.Errorf("%s: entry %d sets clearApproved without an approved clear", flagName, i)
		}

		key := roleKey{uid, entry.Role}
		if prev, dup := byKey[key]; dup {
			if prev != entry {
				return nil, fmt.Errorf("%s: conflicting duplicate entries for project %s role %s", flagName, uid, entry.Role)
			}
			continue
		}
		byKey[key] = entry

		if !entry.Approved {
			continue
		}
		if !selected[uid] {
			return nil, fmt.Errorf("%s: approved project %s is not in --reconcile-project-staff-file", flagName, uid)
		}
		if entry.Action == "" {
			return nil, fmt.Errorf("%s: entry %d is approved but has no action", flagName, i)
		}
		if entry.ProjectSFID == "" || entry.V2ETag == "" || entry.V1Revision == 0 || entry.MappingRevision == 0 {
			return nil, fmt.Errorf("%s: approved entry %d is missing its observed versions", flagName, i)
		}
		if !validProjectStaffFingerprint(entry.TargetFingerprint) {
			return nil, fmt.Errorf("%s: approved entry %d has a missing or invalid targetFingerprint; generate a fresh report", flagName, i)
		}
		group, ok := groups[uid]
		if !ok {
			group = &projectStaffApprovalGroup{
				projectUID:      uid,
				projectSFID:     entry.ProjectSFID,
				v2ETag:          entry.V2ETag,
				v1Revision:      entry.V1Revision,
				mappingRevision: entry.MappingRevision,
				fingerprintSalt: plan.FingerprintSalt,
			}
			groups[uid] = group
		} else if group.projectSFID != entry.ProjectSFID || group.v2ETag != entry.V2ETag ||
			group.v1Revision != entry.V1Revision || group.mappingRevision != entry.MappingRevision {
			return nil, fmt.Errorf("%s: approved entries for project %s disagree on observed versions", flagName, uid)
		}
		group.entries = append(group.entries, entry)
	}

	var out []projectStaffApprovalGroup
	for _, uid := range selection {
		if group, ok := groups[uid]; ok {
			sort.SliceStable(group.entries, func(a, b int) bool {
				return projectStaffRoleIndex(group.entries[a].Role) < projectStaffRoleIndex(group.entries[b].Role)
			})
			out = append(out, *group)
		}
	}
	return out, nil
}

func projectStaffRoleIndex(role string) int {
	for i, spec := range projectStaffRoles {
		if spec.role == role {
			return i
		}
	}
	return len(projectStaffRoles)
}

// projectStaffMappingReader is the read-only view of v1-mappings this command
// uses. It has no write methods, so the command cannot write mappings.
type projectStaffMappingReader interface {
	Get(ctx context.Context, key string) (MappingEntry, error)
}

// Injectable for tests.
var (
	projectStaffOpenPGPool   = initPGPool
	projectStaffCheckPGTable = checkV1MappingsTableExists
)

// openProjectStaffMappingReader returns a read-only mapping reader for the
// configured V1_MAPPINGS_STORE_MODE: the existing KV bucket for kv and for
// KV-authoritative dual, and the existing Postgres table for postgres. It
// never applies schema, never creates a bucket or table, and never starts the
// dual-store mirror worker. The returned close function releases any pool.
func openProjectStaffMappingReader(ctx context.Context, cfg *Config, kv jetstream.KeyValue) (projectStaffMappingReader, func(), error) {
	switch cfg.V1MappingsStoreMode {
	case V1MappingsStoreModeKV, V1MappingsStoreModeDual:
		if kv == nil {
			return nil, nil, errors.New("v1-mappings KV bucket is not open")
		}
		return newKVMappingStore(kv), func() {}, nil
	case V1MappingsStoreModePostgres:
		pool, err := projectStaffOpenPGPool(ctx, cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("open v1-mappings Postgres pool: %w", err)
		}
		closePool := func() {
			if pool != nil {
				pool.Close()
			}
		}
		if err := projectStaffCheckPGTable(ctx, pool); err != nil {
			closePool()
			return nil, nil, err
		}
		return newPGMappingStore(pool), closePool, nil
	default:
		return nil, nil, fmt.Errorf("invalid v1-mappings store mode %q", cfg.V1MappingsStoreMode)
	}
}

// checkV1MappingsTableExists fails when the v1_mappings table is absent. It
// only reads the catalog; creating the table is not this command's job.
func checkV1MappingsTableExists(ctx context.Context, pool *pgxpool.Pool) error {
	qCtx, cancel := context.WithTimeout(ctx, defaultPGMappingStoreTimeout)
	defer cancel()
	var name *string
	if err := pool.QueryRow(qCtx, "SELECT to_regclass($1)::text", v1MappingsTableName).Scan(&name); err != nil {
		return fmt.Errorf("check v1-mappings table: %w", err)
	}
	if name == nil {
		return fmt.Errorf("v1-mappings table %s does not exist", v1MappingsTableName)
	}
	return nil
}

// Injectable for tests.
var (
	reconcileFetchProjectSettingsFn = fetchProjectSettings
	reconcileResolveStaffByUsername = dbResolveUniqueStaffSFIDByUsername
	reconcileResolveStaffByEmail    = dbResolveUniqueStaffSFIDByEmail
)

// projectStaffOperationTimeout bounds one project's reads and PATCH. Vars so
// tests can shorten them.
var (
	projectStaffOperationTimeout     = 30 * time.Second
	projectStaffConvergenceTimeout   = 60 * time.Second
	projectStaffConvergenceInterval  = 2 * time.Second
	projectStaffConvergenceReadLimit = 5 * time.Second
)

// projectStaffReconcileResult counts entries by outcome.
type projectStaffReconcileResult struct {
	apply    bool
	projects int
	outcomes map[string]int
}

func (res *projectStaffReconcileResult) count(outcome string) {
	if res.outcomes == nil {
		res.outcomes = map[string]int{}
	}
	res.outcomes[outcome]++
}

// logFields returns the run summary for logger.With.
func (res projectStaffReconcileResult) logFields() []any {
	fields := []any{"apply", res.apply, "projects", res.projects}
	keys := make([]string, 0, len(res.outcomes))
	for k := range res.outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fields = append(fields, k, res.outcomes[k])
	}
	return fields
}

// failed reports whether the run must exit non-zero. A report fails on any
// read error. An apply fails unless every approved entry was patched and
// converged, was already in sync, or was a clear the reviewer did not approve.
func (res projectStaffReconcileResult) failed() bool {
	if !res.apply {
		return res.outcomes[projectStaffOutcomeError] > 0
	}
	for outcome, n := range res.outcomes {
		if n == 0 {
			continue
		}
		switch outcome {
		case projectStaffOutcomePatched, projectStaffOutcomeNowInSync, projectStaffOutcomeClearNotApproved:
		default:
			return true
		}
	}
	return false
}

// projectStaffObservation is one project's current state. outcome is set when
// the project cannot be compared (no mapping, no v1 record, read failure).
type projectStaffObservation struct {
	projectSFID     string
	mappingRevision uint64
	v2ETag          string
	v1Revision      uint64
	settings        *projectservice.ProjectSettings
	v1Staff         map[string]string
	outcome         string
	reason          string
}

// observeProjectStaff reads the reverse mapping, the v2 settings and the v1
// record of one project, with their versions.
func observeProjectStaff(ctx context.Context, mappings projectStaffMappingReader, projectUID string) projectStaffObservation {
	var obs projectStaffObservation

	entry, err := mappings.Get(ctx, "project.uid."+projectUID)
	switch {
	case errors.Is(err, ErrKeyNotFound):
		obs.outcome = projectStaffOutcomeNoProjectMapping
		return obs
	case err != nil:
		obs.outcome, obs.reason = projectStaffOutcomeError, "mapping_read_failed"
		return obs
	case isTombstonedMapping(entry.Value) || strings.TrimSpace(string(entry.Value)) == "":
		obs.outcome = projectStaffOutcomeNoProjectMapping
		return obs
	}
	obs.projectSFID = strings.TrimSpace(string(entry.Value))
	obs.mappingRevision = entry.Revision

	settings, etag, err := reconcileFetchProjectSettingsFn(ctx, projectUID)
	if err != nil {
		var notFound *projectservice.NotFoundError
		if errors.As(err, &notFound) {
			obs.outcome = projectStaffOutcomeV2ProjectNotFound
		} else {
			obs.outcome, obs.reason = projectStaffOutcomeError, "v2_settings_read_failed"
		}
		return obs
	}
	if settings == nil {
		obs.outcome, obs.reason = projectStaffOutcomeError, "v2_settings_empty"
		return obs
	}
	obs.settings = settings
	obs.v2ETag = etag

	snapshot, err := readV1ProjectStaffSnapshot(ctx, obs.projectSFID)
	if err != nil {
		obs.outcome, obs.reason = projectStaffOutcomeError, "v1_record_read_failed"
		return obs
	}
	if snapshot == nil {
		obs.outcome = projectStaffOutcomeNoV1Record
		return obs
	}
	obs.v1Staff = snapshot.staff
	obs.v1Revision = snapshot.revision
	return obs
}

// projectStaffRoleState is the comparison of one role. target is the v1
// PATCH value (a contact SFID or the clear literal); it never leaves memory.
type projectStaffRoleState struct {
	outcome string
	reason  string
	action  string
	target  string
}

// resolveProjectStaffContact preserves the bridge's username-first fallback,
// but uses uniqueness-checking DB reads. Ambiguity is a terminal result: do
// not fall through to another identifier that could select a different owner.
func resolveProjectStaffContact(ctx context.Context, info *events.UserInfo) (string, error) {
	if info == nil {
		return v1ClearStaffValue, nil
	}
	if username := strings.TrimSpace(info.Username); username != "" {
		contact, err := reconcileResolveStaffByUsername(ctx, username)
		if err != nil || contact != "" {
			return contact, err
		}
	}
	if email := strings.TrimSpace(info.Email); email != "" {
		return reconcileResolveStaffByEmail(ctx, email)
	}
	return "", nil
}

// classifyProjectStaffRole resolves the v2 assignee to a v1 contact and
// compares it with v1. An empty v2 role is a clear; a v2 assignee that cannot
// be resolved is unresolved_contact, never a clear.
func classifyProjectStaffRole(ctx context.Context, log *slog.Logger, obs projectStaffObservation, spec projectStaffRoleSpec) projectStaffRoleState {
	target, err := resolveProjectStaffContact(ctx, projectStaffEventUser(spec.v2User(obs.settings)))
	if errors.Is(err, errAmbiguousProjectStaffContact) {
		return projectStaffRoleState{outcome: projectStaffOutcomeAmbiguousContact}
	}
	if err != nil {
		return projectStaffRoleState{outcome: projectStaffOutcomeError, reason: "contact_lookup_failed"}
	}
	if target == "" {
		log.With("field", spec.role).WarnContext(ctx, "could not resolve v1 contact for staff user, leaving v1 field unchanged")
		return projectStaffRoleState{outcome: projectStaffOutcomeUnresolvedContact}
	}
	if target == staffValueOrNone(obs.v1Staff[spec.v1Field]) {
		return projectStaffRoleState{outcome: projectStaffOutcomeInSync}
	}
	action := projectStaffActionAssign
	if target == v1ClearStaffValue {
		action = projectStaffActionClear
	}
	return projectStaffRoleState{outcome: projectStaffOutcomeDrift, action: action, target: target}
}

// projectStaffEventUser converts a v2 settings user to the event shape the
// bridge's resolver takes. A nil or all-empty user is an empty role.
func projectStaffEventUser(u *projectservice.UserInfo) *events.UserInfo {
	if userInfoPtrsEqual(u, nil) {
		return nil
	}
	return &events.UserInfo{
		Name:     stringPtrToString(u.Name),
		Email:    stringPtrToString(u.Email),
		Username: stringPtrToString(u.Username),
		Avatar:   stringPtrToString(u.Avatar),
	}
}

// projectStaffTargetFingerprint binds approval to the resolved contact without
// storing its SFID. The per-report salt prevents correlation across reports;
// these fingerprints are pseudonymous data, not anonymized identities.
func projectStaffTargetFingerprint(salt, projectUID, role, target string) string {
	digest := sha256.Sum256([]byte(salt + "\x00" + projectUID + "\x00" + role + "\x00" + target))
	return hex.EncodeToString(digest[:])
}

func validProjectStaffFingerprint(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// reconcileProjectStaff runs one report or apply pass over the selection. It
// returns the report (also written for apply results) and the counters. An
// error means the run stopped early: cancellation, or an apply PATCH whose
// outcome could not be confirmed.
func reconcileProjectStaff(ctx context.Context, in *projectStaffReconcileInput, mappings projectStaffMappingReader) (*projectStaffReport, projectStaffReconcileResult, error) {
	res := projectStaffReconcileResult{apply: in.opts.apply}
	mode := projectStaffReportModeReport
	if in.opts.apply {
		mode = projectStaffReportModeApply
	}
	report := &projectStaffReport{
		SchemaVersion:   projectStaffReportSchemaVersion,
		Mode:            mode,
		GatewayOrigin:   in.gatewayOrigin,
		FingerprintSalt: in.fingerprintSalt,
	}
	if !in.opts.apply {
		salt := make([]byte, sha256.Size)
		if _, err := rand.Read(salt); err != nil {
			return nil, res, fmt.Errorf("generate project staff report salt: %w", err)
		}
		report.FingerprintSalt = hex.EncodeToString(salt)
	}

	var err error
	if in.opts.apply {
		err = applyProjectStaff(ctx, in, mappings, report, &res)
	} else {
		err = reportProjectStaff(ctx, in, mappings, report, &res)
	}

	report.Summary = map[string]int{}
	for _, entry := range report.Entries {
		report.Summary[entry.Outcome]++
		res.count(entry.Outcome)
	}
	return report, res, err
}

// reportProjectStaff compares every selected project and role. It only reads.
func reportProjectStaff(ctx context.Context, in *projectStaffReconcileInput, mappings projectStaffMappingReader, report *projectStaffReport, res *projectStaffReconcileResult) error {
	for _, uid := range in.projectUIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		res.projects++
		log := logger.With("project_uid", uid)
		opCtx, cancel := context.WithTimeout(ctx, projectStaffOperationTimeout)
		obs := observeProjectStaff(opCtx, mappings, uid)
		for _, spec := range projectStaffRoles {
			entry := projectStaffReportEntry{
				ProjectUID:      uid,
				ProjectSFID:     obs.projectSFID,
				Role:            spec.role,
				V2ETag:          obs.v2ETag,
				V1Revision:      obs.v1Revision,
				MappingRevision: obs.mappingRevision,
			}
			if obs.outcome != "" {
				entry.Outcome, entry.Reason = obs.outcome, obs.reason
			} else {
				state := classifyProjectStaffRole(opCtx, log, obs, spec)
				entry.Outcome, entry.Reason, entry.Action = state.outcome, state.reason, state.action
				if state.outcome == projectStaffOutcomeDrift {
					entry.TargetFingerprint = projectStaffTargetFingerprint(report.FingerprintSalt, uid, spec.role, state.target)
				}
			}
			logProjectStaffEntry(ctx, entry)
			report.Entries = append(report.Entries, entry)
		}
		cancel()
	}
	return nil
}

// applyProjectStaff repairs the approved roles, one project at a time. It
// stops at the first PATCH whose outcome is unknown; the remaining approvals
// are reported as not_attempted and need a fresh report and review.
func applyProjectStaff(ctx context.Context, in *projectStaffReconcileInput, mappings projectStaffMappingReader, report *projectStaffReport, res *projectStaffReconcileResult) error {
	writeInterval := time.Duration(math.Ceil(float64(time.Second) / in.opts.emitRate))
	var nextPatchAt time.Time
	var stopErr error
	for _, group := range in.approvals {
		res.projects++
		if stopErr != nil {
			for _, entry := range group.entries {
				entry.Outcome, entry.Reason = projectStaffOutcomeNotAttempted, ""
				logProjectStaffEntry(ctx, entry)
				report.Entries = append(report.Entries, entry)
			}
			continue
		}
		entries, err := applyProjectStaffGroup(ctx, mappings, writeInterval, &nextPatchAt, group)
		for _, entry := range entries {
			logProjectStaffEntry(ctx, entry)
		}
		report.Entries = append(report.Entries, entries...)
		if err != nil {
			stopErr = err
		}
	}
	return stopErr
}

// applyProjectStaffGroup re-checks one project against its approved versions
// and PATCHes only the approved roles that still differ the same way.
func applyProjectStaffGroup(ctx context.Context, mappings projectStaffMappingReader, writeInterval time.Duration, nextPatchAt *time.Time, group projectStaffApprovalGroup) ([]projectStaffReportEntry, error) {
	entries := append([]projectStaffReportEntry(nil), group.entries...)
	setAll := func(idx []int, outcome, reason string) {
		for _, i := range idx {
			entries[i].Outcome, entries[i].Reason = outcome, reason
		}
	}

	// A clear needs its own approval; without it the role is left alone.
	var pending []int
	for i := range entries {
		if entries[i].Action == projectStaffActionClear && !entries[i].ClearApproved {
			entries[i].Outcome, entries[i].Reason = projectStaffOutcomeClearNotApproved, ""
			continue
		}
		pending = append(pending, i)
	}
	if len(pending) == 0 {
		return entries, nil
	}

	log := logger.With("project_uid", group.projectUID)
	opCtx, cancel := context.WithTimeout(ctx, projectStaffOperationTimeout)
	defer cancel()

	obs := observeProjectStaff(opCtx, mappings, group.projectUID)
	switch {
	case obs.outcome == projectStaffOutcomeNoProjectMapping:
		setAll(pending, projectStaffOutcomeStaleMapping, "mapping_removed")
		return entries, nil
	case obs.outcome == projectStaffOutcomeError && obs.reason == "mapping_read_failed":
		setAll(pending, obs.outcome, obs.reason)
		return entries, nil
	case obs.projectSFID != group.projectSFID || obs.mappingRevision != group.mappingRevision:
		setAll(pending, projectStaffOutcomeStaleMapping, "")
		return entries, nil
	case obs.outcome != "" && obs.outcome != projectStaffOutcomeNoV1Record:
		setAll(pending, obs.outcome, obs.reason)
		return entries, nil
	case obs.v2ETag != group.v2ETag:
		setAll(pending, projectStaffOutcomeStaleV2, "")
		return entries, nil
	case obs.outcome == projectStaffOutcomeNoV1Record:
		setAll(pending, obs.outcome, "")
		return entries, nil
	case obs.v1Revision != group.v1Revision:
		setAll(pending, projectStaffOutcomeStaleV1, "")
		return entries, nil
	}

	payload := map[string]string{}
	targets := map[string]string{} // v1 field -> PATCH value
	var patched []int
	for _, i := range pending {
		spec, _ := projectStaffRoleByName(entries[i].Role)
		state := classifyProjectStaffRole(opCtx, log, obs, spec)
		switch {
		case state.outcome == projectStaffOutcomeInSync:
			entries[i].Outcome, entries[i].Reason = projectStaffOutcomeNowInSync, ""
		case state.outcome != projectStaffOutcomeDrift:
			entries[i].Outcome, entries[i].Reason = state.outcome, state.reason
		case state.action != entries[i].Action:
			entries[i].Outcome, entries[i].Reason = projectStaffOutcomeStaleAction, ""
		case projectStaffTargetFingerprint(group.fingerprintSalt, group.projectUID, spec.role, state.target) != entries[i].TargetFingerprint:
			entries[i].Outcome, entries[i].Reason = projectStaffOutcomeStaleTarget, ""
		default:
			payload[spec.payloadKey] = state.target
			targets[spec.v1Field] = state.target
			patched = append(patched, i)
		}
	}
	if len(payload) == 0 {
		return entries, nil
	}

	// Space PATCHes from the preceding request's completion, not a token
	// consumed before authentication. A slow OAuth fetch must not refill the
	// write allowance and permit back-to-back dispatches. Waiting still shares
	// this project's operation budget; no request is sent if it cannot fit.
	if delay := time.Until(*nextPatchAt); delay > 0 {
		if deadline, ok := opCtx.Deadline(); ok && nextPatchAt.After(deadline) {
			setAll(patched, projectStaffOutcomeNotAttempted, "")
			return entries, context.DeadlineExceeded
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-opCtx.Done():
			setAll(patched, projectStaffOutcomeNotAttempted, "")
			return entries, opCtx.Err()
		case <-timer.C:
		}
	}
	if err := opCtx.Err(); err != nil {
		setAll(patched, projectStaffOutcomeNotAttempted, "")
		return entries, err
	}

	err := patchV1ProjectStaff(opCtx, group.projectSFID, payload)
	*nextPatchAt = time.Now().Add(writeInterval)
	if err != nil {
		var statusErr *v1ProjectStaffStatusError
		switch {
		case errors.Is(err, errV1ProjectNotFound):
			setAll(patched, projectStaffOutcomeV1ProjectNotFound, "")
			return entries, nil
		case errors.As(err, &statusErr) && statusErr.status < http.StatusInternalServerError:
			setAll(patched, projectStaffOutcomeRejected, fmt.Sprintf("http_%d", statusErr.status))
			return entries, nil
		}
		// The request may have reached v1. Do not retry and do not continue:
		// the operator must observe the project and approve a fresh report.
		reason := "transport_error"
		switch {
		case errors.As(err, &statusErr):
			reason = fmt.Sprintf("http_%d", statusErr.status)
		case errors.Is(err, context.DeadlineExceeded):
			reason = "timeout"
		}
		setAll(patched, projectStaffOutcomeUnconfirmed, reason)
		return entries, fmt.Errorf("v1 PATCH outcome unconfirmed for project %s (%s); stopping", group.projectUID, reason)
	}

	if waitProjectStaffConvergence(ctx, group.projectSFID, targets) {
		setAll(patched, projectStaffOutcomePatched, "")
	} else {
		setAll(patched, projectStaffOutcomePatchedUnverified, "replication_pending")
	}
	return entries, nil
}

// waitProjectStaffConvergence polls the replicated v1 record until every
// patched field holds its target, for at most projectStaffConvergenceTimeout.
func waitProjectStaffConvergence(ctx context.Context, sfid string, targets map[string]string) bool {
	ctx, cancel := context.WithTimeout(ctx, projectStaffConvergenceTimeout)
	defer cancel()
	for ctx.Err() == nil {
		readCtx, cancel := context.WithTimeout(ctx, projectStaffConvergenceReadLimit)
		snapshot, err := readV1ProjectStaffSnapshot(readCtx, sfid)
		cancel()
		if err == nil && snapshot != nil && ctx.Err() == nil {
			converged := true
			for field, target := range targets {
				if staffValueOrNone(snapshot.staff[field]) != target {
					converged = false
					break
				}
			}
			if converged {
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(projectStaffConvergenceInterval):
		}
	}
	return false
}

// logProjectStaffEntry logs one entry: resource identifiers, role and codes only.
func logProjectStaffEntry(ctx context.Context, entry projectStaffReportEntry) {
	log := logger.With("project_uid", entry.ProjectUID, "role", entry.Role, "outcome", entry.Outcome)
	if entry.Action != "" {
		log = log.With("action", entry.Action)
	}
	if entry.Reason != "" {
		log = log.With("reason", entry.Reason)
	}
	switch entry.Outcome {
	case projectStaffOutcomeError, projectStaffOutcomeRejected, projectStaffOutcomeUnconfirmed,
		projectStaffOutcomePatchedUnverified, projectStaffOutcomeV1ProjectNotFound:
		log.WarnContext(ctx, "project staff reconcile: entry needs attention")
	default:
		log.InfoContext(ctx, "project staff reconcile: entry")
	}
}

// writeProjectStaffReport creates path (it must not exist) with mode 0600 and
// writes the report. A partially written file is removed.
func writeProjectStaffReport(path string, report *projectStaffReport) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // operator-supplied path
	if err != nil {
		return fmt.Errorf("create project staff report: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close project staff report: %w", closeErr)
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return fmt.Errorf("write project staff report: %w", err)
	}
	return nil
}

// runProjectStaffReconcile is the --reconcile-project-staff-file one-shot. It
// opens the read-only mapping reader, runs the pass, writes the report and
// releases the pool and v1 database it used. Returns the process exit code.
func runProjectStaffReconcile(ctx context.Context, in *projectStaffReconcileInput) int {
	defer func() {
		if v1DB != nil {
			_ = v1DB.Close()
		}
	}()

	mappings, closeMappings, err := openProjectStaffMappingReader(ctx, cfg, mappingsKV)
	if err != nil {
		logger.With(errKey, err, "mode", string(cfg.V1MappingsStoreMode)).Error("error opening v1-mappings reader for project staff reconciliation")
		return 1
	}
	defer closeMappings()

	logger.With(
		"apply", in.opts.apply,
		"projects_selected", len(in.projectUIDs),
		"projects_approved", len(in.approvals),
		"emit_rate", in.opts.emitRate,
		"mode", string(cfg.V1MappingsStoreMode),
	).Info("starting project staff reconciliation")

	report, res, runErr := reconcileProjectStaff(ctx, in, mappings)

	exitCode := 0
	if in.opts.reportPath != "" {
		if err := writeProjectStaffReport(in.opts.reportPath, report); err != nil {
			logger.With(errKey, err).Error("error writing project staff report")
			exitCode = 1
		}
	}
	switch {
	case runErr != nil:
		logger.With(append(res.logFields(), errKey, runErr)...).Error("project staff reconciliation stopped")
		exitCode = 1
	case res.failed():
		logger.With(res.logFields()...).Error("project staff reconciliation completed with entries that need attention")
		exitCode = 1
	default:
		logger.With(res.logFields()...).Info("project staff reconciliation completed successfully")
	}
	return exitCode
}
