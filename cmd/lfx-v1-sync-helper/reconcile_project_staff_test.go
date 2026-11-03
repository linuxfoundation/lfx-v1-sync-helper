// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

// Synthetic fixtures. The canary values stand in for staff identity and must
// never appear in a report or a log line.
const (
	staffTestProjectA        = "11111111-1111-4111-8111-111111111111"
	staffTestProjectB        = "22222222-2222-4222-8222-222222222222"
	staffTestSFIDA           = "a0941000002wSYNAAA"
	staffTestSFIDB           = "a0941000002wSYNBBB"
	staffTestFingerprintSalt = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	staffCanaryEDUsername = "canary-ed-username"
	staffCanaryEDEmail    = "canary-ed@example.invalid"
	staffCanaryEDName     = "Canary Executive"
	staffCanaryPMUsername = "canary-pm-username"
	staffCanaryManualMail = "canary-manual@example.invalid"
	staffCanaryEDContact  = "003CANARYED0000001"
	staffCanaryPMContact  = "003CANARYPM0000001"
	staffCanaryOldContact = "003CANARYOLD000001"
)

var staffCanaries = []string{
	staffCanaryEDUsername, staffCanaryEDEmail, staffCanaryEDName, staffCanaryPMUsername,
	staffCanaryManualMail, staffCanaryEDContact, staffCanaryPMContact, staffCanaryOldContact,
}

type staffTestPatch struct {
	path string
	body map[string]string
}

// staffTestEnv replaces every dependency of the reconciliation with an
// in-memory fake: the mapping reader, v2 settings, the v1-objects snapshot,
// the v1 DB resolvers and the v1 API Gateway (an httptest server).
type staffTestEnv struct {
	mu          sync.Mutex
	store       *fakeMappingStore
	settings    map[string]*projectservice.ProjectSettings
	etags       map[string]string
	settingsErr map[string]error
	v1          map[string]*v1ProjectStaffSnapshot
	contacts    map[string]string // username or email -> contact SFID
	lookupErr   error
	patches     []staffTestPatch
	patchStatus int
	patchDelay  time.Duration
	replicate   bool // apply a successful PATCH to the v1 snapshot
	logs        *bytes.Buffer
	logMu       sync.Mutex
}

type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func setupProjectStaffTest(t *testing.T) *staffTestEnv {
	t.Helper()
	env := &staffTestEnv{
		store:       newFakeMappingStore(),
		settings:    map[string]*projectservice.ProjectSettings{},
		etags:       map[string]string{},
		settingsErr: map[string]error{},
		v1:          map[string]*v1ProjectStaffSnapshot{},
		contacts: map[string]string{
			staffCanaryEDUsername: staffCanaryEDContact,
			staffCanaryPMUsername: staffCanaryPMContact,
		},
		patchStatus: http.StatusOK,
		replicate:   true,
		logs:        &bytes.Buffer{},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		env.mu.Lock()
		env.patches = append(env.patches, staffTestPatch{path: r.URL.Path, body: body})
		status, delay, replicate := env.patchStatus, env.patchDelay, env.replicate
		env.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if replicate && (status == http.StatusOK || status == http.StatusNoContent) {
			env.mu.Lock()
			sfid := strings.TrimPrefix(r.URL.Path, "/project-service/v1/projects/")
			if snap := env.v1[sfid]; snap != nil {
				for key, value := range body {
					field := map[string]string{"ExecutiveDirectorID": "executive_director__c", "ProgramManagerID": "program_manager__c"}[key]
					if value == v1ClearStaffValue {
						value = ""
					}
					snap.staff[field] = value
				}
				snap.revision++
			}
			env.mu.Unlock()
		}
		// A body that echoes the payload, as an upstream error might.
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	gateway, err := url.Parse(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}

	origCfg, origLogger, origClient := cfg, logger, v1HTTPClient
	origSnapshot, origFetch := readV1ProjectStaffSnapshot, reconcileFetchProjectSettingsFn
	origByUsername, origByEmail := resolveV1UserSFIDByUsername, resolveV1UserSFIDByEmail
	origOp, origConv, origInterval := projectStaffOperationTimeout, projectStaffConvergenceTimeout, projectStaffConvergenceInterval

	cfg = &Config{LFXAPIGateway: gateway, V1MappingsStoreMode: V1MappingsStoreModeKV}
	logger = slog.New(slog.NewJSONHandler(lockedWriter{&env.logMu, env.logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	v1HTTPClient = srv.Client()
	projectStaffOperationTimeout = 2 * time.Second
	projectStaffConvergenceTimeout = 100 * time.Millisecond
	projectStaffConvergenceInterval = 10 * time.Millisecond

	readV1ProjectStaffSnapshot = func(_ context.Context, sfid string) (*v1ProjectStaffSnapshot, error) {
		env.mu.Lock()
		defer env.mu.Unlock()
		snap, ok := env.v1[sfid]
		if !ok {
			return nil, nil
		}
		staff := map[string]string{}
		for k, v := range snap.staff {
			staff[k] = v
		}
		return &v1ProjectStaffSnapshot{staff: staff, revision: snap.revision}, nil
	}
	reconcileFetchProjectSettingsFn = func(_ context.Context, uid string) (*projectservice.ProjectSettings, string, error) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if err := env.settingsErr[uid]; err != nil {
			return nil, "", err
		}
		return env.settings[uid], env.etags[uid], nil
	}
	resolve := func(_ context.Context, id string) (string, error) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if env.lookupErr != nil {
			return "", env.lookupErr
		}
		return env.contacts[id], nil
	}
	resolveV1UserSFIDByUsername = resolve
	resolveV1UserSFIDByEmail = resolve

	t.Cleanup(func() {
		cfg, logger, v1HTTPClient = origCfg, origLogger, origClient
		readV1ProjectStaffSnapshot, reconcileFetchProjectSettingsFn = origSnapshot, origFetch
		resolveV1UserSFIDByUsername, resolveV1UserSFIDByEmail = origByUsername, origByEmail
		projectStaffOperationTimeout, projectStaffConvergenceTimeout, projectStaffConvergenceInterval = origOp, origConv, origInterval
	})
	return env
}

func strPtr(s string) *string { return &s }

// seedProject maps uid to sfid and seeds v2 settings and the v1 record.
func (env *staffTestEnv) seedProject(t *testing.T, uid, sfid string, ed, pm *projectservice.UserInfo, v1ED, v1PM string) {
	t.Helper()
	if _, err := env.store.Put(t.Context(), "project.uid."+uid, []byte(sfid)); err != nil {
		t.Fatal(err)
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	env.settings[uid] = &projectservice.ProjectSettings{UID: strPtr(uid), ExecutiveDirector: ed, ProgramManager: pm}
	env.etags[uid] = "7"
	env.v1[sfid] = &v1ProjectStaffSnapshot{
		staff:    map[string]string{"executive_director__c": v1ED, "program_manager__c": v1PM},
		revision: 40,
	}
}

func canaryED() *projectservice.UserInfo {
	return &projectservice.UserInfo{Name: strPtr(staffCanaryEDName), Email: strPtr(staffCanaryEDEmail), Username: strPtr(staffCanaryEDUsername)}
}

func canaryPM() *projectservice.UserInfo {
	return &projectservice.UserInfo{Username: strPtr(staffCanaryPMUsername)}
}

func (env *staffTestEnv) patchCalls() []staffTestPatch {
	env.mu.Lock()
	defer env.mu.Unlock()
	return append([]staffTestPatch(nil), env.patches...)
}

func (env *staffTestEnv) assertNoMappingWrites(t *testing.T) {
	t.Helper()
	if s := env.store; s.putCalls != 1*len(env.settings) || s.updateCalls != 0 || s.createCalls != 0 || s.deleteCalls != 0 {
		// putCalls only counts the seed writes.
		t.Errorf("mapping writes: put=%d (seeded %d) update=%d create=%d delete=%d; want no writes by the command",
			s.putCalls, len(env.settings), s.updateCalls, s.createCalls, s.deleteCalls)
	}
}

// assertNoCanaries fails if any staff identity value reached the report or
// the logs.
func (env *staffTestEnv) assertNoCanaries(t *testing.T, report *projectStaffReport) {
	t.Helper()
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	env.logMu.Lock()
	logs := env.logs.String()
	env.logMu.Unlock()
	for _, canary := range staffCanaries {
		if strings.Contains(string(data), canary) {
			t.Errorf("report contains identity value %q", canary)
		}
		if strings.Contains(logs, canary) {
			t.Errorf("logs contain identity value %q", canary)
		}
	}
}

func reportInput(uids ...string) *projectStaffReconcileInput {
	return &projectStaffReconcileInput{
		opts:          projectStaffReconcileOptions{emitRate: 1000},
		projectUIDs:   uids,
		gatewayOrigin: projectStaffGatewayOrigin(cfg.LFXAPIGateway),
	}
}

// approve marks the report entries selected by pick as approved (and
// clear-approved when clear is true), validates the plan and returns an
// apply input, as a reviewer editing the report would.
func approve(t *testing.T, report *projectStaffReport, uids []string, pick func(projectStaffReportEntry) bool, clear bool) *projectStaffReconcileInput {
	t.Helper()
	plan := *report
	plan.Entries = append([]projectStaffReportEntry(nil), report.Entries...)
	for i := range plan.Entries {
		if pick(plan.Entries[i]) {
			plan.Entries[i].Approved = true
			plan.Entries[i].ClearApproved = clear && plan.Entries[i].Action == projectStaffActionClear
		}
	}
	approvals, err := validateProjectStaffApprovals(&plan, uids)
	if err != nil {
		t.Fatalf("validateProjectStaffApprovals: %v", err)
	}
	return &projectStaffReconcileInput{
		opts:              projectStaffReconcileOptions{apply: true, emitRate: 1000},
		projectUIDs:       uids,
		planGatewayOrigin: plan.GatewayOrigin,
		fingerprintSalt:   plan.FingerprintSalt,
		approvals:         approvals,
		gatewayOrigin:     projectStaffGatewayOrigin(cfg.LFXAPIGateway),
	}
}

func runReport(t *testing.T, env *staffTestEnv, uids ...string) (*projectStaffReport, projectStaffReconcileResult) {
	t.Helper()
	report, res, err := reconcileProjectStaff(t.Context(), reportInput(uids...), env.store)
	if err != nil {
		t.Fatalf("report run: %v", err)
	}
	return report, res
}

func entryFor(t *testing.T, report *projectStaffReport, uid, role string) projectStaffReportEntry {
	t.Helper()
	for _, e := range report.Entries {
		if e.ProjectUID == uid && e.Role == role {
			return e
		}
	}
	t.Fatalf("no entry for %s %s", uid, role)
	return projectStaffReportEntry{}
}

func isRole(uid, role string) func(projectStaffReportEntry) bool {
	return func(e projectStaffReportEntry) bool { return e.ProjectUID == uid && e.Role == role }
}

func TestReconcileProjectStaff(t *testing.T) {
	t.Run("report finds drift without any write", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		// ED differs (assign), PM is empty in v2 but set in v1 (clear).
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, staffCanaryOldContact, staffCanaryPMContact)

		report, res := runReport(t, env, staffTestProjectA)

		ed := entryFor(t, report, staffTestProjectA, projectStaffRoleExecutiveDirector)
		if ed.Outcome != projectStaffOutcomeDrift || ed.Action != projectStaffActionAssign {
			t.Errorf("ED = %s/%s, want drift/assign", ed.Outcome, ed.Action)
		}
		pm := entryFor(t, report, staffTestProjectA, projectStaffRoleProgramManager)
		if pm.Outcome != projectStaffOutcomeDrift || pm.Action != projectStaffActionClear {
			t.Errorf("PM = %s/%s, want drift/clear", pm.Outcome, pm.Action)
		}
		if ed.ProjectSFID != staffTestSFIDA || ed.V2ETag != "7" || ed.V1Revision != 40 || ed.MappingRevision != 1 {
			t.Errorf("ED versions = %+v, want sfid/etag/revisions recorded", ed)
		}
		if ed.Approved || ed.ClearApproved || pm.Approved || pm.ClearApproved {
			t.Error("report must not pre-approve anything")
		}
		if report.Mode != projectStaffReportModeReport || report.SchemaVersion != projectStaffReportSchemaVersion || report.GatewayOrigin == "" {
			t.Errorf("report header = %d/%s/%s", report.SchemaVersion, report.Mode, report.GatewayOrigin)
		}
		if report.Summary[projectStaffOutcomeDrift] != 2 || res.failed() {
			t.Errorf("summary = %v failed=%t", report.Summary, res.failed())
		}
		if calls := env.patchCalls(); len(calls) != 0 {
			t.Fatalf("report mode sent %d v1 requests, want 0", len(calls))
		}
		env.assertNoMappingWrites(t)
		env.assertNoCanaries(t, report)
	})

	t.Run("target fingerprints use a fresh salt per report", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, staffCanaryOldContact, "")
		first, _ := runReport(t, env, staffTestProjectA)
		second, _ := runReport(t, env, staffTestProjectA)
		a := entryFor(t, first, staffTestProjectA, projectStaffRoleExecutiveDirector)
		b := entryFor(t, second, staffTestProjectA, projectStaffRoleExecutiveDirector)
		if !validProjectStaffFingerprint(first.FingerprintSalt) || !validProjectStaffFingerprint(a.TargetFingerprint) {
			t.Fatal("report must contain a valid salt and drift target fingerprint")
		}
		if first.FingerprintSalt == second.FingerprintSalt || a.TargetFingerprint == b.TargetFingerprint {
			t.Fatal("separate reports must not have linkable target fingerprints")
		}
		env.assertNoCanaries(t, first)
		env.assertNoCanaries(t, second)
	})

	t.Run("a modified target fingerprint cannot authorize a write", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, staffCanaryOldContact, "")
		report, _ := runReport(t, env, staffTestProjectA)
		in := approve(t, report, []string{staffTestProjectA}, isRole(staffTestProjectA, projectStaffRoleExecutiveDirector), false)
		in.approvals[0].entries[0].TargetFingerprint = strings.Repeat("0", 64)
		applied, res, err := reconcileProjectStaff(t.Context(), in, env.store)
		if err != nil {
			t.Fatal(err)
		}
		if got := entryFor(t, applied, staffTestProjectA, projectStaffRoleExecutiveDirector); got.Outcome != projectStaffOutcomeStaleTarget {
			t.Errorf("outcome = %s, want stale_target", got.Outcome)
		}
		if len(env.patchCalls()) != 0 || !res.failed() {
			t.Fatal("a modified target fingerprint must fail without a PATCH")
		}
	})

	t.Run("report classifies skips and never turns them into clears", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		const (
			unmapped   = "33333333-3333-4333-8333-333333333333"
			tombstoned = "44444444-4444-4444-8444-444444444444"
			v2Missing  = "55555555-5555-4555-8555-555555555555"
			noV1       = "66666666-6666-4666-8666-666666666666"
		)
		manual := &projectservice.UserInfo{Name: strPtr(staffCanaryEDName), Email: strPtr(staffCanaryManualMail)}
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, manual, canaryPM(), staffCanaryOldContact, staffCanaryPMContact)
		if _, err := env.store.Put(t.Context(), "project.uid."+tombstoned, []byte(tombstoneMarker)); err != nil {
			t.Fatal(err)
		}
		env.seedProject(t, v2Missing, "a0941000002wSYNCCC", nil, nil, "", "")
		env.settingsErr[v2Missing] = wrapProjectServiceError("failed to fetch project settings", &projectservice.NotFoundError{Message: "not found"})
		env.seedProject(t, noV1, "a0941000002wSYNDDD", nil, nil, "", "")
		delete(env.v1, "a0941000002wSYNDDD")

		report, res := runReport(t, env, staffTestProjectA, unmapped, tombstoned, v2Missing, noV1)

		cases := []struct {
			uid, role, want string
		}{
			{staffTestProjectA, projectStaffRoleExecutiveDirector, projectStaffOutcomeUnresolvedContact},
			{staffTestProjectA, projectStaffRoleProgramManager, projectStaffOutcomeInSync},
			{unmapped, projectStaffRoleExecutiveDirector, projectStaffOutcomeNoProjectMapping},
			{tombstoned, projectStaffRoleProgramManager, projectStaffOutcomeNoProjectMapping},
			{v2Missing, projectStaffRoleExecutiveDirector, projectStaffOutcomeV2ProjectNotFound},
			{noV1, projectStaffRoleProgramManager, projectStaffOutcomeNoV1Record},
		}
		for _, tc := range cases {
			if got := entryFor(t, report, tc.uid, tc.role); got.Outcome != tc.want || got.Action != "" {
				t.Errorf("%s %s = %s/%q, want %s with no action", tc.uid, tc.role, got.Outcome, got.Action, tc.want)
			}
		}
		if res.failed() {
			t.Errorf("skips alone must not fail a report: %v", res.outcomes)
		}
		env.assertNoCanaries(t, report)
	})

	t.Run("report counts lookup failures as errors", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, "", "")
		env.lookupErr = errors.New("v1 DB unavailable")

		report, res := runReport(t, env, staffTestProjectA)
		if got := entryFor(t, report, staffTestProjectA, projectStaffRoleExecutiveDirector); got.Outcome != projectStaffOutcomeError || got.Reason != "contact_lookup_failed" {
			t.Errorf("ED = %s/%s, want error/contact_lookup_failed", got.Outcome, got.Reason)
		}
		if !res.failed() {
			t.Error("a report with read errors must fail")
		}
	})

	t.Run("apply patches only the approved role and converges", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, staffCanaryOldContact, staffCanaryPMContact)
		report, _ := runReport(t, env, staffTestProjectA)

		in := approve(t, report, []string{staffTestProjectA}, isRole(staffTestProjectA, projectStaffRoleExecutiveDirector), false)
		applied, res, err := reconcileProjectStaff(t.Context(), in, env.store)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		calls := env.patchCalls()
		if len(calls) != 1 {
			t.Fatalf("PATCH calls = %d, want 1", len(calls))
		}
		if calls[0].path != "/project-service/v1/projects/"+staffTestSFIDA {
			t.Errorf("PATCH path = %s", calls[0].path)
		}
		if got := calls[0].body["ExecutiveDirectorID"]; got != staffCanaryEDContact {
			t.Errorf("ExecutiveDirectorID = %q, want the resolved contact", got)
		}
		if _, present := calls[0].body["ProgramManagerID"]; present {
			t.Error("unapproved PM must not be sent")
		}
		if got := entryFor(t, applied, staffTestProjectA, projectStaffRoleExecutiveDirector); got.Outcome != projectStaffOutcomePatched {
			t.Errorf("ED outcome = %s, want patched", got.Outcome)
		}
		if len(applied.Entries) != 1 || applied.Mode != projectStaffReportModeApply {
			t.Errorf("apply report = %d entries mode %s, want only the approved entry", len(applied.Entries), applied.Mode)
		}
		if res.failed() {
			t.Errorf("apply failed: %v", res.outcomes)
		}

		// AC-5: a repeated report shows the approved role in sync and leaves
		// the unapproved PM drift for manual disposition.
		again, _ := runReport(t, env, staffTestProjectA)
		if got := entryFor(t, again, staffTestProjectA, projectStaffRoleExecutiveDirector); got.Outcome != projectStaffOutcomeInSync {
			t.Errorf("rerun ED = %s, want in_sync", got.Outcome)
		}
		if got := entryFor(t, again, staffTestProjectA, projectStaffRoleProgramManager); got.Outcome != projectStaffOutcomeDrift {
			t.Errorf("rerun PM = %s, want drift (never approved)", got.Outcome)
		}
		env.assertNoMappingWrites(t)
		env.assertNoCanaries(t, applied)
	})

	t.Run("clear needs clearApproved", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		env.seedProject(t, staffTestProjectA, staffTestSFIDA, nil, nil, "", staffCanaryPMContact)
		report, _ := runReport(t, env, staffTestProjectA)
		pick := isRole(staffTestProjectA, projectStaffRoleProgramManager)

		applied, res, err := reconcileProjectStaff(t.Context(), approve(t, report, []string{staffTestProjectA}, pick, false), env.store)
		if err != nil {
			t.Fatal(err)
		}
		if got := entryFor(t, applied, staffTestProjectA, projectStaffRoleProgramManager); got.Outcome != projectStaffOutcomeClearNotApproved {
			t.Errorf("PM = %s, want clear_not_approved", got.Outcome)
		}
		if len(env.patchCalls()) != 0 || res.failed() {
			t.Fatalf("clear without clearApproved: PATCH calls = %d failed=%t, want 0/false", len(env.patchCalls()), res.failed())
		}

		applied, _, err = reconcileProjectStaff(t.Context(), approve(t, report, []string{staffTestProjectA}, pick, true), env.store)
		if err != nil {
			t.Fatal(err)
		}
		calls := env.patchCalls()
		if len(calls) != 1 || calls[0].body["ProgramManagerID"] != v1ClearStaffValue {
			t.Fatalf("PATCH calls = %+v, want one clear", calls)
		}
		if got := entryFor(t, applied, staffTestProjectA, projectStaffRoleProgramManager); got.Outcome != projectStaffOutcomePatched {
			t.Errorf("PM = %s, want patched", got.Outcome)
		}
	})

	t.Run("stale observations are not overwritten", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(t *testing.T, env *staffTestEnv)
			want   string
		}{
			{"v2 etag changed", func(_ *testing.T, env *staffTestEnv) { env.etags[staffTestProjectA] = "8" }, projectStaffOutcomeStaleV2},
			{"v1 revision changed", func(_ *testing.T, env *staffTestEnv) { env.v1[staffTestSFIDA].revision++ }, projectStaffOutcomeStaleV1},
			{"mapping rewritten", func(t *testing.T, env *staffTestEnv) {
				if _, err := env.store.Put(t.Context(), "project.uid."+staffTestProjectA, []byte(staffTestSFIDA)); err != nil {
					t.Fatal(err)
				}
			}, projectStaffOutcomeStaleMapping},
			{"mapping removed", func(_ *testing.T, env *staffTestEnv) {
				delete(env.store.data, "project.uid."+staffTestProjectA)
			}, projectStaffOutcomeStaleMapping},
			{"v1 record deleted", func(_ *testing.T, env *staffTestEnv) { delete(env.v1, staffTestSFIDA) }, projectStaffOutcomeNoV1Record},
			{"v2 role changed under the same etag", func(_ *testing.T, env *staffTestEnv) {
				env.settings[staffTestProjectA].ExecutiveDirector = nil
			}, projectStaffOutcomeStaleAction},
			{"contact now matches v1", func(_ *testing.T, env *staffTestEnv) {
				env.contacts[staffCanaryEDUsername] = staffCanaryOldContact
			}, projectStaffOutcomeNowInSync},
			{"contact resolves to another contact", func(_ *testing.T, env *staffTestEnv) {
				env.contacts[staffCanaryEDUsername] = staffCanaryPMContact
			}, projectStaffOutcomeStaleTarget},
			{"contact no longer resolves", func(_ *testing.T, env *staffTestEnv) {
				delete(env.contacts, staffCanaryEDUsername)
				delete(env.contacts, staffCanaryEDEmail)
			}, projectStaffOutcomeUnresolvedContact},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				env := setupProjectStaffTest(t)
				env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, staffCanaryOldContact, "")
				report, _ := runReport(t, env, staffTestProjectA)
				in := approve(t, report, []string{staffTestProjectA}, isRole(staffTestProjectA, projectStaffRoleExecutiveDirector), false)
				env.mu.Lock()
				tc.mutate(t, env)
				env.mu.Unlock()

				applied, _, err := reconcileProjectStaff(t.Context(), in, env.store)
				if err != nil {
					t.Fatal(err)
				}
				if got := entryFor(t, applied, staffTestProjectA, projectStaffRoleExecutiveDirector); got.Outcome != tc.want {
					t.Errorf("ED = %s, want %s", got.Outcome, tc.want)
				}
				if n := len(env.patchCalls()); n != 0 {
					t.Errorf("PATCH calls = %d, want 0", n)
				}
			})
		}
	})

	t.Run("PATCH outcomes", func(t *testing.T) {
		cases := []struct {
			name        string
			status      int
			delay       time.Duration
			replicate   bool
			wantPatches int
			wantFirst   string
			wantReason  string
			wantSecond  string
			wantErr     bool
		}{
			{"4xx rejected, run continues", http.StatusUnprocessableEntity, 0, true, 2, projectStaffOutcomeRejected, "http_422", projectStaffOutcomeRejected, false},
			{"404 platform-native", http.StatusNotFound, 0, true, 2, projectStaffOutcomeV1ProjectNotFound, "", projectStaffOutcomeV1ProjectNotFound, false},
			{"5xx unconfirmed, run stops", http.StatusBadGateway, 0, true, 1, projectStaffOutcomeUnconfirmed, "http_502", projectStaffOutcomeNotAttempted, true},
			{"timeout unconfirmed, run stops", http.StatusOK, time.Second, true, 1, projectStaffOutcomeUnconfirmed, "timeout", projectStaffOutcomeNotAttempted, true},
			{"no replication is unconverged", http.StatusOK, 0, false, 2, projectStaffOutcomePatchedUnverified, "replication_pending", projectStaffOutcomePatchedUnverified, false},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				env := setupProjectStaffTest(t)
				projectStaffOperationTimeout = 200 * time.Millisecond
				env.seedProject(t, staffTestProjectA, staffTestSFIDA, canaryED(), nil, staffCanaryOldContact, "")
				env.seedProject(t, staffTestProjectB, staffTestSFIDB, canaryED(), nil, staffCanaryOldContact, "")
				uids := []string{staffTestProjectA, staffTestProjectB}
				report, _ := runReport(t, env, uids...)
				in := approve(t, report, uids, func(e projectStaffReportEntry) bool { return e.Role == projectStaffRoleExecutiveDirector }, false)
				env.mu.Lock()
				env.patchStatus, env.patchDelay, env.replicate = tc.status, tc.delay, tc.replicate
				env.mu.Unlock()

				applied, res, err := reconcileProjectStaff(t.Context(), in, env.store)
				if (err != nil) != tc.wantErr {
					t.Fatalf("err = %v, wantErr %t", err, tc.wantErr)
				}
				if n := len(env.patchCalls()); n != tc.wantPatches {
					t.Errorf("PATCH calls = %d, want %d (no retry)", n, tc.wantPatches)
				}
				first := entryFor(t, applied, staffTestProjectA, projectStaffRoleExecutiveDirector)
				if first.Outcome != tc.wantFirst || first.Reason != tc.wantReason {
					t.Errorf("first = %s/%s, want %s/%s", first.Outcome, first.Reason, tc.wantFirst, tc.wantReason)
				}
				if second := entryFor(t, applied, staffTestProjectB, projectStaffRoleExecutiveDirector); second.Outcome != tc.wantSecond {
					t.Errorf("second = %s, want %s", second.Outcome, tc.wantSecond)
				}
				if !res.failed() {
					t.Error("run must fail when an approval was not confirmed")
				}
				// The upstream body echoes the payload; it must not reach logs.
				env.assertNoCanaries(t, applied)
			})
		}
	})

	t.Run("writes are paced by emit rate", func(t *testing.T) {
		env := setupProjectStaffTest(t)
		const third = "77777777-7777-4777-8777-777777777777"
		uids := []string{staffTestProjectA, staffTestProjectB, third}
		for i, uid := range uids {
			env.seedProject(t, uid, fmt.Sprintf("a0941000002wPACE%02d", i), canaryED(), nil, "", "")
		}
		report, _ := runReport(t, env, uids...)
		in := approve(t, report, uids, func(e projectStaffReportEntry) bool { return e.Action != "" }, false)
		in.opts.emitRate = 20 // burst 1, then one write per 50ms

		start := time.Now()
		if _, _, err := reconcileProjectStaff(t.Context(), in, env.store); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
			t.Errorf("3 paced writes took %s, want >= ~100ms", elapsed)
		}
		if n := len(env.patchCalls()); n != 3 {
			t.Errorf("PATCH calls = %d, want 3", n)
		}
	})

	t.Run("approved plan validation", func(t *testing.T) {
		base := func() *projectStaffReport {
			return &projectStaffReport{
				SchemaVersion: projectStaffReportSchemaVersion, Mode: projectStaffReportModeReport, GatewayOrigin: "https://gw.example.invalid",
				FingerprintSalt: staffTestFingerprintSalt,
				Entries: []projectStaffReportEntry{{
					ProjectUID: staffTestProjectA, ProjectSFID: staffTestSFIDA, Role: projectStaffRoleExecutiveDirector,
					Action: projectStaffActionAssign, Outcome: projectStaffOutcomeDrift, V2ETag: "7", V1Revision: 40, MappingRevision: 1,
					TargetFingerprint: projectStaffTargetFingerprint(staffTestFingerprintSalt, staffTestProjectA, projectStaffRoleExecutiveDirector, staffCanaryEDContact),
					Approved:          true,
				}},
			}
		}
		cases := []struct {
			name    string
			edit    func(p *projectStaffReport)
			wantErr string
		}{
			{"valid", func(*projectStaffReport) {}, ""},
			{"unsupported role", func(p *projectStaffReport) { p.Entries[0].Role = "opportunity_owner" }, "unsupported role"},
			{"unsupported action", func(p *projectStaffReport) { p.Entries[0].Action = "replace" }, "unsupported action"},
			{"approved without action", func(p *projectStaffReport) { p.Entries[0].Action = "" }, "no action"},
			{"clearApproved on assign", func(p *projectStaffReport) { p.Entries[0].ClearApproved = true }, "clearApproved"},
			{"missing versions", func(p *projectStaffReport) { p.Entries[0].V1Revision = 0 }, "observed versions"},
			{"missing salt", func(p *projectStaffReport) { p.FingerprintSalt = "" }, "fingerprintSalt"},
			{"invalid salt", func(p *projectStaffReport) { p.FingerprintSalt = strings.Repeat("z", 64) }, "fingerprintSalt"},
			{"missing target fingerprint", func(p *projectStaffReport) { p.Entries[0].TargetFingerprint = "" }, "targetFingerprint"},
			{"invalid target fingerprint", func(p *projectStaffReport) { p.Entries[0].TargetFingerprint = strings.Repeat("z", 64) }, "targetFingerprint"},
			{"outside selection", func(p *projectStaffReport) { p.Entries[0].ProjectUID = staffTestProjectB }, "not in"},
			{"identical duplicate", func(p *projectStaffReport) { p.Entries = append(p.Entries, p.Entries[0]) }, ""},
			{"conflicting duplicate", func(p *projectStaffReport) {
				dup := p.Entries[0]
				dup.Approved = false
				p.Entries = append(p.Entries, dup)
			}, "conflicting duplicate"},
			{"versions disagree within a project", func(p *projectStaffReport) {
				pm := p.Entries[0]
				pm.Role, pm.V2ETag = projectStaffRoleProgramManager, "9"
				p.Entries = append(p.Entries, pm)
			}, "disagree"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				plan := base()
				tc.edit(plan)
				_, err := validateProjectStaffApprovals(plan, []string{staffTestProjectA})
				if tc.wantErr == "" && err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("approved plan must match the environment", func(t *testing.T) {
		in := &projectStaffReconcileInput{opts: projectStaffReconcileOptions{apply: true}, planGatewayOrigin: "https://api-gw.dev.example.invalid"}
		other, _ := url.Parse("https://api-gw.prod.example.invalid/")
		if err := in.checkGateway(other); err == nil {
			t.Fatal("apply against another gateway must be rejected")
		}
		same, _ := url.Parse("https://API-GW.dev.example.invalid/")
		if err := in.checkGateway(same); err != nil {
			t.Fatalf("same gateway rejected: %v", err)
		}
		if err := (&projectStaffReconcileInput{}).checkGateway(nil); err == nil {
			t.Fatal("missing gateway must be rejected")
		}
	})

	t.Run("mapping reader follows the store mode without DDL", func(t *testing.T) {
		origOpen, origCheck := projectStaffOpenPGPool, projectStaffCheckPGTable
		t.Cleanup(func() { projectStaffOpenPGPool, projectStaffCheckPGTable = origOpen, origCheck })
		var opened, checked int
		tableErr := error(nil)
		projectStaffOpenPGPool = func(context.Context, *Config) (*pgxpool.Pool, error) { opened++; return nil, nil }
		projectStaffCheckPGTable = func(context.Context, *pgxpool.Pool) error { checked++; return tableErr }

		kv := newFakeKV()
		for _, mode := range []V1MappingsStoreMode{V1MappingsStoreModeKV, V1MappingsStoreModeDual} {
			reader, closeFn, err := openProjectStaffMappingReader(t.Context(), &Config{V1MappingsStoreMode: mode}, kv)
			if err != nil {
				t.Fatalf("%s: %v", mode, err)
			}
			if _, ok := reader.(*kvMappingStore); !ok {
				t.Errorf("%s reader = %T, want the KV adapter (KV is authoritative)", mode, reader)
			}
			closeFn()
		}
		if opened != 0 {
			t.Fatalf("kv/dual opened Postgres %d times, want 0", opened)
		}

		reader, closeFn, err := openProjectStaffMappingReader(t.Context(), &Config{V1MappingsStoreMode: V1MappingsStoreModePostgres}, kv)
		if err != nil {
			t.Fatal(err)
		}
		closeFn()
		if _, ok := reader.(*pgMappingStore); !ok || opened != 1 || checked != 1 {
			t.Errorf("postgres reader = %T opened=%d checked=%d, want pg store via the no-DDL pool and a table check", reader, opened, checked)
		}

		tableErr = errors.New("v1-mappings table v1_mappings does not exist")
		if _, _, err := openProjectStaffMappingReader(t.Context(), &Config{V1MappingsStoreMode: V1MappingsStoreModePostgres}, kv); err == nil {
			t.Error("a missing table must fail instead of being created")
		}
		if _, _, err := openProjectStaffMappingReader(t.Context(), &Config{V1MappingsStoreMode: "bogus"}, kv); err == nil {
			t.Error("an invalid mode must fail")
		}
		if len(kv.data) != 0 {
			t.Error("opening a reader must not write to the bucket")
		}
	})

	t.Run("report file is new and private", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "report.json")
		report := &projectStaffReport{SchemaVersion: projectStaffReportSchemaVersion, Mode: projectStaffReportModeReport, GatewayOrigin: "https://gw.example.invalid", FingerprintSalt: staffTestFingerprintSalt}
		if err := writeProjectStaffReport(path, report); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("report mode = %o, want no group/other access", perm)
		}
		if err := writeProjectStaffReport(path, report); err == nil {
			t.Error("an existing report must not be overwritten")
		}
		// A written report round-trips as an approval input.
		if _, err := readProjectStaffReport(path); err != nil {
			t.Errorf("report does not read back: %v", err)
		}
	})
}

func TestWaitProjectStaffConvergence(t *testing.T) {
	setup := func(t *testing.T, timeout, readLimit time.Duration) {
		t.Helper()
		origRead := readV1ProjectStaffSnapshot
		origTimeout, origReadLimit, origInterval := projectStaffConvergenceTimeout, projectStaffConvergenceReadLimit, projectStaffConvergenceInterval
		t.Cleanup(func() {
			readV1ProjectStaffSnapshot = origRead
			projectStaffConvergenceTimeout, projectStaffConvergenceReadLimit, projectStaffConvergenceInterval = origTimeout, origReadLimit, origInterval
		})
		projectStaffConvergenceTimeout = timeout
		projectStaffConvergenceReadLimit = readLimit
		projectStaffConvergenceInterval = 10 * time.Millisecond
	}

	t.Run("stalled read respects overall deadline", func(t *testing.T) {
		setup(t, 30*time.Millisecond, 300*time.Millisecond)
		start := time.Now()
		readV1ProjectStaffSnapshot = func(ctx context.Context, _ string) (*v1ProjectStaffSnapshot, error) {
			deadline, ok := ctx.Deadline()
			if !ok || deadline.After(start.Add(projectStaffConvergenceTimeout+20*time.Millisecond)) {
				t.Errorf("read deadline = %s, want the overall convergence deadline", deadline)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if waitProjectStaffConvergence(t.Context(), staffTestSFIDA, map[string]string{"executive_director__c": staffCanaryEDContact}) {
			t.Fatal("a stalled read must not report convergence")
		}
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Errorf("convergence took %s, want the 30ms overall budget, not the 300ms read budget", elapsed)
		}
	})

	t.Run("late poll inherits the same deadline", func(t *testing.T) {
		setup(t, 80*time.Millisecond, 400*time.Millisecond)
		var firstDeadline time.Time
		calls := 0
		readV1ProjectStaffSnapshot = func(ctx context.Context, _ string) (*v1ProjectStaffSnapshot, error) {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("convergence read has no deadline")
			}
			if calls == 1 {
				firstDeadline = deadline
				return &v1ProjectStaffSnapshot{staff: map[string]string{}}, nil
			}
			if !deadline.Equal(firstDeadline) {
				t.Errorf("late read deadline = %s, want original %s", deadline, firstDeadline)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if waitProjectStaffConvergence(t.Context(), staffTestSFIDA, map[string]string{"executive_director__c": staffCanaryEDContact}) {
			t.Fatal("unreplicated fields must not report convergence")
		}
		if calls != 2 {
			t.Errorf("read calls = %d, want an initial poll and one stalled late poll", calls)
		}
	})
}

func TestProjectStaffReconcileOptions(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	selection := write("selection.txt", "# cohort\n"+strings.ToUpper(staffTestProjectA)+"\n\n"+staffTestProjectA+"\n  "+staffTestProjectB+"  \n")
	existing := write("existing.json", "{}")
	var many strings.Builder
	for i := 0; i <= projectStaffMaxProjects; i++ {
		fmt.Fprintf(&many, "%08x-0000-4000-8000-000000000000\n", i)
	}
	plan := func(name string, mutate func(*projectStaffReport)) string {
		p := &projectStaffReport{
			SchemaVersion: projectStaffReportSchemaVersion, Mode: projectStaffReportModeReport, GatewayOrigin: "https://gw.example.invalid",
			FingerprintSalt: staffTestFingerprintSalt,
			Entries: []projectStaffReportEntry{{
				ProjectUID: staffTestProjectA, ProjectSFID: staffTestSFIDA, Role: projectStaffRoleProgramManager,
				Action: projectStaffActionClear, Outcome: projectStaffOutcomeDrift, V2ETag: "7", V1Revision: 40, MappingRevision: 1,
				TargetFingerprint: projectStaffTargetFingerprint(staffTestFingerprintSalt, staffTestProjectA, projectStaffRoleProgramManager, v1ClearStaffValue),
				Approved:          true, ClearApproved: true,
			}},
		}
		if mutate != nil {
			mutate(p)
		}
		data, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		return write(name, string(data))
	}
	goodPlan := plan("plan.json", nil)
	newReport := func() string { return filepath.Join(dir, fmt.Sprintf("out-%d.json", time.Now().UnixNano())) }

	cases := []struct {
		name          string
		opts          projectStaffReconcileOptions
		wantErr       string
		wantProjects  int
		wantApprovals int
	}{
		{"report-only", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), emitRate: 2}, "", 2, 0},
		{"report-only with shared dry-run", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), dryRun: true, emitRate: 2}, "", 2, 0},
		{"apply", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: goodPlan, emitRate: 2}, "", 2, 1},
		{"apply with results report", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: goodPlan, reportPath: newReport(), emitRate: 2}, "", 2, 1},
		{"report flag without selection", projectStaffReconcileOptions{reportPath: newReport(), emitRate: 2}, "require --reconcile-project-staff-file", 0, 0},
		{"apply flag without selection", projectStaffReconcileOptions{apply: true, emitRate: 2}, "require --reconcile-project-staff-file", 0, 0},
		{"report-only without report file", projectStaffReconcileOptions{selectionPath: selection, emitRate: 2}, "--project-staff-report is required", 0, 0},
		{"report-only with approved plan", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), approvedPlanPath: goodPlan, emitRate: 2}, "only used with --apply-project-staff", 0, 0},
		{"apply without plan", projectStaffReconcileOptions{selectionPath: selection, apply: true, emitRate: 2}, "requires --project-staff-approved-plan", 0, 0},
		{"apply with dry-run", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: goodPlan, dryRun: true, emitRate: 2}, "contradicts", 0, 0},
		{"zero emit rate", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), emitRate: 0}, "--emit-rate", 0, 0},
		{"NaN emit rate", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), emitRate: math.NaN()}, "--emit-rate", 0, 0},
		{"positive infinite emit rate", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), emitRate: math.Inf(1)}, "--emit-rate", 0, 0},
		{"negative infinite emit rate", projectStaffReconcileOptions{selectionPath: selection, reportPath: newReport(), emitRate: math.Inf(-1)}, "--emit-rate", 0, 0},
		{"report path is the plan", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: goodPlan, reportPath: goodPlan, emitRate: 2}, "must not be the approved plan", 0, 0},
		{"report path exists", projectStaffReconcileOptions{selectionPath: selection, reportPath: existing, emitRate: 2}, "already exists", 0, 0},
		{"missing selection file", projectStaffReconcileOptions{selectionPath: filepath.Join(dir, "absent.txt"), reportPath: newReport(), emitRate: 2}, "no such file", 0, 0},
		{"empty selection", projectStaffReconcileOptions{selectionPath: write("empty.txt", "\n# only comments\n"), reportPath: newReport(), emitRate: 2}, "no project UIDs", 0, 0},
		{"invalid line", projectStaffReconcileOptions{selectionPath: write("bad.txt", staffTestProjectA+"\n"+staffCanaryEDEmail+"\n"), reportPath: newReport(), emitRate: 2}, "line 2 is not a project UID", 0, 0},
		{"non-canonical UID", projectStaffReconcileOptions{selectionPath: write("urn.txt", "urn:uuid:"+staffTestProjectA+"\n"), reportPath: newReport(), emitRate: 2}, "not a project UID", 0, 0},
		{"not UTF-8", projectStaffReconcileOptions{selectionPath: write("latin1.txt", "\xff\xfe\n"), reportPath: newReport(), emitRate: 2}, "UTF-8", 0, 0},
		{"too many projects", projectStaffReconcileOptions{selectionPath: write("many.txt", many.String()), reportPath: newReport(), emitRate: 2}, "more than 1000", 0, 0},
		{"plan with old schema", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: plan("v1.json", func(p *projectStaffReport) { p.SchemaVersion = 1 }), emitRate: 2}, "unsupported schemaVersion", 0, 0},
		{"plan is an apply result", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: plan("applied.json", func(p *projectStaffReport) { p.Mode = projectStaffReportModeApply }), emitRate: 2}, "not a report-only run", 0, 0},
		{"plan without environment", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: plan("noenv.json", func(p *projectStaffReport) { p.GatewayOrigin = "" }), emitRate: 2}, "gatewayOrigin", 0, 0},
		{"plan with unknown field", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: write("extra.json", `{"schemaVersion":1,"mode":"report","gatewayOrigin":"https://gw.example.invalid","entries":[{"projectUid":"`+staffTestProjectA+`","role":"program_manager","outcome":"drift","email":"`+staffCanaryEDEmail+`"}]}`), emitRate: 2}, "unknown field", 0, 0},
		{"plan with unsupported role", projectStaffReconcileOptions{selectionPath: selection, apply: true, approvedPlanPath: plan("role.json", func(p *projectStaffReport) { p.Entries[0].Role = "opportunity_owner" }), emitRate: 2}, "unsupported role", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Validation runs before configuration and clients exist.
			origCfg := cfg
			cfg = nil
			t.Cleanup(func() { cfg = origCfg })

			in, err := prepareProjectStaffReconcile(tc.opts)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), staffCanaryEDEmail) {
					t.Errorf("error echoes input content: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(in.projectUIDs) != tc.wantProjects || len(in.approvals) != tc.wantApprovals {
				t.Errorf("projects=%d approvals=%d, want %d/%d", len(in.projectUIDs), len(in.approvals), tc.wantProjects, tc.wantApprovals)
			}
			if in.projectUIDs[0] != staffTestProjectA {
				t.Errorf("first UID = %s, want normalized %s", in.projectUIDs[0], staffTestProjectA)
			}
			if _, statErr := os.Stat(tc.opts.reportPath); tc.opts.reportPath != "" && tc.opts.reportPath != existing && statErr == nil {
				t.Error("validation must not create the report file")
			}
		})
	}

	if (projectStaffReconcileOptions{dryRun: true, emitRate: 2}).requested() {
		t.Error("the shared --dry-run and --emit-rate flags alone must not select this command")
	}
}
