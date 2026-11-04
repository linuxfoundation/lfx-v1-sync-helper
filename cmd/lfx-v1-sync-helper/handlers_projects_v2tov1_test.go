// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
	nats "github.com/nats-io/nats.go"
)

const (
	bridgeTestProjectUID  = "037853e2-9835-410f-a369-8cf4528b8f47"
	bridgeTestProjectSFID = "a0941000002wBycAAE"
	bridgeTestPMSFID      = "0032M00003EhB73QAF"
)

// staffBridgeSpy records PATCH requests received by the httptest server
// standing in for the v1 API Gateway.
type staffBridgeSpy struct {
	patchCalls  int
	lastMethod  string
	lastPath    string
	lastBody    map[string]string
	patchStatus int
}

// setupStaffBridgeTest initialises the package-level globals and
// function-variable seams that handleProjectSettingsUpdated depends on, and
// registers cleanup restoring the originals (pattern: setupMembersTestGlobals
// in client_members_test.go). Defaults: project mapped, v1 staff empty, user
// resolution misses. Tests override the seams as needed.
func setupStaffBridgeTest(t *testing.T, patchStatus int) *staffBridgeSpy {
	t.Helper()

	spy := &staffBridgeSpy{patchStatus: patchStatus}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy.patchCalls++
		spy.lastMethod = r.Method
		spy.lastPath = r.URL.Path
		spy.lastBody = map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&spy.lastBody)
		w.WriteHeader(spy.patchStatus)
	}))
	t.Cleanup(srv.Close)

	origCfg := cfg
	origLogger := logger
	origV1HTTPClient := v1HTTPClient
	origGetSFID := getV1ProjectSFIDByUID
	origGetStaff := getV1ProjectStaffSFIDs
	origByUsername := resolveV1UserSFIDByUsername
	origByEmail := resolveV1UserSFIDByEmail
	origWrites := recentV1StaffWrites
	origNow := staffWriteNow

	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	gwURL, err := url.Parse(srv.URL + "/")
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}
	cfg = &Config{
		HeimdallClientID: "test-heimdall-client",
		LFXAPIGateway:    gwURL,
	}
	v1HTTPClient = srv.Client()

	getV1ProjectSFIDByUID = func(context.Context, string) (string, error) { return bridgeTestProjectSFID, nil }
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{}, nil
	}
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) { return "", nil }
	resolveV1UserSFIDByEmail = func(context.Context, string) (string, error) { return "", nil }
	recentV1StaffWrites = newV1StaffWriteLog()

	t.Cleanup(func() {
		cfg = origCfg
		logger = origLogger
		v1HTTPClient = origV1HTTPClient
		getV1ProjectSFIDByUID = origGetSFID
		getV1ProjectStaffSFIDs = origGetStaff
		resolveV1UserSFIDByUsername = origByUsername
		resolveV1UserSFIDByEmail = origByEmail
		recentV1StaffWrites = origWrites
		staffWriteNow = origNow
	})

	return spy
}

// settingsUpdatedMsg marshals a ProjectSettingsUpdatedMessage carrying the
// given old/new staff snapshots into a NATS message.
func settingsUpdatedMsg(t *testing.T, actor string, oldSettings, newSettings events.ProjectSettings) *nats.Msg {
	t.Helper()
	msg := events.ProjectSettingsUpdatedMessage{
		ProjectUID:  bridgeTestProjectUID,
		OldSettings: oldSettings,
		NewSettings: newSettings,
		Actor:       events.Actor{Username: actor},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	return &nats.Msg{Data: data}
}

func TestHandleProjectSettingsUpdated_StaffChangeWritesV1(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		if username == "staffpm" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm", Email: "staffpm@example.com"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
	if spy.lastMethod != http.MethodPatch {
		t.Errorf("method = %s, want PATCH", spy.lastMethod)
	}
	if want := "/project-service/v1/projects/" + bridgeTestProjectSFID; spy.lastPath != want {
		t.Errorf("path = %s, want %s", spy.lastPath, want)
	}
	if got := spy.lastBody["ProgramManagerID"]; got != bridgeTestPMSFID {
		t.Errorf("ProgramManagerID = %q, want %q", got, bridgeTestPMSFID)
	}
	// The unchanged ED must be omitted entirely: the v1 PATCH is partial, and
	// re-sending it from the (possibly stale) v1-objects replica could revert a
	// newer v1 assignment.
	if got, present := spy.lastBody["ExecutiveDirectorID"]; present {
		t.Errorf("ExecutiveDirectorID = %q in payload, want omitted (unchanged field)", got)
	}
}

func TestHandleProjectSettingsUpdated_NoStaffChangeSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)

	pm := &events.UserInfo{Username: "staffpm", Email: "staffpm@example.com"}
	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ProgramManager: pm, MissionStatement: "old"},
		events.ProjectSettings{ProgramManager: pm, MissionStatement: "new"})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (mission-only change must not sync staff)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_OpportunityOwnerOnlyIgnored(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)

	// AC-2: opportunity_owner diffs are logged and ignored — SFDC-owned,
	// one-way v1->v2.
	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{OpportunityOwner: &events.UserInfo{Username: "staffowner"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (opportunity_owner must never sync v2->v1)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_OriginSkip(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)

	// AC-3: events from our own v1->v2 sync carry the Heimdall machine
	// principal and are skipped.
	msg := settingsUpdatedMsg(t, "test-heimdall-client@clients",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (own-sync event must be skipped)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_OtherMachineActorStillSyncs(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) { return bridgeTestPMSFID, nil }

	// Only our own client ID is skipped; a different M2M actor (e.g. a manual
	// API correction) must still sync.
	msg := settingsUpdatedMsg(t, "some-other-client@clients",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_UnmappedProjectSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	getV1ProjectSFIDByUID = func(context.Context, string) (string, error) { return "", nil }

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (unmapped project must be skipped)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_NoDiffVsV1Skips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) { return bridgeTestPMSFID, nil }
	// AC-4: v1 already holds the resolved value (PCC-originated edit).
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"program_manager__c": bridgeTestPMSFID}, nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (no staff diff vs v1)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_ClearWritesNone(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"program_manager__c": bridgeTestPMSFID}, nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}},
		events.ProjectSettings{})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
	if got := spy.lastBody["ProgramManagerID"]; got != v1ClearStaffValue {
		t.Errorf("ProgramManagerID = %q, want %q (clear literal)", got, v1ClearStaffValue)
	}
}

// A user assigned in v2 with no v1 contact — e.g. an LFXSS manual staff entry,
// which carries name + email and an empty username (updateProjectStaff in
// lfx-self-serve apps/lfx-one/src/server/services/project.service.ts) — must
// leave the existing v1 assignment alone rather than clearing it.
func TestHandleProjectSettingsUpdated_UnresolvableUserLeavesV1Unchanged(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"program_manager__c": bridgeTestPMSFID}, nil
	}
	// Default seams resolve nothing for this user.

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Name: "Ghost User", Email: "ghost@example.com"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (unresolvable assignee must not clear the v1 field)", spy.patchCalls)
	}
}

// An unresolvable field must not block the sibling field that did resolve.
func TestHandleProjectSettingsUpdated_UnresolvableFieldDoesNotBlockSibling(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		if username == "staffpm" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{
			ExecutiveDirector: &events.UserInfo{Name: "Ghost User", Email: "ghost@example.com"},
			ProgramManager:    &events.UserInfo{Username: "staffpm"},
		})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
	if got := spy.lastBody["ProgramManagerID"]; got != bridgeTestPMSFID {
		t.Errorf("ProgramManagerID = %q, want %q", got, bridgeTestPMSFID)
	}
	if got, present := spy.lastBody["ExecutiveDirectorID"]; present {
		t.Errorf("ExecutiveDirectorID = %q in payload, want omitted (unresolvable)", got)
	}
}

func TestHandleProjectSettingsUpdated_EmailFallback(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	usernameCalled := false
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) {
		usernameCalled = true
		return "", nil
	}
	resolveV1UserSFIDByEmail = func(_ context.Context, email string) (string, error) {
		if email == "staffemail@example.com" {
			return "0032M00002cQF7WQAW", nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Email: "staffemail@example.com"}})
	handleProjectSettingsUpdated(msg)

	if usernameCalled {
		t.Error("username resolver called for empty username; want email fallback only")
	}
	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
	if got := spy.lastBody["ProgramManagerID"]; got != "0032M00002cQF7WQAW" {
		t.Errorf("ProgramManagerID = %q, want %q", got, "0032M00002cQF7WQAW")
	}
}

func TestHandleProjectSettingsUpdated_ResolutionErrorAborts(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) {
		return "", errors.New("v1 DB unavailable")
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (transient resolution failure must not write)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_MissingV1RecordSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) { return bridgeTestPMSFID, nil }
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) { return nil, nil }

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (missing v1 record must be skipped)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_Patch404IsTerminalSkip(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusNotFound)
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) { return bridgeTestPMSFID, nil }

	// Platform-native (lf...) projects 404 on the v1 API; the handler warns and
	// moves on without retrying.
	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want exactly 1 (no retry on 404)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_EDChangeOmitsUnchangedPM(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		if username == "staffed" {
			return "0034100001oQRLDAA4", nil
		}
		return "", nil
	}
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"program_manager__c": bridgeTestPMSFID}, nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "staffpm"}},
		events.ProjectSettings{
			ProgramManager:    &events.UserInfo{Username: "staffpm"},
			ExecutiveDirector: &events.UserInfo{Username: "staffed"},
		})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
	if got := spy.lastBody["ExecutiveDirectorID"]; got != "0034100001oQRLDAA4" {
		t.Errorf("ExecutiveDirectorID = %q, want %q", got, "0034100001oQRLDAA4")
	}
	// The unchanged PM is left out of the partial PATCH entirely.
	if got, present := spy.lastBody["ProgramManagerID"]; present {
		t.Errorf("ProgramManagerID = %q in payload, want omitted (unchanged field)", got)
	}
}

// Regression (GH-1802): the LFXSS staff dialog saves one role at a time, so a
// second edit can arrive while v1-objects still shows the pre-first-edit value
// for the other role. That stale value must never be echoed back to v1.
func TestHandleProjectSettingsUpdated_StaleReplicaDoesNotRevertSiblingField(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		if username == "staffpm" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}
	// v1 already carries the new ED from the first edit, but the KV replica
	// still holds the previous one because the WAL pipeline has not caught up.
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"executive_director__c": "0034100001staleED"}, nil
	}

	ed := &events.UserInfo{Username: "staffed"}
	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: ed},
		events.ProjectSettings{ExecutiveDirector: ed, ProgramManager: &events.UserInfo{Username: "staffpm"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
	if got := spy.lastBody["ProgramManagerID"]; got != bridgeTestPMSFID {
		t.Errorf("ProgramManagerID = %q, want %q", got, bridgeTestPMSFID)
	}
	if got, present := spy.lastBody["ExecutiveDirectorID"]; present {
		t.Errorf("ExecutiveDirectorID = %q sent from the stale replica, want omitted", got)
	}
}

// staleStaffReplica stubs a v1-objects ED value that tests change between
// events to model WAL replication lag, and resolves usernames from a fixed
// table of synthetic contacts.
func staleStaffReplica(t *testing.T, replicatedED *string) {
	t.Helper()
	contacts := map[string]string{"stafforiginal": "0034100001origED", "staffnew": "0034100001newED", "staffpcc": "0034100001pccED"}
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		return contacts[username], nil
	}
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"executive_director__c": *replicatedED}, nil
	}
}

// Regression (lfx-self-serve#5034): assign a new ED, then restore the original
// before v1-objects replicates the first PATCH. The stale replica still shows
// the original, but v1 holds the new ED, so the restore must be sent.
func TestHandleProjectSettingsUpdated_RevertBeforeReplicationPatches(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	replicatedED := "0034100001origED"
	staleStaffReplica(t, &replicatedED)

	original := &events.UserInfo{Username: "stafforiginal"}
	assigned := &events.UserInfo{Username: "staffnew"}
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: original},
		events.ProjectSettings{ExecutiveDirector: assigned}))
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: assigned},
		events.ProjectSettings{ExecutiveDirector: original}))

	if spy.patchCalls != 2 {
		t.Fatalf("PATCH calls = %d, want 2 (assign, then restore)", spy.patchCalls)
	}
	if got := spy.lastBody["ExecutiveDirectorID"]; got != "0034100001origED" {
		t.Errorf("ExecutiveDirectorID = %q, want the restored original", got)
	}
}

// Regression (lfx-self-serve#5034): assign an ED to a project that had none,
// then clear it before v1-objects replicates. The stale replica is still
// empty, but v1 holds the assignment, so the clear must be sent.
func TestHandleProjectSettingsUpdated_ClearBeforeReplicationPatches(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	replicatedED := ""
	staleStaffReplica(t, &replicatedED)

	assigned := &events.UserInfo{Username: "staffnew"}
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{ExecutiveDirector: assigned}))
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: assigned},
		events.ProjectSettings{}))

	if spy.patchCalls != 2 {
		t.Fatalf("PATCH calls = %d, want 2 (assign, then clear)", spy.patchCalls)
	}
	if got := spy.lastBody["ExecutiveDirectorID"]; got != v1ClearStaffValue {
		t.Errorf("ExecutiveDirectorID = %q, want %q", got, v1ClearStaffValue)
	}
}

// Once v1-objects shows the bridge's write, the replica is authoritative
// again: a later PCC edit echoed through v1->v2 must still no-op.
func TestHandleProjectSettingsUpdated_ReplicatedWriteNoLongerShadowsV1(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	replicatedED := "0034100001origED"
	staleStaffReplica(t, &replicatedED)

	assigned := &events.UserInfo{Username: "staffnew"}
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: &events.UserInfo{Username: "stafforiginal"}},
		events.ProjectSettings{ExecutiveDirector: assigned}))

	// The write replicates, then is superseded by a PCC edit whose v1->v2 echo
	// arrives after v1-objects carries it.
	replicatedED = "0034100001newED"
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "v1-user",
		events.ProjectSettings{},
		events.ProjectSettings{ExecutiveDirector: assigned}))
	replicatedED = "0034100001pccED"
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "v1-user",
		events.ProjectSettings{ExecutiveDirector: assigned},
		events.ProjectSettings{ExecutiveDirector: &events.UserInfo{Username: "staffpcc"}}))

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1 (the v2 assignment only; echoes no-op)", spy.patchCalls)
	}
}

// A write that never shows up in v1-objects stops shadowing it after
// v1StaffWriteFreshness, so the replica decides again.
func TestHandleProjectSettingsUpdated_ExpiredWriteFallsBackToReplica(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	replicatedED := "0034100001origED"
	staleStaffReplica(t, &replicatedED)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	staffWriteNow = func() time.Time { return now }

	original := &events.UserInfo{Username: "stafforiginal"}
	assigned := &events.UserInfo{Username: "staffnew"}
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: original},
		events.ProjectSettings{ExecutiveDirector: assigned}))
	now = now.Add(v1StaffWriteFreshness + time.Second)
	handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{ExecutiveDirector: assigned},
		events.ProjectSettings{ExecutiveDirector: original}))

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1 (expired write no longer overrides the replica)", spy.patchCalls)
	}
}

// A 5xx may have reached v1, so the attempted value is remembered and a revert
// is still sent; a 4xx was rejected, so the replica decides.
func TestHandleProjectSettingsUpdated_FailedWriteOutcome(t *testing.T) {
	tests := []struct {
		name        string
		firstStatus int
		wantCalls   int
	}{
		{"5xx outcome unknown, revert sent", http.StatusBadGateway, 2},
		{"4xx rejected, revert matches replica", http.StatusBadRequest, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spy := setupStaffBridgeTest(t, tc.firstStatus)
			replicatedED := "0034100001origED"
			staleStaffReplica(t, &replicatedED)

			original := &events.UserInfo{Username: "stafforiginal"}
			assigned := &events.UserInfo{Username: "staffnew"}
			handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
				events.ProjectSettings{ExecutiveDirector: original},
				events.ProjectSettings{ExecutiveDirector: assigned}))
			spy.patchStatus = http.StatusOK
			handleProjectSettingsUpdated(settingsUpdatedMsg(t, "test-actor",
				events.ProjectSettings{ExecutiveDirector: assigned},
				events.ProjectSettings{ExecutiveDirector: original}))

			if spy.patchCalls != tc.wantCalls {
				t.Fatalf("PATCH calls = %d, want %d", spy.patchCalls, tc.wantCalls)
			}
		})
	}
}

func TestHandleProjectSettingsUpdated_InvalidEventSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	handleProjectSettingsUpdated(&nats.Msg{Data: []byte("not-json")})
	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0", spy.patchCalls)
	}
}

// The reconciliation reads the same v1 record as the bridge, plus its
// v1-objects revision; the bridge's field reader must return the same staff.
func TestHandleProjectSettingsUpdated_VersionedSnapshotMatchesFieldReader(t *testing.T) {
	origKV := v1KV
	kv := newFakeKV()
	v1KV = kv
	t.Cleanup(func() { v1KV = origKV })

	key := "salesforce-project__c." + bridgeTestProjectSFID
	kv.data[key] = []byte(`{"sfid":"` + bridgeTestProjectSFID + `","executive_director__c":" 0034100001oQRLDAA4 ","program_manager__c":"` + bridgeTestPMSFID + `"}`)
	kv.rev[key] = 12

	snapshot, err := readV1ProjectStaffSnapshot(t.Context(), bridgeTestProjectSFID)
	if err != nil || snapshot == nil {
		t.Fatalf("readV1ProjectStaffSnapshot = %v, %v", snapshot, err)
	}
	if snapshot.revision != 12 {
		t.Errorf("revision = %d, want 12", snapshot.revision)
	}
	if got := snapshot.staff["executive_director__c"]; got != "0034100001oQRLDAA4" {
		t.Errorf("executive_director__c = %q, want trimmed value", got)
	}
	staff, err := getV1ProjectStaffSFIDs(t.Context(), bridgeTestProjectSFID)
	if err != nil || staff["program_manager__c"] != bridgeTestPMSFID || staff["executive_director__c"] != "0034100001oQRLDAA4" {
		t.Errorf("getV1ProjectStaffSFIDs = %v, %v; want the snapshot's staff", staff, err)
	}

	kv.data[key] = []byte(`{"sfid":"` + bridgeTestProjectSFID + `","isdeleted":true}`)
	if snapshot, err := readV1ProjectStaffSnapshot(t.Context(), bridgeTestProjectSFID); err != nil || snapshot != nil {
		t.Errorf("soft-deleted record = %v, %v; want nil, nil", snapshot, err)
	}
	if staff, err := getV1ProjectStaffSFIDs(t.Context(), "a0941000002wABSENT"); err != nil || staff != nil {
		t.Errorf("absent record = %v, %v; want nil, nil", staff, err)
	}
}

// Bridge logs name the staff field, never the person or the contact SFID.
func TestHandleProjectSettingsUpdated_LogsOmitStaffIdentity(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	var logs bytes.Buffer
	logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		if username == "canary-pm-username" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "test-actor",
		events.ProjectSettings{},
		events.ProjectSettings{
			ExecutiveDirector: &events.UserInfo{Name: "Canary Manual", Email: "canary-manual@example.invalid"},
			ProgramManager:    &events.UserInfo{Username: "canary-pm-username", Email: "canary-pm@example.invalid"},
		})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 || spy.lastBody["ProgramManagerID"] != bridgeTestPMSFID {
		t.Fatalf("PATCH calls = %d body = %v; want the resolved PM only", spy.patchCalls, spy.lastBody)
	}
	out := logs.String()
	for _, value := range []string{"canary-manual@example.invalid", "Canary Manual", "canary-pm-username", "canary-pm@example.invalid", bridgeTestPMSFID} {
		if strings.Contains(out, value) {
			t.Errorf("logs contain staff identity value %q", value)
		}
	}
	if !strings.Contains(out, "ProgramManagerID") || !strings.Contains(out, `"field":"executive_director"`) {
		t.Errorf("logs should still name the fields: %s", out)
	}
}

func TestStaffUserInfosEqual(t *testing.T) {
	staffpm := &events.UserInfo{Username: "staffpm", Email: "staffpm@example.com"}
	tests := []struct {
		name string
		a, b *events.UserInfo
		want bool
	}{
		{"both nil", nil, nil, true},
		{"nil vs set", nil, staffpm, false},
		{"set vs nil", staffpm, nil, false},
		{"identical", staffpm, &events.UserInfo{Username: "staffpm", Email: "staffpm@example.com"}, true},
		{"name/avatar drift only", staffpm, &events.UserInfo{Username: "staffpm", Email: "staffpm@example.com", Name: "Staff PM", Avatar: "x"}, true},
		{"username differs", staffpm, &events.UserInfo{Username: "staffpm2", Email: "staffpm@example.com"}, false},
		{"email differs", staffpm, &events.UserInfo{Username: "staffpm", Email: "other@example.com"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := staffUserInfosEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("staffUserInfosEqual() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestStaffValueOrNone(t *testing.T) {
	if got := staffValueOrNone(""); got != v1ClearStaffValue {
		t.Errorf("staffValueOrNone(\"\") = %q, want %q", got, v1ClearStaffValue)
	}
	if got := staffValueOrNone(bridgeTestPMSFID); got != bridgeTestPMSFID {
		t.Errorf("staffValueOrNone(%q) = %q, want unchanged", bridgeTestPMSFID, got)
	}
}

func TestPatchV1ProjectStaff(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantErr    bool
		wantNotFnd bool
	}{
		{"200 OK", http.StatusOK, false, false},
		{"204 No Content", http.StatusNoContent, false, false},
		{"404 maps to sentinel", http.StatusNotFound, true, true},
		{"500 is a plain error", http.StatusInternalServerError, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spy := setupStaffBridgeTest(t, tc.status)

			err := patchV1ProjectStaff(context.Background(), bridgeTestProjectSFID, map[string]string{
				"ExecutiveDirectorID": "0034100001oQRLDAA4",
				"ProgramManagerID":    v1ClearStaffValue,
			})

			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantNotFnd && !errors.Is(err, errV1ProjectNotFound) {
				t.Errorf("error = %v, want errors.Is(errV1ProjectNotFound)", err)
			}
			if !tc.wantNotFnd && tc.wantErr && errors.Is(err, errV1ProjectNotFound) {
				t.Errorf("error = %v, must not wrap errV1ProjectNotFound", err)
			}
			if !tc.wantNotFnd && tc.wantErr {
				var statusErr *v1ProjectStaffStatusError
				if !errors.As(err, &statusErr) || statusErr.status != tc.status {
					t.Errorf("error = %v, want *v1ProjectStaffStatusError with status %d", err, tc.status)
				}
			}
			if spy.patchCalls != 1 {
				t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
			}
			if got := spy.lastBody["ExecutiveDirectorID"]; got != "0034100001oQRLDAA4" {
				t.Errorf("ExecutiveDirectorID = %q, want %q", got, "0034100001oQRLDAA4")
			}
			if got := spy.lastBody["ProgramManagerID"]; got != v1ClearStaffValue {
				t.Errorf("ProgramManagerID = %q, want %q", got, v1ClearStaffValue)
			}
		})
	}
}
