// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

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

	t.Cleanup(func() {
		cfg = origCfg
		logger = origLogger
		v1HTTPClient = origV1HTTPClient
		getV1ProjectSFIDByUID = origGetSFID
		getV1ProjectStaffSFIDs = origGetStaff
		resolveV1UserSFIDByUsername = origByUsername
		resolveV1UserSFIDByEmail = origByEmail
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
		if username == "kperez" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez", Email: "kperez@linuxfoundation.org"}})
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

	pm := &events.UserInfo{Username: "kperez", Email: "kperez@linuxfoundation.org"}
	msg := settingsUpdatedMsg(t, "audigregorie",
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
	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{OpportunityOwner: &events.UserInfo{Username: "mwhite"}})
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
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
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
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want 1", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_UnmappedProjectSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	getV1ProjectSFIDByUID = func(context.Context, string) (string, error) { return "", nil }

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
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

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
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

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}},
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

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}},
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
		if username == "kperez" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{
			ExecutiveDirector: &events.UserInfo{Name: "Ghost User", Email: "ghost@example.com"},
			ProgramManager:    &events.UserInfo{Username: "kperez"},
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
		if email == "caseycain@linuxfoundation.org" {
			return "0032M00002cQF7WQAW", nil
		}
		return "", nil
	}

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Email: "caseycain@linuxfoundation.org"}})
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

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0 (transient resolution failure must not write)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_MissingV1RecordSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(context.Context, string) (string, error) { return bridgeTestPMSFID, nil }
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) { return nil, nil }

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
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
	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{},
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}})
	handleProjectSettingsUpdated(msg)

	if spy.patchCalls != 1 {
		t.Fatalf("PATCH calls = %d, want exactly 1 (no retry on 404)", spy.patchCalls)
	}
}

func TestHandleProjectSettingsUpdated_EDChangeOmitsUnchangedPM(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	resolveV1UserSFIDByUsername = func(_ context.Context, username string) (string, error) {
		if username == "asitha" {
			return "0034100001oQRLDAA4", nil
		}
		return "", nil
	}
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"program_manager__c": bridgeTestPMSFID}, nil
	}

	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{ProgramManager: &events.UserInfo{Username: "kperez"}},
		events.ProjectSettings{
			ProgramManager:    &events.UserInfo{Username: "kperez"},
			ExecutiveDirector: &events.UserInfo{Username: "asitha"},
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
		if username == "kperez" {
			return bridgeTestPMSFID, nil
		}
		return "", nil
	}
	// v1 already carries the new ED from the first edit, but the KV replica
	// still holds the previous one because the WAL pipeline has not caught up.
	getV1ProjectStaffSFIDs = func(context.Context, string) (map[string]string, error) {
		return map[string]string{"executive_director__c": "0034100001staleED"}, nil
	}

	ed := &events.UserInfo{Username: "asitha"}
	msg := settingsUpdatedMsg(t, "audigregorie",
		events.ProjectSettings{ExecutiveDirector: ed},
		events.ProjectSettings{ExecutiveDirector: ed, ProgramManager: &events.UserInfo{Username: "kperez"}})
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

func TestHandleProjectSettingsUpdated_InvalidEventSkips(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	handleProjectSettingsUpdated(&nats.Msg{Data: []byte("not-json")})
	if spy.patchCalls != 0 {
		t.Fatalf("PATCH calls = %d, want 0", spy.patchCalls)
	}
}

func TestStaffUserInfosEqual(t *testing.T) {
	kperez := &events.UserInfo{Username: "kperez", Email: "kperez@linuxfoundation.org"}
	tests := []struct {
		name string
		a, b *events.UserInfo
		want bool
	}{
		{"both nil", nil, nil, true},
		{"nil vs set", nil, kperez, false},
		{"set vs nil", kperez, nil, false},
		{"identical", kperez, &events.UserInfo{Username: "kperez", Email: "kperez@linuxfoundation.org"}, true},
		{"name/avatar drift only", kperez, &events.UserInfo{Username: "kperez", Email: "kperez@linuxfoundation.org", Name: "K. Perez", Avatar: "x"}, true},
		{"username differs", kperez, &events.UserInfo{Username: "kperez2", Email: "kperez@linuxfoundation.org"}, false},
		{"email differs", kperez, &events.UserInfo{Username: "kperez", Email: "other@linuxfoundation.org"}, false},
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
