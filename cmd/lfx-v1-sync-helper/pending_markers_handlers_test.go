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
	"strings"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
	nats "github.com/nats-io/nats.go"
)

// setupHandlerMarkerTest wires cfg + mappingStore + logger + patched
// fetchProjectBase and fetchCommitteeBase for the tests below.
// Package-level globals are restored via t.Cleanup.
func setupHandlerMarkerTest(t *testing.T) *fakeMappingStore {
	t.Helper()
	fake := newFakeMappingStore()

	origCfg := cfg
	origStore := mappingStore
	origLogger := logger
	origFetchProject := fetchProjectBase
	origFetchCommittee := fetchCommitteeBase
	origFetchV1Committee := fetchV1CommitteeData
	origReadLatestV1 := readLatestV1ProjectRow
	origFetchLiveLead := fetchLiveFormationLead

	cfg = &Config{Auth0ClientID: "test-client-id"}
	mappingStore = fake
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	readLatestV1ProjectRow = func(_ context.Context, _ string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "a000000000AAA", "slug__c": "test-slug", "name": "Test Project"}, 1, nil
	}

	t.Cleanup(func() {
		cfg = origCfg
		mappingStore = origStore
		logger = origLogger
		fetchProjectBase = origFetchProject
		fetchCommitteeBase = origFetchCommittee
		fetchV1CommitteeData = origFetchV1Committee
		readLatestV1ProjectRow = origReadLatestV1
		fetchLiveFormationLead = origFetchLiveLead
	})
	return fake
}

// TestHandleProjectUpdate_MarkerCleanedOnFetchBaseError pins the PR
// #170 T4 fix: when updateProject errors before mutating v2 (here,
// fetchProjectBase fails), handleProjectUpdate must delete the
// pending indexer-echo marker so it does not sit as garbage and
// silence a legitimate v2-native update within the freshness window.
//
// Regression guard: restoring the `err == nil && !baseMutated` gate
// (the pre-fix condition) would fail this test.
func TestHandleProjectUpdate_MarkerCleanedOnFetchBaseError(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const (
		sfid = "a000000000AAA"
		uid  = "proj-uid-1"
	)

	// Seed the SFID→UID mapping so handleProjectUpdate takes the
	// update branch instead of the create branch.
	if _, err := fake.Put(context.Background(), "project.sfid."+sfid, []byte(uid)); err != nil {
		t.Fatalf("seed sfid mapping: %v", err)
	}
	// Reverse mapping used by mapV1DataToProjectUpdateBasePayload for
	// parent lookups (safe to leave empty for this test — no parent
	// specified in v1Data).

	// Patch fetchProjectBase to fail synchronously — this is the
	// exact "updateProject errored before mutating" failure mode T4
	// calls out.
	sentinel := errors.New("simulated fetchProjectBase failure")
	fetchProjectBase = func(_ context.Context, _ string) (*projectservice.ProjectBase, string, error) {
		return nil, "", sentinel
	}

	v1Data := map[string]any{
		"sfid":              sfid,
		"slug__c":           "test-slug",
		"name":              "Test Project",
		"description__c":    "desc",
		"lastmodifiedbyid":  "0050M000009AbcDEQ", // not our client id → shouldSkipSync false
		"admin_category__c": "Sandbox Projects",
	}

	// handleProjectUpdate should return true (NACK — updateProject
	// returned a transient error) but MUST have cleaned up the
	// marker before returning.
	//
	// We don't assert on the return value because the current
	// implementation of updateProject wraps fetchProjectBase's
	// error without the *errTransientStore sentinel, so
	// isTransientStoreErr classifies it as permanent → ACK (return
	// false). Either way, the invariant we care about is the same:
	// the marker must be gone.
	handleProjectUpdate(context.Background(), "salesforce-project__c."+sfid, v1Data)

	markerKey := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, uid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("marker still present after updateProject error: err=%v — the `err == nil && !mutated` regression is back", err)
	}
}

func TestHandleProjectUpdate_RefreshBeforeMapping(t *testing.T) {
	setupHandlerMarkerTest(t)
	const sfid = "a000000000AAA"
	oldWrites := recentV1StaffWrites
	recentV1StaffWrites = newV1StaffWriteLog()
	t.Cleanup(func() { recentV1StaffWrites = oldWrites })
	for _, tt := range []struct {
		name           string
		latest         map[string]any
		readErr        error
		pending        bool
		wantRetry      bool
		wantUpdate     bool
		confirmedFloor bool
		olderDelivery  bool
		deferredHuman  bool
		activeMarker   bool
		observedHuman  bool
		historicalMask bool
	}{
		{name: "latest record deleted"},
		{name: "human rename survives later v2-authored staff PATCH", latest: map[string]any{"sfid": sfid, "name": "Renamed Project", "slug__c": "test-project", "parent_project__c": "parent", "lastmodifiedbyid": "test-client-id@clients"}, wantUpdate: true},
		{name: "latest soft deleted", latest: map[string]any{"sfid": sfid, "isdeleted": true}},
		{name: "read failure redelivers", readErr: errors.New("test KV failure"), wantRetry: true},
		{name: "reverse write pending does not redeliver", latest: map[string]any{"sfid": sfid, "name": "Test Project", "slug__c": "test-project", "parent_project__c": "parent", "formation_lead_email": "old@example.com"}, pending: true, wantUpdate: true},
		{name: "delayed floor plus one human row preserves confirmed live contact", latest: map[string]any{"sfid": sfid, "name": "Test Project", "slug__c": "test-project", "parent_project__c": "parent", "formation_lead_email": "old@example.com"}, confirmedFloor: true, wantUpdate: true},
		{name: "active marker survives restart and base-only save", latest: map[string]any{"sfid": sfid, "name": "Renamed Project", "slug__c": "test-project", "parent_project__c": "parent", "formation_lead_email": "old@example.com", formationLeadChangesKey: []any{}}, activeMarker: true, wantUpdate: true},
		{name: "observed revision cannot suppress unprocessed human contact", latest: map[string]any{"sfid": sfid, "name": "Test Project", "slug__c": "test-project", "parent_project__c": "parent", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}, observedHuman: true, wantUpdate: true, wantRetry: true},
		{name: "historical delivery mask cannot override marker baseline", latest: map[string]any{"sfid": sfid, "name": "Test Project", "slug__c": "test-project", "parent_project__c": "parent", "lastmodifiedbyid": "human-editor", "formation_lead_email": "stale@example.com", formationLeadChangesKey: []any{}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}, historicalMask: true, wantUpdate: true},
		{name: "older redelivery cannot supersede newer human revision", latest: map[string]any{"sfid": sfid, "name": "Test Project", "slug__c": "test-project", "parent_project__c": "parent", "formation_lead_email": "stale@example.com"}, olderDelivery: true, wantUpdate: true},
		{name: "deferred human v2 failure redelivers", latest: map[string]any{"sfid": sfid, "name": "Test Project", "slug__c": "test-project", "parent_project__c": "parent", "formation_lead_email": "human@example.com"}, deferredHuman: true, wantUpdate: true, wantRetry: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Each ordering case needs an independent marker baseline.
			mappingStore = newFakeMappingStore()
			if _, err := mappingStore.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
				t.Fatal(err)
			}
			if _, err := mappingStore.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
				t.Fatal(err)
			}
			recentV1StaffWrites = newV1StaffWriteLog()
			if tt.pending {
				recentV1StaffWrites.record(sfid, map[string]string{"formation_lead_email": "new@example.com"}, 1, staffWriteNow(),
					map[string]string{"formation_lead_email": "old@example.com"})
			}
			if tt.deferredHuman {
				recentV1StaffWrites.record(sfid, map[string]string{"formation_lead_email": "new@example.com"}, 1, staffWriteNow(),
					map[string]string{"formation_lead_email": "old@example.com"})
			}
			if tt.confirmedFloor {
				id := recentV1StaffWrites.record(sfid, map[string]string{"formation_lead_email": "new@example.com"}, 1, staffWriteNow())
				recentV1StaffWrites.confirm(id)
				fetchLiveFormationLead = func(context.Context, string) (*events.FormationLead, error) {
					return &events.FormationLead{Email: "new@example.com"}, nil
				}
			}
			if tt.olderDelivery {
				if err := markFormationLeadReconcile(t.Context(), "test-uid", sfid, 13); err != nil {
					t.Fatal(err)
				}
			}
			if tt.activeMarker {
				if err := markFormationLeadReconcile(t.Context(), "test-uid", sfid, 1); err != nil {
					t.Fatal(err)
				}
			}
			if tt.observedHuman {
				if err := markFormationLeadReconcile(t.Context(), "test-uid", sfid, 9, map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
					t.Fatal(err)
				}
				if err := rearmFormationLeadReconcile(t.Context(), "test-uid", sfid, 13, map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.historicalMask {
				if err := markFormationLeadReconcile(t.Context(), "test-uid", sfid, 9, map[string]uint64{"name": 0, "email": 1, "avatar": 0}); err != nil {
					t.Fatal(err)
				}
				if err := rearmFormationLeadReconcile(t.Context(), "test-uid", sfid, 12, map[string]uint64{"name": 0, "email": 1, "avatar": 0}); err != nil {
					t.Fatal(err)
				}
			}
			readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
				if tt.observedHuman || tt.historicalMask {
					return tt.latest, 12, tt.readErr
				}
				return tt.latest, 2, tt.readErr
			}
			updateReached := false
			fetchProjectBase = func(context.Context, string) (*projectservice.ProjectBase, string, error) {
				updateReached = true
				return nil, "", errors.New("test stopped after reaching unrelated project update")
			}
			var got bool
			if tt.activeMarker {
				got = handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid,
					map[string]any{"sfid": sfid, "lastmodifiedbyid": "0050M000009AbcDEQ", formationLeadChangesKey: []any{}}, 2)
			} else if tt.observedHuman || tt.historicalMask {
				email := "human@example.com"
				if tt.historicalMask {
					email = "stale@example.com"
				}
				got = handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid,
					map[string]any{"sfid": sfid, "lastmodifiedbyid": "human-editor", "formation_lead_email": email, formationLeadChangesKey: []any{"email"}}, 11)
			} else if tt.olderDelivery {
				got = handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, map[string]any{"sfid": sfid, "lastmodifiedbyid": "0050M000009AbcDEQ", "formation_lead_email": "older@example.com"}, 11)
			} else {
				got = handleProjectUpdate(t.Context(), "salesforce-project__c."+sfid, map[string]any{"sfid": sfid, "lastmodifiedbyid": "0050M000009AbcDEQ"})
			}
			if got != tt.wantRetry {
				t.Errorf("retry = %t, want %t", got, tt.wantRetry)
			}
			if updateReached != tt.wantUpdate {
				t.Errorf("project update reached = %t, want %t", updateReached, tt.wantUpdate)
			}
		})
	}
}

func TestHandleProjectUpdate_ContactMaskWithoutPendingWrite(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origUpdate := updateProjectFromV1
	t.Cleanup(func() { updateProjectFromV1 = origUpdate })
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		mask      []any
		wantName  bool
		ambiguous bool
	}{
		{name: "project-only rename preserves queued v2 email", mask: []any{}},
		{name: "PCC name edit preserves queued v2 email", mask: []any{"name"}, wantName: true},
		{name: "late bridge echo and rename cannot process overwritten PCC email", mask: []any{}, ambiguous: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			latest := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Renamed Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Alice Example", "formation_lead_email": "old@example.com", formationLeadChangesKey: tt.mask}
			if tt.ambiguous {
				latest[formationLeadVersionsKey] = map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}
				latest[formationLeadCurrentHumanKey] = map[string]any{"name": false, "email": false, "avatar": false}
				if err := markFormationLeadReconcile(t.Context(), "test-uid", sfid, 9, map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
					t.Fatal(err)
				}
				if err := rearmFormationLeadReconcile(t.Context(), "test-uid", sfid, 12, map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
					t.Fatal(err)
				}
			}
			readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
				if tt.ambiguous {
					return latest, 13, nil
				}
				return latest, 2, nil
			}
			called := false
			updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
				called = true
				if tt.wantName {
					if payload.FormationLead == nil || payload.FormationLead.Name == nil || *payload.FormationLead.Name != "Alice Example" || payload.FormationLead.Email != nil || !clears.FormationLeadPartial {
						t.Errorf("name-only PCC edit must retain v2 email on hydration: lead=%+v clears=%+v", payload.FormationLead, clears)
					}
				} else if payload.FormationLead != nil || clears.FormationLead {
					t.Errorf("base-only edit must preserve entire queued v2 lead: lead=%+v clears=%+v", payload.FormationLead, clears)
				}
				return false, nil
			}
			delivered, deliveredRev := latest, uint64(2)
			if tt.ambiguous {
				delivered = map[string]any{"sfid": sfid, "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", formationLeadChangesKey: []any{"email"}}
				deliveredRev = 11
			}
			if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, delivered, deliveredRev); retry || !called {
				t.Errorf("forward masked project update: retry=%t called=%t", retry, called)
			}
			if tt.ambiguous {
				marker, found, err := formationLeadObligation(t.Context(), "test-uid")
				if err != nil || !found || marker.ProcessedHumanRev != 9 {
					t.Errorf("overwritten human email was marked processed: marker=%+v found=%t err=%v", marker, found, err)
				}
			}
		})
	}
}

func TestHandleProjectUpdate_FailedContactSurvivesBaseOnlySave(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origUpdate, origWrites := updateProjectFromV1, recentV1StaffWrites
	t.Cleanup(func() { updateProjectFromV1, recentV1StaffWrites = origUpdate, origWrites })
	recentV1StaffWrites = newV1StaffWriteLog()
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	row := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}
	revision := uint64(2)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return row, revision, nil
	}
	writeCount := 0
	updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
		writeCount++
		if !clears.FormationLeadPartial || payload.FormationLead == nil || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "human@example.com" {
			t.Errorf("lost failed PCC email before v2 write: lead=%+v clears=%+v", payload.FormationLead, clears)
		}
		if writeCount == 1 {
			return false, errors.New("test v2 write unavailable")
		}
		return false, nil
	}
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, row, revision); !retry {
		t.Fatal("failed PCC contact write was acknowledged without retry")
	}
	marker, found, err := formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.Completed || marker.ProcessedHumanRev != 0 || marker.FieldVersions["email"] != 0 {
		t.Fatalf("failed contact write lacks durable pre-edit baseline: %+v found=%t err=%v", marker, found, err)
	}
	// Simulate a restart and an unrelated PCC rename after JetStream gives
	// up retrying the original contact delivery.
	recentV1StaffWrites = newV1StaffWriteLog()
	row = map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Renamed Project", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", formationLeadChangesKey: []any{}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}
	revision = 3
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, row, revision); retry || writeCount != 2 {
		t.Errorf("base-only save did not recover missed PCC contact: retry=%t writes=%d", retry, writeCount)
	}
}

func TestHandleProjectUpdate_QueuedContactInReverseSnapshot(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origUpdate, origWrites := updateProjectFromV1, recentV1StaffWrites
	t.Cleanup(func() { updateProjectFromV1, recentV1StaffWrites = origUpdate, origWrites })
	recentV1StaffWrites = newV1StaffWriteLog()
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	if err := markFormationLeadReconcile(t.Context(), "test-uid", sfid, 9,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	markerKey := formationReconcilePrefix + "test-uid"
	entry, err := store.Get(t.Context(), markerKey)
	if err != nil {
		t.Fatal(err)
	}
	var marker formationReconcileObligation
	if err := json.Unmarshal(entry.Value, &marker); err != nil {
		t.Fatal(err)
	}
	marker.Completed = true
	encoded, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(t.Context(), markerKey, encoded, entry.Revision); err != nil {
		t.Fatal(err)
	}
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", sfid, 13,
		map[string]uint64{"name": 0, "email": 1, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	latest := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return latest, 12, nil
	}
	called := false
	updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
		called = true
		if !clears.FormationLeadPartial || payload.FormationLead == nil || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "human@example.com" {
			t.Errorf("queued email was lost because reverse snapshot already observed it: lead=%+v clears=%+v", payload.FormationLead, clears)
		}
		return false, nil
	}
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, latest, 11); retry || !called {
		t.Errorf("queued PCC contact did not reach v2: retry=%t called=%t", retry, called)
	}
}

func TestHandleProjectUpdate_InitialContactProcessingHistory(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origCreate, origUpdate := createProjectFromV1, updateProjectFromV1
	t.Cleanup(func() { createProjectFromV1, updateProjectFromV1 = origCreate, origUpdate })
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	row := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Alice Example", "formation_lead_email": "alice@example.com", formationLeadChangesKey: []any{"name", "email"}, formationLeadVersionsKey: map[string]any{"name": float64(1), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": true, "email": true, "avatar": false}}
	revision := uint64(1)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return row, revision, nil
	}
	createProjectFromV1 = func(_ context.Context, payload *projectservice.CreateProjectPayload, _ string) (*projectservice.ProjectFull, error) {
		if payload.FormationLead == nil || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "alice@example.com" {
			t.Errorf("create did not sync initial v1 email: %+v", payload.FormationLead)
		}
		uid := "test-uid"
		return &projectservice.ProjectFull{UID: &uid}, nil
	}
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, row, revision); retry {
		t.Fatal("initial contact create requested retry")
	}
	marker, found, err := formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.ProcessedFields["name"] != 1 || marker.ProcessedFields["email"] != 1 {
		t.Fatalf("initial v1 contact missing processed generations: marker=%+v found=%t err=%v", marker, found, err)
	}
	updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
		if !clears.FormationLeadPartial || payload.FormationLead == nil || payload.FormationLead.Name == nil || payload.FormationLead.Email != nil {
			t.Errorf("later name-only edit must preserve initial email: lead=%+v clears=%+v", payload.FormationLead, clears)
		}
		return false, nil
	}
	row = map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Bob Fixture", "formation_lead_email": "alice@example.com", formationLeadChangesKey: []any{"name"}, formationLeadVersionsKey: map[string]any{"name": float64(2), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": true, "email": true, "avatar": false}}
	revision = 2
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, row, revision); retry {
		t.Fatal("later name-only contact edit requested retry")
	}
	marker, found, err = formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.ProcessedFields["name"] != 2 || marker.ProcessedFields["email"] != 1 {
		t.Errorf("partial update lost already-synced email watermark: marker=%+v found=%t err=%v", marker, found, err)
	}
}

func TestHandleProjectUpdate_TwoQueuedFieldsBeforeFirstMarker(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origUpdate := updateProjectFromV1
	t.Cleanup(func() { updateProjectFromV1 = origUpdate })
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	delivered := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Bob Fixture", "formation_lead_email": "alice@example.com", formationLeadChangesKey: []any{"name"}, formationLeadVersionsKey: map[string]any{"name": float64(2), "email": float64(1), "avatar": float64(0)}}
	latest := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Bob Fixture", "formation_lead_email": "bob@example.com", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(2), "email": float64(2), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": true, "email": true, "avatar": false}}
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return latest, 3, nil
	}
	called := false
	updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
		called = true
		if !clears.FormationLeadPartial || payload.FormationLead == nil || payload.FormationLead.Name == nil || *payload.FormationLead.Name != "Bob Fixture" || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "bob@example.com" {
			t.Errorf("refreshed contact failed to include both queued edits: lead=%+v clears=%+v", payload.FormationLead, clears)
		}
		return false, nil
	}
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, delivered, 2); retry || !called {
		t.Errorf("queued contact merge: retry=%t called=%t", retry, called)
	}
	marker, found, err := formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.ProcessedFields["name"] != 2 || marker.ProcessedFields["email"] != 2 {
		t.Errorf("queued email was not recorded after v2 accepted it: marker=%+v found=%t err=%v", marker, found, err)
	}
}

func TestHandleProjectUpdate_LegacyContactDoesNotCreateUnrepairableMarker(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origUpdate := updateProjectFromV1
	t.Cleanup(func() { updateProjectFromV1 = origUpdate })
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	updates := 0
	updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
		updates++
		if !clears.FormationLeadPartial || payload.FormationLead == nil {
			t.Errorf("legacy contact edit was not forwarded: lead=%+v clears=%+v", payload.FormationLead, clears)
		}
		return false, nil
	}
	for i, mask := range []any{[]any{"name"}, []any{"email"}} {
		row := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Alice Example", "formation_lead_email": "alice@example.com", formationLeadChangesKey: mask}
		if i == 1 {
			row["formation_lead_email"] = "bob@example.com"
		}
		revision := uint64(i + 2)
		readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
			return row, revision, nil
		}
		if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, row, revision); retry {
			t.Errorf("legacy contact edit %d unexpectedly requested retry", i)
		}
		if _, found, err := formationLeadObligation(t.Context(), "test-uid"); err != nil || found {
			t.Errorf("successful legacy edit %d created an unrepairable marker: found=%t err=%v", i, found, err)
		}
	}
	if updates != 2 {
		t.Errorf("legacy contact updates=%d, want both edits forwarded", updates)
	}
}

func TestHandleProjectUpdate_RecoveredDeliveryKeepsItsGenerations(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origUpdate, origWrites := updateProjectFromV1, recentV1StaffWrites
	t.Cleanup(func() { updateProjectFromV1, recentV1StaffWrites = origUpdate, origWrites })
	recentV1StaffWrites = newV1StaffWriteLog()
	const sfid = "a000000000AAA"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte("test-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.sfid.parent", []byte("parent-uid")); err != nil {
		t.Fatal(err)
	}
	baseline := map[string]uint64{"name": 0, "email": 0, "avatar": 0}
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", sfid, 10, baseline); err != nil {
		t.Fatal(err)
	}
	writeID := recentV1StaffWrites.record(sfid, map[string]string{"formation_lead_email": "bridge@example.com"}, 10, staffWriteNow(), map[string]string{"formation_lead_email": "old@example.com"})
	recentV1StaffWrites.confirm(writeID)
	recentV1StaffWrites.observeReplication(sfid, map[string]any{"formation_lead_email": "bridge@example.com"}, 13)
	latest := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "test-client-id@clients", "formation_lead_email": "bridge@example.com", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(2), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": false, "avatar": false}}
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return latest, 13, nil
	}
	delivered := map[string]any{"sfid": sfid, "slug__c": "test-project", "parent_project__c": "parent", "name": "Test Project", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human1@example.com", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}
	called := false
	updateProjectFromV1 = func(_ context.Context, _ *projectservice.UpdateProjectBasePayload, payload *projectservice.UpdateProjectSettingsPayload, clears staffClearFlags, _ string) (bool, error) {
		called = true
		if !clears.FormationLeadPartial || payload.FormationLead == nil || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "human1@example.com" {
			t.Errorf("older human delivery not recovered: lead=%+v clears=%+v", payload.FormationLead, clears)
		}
		return false, nil
	}
	if retry := handleProjectUpdateAtRevision(t.Context(), "salesforce-project__c."+sfid, delivered, 11); retry || !called {
		t.Errorf("recovered delivery: retry=%t called=%t", retry, called)
	}
	marker, found, err := formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.ProcessedFields["email"] != 1 {
		t.Errorf("newer generation falsely marked processed with older value: marker=%+v found=%t err=%v", marker, found, err)
	}
}

func TestReadLatestV1ProjectRow(t *testing.T) {
	origKV := v1KV
	kv := newFakeKV()
	v1KV = kv
	t.Cleanup(func() { v1KV = origKV })
	key := "salesforce-project__c.a000000000AAA"
	kv.data[key] = []byte(`{"sfid":"a000000000AAA","formation_lead_email":"new@example.com"}`)
	kv.rev[key] = 8
	row, revision, err := readLatestV1ProjectRow(t.Context(), key)
	if err != nil || revision != 8 || row["formation_lead_email"] != "new@example.com" {
		t.Errorf("latest row = %v (revision %d), error %v; want latest email at revision 8", row, revision, err)
	}
	row, revision, err = readLatestV1ProjectRow(t.Context(), key+"-missing")
	if err != nil || row != nil || revision != 0 {
		t.Errorf("missing row = %v (revision %d), error %v; want nil, 0, nil", row, revision, err)
	}
}

// TestHandleProjectDelete_MarkerCleanedOnDeleteError pins the PR
// #170 T5 fix: when deleteProject errors, handleProjectDelete must
// roll back the pending indexer-echo marker so an unrelated genuine
// v2-native delete for the same UID within the freshness window is
// not silenced by the v2→v1 side.
//
// Regression guard: removing the deletePendingMarker call in the
// deleteProject error branch would fail this test.
func TestHandleProjectDelete_MarkerCleanedOnDeleteError(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const (
		sfid = "a000000000AAB"
		uid  = "proj-uid-2"
	)

	// Seed the SFID→UID mapping.
	if _, err := fake.Put(context.Background(), "project.sfid."+sfid, []byte(uid)); err != nil {
		t.Fatalf("seed sfid mapping: %v", err)
	}

	// Patch fetchProjectBase to fail — deleteProject calls it first
	// (see client_projects.go deleteProject) so the delete API call
	// never happens. This exercises the "delete errored, marker must
	// be rolled back" path.
	sentinel := errors.New("simulated fetchProjectBase failure inside deleteProject")
	fetchProjectBase = func(_ context.Context, _ string) (*projectservice.ProjectBase, string, error) {
		return nil, "", sentinel
	}

	// handleProjectDelete returns true (retry) on delete error, but
	// the marker must be gone.
	got := handleProjectDelete(context.Background(), "salesforce-project__c."+sfid, sfid, "test-principal")
	if !got {
		t.Errorf("handleProjectDelete returned false; expected true (NACK for retry)")
	}

	markerKey := pendingMarkerKey(markerV1ToV2, markerOpDelete, markerResourceProject, uid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("marker still present after deleteProject error: err=%v — the T5 rollback is missing", err)
	}
}

func TestHandleProjectDelete_TombstonesFormationLeadRepair(t *testing.T) {
	store := setupHandlerMarkerTest(t)
	origDelete := deleteProjectFromV1
	t.Cleanup(func() { deleteProjectFromV1 = origDelete })
	const sfid, uid = "a000000000AAB", "test-uid"
	if _, err := store.Put(t.Context(), "project.sfid."+sfid, []byte(uid)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "project.uid."+uid, []byte(sfid)); err != nil {
		t.Fatal(err)
	}
	if err := rearmFormationLeadReconcile(t.Context(), uid, sfid, 1,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	deleteProjectFromV1 = func(context.Context, string, string) error { return nil }
	if retry := handleProjectDelete(t.Context(), "salesforce-project__c."+sfid, sfid, "test-principal"); retry {
		t.Fatal("confirmed delete unexpectedly requested retry")
	}
	entry, err := store.Get(t.Context(), formationReconcilePrefix+uid)
	if err != nil || !isTombstonedMapping(entry.Value) {
		t.Errorf("deleted project retained active Formation Lead repair: entry=%+v err=%v", entry, err)
	}
}

// TestHandleProjectDelete_ConsumesFreshMarkerBeforeMappingLookup
// pins the PR #170 T3 (thread 3) fix: the v2_to_v1 delete marker
// consume runs at the very top of handleProjectDelete, before any
// mapping lookup. If the writer's tombstone landed first (mapping
// tombstoned before the WAL echo arrived), the pre-fix ordering
// would bail at isTombstonedMapping and leak the marker.
func TestHandleProjectDelete_ConsumesFreshMarkerBeforeMappingLookup(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const sfid = "a000000000AAC"

	// Seed a fresh v2_to_v1 delete marker keyed by SFID (simulating
	// syncProjectDeleteToV1 having written it before the v1 DELETE).
	writePendingMarker(context.Background(), markerV2ToV1, markerOpDelete, markerResourceProject, sfid)

	// Deliberately DO NOT seed a mapping. If the marker consume were
	// still gated behind the mapping lookup, the ErrKeyNotFound bail
	// would leave the marker in place. With the T3 fix, consume runs
	// first and clears the marker.
	handleProjectDelete(context.Background(), "salesforce-project__c."+sfid, sfid, "test-principal")

	markerKey := pendingMarkerKey(markerV2ToV1, markerOpDelete, markerResourceProject, sfid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("v2_to_v1 delete marker still present after handleProjectDelete: err=%v — consume-before-mapping-lookup regression is back", err)
	}
}

// TestCommitteeDeleteDispatcher_ConsumesMarkerOnTombstonedMapping
// pins the follow-up copilot review fix: the dispatcher for
// lfx.committee.deleted events must consume the pending
// v1_to_v2.delete.committee marker BEFORE looking up the reverse
// mapping. Concrete race:
//
//   - handleCommitteeDelete writes the marker, calls v2 DELETE,
//     tombstones committee.uid.<uid> AFTER the API call.
//   - v2 committee-service publishes the indexer event.
//   - If the tombstone lands before the event arrives here, the
//     dispatcher reads the tombstoned mapping, splitTwoParts("!del")
//     returns empty SFIDs, and the branch returns nil WITHOUT ever
//     calling syncCommitteeDeleteToV1. Pre-fix: the marker inside
//     syncCommitteeDeleteToV1 is never reached → marker leaks
//     forever.
//
// This test exercises the exact race outcome by seeding a fresh
// marker AND a tombstoned reverse mapping, then verifying the marker
// is gone after the dispatcher returns.
func TestCommitteeDeleteDispatcher_ConsumesMarkerOnTombstonedMapping(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const uid = "cmt-uid-1"

	// Seed a fresh v1_to_v2 delete marker (as handleCommitteeDelete
	// would have written before the v2 DELETE succeeded).
	writePendingMarker(context.Background(), markerV1ToV2, markerOpDelete, markerResourceCommittee, uid)

	// Seed the reverse mapping as tombstoned — simulates the race
	// where handleCommitteeDelete's post-DELETE tombstoning landed
	// before this event arrived at the dispatcher.
	if _, err := fake.Put(context.Background(), "committee.uid."+uid, []byte(tombstoneMarker)); err != nil {
		t.Fatalf("seed tombstoned reverse mapping: %v", err)
	}

	// Build a minimal deleted-committee indexer event and dispatch it.
	event := indexingEvent{
		ObjectID:   uid,
		ObjectType: "committee",
		Action:     "deleted",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if err := processCommitteeIndexingEvent(context.Background(), "lfx.committee.deleted", payload); err != nil {
		t.Fatalf("processCommitteeIndexingEvent: %v", err)
	}

	markerKey := pendingMarkerKey(markerV1ToV2, markerOpDelete, markerResourceCommittee, uid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("v1_to_v2 delete marker still present after committee delete dispatcher on tombstoned mapping: err=%v — consume-before-mapping-lookup regression is back", err)
	}
}

// TestHandleCommitteeUpdate_MarkerCleanedOnFetchBaseError is the
// symmetric committee-side counterpart to
// TestHandleProjectUpdate_MarkerCleanedOnFetchBaseError. Pins the T4
// fix on the committee update path: when updateCommittee errors
// before mutating v2 (here, fetchCommitteeBase fails),
// handleCommitteeUpdate must delete the pending indexer-echo marker.
//
// Regression guard: restoring the `err == nil && mutated == false`
// gate on the committee update site (or reverting updateCommittee's
// `(bool, ..., error)` signature so the caller can't distinguish
// no-op from success) would fail this test.
func TestHandleCommitteeUpdate_MarkerCleanedOnFetchBaseError(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const (
		committeeSFID = "c000000000BBB"
		projectSFID   = "p000000000AAA"
		uid           = "cmt-uid-3"
	)

	// Seed the committee SFID→UID mapping so handleCommitteeUpdate
	// takes the update branch.
	if _, err := fake.Put(context.Background(), "committee.sfid."+committeeSFID, []byte(uid)); err != nil {
		t.Fatalf("seed committee sfid mapping: %v", err)
	}
	// Seed the parent project SFID→UID mapping so
	// mapV1DataToCommitteeUpdateBasePayload can resolve the parent.
	if _, err := fake.Put(context.Background(), "project.sfid."+projectSFID, []byte("proj-uid-parent")); err != nil {
		t.Fatalf("seed parent project mapping: %v", err)
	}

	// Patch fetchCommitteeBase to fail synchronously — exactly
	// mirrors the project-side test.
	sentinel := errors.New("simulated fetchCommitteeBase failure")
	fetchCommitteeBase = func(_ context.Context, _ string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		return nil, "", sentinel
	}

	v1Data := map[string]any{
		"sfid":             committeeSFID,
		"mailing_list__c":  "Test Committee",
		"description__c":   "desc",
		"project_name__c":  projectSFID,
		"lastmodifiedbyid": "0050M000009AbcDEQ", // not our client id → shouldSkipSync false
	}
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) {
		return v1Data, true, nil
	}

	handleCommitteeUpdate(context.Background(), "platform-collaboration__c."+committeeSFID, v1Data)

	markerKey := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceCommittee, uid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("marker still present after updateCommittee error: err=%v — the T4 committee cleanup regression is back", err)
	}
}

// TestProjectDeleteDispatcher_ConsumesMarkerOnTombstonedMapping is
// the symmetric project-side counterpart to
// TestCommitteeDeleteDispatcher_ConsumesMarkerOnTombstonedMapping.
// Pins the post-copilot follow-up fix on projectIndexerEventHandler:
// consume must run at the top of the `deleted` branch so a
// tombstoned reverse mapping (post-DELETE handleProjectDelete state)
// does not short-circuit the dispatcher and leak the marker.
//
// Regression guard: restoring the consume inside syncProjectDeleteToV1
// (which is never reached when lookupV1ProjectSFIDForEvent returns
// "mapping tombstoned") would fail this test.
func TestProjectDeleteDispatcher_ConsumesMarkerOnTombstonedMapping(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const uid = "proj-uid-3"

	// Seed a fresh v1_to_v2 delete marker (as handleProjectDelete
	// would have written before the v2 DELETE succeeded).
	writePendingMarker(context.Background(), markerV1ToV2, markerOpDelete, markerResourceProject, uid)

	// Seed the reverse mapping as tombstoned — simulates the race
	// where handleProjectDelete's post-DELETE tombstoning landed
	// before this event arrived at the dispatcher.
	if _, err := fake.Put(context.Background(), "project.uid."+uid, []byte(tombstoneMarker)); err != nil {
		t.Fatalf("seed tombstoned reverse mapping: %v", err)
	}

	// Build a minimal deleted-project indexer event and dispatch it
	// via the projectIndexerEventHandler(*nats.Msg) entry point.
	event := indexingEvent{
		ObjectID:   uid,
		ObjectType: "project",
		Action:     "deleted",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	projectIndexerEventHandler(&nats.Msg{Subject: "lfx.project.deleted", Data: payload})

	markerKey := pendingMarkerKey(markerV1ToV2, markerOpDelete, markerResourceProject, uid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("v1_to_v2 delete marker still present after project delete dispatcher on tombstoned mapping: err=%v — consume-at-dispatch-top regression is back", err)
	}
}

// TestCommitteeMemberDeleteDispatcher_ConsumesMarkerOnTombstonedMapping
// is the symmetric committee-member-side counterpart. Pins the
// post-copilot follow-up fix on processCommitteeMemberIndexingEvent:
// consume must run at the top of the `deleted` branch so a
// tombstoned reverse mapping (post-DELETE handleCommitteeMemberDelete
// state) does not short-circuit the dispatcher via
// parseCommitteeMemberReverseMapping's ok=false path and leak the
// marker.
//
// Regression guard: restoring the consume inside
// syncCommitteeMemberDeleteToV1 (which is never reached when
// parseCommitteeMemberReverseMapping returns ok=false) would fail
// this test.
func TestCommitteeMemberDeleteDispatcher_ConsumesMarkerOnTombstonedMapping(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const uid = "member-uid-1"

	// Seed a fresh v1_to_v2 delete marker (as
	// handleCommitteeMemberDelete would have written before the v2
	// DELETE succeeded).
	writePendingMarker(context.Background(), markerV1ToV2, markerOpDelete, markerResourceCommitteeMember, uid)

	// Seed the reverse mapping as tombstoned.
	if _, err := fake.Put(context.Background(), "committee_member.uid."+uid, []byte(tombstoneMarker)); err != nil {
		t.Fatalf("seed tombstoned reverse mapping: %v", err)
	}

	// Build a minimal deleted-committee-member indexer event and
	// dispatch it.
	event := indexingEvent{
		ObjectID:   uid,
		ObjectType: "committee_member",
		Action:     "deleted",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if err := processCommitteeMemberIndexingEvent(context.Background(), "lfx.committee_member.deleted", payload); err != nil {
		t.Fatalf("processCommitteeMemberIndexingEvent: %v", err)
	}

	markerKey := pendingMarkerKey(markerV1ToV2, markerOpDelete, markerResourceCommitteeMember, uid)
	if _, err := fake.Get(context.Background(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("v1_to_v2 delete marker still present after committee member delete dispatcher on tombstoned mapping: err=%v — consume-at-dispatch-top regression is back", err)
	}
}

// TestCommitteeMemberDeleteDispatcher_TombstonedMappingNoMarker pins the
// fix for re-delivered deleted events: when the reverse mapping is already
// tombstoned and there is NO v1_to_v2 pending marker (already consumed on
// the first delivery, or absent because the delete originated in v2),
// processCommitteeMemberIndexingEvent must return nil cleanly and log at
// Info level — not fall through to the "unexpected format" Warn path.
//
// This is the source of the "committee member reverse mapping has
// unexpected format, skipping" warnings seen in production with
// mapping_value: '!del' (linuxfoundation/lfx-self-serve-ops#24).
// Regression guard: removing the isTombstonedMapping guard after the
// reverse-mapping lookup in the deleted branch causes this test to fail
// because the Warn message appears and the Info tombstone message does not.
func TestCommitteeMemberDeleteDispatcher_TombstonedMappingNoMarker(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const uid = "member-uid-v2-delete"

	// No pending marker — already consumed on a prior delivery (or absent
	// because the delete originated in v2).

	// Seed the reverse mapping as tombstoned.
	if _, err := fake.Put(context.Background(), "committee_member.uid."+uid, []byte(tombstoneMarker)); err != nil {
		t.Fatalf("seed tombstoned reverse mapping: %v", err)
	}

	// Capture log output for this call by replacing the package logger with
	// a text handler writing to a buffer, then restoring it after.
	var buf bytes.Buffer
	origLogger := logger
	logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() { logger = origLogger })

	event := indexingEvent{
		ObjectID:   uid,
		ObjectType: "committee_member",
		Action:     "deleted",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if err := processCommitteeMemberIndexingEvent(context.Background(), "lfx.committee_member.deleted", payload); err != nil {
		t.Fatalf("processCommitteeMemberIndexingEvent: unexpected error = %v", err)
	}

	logs := buf.String()
	// The tombstone-specific Info message must appear.
	if !strings.Contains(logs, "already tombstoned") {
		t.Errorf("expected tombstone Info log, got:\n%s", logs)
	}
	// The "unexpected format" Warn must NOT appear — that's the regression we're guarding.
	if strings.Contains(logs, "unexpected format") {
		t.Errorf("unexpected-format Warn fired for a tombstoned mapping (regression):\n%s", logs)
	}
}

// TestCommitteeMemberUpdateDispatcher_TombstonedMappingSkipsSync pins the
// fix for the updated-branch tombstone guard: when a committee_member
// "updated" indexer event arrives but the reverse mapping is already
// tombstoned (!del), processCommitteeMemberIndexingEvent must return nil
// and log at Info level — not fall through to the "unexpected format" Warn.
//
// Regression guard: removing the isTombstonedMapping guard before
// parseCommitteeMemberReverseMapping in the updated branch causes this
// test to fail because the Warn message appears and the Info tombstone
// message does not.
func TestCommitteeMemberUpdateDispatcher_TombstonedMappingSkipsSync(t *testing.T) {
	fake := setupHandlerMarkerTest(t)

	const uid = "member-uid-stale-update"

	// Seed the reverse mapping as tombstoned — member was previously deleted.
	if _, err := fake.Put(context.Background(), "committee_member.uid."+uid, []byte(tombstoneMarker)); err != nil {
		t.Fatalf("seed tombstoned reverse mapping: %v", err)
	}

	// Capture log output for this call.
	var buf bytes.Buffer
	origLogger := logger
	logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() { logger = origLogger })

	event := indexingEvent{
		ObjectID:   uid,
		ObjectType: "committee_member",
		Action:     "updated",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if err := processCommitteeMemberIndexingEvent(context.Background(), "lfx.committee_member.updated", payload); err != nil {
		t.Fatalf("processCommitteeMemberIndexingEvent: unexpected error = %v", err)
	}

	logs := buf.String()
	// The tombstone-specific Info message must appear.
	if !strings.Contains(logs, "tombstoned") {
		t.Errorf("expected tombstone Info log, got:\n%s", logs)
	}
	// The "unexpected format" Warn must NOT appear.
	if strings.Contains(logs, "unexpected format") {
		t.Errorf("unexpected-format Warn fired for a tombstoned mapping (regression):\n%s", logs)
	}
}
