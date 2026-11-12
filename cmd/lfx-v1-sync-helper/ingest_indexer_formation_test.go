// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
)

func TestSyncInitialFormationLeadToV1(t *testing.T) {
	origFetch, origPatch, origWrites := fetchInitialFormationSettings, patchInitialFormationLead, recentV1StaffWrites
	origStore := mappingStore
	t.Cleanup(func() {
		fetchInitialFormationSettings, patchInitialFormationLead, recentV1StaffWrites = origFetch, origPatch, origWrites
		mappingStore = origStore
	})
	for _, tt := range []struct {
		name        string
		settings    *projectservice.ProjectSettings
		fetchErr    error
		patchErr    error
		wantPatch   bool
		wantErr     bool
		wantPending bool
		wantMarker  bool
	}{
		{name: "no lead", settings: &projectservice.ProjectSettings{}},
		{name: "initial lead", settings: &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{
			Name: stringToStringPtr("Alice Example"), Email: stringToStringPtr("alice@example.com"),
		}}, wantPatch: true},
		{name: "settings read fails", fetchErr: errors.New("test v2 error"), wantErr: true, wantMarker: true},
		{name: "v1 rejects permission", settings: &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{
			Name: stringToStringPtr("Alice Example"),
		}}, patchErr: &v1ProjectStaffStatusError{status: http.StatusForbidden}, wantPatch: true, wantErr: true},
		{name: "v1 times out", settings: &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{
			Name: stringToStringPtr("Alice Example"),
		}}, patchErr: errors.New("test timeout"), wantPatch: true, wantErr: true, wantPending: true, wantMarker: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recentV1StaffWrites = newV1StaffWriteLog()
			mappingStore = newFakeMappingStore()
			fetchInitialFormationSettings = func(_ context.Context, uid string) (*projectservice.ProjectSettings, string, error) {
				if uid != "test-uid" {
					t.Fatalf("uid = %q", uid)
				}
				return tt.settings, "1", tt.fetchErr
			}
			patched := false
			patchInitialFormationLead = func(_ context.Context, sfid string, fields any) error {
				patched = true
				if tt.settings != nil && tt.settings.FormationLead != nil {
					marker, found, err := formationLeadObligation(t.Context(), "test-uid")
					if err != nil || !found || marker.Completed {
						t.Errorf("initial PATCH dispatched without durable repair marker: marker=%+v found=%t err=%v", marker, found, err)
					}
				}
				if sfid != "test-sfid" {
					t.Fatalf("sfid = %q", sfid)
				}
				values, ok := fields.(map[string]any)
				if !ok || values["FormationLead"] == nil {
					t.Fatalf("PATCH fields = %#v", fields)
				}
				return tt.patchErr
			}
			err := syncInitialFormationLeadToV1(t.Context(), "test-uid", "test-sfid")
			if (err != nil) != tt.wantErr || patched != tt.wantPatch {
				t.Errorf("sync err=%v patched=%t, want err=%t patched=%t", err, patched, tt.wantErr, tt.wantPatch)
			}
			writes := recentV1StaffWrites.writes[v1StaffWriteKey("test-sfid", "formation_lead_name")]
			if (len(writes) > 0) != tt.wantPending && tt.patchErr != nil {
				t.Errorf("pending writes = %+v, want pending=%t", writes, tt.wantPending)
			}
			marker, markerErr := mappingStore.Get(t.Context(), formationReconcilePrefix+"test-uid")
			if tt.wantMarker && (markerErr != nil || len(marker.Value) == 0) {
				t.Errorf("uncertain initial PATCH lost durable repair marker: err=%v", markerErr)
			}
		})
	}
}

func TestSyncInitialFormationLeadToV1_AbortsWithoutMarker(t *testing.T) {
	origFetch, origPatch, origStore := fetchInitialFormationSettings, patchInitialFormationLead, mappingStore
	t.Cleanup(func() {
		fetchInitialFormationSettings, patchInitialFormationLead, mappingStore = origFetch, origPatch, origStore
	})
	store := newFakeMappingStore()
	store.nextCreateErr = errors.New("test marker unavailable")
	mappingStore = store
	fetchInitialFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("alice@example.com")}}, "1", nil
	}
	patchInitialFormationLead = func(context.Context, string, any) error {
		t.Fatal("initial PATCH was sent without a durable obligation")
		return nil
	}
	if err := syncInitialFormationLeadToV1(t.Context(), "test-uid", "test-sfid"); err == nil {
		t.Fatal("failed marker write did not abort initial PATCH")
	}
}

func TestSyncInitialFormationLeadToV1_SerializesSettingsEdits(t *testing.T) {
	spy := setupStaffBridgeTest(t, http.StatusOK)
	initial := &events.FormationLead{Name: "Alice Example", Email: "alice@example.com"}
	newer := &events.FormationLead{Name: "Bob Fixture", Email: "bob@example.com"}
	spy.liveFormationLead = initial
	origFetch := fetchInitialFormationSettings
	t.Cleanup(func() { fetchInitialFormationSettings = origFetch })
	fetched, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	fetchInitialFormationSettings = func(ctx context.Context, _ string) (*projectservice.ProjectSettings, string, error) {
		close(fetched)
		select {
		case <-release:
			return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{
				Name: stringToStringPtr(initial.Name), Email: stringToStringPtr(initial.Email),
			}}, "1", nil
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	initialDone := make(chan error, 1)
	go func() {
		initialDone <- syncInitialFormationLeadToV1(t.Context(), bridgeTestProjectUID, bridgeTestProjectSFID)
	}()
	<-fetched // Initial helper holds the project lock while its settings read is in flight.
	settingsDone := make(chan struct{})
	msg := settingsUpdatedMsg(t, "test-actor", events.ProjectSettings{FormationLead: initial}, events.ProjectSettings{FormationLead: newer})
	go func() {
		handleProjectSettingsUpdated(msg)
		close(settingsDone)
	}()
	select {
	case <-settingsDone:
		t.Fatal("newer settings edit raced past the in-flight initial lead sync")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-initialDone; err != nil {
		t.Fatal(err)
	}
	<-settingsDone
	if spy.patchCalls != 2 {
		t.Fatalf("PATCH calls = %d, want initial and newer edit", spy.patchCalls)
	}
	var got events.FormationLead
	if err := json.Unmarshal(spy.lastJSON["FormationLead"], &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != newer.Name || got.Email != newer.Email {
		t.Errorf("final PATCH = %+v, want newer contact %+v", got, newer)
	}
}
