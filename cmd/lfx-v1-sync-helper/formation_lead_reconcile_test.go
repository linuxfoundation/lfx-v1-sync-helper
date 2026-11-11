// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/linuxfoundation/lfx-v2-project-service/pkg/events"
)

func TestReconcileFormationLead(t *testing.T) {
	store := newFakeMappingStore()
	origStore, origCfg, origLogger := mappingStore, cfg, logger
	origWrites := recentV1StaffWrites
	origNow, origRead := formationReconcileNow, readLatestV1ProjectRow
	origStaffNow := staffWriteNow
	origSettings, origLive, origLiveState, origPatch := fetchDeferredFormationSettings, fetchLiveFormationLead, fetchLiveFormationLeadState, patchDeferredFormationLead
	origApplyHuman := applyNewerHumanFormation
	t.Cleanup(func() {
		mappingStore, cfg, logger = origStore, origCfg, origLogger
		recentV1StaffWrites = origWrites
		formationReconcileNow, readLatestV1ProjectRow = origNow, origRead
		staffWriteNow = origStaffNow
		fetchDeferredFormationSettings, fetchLiveFormationLead, fetchLiveFormationLeadState, patchDeferredFormationLead = origSettings, origLive, origLiveState, origPatch
		applyNewerHumanFormation = origApplyHuman
	})
	mappingStore = store
	recentV1StaffWrites = newV1StaffWriteLog()
	cfg = &Config{Auth0ClientID: "test-client-id"}
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	staffWriteNow = func() time.Time { return now }
	const key = formationReconcilePrefix + "test-uid"
	if err := markFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10, map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(t.Context(), key)
	if err != nil || strings.Contains(string(entry.Value), "alice@example.com") {
		t.Fatalf("durable marker = %s, error %v; no PII allowed", entry.Value, err)
	}
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "test-client-id@clients", "systemmodstamp": "2026-11-11T00:00:00Z", formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(0), "avatar": float64(0)}}, 11, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{
			Name: stringToStringPtr("Alice Example"), Email: stringToStringPtr("alice@example.com"),
		}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Name: "Old Contact", Email: "old@example.com"}, SystemModStamp: "2026-11-11T00:00:00Z"}, nil
	}
	patches := 0
	patchDeferredFormationLead = func(ctx context.Context, _ string, fields any) error {
		patches++
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > v1ProjectStaffSyncTimeout || time.Until(deadline) < v1ProjectStaffSyncTimeout-time.Second {
			t.Errorf("repair PATCH deadline = %v, want bounded request after acquiring lock", deadline)
		}
		if fields == nil {
			t.Error("missing contact PATCH")
		}
		return nil
	}
	if err := reconcileFormationLead(t.Context(), key); err != nil || patches != 0 {
		t.Fatalf("early reconcile error=%v patches=%d", err, patches)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Name: "New PCC Contact"}, SystemModStamp: "2026-11-11T00:00:01Z"}, nil
	}
	if err := reconcileFormationLead(t.Context(), key); err == nil || patches != 0 {
		t.Fatalf("unreplicated live PCC edit must defer: err=%v patches=%d", err, patches)
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Name: "Old Contact", Email: "old@example.com"}, SystemModStamp: "2026-11-11T00:00:00Z"}, nil
	}
	patchDeferredFormationLead = func(_ context.Context, _ string, _ any) error {
		patches++
		marker, found, readErr := formationLeadObligation(t.Context(), "test-uid")
		if readErr != nil || !found || !marker.Due.After(now) {
			t.Errorf("repair PATCH sent without a fresh durable deadline: marker=%+v found=%t err=%v", marker, found, readErr)
		}
		if patches == 1 {
			return errors.New("test transient failure")
		}
		return nil
	}
	store.nextUpdateErr = errors.New("test marker unavailable")
	if err := reconcileFormationLead(t.Context(), key); err == nil || patches != 0 {
		t.Fatalf("repair PATCH ran without a persisted deadline: err=%v patches=%d", err, patches)
	}
	if err := reconcileFormationLead(t.Context(), key); err == nil {
		t.Fatal("transient PATCH error must keep durable marker")
	}
	if _, err := store.Get(t.Context(), key); err != nil {
		t.Fatal("failed reconciliation lost marker", err)
	}
	if err := reconcileFormationLead(t.Context(), key); err != nil || patches != 1 {
		t.Fatalf("uncertain PATCH retried too early: err=%v patches=%d", err, patches)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	// Simulates a process restart: the in-memory write log is not involved.
	if err := reconcileFormationLead(t.Context(), key); err != nil || patches != 2 {
		t.Fatalf("restart retry error=%v patches=%d, want 2", err, patches)
	}
	entry, err = store.Get(t.Context(), key)
	var pending formationReconcileObligation
	if err != nil || json.Unmarshal(entry.Value, &pending) != nil || pending.Completed {
		t.Fatalf("successful HTTP repair must await its WAL echo: marker=%+v err=%v", pending, err)
	}
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "test-client-id@clients", "formation_lead_name": "Alice Example", "formation_lead_email": "alice@example.com", "systemmodstamp": "2026-11-11T00:00:02Z", formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(0), "avatar": float64(0)}}, 12, nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Name: "Alice Example", Email: "alice@example.com"}, SystemModStamp: "2026-11-11T00:00:02Z"}, nil
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	if err := reconcileFormationLead(t.Context(), key); err != nil {
		t.Fatalf("matching WAL echo did not complete repair: %v", err)
	}
	entry, err = store.Get(t.Context(), key)
	var completed formationReconcileObligation
	if err != nil || json.Unmarshal(entry.Value, &completed) != nil || !completed.Completed || completed.SourceRev != 12 {
		t.Fatalf("completed marker = %v, error %v; want observed WAL revision 12", entry, err)
	}
	if older, err := olderFormationLeadDelivery(t.Context(), "test-uid", 9); err != nil || !older {
		t.Errorf("older human delivery should be rejected after completion: older=%t err=%v", older, err)
	}
	if handled, err := olderFormationLeadDelivery(t.Context(), "test-uid", 10); err != nil || !handled {
		t.Errorf("same human revision redelivered after restart must preserve its contact: handled=%t err=%v", handled, err)
	}
	if err := markFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 9); err != nil {
		t.Fatal(err)
	}
	if entryAfter, err := store.Get(t.Context(), key); err != nil || string(entryAfter.Value) != string(entry.Value) {
		t.Fatal("older delivery replaced the completed high-water mark")
	}
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 12, map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Carol Fixture", "formation_lead_email": " newer@example.com ", "systemmodstamp": "2026-11-11T00:00:03Z", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(1), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": true, "email": true, "avatar": false}}, 13, nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "later-bridge@example.com"}, SystemModStamp: "2026-11-11T00:00:04Z"}, nil
	}
	if err := reconcileFormationLead(t.Context(), key); err == nil {
		t.Fatal("human KV row preceding a newer live reverse PATCH must remain deferred")
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Name: "Carol Fixture", Email: " newer@example.com "}, SystemModStamp: "2026-11-11T00:00:03Z"}, nil
	}
	humanWrites := 0
	applyNewerHumanFormation = func(_ context.Context, payload *projectservice.UpdateProjectSettingsPayload) error {
		humanWrites++
		if payload.FormationLead == nil || stringPtrToString(payload.FormationLead.Email) != "newer@example.com" ||
			stringPtrToString(payload.FormationLead.Name) != "Carol Fixture" {
			t.Errorf("newer human lead = %+v, want both cumulative name and email edits despite latest mask being email-only", payload.FormationLead)
		}
		return nil
	}
	if err := reconcileFormationLead(t.Context(), key); err != nil || humanWrites != 1 || patches != 2 {
		t.Fatalf("newer human reconciliation error=%v humanWrites=%d v1Patches=%d", err, humanWrites, patches)
	}
	finalEntry, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var finalMarker formationReconcileObligation
	if err := json.Unmarshal(finalEntry.Value, &finalMarker); err != nil || finalMarker.Completed || finalMarker.ProcessedHumanRev != 13 {
		t.Errorf("partial human edit must retain repair marker at processed revision 13: marker=%+v error=%v", finalMarker, err)
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{
			Name: stringToStringPtr("Carol Fixture"), Email: stringToStringPtr("newer@example.com"),
		}}, "2", nil
	}
	if err := reconcileFormationLead(t.Context(), key); err != nil || patches != 3 {
		t.Fatalf("second pass did not repair v1 after partial human merge: err=%v patches=%d", err, patches)
	}
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "test-client-id@clients", "formation_lead_name": "Carol Fixture", "formation_lead_email": "newer@example.com", "systemmodstamp": "2026-11-11T00:00:05Z", formationLeadVersionsKey: map[string]any{"name": float64(1), "email": float64(1), "avatar": float64(0)}}, 14, nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Name: "Carol Fixture", Email: "newer@example.com"}, SystemModStamp: "2026-11-11T00:00:05Z"}, nil
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	if err := reconcileFormationLead(t.Context(), key); err != nil {
		t.Fatalf("second repair WAL echo was not confirmed: %v", err)
	}
	finalEntry, err = store.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(finalEntry.Value, &finalMarker); err != nil || !finalMarker.Completed || finalMarker.SourceRev != 14 || finalMarker.ProcessedHumanRev != 13 {
		t.Errorf("second-pass marker = %+v, error %v; want completed observed 14, processed human 13", finalMarker, err)
	}
	if older, err := olderFormationLeadDelivery(t.Context(), "test-uid", 11); err != nil || !older {
		t.Errorf("redelivery 11 was not rejected after accepting revision 12: older=%t err=%v", older, err)
	}
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10); err != nil {
		t.Fatal(err)
	}
	rearmed, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var later formationReconcileObligation
	if err := json.Unmarshal(rearmed.Value, &later); err != nil || later.Completed || later.SourceRev != 14 || !later.Due.After(now) {
		t.Errorf("uncertain reverse PATCH did not re-arm high-water marker: %+v err=%v", later, err)
	}
	// Clearing the last populated v1 contact field must send an explicit
	// empty string, not a nil pointer that hydration interprets as preserve.
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "systemmodstamp": "2026-11-11T00:00:06Z", formationLeadChangesKey: []any{"email"}, formationLeadVersionsKey: map[string]any{"name": float64(1), "email": float64(2), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": true, "email": true, "avatar": false}}, 15, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("alice@example.com")}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{}, SystemModStamp: "2026-11-11T00:00:06Z"}, nil
	}
	cleared := false
	applyNewerHumanFormation = func(_ context.Context, payload *projectservice.UpdateProjectSettingsPayload) error {
		cleared = payload.FormationLead != nil && payload.FormationLead.Email != nil && *payload.FormationLead.Email == ""
		return nil
	}
	if err := reconcileFormationLead(t.Context(), key); err != nil || !cleared {
		t.Errorf("last contact field clear lost: err=%v explicitEmptyEmail=%t", err, cleared)
	}
}

func TestRearmFormationLeadReconcile_CompletedBaseline(t *testing.T) {
	origStore := mappingStore
	store := newFakeMappingStore()
	mappingStore = store
	t.Cleanup(func() { mappingStore = origStore })
	const key = formationReconcilePrefix + "test-uid"
	if err := markFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 1, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var marker formationReconcileObligation
	if err := json.Unmarshal(entry.Value, &marker); err != nil {
		t.Fatal(err)
	}
	marker.Completed = true
	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(t.Context(), key, data, entry.Revision); err != nil {
		t.Fatal(err)
	}
	// An intervening human email edit was already synced before this
	// reverse dispatch. Its generation belongs in the new baseline.
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 14,
		map[string]uint64{"name": 0, "email": 2, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	entry, err = store.Get(t.Context(), key)
	marker = formationReconcileObligation{}
	if err != nil || json.Unmarshal(entry.Value, &marker) != nil || marker.Completed ||
		marker.SourceRev != 14 || marker.ProcessedHumanRev != 10 || marker.FieldVersions["email"] != 2 {
		t.Fatalf("rearmed marker must retain processed revision and fresh dispatch baseline: %+v err=%v", marker, err)
	}
}

func TestReconcileFormationLead_LateBridgeEchoThenRename(t *testing.T) {
	origStore, origCfg, origWrites := mappingStore, cfg, recentV1StaffWrites
	origNow, origRead := formationReconcileNow, readLatestV1ProjectRow
	origSettings, origLive, origPatch, origHuman := fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation
	t.Cleanup(func() {
		mappingStore, cfg, recentV1StaffWrites = origStore, origCfg, origWrites
		formationReconcileNow, readLatestV1ProjectRow = origNow, origRead
		fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation = origSettings, origLive, origPatch, origHuman
	})
	mappingStore = newFakeMappingStore()
	cfg = &Config{Auth0ClientID: "test-client-id"}
	recentV1StaffWrites = newV1StaffWriteLog()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	baseline := map[string]uint64{"name": 0, "email": 1, "avatar": 0}
	if err := markFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 9, baseline); err != nil {
		t.Fatal(err)
	}
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10, baseline); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "formation_lead_email": "stale@example.com", "systemmodstamp": "2026-11-11T00:00:02Z", formationLeadChangesKey: []any{}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}}, 12, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("new@example.com")}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "stale@example.com"}, SystemModStamp: "2026-11-11T00:00:02Z"}, nil
	}
	humanWrites, repairs := 0, 0
	applyNewerHumanFormation = func(context.Context, *projectservice.UpdateProjectSettingsPayload) error {
		humanWrites++
		return nil
	}
	patchDeferredFormationLead = func(_ context.Context, _ string, payload any) error {
		repairs++
		if !strings.Contains(fmt.Sprint(payload), "new@example.com") {
			t.Errorf("repair payload %v does not restore newer v2 contact", payload)
		}
		return nil
	}
	if err := reconcileFormationLead(t.Context(), formationReconcilePrefix+"test-uid"); err != nil || humanWrites != 0 || repairs != 1 {
		t.Errorf("base-only rename after late bridge echo must repair v1, not revert v2: err=%v humanWrites=%d repairs=%d", err, humanWrites, repairs)
	}
	// This time a PCC email edit advanced the generation before the late
	// bridge PATCH overwrote its value. A subsequent rename must not treat
	// the bridge's current stale value as that human's original intent.
	mappingStore = newFakeMappingStore()
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "formation_lead_email": "stale@example.com", "systemmodstamp": "2026-11-11T00:00:03Z", formationLeadChangesKey: []any{}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": false, "avatar": false}}, 13, nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "stale@example.com"}, SystemModStamp: "2026-11-11T00:00:03Z"}, nil
	}
	if err := reconcileFormationLead(t.Context(), formationReconcilePrefix+"test-uid"); err == nil || humanWrites != 0 || repairs != 1 {
		t.Errorf("overwritten human contact must remain pending, without marking stale bridge value processed: err=%v humanWrites=%d repairs=%d", err, humanWrites, repairs)
	}
	entry, err := mappingStore.Get(t.Context(), formationReconcilePrefix+"test-uid")
	var marker formationReconcileObligation
	if err != nil || json.Unmarshal(entry.Value, &marker) != nil || marker.ProcessedHumanRev != 0 || marker.Completed {
		t.Errorf("ambiguous human contact was acknowledged: marker=%+v err=%v", marker, err)
	}
}

func TestReconcileFormationLead_InitialEmptyContactAfterRename(t *testing.T) {
	origStore, origCfg, origWrites := mappingStore, cfg, recentV1StaffWrites
	origNow, origRead := formationReconcileNow, readLatestV1ProjectRow
	origSettings, origLive, origPatch := fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead
	t.Cleanup(func() {
		mappingStore, cfg, recentV1StaffWrites = origStore, origCfg, origWrites
		formationReconcileNow, readLatestV1ProjectRow = origNow, origRead
		fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead = origSettings, origLive, origPatch
	})
	mappingStore = newFakeMappingStore()
	cfg = &Config{Auth0ClientID: "test-client-id"}
	recentV1StaffWrites = newV1StaffWriteLog()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	if err := retainUncertainFormationLeadWrite(t.Context(), "test-uid", "test-sfid", 0); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "name": "Renamed Project", "systemmodstamp": "2026-11-11T00:00:02Z", formationLeadChangesKey: []any{}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(0), "avatar": float64(0)}}, 12, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("new@example.com")}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{}, SystemModStamp: "2026-11-11T00:00:02Z"}, nil
	}
	repairs := 0
	patchDeferredFormationLead = func(_ context.Context, _ string, payload any) error {
		repairs++
		if !strings.Contains(fmt.Sprint(payload), "new@example.com") {
			t.Errorf("initial repair did not use v2 contact: %v", payload)
		}
		return nil
	}
	if err := reconcileFormationLead(t.Context(), formationReconcilePrefix+"test-uid"); err != nil || repairs != 1 {
		t.Fatalf("initial empty v1 contact after rename should be repaired: err=%v patches=%d", err, repairs)
	}
	entry, err := mappingStore.Get(t.Context(), formationReconcilePrefix+"test-uid")
	var marker formationReconcileObligation
	if err != nil || json.Unmarshal(entry.Value, &marker) != nil || marker.Completed {
		t.Fatalf("initial repair should await WAL echo: marker=%+v err=%v", marker, err)
	}
	// An actual contact edit before baseline establishment cannot be
	// mistaken for an empty initial v1 contact.
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", "systemmodstamp": "2026-11-11T00:00:03Z", formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}}, 13, nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "human@example.com"}, SystemModStamp: "2026-11-11T00:00:03Z"}, nil
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	if err := reconcileFormationLead(t.Context(), formationReconcilePrefix+"test-uid"); err == nil || repairs != 1 {
		t.Errorf("initial marker without a human baseline must retain later contact edit: err=%v patches=%d", err, repairs)
	}
}

func TestReconcileFormationLead_FailedForwardContactAfterRetries(t *testing.T) {
	origStore, origCfg, origWrites := mappingStore, cfg, recentV1StaffWrites
	origNow, origRead := formationReconcileNow, readLatestV1ProjectRow
	origSettings, origLive, origPatch, origHuman := fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation
	t.Cleanup(func() {
		mappingStore, cfg, recentV1StaffWrites = origStore, origCfg, origWrites
		formationReconcileNow, readLatestV1ProjectRow = origNow, origRead
		fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation = origSettings, origLive, origPatch, origHuman
	})
	mappingStore = newFakeMappingStore()
	cfg = &Config{Auth0ClientID: "test-client-id"}
	recentV1StaffWrites = newV1StaffWriteLog()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	// This is the identifier-only pre-edit baseline retained after all
	// JetStream deliveries of the failed v2 settings write have expired.
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "name": "Renamed Project", "formation_lead_email": "human@example.com", "systemmodstamp": "2026-11-11T00:00:02Z", formationLeadChangesKey: []any{}, formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": true, "avatar": false}}, 11, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("old@example.com")}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "human@example.com"}, SystemModStamp: "2026-11-11T00:00:02Z"}, nil
	}
	humanWrites, reversePatches := 0, 0
	applyNewerHumanFormation = func(_ context.Context, payload *projectservice.UpdateProjectSettingsPayload) error {
		humanWrites++
		if payload.FormationLead == nil || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "human@example.com" {
			t.Errorf("durable forward repair did not apply PCC contact: %+v", payload.FormationLead)
		}
		return nil
	}
	patchDeferredFormationLead = func(context.Context, string, any) error { reversePatches++; return nil }
	if err := reconcileFormationLead(t.Context(), formationReconcilePrefix+"test-uid"); err != nil || humanWrites != 1 || reversePatches != 0 {
		t.Errorf("forward repair after retry exhaustion: err=%v humanWrites=%d reversePatches=%d", err, humanWrites, reversePatches)
	}
	marker, found, err := formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.Completed || marker.ProcessedHumanRev != 11 {
		t.Errorf("forward repair did not retain processed watermark: marker=%+v found=%t err=%v", marker, found, err)
	}
}

func TestReconcileFormationLead_QueuedHumanBeforeReverseSnapshot(t *testing.T) {
	origStore, origCfg, origWrites := mappingStore, cfg, recentV1StaffWrites
	origNow, origRead := formationReconcileNow, readLatestV1ProjectRow
	origSettings, origLive, origPatch, origHuman := fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation
	t.Cleanup(func() {
		mappingStore, cfg, recentV1StaffWrites = origStore, origCfg, origWrites
		formationReconcileNow, readLatestV1ProjectRow = origNow, origRead
		fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation = origSettings, origLive, origPatch, origHuman
	})
	store := newFakeMappingStore()
	mappingStore = store
	cfg = &Config{Auth0ClientID: "test-client-id"}
	recentV1StaffWrites = newV1StaffWriteLog()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	const key = formationReconcilePrefix + "test-uid"
	if err := markFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 9,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(t.Context(), key)
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
	if _, err := store.Update(t.Context(), key, encoded, entry.Revision); err != nil {
		t.Fatal(err)
	}
	// An email edit is replicated but still queued for v2 when an avatar
	// reverse PATCH snapshots its generation. An unrelated ED/PM PATCH
	// makes the latest project row service-authored without touching email.
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 1, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	currentHuman := true
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "test-client-id@clients", "formation_lead_email": "human@example.com", "systemmodstamp": "2026-11-11T00:00:02Z", formationLeadVersionsKey: map[string]any{"name": float64(0), "email": float64(1), "avatar": float64(0)}, formationLeadCurrentHumanKey: map[string]any{"name": false, "email": currentHuman, "avatar": false}}, 11, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("old@example.com")}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "human@example.com"}, SystemModStamp: "2026-11-11T00:00:02Z"}, nil
	}
	humanWrites, reversePatches := 0, 0
	applyNewerHumanFormation = func(_ context.Context, payload *projectservice.UpdateProjectSettingsPayload) error {
		humanWrites++
		if payload.FormationLead == nil || payload.FormationLead.Email == nil || *payload.FormationLead.Email != "human@example.com" {
			t.Errorf("queued email not applied to v2: %+v", payload.FormationLead)
		}
		return nil
	}
	patchDeferredFormationLead = func(context.Context, string, any) error { reversePatches++; return nil }
	if err := reconcileFormationLead(t.Context(), key); err != nil || humanWrites != 1 || reversePatches != 0 {
		t.Errorf("service-authored ED/PM row must not hide queued PCC email: err=%v humanWrites=%d reversePatches=%d", err, humanWrites, reversePatches)
	}
	marker, found, err := formationLeadObligation(t.Context(), "test-uid")
	if err != nil || !found || marker.ProcessedFields["email"] != 1 {
		t.Errorf("processed email generation not retained: marker=%+v found=%t err=%v", marker, found, err)
	}
	// An existing nonzero replica generation with no prior processed
	// watermark is ambiguous: it may be queued or already synced.
	mappingStore = newFakeMappingStore()
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 1, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	if err := reconcileFormationLead(t.Context(), key); err == nil || humanWrites != 1 || reversePatches != 0 {
		t.Errorf("unknown processed history must fail closed: err=%v humanWrites=%d reversePatches=%d", err, humanWrites, reversePatches)
	}
	// If that reverse PATCH had overwritten email, the generation alone
	// does not recover the human's value. Leave the marker unresolved.
	mappingStore = newFakeMappingStore()
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	currentHuman = false
	if err := reconcileFormationLead(t.Context(), key); err == nil || humanWrites != 1 || reversePatches != 0 {
		t.Errorf("overwritten PCC email must not be repaired from stale v2: err=%v humanWrites=%d reversePatches=%d", err, humanWrites, reversePatches)
	}
}

func TestReconcileFormationLead_DeletedProjectMarker(t *testing.T) {
	origStore, origCfg, origNow, origRead := mappingStore, cfg, formationReconcileNow, readLatestV1ProjectRow
	t.Cleanup(func() {
		mappingStore, cfg, formationReconcileNow, readLatestV1ProjectRow = origStore, origCfg, origNow, origRead
	})
	store := newFakeMappingStore()
	mappingStore = store
	cfg = &Config{}
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	if err := rearmFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10,
		map[string]uint64{"name": 0, "email": 0, "avatar": 0}); err != nil {
		t.Fatal(err)
	}
	if err := tombstoneMapping(t.Context(), "project.sfid.test-sfid"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		t.Fatal("deleted project marker must not query the v1 project")
		return nil, 0, nil
	}
	key := formationReconcilePrefix + "test-uid"
	if err := reconcileFormationLead(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(t.Context(), key)
	if err != nil || !isTombstonedMapping(entry.Value) {
		t.Errorf("orphaned marker was not tombstoned: entry=%+v err=%v", entry, err)
	}
	if err := reconcileFormationLead(t.Context(), key); err != nil {
		t.Errorf("tombstoned marker was retried: %v", err)
	}
}

type formationGetSignalStore struct {
	MappingStore
	firstGet chan struct{}
	gets     atomic.Int32
}

func (s *formationGetSignalStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	entry, err := s.MappingStore.Get(ctx, key)
	if strings.HasPrefix(key, formationReconcilePrefix) && s.gets.Add(1) == 1 {
		close(s.firstGet)
	}
	return entry, err
}

func TestReconcileFormationLead_RearmedWhileWaitingForLock(t *testing.T) {
	origStore, origNow, origCfg := mappingStore, formationReconcileNow, cfg
	defer func() { mappingStore, formationReconcileNow, cfg = origStore, origNow, origCfg }()
	store := newFakeMappingStore()
	mappingStore = store
	cfg = &Config{}
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	const uid, sfid = "test-uid", "test-sfid"
	if err := markFormationLeadReconcile(t.Context(), uid, sfid, 10); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	signalStore := &formationGetSignalStore{MappingStore: store, firstGet: make(chan struct{})}
	mappingStore = signalStore
	unlock := lockFormationLeadProject(uid)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- reconcileFormationLead(t.Context(), formationReconcilePrefix+uid) }()
	<-signalStore.firstGet // Worker read the old due time and is waiting for lock.
	if err := rearmFormationLeadReconcile(t.Context(), uid, sfid, 10); err != nil {
		t.Fatal(err)
	}
	unlock()
	unlock = nil
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(t.Context(), formationReconcilePrefix+uid)
	var marker formationReconcileObligation
	if err != nil || json.Unmarshal(entry.Value, &marker) != nil || marker.Completed || !marker.Due.After(now) {
		t.Errorf("rearmed marker was completed or due before uncertainty elapsed: marker=%+v err=%v", marker, err)
	}
}

func TestParseFormationStamp(t *testing.T) {
	for _, value := range []any{
		"2026-11-11T00:00:00Z", "2026-11-11T00:00:00.123456Z",
		"2026-11-11 00:00:00.123456", "2026-11-11T00:00:00.123456",
	} {
		if _, ok := parseFormationStamp(value); !ok {
			t.Errorf("valid timestamp %v was rejected", value)
		}
	}
	for _, value := range []any{"", nil, 12, "invalid"} {
		if _, ok := parseFormationStamp(value); ok {
			t.Errorf("invalid timestamp %v was accepted", value)
		}
	}
}

func TestReconcileFormationLead_BaseOnlyRetainsAmbiguousMarker(t *testing.T) {
	origStore, origCfg, origWrites := mappingStore, cfg, recentV1StaffWrites
	origNow, origRead := formationReconcileNow, readLatestV1ProjectRow
	origSettings, origLive, origPatch, origHuman := fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation
	t.Cleanup(func() {
		mappingStore, cfg, recentV1StaffWrites = origStore, origCfg, origWrites
		formationReconcileNow, readLatestV1ProjectRow = origNow, origRead
		fetchDeferredFormationSettings, fetchLiveFormationLeadState, patchDeferredFormationLead, applyNewerHumanFormation = origSettings, origLive, origPatch, origHuman
	})
	mappingStore = newFakeMappingStore()
	cfg = &Config{Auth0ClientID: "test-client-id"}
	recentV1StaffWrites = newV1StaffWriteLog()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	formationReconcileNow = func() time.Time { return now }
	const key = formationReconcilePrefix + "test-uid"
	if err := markFormationLeadReconcile(t.Context(), "test-uid", "test-sfid", 10); err != nil {
		t.Fatal(err)
	}
	now = now.Add(v1StaffWriteFreshness + time.Second)
	readLatestV1ProjectRow = func(context.Context, string) (map[string]any, uint64, error) {
		return map[string]any{"sfid": "test-sfid", "lastmodifiedbyid": "human-editor", "formation_lead_email": "old@example.com", "systemmodstamp": "2026-11-11T00:00:01Z", formationLeadChangesKey: []any{}}, 11, nil
	}
	fetchDeferredFormationSettings = func(context.Context, string) (*projectservice.ProjectSettings, string, error) {
		return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("new@example.com")}}, "1", nil
	}
	fetchLiveFormationLeadState = func(context.Context, string) (*liveFormationLeadState, error) {
		return &liveFormationLeadState{Lead: &events.FormationLead{Email: "old@example.com"}, SystemModStamp: "2026-11-11T00:00:01Z"}, nil
	}
	humanWrites, repairs := 0, 0
	applyNewerHumanFormation = func(context.Context, *projectservice.UpdateProjectSettingsPayload) error {
		humanWrites++
		return nil
	}
	patchDeferredFormationLead = func(context.Context, string, any) error { repairs++; return nil }
	if err := reconcileFormationLead(t.Context(), key); err == nil || humanWrites != 0 || repairs != 0 {
		t.Errorf("base-only row must retain ambiguous marker without writes: err=%v humanWrites=%d repairs=%d", err, humanWrites, repairs)
	}
	entry, err := mappingStore.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var marker formationReconcileObligation
	if err := json.Unmarshal(entry.Value, &marker); err != nil || marker.Completed {
		t.Errorf("ambiguous marker was completed or lost: marker=%+v err=%v", marker, err)
	}
}
