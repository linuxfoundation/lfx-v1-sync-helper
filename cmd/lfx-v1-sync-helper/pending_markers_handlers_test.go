// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
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

	cfg = &Config{Auth0ClientID: "test-client-id"}
	mappingStore = fake
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Cleanup(func() {
		cfg = origCfg
		mappingStore = origStore
		logger = origLogger
		fetchProjectBase = origFetchProject
		fetchCommitteeBase = origFetchCommittee
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
