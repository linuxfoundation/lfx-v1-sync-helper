// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"
)

// setupPendingMarkerTest swaps in a fakeMappingStore + discard logger
// for the package-level globals and returns the store for assertions.
// Package-level globals are restored via t.Cleanup.
func setupPendingMarkerTest(t *testing.T) *fakeMappingStore {
	t.Helper()
	fake := newFakeMappingStore()
	origStore := mappingStore
	origLogger := logger
	mappingStore = fake
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Cleanup(func() {
		mappingStore = origStore
		logger = origLogger
	})
	return fake
}

func TestPendingMarkerKey(t *testing.T) {
	// Golden format assertion — changing this breaks in-flight
	// markers left by an older pod during a rolling deploy, so pin it.
	got := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "abc-123")
	want := "pending.v1_to_v2.update.project.abc-123"
	if got != want {
		t.Errorf("pendingMarkerKey = %q, want %q", got, want)
	}

	got = pendingMarkerKey(markerV2ToV1, markerOpDelete, markerResourceCommitteeMember, "u-42")
	want = "pending.v2_to_v1.delete.committee_member.u-42"
	if got != want {
		t.Errorf("pendingMarkerKey (member/delete) = %q, want %q", got, want)
	}

	// v2_to_v1.delete markers key on the v1 SFID, not the v2 UID — this
	// is the fix for the delete-marker leak (PR #170 review, thread 3).
	// Pin the format to catch anyone accidentally reverting to a UID key.
	got = pendingMarkerKey(markerV2ToV1, markerOpDelete, markerResourceProject, "a000000000AAA")
	want = "pending.v2_to_v1.delete.project.a000000000AAA"
	if got != want {
		t.Errorf("pendingMarkerKey (project/v2_to_v1/delete, SFID key) = %q, want %q", got, want)
	}
}

func TestConsumePendingMarker_MissingReturnsFalse(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if got {
		t.Errorf("consumePendingMarker on empty store = true, want false")
	}
	// Missing key must not touch delete counter.
	if fake.deleteCalls != 0 {
		t.Errorf("Delete called %d times on missing marker; want 0", fake.deleteCalls)
	}
}

func TestConsumePendingMarker_EmptyUIDReturnsFalse(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "")
	if got {
		t.Errorf("consumePendingMarker with empty uid = true, want false")
	}
	// Must not have touched the store at all.
	if fake.getCalls != 0 || fake.deleteCalls != 0 {
		t.Errorf("empty-uid call touched store: gets=%d deletes=%d", fake.getCalls, fake.deleteCalls)
	}
}

func TestConsumePendingMarker_FreshReturnsTrueAndDeletes(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if _, err := fake.Get(context.Background(), key); err != nil {
		t.Fatalf("marker was not written: %v", err)
	}

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if !got {
		t.Errorf("consumePendingMarker on fresh marker = false, want true")
	}
	// Marker should now be deleted so a later legitimate update is not
	// suppressed.
	if _, err := fake.Get(context.Background(), key); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("marker was not deleted after consume: err=%v", err)
	}
}

func TestConsumePendingMarker_StaleReturnsFalseAndDeletes(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	// Write a marker with a timestamp older than the freshness window.
	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-stale")
	staleTs := time.Now().Add(-2 * pendingMarkerFreshness).Unix()
	if _, err := fake.Put(context.Background(), key, []byte(strconv.FormatInt(staleTs, 10))); err != nil {
		t.Fatalf("seed stale marker: %v", err)
	}

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-stale")
	if got {
		t.Errorf("consumePendingMarker on stale marker = true, want false (must not suppress a genuine event past the freshness window)")
	}
	// Stale marker must still be cleaned up.
	if _, err := fake.Get(context.Background(), key); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("stale marker was not garbage-collected on read: err=%v", err)
	}
}

func TestConsumePendingMarker_UnparseableTimestampReturnsFalseAndDeletes(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-junk")
	if _, err := fake.Put(context.Background(), key, []byte("not-a-timestamp")); err != nil {
		t.Fatalf("seed junk marker: %v", err)
	}

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-junk")
	if got {
		t.Errorf("consumePendingMarker with unparseable value = true, want false")
	}
	if _, err := fake.Get(context.Background(), key); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("junk marker was not garbage-collected on read: err=%v", err)
	}
}

func TestConsumePendingMarker_StoreErrorFailsOpen(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	sentinel := errors.New("kv boom")
	fake.nextGetErr = sentinel

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-x")
	if got {
		t.Errorf("consumePendingMarker on Get error = true; want false (fail-open — a redundant PATCH is better than a lost event)")
	}
	// Do not attempt Delete when Get failed — we don't know if the key
	// even exists.
	if fake.deleteCalls != 0 {
		t.Errorf("Delete called %d times after Get error; want 0", fake.deleteCalls)
	}
}

// TestConsumePendingMarker_DeleteFailureOnFreshMarkerFailsOpen pins
// the PR #170 T7 fix: when Delete fails on a fresh marker, return
// false so a subsequent legitimate event within the freshness window
// cannot also match and be silenced. Trade: one redundant PATCH on
// this event; never a silent event drop.
func TestConsumePendingMarker_DeleteFailureOnFreshMarkerFailsOpen(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")

	sentinel := errors.New("kv delete boom")
	fake.nextDeleteErr = sentinel

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if got {
		t.Errorf("consumePendingMarker returned true when Delete failed on a fresh marker; want false (fail-open — otherwise the marker stays live and silences the next legitimate event)")
	}
	// Marker must still be present (Delete failed).
	if _, err := fake.Get(context.Background(), key); err != nil {
		t.Errorf("marker unexpectedly gone after failed Delete: err=%v", err)
	}
}

func TestWritePendingMarker_EmptyUIDIsNoop(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "")
	if fake.putCalls != 0 {
		t.Errorf("Put called %d times for empty uid; want 0", fake.putCalls)
	}
}

func TestWritePendingMarker_UnconditionalPutRefreshesTimestamp(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	// Seed a stale marker.
	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	staleTs := time.Now().Add(-2 * pendingMarkerFreshness).Unix()
	if _, err := fake.Put(context.Background(), key, []byte(strconv.FormatInt(staleTs, 10))); err != nil {
		t.Fatalf("seed stale marker: %v", err)
	}

	// Rewrite via writePendingMarker — should overwrite with a fresh
	// timestamp, not leave the stale one in place.
	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")

	got := consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if !got {
		t.Errorf("consumePendingMarker after refresh = false, want true (rewrite must overwrite stale timestamp)")
	}
}

func TestWritePendingMarker_StoreErrorSwallowedAsBestEffort(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	sentinel := errors.New("kv boom")
	fake.nextPutErr = sentinel

	// writePendingMarker returns nothing — assert it does not panic and
	// that no marker was persisted.
	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")

	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if _, err := fake.Get(context.Background(), key); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("marker persisted after failed Put: err=%v", err)
	}
}

// TestPendingMarker_DirectionScopingIsolation confirms that a v1→v2
// marker is not consumed by a v2→v1 read (or vice versa). This is the
// core property that prevents cross-direction races (e.g. a v1 delete
// marker suppressing a v2 update indexer event).
func TestPendingMarker_DirectionScopingIsolation(t *testing.T) {
	setupPendingMarkerTest(t)

	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-shared")

	// Consuming with the opposite direction must NOT find the marker.
	if consumePendingMarker(context.Background(), markerV2ToV1, markerOpUpdate, markerResourceProject, "uid-shared") {
		t.Errorf("v2_to_v1 consume found a v1_to_v2 marker — direction scoping broken")
	}
	// Original marker should still be present.
	if !consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-shared") {
		t.Errorf("v1_to_v2 consume missed its own marker after opposite-direction miss")
	}
}

// TestPendingMarker_OpAndResourceScopingIsolation confirms that a
// project-update marker is not consumed by a project-delete read, nor
// by a committee-update read. Combined with the direction test above
// this pins the full (direction, op, resource, uid) namespacing
// contract.
func TestPendingMarker_OpAndResourceScopingIsolation(t *testing.T) {
	setupPendingMarkerTest(t)

	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-shared")

	// Wrong op.
	if consumePendingMarker(context.Background(), markerV1ToV2, markerOpDelete, markerResourceProject, "uid-shared") {
		t.Errorf("delete consume found an update marker — op scoping broken")
	}
	// Wrong resource.
	if consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceCommittee, "uid-shared") {
		t.Errorf("committee consume found a project marker — resource scoping broken")
	}
	// Wrong UID.
	if consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-other") {
		t.Errorf("uid=other consume found the uid=shared marker — uid scoping broken")
	}
	// Original marker must still be there.
	if !consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-shared") {
		t.Errorf("original marker was consumed by one of the scoped-miss reads")
	}
}

// TestDeletePendingMarker_RemovesLiveMarker covers the update-path
// no-op cleanup case: handler wrote a marker, updateProject /
// updateCommittee / updateCommitteeMember reported no mutation, and
// the handler must remove the dangling marker so a genuine v2 update
// within the freshness window is not silenced.
func TestDeletePendingMarker_RemovesLiveMarker(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	writePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
	if _, err := fake.Get(context.Background(), key); err != nil {
		t.Fatalf("marker not written: %v", err)
	}

	deletePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")

	if _, err := fake.Get(context.Background(), key); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("marker still present after deletePendingMarker: err=%v", err)
	}

	// A subsequent consume must therefore NOT suppress — the update
	// no-op did not silence the next genuine event.
	if consumePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1") {
		t.Errorf("consumePendingMarker returned true after deletePendingMarker; marker was not actually removed")
	}
}

func TestDeletePendingMarker_MissingIsNoop(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	// Nothing seeded — must not blow up.
	deletePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-missing")
	if fake.deleteCalls != 1 {
		t.Errorf("Delete calls = %d, want 1 (Delete is called unconditionally; store treats missing key as a no-op)", fake.deleteCalls)
	}
}

func TestDeletePendingMarker_EmptyIdentifierIsNoop(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	deletePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "")
	if fake.deleteCalls != 0 {
		t.Errorf("Delete called %d times for empty identifier; want 0", fake.deleteCalls)
	}
}

func TestDeletePendingMarker_StoreErrorSwallowed(t *testing.T) {
	fake := setupPendingMarkerTest(t)

	sentinel := errors.New("kv boom")
	fake.nextDeleteErr = sentinel

	// Must not panic — the freshness window still bounds any lingering
	// marker so this is intentionally best-effort.
	deletePendingMarker(context.Background(), markerV1ToV2, markerOpUpdate, markerResourceProject, "uid-1")
}
