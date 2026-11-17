// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

type memberCleanupFaultStore struct {
	MappingStore
	key    string
	value  string
	failed bool
}

func (s *memberCleanupFaultStore) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !s.failed && key == s.key && strings.Contains(string(value), s.value) {
		s.failed = true
		return 0, errors.New("synthetic cleanup persistence outage")
	}
	return s.MappingStore.Update(ctx, key, value, revision)
}

func seedReplacementMember(t *testing.T, h *syncTestHarness) {
	t.Helper()
	h.seed(t, "committee_member.sfid.replacement-record", "committee-uid:replacement-uid")
	h.seed(t, "committee_member.uid.replacement-uid", "project-sfid:committee-sfid:contact-sfid")
	h.seed(t, committeeMemberRecordSFIDKey("replacement-uid"), "replacement-record")
}

func assertReplacementMember(t *testing.T, h *syncTestHarness) {
	t.Helper()
	h.assertMapping(t, "committee_member.sfid.replacement-record", "committee-uid:replacement-uid")
	h.assertMapping(t, "committee_member.uid.replacement-uid", "project-sfid:committee-sfid:contact-sfid")
	h.assertMapping(t, committeeMemberRecordSFIDKey("replacement-uid"), "replacement-record")
}

func TestMemberCleanupReplayPreservesReplacement(t *testing.T) {
	for _, window := range []string{"complete", "legacy complete", "partial tombstones", "completion write fails", "confirmed result write fails"} {
		t.Run(window, func(t *testing.T) {
			h := setupMemberLifecycle(t)
			v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
			if err := createLifecycleMember(t); err != nil {
				t.Fatal(err)
			}
			switch window {
			case "partial tombstones":
				mappingStore = &memberCleanupFaultStore{MappingStore: h.store, key: committeeMemberRecordSFIDKey("member-uid"), value: tombstoneMarker}
			case "completion write fails":
				mappingStore = &memberCleanupFaultStore{MappingStore: h.store, key: memberCleanupKey("member-uid"), value: `"complete"`}
			case "confirmed result write fails":
				mappingStore = &memberCleanupFaultStore{MappingStore: h.store, key: memberCleanupKey("member-uid"), value: `"deleted"`}
			}
			err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil)
			if window == "complete" || window == "legacy complete" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("persistence failure must not report success")
			}
			if window == "legacy complete" {
				if err := h.store.Delete(t.Context(), memberCleanupKey("member-uid")); err != nil {
					t.Fatal(err)
				}
			}
			seedReplacementMember(t, h)
			before := len(h.recordedRequests())
			err = lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil)
			if window == "confirmed result write fails" {
				if !errors.Is(err, errCommitteeCreatePending) {
					t.Fatalf("uncertain persistence = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if len(h.recordedRequests()) != before {
				t.Fatal("replay sent another contact-addressed DELETE")
			}
			assertReplacementMember(t, h)
			if window != "confirmed result write fails" {
				h.assertMapping(t, "committee_member.uid.member-uid", tombstoneMarker)
				h.assertMapping(t, committeeMemberRecordSFIDKey("member-uid"), tombstoneMarker)
			}
		})
	}
}

type lostMemberDeleteResponseTransport struct{ base http.RoundTripper }

func (r lostMemberDeleteResponseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(req)
	if err == nil && req.Method == http.MethodDelete {
		_ = response.Body.Close()
		return nil, errors.New("synthetic committed DELETE response loss")
	}
	return response, err
}

func TestMemberCleanupLostResponseNeverRepeatsDelete(t *testing.T) {
	h := setupMemberLifecycle(t)
	v1HTTPClient.Transport = lostMemberDeleteResponseTransport{base: &lifecycleTransport{base: v1HTTPClient.Transport}}
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatal(err)
	}
	seedReplacementMember(t, h)
	for i := 0; i < 3; i++ {
		if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); !errors.Is(err, errCommitteeCreatePending) {
			t.Fatal(err)
		}
	}
	if requests := h.recordedRequests(); len(requests) != 2 || requests[1].Method != http.MethodDelete {
		t.Fatalf("requests = %+v", requests)
	}
	assertReplacementMember(t, h)
}

type concurrentMemberCleanupTransport struct {
	base         http.RoundTripper
	duringDelete func()
}

func (r concurrentMemberCleanupTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete {
		r.duringDelete()
	}
	return r.base.RoundTrip(req)
}

func TestMemberCleanupConcurrentDeliveryHasOneDeleteOwner(t *testing.T) {
	h := setupMemberLifecycle(t)
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	v1HTTPClient.Transport = concurrentMemberCleanupTransport{
		base: &lifecycleTransport{base: v1HTTPClient.Transport},
		duringDelete: func() {
			if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); !errors.Is(err, errCommitteeCreatePending) {
				t.Errorf("concurrent cleanup = %v, want pending", err)
			}
		},
	}
	if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 2 {
		t.Fatal("concurrent cleanup repeated DELETE")
	}
	h.assertMapping(t, "committee_member.uid.member-uid", tombstoneMarker)
}

func TestMemberCleanupDispatchRequiresDurableFence(t *testing.T) {
	h := setupMemberLifecycle(t)
	v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	if err := recordCreateDeletion(t.Context(), "committee_member", "member-uid"); err != nil {
		t.Fatal(err)
	}
	result, _, err := readMemberCreateResult(t.Context(), "member-uid")
	if err != nil {
		t.Fatal(err)
	}
	h.store.nextCreateErr = errors.New("synthetic dispatch fence write failure")
	if err := cleanupRecordedMemberCreate(t.Context(), "member-uid", result); err == nil {
		t.Fatal("dispatch persistence failure must retry")
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("DELETE was sent without a dispatch fence")
	}
	if err := cleanupRecordedMemberCreate(t.Context(), "member-uid", result); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 2 {
		t.Fatal("pre-dispatch storage failure did not retry safely")
	}
}

type memberCleanupOwnershipRaceStore struct {
	MappingStore
	raced bool
}

func (s *memberCleanupOwnershipRaceStore) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !s.raced && key == "committee_member.sfid.record-sfid" && string(value) == tombstoneMarker {
		s.raced = true
		if _, err := s.Put(ctx, key, []byte("committee-uid:another-owner")); err != nil {
			return 0, err
		}
	}
	return s.MappingStore.Update(ctx, key, value, revision)
}

func TestMemberCleanupTombstoneCASPreservesConcurrentOwner(t *testing.T) {
	h := setupMemberLifecycle(t)
	v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	mappingStore = &memberCleanupOwnershipRaceStore{MappingStore: h.store}
	if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); !errors.Is(err, ErrRevisionMismatch) {
		t.Fatalf("CAS race = %v", err)
	}
	if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("conflicting retry = %v", err)
	}
	h.assertMapping(t, "committee_member.sfid.record-sfid", "committee-uid:another-owner")
	if len(h.recordedRequests()) != 2 {
		t.Fatal("ownership conflict caused a second DELETE")
	}
}

func TestMemberCleanupLegacyPartialFailsClosed(t *testing.T) {
	h := setupMemberLifecycle(t)
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	h.seed(t, "committee_member.sfid.record-sfid", tombstoneMarker)
	seedReplacementMember(t, h)
	if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("legacy partial cleanup = %v", err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("legacy partial cleanup sent DELETE")
	}
	assertReplacementMember(t, h)
}

func TestMemberCleanupPrimaryOwnership(t *testing.T) {
	for _, name := range []string{"original", "absent", "replaced", "deleted original", "duplicates", "wrong contact", "replica", "unavailable", "no record ID", "corrupt snapshot"} {
		t.Run(name, func(t *testing.T) {
			h := setupMemberLifecycle(t)
			v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
			if err := createLifecycleMember(t); err != nil {
				t.Fatal(err)
			}
			state := memberCleanupState{Primary: true, Rows: []memberCleanupRow{{ID: "record-sfid", CommitteeID: "committee-sfid", ContactID: "contact-sfid"}}}
			switch name {
			case "absent":
				state.Rows = nil
			case "replaced":
				state.Rows[0].ID = "replacement-record"
			case "deleted original":
				state.Rows[0].Deleted = true
			case "duplicates":
				state.Rows = append(state.Rows, memberCleanupRow{ID: "duplicate", CommitteeID: "committee-sfid", ContactID: "contact-sfid"})
			case "wrong contact":
				state.Rows[0].ContactID = "someone-else"
			case "replica":
				state.Primary = false
			case "no record ID":
				result, _, err := readMemberCreateResult(t.Context(), "member-uid")
				if err != nil {
					t.Fatal(err)
				}
				result.RecordID = ""
				encoded, _ := json.Marshal(result)
				h.seed(t, memberCreateResultKey("member-uid"), string(encoded))
			case "corrupt snapshot":
				state.Rows[0].ID = ""
			}
			readMemberCleanupState = func(context.Context, *memberCreateResult) (memberCleanupState, error) {
				if name == "unavailable" {
					return memberCleanupState{}, errors.New("synthetic database outage")
				}
				return state, nil
			}
			err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil)
			success := name == "original" || name == "absent" || name == "replaced" || name == "deleted original"
			if (err == nil) != success {
				t.Fatalf("cleanup = %v", err)
			}
			wantRequests := 1
			if name == "original" {
				wantRequests++
			}
			if len(h.recordedRequests()) != wantRequests {
				t.Fatalf("requests = %+v", h.recordedRequests())
			}
		})
	}
}

type parentAttemptReadErrorStore struct{ MappingStore }

func (s parentAttemptReadErrorStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	if key == committeeCreateAttemptKey("parent-uid") {
		return MappingEntry{}, errors.New("synthetic parent attempt read outage")
	}
	return s.MappingStore.Get(ctx, key)
}

func TestChildCommitteeWaitsForParentRecovery(t *testing.T) {
	for _, parent := range []string{"pending", "untracked", "corrupt", "tombstoned", "deleted", "wrong project", "transient"} {
		t.Run(parent, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"child-sfid"}`)
			h.seed(t, "project.uid.project-uid", "project-sfid")
			switch parent {
			case "pending", "wrong project":
				attempt := committeeCreateAttempt{Version: 1, OperationID: "11111111-2222-4333-8444-555555555555", ProjectSFID: "project-sfid", Fingerprint: committeeCreateFingerprint(recoveryTestPayload()), CreatorHash: committeeCreateHash([]byte("synthetic-sync-client@clients")), StartedAt: time.Now()}
				if parent == "wrong project" {
					attempt.ProjectSFID = "other-project"
				}
				encoded, _ := json.Marshal(attempt)
				h.seed(t, committeeCreateAttemptKey("parent-uid"), string(encoded))
			case "corrupt":
				h.seed(t, committeeCreateAttemptKey("parent-uid"), `{}`)
			case "tombstoned":
				h.seed(t, "committee.uid.parent-uid", tombstoneMarker)
			case "deleted":
				if err := recordCreateDeletion(t.Context(), "committee", "parent-uid"); err != nil {
					t.Fatal(err)
				}
			case "transient":
				mappingStore = parentAttemptReadErrorStore{MappingStore: h.store}
			}
			data := map[string]any{"name": "Child Committee", "project_uid": "project-uid", "parent_uid": "parent-uid"}
			err := syncCommitteeCreateToV1(t.Context(), "child-uid", "project-sfid", data)
			if parent == "pending" && !errors.Is(err, errCommitteeCreatePending) {
				t.Fatal(err)
			}
			if parent == "untracked" && (!errors.Is(err, ErrKeyNotFound) || errors.Is(err, errCommitteeCreatePending)) {
				t.Fatal(err)
			}
			if (parent == "corrupt" || parent == "wrong project" || parent == "deleted") && !errors.Is(err, errCommitteeCreateManual) {
				t.Fatal(err)
			}
			if parent == "transient" && (err == nil || errors.Is(err, errCommitteeCreatePending) || errors.Is(err, ErrKeyNotFound)) {
				t.Fatal(err)
			}
			if parent == "tombstoned" && (err == nil || errors.Is(err, errCommitteeCreatePending)) {
				t.Fatal(err)
			}
			if len(h.recordedRequests()) != 0 {
				t.Fatal("child created before parent")
			}
			if parent != "pending" {
				return
			}
			body, _ := json.Marshal(indexingEventBody{Data: data})
			event, _ := json.Marshal(indexingEvent{ObjectID: "child-uid", Action: "created", Body: body})
			msg := &fakeMsg{subject: "lfx.committee.created", data: event}
			newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)(msg)
			if msg.nakCalls != 1 || msg.nakDelay != committeeEventsMappingRetryDelay {
				t.Fatalf("retry = %+v", msg)
			}
			h.seed(t, "committee.uid.parent-uid", "project-sfid:parent-sfid")
			msg = &fakeMsg{subject: "lfx.committee.created", data: event, deliveries: 2}
			newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)(msg)
			if msg.ackCalls != 1 || len(h.recordedRequests()) != 1 {
				t.Fatalf("recovered delivery = %+v", msg)
			}
			if h.recordedRequests()[0].Body["CommitteeID"] != "parent-sfid" {
				t.Fatal("child lost its parent")
			}
		})
	}
}
