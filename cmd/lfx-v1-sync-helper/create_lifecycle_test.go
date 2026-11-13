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

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

func lifecycleEvent(t *testing.T, kind, action, uid string, data map[string]any) error {
	t.Helper()
	body, err := json.Marshal(indexingEventBody{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(indexingEvent{ObjectID: uid, ObjectType: kind, Action: action, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if kind == "committee_member" {
		return processCommitteeMemberIndexingEvent(t.Context(), "lfx."+kind+"."+action, encoded)
	}
	return processCommitteeIndexingEvent(t.Context(), "lfx."+kind+"."+action, encoded)
}

func TestCommitteeCreateParentNormalization(t *testing.T) {
	for _, name := range []string{"omitted", "empty", "null", "whitespace", "parent"} {
		t.Run(name, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			data := map[string]any{"name": "Synthetic Committee"}
			want := ""
			switch name {
			case "empty":
				data["parent_uid"] = ""
			case "null":
				data["parent_uid"] = nil
			case "whitespace":
				data["parent_uid"] = " \t "
			case "parent":
				data["parent_uid"], want = "parent-uid", "parent-sfid"
				h.seed(t, "committee.uid.parent-uid", "project-sfid:parent-sfid")
			}
			checkCommitteeCreateV2 = func(_ context.Context, _, _, _, parent string) error {
				if parent != want {
					t.Errorf("create identity parent = %q, want %q", parent, want)
				}
				return nil
			}
			if err := syncCommitteeCreateToV1(t.Context(), "committee-uid", "project-sfid", data); err != nil {
				t.Fatal(err)
			}
			attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
			if err != nil || attempt.ParentSFID != want {
				t.Fatalf("attempt = %+v, %v", attempt, err)
			}
			requests := h.recordedRequests()
			if len(requests) != 1 {
				t.Fatalf("requests = %+v", requests)
			}
			got, _ := requests[0].Body["CommitteeID"].(string)
			if got != want {
				t.Fatalf("POST parent = %q, want %q", got, want)
			}
			if name != "omitted" && name != "parent" {
				parent, err := resolveV2CommitteeParentSFID(t.Context(), "project-sfid", data, false)
				if err != nil || parent == nil || *parent != "NONE" {
					t.Fatalf("PATCH clearing changed: %v, %v", parent, err)
				}
			}
		})
	}
}

func setupMemberLifecycle(t *testing.T) *syncTestHarness {
	t.Helper()
	h := setupSyncTest(t, 200, `{"ID":"record-sfid","MemberID":"contact-sfid"}`)
	readMemberCleanupState = func(_ context.Context, result *memberCreateResult) (memberCleanupState, error) {
		return memberCleanupState{Primary: true, Rows: []memberCleanupRow{{ID: result.RecordID, CommitteeID: result.CommitteeSFID, ContactID: result.MemberID}}}, nil
	}
	origResolve := resolveV1UserSFIDByEmailFn
	t.Cleanup(func() { resolveV1UserSFIDByEmailFn = origResolve })
	resolveV1UserSFIDByEmailFn = func(context.Context, string) (string, error) { return "", nil }
	fetchMemberForCreate = func(context.Context, string, string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeMemberFullWithReadonlyAttributes{
			UID: ptr("member-uid"), CommitteeUID: ptr("committee-uid"), Email: ptr("current@example.com"),
			FirstName: ptr("Current"), LastName: ptr("Member"), JobTitle: ptr("Engineer"), Status: "Active", AppointedBy: "Community",
			Role: &struct {
				Name      string
				StartDate *string
				EndDate   *string
			}{Name: "Chair", StartDate: ptr("2026-01-01")},
			Voting: &struct {
				Status    string
				StartDate *string
				EndDate   *string
			}{Status: "Voting Rep", EndDate: ptr("2027-01-01")},
			Organization: &struct {
				ID      *string
				Name    *string
				Website *string
			}{ID: ptr(testMemberOrgSFID)},
		}, "etag", nil
	}
	return h
}

func createLifecycleMember(t *testing.T) error {
	t.Helper()
	return syncCommitteeMemberCreateToV1(t.Context(), "member-uid", "committee-uid", "project-sfid", "committee-sfid", map[string]any{
		"email": "stale@example.com", "first_name": "Stale", "voting": map[string]any{"status": "Observer"},
	})
}

func TestMemberCreateUsesCurrentFields(t *testing.T) {
	h := setupMemberLifecycle(t)
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"Email": "current@example.com", "FirstName": "Current", "LastName": "Member", "Title": "Engineer",
		"Status": "Active", "AppointedBy": "Community", "Role": "Chair", "RoleStartDate": "2026-01-01",
		"VotingStatus": "Voting Rep", "VotingEndDate": "2027-01-01", "OrganizationID": testMemberOrgSFID,
	}
	assertSyncRequest(t, h, http.MethodPost, "/project-service/v2/projects/project-sfid/committees/committee-sfid/members", want)
	entry, _ := h.store.Get(t.Context(), memberCreateResultKey("member-uid"))
	if strings.Contains(string(entry.Value), "example.com") || strings.Contains(string(entry.Value), "Engineer") {
		t.Fatal("member attempt retained profile data")
	}
}

func TestMemberCreateCurrentReadFailures(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient", true: "deleted"}[missing], func(t *testing.T) {
			h := setupMemberLifecycle(t)
			fetchMemberForCreate = func(context.Context, string, string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				if missing {
					return nil, "", &committeeservice.NotFoundError{Message: "deleted"}
				}
				return nil, "", errors.New("synthetic read outage")
			}
			err := createLifecycleMember(t)
			if (err == nil) != missing || len(h.recordedRequests()) != 0 {
				t.Fatalf("create = %v, requests = %+v", err, h.recordedRequests())
			}
			if missing {
				deleted, err := hasCreateDeletion(t.Context(), "committee_member", "member-uid")
				if err != nil || !deleted {
					t.Fatal("NotFound did not retain deletion intent")
				}
			}
		})
	}
}

func TestDelayedMemberCreateAfterDelete(t *testing.T) {
	h := setupMemberLifecycle(t)
	// Record an ambiguous parent POST, then let its member create wait.
	state := recoveryTestState(t)
	attempt := committeeCreateAttempt{
		Version: 3, Phase: "dispatched", ScopeKey: committeeScopeKey("project-sfid", state.NormalizedName), OperationID: "11111111-2222-4333-8444-555555555555", ProjectSFID: "project-sfid",
		Fingerprint: committeeCreateFingerprint(recoveryTestPayload()), CreatorHash: committeeCreateHash([]byte("synthetic-sync-client@clients")), StartedAt: state.Now,
	}
	encoded, _ := json.Marshal(attempt)
	h.seed(t, committeeCreateAttemptKey("committee-uid"), string(encoded))
	if err := claimCommitteeCreateScope(t.Context(), "committee-uid", &attempt); err != nil {
		t.Fatal(err)
	}
	data := map[string]any{"committee_uid": "committee-uid", "email": "stale@example.com"}
	if err := lifecycleEvent(t, "committee_member", "created", "member-uid", data); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatal(err)
	}
	if err := lifecycleEvent(t, "committee_member", "deleted", "member-uid", nil); err != nil {
		t.Fatal(err)
	}
	state.Now, state.Candidates = state.Now.Add(71*time.Second), []committeeCreateCandidate{recoveryTestCandidate(attempt.StartedAt)}
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
		t.Fatal(err)
	}
	if err := lifecycleEvent(t, "committee_member", "created", "member-uid", data); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 0 {
		t.Fatal("stale create resurrected deleted member")
	}
}

type lifecycleTransport struct {
	base       http.RoundTripper
	afterPost  func()
	failDelete bool
}

func (r *lifecycleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete && r.failDelete {
		r.failDelete = false
		return nil, errors.New("synthetic delete outage")
	}
	response, err := r.base.RoundTrip(req)
	if req.Method == http.MethodDelete && response != nil {
		response.StatusCode = http.StatusNoContent
	}
	if req.Method == http.MethodPost && err == nil && r.afterPost != nil {
		callback := r.afterPost
		r.afterPost = nil
		callback()
	}
	return response, err
}

type lifecycleClaimStore struct {
	MappingStore
	key   string
	after bool
	hook  func()
}

func (s *lifecycleClaimStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if key != s.key || s.hook == nil {
		return s.MappingStore.Create(ctx, key, value)
	}
	hook := s.hook
	s.hook = nil // Cleanup may claim the same key recursively.
	if !s.after {
		hook()
	}
	revision, err := s.MappingStore.Create(ctx, key, value)
	if s.after {
		hook()
	}
	return revision, err
}

func TestCreateDeletionCompletionWindows(t *testing.T) {
	for _, kind := range []string{"committee", "committee_member"} {
		for _, window := range []string{"post in flight", "before reverse claim", "after reverse claim"} {
			t.Run(kind+"/"+window, func(t *testing.T) {
				var h *syncTestHarness
				uid, forward := "committee-uid", "committee.sfid.committee-sfid"
				if kind == "committee" {
					h = setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
					recoveryTestState(t)
				} else {
					h = setupMemberLifecycle(t)
					uid, forward = "member-uid", "committee_member.sfid.record-sfid"
				}
				v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
				deleteEvent := func() {
					err := lifecycleEvent(t, kind, "deleted", uid, nil)
					if window == "post in flight" {
						if !errors.Is(err, errCommitteeCreatePending) {
							t.Errorf("in-flight deletion = %v, want pending", err)
						}
					} else if err != nil {
						t.Errorf("deletion = %v", err)
					}
				}
				if window == "post in flight" {
					v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport, afterPost: deleteEvent}
				} else {
					mappingStore = &lifecycleClaimStore{MappingStore: h.store, key: kind + ".uid." + uid, after: window == "after reverse claim", hook: deleteEvent}
				}
				var err error
				if kind == "committee" {
					err = createOrRecoverCommittee(t.Context(), uid, "project-sfid", recoveryTestPayload())
				} else {
					err = createLifecycleMember(t)
				}
				if err != nil {
					t.Fatal(err)
				}
				h.assertMapping(t, forward, tombstoneMarker)
				h.assertMapping(t, kind+".uid."+uid, tombstoneMarker)
				posts, deletes := 0, 0
				for _, request := range h.recordedRequests() {
					if request.Method == http.MethodPost {
						posts++
					}
					if request.Method == http.MethodDelete {
						deletes++
					}
				}
				if posts != 1 || deletes == 0 {
					t.Fatalf("POSTs = %d, DELETEs = %d", posts, deletes)
				}
			})
		}
	}
}

func TestMemberCreateCleanupRetriesFromRecordedResult(t *testing.T) {
	h := setupMemberLifecycle(t)
	transport := &lifecycleTransport{base: v1HTTPClient.Transport}
	readState := readMemberCleanupState
	reads := 0
	readMemberCleanupState = func(ctx context.Context, result *memberCreateResult) (memberCleanupState, error) {
		reads++
		if reads == 1 {
			return memberCleanupState{}, errors.New("synthetic pre-dispatch database outage")
		}
		return readState(ctx, result)
	}
	transport.afterPost = func() {
		if err := recordCreateDeletion(t.Context(), "committee_member", "member-uid"); err != nil {
			t.Fatal(err)
		}
	}
	v1HTTPClient.Transport = transport
	if err := createLifecycleMember(t); err == nil {
		t.Fatal("pre-dispatch read outage must retry")
	}
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	h.assertMapping(t, "committee_member.uid.member-uid", tombstoneMarker)
	requests := h.recordedRequests()
	if len(requests) != 2 || requests[0].Method != http.MethodPost || requests[1].Method != http.MethodDelete {
		t.Fatalf("retry repeated POST: %+v", requests)
	}
}

func TestMemberCreateRefreshAfterPost(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient completion read", true: "deleted before completion"}[missing], func(t *testing.T) {
			h := setupMemberLifecycle(t)
			v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
			fetch := fetchMemberForCreate
			calls := 0
			fetchMemberForCreate = func(ctx context.Context, committeeUID, uid string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				calls++
				if calls == 2 {
					if missing {
						return nil, "", &committeeservice.NotFoundError{Message: "deleted"}
					}
					return nil, "", errors.New("synthetic read outage")
				}
				return fetch(ctx, committeeUID, uid)
			}
			err := createLifecycleMember(t)
			if missing {
				if err != nil {
					t.Fatal(err)
				}
				h.assertMapping(t, "committee_member.uid.member-uid", tombstoneMarker)
			} else {
				if err == nil {
					t.Fatal("completion read outage must retry")
				}
				if err := createLifecycleMember(t); err != nil {
					t.Fatal(err)
				}
				h.assertMapping(t, "committee_member.uid.member-uid", "project-sfid:committee-sfid:contact-sfid")
			}
			posts := 0
			for _, request := range h.recordedRequests() {
				if request.Method == http.MethodPost {
					posts++
				}
			}
			if posts != 1 {
				t.Fatalf("POSTs = %d, want 1", posts)
			}
		})
	}
}

func TestMemberCreateConcurrentDelivery(t *testing.T) {
	h := setupMemberLifecycle(t)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	v1HTTPClient.Transport = &recoveryGateTransport{base: v1HTTPClient.Transport, started: started, release: release}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- syncCommitteeMemberCreateToV1(ctx, "member-uid", "committee-uid", "project-sfid", "committee-sfid", nil)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("POST did not start")
	}
	if err := createLifecycleMember(t); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatalf("concurrent delivery = %v", err)
	}
	// Unblock without closing the channel twice; this transport waits for one
	// receive, and the deferred close releases it even on a failed assertion.
	release <- struct{}{}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("concurrent delivery repeated POST")
	}
}

func TestRecordedCreateCleanupPreservesConflicts(t *testing.T) {
	for _, kind := range []string{"committee", "committee_member"} {
		t.Run(kind, func(t *testing.T) {
			var h *syncTestHarness
			uid, forward := "committee-uid", "committee.sfid.committee-sfid"
			if kind == "committee" {
				h = setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
				recoveryTestState(t)
				if err := createOrRecoverCommittee(t.Context(), uid, "project-sfid", recoveryTestPayload()); err != nil {
					t.Fatal(err)
				}
			} else {
				h = setupMemberLifecycle(t)
				uid, forward = "member-uid", "committee_member.sfid.record-sfid"
				if err := createLifecycleMember(t); err != nil {
					t.Fatal(err)
				}
			}
			h.seed(t, forward, "another-owner")
			if err := lifecycleEvent(t, kind, "deleted", uid, nil); !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("conflicting cleanup = %v", err)
			}
			h.assertMapping(t, forward, "another-owner")
			if len(h.recordedRequests()) != 1 {
				t.Fatal("cleanup deleted a conflicting result")
			}
		})
	}
}

func TestMemberCreateAttemptStorageAndAmbiguousOutcome(t *testing.T) {
	for _, failClaim := range []bool{false, true} {
		t.Run(map[bool]string{false: "unreadable result", true: "attempt write fails"}[failClaim], func(t *testing.T) {
			h := setupMemberLifecycle(t)
			if failClaim {
				h.store.nextCreateErr = errors.New("synthetic attempt storage outage")
			} else {
				// Simulate losing a committed response, not a confirmed rejection.
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				v1HTTPClient.Transport = canceledCommitteeResponseTransport{base: v1HTTPClient.Transport, cancel: cancel}
				if err := syncCommitteeMemberCreateToV1(ctx, "member-uid", "committee-uid", "project-sfid", "committee-sfid", nil); !errors.Is(err, errCommitteeCreatePending) {
					t.Fatalf("lost response = %v", err)
				}
			}
			if err := createLifecycleMember(t); err == nil {
				t.Fatal("unresolved attempt unexpectedly succeeded")
			}
			wantPosts := 1
			if failClaim {
				wantPosts = 0
			}
			if len(h.recordedRequests()) != wantPosts {
				t.Fatal("POST was not fenced by durable attempt")
			}
		})
	}
}
