// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

type memberReconcileTransport struct {
	base       http.RoundTripper
	failPatch  bool
	afterPost  func()
	afterPatch func()
}

func (r *memberReconcileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(req)
	if err != nil {
		return response, err
	}
	if req.Method == http.MethodPost && r.afterPost != nil {
		r.afterPost()
	}
	if req.Method == http.MethodPatch {
		if r.failPatch {
			r.failPatch = false
			response.StatusCode = http.StatusServiceUnavailable
		}
		if r.afterPatch != nil {
			r.afterPatch()
		}
	}
	return response, nil
}

func TestMemberCreateReplayReconcilesCurrentFields(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(map[bool]string{false: "edited", true: "cleared"}[clear], func(t *testing.T) {
			h := setupMemberLifecycle(t)
			fetch := fetchMemberForCreate
			edited := false
			fetchMemberForCreate = func(ctx context.Context, committeeUID, uid string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				member, etag, err := fetch(ctx, committeeUID, uid)
				if edited && err == nil {
					if clear {
						member.Voting, member.Role = nil, nil
					} else {
						member.Voting.Status, member.Role.Name = "Observer", "Vice Chair"
					}
				}
				return member, etag, err
			}
			mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee_member.sfid.record-sfid"}
			if err := createLifecycleMember(t); err == nil {
				t.Fatal("expected mapping failure after POST")
			}
			edited = true
			if err := lifecycleEvent(t, "committee_member", "updated", "member-uid", map[string]any{"voting": map[string]any{"status": "Observer"}}); err != nil {
				t.Fatal(err)
			}
			if err := createLifecycleMember(t); err != nil {
				t.Fatal(err)
			}
			requests := h.recordedRequests()
			if len(requests) != 2 || requests[0].Method != http.MethodPost || requests[1].Method != http.MethodPatch {
				t.Fatalf("requests = %+v", requests)
			}
			wantRole := "Vice Chair"
			if clear {
				wantRole = v1NoRole
			}
			if requests[1].Body["Role"] != wantRole || requests[1].Body["VotingStatus"] != "Observer" {
				t.Fatalf("current PATCH = %+v", requests[1])
			}
			if requests[1].PreserveOmittedDates != "true" {
				t.Fatal("reconciliation must use the existing omitted-date contract")
			}
			if clear {
				for _, key := range []string{"RoleStartDate", "RoleEndDate", "VotingStartDate", "VotingEndDate"} {
					if requests[1].Body[key] != "" {
						t.Fatalf("%s was not explicitly cleared: %+v", key, requests[1].Body)
					}
				}
			}
			h.assertMapping(t, "committee_member.uid.member-uid", "project-sfid:committee-sfid:contact-sfid")
		})
	}
}

func TestMemberCreateReplayRetriesPatchWithoutPost(t *testing.T) {
	h := setupMemberLifecycle(t)
	transport := &memberReconcileTransport{base: v1HTTPClient.Transport, failPatch: true}
	v1HTTPClient.Transport = transport
	mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee_member.sfid.record-sfid"}
	if err := createLifecycleMember(t); err == nil {
		t.Fatal("expected mapping failure")
	}
	if err := createLifecycleMember(t); err == nil {
		t.Fatal("failed reconciliation PATCH must retry")
	}
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	requests := h.recordedRequests()
	if len(requests) != 3 || requests[0].Method != http.MethodPost || requests[1].Method != http.MethodPatch || requests[2].Method != http.MethodPatch {
		t.Fatalf("retry = %+v", requests)
	}
}

func TestMemberCreateDetectsEditDuringPost(t *testing.T) {
	h := setupMemberLifecycle(t)
	fetch := fetchMemberForCreate
	edited := false
	fetchMemberForCreate = func(ctx context.Context, committeeUID, uid string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
		member, etag, err := fetch(ctx, committeeUID, uid)
		if edited && err == nil {
			member.Role.Name = "Vice Chair"
		}
		return member, etag, err
	}
	v1HTTPClient.Transport = &memberReconcileTransport{base: v1HTTPClient.Transport, afterPost: func() { edited = true }}
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	requests := h.recordedRequests()
	if len(requests) != 2 || requests[1].Method != http.MethodPatch || requests[1].Body["Role"] != "Vice Chair" {
		t.Fatalf("POST-window edit = %+v", requests)
	}
}

func TestMemberReconcilePreservesDeletionAndOwnership(t *testing.T) {
	for _, change := range []string{"deleted", "new owner", "new edit"} {
		t.Run(change, func(t *testing.T) {
			h := setupMemberLifecycle(t)
			mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee_member.sfid.record-sfid"}
			if err := createLifecycleMember(t); err == nil {
				t.Fatal("expected mapping failure")
			}
			fetch := fetchMemberForCreate
			edited := false
			fetchMemberForCreate = func(ctx context.Context, committeeUID, uid string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				member, etag, err := fetch(ctx, committeeUID, uid)
				if edited && err == nil {
					member.Role.Name = "Newer Chair"
				}
				return member, etag, err
			}
			v1HTTPClient.Transport = &memberReconcileTransport{base: &lifecycleTransport{base: v1HTTPClient.Transport}, afterPatch: func() {
				switch change {
				case "deleted":
					if err := recordCreateDeletion(t.Context(), "committee_member", "member-uid"); err != nil {
						t.Error(err)
					}
				case "new owner":
					h.seed(t, "committee_member.sfid.record-sfid", "committee-uid:another-owner")
				case "new edit":
					edited = true
				}
			}}
			err := createLifecycleMember(t)
			switch change {
			case "deleted":
				if err != nil {
					t.Fatal(err)
				}
				h.assertMapping(t, "committee_member.uid.member-uid", tombstoneMarker)
			case "new owner":
				if !errors.Is(err, errCommitteeCreateManual) {
					t.Fatalf("owner change = %v", err)
				}
				h.assertMapping(t, "committee_member.sfid.record-sfid", "committee-uid:another-owner")
			case "new edit":
				if !errors.Is(err, errCommitteeCreatePending) {
					t.Fatalf("edit during PATCH = %v", err)
				}
			}
		})
	}
}
