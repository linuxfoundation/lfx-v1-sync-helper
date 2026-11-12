// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestMemberRecoveryRequiresOriginalPrimaryRecord(t *testing.T) {
	for _, kind := range []string{"original", "replaced", "absent", "duplicate", "missing record ID", "replica", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			h := setupMemberLifecycle(t)
			mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee_member.sfid.record-sfid"}
			if err := createLifecycleMember(t); err == nil {
				t.Fatal("expected mapping failure after POST")
			}
			state := memberCleanupState{Primary: true, Rows: []memberCleanupRow{{ID: "record-sfid", CommitteeID: "committee-sfid", ContactID: "contact-sfid"}}}
			switch kind {
			case "replaced":
				state.Rows[0].Deleted = true
				state.Rows = append(state.Rows, memberCleanupRow{ID: "replacement-record", CommitteeID: "committee-sfid", ContactID: "contact-sfid"})
			case "absent":
				state.Rows = nil
			case "duplicate":
				state.Rows = append(state.Rows, memberCleanupRow{ID: "other-record", CommitteeID: "committee-sfid", ContactID: "contact-sfid"})
			case "replica":
				state.Primary = false
			case "missing record ID":
				result, _, err := readMemberCreateResult(t.Context(), "member-uid")
				if err != nil {
					t.Fatal(err)
				}
				result.RecordID = ""
				data, _ := json.Marshal(result)
				h.seed(t, memberCreateResultKey("member-uid"), string(data))
			}
			readMemberCleanupState = func(context.Context, *memberCreateResult) (memberCleanupState, error) {
				if kind == "unavailable" {
					return memberCleanupState{}, errors.New("synthetic primary outage")
				}
				return state, nil
			}
			err := createLifecycleMember(t)
			if kind == "original" {
				if err != nil {
					t.Fatal(err)
				}
				requests := h.recordedRequests()
				if len(requests) != 2 || requests[1].Method != http.MethodPatch {
					t.Fatalf("requests = %+v", requests)
				}
				return
			}
			if err == nil {
				t.Fatal("unproven membership was accepted")
			}
			if kind != "replica" && kind != "unavailable" && !errors.Is(err, errCommitteeCreateManual) {
				t.Fatal(err)
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("replay PATCHed an unproven/replaced membership")
			}
			if _, err := h.store.Get(t.Context(), "committee_member.uid.member-uid"); !errors.Is(err, ErrKeyNotFound) {
				t.Fatal("unproven replay claimed reverse mapping")
			}
		})
	}
}

func TestMemberRecoveryPrimaryReadRetriesWithoutPost(t *testing.T) {
	h := setupMemberLifecycle(t)
	mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee_member.sfid.record-sfid"}
	if err := createLifecycleMember(t); err == nil {
		t.Fatal("expected mapping failure")
	}
	read := readMemberCleanupState
	readMemberCleanupState = func(context.Context, *memberCreateResult) (memberCleanupState, error) {
		return memberCleanupState{}, errors.New("synthetic database outage")
	}
	if err := createLifecycleMember(t); err == nil {
		t.Fatal("primary outage must retry")
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("primary outage permitted PATCH/POST")
	}
	readMemberCleanupState = read
	if err := createLifecycleMember(t); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 2 {
		t.Fatal("recovery should issue one PATCH and no extra POST")
	}
}

func TestMemberRecoveryDetectsReplacementDuringPatch(t *testing.T) {
	h := setupMemberLifecycle(t)
	mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee_member.sfid.record-sfid"}
	if err := createLifecycleMember(t); err == nil {
		t.Fatal("expected mapping failure")
	}
	v1HTTPClient.Transport = &memberReconcileTransport{base: v1HTTPClient.Transport, afterPatch: func() {
		readMemberCleanupState = func(context.Context, *memberCreateResult) (memberCleanupState, error) {
			return memberCleanupState{Primary: true, Rows: []memberCleanupRow{{ID: "replacement-record", CommitteeID: "committee-sfid", ContactID: "contact-sfid"}}}, nil
		}
	}}
	if err := createLifecycleMember(t); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("replacement during PATCH = %v", err)
	}
	if len(h.recordedRequests()) != 2 {
		t.Fatal("unexpected write after replacement")
	}
	if err := createLifecycleMember(t); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("replacement replay = %v", err)
	}
	if len(h.recordedRequests()) != 2 {
		t.Fatal("replacement replay sent another PATCH")
	}
}
