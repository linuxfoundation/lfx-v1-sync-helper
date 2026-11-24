// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go"
)

func TestCommitteeScopeBlocksDeletedPredecessorAdoption(t *testing.T) {
	h := setupSyncTest(t, 503, `{}`)
	state := recoveryTestState(t)
	start := state.Now
	var owner atomic.Value
	owner.Store("committee-A")
	oldBase, oldNATS := fetchCommitteeBase, natsConn
	nc, err := nats.Connect(startCommitteeEventsTestBroker(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close(); fetchCommitteeBase, natsConn = oldBase, oldNATS })
	natsConn = nc
	fetchCommitteeBase = func(_ context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		if uid != owner.Load().(string) {
			return nil, "", &committeeservice.NotFoundError{Message: "old committee deleted"}
		}
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{UID: &uid, ProjectUID: ptr("project-uid"), Name: ptr("Synthetic Committee")}, "etag", nil
	}
	checkCommitteeCreateV2, checkCommitteeCreatePayload = validateCommitteeCreateV2, validateCommitteeCreatePayload
	h.seed(t, "project.uid.project-uid", "project-sfid")
	if _, err := nc.Subscribe("lfx.committee-api.name_to_uid", func(msg *nats.Msg) {
		data, _ := json.Marshal(committeeNameToUIDResponse{CommitteeUID: owner.Load().(string)})
		_ = msg.Respond(data)
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := createOrRecoverCommittee(t.Context(), "committee-A", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatal(err)
	}
	owner.Store("committee-B")
	if err := lifecycleEvent(t, "committee", "deleted", "committee-A", nil); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatal(err)
	}
	state.Now = start.Add(5 * time.Second)
	if err := createOrRecoverCommittee(t.Context(), "committee-B", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("competing dispatch = %v", err)
	}
	// The predecessor's delayed commit falls within both request windows.
	candidate := recoveryTestCandidate(start)
	candidate.CreatedAt, candidate.ModifiedAt = start.Add(6*time.Second), start.Add(6*time.Second)
	state.Now, state.Candidates = start.Add(80*time.Second), []committeeCreateCandidate{candidate}
	if err := createOrRecoverCommittee(t.Context(), "committee-B", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("competing adoption = %v", err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("replacement issued POST while predecessor unresolved")
	}
	if _, err := h.store.Get(t.Context(), "committee.uid.committee-B"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal("replacement adopted predecessor")
	}
	attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-A")
	if err != nil || verifyCommitteeCreateScope(t.Context(), "committee-A", attempt) != nil {
		t.Fatal("predecessor lost dispatch ownership")
	}
	entry, _ := h.store.Get(t.Context(), attempt.ScopeKey)
	if strings.Contains(string(entry.Value), "Synthetic Committee") || strings.Contains(attempt.ScopeKey, "synthetic committee") {
		t.Fatal("scope retained raw name")
	}
}

func TestCommitteeScopeOwnershipAndIntegrity(t *testing.T) {
	for _, kind := range []string{"another UID", "another operation", "missing", "invalid", "tombstone"} {
		t.Run(kind, func(t *testing.T) {
			h := setupSyncTest(t, 503, `{}`)
			state := recoveryTestState(t)
			start := state.Now
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
				t.Fatal(err)
			}
			attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
			if err != nil {
				t.Fatal(err)
			}
			value := `{"version":1,"uid":"committee-uid","operation_id":"different"}`
			switch kind {
			case "another UID":
				value = `{"version":1,"uid":"another-uid","operation_id":"different"}`
			case "missing":
				if err := h.store.Delete(t.Context(), attempt.ScopeKey); err != nil {
					t.Fatal(err)
				}
			case "invalid":
				value = `{}`
			case "tombstone":
				value = tombstoneMarker
			}
			if kind != "missing" {
				h.seed(t, attempt.ScopeKey, value)
			}
			state.Now, state.Candidates = start.Add(71*time.Second), []committeeCreateCandidate{recoveryTestCandidate(start)}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("invalid scope adoption = %v", err)
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("scope conflict repeated POST")
			}
		})
	}
}

func TestCommitteeScopeProjectAndCanonicalName(t *testing.T) {
	h := setupSyncTest(t, 201, `{}`)
	a := &committeeCreateAttempt{OperationID: "op-A", ScopeKey: committeeScopeKey("project-A", "db-canonical-name")}
	if err := claimCommitteeCreateScope(t.Context(), "uid-A", a); err != nil {
		t.Fatal(err)
	}
	b := &committeeCreateAttempt{OperationID: "op-B", ScopeKey: committeeScopeKey("project-A", "db-canonical-name")}
	if err := claimCommitteeCreateScope(t.Context(), "uid-B", b); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("same database name = %v", err)
	}
	b.ScopeKey = committeeScopeKey("project-B", "db-canonical-name")
	if err := claimCommitteeCreateScope(t.Context(), "uid-B", b); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 0 {
		t.Fatal("scope operations issued HTTP")
	}
}

func TestCommitteeScopedAdoptionRolloutAndLegacy(t *testing.T) {
	for _, kind := range []string{"gate disabled", "legacy dispatched", "legacy known ID"} {
		t.Run(kind, func(t *testing.T) {
			h := setupSyncTest(t, 503, `{}`)
			state := recoveryTestState(t)
			start := state.Now
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
				t.Fatal(err)
			}
			attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
			if err != nil {
				t.Fatal(err)
			}
			if kind == "gate disabled" {
				cfg.CommitteeScopedAdoptionEnabled = false
			} else {
				attempt.Version, attempt.ScopeKey = 2, ""
				if kind == "legacy known ID" {
					attempt.V1ID = "committee-sfid"
				}
				data, _ := json.Marshal(attempt)
				h.seed(t, committeeCreateAttemptKey("committee-uid"), string(data))
			}
			state.Now, state.Candidates = start.Add(71*time.Second), []committeeCreateCandidate{recoveryTestCandidate(start)}
			err = createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
			if kind == "legacy known ID" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("unsafe rollout adoption = %v", err)
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("rollout/legacy decision repeated POST")
			}
		})
	}
}

func TestCommitteeScopedAdoptionConfigDefaultsClosed(t *testing.T) {
	for _, value := range []string{"", "false", "invalid", "true"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnvs(t)
			t.Setenv("COMMITTEE_SCOPED_ADOPTION_ENABLED", value)
			config, err := LoadConfig()
			if err != nil {
				t.Fatal(err)
			}
			if config.CommitteeScopedAdoptionEnabled != (value == "true") {
				t.Fatal("scoped adoption gate did not fail closed")
			}
		})
	}
}

type scopeCreateFaultStore struct {
	MappingStore
	loseResponse bool
	failed       bool
}

func (s *scopeCreateFaultStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if strings.HasPrefix(key, committeeScopePrefix) && !s.failed {
		s.failed = true
		if s.loseResponse {
			if _, err := s.MappingStore.Create(ctx, key, value); err != nil {
				return 0, err
			}
		}
		return 0, errors.New("synthetic scope persistence outage")
	}
	return s.MappingStore.Create(ctx, key, value)
}

func TestCommitteeDispatchRequiresDurableScope(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "write failed", true: "response lost"}[lost], func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			recoveryTestState(t)
			mappingStore = &scopeCreateFaultStore{MappingStore: h.store, loseResponse: lost}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err == nil {
				t.Fatal("scope persistence failure accepted")
			}
			if len(h.recordedRequests()) != 0 {
				t.Fatal("POST sent without confirmed scope ownership")
			}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
				t.Fatal(err)
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("scope retry did not issue exactly one POST")
			}
		})
	}
}

func TestCommitteeScopeRetainedAfterKnownCleanup(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	cfg.CommitteeScopedAdoptionEnabled = false // Direct successful results still work.
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
		t.Fatal(err)
	}
	attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
	if err != nil {
		t.Fatal(err)
	}
	v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
	if err := lifecycleEvent(t, "committee", "deleted", "committee-uid", nil); err != nil {
		t.Fatal(err)
	}
	if err := verifyCommitteeCreateScope(t.Context(), "committee-uid", attempt); err != nil {
		t.Fatal("cleanup released permanent scope ownership")
	}
	if err := createOrRecoverCommittee(t.Context(), "another-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("automatic scope handoff = %v", err)
	}
	if len(h.recordedRequests()) != 2 {
		t.Fatal("name reuse issued an unapproved POST")
	}
}
