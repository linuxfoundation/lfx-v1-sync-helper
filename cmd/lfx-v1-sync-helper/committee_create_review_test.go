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
	"github.com/nats-io/nats.go"
)

type committeeOwnerChangeTransport struct {
	base        http.RoundTripper
	afterDelete func()
}

func (r committeeOwnerChangeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(req)
	if err == nil && req.Method == http.MethodDelete {
		r.afterDelete()
	}
	return response, err
}

func TestCommitteeCleanupPreservesOwnerDuringDelete(t *testing.T) {
	for _, key := range []string{"committee.sfid.committee-sfid", "committee.uid.committee-uid"} {
		t.Run(key, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			recoveryTestState(t)
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
				t.Fatal(err)
			}
			v1HTTPClient.Transport = committeeOwnerChangeTransport{
				base:        &lifecycleTransport{base: v1HTTPClient.Transport},
				afterDelete: func() { h.seed(t, key, "another-owner") },
			}
			if err := lifecycleEvent(t, "committee", "deleted", "committee-uid", nil); !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("cleanup = %v", err)
			}
			h.assertMapping(t, key, "another-owner")
			if err := lifecycleEvent(t, "committee", "deleted", "committee-uid", nil); !errors.Is(err, errCommitteeCreateManual) {
				t.Fatal(err)
			}
			if len(h.recordedRequests()) != 2 {
				t.Fatal("conflicting replay issued another DELETE")
			}
		})
	}
}

type committeeCleanupCASRaceStore struct {
	MappingStore
	raced bool
}

func (s *committeeCleanupCASRaceStore) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !s.raced && key == "committee.sfid.committee-sfid" && string(value) == tombstoneMarker {
		s.raced = true
		if _, err := s.Put(ctx, key, []byte("another-owner")); err != nil {
			return 0, err
		}
	}
	return s.MappingStore.Update(ctx, key, value, revision)
}

func TestCommitteeCleanupCASPreservesConcurrentOwner(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
		t.Fatal(err)
	}
	v1HTTPClient.Transport = &lifecycleTransport{base: v1HTTPClient.Transport}
	mappingStore = &committeeCleanupCASRaceStore{MappingStore: h.store}
	if err := lifecycleEvent(t, "committee", "deleted", "committee-uid", nil); !errors.Is(err, ErrRevisionMismatch) {
		t.Fatalf("CAS race = %v", err)
	}
	h.assertMapping(t, "committee.sfid.committee-sfid", "another-owner")
}

type committeeDispatchFaultStore struct {
	MappingStore
	loseResponse bool
	failed       bool
}

func (s *committeeDispatchFaultStore) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !s.failed && key == committeeCreateAttemptKey("committee-uid") && strings.Contains(string(value), `"dispatched"`) {
		s.failed = true
		if s.loseResponse {
			if _, err := s.MappingStore.Update(ctx, key, value, revision); err != nil {
				return 0, err
			}
		}
		return 0, errors.New("synthetic dispatch write failure")
	}
	return s.MappingStore.Update(ctx, key, value, revision)
}

func TestPreparedCommitteeCreateRetriesBeforePost(t *testing.T) {
	for _, failure := range []string{"identity", "payload", "snapshot", "dispatch write", "lost dispatch response"} {
		t.Run(failure, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			state := recoveryTestState(t)
			checks := 0
			check := func(context.Context, string, string, string, string) error {
				checks++
				if checks == 2 {
					return errors.New("synthetic transient pre-dispatch validation failure")
				}
				return nil
			}
			switch failure {
			case "identity":
				checkCommitteeCreateV2 = check
			case "payload":
				checkCommitteeCreatePayload = func(ctx context.Context, uid, project, fingerprint string) error {
					return check(ctx, uid, project, fingerprint, "")
				}
			case "snapshot":
				read := readCommitteeCreateState
				readCommitteeCreateState = func(ctx context.Context, project, name string) (committeeCreateState, error) {
					checks++
					if checks == 2 {
						return committeeCreateState{}, errors.New("synthetic dispatch snapshot outage")
					}
					return read(ctx, project, name)
				}
			case "dispatch write", "lost dispatch response":
				mappingStore = &committeeDispatchFaultStore{MappingStore: h.store, loseResponse: failure == "lost dispatch response"}
			}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err == nil {
				t.Fatal("expected pre-POST failure")
			}
			if len(h.recordedRequests()) != 0 {
				t.Fatal("failed validation/dispatch write sent POST")
			}
			attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
			if err != nil || attempt.Version != 3 {
				t.Fatalf("attempt = %+v, %v", attempt, err)
			}
			state.Now = state.Now.Add(time.Hour)
			err = createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
			if failure == "lost dispatch response" {
				if attempt.Phase != "dispatched" || !errors.Is(err, errCommitteeCreatePending) || len(h.recordedRequests()) != 0 {
					t.Fatalf("uncertain dispatch retried: %+v, %v", attempt, err)
				}
				return
			}
			if attempt.Phase != "prepared" || err != nil || len(h.recordedRequests()) != 1 {
				t.Fatalf("prepared retry = %+v, %v", attempt, err)
			}
			attempt, _, err = readCommitteeCreateAttempt(t.Context(), "committee-uid")
			if err != nil || attempt.Phase != "dispatched" || !attempt.StartedAt.Equal(state.Now) {
				t.Fatalf("dispatch clock/phase = %+v, %v", attempt, err)
			}
		})
	}
}

type committeeDispatchRaceStore struct {
	MappingStore
	beforeDispatch func()
}

func (s *committeeDispatchRaceStore) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if key == committeeCreateAttemptKey("committee-uid") && s.beforeDispatch != nil {
		hook := s.beforeDispatch
		s.beforeDispatch = nil
		hook()
	}
	return s.MappingStore.Update(ctx, key, value, revision)
}

func TestPreparedCommitteeDispatchHasOneCASWinner(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	mappingStore = &committeeDispatchRaceStore{MappingStore: h.store, beforeDispatch: func() {
		if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
			t.Error(err)
		}
	}}
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatalf("CAS loser = %v", err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("prepared deliveries duplicated POST")
	}
	h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
}

func installCurrentCommitteeForRecovery(t *testing.T, h *syncTestHarness, base *committeeservice.CommitteeBaseWithReadonlyAttributes) {
	t.Helper()
	oldBase, oldNATS := fetchCommitteeBase, natsConn
	nc, err := nats.Connect(startCommitteeEventsTestBroker(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close(); fetchCommitteeBase, natsConn = oldBase, oldNATS })
	natsConn = nc
	fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		return base, "etag", nil
	}
	checkCommitteeCreateV2, checkCommitteeCreatePayload = validateCommitteeCreateV2, validateCommitteeCreatePayload
	h.seed(t, "project.uid.project-uid", "project-sfid")
	if _, err := nc.Subscribe("lfx.committee-api.name_to_uid", func(msg *nats.Msg) {
		encoded, _ := json.Marshal(committeeNameToUIDResponse{CommitteeUID: "committee-uid"})
		_ = msg.Respond(encoded)
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitteeRecoverySurfacesCurrentPayloadDrift(t *testing.T) {
	for _, field := range []string{"voting", "visibility", "description", "settings", "mailing list"} {
		t.Run(field, func(t *testing.T) {
			h := setupSyncTest(t, 503, `{}`)
			state := recoveryTestState(t)
			start := state.Now
			base := &committeeservice.CommitteeBaseWithReadonlyAttributes{UID: ptr("committee-uid"), ProjectUID: ptr("project-uid"), Name: ptr("Synthetic Committee")}
			installCurrentCommitteeForRecovery(t, h, base)
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
				t.Fatal(err)
			}
			switch field {
			case "voting":
				base.EnableVoting = true
			case "visibility":
				base.Public = true
			case "description":
				base.Description = ptr("Edited")
			case "mailing list":
				base.MailingList = ptr("edited@example.com")
			case "settings":
				fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
					return &committeeservice.CommitteeSettingsWithReadonlyAttributes{BusinessEmailRequired: true}, "etag", nil
				}
			}
			if err := lifecycleEvent(t, "committee", "updated", "committee-uid", map[string]any{"enable_voting": true}); err != nil {
				t.Fatal(err)
			}
			state.Now, state.Candidates = start.Add(71*time.Second), []committeeCreateCandidate{recoveryTestCandidate(start)}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("drift = %v", err)
			}
			if _, err := h.store.Get(t.Context(), "committee.uid.committee-uid"); !errors.Is(err, ErrKeyNotFound) {
				t.Fatal("drift acquired a mapping")
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("drift repeated POST or overwrote candidate")
			}
		})
	}
}

func TestCommitteePayloadDriftDuringMappingCompletion(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	base := &committeeservice.CommitteeBaseWithReadonlyAttributes{UID: ptr("committee-uid"), ProjectUID: ptr("project-uid"), Name: ptr("Synthetic Committee")}
	installCurrentCommitteeForRecovery(t, h, base)
	mappingStore = &lifecycleClaimStore{MappingStore: h.store, key: "committee.uid.committee-uid", hook: func() {
		base.EnableVoting = true
		if err := lifecycleEvent(t, "committee", "updated", "committee-uid", map[string]any{"enable_voting": true}); err != nil {
			t.Error(err)
		}
	}}
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("mapping-window drift = %v", err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("drift must be surfaced, not silently PATCHed")
	}
}

func TestCommitteeAttemptLegacyAndPhaseValidation(t *testing.T) {
	for _, kind := range []string{"legacy dispatched", "prepared", "unknown phase", "prepared with result"} {
		t.Run(kind, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			state := recoveryTestState(t)
			attempt := committeeCreateAttempt{Version: 2, Phase: "prepared", OperationID: "11111111-2222-4333-8444-555555555555", ProjectSFID: "project-sfid", StartedAt: state.Now.Add(-time.Hour), Fingerprint: committeeCreateFingerprint(recoveryTestPayload()), CreatorHash: committeeCreateHash([]byte("synthetic-sync-client@clients"))}
			switch kind {
			case "legacy dispatched":
				attempt.Version, attempt.Phase = 1, ""
			case "unknown phase":
				attempt.Phase = "unknown"
			case "prepared with result":
				attempt.V1ID = "committee-sfid"
			}
			data, _ := json.Marshal(attempt)
			h.seed(t, committeeCreateAttemptKey("committee-uid"), string(data))
			err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
			switch kind {
			case "legacy dispatched":
				if !errors.Is(err, errCommitteeCreateManual) {
					t.Fatal(err)
				}
			case "prepared":
				if err != nil {
					t.Fatal(err)
				}
			default:
				if !errors.Is(err, errCommitteeCreateManual) {
					t.Fatal(err)
				}
			}
			wantPosts := 0
			if kind == "prepared" {
				wantPosts = 1
			}
			if len(h.recordedRequests()) != wantPosts {
				t.Fatal("legacy or invalid phase allowed POST")
			}
		})
	}
}

func TestCommitteeCurrentPayloadNormalizationAndReadErrors(t *testing.T) {
	for _, kind := range []string{"normalized", "settings missing", "settings unavailable", "base unavailable", "base deleted"} {
		t.Run(kind, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{}`)
			base := &committeeservice.CommitteeBaseWithReadonlyAttributes{UID: ptr("committee-uid"), ProjectUID: ptr("project-uid"), Name: ptr("Synthetic Committee"), SsoGroupName: ptr("derived"), DisplayName: ptr("ignored private name")}
			installCurrentCommitteeForRecovery(t, h, base)
			switch kind {
			case "settings missing", "settings unavailable":
				fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
					if kind == "settings missing" {
						return nil, "", &committeeservice.NotFoundError{Message: "legacy settings absent"}
					}
					return nil, "", errors.New("synthetic settings outage")
				}
			case "base unavailable", "base deleted":
				fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
					if kind == "base deleted" {
						return nil, "", &committeeservice.NotFoundError{Message: "deleted"}
					}
					return nil, "", errors.New("synthetic base outage")
				}
			}
			err := validateCommitteeCreatePayload(t.Context(), "committee-uid", "project-sfid", committeeCreateFingerprint(recoveryTestPayload()))
			if kind == "normalized" || kind == "settings missing" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("read failure accepted")
			}
			if kind == "base deleted" && !errors.Is(err, errCommitteeCreateManual) {
				t.Fatal(err)
			}
			if len(h.recordedRequests()) != 0 {
				t.Fatal("read-only validation wrote to v1")
			}
		})
	}
}
