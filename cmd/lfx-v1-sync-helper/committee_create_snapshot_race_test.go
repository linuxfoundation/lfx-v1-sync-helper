// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type committeeResultWriteFailureStore struct {
	MappingStore
	failed bool
}

func (s *committeeResultWriteFailureStore) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !s.failed && key == committeeCreateAttemptKey("committee-uid") && strings.Contains(string(value), `"v1_id"`) {
		s.failed = true
		return 0, errors.New("synthetic result persistence failure after peer POST")
	}
	return s.MappingStore.Update(ctx, key, value, revision)
}

func seedPreparedCommitteeDelivery(t *testing.T, h *syncTestHarness, state *committeeCreateState) ([]byte, committeeCreateAttempt) {
	t.Helper()
	h.seed(t, "project.uid.project-uid", "project-sfid")
	attempt := committeeCreateAttempt{
		Version: 3, Phase: "prepared", ScopeKey: committeeScopeKey("project-sfid", state.NormalizedName),
		OperationID: "11111111-2222-4333-8444-555555555555", ProjectSFID: "project-sfid", StartedAt: state.Now,
		Fingerprint: committeeCreateFingerprint(recoveryTestPayload()), CreatorHash: committeeCreateHash([]byte("synthetic-sync-client@clients")),
	}
	encoded, _ := json.Marshal(attempt)
	h.seed(t, committeeCreateAttemptKey("committee-uid"), string(encoded))
	if err := claimCommitteeCreateScope(t.Context(), "committee-uid", &attempt); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(indexingEventBody{Data: map[string]any{"project_uid": "project-uid", "name": "Synthetic Committee", "category": "Other"}})
	event, _ := json.Marshal(indexingEvent{ObjectID: "committee-uid", Action: "created", Body: body})
	return event, attempt
}

func TestCommitteeDispatchSnapshotRaceKeepsRecoveryPending(t *testing.T) {
	for _, failure := range []string{"result", "forward mapping", "reverse mapping"} {
		t.Run(failure, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			state := recoveryTestState(t)
			start := state.Now
			event, _ := seedPreparedCommitteeDelivery(t, h, state)
			switch failure {
			case "result":
				mappingStore = &committeeResultWriteFailureStore{MappingStore: h.store}
			case "forward mapping":
				mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee.sfid.committee-sfid"}
			case "reverse mapping":
				mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: "committee.uid.committee-uid"}
			}
			handler := newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)
			read, reads := readCommitteeCreateState, 0
			readCommitteeCreateState = func(ctx context.Context, project, name string) (committeeCreateState, error) {
				reads++
				if reads == 2 {
					// The outer delivery still holds prepared revision 1. Its peer
					// dispatches/commits while this second snapshot is in flight,
					// then NAKs because result or mapping persistence fails.
					peer := &fakeMsg{subject: "lfx.committee.created", data: event, deliveries: 1}
					handler(peer)
					if peer.nakCalls != 1 || peer.ackCalls != 0 {
						t.Fatalf("peer = %+v", peer)
					}
					state.Now, state.Candidates = start.Add(2*time.Second), []committeeCreateCandidate{recoveryTestCandidate(start)}
				}
				return read(ctx, project, name)
			}
			stale := &fakeMsg{subject: "lfx.committee.created", data: event, deliveries: 2}
			handler(stale)
			if stale.ackCalls != 0 || stale.nakCalls != 1 || stale.nakDelay != committeeEventsMappingRetryDelay {
				t.Fatalf("stale delivery must delay recovery, not ACK: %+v", stale)
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("expected exactly one committed POST")
			}
			state.Now = start.Add(71 * time.Second)
			retry := &fakeMsg{subject: "lfx.committee.created", data: event, deliveries: 3}
			handler(retry)
			if retry.ackCalls != 1 || retry.nakCalls != 0 {
				t.Fatalf("recovery delivery = %+v", retry)
			}
			h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
			h.assertMapping(t, "committee.sfid.committee-sfid", "committee-uid")
			if len(h.recordedRequests()) != 1 {
				t.Fatal("recovery repeated POST")
			}
		})
	}
}

func TestCommitteeDispatchSnapshotRecheckDoesNotHideConflicts(t *testing.T) {
	for _, kind := range []string{"unchanged", "different operation", "read unavailable"} {
		t.Run(kind, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{}`)
			state := recoveryTestState(t)
			event, attempt := seedPreparedCommitteeDelivery(t, h, state)
			read, reads := readCommitteeCreateState, 0
			readCommitteeCreateState = func(ctx context.Context, project, name string) (committeeCreateState, error) {
				reads++
				if reads == 2 {
					state.Candidates = []committeeCreateCandidate{recoveryTestCandidate(state.Now)}
					switch kind {
					case "different operation":
						attempt.OperationID = "22222222-2222-4333-8444-555555555555"
						data, _ := json.Marshal(attempt)
						h.seed(t, committeeCreateAttemptKey("committee-uid"), string(data))
					case "read unavailable":
						h.store.nextGetErr = errors.New("synthetic attempt reread outage")
					}
				}
				return read(ctx, project, name)
			}
			msg := &fakeMsg{subject: "lfx.committee.created", data: event, deliveries: 2}
			newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)(msg)
			if kind == "read unavailable" {
				if msg.nakCalls != 1 || msg.ackCalls != 0 {
					t.Fatalf("unavailable recheck must retry: %+v", msg)
				}
			} else if msg.ackCalls != 1 || msg.nakCalls != 0 {
				t.Fatalf("genuine/identity conflict must remain manual: %+v", msg)
			}
			if len(h.recordedRequests()) != 0 {
				t.Fatal("candidate/recheck allowed POST")
			}
		})
	}
}
