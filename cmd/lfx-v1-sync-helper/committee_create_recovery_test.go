// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go"
)

func recoveryTestPayload() projectServiceCommitteeCreate {
	return projectServiceCommitteeCreate{Name: "Synthetic Committee", Category: "Other"}
}

func recoveryTestCandidate(start time.Time) committeeCreateCandidate {
	created := start.Add(time.Second)
	return committeeCreateCandidate{
		ID: "committee-sfid", CreatedAt: created, ModifiedAt: created,
		CreatedBy: "synthetic-sync-client@clients", Payload: recoveryTestPayload(),
	}
}

func recoveryTestState(t *testing.T) *committeeCreateState {
	t.Helper()
	state := &committeeCreateState{Now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Primary: true, NormalizedName: "synthetic committee"}
	readCommitteeCreateState = func(ctx context.Context, project, name string) (committeeCreateState, error) {
		if project != "project-sfid" || name != "Synthetic Committee" {
			t.Errorf("unexpected recovery scope: %q / %q", project, name)
		}
		if err := ctx.Err(); err != nil {
			return committeeCreateState{}, err
		}
		return *state, nil
	}
	return state
}

func TestCommitteeCreateRecoveryLostResponse(t *testing.T) {
	// An unreadable 201 is ambiguous even though v1 has committed the insert.
	h := setupSyncTest(t, 201, `{"ID":`)
	state := recoveryTestState(t)
	start := state.Now
	call := func() error {
		return createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
	}
	if err := call(); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatalf("first create = %v, want pending recovery", err)
	}
	attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
	if err != nil || !attempt.StartedAt.Equal(start) || attempt.V1ID != "" {
		t.Fatalf("attempt = %+v, %v", attempt, err)
	}
	entry, _ := h.store.Get(t.Context(), committeeCreateAttemptKey("committee-uid"))
	if strings.Contains(string(entry.Value), "Synthetic Committee") || strings.Contains(string(entry.Value), "synthetic-sync-client") {
		t.Fatal("attempt must retain fingerprints, not raw payload or creator")
	}
	state.Candidates = []committeeCreateCandidate{recoveryTestCandidate(start)}
	state.Now = start.Add(30 * time.Second)
	if err := call(); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatalf("delivery during original POST budget = %v", err)
	}
	state.Now = start.Add(71 * time.Second)
	if err := call(); err != nil {
		t.Fatal(err)
	}
	h.assertMapping(t, "committee.sfid.committee-sfid", "committee-uid")
	h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	// A later replay uses the recorded ID, not another POST, even if the
	// successfully linked committee has since acquired members.
	state.Candidates[0].Members = 3
	if err := call(); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("recovery/replay must never repeat the create POST")
	}
}

func TestCommitteeCreateRecoveryRejectsUnsafeCandidates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*committeeCreateCandidate)
	}{
		{"older committee", func(c *committeeCreateCandidate) {
			c.CreatedAt = c.CreatedAt.Add(-time.Hour)
			c.ModifiedAt = c.CreatedAt
		}},
		{"outside create window", func(c *committeeCreateCandidate) {
			c.CreatedAt = c.CreatedAt.Add(time.Hour)
			c.ModifiedAt = c.CreatedAt
		}},
		{"different creator", func(c *committeeCreateCandidate) { c.CreatedBy = "different-client@clients" }},
		{"subsequent edit", func(c *committeeCreateCandidate) { c.ModifiedAt = c.ModifiedAt.Add(time.Second) }},
		{"member history", func(c *committeeCreateCandidate) { c.Members = 1 }},
		{"deleted", func(c *committeeCreateCandidate) { c.Deleted = true }},
		{"missing ID", func(c *committeeCreateCandidate) { c.ID = "" }},
		{"different category", func(c *committeeCreateCandidate) { c.Payload.Category = "Board" }},
		{"different parent", func(c *committeeCreateCandidate) { c.Payload.CommitteeID = "other-parent" }},
		{"different name", func(c *committeeCreateCandidate) { c.Payload.Name = "Different Committee" }},
		{"different voting", func(c *committeeCreateCandidate) { c.Payload.EnableVoting = ptr(true) }},
		{"different visibility", func(c *committeeCreateCandidate) { c.Payload.PublicEnabled = ptr(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupSyncTest(t, 409, `{"message":"already exists"}`)
			state := recoveryTestState(t)
			start := state.Now
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
				t.Fatal(err)
			}
			candidate := recoveryTestCandidate(start)
			tc.mutate(&candidate)
			state.Now, state.Candidates = start.Add(71*time.Second), []committeeCreateCandidate{candidate}
			err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
			if !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("unsafe candidate = %v, want manual reconciliation", err)
			}
			if _, err := h.store.Get(t.Context(), "committee.uid.committee-uid"); !errors.Is(err, ErrKeyNotFound) {
				t.Fatal("unsafe candidate acquired a mapping")
			}
			if len(h.recordedRequests()) != 1 {
				t.Fatal("unsafe candidate must not trigger another POST")
			}
		})
	}
}

func TestCommitteeCreateRecoveryPreflightAndMissingCandidate(t *testing.T) {
	for _, test := range []string{"existing", "replica", "unavailable", "multiple", "missing", "changed request", "v2 deleted"} {
		t.Run(test, func(t *testing.T) {
			h := setupSyncTest(t, 503, `{}`)
			state := recoveryTestState(t)
			start := state.Now
			payload := recoveryTestPayload()
			switch test {
			case "existing":
				state.Candidates = []committeeCreateCandidate{recoveryTestCandidate(start)}
			case "replica":
				state.Primary = false
			case "unavailable":
				readCommitteeCreateState = func(context.Context, string, string) (committeeCreateState, error) {
					return committeeCreateState{}, errors.New("synthetic database unavailable")
				}
			}
			err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", payload)
			if err == nil {
				t.Fatal("preflight/ambiguous response unexpectedly succeeded")
			}
			if test == "existing" || test == "replica" || test == "unavailable" {
				if len(h.recordedRequests()) != 0 {
					t.Fatal("unsafe preflight must not issue POST")
				}
				return
			}
			state.Now = start.Add(71 * time.Second)
			state.Candidates = []committeeCreateCandidate{recoveryTestCandidate(start)}
			switch test {
			case "multiple":
				state.Candidates = append(state.Candidates, state.Candidates[0])
			case "missing":
				state.Candidates = nil
			case "changed request":
				payload.Category = "Board"
			case "v2 deleted":
				checkCommitteeCreateV2 = func(context.Context, string, string, string, string) error { return errCommitteeCreateManual }
			}
			err = createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", payload)
			want := errCommitteeCreateManual
			if test == "missing" {
				want = errCommitteeCreatePending
			}
			if !errors.Is(err, want) || len(h.recordedRequests()) != 1 {
				t.Fatalf("recovery = %v, requests=%d", err, len(h.recordedRequests()))
			}
		})
	}
}

type failingCommitteeClaimStore struct {
	MappingStore
	key string
}

func (s *failingCommitteeClaimStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if key == s.key {
		s.key = ""
		return 0, errors.New("synthetic mapping write failure")
	}
	return s.MappingStore.Create(ctx, key, value)
}

func TestCommitteeCreateRecoveryPartialMappings(t *testing.T) {
	for _, key := range []string{"committee.sfid.committee-sfid", "committee.uid.committee-uid"} {
		t.Run(key, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			state := recoveryTestState(t)
			mappingStore = &failingCommitteeClaimStore{MappingStore: h.store, key: key}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err == nil {
				t.Fatal("expected partial mapping write failure")
			}
			state.Candidates = []committeeCreateCandidate{recoveryTestCandidate(state.Now)}
			if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
				t.Fatal(err)
			}
			h.assertMapping(t, "committee.sfid.committee-sfid", "committee-uid")
			h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
			if len(h.recordedRequests()) != 1 {
				t.Fatal("mapping recovery repeated POST")
			}
		})
	}
}

func TestCommitteeCreateMappingOwnership(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"committee.sfid.committee-sfid", "another-uid"},
		{"committee.sfid.committee-sfid", tombstoneMarker},
		{"committee.uid.committee-uid", "other-project:other-committee"},
		{"committee.uid.committee-uid", tombstoneMarker},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{}`)
			h.seed(t, tc.key, tc.value)
			if err := completeCommitteeCreateMapping(t.Context(), "committee-uid", "project-sfid", "committee-sfid"); !errors.Is(err, errCommitteeCreateManual) {
				t.Fatal(err)
			}
			h.assertMapping(t, tc.key, tc.value)
			if h.store.createCalls != 0 {
				t.Fatal("conflicting/tombstoned mapping must block all claims")
			}
		})
	}
}

type recoveryGateTransport struct {
	started chan struct{}
	release <-chan struct{}
	base    http.RoundTripper
}

func (r *recoveryGateTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	close(r.started)
	select {
	case <-r.release:
		return r.base.RoundTrip(req)
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

func TestCommitteeCreateRecoveryConcurrentDelivery(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	release, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	v1HTTPClient.Transport = &recoveryGateTransport{started: started, release: release, base: v1HTTPClient.Transport}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- createOrRecoverCommittee(ctx, "committee-uid", "project-sfid", recoveryTestPayload())
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first POST did not start")
	}
	if err := createOrRecoverCommittee(ctx, "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) {
		t.Fatalf("concurrent delivery = %v", err)
	}
	once.Do(func() { close(release) })
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("concurrent delivery duplicated POST")
	}
}

func TestCommitteeCreateRecoveryFingerprintNormalization(t *testing.T) {
	a := recoveryTestPayload()
	b := a
	b.SSOGroupEnabled, b.PublicEnabled, b.EnableVoting, b.BusinessEmailRequired = ptr(false), ptr(false), ptr(false), ptr(false)
	b.SSOGroupName, b.PublicName = "server-generated-name", "ignored-while-private"
	if committeeCreateFingerprint(a) != committeeCreateFingerprint(b) {
		t.Fatal("documented defaults must compare equally")
	}
	data, err := json.Marshal(a)
	if err != nil || len(data) == 0 {
		t.Fatal(err)
	}
}

func TestCommitteeCreateRecoveryRepairsForwardBehindLoopGuard(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
		t.Fatal(err)
	}
	if err := h.store.Delete(t.Context(), "committee.sfid.committee-sfid"); err != nil {
		t.Fatal(err)
	}
	if err := syncCommitteeCreateToV1(t.Context(), "committee-uid", "project-sfid", map[string]any{"name": "Synthetic Committee"}); err != nil {
		t.Fatal(err)
	}
	h.assertMapping(t, "committee.sfid.committee-sfid", "committee-uid")
	if len(h.recordedRequests()) != 1 {
		t.Fatal("partial forward mapping recovery must not repeat POST")
	}
}

func TestCommitteeCreateRecoveryDeliveryPolicy(t *testing.T) {
	var output bytes.Buffer
	original := logger
	logger = slog.New(slog.NewJSONHandler(&output, nil))
	t.Cleanup(func() { logger = original })
	for _, subject := range []string{"lfx.committee.created", "lfx.committee_member.created", "lfx.committee_settings.created"} {
		manual := func(context.Context, string, []byte) error { return errCommitteeCreateManual }
		msg := &fakeMsg{subject: subject}
		newCommitteeEventsIngestHandler(manual, manual, manual)(msg)
		if msg.ackCalls != 1 || msg.nakCalls != 0 {
			t.Fatal("explicit manual-reconciliation decisions must ACK after surfacing the error")
		}
		pending := func(context.Context, string, []byte) error { return errCommitteeCreatePending }
		msg = &fakeMsg{subject: subject, deliveries: 3}
		newCommitteeEventsIngestHandler(pending, pending, pending)(msg)
		if msg.ackCalls != 0 || msg.nakCalls != 1 || msg.nakDelay != 70*time.Second {
			t.Fatal("pending recovery must use delayed NAK, including the last delivery")
		}
	}
	if !strings.Contains(output.String(), "retries exhausted; manual reconciliation required") ||
		!strings.Contains(output.String(), "committee create rejected for manual reconciliation") {
		t.Fatal("permanent rejection and exhausted retries must be operationally visible")
	}
}

func TestCommitteeMemberWaitsForRecordedCreate(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		t.Run(map[bool]string{false: "untracked", true: "pending create"}[recorded], func(t *testing.T) {
			h := setupSyncTest(t, 201, `{}`)
			if recorded {
				attempt := committeeCreateAttempt{
					Version: 1, OperationID: "11111111-2222-4333-8444-555555555555", ProjectSFID: "project-sfid",
					Fingerprint: committeeCreateHash([]byte("synthetic payload")), CreatorHash: committeeCreateHash([]byte("synthetic creator")), StartedAt: time.Now(),
				}
				value, _ := json.Marshal(attempt)
				h.seed(t, committeeCreateAttemptKey("committee-uid"), string(value))
			}
			data, _ := json.Marshal(indexingEvent{
				ObjectID: "member-uid", ObjectType: "committee_member", Action: "created",
				Body: json.RawMessage(`{"data":{"committee_uid":"committee-uid"}}`),
			})
			msg := &fakeMsg{subject: "lfx.committee_member.created", data: data}
			newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent, processCommitteeSettingsIndexingEvent)(msg)
			if recorded && (!msg.nacked || msg.nakDelay != 70*time.Second) {
				t.Fatal("member must wait for its tracked committee create to recover")
			}
			if !recorded && !msg.acked {
				t.Fatal("untracked/v2-only committees retain their permanent skip behavior")
			}
			if len(h.recordedRequests()) != 0 {
				t.Fatal("missing parent mapping must not issue member POST")
			}
		})
	}
}

type canceledCommitteeResponseTransport struct {
	base   http.RoundTripper
	cancel context.CancelFunc
}

func (r canceledCommitteeResponseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	_ = resp.Body.Close()
	r.cancel()
	return nil, context.Canceled
}

func TestCommitteeCreateRecoveryUsesFreshContext(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	state := recoveryTestState(t)
	start := state.Now
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	v1HTTPClient.Transport = canceledCommitteeResponseTransport{base: v1HTTPClient.Transport, cancel: cancel}
	if err := createOrRecoverCommittee(ctx, "committee-uid", "project-sfid", recoveryTestPayload()); !errors.Is(err, errCommitteeCreatePending) || ctx.Err() == nil {
		t.Fatalf("canceled response = %v, context error = %v", err, ctx.Err())
	}
	state.Now, state.Candidates = start.Add(71*time.Second), []committeeCreateCandidate{recoveryTestCandidate(start)}
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
		t.Fatal(err)
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("fresh delivery must reconcile, not POST again")
	}
	h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
}

func TestCommitteeCreateRecoveryUsesDualStore(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	shadow := newFakeMappingStore()
	dual := newDualMappingStore(shadow, h.store, discardLogger())
	t.Cleanup(func() { _ = dual.Close() })
	mappingStore = dual
	if err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload()); err != nil {
		t.Fatal(err)
	}
	if err := dual.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{committeeCreateAttemptKey("committee-uid"), "committee.sfid.committee-sfid", "committee.uid.committee-uid"} {
		kv, kvErr := h.store.Get(t.Context(), key)
		pg, pgErr := shadow.Get(t.Context(), key)
		if kvErr != nil || pgErr != nil || string(kv.Value) != string(pg.Value) {
			t.Fatalf("dual-store state differs for %s: %v / %v", key, kvErr, pgErr)
		}
	}
}

func TestCommitteeCreateRecoveryInvalidAttemptAndRejectedRequest(t *testing.T) {
	for _, name := range []string{"corrupt attempt", "attempt write fails", "v1 rejects request"} {
		t.Run(name, func(t *testing.T) {
			h := setupSyncTest(t, 422, `{"message":"invalid request"}`)
			recoveryTestState(t)
			switch name {
			case "corrupt attempt":
				h.seed(t, committeeCreateAttemptKey("committee-uid"), `{"operation_id":"unknown"}`)
			case "attempt write fails":
				h.store.nextCreateErr = errors.New("synthetic attempt storage failure")
			}
			err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
			if err == nil {
				t.Fatal("expected create rejection")
			}
			requests := 0
			if name == "v1 rejects request" {
				requests = 1
			}
			if len(h.recordedRequests()) != requests {
				t.Fatal("POST must be fenced by durable attempt storage")
			}
			if name != "attempt write fails" && !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("invalid attempt/request = %v, want manual reconciliation", err)
			}
		})
	}
}

func TestCommitteeCreateRecoveryCurrentV2Identity(t *testing.T) {
	for _, name := range []string{"valid", "valid parent", "renamed", "deleted", "project changed", "project tombstoned", "parent changed", "parent wrong project", "different name owner", "name missing"} {
		t.Run(name, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{}`)
			base := &committeeservice.CommitteeBaseWithReadonlyAttributes{
				UID: ptr("committee-uid"), ProjectUID: ptr("project-uid"), Name: ptr("Synthetic Committee"),
			}
			originalBase, originalNATS := fetchCommitteeBase, natsConn
			t.Cleanup(func() { fetchCommitteeBase, natsConn = originalBase, originalNATS })
			fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
				if name == "deleted" {
					return nil, "", &committeeservice.NotFoundError{Message: "synthetic missing committee"}
				}
				return base, "revision", nil
			}
			h.seed(t, "project.uid.project-uid", "project-sfid")
			switch name {
			case "renamed":
				base.Name = ptr("Different Committee")
			case "project changed":
				h.seed(t, "project.uid.project-uid", "other-project-sfid")
			case "project tombstoned":
				h.seed(t, "project.uid.project-uid", tombstoneMarker)
			case "parent changed", "valid parent":
				base.ParentUID = ptr("parent-uid")
				h.seed(t, "committee.uid.parent-uid", "project-sfid:parent-sfid")
			case "parent wrong project":
				base.ParentUID = ptr("parent-uid")
				h.seed(t, "committee.uid.parent-uid", "other-project:parent-sfid")
			}
			if name == "valid" || name == "valid parent" || name == "different name owner" || name == "name missing" {
				nc, err := nats.Connect(startCommitteeEventsTestBroker(t))
				if err != nil {
					t.Fatal(err)
				}
				natsConn = nc
				t.Cleanup(nc.Close)
				if _, err := nc.Subscribe("lfx.committee-api.name_to_uid", func(msg *nats.Msg) {
					owner := "committee-uid"
					switch name {
					case "different name owner":
						owner = "another-uid"
					case "name missing":
						owner = ""
					}
					response, _ := json.Marshal(committeeNameToUIDResponse{CommitteeUID: owner})
					_ = msg.Respond(response)
				}); err != nil {
					t.Fatal(err)
				}
			}
			parent := ""
			if name == "valid parent" {
				parent = "parent-sfid"
			}
			err := validateCommitteeCreateV2(t.Context(), "committee-uid", "project-sfid", "Synthetic Committee", parent)
			if name == "valid" || name == "valid parent" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errCommitteeCreateManual) {
				t.Fatalf("changed v2 identity = %v, want manual reconciliation", err)
			}
		})
	}
}

func TestCommitteeCreateRecoveryV2DeletedDuringPost(t *testing.T) {
	h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
	recoveryTestState(t)
	deleted := false
	checkCommitteeCreateV2 = func(context.Context, string, string, string, string) error {
		if deleted {
			return errCommitteeCreateManual
		}
		return nil
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	v1HTTPClient.Transport = &recoveryGateTransport{started: started, release: release, base: v1HTTPClient.Transport}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- createOrRecoverCommittee(ctx, "committee-uid", "project-sfid", recoveryTestPayload())
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("POST did not start")
	}
	deleted = true // The request/release channels order all reads of this flag.
	once.Do(func() { close(release) })
	if err := <-result; !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("deleted native committee = %v", err)
	}
	if _, err := h.store.Get(t.Context(), "committee.uid.committee-uid"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal("deleted native committee acquired a mapping")
	}
	attempt, _, err := readCommitteeCreateAttempt(t.Context(), "committee-uid")
	if err != nil || attempt.V1ID != "committee-sfid" {
		t.Fatal("retain the committed v1 result for manual cleanup")
	}
}

func TestCommitteeCreateRecoverySiblingCommitsDuringSnapshot(t *testing.T) {
	h := setupSyncTest(t, 201, `{}`)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	readCommitteeCreateState = func(context.Context, string, string) (committeeCreateState, error) {
		attempt := committeeCreateAttempt{
			Version: 1, OperationID: "11111111-2222-4333-8444-555555555555", ProjectSFID: "project-sfid",
			Fingerprint: committeeCreateFingerprint(recoveryTestPayload()), CreatorHash: committeeCreateHash([]byte("synthetic-sync-client@clients")), StartedAt: start,
		}
		data, _ := json.Marshal(attempt)
		h.seed(t, committeeCreateAttemptKey("committee-uid"), string(data))
		return committeeCreateState{Now: start, Primary: true, NormalizedName: "synthetic committee", Candidates: []committeeCreateCandidate{recoveryTestCandidate(start)}}, nil
	}
	err := createOrRecoverCommittee(t.Context(), "committee-uid", "project-sfid", recoveryTestPayload())
	if !errors.Is(err, errCommitteeCreatePending) || len(h.recordedRequests()) != 0 {
		t.Fatalf("sibling race = %v; must wait without POST or permanent rejection", err)
	}
}

type tombstoningCommitteeClaimStore struct{ MappingStore }

func (s tombstoningCommitteeClaimStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	revision, err := s.MappingStore.Create(ctx, key, value)
	if err == nil && key == "committee.uid.committee-uid" {
		_, err = s.Put(ctx, "committee.sfid.committee-sfid", []byte(tombstoneMarker))
	}
	return revision, err
}

func TestCommitteeCreateRecoveryDeleteRacesWithMappingCompletion(t *testing.T) {
	h := setupSyncTest(t, 201, `{}`)
	mappingStore = tombstoningCommitteeClaimStore{MappingStore: h.store}
	err := completeCommitteeCreateMapping(t.Context(), "committee-uid", "project-sfid", "committee-sfid")
	if !errors.Is(err, errCommitteeCreateManual) {
		t.Fatalf("concurrent tombstone = %v; must not report successful completion", err)
	}
	h.assertMapping(t, "committee.sfid.committee-sfid", tombstoneMarker)
}
