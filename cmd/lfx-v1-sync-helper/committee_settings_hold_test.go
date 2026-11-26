// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/patrickmn/go-cache"
)

func seedSettingsHold(t *testing.T, store *fakeMappingStore, hold committeeSettingsHold) {
	t.Helper()
	if hold.CommitteeUID == "" {
		hold.CommitteeUID = "committee-uid"
	}
	value, err := json.Marshal(hold)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), committeeSettingsHoldKey(hold.CommitteeUID), value); err != nil {
		t.Fatal(err)
	}
}

func TestCommitteeSettingHeld(t *testing.T) {
	recent := time.Now().Add(-time.Minute)
	atGrace := time.Now().Add(-settingsHoldGrace)
	old := time.Now().Add(-settingsHoldGrace - time.Minute)
	votingOnly := []string{holdFieldEnableVoting}
	for _, tt := range []struct {
		name    string
		hold    *committeeSettingsHold
		raw     string
		getErr  error
		field   string
		want    bool
		wantErr bool
	}{
		{name: "no hold"},
		{name: "captured", hold: &committeeSettingsHold{State: holdStateCaptured, RelaxedFields: votingOnly}, want: true},
		{name: "relaxed", hold: &committeeSettingsHold{State: holdStateRelaxed, RelaxedFields: votingOnly}, want: true},
		{name: "relaxed field not held", hold: &committeeSettingsHold{State: holdStateRelaxed, RelaxedFields: votingOnly}, field: holdFieldBusinessEmailRequired},
		{name: "no recorded fields holds both", hold: &committeeSettingsHold{State: holdStateRelaxed}, field: holdFieldBusinessEmailRequired, want: true},
		{name: "restored within grace", hold: &committeeSettingsHold{State: holdStateRestored, RestoredAt: &recent, RelaxedFields: votingOnly}, want: true},
		{name: "restored at grace boundary", hold: &committeeSettingsHold{State: holdStateRestored, RestoredAt: &atGrace, RelaxedFields: votingOnly}},
		{name: "restored after grace", hold: &committeeSettingsHold{State: holdStateRestored, RestoredAt: &old, RelaxedFields: votingOnly}},
		{name: "restored without timestamp", hold: &committeeSettingsHold{State: holdStateRestored, RelaxedFields: votingOnly}},
		{name: "unknown state fails closed", hold: &committeeSettingsHold{State: "other", RelaxedFields: votingOnly}, field: holdFieldBusinessEmailRequired, want: true},
		{name: "undecodable", raw: "{", wantErr: true},
		{name: "store error", getErr: errors.New("nats down"), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			if tt.hold != nil {
				seedSettingsHold(t, h.store, *tt.hold)
			}
			if tt.raw != "" {
				h.seed(t, committeeSettingsHoldKey("committee-uid"), tt.raw)
			}
			field := tt.field
			if field == "" {
				field = holdFieldEnableVoting
			}
			h.store.nextGetErr = tt.getErr
			got, err := committeeSettingHeld(t.Context(), "committee-uid", field)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !isTransientStoreErr(err) {
				t.Errorf("error %v must be transient so the event is redelivered", err)
			}
			if got != tt.want {
				t.Errorf("held = %v, want %v", got, tt.want)
			}
		})
	}
}

// heldBusinessEmail is a hold that relaxed only business_email_required.
var heldBusinessEmail = committeeSettingsHold{State: holdStateRelaxed, RelaxedFields: []string{holdFieldBusinessEmailRequired}}

func TestSettingsHoldV1SettingsSyncKeepsOtherSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		v1       map[string]any
		wantPut  bool
		wantBody map[string]any
	}{
		{
			name:     "social listening still syncs",
			v1:       map[string]any{"business_email_required__c": true, "members_can_view_social_listening__c": true},
			wantPut:  true,
			wantBody: map[string]any{"business_email_required": false, "members_can_view_social_listening": true},
		},
		{
			name: "held field change alone is not synced",
			v1:   map[string]any{"business_email_required__c": true, "members_can_view_social_listening__c": false},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			seedSettingsHold(t, h.store, heldBusinessEmail)
			tt.v1["sfid"] = "committee-sfid"
			fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return tt.v1, true, nil }
			var body map[string]any
			setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					w.Header().Set("ETag", "42")
					_ = json.NewEncoder(w).Encode(settingsResponseWith(false, false))
					return
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				_ = json.NewEncoder(w).Encode(settingsResponseWith(false, true))
			})
			if err := updateCommitteeSettingsFromV1(t.Context(), "committee-uid", tt.v1, ""); err != nil {
				t.Fatal(err)
			}
			if (body != nil) != tt.wantPut {
				t.Fatalf("settings PUT = %v, want %v", body, tt.wantPut)
			}
			for k, v := range tt.wantBody {
				if body[k] != v {
					t.Errorf("%s = %v, want %v", k, body[k], v)
				}
			}
		})
	}

	t.Run("hold lookup failure is retried", func(t *testing.T) {
		setupSyncTest(t, 200, `{}`)
		v1 := map[string]any{"sfid": "committee-sfid", "business_email_required__c": true}
		fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return v1, true, nil }
		orig := committeeSettingHeld
		committeeSettingHeld = func(context.Context, string, string) (bool, error) {
			return false, wrapTransientStoreErr(errors.New("nats down"))
		}
		t.Cleanup(func() { committeeSettingHeld = orig })
		err := updateCommitteeSettingsFromV1(t.Context(), "committee-uid", v1, "")
		if err == nil || !isTransientStoreErr(err) {
			t.Errorf("err = %v, want a transient error", err)
		}
	})
}

func TestSettingsHoldV2SettingsEventOmitsHeldField(t *testing.T) {
	for _, echo := range []bool{false, true} {
		t.Run(map[bool]string{false: "not echo", true: "echo"}[echo], func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
			seedSettingsHold(t, h.store, heldBusinessEmail)
			fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
				return &committeeservice.CommitteeSettingsWithReadonlyAttributes{MembersCanViewSocialListening: true}, "42", nil
			}
			if echo {
				writePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid")
			}
			msg := dispatchSettings(t, "updated", map[string]any{"business_email_required": false})
			if !msg.acked {
				t.Fatal("expected ACK")
			}
			requests := h.recordedRequests()
			if echo {
				if len(requests) != 0 {
					t.Errorf("an echo must not PATCH v1: %+v", requests)
				}
				if consumePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid") {
					t.Error("the echo must still consume its marker")
				}
				return
			}
			assertSyncRequest(t, h, http.MethodPatch, "/project-service/v2/projects/project-sfid/committees/committee-sfid",
				map[string]any{"MembersCanViewSocialListening": true})
		})
	}

	t.Run("hold lookup failure is retried", func(t *testing.T) {
		h := setupSyncTest(t, 200, `{}`)
		h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
		orig := committeeSettingHeld
		committeeSettingHeld = func(context.Context, string, string) (bool, error) {
			return false, wrapTransientStoreErr(errors.New("nats down"))
		}
		t.Cleanup(func() { committeeSettingHeld = orig })
		if msg := dispatchSettings(t, "updated", map[string]any{"business_email_required": false}); !msg.nacked {
			t.Error("expected NAK")
		}
		if requests := h.recordedRequests(); len(requests) != 0 {
			t.Errorf("unexpected v1 requests: %+v", requests)
		}
	})
}

func TestSettingsHoldOmitsEnableVotingFromV1Update(t *testing.T) {
	for _, tt := range []struct {
		name     string
		hold     *committeeSettingsHold
		wantSent bool
	}{
		{name: "not held", wantSent: true},
		{name: "enable_voting held", hold: &committeeSettingsHold{State: holdStateRelaxed, RelaxedFields: []string{holdFieldEnableVoting}}},
		{name: "only business email held", hold: &heldBusinessEmail, wantSent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			stubSettingsBase(t, "project-uid", nil)
			if tt.hold != nil {
				seedSettingsHold(t, h.store, *tt.hold)
			}
			data := map[string]any{"name": "Committee", "enable_voting": false}
			if err := syncCommitteeUpdateToV1(t.Context(), "committee-uid", "project-a", "committee-sfid", data); err != nil {
				t.Fatal(err)
			}
			requests := h.recordedRequests()
			if len(requests) != 1 {
				t.Fatalf("v1 update requests = %+v", requests)
			}
			if _, sent := requests[0].Body["EnableVoting"]; sent != tt.wantSent {
				t.Errorf("EnableVoting sent = %v, want %v", sent, tt.wantSent)
			}
			if requests[0].Body["Name"] != "Committee" {
				t.Errorf("other fields must still sync: %+v", requests[0].Body)
			}
		})
	}

	t.Run("hold lookup failure is retried", func(t *testing.T) {
		h := setupSyncTest(t, 200, `{}`)
		orig := committeeHasSettingsHold
		committeeHasSettingsHold = func(context.Context, string) (bool, error) {
			return false, wrapTransientStoreErr(errors.New("nats down"))
		}
		t.Cleanup(func() { committeeHasSettingsHold = orig })
		if err := syncCommitteeUpdateToV1(t.Context(), "committee-uid", "project-a", "committee-sfid", map[string]any{"name": "Committee", "enable_voting": false}); err == nil {
			t.Error("expected an error for redelivery")
		}
		if requests := h.recordedRequests(); len(requests) != 0 {
			t.Errorf("unexpected v1 requests: %+v", requests)
		}
	})
}

func TestSettingsHoldKeepsV2EnableVotingOnV1BaseUpdate(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "not held", true: "held"}[held], func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			h.seed(t, "project.sfid.project-sfid", "project-uid")
			if held {
				seedSettingsHold(t, h.store, committeeSettingsHold{State: holdStateRelaxed, RelaxedFields: []string{holdFieldEnableVoting}})
			}
			origFetch, origClient, origCache := fetchCommitteeBase, committeeClient, jwtTokenCache
			t.Cleanup(func() { fetchCommitteeBase, committeeClient, jwtTokenCache = origFetch, origClient, origCache })
			fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
				return &committeeservice.CommitteeBaseWithReadonlyAttributes{
					UID: sptr("committee-uid"), ProjectUID: sptr("project-uid"), Name: sptr("Committee"), Category: sptr("Other"),
				}, "etag", nil
			}
			jwtTokenCache = cache.New(time.Minute, time.Minute)
			jwtTokenCache.Set("jwt-"+committeeServiceAudience+"-", "test-token", cache.DefaultExpiration)
			var sent *committeeservice.UpdateCommitteeBasePayload
			committeeClient = &committeeservice.Client{
				UpdateCommitteeBaseEndpoint: func(_ context.Context, req any) (any, error) {
					sent = req.(*committeeservice.UpdateCommitteeBasePayload)
					return &committeeservice.CommitteeBaseWithReadonlyAttributes{}, nil
				},
			}
			v1Data := map[string]any{"sfid": "committee-sfid", "mailing_list__c": "Committee", "project_name__c": "project-sfid", "type__c": "Other", "enable_voting__c": true}
			mutated, _, err := updateCommittee(t.Context(), "committee-uid", v1Data, "")
			if err != nil {
				t.Fatal(err)
			}
			if mutated == held {
				t.Errorf("mutated = %v with held = %v", mutated, held)
			}
			if !held && (sent == nil || !sent.EnableVoting) {
				t.Errorf("an unheld committee must take v1's enable_voting: %+v", sent)
			}
		})
	}
}

// TestSettingsHoldRequestShapes guards the settings PUT the backfill sends:
// writers and auditors must stay out so no access email is sent.
func TestSettingsHoldRequestShapes(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	var body map[string]any
	setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			w.Header().Set("ETag", "42")
			_ = json.NewEncoder(w).Encode(settingsResponseWith(true, true))
			return
		}
		if r.Header.Get("If-Match") != "42" {
			t.Errorf("If-Match = %q", r.Header.Get("If-Match"))
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		_ = json.NewEncoder(w).Encode(settingsResponseWith(false, true))
	})
	origUpdate := updateCommitteeSettingsFn
	updateCommitteeSettingsFn = updateCommitteeSettingsAPI
	t.Cleanup(func() { updateCommitteeSettingsFn = origUpdate })

	if err := writeCommitteeSetting(t.Context(), "committee-uid", holdFieldBusinessEmailRequired, false, false); err != nil {
		t.Fatal(err)
	}
	if body["business_email_required"] != false || body["members_can_view_social_listening"] != true {
		t.Errorf("unexpected settings body: %+v", body)
	}
	for _, field := range []string{"writers", "auditors", "chat_webhook_url"} {
		if _, ok := body[field]; ok {
			t.Errorf("settings body must omit %s: %+v", field, body)
		}
	}
	if !consumePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid") {
		t.Error("the settings write must leave a v1_to_v2 marker for its echo")
	}
	_ = h
}

func TestSettingsHoldRecheckedAfterSettingsRead(t *testing.T) {
	// The backfill writes its hold and relaxes business_email_required between
	// a sync's first look and its settings read. The read sees the relaxed
	// value, so the hold must be checked after it.
	relaxDuringRead := func(t *testing.T, h *syncTestHarness) {
		fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
			seedSettingsHold(t, h.store, heldBusinessEmail)
			return &committeeservice.CommitteeSettingsWithReadonlyAttributes{MembersCanViewSocialListening: true, MemberVisibility: "hidden"}, "42", nil
		}
	}

	t.Run("v1 to v2", func(t *testing.T) {
		h := setupSyncTest(t, 200, `{}`)
		v1 := map[string]any{"sfid": "committee-sfid", "business_email_required__c": true, "members_can_view_social_listening__c": true}
		fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return v1, true, nil }
		relaxDuringRead(t, h)
		origClient := committeeClient
		t.Cleanup(func() { committeeClient = origClient })
		committeeClient = &committeeservice.Client{UpdateCommitteeSettingsEndpoint: func(context.Context, any) (any, error) {
			t.Error("v1's business_email_required must not overwrite the relaxed value")
			return nil, nil
		}}
		if err := updateCommitteeSettingsFromV1(t.Context(), "committee-uid", v1, ""); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("v2 to v1", func(t *testing.T) {
		h := setupSyncTest(t, 200, `{}`)
		h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
		relaxDuringRead(t, h)
		if msg := dispatchSettings(t, "updated", map[string]any{"business_email_required": false}); !msg.acked {
			t.Fatal("expected ACK")
		}
		assertSyncRequest(t, h, http.MethodPatch, "/project-service/v2/projects/project-sfid/committees/committee-sfid",
			map[string]any{"MembersCanViewSocialListening": true})
	})
}

func TestSettingsHoldDelayedEnableVotingEvent(t *testing.T) {
	// The stream retains events for weeks: a committee the backfill ever held
	// sends its current v2 enable_voting, not the event snapshot.
	old := time.Now().Add(-24 * time.Hour)
	for _, tt := range []struct {
		name    string
		hold    *committeeSettingsHold
		current *bool // nil: the committee was deleted from v2
		want    any   // nil: EnableVoting omitted
	}{
		{name: "never held sends the snapshot", want: false},
		{name: "restored long ago sends current v2", hold: &committeeSettingsHold{State: holdStateRestored, RestoredAt: &old, RelaxedFields: []string{holdFieldEnableVoting}}, current: ptrBool(true), want: true},
		{name: "deleted committee omits it", hold: &committeeSettingsHold{State: holdStateRestored, RestoredAt: &old, RelaxedFields: []string{holdFieldEnableVoting}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			if tt.hold != nil {
				seedSettingsHold(t, h.store, *tt.hold)
			}
			orig := fetchCommitteeBase
			t.Cleanup(func() { fetchCommitteeBase = orig })
			fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
				if tt.hold == nil {
					t.Error("a never-held committee must not read v2")
				}
				if tt.current == nil {
					return nil, "", &committeeservice.NotFoundError{Message: "gone"}
				}
				return &committeeservice.CommitteeBaseWithReadonlyAttributes{EnableVoting: *tt.current}, "etag", nil
			}
			data := map[string]any{"name": "Committee", "enable_voting": false} // the backfill's old "off" snapshot
			if err := syncCommitteeUpdateToV1(t.Context(), "committee-uid", "project-a", "committee-sfid", data); err != nil {
				t.Fatal(err)
			}
			requests := h.recordedRequests()
			if len(requests) != 1 {
				t.Fatalf("v1 update requests = %+v", requests)
			}
			if got := requests[0].Body["EnableVoting"]; got != tt.want {
				t.Errorf("EnableVoting = %v, want %v", got, tt.want)
			}
		})
	}
}

func ptrBool(v bool) *bool { return &v }
