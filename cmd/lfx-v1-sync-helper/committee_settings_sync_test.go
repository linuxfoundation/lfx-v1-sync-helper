// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	committeeclient "github.com/linuxfoundation/lfx-v2-committee-service/gen/http/committee_service/client"
	"github.com/patrickmn/go-cache"
	goahttp "goa.design/goa/v3/http"
)

func setupSettingsClient(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := committeeclient.NewClient(u.Scheme, u.Host, srv.Client(), goahttp.RequestEncoder, goahttp.ResponseDecoder, false)
	origClient, origCache, origFetch := committeeClient, jwtTokenCache, fetchCommitteeSettings
	committeeClient = &committeeservice.Client{
		GetCommitteeSettingsEndpoint:    client.GetCommitteeSettings(),
		UpdateCommitteeSettingsEndpoint: client.UpdateCommitteeSettings(),
	}
	jwtTokenCache = cache.New(time.Minute, time.Minute)
	for _, principal := range []string{"", "test-principal"} {
		jwtTokenCache.Set("jwt-"+committeeServiceAudience+"-"+principal, "test-token", cache.DefaultExpiration)
	}
	fetchCommitteeSettings = fetchCommitteeSettingsAPI
	t.Cleanup(func() {
		committeeClient, jwtTokenCache, fetchCommitteeSettings = origClient, origCache, origFetch
	})
}

func settingsResponse(required bool) map[string]any {
	return map[string]any{
		"uid": "00000000-0000-4000-8000-000000000001", "business_email_required": required,
		"member_visibility": "hidden", "show_meeting_attendees": true,
		"last_reviewed_at": "2026-01-01T00:00:00Z", "last_reviewed_by": "test-reviewer",
		"writers":  []any{map[string]any{"username": "test-writer"}},
		"auditors": []any{map[string]any{"username": "test-auditor"}},
	}
}

func TestCommitteeSettingsFromV1(t *testing.T) {
	for _, tt := range []struct {
		name      string
		data      map[string]any
		current   bool
		getStatus int
		putStatus int
		noETag    bool
		wantPut   bool
		wantErr   bool
	}{
		{name: "enable", data: map[string]any{"business_email_required__c": true}, wantPut: true},
		{name: "disable", data: map[string]any{"business_email_required__c": false}, current: true, wantPut: true},
		{name: "unchanged", data: map[string]any{"business_email_required__c": true}, current: true},
		{name: "absent", data: map[string]any{}},
		{name: "null", data: map[string]any{"business_email_required__c": nil}},
		{name: "malformed", data: map[string]any{"business_email_required__c": "true"}},
		{name: "read failure", data: map[string]any{"business_email_required__c": true}, getStatus: 503, wantErr: true},
		{name: "settings missing", data: map[string]any{"business_email_required__c": true}, getStatus: 404},
		{name: "conflict", data: map[string]any{"business_email_required__c": true}, putStatus: 409, wantPut: true, wantErr: true},
		{name: "missing ETag", data: map[string]any{"business_email_required__c": true}, noETag: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			tt.data["sfid"] = "committee-sfid"
			fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) {
				return tt.data, true, nil
			}
			var gets, puts int
			setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("missing settings API authentication")
				}
				if r.Method == http.MethodGet {
					gets++
					if tt.getStatus != 0 {
						w.WriteHeader(tt.getStatus)
						_, _ = w.Write([]byte(`{"message":"settings unavailable"}`))
						return
					}
					if !tt.noETag {
						w.Header().Set("ETag", "42")
					}
					_ = json.NewEncoder(w).Encode(settingsResponse(tt.current))
					return
				}
				puts++
				if r.Method != http.MethodPut || r.URL.Path != "/committees/committee-uid/settings" || r.Header.Get("If-Match") != "42" {
					t.Errorf("unexpected settings write: %s %s If-Match=%q", r.Method, r.URL.Path, r.Header.Get("If-Match"))
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				want := map[string]any{
					"business_email_required": tt.data["business_email_required__c"],
					"member_visibility":       "hidden", "show_meeting_attendees": true,
					"last_reviewed_at": "2026-01-01T00:00:00Z", "last_reviewed_by": "test-reviewer",
				}
				assertUpdateJSON(t, body, want)
				if tt.putStatus != 0 {
					w.WriteHeader(tt.putStatus)
					_, _ = w.Write([]byte(`{"message":"settings changed concurrently"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(settingsResponse(!tt.current))
			})
			err := updateCommitteeSettingsFromV1(t.Context(), "committee-uid", tt.data, "test-principal")
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			wantPuts := 0
			if tt.wantPut {
				wantPuts = 1
			}
			if puts != wantPuts {
				t.Errorf("PUTs = %d, want %d", puts, wantPuts)
			}
			if _, present := tt.data["business_email_required__c"].(bool); !present && gets != 0 {
				t.Error("absent or malformed v1 flag should not fetch settings")
			}
			key := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid")
			_, markerErr := h.store.Get(t.Context(), key)
			if (markerErr == nil) != (tt.wantPut && !tt.wantErr) {
				t.Errorf("unexpected pending marker state: %v", markerErr)
			}
		})
	}
}

func dispatchSettings(t *testing.T, action string, data map[string]any) *fakeMsg {
	t.Helper()
	msg := &fakeMsg{
		subject: "lfx.committee_settings." + action,
		data:    committeeRetryEvent(t, "committee_settings", action, "committee-uid", data),
	}
	committeeEventsIngestHandler(msg)
	if msg.ackCalls+msg.nakCalls != 1 {
		t.Fatal("settings event must ACK or NAK exactly once")
	}
	return msg
}

func TestCommitteeSettingsToV1(t *testing.T) {
	for _, action := range []string{"created", "updated"} {
		for _, required := range []bool{false, true} {
			t.Run(action+"/"+map[bool]string{false: "false", true: "true"}[required], func(t *testing.T) {
				h := setupSyncTest(t, 200, `{}`)
				h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
				fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
					return &committeeservice.CommitteeSettingsWithReadonlyAttributes{BusinessEmailRequired: required}, "42", nil
				}
				// Deliberately use the opposite snapshot: retries must use current v2 state.
				msg := dispatchSettings(t, action, map[string]any{"business_email_required": !required})
				if !msg.acked {
					t.Fatal("expected ACK")
				}
				assertSyncRequest(t, h, http.MethodPatch, "/project-service/v2/projects/project-sfid/committees/committee-sfid", map[string]any{"BusinessEmailRequired": required})
			})
		}
	}
}

func TestCommitteeSettingsToV1SkipsAndRetries(t *testing.T) {
	for _, tt := range []struct {
		name     string
		action   string
		data     map[string]any
		mapping  string
		fetchErr error
		status   int
		echo     bool
		wantNAK  bool
	}{
		{name: "missing mapping retries", mapping: "", wantNAK: true},
		{name: "tombstone", mapping: tombstoneMarker},
		{name: "malformed mapping", mapping: "bad"},
		{name: "settings unavailable", mapping: "project-sfid:committee-sfid", fetchErr: errors.New("unavailable"), wantNAK: true},
		{name: "settings deleted", mapping: "project-sfid:committee-sfid", fetchErr: &committeeservice.NotFoundError{Message: "deleted"}},
		{name: "v1 API failure", mapping: "project-sfid:committee-sfid", status: 503, wantNAK: true},
		{name: "echo", mapping: "project-sfid:committee-sfid", echo: true},
		{name: "deleted event", action: "deleted", mapping: "project-sfid:committee-sfid"},
		{name: "absent flag", data: map[string]any{}, mapping: "project-sfid:committee-sfid"},
		{name: "null flag", data: map[string]any{"business_email_required": nil}, mapping: "project-sfid:committee-sfid"},
		{name: "malformed flag", data: map[string]any{"business_email_required": "true"}, mapping: "project-sfid:committee-sfid"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status := tt.status
			if status == 0 {
				status = 200
			}
			h := setupSyncTest(t, status, `{}`)
			stubSettingsBase(t, "project-uid", nil)
			h.seed(t, "project.uid.project-uid", "project-sfid")
			if tt.mapping != "" {
				h.seed(t, "committee.uid.committee-uid", tt.mapping)
			}
			fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
				return &committeeservice.CommitteeSettingsWithReadonlyAttributes{}, "42", tt.fetchErr
			}
			if tt.echo {
				writePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid")
			}
			action, data := tt.action, tt.data
			if action == "" {
				action = "updated"
			}
			if data == nil {
				data = map[string]any{"business_email_required": true}
			}
			msg := dispatchSettings(t, action, data)
			if msg.nacked != tt.wantNAK {
				t.Errorf("NAK = %v, want %v", msg.nacked, tt.wantNAK)
			}
			wantRequests := 0
			if tt.status != 0 {
				wantRequests = 1
			}
			if len(h.recordedRequests()) != wantRequests {
				t.Errorf("unexpected v1 requests: %+v", h.recordedRequests())
			}
			if tt.echo && consumePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid") {
				t.Error("settings echo did not consume its marker")
			}
		})
	}
}

func TestCommitteeSettingsMappingRaceRecovers(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	stubSettingsBase(t, "project-uid", nil)
	h.seed(t, "project.uid.project-uid", "project-sfid")
	data := map[string]any{"business_email_required": true}
	msg := dispatchSettings(t, "created", data)
	if !msg.nacked || msg.nakDelay != 30*time.Second {
		t.Fatal("unmapped settings create must retry after allowing the base create to finish")
	}
	h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	if !dispatchSettings(t, "created", data).acked {
		t.Fatal("settings create should recover after mapping appears")
	}
	if len(h.recordedRequests()) != 1 {
		t.Fatal("expected one PATCH after mapping appears")
	}
}

// Reproduce a settings-created event that checks for an echo before the v1
// create has returned, then sees the newly written mapping on the same pass.
type settingsCreateMappingRaceStore struct {
	MappingStore
	markerSeen bool
	created    bool
}

func (s *settingsCreateMappingRaceStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	if key == pendingMarkerKey(markerV1ToV2, markerOpCreate, markerResourceCommitteeSettings, "committee-uid") {
		s.markerSeen = true
	}
	if key == "committee.uid.committee-uid" && s.markerSeen && !s.created {
		s.created = true
		writePendingMarker(ctx, markerV1ToV2, markerOpCreate, markerResourceCommitteeSettings, "committee-uid")
		if _, err := s.Put(ctx, key, []byte("project-sfid:committee-sfid")); err != nil {
			return MappingEntry{}, err
		}
	}
	return s.MappingStore.Get(ctx, key)
}

func TestCommitteeSettingsCreateEchoMappingAppearsDuringLookup(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	s := &settingsCreateMappingRaceStore{MappingStore: h.store}
	mappingStore = s
	fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
		t.Error("in-flight create echo must not read settings or PATCH v1")
		return nil, "", errors.New("unexpected read")
	}
	if !dispatchSettings(t, "created", map[string]any{"business_email_required": true}).acked {
		t.Fatal("in-flight create echo should ACK")
	}
	if !s.created || len(h.recordedRequests()) != 0 {
		t.Fatalf("create echo must be suppressed after mapping appears; created=%v requests=%v", s.created, h.recordedRequests())
	}
	markerKey := pendingMarkerKey(markerV1ToV2, markerOpCreate, markerResourceCommitteeSettings, "committee-uid")
	if _, err := h.store.Get(t.Context(), markerKey); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("in-flight create echo left its marker: %v", err)
	}
}

func TestCommitteeCreateReadsSettingsToV1(t *testing.T) {
	for _, tt := range []struct {
		name     string
		required bool
		fetchErr error
	}{
		{name: "enabled", required: true},
		{name: "disabled"},
		{name: "settings unavailable", fetchErr: errors.New("unavailable")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			h.seed(t, "project.uid.project-uid", "project-sfid")
			fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
				return &committeeservice.CommitteeSettingsWithReadonlyAttributes{BusinessEmailRequired: tt.required}, "42", tt.fetchErr
			}
			msg := &fakeMsg{subject: "lfx.committee.created", data: committeeRetryEvent(t, "committee", "created", "committee-uid", map[string]any{
				"name": "Committee", "project_uid": "project-uid", "category": "Other",
			})}
			committeeEventsIngestHandler(msg)
			if tt.fetchErr != nil {
				if !msg.nacked || len(h.recordedRequests()) != 0 {
					t.Fatal("failed settings fetch must retry without creating in v1")
				}
				return
			}
			if !msg.acked {
				t.Fatal("create should ACK")
			}
			assertSyncRequest(t, h, http.MethodPost, "/project-service/v2/projects/project-sfid/committees", map[string]any{
				"Name": "Committee", "Category": "Other", "BusinessEmailRequired": tt.required,
			})
			h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
		})
	}
}

// Even when the base is unchanged, WAL must update the settings and redeliver
// a failed settings write. Its echo must not trigger a v1 PATCH or consume the
// separate base marker. The v2-to-v1 WAL echo is skipped by service identity.
func TestCommitteeSettingsWALUpdateAndEcho(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	h.seed(t, "committee.sfid.committee-sfid", "committee-uid")
	h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	h.seed(t, "project.sfid.project-sfid", "project-uid")
	origFetchBase := fetchCommitteeBase
	t.Cleanup(func() { fetchCommitteeBase = origFetchBase })
	fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{
			UID: sptr("committee-uid"), Name: sptr("Committee"), ProjectUID: sptr("project-uid"), Category: sptr("Other"),
		}, "base-etag", nil
	}
	puts := 0
	setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			w.Header().Set("ETag", "42")
			_ = json.NewEncoder(w).Encode(settingsResponse(false))
			return
		}
		puts++
		if puts == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"concurrent edit"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(settingsResponse(true))
	})
	v1Data := map[string]any{
		"sfid": "committee-sfid", "project_name__c": "project-sfid", "mailing_list__c": "Committee", "type__c": "Other",
		"business_email_required__c": true,
	}
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) {
		return v1Data, true, nil
	}
	value, err := json.Marshal(v1Data)
	if err != nil {
		t.Fatal(err)
	}
	entry := &fakeKVEntry{key: "platform-collaboration__c.committee-sfid", value: value}
	if !handleKVPut(t.Context(), entry) {
		t.Fatal("settings conflict must NACK WAL for retry")
	}
	if consumePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommitteeSettings, "committee-uid") {
		t.Fatal("failed settings write left a stale marker")
	}
	if handleKVPut(t.Context(), entry) {
		t.Fatal("successful settings retry should ACK WAL")
	}
	if puts != 2 {
		t.Fatalf("settings PUTs = %d, want 2", puts)
	}
	if len(h.recordedRequests()) != 0 {
		t.Fatal("v1-originated update should not write back to v1")
	}
	writePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommittee, "committee-uid")
	if !dispatchSettings(t, "updated", map[string]any{"business_email_required": true}).acked {
		t.Fatal("settings echo should ACK")
	}
	if len(h.recordedRequests()) != 0 {
		t.Fatal("settings echo caused a v1 PATCH")
	}
	if !consumePendingMarker(t.Context(), markerV1ToV2, markerOpUpdate, markerResourceCommittee, "committee-uid") {
		t.Error("settings echo consumed the base marker")
	}
	cfg.Auth0ClientID = "test-client"
	v1Data["lastmodifiedbyid"] = "test-client@clients"
	value, err = json.Marshal(v1Data)
	if err != nil {
		t.Fatal(err)
	}
	entry.value = value
	if handleKVPut(t.Context(), entry) || puts != 2 {
		t.Fatal("service-authored WAL echo should be skipped")
	}
}

func TestCommitteeSettingsV1CreateEcho(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	h.seed(t, "project.sfid.project-sfid", "project-uid")
	setupSettingsClient(t, func(http.ResponseWriter, *http.Request) { t.Error("v1 create echo must not call settings API") })
	committeeClient.CreateCommitteeEndpoint = func(_ context.Context, input any) (any, error) {
		payload := input.(*committeeservice.CreateCommitteePayload)
		if !payload.BusinessEmailRequired {
			t.Error("v1 create must copy the settings flag")
		}
		return &committeeservice.CommitteeFullWithReadonlyAttributes{UID: sptr("committee-uid"), BusinessEmailRequired: true}, nil
	}
	if handleCommitteeUpdate(t.Context(), "platform-collaboration__c.committee-sfid", map[string]any{
		"sfid": "committee-sfid", "project_name__c": "project-sfid", "mailing_list__c": "Committee", "type__c": "Other",
		"business_email_required__c": true,
	}) {
		t.Fatal("v1 create should ACK")
	}
	h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	if !dispatchSettings(t, "created", map[string]any{"business_email_required": true}).acked {
		t.Fatal("v1 create settings echo should ACK")
	}
	if len(h.recordedRequests()) != 0 {
		t.Error("v1 create settings echo caused a v1 PATCH")
	}
}

func TestCommitteeSettingsUpdateDoesNotConsumeCreateMarker(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	writePendingMarker(t.Context(), markerV1ToV2, markerOpCreate, markerResourceCommitteeSettings, "committee-uid")
	if !dispatchSettings(t, "updated", map[string]any{"business_email_required": false}).acked {
		t.Fatal("genuine settings edit should ACK")
	}
	assertSyncRequest(t, h, http.MethodPatch, "/project-service/v2/projects/project-sfid/committees/committee-sfid", map[string]any{"BusinessEmailRequired": false})
	key := pendingMarkerKey(markerV1ToV2, markerOpCreate, markerResourceCommitteeSettings, "committee-uid")
	if _, err := h.store.Get(t.Context(), key); err != nil {
		t.Fatal("settings update consumed the create marker")
	}
	if !dispatchSettings(t, "created", map[string]any{"business_email_required": true}).acked {
		t.Fatal("delayed create echo should ACK")
	}
	if len(h.recordedRequests()) != 1 {
		t.Error("delayed create echo issued an extra v1 PATCH")
	}
	if _, err := h.store.Get(t.Context(), key); !errors.Is(err, ErrKeyNotFound) {
		t.Error("create echo did not consume its marker")
	}
}
