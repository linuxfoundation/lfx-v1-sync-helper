// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	goahttp "goa.design/goa/v3/http"
)

func stubSettingsBase(t *testing.T, projectUID string, err error) {
	t.Helper()
	orig := fetchCommitteeBase
	fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		if err != nil {
			return nil, "", err
		}
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{
			UID: sptr("committee-uid"), ProjectUID: &projectUID, Name: sptr("Committee"), Category: sptr("Other"),
		}, "base-etag", nil
	}
	t.Cleanup(func() { fetchCommitteeBase = orig })
}

func settingsWALData(required bool) map[string]any {
	return map[string]any{
		"sfid": "committee-sfid", "project_name__c": "project-sfid",
		"mailing_list__c": "Committee", "type__c": "Other", "business_email_required__c": required,
	}
}

func seedSettingsMappings(t *testing.T, h *syncTestHarness) {
	t.Helper()
	h.seed(t, "committee.sfid.committee-sfid", "committee-uid")
	h.seed(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	h.seed(t, "project.sfid.project-sfid", "project-uid")
}

func TestCommitteeSettingsPermanentAndEligibleMappingMisses(t *testing.T) {
	for _, action := range []string{"created", "updated"} {
		for _, tt := range []struct {
			name           string
			projectMapping string
			baseErr        error
			wantRetry      bool
		}{
			{name: "v2-only"},
			{name: "parent tombstoned", projectMapping: tombstoneMarker},
			{name: "deleted committee", baseErr: wrapCommitteeServiceError("get base", &committeeservice.NotFoundError{Message: "gone"})},
			{name: "base unavailable", baseErr: &committeeservice.ServiceUnavailableError{Message: "later"}, wantRetry: true},
			{name: "eligible create race", projectMapping: "project-sfid", wantRetry: true},
		} {
			t.Run(action+"/"+tt.name, func(t *testing.T) {
				h := setupSyncTest(t, 200, `{}`)
				stubSettingsBase(t, "project-uid", tt.baseErr)
				if tt.projectMapping != "" {
					h.seed(t, "project.uid.project-uid", tt.projectMapping)
				}
				msg := dispatchSettings(t, action, map[string]any{"business_email_required": true})
				if msg.nacked != tt.wantRetry {
					t.Errorf("NAK = %v, want %v", msg.nacked, tt.wantRetry)
				}
				if tt.projectMapping == "project-sfid" && msg.nakDelay != 30*time.Second {
					t.Error("eligible mapping race must retain delayed retry")
				}
				if len(h.recordedRequests()) != 0 {
					t.Error("mapping miss must not write v1")
				}
			})
		}
	}
}

func TestCommitteeCreateMissingSettingsChecksLiveBase(t *testing.T) {
	for _, tt := range []struct {
		name       string
		baseErr    error
		wantCreate bool
		wantRetry  bool
	}{
		{name: "live legacy committee", wantCreate: true},
		{name: "deleted committee", baseErr: wrapCommitteeServiceError("get base", &committeeservice.NotFoundError{Message: "gone"})},
		{name: "base unavailable", baseErr: &committeeservice.ServiceUnavailableError{Message: "later"}, wantRetry: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, http.StatusCreated, `{"ID":"committee-sfid"}`)
			h.seed(t, "project.uid.project-uid", "project-sfid")
			stubSettingsBase(t, "project-uid", tt.baseErr)
			// Use the real generated HTTP client and error wrapper for settings 404.
			setupSettingsClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"settings not found"}`))
			})
			_, _, err := fetchCommitteeSettings(t.Context(), "committee-uid")
			var notFound *committeeservice.NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("wrapped settings error must preserve NotFound: %v", err)
			}
			c := indexerSyncCase{resource: markerResourceCommittee, uid: "committee-uid", sfid: "committee-sfid"}
			acked := c.dispatch(t, "created", map[string]any{"project_uid": "project-uid", "name": "Committee", "category": "Other"})
			if acked == tt.wantRetry {
				t.Errorf("ACK = %v, wantRetry %v", acked, tt.wantRetry)
			}
			if tt.wantCreate {
				assertSyncRequest(t, h, http.MethodPost, "/project-service/v2/projects/project-sfid/committees", map[string]any{"Name": "Committee", "Category": "Other"})
			} else if len(h.recordedRequests()) != 0 {
				t.Error("deleted or unverified committee must not be created")
			}
		})
	}
}

type settingsWALMessage struct{ fakeMsg }

func (m *settingsWALMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}

func deliverSettingsWAL(t *testing.T, data map[string]any) *settingsWALMessage {
	t.Helper()
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	msg := &settingsWALMessage{fakeMsg: fakeMsg{subject: "$KV.v1-objects.platform-collaboration__c.committee-sfid", data: body}}
	kvMessageHandler(msg)
	if msg.ackCalls+msg.nakCalls != 1 {
		t.Fatal("WAL delivery must ACK or NAK once")
	}
	return msg
}

func TestCommitteeSettingsWALRedeliveryUsesCurrentV1(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	seedSettingsMappings(t, h)
	stubSettingsBase(t, "project-uid", nil)
	origKV := v1KV
	kv := newFakeKV()
	v1KV = kv
	t.Cleanup(func() { v1KV = origKV })
	var writes []bool
	v2Required := false
	setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			w.Header().Set("ETag", fmt.Sprint(len(writes)+1))
			_ = json.NewEncoder(w).Encode(settingsResponse(v2Required))
			return
		}
		var body struct {
			Required bool `json:"business_email_required"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		v2Required = body.Required
		writes = append(writes, body.Required)
		if len(writes) == 1 {
			// Model a mutation whose acknowledgement/publication failed.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"temporary failure"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(settingsResponse(v2Required))
	})
	base := fetchCommitteeBase
	baseName := "Initial name"
	var baseWrites []string
	fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		b, etag, err := base(ctx, uid)
		b.Name = &baseName
		return b, etag, err
	}
	committeeClient.UpdateCommitteeBaseEndpoint = func(_ context.Context, input any) (any, error) {
		baseName = input.(*committeeservice.UpdateCommitteeBasePayload).Name
		baseWrites = append(baseWrites, baseName)
		b, _, _ := fetchCommitteeBase(t.Context(), "committee-uid")
		return b, nil
	}
	key := "platform-collaboration__c.committee-sfid"
	old := settingsWALData(true)
	newer := settingsWALData(false)
	old["mailing_list__c"] = "Rename A"
	newer["mailing_list__c"] = "Rename B"
	var err error
	kv.data[key], err = json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if !deliverSettingsWAL(t, old).nacked {
		t.Fatal("first write must retry")
	}
	// The newer WAL image is now in KV; deliberately exercise MessagePack too.
	kv.data[key], err = msgpack.Marshal(newer)
	if err != nil {
		t.Fatal(err)
	}
	if !deliverSettingsWAL(t, newer).acked {
		t.Fatal("newer disable should succeed")
	}
	if !deliverSettingsWAL(t, old).acked {
		t.Fatal("old enable redelivery should ACK without reverting")
	}
	if v2Required || len(writes) != 2 || !writes[0] || writes[1] {
		t.Fatalf("stale redelivery changed current settings: writes=%v required=%v", writes, v2Required)
	}
	if baseName != "Rename B" || len(baseWrites) != 2 || baseWrites[0] != "Rename A" || baseWrites[1] != "Rename B" {
		t.Fatalf("stale settings retry reverted the newer base rename: writes=%v name=%q", baseWrites, baseName)
	}
}

func TestCommitteeSettingsLatestV1MissingDeletedAndUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   string
		readErr error
		wantErr bool
	}{
		{name: "missing"},
		{name: "tombstone", value: tombstoneMarker},
		{name: "soft-deleted", value: `{"business_email_required__c":true,"_sdc_deleted_at":"2026-01-01T00:00:00Z"}`},
		{name: "SFDC-deleted", value: `{"business_email_required__c":true,"isdeleted":true}`},
		{name: "flag absent", value: `{"sfid":"committee-sfid"}`},
		{name: "malformed record", value: `not-json-or-msgpack`, wantErr: true},
		{name: "KV unavailable", readErr: errors.New("KV unavailable"), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setupSyncTest(t, 200, `{}`)
			origKV := v1KV
			kv := newFakeKV()
			v1KV = kv
			t.Cleanup(func() { v1KV = origKV })
			if tt.value != "" {
				kv.data["platform-collaboration__c.committee-sfid"] = []byte(tt.value)
			}
			if tt.readErr != nil {
				fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return nil, false, tt.readErr }
			}
			fetchCommitteeSettings = func(context.Context, string) (*committeeservice.CommitteeSettingsWithReadonlyAttributes, string, error) {
				t.Error("no live v1 flag: settings API must not be called")
				return nil, "", errors.New("unexpected read")
			}
			err := updateCommitteeSettingsFromV1(t.Context(), "committee-uid", settingsWALData(true), "test-principal")
			if (err != nil) != tt.wantErr {
				t.Errorf("error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestRetryableCommitteeBaseErrors(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"503", &committeeservice.ServiceUnavailableError{}, true},
		{"500", &committeeservice.InternalServerError{}, true},
		{"409", &committeeservice.ConflictError{}, true},
		{"deadline", context.DeadlineExceeded, true},
		{"network", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}, true},
		{"gateway", goahttp.ErrInvalidResponse("committee", "update", 502, ""), true},
		{"rate limit", goahttp.ErrInvalidResponse("committee", "update", 429, ""), true},
		{"validation", &committeeservice.BadRequestError{}, false},
		{"forbidden", &committeeservice.ForbiddenError{}, false},
		{"not found", &committeeservice.NotFoundError{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableCommitteeSyncError(wrapCommitteeServiceError("base operation", tt.err)); got != tt.retry {
				t.Errorf("retry=%v,want %v", got, tt.retry)
			}
		})
	}
}

func TestCommitteeBaseFailuresRetryBeforeSettings(t *testing.T) {
	for _, failure := range []string{"read", "update"} {
		t.Run(failure, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			seedSettingsMappings(t, h)
			stubSettingsBase(t, "project-uid", nil)
			data := settingsWALData(true)
			fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return data, true, nil }
			puts := 0
			setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					w.Header().Set("ETag", "42")
				} else {
					puts++
				}
				_ = json.NewEncoder(w).Encode(settingsResponse(r.Method == http.MethodPut))
			})
			base := fetchCommitteeBase
			calls := 0
			if failure == "read" {
				fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
					calls++
					if calls == 1 {
						return nil, "", wrapCommitteeServiceError("base read", &committeeservice.ServiceUnavailableError{Message: "later"})
					}
					return base(ctx, uid)
				}
			} else {
				fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
					b, etag, err := base(ctx, uid)
					b.Name = sptr("Old name")
					return b, etag, err
				}
				committeeClient.UpdateCommitteeBaseEndpoint = func(context.Context, any) (any, error) {
					calls++
					if calls == 1 {
						return nil, &committeeservice.ConflictError{Message: "concurrent base edit"}
					}
					b, _, _ := base(t.Context(), "committee-uid")
					return b, nil
				}
			}
			if !deliverSettingsWAL(t, data).nacked || puts != 0 {
				t.Fatal("transient base failure must retry, not ACK or write settings")
			}
			if !deliverSettingsWAL(t, data).acked || puts != 1 {
				t.Fatal("retry should finish the pending settings change")
			}
		})
	}
}

func TestCommitteeSettingsFailureDoesNotSkipSSONameWriteback(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	seedSettingsMappings(t, h)
	stubSettingsBase(t, "project-uid", nil)
	data := settingsWALData(true)
	latest := settingsWALData(true)
	data["lastmodifiedbyid"] = "test-v1-editor"
	latest["lastmodifiedbyid"] = "test-v1-editor"
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return latest, true, nil }
	puts := 0
	setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.Header.Get("Authorization") != "Bearer human-token" {
			t.Errorf("settings write attributed to wrong editor: %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		if len(h.recordedRequests()) != 1 {
			t.Error("base SSO-name writeback must complete before settings")
		}
		if r.Method == http.MethodGet {
			w.Header().Set("ETag", "42")
			_ = json.NewEncoder(w).Encode(settingsResponse(false))
			return
		}
		puts++
		if puts == 1 {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"message":"settings unavailable"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(settingsResponse(true))
	})
	jwtTokenCache.Set("jwt-"+committeeServiceAudience+"-test-v1-editor", "human-token", time.Minute)
	jwtTokenCache.Set("jwt-"+committeeServiceAudience+"-test-client@clients", "service-token", time.Minute)
	base := fetchCommitteeBase
	mutated := false
	fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		b, etag, err := base(ctx, uid)
		if !mutated {
			b.Name = sptr("Old name")
		}
		return b, etag, err
	}
	committeeClient.UpdateCommitteeBaseEndpoint = func(context.Context, any) (any, error) {
		mutated = true
		b, _, _ := base(t.Context(), "committee-uid")
		b.SsoGroupName = sptr("test-project-committee")
		return b, nil
	}
	if !deliverSettingsWAL(t, data).nacked {
		t.Fatal("settings failure must retry")
	}
	assertSyncRequest(t, h, http.MethodPatch, "/project-service/v2/projects/project-sfid/committees/committee-sfid", map[string]any{"SSOGroupName": "test-project-committee"})
	h.assertMapping(t, "committee.uid.committee-uid", "project-sfid:committee-sfid")
	// The completed SSO-name PATCH is now replicated as a service-authored row.
	// Retrying the original human-authored WAL event must still finish settings.
	cfg.Auth0ClientID = "test-client"
	latest["lastmodifiedbyid"] = "test-client@clients"
	if !deliverSettingsWAL(t, data).acked || puts != 2 {
		t.Fatal("retry should finish settings even when refreshed base input is service-authored")
	}
	if len(h.recordedRequests()) != 1 {
		t.Error("SSO name writeback should have completed on the first attempt")
	}
}

func TestCommitteeWALMissingSettingsDoesNotRetry(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	seedSettingsMappings(t, h)
	stubSettingsBase(t, "project-uid", nil)
	data := settingsWALData(true)
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return data, true, nil }
	setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("missing settings must not cause a PUT")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"settings not found"}`))
	})
	if !deliverSettingsWAL(t, data).acked {
		t.Fatal("permanently missing settings should ACK, not retry")
	}
	if len(h.recordedRequests()) != 0 {
		t.Fatal("missing settings must not produce a v1 PATCH")
	}
}

func TestCommitteeSettingsPATCHDoesNotSuppressPendingRename(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	seedSettingsMappings(t, h)
	stubSettingsBase(t, "project-uid", nil)
	cfg.Auth0ClientID = "test-client"
	setupSettingsClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("v2 settings already agree; only GET is expected")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", "42")
		_ = json.NewEncoder(w).Encode(settingsResponse(false))
	})
	origKV := v1KV
	kv := newFakeKV()
	v1KV = kv
	t.Cleanup(func() { v1KV = origKV })
	key := "platform-collaboration__c.committee-sfid"
	rename := settingsWALData(true)
	rename["mailing_list__c"] = "Pending rename"
	rename["lastmodifiedbyid"] = "test-v1-editor"
	var err error
	kv.data[key], err = json.Marshal(rename)
	if err != nil {
		t.Fatal(err)
	}
	// A v2 settings edit writes only the boolean into v1 while the human
	// rename's WAL event is still waiting. The rename survives that PATCH.
	if !dispatchSettings(t, "updated", map[string]any{"business_email_required": false}).acked {
		t.Fatal("settings PATCH should succeed")
	}
	assertSyncRequest(t, h, http.MethodPatch, "/project-service/v2/projects/project-sfid/committees/committee-sfid", map[string]any{"BusinessEmailRequired": false})
	latest := settingsWALData(false)
	latest["mailing_list__c"] = "Pending rename"
	latest["lastmodifiedbyid"] = "test-client@clients"
	kv.data[key], err = msgpack.Marshal(latest)
	if err != nil {
		t.Fatal(err)
	}
	jwtTokenCache.Set("jwt-"+committeeServiceAudience+"-"+extractV1Principal(t.Context(), latest), "test-token", time.Minute)
	jwtTokenCache.Set("jwt-"+committeeServiceAudience+"-test-v1-editor", "human-token", time.Minute)
	base := fetchCommitteeBase
	baseName, baseWrites := "Old v2 name", 0
	fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		b, etag, err := base(ctx, uid)
		b.Name = &baseName
		return b, etag, err
	}
	committeeClient.UpdateCommitteeBaseEndpoint = func(_ context.Context, input any) (any, error) {
		payload := input.(*committeeservice.UpdateCommitteeBasePayload)
		if payload.BearerToken == nil || *payload.BearerToken != "human-token" {
			t.Errorf("pending rename attributed to wrong editor: %v", payload.BearerToken)
		}
		baseName = payload.Name
		baseWrites++
		b, _, _ := fetchCommitteeBase(t.Context(), "committee-uid")
		return b, nil
	}
	if !deliverSettingsWAL(t, rename).acked {
		t.Fatal("pending human rename should ACK")
	}
	if baseName != "Pending rename" || baseWrites != 1 {
		t.Fatalf("settings PATCH suppressed pending rename: name=%q writes=%d", baseName, baseWrites)
	}
	if !deliverSettingsWAL(t, latest).acked || baseWrites != 1 {
		t.Fatal("the genuine service-authored WAL echo must still be skipped")
	}
}

func TestCommitteeSettingsPATCHPreservesV2TechnicalCategory(t *testing.T) {
	h := setupSyncTest(t, 200, `{}`)
	seedSettingsMappings(t, h)
	setupSettingsClient(t, func(http.ResponseWriter, *http.Request) { t.Error("absent flag must not touch settings") })
	data := settingsWALData(false)
	delete(data, "business_email_required__c")
	data["mailing_list__c"] = "Architecture Council"
	data["type__c"] = "Other" // Original human rename preceded the v2 category edit.
	latest := settingsWALData(false)
	delete(latest, "business_email_required__c")
	latest["mailing_list__c"] = "Architecture Council"
	latest["type__c"] = "Technical Oversight Committee/Technical Advisory Committee"
	latest["lastmodifiedbyid"] = "test-client@clients"
	cfg.Auth0ClientID = "test-client"
	fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return latest, true, nil }
	origBase := fetchCommitteeBase
	t.Cleanup(func() { fetchCommitteeBase = origBase })
	fetchCommitteeBase = func(context.Context, string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{
			UID: sptr("committee-uid"), ProjectUID: sptr("project-uid"),
			Name: sptr("Old v2 name"), Category: sptr("Technical Advisory Committee"),
		}, "base-etag", nil
	}
	var got *committeeservice.UpdateCommitteeBasePayload
	committeeClient.UpdateCommitteeBaseEndpoint = func(_ context.Context, input any) (any, error) {
		got = input.(*committeeservice.UpdateCommitteeBasePayload)
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{}, nil
	}
	if handleCommitteeUpdate(t.Context(), "platform-collaboration__c.committee-sfid", data) {
		t.Fatal("pending human rename should ACK")
	}
	if got == nil || got.Name != "Architecture Council" || got.Category != "Technical Advisory Committee" {
		t.Fatalf("pending rename must not overwrite v2 category: %+v", got)
	}
}

func TestCommitteeBaseRetryRefreshesAbsentAndNullSettingsFlag(t *testing.T) {
	for _, flag := range []string{"absent", "null"} {
		t.Run(flag, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			seedSettingsMappings(t, h)
			stubSettingsBase(t, "project-uid", nil)
			setupSettingsClient(t, func(http.ResponseWriter, *http.Request) { t.Error("no boolean flag: settings API must not be called") })
			old, newer := settingsWALData(true), settingsWALData(false)
			old["mailing_list__c"], newer["mailing_list__c"] = "Rename A", "Rename B"
			if flag == "absent" {
				delete(old, "business_email_required__c")
				delete(newer, "business_email_required__c")
			} else {
				old["business_email_required__c"], newer["business_email_required__c"] = nil, nil
			}
			latest := old
			fetchV1CommitteeData = func(context.Context, string) (map[string]any, bool, error) { return latest, true, nil }
			base := fetchCommitteeBase
			baseName := "Initial name"
			var attempts []string
			fetchCommitteeBase = func(ctx context.Context, uid string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
				b, etag, err := base(ctx, uid)
				b.Name = &baseName
				return b, etag, err
			}
			committeeClient.UpdateCommitteeBaseEndpoint = func(_ context.Context, input any) (any, error) {
				name := input.(*committeeservice.UpdateCommitteeBasePayload).Name
				attempts = append(attempts, name)
				if len(attempts) == 1 {
					return nil, &committeeservice.ServiceUnavailableError{Message: "later"}
				}
				baseName = name
				b, _, _ := fetchCommitteeBase(t.Context(), "committee-uid")
				return b, nil
			}
			if !deliverSettingsWAL(t, old).nacked {
				t.Fatal("temporary rename failure should retry")
			}
			latest = newer
			if !deliverSettingsWAL(t, newer).acked {
				t.Fatal("newer rename should succeed")
			}
			if !deliverSettingsWAL(t, old).acked {
				t.Fatal("old rename redelivery should ACK without reverting")
			}
			if baseName != "Rename B" || len(attempts) != 2 || attempts[0] != "Rename A" || attempts[1] != "Rename B" {
				t.Fatalf("unrefreshed %s-flag retry reverted newer rename: name=%q attempts=%v", flag, baseName, attempts)
			}
		})
	}
}
