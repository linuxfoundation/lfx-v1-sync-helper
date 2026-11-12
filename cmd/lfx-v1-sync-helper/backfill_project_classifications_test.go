// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/nats-io/nats.go/jetstream"
)

func TestClassificationBackfillPayload(t *testing.T) {
	current := &projectservice.ProjectBase{
		Name: stringToStringPtr("Test Project"), Slug: stringToStringPtr("test-project"),
		Description: stringToStringPtr("description"), ParentUID: stringToStringPtr("parent-uid"),
		ContactEmail: stringToStringPtr("contact@example.com"),
		Industry:     []string{"Energy & Climate"}, Technology: []string{"Blockchain"},
	}
	payload := classificationBackfillPayload("00000000-0000-4000-8000-000000000001", "12", current,
		[]string{"Manufacturing"}, []string{}, []string{"Open Data"})
	if *payload.IfMatch != "12" || payload.Name != "Test Project" || payload.ContactEmail != current.ContactEmail ||
		!reflect.DeepEqual(payload.Industry, []string{"Manufacturing"}) || payload.Technology == nil || len(payload.Technology) != 0 ||
		!reflect.DeepEqual(payload.TechnicalActivityType, []string{"Open Data"}) {
		t.Fatalf("payload did not preserve base fields and classifications: %+v", payload)
	}
}

func TestBackfillProjectClassifications(t *testing.T) {
	const (
		sfid = "test-sfid"
		uid  = "00000000-0000-4000-8000-000000000001"
	)
	for _, tt := range []struct {
		name             string
		v1Industry       string
		v2Industry       []string
		dryRun           bool
		stale            bool
		malformed        bool
		failWrite        bool
		wantChanged      int
		wantUpdated      int
		wantV2Only       int
		wantSkipped      int
		wantErrors       int
		wantMarkerRemain bool
	}{
		{name: "dry run plans change", v1Industry: "Energy & Climate", dryRun: true, wantChanged: 1},
		{name: "live write with echo marker", v1Industry: "Energy & Climate", wantChanged: 1, wantUpdated: 1, wantMarkerRemain: true},
		{name: "matching values are idempotent", v1Industry: "Energy & Climate", v2Industry: []string{"Energy & Climate"}},
		{name: "empty v1 cannot clear v2-only value", v1Industry: "", v2Industry: []string{"Energy & Climate"}, wantV2Only: 1},
		{name: "changed replica is skipped", v1Industry: "Energy & Climate", stale: true, wantChanged: 1, wantSkipped: 1},
		{name: "failed write cleans marker", v1Industry: "Energy & Climate", failWrite: true, wantChanged: 1, wantErrors: 1},
		{name: "malformed row identifies key without data", malformed: true, wantErrors: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			origLogger := logger
			logger = slog.New(slog.NewJSONHandler(&logs, nil))
			origScan, origMappings := scanClassificationProjects, collectClassificationMappings
			origFetch, origRead, origApply := fetchClassificationBase, readClassificationProject, applyClassificationBase
			origCfg, origStore := cfg, mappingStore
			t.Cleanup(func() {
				scanClassificationProjects, collectClassificationMappings = origScan, origMappings
				fetchClassificationBase, readClassificationProject, applyClassificationBase = origFetch, origRead, origApply
				cfg, mappingStore = origCfg, origStore
				logger = origLogger
			})
			cfg = &Config{V1MappingsStoreMode: V1MappingsStoreModeKV}
			store := newFakeMappingStore()
			mappingStore = store
			if _, err := store.Put(t.Context(), projectSFIDMappingKeyPrefix+sfid, []byte(uid)); err != nil {
				t.Fatal(err)
			}
			row := map[string]any{"sfid": sfid, "industry__c": tt.v1Industry, "technology__c": "", "technical_activity_type__c": ""}
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			if tt.malformed {
				encoded = []byte("sensitive malformed row")
			}
			scanClassificationProjects = func(_ context.Context, _ jetstream.JetStream, _, _ string, _ time.Duration) (map[string][]byte, error) {
				return map[string][]byte{"$KV.v1-objects.salesforce-project__c." + sfid: encoded}, nil
			}
			collectClassificationMappings = func(context.Context) (map[string]string, map[string]struct{}, error) {
				return map[string]string{sfid: uid}, nil, nil
			}
			fetchClassificationBase = func(context.Context, string) (*projectservice.ProjectBase, string, error) {
				return &projectservice.ProjectBase{Industry: tt.v2Industry, Name: stringToStringPtr("Test Project"), Slug: stringToStringPtr("test-project")}, "3", nil
			}
			readClassificationProject = func(context.Context, string) (map[string]any, bool, error) {
				fresh := map[string]any{"sfid": sfid, "industry__c": tt.v1Industry, "technology__c": "", "technical_activity_type__c": ""}
				if tt.stale {
					fresh["industry__c"] = "Open Data"
				}
				return fresh, true, nil
			}
			applied := 0
			applyClassificationBase = func(_ context.Context, payload *projectservice.UpdateProjectBasePayload) error {
				applied++
				if payload.IfMatch == nil || *payload.IfMatch != "3" || !reflect.DeepEqual(payload.Industry, []string{"Energy & Climate"}) {
					t.Errorf("unexpected base payload: %+v", payload)
				}
				if tt.failWrite {
					return errors.New("test write failed")
				}
				return nil
			}
			result, err := backfillProjectClassifications(t.Context(), tt.dryRun, 100, 10000)
			if (err != nil) != (tt.wantErrors > 0) {
				t.Errorf("error = %v, wantErrors = %d", err, tt.wantErrors)
			}
			wantMapped := 1
			wantApplied := tt.wantUpdated + tt.wantErrors
			if tt.malformed {
				wantMapped = 0
				wantApplied = 0
			}
			if result.scanned != 1 || result.mapped != wantMapped || result.changed != tt.wantChanged || result.updated != tt.wantUpdated ||
				result.v2Only != tt.wantV2Only || result.skipped != tt.wantSkipped || result.errors != tt.wantErrors || applied != wantApplied {
				t.Errorf("result=%+v applied=%d", result, applied)
			}
			_, markerErr := store.Get(t.Context(), pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceProject, uid))
			if (markerErr == nil) != tt.wantMarkerRemain {
				t.Errorf("marker present=%t, want=%t", markerErr == nil, tt.wantMarkerRemain)
			}
			if tt.wantV2Only > 0 {
				if !strings.Contains(logs.String(), `"project_uid":"`+uid+`"`) || !strings.Contains(logs.String(), `"field":"industry"`) {
					t.Errorf("conflict log missing UID and field: %s", logs.String())
				}
				if strings.Contains(logs.String(), "Energy & Climate") {
					t.Error("conflict log exposed classification values")
				}
			}
			if tt.malformed {
				if !strings.Contains(logs.String(), `"key":"$KV.v1-objects.salesforce-project__c.`+sfid+`"`) ||
					strings.Contains(logs.String(), "sensitive malformed row") {
					t.Errorf("decode error log must identify the key without exposing row contents: %s", logs.String())
				}
			}
		})
	}
	for _, tt := range []struct {
		name  string
		rate  float64
		limit int
	}{
		{name: "zero rate", rate: 0},
		{name: "negative rate", rate: -1},
		{name: "NaN rate", rate: math.NaN()},
		{name: "positive infinity", rate: math.Inf(1)},
		{name: "negative infinity", rate: math.Inf(-1)},
		{name: "rate rounds to zero nanoseconds", rate: 2e9},
		{name: "rate overflows duration", rate: 1e-20},
		{name: "negative limit", rate: 2, limit: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Validation must fail before scanning KV, even on a dry run.
			original := scanClassificationProjects
			t.Cleanup(func() { scanClassificationProjects = original })
			scanClassificationProjects = func(_ context.Context, _ jetstream.JetStream, _, _ string, _ time.Duration) (map[string][]byte, error) {
				t.Error("invalid rate reached the KV scan")
				return nil, nil
			}
			if _, err := backfillProjectClassifications(t.Context(), true, tt.limit, tt.rate); err == nil {
				t.Error("invalid rate/limit was accepted")
			}
		})
	}
}
