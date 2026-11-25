// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFormationLeadBackfillPayload(t *testing.T) {
	current := &projectservice.ProjectSettings{
		MissionStatement: stringToStringPtr("Existing mission"),
		Writers:          []*projectservice.UserInfo{{Email: stringToStringPtr("alice@example.com")}},
		FormationLead:    &projectservice.FormationLead{Email: stringToStringPtr("alice@example.com")},
	}
	for _, tt := range []struct {
		name      string
		lead      *projectservice.FormationLead
		wantEmail string
		wantClear bool
	}{
		{name: "replace", lead: &projectservice.FormationLead{Name: stringToStringPtr("Bob Fixture"), Email: stringToStringPtr("bob@example.com")}, wantEmail: "bob@example.com"},
		{name: "clear", wantClear: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := formationLeadBackfillPayload("test-uid", "3", current, tt.lead)
			if payload.UID == nil || *payload.UID != "test-uid" || payload.IfMatch == nil || *payload.IfMatch != "3" {
				t.Errorf("UID/IfMatch = %v/%v, want test-uid/3", payload.UID, payload.IfMatch)
			}
			if payload.FormationLead == nil {
				t.Fatal("FormationLead must be non-nil; v2 interprets nil as preserve")
			}
			if tt.wantClear && !formationLeadsEqual(payload.FormationLead, nil) {
				t.Errorf("FormationLead = %+v, want explicit empty object", payload.FormationLead)
			}
			if !tt.wantClear && stringPtrToString(payload.FormationLead.Email) != tt.wantEmail {
				t.Errorf("FormationLead.Email = %q, want %q", stringPtrToString(payload.FormationLead.Email), tt.wantEmail)
			}
			if payload.MissionStatement != current.MissionStatement || len(payload.Writers) != 1 || payload.Writers[0] != current.Writers[0] {
				t.Error("unrelated settings were not preserved on full-document PUT")
			}
		})
	}
}

func TestBackfillFormationLeads(t *testing.T) {
	origCfg := cfg
	origLogger := logger
	origScan, origMappings := scanFormationLeadProjects, collectFormationLeadMappings
	origFetch, origApply := fetchFormationLeadSettings, applyFormationLeadSettings
	t.Cleanup(func() {
		cfg = origCfg
		logger = origLogger
		scanFormationLeadProjects, collectFormationLeadMappings = origScan, origMappings
		fetchFormationLeadSettings, applyFormationLeadSettings = origFetch, origApply
	})
	cfg = &Config{}
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, rate := range []float64{math.Inf(1), math.NaN(), 1e10, math.SmallestNonzeroFloat64, 0} {
		if _, err := backfillFormationLeads(t.Context(), true, 0, rate); err == nil {
			t.Errorf("invalid emit rate %g did not return an error", rate)
		}
	}
	const prefix = "$KV.v1-objects.salesforce-project__c."
	scanFormationLeadProjects = func(_ context.Context, _ jetstream.JetStream, _, _ string, _ time.Duration) (map[string][]byte, error) {
		return map[string][]byte{
			prefix + "deleted":  []byte(`{"sfid":"deleted","isdeleted":true}`),
			prefix + "unmapped": []byte(`{"sfid":"unmapped","formation_lead_email":"unmapped@example.com"}`),
			prefix + "matching": []byte(`{"sfid":"matching","formation_lead_email":"same@example.com"}`),
			prefix + "first":    []byte(`{"sfid":"first","formation_lead_email":"first@example.com"}`),
			prefix + "second":   []byte(`{"sfid":"second","formation_lead_email":"second@example.com"}`),
			prefix + "v2only":   []byte(`{"sfid":"v2only"}`),
		}, nil
	}
	collectFormationLeadMappings = func(context.Context) (map[string]string, map[string]struct{}, error) {
		return map[string]string{
			"deleted": "uid-deleted", "matching": "uid-matching", "first": "uid-first", "second": "uid-second", "v2only": "uid-v2only",
		}, nil, nil
	}
	fetchFormationLeadSettings = func(_ context.Context, uid string) (*projectservice.ProjectSettings, string, error) {
		switch uid {
		case "uid-matching":
			return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("same@example.com")}}, "1", nil
		case "uid-v2only":
			return &projectservice.ProjectSettings{FormationLead: &projectservice.FormationLead{Email: stringToStringPtr("v2@example.com")}}, "1", nil
		default:
			return &projectservice.ProjectSettings{}, "1", nil
		}
	}
	for _, tt := range []struct {
		name                    string
		dryRun                  bool
		limit                   int
		failApply               bool
		generatedError          bool
		wantUpdated, wantErrors int
	}{
		{name: "dry run counts differences without writes", dryRun: true},
		{name: "limit caps successful writes", limit: 1, wantUpdated: 1},
		{name: "unlimited run writes both changes", limit: 0, wantUpdated: 2},
		{name: "write failure is counted", limit: 1, failApply: true, wantErrors: 2},
		{name: "generated error detail survives logging", limit: 1, failApply: true, generatedError: true, wantErrors: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger = slog.New(slog.NewTextHandler(&logs, nil))
			calls := 0
			applyFormationLeadSettings = func(_ context.Context, _ *projectservice.UpdateProjectSettingsPayload) error {
				calls++
				if tt.failApply {
					if tt.generatedError {
						return &projectservice.ConflictError{Code: "Conflict", Message: "test etag conflict"}
					}
					return errors.New("test write error")
				}
				return nil
			}
			res, err := backfillFormationLeads(t.Context(), tt.dryRun, tt.limit, 1_000_000)
			if res.scanned != 6 || res.mapped != 4 || res.skipped != 2 || res.changed != 2 || res.v2Only != 1 || res.updated != tt.wantUpdated || res.errors != tt.wantErrors {
				t.Errorf("backfill result = %+v, want scanned=6 mapped=4 skipped=2 changed=2 v2Only=1 updated=%d errors=%d", res, tt.wantUpdated, tt.wantErrors)
			}
			if (err != nil) != tt.failApply {
				t.Errorf("backfill error = %v, want failure=%t", err, tt.failApply)
			}
			if tt.dryRun && calls != 0 {
				t.Errorf("dry run called apply %d times, want 0", calls)
			}
			if !tt.dryRun && !tt.failApply && calls != tt.wantUpdated {
				t.Errorf("apply calls = %d, want %d", calls, tt.wantUpdated)
			}
			if tt.generatedError && !strings.Contains(logs.String(), "test etag conflict") {
				t.Errorf("generated error detail missing from logs: %s", logs.String())
			}
		})
	}
}
