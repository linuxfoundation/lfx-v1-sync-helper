// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"testing"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

// No test case populates a staff SFID with a resolvable value: lookupStaffUser
// would reach the uninitialized v1DB handle and panic, while
// lookupOpportunityOwner's lookupB2BUser would reach the uninitialized v1KV
// handle, so a passing run also proves the empty-field path performs no user
// lookup.
func TestMapV1DataToProjectUpdateSettingsPayload_StaffClearFlags(t *testing.T) {
	tests := []struct {
		name       string
		v1Data     map[string]any
		wantClears staffClearFlags
	}{
		{
			name:       "all staff fields absent",
			v1Data:     map[string]any{},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true, FormationLead: true},
		},
		{
			name: "empty strings",
			v1Data: map[string]any{
				"executive_director__c": "",
				"program_manager__c":    "",
			},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true, FormationLead: true},
		},
		{
			name: "whitespace-only strings",
			v1Data: map[string]any{
				"executive_director__c": "   ",
				"program_manager__c":    "\t\n ",
			},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true, FormationLead: true},
		},
		{
			// Mirrors lookupStaffUser's `!ok || sfid == ""` semantics: a
			// non-string value is treated the same as an empty field.
			name: "non-string staff values treated as cleared",
			v1Data: map[string]any{
				"executive_director__c": 123,
				"program_manager__c":    nil,
			},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true, FormationLead: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, clears, err := mapV1DataToProjectUpdateSettingsPayload(context.Background(), "project-uid", tt.v1Data)
			if err != nil {
				t.Fatalf("mapV1DataToProjectUpdateSettingsPayload returned error: %v", err)
			}
			if clears != tt.wantClears {
				t.Errorf("clears = %+v, want %+v", clears, tt.wantClears)
			}
			if payload.ExecutiveDirector != nil {
				t.Errorf("payload.ExecutiveDirector = %+v, want nil on the clear path", payload.ExecutiveDirector)
			}
			if payload.ProgramManager != nil {
				t.Errorf("payload.ProgramManager = %+v, want nil on the clear path", payload.ProgramManager)
			}
			if payload.OpportunityOwner != nil {
				t.Errorf("payload.OpportunityOwner = %+v, want nil on the clear path", payload.OpportunityOwner)
			}
			if payload.UID == nil || *payload.UID != "project-uid" {
				t.Errorf("payload.UID = %v, want %q", payload.UID, "project-uid")
			}
		})
	}
}

func TestFormationLeadFromV1(t *testing.T) {
	for _, tt := range []struct {
		name                            string
		row                             map[string]any
		wantName, wantEmail, wantAvatar string
	}{
		{name: "absent", row: map[string]any{}},
		{name: "all empty", row: map[string]any{"formation_lead_name": "", "formation_lead_email": "", "formation_lead_avatar": ""}},
		{name: "name and email", row: map[string]any{"formation_lead_name": "Alice Example", "formation_lead_email": "alice@example.com"}, wantName: "Alice Example", wantEmail: "alice@example.com"},
		{name: "padded email", row: map[string]any{"formation_lead_name": " Alice Example ", "formation_lead_email": " alice@example.com ", "formation_lead_avatar": " https://example.com/avatar.png "}, wantName: " Alice Example ", wantEmail: "alice@example.com", wantAvatar: " https://example.com/avatar.png "},
		{name: "avatar retained", row: map[string]any{"formation_lead_avatar": "https://example.com/avatar.png"}, wantAvatar: "https://example.com/avatar.png"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lead := formationLeadFromV1(tt.row)
			if tt.wantName == "" && tt.wantEmail == "" && tt.wantAvatar == "" {
				if lead != nil {
					t.Errorf("formationLeadFromV1 = %+v, want nil", lead)
				}
				return
			}
			if lead == nil || *lead.Name != tt.wantName || *lead.Email != tt.wantEmail || *lead.Avatar != tt.wantAvatar {
				t.Errorf("formationLeadFromV1 = %+v, want name=%q email=%q avatar=%q", lead, tt.wantName, tt.wantEmail, tt.wantAvatar)
			}
		})
	}
}

func TestPreservePendingFormationLead(t *testing.T) {
	settings := &projectservice.UpdateProjectSettingsPayload{
		FormationLead:    &projectservice.FormationLead{Email: stringToStringPtr("stale@example.com")},
		MissionStatement: stringToStringPtr("latest project edit"),
	}
	clears := staffClearFlags{FormationLead: true, ExecutiveDirector: true}
	preservePendingFormationLead(settings, &clears)
	if settings.FormationLead != nil || clears.FormationLead || !clears.ExecutiveDirector || stringPtrToString(settings.MissionStatement) != "latest project edit" {
		t.Errorf("pending lead preservation changed unrelated settings: settings=%+v clears=%+v", settings, clears)
	}
}
