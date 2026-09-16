// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"testing"
)

// No test case populates a staff SFID with a resolvable value: lookupStaffUser
// would reach the uninitialized v1DB handle and panic, and
// lookupOpportunityOwner's lookupB2BUser the uninitialized v1KV handle, so a
// passing run also proves the empty-field path performs no user lookup.
func TestMapV1DataToProjectUpdateSettingsPayload_StaffClearFlags(t *testing.T) {
	tests := []struct {
		name       string
		v1Data     map[string]any
		wantClears staffClearFlags
	}{
		{
			name:       "all staff fields absent",
			v1Data:     map[string]any{},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true},
		},
		{
			name: "empty strings",
			v1Data: map[string]any{
				"executive_director__c": "",
				"program_manager__c":    "",
			},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true},
		},
		{
			name: "whitespace-only strings",
			v1Data: map[string]any{
				"executive_director__c": "   ",
				"program_manager__c":    "\t\n ",
			},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true},
		},
		{
			// Mirrors lookupStaffUser's `!ok || sfid == ""` semantics: a
			// non-string value is treated the same as an empty field.
			name: "non-string staff values treated as cleared",
			v1Data: map[string]any{
				"executive_director__c": 123,
				"program_manager__c":    nil,
			},
			wantClears: staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true},
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
