// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

func TestProjectSettingsWriteMayBeNeeded(t *testing.T) {
	jdoe := &projectservice.UserInfo{Username: stringToStringPtr("jdoe"), Name: stringToStringPtr("J Doe")}

	tests := []struct {
		name    string
		payload *projectservice.UpdateProjectSettingsPayload
		clears  staffClearFlags
		want    bool
	}{
		// The GH-179 regression guard: a staff clear with an otherwise all-nil
		// payload must still pass the gate so the full-replace PUT can clear
		// the role. Dropping a clear term from the predicate fails these.
		{
			name:    "ED clear flag alone passes",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			clears:  staffClearFlags{ExecutiveDirector: true},
			want:    true,
		},
		{
			name:    "PM clear flag alone passes",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			clears:  staffClearFlags{ProgramManager: true},
			want:    true,
		},
		{
			name:    "OO clear flag alone passes",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			clears:  staffClearFlags{OpportunityOwner: true},
			want:    true,
		},
		{
			name:    "no flags and all-nil payload skips the write",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			want:    false,
		},
		{
			name:    "mission statement passes",
			payload: &projectservice.UpdateProjectSettingsPayload{MissionStatement: stringToStringPtr("m")},
			want:    true,
		},
		{
			name:    "announcement date passes",
			payload: &projectservice.UpdateProjectSettingsPayload{AnnouncementDate: stringToStringPtr("2026-01-01")},
			want:    true,
		},
		{
			name:    "executive director passes",
			payload: &projectservice.UpdateProjectSettingsPayload{ExecutiveDirector: jdoe},
			want:    true,
		},
		{
			name:    "program manager passes",
			payload: &projectservice.UpdateProjectSettingsPayload{ProgramManager: jdoe},
			want:    true,
		},
		{
			name:    "opportunity owner passes",
			payload: &projectservice.UpdateProjectSettingsPayload{OpportunityOwner: jdoe},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := projectSettingsWriteMayBeNeeded(tt.payload, tt.clears); got != tt.want {
				t.Errorf("projectSettingsWriteMayBeNeeded() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProjectSettingsUpdateNeeded(t *testing.T) {
	jdoe := &projectservice.UserInfo{Username: stringToStringPtr("jdoe"), Name: stringToStringPtr("J Doe")}
	jdoeDup := &projectservice.UserInfo{Username: stringToStringPtr("jdoe"), Name: stringToStringPtr("J Doe")}
	asmith := &projectservice.UserInfo{Username: stringToStringPtr("asmith"), Name: stringToStringPtr("A Smith")}

	tests := []struct {
		name    string
		payload *projectservice.UpdateProjectSettingsPayload
		current *projectservice.ProjectSettings
		clears  staffClearFlags
		want    bool
	}{
		{
			name:    "ED clear pending and current role populated",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{ExecutiveDirector: jdoe},
			clears:  staffClearFlags{ExecutiveDirector: true},
			want:    true,
		},
		{
			name:    "ED clear pending and current role already empty",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{},
			clears:  staffClearFlags{ExecutiveDirector: true},
			want:    false,
		},
		{
			name:    "ED clear pending and current role an all-empty struct (nil ≡ all-empty)",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{ExecutiveDirector: &projectservice.UserInfo{}},
			clears:  staffClearFlags{ExecutiveDirector: true},
			want:    false,
		},
		{
			name:    "PM clear pending and current role populated",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{ProgramManager: asmith},
			clears:  staffClearFlags{ProgramManager: true},
			want:    true,
		},
		{
			name:    "OO clear pending and current role populated",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{OpportunityOwner: jdoe},
			clears:  staffClearFlags{OpportunityOwner: true},
			want:    true,
		},
		{
			name:    "OO clear pending and current role already empty",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{},
			clears:  staffClearFlags{OpportunityOwner: true},
			want:    false,
		},
		{
			name:    "staff reassignment differs from current",
			payload: &projectservice.UpdateProjectSettingsPayload{ExecutiveDirector: asmith},
			current: &projectservice.ProjectSettings{ExecutiveDirector: jdoe},
			want:    true,
		},
		{
			name:    "staff payload equal to current",
			payload: &projectservice.UpdateProjectSettingsPayload{ExecutiveDirector: jdoeDup, ProgramManager: asmith},
			current: &projectservice.ProjectSettings{ExecutiveDirector: jdoe, ProgramManager: asmith},
			want:    false,
		},
		{
			// Field set in v1 but unresolvable: mapper returns nil with the
			// flag unset, and whatever v2 has must be left alone.
			name:    "lookup-failure shape: nil payload field, no clear flag, current populated — preserved",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{ExecutiveDirector: jdoe},
			clears:  staffClearFlags{},
			want:    false,
		},
		{
			name:    "mission statement change",
			payload: &projectservice.UpdateProjectSettingsPayload{MissionStatement: stringToStringPtr("new")},
			current: &projectservice.ProjectSettings{MissionStatement: stringToStringPtr("old")},
			want:    true,
		},
		{
			name:    "announcement date change",
			payload: &projectservice.UpdateProjectSettingsPayload{AnnouncementDate: stringToStringPtr("2026-01-01")},
			current: &projectservice.ProjectSettings{AnnouncementDate: stringToStringPtr("2025-01-01")},
			want:    true,
		},
		{
			name:    "opportunity owner change",
			payload: &projectservice.UpdateProjectSettingsPayload{OpportunityOwner: asmith},
			current: &projectservice.ProjectSettings{OpportunityOwner: jdoe},
			want:    true,
		},
		{
			name:    "no payload fields, no flags, populated current — no-op",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{ExecutiveDirector: jdoe, MissionStatement: stringToStringPtr("m")},
			want:    false,
		},
		{
			name:    "ED clear rides along with an equal PM field",
			payload: &projectservice.UpdateProjectSettingsPayload{ProgramManager: asmith},
			current: &projectservice.ProjectSettings{ExecutiveDirector: jdoe, ProgramManager: asmith},
			clears:  staffClearFlags{ExecutiveDirector: true},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := projectSettingsUpdateNeeded(tt.payload, tt.current, tt.clears); got != tt.want {
				t.Errorf("projectSettingsUpdateNeeded() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHydrateSettingsPayload(t *testing.T) {
	jdoe := &projectservice.UserInfo{Username: stringToStringPtr("jdoe"), Name: stringToStringPtr("J Doe")}
	asmith := &projectservice.UserInfo{Username: stringToStringPtr("asmith"), Name: stringToStringPtr("A Smith")}
	writers := []*projectservice.UserInfo{jdoe}

	fullCurrent := func() *projectservice.ProjectSettings {
		return &projectservice.ProjectSettings{
			MissionStatement:    stringToStringPtr("mission"),
			AnnouncementDate:    stringToStringPtr("2026-01-01"),
			ExecutiveDirector:   jdoe,
			ProgramManager:      asmith,
			OpportunityOwner:    jdoe,
			Writers:             writers,
			MeetingCoordinators: writers,
			Auditors:            writers,
		}
	}

	tests := []struct {
		name    string
		payload *projectservice.UpdateProjectSettingsPayload
		current *projectservice.ProjectSettings
		clears  staffClearFlags
		// want nil = field must stay nil after hydration (deliberate clear).
		wantEDNil bool
		wantPMNil bool
		wantOONil bool
	}{
		{
			name:    "all-nil payload round-trips every current field",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: fullCurrent(),
		},
		{
			name:    "set fields are not overwritten",
			payload: &projectservice.UpdateProjectSettingsPayload{MissionStatement: stringToStringPtr("new"), ExecutiveDirector: asmith},
			current: fullCurrent(),
		},
		{
			// The Copilot-flagged case: ED deliberately cleared while the PM
			// lookup failed — PM must be preserved, not wiped by the PUT.
			name:      "ED clear preserves a lookup-failed PM",
			payload:   &projectservice.UpdateProjectSettingsPayload{},
			current:   fullCurrent(),
			clears:    staffClearFlags{ExecutiveDirector: true},
			wantEDNil: true,
		},
		{
			name:      "all three staff clears stay nil",
			payload:   &projectservice.UpdateProjectSettingsPayload{},
			current:   fullCurrent(),
			clears:    staffClearFlags{ExecutiveDirector: true, ProgramManager: true, OpportunityOwner: true},
			wantEDNil: true,
			wantPMNil: true,
			wantOONil: true,
		},
		{
			name:      "OO clear stays nil while ED and PM hydrate",
			payload:   &projectservice.UpdateProjectSettingsPayload{},
			current:   fullCurrent(),
			clears:    staffClearFlags{OpportunityOwner: true},
			wantOONil: true,
		},
		{
			name:    "empty current leaves payload fields nil",
			payload: &projectservice.UpdateProjectSettingsPayload{},
			current: &projectservice.ProjectSettings{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture explicitly-set values before hydration to verify they survive.
			setMission := tt.payload.MissionStatement
			setED := tt.payload.ExecutiveDirector

			hydrateSettingsPayload(tt.payload, tt.current, tt.clears)

			if setMission != nil && tt.payload.MissionStatement != setMission {
				t.Errorf("MissionStatement overwritten: got %v, want original %v", *tt.payload.MissionStatement, *setMission)
			}
			if setMission == nil && stringPtrToString(tt.payload.MissionStatement) != stringPtrToString(tt.current.MissionStatement) {
				t.Errorf("MissionStatement not hydrated: got %v, want %v", stringPtrToString(tt.payload.MissionStatement), stringPtrToString(tt.current.MissionStatement))
			}
			if stringPtrToString(tt.payload.AnnouncementDate) != stringPtrToString(tt.current.AnnouncementDate) {
				t.Errorf("AnnouncementDate = %v, want %v", stringPtrToString(tt.payload.AnnouncementDate), stringPtrToString(tt.current.AnnouncementDate))
			}
			if tt.wantEDNil {
				if tt.payload.ExecutiveDirector != nil {
					t.Errorf("ExecutiveDirector = %v, want nil (deliberate clear)", tt.payload.ExecutiveDirector)
				}
			} else if setED != nil {
				if tt.payload.ExecutiveDirector != setED {
					t.Errorf("ExecutiveDirector overwritten, want original")
				}
			} else if !userInfoPtrsEqual(tt.payload.ExecutiveDirector, tt.current.ExecutiveDirector) {
				t.Errorf("ExecutiveDirector not hydrated from current")
			}
			if tt.wantPMNil {
				if tt.payload.ProgramManager != nil {
					t.Errorf("ProgramManager = %v, want nil (deliberate clear)", tt.payload.ProgramManager)
				}
			} else if !userInfoPtrsEqual(tt.payload.ProgramManager, tt.current.ProgramManager) {
				t.Errorf("ProgramManager not hydrated from current")
			}
			if tt.wantOONil {
				if tt.payload.OpportunityOwner != nil {
					t.Errorf("OpportunityOwner = %v, want nil (deliberate clear)", tt.payload.OpportunityOwner)
				}
			} else if !userInfoPtrsEqual(tt.payload.OpportunityOwner, tt.current.OpportunityOwner) {
				t.Errorf("OpportunityOwner not hydrated from current")
			}
			if !userInfoSlicesEqual(tt.payload.Writers, tt.current.Writers) {
				t.Errorf("Writers not hydrated from current")
			}
			if !userInfoSlicesEqual(tt.payload.MeetingCoordinators, tt.current.MeetingCoordinators) {
				t.Errorf("MeetingCoordinators not hydrated from current")
			}
			if !userInfoSlicesEqual(tt.payload.Auditors, tt.current.Auditors) {
				t.Errorf("Auditors not hydrated from current")
			}
		})
	}
}
