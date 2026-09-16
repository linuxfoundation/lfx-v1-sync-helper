// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

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
