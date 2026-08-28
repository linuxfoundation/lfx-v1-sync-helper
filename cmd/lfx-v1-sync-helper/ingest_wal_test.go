// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import "testing"

func TestWALEventGetSFID(t *testing.T) {
	tests := []struct {
		name     string
		event    WALEvent
		wantSFID string
		wantOK   bool
	}{
		{
			name: "sfid-keyed table (default) on insert/update",
			event: WALEvent{
				Table:  "merged_user",
				Action: "UPDATE",
				Data:   map[string]interface{}{"sfid": "003xx000"},
			},
			wantSFID: "003xx000",
			wantOK:   true,
		},
		{
			name: "sfid-keyed table on delete reads DataOld",
			event: WALEvent{
				Table:   "alternate_email__c",
				Action:  "DELETE",
				DataOld: map[string]interface{}{"sfid": "003xx001"},
			},
			wantSFID: "003xx001",
			wantOK:   true,
		},
		{
			name: "user_skills falls back to id column on insert/update",
			event: WALEvent{
				Table:  "user_skills",
				Action: "INSERT",
				Data:   map[string]interface{}{"id": "usk-1", "lfid": "jdoe", "skill_id": "sk-1"},
			},
			wantSFID: "usk-1",
			wantOK:   true,
		},
		{
			name: "user_skills falls back to id column on delete via DataOld",
			event: WALEvent{
				Table:   "user_skills",
				Action:  "DELETE",
				DataOld: map[string]interface{}{"id": "usk-1"},
			},
			wantSFID: "usk-1",
			wantOK:   true,
		},
		{
			name: "user_skills with sfid present is still ignored in favor of id",
			event: WALEvent{
				Table: "user_skills",
				Data:  map[string]interface{}{"id": "usk-1", "sfid": "should-not-be-used"},
			},
			wantSFID: "usk-1",
			wantOK:   true,
		},
		{
			name: "missing pk column returns not-ok",
			event: WALEvent{
				Table: "merged_user",
				Data:  map[string]interface{}{"username__c": "jdoe"},
			},
			wantSFID: "",
			wantOK:   false,
		},
		{
			name:     "nil data source returns not-ok",
			event:    WALEvent{Table: "merged_user", Action: "DELETE"},
			wantSFID: "",
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSFID, gotOK := tt.event.GetSFID()
			if gotSFID != tt.wantSFID || gotOK != tt.wantOK {
				t.Errorf("GetSFID() = (%q, %v), want (%q, %v)", gotSFID, gotOK, tt.wantSFID, tt.wantOK)
			}
		})
	}
}
