// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"testing"
)

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

// TestShouldUpdateBasedOnCommitTime covers the timestampless-table update
// decision used by walTimestamplessTables (e.g. user_skills), which has no
// systemmodstamp/lastmodifieddate columns to compare.
func TestShouldUpdateBasedOnCommitTime(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		newCommitTime string
		existingData  map[string]interface{}
		want          bool
	}{
		{
			name:          "newer commit time updates",
			newCommitTime: "2024-01-02T00:00:00Z",
			existingData:  map[string]interface{}{"_sdc_extracted_at": "2024-01-01T00:00:00Z"},
			want:          true,
		},
		{
			name:          "equal commit time skips",
			newCommitTime: "2024-01-01T00:00:00Z",
			existingData:  map[string]interface{}{"_sdc_extracted_at": "2024-01-01T00:00:00Z"},
			want:          false,
		},
		{
			name:          "older commit time skips",
			newCommitTime: "2023-12-31T00:00:00Z",
			existingData:  map[string]interface{}{"_sdc_extracted_at": "2024-01-01T00:00:00Z"},
			want:          false,
		},
		{
			name:          "unparseable new commit time skips",
			newCommitTime: "not-a-timestamp",
			existingData:  map[string]interface{}{"_sdc_extracted_at": "2024-01-01T00:00:00Z"},
			want:          false,
		},
		{
			name:          "missing existing commit time updates",
			newCommitTime: "2024-01-01T00:00:00Z",
			existingData:  map[string]interface{}{},
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldUpdateBasedOnCommitTime(ctx, tt.newCommitTime, tt.existingData, "salesforce-user_skills.usk-1")
			if got != tt.want {
				t.Errorf("shouldUpdateBasedOnCommitTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHandleWALUpsert_UserSkillsCommitTimeBranch confirms handleWALUpsert
// routes user_skills (a walTimestamplessTables member) through
// shouldUpdateBasedOnCommitTime rather than shouldUpdateBasedOnTimestamps: a
// later CommitTime overwrites the existing KV entry, while an equal or
// earlier one is skipped.
func TestHandleWALUpsert_UserSkillsCommitTimeBranch(t *testing.T) {
	newEvent := func(commitTime string) *WALEvent {
		return &WALEvent{
			Schema:     "salesforce",
			Table:      "user_skills",
			Action:     "UPDATE",
			CommitTime: commitTime,
			Data:       map[string]interface{}{"id": "usk-1", "lfid": "jdoe", "skill_id": "sk-2"},
		}
	}

	t.Run("later commit time overwrites the existing entry", func(t *testing.T) {
		origCfg, origKV := cfg, v1KV
		cfg = &Config{}
		fake := newFakeKV()
		v1KV = fake
		defer func() {
			cfg = origCfg
			v1KV = origKV
		}()

		key := "salesforce-user_skills.usk-1"
		existing, _ := json.Marshal(map[string]interface{}{
			"id": "usk-1", "lfid": "jdoe", "skill_id": "sk-1", "_sdc_extracted_at": "2024-01-01T00:00:00Z",
		})
		fake.data[key] = existing
		fake.rev[key] = 1

		if retry := handleWALUpsert(context.Background(), newEvent("2024-01-02T00:00:00Z")); retry {
			t.Fatal("expected no retry")
		}
		if fake.rev[key] != 2 {
			t.Fatalf("expected revision to advance to 2, got %d", fake.rev[key])
		}
		var got map[string]interface{}
		if err := json.Unmarshal(fake.data[key], &got); err != nil {
			t.Fatalf("failed to unmarshal updated entry: %v", err)
		}
		if got["skill_id"] != "sk-2" {
			t.Errorf("expected the newer skill_id to overwrite the existing entry, got %v", got["skill_id"])
		}
	})

	t.Run("equal or earlier commit time is skipped", func(t *testing.T) {
		origCfg, origKV := cfg, v1KV
		cfg = &Config{}
		fake := newFakeKV()
		v1KV = fake
		defer func() {
			cfg = origCfg
			v1KV = origKV
		}()

		key := "salesforce-user_skills.usk-1"
		existing, _ := json.Marshal(map[string]interface{}{
			"id": "usk-1", "lfid": "jdoe", "skill_id": "sk-1", "_sdc_extracted_at": "2024-01-02T00:00:00Z",
		})
		fake.data[key] = existing
		fake.rev[key] = 1

		if retry := handleWALUpsert(context.Background(), newEvent("2024-01-02T00:00:00Z")); retry {
			t.Fatal("expected no retry")
		}
		if fake.rev[key] != 1 {
			t.Errorf("expected revision to stay at 1 (update skipped), got %d", fake.rev[key])
		}
		var got map[string]interface{}
		if err := json.Unmarshal(fake.data[key], &got); err != nil {
			t.Fatalf("failed to unmarshal entry: %v", err)
		}
		if got["skill_id"] != "sk-1" {
			t.Errorf("expected the existing skill_id to be preserved, got %v", got["skill_id"])
		}
	})
}
