// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"testing"
)

func TestChangedFormationLeadFields(t *testing.T) {
	old := map[string]any{"formation_lead_name": "Alice Example", "formation_lead_email": " alice@example.com ", "formation_lead_avatar": "https://example.com/a.png"}
	for _, tt := range []struct {
		name string
		next map[string]any
		want []string
	}{
		{name: "base-only edit", next: map[string]any{"name": "Renamed", "formation_lead_name": "Alice Example", "formation_lead_email": "alice@example.com", "formation_lead_avatar": "https://example.com/a.png"}},
		{name: "PCC name edit", next: map[string]any{"formation_lead_name": "Bob Fixture", "formation_lead_email": "alice@example.com", "formation_lead_avatar": "https://example.com/a.png"}, want: []string{"name"}},
		{name: "email and avatar edit", next: map[string]any{"formation_lead_name": "Alice Example", "formation_lead_email": "bob@example.com", "formation_lead_avatar": "https://example.com/b.png"}, want: []string{"email", "avatar"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fields := changedFormationLeadFields(old, tt.next)
			if len(fields) != len(tt.want) {
				t.Fatalf("changed fields = %v, want %v", fields, tt.want)
			}
			for i := range fields {
				if fields[i] != tt.want[i] {
					t.Errorf("changed fields = %v, want %v", fields, tt.want)
				}
			}
			mask, present := formationLeadChanges(map[string]any{formationLeadChangesKey: fields})
			if !present || len(mask) != len(fields) {
				t.Errorf("stored field mask = %v, present=%t; want %v", mask, present, fields)
			}
		})
	}
}

func TestHandleWALUpsert_FormationLeadChangeMask(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{Auth0ClientID: "test-client-id"}
	fake := newFakeKV()
	v1KV = fake
	t.Cleanup(func() { cfg, v1KV = origCfg, origKV })
	const key = "salesforce-project__c.test-sfid"
	old := map[string]any{
		"sfid": "test-sfid", "name": "Test Project", "formation_lead_name": "Alice Example",
		"formation_lead_email": "alice@example.com", "systemmodstamp": "2026-11-11T00:00:00Z",
		formationLeadVersionsKey:     map[string]uint64{"name": 0, "email": 0, "avatar": 0},
		formationLeadCurrentHumanKey: map[string]bool{"name": false, "email": false, "avatar": false},
	}
	data, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	fake.data[key], fake.rev[key] = data, 1
	for i, tt := range []struct {
		name, email, editor, want string
		wantEmailVersion          uint64
		wantCurrentHuman          bool
	}{
		{name: "base-only project rename", email: "alice@example.com", editor: "human-editor"},
		{name: "contact email edit", email: "bob@example.com", editor: "human-editor", want: "email", wantEmailVersion: 1, wantCurrentHuman: true},
		{name: "late reverse PATCH echo", email: "stale@example.com", editor: "test-client-id@clients", want: "email", wantEmailVersion: 1},
		{name: "later base-only rename retains human-only history", email: "stale@example.com", editor: "human-editor", wantEmailVersion: 1},
		{name: "later PCC contact edit increments history", email: "carol@example.com", editor: "human-editor", want: "email", wantEmailVersion: 2, wantCurrentHuman: true},
	} {
		event := &WALEvent{Schema: "salesforce", Table: "project__c", Action: "UPDATE", CommitTime: "2026-11-11T00:00:01Z",
			Data: map[string]any{"sfid": "test-sfid", "name": tt.name, "formation_lead_name": "Alice Example",
				"formation_lead_email": tt.email, "lastmodifiedbyid": tt.editor, "systemmodstamp": "2026-11-11T00:00:0" + string(rune('1'+i)) + "Z"},
		}
		if handleWALUpsert(context.Background(), event) {
			t.Fatal("unexpected WAL retry")
		}
		var stored map[string]any
		if err := json.Unmarshal(fake.data[key], &stored); err != nil {
			t.Fatal(err)
		}
		mask, present := formationLeadChanges(stored)
		wantCount := 0
		if tt.want != "" {
			wantCount = 1
		}
		if !present || len(mask) != wantCount || (tt.want != "" && !mask[tt.want]) {
			t.Errorf("%s: durable change mask=%v present=%t, want %q", tt.name, mask, present, tt.want)
		}
		versions, ok := formationLeadFieldVersions(stored)
		if !ok || versions["email"] != tt.wantEmailVersion {
			t.Errorf("%s: cumulative email generation=%v present=%t, want %d", tt.name, versions, ok, tt.wantEmailVersion)
		}
		currentHuman, ok := formationLeadCurrentHumanFields(stored)
		if !ok || currentHuman["email"] != tt.wantCurrentHuman {
			t.Errorf("%s: current email human provenance=%v present=%t, want %t", tt.name, currentHuman, ok, tt.wantCurrentHuman)
		}
	}
}

func TestHandleWALUpsert_FormationLeadFirstInsert(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{Auth0ClientID: "test-client-id"}
	fake := newFakeKV()
	v1KV = fake
	t.Cleanup(func() { cfg, v1KV = origCfg, origKV })
	for _, tt := range []struct {
		name, sfid, email string
		wantVersion       uint64
	}{
		{name: "empty initial contact", sfid: "test-empty", wantVersion: 0},
		{name: "human contact already present on first WAL row", sfid: "test-human", email: "human@example.com", wantVersion: 1},
	} {
		key := "salesforce-project__c." + tt.sfid
		event := &WALEvent{Schema: "salesforce", Table: "project__c", Action: "INSERT", CommitTime: "2026-11-11T00:00:01Z",
			Data: map[string]any{"sfid": tt.sfid, "lastmodifiedbyid": "human-editor", "formation_lead_email": tt.email, "systemmodstamp": "2026-11-11T00:00:01Z"}}
		if handleWALUpsert(t.Context(), event) {
			t.Fatalf("%s: unexpected WAL retry", tt.name)
		}
		var stored map[string]any
		if err := json.Unmarshal(fake.data[key], &stored); err != nil {
			t.Fatal(err)
		}
		versions, ok := formationLeadFieldVersions(stored)
		if !ok || versions["email"] != tt.wantVersion {
			t.Errorf("%s: first WAL generation=%v present=%t, want %d", tt.name, versions, ok, tt.wantVersion)
		}
	}
}

func TestHandleWALUpsert_FormationLeadLegacyHistory(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{Auth0ClientID: "test-client-id"}
	fake := newFakeKV()
	v1KV = fake
	t.Cleanup(func() { cfg, v1KV = origCfg, origKV })
	const key = "salesforce-project__c.test-sfid"
	old := map[string]any{"sfid": "test-sfid", "formation_lead_name": "Alice Example", "formation_lead_email": "alice@example.com", "systemmodstamp": "2026-11-11T00:00:00Z"}
	data, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	fake.data[key], fake.rev[key] = data, 1
	for i, email := range []string{"alice@example.com", "bob@example.com"} {
		event := &WALEvent{Schema: "salesforce", Table: "project__c", Action: "UPDATE", CommitTime: "2026-11-11T00:00:01Z",
			Data: map[string]any{"sfid": "test-sfid", "name": "Renamed Project", "lastmodifiedbyid": "human-editor", "formation_lead_name": "Alice Example", "formation_lead_email": email, "systemmodstamp": "2026-11-11T00:00:0" + string(rune('1'+i)) + "Z"}}
		if handleWALUpsert(t.Context(), event) {
			t.Fatal("unexpected WAL retry")
		}
		var stored map[string]any
		if err := json.Unmarshal(fake.data[key], &stored); err != nil {
			t.Fatal(err)
		}
		if versions, ok := formationLeadFieldVersions(stored); ok {
			t.Errorf("legacy row invented trustworthy contact history: %+v", versions)
		}
	}
	// An unreadable existing entry is not equivalent to a first insert.
	const unreadableKey = "salesforce-project__c.unreadable"
	fake.data[unreadableKey], fake.rev[unreadableKey] = []byte{}, 1
	event := &WALEvent{Schema: "salesforce", Table: "project__c", Action: "UPDATE", CommitTime: "2026-11-11T00:00:03Z",
		Data: map[string]any{"sfid": "unreadable", "lastmodifiedbyid": "human-editor", "formation_lead_email": "human@example.com", "systemmodstamp": "2026-11-11T00:00:03Z"}}
	if handleWALUpsert(t.Context(), event) {
		t.Fatal("unexpected WAL retry on existing unreadable row")
	}
	var stored map[string]any
	if err := json.Unmarshal(fake.data[unreadableKey], &stored); err != nil {
		t.Fatal(err)
	}
	if versions, ok := formationLeadFieldVersions(stored); ok {
		t.Errorf("unreadable prior entry invented a zero baseline: %+v", versions)
	}
}
