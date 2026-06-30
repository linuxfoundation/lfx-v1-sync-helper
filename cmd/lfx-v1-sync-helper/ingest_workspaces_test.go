// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestIsDeletedRecord covers all soft-delete detection paths.
func TestIsDeletedRecord(t *testing.T) {
	tests := []struct {
		name string
		m    map[string]any
		want bool
	}{
		{"is_deleted true", map[string]any{"is_deleted": true}, true},
		{"is_deleted false", map[string]any{"is_deleted": false}, false},
		{"_sdc_deleted_at non-empty string", map[string]any{"_sdc_deleted_at": "2024-01-01"}, true},
		{"_sdc_deleted_at empty string", map[string]any{"_sdc_deleted_at": ""}, false},
		{"_sdc_deleted_at whitespace", map[string]any{"_sdc_deleted_at": "   "}, false},
		{"_sdc_deleted_at non-nil non-string", map[string]any{"_sdc_deleted_at": 1}, true},
		{"empty map", map[string]any{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDeletedRecord(tc.m); got != tc.want {
				t.Errorf("isDeletedRecord() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestStringField covers multi-key extraction.
func TestStringField(t *testing.T) {
	m := map[string]any{
		"a": "val-a",
		"b": "",
		"c": "val-c",
	}
	if got := stringField(m, "missing", "b", "c"); got != "val-c" {
		t.Errorf("got %q, want %q", got, "val-c")
	}
	if got := stringField(m, "a"); got != "val-a" {
		t.Errorf("got %q, want %q", got, "val-a")
	}
	if got := stringField(m, "missing"); got != "" {
		t.Errorf("got %q, want %q", got, "")
	}
}

// TestFnv32hexDeterministic confirms the hash is stable across calls.
func TestFnv32hexDeterministic(t *testing.T) {
	h1 := fnv32hex("my workspace name")
	h2 := fnv32hex("my workspace name")
	if h1 != h2 {
		t.Errorf("fnv32hex not deterministic: %q != %q", h1, h2)
	}
	if fnv32hex("a") == fnv32hex("b") {
		t.Error("fnv32hex collision for different inputs")
	}
}

// TestWorkspaceCacheKey verifies the key format.
func TestWorkspaceCacheKey(t *testing.T) {
	key := workspaceCacheKey("org-123", "My Workspace")
	if key == "" {
		t.Fatal("cache key is empty")
	}
	expected := "workspace.uid.org-123." + fnv32hex("My Workspace")
	if key != expected {
		t.Errorf("cache key = %q, want %q", key, expected)
	}
}

// TestResolveProjectUIDs verifies project UID resolution: SFID lookup, uuid:slug extraction,
// deleted-project exclusion, and unmappable-project skip behavior.
func TestResolveProjectUIDs(t *testing.T) {
	ctx := context.Background()
	mappings := map[string]string{
		"sfid-A": "uid-A",
		"sfid-B": "uid-B",
	}

	tests := []struct {
		name     string
		projects []legacyWorkspaceProject
		wantUIDs []string
	}{
		{
			name: "all mappable",
			projects: []legacyWorkspaceProject{
				{ProjectSFID: "sfid-A"},
				{ProjectSFID: "sfid-B"},
			},
			wantUIDs: []string{"uid-A", "uid-B"},
		},
		{
			name: "one unmappable — workspace still proceeds with mappable subset",
			projects: []legacyWorkspaceProject{
				{ProjectSFID: "sfid-A"},
				{ProjectSFID: "sfid-MISSING"},
			},
			wantUIDs: []string{"uid-A"},
		},
		{
			name: "deleted project excluded",
			projects: []legacyWorkspaceProject{
				{ProjectSFID: "sfid-A", Deleted: true},
				{ProjectSFID: "sfid-B"},
			},
			wantUIDs: []string{"uid-B"},
		},
		{
			name:     "empty projects list",
			projects: []legacyWorkspaceProject{},
			wantUIDs: nil,
		},
		{
			// platform.organization_workspace_project stores project_id as
			// "uuid:slug" when the record was created via the v2 platform.
			// The UUID prefix is extracted directly without an SFID map lookup.
			name: "uuid:slug composite - uuid extracted",
			projects: []legacyWorkspaceProject{
				{ProjectSFID: "56fa1b4b-eca7-4824-a635-504a5e9a38cb:iree"},
				{ProjectSFID: "8234d49d-b9ca-4ba0-a287-0c7585c96590:ptproject"},
			},
			wantUIDs: []string{
				"56fa1b4b-eca7-4824-a635-504a5e9a38cb",
				"8234d49d-b9ca-4ba0-a287-0c7585c96590",
			},
		},
		{
			// Mixed: some uuid:slug and some legacy SFIDs in the same workspace.
			name: "mixed uuid:slug and sfid",
			projects: []legacyWorkspaceProject{
				{ProjectSFID: "56fa1b4b-eca7-4824-a635-504a5e9a38cb:iree"},
				{ProjectSFID: "sfid-A"},
				{ProjectSFID: "sfid-MISSING"},
			},
			wantUIDs: []string{
				"56fa1b4b-eca7-4824-a635-504a5e9a38cb",
				"uid-A",
			},
		},
		{
			// A uuid:slug where the slug portion contains a colon (edge case) —
			// only the first segment is used as the UUID.
			name: "uuid:slug with colon in slug",
			projects: []legacyWorkspaceProject{
				{ProjectSFID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:slug:extra"},
			},
			wantUIDs: []string{"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ws := legacyWorkspace{ID: "ws-1", Name: "Test", OrgSFID: "sfid-org", Projects: tc.projects}
			uids := resolveProjectUIDs(ctx, ws, mappings)

			if len(uids) != len(tc.wantUIDs) {
				t.Errorf("uid count = %d, want %d; got %v", len(uids), len(tc.wantUIDs), uids)
			}
			for i, want := range tc.wantUIDs {
				if i >= len(uids) {
					t.Errorf("missing uid at index %d; want %q", i, want)
					continue
				}
				if uids[i] != want {
					t.Errorf("uid[%d] = %q, want %q", i, uids[i], want)
				}
			}
		})
	}
}

// TestLoopPrevention confirms shouldSkipSync detects v2-authored records (T016).
// shouldSkipSync is defined in handlers.go; this test drives it indirectly
// through the same check reconcileWorkspace uses.
func TestLoopPrevention(t *testing.T) {
	origCfg := cfg
	cfg = &Config{Auth0ClientID: "my-client-id"}
	t.Cleanup(func() { cfg = origCfg })

	ctx := context.Background()

	// v2-authored record must be skipped.
	v2Data := map[string]any{"lastmodifiedbyid": "my-client-id@clients"}
	if !shouldSkipSync(ctx, v2Data) {
		t.Error("v2-authored record was not detected by shouldSkipSync")
	}

	// v1-authored record must pass through.
	v1Data := map[string]any{"lastmodifiedbyid": "some-v1-user"}
	if shouldSkipSync(ctx, v1Data) {
		t.Error("v1-authored record was incorrectly flagged by shouldSkipSync")
	}
}

func TestCreateAndCacheWorkspaceSkipsMissingOrg(t *testing.T) {
	setupMembersTestGlobals(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"org not found"}`))
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	skipped := 0
	errors := 0
	uid, projects, wasCreated, err := createAndCacheWorkspace(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Missing Org WS"},
		"missing-org-uid",
		false,
		&skipped,
		&errors,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uid != "" || projects != nil || wasCreated {
		t.Fatalf("got uid=%q projects=%v wasCreated=%v, want empty skip result", uid, projects, wasCreated)
	}
	if skipped != 1 {
		t.Fatalf("skipped = %d, want 1", skipped)
	}
	if errors != 0 {
		t.Fatalf("errors = %d, want 0", errors)
	}
}
