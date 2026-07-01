// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
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

// TestDesiredProjectSlugs verifies that every non-deleted, non-empty
// project_id is sent verbatim as project_slug: no split on ":", no NATS or
// v1-mappings lookup, and no row is skipped for its shape (uuid:slug,
// raw SFID, or anything else).
func TestDesiredProjectSlugs(t *testing.T) {
	ctx := context.Background()

	ws := legacyWorkspace{
		ID:      "ws-1",
		Name:    "Test",
		OrgSFID: "sfid-org",
		Projects: []legacyWorkspaceProject{
			{ProjectSFID: "56fa1b4b-eca7-4824-a635-504a5e9a38cb:iree"},
			{ProjectSFID: "sfid-A"},
			{ProjectSFID: "sfid-B", Deleted: true},
			{ProjectSFID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:slug:extra"},
			{ProjectSFID: ""},
		},
	}

	got := desiredProjectSlugs(ctx, ws)
	want := []string{
		"56fa1b4b-eca7-4824-a635-504a5e9a38cb:iree",
		"sfid-A",
		"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:slug:extra",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("desiredProjectSlugs() = %v, want %v", got, want)
	}
}

func TestReconcileProjectsDoesNotCachePartialProjectFailures(t *testing.T) {
	setupMembersTestGlobals(t)

	responseBody := workspaceBulkResponse{
		Workspace: workspaceResponse{UID: "ws-001", Name: "Test WS"},
		Failed: []workspaceBulkAddItemError{
			{Slug: "vllm", Error: "unknown project \"vllm\": project not found"},
		},
	}
	bodyBytes, _ := json.Marshal(responseBody)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodyBytes)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	updated := 0
	projectsAdded := 0
	projectsRemoved := 0
	errors := 0

	reconcileProjects(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Test WS"},
		"org-001",
		"ws-001",
		[]string{"vllm"},
		nil,
		false,
		false,
		&updated,
		&projectsAdded,
		&projectsRemoved,
		&errors,
	)

	if projectsAdded != 0 {
		t.Fatalf("projectsAdded = %d, want 0", projectsAdded)
	}
	if updated != 0 {
		t.Fatalf("updated = %d, want 0; failed associations must not cache project-set success", updated)
	}
	if errors != 1 {
		t.Fatalf("errors = %d, want 1", errors)
	}
}

// TestReconcileProjectsDoesNotCacheOnPartialSuccess verifies that when one
// project in a bulk-add succeeds and another fails, the successful add is
// still counted (updated, projectsAdded) but the project-set cache is not
// persisted — a re-run must recompute the delta rather than trust a partial
// apply. mappingsKV is left nil by setupMembersTestGlobals, so a stray
// putWorkspaceCacheEntry call here would panic.
func TestReconcileProjectsDoesNotCacheOnPartialSuccess(t *testing.T) {
	setupMembersTestGlobals(t)

	responseBody := workspaceBulkResponse{
		Workspace: workspaceResponse{
			UID:  "ws-001",
			Name: "Test WS",
			Projects: []workspaceProject{
				{UID: "gen-uid-vllm", Slug: "vllm"},
			},
		},
		Succeeded: []string{"vllm"},
		Failed: []workspaceBulkAddItemError{
			{Slug: "bad-project", Error: "unknown project \"bad-project\": project not found"},
		},
	}
	bodyBytes, _ := json.Marshal(responseBody)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodyBytes)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	updated := 0
	projectsAdded := 0
	projectsRemoved := 0
	errors := 0

	reconcileProjects(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Test WS"},
		"org-001",
		"ws-001",
		[]string{"vllm", "bad-project"},
		nil,
		false,
		false,
		&updated,
		&projectsAdded,
		&projectsRemoved,
		&errors,
	)

	if projectsAdded != 1 {
		t.Fatalf("projectsAdded = %d, want 1", projectsAdded)
	}
	if updated != 1 {
		t.Fatalf("updated = %d, want 1; a partial success must still be reported as an update", updated)
	}
	if errors != 1 {
		t.Fatalf("errors = %d, want 1", errors)
	}
}

// TestReconcileProjectsDedupesDuplicateDesiredSlugs verifies that a
// workspace with two non-deleted associations sharing the same project_id
// sends each slug to member-service only once, instead of a duplicate
// bulk-add entry per repeated desiredSlugs value. The response reports the
// slug as failed (rather than succeeded) so the assertion can be made
// without exercising the project-set cache write, which needs a mappingsKV
// that setupMembersTestGlobals does not provide (see
// TestReconcileProjectsDoesNotCacheOnPartialSuccess).
func TestReconcileProjectsDedupesDuplicateDesiredSlugs(t *testing.T) {
	setupMembersTestGlobals(t)

	var gotSlugs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body workspaceBulkAddBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		for _, item := range body.Projects {
			gotSlugs = append(gotSlugs, item.Slug)
		}
		responseBody := workspaceBulkResponse{
			Workspace: workspaceResponse{UID: "ws-001"},
			Failed: []workspaceBulkAddItemError{
				{Slug: "vllm", Error: "unknown project \"vllm\": project not found"},
			},
		}
		bodyBytes, _ := json.Marshal(responseBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodyBytes)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	updated := 0
	projectsAdded := 0
	projectsRemoved := 0
	errors := 0

	reconcileProjects(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Test WS"},
		"org-001",
		"ws-001",
		[]string{"vllm", "vllm"},
		nil,
		false,
		false,
		&updated,
		&projectsAdded,
		&projectsRemoved,
		&errors,
	)

	want := []string{"vllm"}
	if !reflect.DeepEqual(gotSlugs, want) {
		t.Fatalf("bulk-add request slugs = %v, want %v", gotSlugs, want)
	}
	if errors != 1 {
		t.Fatalf("errors = %d, want 1", errors)
	}
}

// TestReconcileProjectsDryRunMakesNoCallsAndNoCacheWrite verifies dry-run
// computes planned add/remove counts without calling member-service or
// writing to the workspace project cache.
func TestReconcileProjectsDryRunMakesNoCallsAndNoCacheWrite(t *testing.T) {
	setupMembersTestGlobals(t)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected member-service call in dry-run: %s %s", r.Method, r.URL.Path)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	updated := 0
	projectsAdded := 0
	projectsRemoved := 0
	errors := 0

	reconcileProjects(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Test WS"},
		"org-001",
		"ws-001",
		[]string{"vllm"},
		[]workspaceCacheProject{{Slug: "stale-project", UID: "uid-stale"}},
		true,
		false,
		&updated,
		&projectsAdded,
		&projectsRemoved,
		&errors,
	)

	if projectsAdded != 1 {
		t.Fatalf("projectsAdded = %d, want 1", projectsAdded)
	}
	if projectsRemoved != 1 {
		t.Fatalf("projectsRemoved = %d, want 1", projectsRemoved)
	}
	if updated != 1 {
		t.Fatalf("updated = %d, want 1", updated)
	}
	if errors != 0 {
		t.Fatalf("errors = %d, want 0", errors)
	}
}

// TestBulkAddProjectsMatchesGeneratedUIDFromWorkspaceProjects verifies a
// successful bulk-add returns the project_slug -> generated project_uid
// pairs matched from the response's nested workspace.projects[], not from
// the succeeded list (which carries slugs only).
func TestBulkAddProjectsMatchesGeneratedUIDFromWorkspaceProjects(t *testing.T) {
	setupMembersTestGlobals(t)

	responseBody := workspaceBulkResponse{
		Workspace: workspaceResponse{
			UID:  "ws-001",
			Name: "Test WS",
			Projects: []workspaceProject{
				{UID: "gen-uid-vllm", Slug: "vllm"},
			},
		},
		Succeeded: []string{"vllm"},
	}
	bodyBytes, _ := json.Marshal(responseBody)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodyBytes)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	projectsAdded := 0
	errors := 0
	added := bulkAddProjects(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Test WS"},
		"org-001", "ws-001",
		[]string{"vllm"},
		false,
		&projectsAdded, &errors,
	)

	if errors != 0 {
		t.Fatalf("errors = %d, want 0", errors)
	}
	if projectsAdded != 1 {
		t.Fatalf("projectsAdded = %d, want 1", projectsAdded)
	}
	want := []workspaceCacheProject{{Slug: "vllm", UID: "gen-uid-vllm"}}
	if !reflect.DeepEqual(added, want) {
		t.Fatalf("added = %v, want %v", added, want)
	}
}

// TestBulkAddProjectsCountsErrorOnMissingUIDMatch verifies that a slug
// reported as succeeded but absent from workspace.projects[] is counted as
// an error and not silently dropped.
func TestBulkAddProjectsCountsErrorOnMissingUIDMatch(t *testing.T) {
	setupMembersTestGlobals(t)

	responseBody := workspaceBulkResponse{
		Workspace: workspaceResponse{
			UID:  "ws-001",
			Name: "Test WS",
		},
		Succeeded: []string{"vllm"},
	}
	bodyBytes, _ := json.Marshal(responseBody)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodyBytes)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	projectsAdded := 0
	errors := 0
	added := bulkAddProjects(
		context.Background(),
		legacyWorkspace{ID: "ws-1", Name: "Test WS"},
		"org-001", "ws-001",
		[]string{"vllm"},
		false,
		&projectsAdded, &errors,
	)

	if errors != 1 {
		t.Fatalf("errors = %d, want 1", errors)
	}
	if projectsAdded != 0 {
		t.Fatalf("projectsAdded = %d, want 0", projectsAdded)
	}
	if len(added) != 0 {
		t.Fatalf("added = %v, want empty", added)
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
