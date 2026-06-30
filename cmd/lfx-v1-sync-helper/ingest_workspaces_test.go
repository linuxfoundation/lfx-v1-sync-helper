// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
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

func TestProjectSlugCacheKey(t *testing.T) {
	got := projectSlugCacheKey("vllm.with.unsafe:chars")
	want := "project.slug." + fnv32hex("vllm.with.unsafe:chars")
	if got != want {
		t.Fatalf("projectSlugCacheKey() = %q, want %q", got, want)
	}
}

// TestResolveProjectUIDs verifies project UID resolution: SFID lookup, uuid:slug
// slug resolution, deleted-project exclusion, and unmappable-project skip behavior.
func TestResolveProjectUIDs(t *testing.T) {
	ctx := context.Background()
	mappings := map[string]string{
		"sfid-A": "uid-A",
		"sfid-B": "uid-B",
	}

	origLookup := lookupProjectUIDBySlugCachedFn
	t.Cleanup(func() { lookupProjectUIDBySlugCachedFn = origLookup })

	var calls []string
	lookupProjectUIDBySlugCachedFn = func(_ context.Context, slug string, dryRun bool, memo map[string]string) (string, error) {
		if dryRun {
			t.Fatalf("dryRun = true, want false")
		}
		calls = append(calls, slug)
		switch slug {
		case "iree":
			memo[slug] = "uid-iree"
			return "uid-iree", nil
		case "ptproject":
			memo[slug] = "uid-ptproject"
			return "uid-ptproject", nil
		default:
			return "", nil
		}
	}

	ws := legacyWorkspace{
		ID:      "ws-1",
		Name:    "Test",
		OrgSFID: "sfid-org",
		Projects: []legacyWorkspaceProject{
			{ProjectSFID: "56fa1b4b-eca7-4824-a635-504a5e9a38cb:iree"},
			{ProjectSFID: "sfid-A"},
			{ProjectSFID: "sfid-MISSING"},
			{ProjectSFID: "sfid-B", Deleted: true},
			{ProjectSFID: "8234d49d-b9ca-4ba0-a287-0c7585c96590:ptproject"},
			{ProjectSFID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:slug:extra"},
		},
	}

	uids, slugByUID, complete := resolveProjectUIDs(ctx, ws, mappings, false, map[string]string{})
	if !complete {
		t.Fatal("complete = false, want true")
	}

	wantUIDs := []string{"uid-iree", "uid-A", "uid-ptproject"}
	if !reflect.DeepEqual(uids, wantUIDs) {
		t.Fatalf("uids = %v, want %v", uids, wantUIDs)
	}
	wantSlugByUID := map[string]string{
		"uid-iree":      "iree",
		"uid-ptproject": "ptproject",
	}
	if !reflect.DeepEqual(slugByUID, wantSlugByUID) {
		t.Fatalf("slugByUID = %v, want %v", slugByUID, wantSlugByUID)
	}
	if !reflect.DeepEqual(calls, []string{"iree", "ptproject", "slug:extra"}) {
		t.Fatalf("slug lookup calls = %v, want [iree ptproject slug:extra]", calls)
	}
}

func TestResolveProjectUIDsLookupErrorMarksIncomplete(t *testing.T) {
	ctx := context.Background()

	origLookup := lookupProjectUIDBySlugCachedFn
	t.Cleanup(func() { lookupProjectUIDBySlugCachedFn = origLookup })

	lookupProjectUIDBySlugCachedFn = func(_ context.Context, slug string, _ bool, _ map[string]string) (string, error) {
		if slug != "vllm" {
			t.Fatalf("slug = %q, want vllm", slug)
		}
		return "", errors.New("nats timeout")
	}

	ws := legacyWorkspace{
		ID: "ws-1",
		Projects: []legacyWorkspaceProject{
			{ProjectSFID: "56fa1b4b-eca7-4824-a635-504a5e9a38cb:vllm"},
		},
	}

	uids, slugByUID, complete := resolveProjectUIDs(ctx, ws, nil, false, map[string]string{})
	if complete {
		t.Fatal("complete = true, want false")
	}
	if len(uids) != 0 {
		t.Fatalf("uids = %v, want empty", uids)
	}
	if len(slugByUID) != 0 {
		t.Fatalf("slugByUID = %v, want empty", slugByUID)
	}
}

func TestLookupProjectUIDBySlugCached(t *testing.T) {
	ctx := context.Background()

	origGet := mappingKVGetValueFn
	origPut := mappingKVPutValueFn
	origLookup := getProjectUIDBySlugFn
	t.Cleanup(func() {
		mappingKVGetValueFn = origGet
		mappingKVPutValueFn = origPut
		getProjectUIDBySlugFn = origLookup
	})

	stored := map[string][]byte{}
	rpcCalls := 0
	mappingKVGetValueFn = func(_ context.Context, key string) ([]byte, error) {
		if val, ok := stored[key]; ok {
			return val, nil
		}
		return nil, jetstream.ErrKeyNotFound
	}
	mappingKVPutValueFn = func(_ context.Context, key string, value []byte) error {
		stored[key] = append([]byte(nil), value...)
		return nil
	}
	getProjectUIDBySlugFn = func(_ context.Context, slug string) (string, error) {
		rpcCalls++
		if slug != "vllm" {
			t.Fatalf("slug = %q, want vllm", slug)
		}
		return "uid-vllm", nil
	}

	memo := map[string]string{}
	uid, err := lookupProjectUIDBySlugCached(context.Background(), "vllm", false, memo)
	if err != nil {
		t.Fatalf("lookupProjectUIDBySlugCached() error = %v", err)
	}
	if uid != "uid-vllm" {
		t.Fatalf("uid = %q, want uid-vllm", uid)
	}
	if rpcCalls != 1 {
		t.Fatalf("rpcCalls = %d, want 1", rpcCalls)
	}
	key := projectSlugCacheKey("vllm")
	if got := string(stored[key]); got != "uid-vllm" {
		t.Fatalf("stored[%q] = %q, want uid-vllm", key, got)
	}

	uid, err = lookupProjectUIDBySlugCached(ctx, "vllm", false, memo)
	if err != nil {
		t.Fatalf("second lookup error = %v", err)
	}
	if uid != "uid-vllm" || rpcCalls != 1 {
		t.Fatalf("second lookup uid=%q rpcCalls=%d, want uid-vllm and 1 call", uid, rpcCalls)
	}

	uid, err = lookupProjectUIDBySlugCached(ctx, "vllm", false, map[string]string{})
	if err != nil {
		t.Fatalf("persistent cache lookup error = %v", err)
	}
	if uid != "uid-vllm" || rpcCalls != 1 {
		t.Fatalf("persistent cache lookup uid=%q rpcCalls=%d, want uid-vllm and 1 call", uid, rpcCalls)
	}
}

func TestLookupProjectUIDBySlugCachedDryRunDoesNotWrite(t *testing.T) {
	origGet := mappingKVGetValueFn
	origPut := mappingKVPutValueFn
	origLookup := getProjectUIDBySlugFn
	t.Cleanup(func() {
		mappingKVGetValueFn = origGet
		mappingKVPutValueFn = origPut
		getProjectUIDBySlugFn = origLookup
	})

	putCalls := 0
	mappingKVGetValueFn = func(_ context.Context, _ string) ([]byte, error) {
		return nil, jetstream.ErrKeyNotFound
	}
	mappingKVPutValueFn = func(_ context.Context, _ string, _ []byte) error {
		putCalls++
		return nil
	}
	getProjectUIDBySlugFn = func(_ context.Context, _ string) (string, error) {
		return "uid-vllm", nil
	}

	uid, err := lookupProjectUIDBySlugCached(context.Background(), "vllm", true, map[string]string{})
	if err != nil {
		t.Fatalf("lookupProjectUIDBySlugCached() error = %v", err)
	}
	if uid != "uid-vllm" {
		t.Fatalf("uid = %q, want uid-vllm", uid)
	}
	if putCalls != 0 {
		t.Fatalf("putCalls = %d, want 0 for dry-run", putCalls)
	}
}

func TestReconcileProjectsDoesNotCachePartialProjectFailures(t *testing.T) {
	setupMembersTestGlobals(t)

	responseBody := workspaceBulkResponse{
		Workspace: workspaceResponse{UID: "ws-001", Name: "Test WS"},
		Failed: []workspaceBulkAddItemError{
			{ProjectID: "uid-vllm", Error: "unknown project \"uid-vllm\": project not found"},
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
		[]string{"uid-vllm"},
		nil,
		map[string]string{"uid-vllm": "vllm"},
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
