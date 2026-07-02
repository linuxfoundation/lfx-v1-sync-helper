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
	"testing"
)

// TestCreateWorkspace covers the 201 and 409 response paths.
func TestCreateWorkspace(t *testing.T) {
	setupMembersTestGlobals(t)

	tests := []struct {
		name         string
		status       int
		responseBody string
		wantUID      string
		wantConflict bool
		wantErr      bool
	}{
		{
			name:         "201 returns workspace uid",
			status:       http.StatusCreated,
			responseBody: `{"uid":"ws-001","name":"Test WS","projects":[]}`,
			wantUID:      "ws-001",
			wantConflict: false,
		},
		{
			name:         "409 returns conflict signal",
			status:       http.StatusConflict,
			responseBody: `{"message":"Workspace name already exists"}`,
			wantUID:      "",
			wantConflict: true,
		},
		{
			name:    "500 returns error",
			status:  http.StatusInternalServerError,
			wantErr: true,
		},
		{
			name:    "404 org not found returns sentinel",
			status:  http.StatusNotFound,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("expected POST, got %s", r.Method)
				}
				w.WriteHeader(tc.status)
				if tc.responseBody != "" {
					_, _ = w.Write([]byte(tc.responseBody))
				}
			}))
			defer srv.Close()

			u, _ := url.Parse(srv.URL)
			cfg.MemberServiceURL = u

			ws, conflict, err := createWorkspace(context.Background(), "org-001", "Test WS")

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.status == http.StatusNotFound && !errors.Is(err, errWorkspaceOrgNotFound) {
					t.Fatalf("err = %v, want errWorkspaceOrgNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if conflict != tc.wantConflict {
				t.Errorf("conflict = %v, want %v", conflict, tc.wantConflict)
			}
			if tc.wantUID != "" && (ws == nil || ws.UID != tc.wantUID) {
				t.Errorf("uid = %q, want %q", func() string {
					if ws == nil {
						return ""
					}
					return ws.UID
				}(), tc.wantUID)
			}
		})
	}
}

// TestDeleteWorkspace covers the 204, 404 (already gone), and error paths.
func TestDeleteWorkspace(t *testing.T) {
	setupMembersTestGlobals(t)

	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"204 success", http.StatusNoContent, false},
		{"404 already gone is success", http.StatusNotFound, false},
		{"500 returns error", http.StatusInternalServerError, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("expected DELETE, got %s", r.Method)
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			u, _ := url.Parse(srv.URL)
			cfg.MemberServiceURL = u

			err := deleteWorkspace(context.Background(), "org-001", "ws-001")
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestRemoveWorkspaceProject covers the 200, 204, 404 (idempotent), and error paths.
func TestRemoveWorkspaceProject(t *testing.T) {
	setupMembersTestGlobals(t)

	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"200 success", http.StatusOK, false},
		{"204 success", http.StatusNoContent, false},
		{"404 already removed is success", http.StatusNotFound, false},
		{"500 returns error", http.StatusInternalServerError, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("expected DELETE, got %s", r.Method)
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			u, _ := url.Parse(srv.URL)
			cfg.MemberServiceURL = u

			err := removeWorkspaceProject(context.Background(), "org-001", "ws-001", "proj-001")
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestBulkAddWorkspaceProjects covers the succeeded/failed response parsing.
func TestBulkAddWorkspaceProjects(t *testing.T) {
	setupMembersTestGlobals(t)

	responseBody := workspaceBulkResponse{
		Workspace: workspaceResponse{UID: "ws-001", Name: "Test WS"},
		Succeeded: []string{"proj-a", "proj-b"},
		Failed: []workspaceBulkAddItemError{
			{Slug: "proj-c", Error: "project not found"},
		},
	}
	bodyBytes, _ := json.Marshal(responseBody)

	var gotBody workspaceBulkAddBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodyBytes)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cfg.MemberServiceURL = u

	resp, err := bulkAddWorkspaceProjects(context.Background(), "org-001", "ws-001", []string{"proj-a", "proj-b", "proj-c"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Succeeded) != 2 {
		t.Errorf("succeeded count = %d, want 2", len(resp.Succeeded))
	}
	if len(resp.Failed) != 1 {
		t.Errorf("failed count = %d, want 1", len(resp.Failed))
	}
	if resp.Failed[0].Slug != "proj-c" {
		t.Errorf("failed[0].project_slug = %q, want %q", resp.Failed[0].Slug, "proj-c")
	}

	wantProjects := []workspaceBulkAddItem{{Slug: "proj-a"}, {Slug: "proj-b"}, {Slug: "proj-c"}}
	if len(gotBody.Projects) != len(wantProjects) {
		t.Fatalf("request body projects = %+v, want %+v", gotBody.Projects, wantProjects)
	}
	for i, p := range gotBody.Projects {
		if p != wantProjects[i] {
			t.Errorf("request body projects[%d] = %+v, want %+v", i, p, wantProjects[i])
		}
		if p.Name != "" {
			t.Errorf("request body projects[%d].project_name = %q, want empty", i, p.Name)
		}
	}
}
