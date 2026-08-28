// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
)

// TestReconcileV1Skills covers the set-diff logic against a fake user-service.
func TestReconcileV1Skills(t *testing.T) {
	tests := []struct {
		name        string
		metadata    map[string]any
		current     []userSkillEntry
		wantAdded   []string // names posted, order-independent
		wantRemoved []string // ids deleted, order-independent
		wantNoCalls bool     // neither POST nor DELETE should happen
	}{
		{
			name:        "missing skills key is a no-op",
			metadata:    map[string]any{},
			current:     []userSkillEntry{{ID: "1", Name: "Go"}},
			wantNoCalls: true,
		},
		{
			name:        "unchanged set, different order, causes no churn",
			metadata:    map[string]any{"skills": "Python, Go"},
			current:     []userSkillEntry{{ID: "1", Name: "Go"}, {ID: "2", Name: "Python"}},
			wantNoCalls: true,
		},
		{
			name:        "case-insensitive match causes no churn",
			metadata:    map[string]any{"skills": "go"},
			current:     []userSkillEntry{{ID: "1", Name: "GO"}},
			wantNoCalls: true,
		},
		{
			name:        "adds and removes correctly",
			metadata:    map[string]any{"skills": "Go, Rust"},
			current:     []userSkillEntry{{ID: "1", Name: "Go"}, {ID: "2", Name: "Python"}},
			wantAdded:   []string{"Rust"},
			wantRemoved: []string{"2"},
		},
		{
			name:        "empty string clears all",
			metadata:    map[string]any{"skills": ""},
			current:     []userSkillEntry{{ID: "1", Name: "Go"}, {ID: "2", Name: "Python"}},
			wantRemoved: []string{"1", "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotAdded []string
			var gotRemoved []string

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_ = json.NewEncoder(w).Encode(tt.current)
				case http.MethodPost:
					var names []string
					_ = json.NewDecoder(r.Body).Decode(&names)
					gotAdded = append(gotAdded, names...)
					w.WriteHeader(http.StatusCreated)
				case http.MethodDelete:
					// URL path is .../skills/{id}
					gotRemoved = append(gotRemoved, lastPathSegment(r.URL.Path))
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Fatalf("unexpected method %s", r.Method)
				}
			}))
			defer srv.Close()

			setupFetchTestGlobals(t, srv.URL)

			err := reconcileV1Skills(context.Background(), "sfid1", tt.metadata)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantNoCalls {
				if len(gotAdded) != 0 || len(gotRemoved) != 0 {
					t.Errorf("expected no add/remove calls, got added=%v removed=%v", gotAdded, gotRemoved)
				}
				return
			}

			sort.Strings(gotAdded)
			wantAdded := append([]string{}, tt.wantAdded...)
			sort.Strings(wantAdded)
			if fmt.Sprint(gotAdded) != fmt.Sprint(wantAdded) {
				t.Errorf("added = %v, want %v", gotAdded, wantAdded)
			}

			sort.Strings(gotRemoved)
			wantRemoved := append([]string{}, tt.wantRemoved...)
			sort.Strings(wantRemoved)
			if fmt.Sprint(gotRemoved) != fmt.Sprint(wantRemoved) {
				t.Errorf("removed = %v, want %v", gotRemoved, wantRemoved)
			}
		})
	}
}

// TestReconcileV1Skills_GetError confirms a GET failure short-circuits before
// any add/remove calls are attempted.
func TestReconcileV1Skills_GetError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	setupFetchTestGlobals(t, srv.URL)

	err := reconcileV1Skills(context.Background(), "sfid1", map[string]any{"skills": "Go"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// lastPathSegment returns the final "/"-delimited segment of a URL path.
func lastPathSegment(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}
