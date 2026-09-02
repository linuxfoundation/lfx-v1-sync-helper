// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/auth0/go-auth0"
	"github.com/auth0/go-auth0/management"
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
		{
			name:        "non-string skills value is a no-op",
			metadata:    map[string]any{"skills": 123},
			current:     []userSkillEntry{{ID: "1", Name: "Go"}},
			wantNoCalls: true,
		},
		{
			// Σ (capital sigma) and ς (final sigma) are case-equivalent under
			// Unicode case folding but lowercase differently under
			// strings.ToLower, so this only causes no churn if the diff uses
			// cases.Fold() consistently on both sides.
			name:        "unicode case-fold match causes no churn",
			metadata:    map[string]any{"skills": "Σ"},
			current:     []userSkillEntry{{ID: "1", Name: "ς"}},
			wantNoCalls: true,
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

// TestReconcileV1Skills_SuppressesRemovalWhenDesiredAtCap confirms that when
// the v2 skills field has auth0SkillsMaxCount entries (the same cap
// normalizeSkillsForAuth0 and auth-service's own sanitizer apply), v1 skills
// absent from that set are NOT deleted — they may simply be past the
// truncation boundary rather than actually removed in v2 — while new skills
// still get added.
func TestReconcileV1Skills_SuppressesRemovalWhenDesiredAtCap(t *testing.T) {
	names := make([]string, 0, auth0SkillsMaxCount)
	for i := 0; i < auth0SkillsMaxCount; i++ {
		names = append(names, fmt.Sprintf("skill-%d", i))
	}
	metadata := map[string]any{"skills": strings.Join(names, ", ")}

	// v1 already has one skill that isn't in the capped set ("legacy-skill")
	// plus one that is ("skill-0", to prove adds are still computed).
	current := []userSkillEntry{
		{ID: "legacy", Name: "legacy-skill"},
	}

	var gotAdded []string
	var gotRemoved []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(current)
		case http.MethodPost:
			var posted []string
			_ = json.NewDecoder(r.Body).Decode(&posted)
			gotAdded = append(gotAdded, posted...)
			w.WriteHeader(http.StatusCreated)
		case http.MethodDelete:
			gotRemoved = append(gotRemoved, lastPathSegment(r.URL.Path))
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()

	setupFetchTestGlobals(t, srv.URL)

	if err := reconcileV1Skills(context.Background(), "sfid1", metadata); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(gotRemoved) != 0 {
		t.Errorf("expected no removals when desired set is at the cap, got %v", gotRemoved)
	}
	if len(gotAdded) != auth0SkillsMaxCount {
		t.Errorf("expected %d additions, got %d: %v", auth0SkillsMaxCount, len(gotAdded), gotAdded)
	}
}

// TestReconcileV1Skills_SuppressesRemovalWhenDesiredAtLengthCap confirms that
// removals are also suppressed when the joined skills string is at (or near)
// auth0SkillsMaxLength runes, even though the item count is far below
// auth0SkillsMaxCount. A handful of long skill names can trip the length cap
// well before the item-count cap, so the truncated tail must not be read as
// real v1-side removals.
func TestReconcileV1Skills_SuppressesRemovalWhenDesiredAtLengthCap(t *testing.T) {
	// Two long names joined by ", " land right at the auth0SkillsMaxLength
	// boundary, well under auth0SkillsMaxCount items.
	first := strings.Repeat("a", auth0SkillsMaxLength/2)
	second := strings.Repeat("b", auth0SkillsMaxLength/2-2) // account for ", "
	metadata := map[string]any{"skills": first + ", " + second}

	// v1 has a skill not present in the (possibly truncated) desired set.
	current := []userSkillEntry{
		{ID: "legacy", Name: "legacy-skill"},
	}

	var gotAdded []string
	var gotRemoved []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(current)
		case http.MethodPost:
			var posted []string
			_ = json.NewDecoder(r.Body).Decode(&posted)
			gotAdded = append(gotAdded, posted...)
			w.WriteHeader(http.StatusCreated)
		case http.MethodDelete:
			gotRemoved = append(gotRemoved, lastPathSegment(r.URL.Path))
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()

	setupFetchTestGlobals(t, srv.URL)

	if err := reconcileV1Skills(context.Background(), "sfid1", metadata); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(gotRemoved) != 0 {
		t.Errorf("expected no removals when desired string is at the length cap, got %v", gotRemoved)
	}
	if len(gotAdded) != 2 {
		t.Errorf("expected 2 additions, got %d: %v", len(gotAdded), gotAdded)
	}
}

// TestResolveSkillsMetadata covers the re-read-vs-fallback behavior used by
// handleUserProfileUpdated to pick the metadata reconcileV1Skills diffs
// against.
func TestResolveSkillsMetadata(t *testing.T) {
	origAuth0Users := auth0Users
	t.Cleanup(func() { auth0Users = origAuth0Users })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	event := userProfileUpdatedEvent{
		UserID:   "auth0|alice",
		Metadata: map[string]any{"skills": "stale-event-value"},
	}

	t.Run("re-read succeeds, prefers live Auth0 metadata over the event snapshot", func(t *testing.T) {
		fake := &fakeAuth0Users{
			users: map[string]*management.User{
				"auth0|alice": {
					ID:           auth0.String("auth0|alice"),
					UserMetadata: &map[string]any{"skills": "live-value"},
				},
			},
		}
		auth0Users = fake

		got := resolveSkillsMetadata(context.Background(), log, "sfid1", event)
		if got["skills"] != "live-value" {
			t.Errorf("skills = %v, want live-value", got["skills"])
		}

		// This handler runs off a core NATS callback with no deadline of its
		// own (context.Background() at the call site), so resolveSkillsMetadata
		// must bind the Auth0 read to auth0CallTimeout itself, or a stalled
		// Auth0 request could block that callback indefinitely.
		if fake.readCtx == nil {
			t.Fatal("fetchAuth0User was not called")
		}
		if _, ok := fake.readCtx.Deadline(); !ok {
			t.Error("expected the context passed to fetchAuth0User to carry a deadline")
		}
	})

	t.Run("re-read fails, falls back to the event snapshot", func(t *testing.T) {
		auth0Users = &fakeAuth0Users{readErr: &mgmtError{status: http.StatusInternalServerError, message: "boom"}}

		got := resolveSkillsMetadata(context.Background(), log, "sfid1", event)
		if got["skills"] != "stale-event-value" {
			t.Errorf("skills = %v, want stale-event-value (fallback)", got["skills"])
		}
	})
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
