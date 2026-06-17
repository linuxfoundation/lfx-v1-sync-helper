// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// ---- helpers ----------------------------------------------------------------

// makeInvite builds a minimal acsOrgInvite with the given email, role, and
// updated_at epoch.
func makeInvite(email, roleName string, updatedAt int64) acsOrgInvite {
	return acsOrgInvite{
		InviteID:  email + "-id",
		Email:     email,
		FirstName: "First",
		LastName:  "Last",
		ScopeID:   "sfid1",
		RoleName:  roleName,
		Status:    "pending",
		UpdatedAt: fmt.Sprintf("%d", updatedAt),
	}
}

// ---- mergeOrgInvitesWithACS tests -------------------------------------------

// TestMergeOrgInvitesWithACS covers the email-keyed merge used for pending invites.
func TestMergeOrgInvitesWithACS(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Unix()

	t.Run("empty existing + one invite = one entry added", func(t *testing.T) {
		inv := makeInvite("alice@example.com", acsOrgRoleNameAdmin, now)
		merged, added := mergeOrgInvitesWithACS(ctx, nil, nil, []acsOrgInvite{inv}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged, got %d", len(merged))
		}
		entry := merged[0]
		if entry.Username != nil {
			t.Errorf("pending invite must have nil username, got %q", *entry.Username)
		}
		if entry.Email != "alice@example.com" {
			t.Errorf("email = %q, want %q", entry.Email, "alice@example.com")
		}
		if entry.InvitedAs != "writer" {
			t.Errorf("invited_as = %q, want %q", entry.InvitedAs, "writer")
		}
	})

	t.Run("name populated from first_name + last_name", func(t *testing.T) {
		inv := acsOrgInvite{
			Email: "bob@example.com", FirstName: "Bob", LastName: "Smith",
			RoleName: acsOrgRoleNameViewer, UpdatedAt: fmt.Sprintf("%d", now),
		}
		merged, added := mergeOrgInvitesWithACS(ctx, nil, nil, []acsOrgInvite{inv}, "auditors", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		entry := merged[0]
		if entry.Name == nil || *entry.Name != "Bob Smith" {
			t.Errorf("name = %v, want %q", entry.Name, "Bob Smith")
		}
		if entry.InvitedAs != "auditor" {
			t.Errorf("invited_as = %q, want %q", entry.InvitedAs, "auditor")
		}
	})

	t.Run("invite skipped if email already in target relation (case-insensitive)", func(t *testing.T) {
		username := "alice"
		existing := []*b2bOrgUser{{Email: "Alice@Example.COM", Username: &username, InvitedAs: "writer"}}
		inv := makeInvite("alice@example.com", acsOrgRoleNameAdmin, now)
		_, added := mergeOrgInvitesWithACS(ctx, existing, nil, []acsOrgInvite{inv}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added (email already present), got %d", added)
		}
	})

	t.Run("invite skipped if email already in other relation (cross-relation dedupe)", func(t *testing.T) {
		// alice is already an auditor; her writer invite must be skipped.
		username := "alice"
		auditorsSlice := []*b2bOrgUser{{Email: "alice@example.com", Username: &username, InvitedAs: "auditor"}}
		inv := makeInvite("alice@example.com", acsOrgRoleNameAdmin, now)
		_, added := mergeOrgInvitesWithACS(ctx, nil, auditorsSlice, []acsOrgInvite{inv}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added (email in other relation), got %d", added)
		}
	})

	t.Run("invite with blank email is skipped", func(t *testing.T) {
		inv := acsOrgInvite{RoleName: acsOrgRoleNameAdmin, UpdatedAt: fmt.Sprintf("%d", now)}
		_, added := mergeOrgInvitesWithACS(ctx, nil, nil, []acsOrgInvite{inv}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added for blank email, got %d", added)
		}
	})

	t.Run("duplicate emails within invite list are deduplicated", func(t *testing.T) {
		inv1 := makeInvite("dup@example.com", acsOrgRoleNameAdmin, now)
		inv2 := makeInvite("dup@example.com", acsOrgRoleNameAdmin, now)
		_, added := mergeOrgInvitesWithACS(ctx, nil, nil, []acsOrgInvite{inv1, inv2}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added (duplicate deduplicated), got %d", added)
		}
	})

	t.Run("existing v2 entries are preserved", func(t *testing.T) {
		existing := []*b2bOrgUser{makeOrgUser("carol", "writer")}
		inv := makeInvite("dave@example.com", acsOrgRoleNameAdmin, now)
		merged, added := mergeOrgInvitesWithACS(ctx, existing, nil, []acsOrgInvite{inv}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 2 {
			t.Fatalf("want 2 total (carol preserved + dave added), got %d", len(merged))
		}
	})
}

// ---- fetchACSOrgInvitesByRole tests -----------------------------------------

// TestFetchACSOrgInvitesByRole_TimeFilter confirms that only invites within the
// 1-year window are included and that role filtering works.
func TestFetchACSOrgInvitesByRole_TimeFilter(t *testing.T) {
	cutoff := time.Now().AddDate(-1, 0, 0).Unix()
	justInside := cutoff + 10     // 10 seconds inside window — must be included
	justOutside := cutoff - 10    // 10 seconds outside window — must be skipped
	unparseable := "not-a-number" // must log + skip

	tests := []struct {
		name         string
		updatedAt    string
		roleName     string
		wantWriters  int
		wantAuditors int
	}{
		{"admin inside window", fmt.Sprintf("%d", justInside), acsOrgRoleNameAdmin, 1, 0},
		{"viewer inside window", fmt.Sprintf("%d", justInside), acsOrgRoleNameViewer, 0, 1},
		{"admin outside window", fmt.Sprintf("%d", justOutside), acsOrgRoleNameAdmin, 0, 0},
		{"unparseable updated_at", unparseable, acsOrgRoleNameAdmin, 0, 0},
		{"unknown role skipped", fmt.Sprintf("%d", justInside), "contact", 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{
				"data":[{
					"invite_id":"inv1","email":"test@example.com",
					"first_name":"T","last_name":"T",
					"scope_id":"sfid1","role_name":%q,
					"status":"pending","updated_at":%q,"expired":false
				}],
				"metadata":{"TotalSize":1,"Offset":0,"PageSize":1}
			}`, tc.roleName, tc.updatedAt)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				if got := q.Get("scopeid"); got != "sfid1" {
					t.Errorf("expected scopeid=sfid1, got %q", got)
				}
				if got := q.Get("rolenames"); got != acsOrgRoleNameAdmin+","+acsOrgRoleNameViewer {
					t.Errorf("expected rolenames=%q, got %q", acsOrgRoleNameAdmin+","+acsOrgRoleNameViewer, got)
				}
				if got := q.Get("status"); got != "pending" {
					t.Errorf("expected status=pending, got %q", got)
				}
				if got := q.Get("showuniqueusers"); got != "true" {
					t.Errorf("expected showuniqueusers=true, got %q", got)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, body)
			}))
			defer srv.Close()

			setupFetchTestGlobals(t, srv.URL)

			result, err := fetchACSOrgInvitesByRole(context.Background(), "sfid1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := len(result.Writers); got != tc.wantWriters {
				t.Errorf("Writers count = %d, want %d", got, tc.wantWriters)
			}
			if got := len(result.Auditors); got != tc.wantAuditors {
				t.Errorf("Auditors count = %d, want %d", got, tc.wantAuditors)
			}
		})
	}
}

// TestFetchACSOrgInvitesByRole_Pagination confirms multi-page fetching stops
// when rawFetched >= TotalSize.
func TestFetchACSOrgInvitesByRole_Pagination(t *testing.T) {
	inside := time.Now().AddDate(-1, 0, 0).Unix() + 100
	calls := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if got := q.Get("scopeid"); got != "sfid1" {
			t.Errorf("expected scopeid=sfid1, got %q", got)
		}
		if got := q.Get("rolenames"); got != acsOrgRoleNameAdmin+","+acsOrgRoleNameViewer {
			t.Errorf("expected rolenames=%q, got %q", acsOrgRoleNameAdmin+","+acsOrgRoleNameViewer, got)
		}
		if got := q.Get("status"); got != "pending" {
			t.Errorf("expected status=pending, got %q", got)
		}
		if got := q.Get("showuniqueusers"); got != "true" {
			t.Errorf("expected showuniqueusers=true, got %q", got)
		}
		offset := q.Get("offset")
		var body string
		switch offset {
		case "0":
			// Page 1: 1 result, TotalSize=2 → must fetch page 2.
			body = fmt.Sprintf(`{
				"data":[{"invite_id":"inv1","email":"p1@example.com","role_name":%q,"status":"pending","updated_at":"%d"}],
				"metadata":{"TotalSize":2,"Offset":0,"PageSize":1}
			}`, acsOrgRoleNameAdmin, inside)
		default:
			// Page 2: 1 result, rawFetched(2) >= TotalSize(2) → stop.
			body = fmt.Sprintf(`{
				"data":[{"invite_id":"inv2","email":"p2@example.com","role_name":%q,"status":"pending","updated_at":"%d"}],
				"metadata":{"TotalSize":2,"Offset":1,"PageSize":1}
			}`, acsOrgRoleNameAdmin, inside)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	setupFetchTestGlobals(t, srv.URL)

	result, err := fetchACSOrgInvitesByRole(context.Background(), "sfid1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 API calls, got %d", calls)
	}
	if len(result.Writers) != 2 {
		t.Errorf("expected 2 writers, got %d", len(result.Writers))
	}
}

// TestFetchACSOrgInvitesByRole_404 confirms a 404 is treated as empty (no error).
func TestFetchACSOrgInvitesByRole_404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	setupFetchTestGlobals(t, srv.URL)

	result, err := fetchACSOrgInvitesByRole(context.Background(), "sfid1")
	if err != nil {
		t.Fatalf("404 should not return error, got: %v", err)
	}
	if len(result.Writers)+len(result.Auditors) != 0 {
		t.Errorf("expected empty result on 404, got writers=%d auditors=%d",
			len(result.Writers), len(result.Auditors))
	}
}

// setupFetchTestGlobals wires cfg.LFXAPIGateway and v1HTTPClient to point at
// the given test server URL for the duration of the test.
func setupFetchTestGlobals(t *testing.T, serverURL string) {
	t.Helper()
	origCfg := cfg
	origV1 := v1HTTPClient

	u, err := url.Parse(serverURL + "/")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	cfg = &Config{}
	cfg.LFXAPIGateway = u
	v1HTTPClient = http.DefaultClient

	t.Cleanup(func() {
		cfg = origCfg
		v1HTTPClient = origV1
	})
}
