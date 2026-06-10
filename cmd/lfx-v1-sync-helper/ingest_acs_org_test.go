// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"testing"

	sfutil "github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
	"github.com/vmihailenco/msgpack/v5"
)

// TestNormaliseOrgUserSlice confirms nil and [] are treated as equivalent.
func TestNormaliseOrgUserSlice(t *testing.T) {
	if got := normaliseOrgUserSlice(nil); got == nil {
		t.Fatal("normaliseOrgUserSlice(nil) returned nil, want empty slice")
	}
	if got := normaliseOrgUserSlice([]*b2bOrgUser{}); got == nil {
		t.Fatal("normaliseOrgUserSlice([]) returned nil")
	}
}

// TestMergeOrgUsersWithACS covers the core merge scenarios.
func TestMergeOrgUsersWithACS(t *testing.T) {
	ctx := context.Background()

	alice := makeOrgUser("alice", "writer")

	t.Run("empty existing + one ACS user = one entry added", func(t *testing.T) {
		merged, added := mergeOrgUsersWithACS(ctx, nil, []acsOrgGrantUser{{Username: "alice", Email: "alice@example.com"}}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged, got %d", len(merged))
		}
	})

	t.Run("ACS user already in v2 is not duplicated", func(t *testing.T) {
		username := "alice"
		existing := []*b2bOrgUser{{Username: &username, Email: "alice@example.com", InvitedAs: "writer"}}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsOrgGrantUser{{Username: "alice", Email: "alice@example.com"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added (already present), got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged (no duplicates), got %d", len(merged))
		}
	})

	t.Run("legacy auth0-prefixed v2 username matches plain ACS username", func(t *testing.T) {
		username := "auth0|alice"
		existing := []*b2bOrgUser{{Username: &username, Email: "alice@example.com", InvitedAs: "writer"}}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsOrgGrantUser{{Username: "alice", Email: "alice@example.com"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added (legacy auth0| entry treated as present), got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged (no duplicates), got %d", len(merged))
		}
	})

	t.Run("duplicate ACS auth0-prefixed and plain usernames are not both added", func(t *testing.T) {
		merged, added := mergeOrgUsersWithACS(ctx, nil, []acsOrgGrantUser{
			{Username: "auth0|alice", Email: "alice@example.com"},
			{Username: "alice", Email: "alice@example.com"},
		}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("want 1 merged, got %d", len(merged))
		}
		if merged[0].Username == nil || *merged[0].Username != "alice" {
			t.Errorf("want plain username %q, got %v", "alice", merged[0].Username)
		}
	})

	t.Run("existing v2-only user is preserved", func(t *testing.T) {
		existing := []*b2bOrgUser{alice}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsOrgGrantUser{{Username: "bob", Email: "bob@example.com"}}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		if len(merged) != 2 {
			t.Fatalf("want 2 merged (alice preserved + bob added), got %d", len(merged))
		}
	})

	t.Run("multiple ACS users each retain distinct usernames", func(t *testing.T) {
		merged, added := mergeOrgUsersWithACS(ctx, nil, []acsOrgGrantUser{
			{Username: "alice", Email: "alice@example.com"},
			{Username: "bob", Email: "bob@example.com"},
		}, "writers", "sfid1", "uid1")
		if added != 2 {
			t.Fatalf("want 2 added, got %d", added)
		}
		if len(merged) != 2 {
			t.Fatalf("want 2 merged, got %d", len(merged))
		}
		got := map[string]struct{}{}
		for _, entry := range merged {
			if entry.Username == nil {
				t.Fatal("expected username pointer")
			}
			got[*entry.Username] = struct{}{}
		}
		if len(got) != 2 {
			t.Fatalf("want 2 distinct usernames, got %v", got)
		}
		if _, ok := got["alice"]; !ok {
			t.Errorf("missing alice in %v", got)
		}
		if _, ok := got["bob"]; !ok {
			t.Errorf("missing bob in %v", got)
		}
	})

	t.Run("additive union of two ACS users, one already present", func(t *testing.T) {
		existing := []*b2bOrgUser{alice}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsOrgGrantUser{
			{Username: "alice", Email: "alice@example.com"},
			{Username: "bob", Email: "bob@example.com"},
		}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added (bob only), got %d", added)
		}
		if len(merged) != 2 {
			t.Fatalf("want 2 merged, got %d", len(merged))
		}
	})

	t.Run("ACS user with empty username is skipped", func(t *testing.T) {
		_, added := mergeOrgUsersWithACS(ctx, nil, []acsOrgGrantUser{{Username: "", Email: "nobody@example.com"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added for empty username, got %d", added)
		}
	})

	t.Run("ACS user with no email is skipped (no placeholder written)", func(t *testing.T) {
		// v1HTTPClient is nil in unit tests so the KV fallback is not attempted;
		// a user with no endpoint email must be skipped, not written as @placeholder.invalid.
		merged, added := mergeOrgUsersWithACS(ctx, nil, []acsOrgGrantUser{{Username: "ghost"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("want 0 added for no-email user, got %d", added)
		}
		if len(merged) != 0 {
			t.Fatalf("want empty merged for no-email user, got %d", len(merged))
		}
	})

	t.Run("endpoint email and name populate the entry", func(t *testing.T) {
		merged, added := mergeOrgUsersWithACS(ctx, nil, []acsOrgGrantUser{{
			Username:  "maxnebl",
			Email:     "linux@23ro.de",
			FirstName: "Maximilian",
			LastName:  "Nebl",
			LogoURL:   "https://example.com/avatar.png",
		}}, "writers", "sfid1", "uid1")
		if added != 1 {
			t.Fatalf("want 1 added, got %d", added)
		}
		entry := merged[0]
		if entry.Email != "linux@23ro.de" {
			t.Errorf("email = %q, want %q", entry.Email, "linux@23ro.de")
		}
		if entry.Name == nil || *entry.Name != "Maximilian Nebl" {
			t.Errorf("name = %v, want %q", entry.Name, "Maximilian Nebl")
		}
		if entry.Avatar == nil || *entry.Avatar != "https://example.com/avatar.png" {
			t.Errorf("avatar = %v, want %q", entry.Avatar, "https://example.com/avatar.png")
		}
	})

	t.Run("nil slice treated same as empty slice (no-op path)", func(t *testing.T) {
		// nil and [] existing slices must behave identically (both have no prior entries).
		_, addedFromNil := mergeOrgUsersWithACS(ctx, normaliseOrgUserSlice(nil), []acsOrgGrantUser{{Username: "alice", Email: "alice@example.com"}}, "writers", "sfid1", "uid1")
		_, addedFromEmpty := mergeOrgUsersWithACS(ctx, normaliseOrgUserSlice([]*b2bOrgUser{}), []acsOrgGrantUser{{Username: "alice", Email: "alice@example.com"}}, "writers", "sfid1", "uid1")
		if addedFromNil != addedFromEmpty {
			t.Fatalf("nil and [] produced different added counts: %d vs %d", addedFromNil, addedFromEmpty)
		}

		// A non-nil existing slice that already contains alice must not duplicate her.
		username := "alice"
		existing := []*b2bOrgUser{{Username: &username, Email: "alice@example.com", InvitedAs: "writer"}}
		merged, added := mergeOrgUsersWithACS(ctx, existing, []acsOrgGrantUser{{Username: "alice", Email: "alice@example.com"}}, "writers", "sfid1", "uid1")
		if added != 0 {
			t.Fatalf("alice already exists: expected 0 added, got %d", added)
		}
		if len(merged) != 1 {
			t.Fatalf("expected 1 entry in merged result, got %d", len(merged))
		}
	})
}

// TestNormalizeSFID18 confirms sfutil.Normalize18 produces the canonical 18-char
// SFID, is idempotent, and rejects invalid inputs.
func TestNormalizeSFID18(t *testing.T) {
	// 15-char fixture; expected 18-char computed from the checksum algorithm.
	const id15 = "0014100000Te0OK"
	const want18 = "0014100000Te0OKAAZ"

	got, err := sfutil.Normalize18(id15)
	if err != nil {
		t.Fatalf("sfutil.Normalize18(%q) error: %v", id15, err)
	}
	if got != want18 {
		t.Fatalf("sfutil.Normalize18(%q) = %q, want %q", id15, got, want18)
	}

	// Idempotency: 18-char input returns the same 18-char output.
	got2, err := sfutil.Normalize18(want18)
	if err != nil {
		t.Fatalf("sfutil.Normalize18(%q) (idempotency) error: %v", want18, err)
	}
	if got2 != want18 {
		t.Fatalf("sfutil.Normalize18 not idempotent: %q != %q", got2, want18)
	}

	// Error cases.
	if _, err := sfutil.Normalize18(""); err == nil {
		t.Fatal("expected error for empty input")
	}
	if _, err := sfutil.Normalize18("tooshort"); err == nil {
		t.Fatal("expected error for too-short input")
	}
	if _, err := sfutil.Normalize18("invalid!chars!!"); err == nil {
		t.Fatal("expected error for invalid chars")
	}
}

// TestIsLiveMemberOrgAccount covers the decode + deletion + membership filter path.
func TestIsLiveMemberOrgAccount(t *testing.T) {
	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		return b
	}
	mustMsgpack := func(v any) []byte {
		b, err := msgpack.Marshal(v)
		if err != nil {
			t.Fatalf("msgpack.Marshal: %v", err)
		}
		return b
	}

	tests := []struct {
		name    string
		data    []byte
		want    bool
		wantErr bool
	}{
		{name: "nil payload", data: nil, want: false},
		{name: "empty payload", data: []byte{}, want: false},
		{
			name: "live member org (JSON)",
			data: mustJSON(map[string]any{"IsDeleted": false, "IsMember__c": true}),
			want: true,
		},
		{
			name: "live member org (msgpack)",
			data: mustMsgpack(map[string]any{"IsDeleted": false, "IsMember__c": true}),
			want: true,
		},
		{
			name: "IsDeleted=true",
			data: mustJSON(map[string]any{"IsDeleted": true, "IsMember__c": true}),
			want: false,
		},
		{
			name: "IsMember__c=false",
			data: mustJSON(map[string]any{"IsDeleted": false, "IsMember__c": false}),
			want: false,
		},
		{
			name: "IsMember__c missing",
			data: mustJSON(map[string]any{"IsDeleted": false}),
			want: false,
		},
		{
			name: "_sdc_deleted_at set (WAL soft-delete)",
			data: mustJSON(map[string]any{"IsDeleted": false, "IsMember__c": true, "_sdc_deleted_at": "2026-01-01T00:00:00Z"}),
			want: false,
		},
		{
			name: "_sdc_deleted_at empty string (not deleted)",
			data: mustJSON(map[string]any{"IsDeleted": false, "IsMember__c": true, "_sdc_deleted_at": ""}),
			want: true,
		},
		{
			name:    "IsDeleted not bool",
			data:    mustJSON(map[string]any{"IsDeleted": "yes", "IsMember__c": true}),
			want:    false,
			wantErr: true,
		},
		{
			name:    "IsMember__c not bool",
			data:    mustJSON(map[string]any{"IsDeleted": false, "IsMember__c": "yes"}),
			want:    false,
			wantErr: true,
		},
		{
			name:    "decode failure",
			data:    []byte("not-json-or-msgpack"),
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isLiveMemberOrgAccount(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// helpers

func makeOrgUser(username, invitedAs string) *b2bOrgUser {
	return &b2bOrgUser{
		Email:     username + "@example.com",
		Username:  &username,
		InvitedAs: invitedAs,
	}
}
